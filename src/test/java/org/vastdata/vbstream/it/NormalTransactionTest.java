package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NormalTransactionTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_norm");
        PgTestEnv.dropSlotQuietly("slot_lsn");
    }

    @Test
    void decodesBeginRelationDmlCommitSequence() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_norm(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_norm",
                "CREATE PUBLICATION pub_norm FOR TABLE t_norm",
                "TRUNCATE t_norm");
        // 停止条件取第 3 个 Commit：三条语句三个事务，Update/Delete 消息在时序上必先于第 3 个 Commit，
        // 若只等首个 Commit 会在 Delete 解码前就断言（首跑实测竞态）。
        AtomicInteger committedTxns = new AtomicInteger();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_norm", "pub_norm"),
                msg -> msg instanceof PgOutputMessage.Commit && committedTxns.incrementAndGet() >= 3)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_norm VALUES (1, 'a')",
                    "UPDATE t_norm SET v = 'b' WHERE id = 1",
                    "DELETE FROM t_norm WHERE id = 1");
            harness.awaitTermination(Duration.ofSeconds(30));

            // 显式类型见证 <Class<?>>：裸 Object::getClass 触发 IDEA 捕获误报，改转型 lambda 则 javac 报错，见证写法两边皆过
            List<Class<?>> types = harness.messages().stream().<Class<?>>map(Object::getClass).toList();

            // 首个事务精确子序列：第一个 Begin 到其后第一个 Commit（含）
            int firstBegin = types.indexOf(PgOutputMessage.Begin.class);
            int firstCommit = types.indexOf(PgOutputMessage.Commit.class);
            assertTrue(firstBegin >= 0 && firstCommit > firstBegin, "消息序列异常: " + types);
            assertEquals(List.of(PgOutputMessage.Begin.class, PgOutputMessage.Relation.class,
                            PgOutputMessage.Insert.class, PgOutputMessage.Commit.class),
                    types.subList(firstBegin, firstCommit + 1), "首个事务消息序列不符");

            // 计数：三条 autocommit 语句 = 三个事务，容器内无其他写者
            assertEquals(3, Collections.frequency(types, PgOutputMessage.Begin.class), "Begin 计数: " + types);
            assertEquals(3, Collections.frequency(types, PgOutputMessage.Commit.class), "Commit 计数: " + types);
            assertEquals(1, Collections.frequency(types, PgOutputMessage.Insert.class), "Insert 计数: " + types);
            assertEquals(1, Collections.frequency(types, PgOutputMessage.Update.class), "Update 计数: " + types);
            assertEquals(1, Collections.frequency(types, PgOutputMessage.Delete.class), "Delete 计数: " + types);

            PgOutputMessage.Insert insert = (PgOutputMessage.Insert) harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .findFirst().orElseThrow();
            assertEquals(new TupleValue.Text("1"), insert.newTuple().columns().get(0));
            assertEquals(new TupleValue.Text("a"), insert.newTuple().columns().get(1));
        }
    }

    @Test
    void feedbackIsAdoptedByServerAndConfirmedFlushAdvances() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_lsn(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_lsn",
                "CREATE PUBLICATION pub_lsn FOR TABLE t_lsn",
                "TRUNCATE t_lsn");
        // 停止条件：第 2 个 Commit（本用例共两段各一个事务）
        AtomicInteger committedTxns = new AtomicInteger();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_lsn", "pub_lsn"),
                msg -> msg instanceof PgOutputMessage.Commit
                        && committedTxns.incrementAndGet() == 2)) {
            // 基线 = 建槽位点；两段断言都必须越过它
            String baseline = PgTestEnv.queryConfirmedFlushLsn("slot_lsn");
            assertTrue(baseline != null && !"0/0".equals(baseline), "基线应为创建位点，实际: " + baseline);

            // 第一段（客户端职责边界）：DML 后，run() 周期上报的 standby status 应被服务端采纳进
            // pg_stat_replication.flush_lsn。若反馈代码缺失，flush_lsn 恒为 NULL，本段必红（非恒真断言）。
            PgTestEnv.execSql("INSERT INTO t_lsn VALUES (1, 'x')");
            awaitPredicate(Duration.ofSeconds(10),
                    () -> PgTestEnv.standbyFlushBeyond("slot_lsn", baseline),
                    () -> "第一段失败：standby flush_lsn 10s 未越过基线 " + baseline
                            + "（status 未被服务端采纳，反馈链路断裂）");

            // 第二段（完整闭环）：再写入触发新一轮解码发送。注意 confirmed_flush_lsn 由 walsender 在
            // 解码推进时落库（candidate 机制），空闲期不推进、但确认不丢失——本次 WAL 活动应使其一步
            // 跳到客户端已确认位点（越过基线），证明第一段上报的确认被最终持久化。
            PgTestEnv.execSql("INSERT INTO t_lsn VALUES (2, 'y')");
            harness.awaitTermination(Duration.ofSeconds(30));
            awaitPredicate(Duration.ofSeconds(10),
                    () -> PgTestEnv.confirmedFlushBeyond("slot_lsn", baseline),
                    () -> "第二段失败：confirmed_flush_lsn 在解码活动后 10s 未越过基线 " + baseline
                            + "（确认未随 WAL 活动落库，当前: "
                            + PgTestEnv.queryConfirmedFlushLsn("slot_lsn") + "）");
        }
    }

    /** 条件轮询：每 250ms 检查一次，超时抛 AssertionError（消息由 describe 惰性提供）。条件与描述均可抛 SQLException。 */
    private interface ThrowingCondition {
        boolean test() throws Exception;
    }

    private interface ThrowingDescribe {
        String get() throws Exception;
    }

    private static void awaitPredicate(Duration timeout, ThrowingCondition condition,
                                       ThrowingDescribe describe) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.test()) {
            assertTrue(System.nanoTime() < deadline, describe.get());
            Thread.sleep(250);
        }
    }
}
