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
    void feedbackAdvancesConfirmedFlushLsn() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_lsn(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_lsn",
                "CREATE PUBLICATION pub_lsn FOR TABLE t_lsn",
                "TRUNCATE t_lsn");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_lsn", "pub_lsn"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            // 槽刚创建，confirmed_flush_lsn = 创建位点，即反馈必须超越的基线
            String before = PgTestEnv.queryConfirmedFlushLsn("slot_lsn");
            assertTrue(before != null && !"0/0".equals(before), "基线应为创建位点，实际: " + before);

            PgTestEnv.execSql("INSERT INTO t_lsn VALUES (1, 'x')");
            harness.awaitTermination(Duration.ofSeconds(30));

            // 会话保持打开下轮询：run() 设置 flushed LSN 后由 pgjdbc 周期状态包上报，服务端推进
            // confirmed_flush_lsn；若反馈代码缺失则 flushed 恒 0/0，永不超过基线（非恒真断言）
            String after = before;
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!PgTestEnv.confirmedFlushBeyond("slot_lsn", before)) {
                assertTrue(System.nanoTime() < deadline,
                        "confirmed_flush_lsn 10s 内未越过基线，before=" + before + ", after=" + after);
                Thread.sleep(250);
                after = PgTestEnv.queryConfirmedFlushLsn("slot_lsn");
            }
        }
    }
}
