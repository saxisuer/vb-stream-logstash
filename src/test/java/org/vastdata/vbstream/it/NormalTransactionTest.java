package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
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
            assertTrue(types.contains(PgOutputMessage.Begin.class), "缺 Begin: " + types);
            assertTrue(types.contains(PgOutputMessage.Relation.class), "缺 Relation: " + types);
            assertTrue(types.contains(PgOutputMessage.Insert.class), "缺 Insert: " + types);
            assertTrue(types.contains(PgOutputMessage.Update.class), "缺 Update: " + types);
            assertTrue(types.contains(PgOutputMessage.Delete.class), "缺 Delete: " + types);
            assertTrue(types.contains(PgOutputMessage.Commit.class), "缺 Commit: " + types);

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
            PgTestEnv.execSql("INSERT INTO t_lsn VALUES (1, 'x')");
            harness.awaitTermination(Duration.ofSeconds(30));
            Thread.sleep(2_500); // 等 feedbackInterval=2s 的 forceUpdateStatus
        }
        try (Connection c = PgTestEnv.newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'slot_lsn'")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next(), "槽应存在");
            assertTrue(rs.getString(1) != null && !"0/0".equals(rs.getString(1)),
                    "confirmed_flush_lsn 应已推进，实际: " + rs.getString(1));
        }
    }
}
