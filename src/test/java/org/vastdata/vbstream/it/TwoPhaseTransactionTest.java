package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TwoPhaseTransactionTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_2pc");
    }

    private static void prepareTable() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_2pc(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_2pc",
                "CREATE PUBLICATION pub_2pc FOR TABLE t_2pc",
                "TRUNCATE t_2pc");
    }

    /** spec 用例 3：PREPARE → b/变更/P；COMMIT PREPARED → K，gid 匹配。 */
    @Test
    void prepareThenCommitPrepared() throws Exception {
        prepareTable();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_2pc", "pub_2pc"),
                msg -> msg instanceof PgOutputMessage.CommitPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("BEGIN");
                st.execute("INSERT INTO t_2pc VALUES (1, 'prepare-commit')");
                st.execute("PREPARE TRANSACTION 'gid_c1'");
                st.execute("COMMIT PREPARED 'gid_c1'");
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.BeginPrepare b
                    && "gid_c1".equals(b.gid())), "应出现 BeginPrepare(gid_c1)");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.Prepare p
                    && "gid_c1".equals(p.gid())), "应出现 Prepare(gid_c1)");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.CommitPrepared k
                    && "gid_c1".equals(k.gid())), "应出现 CommitPrepared(gid_c1)");
        }
    }

    /** spec 用例 4：PREPARE → ROLLBACK PREPARED → r，gid 匹配。 */
    @Test
    void prepareThenRollbackPrepared() throws Exception {
        prepareTable();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_2pc", "pub_2pc"),
                msg -> msg instanceof PgOutputMessage.RollbackPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("BEGIN");
                st.execute("INSERT INTO t_2pc VALUES (2, 'prepare-rollback')");
                st.execute("PREPARE TRANSACTION 'gid_r1'");
                st.execute("ROLLBACK PREPARED 'gid_r1'");
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.RollbackPrepared r
                    && "gid_r1".equals(r.gid())), "应出现 RollbackPrepared(gid_r1)");
            assertTrue(harness.messages().stream().noneMatch(m -> m instanceof PgOutputMessage.CommitPrepared));
        }
    }

    /**
     * spec 用例 3 深化：大 2PC 事务走流式路径——流块下发 + StreamPrepare('p') 收尾 + K 提交确认。
     * 计划稿原版先把大事务普通提交再 prepare 空事务，永远触发不了 'p'（空 prepare 只发 b/P/K）；
     * 修正：autocommit 连接 + 裸语句让 500 行整体进 PREPARE（驱动不插手事务状态，规避 pgjdbc
     * autocommit 管理与服务器端两阶段命令混用的问题）。停止条件取 K——按 WAL 顺序 'p' 必先入列。
     */
    @Test
    void largePreparedTransactionEndsWithStreamPrepare() throws Exception {
        prepareTable();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_2pc", "pub_2pc"),
                msg -> msg instanceof PgOutputMessage.CommitPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection()) { // autocommit=true，裸语句管理事务
                Statement st = c.createStatement();
                st.execute("BEGIN");
                for (int i = 0; i < 500; i++) {
                    st.execute("INSERT INTO t_2pc VALUES (" + (1000 + i) + ", repeat('z', 4096))");
                }
                st.execute("PREPARE TRANSACTION 'gid_big1'");
                st.execute("COMMIT PREPARED 'gid_big1'");
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            long insertCount = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert).count();
            assertEquals(500, insertCount, "500 行 Insert 应全部下发");

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.StreamStart),
                    "2MB 大事务（>64kB work_mem）应出现流式块");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.StreamPrepare p
                    && "gid_big1".equals(p.gid())), "流式 2PC 应以 StreamPrepare(gid_big1) 收尾");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.CommitPrepared k
                    && "gid_big1".equals(k.gid())), "COMMIT PREPARED 应产生 CommitPrepared(gid_big1)");
            assertFalse(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.Commit),
                    "prepared 事务以 p/K 收尾，不应出现顶层 Commit");
        }
    }
}
