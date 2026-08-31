package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamedTransactionTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_stream");
        PgTestEnv.dropSlotQuietly("slot_par");
    }

    /** spec 用例 2：64kB work_mem 下，单事务 500 行×8KB 触发流式分块下发。 */
    @Test
    void largeTransactionStreamsInSegments() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_stream(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_stream",
                "CREATE PUBLICATION pub_stream FOR TABLE t_stream",
                "TRUNCATE t_stream");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_stream", "pub_stream"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO t_stream VALUES (?, repeat('x', 8192))")) {
                    for (int i = 0; i < 500; i++) {
                        ps.setInt(1, i);
                        ps.executeUpdate();
                    }
                }
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<PgOutputMessage> messages = harness.messages();
            List<PgOutputMessage.StreamStart> starts = messages.stream()
                    .filter(m -> m instanceof PgOutputMessage.StreamStart)
                    .map(m -> (PgOutputMessage.StreamStart) m).toList();
            assertFalse(starts.isEmpty(), "应出现流式块 StreamStart，实际消息类型: "
                    + messages.stream().map(m -> m.getClass().getSimpleName()).distinct().toList());
            assertTrue(starts.get(0).firstSegment(), "首个流块 firstSegment 应为 true");

            long streamedXid = starts.get(0).xid();
            PgOutputMessage.Insert streamedInsert = messages.stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert i && i.streamXid().isPresent())
                    .map(m -> (PgOutputMessage.Insert) m)
                    .findFirst().orElseThrow(() -> new AssertionError("流块内 Insert 应带 streamXid"));
            assertEquals(streamedXid, streamedInsert.streamXid().getAsLong());

            long insertCount = messages.stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert).count();
            assertEquals(500, insertCount, "500 行 Insert 应全部流式下发");

            assertTrue(messages.stream().anyMatch(m -> m instanceof PgOutputMessage.StreamCommit));
            assertTrue(messages.stream().noneMatch(m -> m instanceof PgOutputMessage.Commit),
                    "流式事务最终以 StreamCommit 收尾，不应再出现顶层 Commit");
        }
    }

    /** spec 用例 5：parallel 模式下子事务回滚产生带附加字段的 StreamAbort，且无错位。 */
    @Test
    void parallelModeStreamAbortCarriesExtraFields() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_par(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_par",
                "CREATE PUBLICATION pub_par FOR TABLE t_par",
                "TRUNCATE t_par");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_par", "pub_par"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                Statement st = c.createStatement();
                for (int i = 0; i < 300; i++) {
                    st.execute("INSERT INTO t_par VALUES (" + i + ", repeat('p', 4096))");
                }
                st.execute("SAVEPOINT sp1");
                // 子事务数据量必须超过 logical_decoding_work_mem(64kB) 让其变更被流式发出——
                // PG 只对"已流式"的子事务发 StreamAbort（ReorderBufferAbortSub 的
                // rbtxn_is_streamed 门槛），未流式的子事务回滚是静默丢弃。
                for (int i = 900; i < 960; i++) {
                    st.execute("INSERT INTO t_par VALUES (" + i + ", repeat('q', 8192))");
                }
                st.execute("ROLLBACK TO SAVEPOINT sp1"); // 触发 StreamAbort
                for (int i = 1000; i < 1100; i++) {
                    st.execute("INSERT INTO t_par VALUES (" + i + ", repeat('r', 4096))");
                }
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<PgOutputMessage.StreamAbort> aborts = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.StreamAbort)
                    .map(m -> (PgOutputMessage.StreamAbort) m).toList();
            assertFalse(aborts.isEmpty(), "子事务回滚应产生 StreamAbort");
            assertTrue(aborts.get(aborts.size() - 1).abortLsn().isPresent(),
                    "parallel 模式 StreamAbort 应携带 abortLsn 附加字段");
        }
    }
}
