package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.BinaryValueDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * pgoutput binary 模式端到端（START_REPLICATION 传 binary 'on'，PG 16+；容器 PG 18）：类型矩阵列的
 * 'b' 载荷经 {@link BinaryValueDecoder} 解码后与 PG 自身文本输出对照；REPLICA IDENTITY FULL 旧元组与
 * TOAST 未变语义不受 binary 模式影响；流式大事务的 binary 载荷走流式外壳正常解码。
 *
 * <p>oracle 策略：无时区类型（date/time/timetz/timestamp/numeric/文本/整数/浮点/bytea/uuid）以
 * JDBC getString（原始列，text 传输形态——pgjdbc 原样返回服务端 output 函数的输出，与 walsender
 * text 模式同源；注意不能用 {@code ::text} cast——bool 的 cast 输出 "true" 而 bool_out 是 "t"）；
 * timestamptz 的 getString 带时区偏移后缀而 decoder 按系统时区渲染墙钟（形态不同），改走语义
 * 对照——decoder 输出 parse 回 Instant 与 JDBC getTimestamp 等价断言（pgjdbc 把普通连接的
 * TimeZone 设为 JVM 默认，与 decoder 的 systemDefault 同源）。断言均在 harness 块内做（停止条件
 * 锚最后一个预期消息，被断言消息全部先于它到达，照 DataTypeTest 习语）。
 */
class BinaryOutputTest {

    /** 类型矩阵表列数（含降级对照的 jsonb/interval 两列）。 */
    private static final int COLUMN_COUNT = 21;

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_bin");
        PgTestEnv.dropSlotQuietly("slot_bins");
    }

    @Test
    void binaryValuesDecodeAcrossTypeMatrix() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_bin ("
                        + " c_bool boolean, c_small smallint, c_int integer, c_big bigint,"
                        + " c_oid oid,"
                        + " c_real real, c_double double precision,"
                        + " c_num numeric(30,6),"
                        + " c_varchar varchar(64), c_text text, c_char char(8), c_json json,"
                        + " c_bytea bytea,"
                        + " c_date date, c_time time, c_timetz timetz,"
                        + " c_ts timestamp, c_tstz timestamptz, c_uuid uuid,"
                        + " c_jsonb jsonb, c_interval interval)",
                "DROP PUBLICATION IF EXISTS pub_bin",
                "CREATE PUBLICATION pub_bin FOR TABLE t_bin",
                "TRUNCATE t_bin");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_bin", "pub_bin", true),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_bin VALUES ("
                            + " true, -32768, 2147483647, 9223372036854775807,"
                            + " 4294967295,"
                            + " 1.5, 2.718281828459045,"
                            + " 12345678901234567890.123456,"
                            + " 'hello varchar', 'hello text 世界', 'abc', '{\"k\": 1}',"
                            + " '\\xDEADBEEF',"
                            + " '2026-09-09', '12:34:56.789012', '12:34:56.789+08',"
                            + " '2026-08-27 10:20:30.123456', '2026-08-27 10:20:30.123456+08',"
                            + " 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',"
                            + " '{\"k\": 1}', '1 year 2 mons 3 days 04:05:06')");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Insert insert = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .map(m -> (PgOutputMessage.Insert) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Insert 消息"));
            assertTrue(insert.streamXid().isEmpty(), "顶层消息不应带 streamXid");
            assertEquals(COLUMN_COUNT, insert.newTuple().columns().size(), "列数应与表定义一致");

            PgOutputMessage.Relation relation = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Relation r && "t_bin".equals(r.table()))
                    .map(m -> (PgOutputMessage.Relation) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Relation(t_bin)"));

            // 全列 'b' 种类（binary 模式下所有数据列走 typsend）
            for (int i = 0; i < COLUMN_COUNT; i++) {
                String column = relation.columns().get(i).name();
                assertInstanceOf(TupleValue.Binary.class, insert.newTuple().columns().get(i),
                        column + " 应为二进制种类");
            }

            // oracle ①：JDBC getString（原始列 text 传输形态，与 walsender text 模式同源；不含 tstz 与矩阵外两列）
            List<String> textOracle;
            try (Connection c = PgTestEnv.newSqlConnection();
                 ResultSet rs = c.createStatement().executeQuery(
                         "SELECT c_bool, c_small, c_int, c_big,"
                                 + " c_oid, c_real, c_double, c_num,"
                                 + " c_varchar, c_text, c_char, c_json,"
                                 + " c_bytea, c_date, c_time, c_timetz,"
                                 + " c_ts, c_uuid FROM t_bin")) {
                assertTrue(rs.next());
                textOracle = new ArrayList<>();
                for (int i = 1; i <= 18; i++) {
                    textOracle.add(rs.getString(i));
                }
            }
            int[] textColumns = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18};
            for (int j = 0; j < textColumns.length; j++) {
                int i = textColumns[j];
                String column = relation.columns().get(i).name();
                String decoded = decodeAt(insert, relation, i);
                assertEquals(textOracle.get(j), decoded, column + " 解码值应与 PG 文本输出一致");
            }

            // oracle ②：timestamptz 语义对照（decoder 墙钟字符串 parse 回 Instant == JDBC getTimestamp）
            Instant expectedTstz;
            try (Connection c = PgTestEnv.newSqlConnection();
                 ResultSet rs = c.createStatement().executeQuery("SELECT c_tstz FROM t_bin")) {
                assertTrue(rs.next());
                expectedTstz = rs.getTimestamp(1).toInstant();
            }
            Instant actualTstz = LocalDateTime.parse(decodeAt(insert, relation, 17).replace(' ', 'T'))
                    .atZone(ZoneId.systemDefault()).toInstant();
            assertEquals(expectedTstz, actualTstz,
                    "c_tstz 解码时刻应与 JDBC 读取等价（时区偏移后缀的形态差异之外语义一致）");

            // 矩阵外类型降级：jsonb(3802)/interval(1186) → "0x" 十六进制原文（不抛异常、可观测）
            for (int i = 19; i < 21; i++) {
                String column = relation.columns().get(i).name();
                String decoded = decodeAt(insert, relation, i);
                assertTrue(decoded.startsWith("0x"), column + " 矩阵外类型应降级十六进制: " + decoded);
            }
        }
    }

    /**
     * REPLICA IDENTITY FULL + TOAST：UPDATE 的 old 元组（'O' 整行镜像）在 binary 模式同为 'b' 种类；
     * 未修改的已 TOAST 大 bytea 在 new 元组走 'u'（TOAST 未变语义不因 binary 模式改变）。
     */
    @Test
    void replicaIdentityFullOldTupleBinaryAndToastUnchanged() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_bin_full(id int PRIMARY KEY, payload bytea, note text)",
                "ALTER TABLE t_bin_full REPLICA IDENTITY FULL",
                "DROP PUBLICATION IF EXISTS pub_bin",
                "CREATE PUBLICATION pub_bin FOR TABLE t_bin_full",
                "TRUNCATE t_bin_full");
        AtomicInteger committedTxns = new AtomicInteger();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_bin", "pub_bin", true),
                msg -> msg instanceof PgOutputMessage.Commit && committedTxns.incrementAndGet() >= 2)) {
            PgTestEnv.execSql(
                    // 4KB 不可压缩载荷确保 TOAST out-of-line 存储（随机 md5 hex 经 decode 转 bytea，pglz 压不动）
                    "INSERT INTO t_bin_full VALUES (1, decode((SELECT string_agg(md5(random()::text), '') "
                            + "FROM generate_series(1, 256)), 'hex'), 'initial')",
                    "UPDATE t_bin_full SET note = 'changed' WHERE id = 1");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Update update = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Update)
                    .map(m -> (PgOutputMessage.Update) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Update 消息"));

            // REPLICA IDENTITY FULL：old 元组存在（'O' 整行镜像）且列值为 'b'
            TupleValue oldPayload = update.oldTuple()
                    .orElseThrow(() -> new AssertionError("RI FULL 应携带旧元组"))
                    .columns().get(1);
            assertInstanceOf(TupleValue.Binary.class, oldPayload, "old 元组 payload 应为二进制种类");
            // 新元组：未修改的 TOAST 列为 'u'（值不发送），已修改列为 'b'
            assertInstanceOf(TupleValue.UnchangedToast.class, update.newTuple().columns().get(1),
                    "未修改的 TOAST 列应为 'u' 未变种类");
            assertInstanceOf(TupleValue.Binary.class, update.newTuple().columns().get(2),
                    "已修改列应为二进制种类");
        }
    }

    /**
     * 流式大事务 binary 载荷：200 行 × 1KB 不可压缩 bytea（约 200KB，越过 64kB work_mem 驱逐阈值）
     * 单事务触发流式分段——binary 模式不改变流式外壳（StreamStart/Stop/Commit 与 xid 前缀照旧），
     * 流块内 Insert 携带 streamXid 且 'b' 载荷长度保真。
     */
    @Test
    void streamedLargeTransactionBinaryPayloadsDecode() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_bin_stream(id int, payload bytea)",
                "DROP PUBLICATION IF EXISTS pub_bins",
                "CREATE PUBLICATION pub_bins FOR TABLE t_bin_stream",
                "TRUNCATE t_bin_stream");
        // 停止条件二选一（照 ReaderThroughputTest：work_mem 64kB 下大事务走流式，收尾是 StreamCommit）
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_bins", "pub_bins", true),
                msg -> msg instanceof PgOutputMessage.StreamCommit || msg instanceof PgOutputMessage.Commit)) {
            StringBuilder values = new StringBuilder();
            for (int i = 1; i <= 200; i++) {
                if (i > 1) {
                    values.append(',');
                }
                // 64 个 md5 = 2048 hex 字符 → 1024 字节随机载荷（gen_random_bytes 在 pgcrypto 扩展，容器未装）
                values.append('(').append(i).append(", decode((SELECT string_agg(md5(random()::text), '') ")
                        .append("FROM generate_series(1, 64)), 'hex'))");
            }
            PgTestEnv.execSql("INSERT INTO t_bin_stream VALUES " + values);
            harness.awaitTermination(Duration.ofSeconds(60));

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.StreamStart),
                    "200KB 单事务应触发流式分段");
            long inserts = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert).count();
            assertEquals(200, inserts, "流式大事务应完整收到 200 行");

            PgOutputMessage.Insert anyStreamed = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .map(m -> (PgOutputMessage.Insert) m)
                    .filter(m -> m.streamXid().isPresent())
                    .findFirst().orElseThrow(() -> new AssertionError("流块内 Insert 应携带 streamXid"));
            TupleValue payload = anyStreamed.newTuple().columns().get(1);
            assertInstanceOf(TupleValue.Binary.class, payload, "流块内载荷应为二进制种类");
            assertEquals(1024, ((TupleValue.Binary) payload).value().length, "gen_random_bytes(1024) 载荷长度");
        }
    }

    /** 按列位取 'b' 载荷的解码结果（Relation 快照列 typeId + Insert 对位列）。 */
    private static String decodeAt(PgOutputMessage.Insert insert, PgOutputMessage.Relation relation, int i) {
        return BinaryValueDecoder.decode(relation.columns().get(i).typeId(),
                ((TupleValue.Binary) insert.newTuple().columns().get(i)).value());
    }
}
