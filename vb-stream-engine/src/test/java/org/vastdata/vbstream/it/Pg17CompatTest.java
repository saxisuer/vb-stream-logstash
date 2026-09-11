package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.BinaryValueDecoder;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 源库降级 PG 17 的端到端兼容性实证（2026-09-11 REL_17_STABLE vs REL_18_STABLE 源码审计的真库
 * 佐证）：引擎按 PG 18 开发，本组在 postgres:17 容器（{@link Pg17TestEnv}）上复刻关键协议面——
 * Relation 元数据（含 typmod）、binary 值模式（BinaryValueDecoder 矩阵）、流式外壳、两阶段、
 * parallel 模式 StreamAbort 附加字段。断言策略沿 BinaryOutputTest/DataTypeTest 的 oracle 习语：
 * 以 JDBC getString（原始列 text 形态，与 walsender text 模式同源）对照解码值，不硬编码期望值。
 */
class Pg17CompatTest {

    /** 本组全部槽名——@AfterAll 统一清理（类级收尾减少逐用例往返）。 */
    private static final String[] SLOTS = {"slot17_meta", "slot17_bin", "slot17_str", "slot17_2pc", "slot17_abt"};

    /**
     * 流式场景的行载荷表达式（照 TransactionAssemblyTest.RAND_PAYLOAD）：512 个不同 md5 拼接
     * ≈16KB 不可压缩——reorder buffer 按 TOAST 后实际字节记账，规则图案载荷被 pglz 压到百字节级
     * 永不越过 64kB work_mem 触发流式驱逐；全核心函数无扩展依赖。
     */
    private static final String RAND_PAYLOAD =
            "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512))";

    /** 防镜像漂移守卫：容器大版本必须真是 17——否则整组测试变成 PG 18 的假阳性复刻。 */
    @BeforeAll
    static void containerMustBePg17() throws Exception {
        assertEquals(17, Pg17TestEnv.majorVersion(), "Pg17TestEnv 容器应为 PostgreSQL 17");
    }

    @AfterAll
    static void cleanupSlots() {
        for (String slot : SLOTS) {
            Pg17TestEnv.dropSlotQuietly(slot);
        }
    }

    /**
     * Relation 消息元数据（列名/typeId/typmod）与 pg_attribute 逐列全等——PG 17 源码已证
     * logicalrep_write_attrs 的 [flags, name, typid, typmod] 写法与 18 逐字节一致（typmod 自
     * PG 10 起即下发），本用例真库实证解码对齐。oracle：attname/atttypid/atttypmod 按 attnum
     * 升序（Relation 列序 = attnum 序，跳过 dropped 列）；表定义覆盖正 typmod（varchar(16)/
     * char(8)/numeric(12,4)/timestamp(3)/timestamptz(0)）与 -1 无修饰（int/text/裸 timestamp）。
     * 边界：先建表/publication 再起 harness（槽收不到建流前的 DDL），普通事务 INSERT 后以
     * Commit 为停止条件，顺带断言 Insert 到达且不带 streamXid（普通事务冒烟）。
     */
    @Test
    void relationTypmodAlignsWithCatalog() throws Exception {
        Pg17TestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_v17_meta("
                        + " id int, c_vc varchar(16), c_char char(8), c_num numeric(12,4),"
                        + " c_ts timestamp(3), c_tstz timestamptz(0), c_plain timestamp, c_text text)",
                "DROP PUBLICATION IF EXISTS pub_v17_meta",
                "CREATE PUBLICATION pub_v17_meta FOR TABLE t_v17_meta",
                "TRUNCATE t_v17_meta");
        try (SessionHarness harness = SessionHarness.start(
                Pg17TestEnv.newConfig("slot17_meta", "pub_v17_meta"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            Pg17TestEnv.execSql("INSERT INTO t_v17_meta VALUES (1, 'vc', 'ch', 12.3456,"
                    + " '2026-09-11 10:20:30.123', '2026-09-11 10:20:30+08',"
                    + " '2026-09-11 10:20:30.456789', 'text')");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Relation relation = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Relation r && "t_v17_meta".equals(r.table()))
                    .map(m -> (PgOutputMessage.Relation) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Relation(t_v17_meta)"));
            List<String[]> catalog;
            try (Connection c = Pg17TestEnv.newSqlConnection();
                 ResultSet rs = c.createStatement().executeQuery(
                         "SELECT a.attname, a.atttypid, a.atttypmod FROM pg_attribute a"
                                 + " WHERE a.attrelid = 't_v17_meta'::regclass AND a.attnum > 0"
                                 + " AND NOT a.attisdropped ORDER BY a.attnum")) {
                catalog = new ArrayList<>();
                while (rs.next()) {
                    catalog.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3)});
                }
            }
            assertEquals(catalog.size(), relation.columns().size(), "Relation 列数应与 pg_attribute 一致");
            for (int i = 0; i < catalog.size(); i++) {
                String[] expect = catalog.get(i);
                Column col = relation.columns().get(i);
                assertEquals(expect[0], col.name(), "第 " + i + " 列名");
                assertEquals(Integer.parseInt(expect[1]), col.typeId(), "第 " + i + " 列 typeId");
                assertEquals(Integer.parseInt(expect[2]), col.typeModifier(), "第 " + i + " 列 typmod");
            }
            PgOutputMessage.Insert insert = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .map(m -> (PgOutputMessage.Insert) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Insert 消息"));
            assertTrue(insert.streamXid().isEmpty(), "普通事务 Insert 不应带 streamXid");
        }
    }

    /**
     * binary 值模式五类型族对照（BinaryOutputTest.fiveTypeFamiliesRoundTripWithCollections 的
     * PG 17 复刻）：interval 正负形态（含混合符号）、numeric/timestamp/date/time、文本与定长
     * 字符、数组矩阵（NULL 元素、引号与反斜杠转义、二维嵌套、非零 lowerBound、bool 三态、bytea
     * 数组）——'b' 载荷经 {@link BinaryValueDecoder} 解码后与 JDBC getString 全列对照（原始列
     * text 形态即服务端 output 函数输出；不用 ::text cast——bool 的 cast 输出 "true" 而
     * bool_out 是 "t"）。审计已证 17 个 typsend 函数在 REL_17/18_STABLE 逐字节一致，本用例
     * 真库实证值级对齐；不含 timestamptz（getString 带时区偏移后缀与 decoder 墙钟渲染有形态
     * 差，该语义对照已在 PG 18 侧覆盖，值字节格式由 timestamp 同族覆盖）。
     */
    @Test
    void binaryFiveTypeFamiliesMatchTextOutput() throws Exception {
        Pg17TestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_v17_bin("
                        + " c_iv interval, c_iv_neg interval,"
                        + " c_num numeric(12,4), c_ts timestamp, c_date date, c_time time,"
                        + " c_text text, c_bpchar char(8),"
                        + " c_i4s int[], c_bools boolean[], c_nulls int[],"
                        + " c_2d int[][], c_lb int[], c_bytes bytea[])",
                "DROP PUBLICATION IF EXISTS pub_v17_bin",
                "CREATE PUBLICATION pub_v17_bin FOR TABLE t_v17_bin",
                "TRUNCATE t_v17_bin");
        try (SessionHarness harness = SessionHarness.start(
                Pg17TestEnv.newConfig("slot17_bin", "pub_v17_bin", true),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            Pg17TestEnv.execSql(
                    "INSERT INTO t_v17_bin VALUES ("
                            + " '1 year 2 mons 3 days 04:05:06', '-1 mons +2 days -03:00:00.5',"
                            + " 12345678.9012, '2026-09-11 10:20:30.123456', '2026-09-11', '12:34:56.789012',"
                            + " 'hello 世界', 'ab',"
                            + " ARRAY[1, 2, NULL, 4],"
                            + " ARRAY[true, false, NULL],"
                            + " ARRAY[1, NULL, 3],"
                            + " ARRAY[[1,2],[3,4]],"
                            + " '[-2:1]={1,2,3,4}',"
                            + " ARRAY['\\xDEADBEEF'::bytea, NULL])");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Insert insert = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .map(m -> (PgOutputMessage.Insert) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Insert 消息"));
            PgOutputMessage.Relation relation = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Relation r && "t_v17_bin".equals(r.table()))
                    .map(m -> (PgOutputMessage.Relation) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Relation(t_v17_bin)"));
            int columns = relation.columns().size();
            for (int i = 0; i < columns; i++) {
                String column = relation.columns().get(i).name();
                assertInstanceOf(TupleValue.Binary.class, insert.newTuple().columns().get(i),
                        column + " 应为二进制种类");
            }
            List<String> oracle;
            try (Connection c = Pg17TestEnv.newSqlConnection();
                 ResultSet rs = c.createStatement().executeQuery("SELECT * FROM t_v17_bin")) {
                assertTrue(rs.next());
                oracle = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    oracle.add(rs.getString(i));
                }
            }
            for (int i = 0; i < columns; i++) {
                String column = relation.columns().get(i).name();
                String decoded = decodeAt(insert, relation, i);
                assertEquals(oracle.get(i), decoded, column + " 解码值应与 PG 17 文本输出一致");
            }
        }
    }

    /**
     * 流式大事务的 binary 载荷（BinaryOutputTest.streamedLargeTransactionBinaryPayloadsDecode 的
     * PG 17 复刻）：200 行 × 1KB 不可压缩 bytea 单事务越过 64kB work_mem 驱逐阈值触发流式分段
     * ——binary 模式不改变流式外壳（StreamStart/Stop/Commit 与 xid 前缀照旧），流块内 Insert
     * 携带 streamXid 且 'b' 载荷长度保真。停止条件二选一（照 ReaderThroughputTest 习语：work_mem
     * 64kB 下大事务收尾是 StreamCommit，防御性兼容 Commit）。
     */
    @Test
    void streamedLargeTransactionBinaryPayloadsDecode() throws Exception {
        Pg17TestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_v17_stream(id int, payload bytea)",
                "DROP PUBLICATION IF EXISTS pub_v17_str",
                "CREATE PUBLICATION pub_v17_str FOR TABLE t_v17_stream",
                "TRUNCATE t_v17_stream");
        try (SessionHarness harness = SessionHarness.start(
                Pg17TestEnv.newConfig("slot17_str", "pub_v17_str", true),
                msg -> msg instanceof PgOutputMessage.StreamCommit || msg instanceof PgOutputMessage.Commit)) {
            StringBuilder values = new StringBuilder();
            for (int i = 1; i <= 200; i++) {
                if (i > 1) {
                    values.append(',');
                }
                // 64 个 md5 = 2048 hex 字符 → 1024 字节随机载荷（gen_random_bytes 需 pgcrypto，容器未装）
                values.append('(').append(i).append(", decode((SELECT string_agg(md5(random()::text), '') ")
                        .append("FROM generate_series(1, 64)), 'hex'))");
            }
            Pg17TestEnv.execSql("INSERT INTO t_v17_stream VALUES " + values);
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
            assertEquals(1024, ((TupleValue.Binary) payload).value().length, "1024 字节随机载荷长度");
        }
    }

    /**
     * 两阶段提交（TwoPhaseTransactionTest.prepareThenCommitPrepared 的 PG 17 复刻）：裸语句
     * BEGIN/INSERT/PREPARE/COMMIT PREPARED（autocommit 连接，驱动不插手服务器端两阶段命令），
     * 断言 b/P/K 三消息按 gid 匹配——PG 17 的 pg_create_logical_replication_slot 4 参调用（第 5 参
     * failover 默认 false）与 two_phase 槽属性在真库上走通。
     */
    @Test
    void twoPhasePrepareCommitPreparedByGid() throws Exception {
        Pg17TestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_v17_2pc(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_v17_2pc",
                "CREATE PUBLICATION pub_v17_2pc FOR TABLE t_v17_2pc",
                "TRUNCATE t_v17_2pc");
        try (SessionHarness harness = SessionHarness.start(
                Pg17TestEnv.newConfig("slot17_2pc", "pub_v17_2pc"),
                msg -> msg instanceof PgOutputMessage.CommitPrepared)) {
            try (Connection c = Pg17TestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("BEGIN");
                st.execute("INSERT INTO t_v17_2pc VALUES (1, 'pg17-prepare-commit')");
                st.execute("PREPARE TRANSACTION 'gid_pg17_vb'");
                st.execute("COMMIT PREPARED 'gid_pg17_vb'");
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.BeginPrepare b
                            && "gid_pg17_vb".equals(b.gid())), "应出现 BeginPrepare(gid_pg17_vb)");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.Prepare p
                            && "gid_pg17_vb".equals(p.gid())), "应出现 Prepare(gid_pg17_vb)");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.CommitPrepared k
                            && "gid_pg17_vb".equals(k.gid())), "应出现 CommitPrepared(gid_pg17_vb)");
        }
    }

    /**
     * 流式事务的子事务回滚携带 parallel 附加字段（TransactionAssemblyTest 流式回滚场景的协议面
     * 裁剪）：30 行 × 16KB 不可压缩载荷越过驱逐阈值后 SAVEPOINT 内再写 10 行并 ROLLBACK TO——
     * PG 对已流式子事务发 StreamAbort，parallel 模式下该消息附加 abortLsn/abortTimestamp 两个
     * I64 字段（REL_17/18 的 logicalrep_write_stream_abort 同构，源码已证），本用例真库实证
     * decoder 读到了这两个字段。关键断言分三层：①StreamAbort 存在（前置——未流式子事务的回滚
     * 被 PG 静默丢弃，缺此断言场景空转通过）；②abortLsn/abortTimestamp 均非 empty——若 PG 17
     * 未按 parallel 形态附加，decoder 会因剩余字节检查抛 ProtocolMisalignmentException 或字段
     * 恒 empty；③全流解码零异常（harness 的 failure 通道，awaitTermination 已隐含）。
     */
    @Test
    void streamedSubtransactionAbortCarriesParallelFields() throws Exception {
        Pg17TestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_v17_abt(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_v17_abt",
                "CREATE PUBLICATION pub_v17_abt FOR TABLE t_v17_abt",
                "TRUNCATE t_v17_abt");
        try (SessionHarness harness = SessionHarness.start(
                Pg17TestEnv.newConfig("slot17_abt", "pub_v17_abt"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = Pg17TestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                for (int i = 1; i <= 30; i++) {
                    st.execute("INSERT INTO t_v17_abt VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                    Thread.sleep(75); // 行间停顿让驱逐发生在事务进行中，贴近 stream 真实路径
                }
                st.execute("SAVEPOINT sp1");
                for (int i = 201; i <= 210; i++) {
                    st.execute("INSERT INTO t_v17_abt VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                    Thread.sleep(75);
                }
                st.execute("ROLLBACK TO SAVEPOINT sp1");
                st.execute("INSERT INTO t_v17_abt VALUES (999, 'tail')");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60)); // 会话/解码异常经 failure 通道在此上抛
            PgOutputMessage.StreamAbort abort = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.StreamAbort)
                    .map(m -> (PgOutputMessage.StreamAbort) m)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("子事务回滚应产生 StreamAbort（未流式的子事务"
                            + "回滚被 PG 静默丢弃，场景将空转）"));
            assertTrue(abort.abortLsn().isPresent(), "parallel 模式 StreamAbort 应附加 abortLsn");
            assertTrue(abort.abortTimestamp().isPresent(), "parallel 模式 StreamAbort 应附加 abortTimestamp");
        }
    }

    /** 按列位取 'b' 载荷的解码结果（Relation 快照列 typeId + Insert 对位列，照 BinaryOutputTest 同款）。 */
    private static String decodeAt(PgOutputMessage.Insert insert, PgOutputMessage.Relation relation, int i) {
        return BinaryValueDecoder.decode(relation.columns().get(i).typeId(),
                ((TupleValue.Binary) insert.newTuple().columns().get(i)).value());
    }
}
