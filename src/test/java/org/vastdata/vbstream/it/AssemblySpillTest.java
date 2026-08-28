package org.vastdata.vbstream.it;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.DmlKind;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.replication.SpillConfig;
import org.vastdata.vbstream.replication.SpillWatermarkProbe;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.TransactionKind;
import org.vastdata.vbstream.replication.TxChange;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 溢写（spill）路径集成测试四场景（assembly-spill 设计 §6 验收）：全部走 SessionHarness 的
 * raw 录制 → close → 离线回放契约——"同一条字节流"喂不同 {@link SpillConfig} 阈值的组装器，
 * 对比输出 {@link Transaction} 是否严格相等（spill 无损的确定性证明：同一字节流双配置对照，
 * 规避两次录制的数据随机性）。低水位观测经 {@link SpillWatermarkProbe}（跨包测试桥）：
 * -1 = 溢写池从未建立（spill 未发生），&gt; -1 = spill 确已发生——防"阈值配小但 spill 被短路"
 * 的假绿等价断言。容器/槽清理复用 PgTestEnv；场景表各自独立（名字带 spill 前缀），
 * setup 统一 DROP+CREATE 自愈（容器跨测试类共享，上次异常退出不留残）。harness 生命周期统一
 * try/finally 而非 try-with-resources：资源变量出块后仍可引用，保证全部断言在 close 之后读
 * 已停止增长的录制列表（"先 close 后断言"顺序契约）。
 */
class AssemblySpillTest {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(AssemblySpillTest.class);

    /** 本测试类共用的复制槽名：cleanup 与各场景 newConfig 统一引用，防多处字面量拼写漂移。 */
    private static final String SLOT = "slot_spill";

    /**
     * 大阈值对照配置：64MiB（SpillConfig 默认值量级）——spill 启用但本类场景的数据量（≤240KB）
     * 全程不会越限，作为"纯内存组装"基线；其溢写低水位应恒为 -1（溢写池从未建立）。
     */
    private static final long BIG_THRESHOLD = 64L * 1024 * 1024;

    /**
     * 小阈值溢写配置：32KiB——流式载荷行（约 16KB/行）累计 2~3 行即越限触发 spillAll，
     * 保证场景内 spill 真实发生（低水位 &gt; -1 是等价性断言的前置条件）。取 32KiB 而非
     * 64KiB 是为了与场景 1 的"服务端不驱逐"约束共存：该场景单事务约 50KB（3 行载荷），
     * 服务端 64kB work_mem 恒不驱逐（确定性 NORMAL 路径），客户端 32KiB 仍必溢写。
     */
    private static final long SMALL_THRESHOLD = 32L * 1024;

    /**
     * 行载荷表达式：512 个不同 md5(random()) 拼接 ≈ 16KB 且不可压缩（pg_column_size 实测存满
     * 16384）——reorder buffer 的 rb->size 按 TOAST 压缩后实际数据量记账，规则图案载荷会被
     * pglz 压到百字节级而永不触发流式驱逐（CLAUDE.md 领域要点，实测踩坑结论）。
     */
    private static final String RAND_PAYLOAD =
            "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512))";

    /**
     * 每用例前清残留槽：上次运行可能异常退出（如 JVM 中断、@AfterEach 前抛 Error）留下同名槽，
     * ensureSlot 会复用旧槽并从其 confirmed_flush_lsn 续传，静默吞掉本用例先于建流写入的事务。
     * dropSlotQuietly 幂等（槽不存在时静默）。
     */
    @BeforeEach
    void purgeLeftoverSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    /** 每用例后清理本测试专用槽：先杀 walsender 再删，避免槽残留跨用例干扰（槽不存在时静默）。 */
    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 场景 1（核心验收）——spill 无损等价性：同一条录制字节流喂两个仅阈值不同的组装器
     * （64MiB 恒不溢写 vs 64KiB 必溢写），输出 List&lt;Transaction&gt; 必须完全相等（record 值相等，
     * 含 xid/LSN/时间戳/逐变更元组与 Relation 快照）。
     * 构造手法：单连接 5 个串行 NORMAL 事务（混合 DML——事务 1：10 行 INSERT 含 3 行 16KB 不可
     * 压缩载荷，单事务约 50KB：服务端 64kB work_mem 恒不驱逐（确定性 NORMAL 路径，流式与否不随
     * 执行时机漂移），客户端 32KB 阈值回放中段触发 spillAll；事务 2：v_text 换 16KB 随机载荷；
     * 事务 3：同行仅改数值列，new tuple 的 v_text 为 'u'（unchanged TOAST）；事务 4：一条语句批量
     * UPDATE 3 行；事务 5：一条语句批量 DELETE 2 行）。
     * 断言依据（按依赖顺序）：小阈值实例低水位 &gt; -1 且大阈值实例 == -1（双配置确实走出不同存储
     * 路径，防"spill 被短路、两边都是纯内存"的空转等价）→ 双配置输出完全相等 → 非空转内容锚定：
     * 恰 5 个 NORMAL 事务、变更数 [10,1,1,3,2]、各事务末条 DML [U,U,U,D]、事务 3 的 v_text 列
     * 携带 UnchangedToast。录制超时/会话异常由 awaitTermination 抛 AssertionError；回放异常
     * （协议错位/Relation require miss）由 onRaw 原样上抛终结用例。
     */
    @Test
    void spilledAndMemoryReplayOfSameRecordingAreIdentical(@TempDir Path dir) throws Exception {
        PgTestEnv.execSql(
                "DROP PUBLICATION IF EXISTS pub_spill_types",
                "DROP TABLE IF EXISTS t_spill_types",
                "CREATE TABLE t_spill_types("
                        + "id int PRIMARY KEY, v_text text, v_bigint bigint, v_float double precision, "
                        + "v_numeric numeric, v_date date, v_time time, v_ts timestamp)",
                "CREATE PUBLICATION pub_spill_types FOR TABLE t_spill_types");
        AtomicInteger commits = new AtomicInteger();
        SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_spill_types"),
                msg -> msg instanceof PgOutputMessage.Commit && commits.incrementAndGet() >= 5);
        try {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                // 事务 1：10 行 INSERT——id 1..5 与 9..10 为类型字面量（空串/unicode/转义/极值/NULL），
                // id 6..8 携 16KB 随机载荷（约 50KB：服务端不驱逐走 NORMAL，客户端 32KB 阈值中途溢写）
                st.execute("INSERT INTO t_spill_types VALUES "
                        + "(1, 'hello', 100, 3.14, 1234.5678, '2026-08-27', '09:15:00', '2026-08-27 09:15:00'), "
                        + "(2, '世界 hello', -42, 2.718281828459045, 1.5, '2000-01-01', '23:59:59.999999', '2000-01-01 23:59:59.999999'), "
                        + "(3, '', 0, 0, 0.0000, '1999-12-31', '00:00:00', '1999-12-31 00:00:00'), "
                        + "(4, E'tab\\tand unicode ☃', 9223372036854775807, -1e+308, 99999999.99, '2024-02-29', '12:30:45', '2024-02-29 12:30:45'), "
                        + "(5, 'quote''s % _ \\ back', 42, 1e-15, 0.000000000001, '1970-01-01', '06:00:00', '1970-01-01 06:00:00'), "
                        + "(6, " + RAND_PAYLOAD + ", 1, 1.1, 1.11, '2026-01-01', '11:11:11', '2026-01-01 11:11:11'), "
                        + "(7, " + RAND_PAYLOAD + ", NULL, NULL, NULL, '2025-06-15', NULL, '2025-06-15 14:00:00'), "
                        + "(8, " + RAND_PAYLOAD + ", 9, 9.9, 9.99, NULL, '10:20:30', NULL), "
                        + "(9, 'row-nine 中文🙂', -1, -2.5, -3.75, '2026-08-27', '13:45:30', '2026-08-27 13:45:30.123456'), "
                        + "(10, 'row-ten', 10, 10.10, 10.101, '2026-10-10', '10:10:10', '2026-10-10 10:10:10')");
                c.commit();
                // 事务 2：text 换 16KB 随机载荷（TOAST 化）
                st.execute("UPDATE t_spill_types SET v_text = " + RAND_PAYLOAD + " WHERE id = 2");
                c.commit();
                // 事务 3：同行仅改数值列——new tuple 的 v_text 预期 'u'（unchanged TOAST，大字段不重传）
                st.execute("UPDATE t_spill_types SET v_bigint = 1234567890123, v_float = 9.87654321 WHERE id = 2");
                c.commit();
                // 事务 4：一条语句批量改 3 行——单事务 3 个 Update 变更（CASE 写死目标值保证可断言）
                st.execute("UPDATE t_spill_types SET v_float = CASE id "
                        + "WHEN 3 THEN 0.5 WHEN 4 THEN -5e+307 WHEN 5 THEN 5e-16 END "
                        + "WHERE id IN (3, 4, 5)");
                c.commit();
                // 事务 5：一条语句删 2 行——before 为主键元组（replica identity 默认）
                st.execute("DELETE FROM t_spill_types WHERE id IN (6, 7)");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(30));
        } finally {
            harness.close();
        }

        // close 后取全量录制（"先 close 后断言"顺序契约），同一条字节流喂双配置
        List<byte[]> recording = List.copyOf(harness.rawMessages());
        ReplayOutcome memoryOnly = replayRecording(recording, BIG_THRESHOLD, dir.resolve("spill-big"));
        ReplayOutcome spilled = replayRecording(recording, SMALL_THRESHOLD, dir.resolve("spill-small"));

        // 前置（防假绿）：小阈值确已溢写（溢写池建立）、大阈值确未溢写——两实例走出不同存储路径
        assertTrue(spilled.finalWatermark() > -1L, () -> "小阈值应真实溢写: watermark=" + spilled.finalWatermark());
        assertEquals(-1L, memoryOnly.finalWatermark(), () -> "大阈值不应溢写: watermark=" + memoryOnly.finalWatermark());

        // 核心断言：同一字节流双配置输出完全相等（失败消息走摘要化 firstDivergence，防 16KB 载荷爆炸）
        assertTrue(memoryOnly.transactions().equals(spilled.transactions()),
                () -> "同字节流双配置输出应严格相等: " + firstDivergence(memoryOnly.transactions(), spilled.transactions()));

        // 非空转锚定：等价不是"两边同样错"——内容与结构逐项钉死
        List<Transaction> txns = memoryOnly.transactions();
        assertEquals(5, txns.size(), () -> "恰 5 个事务: " + summarize(txns));
        for (Transaction t : txns) {
            assertEquals(TransactionKind.NORMAL, t.kind(), () -> summarize(txns));
        }
        assertEquals(List.of(10, 1, 1, 3, 2),
                txns.stream().map(t -> t.changes().size()).toList(),
                () -> "各事务变更数 [INSERT10/UPDATE1/UPDATE1/UPDATE3/DELETE2]: " + summarize(txns));
        assertEquals(List.of(DmlKind.UPDATE, DmlKind.UPDATE, DmlKind.UPDATE, DmlKind.DELETE),
                txns.stream().skip(1)
                        .map(t -> ((RowChange) t.changes().get(t.changes().size() - 1)).dml()).toList(),
                () -> "事务 2..5 末条变更的 DML 种类: " + summarize(txns));
        RowChange first = (RowChange) txns.get(0).changes().get(0);
        assertEquals(DmlKind.INSERT, first.dml());
        assertEquals("t_spill_types", first.relation().table());
        assertEquals("1", ((TupleValue.Text) first.after().orElseThrow().columns().get(0)).value());
        // 事务 3：仅改数值列，v_text（列序 1）为 unchanged TOAST——溢写回读后 'u' 标志保真
        RowChange numericOnly = (RowChange) txns.get(2).changes().get(0);
        assertTrue(numericOnly.after().orElseThrow().columns().get(1) instanceof TupleValue.UnchangedToast,
                "未动的大字段列应携带 unchanged-TOAST 标志: " + tupleShape(numericOnly.after().orElseThrow()));
    }

    /**
     * 场景 2——流式大事务交错 + spill：双连接交错手法（参照 TransactionAssemblyTest 场景 4）+
     * SAVEPOINT 子事务回滚（参照其场景 2）构造"两并发 STREAMED 事务多桶交错 + StreamAbort 剔除"，
     * 同一条录制喂 64MiB 与 64KiB 双配置回放。
     * 构造手法：A/B 两连接各自 BEGIN 后逐行交替 INSERT（各 6 行 ×16KB 不可压缩载荷，行间 150ms
     * 让驱逐发生在事务进行中）；A 再 SAVEPOINT 写 6 行（96KB 重新越过 64kB 全局水位，确保子事务
     * 变更也被流式下发）后 ROLLBACK TO（由 StreamAbort 剔除）+ 1 行尾行；先 commit A 后
     * commit B，停条件取第 2 个 StreamCommit（AtomicInteger 计数防首个即停漏录 B）。全局
     * rb->size 超限轮番驱逐两事务，流段交错下发；小阈值客户端在两桶累计约 32KB 时 spillAll
     * （多桶整体转储），其后 A/B 追加在共享 appender 上交错落盘（单桶多连续段的真实路径）。
     * 断言依据：前置录制流含 StreamAbort（未流式的子事务回滚被 PG 静默丢弃，缺此断言场景空转）→
     * 小阈值低水位 &gt; -1、大阈值 == -1 → 双配置输出完全相等 → 内容锚定：恰 2 个 STREAMED 事务、
     * xid 各异、A 桶 7 行（6+尾行，201..206 一条不混入）、B 桶 6 行、两桶各自行 id 无重复。
     */
    @Test
    void streamedInterleavedLargeTransactionsSpillEquivalently(@TempDir Path dir) throws Exception {
        PgTestEnv.execSql(
                "DROP PUBLICATION IF EXISTS pub_spill_stream",
                "DROP TABLE IF EXISTS t_spill_stream",
                "CREATE TABLE t_spill_stream(id int PRIMARY KEY, payload text)",
                "CREATE PUBLICATION pub_spill_stream FOR TABLE t_spill_stream");
        AtomicInteger streamCommits = new AtomicInteger();
        SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_spill_stream"),
                msg -> msg instanceof PgOutputMessage.StreamCommit
                        && streamCommits.incrementAndGet() >= 2);
        try {
            try (Connection a = PgTestEnv.newSqlConnection(); Connection b = PgTestEnv.newSqlConnection()) {
                a.setAutoCommit(false);
                b.setAutoCommit(false);
                try (Statement sa = a.createStatement(); Statement sb = b.createStatement()) {
                    for (int i = 1; i <= 6; i++) {
                        sa.execute("INSERT INTO t_spill_stream VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                        sb.execute("INSERT INTO t_spill_stream VALUES (" + (100000 + i) + ", " + RAND_PAYLOAD + ")");
                        // 第二道保险：驱逐发生在两事务仍进行中（驱逐由入队内存记账触发，非必要条件）
                        Thread.sleep(150);
                    }
                    // A 的子事务：写入后回滚——变更已被流式下发，回放须按 StreamAbort 剔除；
                    // 6 行×16KB=96KB 必然重新越过 64kB 全局水位触发再次驱逐（首版 2 行仅 32KB，
                    // 交错主循环的驱逐已把水位清掉、不再触发，子事务未被流式 → 回滚被 PG 静默丢弃）
                    sa.execute("SAVEPOINT sp1");
                    for (int i = 201; i <= 206; i++) {
                        sa.execute("INSERT INTO t_spill_stream VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                        Thread.sleep(75);
                    }
                    sa.execute("ROLLBACK TO SAVEPOINT sp1");
                    sa.execute("INSERT INTO t_spill_stream VALUES (999, 'tail')");
                }
                a.commit();
                b.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));
        } finally {
            harness.close();
        }

        // 前置存在断言（close 后读全量）：子事务回滚确走了流式路径，否则场景空转通过
        List<byte[]> recording = List.copyOf(harness.rawMessages());
        assertTrue(hasStreamAbort(recording),
                "子事务回滚应产生 StreamAbort（未流式的子事务被 PG 静默丢弃，场景将空转）");

        ReplayOutcome memoryOnly = replayRecording(recording, BIG_THRESHOLD, dir.resolve("spill-big"));
        ReplayOutcome spilled = replayRecording(recording, SMALL_THRESHOLD, dir.resolve("spill-small"));

        assertTrue(spilled.finalWatermark() > -1L, () -> "小阈值应真实溢写: watermark=" + spilled.finalWatermark());
        assertEquals(-1L, memoryOnly.finalWatermark(), () -> "大阈值不应溢写: watermark=" + memoryOnly.finalWatermark());
        assertTrue(memoryOnly.transactions().equals(spilled.transactions()),
                () -> "同字节流双配置输出应严格相等: " + firstDivergence(memoryOnly.transactions(), spilled.transactions()));

        // 内容锚定（等价非空转）：2 个 STREAMED 事务，A 桶 7 行（无 201/202），B 桶 6 行
        List<Transaction> txns = memoryOnly.transactions();
        assertEquals(2, txns.size(), () -> "两并发大事务各输出一次: " + summarize(txns));
        assertNotEquals(txns.get(0).xid(), txns.get(1).xid(), () -> summarize(txns));
        Transaction txnA = null;
        for (Transaction t : txns) {
            assertEquals(TransactionKind.STREAMED, t.kind(), () -> summarize(txns));
            List<Integer> ids = idsOf(t).stream().map(Integer::parseInt).toList();
            if (ids.contains(999)) {
                txnA = t;
                assertEquals(7, t.changes().size(), () -> "A 桶 6 行 + 尾行（被回滚 6 行剔除）: " + summarize(txns)
                        + " A.ids=" + idsOf(t) + " A.streamXids=" + streamXidsOf(t));
                for (int id : ids) {
                    assertTrue(id <= 6 || id == 999,
                            () -> "被回滚子事务的行混入（或 B 桶行混入）: xid=" + t.xid() + " ids=" + ids);
                }
            } else {
                assertEquals(6, t.changes().size(), () -> "B 桶 6 行完整: " + summarize(txns));
                for (int id : ids) {
                    assertTrue(id >= 100001 && id <= 100006,
                            () -> "B 桶外行混入: xid=" + t.xid() + " ids=" + ids);
                }
            }
            assertEquals(t.changes().size(), ids.stream().distinct().count(),
                    () -> "行 id 不重复: xid=" + t.xid() + " ids=" + ids);
        }
        assertTrue(txnA != null, () -> "应存在含尾行 999 的 A 桶事务: " + summarize(txns));
    }

    /**
     * 场景 3——DDL 的 asOf 版本渲染：大事务（阈值以上）内**同事务 DDL**——前段按旧列数插入、
     * ALTER TABLE ADD COLUMN、后段按新列数插入，服务端在事务中段重发新版本 Relation，
     * 回放必须按单元自身 seq 取"变更时刻"的表定义（VersionedRelationRegistry asOf，设计 §4.4），
     * 旧单元按新 schema 渲染即列错位。
     * 构造手法（与 brief 的双连接写法差异说明）：PG 锁序决定 conn2 的 ALTER 必然阻塞至 conn1
     * 提交之后（ADD COLUMN 取 ACCESS EXCLUSIVE，与 conn1 已持有的 ROW EXCLUSIVE 冲突），
     * "conn1 后段插入落在新 schema"在双连接手法下不可达；同事务 DDL 是同一断言意图（同一 oid
     * 前后两版 Relation + 同桶跨版本单元）在真实库上的可达构造——DDL 在自身事务内即时生效，
     * 前段 3 行（48KB）入桶溢写后继续追加后段，单桶同时含 v1/v2 两代单元。服务端侧总记账
     * 48KB+2 小行 &lt; 64kB work_mem，事务走确定性 NORMAL 路径（不依赖驱逐时机）。
     * 断言依据：前置录制流中该表的 Relation 恰按 [3 列, 4 列] 两版到达（asOf 非空转：线上确有
     * 两版）→ 小阈值（16KB）低水位 &gt; -1（3 行×16KB 必溢写）→ 恰 1 个 NORMAL 事务 5 条变更：
     * 前 3 条 relation().columns() 为 3 列且末列名 v_tag、元组 3 值，后 2 条 4 列、末列名
     * ddl_probe 且元组第 4 值等于写入值——任何按"最新版本"渲染旧单元的实现都会在此错位失败。
     * 测试尾无需清理列结构：专表 t_spill_ddl + setup DROP/CREATE 自愈，不影响其他测试。
     */
    @Test
    void ddlInsideLargeTransactionRendersAsOfRelationVersions(@TempDir Path dir) throws Exception {
        PgTestEnv.execSql(
                "DROP PUBLICATION IF EXISTS pub_spill_ddl",
                "DROP TABLE IF EXISTS t_spill_ddl",
                "CREATE TABLE t_spill_ddl(id int PRIMARY KEY, payload text, v_tag text)",
                "CREATE PUBLICATION pub_spill_ddl FOR TABLE t_spill_ddl");
        SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_spill_ddl"),
                msg -> msg instanceof PgOutputMessage.Commit);
        try {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                // 前段：3 行 16KB 载荷（48KB > 客户端 16KB 阈值 → 溢写）——Relation v1（3 列）
                for (int i = 1; i <= 3; i++) {
                    st.execute("INSERT INTO t_spill_ddl VALUES (" + i + ", " + RAND_PAYLOAD + ", 'pre-" + i + "')");
                }
                // 事务中段 DDL：服务端随后对同 oid 重发 Relation v2（4 列）
                st.execute("ALTER TABLE t_spill_ddl ADD COLUMN ddl_probe int");
                // 后段：显式列清单写入新列——Relation v2
                st.execute("INSERT INTO t_spill_ddl (id, payload, v_tag, ddl_probe) VALUES (4, 'post-a', 'tag', 40)");
                st.execute("INSERT INTO t_spill_ddl (id, payload, v_tag, ddl_probe) VALUES (5, 'post-b', 'tag', 50)");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(30));
        } finally {
            harness.close();
        }

        // 前置（close 后读全量）：该表 Relation 恰两版到达——前 3 列后 4 列（线上确有两版，asOf 有物可考）
        List<Integer> versions = harness.messages().stream()
                .filter(m -> m instanceof PgOutputMessage.Relation r && r.table().equals("t_spill_ddl"))
                .map(m -> ((PgOutputMessage.Relation) m).columns().size())
                .toList();
        assertEquals(List.of(3, 4), versions, "事务中段应重发新版本 Relation（两版各一次）");

        // 客户端阈值 16KB：3 行×约 16KB 于第 2 行即越限 spillAll，前段单元先行落盘
        List<byte[]> recording = List.copyOf(harness.rawMessages());
        ReplayOutcome spilled = replayRecording(recording, 16L * 1024, dir.resolve("spill-small"));
        assertTrue(spilled.finalWatermark() > -1L, () -> "16KB 阈值应真实溢写: watermark=" + spilled.finalWatermark());

        List<Transaction> txns = spilled.transactions();
        assertEquals(1, txns.size(), () -> "恰 1 个事务: " + summarize(txns));
        Transaction t = txns.get(0);
        assertEquals(TransactionKind.NORMAL, t.kind(), () -> summarize(txns));
        assertEquals(5, t.changes().size(), () -> "前段 3 行 + 后段 2 行: " + summarize(txns));
        for (int j = 0; j < t.changes().size(); j++) {
            final int idx = j;   // lambda 失败消息捕获用（循环变量非最终变量）
            RowChange rc = (RowChange) t.changes().get(j);
            boolean preDdl = j < 3;
            // asOf 断言核心：前段按 v1（3 列）、后段按 v2（4 列，末列 ddl_probe）渲染，各自元组列数对齐
            assertEquals(preDdl ? 3 : 4, rc.relation().columns().size(),
                    () -> "变更 #" + idx + " Relation 列数（前段旧/后段新）: " + changeDigest(t));
            assertEquals(preDdl ? "v_tag" : "ddl_probe",
                    rc.relation().columns().get(rc.relation().columns().size() - 1).name(),
                    () -> "变更 #" + idx + " 末列名: " + changeDigest(t));
            TupleData after = rc.after().orElseThrow();
            assertEquals(rc.relation().columns().size(), after.columns().size(),
                    () -> "变更 #" + idx + " 元组列数须与 Relation 对齐: " + changeDigest(t));
            if (!preDdl) {
                int expected = j == 3 ? 40 : 50;
                assertEquals(String.valueOf(expected),
                        ((TupleValue.Text) after.columns().get(3)).value(),
                        () -> "后段行 ddl_probe 值: " + changeDigest(t));
            }
        }
    }

    /**
     * 场景 4——大事务回滚后的溢写垃圾回收：spill 后整体 ROLLBACK 的流式大事务（StreamAbort 整桶
     * 丢弃）+ 随后一个小事务 COMMIT，断言输出仅含小事务、无异常、溢写低水位越过被回滚桶的区间
     * 起点——回放过程中在首个 StreamAbort 前观测到的水位即"被回滚桶的区间起点"（彼时它是唯一
     * 存活的 SPILLED 桶，其 firstIndex 即低水位；丢弃后低水位跳到 lastAppended+1，垃圾可回收）。
     * 构造手法：单连接显式事务逐行 8×16KB（128KB，服务端 64kB work_mem 必驱逐 → STREAMED；
     * 客户端 32KB 阈值在约第 2 行 spillAll）→ ROLLBACK（顶层回滚 → StreamAbort(top==sub) 整桶
     * 丢弃，已落盘单元成垃圾）→ 同连接小事务 INSERT 一行 'small-commit' 后 COMMIT（停条件取
     * 首个 Commit——被回滚事务永不产生 Commit/StreamCommit 终结消息）。
     * 断言依据：前置录制流含 StreamAbort（非流式回滚被 PG 静默丢弃、无单元入桶，场景空转）→
     * 回放首个 'A' 前低水位 &gt; -1（回滚桶确已溢写且回收有物）→ 回放结束后低水位严格大于回滚前
     * 观测值（垃圾不再挡低水位）→ 输出恰 1 个 NORMAL 事务、1 条变更且内容正确（溢写垃圾不污染
     * 后续读回放）。回放全程无异常由 onRaw 的 fail-fast 语义隐式断言（异常原样上抛即用例失败）。
     */
    @Test
    void rolledBackSpilledTransactionAdvancesWatermark(@TempDir Path dir) throws Exception {
        PgTestEnv.execSql(
                "DROP PUBLICATION IF EXISTS pub_spill_rb",
                "DROP TABLE IF EXISTS t_spill_rb",
                "CREATE TABLE t_spill_rb(id int PRIMARY KEY, payload text)",
                "CREATE PUBLICATION pub_spill_rb FOR TABLE t_spill_rb");
        SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_spill_rb"),
                msg -> msg instanceof PgOutputMessage.Commit);
        try {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                for (int i = 1; i <= 8; i++) {
                    st.execute("INSERT INTO t_spill_rb VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                    Thread.sleep(75);
                }
                c.rollback();
                // 随后小事务：被回滚大事务之后唯一应输出的事务
                st.execute("INSERT INTO t_spill_rb VALUES (999, 'small-commit')");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));
        } finally {
            harness.close();
        }

        // 前置存在断言（close 后读全量）：大事务确被流式（回滚以 StreamAbort 显式下发）
        List<byte[]> recording = List.copyOf(harness.rawMessages());
        assertTrue(hasStreamAbort(recording),
                "流式回滚应产生 StreamAbort（非流式回滚被 PG 静默丢弃，场景将空转）");

        ReplayOutcome spilled = replayRecording(recording, SMALL_THRESHOLD, dir.resolve("spill-small"));

        // 低水位推进：回滚前（桶存活，floor=其 firstIndex）< 回滚后（垃圾让位，跳到 lastAppended+1）
        assertTrue(spilled.watermarkBeforeFirstAbort() > -1L,
                () -> "首个 StreamAbort 前应已溢写（被回滚桶为存活 SPILLED 桶）: watermark="
                        + spilled.watermarkBeforeFirstAbort());
        assertTrue(spilled.finalWatermark() > spilled.watermarkBeforeFirstAbort(),
                () -> "回滚后低水位应越过被回滚桶区间起点（垃圾可回收）: 回滚前="
                        + spilled.watermarkBeforeFirstAbort() + " 回滚后=" + spilled.finalWatermark());

        // 输出仅含小事务（回滚大事务零输出、无半截事务），内容正确
        List<Transaction> txns = spilled.transactions();
        assertEquals(1, txns.size(), () -> "仅小事务输出: " + summarize(txns));
        Transaction t = txns.get(0);
        assertEquals(TransactionKind.NORMAL, t.kind(), () -> summarize(txns));
        assertEquals(1, t.changes().size(), () -> summarize(txns));
        RowChange rc = (RowChange) t.changes().get(0);
        assertEquals(DmlKind.INSERT, rc.dml());
        assertEquals("999", ((TupleValue.Text) rc.after().orElseThrow().columns().get(0)).value());
        assertEquals("small-commit", ((TupleValue.Text) rc.after().orElseThrow().columns().get(1)).value());
    }

    /**
     * 提取事务内全部行变更的 streamXid 序列（诊断辅助：定位子事务剔除失效时"哪段变更归属哪个
     * （子）xid"）。与 id 对位读取，只用于断言失败消息的摘要。
     */
    private static List<String> streamXidsOf(Transaction t) {
        return t.changes().stream()
                .map(ch -> ch.streamXid().isPresent() ? String.valueOf(ch.streamXid().getAsLong()) : "-")
                .toList();
    }

    // ---- 以下为录制窥探与离线回放的基础设施 ----

    /**
     * 录制流中是否存在 StreamAbort（类型字节 'A'）。
     * 依据：pgoutput 消息首字节即类型标识（'A' = StreamAbort，spec 附录 B.4），离线窥探无需解码；
     * 仅作"回滚确被流式下发"的前置存在断言，不参与等价性比较。
     */
    private static boolean hasStreamAbort(List<byte[]> recording) {
        return recording.stream().anyMatch(raw -> raw[0] == 'A');
    }

    /**
     * 离线回放录制流（raw 字节驱动组装器）并观测溢写低水位：全部原始字节喂**新**组装器
     * （独立 VersionedRelationRegistry——seq/版本日志随本组装器重建，与其它回放互不共享），
     * 收集输出的 Transaction，并在（a）回放结束后、（b）首个 StreamAbort('A') 喂入前两个时点
     * 经 {@link SpillWatermarkProbe} 快照低水位——(b) 供回滚场景断言"被回滚桶的区间起点"。
     * 边界与异常：回放中组装器的 fail-fast（桶缺失/Relation require miss/协议错位）原样上抛 =
     * 等效在线校验；finally 关闭组装器释放溢写池 mmap（否则 @TempDir 清理可能因映射未释放失败）。
     * 回放模式取 PARALLEL——与 PgTestEnv.newConfig 固定的 streaming 参数一致（StreamAbort 附加
     * 字段的有无由该模式决定，回放解码必须与录制时一致）。
     *
     * @param rawMessages    全量录制（close 后的确定性快照）
     * @param thresholdBytes spill 阈值（&gt;0 启用；越大越难越限）
     * @param spillDir       溢写池目录（MessageSpool 构造时自建并清空）
     * @return 输出事务列表 + 两个低水位观测点
     */
    private static ReplayOutcome replayRecording(List<byte[]> rawMessages, long thresholdBytes, Path spillDir) {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add, StreamingMode.PARALLEL,
                registry, new SpillConfig(thresholdBytes, spillDir, LegacyRollCycles.MINUTELY));
        long beforeFirstAbort = -1L;
        try {
            for (byte[] raw : rawMessages) {
                if (raw[0] == 'A' && beforeFirstAbort < 0L) {
                    beforeFirstAbort = SpillWatermarkProbe.of(assembler);   // 首个 StreamAbort 前快照
                }
                assembler.onRaw(raw);
            }
            return new ReplayOutcome(List.copyOf(out), SpillWatermarkProbe.of(assembler), beforeFirstAbort);
        } finally {
            assembler.close();
        }
    }

    /**
     * 一次离线回放的产物与观测点。
     *
     * @param transactions             组装器输出的全部事务（回调序，不可变）
     * @param finalWatermark           回放结束后的溢写低水位（-1 = 溢写池从未建立，即 spill 未发生）
     * @param watermarkBeforeFirstAbort 首个 StreamAbort('A') 喂入前的低水位快照；录制无 'A' 或其前
     *                                  溢写未发生时为 -1（供回滚场景断言被回滚桶的区间起点）
     */
    private record ReplayOutcome(List<Transaction> transactions, long finalWatermark,
                                 long watermarkBeforeFirstAbort) {
    }

    /**
     * 提取事务内全部行变更首列（id）的文本值序列（桶归属/去重断言用）。
     * 仅对含 after 元组的 RowChange 安全：DELETE 的 after 为 empty，orElseThrow 会抛——
     * 本类断言 id 的场景均为纯 INSERT 事务，不触达该边界。
     */
    private static List<String> idsOf(Transaction t) {
        return t.changes().stream()
                .map(ch -> ((TupleValue.Text) ((RowChange) ch).after().orElseThrow()
                        .columns().get(0)).value())
                .toList();
    }

    /**
     * 摘要化失败消息：把事务列表压成 xid/kind/变更数三元组——本类场景载荷列达 16KB，
     * 直接拼 Transaction.toString() 会让断言输出爆炸，只保留定位所需的摘要。
     */
    private static String summarize(List<Transaction> txns) {
        return txns.stream()
                .map(t -> "Transaction[xid=" + t.xid() + ", kind=" + t.kind() + ", changes=" + t.changes().size() + "]")
                .toList().toString();
    }

    /**
     * 双配置等价断言失败时的首个分歧定位（摘要化，防大载荷爆炸）：先比事务数，再逐事务比
     * （xid/kind/commitLsn/变更数），再在首个不等事务内逐变更定位（dml/表名/Relation 列数/
     * 元组形态——元组只展开"值形态"（Text 长度或变体类名），不展开内容）。
     * 返回首个分歧的紧凑描述；两列表相等时不应被调用（返回占位文案）。
     */
    private static String firstDivergence(List<Transaction> a, List<Transaction> b) {
        if (a.size() != b.size()) {
            return "事务数不同: " + summarize(a) + " vs " + summarize(b);
        }
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i).equals(b.get(i))) {
                continue;
            }
            Transaction ta = a.get(i);
            Transaction tb = b.get(i);
            StringBuilder sb = new StringBuilder("事务[" + i + "] 分歧: xid=" + ta.xid() + "/" + tb.xid()
                    + " kind=" + ta.kind() + "/" + tb.kind()
                    + " commitLsn=0x" + Long.toHexString(ta.commitLsn())
                    + "/0x" + Long.toHexString(tb.commitLsn())
                    + " 变更数=" + ta.changes().size() + "/" + tb.changes().size() + "; ");
            for (int j = 0; j < Math.min(ta.changes().size(), tb.changes().size()); j++) {
                if (!ta.changes().get(j).equals(tb.changes().get(j))) {
                    sb.append("首个不等变更 #").append(j).append(": ")
                            .append(describeChange(ta.changes().get(j)))
                            .append(" vs ").append(describeChange(tb.changes().get(j)));
                    return sb.toString();
                }
            }
            return sb.append("（变更前缀一致）").toString();
        }
        return "（无分歧——不应到达）";
    }

    /**
     * 单条变更的摘要描述：RowChange 展开为 dml/表名/Relation 列数/before-after 元组形态，
     * 其余 TxChange 只取类名——等价断言失败时足以定位"哪条变更、哪个维度"且不展开载荷内容。
     */
    private static String describeChange(TxChange change) {
        if (change instanceof RowChange rc) {
            return "RowChange[" + rc.dml() + " " + rc.relation().table()
                    + " 列数=" + rc.relation().columns().size()
                    + " after=" + rc.after().map(AssemblySpillTest::tupleShape).orElse("empty")
                    + " before=" + rc.before().map(AssemblySpillTest::tupleShape).orElse("empty") + "]";
        }
        return change.getClass().getSimpleName();
    }

    /**
     * 单事务全部变更的轻量摘要（场景 3 列错位诊断）：逐变更取 dml + Relation 列名序列 + 元组
     * 形态拼接为单行（载荷 16KB 也不展开内容）。
     */
    private static String changeDigest(Transaction t) {
        return t.changes().stream()
                .map(ch -> {
                    if (ch instanceof RowChange rc) {
                        return rc.dml() + " 列" + rc.relation().columns().size() + "("
                                + String.join(",", rc.relation().columns().stream()
                                        .map(col -> col.name()).toList()) + ") "
                                + rc.after().map(AssemblySpillTest::tupleShape).orElse("empty");
                    }
                    return ch.getClass().getSimpleName();
                })
                .toList().toString();
    }

    /**
     * 元组的"形态"摘要：逐列取 Text 的字符长度（16KB 载荷只占 5 个字符位）或变体类名
     * （NULL/UnchangedToast）——足以暴露列数错位与 'u' 标志丢失，不暴露内容。
     */
    private static String tupleShape(TupleData tuple) {
        return tuple.columns().stream()
                .map(v -> v instanceof TupleValue.Text x ? "text:" + x.value().length() : v.getClass().getSimpleName())
                .toList().toString();
    }
}
