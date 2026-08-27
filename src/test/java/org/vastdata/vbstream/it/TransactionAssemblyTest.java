package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.DmlKind;
import org.vastdata.vbstream.replication.RelationRegistry;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.TransactionKind;
import org.vastdata.vbstream.replication.TxChange;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务组装集成测试：真库构造场景（spec §6.2），SessionHarness 录制 pgoutput 消息后
 * 离线回放给新 RelationRegistry + TransactionAssembler（组装器为确定性纯状态机，
 * 回放结果与在线组装一致），断言 Transaction 完整性。容器/槽清理复用 PgTestEnv。
 */
class TransactionAssemblyTest {

    /** 本测试类共用的复制槽名：cleanup 与各场景 newConfig 统一引用，防多处字面量拼写漂移。 */
    private static final String SLOT = "slot_assembly";

    /**
     * 流式场景的行载荷表达式：512 个不同 md5(random()) 拼接 = 16384 个随机十六进制字符 ≈ 16KB
     * 且不可压缩（pg_column_size 实测存满 16384 字节）。必须不可压缩——reorder buffer 的
     * rb->size 按 TOAST 后实际数据量记账，规则图案（如 repeat(md5,512)）被 pglz 压到约 232 字节，
     * 少量行永不越过 64kB work_mem 触发流式驱逐（本项目实测踩坑）。全核心函数、无扩展依赖。
     * 规模对账：可压缩载荷并非不可用，只是需数百行规模才能靠总量越过阈值
     * （StreamedTransactionTest 的 500 行×8KB repeat 方案即此路线，与本类"少量行×不可压缩"互补）。
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
     * 离线回放录制流：Relation 先经 registry（与 Main 装配顺序一致——PgReplicationSession.run
     * 在线时内置同样顺序），全部消息喂新组装器，收集输出的 Transaction。
     * 回放中组装器的 fail-fast 同样会抛（等效在线校验）。
     */
    private static List<Transaction> assembleRecording(List<PgOutputMessage> messages) {
        RelationRegistry registry = new RelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add);
        for (PgOutputMessage m : messages) {
            registry.accept(m);
            assembler.accept(m, registry);
        }
        return out;
    }

    /**
     * 提取事务内全部行变更首列（id）的文本值序列（逐值断言用）。
     * 仅对含 after 元组的 RowChange 安全：DELETE 变更 after 为 empty，orElseThrow 会抛
     * NoSuchElementException——场景 1 含 DELETE 故不走本辅助（dml 序列 + 逐值断言已覆盖），
     * 场景 4 纯 INSERT 用它做桶归属与去重断言。
     */
    private static List<String> idsOf(Transaction t) {
        return t.changes().stream()
                .map(ch -> ((TupleValue.Text) ((RowChange) ch).after().orElseThrow()
                        .columns().get(0)).value())
                .toList();
    }

    /**
     * 场景 1（spec §6.2）：普通多语句事务组装完整性——单连接显式 BEGIN 内 I/I/U/D，COMMIT 后恰一个 NORMAL。
     * 关键步骤：建表/建 publication/TRUNCATE 在 harness.start 之前（槽从创建位点起，收不到之前的 DDL）→
     * 单 JDBC 连接显式事务执行两条 INSERT + 一条 UPDATE + 一条 DELETE 后 commit → 等首个 Commit 消息 →
     * 离线回放断言：事务数恰一、kind=NORMAL、gid 空、xid>0、changes 4 条且 dml 序列与数据值
     * （含 Relation 快照的表名/列名）正确。任何步骤失败由 awaitTermination 的超时断言或 assertEquals 抛出。
     */
    @Test
    void assemblesNormalMultiStatementTransaction() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_assembly",
                "CREATE PUBLICATION pub_assembly FOR TABLE t_assembly",
                "TRUNCATE t_assembly");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_assembly"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("INSERT INTO t_assembly VALUES (1,'a'),(2,'b')");
                st.execute("UPDATE t_assembly SET v='c' WHERE id=1");
                st.execute("DELETE FROM t_assembly WHERE id=2");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), () -> "应恰一个事务: " + summarize(txns));
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.NORMAL, t.kind());
            assertNull(t.gid());
            assertTrue(t.xid() > 0, "xid 应来自 Begin: " + t.xid());
            List<DmlKind> dmls = t.changes().stream()
                    .map(ch -> ((RowChange) ch).dml()).toList();
            assertEquals(4, t.changes().size(), () -> "实际变更: " + dmls);
            assertEquals(List.of(DmlKind.INSERT, DmlKind.INSERT, DmlKind.UPDATE, DmlKind.DELETE), dmls);
            RowChange first = (RowChange) t.changes().get(0);
            assertEquals("t_assembly", first.relation().table());
            // 列名来自变更内嵌的 Relation 快照（spec §6.2 场景 1 断言点），而非仅当前 registry 状态
            assertEquals(List.of("id", "v"), first.relation().columns().stream()
                    .map(Column::name).toList());
            assertEquals("1", ((TupleValue.Text) first.after().orElseThrow().columns().get(0)).value());
            assertEquals("a", ((TupleValue.Text) first.after().orElseThrow().columns().get(1)).value());
        }
    }

    /**
     * 场景 2（spec §6.2）：流式大事务 + 子事务回滚——单事务逐行 16KB 写入越过 64kB work_mem
     * 触发流式驱逐；SAVEPOINT 后写入的子事务变更同样被流式下发，ROLLBACK TO 由 StreamAbort 剔除。
     * 关键步骤：建表/publication/TRUNCATE 先于 harness.start（槽收不到创建位点之前的 DDL）→
     * 逐行 INSERT 30 行；载荷用 RAND_PAYLOAD（约 16KB 不可压缩随机十六进制）——rb->size 按 TOAST 后实际数据量记账，
     * 实测可压缩的 repeat 载荷被 pglz 压到百字节级、41 行总记账远不到 64kB，永不触发流式
     * （首版用 repeat(md5,512) 即栽在此，整事务走 Begin..Commit 的 NORMAL 路径）。
     * 驱逐时机：reorder buffer 在变更入队时按逐变更内存记账检查触发，与 wal writer 的周期 flush
     * 节律无关（大体量下 WAL 缓冲压力自然推动 flush，紧循环同样触发——StreamedTransactionTest
     * 无 sleep 即证）；行间 sleep 是第二道保险——让驱逐发生在事务仍进行中，更贴近 stream 模式的
     * 真实路径，非触发流式的必要条件。
     * 随机载荷下约第 5 行起 rb->size 越过 64kB 触发驱逐 → SAVEPOINT 内再写 10 行后 ROLLBACK TO
     * （子事务变更随顶层事务流式下发——PG 只对已流式子事务发 StreamAbort，未流式子事务回滚是
     * 静默丢弃）→ 尾行 → commit。断言（按依赖顺序）：先确认录制流中 StreamAbort 存在——
     * 若子事务未被流式，回滚被 PG 静默丢弃、可观察结果与本场景预期相同（31 条、无 201..210），
     * 缺此前置断言场景会在流式未触发时空转通过；再断言恰一个 STREAMED 事务、存活 31 条变更
     * （30 行 + 尾行）、被回滚子事务的 id（201..210）一条不混入。若流式未触发（kind 落成 NORMAL）
     * 或剔除失效（计数 41），对应断言即失败并给出摘要化上下文。
     */
    @Test
    void streamedTransactionWithSubtransactionRollbackAssemblesCleanly() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_stream(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_assembly_stream",
                "CREATE PUBLICATION pub_assembly_stream FOR TABLE t_assembly_stream",
                "TRUNCATE t_assembly_stream");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_assembly_stream"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                for (int i = 1; i <= 30; i++) {
                    st.execute("INSERT INTO t_assembly_stream VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                    // 第二道保险：驱逐发生在事务仍进行中（驱逐由入队内存记账触发、不依赖 wal writer 周期 flush 节律，非必要条件）
                    Thread.sleep(75);
                }
                // 子事务：写入后回滚（这些变更会被流式下发，再由 StreamAbort 剔除）
                st.execute("SAVEPOINT sp1");
                for (int i = 201; i <= 210; i++) {
                    st.execute("INSERT INTO t_assembly_stream VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                    Thread.sleep(75);
                }
                st.execute("ROLLBACK TO SAVEPOINT sp1");
                st.execute("INSERT INTO t_assembly_stream VALUES (999, 'tail')");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            // 前置存在断言（I1）：确认回滚的子事务真的走了流式路径——PG 只对已流式子事务发 StreamAbort，
            // 未流式的回滚被静默丢弃且可观察结果相同，缺此断言场景会在流式未触发时空转通过
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.StreamAbort),
                    () -> "子事务回滚应产生 StreamAbort（未流式的子事务被 PG 静默丢弃，场景将空转）");

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), () -> "应恰一个流式事务: " + summarize(txns));
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.STREAMED, t.kind(), () -> summarize(txns));
            assertEquals(31, t.changes().size(),
                    () -> "存活 30 行 + 尾行；被回滚的 10 行须被 StreamAbort 剔除: " + summarize(txns));
            // 被回滚子事务的 id（201..210）不得出现
            for (TxChange change : t.changes()) {
                RowChange rc = (RowChange) change;
                int id = Integer.parseInt(((TupleValue.Text) rc.after().orElseThrow().columns().get(0)).value());
                assertTrue(id < 201 || id == 999,
                        () -> "被回滚子事务的行混入: xid=" + t.xid() + " id=" + id);
            }
        }
    }

    /**
     * 场景 3（spec §6.2）：2PC 双路——COMMIT PREPARED 的 gid 经 CommitPrepared 输出为 TWO_PHASE 事务，
     * ROLLBACK PREPARED 的 gid 由 RollbackPrepared 静默丢弃（组装器不回调）。
     * 关键步骤：建表/publication/TRUNCATE 先于 harness.start（槽收不到创建位点之前的 DDL）→
     * 单连接顺序两个 PREPARE TRANSACTION——每次 PREPARE 后服务端事务块即结束、prepared 事务脱离会话，
     * 连接可直接开新事务，close 时无未决事务 → 新连接先 COMMIT PREPARED 提交侧、再 ROLLBACK PREPARED
     * 回滚侧；按 WAL 顺序 RollbackPrepared 是序列中最后到达的终结消息，停止条件取它保证全量录制不漏。
     * 断言：恰一个 TWO_PHASE 事务、gid 为提交侧 gid_commit、xid>0、1 条 INSERT 变更且行数据
     * （id=1、v=x）正确——回滚侧 gid 一条不输出。
     * 超时或会话异常由 awaitTermination 抛 AssertionError。
     */
    @Test
    void twoPhaseCommitAndRollbackBothHandled() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_2pc(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_assembly_2pc",
                "CREATE PUBLICATION pub_assembly_2pc FOR TABLE t_assembly_2pc",
                "TRUNCATE t_assembly_2pc");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_assembly_2pc"),
                msg -> msg instanceof PgOutputMessage.RollbackPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("INSERT INTO t_assembly_2pc VALUES (1,'x')");
                st.execute("PREPARE TRANSACTION 'gid_commit'");
                st.execute("INSERT INTO t_assembly_2pc VALUES (2,'y')");
                st.execute("PREPARE TRANSACTION 'gid_rollback'");
            }
            PgTestEnv.execSql("COMMIT PREPARED 'gid_commit'");
            PgTestEnv.execSql("ROLLBACK PREPARED 'gid_rollback'");
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), () -> "仅 COMMIT PREPARED 的 gid 输出: " + summarize(txns));
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.TWO_PHASE, t.kind(), () -> summarize(txns));
            assertEquals("gid_commit", t.gid());
            assertTrue(t.xid() > 0, "xid 应来自 BeginPrepare: " + t.xid());
            assertEquals(1, t.changes().size(), () -> "提交侧仅 1 条变更: " + summarize(txns));
            RowChange first = (RowChange) t.changes().get(0);
            assertEquals(DmlKind.INSERT, first.dml());
            // 行数据断言（场景 1 的列值写法）：确认输出的是提交侧那行的内容
            assertEquals("1", ((TupleValue.Text) first.after().orElseThrow().columns().get(0)).value());
            assertEquals("x", ((TupleValue.Text) first.after().orElseThrow().columns().get(1)).value());
        }
    }

    /**
     * 场景 4（spec §6.2/§4.2）：双连接并发大事务多桶交错——两条连接各自 BEGIN 后交替逐行写入，
     * 两事务在 WAL 中交错且各自独立越过 64kB work_mem，全局 rb->size 超限时 LargestStreamableTopTXN
     * 轮番驱逐两事务，流段交错下发（组装器多桶设计的真实路径验证）。
     * 关键步骤：A 写 id 1..10、B 写 id 100001..100010 逐行交替 INSERT，轮间 sleep 同场景 2 的定位
     * （第二道保险：驱逐发生在两事务仍进行中、更贴近 stream 模式真实路径；驱逐由入队内存记账
     * 触发、不依赖 wal writer 周期 flush 节律，非必要条件）；载荷同场景 2 用
     * RAND_PAYLOAD（16KB 不可压缩）——每行真实记账 16KB，单事务 10 行 160KB 独立超限，两事务必然
     * 都被驱逐流式 → 先 commit A 后 commit B → 等第 2 个 StreamCommit（AtomicInteger 计数避免
     * 首个 StreamCommit 即停、漏录 B）。断言：恰两个 STREAMED 事务、xid 各异、各 10 行完整、
     * 每个事务的行 id 整组落在同一桶区间（1..10 或 100001..100010，互不混流）且无重复。
     * 等待超时或会话异常由 harness 抛 AssertionError。
     */
    @Test
    void twoConcurrentLargeTransactionsAssembleIndependently() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_inter(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_assembly_inter",
                "CREATE PUBLICATION pub_assembly_inter FOR TABLE t_assembly_inter",
                "TRUNCATE t_assembly_inter");
        AtomicInteger streamCommits = new AtomicInteger();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_assembly_inter"),
                msg -> msg instanceof PgOutputMessage.StreamCommit
                        && streamCommits.incrementAndGet() >= 2)) {
            try (Connection a = PgTestEnv.newSqlConnection(); Connection b = PgTestEnv.newSqlConnection()) {
                a.setAutoCommit(false);
                b.setAutoCommit(false);
                try (Statement sa = a.createStatement(); Statement sb = b.createStatement()) {
                    for (int i = 1; i <= 10; i++) {
                        sa.execute("INSERT INTO t_assembly_inter VALUES (" + i + ", " + RAND_PAYLOAD + ")");
                        sb.execute("INSERT INTO t_assembly_inter VALUES (" + (100000 + i) + ", " + RAND_PAYLOAD + ")");
                        // 第二道保险：驱逐发生在两事务仍进行中（非必要条件，见方法 javadoc）
                        Thread.sleep(150);
                    }
                }
                a.commit();
                b.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(2, txns.size(), () -> "两并发大事务各输出一次: " + summarize(txns));
            assertNotEquals(txns.get(0).xid(), txns.get(1).xid(), () -> "两事务 xid 应各异: " + summarize(txns));
            for (Transaction t : txns) {
                assertEquals(TransactionKind.STREAMED, t.kind(), () -> summarize(List.of(t)));
                assertEquals(10, t.changes().size(), () -> "各自 10 行完整: " + summarize(List.of(t)));
            }
            // 两桶互不混流：每个事务的行 id 必须整组落在 1..10（A）或 100001..100010（B）之一
            for (Transaction t : txns) {
                List<Long> ids = idsOf(t).stream().map(Long::parseLong).toList();
                boolean isLowSet = ids.get(0) < 100;
                for (long id : ids) {
                    assertEquals(isLowSet, id < 100, () -> "事务内混入他桶行: xid=" + t.xid() + " id=" + id);
                }
                assertEquals(10, ids.stream().distinct().count(),
                        () -> "行 id 不重复: xid=" + t.xid() + " ids=" + ids);
            }
        }
    }

    /**
     * 摘要化失败消息：把事务列表压成 xid/kind/变更数三元组——本类场景的 payload 列达 16KB，
     * 直接拼 Transaction.toString() 会让断言输出爆炸，只保留定位所需的摘要。
     */
    private static String summarize(List<Transaction> txns) {
        return txns.stream()
                .map(t -> "Transaction[xid=" + t.xid() + ", kind=" + t.kind() + ", changes=" + t.changes().size() + "]")
                .toList().toString();
    }

    /**
     * 场景 5（类型覆盖，用户补充需求）：日期/时间/整数/浮点/字符串各类型列值端到端往返——
     * pgoutput 默认 text 格式下所有列值都以后端文本表示传输（TupleValue.Text），本场景验证
     * "PG 列类型 → 文本编码 → decoder → 组装 RowChange"全链路对各类型的正确性。
     * 关键步骤：8 列表（int 主键/text/bigint/double precision/numeric/date/time/timestamp）单事务
     * I/U/D → 断言 INSERT after 逐列文本值（含 UTF-8 中文与微秒精度 timestamp）；UPDATE 只改
     * 非键列（v_float/v_date）——REPLICA IDENTITY DEFAULT 下 pgoutput 不发旧镜像（before 为 empty）
     * 且新值正确；DELETE 的 before 为键元组（'K'——按 Relation 全列宽发送、非键列 NULL 占位，实测
     * [1, Null×7]），after 为 empty。
     * 预期文本表示均为 PG 后端标准输出：float8 shortest-round-trip（3.14159）、numeric 原样、
     * date/time 无时区后缀、timestamp 保留微秒。
     */
    @Test
    void multiTypeValuesRoundTripThroughAssembly() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_types("
                        + "id int PRIMARY KEY, v_text text, v_bigint bigint, v_float double precision, "
                        + "v_numeric numeric, v_date date, v_time time, v_ts timestamp)",
                "DROP PUBLICATION IF EXISTS pub_assembly_types",
                "CREATE PUBLICATION pub_assembly_types FOR TABLE t_assembly_types",
                "TRUNCATE t_assembly_types");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig(SLOT, "pub_assembly_types"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("INSERT INTO t_assembly_types VALUES (1, 'hello 世界', 42, 3.14159, "
                        + "12345.6789, '2026-08-27', '10:30:00', '2026-08-27 10:30:00.123456')");
                st.execute("UPDATE t_assembly_types SET v_float=2.5, v_date='2026-12-31' WHERE id=1");
                st.execute("DELETE FROM t_assembly_types WHERE id=1");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), () -> "应恰一个事务: " + summarize(txns));
            Transaction t = txns.get(0);
            assertEquals(3, t.changes().size(), () -> "I/U/D 三条变更: " + summarize(txns));

            // INSERT：after 整行 8 列逐值断言（各类型的标准文本表示）
            RowChange insert = (RowChange) t.changes().get(0);
            assertEquals(DmlKind.INSERT, insert.dml());
            assertEquals(List.of("1", "hello 世界", "42", "3.14159", "12345.6789",
                    "2026-08-27", "10:30:00", "2026-08-27 10:30:00.123456"),
                    textsOf(insert.after().orElseThrow()), "INSERT 各类型列值文本表示");

            // UPDATE：只改非键列——REPLICA IDENTITY DEFAULT 下不发旧镜像；after 两列新值、其余不变
            RowChange update = (RowChange) t.changes().get(1);
            assertEquals(DmlKind.UPDATE, update.dml());
            assertTrue(update.before().isEmpty(), "非键列更新不应携带旧镜像（replica identity 默认）");
            assertEquals(List.of("1", "hello 世界", "42", "2.5", "12345.6789",
                    "2026-12-31", "10:30:00", "2026-08-27 10:30:00.123456"),
                    textsOf(update.after().orElseThrow()), "UPDATE 后各类型列值");

            // DELETE：before 为键元组——pgoutput 按 Relation 全列宽发送，WAL 未记录的非键列取值为 NULL
            //（实测行为：[1, Null×7]），after 为 empty
            RowChange delete = (RowChange) t.changes().get(2);
            assertEquals(DmlKind.DELETE, delete.dml());
            assertTrue(delete.after().isEmpty(), "DELETE 无 after 元组");
            assertEquals(List.of("1", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"),
                    textsOf(delete.before().orElseThrow()), "DELETE 键元组：主键有值、非键列 NULL 占位");
        }
    }

    /**
     * 提取元组全部列值的文本序列（断言各类型列的文本表示）。
     * Text 值取其文本；Null 渲染为 "NULL"（DELETE 键元组的非键列占位）；
     * UnchangedToast/Binary 不属于本场景列，出现即以类型名暴露在断言差异里。
     */
    private static List<String> textsOf(org.vastdata.vbstream.protocol.TupleData tuple) {
        return tuple.columns().stream()
                .map(v -> v instanceof TupleValue.Text t ? t.value()
                        : v instanceof TupleValue.Null ? "NULL"
                        : "<" + v.getClass().getSimpleName() + ">")
                .toList();
    }
}
