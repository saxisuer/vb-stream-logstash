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
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.PipeWatermarkProbe;
import org.vastdata.vbstream.replication.RowChange;
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
 * 管道（MessagePipe）存储路径集成测试（1.7 起单形态——组装器纯 CQ index 段记账，原"双阈值
 * 等价"场景已删，等价性移至 Task 6 的同步/异步对照）：全部走 SessionHarness 的 raw 录制 →
 * close → 离线回放契约——同一条字节流回放给组装器，数据经管道 append→readRange 往返后断言
 * 输出 {@link Transaction} 完整性（多桶交错段、事务内 DDL asOf 渲染、回滚后低水位推进删档）。
 * 低水位观测经 {@link PipeWatermarkProbe}（跨包测试桥）：CQ 删除低水位 ≥0（无 -1 哨兵——管道
 * 恒存在），正值即"有存活桶或已 append"。容器/槽清理复用 PgTestEnv；场景表各自独立（名字带
 * spill 前缀，类名 Task 8 一并处理），setup 统一 DROP+CREATE 自愈（容器跨测试类共享，上次异常
 * 退出不留残）。harness 生命周期统一 try/finally 而非 try-with-resources：资源变量出块后仍可
 * 引用，保证全部断言在 close 之后读已停止增长的录制列表（"先 close 后断言"顺序契约）。
 */
class AssemblySpillTest {

    /** 本测试类共用的复制槽名：cleanup 与各场景 newConfig 统一引用，防多处字面量拼写漂移。 */
    private static final String SLOT = "slot_spill";

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
     * 场景 2——流式大事务交错 + 多连续段：双连接交错手法（参照 TransactionAssemblyTest 场景 4）+
     * SAVEPOINT 子事务回滚（参照其场景 2）构造"两并发 STREAMED 事务多桶交错 + StreamAbort 剔除"，
     * 同一条录制喂两个独立管道目录的组装器回放（1.7 单形态，双回放兼作确定性对照）。
     * 构造手法：A/B 两连接各自 BEGIN 后逐行交替 INSERT（各 6 行 ×16KB 不可压缩载荷，行间 150ms
     * 让驱逐发生在事务进行中）；A 再 SAVEPOINT 写 6 行（96KB 重新越过 64kB 全局水位，确保子事务
     * 变更也被流式下发）后 ROLLBACK TO（由 StreamAbort 剔除）+ 1 行尾行；先 commit A 后
     * commit B，停条件取第 2 个 StreamCommit（AtomicInteger 计数防首个即停漏录 B）。全局
     * rb->size 超限轮番驱逐两事务，流段交错下发；客户端单 appender 管道上 A/B 的流段交错 append，
     * 每桶按"上一次 append 归属"切成多个连续段（单桶多段回放的真实路径）。
     * 断言依据：前置录制流含 StreamAbort（未流式的子事务回滚被 PG 静默丢弃，缺此断言场景空转）→
     * 回放后低水位 &gt; 0（消息确经管道 append，非空转）→ 两次回放输出完全相等（确定性）→
     * 内容锚定：恰 2 个 STREAMED 事务、xid 各异、A 桶 7 行（6+尾行，201..206 一条不混入）、
     * B 桶 6 行、两桶各自行 id 无重复。
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

        ReplayOutcome memoryOnly = replayRecording(recording, dir.resolve("pipe-a"));
        ReplayOutcome spilled = replayRecording(recording, dir.resolve("pipe-b"));

        assertTrue(spilled.finalWatermark() > 0L, () -> "回放应经管道 append（水位=maxAppended+1）: watermark="
                + spilled.finalWatermark());
        assertTrue(memoryOnly.transactions().equals(spilled.transactions()),
                () -> "同字节流两次回放输出应严格相等（确定性）: "
                        + firstDivergence(memoryOnly.transactions(), spilled.transactions()));

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
     * 场景 3——DDL 的 asOf 版本渲染：大事务（大体量）内**同事务 DDL**——前段按旧列数插入、
     * ALTER TABLE ADD COLUMN、后段按新列数插入，服务端在事务中段重发新版本 Relation，
     * 回放必须按单元自身 seq 取"变更时刻"的表定义（VersionedRelationRegistry asOf，设计 §4.4），
     * 旧单元按新 schema 渲染即列错位。
     * 构造手法（与 brief 的双连接写法差异说明）：PG 锁序决定 conn2 的 ALTER 必然阻塞至 conn1
     * 提交之后（ADD COLUMN 取 ACCESS EXCLUSIVE，与 conn1 已持有的 ROW EXCLUSIVE 冲突），
     * "conn1 后段插入落在新 schema"在双连接手法下不可达；同事务 DDL 是同一断言意图（同一 oid
     * 前后两版 Relation + 同桶跨版本单元）在真实库上的可达构造——DDL 在自身事务内即时生效，
     * 前段 3 行（48KB）先入管道后继续追加后段，单桶同时含 v1/v2 两代单元。服务端侧总记账
     * 48KB+2 小行 &lt; 64kB work_mem，事务走确定性 NORMAL 路径（不依赖驱逐时机）。
     * 断言依据：前置录制流中该表的 Relation 恰按 [3 列, 4 列] 两版到达（asOf 非空转：线上确有
     * 两版）→ 回放后低水位 &gt; 0（消息确经管道 append）→ 恰 1 个 NORMAL 事务 5 条变更：
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
                // 前段：3 行 16KB 载荷（48KB 大体量，保证跨 DDL 的多单元场景）——Relation v1（3 列）
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

        // 全量录制经管道回放（1.7 单形态：前段单元与后段单元同经 append→readRange 往返）
        List<byte[]> recording = List.copyOf(harness.rawMessages());
        ReplayOutcome spilled = replayRecording(recording, dir.resolve("pipe-ddl"));
        assertTrue(spilled.finalWatermark() > 0L, () -> "回放应经管道 append: watermark=" + spilled.finalWatermark());

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
     * 场景 4——大事务回滚后的管道垃圾回收：整体 ROLLBACK 的流式大事务（StreamAbort 整桶
     * 丢弃）+ 随后一个小事务 COMMIT，断言输出仅含小事务、无异常、CQ 删除低水位越过被回滚桶的
     * 区间起点——回放过程中在首个 StreamAbort 前观测到的水位即"被回滚桶的区间起点"（彼时它是
     * 唯一存活的带单元桶，其 firstIndex 即低水位；丢弃后低水位跳到 maxAppended+1，垃圾可回收）。
     * 构造手法：单连接显式事务逐行 8×16KB（128KB，服务端 64kB work_mem 必驱逐 → STREAMED）→
     * ROLLBACK（顶层回滚 → StreamAbort(top==sub) 整桶丢弃，管道里已 append 的单元成垃圾）→
     * 同连接小事务 INSERT 一行 'small-commit' 后 COMMIT（停条件取首个 Commit——被回滚事务
     * 永不产生 Commit/StreamCommit 终结消息）。
     * 断言依据：前置录制流含 StreamAbort（非流式回滚被 PG 静默丢弃、无单元入桶，场景空转）→
     * 回放首个 'A' 前低水位 &gt; 0（被回滚桶存活且回收有物）→ 回放结束后低水位严格大于回滚前
     * 观测值（垃圾不再挡低水位）→ 输出恰 1 个 NORMAL 事务、1 条变更且内容正确（管道垃圾不污染
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

        ReplayOutcome spilled = replayRecording(recording, dir.resolve("pipe-rb"));

        // 低水位推进：回滚前（桶存活，floor=其 firstIndex）< 回滚后（垃圾让位，跳到 maxAppended+1）
        assertTrue(spilled.watermarkBeforeFirstAbort() > 0L,
                () -> "首个 StreamAbort 前被回滚桶应已 append 且存活（其 firstIndex 即水位）: watermark="
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
     * 离线回放录制流（raw 字节驱动组装器）并观测 CQ 删除低水位：全部原始字节喂**新**组装器
     * （独立 VersionedRelationRegistry——seq/版本日志随本组装器重建，与其它回放互不共享），
     * 收集输出的 Transaction，并在（a）回放结束后、（b）首个 StreamAbort('A') 喂入前两个时点
     * 经 {@link PipeWatermarkProbe} 快照低水位——(b) 供回滚场景断言"被回滚桶的区间起点"。
     * 边界与异常：回放中组装器的 fail-fast（桶缺失/Relation require miss/协议错位）原样上抛 =
     * 等效在线校验；finally 关闭组装器释放管道 mmap（否则 @TempDir 清理可能因映射未释放失败）。
     * 回放模式取 PARALLEL——与 PgTestEnv.newConfig 固定的 streaming 参数一致（StreamAbort 附加
     * 字段的有无由该模式决定，回放解码必须与录制时一致）。
     *
     * @param rawMessages 全量录制（close 后的确定性快照）
     * @param pipeDir     管道目录（MessagePipe 构造时自建并清空）
     * @return 输出事务列表 + 两个低水位观测点
     */
    private static ReplayOutcome replayRecording(List<byte[]> rawMessages, Path pipeDir) {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add, StreamingMode.PARALLEL,
                registry, new PipeConfig(pipeDir, LegacyRollCycles.MINUTELY));
        long beforeFirstAbort = -1L;
        try {
            for (byte[] raw : rawMessages) {
                if (raw[0] == 'A' && beforeFirstAbort < 0L) {
                    beforeFirstAbort = PipeWatermarkProbe.of(assembler);   // 首个 StreamAbort 前快照
                }
                assembler.onRaw(raw);
            }
            return new ReplayOutcome(List.copyOf(out), PipeWatermarkProbe.of(assembler), beforeFirstAbort);
        } finally {
            assembler.close();
        }
    }

    /**
     * 一次离线回放的产物与观测点。
     *
     * @param transactions              组装器输出的全部事务（回调序，不可变）
     * @param finalWatermark            回放结束后的 CQ 删除低水位（≥0：无存活桶时 = maxAppended+1，
     *                                  即全部已 append 条目数意义上的"垃圾上界"）
     * @param watermarkBeforeFirstAbort 首个 StreamAbort('A') 喂入前的低水位快照；录制无 'A' 时为 -1
     *                                  （供回滚场景断言被回滚桶的区间起点——彼时它是唯一存活带单元桶，
     *                                  快照即其 firstIndex）
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
