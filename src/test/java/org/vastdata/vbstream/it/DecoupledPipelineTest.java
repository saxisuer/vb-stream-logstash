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
import org.vastdata.vbstream.replication.TransactionRecorder;
import org.vastdata.vbstream.replication.TransactionKind;
import org.vastdata.vbstream.replication.TxChange;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 解耦管道（reader → CQ 管道 → consumer，1.7 里程碑形态）三场景集成测试：录制侧仍是
 * SessionHarness 的 raw 双轨（真库真字节），回放侧从 1.6 的同步组装器升级为**异步组装器**
 * （真实双线程——测试线程充当 reader 逐条 onRaw，组装器自起的 transaction-consumer 线程
 * 消费交接队列），全部断言在 close 毒丸排干之后进行（"已提交未输出的事务不丢"的排干承诺
 * 使输出成为确定性终态）。断言语义与 1.6 溢写专项一脉相承：多桶交错段的事务完整性、
 * 事务内 DDL asOf 前后段渲染、回滚后低水位推进删档。低水位观测经 {@link PipeWatermarkProbe}
 * （跨包测试桥）：CQ 删除低水位 ≥0（无 -1 哨兵——管道恒存在），正值即"有存活桶或已 append"；
 * 回滚场景另注入一个陈旧 cycle 档名的滚动文件，验证桶完结点的 releaseBelow **实际删档**。
 * 容器/槽清理复用 PgTestEnv；场景表各自独立，setup 统一 DROP+CREATE 自愈（容器跨测试类
 * 共享，上次异常退出不留残）。
 */
class DecoupledPipelineTest {

    /** 本测试类共用的复制槽名：cleanup 与各场景 newConfig 统一引用，防多处字面量拼写漂移。 */
    private static final String SLOT = "slot_pipeline";

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
     * ①——流式大事务交错 + 多连续段：双连接交错手法（参照 TransactionAssemblyTest 场景 4）+
     * SAVEPOINT 子事务回滚（参照其场景 2）构造"两并发 STREAMED 事务多桶交错 + StreamAbort 剔除"，
     * 同一条录制先后喂两个独立管道目录的**异步**组装器回放（双回放兼作确定性对照——同步/异步
     * 的形态等价另由 DecoupledEquivalenceTest 单测锚定，此处验证异步管道在真实字节流上的确定性）。
     * 构造手法：A/B 两连接各自 BEGIN 后逐行交替 INSERT（各 6 行 ×16KB 不可压缩载荷，行间 150ms
     * 让驱逐发生在事务进行中）；A 再 SAVEPOINT 写 6 行（96KB 重新越过 64kB 全局水位，确保子事务
     * 变更也被流式下发）后 ROLLBACK TO（由 StreamAbort 剔除）+ 1 行尾行；先 commit A 后
     * commit B，停条件取第 2 个 StreamCommit（AtomicInteger 计数防首个即停漏录 B）。全局
     * rb->size 超限轮番驱逐两事务，流段交错下发；客户端单 appender 管道上 A/B 的流段交错 append，
     * 每桶按"上一次 append 归属"切成多个连续段（单桶多段回放的真实路径）。
     * 断言依据：前置录制流含 StreamAbort（未流式的子事务回滚被 PG 静默丢弃，缺此断言场景空转）→
     * 回放后低水位 &gt; 0（消息确经管道 append，非空转）→ 两次异步回放（各自独立 consumer 线程、
     * close 排干后）输出完全相等（确定性）→ 内容锚定：恰 2 个 STREAMED 事务、xid 各异、
     * A 桶 7 行（6+尾行，201..206 一条不混入）、B 桶 6 行、两桶各自行 id 无重复。
     */
    @Test
    void streamedInterleavedLargeTransactionsOutputDeterministically(@TempDir Path dir) throws Exception {
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

        ReplayOutcome replayA = replayAsync(recording, dir.resolve("pipe-a"), () -> { });
        ReplayOutcome replayB = replayAsync(recording, dir.resolve("pipe-b"), () -> { });

        assertTrue(replayB.finalWatermark() > 0L, () -> "回放应经管道 append（水位=maxAppended+1）: watermark="
                + replayB.finalWatermark());
        assertTrue(replayA.transactions().equals(replayB.transactions()),
                () -> "同字节流两次异步回放输出应严格相等（确定性）: "
                        + firstDivergence(replayA.transactions(), replayB.transactions()));

        // 内容锚定（等价非空转）：2 个 STREAMED 事务，A 桶 7 行（无 201/202），B 桶 6 行
        List<Transaction> txns = replayA.transactions();
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
     * ②——DDL 的 asOf 版本渲染：大事务（大体量）内**同事务 DDL**——前段按旧列数插入、
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
     * ddl_probe 且元组第 4 值等于写入值——任何按"最新版本"渲染旧单元的实现都会在此错位失败
     * （回放发生在 consumer 线程、经 close 排干后断言，1.7 异步形态下同一断言原样成立）。
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

        // 全量录制经异步管道回放（前段单元与后段单元同经 append→readRange 往返，consumer 线程渲染）
        List<byte[]> recording = List.copyOf(harness.rawMessages());
        ReplayOutcome replayed = replayAsync(recording, dir.resolve("pipe-ddl"), () -> { });
        assertTrue(replayed.finalWatermark() > 0L, () -> "回放应经管道 append: watermark=" + replayed.finalWatermark());

        List<Transaction> txns = replayed.transactions();
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
     * ③——大事务回滚后的管道垃圾回收：整体 ROLLBACK 的流式大事务（StreamAbort 整桶
     * 丢弃）+ 随后一个小事务 COMMIT，断言输出仅含小事务、无异常、CQ 删除低水位越过被回滚桶的
     * 区间起点——回放过程中在首个 StreamAbort 前观测到的水位即"被回滚桶的区间起点"（彼时它是
     * 唯一存活的带单元桶，其 firstIndex 即低水位；丢弃后低水位跳到 maxAppended+1，垃圾可回收）。
     * 删档实证（1.7 新增）：MINUTELY 滚动周期下一次回放内的水位推进落在本 cycle，真实数据文件
     * 永不满足删除条件——故在首个 'A' 喂入前（彼时管道已建且已 append 真实数据、被回滚桶仍存活）
     * 注入一个陈旧 cycle 档名（2020-01-01）的滚动文件，'A' 触发的整桶丢弃 → releaseBelow 应把
     * 它删除（cycle 远低于 needed-1），而队列自身的真实数据文件必须保留（不过度删除）。
     * 构造手法：单连接显式事务逐行 8×16KB（128KB，服务端 64kB work_mem 必驱逐 → STREAMED）→
     * ROLLBACK（顶层回滚 → StreamAbort(top==sub) 整桶丢弃，管道里已 append 的单元成垃圾）→
     * 同连接小事务 INSERT 一行 'small-commit' 后 COMMIT（停条件取首个 Commit——被回滚事务
     * 永不产生 Commit/StreamCommit 终结消息）。
     * 断言依据：前置录制流含 StreamAbort（非流式回滚被 PG 静默丢弃、无单元入桶，场景空转）→
     * 回放首个 'A' 前低水位 &gt; 0（被回滚桶存活且回收有物）→ 回放结束后低水位严格大于回滚前
     * 观测值（垃圾不再挡低水位）→ 注入的陈旧档文件已消失 + 真实数据文件仍在（releaseBelow
     * 实际删档且只删该删的）→ 输出恰 1 个 NORMAL 事务、1 条变更且内容正确（管道垃圾不污染
     * 后续读回放）。回放全程无异常由 onFailure 标志断言（fail-fast 时 close 后用例失败）。
     */
    @Test
    void rolledBackStreamedTransactionAdvancesWatermarkAndDeletesStaleRollFiles(@TempDir Path dir) throws Exception {
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

        // 注入的陈旧滚动文件：文件名按 MINUTELY 格式 yyyyMMdd-HHmm 解析出远早于当前 cycle 的档位；
        // 内容无关（deletableFiles 只按名解析），由 replayAsync 的首个 'A' 钩子在管道建立后创建。
        // 时序隐含依赖（1.7.1 MessagePipe 档位节流后）：注入必须先于管道对当前档位的首次扫描——
        // 节流使同档位内的删档检查按 cycle 推进延迟，若首个桶完结点先于注入，陈旧文件要到下一档位
        // 才会被扫到（MINUTELY 下分钟级），notExists 断言将难排查地红；当前录制形状首个完结点
        // 恰是首个 'A'（其前无任何 Commit/StreamCommit），依赖成立
        Path pipeDir = dir.resolve("pipe-rb");
        Path staleRoll = pipeDir.resolve("20200101-0000.cq4");
        ReplayOutcome replayed = replayAsync(recording, pipeDir, () -> {
            try {
                Files.createFile(staleRoll);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        // 低水位推进：回滚前（桶存活，floor=其 firstIndex）< 回滚后（垃圾让位，跳到 maxAppended+1）
        assertTrue(replayed.watermarkBeforeFirstAbort() > 0L,
                () -> "首个 StreamAbort 前被回滚桶应已 append 且存活（其 firstIndex 即水位）: watermark="
                        + replayed.watermarkBeforeFirstAbort());
        assertTrue(replayed.finalWatermark() > replayed.watermarkBeforeFirstAbort(),
                () -> "回滚后低水位应越过被回滚桶区间起点（垃圾可回收）: 回滚前="
                        + replayed.watermarkBeforeFirstAbort() + " 回滚后=" + replayed.finalWatermark());

        // 删档实证：陈旧 cycle 文件被桶完结点的 releaseBelow 删除，真实数据文件保留
        assertTrue(Files.notExists(staleRoll),
                () -> "低水位释放应删除陈旧 cycle 滚动文件: " + staleRoll + " 仍存在");
        assertTrue(countRollFiles(pipeDir) >= 1,
                () -> "队列真实数据滚动文件应保留（删除不得过度）: 目录=" + pipeDir);

        // 输出仅含小事务（回滚大事务零输出、无半截事务），内容正确
        List<Transaction> txns = replayed.transactions();
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

    // ---- 以下为录制窥探与异步回放的基础设施 ----

    /**
     * 录制流中是否存在 StreamAbort（类型字节 'A'）。
     * 依据：pgoutput 消息首字节即类型标识（'A' = StreamAbort，spec 附录 B.4），离线窥探无需解码；
     * 仅作"回滚确被流式下发"的前置存在断言，不参与等价性比较。
     */
    private static boolean hasStreamAbort(List<byte[]> recording) {
        return recording.stream().anyMatch(raw -> raw[0] == 'A');
    }

    /**
     * 离线回放录制流（**异步组装器**，1.7 真实双线程形态）：测试线程充当 reader 逐条 onRaw，
     * 组装器自起的 transaction-consumer 线程并行消费交接队列，close 毒丸排干后输出为确定性终态。
     * 关键步骤：新建异步组装器（独立 registry/frontier/管道目录——seq/版本日志随本组装器重建，
     * 与其它回放互不共享）→ 逐条喂 raw，遇到首个 'A' 前先快照低水位并执行 {@code atFirstAbort}
     * 钩子（回滚场景在此注入陈旧滚动文件——彼时被回滚桶仍存活且管道已 append 真实数据）→
     * finally close（毒丸 → consumer 排干余桶 → join → 关管道）→ 断言无 consumer 失败后收集产物。
     * 边界与异常：consumer 回放失败经 onFailure 置 failed 标志，close 后断言失败（fail-fast 语义
     * 与旧同步形态的异常上抛等价，异常堆栈在 consumer 的 ERROR 日志里）；close 的 join 建立测试
     * 线程读输出列表前的 happens-before；低水位在 close 后经探针读取（纯内存 reader 侧状态，
     * 管道已关也无 IO）。
     * 回放模式取 PARALLEL——与 PgTestEnv.newConfig 固定的 streaming 参数一致（StreamAbort 附加
     * 字段的有无由该模式决定，回放解码必须与录制时一致）。
     *
     * @param rawMessages 全量录制（close 后的确定性快照）
     * @param pipeDir     管道目录（MessagePipe 构造时自建并清空）
     * @param atFirstAbort 首个 StreamAbort('A') 喂入前的钩子（注入删档观测用，无操作传空 Runnable）
     * @return 输出事务列表 + 两个低水位观测点（close 排干后）
     */
    private static ReplayOutcome replayAsync(List<byte[]> rawMessages, Path pipeDir, Runnable atFirstAbort) {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        // 2.0 起组装器回调流式事件，经 TransactionRecorder 重组回整块——既有断言零改动的
        // 等价币（跨线程安全前提：close 的 join 建立读侧 happens-before，见下）
        TransactionRecorder out = new TransactionRecorder();
        AtomicLong frontier = new AtomicLong();
        AtomicBoolean consumerFailed = new AtomicBoolean();
        TransactionAssembler assembler = new TransactionAssembler(out, StreamingMode.PARALLEL,
                registry, new PipeConfig(pipeDir, LegacyRollCycles.MINUTELY),
                (msg, view) -> { }, frontier, () -> consumerFailed.set(true));
        long beforeFirstAbort = -1L;
        try {
            for (byte[] raw : rawMessages) {
                if (raw[0] == 'A' && beforeFirstAbort < 0L) {
                    beforeFirstAbort = PipeWatermarkProbe.of(assembler);   // 首个 StreamAbort 前快照
                    atFirstAbort.run();
                }
                assembler.onRaw(raw);
            }
        } finally {
            assembler.close();   // 毒丸排干：已提交未输出的事务不丢；join 建立读侧 happens-before
        }
        assertFalse(consumerFailed.get(), "consumer 回放失败（fail-fast）——异常堆栈见 transaction-consumer 的 ERROR 日志");
        return new ReplayOutcome(List.copyOf(out.transactions()), PipeWatermarkProbe.of(assembler), beforeFirstAbort);
    }

    /**
     * 统计管道目录下的滚动数据文件数（.cq4 后缀；队列元数据表 metadata.cq4t 后缀不同天然排除）。
     * 删档断言的两面之一：注入的陈旧文件应消失，而队列自身的真实数据文件（至少当前 cycle 一档）
     * 必须仍在——证明删除精确落在低水位之下而非清空目录。
     */
    private static int countRollFiles(Path pipeDir) throws IOException {
        try (Stream<Path> entries = Files.list(pipeDir)) {
            return (int) entries
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".cq4"))
                    .count();
        }
    }

    /**
     * 一次异步回放的产物与观测点。
     *
     * @param transactions              组装器输出的全部事务（回调序，不可变；close 排干后的终态）
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
     * 双回放等价断言失败时的首个分歧定位（摘要化，防大载荷爆炸）：先比事务数，再逐事务比
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
                    + " after=" + rc.after().map(DecoupledPipelineTest::tupleShape).orElse("empty")
                    + " before=" + rc.before().map(DecoupledPipelineTest::tupleShape).orElse("empty") + "]";
        }
        return change.getClass().getSimpleName();
    }

    /**
     * 单事务全部变更的轻量摘要（②列错位诊断）：逐变更取 dml + Relation 列名序列 + 元组
     * 形态拼接为单行（载荷 16KB 也不展开内容）。
     */
    private static String changeDigest(Transaction t) {
        return t.changes().stream()
                .map(ch -> {
                    if (ch instanceof RowChange rc) {
                        return rc.dml() + " 列" + rc.relation().columns().size() + "("
                                + String.join(",", rc.relation().columns().stream()
                                        .map(col -> col.name()).toList()) + ") "
                                + rc.after().map(DecoupledPipelineTest::tupleShape).orElse("empty");
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
