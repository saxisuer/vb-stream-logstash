package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.OutputRenderer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 双路对拍场景 1（v2 Task 8 验收核心）——同一 PG 18 容器、同一批 DML，engine 逻辑解码路
 * （in-process {@code PgReplicationSession + TransactionAssembler + ConsoleRenderer}，经
 * {@link EnginePathRunner}）与 wal 直解路（{@link WalSource} DML 面 + {@link OutputRenderer}）
 * 各自独立槽位捕获 CDC 输出行，归一化（按 xid 求交集）后逐行 diff 断言空。
 *
 * <p><b>对齐裁定（任务书 §Interfaces）</b>：xid 与 commitLsn/commitTs 双路必然一致
 * （同一 server 同一提交记录），唯一不稳定面是两路起停窗口造成的事务集合差——对拍前先按
 * xid 求交集，断言交集非空 + 交集内逐行 diff 空 + 窗口外事务在失败信息中说明。头行
 * {@code changes=N} 的已知语义差：engine 取 aborted 过滤前到达面（非流式形态下被回滚的
 * 子事务行从未到达组装器），wal 取桶记账（过滤前）——SAVEPOINT 场景 engine N=实付、
 * wal N=记账，断言按 "engine.changes == 行数 && wal.changes >= 行数" 归一化。</p>
 *
 * <p><b>截断 UPDATE 面（Task 8.5 更新）</b>：截断新元组的重建在 {@code XactGrouper} 已
 * 落（FULL 身份经旧元组字节拼装，离线锚在 {@code XactGrouperTest} 手造字节用例）；场景
 * 2 的 UPDATE 全列赋值保留（首尾列都变的对照形态），场景 6 追加中段列 UPDATE（首尾列
 * 不变——截断诱发值形态 + {@code skippedTruncatedRows==0} 哨兵）。<b>真实 PG 分布注记
 * （REL_18 heapam.c 实源钉）</b>：前缀/后缀省略门是 {@code !RelationIsLogicallyLogged}
 * （wal_level &lt; logical），与 CONTAINS_OLD 旧元组记录（需 ≥ logical）互斥——本环境
 * 逻辑流下用户表 UPDATE 恒全量记录，截断重建/跳过双计数恒 0 作回归哨兵。</p>
 *
 * <p>场景组（任务书场景矩阵第 1 组 + Task 8.5 场景 6）：单行事务 / 单事务多语句（I+U+D）/
 * 双连接交错事务 / 回滚零输出 / 子事务 SAVEPOINT 回滚剔除 / 截断 UPDATE 中段列。表形态
 * {@code REPLICA IDENTITY FULL}——UPDATE 与 DELETE 双路恒携带整行前像（replica identity
 * 面对称，BEFORE 渲染可对拍）。需要本机 Docker。</p>
 */
class DualPathParityIT {

    /** 两路各自的就绪/追平等待上限（毫秒）——wal 路首场景含 catalog 引导余量。 */
    private static final long AWAIT_MS = 60_000;

    /** 轮询间隔（毫秒）。 */
    private static final long POLL_MS = 200;

    /** wal 接收器终态失败的堆栈留痕通道（surefire 报告只留 toString，堆栈在此补面）。 */
    private static final Logger LOG_WAL_TERMINAL = LoggerFactory.getLogger(DualPathParityIT.class);

    /** 对拍表 DDL：六类型列（int4/text/bool/numeric/date/timestamptz 全在两路渲染矩阵交面）+ FULL 前像。 */
    private static final String[] TABLE_DDL = {
            "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, flag bool, val numeric,"
                    + " d date, ts timestamptz, PRIMARY KEY (id))",
            "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
    };

    /** 头行解析模式：TXN-BEGIN xid=.. kind=.. gid=.. commitLsn=0x.. commitTs=.. changes=..（两路同格式）。 */
    private static final Pattern HEADER = Pattern.compile(
            "TXN-BEGIN xid=(\\d+) kind=(\\S+) gid=(\\S+) commitLsn=0x([0-9a-f]+) commitTs=(\\S+) changes=(\\d+)");

    // ---- 场景 1：单行事务 ----

    /** 单行 INSERT 单事务：交集内头行/行/尾逐字节对齐。 */
    @Test
    void singleRowInsertParityAcrossPaths() throws Exception {
        runParity("单行事务", 1, conn -> {
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
        });
    }

    // ---- 场景 2：单事务多语句（I + U + D 混合，含 NULL 值）----

    /** 一笔事务内 INSERT×2（含 NULL 列）→ UPDATE×2（全列赋值防截断）→ DELETE×1：行序与值逐字节对齐。 */
    @Test
    void multiStatementTxnParityAcrossPaths() throws Exception {
        runParity("多语句事务", 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, NULL, false, NULL, NULL, NULL)");
            exec(conn, "UPDATE parity.t_parity SET id = 11, name = 'alice2', flag = false, val = 99.5,"
                    + " d = '2026-11-01', ts = timestamptz '2026-11-01 01:02:03.000001+00' WHERE id = 1");
            exec(conn, "UPDATE parity.t_parity SET id = 22, name = 'bob2', flag = true, val = 0.0001,"
                    + " d = '2026-11-02', ts = timestamptz '2026-11-02 05:06:07.123456+00' WHERE id = 2");
            exec(conn, "DELETE FROM parity.t_parity WHERE id = 11");
            conn.commit();
        });
    }

    // ---- 场景 3：双连接交错事务 ----

    /** 两连接交错写、按序提交：两路各自两个事务桶、桶内行归属正确、提交序一致。 */
    @Test
    void interleavedTxnsParityAcrossPaths() throws Exception {
        runParity("交错事务", 2, conn -> {
            try (Connection c1 = ParityEnv.newSqlConnection(); Connection c2 = ParityEnv.newSqlConnection()) {
                c1.setAutoCommit(false);
                c2.setAutoCommit(false);
                exec(c1, "INSERT INTO parity.t_parity VALUES (1, 'a1', true, 1.1, '2026-10-07',"
                        + " timestamptz '2026-10-07 01:00:00+00')");
                exec(c2, "INSERT INTO parity.t_parity VALUES (101, 'b1', false, 2.2, '2026-10-08',"
                        + " timestamptz '2026-10-08 02:00:00+00')");
                exec(c1, "INSERT INTO parity.t_parity VALUES (2, 'a2', true, 3.3, '2026-10-09',"
                        + " timestamptz '2026-10-09 03:00:00+00')");
                exec(c2, "INSERT INTO parity.t_parity VALUES (102, 'b2', false, 4.4, '2026-10-10',"
                        + " timestamptz '2026-10-10 04:00:00+00')");
                c1.commit();
                c2.commit();
            }
        });
    }

    // ---- 场景 4：回滚零输出 ----

    /** 回滚事务两路零输出（头/行/尾皆无），随后的提交事务对拍完整——证明流未被回滚污染。 */
    @Test
    void rolledBackTxnEmitsNothingOnBothPaths() throws Exception {
        runParity("回滚零输出", 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'doomed', true, 66.6,"
                    + " '2026-10-07', timestamptz '2026-10-07 07:07:07+00')");
            conn.rollback();
            conn.setAutoCommit(true);
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 'committed', false, 77.7,"
                    + " '2026-10-08', timestamptz '2026-10-08 08:08:08+00')");
        });
    }

    // ---- 场景 5：子事务 SAVEPOINT 回滚剔除 ----

    /** SAVEPOINT 子事务行被剔除（两路都只见 s1/s3）；头行 changes=N 按语义归一化（engine=实付、wal=记账=实付+1）。 */
    @Test
    void savepointSubTxnFilteredRowsParity() throws Exception {
        runParity("子事务 SAVEPOINT", 1, 1L, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 's1', true, 1.0, '2026-10-07',"
                    + " timestamptz '2026-10-07 01:01:01+00')");
            exec(conn, "SAVEPOINT sp");
            exec(conn, "INSERT INTO parity.t_parity VALUES (2, 's2-doomed', false, 2.0, '2026-10-07',"
                    + " timestamptz '2026-10-07 02:02:02+00')");
            exec(conn, "ROLLBACK TO SAVEPOINT sp");
            exec(conn, "INSERT INTO parity.t_parity VALUES (3, 's3', true, 3.0, '2026-10-07',"
                    + " timestamptz '2026-10-07 03:03:03+00')");
            conn.commit();
        });
    }

    // ---- 场景 6：截断 UPDATE 形态（中段列变更，Task 8.5）----

    /**
     * 截断 UPDATE 对拍（Task 8.5 场景 C）：UPDATE 只 SET 中段列（首列 id 与尾列 ts 不变
     * ——正是 PG 对新元组做前缀/后缀省略的值形态），REPLICA IDENTITY FULL 表双路 diff
     * 空 + wal 路 {@code skippedTruncatedRows==0}（截断行未被跳过——重建生效面或全量
     * 记录面，皆不出缺行）。
     *
     * <p><b>真实 PG 分布注记（REL_18 heapam.c 实源钉，2026-10 curl）</b>：前缀/后缀省略
     * 的门是 {@code oldbuf==newbuf && !RelationIsLogicallyLogged && !XLogCheckBufferNeedsBackup}
     * ——本环境 wal_level=logical 下用户表 UPDATE <b>恒全量记录新元组</b>（且 CONTAINS_OLD
     * 与截断在实源门上互斥）。故本场景实际钉住的是：中段列 UPDATE 的 FULL 前像/后像双路
     * 渲染逐字节等价 + 截断跳过计数零的回归哨兵；截断重建本体（拼装坐标/前缀取旧元组自身
     * t_hoff）由 {@code XactGrouperTest} 手造字节用例离线钉。</p>
     */
    @Test
    void midColumnUpdateFullIdentityParityAndZeroTruncationSkips() throws Exception {
        long skipped = runParity("截断 UPDATE 中段列", 1, conn -> {
            conn.setAutoCommit(false);
            exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'alice', true, 12.345,"
                    + " '2026-10-07', timestamptz '2026-10-07 04:34:56.789012+00')");
            exec(conn, "UPDATE parity.t_parity SET name = 'alice-mid', flag = false, val = 99.5,"
                    + " d = '2026-11-01' WHERE id = 1");   // 首列 id / 尾列 ts 不变
            conn.commit();
        });
        assertEquals(0, skipped, "截断 UPDATE 跳过计数应为 0（重建生效/全量记录，不出缺行）");
    }

    // ---- 对拍骨架 ----

    /**
     * 单场景对拍骨架（无 aborted 形态——walAborted=0 委派全参档）。
     *
     * @param scenario     场景名（断言消息上下文）
     * @param expectedTxns 场景内已提交（产生用户表行）的事务数——两路各自的追平目标
     * @param dml          DML 执行器（收一条普通连接，语句异常即测试失败）
     * @return wal 路会话累计的截断 UPDATE 跳过计数（停流后读——close 含接收线程 join）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private long runParity(String scenario, long expectedTxns, SqlConsumer<Connection> dml) throws Exception {
        return runParity(scenario, expectedTxns, 0L, dml);
    }

    /**
     * 单场景对拍骨架：重置环境（槽/发布/表重建）→ 两路起流 → 执行 DML → 各自等待追平
     * （expectedTxns 个已提交事务输出完毕）→ 停流取捕获行 → 归一化对拍。
     *
     * <p>关键步骤：①{@link ParityEnv#resetScenario(String...)} 重建表与 publication、清两路
     * 槽位（每场景独立起点，窗口差最小化）；②engine 路（{@link EnginePathRunner}）与 wal 路
     * （{@link WalSource} + {@link OutputRenderer}）各挂各的 CDC logger 捕获 appender 后
     * 依次启动；③DML 执行器跑场景语句；④engine 路等待 emittedTxns 达标、wal 路等待
     * dmlEmittedBuckets 达标（volatile 计数轮询）；⑤先停 wal 再停 engine（engine 停机含
     * 毒丸排干——已提交未输出事务不丢）；⑥{@link #assertParity(List, List, String, long)}
     * 归一化对拍。边界与异常语义：任一路等待超时/启动失败即 fail（消息带场景名）。</p>
     *
     * @param scenario      场景名（断言消息上下文）
     * @param expectedTxns  场景内已提交（产生用户表行）的事务数——两路各自的追平目标
     * @param walAborted    场景内被回滚的子事务行数（wal 记账上界断言的期望差）
     * @param dml           DML 执行器（收一条普通连接，语句异常即测试失败）
     * @return wal 路会话累计的截断 UPDATE 跳过计数（停流后读——close 含接收线程 join，
     *                     happens-before 成立；场景断言回归哨兵用）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private long runParity(String scenario, long expectedTxns, long walAborted, SqlConsumer<Connection> dml)
            throws Exception {
        ParityEnv.resetScenario(TABLE_DDL);
        EnginePathRunner engine = new EnginePathRunner(ParityEnv.engineConfig());
        ParityEnv.CdcCapture walCapture = ParityEnv.capture("org.vastdata.vbstream.walsource.cdc");
        WalSource wal = new WalSource(ParityEnv.walSourceConfig(), new OutputRenderer());
        Throwable primary = null;
        List<String> engineLines = List.of();
        List<String> walLines = List.of();
        try {
            engine.start();
            wal.start();
            try (Connection conn = ParityEnv.newSqlConnection()) {
                dml.accept(conn);
            }
            await(() -> engine.emittedTxns() >= expectedTxns,
                    scenario + ": engine 路未输出 " + expectedTxns + " 个事务（emitted="
                            + engine.emittedTxns() + ", consumerFailed=" + engine.failed() + "）");
            await(() -> wal.dmlEmittedBuckets() >= expectedTxns,
                    scenario + ": wal 路未发射 " + expectedTxns + " 个桶（buckets="
                            + wal.dmlEmittedBuckets() + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } catch (Throwable t) {
            primary = t;   // 不在 finally 内断言——finally 的断言失败会吞掉真正的根因
        } finally {
            wal.close();
            walLines = walCapture.closeAndDrain();
            engineLines = engine.stopAndDrain();
        }
        if (primary != null) {
            wal.receiverTerminalFailure().ifPresent(t -> LOG_WAL_TERMINAL.error(
                    "wal 接收器终态失败堆栈（{}）", scenario, t));
            fail(scenario + ": 对拍前置失败——" + primary + "\nengine 捕获=" + engineLines
                    + "\nwal 捕获=" + walLines, primary);
        }
        assertParity(engineLines, walLines, scenario, walAborted);
        return wal.dmlSkippedTruncatedRows();
    }

    /**
     * 归一化对拍：两路捕获行各自解析为 xid → 事务块（头行字段 + 行文本序列），按 xid 求交集，
     * 断言交集非空 + 交集内逐行 diff 空 + 头行字段（kind/gid/commitLsn/commitTs）逐字符一致。
     *
     * <p>关键步骤：①行流按 TXN-BEGIN/TXN-END 切块（行文本 {@code "  [n] "} 前缀原样保留，
     * 行号两路天然同序）；②窗口差处理——交集外的 xid 只进失败信息描述，不作断言失败（起停
     * 窗口差异属两路固有）；③交集内断言：行文本序列与尾行逐字节相等、kind/gid/commitLsn
     * （hex）/commitTs（ISO-8601 字符串）相等；changes 归一化——engine.changes == 行数
     * （非流式形态下到达面即实付面），wal.changes &ge; 行数（桶记账 = aborted 过滤前，
     * SAVEPOINT 场景大于实付属正确语义）。边界与异常语义：任何 diff 以"场景 + xid + 两侧
     * 内容"组装失败消息（两侧全量块列表附后，防只看到首个差异）。</p>
     *
     * @param engineLines engine 路捕获行（logger org.vastdata.vbstream.cdc）
     * @param walLines    wal 路捕获行（logger org.vastdata.vbstream.walsource.cdc）
     * @param scenario    场景名（失败消息上下文）
     * @param walAborted  场景内被回滚的子事务行数（wal 桶记账与实付的期望差；无 aborted 为 0
     *                    ——Task 8.5 上界补丁：记账 = 实付 + aborted 恰等式，越过即记账异常）
     */
    private static void assertParity(List<String> engineLines, List<String> walLines, String scenario,
            long walAborted) {
        Map<Long, TxBlock> engine = parse(engineLines);
        Map<Long, TxBlock> wal = parse(walLines);
        Set<Long> intersection = new LinkedHashSet<>(engine.keySet());
        intersection.retainAll(wal.keySet());
        assertFalse(intersection.isEmpty(), () -> scenario + ": 对拍交集为空——engine xids=" + engine.keySet()
                + ", wal xids=" + wal.keySet());

        Set<Long> engineOnly = new LinkedHashSet<>(engine.keySet());
        engineOnly.removeAll(wal.keySet());
        Set<Long> walOnly = new LinkedHashSet<>(wal.keySet());
        walOnly.removeAll(engine.keySet());

        for (Long xid : intersection) {
            TxBlock e = engine.get(xid);
            TxBlock w = wal.get(xid);
            assertEquals(e.rows, w.rows, () -> scenario + ": xid=" + xid + " 行序列 diff");
            assertEquals(e.endLine, w.endLine, () -> scenario + ": xid=" + xid + " TXN-END 尾行整行 diff");
            assertEquals(e.kind, w.kind, () -> scenario + ": xid=" + xid + " kind diff");
            assertEquals(e.gid, w.gid, () -> scenario + ": xid=" + xid + " gid diff");
            assertEquals(e.commitLsnHex, w.commitLsnHex, () -> scenario + ": xid=" + xid + " commitLsn diff");
            assertEquals(e.commitTs, w.commitTs, () -> scenario + ": xid=" + xid + " commitTs diff"
                    + "（engine=" + e.commitTs + ", wal=" + w.commitTs + "）");
            assertEquals(e.rows.size(), (int) e.changes, () -> scenario + ": xid=" + xid
                    + " engine changes 应等于实付行数（非流式形态到达面即实付面）");
            assertEquals(w.rows.size() + walAborted, (int) w.changes, () -> scenario + ": xid=" + xid
                    + " wal changes 记账异常: 期望=实付 " + w.rows.size() + " + 场景 aborted "
                    + walAborted + "，实得 " + w.changes);
        }
        // 窗口外事务集合说明：不作断言失败，但两路捕获了交集外事务时在控制台留痕（起停窗口差异归因面）
        if (!engineOnly.isEmpty() || !walOnly.isEmpty()) {
            ParityEnv.logParityNotice(scenario, engineOnly, walOnly);
        }
    }

    /**
     * 捕获行流 → xid 有序事务块：TXN-BEGIN 开块（正则解头字段），{@code "  [n] "} 行文本
     * 逐条入块，TXN-END 闭块（尾行格式两路恒同——xid 相同即相等，不需单独保存）。
     *
     * <p>边界与异常语义：块未闭合/头行字段缺失抛 AssertionError（格式漂移即对拍基建失效，
     * 优于静默漏比对）。</p>
     *
     * @param lines 捕获行
     * @return xid → 块（保持出现序）
     */
    private static Map<Long, TxBlock> parse(List<String> lines) {
        Map<Long, TxBlock> out = new LinkedHashMap<>();
        TxBlock cur = null;
        for (String line : lines) {
            if (line.startsWith("TXN-BEGIN ")) {
                Matcher m = HEADER.matcher(line);
                assertTrue(m.matches(), "TXN-BEGIN 头行格式漂移: " + line);
                cur = new TxBlock(Long.parseLong(m.group(1)), m.group(2), m.group(3),
                        m.group(4), m.group(5), Long.parseLong(m.group(6)));
                assertTrue(out.putIfAbsent(cur.xid, cur) == null, "同 xid 重复 TXN-BEGIN: " + line);
            } else if (line.startsWith("  [")) {
                assertNotNull(cur, "行文本先于 TXN-BEGIN: " + line);
                cur.rows.add(line);
            } else if (line.startsWith("TXN-END")) {
                assertNotNull(cur, "TXN-END 先于 TXN-BEGIN: " + line);
                cur.endLine = line;
                cur = null;
            } else {
                fail("未知 CDC 行格式（TXN-BEGIN / \"  [n] \" / TXN-END 三前缀均不匹配）: " + line);
            }
        }
        assertNull(cur, "事务块未闭合（TXN-END 缺失）");
        assertFalse(out.isEmpty(), "捕获行无任何事务块: " + lines);
        return out;
    }

    /**
     * 轮询等待条件成立（{@link #POLL_MS} 间隔、{@link #AWAIT_MS} 上限），超时 fail。
     *
     * @param cond 条件（幂等读）
     * @param what 等待对象描述（超时消息上下文）
     * @throws InterruptedException 睡眠被中断
     */
    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MS;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + what);
            }
            Thread.sleep(POLL_MS);
        }
    }

    /**
     * 在连接上顺序执行语句（各自独立执行——事务边界由调用方的 autoCommit/commit 控制）。
     *
     * @param conn 连接
     * @param sqls 语句
     * @throws SQLException 任一语句失败
     */
    private static void exec(Connection conn, String... sqls) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : sqls) {
                st.execute(sql);
            }
        }
    }

    /** 单参 DML 执行器（可抛 SQLException 的受检消费者—— {@link java.util.function.Consumer} 不携检查异常）。 */
    @FunctionalInterface
    private interface SqlConsumer<T> {

        /** 执行场景 DML。 @param t 连接 @throws Exception 底层异常 */
        void accept(T t) throws Exception;
    }

    /**
     * 对拍事务块：头行解析字段（xid/kind/gid/commitLsn hex/commitTs/changes）+ 行文本序列
     * （保持两路输出原样——含行号前缀，行号由各自 flush 期独立分配但序列同构）+ 尾行整行
     * （TXN-END——Task 8.5 补丁：整行保存参与逐字节比，两路格式恒同
     * {@code TXN-END   xid=N}，engine 尾行无 emitted 字段故无字段级归一化）。
     */
    private static final class TxBlock {
        final long xid;
        final String kind;
        final String gid;
        final String commitLsnHex;
        final String commitTs;
        final long changes;
        final List<String> rows = new ArrayList<>();
        String endLine;

        TxBlock(long xid, String kind, String gid, String commitLsnHex, String commitTs, long changes) {
            this.xid = xid;
            this.kind = kind;
            this.gid = gid;
            this.commitLsnHex = commitLsnHex;
            this.commitTs = commitTs;
            this.changes = changes;
        }
    }
}
