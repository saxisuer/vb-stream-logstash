package org.vastdata.vbstream.walsource.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.changes.ChangeStream;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

/**
 * WAL 直解源全装配门面（Task 15 升级：接收面 → catalog 同步全管线）——把槽管理、
 * 版本布局分发、JDBC 一致性引导、物理流接收、ctid 重放、检查点持久化与槽推进装配
 * 为一次 {@link #start()} 调用，供冒烟 {@code Main} 与后续宿主（reader/连接器）复用。
 *
 * <p>装配次序（委派 {@link CatalogSynchronizer#start(Connection, String, WalLayout,
 * String, String, StateConfig, long...)}）：① 普通 JDBC URL 建 SQL 会话；② 同会话查
 * {@code current_setting('server_version_num')} 经 {@link WalLayouts#forServerVersion}
 * 分发布局——错配版本启动期 ISE fail-fast（绝不近似回退）；③ 带凭据档 + 检查点配置
 * 起同步器（自愈接线；state.dir 有有效检查点 → 续传跳过引导，否则全新引导——决策
 * 见同步器 javadoc）。v1 门面无 tracked 面（interest 0 个，纯字典同步形态）。</p>
 *
 * <p>配置面：构造入参 {@link Properties} 读 {@code vb.wal.host/port/db/user/pass/slot}
 * 六键 + {@code vb.wal.state.dir/interval.ms/events} 三键 + v2 两键
 * （{@code vb.wal.tables} 白名单 / {@code vb.wal.dml} DML 输出开关，Task 7），默认
 * {@code localhost:5432/postgres}、{@code postgres/postgres}、槽 {@code wal_source}、
 * state 禁用（三键语义/默认值见 {@link StateConfig}，单一来源不双写）、白名单空 =
 * 全放行、dml 缺省 true。port/interval/events 非数字启动构造期抛
 * {@link NumberFormatException}；{@code vb.wal.dml} 非 true/false 构造期 IAE
 * （fail-fast 优于静默连错端口/吞掉 DML 面）。</p>
 *
 * <p>生命周期：start/close 单程（重复 start 抛 ISE；close 幂等，实现 AutoCloseable；
 * close 含最终 best-effort 检查点 + 槽推进——委派 {@link CatalogSynchronizer#stop()}）；
 * start 中途失败（SQLException/ISE）时内部资源由本类持有，调用方 close 释放。
 * <strong>契约（Med-3）：一个检查点目录同一时刻仅一个活实例</strong>——旧实例后
 * stop 会以陈旧状态覆盖新实例的检查点（文件锁后续任务）。线程约束：
 * start/close 仅装配线程调用；{@link #consumedLsn()} 与 {@link #metrics()} 快照面任意
 * 线程可读（前者 volatile 镜像，后者 {@link WalStreamMetrics#censusSnapshot()} 不可变副本）。</p>
 */
public final class WalSource implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WalSource.class);

    /** PG 主机键（默认 localhost）。 */
    public static final String KEY_HOST = "vb.wal.host";

    /** PG 端口键（默认 5432）。 */
    public static final String KEY_PORT = "vb.wal.port";

    /** 库名键（默认 postgres）。 */
    public static final String KEY_DB = "vb.wal.db";

    /** 用户键（默认 postgres，须有 REPLICATION 权限）。 */
    public static final String KEY_USER = "vb.wal.user";

    /** 密码键（默认 postgres）。 */
    public static final String KEY_PASS = "vb.wal.pass";

    /** 物理复制槽名键（默认 wal_source）。 */
    public static final String KEY_SLOT = "vb.wal.slot";

    /** 检查点目录键（透传 {@link StateConfig#KEY_DIR}——缺省/空 = 检查点禁用）。 */
    public static final String KEY_STATE_DIR = StateConfig.KEY_DIR;

    /** 检查点间隔键（透传 {@link StateConfig#KEY_INTERVAL_MS}）。 */
    public static final String KEY_STATE_INTERVAL_MS = StateConfig.KEY_INTERVAL_MS;

    /** 检查点事件阈值键（透传 {@link StateConfig#KEY_EVENTS}）。 */
    public static final String KEY_STATE_EVENTS = StateConfig.KEY_EVENTS;

    /** DML 输出面表白名单键（逗号分隔 {@code schema.table}；缺省/空 = 用户表全放行）。 */
    public static final String KEY_TABLES = "vb.wal.tables";

    /** DML 输出开关键（默认 true；false 回纯 v1 catalog 形态——ChangeStream 不装配）。 */
    public static final String KEY_DML = "vb.wal.dml";

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String pass;
    private final String slotName;
    private final StateConfig state;

    /** DML 白名单（{@code schema.table} 集；空 = 全放行——透传 ChangeStream/TableFilter）。 */
    private final Set<String> tables;

    /** DML 输出开关与输出 listener（listener 为 null 时 DML 面恒关——纯 catalog 形态）。 */
    private final ChangeOutputListener dmlOut;

    /** 变更流门面（start 后接收线程首条记录到达时惰性建——需要同步器的活快照视图）。 */
    private volatile ChangeStream changeStream;

    /** 同步器全管线装配用 SQL 会话（start 建，close 关；probe 与槽推进复用）。 */
    private Connection sqlConnection;

    /**
     * 同步器（start 建；null = 未 start）。<b>volatile（Task 8 JMM 修复）</b>：接收
     * 线程在 {@code CatalogSynchronizer.start} 内部启动、而 {@code sync} 字段赋值在
     * start 返回路径上——原声明无 HB 边可依（线程启动序只覆盖 start 调用<b>前</b>的
     * 写），接收线程首条记录到达时理论上可见 null/陈旧引用；volatile 补齐可见性，
     * {@link #awaitSync()} 的有界自旋再收窄时间窗（赋值在 start 返回前完成，正常
     * 路径自旋零次通过）。
     */
    private volatile CatalogSynchronizer sync;

    /** start/close 的单程闸门。 */
    private boolean started;
    private boolean closed;

    /**
     * v1 单参构造（纯 catalog 形态）：无 DML 输出 listener——{@code vb.wal.dml} 配置
     * 不复活 DML 面（无输出契约消费者，装配即浪费）。委派
     * {@link #WalSource(Properties, ChangeOutputListener)} 传 null。
     *
     * @param cfg 配置源
     */
    public WalSource(Properties cfg) {
        this(cfg, null);
    }

    /**
     * 从配置装配方面（v2 全参：DML 输出 listener 注入）。
     *
     * <p>关键步骤：六键各自 {@code getProperty(key, 默认值)}——缺键取默认、显式空串保留
     * （pgjdbc 自行报错，不在此猜测补值）；port 经 {@link Integer#parseInt}、state 三键
     * 经 {@link StateConfig#fromProperties} 解析，非数字构造期抛 NumberFormatException
     * （fail-fast）；白名单 {@code vb.wal.tables} 逗号切分（逐项 trim、空项跳过，空集 =
     * 全放行）；DML 开关 {@code vb.wal.dml} <b>严格布尔解析</b>（true/false 之外构造期
     * IAE——静默当 false 会吞掉整个 DML 面）。本方法不建任何连接（纯字段装配，连接
     * 推迟到 {@link #start()}——失败配置不应留半开资源）。</p>
     *
     * @param cfg     配置源（典型 {@code System.getProperty} 收集产物；null 键直接走默认）
     * @param dmlOut  DML 变更事件输出契约实现（null = 纯 v1 catalog 形态）
     * @throws IllegalArgumentException {@code vb.wal.dml} 非 true/false 字面
     */
    public WalSource(Properties cfg, ChangeOutputListener dmlOut) {
        this.host = cfg.getProperty(KEY_HOST, "localhost");
        this.port = Integer.parseInt(cfg.getProperty(KEY_PORT, "5432"));
        this.database = cfg.getProperty(KEY_DB, "postgres");
        this.user = cfg.getProperty(KEY_USER, "postgres");
        this.pass = cfg.getProperty(KEY_PASS, "postgres");
        this.slotName = cfg.getProperty(KEY_SLOT, "wal_source");
        this.state = StateConfig.fromProperties(cfg);
        this.tables = parseWhitelist(cfg.getProperty(KEY_TABLES, ""));
        this.dmlOut = dmlOut;
        String dmlCfg = cfg.getProperty(KEY_DML, "true").trim();
        switch (dmlCfg) {
            case "true" -> {
                // dmlOut 为 null 时下方 dmlEnabled() 恒 false（v1 单参构造路径）
            }
            case "false" -> this.dmlOff = true;
            default -> throw new IllegalArgumentException(
                    KEY_DML + " 须为 true/false: " + dmlCfg + "（静默当 false 会吞掉 DML 输出面）");
        }
    }

    /** 显式关闸（vb.wal.dml=false）；缺省 null = 由 listener 有无决定。 */
    private Boolean dmlOff;

    /**
     * 白名单文本解析：逗号切分 + 逐项 trim + 空项跳过（"public.a, public.b,," 形态）。
     *
     * @param text 配置原文（null 安全按空串）
     * @return 不可变白名单集（空 = 全放行）
     */
    private static Set<String> parseWhitelist(String text) {
        if (text == null || text.isBlank()) {
            return Set.of();
        }
        Set<String> out = new HashSet<>();
        for (String item : text.split(",")) {
            String t = item.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return Set.copyOf(out);
    }

    /**
     * DML 输出面是否装配（构造期裁定）：listener 缺席（v1 单参构造）或
     * {@code vb.wal.dml=false} 时 false。
     *
     * @return true = start 后 sink 串接 ChangeStream
     */
    public boolean dmlEnabled() {
        return dmlOut != null && dmlOff == null;
    }

    /**
     * DML 白名单观测面（配置直读，Main/诊断用）。
     *
     * @return 白名单集（空 = 用户表全放行）
     */
    public Set<String> tables() {
        return tables;
    }

    /**
     * 启动全管线：建 SQL 会话 → 版本分发布局 → 起同步器（续传/引导决策 + 接收线程 +
     * 检查点/槽推进生命周期）；DML 面启用时 sink 串接 {@link ChangeStream}（post-apply
     * 分发：先 catalog 施加后变更组装，as-of 次序保证）。
     *
     * <p>关键步骤：① 普通 JDBC URL（不带 replication 参数）建会话（此后归同步器独占
     * ——probe/槽推进复用；DML 面的 TOAST 回查 probe 同线程串行共享，单写者契约不破）；
     * ② {@code current_setting('server_version_num')}（文本数字，如 180002）经
     * {@link WalLayouts#forServerVersion} 分发，未注册版本 ISE fail-fast；③
     * {@link CatalogSynchronizer#start} 带凭据 + state 配置（interest 0 个）+ 可选
     * post-apply 接缝——ChangeStream 惰性建（快照视图需同步器实例，而 sink 在
     * startInternal 内接线先于 start 返回；接收线程单写者初始化、volatile 发布）。
     * 边界与异常语义：已 start 再调抛 ISE、已 close 再调抛 ISE；SQLException（连不上/
     * 权限不足）原样上抛，已建资源由本类持有、调用方经 {@link #close()} 释放。</p>
     *
     * @throws SQLException SQL 会话建立、槽管理或版本查询失败
     */
    public synchronized void start() throws SQLException {
        if (closed) {
            throw new IllegalStateException("WalSource already closed");
        }
        if (started) {
            throw new IllegalStateException("WalSource already started");
        }
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        sqlConnection = DriverManager.getConnection(url, user, pass);
        int versionNum = queryServerVersionNum(sqlConnection);
        WalLayout layout = WalLayouts.forServerVersion(versionNum);
        Consumer<WalRecord> postApply = dmlEnabled() ? rec -> changeStreamOrInit(layout).onRecord(rec) : null;
        sync = CatalogSynchronizer.start(sqlConnection, slotName, layout, user, pass, state, 0L, postApply);
        started = true;
        LOG.info("WalSource 启动: server_version_num={} slot={} state={} dml={} tables={} 续传起点 {}",
                versionNum, slotName, state.enabled() ? state.dir() : "禁用", dmlEnabled(),
                tables.isEmpty() ? "全放行" : tables, Lsn.format(sync.consumedLsn()));
    }

    /**
     * 惰性建/取变更流门面（接收线程单写者调用；volatile 字段发布——Main 线程的
     * {@link #dmlEmittedBuckets()} 读到非 null 引用即见安全构造）。
     *
     * <p>惰性的原因：ChangeStream 需要 {@code sync.snapshot()} 活视图，而 sink 接线
     * 发生在 {@code CatalogSynchronizer.start} 内部（先于 start 返回、sync 字段赋值）。
     * <b>Task 8 JMM 修复</b>：原注释"happens-before 经线程启动序传递"不成立——接收
     * 线程在 start 内部启动，赋值发生在其后，线程启动 HB 边覆盖不到；现 sync 已
     * volatile（可见性）+ {@link #awaitSync()} 有界自旋（时间窗——赋值与接收线程首条
     * postApply 之间只有 start 的返回路径，正常路径零自旋通过）。</p>
     *
     * @param layout start 解析的版本布局（闭包捕获，避免字段化）
     * @return 变更流门面
     */
    private ChangeStream changeStreamOrInit(WalLayout layout) {
        ChangeStream cs = changeStream;
        if (cs == null) {
            cs = new ChangeStream(awaitSync().snapshot(), sqlConnection, layout, tables, dmlOut);
            changeStream = cs;
        }
        return cs;
    }

    /**
     * 等待 {@code sync} 字段就绪（接收线程调用）：volatile 读 + 有界自旋（上限 10s）。
     *
     * <p>时间窗语义：{@code CatalogSynchronizer.start} 在返回前已完成全部装配且
     * {@code sync = start(...)} 赋值紧随其后——接收线程即便立即收到首条记录，等待
     * 也只是微秒级；上限 10s 防御 start 返回与首条记录之间的极端调度延迟，超时抛
     * ISE（装配序破坏的 fail-fast 信号而非重试面）。</p>
     *
     * @return 已就绪的同步器
     * @throws IllegalStateException 期限内 sync 仍未赋值（start 装配序被破坏）
     */
    private CatalogSynchronizer awaitSync() {
        CatalogSynchronizer s = sync;
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (s == null) {
            if (System.nanoTime() - deadline > 0) {
                throw new IllegalStateException("WalSource.start 装配序破坏：接收线程等待 sync 字段超时");
            }
            Thread.onSpinWait();
            s = sync;
        }
        return s;
    }

    /**
     * DML 观测面：已发射事务桶数（Main 周期 smoke 行数据源；DML 面未装配/尚无记录为 0）。
     *
     * @return 会话累计发射桶数
     */
    public long dmlEmittedBuckets() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.emittedBuckets();
    }

    /**
     * DML 观测面：已发射行数（过滤后实付口径；DML 面未装配/尚无记录为 0）。
     *
     * @return 会话累计发射行数
     */
    public long dmlEmittedRows() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.emittedRows();
    }

    /**
     * DML 观测面：截断 UPDATE 行级跳过计数（liveness guard 观测面，Task 8——对拍 IT
     * 的回归哨兵；停机后读安全：close 含接收线程 join，happens-before 成立）。
     *
     * @return 会话累计跳过的截断 UPDATE 行数（DML 面未装配/尚无记录为 0）
     */
    public long dmlSkippedTruncatedRows() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.skippedTruncatedRows();
    }

    /**
     * DML 观测面：截断 UPDATE 重建成功计数（Task 8.5，与
     * {@link #dmlSkippedTruncatedRows()} 互补的观测面）。
     *
     * @return 会话累计重建的截断 UPDATE 行数（DML 面未装配/尚无记录为 0）
     */
    public long dmlReconstructedTruncatedRows() {
        ChangeStream cs = changeStream;
        return cs == null ? 0L : cs.reconstructedTruncatedRows();
    }

    /**
     * 已消费 LSN 前沿（透传 {@link CatalogSynchronizer#consumedLsn()}）。
     *
     * @return 下一个未消费字节的 LSN；未 start 时为 0
     */
    public long consumedLsn() {
        CatalogSynchronizer s = sync;
        return s == null ? 0L : s.consumedLsn();
    }

    /**
     * 接收计数器观测面（透传 {@link CatalogSynchronizer#streamMetrics()}）。
     *
     * <p>计数实例与接收器同寿——start 前调用抛 ISE（尚无接收线程产出可观测）；
     * 返回引用稳定，调用方轮询同一实例即可。</p>
     *
     * @return 计数器（records/resyncs/contrecords/orphanSkips/carryDrops/reconnects + census）
     * @throws IllegalStateException 尚未 start
     */
    public WalStreamMetrics metrics() {
        CatalogSynchronizer s = sync;
        if (s == null) {
            throw new IllegalStateException("metrics available only after start()");
        }
        return s.streamMetrics();
    }

    /**
     * 接收器终态失败透传面（终审 I2——接收器死亡静默修复）：接收线程因重连耗尽/解析
     * ISE 自行退出后返回根因；Main 周期行检测到终态即 ERROR + exit(1)。运行中/正常
     * 停机/未 start 为 empty。
     *
     * @return 终态根因；运行中或无终态失败为 empty
     */
    public Optional<Throwable> receiverTerminalFailure() {
        CatalogSynchronizer s = sync;
        return s == null ? Optional.empty() : s.terminalFailure();
    }

    /**
     * 最近一次成功落盘的检查点 lsn（透传 {@link CatalogSynchronizer#lastCheckpointLsn()}，
     * Main 周期行的 checkpoint 观测面）。
     *
     * @return 检查点 lsn；state 禁用或尚未落盘为 0
     */
    public long lastCheckpointLsn() {
        CatalogSynchronizer s = sync;
        return s == null ? 0L : s.lastCheckpointLsn();
    }

    /**
     * 最近一次槽推进成功的目标 lsn（透传 {@link CatalogSynchronizer#lastSlotAdvanceLsn()}，
     * Main 周期行的槽推进观测面）。
     *
     * @return 推进目标 lsn；未推过为 0
     */
    public long lastSlotAdvanceLsn() {
        CatalogSynchronizer s = sync;
        return s == null ? 0L : s.lastSlotAdvanceLsn();
    }

    /**
     * 本次 start 是否自检查点续传（透传 {@link CatalogSynchronizer#resumedFromState()}）。
     *
     * @return 续传为 true；state 禁用/引导路径为 false
     */
    public boolean resumedFromState() {
        CatalogSynchronizer s = sync;
        return s != null && s.resumedFromState();
    }

    /**
     * 检查点目录（配置面直读——Main 判定"有 stateDir 时"增打 checkpoint/槽推进观测）。
     *
     * @return 目录；state 禁用为 null
     */
    public Path stateDir() {
        return state.dir();
    }

    /**
     * 幂等停机：先停同步器（含接收线程 join + 最终 best-effort 检查点 + 槽推进），再关
     * SQL 会话。
     *
     * <p>关键步骤：closed 单检幂等（重复调用 no-op）；未 start 时调用合法（仅关可能存在的
     * 半开 SQL 会话——start 中途失败的清理路径）；关会话异常 WARN 吞掉（清理路径不掩盖
     * 主流程）。</p>
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (sync != null) {
            sync.stop();
        }
        if (sqlConnection != null) {
            try {
                sqlConnection.close();
            } catch (SQLException e) {
                LOG.warn("关闭 SQL 会话失败（忽略）: {}", e.getMessage());
            }
        }
        LOG.info("WalSource 已关闭: slot={} 消费前沿 {}（检查点 {} / 槽推进 {}）", slotName,
                Lsn.format(consumedLsn()), Lsn.format(lastCheckpointLsn()), Lsn.format(lastSlotAdvanceLsn()));
    }

    /**
     * 查服务端版本号（决定 WAL 布局描述符的分发输入）。
     *
     * <p>{@code current_setting('server_version_num')} 返回文本形数字（如 "180002"），
     * {@link Integer#parseInt} 直取——非法值（不期望出现）NumberFormatException 上抛交
     * 启动失败面。</p>
     *
     * @param connection 已建立的 SQL 会话
     * @return 版本号（如 180002）
     * @throws SQLException 查询失败
     */
    private static int queryServerVersionNum(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_setting('server_version_num')")) {
            rs.next();
            return Integer.parseInt(rs.getString(1));
        }
    }
}
