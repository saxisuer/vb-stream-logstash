package org.vastdata.vbstream.walsource.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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
 * 六键 + {@code vb.wal.state.dir/interval.ms/events} 三键，默认
 * {@code localhost:5432/postgres}、{@code postgres/postgres}、槽 {@code wal_source}、
 * state 禁用（三键语义/默认值见 {@link StateConfig}，单一来源不双写）。port/interval/
 * events 非数字启动构造期抛 {@link NumberFormatException}（fail-fast 优于静默连错端口）。</p>
 *
 * <p>生命周期：start/close 单程（重复 start 抛 ISE；close 幂等，实现 AutoCloseable；
 * close 含最终 best-effort 检查点 + 槽推进——委派 {@link CatalogSynchronizer#stop()}）；
 * start 中途失败（SQLException/ISE）时内部资源由本类持有，调用方 close 释放。线程约束：
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

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String pass;
    private final String slotName;
    private final StateConfig state;

    /** 同步器全管线装配用 SQL 会话（start 建，close 关；probe 与槽推进复用）。 */
    private Connection sqlConnection;

    /** 同步器（start 建；null = 未 start）。 */
    private CatalogSynchronizer sync;

    /** start/close 的单程闸门。 */
    private boolean started;
    private boolean closed;

    /**
     * 从配置装配方面。
     *
     * <p>关键步骤：六键各自 {@code getProperty(key, 默认值)}——缺键取默认、显式空串保留
     * （pgjdbc 自行报错，不在此猜测补值）；port 经 {@link Integer#parseInt}、state 三键
     * 经 {@link StateConfig#fromProperties} 解析，非数字构造期抛 NumberFormatException
     * （fail-fast）。本方法不建任何连接（纯字段装配，连接推迟到 {@link #start()}——
     * 失败配置不应留半开资源）。</p>
     *
     * @param cfg 配置源（典型 {@code System.getProperty} 收集产物；null 键直接走默认）
     */
    public WalSource(Properties cfg) {
        this.host = cfg.getProperty(KEY_HOST, "localhost");
        this.port = Integer.parseInt(cfg.getProperty(KEY_PORT, "5432"));
        this.database = cfg.getProperty(KEY_DB, "postgres");
        this.user = cfg.getProperty(KEY_USER, "postgres");
        this.pass = cfg.getProperty(KEY_PASS, "postgres");
        this.slotName = cfg.getProperty(KEY_SLOT, "wal_source");
        this.state = StateConfig.fromProperties(cfg);
    }

    /**
     * 启动全管线：建 SQL 会话 → 版本分发布局 → 起同步器（续传/引导决策 + 接收线程 +
     * 检查点/槽推进生命周期）。
     *
     * <p>关键步骤：① 普通 JDBC URL（不带 replication 参数）建会话（此后归同步器独占
     * ——probe/槽推进复用）；② {@code current_setting('server_version_num')}（文本数字，
     * 如 180002）经 {@link WalLayouts#forServerVersion} 分发，未注册版本 ISE fail-fast；
     * ③ {@link CatalogSynchronizer#start} 带凭据 + state 配置（interest 0 个）。
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
        sync = CatalogSynchronizer.start(sqlConnection, slotName, layout, user, pass, state, 0L);
        started = true;
        LOG.info("WalSource 启动: server_version_num={} slot={} state={} 续传起点 {}",
                versionNum, slotName, state.enabled() ? state.dir() : "禁用",
                Lsn.format(sync.consumedLsn()));
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
