package org.vastdata.vbstream.walsource.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.receive.PhysicalSlotManager;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.receive.WalStreamReceiver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * WAL 直解源接收面门面（最小版：仅接收，Task 9）——把槽管理、版本布局分发、流接收三件
 * 装配为一次 {@link #start()} 调用，供冒烟 {@code Main} 与后续宿主（reader/连接器）复用。
 *
 * <p>装配次序（spec 接缝约定）：① 以普通 SQL 连接调 {@link PhysicalSlotManager#ensureSlot}
 * 取续传起点 LSN（新建槽 = 当前 flush LSN，复用槽 = restart_lsn）——物理流命令不带 SLOT，
 * 槽仅作位点锚与 WAL 保留载体（见 {@link WalStreamReceiver} 已知限制）；② 同连接查
 * {@code current_setting('server_version_num')} 经 {@link WalLayouts#forServerVersion} 分发
 * 布局描述符——错配版本启动期 ISE fail-fast（绝不近似回退）；③ 建 {@link WalStreamReceiver}
 * 并以 metrics 计数回调作 sink 启动（记录零下游消费，仅 census 普查与位点推进——后续任务
 * 在此接 catalog 重放/输出）。</p>
 *
 * <p>配置面：构造入参 {@link Properties} 读 {@code vb.wal.host/port/db/user/pass/slot} 六键，
 * 默认 {@code localhost:5432/postgres}、{@code postgres/postgres}、槽 {@code wal_source}。
 * port 非数字启动构造期抛 {@link NumberFormatException}（fail-fast 优于静默连错端口）。</p>
 *
 * <p>生命周期：start/close 单程（重复 start 抛 ISE；close 幂等，实现 AutoCloseable）；
 * start 中途失败（SQLException/ISE）时内部资源由本类持有，调用方 close 释放。线程约束：
 * start/close 仅装配线程调用；{@link #consumedLsn()} 与 {@link #metrics()} 快照面任意线程
 * 可读（前者 volatile 镜像，后者 {@link WalStreamMetrics#censusSnapshot()} 不可变副本）。</p>
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

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String pass;
    private final String slotName;

    /** 槽管理用 SQL 会话（start 建，close 关）。 */
    private Connection sqlConnection;

    /** 接收器（start 建；null = 未 start）。 */
    private WalStreamReceiver receiver;

    /** start/close 的单程闸门。 */
    private boolean started;
    private boolean closed;

    /**
     * 从配置装配方面。
     *
     * <p>关键步骤：六键各自 {@code getProperty(key, 默认值)}——缺键取默认、显式空串保留
     * （pgjdbc 自行报错，不在此猜测补值）。port 经 {@link Integer#parseInt} 解析，非数字
     * 构造期抛 NumberFormatException（fail-fast）。本方法不建任何连接（纯字段装配，
     * 连接推迟到 {@link #start()}——失败配置不应留半开资源）。</p>
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
    }

    /**
     * 启动接收：建 SQL 会话 → 确保槽并取续传起点 → 版本分发布局 → 起接收线程。
     *
     * <p>关键步骤：① 普通 JDBC URL（不带 replication 参数）建会话；②
     * {@link PhysicalSlotManager#ensureSlot} 幂等建槽/复用取起点 LSN；③ 同会话查
     * {@code current_setting('server_version_num')}（文本数字，如 180002）——经
     * {@link WalLayouts#forServerVersion} 分发，未注册版本 ISE fail-fast；④ sink 取
     * {@link WalStreamReceiver#metrics()} 的 {@code countRecord} 方法引用（记录交付即
     * 计数 + census 普查——最小消费面，后续任务替换为真实下游）后 {@code receiver.start}。
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
        long fromLsn = new PhysicalSlotManager(sqlConnection).ensureSlot(slotName);
        int versionNum = queryServerVersionNum(sqlConnection);
        WalLayout layout = WalLayouts.forServerVersion(versionNum);
        receiver = new WalStreamReceiver(host, port, database, user, pass, layout, slotName);
        receiver.start(receiver.metrics()::countRecord, fromLsn);
        started = true;
        LOG.info("WalSource 启动: server_version_num={} slot={} 续传起点 {}",
                versionNum, slotName, Lsn.format(fromLsn));
    }

    /**
     * 已消费 LSN 前沿（透传 {@link WalStreamReceiver#consumedLsn()}）。
     *
     * @return 下一个未消费字节的 LSN；未 start 时为 0
     */
    public long consumedLsn() {
        WalStreamReceiver r = receiver;
        return r == null ? 0L : r.consumedLsn();
    }

    /**
     * 接收计数器观测面（透传 {@link WalStreamReceiver#metrics()}）。
     *
     * <p>计数实例与接收器同寿——start 前调用抛 ISE（尚无接收线程产出可观测）；
     * 返回引用稳定，调用方轮询同一实例即可。</p>
     *
     * @return 计数器（records/resyncs/contrecords/orphanSkips/carryDrops/reconnects + census）
     * @throws IllegalStateException 尚未 start
     */
    public WalStreamMetrics metrics() {
        WalStreamReceiver r = receiver;
        if (r == null) {
            throw new IllegalStateException("metrics available only after start()");
        }
        return r.metrics();
    }

    /**
     * 幂等停机：先停接收器（join 3s 上限），再关 SQL 会话。
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
        if (receiver != null) {
            receiver.stop();
        }
        if (sqlConnection != null) {
            try {
                sqlConnection.close();
            } catch (SQLException e) {
                LOG.warn("关闭 SQL 会话失败（忽略）: {}", e.getMessage());
            }
        }
        LOG.info("WalSource 已关闭: slot={} 消费前沿 {}", slotName, Lsn.format(consumedLsn()));
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
