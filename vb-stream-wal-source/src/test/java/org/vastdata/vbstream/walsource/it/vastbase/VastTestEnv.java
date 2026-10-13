package org.vastdata.vbstream.walsource.it.vastbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.state.StateConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * VastBase G100 真库测试环境（手动档 {@code -Dvb.it.vastbase=true}，vastbase T5）：直连
 * {@code 172.16.107.133:55433/test_a}（opengauss-jdbc 前缀——T0 spike 实测 md5 码 5 一次通过，
 * 无 M2 码 11 死角），静态单例——<b>非 Testcontainers</b>，库是长活真环境。纪律：测试表/
 * 槽/schema 一律 {@link #PREFIX} 前缀 + 用例自清（finally DROP），不触碰既有业务表与逻辑槽
 * （orders/snapshot/gs_系/ora_alerts/test_lock/utl_file_dir 与 sync_orders_slot/vb_e2e_*）。
 * 连接参数经 {@code -Dvb.it.vastbase.{host,port,db,user,pass}} 可覆盖，缺省真库实测值。
 *
 * <p><strong>环境锚（2026-10-13 实测，spec 附录 A / T0 spike 同源）</strong>：
 * {@code version()} 报 "PostgreSQL 9.2.4 ..."（无 openGauss/VastBase branding）、
 * {@code server_version_num}=90204、{@code pg_class.relname} attlen=128（判别第二面）、
 * {@code datcompatibility}='A'、{@code pg_current_wal_flush_lsn()} 不存在（write 位点档，
 * 与 OG 同形）、test 用户 rolsuper=false 但 VACUUM ANALYZE pg_class / CHECKPOINT 均可执行。
 * 物理槽经两参 {@code pg_create_physical_replication_slot(?, false)}（dummy_standby 变体）
 * 建删与 {@code pg_replication_slot_advance}（无 pg_lsn cast）推进均实测可用。</p>
 *
 * <p>线程约束：共享会话 {@link #sql()} 单连接仅供装配线程串行 DDL 用（synchronized 互斥）；
 * 干扰线程/交错 DDL/对拍 probe 等并发面一律 {@link #newSqlConnection()} 自开自关。</p>
 */
public final class VastTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(VastTestEnv.class);

    /** 测试对象前缀（表/槽/schema/publication 共用——真库卫生纪律的唯一命名面）。 */
    public static final String PREFIX = "vb_wal_";

    /** 共享 SQL 会话（懒建单连接；装配线程串行使用，见类 javadoc 线程约束）。 */
    private static volatile Connection shared;

    private VastTestEnv() {
    }

    /** @return 环境主机（缺省真库，-Dvb.it.vastbase.host 覆盖）。 */
    public static String host() {
        return System.getProperty("vb.it.vastbase.host", "172.16.107.133");
    }

    /** @return 环境端口（缺省真库，-Dvb.it.vastbase.port 覆盖）。 */
    public static int port() {
        return Integer.parseInt(System.getProperty("vb.it.vastbase.port", "55433"));
    }

    /** @return 目标库名（缺省真库 A 档测试库，-Dvb.it.vastbase.db 覆盖）。 */
    public static String db() {
        return System.getProperty("vb.it.vastbase.db", "test_a");
    }

    /** @return 连接用户（rolreplication=t，-Dvb.it.vastbase.user 覆盖）。 */
    public static String user() {
        return System.getProperty("vb.it.vastbase.user", "test");
    }

    /** @return 连接口令（-Dvb.it.vastbase.pass 覆盖）。 */
    public static String pass() {
        return System.getProperty("vb.it.vastbase.pass", "Tester123");
    }

    /**
     * SQL 会话 JDBC URL（opengauss-jdbc 前缀——sha256/md5 认证兼容，与运行时 OG 档
     * {@code vb.wal.og.jdbc.prefix=jdbc:opengauss:} 同驱动面）。
     *
     * @return 形如 {@code jdbc:opengauss://host:port/db}
     */
    public static String jdbcUrl() {
        return "jdbc:opengauss://" + host() + ":" + port() + "/" + db();
    }

    /**
     * {@link WalSource} 门面装配参数集（vastbase T5 定形，Task 6/7 复用）：六基础键 +
     * OG 档 direct 通道 + opengauss-jdbc 前缀——即 {@code OgLifecycleIT} 门面全装配用例的
     * 参数形态翻译（{@code og.source=direct} 免 sidecar 依赖、{@code og.jdbc.prefix=
     * jdbc:opengauss:} 走 sha256/md5 兼容驱动）。槽名/state 三键由调用方按用例覆盖。
     *
     * @return 已填 host/port/db/user/pass + OG direct 通道 + JDBC 前缀的参数集（可变，调用方追加）
     */
    public static Properties params() {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(port()));
        cfg.setProperty(WalSource.KEY_DB, db());
        cfg.setProperty(WalSource.KEY_USER, user());
        cfg.setProperty(WalSource.KEY_PASS, pass());
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        return cfg;
    }

    /**
     * 新建一条普通 SQL 会话（opengauss-jdbc）——干扰线程/交错 DDL/对拍 probe 等并发面
     * 一律走本方法（共享会话非线程安全）；调用方负责关闭。
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 连接失败
     */
    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), user(), pass());
    }

    /**
     * 共享 SQL 会话（懒建单连接；已关则重建）——仅装配线程串行 DDL 用。
     *
     * @return 已认证的共享连接
     * @throws SQLException 连接失败
     */
    public static synchronized Connection sql() throws SQLException {
        if (shared == null || shared.isClosed()) {
            shared = DriverManager.getConnection(jdbcUrl(), user(), pass());
        }
        return shared;
    }

    /**
     * 语句序列执行器（各自独立自动提交，任一失败即抛——已执行语句不回滚，清理路径
     * 用 DROP IF EXISTS 幂等吸收）；SELECT 场景各 IT 自持 Statement。
     *
     * @param statements 语句列表
     * @throws SQLException 任一语句失败
     */
    public static void sql(String... statements) throws SQLException {
        try (Statement st = sql().createStatement()) {
            for (String s : statements) {
                st.execute(s);
            }
        }
    }

    /**
     * 建用例级独立 state 目录（{@code target/vastbase-it/<name>}，存在即递归清空——
     * Windows 纪律：独立子目录 + 用例级 wipe，防残留陈旧检查点/句柄锁跨用例互扰；
     * 测试工作目录 = 模块根，target 天然落 gitignore）。
     *
     * @param name 用例名（目录尾段）
     * @return 已清空建好的目录
     */
    public static Path newStateDir(String name) {
        Path dir = Path.of("target", "vastbase-it", name);
        try {
            if (Files.exists(dir)) {
                try (Stream<Path> walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new IllegalStateException("state 目录清理失败: " + p, e);
                        }
                    });
                }
            }
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IllegalStateException("state 目录准备失败: " + dir, e);
        }
        return dir;
    }

    /**
     * 静默删除物理槽（先杀本端 walsender 再删——沿 {@code OgTestEnv.dropPhysicalSlotQuietly}
     * 形态）：direct 通道的裸 v3 复制流 application_name 恒 {@code vb-wal-source}（业务
     * 复制连接不匹配，杀面限本端）；pid 先物化再按值 terminate（防视图谓词下推自杀）；
     * 槽不存在等情况仅 WARN——测试清理路径不得掩盖断言失败。
     *
     * @param slotName 槽名
     */
    public static void dropPhysicalSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            List<Long> pids = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT pid FROM pg_stat_replication WHERE application_name LIKE '%vb-wal-source%'")) {
                while (rs.next()) {
                    pids.add(rs.getLong(1));
                }
            }
            for (long pid : pids) {
                try (Statement st = c.createStatement()) {
                    st.execute("SELECT pg_terminate_backend(" + pid + ")");
                }
            }
            Thread.sleep(200);
            try (PreparedStatement ps = c.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                LOG.debug("VastBase 物理槽 {} 不存在——前置清理空转", slotName);
            } else {
                LOG.warn("清理 VastBase 物理槽 {} 失败: {}", slotName, e.getMessage());
            }
        }
    }

    /**
     * 服务端当前 write 位点（VastBase 无 flush 档位点函数——实测
     * {@code pg_current_wal_flush_lsn()} 不存在，对拍目标前沿与引导 B 同取 write 位点，
     * 与 OG 档 {@code writeLsn} 同语义）。
     *
     * @return {@code pg_current_xlog_location()} 打包 long
     * @throws SQLException 查询失败
     */
    public static long writeLsn(Connection c) throws SQLException {
        return Lsn.parse(queryString(c, "SELECT pg_current_xlog_location()"));
    }

    /**
     * 单值字符串查询助手（恰返一行一列）。
     *
     * @param c   会话连接
     * @param sql 查询
     * @return 首行首列字符串值
     * @throws SQLException 查询失败
     */
    public static String queryString(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    /**
     * StateConfig 的便捷构造（state 目录 + 缺省间隔/阈值 1——任一 catalog 行事件即触发
     * 周期检查点，{@code OgLifecycleIT} 同档）。
     *
     * @param dir state 目录
     * @return 检查点配置
     */
    public static StateConfig stateConfig(Path dir) {
        return new StateConfig(dir, 30_000, 1);
    }
}
