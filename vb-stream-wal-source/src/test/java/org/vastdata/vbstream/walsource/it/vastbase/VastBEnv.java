package org.vastdata.vbstream.walsource.it.vastbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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
 * VastBase G100 <b>B 档</b>测试环境（M4 Task 7，Task 8 的 B 档 IT 消费）：直连
 * {@code 172.16.107.133:55443/test_b}——与 A 档（{@link VastTestEnv}，55433/test_a）
 * 同真库集群不同实例不同库，test_b 是 {@code DBCOMPATIBILITY 'B'}（MySQL 风格）库，
 * 即 M4 的适配对象。静态单例——<b>非 Testcontainers</b>，库是长活真环境；手动档
 * {@code -Dvb.it.vastbase=true} 由各 IT 自行门控（gate 键与 A 档族共用——B 档 IT
 * {@code VastBModeIT} 的 {@code @EnabledIfSystemProperty} 即此键；本类连接覆盖键是
 * 独立的 {@code vb.it.vastb.*} 五键，仅提供连接面）。
 *
 * <p><strong>连接纪律（M4，test_b 抖动实测）</strong>：单持久连接 + 建连重试环
 * （{@link #openConnection()}——Task 8 起委托 {@link VastTestEnv#openConnectionWithRetry}
 * 的 8 次指数退避（1s→15s 封顶，合计 ~75s 坏窗覆盖），替代初版 15×1s 平坦环：实跑
 * 观测到 4 连挂后自愈的持续坏窗，平坦环覆盖面不足）；共享会话 {@link #sql()} 懒建、
 * {@link #newSqlConnection()} 每次独立——两路都经 {@link #openConnection()}。</p>
 *
 * <p><strong>卫生纪律（与 {@link VastTestEnv} 同规）</strong>：测试表/槽/schema 一律
 * {@link #PREFIX} 前缀 + 用例自清（finally DROP），不触碰库内既有业务对象。</p>
 *
 * <p>线程约束：共享会话 {@link #sql()} 单连接仅供装配线程串行 DDL 用（synchronized
 * 互斥）；干扰线程/并发面一律 {@link #newSqlConnection()} 自开自关。</p>
 */
public final class VastBEnv {

    private static final Logger LOG = LoggerFactory.getLogger(VastBEnv.class);

    /** 测试对象前缀（表/槽/schema 共用——B 档库内卫生纪律的唯一命名面，与 VastTestEnv 同规）。 */
    public static final String PREFIX = "vb_wal_";

    /** 共享 SQL 会话（懒建单连接；装配线程串行使用，见类 javadoc 线程约束）。 */
    private static volatile Connection shared;

    private VastBEnv() {
    }

    /** @return 环境主机（缺省真库，-Dvb.it.vastb.host 覆盖）。 */
    public static String host() {
        return System.getProperty("vb.it.vastb.host", "172.16.107.133");
    }

    /** @return 环境端口（缺省 B 档实例，-Dvb.it.vastb.port 覆盖）。 */
    public static int port() {
        return Integer.parseInt(System.getProperty("vb.it.vastb.port", "55443"));
    }

    /** @return 目标库名（缺省 B 档测试库，-Dvb.it.vastb.db 覆盖）。 */
    public static String db() {
        return System.getProperty("vb.it.vastb.db", "test_b");
    }

    /** @return 连接用户（与 A 档真库同账户，-Dvb.it.vastb.user 覆盖）。 */
    public static String user() {
        return System.getProperty("vb.it.vastb.user", "test");
    }

    /** @return 连接口令（-Dvb.it.vastb.pass 覆盖）。 */
    public static String pass() {
        return System.getProperty("vb.it.vastb.pass", "Tester123");
    }

    /**
     * SQL 会话 JDBC URL（opengauss-jdbc 前缀——与 {@link VastTestEnv} 同驱动面，
     * sha256/md5 认证兼容）。
     *
     * @return 形如 {@code jdbc:opengauss://host:port/db}
     */
    public static String jdbcUrl() {
        return "jdbc:opengauss://" + host() + ":" + port() + "/" + db();
    }

    /**
     * {@link WalSource} 门面装配参数集（M4 Task 8 补面，镜像 {@link VastTestEnv#params()}
     * 的 B 档形态）：六基础键取本 env 访问器 + OG 档 direct 通道 + opengauss-jdbc 前缀
     * ——门面判别命中 test_b（relname attlen=128 → {@code WalLayoutVVastbase}）后走 OG
     * 装配分支，direct 通道免 sidecar 依赖（spike 实测 v3 直连对 test_b 全程稳定）。
     * 槽名/state 三键/DML 白名单由调用方按用例覆盖。
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
     * 建连（B 档实例握手抖动吸收——重试环委托 {@link VastTestEnv#openConnectionWithRetry}
     * 的 8 次指数退避形态）。
     *
     * <p>关键步骤：{@code openConnectionWithRetry(jdbcUrl(), user(), pass())} 逐次退避重试
     * {@code DriverManager.getConnection}。边界与异常语义：EOF/读超时等网络抖动均表现为
     * {@link SQLException} 族被环吸收（实跑观测 4 连挂后自愈的持续坏窗，初版 15×1s 平坦
     * 环覆盖不足——Task 8 首跑前改委托，坏窗覆盖 ~75s）；认证失败等确定性异常同样会
     * 重试满（代价仅延迟，真因以透传末次异常为准）。线程约束：任意线程（无共享态）。</p>
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 重试耗尽后透传末次异常
     */
    private static Connection openConnection() throws SQLException {
        return VastTestEnv.openConnectionWithRetry(jdbcUrl(), user(), pass());
    }

    /**
     * 新建一条普通 SQL 会话（opengauss-jdbc）——干扰线程/并发面一律走本方法（共享会话
     * 非线程安全）；调用方负责关闭。经 {@link #openConnection()} 重试环。
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 连接失败（重试耗尽后透传）
     */
    public static Connection newSqlConnection() throws SQLException {
        return openConnection();
    }

    /**
     * 共享 SQL 会话（懒建单连接；已关则重建）——仅装配线程串行 DDL 用，synchronized
     * 互斥。经 {@link #openConnection()} 重试环（M4 test_b 抖动纪律）。
     *
     * @return 已认证的共享连接
     * @throws SQLException 连接失败（重试耗尽后透传）
     */
    public static synchronized Connection sql() throws SQLException {
        if (shared == null || shared.isClosed()) {
            shared = openConnection();
        }
        return shared;
    }

    /**
     * 语句序列执行器（各自独立自动提交，任一失败即抛——已执行语句不回滚，清理路径
     * 用 DROP IF EXISTS 幂等吸收）；与 {@link VastTestEnv#sql(String...)} 同形。
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
     * 建用例级独立 state 目录（{@code target/vastb-it/<name>}，存在即递归清空——与
     * {@link VastTestEnv#newStateDir(String)} 同规：独立子目录 + 用例级 wipe，防残留
     * 陈旧检查点/句柄锁跨用例互扰；B 档独立根目录避免与 A 档 IT 互踩）。
     *
     * @param name 用例名（目录尾段）
     * @return 已清空建好的目录
     */
    public static Path newStateDir(String name) {
        Path dir = Path.of("target", "vastb-it", name);
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
     * 静默删除物理槽（先杀本端 walsender 再删——镜像 {@link VastTestEnv#dropPhysicalSlotQuietly}
     * 形态，连接面换 {@link #newSqlConnection()}）：direct 通道的裸 v3 复制流
     * application_name 恒 {@code vb-wal-source}（业务复制连接不匹配，杀面限本端）；pid
     * 先物化再按值 terminate（防视图谓词下推自杀）；槽不存在等情况仅 WARN——测试清理
     * 路径不得掩盖断言失败。
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
                LOG.debug("B 档物理槽 {} 不存在——前置清理空转", slotName);
            } else {
                LOG.warn("清理 B 档物理槽 {} 失败: {}", slotName, e.getMessage());
            }
        }
    }
}
