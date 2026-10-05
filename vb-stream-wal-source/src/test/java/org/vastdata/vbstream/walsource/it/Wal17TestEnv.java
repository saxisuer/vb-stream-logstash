package org.vastdata.vbstream.walsource.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PG 17 全矩阵 IT（Task 16）专用的干扰容器——postgres:17 镜像 + 与
 * {@link Interference}（PG 18）完全相同的干扰参数形态：autovacuum 拉满
 * （{@code cost_delay=0}、vacuum/analyze 阈值=1）驱动 PRUNE/INPLACE 记录最大化、
 * {@code wal_compression=off}（v1 PageImages 不解压压缩 FPW 的运维前提）。
 * 独立单例，与 18 侧互不干扰（两容器可并存，端口各自映射）。
 *
 * <p>类加载即启动（需要本机 Docker），跨测试类共享——各测试用独立槽名与表名并
 * 自行清理。本容器同时是 WalLayoutV17 差异转录的 live 实查锚（pg_attribute 26 列
 * 含 attcacheoff / pg_class 33 列缺 relallfrozen，转录记录见 WalLayoutV17 类 javadoc
 * 差异表）。</p>
 */
public final class Wal17TestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(Wal17TestEnv.class);

    /** 单例 PG 17 干扰容器：参数与 Interference 逐项一致，仅镜像版本不同。 */
    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("waldb")
            .withUsername("wal")
            .withPassword("wal")
            .withCommand(
                    "postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=16",
                    "-c", "max_wal_senders=16",
                    "-c", "autovacuum_vacuum_cost_delay=0",
                    "-c", "autovacuum_vacuum_threshold=1",
                    "-c", "autovacuum_analyze_threshold=1",
                    "-c", "wal_compression=off");

    static {
        PG.start();
        LOG.info("PG 17 干扰容器就绪（17 全矩阵 IT）: {}", PG.getJdbcUrl());
    }

    /** 私有构造器：静态单例环境，不可实例。 */
    private Wal17TestEnv() {
    }

    /**
     * 新建一条普通 SQL 会话连接（调用方负责关闭）。
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 连接失败
     */
    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    /** @return 容器映射主机名 */
    public static String host() {
        return PG.getHost();
    }

    /** @return 容器映射端口 */
    public static int port() {
        return PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

    /** @return 库名 */
    public static String databaseName() {
        return PG.getDatabaseName();
    }

    /** @return 用户名 */
    public static String username() {
        return PG.getUsername();
    }

    /** @return 密码 */
    public static String password() {
        return PG.getPassword();
    }

    /**
     * 服务端 {@code server_version_num}（170000-179999）——布局分发输入；IT 起同步器
     * 前断言其落在 17 区间（镜像漂移即 fail-fast，防 17 矩阵在错版本容器上空转）。
     *
     * @return 十进制版本号
     * @throws SQLException 查询失败
     */
    public static int serverVersionNum() throws SQLException {
        try (Connection c = newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_setting('server_version_num')::int")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /**
     * 静默删除物理槽（先杀活跃 walsender 再删）；槽不存在等情况仅 WARN——测试清理
     * 路径不得掩盖断言失败（与 {@link Interference#dropPhysicalSlotQuietly} 同款，
     * 面向本容器）。
     *
     * @param slotName 槽名
     */
    public static void dropPhysicalSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
            Thread.sleep(200);
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        } catch (Exception e) {
            LOG.warn("清理物理槽 {} 失败: {}", slotName, e.getMessage());
        }
    }
}
