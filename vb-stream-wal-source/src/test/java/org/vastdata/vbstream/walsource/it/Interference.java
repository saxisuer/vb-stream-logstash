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
 * 对抗性 catalog 对拍专用的干扰容器（Task 13，spec §8）——独立于 {@link WalTestEnv}
 * 单例：autovacuum 拉满（{@code cost_delay=0}、vacuum/analyze 阈值=1）使任何一行变更
 * 都立刻触发 vacuum/analyze 风暴，驱动 watched 目录的 PRUNE/INPLACE 记录最大化；
 * 另带 {@code wal_compression=off}（v1 PageImages 不解压压缩 FPW 的运维前提，
 * 见其 javadoc——CHECKPOINT 后首写会带页镜像，若服务端默认压缩会使对拍流 fail-fast）。
 *
 * <p>类加载即启动（需要本机 Docker），跨测试类/重复执行共享单容器——各测试用独立
 * 槽名与表名并自行清理。容器参数其余项与 {@link WalTestEnv} 对齐
 * （{@code wal_level=logical}、slot/walsender 上限 16）。</p>
 */
public final class Interference {

    private static final Logger LOG = LoggerFactory.getLogger(Interference.class);

    /** 单例干扰容器：autovacuum 拉满 + wal_compression=off，其余与 WalTestEnv 对齐。 */
    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18")
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
        LOG.info("对抗性对拍干扰容器就绪（autovacuum 拉满 + wal_compression=off）: {}", PG.getJdbcUrl());
    }

    /** 私有构造器：静态单例环境，不可实例。 */
    private Interference() {
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
     * 服务端 {@code server_version_num}——布局分发 {@code WalLayouts} 的输入。
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
     * 路径不得掩盖断言失败（与 {@link WalTestEnv#dropPhysicalSlotQuietly} 同款，
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
