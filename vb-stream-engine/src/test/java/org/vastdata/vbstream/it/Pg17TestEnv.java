package org.vastdata.vbstream.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// 照 PgTestEnv 的选型说明：2.0.5 的规范类非泛型，此处用保留泛型签名的兼容类保持用法一致
import org.testcontainers.containers.PostgreSQLContainer;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PG 17 兼容性专用的单例 postgres:17 容器与工具（2026-09-11 源码审计的配套真库实证基座）。
 * 与 {@link PgTestEnv}（postgres:18）**平行互不共享**——engine 按 PG 18 开发，本环境把源库
 * 降级到 PG 17 验证协议端到端一致性；容器参数与 PgTestEnv 逐项相同（wal_level=logical、
 * max_prepared_transactions=16、logical_decoding_work_mem=64kB 流式驱逐、max_slot_wal_keep_size
 * 兜底），确保两版本的测试差异只来自版本本身。类加载即启动（需要本机 Docker），仅在
 * 引用本类的测试（Pg17CompatTest）运行时拉起，不影响其余 IT 的容器拓扑。
 */
public final class Pg17TestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(Pg17TestEnv.class);

    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test")
            .withCommand(
                    "postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=16",
                    "-c", "max_wal_senders=16",
                    "-c", "max_prepared_transactions=16",
                    "-c", "logical_decoding_work_mem=64kB",
                    "-c", "max_slot_wal_keep_size=1GB");

    static {
        PG.start();
        LOG.info("PG 17 测试容器就绪: {}（logical_decoding_work_mem=64kB）", PG.getJdbcUrl());
    }

    private Pg17TestEnv() {
    }

    /** 服务端大版本号（{@code server_version_num}/10000，如 17）——供测试 fail-fast 校验镜像未漂移。 */
    public static int majorVersion() throws SQLException {
        try (Connection c = newSqlConnection();
             ResultSet rs = c.createStatement().executeQuery("SELECT current_setting('server_version_num')")) {
            rs.next();
            return Integer.parseInt(rs.getString(1)) / 10000;
        }
    }

    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    /** binary=false 形态；参数面（proto 4 + PARALLEL + two_phase + 反馈 2s）与 PgTestEnv.newConfig 一致。 */
    public static ReplicationConfig newConfig(String slotName, String publication) {
        return newConfig(slotName, publication, false);
    }

    /** binary=true 形态：pgoutput 二进制值模式（PG 16+ 选项，容器为 PG 17 满足）。 */
    public static ReplicationConfig newConfig(String slotName, String publication, boolean binary) {
        return new ReplicationConfig(
                PG.getHost(), PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                PG.getDatabaseName(), PG.getUsername(), PG.getPassword(),
                slotName, publication,
                4, StreamingMode.PARALLEL, true, binary, 2);
    }

    /** 逐条执行 DDL/DML（每语句独立 autocommit 事务）——建表/publication/TRUNCATE 等测试前置。 */
    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /** 先杀 walsender 再删槽；槽不存在等情况静默忽略（照 PgTestEnv.dropSlotQuietly 的清理语义）。 */
    public static void dropSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            boolean killed = false;
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slotName);
                try (ResultSet rs = ps.executeQuery()) {
                    killed = rs.next();
                }
            }
            if (killed) {
                Thread.sleep(200); // walsender 退出竞态：立即 drop 会报 replication slot is active
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        } catch (Exception e) {
            LOG.warn("清理槽 {} 失败: {}", slotName, e.getMessage());
        }
    }
}
