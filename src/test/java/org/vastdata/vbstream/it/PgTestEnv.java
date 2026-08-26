package org.vastdata.vbstream.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/** 集成测试共享的单例 PG 18 容器与工具。类加载即启动（需要本机 Docker）。 */
public final class PgTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(PgTestEnv.class);

    public static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:18")
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
        LOG.info("测试容器就绪: {}（logical_decoding_work_mem=64kB）", PG.getJdbcUrl());
    }

    private PgTestEnv() {
    }

    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    public static ReplicationConfig newConfig(String slotName, String publication) {
        return new ReplicationConfig(
                PG.getHost(), PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                PG.getDatabaseName(), PG.getUsername(), PG.getPassword(),
                slotName, publication,
                4, StreamingMode.PARALLEL, true, 2);
    }

    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /** 先杀 walsender 再删槽；槽不存在等情况静默忽略。 */
    public static void dropSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slotName);
                ps.executeQuery();
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
