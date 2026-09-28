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
 * PG 17 出厂默认 logical_decoding_work_mem 档的平行单例容器（postgres:17，不传该参数即默认 64MB）。
 * 与 {@link Pg17TestEnv}（64kB 低阈值档）平行互不共享：现有流式场景全部靠压低阈值构造，本环境
 * 回答"不调参的出厂默认档，大事务流式驱逐与子事务回滚能否照常触发"。除 logical_decoding_work_mem
 * 外容器参数与 Pg17TestEnv 逐项相同，确保测试差异只来自这一项。类加载即启动（需要本机 Docker），
 * 仅 Pg17DefaultWorkMemTest（手动开关档，-Dvb.it.pg17.defaultmem=true）引用——常规 mvn test
 * 不拉起、不影响其余 IT 的容器拓扑。
 */
public final class Pg17DefaultMemTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(Pg17DefaultMemTestEnv.class);

    /** 故意不传 logical_decoding_work_mem：出厂默认 64MB 正是本环境的验证对象（防漂移断言见 {@link #workMemSetting}）。 */
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
                    "-c", "max_slot_wal_keep_size=1GB");

    static {
        PG.start();
        LOG.info("PG 17 默认 work_mem 档测试容器就绪: {}（logical_decoding_work_mem 未传参=出厂默认）", PG.getJdbcUrl());
    }

    private Pg17DefaultMemTestEnv() {
    }

    /** 服务端大版本号（{@code server_version_num}/10000，如 17）——供测试 fail-fast 校验镜像未漂移。 */
    public static int majorVersion() throws SQLException {
        try (Connection c = newSqlConnection();
             ResultSet rs = c.createStatement().executeQuery("SELECT current_setting('server_version_num')")) {
            rs.next();
            return Integer.parseInt(rs.getString(1)) / 10000;
        }
    }

    /** 当前 logical_decoding_work_mem 的服务端显示值（如 "64MB"）——验证容器确实运行在出厂默认档。 */
    public static String workMemSetting() throws SQLException {
        try (Connection c = newSqlConnection();
             ResultSet rs = c.createStatement().executeQuery("SELECT current_setting('logical_decoding_work_mem')")) {
            rs.next();
            return rs.getString(1);
        }
    }

    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    /** binary=false 形态；参数面与 Pg17TestEnv.newConfig 一致（proto 4 + PARALLEL + two_phase + 反馈 2s）。 */
    public static ReplicationConfig newConfig(String slotName, String publication) {
        return new ReplicationConfig(
                PG.getHost(), PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                PG.getDatabaseName(), PG.getUsername(), PG.getPassword(),
                slotName, publication,
                4, StreamingMode.PARALLEL, true, false, 2);
    }

    /** 逐条执行 DDL/DML（每语句独立 autocommit 事务）——建表/publication/TRUNCATE 等测试前置。 */
    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /** 先杀 walsender 再删槽；槽不存在等情况静默忽略（照 Pg17TestEnv.dropSlotQuietly 的清理语义）。 */
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
                ps.executeQuery();
            }
        } catch (Exception e) {
            LOG.warn("清理槽 {} 失败: {}", slotName, e.getMessage());
        }
    }
}
