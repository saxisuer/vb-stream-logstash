package org.vastdata.vbstream.reader.it;

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
 * reader IT 共享的单例 PG 18 容器与 SQL 工具——连接器模块 it 包
 * {@code StreamPgTestEnv} 的配方翻译(裁剪:reader IT 不构造流式大事务,不可压缩载荷/
 * standby 位点工具不搬)。类加载即启动,需要本机 Docker;容器跨测试方法共享,故各测试
 * 方法用独立槽名(残留槽会从旧 confirmed_flush_lsn 续传,静默吞掉先于建流写入的事务)。
 * command 参数与连接器 IT 同款:wal_level=logical 是逻辑解码前提,
 * max_prepared_transactions=16 是 two_phase 档前置(冒烟面保持同配方,行为不因配置漂移)。
 */
public final class ReaderPgEnv {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderPgEnv.class);

    /** 单例容器:postgres:18 + 逻辑解码参数(连接器 IT 同款配方)。 */
    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18")
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
        LOG.info("reader IT 容器就绪: {}", PG.getJdbcUrl());
    }

    private ReaderPgEnv() {
    }

    /**
     * 新建普通 SQL 连接(测试线程建表/写入/查询用;与连接器自身的连接互不相干)。
     *
     * @return 已认证的 JDBC 连接,调用方负责关闭
     * @throws SQLException 连接失败原样上抛
     */
    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    /**
     * 逐条执行 DDL/DML(自动提交,每条一个语句)。
     *
     * @param statements 待执行语句序列
     * @throws SQLException 任一语句失败原样上抛
     */
    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /**
     * walsender 视角的客户端 flush 位点(来自 standby status update)是否已覆盖基线
     * (SQL 侧 {@code >=} 比较)——客户端 LSN 确认的<b>即时</b>可观测面。重启场景的安全
     * 停机前置用它而非槽目录的 confirmed_flush:后者由 walsender 在解码推进时
     * (candidate 机制)才落库,空闲库不推进(确认不丢失,下次 WAL 活动一步跳齐——引擎
     * Diag 实证记档,勿当 bug 排查);而 status 包的采纳即时反映在
     * pg_stat_replication.flush_lsn。覆盖判定取 {@code >=}:确认位等于事务 endLsn 即
     * "commit 结束位 ≤ 确认值",重启跳过该事务(连接器 at-least-once 的前沿锚定语义)。
     * 行不存在(无活跃 walsender)或 flush_lsn 为 NULL 时返回 false——反馈缺失时恒
     * false,可作非恒真断言。
     *
     * @param slotName    槽名
     * @param baselineLsn 基线 LSN 文本("X/Y" 形态,通常取已收事务记录的 lsn 最大值)
     * @return 已覆盖为 true,无活跃 walsender/NULL 位点返回 false
     * @throws SQLException 查询失败原样上抛
     */
    public static boolean standbyFlushCovers(String slotName, String baselineLsn) throws SQLException {
        try (Connection c = newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT r.flush_lsn >= ?::pg_lsn FROM pg_stat_replication r "
                             + "JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = ?")) {
            ps.setString(1, baselineLsn);
            ps.setString(2, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /**
     * 等待槽的 walsender 挂上(pg_stat_replication 出现该槽的行)——"建槽完成 +
     * START_REPLICATION 完成 + reader 循环在跑"的可观测汇合点。IT 在 start 引擎后、写入
     * 测试数据前必须等此条件:①建流前写入的事务 WAL 可能落在槽起点之前被跳过;
     * ②进行中的长写事务会拖住建槽的解码一致点等待,先等建流完成再写即可避开
     * (连接器 IT 同习语)。
     *
     * @param slotName     槽名
     * @param timeoutMillis 轮询超时(超时抛 AssertionError——环境/装配问题 fail-fast)
     * @throws InterruptedException sleep 被中断:恢复中断位上抛(测试放弃)
     */
    public static void awaitWalsender(String slotName, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (!walsenderAttached(slotName)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("walsender " + timeoutMillis + "ms 内未挂上槽 " + slotName
                        + "(建流未完成——引擎装配失败或环境异常)");
            }
            Thread.sleep(100);
        }
    }

    /**
     * 槽是否已有活跃 walsender(pg_stat_replication 有行)。查询失败按 false 处理
     * (轮询容错,由调用方的超时兜底)。
     *
     * @param slotName 槽名
     * @return walsender 已挂上为 true
     */
    private static boolean walsenderAttached(String slotName) {
        try (Connection c = newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT 1 FROM pg_stat_replication r "
                             + "JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
        catch (SQLException e) {
            return false;
        }
    }

    /**
     * 先杀 walsender 再删槽;槽不存在等情况静默忽略(WARN)——walsender 退出竞态:立即
     * drop 会报 replication slot is active,先 terminate 再缓冲 200ms(连接器 IT 同款)。
     *
     * @param slotName 槽名
     */
    public static void dropSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            boolean killed = false;
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slotName);
                try (ResultSet rs = ps.executeQuery()) {
                    killed = rs.next(); // 有行即存在活跃 walsender,已被要求终止
                }
            }
            if (killed) {
                Thread.sleep(200);
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        }
        catch (Exception e) {
            LOG.warn("清理槽 {} 失败: {}", slotName, e.getMessage());
        }
    }
}
