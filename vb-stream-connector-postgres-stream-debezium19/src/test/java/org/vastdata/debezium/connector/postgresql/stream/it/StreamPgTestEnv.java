package org.vastdata.debezium.connector.postgresql.stream.it;

import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 连接器 IT 共享的单例 PG 18 容器与工具,引擎 it 包 {@code org.vastdata.vbstream.it.PgTestEnv}
 * 的翻译重写(零引擎 import,行为对齐)。类加载即启动,需要本机 Docker;容器跨 IT 类共享,
 * 故各测试类用独立槽名(残留槽会从旧 confirmed_flush_lsn 续传,静默吞掉先于建流写入的事务)。
 * command 参数与引擎同款:流式构造的关键是 {@code logical_decoding_work_mem=64kB}
 * (reorder buffer 全局驱逐阈值,少量不可压缩行即触发进行中流式下发)与
 * {@code max_prepared_transactions=16}(two_phase/parallel 档前置)。
 */
public final class StreamPgTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(StreamPgTestEnv.class);

    /** 单例容器:postgres:18 + 逻辑解码参数(跨测试类共享,与引擎 it 包同一配方)。 */
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
                    "-c", "logical_decoding_work_mem=64kB",
                    "-c", "max_slot_wal_keep_size=1GB");

    static {
        PG.start();
        LOG.info("连接器 IT 容器就绪: {}（logical_decoding_work_mem=64kB）", PG.getJdbcUrl());
    }

    private StreamPgTestEnv() {
    }

    /**
     * 新建普通 SQL 连接(测试线程写入/查询用;与连接器自身的连接互不相干)。
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
     * 单事务内按行间 sleep 插入不可压缩载荷——流式大事务的标准构造(引擎 it 包同款):
     * 载荷 512 段十六进制串拼接 ≈16KB(对应 SQL 形态 {@code string_agg(md5(random()::text),'')
     * FROM generate_series(1,512)}),pglzip 压不动(TOAST 压缩后记账,规则图案会被压到
     * 百字节级永远越不过 64kB 阈值);行间跨秒分批使 reorder buffer 的 {@code rb->size}
     * (全局)在事务进行中累计越过 64kB,驱逐发生、pgoutput 以 StreamStart/Stream* 块
     * 边收边发,而不是等提交后整体回放。
     *
     * @param table          目标表(列序 id, payload)
     * @param idFrom         起始 id(含),逐行 +1
     * @param rows           行数
     * @param interRowMillis 行间 sleep 毫秒数(跨秒分批,给服务端驱逐窗口)
     * @return id → 实际插入的载荷文本(记录值完整性断言的期望源)
     * @throws SQLException 插入失败原样上抛
     * @throws InterruptedException sleep 被中断:恢复中断位上抛(测试放弃)
     */
    public static Map<Integer, String> insertIncompressibleRows(String table, int idFrom, int rows,
                                                                long interRowMillis) throws SQLException, InterruptedException {
        Map<Integer, String> payloads = new LinkedHashMap<>();
        try (Connection c = newSqlConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
                java.util.Random rnd = new java.util.Random();
                for (int i = 0; i < rows; i++) {
                    String payload = incompressiblePayload(rnd);
                    payloads.put(idFrom + i, payload);
                    ps.setInt(1, idFrom + i);
                    ps.setString(2, payload);
                    ps.executeUpdate();
                    Thread.sleep(interRowMillis);
                }
            }
            finally {
                c.commit();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        return payloads;
    }

    /**
     * 生成 ≈16KB 不可压缩载荷:512 段 32 字符十六进制串拼接(512×32=16384 字符,每段
     * 来自独立随机 long 的高低位展开——十六进制字符分布均匀,pglzip 找不到重复模式)。
     *
     * @param rnd 随机源(调用方持有,行间不重播种子)
     * @return 16384 字符的十六进制文本
     */
    public static String incompressiblePayload(java.util.Random rnd) {
        StringBuilder sb = new StringBuilder(16384);
        for (int i = 0; i < 512; i++) {
            long v = rnd.nextLong();
            sb.append(String.format("%016x", v >>> 32)).append(String.format("%016x", v & 0xffffffffL));
        }
        return sb.toString();
    }

    /**
     * 查询单值 LSN 并解析为 long:结果以十六进制 "X/Y" 字符串形态经
     * {@link LogSequenceNumber#valueOf(String)} 解析(引擎 it 包同款先例)。
     * 无结果行或值为 null 抛 IllegalStateException(锚点查询必然有值,缺失即环境异常,
     * fail-fast 不猜测)。
     *
     * @param sql 返回单列 LSN 文本的查询
     * @return 解析后的 LSN long 值
     * @throws SQLException 查询失败原样上抛
     */
    public static long lsnOf(String sql) throws SQLException {
        try (Connection c = newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next() || rs.getString(1) == null) {
                throw new IllegalStateException("LSN 查询无结果行或为 null: " + sql);
            }
            return LogSequenceNumber.valueOf(rs.getString(1)).asLong();
        }
    }

    /**
     * 当前槽的 confirmed_flush_lsn("0/0" 经 valueOf 解析为 0;槽不存在抛 ISE——调用方
     * 应在建槽后调用)。
     *
     * @param slotName 槽名
     * @return confirmed_flush_lsn 的 long 形态
     * @throws SQLException 查询失败原样上抛
     */
    public static long confirmedFlushLsn(String slotName) throws SQLException {
        return lsnOf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='" + slotName + "'");
    }

    /**
     * confirmed_flush_lsn 是否已越过基线(SQL 侧 pg_lsn 比较)。
     *
     * @param slotName    槽名
     * @param baselineLsn 基线 LSN 文本("X/Y" 形态)
     * @return 已越过为 true;槽不存在返回 false(可作非恒真断言)
     * @throws SQLException 查询失败原样上抛
     */
    public static boolean confirmedFlushBeyond(String slotName, String baselineLsn) throws SQLException {
        try (Connection c = newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT confirmed_flush_lsn > ?::pg_lsn FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, baselineLsn);
            ps.setString(2, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /**
     * walsender 视角的客户端 flush 位点(来自 standby status update)是否已越过基线。
     * 行不存在(无活跃 walsender)或 flush_lsn 为 NULL 时返回 false——反馈代码缺失时恒
     * false,可作非恒真断言(亚秒反馈冒烟用:服务端对 status 包的采纳面)。
     *
     * @param slotName    槽名
     * @param baselineLsn 基线 LSN 文本
     * @return 已越过为 true,无活跃 walsender/false 位点返回 false
     * @throws SQLException 查询失败原样上抛
     */
    public static boolean standbyFlushBeyond(String slotName, String baselineLsn) throws SQLException {
        try (Connection c = newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT r.flush_lsn > ?::pg_lsn FROM pg_stat_replication r "
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
     * START_REPLICATION 完成 + reader 循环在跑"的可观测汇合点。IT 在 start 引擎后、
     * 写入测试数据前必须等此条件:①建流前写入的事务 WAL 可能落在 restart_lsn 之前,
     * 槽直接跳过不重放(引擎 it 包同习语);②进行中的长写事务会拖住
     * pg_create_logical_replication_slot 的解码一致点等待(建槽与其快照之前的进行中
     * XID 事务竞态),先等建流完成再写即可避开。
     *
     * @param slotName 槽名
     * @param timeoutMillis 轮询超时(超时抛 AssertionError——环境/装配问题 fail-fast)
     */
    public static void awaitWalsender(String slotName, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (!walsenderAttached(slotName)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("walsender " + timeoutMillis + "ms 内未挂上槽 " + slotName
                        + "(建流未完成——连接器装配失败或环境异常)");
            }
            Thread.sleep(100);
        }
    }

    /**
     * 槽是否已有活跃 walsender(pg_stat_replication 有行:建槽已完成且 START_REPLICATION
     * 已完成)。查询失败按 false 处理(轮询容错,由调用方的超时兜底)。
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
     * drop 会报 replication slot is active,先 terminate 再缓冲 200ms。
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
