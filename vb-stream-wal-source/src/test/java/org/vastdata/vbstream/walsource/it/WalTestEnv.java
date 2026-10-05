package org.vastdata.vbstream.walsource.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// 2.0.5 的规范类 org.testcontainers.postgresql.PostgreSQLContainer 是非泛型（无 <?> 形态），
// 此处用保留泛型签名的兼容类（与 engine 模块 PgTestEnv 同款），保持 PostgreSQLContainer<?> 用法
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.Container;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * WAL 直解源集成测试共享的单例 PG 18 容器与工具（对齐 engine 模块 {@code PgTestEnv} 范式）。
 *
 * <p>类加载即启动（需要本机 Docker），跨测试类共享——各测试类用独立槽名并在 finally 清理。
 * 容器参数：{@code wal_level=logical}（对齐仓库惯例，物理流兼容）、{@code max_replication_slots=16}、
 * {@code max_wal_senders=16}。物理槽不随复制流附加（pgjdbc 42.7.13 限制，见
 * {@code WalStreamReceiver} javadoc），drop 无需先杀 walsender，但保留与 engine 相同的
 * 先杀后删形态以防后续任务把流挂到槽上。</p>
 */
public final class WalTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(WalTestEnv.class);

    /** 单例容器：物理复制流 + 物理槽所需的全部服务端参数经 withCommand 注入。 */
    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("waldb")
            .withUsername("wal")
            .withPassword("wal")
            .withCommand(
                    "postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=16",
                    "-c", "max_wal_senders=16");

    /** 容器内数据目录（pg_waldump -p 用）——psql 本地 socket 查 {@code show data_directory} 惰性缓存。 */
    private static volatile String dataDirectory;

    static {
        PG.start();
        LOG.info("WAL 测试容器就绪: {}", PG.getJdbcUrl());
    }

    /** 私有构造器：静态单例环境，不可实例。 */
    private WalTestEnv() {
    }

    /**
     * 新建一条普通 SQL 会话连接。
     *
     * <p>调用方负责关闭；物理复制连接不经本方法（见 {@code WalStreamReceiver} 内部拼装）。</p>
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
     * 服务端 {@code server_version_num}（如 180000）——布局分发 {@code WalLayouts} 的输入。
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
     * 顺序执行若干 SQL 语句（各自独立自动提交）。
     *
     * @param statements 语句列表
     * @throws SQLException 任一语句失败即抛（已执行的语句不回滚）
     */
    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /**
     * 在容器内执行 pg_waldump 抽取窗口内的记录行。
     *
     * <p>关键步骤：惰性查一次 {@code show data_directory}（经本地 socket psql，容器环境变量
     * POSTGRES_USER/POSTGRES_DB 免密），随后 {@code pg_waldump -p <dir> -s <start> -e <end>}，
     * stdout+stderr 合流后仅保留 {@code "rmgr: "} 开头的记录行。边界与异常语义：pg_waldump
     * 在窗口末端截到半条记录时常以非零码退出——这是预期形态，不视为失败；真正的环境问题
     * （无二进制/无 WAL 文件）表现为零输出行，由调用方 census 断言兜底暴露。</p>
     *
     * @param startLsn 窗口起点（含；PG 文本 LSN 由 {@link Lsn#format(long)} 渲染）
     * @param endLsn   窗口终点（不含）
     * @return pg_waldump 输出的记录行（已过滤非记录行）
     * @throws IOException          容器 exec 失败
     * @throws InterruptedException 容器 exec 被中断
     */
    public static List<String> pgWaldump(long startLsn, long endLsn) throws IOException, InterruptedException {
        String dir = dataDirectory();
        Container.ExecResult r = PG.execInContainer("sh", "-c",
                "pg_waldump -p '" + dir + "' -s " + Lsn.format(startLsn) + " -e " + Lsn.format(endLsn) + " 2>&1");
        return r.getStdout().lines()
                .filter(line -> line.startsWith("rmgr: "))
                .toList();
    }

    /**
     * 容器内数据目录（惰性缓存）。
     *
     * <p>关键步骤：{@code psql -U $POSTGRES_USER -d $POSTGRES_DB -tA -c 'show data_directory'}
     * 走 Unix socket 的 trust 认证免密；结果 trim 后缓存到 volatile 字段（静态块启动后仅
     * 首个调用者执行查询）。失败（退出码非零）抛 ISE 附 stdout/stderr 诊断。</p>
     *
     * @return PGDATA 绝对路径（容器内）
     * @throws IOException          exec 失败
     * @throws InterruptedException exec 被中断
     */
    private static String dataDirectory() throws IOException, InterruptedException {
        String dir = dataDirectory;
        if (dir == null) {
            Container.ExecResult r = PG.execInContainer("sh", "-c",
                    "psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -tA -c 'show data_directory'");
            if (r.getExitCode() != 0) {
                throw new IllegalStateException("show data_directory 失败: " + r.getStderr());
            }
            dataDirectory = dir = r.getStdout().trim();
            LOG.info("容器 PGDATA: {}", dir);
        }
        return dir;
    }

    /**
     * 静默删除物理槽（先杀活跃 walsender 再删）；槽不存在等情况仅 WARN。
     *
     * <p>关键步骤：{@code pg_terminate_backend(active_pid)} 解除潜在占用 → 200ms 竞态退避 →
     * {@code pg_drop_replication_slot}。任何异常吞掉记 WARN——测试清理路径不得掩盖断言失败。</p>
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
