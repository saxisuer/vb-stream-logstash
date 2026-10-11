package org.vastdata.vbstream.walsource.it;

import org.opengauss.copy.CopyManager;
import org.opengauss.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * openGauss 5.0.0 集成测试共享的单例容器与工具（M1 Task 8——对齐 {@code WalTestEnv} 的
 * 类加载即启动范式，跨测试类共享，需本机 Docker）。
 *
 * <p><strong>容器形态</strong>：官方镜像 {@code opengauss/opengauss:5.0.0}（非 PostgreSQL
 * 系镜像——entrypoint/GS_PASSWORD/起库节奏完全不同，用 {@link GenericContainer} 裸容器），
 * GS_PASSWORD 口令策略要求大小写+数字+特殊字符（{@code Gauss_12345} 的下划线会被
 * entrypoint 拒绝，实测锚）。SQL 面走 opengauss-jdbc（{@code jdbc:opengauss:} URL，
 * 模块 pom 的 runtime 依赖在 test classpath 可见）；复制面由 {@code DirectWalSource}
 * 裸 v3 客户端承担（pgjdbc/opengauss-jdbc 的 replication API 在 OG 上物理流不可用）。</p>
 *
 * <p><strong>wait 策略 = JDBC 重试</strong>（brief 裁定）：镜像起库 60-90s 且日志无稳定
 * 就绪行，GenericContainer 无内建 wait——静态块内以 opengauss-jdbc 轮询建连（上限 5 分钟，
 * 3s 间隔），成功即认为库就绪。就绪后施加两件初始化：</p>
 * <ol>
 *   <li><strong>replication hba 硬前提</strong>（Task 5 裁定）：openGauss 的
 *       {@code host all all} 行不匹配 replication 伪库连接——追加显式
 *       {@code host replication gaussdb 0.0.0.0/0 md5} 行（镜像缺省 pg_hba 恰已含同形行，
 *       grep 幂等防漂移）+ {@code pg_reload_conf()} 生效。口令验证器是 sha256 单存
 *       （password_encryption_type=2）+ hba md5 → 宿主直连命中 AUTH_REQ_MD5_SHA256（码 11）
 *       ——{@code DirectWalSource}/{@code OgAuth} 的真实验证面。</li>
 *   <li><strong>段落盘 bind 卷</strong>：宿主临时目录 bind 到容器 {@code /wal-copy}
 *       （READ_WRITE）+ root exec {@code chmod 777}（bind 属主 root，容器内 pg_receivexlog
 *       以 opengauss 用户写）——File 通道 external 形态的共享面：容器内 pg_receivexlog
 *       落盘、宿主测试线程 tail 同一目录。</li>
 * </ol>
 */
public final class OgTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(OgTestEnv.class);

    /** 初始账户（镜像固定——openGauss 初始账户，复制权限内建，pg_roles 标志位掩藏不反映）。 */
    public static final String USER = "gaussdb";

    /** 口令（GS_PASSWORD；镜像策略要求大小写+数字+特殊字符，下划线不算特殊字符）。 */
    public static final String PASSWORD = "OgTest#2026";

    /** 库名（镜像缺省 postgres 库）。 */
    public static final String DATABASE = "postgres";

    /** 容器内数据目录（pg_hba.conf 所在，镜像固定路径）。 */
    private static final String PG_DATA = "/var/lib/opengauss/data";

    /** 容器内 pg_receivexlog 落盘 bind 点（File 通道 external 形态）。 */
    public static final String CONTAINER_WAL_DIR = "/wal-copy";

    /** 容器内 openGauss bin/lib 路径（gsql/pg_receivexlog/gs_ctl 的运行环境）。 */
    private static final String OG_HOME = "/usr/local/opengauss";

    /** JDBC 就绪重试上限（毫秒）——镜像起库 60-90s，留足余量。 */
    private static final long SQL_READY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

    /** JDBC 就绪重试间隔（毫秒）。 */
    private static final long SQL_READY_RETRY_MS = 3_000;

    /** 段落盘 bind 的宿主侧目录（构造容器前先建——bind 参数需要已存在的路径语义由容器端创建）。 */
    private static final Path HOST_WAL_DIR = createHostWalDir();

    /** 单例容器：暴露 5432，GS_PASSWORD 驱动 entrypoint 建库。 */
    public static final GenericContainer<?> OG = new GenericContainer<>("opengauss/opengauss:5.0.0")
            .withExposedPorts(5432)
            .withEnv("GS_PASSWORD", PASSWORD)
            .withFileSystemBind(HOST_WAL_DIR.toString(), CONTAINER_WAL_DIR, BindMode.READ_WRITE)
            .withStartupTimeout(Duration.ofMinutes(2));

    static {
        OG.start();   // GenericContainer 无 wait 策略——就绪由 JDBC 重试承担
        awaitSqlReady();
        ensureReplicationHba();
        prepareWalCopyDir();
        LOG.info("openGauss 测试容器就绪: {} 密码认证面=码11(md5 hba+sha256 存储) 段bind={}",
                sqlJdbcUrl(), HOST_WAL_DIR);
    }

    /** 私有构造器：静态单例环境，不可实例。 */
    private OgTestEnv() {
    }

    /**
     * 建宿主段落盘目录（容器 bind 目标）。
     *
     * <p>调用点在静态字段初始化链上（容器构造前）；IOException 包装
     * {@link UncheckedIOException} 上抛（静态块失败 = 类加载失败 = 测试失败，语义正确）。</p>
     *
     * @return 已创建的临时目录
     */
    private static Path createHostWalDir() {
        try {
            return Files.createTempDirectory("og-it-wal-copy");
        } catch (IOException e) {
            throw new UncheckedIOException("段落盘 bind 宿主目录创建失败", e);
        }
    }

    /**
     * JDBC 重试直至库就绪（镜像起库 60-90s）。
     *
     * <p>关键步骤：opengauss-jdbc 建连（TCP 密码认证命中 {@code host all all 0.0.0.0/0 sha256}
     * 缺省行——hba 追加前的可行面）→ 成功即返回；SQLException 则睡 3s 重试。超时抛 ISE
     * （附最后异常链）——环境级失败 fail-fast。</p>
     */
    private static void awaitSqlReady() {
        long deadline = System.currentTimeMillis() + SQL_READY_TIMEOUT_MS;
        SQLException last = null;
        while (System.currentTimeMillis() < deadline) {
            try (Connection c = newSqlConnection()) {
                return;
            } catch (SQLException e) {
                last = e;
                sleepQuiet(SQL_READY_RETRY_MS);
            }
        }
        throw new IllegalStateException("openGauss 容器 " + SQL_READY_TIMEOUT_MS
                + "ms 内未就绪（JDBC 重试耗尽）", last);
    }

    /**
     * 施加 replication hba 硬前提（Task 5 裁定——Task 8 部署面落点）。
     *
     * <p>关键步骤：root exec 幂等追加 {@code host replication gaussdb 0.0.0.0/0 md5} 行
     * （grep 命中即跳过——镜像缺省 pg_hba 恰含同形行，追加是防镜像漂移的确定性保证而非
     * 必要条件）；JDBC {@code SELECT pg_reload_conf()} 令其生效（vanilla 同名函数 OG 在档，
     * 免去容器内 gs_ctl reload 的 su/LD_LIBRARY_PATH 环境拼装）。exec 失败抛 ISE——
     * 无该行时宿主侧复制伪库连接被拒，后续 IT 必然全红，前置 fail-fast 优于逐个红。</p>
     */
    private static void ensureReplicationHba() {
        try {
            OG.execInContainer("bash", "-c",
                    "grep -q 'host replication gaussdb' " + PG_DATA + "/pg_hba.conf"
                            + " || echo 'host replication gaussdb 0.0.0.0/0 md5' >> " + PG_DATA + "/pg_hba.conf");
            execSql("SELECT pg_reload_conf()");
        } catch (Exception e) {
            throw new IllegalStateException("replication hba 施加失败", e);
        }
    }

    /**
     * 段落盘 bind 卷的就绪准备：root exec chmod 777。
     *
     * <p>bind 目录容器侧属主 root，而 pg_receivexlog 以 opengauss（uid 1000）用户写段
     * 文件——777 放行（IT 专用容器，权限面不敏感）。</p>
     */
    private static void prepareWalCopyDir() {
        try {
            OG.execInContainer("chmod", "777", CONTAINER_WAL_DIR);
        } catch (Exception e) {
            throw new IllegalStateException("段落盘 bind 卷权限准备失败", e);
        }
    }

    /**
     * SQL 会话 JDBC URL（opengauss-jdbc 前缀）。
     *
     * @return 形如 {@code jdbc:opengauss://host:mappedPort/postgres}
     */
    public static String sqlJdbcUrl() {
        return "jdbc:opengauss://" + host() + ":" + port() + "/" + DATABASE;
    }

    /**
     * 新建一条普通 SQL 会话连接（opengauss-jdbc）。
     *
     * <p>调用方负责关闭；复制连接不经本方法（DirectWalSource 自建裸 v3 通道）。</p>
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 连接失败
     */
    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(sqlJdbcUrl(), USER, PASSWORD);
    }

    /** @return 容器映射主机名 */
    public static String host() {
        return OG.getHost();
    }

    /** @return 容器映射端口 */
    public static int port() {
        return OG.getMappedPort(5432);
    }

    /**
     * 顺序执行若干 SQL 语句（各自独立自动提交）。
     *
     * @param statements 语句列表
     * @throws SQLException 任一语句失败即抛（已执行语句不回滚）
     */
    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /**
     * 服务端当前 write 位点（OG 无 flush 档的位点函数——引导一致性次序契约的同款面）。
     *
     * @return 打包 LSN
     * @throws SQLException 查询失败
     */
    public static long currentXlogLocation() throws SQLException {
        try (Connection c = newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_current_xlog_location()::text")) {
            rs.next();
            return Lsn.parse(rs.getString(1));
        }
    }

    /**
     * 客户端侧 COPY FROM STDIN（opengauss-jdbc CopyManager——服务端侧 COPY 文件被 OG
     * 安全策略禁止，实测锚 {@code COPY to or from a file is prohibited}）。
     *
     * <p>用途：双通道对拍 IT 的 MULTI_INSERT 触发面（COPY 批量装载走 heap2 multi-insert
     * 记录，Task 7 已知限制的语料源——记录面在 walker 层透明，对拍不受 HeapViews 的
     * OG MULTI_INSERT ISE 影响）。</p>
     *
     * @param sql COPY 命令（形如 {@code COPY t FROM STDIN WITH (FORMAT csv)}）
     * @param csv 装载文本
     * @return 装载行数
     * @throws SQLException COPY 失败
     */
    public static long copyIn(String sql, String csv) throws SQLException {
        try (Connection c = newSqlConnection()) {
            CopyManager cp = new CopyManager(c.unwrap(BaseConnection.class));
            return cp.copyIn(sql, new StringReader(csv));
        } catch (IOException e) {
            throw new SQLException("COPY FROM STDIN 流读写失败", e);
        }
    }

    /**
     * 容器内以 opengauss 用户拉起 detached pg_receivexlog 落盘到 bind 卷
     * （File 通道 external 形态——managed 需宿主二进制不可行，IT 一律 external）。
     *
     * <p>关键步骤：{@code docker exec -d} + nohup 后台化（exec 主壳立即退出、子进程存活，
     * 实测锚）；环境面 LD_LIBRARY_PATH（容器内 openGauss 二进制的动态库）+ PGPASSWORD
     * （密码绝不上命令行，与 {@code ReceivexlogProcess} 同契约）；回环 127.0.0.1 连接命中
     * 镜像缺省 pg_hba 的 replication 行。重复调用会叠多个进程——调用方负责先
     * {@link #stopReceivexlog()}。</p>
     */
    public static void startReceivexlog() {
        try {
            OG.execInContainer("bash", "-c",
                    "export LD_LIBRARY_PATH=" + OG_HOME + "/lib PGPASSWORD=" + PASSWORD + "; "
                            + "nohup " + OG_HOME + "/bin/pg_receivexlog -D " + CONTAINER_WAL_DIR
                            + " -h 127.0.0.1 -p 5432 -U " + USER + " > /tmp/pg_receivexlog-it.log 2>&1 &");
        } catch (Exception e) {
            throw new IllegalStateException("容器内 pg_receivexlog 拉起失败", e);
        }
    }

    /**
     * 终止容器内 pg_receivexlog（pkill 按名匹配，无进程时非零退出属预期——吞掉）。
     */
    public static void stopReceivexlog() {
        try {
            OG.execInContainer("bash", "-c", "pkill -x pg_receivexlog || true");
        } catch (Exception e) {
            LOG.warn("pkill pg_receivexlog 失败（忽略）: {}", e.getMessage());
        }
    }

    /**
     * pg_receivexlog 的容器内日志尾（排障面——段拉不起时 IT 失败消息引用）。
     *
     * @return 日志尾文本（无日志文件为空串）
     */
    public static String receivexlogLogTail() {
        try {
            return OG.execInContainer("bash", "-c", "tail -5 /tmp/pg_receivexlog-it.log 2>/dev/null")
                    .getStdout();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 段落盘 bind 的宿主侧目录（File 通道 tail 目标 = 与容器内 /wal-copy 同一文件两面）。
     *
     * @return 宿主侧路径
     */
    public static Path hostWalDir() {
        return HOST_WAL_DIR;
    }

    /**
     * 可中断吞噬的睡（初始化重试节拍——静态块上下程无中断面，防御位）。
     *
     * @param ms 睡眠毫秒
     */
    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
