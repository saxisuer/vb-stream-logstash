package org.vastdata.vbstream.walsource.it;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
// 2.0.5 的规范类 org.testcontainers.postgresql.PostgreSQLContainer 是非泛型（无 <?> 形态），
// 此处用保留泛型签名的兼容类（与 WalTestEnv/PgTestEnv 同款），保持 PostgreSQLContainer<?> 用法
import org.testcontainers.containers.PostgreSQLContainer;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.walsource.api.WalSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 双路对拍（Task 8）共享的单例 PG 18 容器与两路装配面——engine 逻辑解码路
 * （{@link EnginePathRunner}）与 wal 直解路（{@link WalSource} DML 面）跑同一容器，
 * 各自独立槽位、共用 publication {@code vb_parity}。
 *
 * <p>容器参数：<b>{@code logical_decoding_work_mem=64MB}（禁驱逐）</b>——场景 1 的事务
 * 形态钉 NORMAL 路径（无 STREAM-* 流式分段），两路输出面确定性；{@code wal_level=logical}
 * （物理流 + 逻辑槽双兼容）、槽位/walsender 上限 16（两路各占一槽，重跑残留 + 并行测试类
 * 余量）；{@code wal_compression=off}（wal 路 PageImages 不解压压缩 FPW 的运维前提）；
 * {@code max_slot_wal_keep_size=1GB} 兜底。</p>
 *
 * <p><b>场景重置契约</b>（{@link #resetScenario(String...)}）：每场景重建 parity schema
 * + 对拍表 DDL + publication（FOR ALL TABLES——覆盖未来建表），并清两路槽位（先杀
 * 活跃 walsender 再删）——两路槽位都在 DDL 之后、DML 之前重建，起停窗口差最小化
 * （对拍归一化按 xid 交集兜底残余差）。</p>
 */
public final class ParityEnv {

    private static final Logger LOG = (Logger) LoggerFactory.getLogger(ParityEnv.class);

    /** engine 路逻辑槽名（pgoutput；每场景重建）。 */
    public static final String ENGINE_SLOT = "vb_parity_engine";

    /** wal 路物理槽名（每场景重建）。 */
    public static final String WAL_SLOT = "vb_parity_wal";

    /** 两路共用的 publication（FOR ALL TABLES，每场景重建）。 */
    public static final String PUBLICATION = "vb_parity";

    /** 单例容器：双路共需的全部服务端参数经 withCommand 注入。 */
    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("paritydb")
            .withUsername("parity")
            .withPassword("parity")
            .withCommand(
                    "postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=16",
                    "-c", "max_wal_senders=16",
                    "-c", "logical_decoding_work_mem=64MB",
                    "-c", "wal_compression=off",
                    "-c", "max_slot_wal_keep_size=1GB");

    static {
        PG.start();
        LOG.info("双路对拍容器就绪: {}", PG.getJdbcUrl());
    }

    /** 私有构造器：静态单例环境，不可实例。 */
    private ParityEnv() {
    }

    /** @return 新建一条普通 SQL 会话连接（调用方负责关闭）。 @throws SQLException 连接失败 */
    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    /** @return 容器映射主机名。 */
    public static String host() {
        return PG.getHost();
    }

    /** @return 容器映射端口。 */
    public static int port() {
        return PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

    /**
     * engine 路复制配置：proto 4 + <b>streaming OFF</b>（钉 NORMAL 路径——无 STREAM-*
     * 生命周期行，输出面确定性）+ twoPhase false（场景 1 无 2PC，避免 P/K 生命周期行）+
     * binary false（text 模式与 wal 路 {@code DiskValueRenderer} 的 PG text 渲染矩阵
     * 对齐）+ 反馈 2s。
     *
     * @return 已指向本容器与 ENGINE_SLOT/PUBLICATION 的配置
     */
    public static ReplicationConfig engineConfig() {
        return new ReplicationConfig(
                host(), port(), PG.getDatabaseName(), PG.getUsername(), PG.getPassword(),
                ENGINE_SLOT, PUBLICATION,
                4, StreamingMode.OFF, false, false, 2);
    }

    /**
     * wal 路门面配置：六键指向本容器、槽位 WAL_SLOT；state 三键缺省（检查点禁用——
     * 每场景全新引导）；DML 面 vb.wal.dml 缺省 true、白名单缺省全放行（对拍表只有
     * parity schema 一张）。
     *
     * @return WalSource 构造入参配置
     */
    public static Properties walSourceConfig() {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(port()));
        cfg.setProperty(WalSource.KEY_DB, PG.getDatabaseName());
        cfg.setProperty(WalSource.KEY_USER, PG.getUsername());
        cfg.setProperty(WalSource.KEY_PASS, PG.getPassword());
        cfg.setProperty(WalSource.KEY_SLOT, WAL_SLOT);
        return cfg;
    }

    /**
     * 场景重置：清两路槽位 → 重建 parity schema（CASCADE 连表带 DDL 一起清）→ 执行
     * 本场景表 DDL → 重建 publication（FOR ALL TABLES）。
     *
     * <p>关键步骤：①两槽先杀活跃 walsender 再删（占用中 drop 会报 slot is active）；
     * ②{@code DROP SCHEMA parity CASCADE} + {@code CREATE SCHEMA parity}——DDL 与数据
     * 一并清空且不产生 TRUNCATE 记录（TRUNCATE 会让 engine 路多出 wal 路不发射的
     * TRUNCATE 行）；③表 DDL 逐条执行；④publication 先 DROP 后 CREATE（FOR ALL
     * TABLES 覆盖刚建的表）。执行序保证两路槽位重建（路径启动时）晚于 DDL——引导
     * /Relation 缓存均见表的最终形态。</p>
     *
     * @param tableDdl 本场景的建表语句（在 parity schema 内）
     * @throws SQLException 任一语句失败
     */
    public static void resetScenario(String... tableDdl) throws SQLException {
        dropLogicalSlotQuietly(ENGINE_SLOT);
        dropPhysicalSlotQuietly(WAL_SLOT);
        List<String> sql = new java.util.ArrayList<>();
        sql.add("DROP PUBLICATION IF EXISTS " + PUBLICATION);
        sql.add("DROP SCHEMA IF EXISTS parity CASCADE");
        sql.add("CREATE SCHEMA parity");
        sql.addAll(List.of(tableDdl));
        sql.add("CREATE PUBLICATION " + PUBLICATION + " FOR ALL TABLES");
        execSql(sql.toArray(new String[0]));
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
     * 静默删除逻辑槽（先杀活跃 walsender 再删）；槽不存在等情况仅 WARN——测试清理路径
     * 不得掩盖断言失败。
     *
     * @param slotName 槽名
     */
    public static void dropLogicalSlotQuietly(String slotName) {
        dropSlotQuietly(slotName);
    }

    /**
     * 静默删除物理槽（同 {@link #dropLogicalSlotQuietly} 形态——pgjdbc 物理流不附槽，
     * 杀后端属防御性清理）。
     *
     * @param slotName 槽名
     */
    public static void dropPhysicalSlotQuietly(String slotName) {
        dropSlotQuietly(slotName);
    }

    /**
     * 先杀后删的槽清理公共面（逻辑/物理槽同形：{@code pg_drop_replication_slot}）。
     *
     * @param slotName 槽名
     */
    private static void dropSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
            Thread.sleep(200);   // walsender 退出竞态：立即 drop 会报 slot is active
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        } catch (Exception e) {
            LOG.warn("清理槽 {} 失败: {}", slotName, e.getMessage());
        }
    }

    /**
     * 建一个 CDC logger 捕获器：logback {@link ListAppender} 挂到指定 logger（engine 路
     * {@code org.vastdata.vbstream.cdc} / wal 路 {@code org.vastdata.vbstream.walsource.cdc}），
     * 只收该 logger 的行——系统诊断日志与吞吐统计行走别的 logger，捕获面天然干净。
     *
     * @param loggerName CDC 专用 logger 名
     * @return 捕获器（AutoCloseable——停流后 {@link CdcCapture#closeAndDrain()} 摘除并取行）
     */
    public static CdcCapture capture(String loggerName) {
        return new CdcCapture(loggerName);
    }

    /**
     * 对拍窗口差留痕：交集外的 xid 只作控制台 INFO 说明（起停窗口差异归因面），不作
     * 断言失败。
     *
     * @param scenario   场景名
     * @param engineOnly engine 路独有 xid 集
     * @param walOnly    wal 路独有 xid 集
     */
    public static void logParityNotice(String scenario, Set<Long> engineOnly, Set<Long> walOnly) {
        LOG.info("对拍窗口差（{}）: engine 独有 xid={}, wal 独有 xid={}——起停窗口差异属两路固有，"
                + "交集内已逐行对齐", scenario, engineOnly, walOnly);
    }

    /**
     * CDC logger 捕获器：挂 {@link ListAppender} 收指定 logger 的格式化消息行，停流后
     * 摘除并快照取行。
     *
     * <p>线程语义：捕获行由两路各自的输出线程（engine 的 transaction-consumer / wal 的
     * wal-receiver）追加；{@link #closeAndDrain()} 在停流 join 之后调用（happens-before
     * 经线程结束建立），届时无并发写。</p>
     */
    public static final class CdcCapture {

        private final Logger logger;

        private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        /**
         * 挂 appender 到目标 logger（构造即生效——先挂后起流，头部行不漏）。
         *
         * @param loggerName CDC 专用 logger 名
         */
        private CdcCapture(String loggerName) {
            this.logger = (Logger) LoggerFactory.getLogger(loggerName);
            appender.start();
            logger.addAppender(appender);
        }

        /**
         * 摘除 appender 并取全部已捕获行（须在对应路径停流之后调用——线程join 后读，
         * 无并发追加）。
         *
         * @return 格式化消息行（输出序保持）
         */
        public List<String> closeAndDrain() {
            logger.detachAppender(appender);
            return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        }
    }
}
