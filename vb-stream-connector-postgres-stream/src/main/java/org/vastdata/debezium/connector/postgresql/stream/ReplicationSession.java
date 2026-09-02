package org.vastdata.debezium.connector.postgresql.stream;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * pgoutput 复制会话:两条连接(普通 SQL + replication=database),生命周期
 * open → ensureSlot → start → run → close。引擎
 * {@code org.vastdata.vbstream.replication.PgReplicationSession}(225 行)的 1:1 重写
 * (文字参照,非依赖);配置来源改为 {@link Parameters} 参数包——由调用方(流式源)从
 * {@link PostgresStreamConnectorConfig} 组装,会话自身不感知 Connect 配置面,引擎的
 * {@code ReplicationConfig} record 因此不再需要。
 *
 * <p>行为红线(ReplicationSessionTest 离线锚定,真库行为归 Task 8 IT):run 循环五步序
 * (isClosed 守卫 → drain → 封顶回写 → 满间隔才 forceUpdateStatus → 空轮 sleep 100ms)、
 * 槽选项恰四项、LSN 确认按输出前沿封顶、close 次序 流→复制连接→SQL 连接。
 *
 * <p>线程约束:open/ensureSlot/start/close 由装配方(监督壳)串行调用,run 由 reader
 * 线程独占执行——会话自身无内部线程(状态回传内联在轮询循环里,见 run 的 javadoc)。
 */
public final class ReplicationSession implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationSession.class);

    /** PostgreSQL duplicate_object:复制槽已存在。 */
    private static final String SQLSTATE_DUPLICATE_OBJECT = "42710";

    private final Parameters config;
    private Connection sqlConnection;
    private Connection replicationConnection;
    private PGReplicationStream stream;

    /**
     * 以参数包构造会话。仅赋值不开网络——连接在 open() 建立;构造后须按
     * open → ensureSlot → start → run → close 次序驱动,跳步调用属调用方违约
     * (未 open 先 ensureSlot 抛 NPE、未 start 先 run 同理)。
     *
     * @param config 会话参数(来源与默认值语义见 {@link Parameters})
     */
    public ReplicationSession(Parameters config) {
        this.config = config;
    }

    /**
     * 建立两条连接:普通 SQL 连接(jdbcUrl)与复制连接(replicationUrl)共用同一
     * user/password 的 Properties;复制连接失败时自回收半开的 SQL 连接后原样上抛
     * (不留泄漏的悬空连接)。成功即 INFO 一行。
     *
     * @throws SQLException 任一连接失败;失败时已建连接被关闭
     */
    public void open() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.user());
        props.setProperty("password", config.password());
        sqlConnection = DriverManager.getConnection(config.jdbcUrl(), props);
        try {
            replicationConnection = DriverManager.getConnection(config.replicationUrl(), props);
        } catch (SQLException e) {
            closeQuietly(sqlConnection);
            sqlConnection = null;
            throw e;
        }
        LOG.info("连接建立: {} 与复制连接 replication=database", config.jdbcUrl());
    }

    /**
     * 幂等建槽:two_phase 随槽开启(PG 不允许后改);SQLState 42710(duplicate_object)
     * 表示已存在,WARN 后复用——注意槽的 two_phase 属性需与配置匹配,否则 start 时由
     * 服务端报错。实例形态为薄委派,静态工作体是离线单测以假 Connection 锚定 SQL 契约
     * 与分支语义的接缝。
     *
     * @throws SQLException 非 42710 的建槽失败原样上抛
     */
    public void ensureSlot() throws SQLException {
        ensureSlot(sqlConnection, config);
    }

    /**
     * 责任:幂等建槽(ensureSlot 实例形态的静态工作体,行为与引擎逐行一致)。
     * 关键步骤:预编译 {@code pg_create_logical_replication_slot(?, 'pgoutput', false, ?)}
     * ——第 1 参槽名、第 4 参 twoPhase(第 3 参 false=非临时槽);executeQuery 只取副作用;
     * 捕获 SQLException,SQLState 42710 视为"槽已存在"WARN 复用,其余原样上抛。
     * 边界:sqlConnection 为 null(未 open)抛 NPE,属调用次序违约。
     * 线程约束:与 open/start 同为装配方串行调用。
     */
    static void ensureSlot(Connection sqlConnection, Parameters config) throws SQLException {
        try (PreparedStatement ps = sqlConnection.prepareStatement(
                "SELECT pg_create_logical_replication_slot(?, 'pgoutput', false, ?)")) {
            ps.setString(1, config.slotName());
            ps.setBoolean(2, config.twoPhase());
            try (ResultSet ignored = ps.executeQuery()) {
                // 只需副作用:建槽
            }
            LOG.info("复制槽已创建: {}（two_phase={}）", config.slotName(), config.twoPhase());
        } catch (SQLException e) {
            if (SQLSTATE_DUPLICATE_OBJECT.equals(e.getSQLState())) {
                LOG.warn("复制槽 {} 已存在，直接复用；注意槽的 two_phase 属性需与配置匹配，否则 start 时将由服务端报错",
                        config.slotName());
            } else {
                throw e;
            }
        }
    }

    /**
     * 启动复制流:unwrap 复制连接取 PGConnection,经 builder 链装配——槽名、恰四项槽选项
     * ({@link #slotOptions})、起始位 INVALID_LSN(服务端从槽确认位点续发)、状态间隔
     * = feedbackIntervalSeconds(秒)。成功即 INFO 一行。
     *
     * @throws SQLException 复制流启动失败(含槽属性与选项不匹配的服务端拒绝)
     */
    public void start() throws SQLException {
        PGConnection pg = replicationConnection.unwrap(PGConnection.class);
        ChainedLogicalStreamBuilder builder = pg.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(config.slotName());
        slotOptions(config).forEach(builder::withSlotOption);
        stream = builder
                .withStartPosition(LogSequenceNumber.INVALID_LSN)
                .withStatusInterval(config.feedbackIntervalSeconds(), TimeUnit.SECONDS)
                .start();
        LOG.info("复制流已启动: 槽={} publication={} proto=v{} streaming={} twoPhase={}",
                config.slotName(), config.publicationNames(), config.protoVersion(),
                config.streamingParam(), config.twoPhase());
    }

    /**
     * 责任:组装 START_REPLICATION 的槽选项表(纯函数,供 start 与单测直驱)。
     * 关键步骤:恰四项——proto_version(pgoutput 协议版本,流式需 ≥2)、publication_names
     * (pgoutput 协议硬性要求)、streaming(档位参数 off/on/parallel)、two_phase(on/off);
     * LinkedHashMap 保插入序,拼装可测可复现(pgjdbc 侧存入 Properties,选项到达服务端
     * 的顺序不由此处决定)。
     * 边界:不校验参数合法性与档位联合约束(PARALLEL×two_phase 已在
     * PostgresStreamConnectorConfig.validateSlotStreaming 启动期 fail-fast),照单映射。
     */
    static Map<String, String> slotOptions(Parameters config) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("proto_version", Integer.toString(config.protoVersion()));
        options.put("publication_names", config.publicationNames());
        options.put("streaming", config.streamingParam());
        options.put("two_phase", config.twoPhase() ? "on" : "off");
        return options;
    }

    /** 轮询间隔:readPending 非阻塞,空轮 sleep 控 CPU,消息到达延迟上界即此值;搬运过消息的轮不睡(drain,见 {@link #drainPending})。 */
    private static final long POLL_INTERVAL_MILLIS = 100;

    /** 兼容重载:无输出前沿(不封顶)。消息循环细节见 {@link #run(RawMessageListener, LongSupplier)}。 */
    public void run(RawMessageListener listener) throws SQLException, IOException {
        run(listener, () -> 0L);
    }

    /**
     * 消息循环(实例形态,静态工作体的薄委派):调用方线程(reader)执行至流关闭/异常。
     *
     * @param listener       raw 字节消费者(独占数组承诺见接口 javadoc)
     * @param outputFrontier 输出前沿供应者(语义见静态工作体 javadoc)
     */
    public void run(RawMessageListener listener, LongSupplier outputFrontier) throws SQLException, IOException {
        run(stream, config, listener, outputFrontier);
    }

    /**
     * 责任:消息循环主体——每轮五步序(审查硬口径):
     * ①{@code stream.isClosed()} 守卫,已关闭即抛 SQLException(断连感知);
     * ②{@link #drainPending} 非阻塞取尽当前缓冲的全部消息逐条回调;
     * ③每轮经 {@link #capFeedback} 按输出前沿封顶确认值,同值写 setAppliedLSN/
     *   setFlushedLSN;
     * ④距上次反馈满 feedbackIntervalSeconds 才 forceUpdateStatus(计数器节流,status
     *   周期独立于消息到达);
     * ⑤receivedAny 立即续转(积压期不引入 100ms/轮的人为节流),空轮才 sleep 100ms。
     * 选轮询弃阻塞 read() 的理由(实测,pgjdbc 42.7.13 + PG 18):阻塞 read 在空闲期
     * 不按 statusInterval 醒来,status 依赖服务端 keepalive(约 wal_sender_timeout/2,
     * 默认 ~30s)才被触发;轮询使反馈周期可预期(运维从 pg_stat_replication.flush_lsn
     * 及时看到客户端进度),且断连感知更快(isClosed 检查每轮执行)。因此本会话<b>无独立
     * keep-alive 线程</b>——状态回传内联在轮询循环里。
     * 边界:非 null 但 remaining()==0 的载荷(pgjdbc 实际不产生,防御性跳过)不回调;
     * InterruptedException 恢复中断位后转 SQLException 上抛。
     *
     * <p>outputFrontier 语义:consumer 已输出事务的最大 endLsn——LSN 确认锚定输出前沿,
     * crash 时未输出事务必然被 PG 重发;0 = 无 cap(首个事务输出前)。status 包照常按
     * 反馈周期发送,前沿不前进只影响确认值不影响心跳——不会触发 wal_sender_timeout 断连。
     * 每轮在本线程内 getAsLong 读取一次,实现应为廉价、无副作用的读。
     *
     * <p>关于 confirmed_flush_lsn 的服务端行为(Diag 实证,勿再当 bug 排查):standby
     * status 到达后服务端先采纳进 pg_stat_replication.flush_lsn;槽的 confirmed_flush_lsn
     * 由 walsender 在解码推进时(candidate 机制)落库——空闲期不推进,但确认不丢失:
     * 下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点。
     */
    static void run(PGReplicationStream stream, Parameters config, RawMessageListener listener, LongSupplier outputFrontier)
            throws SQLException, IOException {
        long feedbackIntervalNanos = config.feedbackIntervalSeconds() * 1_000_000_000L;
        long lastFeedbackNanos = System.nanoTime();
        while (true) {
            if (stream.isClosed()) {
                throw new SQLException("复制流已结束（连接断开）");
            }
            boolean receivedAny = drainPending(stream, listener);
            long confirmed = capFeedback(stream.getLastReceiveLSN().asLong(), outputFrontier.getAsLong());
            LogSequenceNumber last = LogSequenceNumber.valueOf(confirmed);
            stream.setAppliedLSN(last);
            stream.setFlushedLSN(last);
            if (System.nanoTime() - lastFeedbackNanos >= feedbackIntervalNanos) {
                stream.forceUpdateStatus();
                LOG.debug("LSN 反馈: applied=flushed={}", last);
                lastFeedbackNanos = System.nanoTime();
            }
            if (receivedAny) {
                // 本轮搬运过消息:立即下一轮继续 drain——积压期不引入 100ms/轮的人为节流,
                // onRaw 的真实工作量(管道 append + 记账)即天然 CPU 节流
                continue;
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("复制循环被中断", e);
            }
        }
    }

    /**
     * 责任:非阻塞取尽复制流当前缓冲的全部消息并逐条回调 listener(drain 语义)。
     * 关键步骤:循环 {@code readPending} 直到返回 null——每条消息拷入独占新建数组同步回调;
     * remaining()==0 的载荷防御性跳过(pgjdbc 实际不产生)但同样被消费,drain 自然终止于
     * null。存在动机(引擎 2026-08-31 吞吐冒烟实测踩坑,行为 1:1 保留):旧形态每轮取一条
     * 即固定 sleep 100ms,slot 读取上限被钉死在 ~10 msg/s——5 万行大事务需 90+ 分钟才收完;
     * drain 把节拍与消息条数解耦,积压期连续搬运,空轮才把间歇交还调用方。
     * 边界:readPending 抛出的 SQLException/IOException 原样上抛——断连经此或调用方循环
     * 的 isClosed 守卫终止;非 null 零载荷轮返回 false(视为空轮,无害)。
     * 返回:本轮是否回调过至少一条消息——调用方据此决定立即续转(true)或空转 sleep(false)。
     * 线程约束:与 run 循环同线程(reader)串行执行。
     */
    static boolean drainPending(PGReplicationStream stream, RawMessageListener listener)
            throws SQLException, IOException {
        boolean receivedAny = false;
        ByteBuffer payload;
        while ((payload = stream.readPending()) != null) {
            if (payload.remaining() > 0) {
                byte[] raw = new byte[payload.remaining()];
                payload.get(raw);
                listener.onRaw(raw);
                receivedAny = true;
            }
        }
        return receivedAny;
    }

    /**
     * 责任:反馈位点封顶纯函数——LSN 确认锚定输出前沿,crash 时未输出事务必然被重发
     * (at-least-once)。
     * 关键步骤:前沿 ≤0(尚未有任何事务输出)视为无 cap,反馈已收到值;否则取 min
     * (前沿不会超过已收到,防御性钳制——不得确认未收到的位点)。纯函数无副作用,供
     * run 循环每轮调用与单测直接驱动。
     */
    static long capFeedback(long received, long outputFrontier) {
        return outputFrontier <= 0L ? received : Math.min(received, outputFrontier);
    }

    /**
     * 关闭顺序:流 → 复制连接 → SQL 连接,各步 WARN 吸收(close 永不抛出,停机路径不
     * 被次生异常掩盖)。stream.close 置关闭标志,轮询循环经 isClosed 守卫/readPending
     * 抛错在 ≤100ms 内退出(一个空轮间歇)。
     */
    @Override
    public void close() {
        if (stream != null) {
            try {
                stream.close();
            } catch (SQLException e) {
                LOG.warn("关闭复制流失败: {}", e.getMessage());
            }
        }
        closeQuietly(replicationConnection);
        closeQuietly(sqlConnection);
        LOG.info("复制会话已关闭: 槽={}", config.slotName());
    }

    /**
     * 责任:静默关闭 JDBC 连接(null 安全,异常 WARN 吸收)。
     * 边界:connection 为 null 时直接返回(未 open 或已中途回收)。
     */
    private static void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("关闭连接失败: {}", e.getMessage());
        }
    }

    /**
     * 会话参数包:引擎 {@code ReplicationConfig} record 的瘦身重写——不做系统属性读取,
     * 由调用方(Task 7 的流式源)从 {@link PostgresStreamConnectorConfig} 组装(配置面、
     * 默认值回落与启动期校验都是配置类的职责,会话不感知 Connect)。组件语义:
     * host/port/database——JDBC 三段式地址;user/password——两条连接共用的凭证
     * (Properties 直塞,不走 URL 参数);slotName——复制槽名(建槽与 withSlotName 同源);
     * publicationNames——pgoutput 硬性要求的 publication 名(单个,多 publication 逗号串
     * 由上层拼好传入);protoVersion——pgoutput 协议版本(流式需 ≥2);streamingMode——
     * OFF/ON/PARALLEL 档位,映射 streaming 槽选项;twoPhase——建槽选项与槽选项同源;
     * feedbackIntervalSeconds——LSN 反馈节流周期(秒,来自
     * {@link PostgresStreamConnectorConfig#feedbackIntervalSeconds()})。
     * 不变量:构造后不可变;无默认值回落与校验(调用方组装时已过配置校验)。
     */
    public record Parameters(
            String host, int port, String database, String user, String password,
            String slotName, String publicationNames,
            int protoVersion, StreamingMode streamingMode, boolean twoPhase,
            int feedbackIntervalSeconds) {

        /**
         * 普通 JDBC URL(host:port/database 三段式),供 open() 的 SQL 连接。
         *
         * @return 拼{@code jdbc:postgresql://}前缀的三段式地址
         */
        public String jdbcUrl() {
            return "jdbc:postgresql://%s:%d/%s".formatted(host, port, database);
        }

        /**
         * 复制连接 URL:jdbcUrl 之上必须同时追加 {@code replication=database} 与
         * {@code assumeMinServerVersion=9.4}——后者缺失时 pgjdbc 不把 replication 参数
         * 放进启动包(pgjdbc 文档规定),服务端按普通会话解析 START_REPLICATION 直接报
         * 语法错(引擎真实 PG 18 集成首跑发现)。
         *
         * @return 含两个必备查询参数的复制连接 URL
         */
        public String replicationUrl() {
            return jdbcUrl() + "?replication=database&assumeMinServerVersion=9.4";
        }

        /**
         * START_REPLICATION 的 streaming 参数值:OFF/ON/PARALLEL 档位到协议参数的映射。
         *
         * @return "off"/"on"/"parallel" 三者之一
         */
        public String streamingParam() {
            return switch (streamingMode) {
                case OFF -> "off";
                case ON -> "on";
                case PARALLEL -> "parallel";
            };
        }
    }
}
