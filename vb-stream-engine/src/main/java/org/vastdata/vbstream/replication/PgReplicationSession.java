package org.vastdata.vbstream.replication;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * pgoutput 复制会话：两条连接（普通 SQL + replication=database），
 * 生命周期 open → ensureSlot → start → run → close。
 */
public final class PgReplicationSession implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PgReplicationSession.class);

    /** PostgreSQL duplicate_object：复制槽已存在。 */
    private static final String SQLSTATE_DUPLICATE_OBJECT = "42710";

    private final ReplicationConfig config;
    private Connection sqlConnection;
    private Connection replicationConnection;
    private PGReplicationStream stream;

    public PgReplicationSession(ReplicationConfig config) {
        this.config = config;
    }

    /** 供上层（如 harness 打日志）读取会话配置。 */
    public ReplicationConfig config() {
        return config;
    }

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

    /** 幂等建槽：two_phase 随槽开启；SQLState 42710（duplicate_object）表示已存在，复用。 */
    public void ensureSlot() throws SQLException {
        try (PreparedStatement ps = sqlConnection.prepareStatement(
                "SELECT pg_create_logical_replication_slot(?, 'pgoutput', false, ?)")) {
            ps.setString(1, config.slotName());
            ps.setBoolean(2, config.twoPhase());
            try (ResultSet ignored = ps.executeQuery()) {
                // 只需副作用：建槽
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

    public void start() throws SQLException {
        PGConnection pg = replicationConnection.unwrap(PGConnection.class);
        stream = pg.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(config.slotName())
                .withSlotOption("proto_version", Integer.toString(config.protoVersion()))
                .withSlotOption("publication_names", config.publicationNames())
                .withSlotOption("streaming", config.streamingParam())
                .withSlotOption("two_phase", config.twoPhase() ? "on" : "off")
                .withStartPosition(LogSequenceNumber.INVALID_LSN)
                .withStatusInterval(config.feedbackIntervalSeconds(), TimeUnit.SECONDS)
                .start();
        LOG.info("复制流已启动: 槽={} publication={} proto=v{} streaming={} twoPhase={}",
                config.slotName(), config.publicationNames(), config.protoVersion(),
                config.streamingParam(), config.twoPhase());
    }

    /** 轮询间隔：readPending 非阻塞，空轮 sleep 控 CPU，消息到达延迟上界即此值；搬运过消息的轮不睡（drain，见 {@link #drainPending}）。 */
    private static final long POLL_INTERVAL_MILLIS = 100;

    /** 兼容重载：无输出前沿（不封顶，等价 1.6 行为）。消息循环细节见 {@link #run(RawMessageListener, LongSupplier)}。 */
    public void run(RawMessageListener listener) throws SQLException, IOException {
        run(listener, () -> 0L);
    }

    /**
     * 消息循环：每轮经 {@link #drainPending} 非阻塞取尽当前缓冲的全部消息（drain 语义——空轮才
     * sleep 100ms 间歇，搬运过消息的轮立即续转，读取节拍与消息条数解耦，见该方法 javadoc 的动机）；
     * 按周期 forceUpdateStatus 反馈 LSN，确认值经 {@link #capFeedback} 按输出前沿封顶。会话只做字节交付
     * （解码与 Relation 缓存移交给上层，如 {@link DecodedMessageBridge}），自身不再触碰协议层。
     * 用轮询而非阻塞 read()：实测（pgjdbc 42.7.13 + PG 18）阻塞 read 在空闲期不按 statusInterval 醒来，
     * status 依赖服务端 keepalive（约 wal_sender_timeout/2，默认 ~30s）才被触发；轮询使 status 周期
     * 独立于消息到达（反馈间隔 = feedbackIntervalSeconds，运维可从 pg_stat_replication.flush_lsn 及时
     * 看到客户端进度），且断连感知更快（isClosed 检查每轮执行）。
     * 边界：非 null 但 remaining()==0 的载荷（pgjdbc 实际不产生，防御性跳过）不回调。
     *
     * outputFrontier 语义：consumer 已输出事务的最大 endLsn——LSN 确认锚定输出前沿，crash 时未输出
     * 事务必然被重发（1.7 设计 §5）；0 = 无 cap（首个事务输出前与 1.6 行为一致）。status 包照常按
     * 反馈周期发送，前沿不前进只影响确认值不影响心跳——不会触发 wal_sender_timeout 断连。
     * 线程约束：循环体由调用方线程执行；outputFrontier 每轮在本线程内 getAsLong 读取一次，
     * 实现应为廉价、无副作用的读。
     *
     * 关于 confirmed_flush_lsn 的服务端行为（Diag 实证，勿再当 bug 排查）：standby status 到达后
     * 服务端先采纳进 pg_stat_replication.flush_lsn；槽的 confirmed_flush_lsn 由 walsender 在
     * 解码推进时（candidate 机制）落库——空闲期不推进，但确认不丢失：下一次任何 WAL 活动会使其
     * 一步跳到客户端已确认的最新位点。
     */
    public void run(RawMessageListener listener, LongSupplier outputFrontier) throws SQLException, IOException {
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
                // 本轮搬运过消息：立即下一轮继续 drain——积压期不引入 100ms/轮的人为节流，
                // onRaw 的真实工作量（CQ append + 记账）即天然 CPU 节流
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
     * 责任：非阻塞取尽复制流当前缓冲的全部消息并逐条回调 listener（drain 语义）。
     * 关键步骤：循环 {@code readPending} 直到返回 null——每条消息拷入独占新建数组同步回调
     * （与旧"每轮一条"路径同构）；remaining()==0 的载荷防御性跳过（pgjdbc 实际不产生）但
     * 同样被消费，drain 自然终止于 null。存在动机（2026-08-31 吞吐冒烟实测踩坑）：旧形态
     * 每轮取一条即固定 sleep 100ms，slot 读取上限被钉死在 ~10 msg/s——5 万行大事务需 90+
     * 分钟才收完，Commit 迟迟不达、下游无任何输出；drain 把节拍与消息条数解耦，积压期连续
     * 搬运，空轮才把间歇交还调用方。
     * 边界：readPending 抛出的 SQLException/IOException 原样上抛——断连经此或调用方循环的
     * isClosed 守卫终止；非 null 零载荷轮返回 false（视为空轮，无害——pgjdbc 不产生该形态）。
     * 返回：本轮是否回调过至少一条消息——调用方据此决定立即续转（true）或空转 sleep（false）。
     * 线程约束：与 run 循环同线程（调用方线程）串行执行。
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
     * 责任：反馈位点封顶纯函数（1.7 设计 §5）——LSN 确认锚定输出前沿，crash 时未输出事务必然被重发。
     * 关键步骤：前沿 ≤0（尚未有任何事务输出）视为无 cap，反馈已收到值；否则取 min（前沿不会超过
     * 已收到，防御性钳制）。纯函数无副作用，供 run 循环每轮调用与单测直接驱动。
     */
    static long capFeedback(long received, long outputFrontier) {
        return outputFrontier <= 0L ? received : Math.min(received, outputFrontier);
    }

    /** 供上层在异常退出时打印续传位点；流未启动时返回 INVALID_LSN。 */
    public LogSequenceNumber lastReceiveLsn() {
        return stream != null ? stream.getLastReceiveLSN() : LogSequenceNumber.INVALID_LSN;
    }

    /** 关闭顺序：流 → 复制连接 → SQL 连接。stream.close 置关闭标志，轮询循环经 isClosed 守卫/readPending 抛错退出。 */
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

    private static void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("关闭连接失败: {}", e.getMessage());
        }
    }
}
