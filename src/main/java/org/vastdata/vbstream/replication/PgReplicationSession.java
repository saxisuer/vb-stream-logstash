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

    /** 轮询间隔：readPending 非阻塞，空转 sleep 控 CPU，消息到达延迟上界即此值。 */
    private static final long POLL_INTERVAL_MILLIS = 100;

    /**
     * 消息循环：readPending 非阻塞轮询 → 拷贝单条消息完整字节 → 回调 listener；按周期
     * forceUpdateStatus 反馈 LSN。会话只做字节交付（解码与 Relation 缓存移交给上层，
     * 如 {@link DecodedMessageBridge}），自身不再触碰协议层。
     * 用轮询而非阻塞 read()：实测（pgjdbc 42.7.13 + PG 18）阻塞 read 在空闲期不按 statusInterval 醒来，
     * status 依赖服务端 keepalive（约 wal_sender_timeout/2，默认 ~30s）才被触发；轮询使 status 周期
     * 独立于消息到达（反馈间隔 = feedbackIntervalSeconds，运维可从 pg_stat_replication.flush_lsn 及时
     * 看到客户端进度），且断连感知更快（isClosed 检查每轮执行）。
     * 边界：非 null 但 remaining()==0 的载荷（pgjdbc 实际不产生，防御性跳过）不回调。
     *
     * 关于 confirmed_flush_lsn 的服务端行为（Diag 实证，勿再当 bug 排查）：standby status 到达后
     * 服务端先采纳进 pg_stat_replication.flush_lsn；槽的 confirmed_flush_lsn 由 walsender 在
     * 解码推进时（candidate 机制）落库——空闲期不推进，但确认不丢失：下一次任何 WAL 活动会使其
     * 一步跳到客户端已确认的最新位点。
     */
    public void run(RawMessageListener listener) throws SQLException, IOException {
        long feedbackIntervalNanos = config.feedbackIntervalSeconds() * 1_000_000_000L;
        long lastFeedbackNanos = System.nanoTime();
        while (true) {
            if (stream.isClosed()) {
                throw new SQLException("复制流已结束（连接断开）");
            }
            ByteBuffer payload = stream.readPending(); // 非阻塞；无消息返回 null 属正常
            if (payload != null && payload.remaining() > 0) {
                byte[] raw = new byte[payload.remaining()];
                payload.get(raw);
                listener.onRaw(raw);
            }
            LogSequenceNumber last = stream.getLastReceiveLSN();
            stream.setAppliedLSN(last);
            stream.setFlushedLSN(last);
            if (System.nanoTime() - lastFeedbackNanos >= feedbackIntervalNanos) {
                stream.forceUpdateStatus();
                LOG.debug("LSN 反馈: applied=flushed={}", last);
                lastFeedbackNanos = System.nanoTime();
            }
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("复制循环被中断", e);
            }
        }
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
