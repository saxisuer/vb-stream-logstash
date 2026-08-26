package org.vastdata.vbstream.replication;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;

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

    /** 消息循环：阻塞读 → 解码 → 缓存 Relation → 回调；按周期 forceUpdateStatus 反馈 LSN。 */
    public void run(PgOutputListener listener) throws SQLException, IOException {
        PgOutputDecoder decoder = new PgOutputDecoder(config.streamingMode());
        RelationRegistry registry = new RelationRegistry();
        long feedbackIntervalNanos = config.feedbackIntervalSeconds() * 1_000_000_000L;
        long lastFeedbackNanos = System.nanoTime();
        while (true) {
            ByteBuffer payload = stream.read(); // 阻塞直到下一条消息；连接不活跃时可能返回 null
            if (payload == null) {
                throw new SQLException("复制流已结束（连接断开）");
            }
            PgOutputMessage message = decoder.decode(payload);
            registry.accept(message);
            listener.onMessage(message, registry);
            LogSequenceNumber last = stream.getLastReceiveLSN();
            stream.setAppliedLSN(last);
            stream.setFlushedLSN(last);
            if (System.nanoTime() - lastFeedbackNanos >= feedbackIntervalNanos) {
                stream.forceUpdateStatus();
                LOG.debug("LSN 反馈: applied=flushed={}", last);
                lastFeedbackNanos = System.nanoTime();
            }
        }
    }

    /** 供上层在异常退出时打印续传位点；流未启动时返回 INVALID_LSN。 */
    public LogSequenceNumber lastReceiveLsn() {
        return stream != null ? stream.getLastReceiveLSN() : LogSequenceNumber.INVALID_LSN;
    }

    /** 关闭顺序：流 → 复制连接 → SQL 连接。close 会令阻塞中的 read 抛出异常从而结束 run 循环。 */
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
