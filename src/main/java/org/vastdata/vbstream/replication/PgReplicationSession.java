package org.vastdata.vbstream.replication;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
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

    private final ReplicationConfig config;
    private Connection sqlConnection;
    private Connection replicationConnection;
    private PGReplicationStream stream;

    public PgReplicationSession(ReplicationConfig config) {
        this.config = config;
    }

    public void open() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.user());
        props.setProperty("password", config.password());
        sqlConnection = DriverManager.getConnection(config.jdbcUrl(), props);
        replicationConnection = DriverManager.getConnection(config.replicationUrl(), props);
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
        } catch (SQLException e) {
            if ("42710".equals(e.getSQLState())) {
                System.err.println("WARN: 复制槽 " + config.slotName() + " 已存在，直接复用");
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
    }

    /** 消息循环：阻塞读 → 解码 → 缓存 Relation → 回调；按周期 forceStatusUpdate 反馈 LSN。 */
    public void run(PgOutputListener listener) throws SQLException, IOException {
        PgOutputDecoder decoder = new PgOutputDecoder(config.streamingMode());
        RelationRegistry registry = new RelationRegistry();
        long feedbackIntervalNanos = config.feedbackIntervalSeconds() * 1_000_000_000L;
        long lastFeedbackNanos = System.nanoTime();
        while (true) {
            ByteBuffer payload = stream.read(); // 阻塞直到下一条消息
            PgOutputMessage message = decoder.decode(payload);
            registry.accept(message);
            listener.onMessage(message, registry);
            LogSequenceNumber last = stream.getLastReceiveLSN();
            stream.setAppliedLSN(last);
            stream.setFlushedLSN(last);
            if (System.nanoTime() - lastFeedbackNanos >= feedbackIntervalNanos) {
                stream.forceUpdateStatus();
                lastFeedbackNanos = System.nanoTime();
            }
        }
    }

    /** 关闭顺序：流 → 复制连接 → SQL 连接。close 会令阻塞中的 read 抛出异常从而结束 run 循环。 */
    @Override
    public void close() {
        if (stream != null) {
            try {
                stream.close();
            } catch (SQLException e) {
                System.err.println("WARN: 关闭复制流失败: " + e.getMessage());
            }
        }
        closeQuietly(replicationConnection);
        closeQuietly(sqlConnection);
    }

    private static void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("WARN: 关闭连接失败: " + e.getMessage());
        }
    }
}
