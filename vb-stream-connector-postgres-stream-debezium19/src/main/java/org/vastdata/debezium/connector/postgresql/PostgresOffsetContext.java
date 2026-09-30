/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Clock;

import org.vastdata.debezium.connector.postgresql.connection.Lsn;
import org.vastdata.debezium.connector.postgresql.connection.PostgresConnection;

/**
 * Postgres 连接器的 offset 上下文：source info（lsn/txId/xmin/时间戳/快照位）+ 事务元数据 +
 * 增量快照上下文的封装与持久化形态（{@code getOffset()} 的 map 键面即 offset storage 落盘键）。
 * 复刻自 io.debezium.connector.postgresql.PostgresOffsetContext（debezium-connector-postgres 1.9.7.Final sources，
 * 2026-09-30 裁剪复刻）。裁剪与适配三处（均编译期暴露的最小适配，见 spec §4.3 牵连条款）：
 * （1）删除 {@code asOffsetState()} 方法与 {@code spi.OffsetState} import——调用方全在排除集的槽状态打印面
 * （vanilla 行 255 起）；（2）LOGGER 的类字面量由排除集 {@code PostgresSnapshotChangeEventSource.class}
 * 最小适配为本类 {@code PostgresOffsetContext.class}（仅日志 logger 名变化）；（3）{@code initialContext} 的
 * {@code currentXLogLocation()}/{@code currentTransactionId()} 调用改经 {@code JdbcConnection} 基类的
 * {@code query(...)} 内联同 SQL 同语义查询——两方法在 Task 2 裁剪版 {@code PostgresConnection} 已删
 * （“接缝零调用”裁定的对象是 vanilla import 形态；本类复刻后经私有静态方法等价重建，SQL 与返回值语义逐行对照
 * vanilla PostgresConnection 行 423-449）。
 */
public class PostgresOffsetContext implements OffsetContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresOffsetContext.class);

    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String LAST_COMMIT_LSN_KEY = "lsn_commit";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private boolean lastSnapshotRecord;
    private Lsn lastCompletelyProcessedLsn;
    private Lsn lastCommitLsn;
    private Lsn streamingStoppingLsn = null;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    private PostgresOffsetContext(PostgresConnectorConfig connectorConfig, Lsn lsn, Lsn lastCompletelyProcessedLsn, Lsn lastCommitLsn, Long txId, Instant time,
                                  boolean snapshot,
                                  boolean lastSnapshotRecord, TransactionContext transactionContext,
                                  IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        sourceInfo = new SourceInfo(connectorConfig);

        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lastCommitLsn;
        sourceInfo.update(lsn, time, txId, sourceInfo.xmin(), null);
        sourceInfo.updateLastCommit(lastCommitLsn);
        sourceInfoSchema = sourceInfo.schema();

        this.lastSnapshotRecord = lastSnapshotRecord;
        if (this.lastSnapshotRecord) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.timestamp() != null) {
            result.put(SourceInfo.TIMESTAMP_USEC_KEY, Conversions.toEpochMicros(sourceInfo.timestamp()));
        }
        if (sourceInfo.txId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        }
        if (sourceInfo.lsn() != null) {
            result.put(SourceInfo.LSN_KEY, sourceInfo.lsn().asLong());
        }
        if (sourceInfo.xmin() != null) {
            result.put(SourceInfo.XMIN_KEY, sourceInfo.xmin());
        }
        if (sourceInfo.isSnapshot()) {
            result.put(SourceInfo.SNAPSHOT_KEY, true);
            result.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, lastSnapshotRecord);
        }
        if (lastCompletelyProcessedLsn != null) {
            result.put(LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCompletelyProcessedLsn.asLong());
        }
        if (lastCommitLsn != null) {
            result.put(LAST_COMMIT_LSN_KEY, lastCommitLsn.asLong());
        }
        return sourceInfo.isSnapshot() ? result : incrementalSnapshotContext.store(transactionContext.store(result));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        lastSnapshotRecord = false;
    }

    @Override
    public void preSnapshotCompletion() {
        lastSnapshotRecord = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public void updateWalPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn, Instant commitTime, Long txId, Long xmin, TableId tableId) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        sourceInfo.update(lsn, commitTime, txId, xmin, tableId);
    }

    /**
     * update wal position for lsn events that do not have an associated table or schema
     */
    public void updateWalPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn, Instant commitTime, Long txId, Long xmin) {
        updateWalPosition(lsn, lastCompletelyProcessedLsn, commitTime, txId, xmin, null);
    }

    public void updateCommitPosition(Lsn lsn, Lsn lastCompletelyProcessedLsn) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lsn;
        sourceInfo.updateLastCommit(lsn);
    }

    boolean hasLastKnownPosition() {
        return sourceInfo.lsn() != null;
    }

    boolean hasCompletelyProcessedPosition() {
        return this.lastCompletelyProcessedLsn != null;
    }

    Lsn lsn() {
        return sourceInfo.lsn();
    }

    Lsn lastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }

    Lsn lastCommitLsn() {
        return lastCommitLsn;
    }

    /**
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    Lsn getStreamingStoppingLsn() {
        return streamingStoppingLsn;
    }

    public void setStreamingStoppingLsn(Lsn streamingStoppingLsn) {
        this.streamingStoppingLsn = streamingStoppingLsn;
    }

    Long xmin() {
        return sourceInfo.xmin();
    }

    public static class Loader implements OffsetContext.Loader<PostgresOffsetContext> {

        private final PostgresConnectorConfig connectorConfig;

        public Loader(PostgresConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        private Long readOptionalLong(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((Number) obj).longValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public PostgresOffsetContext load(Map<String, ?> offset) {
            final Lsn lsn = Lsn.valueOf(readOptionalLong(offset, SourceInfo.LSN_KEY));
            final Lsn lastCompletelyProcessedLsn = Lsn.valueOf(readOptionalLong(offset, LAST_COMPLETELY_PROCESSED_LSN_KEY));
            Lsn lastCommitLsn = Lsn.valueOf(readOptionalLong(offset, LAST_COMMIT_LSN_KEY));
            if (lastCommitLsn == null) {
                lastCommitLsn = lastCompletelyProcessedLsn;
            }
            final Long txId = readOptionalLong(offset, SourceInfo.TXID_KEY);

            final Instant useconds = Conversions.toInstantFromMicros((Long) offset.get(SourceInfo.TIMESTAMP_USEC_KEY));
            final boolean snapshot = (boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
            final boolean lastSnapshotRecord = (boolean) ((Map<String, Object>) offset).getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
            return new PostgresOffsetContext(connectorConfig, lsn, lastCompletelyProcessedLsn, lastCommitLsn, txId, useconds, snapshot, lastSnapshotRecord,
                    TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset, false));
        }
    }

    @Override
    public String toString() {
        return "PostgresOffsetContext [sourceInfoSchema=" + sourceInfoSchema + ", sourceInfo=" + sourceInfo
                + ", lastSnapshotRecord=" + lastSnapshotRecord
                + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn + ", lastCommitLsn=" + lastCommitLsn
                + ", streamingStoppingLsn=" + streamingStoppingLsn + ", transactionContext=" + transactionContext
                + ", incrementalSnapshotContext=" + incrementalSnapshotContext + "]";
    }

    public static PostgresOffsetContext initialContext(PostgresConnectorConfig connectorConfig, PostgresConnection jdbcConnection, Clock clock) {
        return initialContext(connectorConfig, jdbcConnection, clock, null, null);
    }

    public static PostgresOffsetContext initialContext(PostgresConnectorConfig connectorConfig, PostgresConnection jdbcConnection, Clock clock, Lsn lastCommitLsn,
                                                       Lsn lastCompletelyProcessedLsn) {
        try {
            LOGGER.info("Creating initial offset context");
            final Lsn lsn = Lsn.valueOf(readCurrentXLogLocation(jdbcConnection));
            final long txId = readCurrentTransactionId(jdbcConnection).longValue();
            LOGGER.info("Read xlogStart at '{}' from transaction '{}'", lsn, txId);
            return new PostgresOffsetContext(
                    connectorConfig,
                    lsn,
                    lastCompletelyProcessedLsn,
                    lastCommitLsn,
                    txId,
                    clock.currentTimeAsInstant(),
                    false,
                    false,
                    new TransactionContext(),
                    new SignalBasedIncrementalSnapshotContext<>(false));
        }
        catch (SQLException e) {
            throw new ConnectException("Database processing error", e);
        }
    }

    /**
     * 责任：在 main 连接上查询当前 WAL 位点（{@code initialContext} 的内联适配件，替代 vanilla
     * {@code PostgresConnection.currentXLogLocation()}——该方法在 Task 2 裁剪版连接已删）。
     * 关键步骤：按服务端大版本选 {@code pg_current_wal_lsn()}（PG 10+）或 {@code pg_current_xlog_location()}，
     * 结果串经 pgjdbc {@code LogSequenceNumber.valueOf} 换 long——SQL 与换算逐行对照 vanilla
     * PostgresConnection 行 440-449。边界：无结果行抛 ISE（"there should always be a valid xlog position"，
     * vanilla 同款）；SQLException 上抛给 {@code initialContext} 的 catch 统一转 ConnectException。
     *
     * @param jdbcConnection main 连接（autoCommit=false 的读事务持有者）
     * @return 当前 WAL 位点的 long 形态
     * @throws SQLException 查询失败
     */
    private static long readCurrentXLogLocation(PostgresConnection jdbcConnection) throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = jdbcConnection.connection().getMetaData().getDatabaseMajorVersion();
        jdbcConnection.query(majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()", rs -> {
            if (!rs.next()) {
                throw new IllegalStateException("there should always be a valid xlog position");
            }
            result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
        });
        return result.get();
    }

    /**
     * 责任：在 main 连接上查询当前事务 ID（{@code initialContext} 的内联适配件，替代 vanilla
     * {@code PostgresConnection.currentTransactionId()}——该方法在 Task 2 裁剪版连接已删）。
     * 关键步骤：执行 {@code select * from txid_current()} 取首行首列 long——SQL 与取值逐行对照 vanilla
     * PostgresConnection 行 423-432（读事务本身未分配 XID 时为 0，由调用方决定语义）。
     * 边界：无行或值为 0 时返回 null（vanilla 同款——本连接器路径上 {@code initialContext} 紧接
     * {@code .longValue()}，读事务持有未分配 XID 的场景由连接器装配的先序读写约定排除）。
     *
     * @param jdbcConnection main 连接（autoCommit=false 的读事务持有者）
     * @return 当前事务 ID；无法读到时 null
     * @throws SQLException 查询失败
     */
    private static Long readCurrentTransactionId(PostgresConnection jdbcConnection) throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        jdbcConnection.query("select * from txid_current()", rs -> {
            if (rs.next()) {
                txId.compareAndSet(0, rs.getLong(1));
            }
        });
        long value = txId.get();
        return value > 0 ? value : null;
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
        sourceInfo.update(instant, (TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }
}
