/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql.connection.pgoutput;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

import org.vastdata.debezium.connector.postgresql.PostgresType;
import org.vastdata.debezium.connector.postgresql.TypeRegistry;
import org.vastdata.debezium.connector.postgresql.connection.PgConnectionSupplier;
import org.vastdata.debezium.connector.postgresql.connection.ReplicationMessage;
import org.vastdata.debezium.connector.postgresql.connection.ReplicationMessageColumnValueResolver;

/**
 * @author Gunnar Morling
 * @author Chris Cranford
 *
 * 复刻自 io.debezium.connector.postgresql.connection.pgoutput.PgOutputReplicationMessage
 * （debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻），逻辑零改动；{@code PgConnectionSupplier}
 * 引用由排除集宿主 {@code PostgresStreamingChangeEventSource} 的嵌套接口切换为本包顶层同名接口（自排除集宿主收编而来）。
 * static {@code getValue} 是本连接器 typed 模式列值转换的运行期主力（调用面在
 * {@code .stream/TypeRegistryColumnValueMapper}）。
 */
public class PgOutputReplicationMessage implements ReplicationMessage {

    private Operation op;
    private Instant commitTimestamp;
    private Long transactionId;
    private String table;
    private List<Column> oldColumns;
    private List<Column> newColumns;

    public PgOutputReplicationMessage(Operation op, String table, Instant commitTimestamp, Long transactionId, List<Column> oldColumns, List<Column> newColumns) {
        this.op = op;
        this.commitTimestamp = commitTimestamp;
        this.transactionId = transactionId;
        this.table = table;
        this.oldColumns = oldColumns;
        this.newColumns = newColumns;
    }

    @Override
    public Operation getOperation() {
        return op;
    }

    @Override
    public Instant getCommitTime() {
        return commitTimestamp;
    }

    @Override
    public OptionalLong getTransactionId() {
        return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns;
    }

    @Override
    public boolean hasTypeMetadata() {
        return true;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    @Override
    public boolean shouldSchemaBeSynchronized() {
        return false;
    }

    /**
     * Converts the value (string representation) coming from PgOutput plugin to
     * a Java value based on the type of the column from the message.  This value will be converted later on if necessary by the
     * connector's value converter to match whatever the Connect schema type expects.
     *
     * Note that the logic here is tightly coupled on the pgoutput plugin logic which writes the actual value.
     *
     * @return the value; may be null
     */
    public static Object getValue(String columnName, PostgresType type, String fullType, String rawValue, final PgConnectionSupplier connection,
                                  boolean includeUnknownDataTypes, TypeRegistry typeRegistry) {
        final PgOutputColumnValue columnValue = new PgOutputColumnValue(rawValue);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, connection, includeUnknownDataTypes, typeRegistry);
    }
}
