/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql;

import org.vastdata.debezium.connector.postgresql.connection.AbstractReplicationMessageColumn;
import org.vastdata.debezium.connector.postgresql.connection.PgConnectionSupplier;

/**
 * Represents a toasted column in a replication stream（原 javadoc 的 {@code ReplicationStream} 链接目标属排除集，改平文）.
 *
 * Some decoder implementations may stream information about a column but provide an indicator that the field was not
 * changed and therefore toasted.  This implementation acts as an indicator for such fields that are contained within
 * a {@link org.vastdata.debezium.connector.postgresql.connection.ReplicationMessage}.
 * 复刻自 io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻），
 * 逻辑零改动；{@code getValue} 参数类型由 {@code PostgresStreamingChangeEventSource.PgConnectionSupplier}（排除集宿主嵌套接口）
 * 切换为本包顶层 {@link PgConnectionSupplier}（Task 1 收编）——实现恒返哨兵值、不触达连接参数，签名切换零行为影响。
 *
 * @author Chris Cranford
 */
public class UnchangedToastedReplicationMessageColumn extends AbstractReplicationMessageColumn {

    /**
     * Marker value indicating an unchanged TOAST column value.
     */
    public static final Object UNCHANGED_TOAST_VALUE = new Object();

    public UnchangedToastedReplicationMessageColumn(String columnName, PostgresType type, String typeWithModifiers, boolean optional, boolean hasMetadata) {
        super(columnName, type, typeWithModifiers, optional, hasMetadata);
    }

    @Override
    public boolean isToastedColumn() {
        return true;
    }

    @Override
    public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
        return UNCHANGED_TOAST_VALUE;
    }
}
