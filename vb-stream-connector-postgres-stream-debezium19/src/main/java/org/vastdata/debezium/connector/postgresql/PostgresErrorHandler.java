/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql;

import java.util.Set;

import org.postgresql.util.PSQLException;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

/**
 * Error handler for Postgres.
 * 复刻自 io.debezium.connector.postgresql.PostgresErrorHandler（debezium-connector-postgres 1.9.7.Final sources，
 * 2026-09-30 复刻），无裁剪、逻辑零改动。适配说明（唯一一处）：基类构造首参的连接器类字面量由 vanilla
 * {@code PostgresConnector.class}（排除集，不搬）最小适配为本模块连接器
 * {@link PostgresStreamConnector}.class——该参仅作 {@code ErrorHandler} 内部 logger 命名，日志归属从
 * vanilla 连接器类名改指本模块连接器，重试判定面（{@link #isRetriable}）零改动。
 *
 * @author Gunnar Morling
 */
public class PostgresErrorHandler extends ErrorHandler {

    @Immutable
    private static final Set<String> RETRIABLE_EXCEPTION_MESSSAGES = Collect.unmodifiableSet(
            "Database connection failed when writing to copy",
            "Database connection failed when reading from copy",
            "An I/O error occurred while sending to the backend",
            "ERROR: could not open relation with OID",
            "This connection has been closed",
            "terminating connection due to unexpected postmaster exit",
            "terminating connection due to administrator command");

    public PostgresErrorHandler(PostgresConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(PostgresStreamConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (isRetriablePsqlException(throwable)) {
            return true;
        }
        else if (throwable instanceof DebeziumException) {
            return isRetriablePsqlException(throwable.getCause());
        }
        return false;
    }

    public boolean isRetriablePsqlException(Throwable throwable) {
        if (throwable != null && throwable instanceof PSQLException && throwable.getMessage() != null) {
            for (String messageText : RETRIABLE_EXCEPTION_MESSSAGES) {
                if (throwable.getMessage().contains(messageText)) {
                    return true;
                }
            }
        }
        return false;
    }
}
