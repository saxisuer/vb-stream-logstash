package org.vastdata.debezium.connector.postgresql.connection;

import org.vastdata.debezium.connector.postgresql.PostgresConnection;

/**
 * 责任:复制消息解码点按需取 JDBC 连接的供给口。
 * 来源:收编自 io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier
 * (debezium-connector-postgres 1.9.7.Final)——原为嵌套接口,自含化时提为顶层(其宿主类属排除集)。
 */
public interface PgConnectionSupplier {
    PostgresConnection get();
}
