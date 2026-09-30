/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.vastdata.debezium.connector.postgresql.connection;

import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;

import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.TimestampUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.vastdata.debezium.connector.postgresql.PostgresType;
import org.vastdata.debezium.connector.postgresql.TypeRegistry;

import org.vastdata.debezium.connector.postgresql.PostgresValueConverter;

/**
 * {@link JdbcConnection} connection extension used for connecting to Postgres instances.
 * 复刻自 io.debezium.connector.postgresql.connection.PostgresConnection（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻）。
 * 裁剪说明：裁成 JDBC 查询包装——删除复制流/槽管理/ServerInfo 探测面（自建 ReplicationSession 已覆盖）：
 * {@code serverInfo()}（原 pg configuration/角色探测，依赖深度裁剪后的 ServerInfo 仅存 ReplicaIdentity）、
 * {@code getReplicationSlotState}/{@code fetchReplicationSlotInfo}/{@code readReplicationSlotInfo}/{@code queryForSlot}/
 * {@code parseConfirmedFlushLsn}/{@code parseRestartLsn}/{@code tryParseLsn} 槽状态查询族、{@code dropReplicationSlot}/
 * {@code dropPublication} 槽/发布管理 SQL、{@code currentTransactionId}/{@code currentXLogLocation}/{@code connectionString}
 * （接缝零调用）、{@code getColumnValue} 覆写（签名引用排除集 PostgresSchema）、
 * {@code PostgresConnection(PostgresConnectorConfig, TypeRegistry, String)} 构造器（签名引用排除集 PostgresConnectorConfig）
 * 及其独占常量/成员；{@code FACTORY} 的端口默认由 {@code PostgresConnectorConfig.PORT.defaultValueAsString()}
 * （排除集类）最小适配为字面值 {@code "5432"}（与 vanilla DEFAULT_PORT 等值）。
 * 适配说明：{@link PostgresValueConverter} 引用本包复刻版，原 vanilla import 已切换为本包简名。
 *
 * @author Horia Chiorean
 */
public class PostgresConnection extends JdbcConnection {

    public static final String CONNECTION_STREAMING = "Debezium Streaming";
    public static final String CONNECTION_SLOT_INFO = "Debezium Slot Info";
    public static final String CONNECTION_DROP_SLOT = "Debezium Drop Slot";
    public static final String CONNECTION_VALIDATE_CONNECTION = "Debezium Validate Connection";
    public static final String CONNECTION_HEARTBEAT = "Debezium Heartbeat";
    public static final String CONNECTION_GENERAL = "Debezium General";

    private static Logger LOGGER = LoggerFactory.getLogger(PostgresConnection.class);

    private static final String URL_PATTERN = "jdbc:postgresql://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
    protected static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            org.postgresql.Driver.class.getName(),
            PostgresConnection.class.getClassLoader(), JdbcConfiguration.PORT.withDefault("5432"));

    private final TypeRegistry typeRegistry;
    private final PostgresDefaultValueConverter defaultValueConverter;

    /**
     * Creates a Postgres connection using the supplied configuration.
     * If necessary this connection is able to resolve data type mappings.
     * Such a connection requires a {@link PostgresValueConverter}, and will provide its own {@link TypeRegistry}.
     * Usually only one such connection per connector is needed.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param valueConverterBuilder supplies a configured {@link PostgresValueConverter} for a given {@link TypeRegistry}
     * @param connectionUsage a symbolic name of the connection to be tracked in monitoring tools
     */
    public PostgresConnection(JdbcConfiguration config, PostgresValueConverterBuilder valueConverterBuilder, String connectionUsage) {
        super(addDefaultSettings(config, connectionUsage), FACTORY, PostgresConnection::validateServerVersion, null, "\"", "\"");

        if (Objects.isNull(valueConverterBuilder)) {
            this.typeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.typeRegistry = new TypeRegistry(this);

            final PostgresValueConverter valueConverter = valueConverterBuilder.build(this.typeRegistry);
            this.defaultValueConverter = new PostgresDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }
    }

    /**
     * Creates a Postgres connection using the supplied configuration.
     * The connector is the regular one without datatype resolution capabilities.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param connectionUsage a symbolic name of the connection to be tracked in monitoring tools
     */
    public PostgresConnection(JdbcConfiguration config, String connectionUsage) {
        this(config, null, connectionUsage);
    }

    static JdbcConfiguration addDefaultSettings(JdbcConfiguration configuration, String connectionUsage) {
        // we require Postgres 9.4 as the minimum server version since that's where logical replication was first introduced
        return JdbcConfiguration.adapt(configuration.edit()
                .with("assumeMinServerVersion", "9.4")
                .with("ApplicationName", connectionUsage)
                .build());
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the given table
     */
    public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement = "SELECT relreplident FROM pg_catalog.pg_class c " +
                "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                replIdentity.append(rs.getString(1));
            }
            else {
                LOGGER.warn("Cannot determine REPLICA IDENTITY information for table '{}'", tableId);
            }
        });
        return ServerInfo.ReplicaIdentity.parseFromDB(replIdentity.toString());
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        }
        catch (SQLException e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
    }

    public Charset getDatabaseCharset() {
        try {
            return Charset.forName(((BaseConnection) connection()).getEncoding().name());
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't obtain encoding for database " + database(), e);
        }
    }

    public TimestampUtils getTimestampUtils() {
        try {
            return ((PgConnection) this.connection()).getTimestampUtils();
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't get timestamp utils from underlying connection", e);
        }
    }

    private static void validateServerVersion(Statement statement) throws SQLException {
        DatabaseMetaData metaData = statement.getConnection().getMetaData();
        int majorVersion = metaData.getDatabaseMajorVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        if (majorVersion < 9 || (majorVersion == 9 && minorVersion < 4)) {
            throw new SQLException("Cannot connect to a version of Postgres lower than 9.4");
        }
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        if (columnName.contains("\"")) {
            columnName = columnName.replaceAll("\"", "\"\"");
        }

        return super.quotedColumnIdString(columnName);
    }

    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnFilter);
    }

    public Optional<Column> readColumnForDecoder(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnNameFilter)
            throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnNameFilter).map(ColumnEditor::create);
    }

    private Optional<ColumnEditor> doReadTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
            throws SQLException {
        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));

            // first source the length/scale from the column metadata provided by the driver
            // this may be overridden below if the column type is a user-defined domain type
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }

            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));

            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            // Lookup the column type from the TypeRegistry
            // For all types, we need to set the Native and Jdbc types by using the root-type
            final PostgresType nativeType = getTypeRegistry().get(column.typeName());
            column.nativeType(nativeType.getRootType().getOid());
            column.jdbcType(nativeType.getRootType().getJdbcId());

            // For domain types, the postgres driver is unable to traverse a nested unbounded
            // hierarchy of types and report the right length/scale of a given type. We use
            // the TypeRegistry to accomplish this since it is capable of traversing the type
            // hierarchy upward to resolve length/scale regardless of hierarchy depth.
            if (TypeRegistry.DOMAIN_TYPE == nativeType.getJdbcId()) {
                column.length(nativeType.getDefaultLength());
                column.scale(nativeType.getDefaultScale());
            }

            final String defaultValueExpression = columnMetadata.getString(13);
            if (defaultValueExpression != null && getDefaultValueConverter().supportConversion(column.typeName())) {
                column.defaultValueExpression(defaultValueExpression);
            }

            return Optional.of(column);
        }

        return Optional.empty();
    }

    public PostgresDefaultValueConverter getDefaultValueConverter() {
        Objects.requireNonNull(defaultValueConverter, "Connection does not provide default value converter");
        return defaultValueConverter;
    }

    public TypeRegistry getTypeRegistry() {
        Objects.requireNonNull(typeRegistry, "Connection does not provide type registry");
        return typeRegistry;
    }

    @Override
    protected String[] supportedTableTypes() {
        return new String[]{ "VIEW", "MATERIALIZED VIEW", "TABLE", "PARTITIONED TABLE" };
    }

    @Override
    protected boolean isTableType(String tableType) {
        return "TABLE".equals(tableType) || "PARTITIONED TABLE".equals(tableType);
    }

    @FunctionalInterface
    public interface PostgresValueConverterBuilder {
        PostgresValueConverter build(TypeRegistry registry);
    }
}
