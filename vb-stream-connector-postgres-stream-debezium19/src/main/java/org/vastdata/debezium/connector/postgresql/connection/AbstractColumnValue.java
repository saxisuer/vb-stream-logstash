/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql.connection;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGmoney;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.vastdata.debezium.connector.postgresql.PostgresType;
import org.vastdata.debezium.connector.postgresql.PostgresValueConverter;
import org.vastdata.debezium.connector.postgresql.TypeRegistry;
import org.vastdata.debezium.connector.postgresql.connection.wal2json.DateTimeFormat;

/**
 * @author Chris Cranford
 *
 * 复刻自 io.debezium.connector.postgresql.connection.AbstractColumnValue（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻）。
 * 保留集闭包缺口补件：spec §4.2 未列本类，但 {@code PgOutputColumnValue}（PgOutputReplicationMessage 强制牵连）
 * 继承之——编译期暴露后随闭包一并复刻（spec §8 风险行）。适配两处：
 * （1）{@code PgConnectionSupplier} 由排除集宿主的嵌套接口切换为本包顶层同名接口（Task 1 收编，同包免 import），
 * 其 {@code get()} 返回本包 {@code PostgresConnection}（vanilla 嵌套接口返回 {@code BaseConnection}）——{@code asArray}
 * 的 {@code PgArray} 构造点按 Task 1 随行裁定改经 {@code connection.get().connection()}（JdbcConnection 暴露的内部
 * 连接访问器）强转 {@code BaseConnection} 取底层 pgjdbc 连接，仍在原 try/catch SQLException 内，异常语义不变；
 * （2）{@link PostgresValueConverter} 的 infinity 常量引用本包 Task 4 复刻版（原 vanilla 桥 import 已翻转）。
 */
public abstract class AbstractColumnValue<T> implements ReplicationMessage.ColumnValue<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractColumnValue.class);

    @Override
    public LocalDate asLocalDate() {
        return DateTimeFormat.get().date(asString());
    }

    @Override
    public Object asTime() {
        return asString();
    }

    @Override
    public Object asLocalTime() {
        return DateTimeFormat.get().time(asString());
    }

    @Override
    public OffsetTime asOffsetTimeUtc() {
        return DateTimeFormat.get().timeWithTimeZone(asString());
    }

    @Override
    public OffsetDateTime asOffsetDateTimeAtUtc() {
        if ("infinity".equals(asString())) {
            return PostgresValueConverter.POSITIVE_INFINITY_OFFSET_DATE_TIME;
        }
        else if ("-infinity".equals(asString())) {
            return PostgresValueConverter.NEGATIVE_INFINITY_OFFSET_DATE_TIME;
        }
        return DateTimeFormat.get().timestampWithTimeZoneToOffsetDateTime(asString()).withOffsetSameInstant(ZoneOffset.UTC);
    }

    @Override
    public Instant asInstant() {
        if ("infinity".equals(asString())) {
            return PostgresValueConverter.POSITIVE_INFINITY_INSTANT;
        }
        else if ("-infinity".equals(asString())) {
            return PostgresValueConverter.NEGATIVE_INFINITY_INSTANT;
        }
        return DateTimeFormat.get().timestampToInstant(asString());
    }

    @Override
    public PGbox asBox() {
        try {
            return new PGbox(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGcircle asCircle() {
        try {
            return new PGcircle(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse circle {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public Object asInterval() {
        try {
            return new PGInterval(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGline asLine() {
        try {
            return new PGline(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGlseg asLseg() {
        try {
            return new PGlseg(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGmoney asMoney() {
        try {
            final String value = asString();
            if (value != null && value.startsWith("-")) {
                final String negativeMoney = "(" + value.substring(1) + ")";
                return new PGmoney(negativeMoney);
            }
            return new PGmoney(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse money {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGpath asPath() {
        try {
            return new PGpath(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGpoint asPoint() {
        try {
            return new PGpoint(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public PGpolygon asPolygon() {
        try {
            return new PGpolygon(asString());
        }
        catch (final SQLException e) {
            LOGGER.error("Failed to parse point {}, {}", asString(), e);
            throw new ConnectException(e);
        }
    }

    @Override
    public boolean isArray(PostgresType type) {
        return type.isArrayType();
    }

    @Override
    public Object asArray(String columnName, PostgresType type, String fullType, PgConnectionSupplier connection) {
        try {
            final String dataString = asString();
            // 复刻适配（Task 1 随行裁定）：供给器返回本包 PostgresConnection，经其内部连接访问器取底层 BaseConnection
            return new PgArray((BaseConnection) connection.get().connection(), type.getOid(), dataString);
        }
        catch (SQLException e) {
            LOGGER.warn("Unexpected exception trying to process PgArray ({}) column '{}', {}", fullType, columnName, e);
        }
        return null;
    }

    @Override
    public Object asDefault(TypeRegistry typeRegistry, int columnType, String columnName, String fullType, boolean includeUnknownDatatypes,
                            PgConnectionSupplier connection) {
        if (includeUnknownDatatypes) {
            // this includes things like PostGIS geoemetries or other custom types
            // leave up to the downstream message recipient to deal with
            LOGGER.debug("processing column '{}' with unknown data type '{}' as byte array", columnName, fullType);
            return asString();
        }
        LOGGER.debug("Unknown column type {} for column {} – ignoring", fullType, columnName);
        return null;
    }
}
