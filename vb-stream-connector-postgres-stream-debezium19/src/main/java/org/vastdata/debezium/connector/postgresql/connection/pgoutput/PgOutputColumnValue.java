/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql.connection.pgoutput;

import java.math.BigDecimal;

import org.vastdata.debezium.connector.postgresql.connection.AbstractColumnValue;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 *
 * 复刻自 io.debezium.connector.postgresql.connection.pgoutput.PgOutputColumnValue
 * （debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻），逻辑零改动。
 * 保留集闭包缺口补件：spec §4.2 的 pgoutput 保留面只列 {@code PgOutputReplicationMessage}，但其
 * static {@code getValue} 构造本类（vanilla 同包 package-private）——编译期暴露的强制牵连（spec §8 风险行），
 * 随父文件一并复刻，不属扩保留集的主动选择。
 */
class PgOutputColumnValue extends AbstractColumnValue<String> {

    private String value;

    PgOutputColumnValue(String value) {
        this.value = value;
    }

    @Override
    public String getRawValue() {
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public String asString() {
        return value;
    }

    @Override
    public Boolean asBoolean() {
        return "t".equalsIgnoreCase(value);
    }

    @Override
    public Integer asInteger() {
        return Integer.valueOf(value);
    }

    @Override
    public Long asLong() {
        return Long.valueOf(value);
    }

    @Override
    public Float asFloat() {
        return Float.valueOf(value);
    }

    @Override
    public Double asDouble() {
        return Double.valueOf(value);
    }

    @Override
    public SpecialValueDecimal asDecimal() {
        if ("NaN".equals(value)) {
            return SpecialValueDecimal.NOT_A_NUMBER;
        }
        else {
            return new SpecialValueDecimal(new BigDecimal(value));
        }
    }

    @Override
    public byte[] asByteArray() {
        return Strings.hexStringToByteArray(value.substring(2));
    }
}
