/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.SnapshotRecord;

/**
 * PG source info 的 Connect struct 构造器（schema 名 {@code io.debezium.connector.postgresql.Source}
 * 原样保留——下游消费方按此 schema 名反序列化，属事件形态契约非类引用）。
 * 复刻自 io.debezium.connector.postgresql.PostgresSourceInfoStructMaker（debezium-connector-postgres
 * 1.9.7.Final sources，2026-09-30 复刻），无裁剪、逻辑零改动。保留集闭包缺口补件（spec §4.3 牵连条款）：
 * spec §4.2 的 21 文件清单未列本类，但复刻的 {@code PostgresConnectorConfig} 必须实现 debezium-core
 * 基类的抽象方法 {@code getSourceInfoStructMaker(Version)}（CommonConnectorConfig 构造期即调用，
 * 每个 Config 实例化都走），且 vanilla 实现的 {@code struct(...)} 参数类型钉死 vanilla {@code SourceInfo}，
 * 不可跨命名空间复用——编译与运行期双面强制，随闭包一并复刻。
 */
public class PostgresSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public PostgresSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.postgresql.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.XMIN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        assert sourceInfo.database() != null
                && sourceInfo.schemaName() != null
                && sourceInfo.tableName() != null;

        Struct result = super.commonStruct(sourceInfo);
        result.put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.schemaName());
        result.put(SourceInfo.TABLE_NAME_KEY, sourceInfo.tableName());
        if (sourceInfo.snapshot() != SnapshotRecord.INCREMENTAL) {
            if (sourceInfo.txId() != null) {
                result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
            }
            if (sourceInfo.lsn() != null) {
                result.put(SourceInfo.LSN_KEY, sourceInfo.lsn().asLong());
            }
            if (sourceInfo.xmin() != null) {
                result.put(SourceInfo.XMIN_KEY, sourceInfo.xmin());
            }
        }
        return result;
    }
}
