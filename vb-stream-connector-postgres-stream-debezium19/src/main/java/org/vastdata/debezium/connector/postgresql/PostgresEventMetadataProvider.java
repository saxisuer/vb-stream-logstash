/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;

/**
 * 事件元数据提供者：从记录 envelope 的 source struct 提取时间戳（两级回落——PG 专属
 * {@code ts_usec} 微秒键优先，缺则基类毫秒键 {@code ts_ms}）、事件位点（lsn/xmin）与事务 ID，
 * 供 coordinator 的 lag/指标/事务元数据面消费。复刻自 io.debezium.connector.postgresql.
 * PostgresEventMetadataProvider（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 复刻），
 * 无裁剪、逻辑零改动（{@code SourceInfo} 常量面同包直引）。包私有可见性与 vanilla 一致——
 * 本连接器运行面实际使用 {@code .stream/StreamEventMetadataProvider}（其 PG 专属分支的同构重写），
 * 本类作为复刻保留集成员维持 vanilla 结构。
 */
class PostgresEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        if (sourceInfo.schema().field(SourceInfo.TIMESTAMP_USEC_KEY) != null) {
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_USEC_KEY);
            return timestamp == null ? null : Conversions.toInstantFromMicros(timestamp);
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        final Long xmin = sourceInfo.getInt64(SourceInfo.XMIN_KEY);
        final Long lsn = sourceInfo.getInt64(SourceInfo.LSN_KEY);
        if (lsn == null) {
            return null;
        }

        Map<String, String> r = Collect.hashMapOf(
                SourceInfo.LSN_KEY, Long.toString(lsn));
        if (xmin != null) {
            r.put(SourceInfo.XMIN_KEY, Long.toString(xmin));
        }
        return r;
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        Long txId = sourceInfo.getInt64(SourceInfo.TXID_KEY);
        if (txId == null) {
            return null;
        }
        return Long.toString(txId);
    }
}
