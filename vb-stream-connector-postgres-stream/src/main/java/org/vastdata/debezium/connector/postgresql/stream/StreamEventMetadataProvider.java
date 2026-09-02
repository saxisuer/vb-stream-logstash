package org.vastdata.debezium.connector.postgresql.stream;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;

/**
 * 本连接器的事件元数据提取器(指标/事务元数据消费):逐方法重写 vanilla
 * {@code io.debezium.connector.postgresql.PostgresEventMetadataProvider}(DBZ 3.6.1.Final
 * 实测——该类<b>包私有</b>,包外不可 import,故自实现同语义副本;{@link SourceInfo}
 * 的键常量是 public,可直引)。三个抽取器都从事件 value 的 {@code source} 结构块取数,
 * 该块的 lsn/txId/ts_usec 字段由 offsetContext 的 SourceInfo 随事务边界更新填充
 * (Begin 置 lsn=lsn_commit=endLsn,TxChange 补 table/txId)。
 *
 * <p>线程约束:无状态,任意线程(实际为 coordinator 的指标 tick 与 consumer 的
 * 事务元数据路径)。
 */
public class StreamEventMetadataProvider implements EventMetadataProvider {

    /**
     * 责任:取事件时间戳——source 块的 ts_usec(微秒)优先,老格式 ts(毫秒)兜底
     * (vanilla 同款两级回落)。边界:value/source 任一为 null 返回 null(非数据事件)。
     */
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

    /**
     * 责任:取事件在事务日志中的唯一定位(lsn,附 xmin 若有)——指标的事务位点展示面。
     * 边界:value/source 为 null 或 source 块无 lsn 返回 null。
     */
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

    /**
     * 责任:取事件所属事务标识(txId 的字符串形态)——事务元数据(事务块 BEGIN/END 事件)
     * 的归属展示面。边界:value/source 为 null 或无 txId 返回 null。
     */
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
