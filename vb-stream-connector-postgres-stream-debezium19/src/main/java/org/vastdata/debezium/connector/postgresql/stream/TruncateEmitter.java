package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;

/**
 * TRUNCATE 变更 → Debezium 变更记录发射器:一条 TRUNCATE 语句的<b>每张受影响表</b>
 * 一个实例(listener 逐表构造逐表 dispatch),信封形态为 vanilla
 * {@code PostgresChangeRecordEmitter.emitTruncateRecord}(DBZ 1.9.7.Final sources 实证
 * 口径——方法体逐行同款,javadoc 记档):
 *
 * <ul>
 *   <li>envelope = {@code tableSchema.getEnvelopeSchema().truncate(sourceInfo, clock)}——
 *       无 before/after 的 TRUNCATE 专用信封(op="t";1.9.7 的 getEnvelopeSchema 返回
 *       Envelope 本体,truncate(Struct, Instant) 签名与 3.x 的 EnvelopeSchema 同形)</li>
 *   <li>{@code receiver.changeRecord(partition, tableSchema, TRUNCATE, null, envelope, offset, null)}
 *       ——<b>key 恒 null</b>(截断无行键)、无 headers(1.9.7 的第 7 参为 ConnectHeaders,
 *       null 即无)、无 tombstone、走普通 data topic(非事务块 topic)</li>
 *   <li>门控不在本类:listener 按 {@code config.getSkippedOperations()} 含 TRUNCATE 与否
 *       决定是否 dispatch(默认跳过;1.9.7 实测:默认跳过由 PG 连接器的废弃键
 *       truncate.handling.mode 默认 skip 经 getSkippedOperations() 覆写并入,发射需
 *       skipped.operations=none 且 truncate.handling.mode=include 双键——见
 *       DispatcherTransactionListener 类 javadoc)</li>
 * </ul>
 *
 * <p><b>options 丢弃对齐</b>:协议的 TRUNCATE 选项位(CASCADE / RESTART_IDENTITY)vanilla
 * 读了即弃(不进信封不进 source 块);本连接器的 {@link TruncateChange#options()} 保留该
 * 信息属<b>超集</b>,发射时同样丢弃——与 vanilla 输出逐字节一致。
 *
 * <p><b>基类构造差异</b>:1.9.7 的 RelationalChangeRecordEmitter 构造为三参
 * (partition/offset/clock,无 CommonConnectorConfig 钩子——3.x 才扩为四参),
 * connectorConfig 不再下传基类,构造形参保留以维持与 3.6.1 版一致的装配面。
 *
 * <p>线程约束:实例仅 consumer 线程触碰(构造于 TxChange 回调内、即用即弃),无状态,
 * 无并发面。
 */
public class TruncateEmitter extends RelationalChangeRecordEmitter<PostgresPartition> {

    /**
     * 构造发射器(不发射——发射发生在 dispatcher 调 {@code emitChangeRecords} 时)。
     * 不携带表定义/变更本体:TRUNCATE 信封只含 source 块与时间戳,表信息经 dispatcher
     * 的 schemaFor(tableId) 从 listener 已安装的版本取。
     *
     * @param partition      事件分区(记录随行)
     * @param offset         事务边界 offset(Begin 已置 lsn=lsn_commit=endLsn)
     * @param clock          记录时间戳时钟
     * @param connectorConfig 连接器配置(装配面参数:1.9.7 基类构造无此钩子,不再下传;
     *                        形参保留维持与 3.6.1 版一致的调用签名)
     */
    public TruncateEmitter(PostgresPartition partition, PostgresOffsetContext offset, Clock clock,
                           PostgresStreamConnectorConfig connectorConfig) {
        super(partition, offset, clock);
    }

    /** 责任:恒 TRUNCATE(vanilla 同款——TRUNCATE 族唯一操作形态)。 */
    @Override
    public Operation getOperation() {
        return Operation.TRUNCATE;
    }

    /**
     * 责任:TRUNCATE 记录本体——vanilla {@code PostgresChangeRecordEmitter.emitTruncateRecord}
     * 逐行照抄:truncate 信封(source 块 + 时间戳,无 before/after)+ changeRecord 以
     * <b>null key</b> 交付(截断作用于全表,无行键)、headers=null。
     * 边界:receiver 抛 InterruptedException 原样上抛(基类契约,dispatcher 面接管)。
     */
    @Override
    protected void emitTruncateRecord(Receiver<PostgresPartition> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(
                getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

    /** 责任:before 侧恒 null——TRUNCATE 无行镜像(vanilla default 分支同款)。 */
    @Override
    protected Object[] getOldColumnValues() {
        return null;
    }

    /** 责任:after 侧恒 null——TRUNCATE 无行镜像(vanilla default 分支同款)。 */
    @Override
    protected Object[] getNewColumnValues() {
        return null;
    }
}
