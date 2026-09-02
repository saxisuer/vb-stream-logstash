package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;

/**
 * 流式事务事件 → Debezium dispatcher 的映射实现({@link StreamingTransactionListener}
 * 的 connector 侧消费者):Begin/TxChange/End 三段事件映射到 Debezium 的输出契约,
 * <b>事务边界 offset</b> 在 Begin 锚定——
 *
 * <ul>
 *   <li><b>Begin</b>:{@code offsetContext.updateCommitPosition(endLsn, endLsn)}——此后本
 *       事务每条记录 getOffset() 的 lsn 与 lsn_commit 皆 endLsn(SourceInfo.updateLastCommit
 *       同源双写,重启续传/事务边界原子性的 offset 面);随后
 *       {@code dispatchTransactionStartedEvent("xid-"+xid, commitTs)}(是否真发事务块
 *       由 shouldProvideTransactionMetadata 在 TransactionMonitor 内部裁决——无谓守卫)</li>
 *   <li><b>RowChange</b>:按 (oid, seq) 从桶快照解析 asOf Table → 版本安装(值相等短路;
 *       变更即 applySchemaChangesForTable 重建 TableSchema,DDL 稀疏可接受)→
 *       updateWalPosition 补 source 块的 table/txId/messageType →
 *       {@code dispatchDataChangeEvent(tableId, RowChangeEmitter)}</li>
 *   <li><b>Truncate/MsgChange</b>:MS2 跳过发射(DEBUG 留痕)——Truncate 变更族 MS3 补,
 *       LogicalMsg 映射(dispatchLogicalDecodingMessage)后续里程碑接</li>
 *   <li><b>End</b>:{@code dispatchTransactionCommittedEvent(commitTs)}——时间戳组件
 *       End 事件不带,沿用 Begin 记住的提交时间戳</li>
 * </ul>
 *
 * <p>线程约束(R1 账本):全部回调仅 consumer 线程({@code transaction-consumer})——
 * dispatchDataChangeEvent/Transaction* 与 bind/resolve(快照绑定)同线程,这是
 * {@link BucketTableResolver} 无并发原语的成立前提,javadoc 重申;schema 单写者同线程。
 * 回调抛出的异常经组装器 fail-fast 截断(End 永不发、前沿不推进,at-least-once);
 * dispatcher 的 InterruptedException 恢复中断位后包装 DebeziumException 上抛(回调
 * 契约无受检异常面,中断不得被吞)。
 */
final class DispatcherTransactionListener implements StreamingTransactionListener {

    private static final Logger LOG = LoggerFactory.getLogger(DispatcherTransactionListener.class);

    /** 事件分区(随记录与事务块事件携带)。 */
    private final PostgresPartition partition;

    /** 事务边界 offset 的载体(Begin 置位、TxChange 补 source 块,记录经 emitter 读取)。 */
    private final PostgresOffsetContext offsetContext;

    /** Debezium dispatcher(数据/事务块事件的出口)。 */
    private final PostgresEventDispatcher<TableId> dispatcher;

    /** schema 组件(版本安装目标,单写者=本 listener 的回调线程)。 */
    private final StreamPostgresSchema schema;

    /** 记录时间戳时钟(emitter 信封时间戳)。 */
    private final Clock clock;

    /** 连接器配置(emitter 信封行为)。 */
    private final PostgresStreamConnectorConfig connectorConfig;

    /** 值映射接缝(emitter 的 Text 类型化与 TOAST 哨兵)。 */
    private final ColumnValueMapper valueMapper;

    /** 桶快照绑定接缝(consumer 在 Begin 前绑定,本 listener 在 TxChange 内 resolve)。 */
    private final BucketTableResolver tableResolver;

    /** 当前事务的 endLsn(Begin 记住,TxChange 的 updateWalPosition 用)。 */
    private long currentEndLsn;

    /** 当前事务的提交时间戳(Begin 记住,End 的事务块事件复用——End 事件不带时间戳)。 */
    private Instant currentCommitTimestamp;

    /** 当前事务的顶层 xid(Begin 记住,TxChange 的 source 块 txId 兜底)。 */
    private long currentXid;

    /**
     * 构造 listener(组装于流式源的 execute,快照绑定由组装器的 consumer 驱动)。
     *
     * @param partition      事件分区
     * @param offsetContext  事务边界 offset 载体(coordinator 传入的 streaming offset)
     * @param dispatcher     Debezium dispatcher
     * @param schema         schema 组件(版本安装目标)
     * @param clock          记录时间戳时钟
     * @param connectorConfig 连接器配置
     * @param valueMapper    值映射接缝(生产实现 TypeRegistryColumnValueMapper)
     * @param tableResolver  桶快照绑定接缝(与组装器共持同一实例)
     */
    DispatcherTransactionListener(PostgresPartition partition, PostgresOffsetContext offsetContext,
                                  PostgresEventDispatcher<TableId> dispatcher, StreamPostgresSchema schema,
                                  Clock clock, PostgresStreamConnectorConfig connectorConfig,
                                  ColumnValueMapper valueMapper, BucketTableResolver tableResolver) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.offsetContext = Objects.requireNonNull(offsetContext, "offsetContext");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.connectorConfig = Objects.requireNonNull(connectorConfig, "connectorConfig");
        this.valueMapper = Objects.requireNonNull(valueMapper, "valueMapper");
        this.tableResolver = Objects.requireNonNull(tableResolver, "tableResolver");
    }

    /**
     * 责任:消费一个事务事件并映射到 dispatcher。关键步骤:按事件形态分派——Begin 锚定
     * 事务边界 offset 并发事务头;RowChange 解析 asOf 版本、安装、补 source 块、发数据
     * 事件;Truncate/MsgChange DEBUG 跳过;End 发事务尾。边界:任何异常原样上抛
     * (组装器 fail-fast——End 未达则前沿不推进,事务重发);要求 bind 先于 TxChange
     * (组装器保证,调用序违约由 {@link BucketTableResolver#resolve} 抛 ISE)。
     */
    @Override
    public void onEvent(TransactionEvent event) {
        if (event instanceof TransactionEvent.Begin begin) {
            onBegin(begin);
        }
        else if (event instanceof RowChange change) {
            onRowChange(change);
        }
        else if (event instanceof TruncateChange || event instanceof MsgChange) {
            LOG.debug("MS2 跳过发射(Truncate 族 MS3 补/LogicalMsg 后续里程碑): {}", event);
        }
        else if (event instanceof TransactionEvent.End end) {
            onEnd(end);
        }
        else {
            throw new IllegalArgumentException("未知事务事件形态: " + event);
        }
    }

    /**
     * 责任:事务头——锚定事务边界 offset(双写 lsn/lsn_commit=endLsn)并记提交时间戳/
     * xid 供后续事件使用,随后发事务块 BEGIN 事件(shouldProvideTransactionMetadata
     * 关闭时 TransactionMonitor 内部 no-op)。边界:dispatcher 中断见
     * {@link #dispatch(Interruptible)}。
     */
    private void onBegin(TransactionEvent.Begin begin) {
        this.currentEndLsn = begin.endLsn();
        this.currentCommitTimestamp = begin.commitTimestamp();
        this.currentXid = begin.xid();
        offsetContext.updateCommitPosition(Lsn.valueOf(begin.endLsn()), Lsn.valueOf(begin.endLsn()));
        dispatch(() -> dispatcher.dispatchTransactionStartedEvent(partition, "xid-" + begin.xid(),
                offsetContext, begin.commitTimestamp()));
    }

    /**
     * 责任:行变更——按 (oid, seq) 解析 asOf Table(变更时刻的表定义)、值相等短路判定
     * 是否重装版本(变更版本 applySchemaChangesForTable 重建 TableSchema,保证
     * dispatchDataChangeEvent 的 schemaFor 命中)、updateWalPosition 补 source 块的
     * table/txId/messageType(记录的 source 元数据),最后发数据事件。
     * 边界:oid 未先行到达(resolve 抛 ISE)或 emitter 抛出 → 原样上抛 fail-fast;
     * dispatch 返回 false(表被过滤器排除)不算异常,静默跳过属过滤语义。
     */
    private void onRowChange(RowChange change) {
        int relationOid = change.relation().relationOid();
        Table table = tableResolver.resolve(relationOid, change.seq()).table();
        Table installed = schema.tableFor(relationOid);
        if (!table.equals(installed)) {
            schema.applySchemaChangesForTable(relationOid, table);
        }
        TableId tableId = table.id();
        Long txId = change.streamXid().isPresent() ? change.streamXid().getAsLong() : currentXid;
        offsetContext.updateWalPosition(Lsn.valueOf(currentEndLsn), Lsn.valueOf(currentEndLsn),
                currentCommitTimestamp, txId, null, tableId, envelopeOperation(change.dml()));
        dispatch(() -> dispatcher.dispatchDataChangeEvent(partition, tableId,
                new RowChangeEmitter(partition, offsetContext, clock, connectorConfig, valueMapper, table, change)));
    }

    /**
     * 责任:事务尾——发事务块 COMMIT 事件(End 事件不带时间戳组件,沿用 Begin 记住的
     * 提交时间戳;End 的返回即组装器对完整消费的确认,前沿随后推进——见
     * TransactionConsumer.processBucket)。
     */
    private void onEnd(TransactionEvent.End end) {
        dispatch(() -> dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, currentCommitTimestamp));
    }

    /**
     * 责任:执行一次 dispatcher 调用,统一处理受检中断——恢复中断位后包装
     * {@link DebeziumException} 上抛(回调契约无受检异常面;中断被吞会让停机请求丢失)。
     * 边界:call 抛出的非受检异常原样穿透(fail-fast 由组装器承担)。
     *
     * @param call 单次 dispatcher 调用(可能抛 InterruptedException)
     */
    private void dispatch(Interruptible call) {
        try {
            call.run();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DebeziumException("dispatcher 调度被中断", e);
        }
    }

    /**
     * 责任:DmlKind → ReplicationMessage.Operation(source 块 messageType 字段的枚举面,
     * vanilla 同名枚举)。边界:三种行级 DML 穷尽。
     */
    private static Operation envelopeOperation(DmlKind dml) {
        return switch (dml) {
            case INSERT -> Operation.INSERT;
            case UPDATE -> Operation.UPDATE;
            case DELETE -> Operation.DELETE;
        };
    }

    /** 测试观测口:offset 载体(事务边界断言用;生产无调用方)。 */
    PostgresOffsetContext offsetForTest() {
        return offsetContext;
    }

    /** 可能抛 InterruptedException 的单次调用(见 {@link #dispatch(Interruptible)})。 */
    @FunctionalInterface
    private interface Interruptible {
        /** 执行调用。@throws InterruptedException 下游队列满等待被中断 */
        void run() throws InterruptedException;
    }
}
