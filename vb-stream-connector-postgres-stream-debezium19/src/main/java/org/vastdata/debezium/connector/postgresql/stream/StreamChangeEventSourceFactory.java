package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

import org.apache.kafka.connect.errors.ConnectException;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * 本连接器的变更源工厂(1.9.7 形态):streaming → {@link PostgresStreamStreamingChangeEventSource}
 * (监督壳 + MS2 管道);snapshot → <b>恒 skipped 的最小实现</b>(本连接器流式-only,快照
 * 数据抽取职能属 vanilla postgresql-connector);incremental → 恒
 * {@code Optional.empty()}(R2 裁定 v1 不接,2026-09-05 审计文档结论节)。
 *
 * <p>1.9.7 接口面与 3.6.1 的两处收敛:{@code getSnapshotChangeEventSource} 只收
 * {@link SnapshotProgressListener} 单参(3.6.1 版另收 NotificationService——1.9.7 无
 * 通知体系);snapshot 源的 {@code execute} 是 (context, partition, previousOffset) 三参
 * 且无 SnapshottingTask(快照任务面是 2.x 的概念,1.9.7 的 coordinator 拿
 * {@code SnapshotResult} 直接判流),故 3.6.1 版 SkippedSnapshotSource 的
 * {@code getSnapshottingTask}/{@code getBlockingSnapshottingTask} 两方法整体删除。
 *
 * <p>snapshot 的 skipped 形态:{@code execute} 返回
 * {@code SnapshotResult.skipped(offset)}——coordinator 拿到 skipped 即直接进入 streaming;
 * previousOffset 为 null(首启无存量 offset)时经
 * {@code PostgresOffsetContext.initialContext} 从 main 连接建初始上下文(时序在 reader
 * 线程创建之前,与 main 连接的 reader 独占 R3 不冲突)。
 *
 * <p>线程约束:工厂方法由 coordinator 线程调用(execute 之前的装配阶段),无并发面。
 */
public class StreamChangeEventSourceFactory
        implements ChangeEventSourceFactory<PostgresPartition, PostgresOffsetContext> {

    private final PostgresStreamConnectorConfig connectorConfig;
    private final PostgresEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final StreamPostgresSchema schema;
    /** main JDBC 连接(初始 offset 上下文与 'R' enrich 的元数据源)。 */
    private final PostgresConnection mainConnection;
    private final TypeRegistry typeRegistry;
    /**
     * 管线指标桥(MS5 Task 4:与 metrics 工厂共享的同一实例,经构造链传给流式源——
     * execute 装配管线后填充读源。单向依赖:Task.start 建桥 → 双工厂各持引用)。
     */
    private final StreamMetricsBridge metricsBridge;

    /**
     * 构造工厂(Task.start 装配点调用一次)。
     *
     * @param connectorConfig 连接器配置
     * @param dispatcher      事件出口
     * @param errorHandler    失败出口
     * @param clock           时间戳时钟
     * @param schema          schema 组件(listener 版本安装目标)
     * @param mainConnection  main JDBC 连接
     * @param typeRegistry    共享类型注册表
     * @param metricsBridge   管线指标桥(与 metrics 工厂同一实例)
     */
    public StreamChangeEventSourceFactory(PostgresStreamConnectorConfig connectorConfig,
                                          PostgresEventDispatcher<TableId> dispatcher,
                                          ErrorHandler errorHandler, Clock clock,
                                          StreamPostgresSchema schema,
                                          PostgresConnection mainConnection,
                                          TypeRegistry typeRegistry,
                                          StreamMetricsBridge metricsBridge) {
        this.connectorConfig = Objects.requireNonNull(connectorConfig, "connectorConfig");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.mainConnection = Objects.requireNonNull(mainConnection, "mainConnection");
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
        this.metricsBridge = Objects.requireNonNull(metricsBridge, "metricsBridge");
    }

    /**
     * 责任:提供快照源——本连接器恒 skipped 的最小实现(参数 listener 收下不用:
     * 快照永不执行,进度回调无消费点)。边界:见 {@link SkippedSnapshotSource}。
     */
    @Override
    public SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<PostgresPartition> snapshotProgressListener) {
        return new SkippedSnapshotSource(connectorConfig, mainConnection, clock);
    }

    /**
     * 责任:提供流式源(监督壳 + MS2 管道;每次 streaming 阶段开始时调用一次)——metricsBridge
     * 随构造链穿入(MS5 Task 4:execute 装配管线后经它填充 MBean 读源)。
     */
    @Override
    public StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> getStreamingChangeEventSource() {
        return new PostgresStreamStreamingChangeEventSource(connectorConfig, dispatcher, errorHandler,
                clock, schema, mainConnection, typeRegistry, metricsBridge);
    }

    /**
     * 责任:增量快照源——恒 {@code Optional.empty()}(R2 裁定 v1 不接 signal-based 增量
     * 快照:2026-09-05 审计文档的接入前置条件五项未满足;coordinator 对 empty 的处理即
     * 完全跳过增量快照面)。显式覆写而非省略(接口 default 同样返回 empty)是为了把
     * 裁定钉在装配点,javadoc 即文档。
     */
    @Override
    public Optional<IncrementalSnapshotChangeEventSource<PostgresPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
            PostgresOffsetContext offsetContext, SnapshotProgressListener<PostgresPartition> snapshotProgressListener,
            DataChangeEventListener<PostgresPartition> dataChangeEventListener) {
        return Optional.empty();
    }

    /**
     * 恒 skipped 的最小快照源:不做任何快照动作,execute 直接返回
     * skipped(previousOffset 或初始上下文),coordinator 随即进入 streaming。
     * 1.9.7 形态注记:接口无 SnapshottingTask 概念,本类只剩 execute 一个抽象方法。
     */
    private static final class SkippedSnapshotSource
            implements SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> {

        private final PostgresStreamConnectorConfig connectorConfig;
        private final PostgresConnection mainConnection;
        private final Clock clock;

        SkippedSnapshotSource(PostgresStreamConnectorConfig connectorConfig, PostgresConnection mainConnection,
                              Clock clock) {
            this.connectorConfig = connectorConfig;
            this.mainConnection = mainConnection;
            this.clock = clock;
        }

        /**
         * 责任:立即返回 skipped——previousOffset 为 null 时经 initialContext 从 main
         * 连接读当前 xlog 位点建初始上下文(时序在 reader 线程创建之前)。initialContext
         * 的 {@code txid_current()} 会给 main 连接(autoCommit=false)强制分配 XID,
         * 查完随即 commit 收敛该事务——否则复制会话在<b>另一条连接</b>上执行的
         * {@code pg_create_logical_replication_slot} 会为等解码一致点而等待这个永不
         * 提交的事务,连接器自死锁(Task 8 IT 首跑实测)。
         * 边界:库查询失败抛 ConnectException(initialContext 语义,装配 fail-fast);
         * commit 失败同样 ConnectException(连接已不可用,后续装配必然失败,fail-fast)。
         */
        @Override
        public SnapshotResult<PostgresOffsetContext> execute(ChangeEventSourceContext context,
                                                             PostgresPartition partition,
                                                             PostgresOffsetContext previousOffset) {
            if (previousOffset != null) {
                return SnapshotResult.skipped(previousOffset);
            }
            PostgresOffsetContext initial = PostgresOffsetContext.initialContext(connectorConfig, mainConnection, clock);
            try {
                mainConnection.commit();
            }
            catch (SQLException e) {
                throw new ConnectException("Failed to commit the initial offset read on the main connection", e);
            }
            return SnapshotResult.skipped(initial);
        }
    }
}
