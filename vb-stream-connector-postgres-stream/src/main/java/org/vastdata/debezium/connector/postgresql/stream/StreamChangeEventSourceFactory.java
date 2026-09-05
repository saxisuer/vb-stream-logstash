package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import org.apache.kafka.connect.errors.ConnectException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 本连接器的变更源工厂:streaming → {@link PostgresStreamStreamingChangeEventSource}
 * (监督壳 + MS2 管道);snapshot → <b>恒 skipped 的最小实现</b>(MS5 换真快照——
 * 真实快照需要快照事务/锁/分块读体系,MS2 只做流式);incremental → 默认
 * {@code Optional.empty()}(不覆写——增量快照体系 MS2 不接)。
 *
 * <p>snapshot 的 skipped 形态(3.6.1 接口签名定型):{@code execute} 返回
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
     * 责任:提供快照源——MS2 恒 skipped 的最小实现(参数 listener/notificationService
     * 收下不用:真快照 MS5 才消费)。边界:见 {@link SkippedSnapshotSource}。
     */
    @Override
    public SnapshotChangeEventSource<PostgresPartition, PostgresOffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<PostgresPartition> snapshotProgressListener,
            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService) {
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
     * 恒 skipped 的最小快照源(MS5 换真):不做任何快照动作,execute 直接返回
     * skipped(previousOffset 或初始上下文),coordinator 随即进入 streaming。
     * SnapshottingTask 两方法返回全 false 的空任务(无 schema/数据快照请求)。
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
                                                             PostgresOffsetContext previousOffset,
                                                             SnapshottingTask snapshottingTask) {
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

        /** 责任:快照任务面——schema/数据快照均不做。 */
        @Override
        public SnapshottingTask getSnapshottingTask(PostgresPartition partition, PostgresOffsetContext previousOffset) {
            return emptyTask();
        }

        /** 责任:阻断快照任务面(信号触发的按需快照)——同样空任务。 */
        @Override
        public SnapshottingTask getBlockingSnapshottingTask(PostgresPartition partition,
                                                            PostgresOffsetContext previousOffset,
                                                            io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration snapshotConfiguration) {
            return emptyTask();
        }

        /** 责任:全 false 的空快照任务(shouldSkipSnapshot 恒 true)。 */
        private static SnapshottingTask emptyTask() {
            return new SnapshottingTask(false, false, List.of(), Map.of(), false);
        }
    }
}
