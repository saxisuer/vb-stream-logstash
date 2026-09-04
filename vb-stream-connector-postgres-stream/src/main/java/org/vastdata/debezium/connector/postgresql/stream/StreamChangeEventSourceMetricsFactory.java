package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;

import java.util.Objects;

/**
 * 本连接器的指标工厂(MS5 Task 4):{@link DefaultChangeEventSourceMetricsFactory} 的换装点
 * ——streaming 指标改为产出 {@link StreamStreamingChangeEventSourceMetrics}(通用面继承 +
 * 八项管线观测扩展),snapshot 指标维持默认(快照恒 skipped,无扩展诉求)。Task.start 以
 * 本工厂替换裸的 Default 工厂,构造期持有的 {@link StreamMetricsBridge} 与传给流式源的
 * 是<b>同一实例</b>——metrics 面(读)与流式源 execute(写 suppliers)经它单向衔接。
 *
 * <p>线程约束:工厂方法由 coordinator 线程在装配期调用一次,无并发面。
 *
 * @param <P> 分区类型(生产为 PostgresPartition)
 */
public class StreamChangeEventSourceMetricsFactory<P extends Partition>
        extends DefaultChangeEventSourceMetricsFactory<P> {

    /** 管线指标桥(与流式源共享的同一实例;metrics 扩展面全部委派它)。 */
    private final StreamMetricsBridge bridge;

    /**
     * 构造工厂。
     *
     * @param bridge Task.start 建立的指标桥实例(须与传入流式源工厂的是同一实例)
     */
    public StreamChangeEventSourceMetricsFactory(StreamMetricsBridge bridge) {
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    /**
     * 责任:产出流式阶段 metrics——本连接器扩展形态(通用面继承 + 管线观测委派 bridge)。
     * 签名与父类一致(debezium-connector-common 3.6.1 的四参形态),coordinator 按接口调用
     * 并在 streaming 阶段开始时 {@code register()} 注册 MBean。
     *
     * @param taskContext             任务上下文
     * @param changeEventQueueMetrics 队列指标
     * @param eventMetadataProvider   事件元数据提供者
     * @param capturedTablesSupplier  已捕获表集合
     * @return 装好 bridge 的 StreamStreamingChangeEventSourceMetrics
     */
    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<P> getStreamingMetrics(
            T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
            EventMetadataProvider eventMetadataProvider, CapturedTablesSupplier capturedTablesSupplier) {
        return new StreamStreamingChangeEventSourceMetrics<>(taskContext, changeEventQueueMetrics,
                eventMetadataProvider, capturedTablesSupplier, bridge);
    }

    /**
     * snapshot 指标维持父类默认(快照恒 skipped,无扩展面)。
     *
     * @return 父类默认的 snapshot metrics
     */
    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<P> getSnapshotMetrics(
            T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
            EventMetadataProvider eventMetadataProvider) {
        return super.getSnapshotMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }
}
