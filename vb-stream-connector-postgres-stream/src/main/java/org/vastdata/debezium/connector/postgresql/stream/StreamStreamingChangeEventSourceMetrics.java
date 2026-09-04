package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Partition;

import java.util.Objects;

/**
 * 本连接器的流式阶段 metrics(MS5 Task 4):{@link DefaultStreamingChangeEventSourceMetrics}
 * 的薄扩展——Debezium 通用 streaming 指标(连接/队列/事务/事件计数)全盘继承,追加
 * {@link StreamStreamingChangeEventSourceMetricsMXBean} 的八项管线观测属性,全部委派
 * {@link StreamMetricsBridge} 的 volatile 预计算值(consumer 统计 tick 内刷新,JMX 读
 * 零锁零计算)。经 {@link StreamChangeEventSourceMetricsFactory} 产出、框架
 * {@code register()} 自动注册为 MBean。
 *
 * <p>生命周期错位:本类在 Task.start 装配期构造(bridge 先存在),而 bridge 的真实
 * 供应商槽要到流式源 execute 建好管线才填充——装配前八项扩展属性读缺省(0/-1),
 * 属设计内形态(见 StreamMetricsBridge 类 javadoc 的线程模型段)。
 *
 * <p>线程约束:与父类同构(@ThreadSafe)——getter 任意线程(JMX),继承面由父类自身
 * 的并发结构保护,扩展面只读 volatile。
 *
 * @param <P> 分区类型(生产为 PostgresPartition)
 */
public class StreamStreamingChangeEventSourceMetrics<P extends Partition>
        extends DefaultStreamingChangeEventSourceMetrics<P>
        implements StreamStreamingChangeEventSourceMetricsMXBean {

    /** 管线观测的预计算读源(全部扩展 getter 委派于此)。 */
    private final StreamMetricsBridge bridge;

    /**
     * 构造扩展流式 metrics(父类四参构造同形追加 bridge)。
     *
     * @param taskContext             任务上下文(JMX ObjectName 与通用配置的来源)
     * @param changeEventQueueMetrics 队列指标(父类队列容量面)
     * @param eventMetadataProvider   事件元数据提供者(父类位置/事务面)
     * @param capturedTablesSupplier  已捕获表集合(父类 getCapturedTables 面)
     * @param bridge                  管线指标桥(Task.start 建立的同一实例,与流式源共享)
     */
    public <T extends CdcSourceTaskContext> StreamStreamingChangeEventSourceMetrics(
            T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
            EventMetadataProvider eventMetadataProvider,
            CapturedTablesSupplier capturedTablesSupplier, StreamMetricsBridge bridge) {
        super(taskContext, changeEventQueueMetrics, eventMetadataProvider, capturedTablesSupplier);
        this.bridge = Objects.requireNonNull(bridge, "bridge");
    }

    /** slot 侧读取字节速率——委派 bridge(tick 预计算 volatile)。 */
    @Override
    public double getSlotReadBytesPerSecond() {
        return bridge.getSlotReadBytesPerSecond();
    }

    /** slot 侧读取消息速率——委派 bridge。 */
    @Override
    public double getSlotReadMessagesPerSecond() {
        return bridge.getSlotReadMessagesPerSecond();
    }

    /** 组装完成速率——委派 bridge。 */
    @Override
    public double getAssembledTxsPerSecond() {
        return bridge.getAssembledTxsPerSecond();
    }

    /** 输出记录速率——委派 bridge。 */
    @Override
    public double getOutputRecordsPerSecond() {
        return bridge.getOutputRecordsPerSecond();
    }

    /** 输出字节速率——委派 bridge。 */
    @Override
    public double getOutputBytesPerSecond() {
        return bridge.getOutputBytesPerSecond();
    }

    /** 复制滞后字节——委派 bridge。 */
    @Override
    public long getLagBytes() {
        return bridge.getLagBytes();
    }

    /** 两阶段挂起 prepared 数——委派 bridge。 */
    @Override
    public int getPendingPreparedCount() {
        return bridge.getPendingPreparedCount();
    }

    /** 管道目录磁盘占用——委派 bridge。 */
    @Override
    public long getPipeDiskUsageBytes() {
        return bridge.getPipeDiskUsageBytes();
    }
}
