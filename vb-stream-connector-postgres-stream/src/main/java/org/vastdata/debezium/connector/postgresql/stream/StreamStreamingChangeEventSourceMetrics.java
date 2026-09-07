package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

import java.util.Objects;

import org.apache.kafka.connect.data.Struct;

/**
 * 本连接器的流式阶段 metrics(MS5 Task 4):{@link DefaultStreamingChangeEventSourceMetrics}
 * 的薄扩展——Debezium 通用 streaming 指标(连接/队列/事务/事件计数)全盘继承,追加
 * {@link StreamStreamingChangeEventSourceMetricsMXBean} 的八项管线观测属性,全部委派
 * {@link StreamMetricsBridge} 的 volatile 预计算值(consumer 统计 tick 内刷新,JMX 读
 * 零锁零计算)。经 {@link StreamChangeEventSourceMetricsFactory} 产出、框架
 * {@code register()} 自动注册为 MBean。
 *
 * <p><b>事件热路径护栏(2026-09-07 Arthas 火焰图实测落地)</b>:{@link #onEvent} 有意
 * 覆写为空实现且<b>不调 super</b>——Debezium 默认路径在每条数据事件的 dispatch 热路径上
 * 无条件做两件重活:①CommonEventMeter 的 {@code lastEvent = metadataProvider.toSummaryString(...)}
 * (EventFormatter 经 SchemaUtil.asDetailedString 逐字段字符串化,内部<b>每事件 new 一个
 * Jackson ObjectMapper</b>);②StreamingMeter 的 MeasurementCollector 队列 offer(每事件
 * LinkedBlockingQueue 唤醒,pthread_cond_signal)。WSL 压测 8B×200 万大事务的 collapsed
 * 火焰图两路合计占全进程 CPU ~40%(MeasurementCollector 37.7% + ObjectMapper 构造 1.5%
 * 及 schema equals 等),是 Debezium 化输出链 ~4.5 万 rec/s 平台的第一大构成。事件计数
 * 观测面由自研 {@link StreamThroughputMetrics} 的五点插桩承担(输出 rec/s 即事件面),
 * 故覆写后 Debezium 内建 MBean 的 totalNumberOfEventsSeen/lastEvent/
 * milliSecondsBehindSource 三字段停更属已知取舍(自研 lagBytes 面不受影响)。
 *
 * <p>生命周期错位:本类在 Task.start 装配期构造(bridge 先存在),而 bridge 的真实
 * 供应商槽要到流式源 execute 建好管线才填充——装配前八项扩展属性读缺省(0/-1),
 * 属设计内形态(见 StreamMetricsBridge 类 javadoc 的线程模型段)。
 *
 * <p>线程约束:与父类同构(@ThreadSafe)——getter 任意线程(JMX),继承面由父类自身
 * 的并发结构保护,扩展面只读 volatile;onEvent 由 consumer 线程(dispatch 链)调用。
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

    /**
     * 责任:事件热路径护栏——空实现,有意不调 super(类 javadoc 的火焰图实测段:默认路径
     * 每事件 toSummaryString 的 ObjectMapper 构造 + MeasurementCollector 队列唤醒合计
     * ~40% CPU)。dispatcher 经 {@code setEventListener} 持本实例,覆写即整链拦截。
     * 取舍:Debezium 内建 MBean 的 totalNumberOfEventsSeen/lastEvent/milliSecondsBehindSource
     * 停更——事件观测走自研 StreamThroughputMetrics(输出 rec/s),lag 走自研 lagBytes。
     * 边界:空方法体即全部行为,JMX 侧无隐藏副作用。
     *
     * @param partition 事件分区(未用)
     * @param source    数据集合 id(未用)
     * @param offset    事件 offset(未用)
     * @param key       记录键(未用)
     * @param value     记录值(未用)
     * @param operation 操作形态(未用)
     */
    @Override
    public void onEvent(P partition, DataCollectionId source, OffsetContext offset,
                        Object key, Struct value, Operation operation) {
        // 有意为空——见方法 javadoc 与类 javadoc 的火焰图实测段
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
