package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * 本连接器流式阶段的扩展 MBean 属性面(MS5 Task 4):在 Debezium 通用 streaming 指标
 * 之上追加管线观测——三段速率(slot 读取字节/消息、组装、输出字节/记录)、复制滞后
 * 字节、两阶段挂起 prepared 数、管道目录磁盘占用。
 *
 * <p>存在理由:JMX 经 {@code registerMBean} 只暴露 MXBean <b>接口</b>上声明的属性——
 * 仅在 metrics 实现类上加 getter 不进 MBean 面。故按 Debezium 惯例(如 MongoDB 连接器
 * 的 {@code MongoDbStreamingChangeEventSourceMetricsMXBean})声明子接口并让
 * {@link StreamStreamingChangeEventSourceMetrics} 实现之,JMX 运行时取最派生的 MXBean
 * 接口,基类属性与扩展属性一并暴露。
 *
 * <p>线程约束:全部为只读 getter,值由 {@link StreamMetricsBridge} 在 consumer 统计
 * tick 内预计算为 volatile——JMX 线程读取零锁零计算零 IO。
 */
public interface StreamStreamingChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /** slot 侧读取字节速率(B/s,10s 窗口差分;管线装配前恒 0)。 */
    double getSlotReadBytesPerSecond();

    /** slot 侧读取消息速率(msg/s,10s 窗口差分;管线装配前恒 0)。 */
    double getSlotReadMessagesPerSecond();

    /** 组装完成(提交交接)速率(tx/s,10s 窗口差分;管线装配前恒 0)。 */
    double getAssembledTxsPerSecond();

    /** 输出记录速率(rec/s,10s 窗口差分,aborted 过滤后实付口径;管线装配前恒 0)。 */
    double getOutputRecordsPerSecond();

    /** 输出字节速率(B/s,10s 窗口差分,回放重读口径;管线装配前恒 0)。 */
    double getOutputBytesPerSecond();

    /** 复制滞后字节(最近收到 LSN − 输出前沿;管线装配前恒 0)。 */
    long getLagBytes();

    /** 两阶段挂起 prepared 数(PREPARE 到 COMMIT/ROLLBACK PREPARED 之间的未决事务;装配前恒 0)。 */
    int getPendingPreparedCount();

    /** 管道目录磁盘占用字节(consumer 统计 tick 内采样;装配前与遍历失败恒 -1)。 */
    long getPipeDiskUsageBytes();
}
