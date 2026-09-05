package org.vastdata.debezium.connector.postgresql.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * 管线指标桥(MS5 Task 4):Task.start 建立的<b>单向数据桥</b>——一端是 metrics 装配面
 * ({@link StreamChangeEventSourceMetricsFactory} 持引用,产出
 * {@link StreamStreamingChangeEventSourceMetrics} 委派本桥),另一端是运行中的管线
 * (流式源 execute 里 {@link #setSuppliers} 填充真实读源)。任务先起(bridge 先存在)
 * 管线后装配(槽位后填),两端生命周期天然错开,故取"volatile 供应商槽 + 未装配缺省值"
 * 形态而不是构造期注入。
 *
 * <p><b>线程模型(brief 设计核心)</b>:计数与速率的写方是 reader/consumer 线程,读方是
 * JMX 任意线程。全部<b>计算集中在 consumer 的 10s 统计 tick</b>(经
 * {@link StreamThroughputMetrics#statsTickHook} 挂到 {@code TransactionConsumer.maybeStats}
 * 的既有周期):{@link #onStatsTick} 在 consumer 线程取 Totals 快照做窗口差分、采样
 * lagBytes / 挂起 prepared / 管道磁盘占用,一次性写入各 volatile 字段——JMX 读侧
 * <b>零锁零计算零 IO</b>,只读 volatile(目录遍历绝不进 JMX 读路径)。
 *
 * <p>缺省语义:装配前({@code setSuppliers} 未调)五项速率与 lagBytes/pendingPreparedCount
 * 恒 0、pipeDiskUsageBytes 恒 -1(未知哨兵);{@code onStatsTick} 未装配时为安全 no-op。
 * tick 内的任何 Throwable 记 WARN 吞掉——指标观测面永不向 consumer 业务路径抛异常
 * (与 {@link StreamThroughputMetrics} 的"指标永不抛异常进业务路径"同一红线)。
 *
 * <p>线程约束:{@code setSuppliers} 由流式源(coordinator 线程)在 execute 装配期调用一次;
 * {@code onStatsTick} 仅由 consumer 线程(统计 tick)调用;getter 任意线程(JMX 线程)。
 * 窗口差分的基线字段({@code lastTickNanos}/{@code lastTickTotals})只被 consumer 线程
 * 触碰,单写者假设成立。
 */
public final class StreamMetricsBridge {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMetricsBridge.class);

    /** 窗口秒数换算(nanoTime 差 → 秒)。 */
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private final LongSupplier clock;

    // ---- volatile 供应商槽(coordinator 写一次,consumer tick 与 JMX 读) ----

    /** 六项速率计数的累计快照读源(窗口差分分子;通常为 metrics::totals)。 */
    private volatile Supplier<StreamThroughputMetrics.Totals> totalsSupplier;
    /** 滞后字节读源(通常为 session.lastReceiveLsn() - frontier.get())。 */
    private volatile LongSupplier lagBytesSupplier;
    /** 管道目录磁盘占用读源(Files.walk 求和,只允许在 tick 内被调)。 */
    private volatile LongSupplier pipeDiskUsageBytesSupplier;
    /** 两阶段挂起 prepared 数读源(assembler.pendingPreparedCount)。 */
    private volatile IntSupplier pendingPreparedCountSupplier;

    // ---- tick 预计算的观测值(consumer 单写 volatile,JMX 只读) ----

    private volatile double slotReadBytesPerSecond;
    private volatile double slotReadMessagesPerSecond;
    private volatile double assembledTxsPerSecond;
    private volatile double outputRecordsPerSecond;
    private volatile double outputBytesPerSecond;
    private volatile long lagBytes;
    private volatile long pipeDiskUsageBytes = -1L;
    private volatile int pendingPreparedCount;

    // ---- 窗口差分基线(仅 consumer 线程触碰,单写者) ----

    /** 是否已立基线(false = 首 tick 只立基线不算速率——显式标志而非 0 哨兵,受控时钟可从 0 起步)。 */
    private boolean hasBaseline;
    /** 上次 tick 的时钟戳(nanoTime 时域)。 */
    private long lastTickNanos;
    /** 上次 tick 的 Totals 快照(速率差分的减数)。 */
    private StreamThroughputMetrics.Totals lastTickTotals;

    /**
     * 生产构造:时钟取 System.nanoTime(窗口差分的时间分母)。
     */
    public StreamMetricsBridge() {
        this(System::nanoTime);
    }

    /**
     * 测试构造:注入受控时钟(配合两 tick 间的手动推进构造确定性窗口)。
     *
     * @param clock tick 时钟(nanoTime 时域)
     */
    StreamMetricsBridge(LongSupplier clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * 责任:填充真实读源(流式源 execute 在管线装配完成后调用一次,单向:bridge 先随任务
     * 存在,槽位此刻才被真实管线填上)。四个供应商各自独立成槽——装配面可分步填充,但
     * 生产路径一次性全填。volatile 写——填充后 consumer tick 与 JMX 读立即可见。
     * 边界:任一参数为 null 抛 NPE(装配期 fail-fast,掩盖 null 只会让 MBean 面挂着假缺省)。
     *
     * @param totalsSupplier             六项速率计数累计快照读源
     * @param lagBytesSupplier           滞后字节读源(收到位 - 输出前沿)
     * @param pipeDiskUsageBytesSupplier 管道目录磁盘占用读源(目录遍历,只在 tick 内被调)
     * @param pendingPreparedCountSupplier 两阶段挂起 prepared 数读源
     */
    public void setSuppliers(Supplier<StreamThroughputMetrics.Totals> totalsSupplier,
                             LongSupplier lagBytesSupplier,
                             LongSupplier pipeDiskUsageBytesSupplier,
                             IntSupplier pendingPreparedCountSupplier) {
        this.totalsSupplier = Objects.requireNonNull(totalsSupplier, "totalsSupplier");
        this.lagBytesSupplier = Objects.requireNonNull(lagBytesSupplier, "lagBytesSupplier");
        this.pipeDiskUsageBytesSupplier = Objects.requireNonNull(pipeDiskUsageBytesSupplier, "pipeDiskUsageBytesSupplier");
        this.pendingPreparedCountSupplier = Objects.requireNonNull(pendingPreparedCountSupplier, "pendingPreparedCountSupplier");
    }

    /**
     * 责任:统计 tick 的预计算与采样(仅 consumer 线程调用,挂在
     * {@code TransactionConsumer.maybeStats} 的 10s 周期——与指标三行报告同一 tick)。
     * 关键步骤:未装配(totals 槽为 null)即 no-op 返回 → 取 Totals 快照与当前时钟 →
     * 已有基线则以"窗口增量 ÷ 实际流逝秒"算五项速率并写 volatile(首 tick 只立基线,
     * 速率保持缺省 0)→ 推进基线 → 采样 lagBytes / 挂起 prepared / 管道磁盘占用写 volatile
     * (磁盘占用的目录遍历发生在这里,JMX 读路径永远不碰 IO)。
     * 边界:流逝时间 ≤0(时钟异常/同戳双 tick)钳为 1ns 防除零;任何 Throwable 记 WARN
     * 吞掉——指标观测面不得向 consumer 业务路径抛异常。
     */
    public void onStatsTick() {
        Supplier<StreamThroughputMetrics.Totals> totals = totalsSupplier;
        if (totals == null) {
            return;   // 管线未装配:安全 no-op(缺省 0/-1 语义)
        }
        try {
            StreamThroughputMetrics.Totals now = totals.get();
            long nanos = clock.getAsLong();
            if (hasBaseline) {
                double seconds = Math.max(1L, nanos - lastTickNanos) / NANOS_PER_SECOND;
                slotReadBytesPerSecond = perSecond(now.slotBytes(), lastTickTotals.slotBytes(), seconds);
                slotReadMessagesPerSecond = perSecond(now.slotMessages(), lastTickTotals.slotMessages(), seconds);
                assembledTxsPerSecond = perSecond(now.assembledTxs(), lastTickTotals.assembledTxs(), seconds);
                outputRecordsPerSecond = perSecond(now.outputRecords(), lastTickTotals.outputRecords(), seconds);
                outputBytesPerSecond = perSecond(now.outputBytes(), lastTickTotals.outputBytes(), seconds);
            }
            hasBaseline = true;
            lastTickNanos = nanos;
            lastTickTotals = now;
            lagBytes = lagBytesSupplier.getAsLong();
            pipeDiskUsageBytes = pipeDiskUsageBytesSupplier.getAsLong();
            pendingPreparedCount = pendingPreparedCountSupplier.getAsInt();
        }
        catch (Throwable t) {
            LOG.warn("指标桥 tick 采样失败(保留上一次观测值)", t);
        }
    }

    /**
     * 窗口速率纯函数:当前累计与上次快照的增量 ÷ 窗口秒数(Totals 只增不清零,增量恒 ≥0)。
     */
    private static double perSecond(long current, long last, double seconds) {
        return (current - last) / seconds;
    }

    /** slot 侧读取字节速率(窗口差分,装配前恒 0;含控制消息与 Relation——与指标日志行同口径)。 */
    public double getSlotReadBytesPerSecond() {
        return slotReadBytesPerSecond;
    }

    /** slot 侧读取消息速率(条/秒,窗口差分,装配前恒 0)。 */
    public double getSlotReadMessagesPerSecond() {
        return slotReadMessagesPerSecond;
    }

    /** 组装完成(提交交接)速率(tx/秒,窗口差分,装配前恒 0)。 */
    public double getAssembledTxsPerSecond() {
        return assembledTxsPerSecond;
    }

    /** 输出记录速率(实付条/秒,窗口差分,装配前恒 0)。 */
    public double getOutputRecordsPerSecond() {
        return outputRecordsPerSecond;
    }

    /** 输出字节速率(回放重读字节/秒,窗口差分,装配前恒 0)。 */
    public double getOutputBytesPerSecond() {
        return outputBytesPerSecond;
    }

    /** 复制滞后字节(最近收到 LSN − 输出前沿;装配前恒 0)。 */
    public long getLagBytes() {
        return lagBytes;
    }

    /** 两阶段挂起 prepared 数(未决 2PC 事务数;装配前恒 0)。 */
    public int getPendingPreparedCount() {
        return pendingPreparedCount;
    }

    /** 管道目录磁盘占用字节(tick 内采样;装配前与遍历失败恒 -1)。 */
    public long getPipeDiskUsageBytes() {
        return pipeDiskUsageBytes;
    }
}
