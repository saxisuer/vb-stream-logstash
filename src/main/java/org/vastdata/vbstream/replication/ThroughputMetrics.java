package org.vastdata.vbstream.replication;

// 注：Maven 坐标是小写 org.hdrhistogram，Java 包名却是大写 H 的 org.HdrHistogram（上游历史命名）
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

/**
 * 管线吞吐与分布指标（2026-08-31 设计，spec docs/superpowers/specs/2026-08-31-throughput-metrics-design.md）：
 * reader/consumer 双线程管线的**本地周期日志行**观测面——六项速率计数（slot 读取 bytes/msg、
 * 组装 tx、输出 bytes/rec/tx）+ 两项分位数分布（事务回放耗时、事务大小，P90/P95/max）。
 *
 * <p>实现取向（设计 §1 选型）：计数与窗口差分手写（消费通道仅日志行，Micrometer/Dropwizard
 * 的速率算法与输出格式都对本场景无净值）；分位数用 {@link SingleWriterRecorder}
 * （HdrHistogram，零传递依赖）——每次报告 {@code getIntervalHistogram()} 取走上一区间，
 * 窗口外样本不稀释当前值。全部计量收在本类后面，未来接监控系统只换实现、埋点点位不动。
 *
 * <p>口径（设计 §2）：slot 侧含全部 pgoutput 消息（控制消息与 Relation——"从槽读到什么"的
 * 诚实口径，与输出侧 records 不可直接对照，bytes 才是两端口径一致的对照对）；组装 tx 仅计
 * 提交交接（回滚丢弃不计）；输出 bytes 为回放重读的管道原始字节（aborted 过滤在重读之后，
 * 被剔除单元的字节也计入——它确实从管道读回了）；输出 records 为 aborted 过滤后实付的
 * TxChange 数；两项分布只收**完整交付**的事务（End 发出，fail-fast 截断的不入）。
 *
 * <p>线程约束（设计 §5）：速率计数器是 {@link LongAdder}——slot 侧 reader 单写、输出侧
 * consumer 单写、报告时 consumer 读，弱一致读对统计语义足够；两个 Recorder 由 consumer
 * 线程**单写**（record 与报告同线程）。指标永不向业务热路径抛异常（越界样本钳制到上界），
 * 无可关闭资源、停机无需收尾。
 */
final class ThroughputMetrics {

    /** 回放耗时可追踪上界：1h（ns）——正常负载远不可达，钳到上界本身即"病态慢"的信号。 */
    private static final long MAX_TRACKED_DURATION_NANOS = 3_600_000_000_000L;
    /** 事务大小可追踪上界：10 亿数据单元——同上，防御性钳制。 */
    private static final long MAX_TRACKED_TX_UNITS = 1_000_000_000L;
    /** 分位数有效位数：2 位（量化误差约 1%），HDR 桶数对数级、内存 KB 量级。 */
    private static final int SIGNIFICANT_DIGITS = 2;
    /** 字节速率单位阶梯（SI 十进制，PG 工具链惯例）。 */
    private static final String[] BYTE_UNITS = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s"};
    /** 耗时单位阶梯（SI 十进制）。 */
    private static final String[] TIME_UNITS = {"ns", "µs", "ms", "s"};
    /** 分布段零样本时的占位（空区间无分位可言）。 */
    private static final String NOT_AVAILABLE = "n/a";

    /**
     * 六项速率计数的累计快照（只增不清零——窗口语义在 {@link #reportLines} 的差分里，
     * 本 record 是接线测试断言全链路插桩正确性的观测面）。
     */
    record Totals(long slotBytes, long slotMessages, long assembledTxs,
                  long outputBytes, long outputRecords, long outputTxs) { }

    private final LongAdder slotBytes = new LongAdder();
    private final LongAdder slotMessages = new LongAdder();
    private final LongAdder assembledTxs = new LongAdder();
    private final LongAdder outputBytes = new LongAdder();
    private final LongAdder outputRecords = new LongAdder();
    private final LongAdder outputTxs = new LongAdder();

    private final SingleWriterRecorder replayNanos =
            new SingleWriterRecorder(1, MAX_TRACKED_DURATION_NANOS, SIGNIFICANT_DIGITS);
    private final SingleWriterRecorder txSizes =
            new SingleWriterRecorder(1, MAX_TRACKED_TX_UNITS, SIGNIFICANT_DIGITS);

    /** 上次报告的 nanoTime 基线——速率差分的时间分母（now - 本值）。 */
    private long lastReportNanos;
    /** 上次报告的计数快照——速率差分的分子（当前累计 - 本值）。 */
    private Totals lastTotals;

    /**
     * 构造指标器并以当前 nanoTime 为首窗基线（生产路径——组装器构造时调用，首窗自此起算）。
     */
    ThroughputMetrics() {
        this(System.nanoTime());
    }

    /**
     * 构造指标器并注入受控基线戳（测试路径——配合 {@link #reportLines} 的显式 nowNanos
     * 构造确定性窗口，不依赖真实睡眠）。
     *
     * @param baselineNanos 首窗基线（System.nanoTime 时域）
     */
    ThroughputMetrics(long baselineNanos) {
        this.lastReportNanos = baselineNanos;
        this.lastTotals = new Totals(0, 0, 0, 0, 0, 0);
    }

    /**
     * 责任：slot 读取记账——一条 raw 消息到达（字节量 + 条数，含控制消息与 Relation）。
     * 边界：只读 raw.length 不触内容，无异常路径。线程：reader 线程（组装器 onRaw 入口）。
     *
     * @param raw 完整单条消息字节（含类型字节与可选流式 xid 前缀）
     */
    void onSlotMessage(byte[] raw) {
        slotBytes.add(raw.length);
        slotMessages.increment();
    }

    /**
     * 责任：组装完成记账——一个桶提交交接（Commit/StreamCommit/CommitPrepared；回滚丢弃不计）。
     * 线程：reader 线程（组装器 handoff）。
     */
    void onTxHandedOff() {
        assembledTxs.increment();
    }

    /**
     * 责任：输出字节记账——回放器从管道重读一个单元的载荷长度（aborted 过滤前计入：
     * 被剔除单元的字节确实从管道读回了，本口径度量的是回读吞吐）。线程：consumer 线程。
     *
     * @param payloadLength 单元载荷字节数
     */
    void onReplayedUnit(int payloadLength) {
        outputBytes.add(payloadLength);
    }

    /**
     * 责任：一个事务完整交付的合并记账——输出 tx +1、实付 records 累加，耗时与事务大小
     * （unitCount，aborted 过滤前）各入一个分布样本。边界：越上界的样本钳制到上界（热路径
     * 防御——指标永不抛异常进业务路径，钳到上界本身已是病态值的信号）；仅完整交付的事务
     * 调用本方法（End 发出后），fail-fast 截断的不入分布。线程：consumer 线程。
     *
     * @param durationNanos   本事务回放全程耗时（processBucket 起止，含下游回调）
     * @param unitCount       桶记账的数据单元数（Begin.expectedChanges 同源，过滤前）
     * @param emittedRecords  实付 TxChange 数（aborted 过滤后）
     */
    void onTxOutput(long durationNanos, long unitCount, long emittedRecords) {
        replayNanos.recordValue(Math.min(durationNanos, MAX_TRACKED_DURATION_NANOS));
        txSizes.recordValue(Math.min(unitCount, MAX_TRACKED_TX_UNITS));
        outputRecords.add(emittedRecords);
        outputTxs.increment();
    }

    /**
     * 责任：生成报告窗口的两行 INFO 文案（吞吐行 + 分布行），并把计数/时间基线推进到本次
     * 报告时刻——**每次调用都是一个窗口边界**，速率 = 六计数窗口内 delta ÷ 实际流逝秒数
     * （nanoTime 差，非固定周期除法）；分布 = Recorder 取走的上一区间（窗口外样本不进本次）。
     * 关键步骤：累计快照 → 差分与格式化 → 取两个区间直方图 → 推进基线。边界：elapsed ≤ 0
     * （时钟异常/测试注入失序）钳为 1ns 防除零；分布段零样本打 n/a；返回的直方图是 Recorder
     * 回收复用的对象，只读即弃、不得持有。线程：consumer 线程（统计 tick；与分布的写入同
     * 线程，Recorder 单写者假设由此成立）。
     *
     * @param nowNanos 报告时刻（System.nanoTime 时域；调用方持有以便与其余统计共用同一戳）
     * @return 两行文案（下标 0 = 吞吐行，1 = 分品行）；格式即契约，单测整行断言
     */
    List<String> reportLines(long nowNanos) {
        Totals now = totals();
        long elapsedNanos = Math.max(1L, nowNanos - lastReportNanos);
        double seconds = elapsedNanos / 1_000_000_000.0;
        String throughput = "吞吐: slot=" + formatBytesPerSec(deltaPerSecond(now.slotBytes(), lastTotals.slotBytes(), seconds))
                + " (" + formatCountPerSec(deltaPerSecond(now.slotMessages(), lastTotals.slotMessages(), seconds)) + " msg/s)"
                + " | 组装=" + formatCountPerSec(deltaPerSecond(now.assembledTxs(), lastTotals.assembledTxs(), seconds)) + " tx/s"
                + " | 输出=" + formatBytesPerSec(deltaPerSecond(now.outputBytes(), lastTotals.outputBytes(), seconds))
                + " (" + formatCountPerSec(deltaPerSecond(now.outputRecords(), lastTotals.outputRecords(), seconds)) + " rec/s, "
                + formatCountPerSec(deltaPerSecond(now.outputTxs(), lastTotals.outputTxs(), seconds)) + " tx/s)";
        String distribution = "分布: 回放耗时 " + intervalPart(replayNanos, ThroughputMetrics::formatNanos)
                + " | 事务大小 " + intervalPart(txSizes, v -> grouped(v) + " rec");
        lastTotals = now;
        lastReportNanos = nowNanos;
        return List.of(throughput, distribution);
    }

    /**
     * 责任：六项速率计数的累计快照（只增不清零，窗口差分归 reportLines 自理）——接线测试
     * 断言组装器→消费器全链路插桩正确性的观测面。边界：LongAdder 弱一致读（统计语义足够）。
     * 线程：任意（各 Adder 并发安全）。
     */
    Totals totals() {
        return new Totals(slotBytes.sum(), slotMessages.sum(), assembledTxs.sum(),
                outputBytes.sum(), outputRecords.sum(), outputTxs.sum());
    }

    /**
     * 责任：分布段文案——取 Recorder 上一区间的 p90/p95/max，经 formatter 渲染成带单位的值
     * （耗时用 {@link #formatNanos} 自带 ns..s 单位、事务大小用 grouped + " rec" 后缀）；
     * 零样本区间整体打 n/a。边界：返回的 Histogram 是 Recorder 回收复用对象，本方法内读毕即弃。
     * 线程：consumer 线程（与 recordValue 同线程）。
     */
    private static String intervalPart(SingleWriterRecorder recorder, LongFunction<String> formatter) {
        Histogram interval = recorder.getIntervalHistogram();
        if (interval.getTotalCount() == 0) {
            return NOT_AVAILABLE;
        }
        return "p90=" + formatter.apply(interval.getValueAtPercentile(90))
                + " p95=" + formatter.apply(interval.getValueAtPercentile(95))
                + " max=" + formatter.apply(interval.getMaxValue());
    }

    /** 窗口内某计数的每秒增量（分子 = 当前累计 - 上次报告累计）。 */
    private static double deltaPerSecond(long current, long last, double seconds) {
        return (current - last) / seconds;
    }

    /**
     * 责任：字节速率格式化——SI 十进制千进位、恒一位小数（"982.4 B/s"、"12.4 MB/s"），
     * 值满 1000 进一档，TB/s 封顶。纯函数。
     */
    static String formatBytesPerSec(double bytesPerSec) {
        double v = bytesPerSec;
        int unit = 0;
        while (v >= 1000 && unit < BYTE_UNITS.length - 1) {
            v /= 1000;
            unit++;
        }
        return String.format(Locale.ROOT, "%.1f %s", v, BYTE_UNITS[unit]);
    }

    /**
     * 责任：耗时格式化——ns→µs→ms→s 千进位；同档内值 &lt;100 一位小数（"3.2ms"）、
     * ≥100 取整（"125ms"）——小值看精度、大值看数量级。纯函数。
     */
    static String formatNanos(long nanos) {
        double v = nanos;
        int unit = 0;
        while (v >= 1000 && unit < TIME_UNITS.length - 1) {
            v /= 1000;
            unit++;
        }
        return v >= 100
                ? String.format(Locale.ROOT, "%.0f%s", v, TIME_UNITS[unit])
                : String.format(Locale.ROOT, "%.1f%s", v, TIME_UNITS[unit]);
    }

    /**
     * 责任：计数速率格式化（msg/rec/tx 每秒）——&lt;100 一位小数（"5.0"）、≥100 整数千分位
     * （"12,346"），与耗时同阈值规则保持一行内读感一致。纯函数。
     */
    static String formatCountPerSec(double countPerSec) {
        return countPerSec >= 100
                ? grouped(Math.round(countPerSec))
                : String.format(Locale.ROOT, "%.1f", countPerSec);
    }

    /** 整数千分位渲染（Locale.ROOT 固定逗号分隔，分位数与计数速率的 ≥100 档共用）。 */
    private static String grouped(long v) {
        return String.format(Locale.ROOT, "%,d", v);
    }
}
