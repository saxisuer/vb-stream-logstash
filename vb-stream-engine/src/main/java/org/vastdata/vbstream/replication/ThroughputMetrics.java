package org.vastdata.vbstream.replication;

// 注：Maven 坐标是小写 org.hdrhistogram，Java 包名却是大写 H 的 org.HdrHistogram（上游历史命名）
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

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
 * <p>会话峰值行（2026-08-31 峰值 spec，同日第二份；速率口径修订见同日秒桶 spec 第三份）：
 * 窗口隔离报告使峰值转瞬即逝——负载只占一个窗口，下一窗口速率归零、分布变 n/a。报告增补
 * 第三行"峰值:"留存八项**会话历史最高**：六项速率峰值取**最高单秒速率**（秒桶——窗口均值
 * 会把突发摊薄约窗口/突发时长之倍数，2026-08-31 WSL 基线实测 20 万条 1 秒到达被 10s 窗口
 * 显示为 1/10 速率，故峰值行改秒级分桶），两项分布 max 不受摊薄影响取区间 max 峰值；空载
 * 窗口峰值行也常驻输出——峰值不随窗口翻页消失；从未有过记录打 n/a。
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

    // 会话峰值（2026-08-31 峰值 spec + 同日秒桶 spec 修订）：六项速率峰值 = 秒桶
    // （SecondBucket，见类尾——埋点单写者线程内分桶结算，报告侧弱一致读 max(已结算峰,
    // 当前桶)）；两项分布区间 max 峰值（long，reportLines 里更新——空窗零速率不构成峰值，
    // 分布峰值取各区间直方图 max 的会话最高）。
    private final LongSupplier clock;
    private final SecondBucket slotBytesSec = new SecondBucket();
    private final SecondBucket slotMsgSec = new SecondBucket();
    private final SecondBucket assembledTxSec = new SecondBucket();
    private final SecondBucket outputBytesSec = new SecondBucket();
    private final SecondBucket outputRecSec = new SecondBucket();
    private final SecondBucket outputTxSec = new SecondBucket();
    private long peakReplayNanos = -1L;
    private long peakTxUnits = -1L;

    /**
     * 构造指标器并以当前 nanoTime 为首窗基线（生产路径——组装器构造时调用，首窗自此起算）。
     */
    ThroughputMetrics() {
        this(System.nanoTime());
    }

    /**
     * 构造指标器并注入受控基线戳（测试路径——配合 {@link #reportLines} 的显式 nowNanos
     * 构造确定性窗口，不依赖真实睡眠；秒桶时钟用真实 System.nanoTime）。
     *
     * @param baselineNanos 首窗基线（System.nanoTime 时域）
     */
    ThroughputMetrics(long baselineNanos) {
        this(baselineNanos, System::nanoTime);
    }

    /**
     * 构造指标器并注入受控基线戳与秒桶时钟（测试路径——秒桶行为可确定性驱动：固定时钟
     * 值把事件钉在同一受控秒内、推进时钟触发跨秒结算，见秒桶 spec §4）。
     *
     * @param baselineNanos 首窗基线（与 clock 同一时域即可，窗口差分用）
     * @param clock         秒桶时钟（生产为 System::nanoTime）
     */
    ThroughputMetrics(long baselineNanos, LongSupplier clock) {
        this.lastReportNanos = baselineNanos;
        this.lastTotals = new Totals(0, 0, 0, 0, 0, 0);
        this.clock = clock;
    }

    /**
     * 责任：slot 读取记账——一条 raw 消息到达（字节量 + 条数，含控制消息与 Relation）。
     * 边界：只读 raw.length 不触内容，无异常路径。线程：reader 线程（组装器 onRaw 入口）。
     *
     * @param raw 完整单条消息字节（含类型字节与可选流式 xid 前缀）
     */
    void onSlotMessage(byte[] raw) {
        long now = clock.getAsLong();
        slotBytes.add(raw.length);
        slotMessages.increment();
        slotBytesSec.bump(now, raw.length);
        slotMsgSec.bump(now, 1L);
    }

    /**
     * 责任：组装完成记账——一个桶提交交接（Commit/StreamCommit/CommitPrepared；回滚丢弃不计）。
     * 线程：reader 线程（组装器 handoff）。
     */
    void onTxHandedOff() {
        assembledTxs.increment();
        assembledTxSec.bump(clock.getAsLong(), 1L);
    }

    /**
     * 责任：输出字节记账——回放器从管道重读一个单元的载荷长度（aborted 过滤前计入：
     * 被剔除单元的字节确实从管道读回了，本口径度量的是回读吞吐）。线程：consumer 线程。
     *
     * @param payloadLength 单元载荷字节数
     */
    void onReplayedUnit(int payloadLength) {
        outputBytes.add(payloadLength);
        outputBytesSec.bump(clock.getAsLong(), payloadLength);
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
        long now = clock.getAsLong();
        replayNanos.recordValue(Math.min(durationNanos, MAX_TRACKED_DURATION_NANOS));
        txSizes.recordValue(Math.min(unitCount, MAX_TRACKED_TX_UNITS));
        outputRecords.add(emittedRecords);
        outputTxs.increment();
        outputRecSec.bump(now, emittedRecords);
        outputTxSec.bump(now, 1L);
    }

    /**
     * 责任：生成报告窗口的三行 INFO 文案（吞吐行 + 分布行 + 峰值行），并把计数/时间基线推进到本次
     * 报告时刻——**每次调用都是一个窗口边界**，速率 = 六计数窗口内 delta ÷ 实际流逝秒数
     * （nanoTime 差，非固定周期除法）；分布 = Recorder 取走的上一区间（窗口外样本不进本次）；
     * 峰值 = 八项会话历史最高的留存快照（窗口速率 &gt; 0 才刷新速率峰值、区间 max 顺手留存
     * 分布峰值——空窗不稀释、零速率不构成峰值，见类 javadoc 峰值段）。
     * 关键步骤：累计快照 → 差分与格式化 → 取两个区间直方图（同时更新分布峰值）→ 刷新六项
     * 速率峰值并格式化峰值行 → 推进基线。边界：elapsed ≤ 0
     * （时钟异常/测试注入失序）钳为 1ns 防除零；分布段零样本打 n/a；返回的直方图是 Recorder
     * 回收复用的对象，只读即弃、不得持有。线程：consumer 线程（统计 tick；与分布的写入同
     * 线程，Recorder 单写者假设由此成立）。
     *
     * @param nowNanos 报告时刻（System.nanoTime 时域；调用方持有以便与其余统计共用同一戳）
     * @return 三行文案（下标 0 = 吞吐行，1 = 分支行，2 = 峰值行）；格式即契约，单测整行断言
     */
    List<String> reportLines(long nowNanos) {
        Totals now = totals();
        long elapsedNanos = Math.max(1L, nowNanos - lastReportNanos);
        double seconds = elapsedNanos / 1_000_000_000.0;
        double slotBytesRate = deltaPerSecond(now.slotBytes(), lastTotals.slotBytes(), seconds);
        double slotMsgRate = deltaPerSecond(now.slotMessages(), lastTotals.slotMessages(), seconds);
        double assembledTxRate = deltaPerSecond(now.assembledTxs(), lastTotals.assembledTxs(), seconds);
        double outputBytesRate = deltaPerSecond(now.outputBytes(), lastTotals.outputBytes(), seconds);
        double outputRecRate = deltaPerSecond(now.outputRecords(), lastTotals.outputRecords(), seconds);
        double outputTxRate = deltaPerSecond(now.outputTxs(), lastTotals.outputTxs(), seconds);
        String throughput = "吞吐: slot=" + formatBytesPerSec(slotBytesRate)
                + " (" + formatCountPerSec(slotMsgRate) + " msg/s)"
                + " | 组装=" + formatCountPerSec(assembledTxRate) + " tx/s"
                + " | 输出=" + formatBytesPerSec(outputBytesRate)
                + " (" + formatCountPerSec(outputRecRate) + " rec/s, " + formatCountPerSec(outputTxRate) + " tx/s)";
        String distribution = "分布: 回放耗时 "
                + intervalPart(replayNanos, ThroughputMetrics::formatNanos,
                        v -> peakReplayNanos = Math.max(peakReplayNanos, v))
                + " | 事务大小 "
                + intervalPart(txSizes, v -> grouped(v) + " rec",
                        v -> peakTxUnits = Math.max(peakTxUnits, v));
        // 速率峰值取秒桶会话最高单秒（秒桶 spec：窗口均值会把突发摊薄，峰值行须反映真实突发；
        // 未结算的当前桶计数作为下界候选参与比较——悬空桶不丢最后一秒的突发）
        String peak = "峰值: slot=" + peakBytesPerSec(slotBytesSec.peakOrCurrent())
                + " (" + peakCountPerSec(slotMsgSec.peakOrCurrent()) + " msg/s)"
                + " | 组装=" + peakCountPerSec(assembledTxSec.peakOrCurrent()) + " tx/s"
                + " | 输出=" + peakBytesPerSec(outputBytesSec.peakOrCurrent())
                + " (" + peakCountPerSec(outputRecSec.peakOrCurrent()) + " rec/s, " + peakCountPerSec(outputTxSec.peakOrCurrent()) + " tx/s)"
                + " | 耗时=" + (peakReplayNanos < 0 ? NOT_AVAILABLE : formatNanos(peakReplayNanos))
                + " | 大小=" + (peakTxUnits < 0 ? NOT_AVAILABLE : grouped(peakTxUnits) + " rec");
        lastTotals = now;
        lastReportNanos = nowNanos;
        return List.of(throughput, distribution, peak);
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
     * 零样本区间整体打 n/a。区间 max 顺手经 peakUpdater 留存会话峰值（2026-08-31 峰值
     * spec §2 #7/#8——区间即将被 Recorder 回收，取走时机仅此一处）。边界：返回的 Histogram
     * 是 Recorder 回收复用对象，本方法内读毕即弃。线程：consumer 线程（与 recordValue 同线程）。
     *
     * @param recorder    分布记录器（replayNanos / txSizes 之一）
     * @param formatter   值 → 带单位文案（耗时与事务大小各一套）
     * @param peakUpdater 区间 max 的会话峰值留存回调（Math::max 语义，调用方闭包峰值字段）
     */
    private String intervalPart(SingleWriterRecorder recorder, LongFunction<String> formatter,
                                LongConsumer peakUpdater) {
        Histogram interval = recorder.getIntervalHistogram();
        if (interval.getTotalCount() == 0) {
            return NOT_AVAILABLE;
        }
        peakUpdater.accept(interval.getMaxValue());
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

    /** 峰值字节速率格式化：≤0（从未有过记录，秒桶空态）打 n/a，其余复用 {@link #formatBytesPerSec}。 */
    private static String peakBytesPerSec(double peak) {
        return peak <= 0 ? NOT_AVAILABLE : formatBytesPerSec(peak);
    }

    /** 峰值计数速率格式化：同上——≤0 打 n/a，其余复用 {@link #formatCountPerSec}。 */
    private static String peakCountPerSec(double peak) {
        return peak <= 0 ? NOT_AVAILABLE : formatCountPerSec(peak);
    }

    /**
     * 秒桶（2026-08-31 秒桶 spec）：一项速率计数的"当前秒累计 + 会话最高单秒速率"。
     * 关键步骤：{@link #bump} 按时钟秒号累计，秒号翻滚即结算上一秒（{@code peak = max(peak, cur)}）
     * 并重置当前桶；{@link #peakOrCurrent} 取"已结算峰与当前未结算桶的较大者"——悬空桶
     * （最后一条消息后秒未走满、无人触发结算）的计数作为该秒速率的**下界**候选，不会高估。
     * 边界效应：消息横跨秒号拆两桶，秒峰为下界、随秒结算收敛——秒级粒度本就是近似。
     * 线程约束：bump 由该埋点的单写者线程调用（slot/组装 = reader、输出 = consumer）；
     * peak/cur 为 volatile——报告线程（consumer）弱一致读，与 LongAdder 的统计语义同级。
     */
    private static final class SecondBucket {

        /** 当前桶秒号（nanoTime/1s；Long.MIN_VALUE 保证首条消息必然开桶）。 */
        private long secNo = Long.MIN_VALUE;
        /** 当前秒内累计（该秒速率的进行值）。 */
        private volatile long cur;
        /** 会话最高单秒速率（仅秒结算时刷新）。 */
        private volatile long peak;

        /** 累计 delta 进当前秒；秒号翻滚时结算上一秒峰值并重置桶。 */
        void bump(long nanos, long delta) {
            long sec = nanos / 1_000_000_000L;
            if (sec != secNo) {
                peak = Math.max(peak, cur);
                cur = 0L;
                secNo = sec;
            }
            cur += delta;
        }

        /** 会话最高单秒速率（含未结算当前桶下界候选）。 */
        long peakOrCurrent() {
            return Math.max(peak, cur);
        }
    }
}
