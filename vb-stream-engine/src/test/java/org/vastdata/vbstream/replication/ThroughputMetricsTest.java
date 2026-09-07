package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThroughputMetrics 单测（2026-08-31 吞吐指标设计），四块行为：
 * ①格式化纯函数（字节/耗时/计数速率）；②吞吐行窗口差分语义；③分布行区间隔离语义；
 * ④rec 秒桶逐条入桶语义（2026-09-07 输出峰值口径修正——伪影回归锚：单大事务跨多秒
 * 回放时峰值不再按回放时长虚高）。
 *
 * <p>夹具约定：全部用例经 {@code new ThroughputMetrics(基准戳)} 注入受控时钟（0 基准 +
 * 显式 nowNanos 报告），不依赖真实睡眠；报告行断言**整行字符串相等**（格式即契约——与
 * consumer 统计行同风格，改动输出格式必须连带改这里）。
 */
class ThroughputMetricsTest {

    /** 常用报告窗口：10s（与 TransactionConsumer 的统计周期同档）。 */
    private static final long TEN_SECONDS = 10_000_000_000L;

    /**
     * 字节速率格式化：SI 十进制千进位，恒一位小数——值满 1000 进一档（B/s→KB/s→MB/s→GB/s）。
     */
    @Test
    void formatBytesPerSecUsesSiUnitsWithSingleDecimal() {
        assertEquals("0.0 B/s", ThroughputMetrics.formatBytesPerSec(0));
        assertEquals("512.0 B/s", ThroughputMetrics.formatBytesPerSec(512));
        assertEquals("982.4 B/s", ThroughputMetrics.formatBytesPerSec(982.4));
        assertEquals("10.0 KB/s", ThroughputMetrics.formatBytesPerSec(10_000));
        assertEquals("12.4 MB/s", ThroughputMetrics.formatBytesPerSec(12_400_000));
        assertEquals("2.0 GB/s", ThroughputMetrics.formatBytesPerSec(2_000_000_000));
    }

    /**
     * 耗时格式化：ns→µs→ms→s 千进位；同档内 <100 保留一位小数、≥100 取整
     * （大值看量级、小值看精度）。
     */
    @Test
    void formatNanosStepsUnitsAndRoundsAtHundred() {
        assertEquals("400ns", ThroughputMetrics.formatNanos(400));
        assertEquals("852µs", ThroughputMetrics.formatNanos(852_000));
        assertEquals("3.2ms", ThroughputMetrics.formatNanos(3_200_000));
        assertEquals("125ms", ThroughputMetrics.formatNanos(125_000_000));
        assertEquals("1.4s", ThroughputMetrics.formatNanos(1_400_000_000));
    }

    /**
     * 计数速率（msg/rec/tx 每秒）格式化：<100 一位小数（"5.0"），≥100 整数千分位
     * （"12,346"）——与耗时同阈值规则，报告行内各数读感一致。
     */
    @Test
    void formatCountPerSecHasDecimalAndIntegerTiers() {
        assertEquals("4.5", ThroughputMetrics.formatCountPerSec(4.5));
        assertEquals("5.0", ThroughputMetrics.formatCountPerSec(5));
        assertEquals("100", ThroughputMetrics.formatCountPerSec(100));
        assertEquals("12,346", ThroughputMetrics.formatCountPerSec(12_345.6));
    }

    /**
     * 吞吐行速率 = 窗口内计数 delta ÷ 实际流逝秒数：驱动一组已知事件后断言整行相等；
     * 同一实例第二窗口无事件则全零——计数器累计（totals() 不清零），报告行只反映窗口。
     */
    @Test
    void rateReportUsesWindowDeltaAndIdleWindowZeroes() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        for (int i = 0; i < 1000; i++) {
            metrics.onSlotMessage(new byte[100]);       // slot: 100,000 B / 1000 msg
        }
        for (int i = 0; i < 50; i++) {
            metrics.onTxHandedOff();                    // 组装: 50 tx
            metrics.onReplayedUnit(100);                // 输出字节: 5,000 B
        }
        for (int i = 0; i < 10; i++) {
            metrics.onTxOutput(1_000_000L, 5L, 5L);     // 输出: 10 tx / 50 rec，样本 1ms/5rec
        }
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertEquals("吞吐: slot=10.0 KB/s (100 msg/s) | 组装=5.0 tx/s | 输出=500.0 B/s (5.0 rec/s, 1.0 tx/s)",
                lines.get(0));
        assertEquals("分布: 回放耗时 p90=1.0ms p95=1.0ms max=1.0ms | 事务大小 p90=5 rec p95=5 rec max=5 rec",
                lines.get(1));
        // 第二窗口（10s→20s）无任何事件：速率全零、分布零样本 n/a——计数器累计但窗口干净
        List<String> idle = metrics.reportLines(2 * TEN_SECONDS);
        assertEquals("吞吐: slot=0.0 B/s (0.0 msg/s) | 组装=0.0 tx/s | 输出=0.0 B/s (0.0 rec/s, 0.0 tx/s)",
                idle.get(0));
        assertEquals("分布: 回放耗时 n/a | 事务大小 n/a", idle.get(1));
    }

    /**
     * 分布行区间隔离（Recorder 语义）：每次报告取走上一区间，首窗样本不进次窗——
     * 次窗单独一个样本时 p90/p95/max 全部精确等于该值（无插值歧义），可整行相等断言。
     */
    @Test
    void distributionReportIsolatesWindowSamples() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        metrics.onTxOutput(3_200_000L, 10L, 10L);
        List<String> first = metrics.reportLines(TEN_SECONDS);
        assertTrue(first.get(1).contains("p90=3.2ms"), "首窗 p90 应为 3.2ms: " + first.get(1));
        assertTrue(first.get(1).contains("p90=10 rec"), "首窗事务大小 p90 应为 10 rec: " + first.get(1));

        metrics.onTxOutput(5_000_000L, 100L, 100L);
        List<String> second = metrics.reportLines(2 * TEN_SECONDS);
        assertEquals("分布: 回放耗时 p90=5.0ms p95=5.0ms max=5.0ms | 事务大小 p90=100 rec p95=100 rec max=100 rec",
                second.get(1));
    }

    /**
     * 越界钳制：耗时超 1h、事务大小超 10 亿的样本钳到上界，reportLines 不抛异常。
     * 断言用 ±2% 容差而非精确相等：HDR 按有效数字做桶级量化，读回值可略越上界
     * （3600s → 3608s）——数据结构的文档化行为，不是钳制失灵。
     */
    @Test
    void distributionReportClampsOutOfRangeSamples() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        metrics.onTxOutput(5L * 3_600_000_000_000L, 2_000_000_000L, 1L);
        List<String> lines = assertDoesNotThrow(() -> metrics.reportLines(TEN_SECONDS));
        String part = lines.get(1);
        java.util.regex.Matcher dur = java.util.regex.Pattern.compile("max=(\\d+)s").matcher(part);
        assertTrue(dur.find(), "应含耗时 max（秒）: " + part);
        assertEquals(3600, Long.parseLong(dur.group(1)), 3600 * 0.02,
                "耗时 max 应钳到 1h 附近: " + part);
        java.util.regex.Matcher size = java.util.regex.Pattern.compile("max=([\\d,]+) rec").matcher(part);
        assertTrue(size.find(), "应含事务大小 max（rec）: " + part);
        assertEquals(1_000_000_000L, Long.parseLong(size.group(1).replace(",", "")),
                1_000_000_000L * 0.02, "事务大小 max 应钳到 10 亿附近: " + part);
    }

    /**
     * totals() 六计数只增不清零，跨报告窗口累计——供接线测试断言全链路插桩
     * （任何一处埋点漏挂在此露馅）。
     */
    @Test
    void totalsAccumulateAcrossWindows() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        metrics.onSlotMessage(new byte[7]);
        metrics.onSlotMessage(new byte[3]);
        metrics.onTxHandedOff();
        metrics.onReplayedUnit(9);
        metrics.onTxOutput(1_000L, 2L, 2L);
        metrics.reportLines(TEN_SECONDS);
        ThroughputMetrics.Totals t = metrics.totals();
        assertEquals(10, t.slotBytes());
        assertEquals(2, t.slotMessages());
        assertEquals(1, t.assembledTxs());
        assertEquals(9, t.outputBytes());
        assertEquals(2, t.outputRecords());
        assertEquals(1, t.outputTxs());
    }

    /**
     * 从未有过任何记录时峰值行八项全部 n/a——空窗零速率不构成峰值（峰值 spec §2）。
     * 整行断言（格式即契约）。
     */
    @Test
    void peakLineIsAllNaBeforeFirstRecord() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertEquals("峰值: slot=n/a (n/a msg/s) | 组装=n/a tx/s | 输出=n/a (n/a rec/s, n/a tx/s) | 耗时=n/a | 大小=n/a",
                lines.get(2));
    }

    /**
     * 峰值不随窗口翻页消失：高窗之后空窗，吞吐行归零、分布行 n/a，峰值行完整保留
     * 高窗的八项会话最高（峰值 spec §1）。速率峰值为最高单秒（秒桶 spec）：事件全部
     * 同秒灌入时秒峰值 = 灌入总量，空窗报告取悬空桶候选。
     */
    @Test
    void peakLineRetainsEightValuesIntoIdleWindow() {
        long[] clock = {500_000_000L};                  // 固定 0.5s：全部事件钉在同一受控秒内，不依赖真实时钟
        ThroughputMetrics metrics = new ThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 1000; i++) {
            metrics.onSlotMessage(new byte[100]);       // slot: 100,000 B / 1000 msg（同秒）
        }
        for (int i = 0; i < 50; i++) {
            metrics.onTxHandedOff();                    // 组装: 50 tx
            metrics.onReplayedUnit(100);                // 输出字节: 5,000 B
        }
        for (int i = 0; i < 10; i++) {
            metrics.onTxOutput(1_000_000L, 5L, 5L);     // 输出: 10 tx / 50 rec，样本 1ms/5rec
            for (int j = 0; j < 5; j++) {
                metrics.onRecordDelivered();            // rec 秒桶逐条入桶（同秒共 50 条——2026-09-07 修正后事务尾不再落 rec 桶）
            }
        }
        metrics.reportLines(TEN_SECONDS);               // 高窗
        List<String> idle = metrics.reportLines(2 * TEN_SECONDS);   // 空窗：吞吐归零、分布 n/a
        assertEquals("吞吐: slot=0.0 B/s (0.0 msg/s) | 组装=0.0 tx/s | 输出=0.0 B/s (0.0 rec/s, 0.0 tx/s)",
                idle.get(0));
        assertEquals("分布: 回放耗时 n/a | 事务大小 n/a", idle.get(1));
        assertEquals("峰值: slot=100.0 KB/s (1,000 msg/s) | 组装=50.0 tx/s | 输出=5.0 KB/s (50.0 rec/s, 10.0 tx/s) | 耗时=1.0ms | 大小=5 rec",
                idle.get(2));
    }

    /**
     * 分布峰值取会话最高：次窗更小的样本使分布行回落（区间隔离），峰值行仍保留首窗
     * max——两个时点的对照即"窗口报告"与"会话峰值"的语义分界（峰值 spec §2 #7/#8）。
     */
    @Test
    void peakLineKeepsSessionMaxDistribution() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        metrics.onTxOutput(5_000_000L, 100L, 100L);
        metrics.reportLines(TEN_SECONDS);
        metrics.onTxOutput(2_000_000L, 50L, 50L);
        List<String> second = metrics.reportLines(2 * TEN_SECONDS);
        assertEquals("分布: 回放耗时 p90=2.0ms p95=2.0ms max=2.0ms | 事务大小 p90=50 rec p95=50 rec max=50 rec",
                second.get(1));
        assertTrue(second.get(2).contains("耗时=5.0ms"), "峰值应保留首窗 5ms: " + second.get(2));
        assertTrue(second.get(2).contains("大小=100 rec"), "峰值应保留首窗 100 rec: " + second.get(2));
    }

    /**
     * 秒桶核心语义：突发不被窗口摊薄（秒桶 spec §4）——同秒灌 2000 条后推进一秒结算，
     * 峰值行如实反映 2,000 msg/s，吞吐行按窗口均值显示摊薄值 20.0 msg/s。两行双语义
     * 的直接对照锚定（2026-08-31 WSL 基线实测暴露的失真场景）。
     */
    @Test
    void peakLineSecondBucketDoesNotDiluteBurst() {
        long[] clock = {500_000_000L};                  // 从 0.5s 起步（首秒内）
        ThroughputMetrics metrics = new ThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 2000; i++) {
            metrics.onSlotMessage(new byte[100]);       // 同一秒内 2000 条 / 200,000 B
        }
        clock[0] = 1_200_000_000L;                      // 推进到下一秒：再灌一条触发上一秒结算
        metrics.onSlotMessage(new byte[100]);
        List<String> lines = metrics.reportLines(10 * TEN_SECONDS);   // 窗口按 100s 计（极限摊薄）
        assertTrue(lines.get(0).contains("(20.0 msg/s)"),
                "吞吐行应按窗口均值摊薄: " + lines.get(0));
        assertTrue(lines.get(2).contains("(2,000 msg/s)"),
                "峰值行应为最高单秒速率、不被窗口摊薄: " + lines.get(2));
    }

    /**
     * 悬空桶下界：当前秒未结算（此后无事件触发结算）时，报告取 max(已结算峰, 当前桶
     * 累计)——最后一秒的突发不因秒未走满而丢失；当前桶计数是该秒速率的下界，不会高估
     * （秒桶 spec §2）。
     */
    @Test
    void peakLineDanglingBucketCountsAsLowerBound() {
        long[] clock = {300_000_000L};                  // 0.3s（秒未走满）
        ThroughputMetrics metrics = new ThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 300; i++) {
            metrics.onSlotMessage(new byte[10]);
        }
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertTrue(lines.get(2).contains("(300 msg/s,") || lines.get(2).contains("(300 msg/s)"),
                "悬空桶 300 条应作峰值候选: " + lines.get(2));
    }

    /**
     * 跨秒结算重置：两秒各灌不同量，峰值取最高单秒（100）而非两秒合计（130）。
     */
    @Test
    void peakLineSettlesToBestSingleSecond() {
        long[] clock = {0L};
        ThroughputMetrics metrics = new ThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 100; i++) {
            metrics.onSlotMessage(new byte[10]);
        }
        clock[0] = 1_500_000_000L;                      // 下一秒
        for (int i = 0; i < 30; i++) {
            metrics.onSlotMessage(new byte[10]);
        }
        clock[0] = 2_500_000_000L;                      // 再推进触发第二秒结算
        metrics.onSlotMessage(new byte[10]);
        List<String> lines = metrics.reportLines(3 * TEN_SECONDS);
        assertTrue(lines.get(2).contains("(100 msg/s)"), "峰值应为最高单秒 100 而非合计: " + lines.get(2));
    }

    /**
     * rec 秒桶逐条入桶——伪影回归锚（2026-09-07 输出峰值口径修正）：修复前 onTxOutput
     * 把整事务 emittedRecords 一次性记进 End 落点的一秒，跨 3 秒回放 3000 条时峰值虚高
     * 为 3,000 rec/s（虚高倍数 ≈ 回放时长）；修复后秒桶只由 onRecordDelivered 逐条驱动，
     * 峰值如实反映最高单秒 1,000，累计不受影响（吞吐行 3000÷40s=75.0 rec/s 照常可见）。
     */
    @Test
    void peakLineRecordsSingleLargeTxPerRecordNotPerTx() {
        long[] clock = {0L};
        ThroughputMetrics metrics = new ThroughputMetrics(0L, () -> clock[0]);
        for (int sec = 0; sec < 3; sec++) {
            clock[0] = sec * 1_000_000_000L + 100_000_000L;   // 每受控秒 1000 条（跨 3 秒回放）
            for (int i = 0; i < 1000; i++) {
                metrics.onRecordDelivered();
            }
        }
        metrics.onTxOutput(3_000_000_000L, 3_000L, 3_000L);   // 事务尾：不再触碰 rec 秒桶
        clock[0] = 3_100_000_000L;                             // 推进触发末秒结算
        metrics.onSlotMessage(new byte[1]);
        List<String> lines = metrics.reportLines(4 * TEN_SECONDS);
        assertTrue(lines.get(2).contains("(1,000 rec/s,"),
                "rec 峰值应为最高单秒 1000 而非整事务落桶的 3000: " + lines.get(2));
        assertTrue(lines.get(0).contains("(75.0 rec/s,"),
                "累计不受逐条化影响（3000÷40s 窗口差分）: " + lines.get(0));
    }

    /**
     * onTxOutput 与 rec 秒桶隔离（同修正的另一半锚定）：仅调用 onTxOutput（无任何
     * onRecordDelivered）时 rec 峰值保持 n/a——修复前会显示事务的 emittedRecords
     * 整数（100 rec/s），露馅即回归。
     */
    @Test
    void peakLineOnTxOutputDoesNotTouchRecBucket() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        metrics.onTxOutput(1_000_000L, 100L, 100L);
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertTrue(lines.get(2).contains("(n/a rec/s,"), "无逐条交付时 rec 峰值应为 n/a: " + lines.get(2));
    }

    /**
     * rec 悬空桶下界（与 slot 侧同语义）：最后一秒的逐条交付未结算时，当前桶计数作
     * 该秒速率的下界候选——不因秒未走满而丢失，也不会高估。
     */
    @Test
    void peakLineRecDanglingBucketCountsAsLowerBound() {
        long[] clock = {300_000_000L};                  // 0.3s（秒未走满）
        ThroughputMetrics metrics = new ThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 300; i++) {
            metrics.onRecordDelivered();
        }
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertTrue(lines.get(2).contains("(300 rec/s,"), "悬空桶 300 条应作峰值候选: " + lines.get(2));
    }
}
