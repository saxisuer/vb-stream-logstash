package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * StreamThroughputMetrics 单测——引擎 ThroughputMetricsTest 的文字参照重写（口径与语义
 * 逐条同构，断言值不变；引擎版见 vb-stream-engine .../replication/ThroughputMetricsTest）：
 * 覆盖三块行为——①单位格式化纯函数（字节速率 SI 千进位一位小数、耗时 ns→µs→ms→s 进位
 * 与 ≥100 取整阈值、计数速率 &lt;100 一位小数/≥100 整数千分位）；②速率报告的窗口差分语义
 * （计数 delta ÷ 实际流逝秒数，第二窗口无事件则归零——计数器累计、速率只看窗口）；
 * ③分位数报告的区间隔离语义（SingleWriterRecorder 每次报告取走上一区间，窗口外样本
 * 不稀释当前值；零样本 n/a；越界值（超过可追踪上界）钳制到上界不向调用方抛异常）。
 *
 * <p>夹具约定：全部用例经 {@code new StreamThroughputMetrics(基准戳)} 注入受控时钟
 * （0 基准 + 显式 nowNanos 报告），不依赖真实睡眠；报告行断言**整行字符串相等**（格式即
 * 契约——与引擎 consumer 统计行同风格，改动输出格式必须连带改这里）。
 */
class StreamThroughputMetricsTest {

    /** 常用报告窗口：10s（与引擎 TransactionConsumer 的统计周期同档）。 */
    private static final long TEN_SECONDS = 10_000_000_000L;

    /**
     * 字节速率格式化：SI 十进制千进位，恒一位小数——0 与不足 1000 的值留在 B/s 档，
     * 每满 1000 进一位档（KB/MB/GB）。
     */
    @Test
    void 字节速率格式化_SI千进位恒一位小数() {
        assertEquals("0.0 B/s", StreamThroughputMetrics.formatBytesPerSec(0));
        assertEquals("512.0 B/s", StreamThroughputMetrics.formatBytesPerSec(512));
        assertEquals("982.4 B/s", StreamThroughputMetrics.formatBytesPerSec(982.4));
        assertEquals("10.0 KB/s", StreamThroughputMetrics.formatBytesPerSec(10_000));
        assertEquals("12.4 MB/s", StreamThroughputMetrics.formatBytesPerSec(12_400_000));
        assertEquals("2.0 GB/s", StreamThroughputMetrics.formatBytesPerSec(2_000_000_000));
    }

    /**
     * 耗时格式化：ns→µs→ms→s 千进位；同档内值 &lt;100 保留一位小数、≥100 取整
     * （3.2ms 与 125ms 的观感一致性——大值看数量级、小值看精度）。
     */
    @Test
    void 耗时格式化_单位进位与整数阈值() {
        assertEquals("400ns", StreamThroughputMetrics.formatNanos(400));
        assertEquals("852µs", StreamThroughputMetrics.formatNanos(852_000));
        assertEquals("3.2ms", StreamThroughputMetrics.formatNanos(3_200_000));
        assertEquals("125ms", StreamThroughputMetrics.formatNanos(125_000_000));
        assertEquals("1.4s", StreamThroughputMetrics.formatNanos(1_400_000_000));
    }

    /**
     * 计数速率（msg/rec/tx 每秒）格式化：&lt;100 一位小数（"5.0"），≥100 整数千分位
     * （"100"、"12,346"）——与耗时同阈值规则，报告行内各数读感一致。
     */
    @Test
    void 计数速率格式化_小数与整数两档() {
        assertEquals("4.5", StreamThroughputMetrics.formatCountPerSec(4.5));
        assertEquals("5.0", StreamThroughputMetrics.formatCountPerSec(5));
        assertEquals("100", StreamThroughputMetrics.formatCountPerSec(100));
        assertEquals("12,346", StreamThroughputMetrics.formatCountPerSec(12_345.6));
    }

    /**
     * 速率报告的核心语义：六项速率全部按"窗口内计数 delta ÷ 实际流逝秒数"计算——
     * 驱动一组已知事件后断言吞吐行**整行相等**；同一实例第二窗口无事件则全零。
     * 计数器本身累计（totals() 不清零），报告行只反映窗口。
     */
    @Test
    void 速率报告_按窗口差分计算且空窗归零() {
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L);
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
     * 分位数报告的**区间隔离**：每次报告取走上一区间的样本（Recorder 语义），首窗样本
     * 不得稀释次窗——第二窗口单独一个 5ms/100rec 样本时，p90/p95/max 全部精确等于该样本。
     * 单值窗口的分位数恒等于该值（无插值歧义），断言可用整行相等。
     */
    @Test
    void 分布报告_窗口隔离首窗样本不进次窗() {
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L);
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
     * 越界钳制：回放耗时超过可追踪上界（1h）、事务大小超过上界（10 亿单元）时，样本钳制到
     * 上界入分布——reportLines 不抛异常，max 落在上界附近。断言用 ±2% 容差而非精确相等：
     * HDR 按 2 位有效数字做**桶级量化**（记录值本身被舍到最近可表示桶，读回可略越上界，
     * 如 3600s → 3608s），这是数据结构的文档化行为，不是钳制失灵。
     * 这是热路径防御：指标永不向业务路径抛异常。
     */
    @Test
    void 分布报告_越界样本钳制到上界不抛() {
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L);
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
     * totals() 六计数累计语义：跨报告窗口不清零——本连接器侧它是 Task 4 MBean 窗口差分的
     * 只读读源（任意线程可读，LongAdder sum 快照线程安全）；每处埋点漏挂即在此露馅。
     */
    @Test
    void totals_跨窗口累计不清零() {
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L);
        metrics.onSlotMessage(new byte[7]);
        metrics.onSlotMessage(new byte[3]);
        metrics.onTxHandedOff();
        metrics.onReplayedUnit(9);
        metrics.onTxOutput(1_000L, 2L, 2L);
        metrics.reportLines(TEN_SECONDS);
        StreamThroughputMetrics.Totals t = metrics.totals();
        assertEquals(10, t.slotBytes());
        assertEquals(2, t.slotMessages());
        assertEquals(1, t.assembledTxs());
        assertEquals(9, t.outputBytes());
        assertEquals(2, t.outputRecords());
        assertEquals(1, t.outputTxs());
    }

    /**
     * 峰值行首窗语义（引擎峰值 spec §2 同构）：从未有过任何记录时八项全部 n/a——
     * 速率峰值在首个非零窗口出现前为无记录（空窗的零速率不构成峰值），分布峰值沿用
     * 零样本语义。整行断言（格式即契约）。
     */
    @Test
    void 峰值行_首窗无记录八项全n_a() {
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L);
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertEquals("峰值: slot=n/a (n/a msg/s) | 组装=n/a tx/s | 输出=n/a (n/a rec/s, n/a tx/s) | 耗时=n/a | 大小=n/a",
                lines.get(2));
    }

    /**
     * 峰值行的存在理由（引擎 spec §1 同构）：高窗之后空窗，吞吐行归零、分布行变 n/a，
     * 峰值行完整保留高窗的八项——峰值不随窗口翻页消失。峰值速率口径为**最高单秒**
     * （秒桶）：事件全部同秒灌入时秒峰值 = 灌入总量，空窗报告取悬空桶候选。
     */
    @Test
    void 峰值行_高窗后空窗八项留存() {
        long[] clock = {500_000_000L};                  // 固定 0.5s：全部事件钉在同一受控秒内，不依赖真实时钟
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 1000; i++) {
            metrics.onSlotMessage(new byte[100]);       // slot: 100,000 B / 1000 msg（同秒）
        }
        for (int i = 0; i < 50; i++) {
            metrics.onTxHandedOff();                    // 组装: 50 tx
            metrics.onReplayedUnit(100);                // 输出字节: 5,000 B
        }
        for (int i = 0; i < 10; i++) {
            metrics.onTxOutput(1_000_000L, 5L, 5L);     // 输出: 10 tx / 50 rec，样本 1ms/5rec
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
     * 峰值与当前窗口的对照（引擎 spec §2 #7/#8 同构）：次窗更小的样本使分布行回落
     * （区间隔离），峰值行仍取会话最高——两个时点的对照即"窗口报告"与"会话峰值"的语义分界。
     */
    @Test
    void 峰值行_分布max会话留存取最高() {
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L);
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
     * 秒桶核心语义（引擎秒桶 spec §4 同构）：突发不被窗口摊薄——受控时钟同秒灌 2000 条后
     * 推进一秒结算，峰值段如实反映 2,000 msg/s，而吞吐行仍按窗口均值显示摊薄值
     * （20.0 msg/s）——两行双语义的直接对照锚定（引擎 2026-08-31 WSL 基线实测暴露的失真场景）。
     */
    @Test
    void 峰值行_秒桶突发不摊薄() {
        long[] clock = {500_000_000L};                  // 从 0.5s 起步（首秒内）
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L, () -> clock[0]);
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
     * 悬空桶下界（引擎秒桶 spec §2 同构）：当前秒未结算（此后再无消息触发结算）时，
     * 报告取 max(已结算峰, 当前桶累计)——最后一秒的突发不因秒未走满而丢失；当前桶计数
     * 是该秒速率的下界，不会高估。
     */
    @Test
    void 峰值行_悬空桶计数作下界候选() {
        long[] clock = {300_000_000L};                  // 0.3s（秒未走满）
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L, () -> clock[0]);
        for (int i = 0; i < 300; i++) {
            metrics.onSlotMessage(new byte[10]);
        }
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertTrue(lines.get(2).contains("(300 msg/s)"), "悬空桶 300 条应作峰值候选: " + lines.get(2));
    }

    /**
     * 秒桶跨秒结算重置：两秒各灌不同量，峰值取最高单秒（100）而非两秒合计（130）。
     */
    @Test
    void 峰值行_跨秒结算峰值取最高单秒() {
        long[] clock = {0L};
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(0L, () -> clock[0]);
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
}
