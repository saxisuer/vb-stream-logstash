package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ThroughputMetrics 单测（2026-08-31 吞吐指标设计）：覆盖三块行为——
 * ①单位格式化纯函数（字节速率 SI 千进位一位小数、耗时 ns→µs→ms→s 进位与 ≥100 取整阈值、
 * 计数速率 &lt;100 一位小数/≥100 整数千分位）；②速率报告的窗口差分语义（计数 delta ÷ 实际流逝
 * 秒数，第二窗口无事件则归零——计数器累计、速率只看窗口）；③分位数报告的区间隔离语义
 * （SingleWriterRecorder 每次报告取走上一区间，窗口外样本不稀释当前值；零样本 n/a；
 * 越界值（超过可追踪上界）钳制到上界不向调用方抛异常）。
 *
 * <p>夹具约定：全部用例经 {@code new ThroughputMetrics(基准戳)} 注入受控时钟（0 基准 +
 * 显式 nowNanos 报告），不依赖真实睡眠；报告行断言**整行字符串相等**（格式即契约——与
 * consumer 统计行同风格，改动输出格式必须连带改这里）。
 */
class ThroughputMetricsTest {

    /** 常用报告窗口：10s（与 TransactionConsumer 的统计周期同档）。 */
    private static final long TEN_SECONDS = 10_000_000_000L;

    /**
     * 字节速率格式化：SI 十进制千进位，恒一位小数——0 与不足 1000 的值留在 B/s 档，
     * 每满 1000 进一位档（KB/MB/GB）。
     */
    @Test
    void 字节速率格式化_SI千进位恒一位小数() {
        assertEquals("0.0 B/s", ThroughputMetrics.formatBytesPerSec(0));
        assertEquals("512.0 B/s", ThroughputMetrics.formatBytesPerSec(512));
        assertEquals("982.4 B/s", ThroughputMetrics.formatBytesPerSec(982.4));
        assertEquals("10.0 KB/s", ThroughputMetrics.formatBytesPerSec(10_000));
        assertEquals("12.4 MB/s", ThroughputMetrics.formatBytesPerSec(12_400_000));
        assertEquals("2.0 GB/s", ThroughputMetrics.formatBytesPerSec(2_000_000_000));
    }

    /**
     * 耗时格式化：ns→µs→ms→s 千进位；同档内值 &lt;100 保留一位小数、≥100 取整
     * （3.2ms 与 125ms 的观感一致性——大值看数量级、小值看精度）。
     */
    @Test
    void 耗时格式化_单位进位与整数阈值() {
        assertEquals("400ns", ThroughputMetrics.formatNanos(400));
        assertEquals("852µs", ThroughputMetrics.formatNanos(852_000));
        assertEquals("3.2ms", ThroughputMetrics.formatNanos(3_200_000));
        assertEquals("125ms", ThroughputMetrics.formatNanos(125_000_000));
        assertEquals("1.4s", ThroughputMetrics.formatNanos(1_400_000_000));
    }

    /**
     * 计数速率（msg/rec/tx 每秒）格式化：&lt;100 一位小数（"5.0"），≥100 整数千分位
     * （"100"、"12,346"）——与耗时同阈值规则，报告行内各数读感一致。
     */
    @Test
    void 计数速率格式化_小数与整数两档() {
        assertEquals("4.5", ThroughputMetrics.formatCountPerSec(4.5));
        assertEquals("5.0", ThroughputMetrics.formatCountPerSec(5));
        assertEquals("100", ThroughputMetrics.formatCountPerSec(100));
        assertEquals("12,346", ThroughputMetrics.formatCountPerSec(12_345.6));
    }

    /**
     * 速率报告的核心语义：六项速率全部按"窗口内计数 delta ÷ 实际流逝秒数"计算——
     * 驱动一组已知事件后断言吞吐行**整行相等**；同一实例第二窗口无事件则全零。
     * 计数器本身累计（totals() 不清零），报告行只反映窗口。
     */
    @Test
    void 速率报告_按窗口差分计算且空窗归零() {
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
     * 分位数报告的**区间隔离**：每次报告取走上一区间的样本（Recorder 语义），首窗样本
     * 不得稀释次窗——第二窗口单独一个 5ms/100rec 样本时，p90/p95/max 全部精确等于该样本。
     * 单值窗口的分位数恒等于该值（无插值歧义），断言可用整行相等。
     */
    @Test
    void 分布报告_窗口隔离首窗样本不进次窗() {
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
     * 越界钳制：回放耗时超过可追踪上界（1h）、事务大小超过上界（10 亿单元）时，样本钳制到
     * 上界入分布——reportLines 不抛异常，max 落在上界附近。断言用 ±2% 容差而非精确相等：
     * HDR 按 2 位有效数字做**桶级量化**（记录值本身被舍到最近可表示桶，读回可略越上界，
     * 如 3600s → 3608s），这是数据结构的文档化行为，不是钳制失灵。
     * 这是热路径防御：指标永不向业务路径抛异常。
     */
    @Test
    void 分布报告_越界样本钳制到上界不抛() {
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
     * totals() 六计数累计语义：跨报告窗口不清零，供接线测试断言组装器→消费器全链路的
     * 插桩正确性（每处单行挂点漏挂即在此露馅）。
     */
    @Test
    void totals_跨窗口累计不清零() {
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
     * 峰值行首窗语义（2026-08-31 峰值 spec §2）：从未有过任何记录时八项全部 n/a——
     * 速率峰值在首个非零窗口出现前为无记录（空窗的零速率不构成峰值），分布峰值沿用
     * 零样本语义。整行断言（格式即契约）。
     */
    @Test
    void 峰值行_首窗无记录八项全n_a() {
        ThroughputMetrics metrics = new ThroughputMetrics(0L);
        List<String> lines = metrics.reportLines(TEN_SECONDS);
        assertEquals("峰值: slot=n/a (n/a msg/s) | 组装=n/a tx/s | 输出=n/a (n/a rec/s, n/a tx/s) | 耗时=n/a | 大小=n/a",
                lines.get(2));
    }

    /**
     * 峰值行的存在理由（spec §1）：高窗之后空窗，吞吐行归零、分布行变 n/a，峰值行完整
     * 保留高窗的八项——峰值不随窗口翻页消失。高窗事件组与"速率报告"用例同构（速率期望
     * 值可交叉核对），空窗后整行断言八项留存。
     */
    @Test
    void 峰值行_高窗后空窗八项留存() {
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
        metrics.reportLines(TEN_SECONDS);               // 高窗
        List<String> idle = metrics.reportLines(2 * TEN_SECONDS);   // 空窗：吞吐归零、分布 n/a
        assertEquals("吞吐: slot=0.0 B/s (0.0 msg/s) | 组装=0.0 tx/s | 输出=0.0 B/s (0.0 rec/s, 0.0 tx/s)",
                idle.get(0));
        assertEquals("分布: 回放耗时 n/a | 事务大小 n/a", idle.get(1));
        assertEquals("峰值: slot=10.0 KB/s (100 msg/s) | 组装=5.0 tx/s | 输出=500.0 B/s (5.0 rec/s, 1.0 tx/s) | 耗时=1.0ms | 大小=5 rec",
                idle.get(2));
    }

    /**
     * 峰值与当前窗口的对照（spec §2 #7/#8）：次窗更小的样本使分布行回落（区间隔离），
     * 峰值行仍取会话最高——两个时点的对照即"窗口报告"与"会话峰值"的语义分界。
     */
    @Test
    void 峰值行_分布max会话留存取最高() {
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
}
