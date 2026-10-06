package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PG 风格浮点格式化器的离线单测（Task 9）——期望值全部 docker 实测钉死
 * （2026-10-07，src/docker PG 18.6 容器，{@code psql -t -A -c "SELECT v::float8::text"}，
 * 双精度/单精度各一组），规则面（定点/科学计数法门限与指数形态）由
 * REL_18_STABLE 源码钉：
 *
 * <ul>
 *   <li>{@code src/common/d2s.c} {@code to_chars}：display exponent
 *       {@code exp >= -4 && exp < 15} 走定点（注释原话 "thresholds ... match printf
 *       defaults"）；科学计数法形态 {@code d[.ddd]e±XX}——指数恒带符号、至少两位
 *       （≥100 三位自然宽，如 {@code 1e+308}/{@code 5e-324}）</li>
 *   <li>{@code src/common/f2s.c} {@code to_chars}：float4 门限 {@code exp >= -4 && exp < 6}
 *       （故 {@code 1e6::float4} 即科学计数法 {@code 1e+06}，而 float8 的
 *       {@code 1000000} 仍是定点）</li>
 *   <li>数字位串是最短往返表示（Ryū）；零打 {@code 0}、负零打 {@code -0}；
 *       NaN/±Infinity 打 {@code NaN}/{@code Infinity}/{@code -Infinity}</li>
 * </ul>
 *
 * <p>被测面是双路对拍（Task 9 场景 2）的值对齐前提：engine 逻辑解码 text 档的浮点
 * 值由服务端 float8out/float4out 渲染（即上述格式），wal 直解路经本格式化器在 Java
 * 侧复刻同一形态。</p>
 */
class PgFloatFormatTest {

    // ---- float8：docker 实测锚（科学计数法区段）----

    /** float8 科学计数法区段：指数恒带符号两位（含负指数零填充）、尾数最短往返。 */
    @Test
    void float8ScientificRegionMatchesPgText() {
        assertEquals("1e+20", PgFloatFormat.format(1e20d));
        assertEquals("4.5e-08", PgFloatFormat.format(4.5e-8d));
        assertEquals("-2.5e+15", PgFloatFormat.format(-2.5e15d));
        assertEquals("1e+15", PgFloatFormat.format(1e15d));
        assertEquals("1e-05", PgFloatFormat.format(1e-5d));
        assertEquals("1.5e-05", PgFloatFormat.format(1.5e-5d));
        assertEquals("6.02214076e+23", PgFloatFormat.format(6.02214076e23d));
        // 指数 ≥100：三位自然宽（d2s.c 打指数的 memcpy 分支）
        assertEquals("1e+100", PgFloatFormat.format(1e100d));
        assertEquals("1e+308", PgFloatFormat.format(1e308d));
        assertEquals("-1.7976931348623157e+308", PgFloatFormat.format(-1.7976931348623157e308d));
        // 最小正规 / 最小次正规（17 位尾数与 1 位尾数两端）
        assertEquals("2.2250738585072014e-308", PgFloatFormat.format(2.2250738585072014e-308d));
        assertEquals("5e-324", PgFloatFormat.format(5e-324d));
        // 次正规中段（最短往返 1 位）
        assertEquals("1e-320", PgFloatFormat.format(1e-320d));
    }

    /** float8 定点区段门限 [-4, 15)：两侧边界值各钉一锚（1e-4 定点、1e-5 科学；1e14 定点、1e15 科学）。 */
    @Test
    void float8FixedRegionThresholdsMatchPrintfDefaults() {
        assertEquals("0.0001", PgFloatFormat.format(1e-4d));
        assertEquals("0.001", PgFloatFormat.format(1e-3d));
        assertEquals("100000000000000", PgFloatFormat.format(1e14d));
        assertEquals("123456789012345", PgFloatFormat.format(123456789012345d));
        assertEquals("999999999999999.9", PgFloatFormat.format(999999999999999.9d));
        assertEquals("1234567.891", PgFloatFormat.format(1234567.891d));
    }

    /** float8 常规小数与整数值形态：无尾随 {@code .0}（{@code 1::float8} 打 {@code 1} 非java 的 {@code 1.0}）。 */
    @Test
    void float8PlainValuesHaveNoTrailingDotZero() {
        assertEquals("0.1", PgFloatFormat.format(0.1d));
        assertEquals("3.14159", PgFloatFormat.format(3.14159d));
        assertEquals("1.5", PgFloatFormat.format(1.5d));
        assertEquals("2.5", PgFloatFormat.format(2.5d));
        assertEquals("-1.5", PgFloatFormat.format(-1.5d));
        assertEquals("1", PgFloatFormat.format(1d));
        assertEquals("10", PgFloatFormat.format(10d));
        assertEquals("100", PgFloatFormat.format(100d));
        assertEquals("9.99", PgFloatFormat.format(9.99d));
        // 17 位最短往返的科学计数法形态（首段补零无、尾串 7 收口）
        assertEquals("0.00012345678901234567", PgFloatFormat.format(0.000123456789012345678d));
    }

    /** float8 零/负零/NaN/±Infinity：PG 逐字输出（负零保留符号位）。 */
    @Test
    void float8ZeroAndSpecialValuesMatchPgText() {
        assertEquals("0", PgFloatFormat.format(0d));
        assertEquals("-0", PgFloatFormat.format(-0d));
        assertEquals("NaN", PgFloatFormat.format(Double.NaN));
        assertEquals("Infinity", PgFloatFormat.format(Double.POSITIVE_INFINITY));
        assertEquals("-Infinity", PgFloatFormat.format(Double.NEGATIVE_INFINITY));
    }

    // ---- float4：docker 实测锚 ----

    /** float4 定点门限 [-4, 6) 比 float8 低一档：{@code 1e6::float4} 即 {@code 1e+06}。 */
    @Test
    void float4FixedRegionThresholdIsExpBelowSix() {
        assertEquals("999999", PgFloatFormat.format(999999f));
        assertEquals("123456", PgFloatFormat.format(123456f));
        assertEquals("150000", PgFloatFormat.format(1.5e5f));
        assertEquals("1e+06", PgFloatFormat.format(1e6f));
        assertEquals("0.0001", PgFloatFormat.format(1e-4f));
        assertEquals("1e-05", PgFloatFormat.format(1e-5f));
        assertEquals("1.5e-05", PgFloatFormat.format(1.5e-5f));
    }

    /** float4 精度边界：最大值/最小正规/最小次正规的最短往返形态（docker 实测）。 */
    @Test
    void float4PrecisionExtremesMatchPgText() {
        assertEquals("3.4028235e+38", PgFloatFormat.format(3.4028235e38f));
        assertEquals("1.1754944e-38", PgFloatFormat.format(1.17549435e-38f));
        assertEquals("1e-45", PgFloatFormat.format(1e-45f));
        assertEquals("1e+20", PgFloatFormat.format(1e20f));
        assertEquals("4.5e-08", PgFloatFormat.format(4.5e-8f));
    }

    /** float4 常规值/零/负零/NaN/±Infinity 与 PG 逐字一致。 */
    @Test
    void float4PlainAndSpecialValuesMatchPgText() {
        assertEquals("0.1", PgFloatFormat.format(0.1f));
        assertEquals("3.14159", PgFloatFormat.format(3.14159f));
        assertEquals("123.456", PgFloatFormat.format(123.456f));
        assertEquals("1.5", PgFloatFormat.format(1.5f));
        assertEquals("1", PgFloatFormat.format(1f));
        assertEquals("0", PgFloatFormat.format(0f));
        assertEquals("-0", PgFloatFormat.format(-0f));
        assertEquals("NaN", PgFloatFormat.format(Float.NaN));
        assertEquals("Infinity", PgFloatFormat.format(Float.POSITIVE_INFINITY));
        assertEquals("-Infinity", PgFloatFormat.format(Float.NEGATIVE_INFINITY));
    }
}
