package org.vastdata.vbstream.walsource.changes;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 磁盘格式值 → PG text 渲染矩阵的离线单测——17 类型 + NULL + dropped 逐一断言 text 形态。
 *
 * <p><b>期望值全部实测钉死</b>（2026-10-07，src/docker PG 18.6 容器，TimeZone=GMT，
 * {@code psql -t -A -c "SELECT ...::text"}），样本与断言逐字一致：</p>
 * <table border="1">
 *   <caption>docker 实测样本表（渲染期望的唯一来源）</caption>
 *   <tr><th>查询</th><th>实测输出</th></tr>
 *   <tr><td>{@code '2026-10-06 12:34:56.123456'::timestamp::text}</td><td>{@code 2026-10-06 12:34:56.123456}</td></tr>
 *   <tr><td>{@code '2026-10-06 12:34:56.100000'::timestamp::text}</td><td>{@code 2026-10-06 12:34:56.1}（尾零截断）</td></tr>
 *   <tr><td>{@code '2026-10-06 12:34:56'::timestamp::text}</td><td>{@code 2026-10-06 12:34:56}</td></tr>
 *   <tr><td>{@code '2026-10-06 12:34'::timestamp::text}</td><td>{@code 2026-10-06 12:34:00}（秒为 0 不省略）</td></tr>
 *   <tr><td>{@code '2026-10-06 12:34:56.123456+00'::timestamptz::text}</td><td>{@code 2026-10-06 12:34:56.123456+00}</td></tr>
 *   <tr><td>{@code '2026-10-06 12:34:56.123456+08'::timestamptz::text}</td><td>{@code 2026-10-06 04:34:56.123456+00}（按会话时区 GMT 渲染）</td></tr>
 *   <tr><td>{@code '12:34:56.123456'::time::text}</td><td>{@code 12:34:56.123456}</td></tr>
 *   <tr><td>{@code '12:34:56.100000'::time::text}</td><td>{@code 12:34:56.1}（尾零截断——任务书"尾零不截?"疑点实测裁定：<b>截</b>）</td></tr>
 *   <tr><td>{@code '2026-10-06'::date::text} / {@code '2000-01-01'} / {@code '1999-12-31'}</td><td>{@code 2026-10-06} / {@code 2000-01-01} / {@code 1999-12-31}</td></tr>
 *   <tr><td>{@code '-123.456'::numeric::text}</td><td>{@code -123.456}</td></tr>
 *   <tr><td>{@code '1.10'::numeric::text}</td><td>{@code 1.10}（dscale=2 保尾零）</td></tr>
 *   <tr><td>{@code '1e20'::numeric::text}</td><td>{@code 100000000000000000000}</td></tr>
 *   <tr><td>{@code '1E-70'::numeric::text}</td><td>{@code 0.} + 69 个 0 + {@code 1}（dscale=70 &gt; 63 → 磁盘必为 long 格式）</td></tr>
 *   <tr><td>{@code 'NaN'/'Infinity'/'-Infinity'::numeric::text}</td><td>{@code NaN} / {@code Infinity} / {@code -Infinity}</td></tr>
 *   <tr><td>{@code 'NaN'/'Infinity'/'-Infinity'::float8::text}</td><td>{@code NaN} / {@code Infinity} / {@code -Infinity}</td></tr>
 *   <tr><td>{@code '1.5'::float8::text} / {@code '1.5'::float4::text}</td><td>{@code 1.5} / {@code 1.5}</td></tr>
 *   <tr><td>{@code '1e20'::float8::text}</td><td>{@code 1e+20}（<b>已知分叉</b>：Java 最短表示 {@code 1.0E20}，与 engine BinaryValueDecoder 同一取舍）</td></tr>
 *   <tr><td>{@code decode('00ff','hex')::text} / 空 bytea</td><td>{@code \x00ff} / {@code \x}</td></tr>
 *   <tr><td>{@code 't'::bool::text}（cast 形态）</td><td>{@code true}——<b>bool_out（pgoutput text 模式）是 {@code t}</b>，渲染取后者对齐 engine</td></tr>
 *   <tr><td>{@code 'a0b1c2d3-e4f5-6789-abcd-ef0123456789'::uuid::text}</td><td>原样小写连字符</td></tr>
 *   <tr><td>int2/int8 边界</td><td>{@code -32768} / {@code -9223372036854775808}</td></tr>
 * </table>
 *
 * <p>磁盘字节的手造推导依据（numeric）：REL_18_STABLE {@code numeric.c} L104-253 格式注释 +
 * {@code make_result} L7984-8008——首 u16 flag 位（&amp;0xC000）：0x8000=短格式、0x0000/0x4000=
 * 长格式、0xC000=特殊值（0xC000 NaN / 0xD000 +Inf / 0xF000 -Inf）；短格式 sign=0x2000、
 * dscale=(h&amp;0x1F80)&gt;&gt;7、weight=7 位二补码（0x0040 符号扩展）；长格式 u16 sign_dscale
 * （高 2 位符号 + 低 14 位 dscale）+ i16 weight；digits 逐 i16 小端 base-10000、首尾零组剥除
 * （零值无 digits）。其余类型磁盘布局：date=i32 天（epoch 2000-01-01）、time/timestamp/
 * timestamptz=i64 微秒（tz 为 UTC 绝对时刻）、uuid=16 字节、全部小端。</p>
 */
class DiskValueRendererTest {

    /** PG catalog oid（与被测实现的分派键一致；pg_type.dat 硬编码）。 */
    private static final int OID_BOOL = 16;
    private static final int OID_BYTEA = 17;
    private static final int OID_INT8 = 20;
    private static final int OID_INT2 = 21;
    private static final int OID_INT4 = 23;
    private static final int OID_TEXT = 25;
    private static final int OID_OID = 26;
    private static final int OID_FLOAT4 = 700;
    private static final int OID_FLOAT8 = 701;
    private static final int OID_BPCHAR = 1042;
    private static final int OID_VARCHAR = 1043;
    private static final int OID_DATE = 1082;
    private static final int OID_TIME = 1083;
    private static final int OID_TIMESTAMP = 1114;
    private static final int OID_TIMESTAMPTZ = 1184;
    private static final int OID_NUMERIC = 1700;
    private static final int OID_UUID = 2950;

    private ListAppender<ILoggingEvent> appender;
    private Logger rendererLogger;

    /**
     * 挂 list appender 到被测类 logger——WARN 一次语义的断言面。
     */
    @BeforeEach
    void attachAppender() {
        rendererLogger = (Logger) LoggerFactory.getLogger(DiskValueRenderer.class);
        appender = new ListAppender<>();
        appender.start();
        rendererLogger.addAppender(appender);
    }

    /**
     * 摘除 appender——防泄漏到其他测试类。
     */
    @AfterEach
    void detachAppender() {
        rendererLogger.detachAppender(appender);
    }

    // ---- 布尔 ----

    /**
     * bool 渲染为 pgoutput text 模式（bool_out）的 {@code t}/{@code f} 单字母形态——
     * 非 {@code ::text} cast 的 {@code true}（两形态差异见类 javadoc 样本表）。
     */
    @Test
    void boolRendersPgOutLetterForm() {
        assertEquals("t", DiskValueRenderer.render(Boolean.TRUE, OID_BOOL));
        assertEquals("f", DiskValueRenderer.render(Boolean.FALSE, OID_BOOL));
        assertEquals("t", DiskValueRenderer.render(new byte[]{1}, OID_BOOL));
        assertEquals("f", DiskValueRenderer.render(new byte[]{0}, OID_BOOL));
    }

    // ---- 整数族 ----

    /**
     * int2/int4/int8/oid 十进制渲染——含负数与两侧边界（oid 为 u32 无符号语义，
     * TupleDecoder 已折算 Long）。
     */
    @Test
    void intFamilyRendersDecimalWithBoundaries() {
        assertEquals("-32768", DiskValueRenderer.render((short) -32768, OID_INT2));
        assertEquals("42", DiskValueRenderer.render(42, OID_INT4));
        assertEquals("-9223372036854775808", DiskValueRenderer.render(Long.MIN_VALUE, OID_INT8));
        assertEquals("4294967295", DiskValueRenderer.render(4294967295L, OID_OID));
    }

    // ---- 浮点族 ----

    /**
     * float4/float8 用 Java 最短表示——普通值与 PG 逐字一致（1.5/NaN/Infinity 实测钉），
     * 科学计数法区段有已知格式差（PG {@code 1e+20} vs Java {@code 1.0E20}，与 engine
     * BinaryValueDecoder 同一取舍，见类 javadoc 样本表）。
     */
    @Test
    void floatFamilyRendersJavaShortestForm() {
        assertEquals("1.5", DiskValueRenderer.render(1.5f, OID_FLOAT4));
        assertEquals("NaN", DiskValueRenderer.render(Float.NaN, OID_FLOAT4));
        assertEquals("1.5", DiskValueRenderer.render(1.5d, OID_FLOAT8));
        assertEquals("-1.5", DiskValueRenderer.render(-1.5d, OID_FLOAT8));
        assertEquals("NaN", DiskValueRenderer.render(Double.NaN, OID_FLOAT8));
        assertEquals("Infinity", DiskValueRenderer.render(Double.POSITIVE_INFINITY, OID_FLOAT8));
        assertEquals("-Infinity", DiskValueRenderer.render(Double.NEGATIVE_INFINITY, OID_FLOAT8));
        assertEquals("1.0E20", DiskValueRenderer.render(1e20d, OID_FLOAT8));
    }

    // ---- 文本族与 bytea ----

    /**
     * text/varchar/bpchar 原文透传（UTF-8 π 与空串在内）；bytea 渲染
     * {@code \x} + 小写十六进制（docker 实测 {@code \x00ff} / 空 {@code \x}）。
     */
    @Test
    void textFamilyPassesThroughAndByteaRendersLowercaseHex() {
        assertEquals("hello π", DiskValueRenderer.render("hello π", OID_TEXT));
        assertEquals("", DiskValueRenderer.render("", OID_VARCHAR));
        assertEquals("  padded  ", DiskValueRenderer.render("  padded  ", OID_BPCHAR));
        assertEquals("varchar via bytes", DiskValueRenderer.render("varchar via bytes".getBytes(), OID_VARCHAR));
        assertEquals("\\x00ff", DiskValueRenderer.render(new byte[]{0x00, (byte) 0xFF}, OID_BYTEA));
        assertEquals("\\x", DiskValueRenderer.render(new byte[0], OID_BYTEA));
        assertEquals("\\xab", DiskValueRenderer.render(new byte[]{(byte) 0xAB}, OID_BYTEA));
    }

    // ---- 日期 ----

    /**
     * date：i32 天数（epoch 2000-01-01，小端）→ ISO 日期——2026-10-06 样本、纪元日与
     * 纪元前一日（负天数路径）三锚，期望值 docker 实测钉。
     */
    @Test
    void dateRendersIsoFromEpoch2000Days() {
        assertEquals("2026-10-06", DiskValueRenderer.render(daysSinceEpoch2000(LocalDate.of(2026, 10, 6)), OID_DATE));
        assertEquals("2000-01-01", DiskValueRenderer.render(i32le(0), OID_DATE));
        assertEquals("1999-12-31", DiskValueRenderer.render(i32le(-1), OID_DATE));
        assertThrows(IllegalStateException.class, () -> DiskValueRenderer.render(new byte[3], OID_DATE));
    }

    // ---- 时间 ----

    /**
     * time：i64 微秒（小端）→ PG 文本形态——秒恒两位（0 不省略）、微秒去尾零
     * （{@code .100000} → {@code .1}，任务书"尾零不截?"疑点的实测裁定）。
     */
    @Test
    void timeRendersPgFormWithTrimmedFraction() {
        assertEquals("12:34:56.123456", DiskValueRenderer.render(timeMicros(LocalTime.of(12, 34, 56, 123_456_000)), OID_TIME));
        assertEquals("12:34:56.1", DiskValueRenderer.render(timeMicros(LocalTime.of(12, 34, 56, 100_000_000)), OID_TIME));
        assertEquals("12:34:56", DiskValueRenderer.render(timeMicros(LocalTime.of(12, 34, 56)), OID_TIME));
        assertEquals("12:34:00", DiskValueRenderer.render(timeMicros(LocalTime.of(12, 34, 0)), OID_TIME));
    }

    // ---- timestamp（无时区）----

    /**
     * timestamp 磁盘路径：i64 微秒（epoch 2000，小端）→ {@code yyyy-MM-dd HH:mm:ss[.frac]}，
     * 尾零截断、秒不省略——四样本 docker 实测钉。
     */
    @Test
    void timestampFromDiskMicrosRendersPgText() {
        assertEquals("2026-10-06 12:34:56.123456",
                DiskValueRenderer.render(tsMicros(LocalDateTime.of(2026, 10, 6, 12, 34, 56, 123_456_000)), OID_TIMESTAMP));
        assertEquals("2026-10-06 12:34:56.1",
                DiskValueRenderer.render(tsMicros(LocalDateTime.of(2026, 10, 6, 12, 34, 56, 100_000_000)), OID_TIMESTAMP));
        assertEquals("2026-10-06 12:34:56",
                DiskValueRenderer.render(tsMicros(LocalDateTime.of(2026, 10, 6, 12, 34, 56)), OID_TIMESTAMP));
        assertEquals("2026-10-06 12:34:00",
                DiskValueRenderer.render(tsMicros(LocalDateTime.of(2026, 10, 6, 12, 34, 0)), OID_TIMESTAMP));
    }

    /**
     * timestamp 的 v1 TupleDecoder 路径：输入是 TupleDecoder 已产出的 ISO 字符串
     * （{@code LocalDateTime.toString()} 形态，"T" 分隔、秒/小数可省略）——渲染须无损重排为
     * PG 文本形态（ISO 的 "2026-10-06T12:34" 不丢秒）。
     */
    @Test
    void timestampFromTupleDecoderIsoStringRendersPgText() {
        assertEquals("2026-10-06 12:34:56.123456",
                DiskValueRenderer.render("2026-10-06T12:34:56.123456", OID_TIMESTAMP));
        assertEquals("2026-10-06 12:34:00",
                DiskValueRenderer.render("2026-10-06T12:34", OID_TIMESTAMP));
    }

    // ---- timestamptz ----

    /**
     * timestamptz：i64 微秒（UTC 绝对时刻，epoch 2000，小端）→ 按 JVM 默认时区渲染并带
     * 数字偏移后缀——对齐 engine BinaryValueDecoder 的行为（pgjdbc 把复制会话时区设为
     * JVM 默认，双路对拍的 engine 侧 text 输出即该时区）。期望经独立 java.time oracle
     * （DateTimeFormatter，不经被测代码）推导；GMT 会话下的容器实测样本（javadoc 样本表）
     * 在 JVM 默认时区恰为 GMT 时与断言逐字一致：{@code 2026-10-06 12:34:56.123456+00}。
     */
    @Test
    void timestamptzRendersAtJvmDefaultZoneWithNumericOffset() {
        Instant inst = LocalDateTime.of(2026, 10, 6, 12, 34, 56, 123_456_000).toInstant(ZoneOffset.UTC);
        assertEquals(expectedTzText(inst), DiskValueRenderer.render(tsMicrosUtc(inst), OID_TIMESTAMPTZ));
        // +08 输入的同一时刻（容器 GMT 渲染为 04:34:56.123456+00）
        Instant shifted = LocalDateTime.of(2026, 10, 6, 4, 34, 56, 123_456_000).toInstant(ZoneOffset.UTC);
        assertEquals(expectedTzText(shifted), DiskValueRenderer.render(tsMicrosUtc(shifted), OID_TIMESTAMPTZ));
    }

    // ---- numeric ----

    /**
     * numeric 短格式（首 u16 flag=0x8000）：-123.456 / 0 / 1.10 / 1e20 四值手造磁盘字节
     * （推导依据见类 javadoc）——期望值全部 docker 实测钉。
     */
    @Test
    void numericShortFormatRendersLosslessDecimal() {
        // -123.456: NEG|dscale3<<7|weight0 = 0xA180；digits [123, 4560]
        assertEquals("-123.456", DiskValueRenderer.render(leShorts(0xA180, 123, 4560), OID_NUMERIC));
        // 零值：无 digits，dscale 0 → 头恰 0x8000
        assertEquals("0", DiskValueRenderer.render(leShorts(0x8000), OID_NUMERIC));
        // 1.10: dscale=2 保尾零（digit[1]=1000 即 .1000 → dscale 截到 2 位）
        assertEquals("1.10", DiskValueRenderer.render(leShorts(0x8100, 1, 1000), OID_NUMERIC));
        // 1e20: weight=5、digits=[1]（尾零组已剥）
        assertEquals("100000000000000000000", DiskValueRenderer.render(leShorts(0x8005, 1), OID_NUMERIC));
    }

    /**
     * numeric 特殊值：2 字节头恰为 0xC000/0xD000/0xF000 → NaN/Infinity/-Infinity
     * （REL_18 numeric.c：NUMERIC_IS_NAN 等为整字相等判定）。
     */
    @Test
    void numericSpecialValuesRenderNamedConstants() {
        assertEquals("NaN", DiskValueRenderer.render(leShorts(0xC000), OID_NUMERIC));
        assertEquals("Infinity", DiskValueRenderer.render(leShorts(0xD000), OID_NUMERIC));
        assertEquals("-Infinity", DiskValueRenderer.render(leShorts(0xF000), OID_NUMERIC));
    }

    /**
     * numeric 长格式（首 u16 flag=0x0000/0x4000）：同值长格式字节（pg_upgrade 存量形态）
     * 与 dscale&gt;63 必然长格式的 1E-70（docker 实测 {@code 0.} + 69 零 + {@code 1}）。
     */
    @Test
    void numericLongFormatRendersSameValueAndDeepScale() {
        // -123.456 长格式：sign_dscale=0x4000|3、weight=0、digits [123, 4560]
        assertEquals("-123.456", DiskValueRenderer.render(leShorts(0x4003, 0, 123, 4560), OID_NUMERIC));
        // 1E-70 长格式：sign_dscale=70、weight=-18（0xFFEE）、digits=[100]
        assertEquals("0." + "0".repeat(69) + "1",
                DiskValueRenderer.render(leShorts(0x0046, 0xFFEE, 100), OID_NUMERIC));
    }

    /**
     * numeric 载荷长度不符（奇数字节 = digit 半截）→ ISE fail-fast。
     */
    @Test
    void numericOddPayloadThrowsIllegalState() {
        assertThrows(IllegalStateException.class,
                () -> DiskValueRenderer.render(new byte[]{0x00, (byte) 0x80, 0x01}, OID_NUMERIC));
    }

    // ---- uuid ----

    /**
     * uuid：16 字节 → 8-4-4-4-12 小写连字符（docker 实测原样）。
     */
    @Test
    void uuidRendersLowercaseHyphenated() {
        byte[] b = new byte[16];
        System.arraycopy(HexFormat.of().parseHex("a0b1c2d3e4f56789abcdef0123456789"), 0, b, 0, 16);
        assertEquals("a0b1c2d3-e4f5-6789-abcd-ef0123456789", DiskValueRenderer.render(b, OID_UUID));
        assertThrows(IllegalStateException.class, () -> DiskValueRenderer.render(new byte[15], OID_UUID));
    }

    // ---- NULL / dropped / 矩阵外 ----

    /**
     * NULL 值：render 返回 null（调用方按 NULL 语义消费）；renderColumn 渲染
     * {@code name=NULL} 字面（区别于空串 {@code name=}）；dropped 列恒
     * {@code name=∅}（值不消费）。
     */
    @Test
    void nullAndDroppedRenderPlaceholders() {
        assertNull(DiskValueRenderer.render(null, OID_TEXT));
        ColumnMeta col = new ColumnMeta(1, "c", OID_INT4, false);
        assertEquals("c=42", DiskValueRenderer.renderColumn(1, 42, col));
        assertEquals("c=NULL", DiskValueRenderer.renderColumn(1, null, col));
        ColumnMeta dropped = new ColumnMeta(2, "d", OID_INT4, true);
        assertEquals("d=∅", DiskValueRenderer.renderColumn(2, null, dropped));
        assertEquals("d=∅", DiskValueRenderer.renderColumn(2, 42, dropped));
    }

    /**
     * 矩阵外 oid：降级 {@code 0x} + 小写十六进制且**每 oid 仅 WARN 一次**——两次渲染
     * 断言恰一条 WARN（logger 类级，观测节流防大事务刷屏）。
     */
    @Test
    void unknownOidDegradesToHexAndWarnsOnce() {
        byte[] raw = {0x01, 0x02};
        assertEquals("0x0102", DiskValueRenderer.render(raw, 705));
        assertEquals("0x0102", DiskValueRenderer.render(raw, 705));
        List<ILoggingEvent> warns = appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN).toList();
        assertEquals(1, warns.size(), "矩阵外 oid 两次渲染应恰 WARN 一次，实际 " + warns.size());
    }

    // ---- 手造字节辅助（小端，对齐 TupleDecoder 的读取风格）----

    /**
     * 把一串 u16 值按小端拼成磁盘字节序列——numeric 头与 digits 的手造面。
     *
     * @param words u16 值序列（numeric 头在前、digits 依序）
     * @return 小端拼接的字节
     */
    private static byte[] leShorts(int... words) {
        byte[] out = new byte[words.length * 2];
        for (int i = 0; i < words.length; i++) {
            out[i * 2] = (byte) (words[i] & 0xFF);
            out[i * 2 + 1] = (byte) ((words[i] >> 8) & 0xFF);
        }
        return out;
    }

    /**
     * i32 小端字节（date 磁盘 datum）。
     *
     * @param v 天数（epoch 2000-01-01 起，可为负）
     * @return 4 字节小端
     */
    private static byte[] i32le(int v) {
        return new byte[]{(byte) v, (byte) (v >> 8), (byte) (v >> 16), (byte) (v >> 24)};
    }

    /**
     * i64 小端字节（time/timestamp/timestamptz 磁盘 datum）。
     *
     * @param v 微秒数
     * @return 8 字节小端
     */
    private static byte[] i64le(long v) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (v >> (8 * i));
        }
        return out;
    }

    /**
     * date 的磁盘天数换算：目标日期相对 2000-01-01 的天数（LocalDate 纪元差）。
     *
     * @param d 目标日期
     * @return 4 字节小端磁盘 datum
     */
    private static byte[] daysSinceEpoch2000(LocalDate d) {
        return i32le((int) (d.toEpochDay() - LocalDate.of(2000, 1, 1).toEpochDay()));
    }

    /**
     * time 的磁盘微秒换算：LocalTime 纳秒折微秒。
     *
     * @param t 目标时刻
     * @return 8 字节小端磁盘 datum
     */
    private static byte[] timeMicros(LocalTime t) {
        return i64le(t.toNanoOfDay() / 1_000);
    }

    /**
     * timestamp（墙钟语义）的磁盘微秒换算：目标本地时间视为 UTC 墙钟，距
     * 2000-01-01T00:00:00Z 的微秒数。
     *
     * @param ldt 目标本地时间
     * @return 8 字节小端磁盘 datum
     */
    private static byte[] tsMicros(LocalDateTime ldt) {
        return i64le(ChronoUnit.MICROS.between(LocalDateTime.of(2000, 1, 1, 0, 0), ldt));
    }

    /**
     * timestamptz 的磁盘微秒换算：绝对时刻距 2000-01-01T00:00:00Z 的微秒数。
     *
     * @param inst 目标绝对时刻
     * @return 8 字节小端磁盘 datum
     */
    private static byte[] tsMicrosUtc(Instant inst) {
        return i64le(ChronoUnit.MICROS.between(Instant.parse("2000-01-01T00:00:00Z"), inst));
    }

    /**
     * timestamptz 期望文本的独立 oracle：java.time DateTimeFormatter 按 JVM 默认时区
     * 排版（yyyy-MM-dd HH:mm:ss + 去尾零微秒 + 数字偏移；ZoneOffset 的 "Z" 特例翻成
     * PG 的 "+00"）——不经过被测代码的任何格式化路径。
     *
     * @param inst 目标绝对时刻
     * @return PG 文本形态期望值
     */
    private static String expectedTzText(Instant inst) {
        ZonedDateTime zdt = inst.atZone(ZoneId.systemDefault());
        String base = zdt.toLocalDate() + " "
                + DateTimeFormatter.ofPattern("HH:mm:ss").format(zdt.toLocalTime());
        int fracMicros = zdt.getNano() / 1_000;
        String frac = "";
        if (fracMicros != 0) {
            String digits = "%06d".formatted(fracMicros);
            int end = digits.length();
            while (end > 0 && digits.charAt(end - 1) == '0') {
                end--;
            }
            frac = "." + digits.substring(0, end);
        }
        ZoneOffset off = zdt.getOffset();
        String offText = off.getTotalSeconds() == 0 ? "+00" : off.toString();
        return base + frac + offText;
    }
}
