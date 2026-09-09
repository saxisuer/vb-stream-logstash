package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * BinaryValueDecoder 类型矩阵字节级单测：手造各类型 typsend 格式字节断言解码值（与 text 模式输出
 * 对齐的可读形态），含 numeric 的 base-10000 边界（补零组/前导零组/特殊值/声明位数）与长度错位
 * fail-fast。OID 常量与格式的依据见被测类 javadoc（PG pg_type.dat / typsend 函数）。
 */
class BinaryValueDecoderTest {

    // ---- 字节构造辅助（big-endian，对齐 wire 格式）----

    /** 有符号/无符号字节序列构造：每项取低 8 位（允许测试里写 0xDE 等字面量）。 */
    private static byte[] bytes(int... unsignedBytes) {
        byte[] out = new byte[unsignedBytes.length];
        for (int i = 0; i < unsignedBytes.length; i++) {
            out[i] = (byte) unsignedBytes[i];
        }
        return out;
    }

    private static byte[] i16(int v) {
        return ByteBuffer.allocate(2).putShort((short) v).array();
    }

    private static byte[] i32(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    private static byte[] i64(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private static byte[] f32(float v) {
        return i32(Float.floatToIntBits(v));
    }

    private static byte[] f64(double v) {
        return i64(Double.doubleToLongBits(v));
    }

    /** numeric 头 + digit 数组拼装（sign 用被测类同源常量语义，0x0000 正/0x4000 负）。 */
    private static byte[] numeric(int sign, int weight, int dscale, int... digits) {
        ByteBuffer buf = ByteBuffer.allocate(8 + digits.length * 2);
        buf.putShort((short) digits.length).putShort((short) weight)
                .putShort((short) sign).putShort((short) dscale);
        for (int d : digits) {
            buf.putShort((short) d);
        }
        return buf.array();
    }

    /** LocalDateTime 相对 PG 纪元（2000-01-01T00:00:00Z）的微秒数（timestamp/timestamptz 载荷）。 */
    private static long microsSincePgEpoch(LocalDateTime ldt) {
        long secs = ldt.toEpochSecond(java.time.ZoneOffset.UTC) - 946684800L;
        return secs * 1_000_000L + ldt.getNano() / 1_000L;
    }

    /** Instant 相对 PG 纪元的微秒数（支持 2000 年以前的负值）。 */
    private static long microsSincePgEpoch(Instant t) {
        long secs = t.getEpochSecond() - 946684800L;
        return secs * 1_000_000L + t.getNano() / 1_000L;
    }

    // ---- 布尔与整数 ----

    @Test
    void boolDecodesTrueAndFalse() {
        assertEquals("t", BinaryValueDecoder.decode(16, bytes(1)));
        assertEquals("f", BinaryValueDecoder.decode(16, bytes(0)));
    }

    @Test
    void signedIntegersDecodeAcrossWidths() {
        assertEquals("32767", BinaryValueDecoder.decode(21, i16(32767)));
        assertEquals("-32768", BinaryValueDecoder.decode(21, i16(-32768)));
        assertEquals("2147483647", BinaryValueDecoder.decode(23, i32(Integer.MAX_VALUE)));
        assertEquals("-2147483648", BinaryValueDecoder.decode(23, i32(Integer.MIN_VALUE)));
        assertEquals("9223372036854775807", BinaryValueDecoder.decode(20, i64(Long.MAX_VALUE)));
        assertEquals("-1", BinaryValueDecoder.decode(20, i64(-1)));
    }

    @Test
    void unsignedIntAliasesDecodeAsPositive() {
        // oid/xid/cid 是 u32：全 1 位型按无符号解释为 4294967295（有符号会是 -1）
        assertEquals("4294967295", BinaryValueDecoder.decode(26, bytes(0xFF, 0xFF, 0xFF, 0xFF)));
        assertEquals("4294967295", BinaryValueDecoder.decode(28, bytes(0xFF, 0xFF, 0xFF, 0xFF)));
        assertEquals("4294967295", BinaryValueDecoder.decode(29, bytes(0xFF, 0xFF, 0xFF, 0xFF)));
    }

    @Test
    void floatsDecodeViaIeeeBits() {
        assertEquals("1.5", BinaryValueDecoder.decode(700, f32(1.5f)));
        assertEquals("2.718281828459045", BinaryValueDecoder.decode(701, f64(2.718281828459045)));
    }

    // ---- 文本与 bytea ----

    @Test
    void textFamilyDecodesUtf8() {
        assertEquals("hello text 世界", BinaryValueDecoder.decode(25, "hello text 世界".getBytes(StandardCharsets.UTF_8)));
        assertEquals("abc", BinaryValueDecoder.decode(1043, "abc".getBytes(StandardCharsets.UTF_8)));
        assertEquals("abc     ", BinaryValueDecoder.decode(1042, "abc     ".getBytes(StandardCharsets.UTF_8))); // bpchar 尾部填充保留
        assertEquals("{\"k\":1}", BinaryValueDecoder.decode(114, "{\"k\":1}".getBytes(StandardCharsets.UTF_8)));
        assertEquals("pg_class", BinaryValueDecoder.decode(19, "pg_class".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void byteaRendersPgHexStringFormat() {
        assertEquals("\\xdeadbeef", BinaryValueDecoder.decode(17, bytes(0xDE, 0xAD, 0xBE, 0xEF)));
        assertEquals("\\x", BinaryValueDecoder.decode(17, new byte[0]));
    }

    // ---- 日期时间 ----

    @Test
    void dateDecodesFromEpochDays() {
        assertEquals("2000-01-01", BinaryValueDecoder.decode(1082, i32(0)));          // 纪元本身
        assertEquals("1999-12-31", BinaryValueDecoder.decode(1082, i32(-1)));         // 纪元前（负天数）
        long days = LocalDate.of(2026, 9, 9).toEpochDay() - 10957L;
        assertEquals("2026-09-09", BinaryValueDecoder.decode(1082, i32((int) days)));
    }

    @Test
    void timeDecodesMicrosOfDay() {
        long micros = LocalTime.of(12, 34, 56, 789012 * 1000).toNanoOfDay() / 1_000L;
        assertEquals("12:34:56.789012", BinaryValueDecoder.decode(1083, i64(micros)));
        assertEquals("00:00:00", BinaryValueDecoder.decode(1083, i64(0)));            // 秒为 0 不省略（PG 形态）
        // 小数尾零裁剪：.789000 → ".789"
        long trailing = LocalTime.of(1, 2, 3, 789000 * 1000).toNanoOfDay() / 1_000L;
        assertEquals("01:02:03.789", BinaryValueDecoder.decode(1083, i64(trailing)));
        // 分钟级整点：恒带秒
        assertEquals("10:20:00", BinaryValueDecoder.decode(1083,
                i64(LocalTime.of(10, 20).toNanoOfDay() / 1_000L)));
    }

    @Test
    void timetzAppendsZoneOffset() {
        long micros = LocalTime.of(12, 34, 56).toNanoOfDay() / 1_000L;
        // timetz_send：i64 微秒 + i32 zone（西经为正：+08 存 -28800，共 12 字节）
        assertEquals("12:34:56+08", BinaryValueDecoder.decode(1266,
                ByteBuffer.allocate(12).putLong(micros).putInt((int) (-8 * 3600L)).array()));
        assertEquals("12:34:56+05:30", BinaryValueDecoder.decode(1266,
                ByteBuffer.allocate(12).putLong(micros).putInt((int) -(5 * 3600 + 1800)).array()));
        assertEquals("12:34:56-08", BinaryValueDecoder.decode(1266,
                ByteBuffer.allocate(12).putLong(micros).putInt((int) (8 * 3600L)).array()));
    }

    @Test
    void timestampDecodesWallClockFromUtcMicros() {
        LocalDateTime ldt = LocalDateTime.of(2026, 8, 27, 10, 20, 30, 123456 * 1000);
        assertEquals("2026-08-27 10:20:30.123456", BinaryValueDecoder.decode(1114, i64(microsSincePgEpoch(ldt))));
        // 纪元前（负微秒，2000 年以前）：floorDiv 语义正确换算
        LocalDateTime before = LocalDateTime.of(1999, 12, 31, 23, 59, 59);
        assertEquals("1999-12-31 23:59:59", BinaryValueDecoder.decode(1114, i64(microsSincePgEpoch(before))));
    }

    @Test
    void timestamptzRendersInSystemZone() {
        Instant instant = Instant.parse("2026-08-27T02:20:30.123456Z");
        String expected = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
                .toString().replace('T', ' ');
        assertEquals(expected, BinaryValueDecoder.decode(1184, i64(microsSincePgEpoch(instant))));
    }

    // ---- numeric（重点）----

    @Test
    void numericPlainDecimal() {
        // 123.45 = [123, 4500] weight=0 dscale=2
        assertEquals("123.45", BinaryValueDecoder.decode(1700, numeric(0x0000, 0, 2, 123, 4500)));
    }

    @Test
    void numericLeadingZeroFractionGroups() {
        // 0.000123 = [1, 2300] weight=-1 dscale=6（首组前导零）
        assertEquals("0.000123", BinaryValueDecoder.decode(1700, numeric(0x0000, -1, 6, 1, 2300)));
    }

    @Test
    void numericNegativeAndZero() {
        assertEquals("-0.5", BinaryValueDecoder.decode(1700, numeric(0x4000, -1, 1, 5000)));
        assertEquals("0", BinaryValueDecoder.decode(1700, numeric(0x0000, 0, 0)));
        assertEquals("-42", BinaryValueDecoder.decode(1700, numeric(0x4000, 0, 0, 42)));
    }

    @Test
    void numericPadsMissingIntegerGroups() {
        // 1e10 = [100] weight=2：整数部分补两组零 → "10000000000"
        assertEquals("10000000000", BinaryValueDecoder.decode(1700, numeric(0x0000, 2, 0, 100)));
        // 12345.6789 = [1, 2345, 6789] weight=1 dscale=4
        assertEquals("12345.6789", BinaryValueDecoder.decode(1700, numeric(0x0000, 1, 4, 1, 2345, 6789)));
    }

    @Test
    void numericScaleBeyondStoredDigitsPadsZero() {
        // 0.50：存储 [5000] weight=-1，dscale 声明 2 位（超出 digit 覆盖部分补零）
        assertEquals("0.50", BinaryValueDecoder.decode(1700, numeric(0x0000, -1, 2, 5000)));
        // dscale 大于 digit 覆盖：123.4500 = [123, 4500] weight=0 dscale=4
        assertEquals("123.4500", BinaryValueDecoder.decode(1700, numeric(0x0000, 0, 4, 123, 4500)));
    }

    @Test
    void numericSpecialValues() {
        assertEquals("NaN", BinaryValueDecoder.decode(1700, numeric(0xC000, 0, 0)));
        assertEquals("Infinity", BinaryValueDecoder.decode(1700, numeric(0xD000, 0, 0)));
        assertEquals("-Infinity", BinaryValueDecoder.decode(1700, numeric(0xF000, 0, 0)));
    }

    @Test
    void numericLargePrecisionIsLossless() {
        // 24 位整数 + 9 位小数：从高位每 4 位一组 digit（每组 ≤9999）——binary 格式无损往返，
        // 与 text 模式逐字符一致（numeric 永不出现科学计数法）
        assertEquals("123456789012345678901234.567890123",
                BinaryValueDecoder.decode(1700, numeric(0x0000, 5, 9,
                        1234, 5678, 9012, 3456, 7890, 1234,   // 整数 6 组（weight=5）
                        5678, 9012, 3000)));                  // 小数 3 组（末组尾零由 dscale=9 截取）
    }

    // ---- uuid 与降级 ----

    @Test
    void uuidRendersLowercaseHyphenated() {
        assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                BinaryValueDecoder.decode(2950, bytes(
                        0xA0, 0xEE, 0xBC, 0x99, 0x9C, 0x0B, 0x4E, 0xF8,
                        0xBB, 0x6D, 0x6B, 0xB9, 0xBD, 0x38, 0x0A, 0x11)));
    }

    @Test
    void unknownOidDegradesToHex() {
        assertEquals("0x0102", BinaryValueDecoder.decode(999999, bytes(1, 2)));
        assertEquals("0x", BinaryValueDecoder.decode(44061, new byte[0])); // enum 等 oid 动态类型走降级
    }

    // ---- interval（interval_out postgres 风格，期望值经 PG 18 实测校准）----

    /** 拼装 interval 载荷（i64 微秒 + i32 天 + i32 月——interval_send 顺序）。 */
    private static byte[] interval(long micros, int days, int months) {
        return ByteBuffer.allocate(16).putLong(micros).putInt(days).putInt(months).array();
    }

    @Test
    void intervalPostgresStyleForms() {
        // month 分解 year/mon（C 向零取整），各段自带符号；"值 ≠ 1" 恒带复数（-1 同样）
        assertEquals("1 year 2 mons 3 days 04:05:06",
                BinaryValueDecoder.decode(1186, interval((4L * 3600 + 5L * 60 + 6) * 1_000_000L, 3, 14)));
        assertEquals("-1 years -6 mons", BinaryValueDecoder.decode(1186, interval(0, 0, -18)));
        assertEquals("-1 mons -2 days", BinaryValueDecoder.decode(1186, interval(0, -2, -1)));
        assertEquals("-02:03:04", BinaryValueDecoder.decode(1186,
                interval(-(2L * 3600 + 3L * 60 + 4) * 1_000_000L, 0, 0)));
        assertEquals("00:00:00", BinaryValueDecoder.decode(1186, interval(0, 0, 0)));
        assertEquals("1 mon 1 day 00:00:00.5",
                BinaryValueDecoder.decode(1186, interval(500_000L, 1, 1)));
        assertEquals("100 years", BinaryValueDecoder.decode(1186, interval(0, 0, 1200)));
        assertEquals("-1 days -02:03:04.000005", BinaryValueDecoder.decode(1186,
                interval(-(2L * 3600 + 3L * 60 + 4) * 1_000_000L - 5, -1, 0)));
        // 累计小时超两位：小时按实际位数输出（%02d 仅最小宽度）
        assertEquals("100:00:00", BinaryValueDecoder.decode(1186, interval(100L * 3600 * 1_000_000L, 0, 0)));
    }

    @Test
    void intervalMixedSignsEmitPlusPrefixAfterNegativePart() {
        // is_before 逐字段传递（AddPostgresIntPart 源码核对 + PG 18 真库校准）：
        // 正分段紧跟负分段之后输出 "+" 前缀
        assertEquals("-1 mons +2 days", BinaryValueDecoder.decode(1186, interval(0, 2, -1)));
        assertEquals("-1 years +02:00:00", BinaryValueDecoder.decode(1186,
                interval(2L * 3600 * 1_000_000L, 0, -12)));
        assertEquals("-1 years -2 mons +3 days 04:00:00", BinaryValueDecoder.decode(1186,
                interval(4L * 3600 * 1_000_000L, 3, -14)));
        // 正分段之后的负时间段：自带 "-"（无 is_before 翻转）
        assertEquals("2 days -03:00:00", BinaryValueDecoder.decode(1186,
                interval(-3L * 3600 * 1_000_000L, 2, 0)));
    }

    // ---- 数组（array_send/array_out 形态）----

    /** 数组元素字节（varlena）：i32 长度 + 载荷；len=-1 为 NULL。 */
    private static byte[] elemVarlena(byte[] payload) {
        return ByteBuffer.allocate(4 + payload.length).putInt(payload.length).put(payload).array();
    }

    /** 数组元素字节（varlena NULL）：i32 -1。 */
    private static byte[] elemNull() {
        return ByteBuffer.allocate(4).putInt(-1).array();
    }

    /** 拼装 array_send 载荷：i32 ndim + i32 hasnull + i32 elemOid + 每维 (i32 dim + i32 lb) + 元素序列。 */
    private static byte[] array(int elemOid, boolean hasNull, int[] dims, int[] lbs, byte[]... elems) {
        ByteBuffer buf = ByteBuffer.allocate(12 + dims.length * 8 + 512);
        buf.putInt(dims.length).putInt(hasNull ? 1 : 0).putInt(elemOid);
        for (int i = 0; i < dims.length; i++) {
            buf.putInt(dims[i]).putInt(lbs[i]);
        }
        for (byte[] e : elems) {
            buf.put(e);
        }
        return Arrays.copyOf(buf.array(), buf.position());
    }

    @Test
    void intArrayWithNullRendersNullLiteral() {
        // _int4(1007)：定长元素同样带 i32 长度前缀（array_send 统一形态），NULL = 前缀 -1
        byte[] arr = array(23, true, new int[]{3}, new int[]{1},
                elemVarlena(i32(1)), elemNull(), elemVarlena(i32(3)));
        assertEquals("{1,NULL,3}", BinaryValueDecoder.decode(1007, arr));
    }

    @Test
    void textArrayQuotingAndEscaping() {
        // _text(1009)：varlena 元素，覆盖 array_out 的全部引号规则（含字面 "NULL" 与 NULL 的区分）
        byte[] arr = array(25, true, new int[]{8}, new int[]{1},
                elemVarlena("a".getBytes()),
                elemVarlena("b,c".getBytes()),
                elemVarlena("d'e".getBytes()),
                elemVarlena("NULL".getBytes()),
                elemNull(),
                elemVarlena(" sp ".getBytes()),
                elemVarlena(new byte[0]),
                elemVarlena("q\"r\\s".getBytes()));
        assertEquals("{a,\"b,c\",d'e,\"NULL\",NULL,\" sp \",\"\",\"q\\\"r\\\\s\"}",
                BinaryValueDecoder.decode(1009, arr));
    }

    @Test
    void twoDimensionalIntArrayRendersNestedBraces() {
        // _int4(1007) 二维 dims=[2,2] → "{{1,2},{3,4}}"
        byte[] arr = array(23, false, new int[]{2, 2}, new int[]{1, 1},
                elemVarlena(i32(1)), elemVarlena(i32(2)), elemVarlena(i32(3)), elemVarlena(i32(4)));
        assertEquals("{{1,2},{3,4}}", BinaryValueDecoder.decode(1007, arr));
    }

    @Test
    void arrayWithNonZeroLowerBoundAppendsPrefix() {
        // lb=-2 dim=4 → "[-2:1]={1,2,3,4}"（上界 = lb + dim - 1）
        byte[] arr = array(23, false, new int[]{4}, new int[]{-2},
                elemVarlena(i32(1)), elemVarlena(i32(2)), elemVarlena(i32(3)), elemVarlena(i32(4)));
        assertEquals("[-2:1]={1,2,3,4}", BinaryValueDecoder.decode(1007, arr));
    }

    @Test
    void boolAndFloat8AndNumericArrays() {
        // _bool(1000)：定长 1 字节带前缀——false(全零字节) 与 NULL(前缀 -1) 无歧义
        assertEquals("{t,f,NULL}", BinaryValueDecoder.decode(1000, array(16, true,
                new int[]{3}, new int[]{1}, elemVarlena(bytes(1)), elemVarlena(bytes(0)), elemNull())));
        // _float8(1022)：定长 8 字节 → "{1.5,-2.5}"
        assertEquals("{1.5,-2.5}", BinaryValueDecoder.decode(1022, array(701, false,
                new int[]{2}, new int[]{1}, elemVarlena(f64(1.5)), elemVarlena(f64(-2.5)))));
        // _numeric(1231)：varlena 元素，载荷为元素的 typsend 形态（numeric 头+digits）
        assertEquals("{123.45,-8.9}", BinaryValueDecoder.decode(1231, array(1700, false,
                new int[]{2}, new int[]{1},
                elemVarlena(numeric(0x0000, 0, 2, 123, 4500)),
                elemVarlena(numeric(0x4000, 0, 1, 8, 9000)))));
    }

    @Test
    void dateAndTimestampArraysRecurseScalarDecoding() {
        // _date(1182)：定长 4 字节 i32 天 → "{2000-01-01,1999-12-31}"
        assertEquals("{2000-01-01,1999-12-31}", BinaryValueDecoder.decode(1182, array(1082, false,
                new int[]{2}, new int[]{1}, elemVarlena(i32(0)), elemVarlena(i32(-1)))));
        // _timestamp(1115)：定长 8 字节 i64 微秒 → 时间文本递归（秒为 0 不省略）；文本含空白 → 带引号
        LocalDateTime ldt = LocalDateTime.of(2026, 1, 1, 10, 20, 0);
        assertEquals("{\"2026-01-01 10:20:00\"}", BinaryValueDecoder.decode(1115, array(1114, false,
                new int[]{1}, new int[]{1}, elemVarlena(i64(microsSincePgEpoch(ldt))))));
    }

    @Test
    void byteaArrayEscapesBackslashInsideQuotes() {
        // _bytea(1001)：varlena 元素输出 "\x.." 含反斜杠 → 加引号并反斜杠转义（array_out 的 \\ 形态）
        byte[] arr = array(17, false, new int[]{1}, new int[]{1},
                elemVarlena(bytes(0xDE, 0xAD, 0xBE, 0xEF)));
        assertEquals("{\"\\\\xdeadbeef\"}", BinaryValueDecoder.decode(1001, arr));
    }

    @Test
    void intervalArrayRendersQuotedElements() {
        // _interval(1187)：元素 varlena（interval 头 16 字节），文本含空白 → 引号
        byte[] oneDay = interval(0, 1, 0);
        byte[] twoDays = interval((3L * 3600 + 4L * 60 + 5) * 1_000_000L, 2, 0);
        assertEquals("{\"1 day\",\"2 days 03:04:05\"}", BinaryValueDecoder.decode(1187, array(1186, false,
                new int[]{2}, new int[]{1}, elemVarlena(oneDay), elemVarlena(twoDays))));
    }

    @Test
    void unknownElemOidDegradesWholeArray() {
        byte[] arr = array(999999, false, new int[]{1}, new int[]{1}, i32(1));
        assertEquals("0x" + HexFormat.of().formatHex(arr), BinaryValueDecoder.decode(1007, arr));
    }

    // ---- fail-fast ----

    @Test
    void malformedLengthFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> BinaryValueDecoder.decode(16, bytes(1, 0)));      // bool 长度 2
        assertThrows(IllegalStateException.class,
                () -> BinaryValueDecoder.decode(2950, bytes(1, 2, 3))); // uuid 长度 3
        assertThrows(IllegalStateException.class,
                () -> BinaryValueDecoder.decode(1700, bytes(0, 1)));    // numeric 头不足 8 字节
        // digit 声明 2 实发 3 个（ndigits 与载荷长度不符）
        ByteBuffer mismatched = ByteBuffer.allocate(8 + 3 * 2);
        mismatched.putShort((short) 2).putShort((short) 0).putShort((short) 0).putShort((short) 0);
        mismatched.putShort((short) 1).putShort((short) 2).putShort((short) 3);
        assertThrows(IllegalStateException.class,
                () -> BinaryValueDecoder.decode(1700, mismatched.array()));
    }
}
