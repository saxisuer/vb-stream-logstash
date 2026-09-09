package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
