package org.vastdata.vbstream.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * pgoutput 二进制值模式的列载荷解释器：把 TupleData 'b' 种类的原始字节（各类型 {@code typsend}
 * 函数的二进制表示，PG 16 起 START_REPLICATION 传 {@code binary 'on'} 时启用）按列 typeId 解释为
 * 与该类型 text 模式输出尽量一致的可读字符串。
 *
 * <p>覆盖矩阵（内建常用类型；OID 为 PG catalog 硬编码，跨版本稳定）：
 * <ul>
 *   <li>布尔/整数：bool（1 字节 0/1 → "t"/"f"）、int2/int4/int8（big-endian 补码 → 十进制）</li>
 *   <li>无符号 32 位别名：oid/xid/cid → 十进制</li>
 *   <li>浮点：float4/float8（IEEE 754 位型）——Java 最短表示，与 PG 文本表示在科学计数法区段可有格式差</li>
 *   <li>文本类：text/varchar/bpchar/name/json（UTF-8 原文；bpchar 尾部填充空格按原样保留）</li>
 *   <li>bytea：原始字节 → "\\x" + 小写十六进制（对齐 PG bytea 文本输出形态）</li>
 *   <li>numeric：base-10000 复合格式（u16 ndigits + i16 weight + u16 sign + u16 dscale + digits），
 *       见 {@link #decodeNumeric}——精度无损，无科学计数法</li>
 *   <li>日期时间：date（i32 天，纪元 2000-01-01）、time（i64 微秒）、timestamp（i64 微秒自 2000-01-01
 *       墙钟）、timestamptz（同前，按系统默认时区渲染——对齐 pgjdbc 把复制会话时区设为 JVM 默认的
 *       text 模式行为）、timetz（time + i64 秒级时区偏移）</li>
 *   <li>uuid：16 字节 → 小写连字符 36 字符形态</li>
 *   <li>interval（2026-09-09 扩）：i64 微秒 + i32 天 + i32 月 → interval_out 的 postgres 风格
 *       （"-1 years -6 mons" / "1 year 2 mons 3 days 04:05:06.5" / 零值 "00:00:00"），见
 *       {@link #decodeInterval}</li>
 *   <li>内建数组（2026-09-09 扩，16 种 oid 见常量区）：array_send 格式（维数/hasnull/elemOid 头 +
 *       递归元素）→ array_out 的 {@code {e1,e2}} 形态——**每个元素一律 i32 长度前缀（NULL = -1）**，
 *       定长与 varlena 同构（PG 18 实测锚定，bool 的 false 与 NULL 由此无歧义）；剥前缀后经
 *       {@link #decode} 递归解释、按 array_out 规则引号化（NULL 裸输出、字面 "NULL"/空串/特殊字符
 *       加引号并双写转义）、非零 lowerBound 打 {@code [lb:ub]=} 前缀，见 {@link #decodeArray}</li>
 * </ul>
 *
 * <p>未覆盖类型（enum/域/jsonb/组合类型/自定义数组——oid 动态或格式复杂）降级为
 * {@code 0x} + 十六进制原文，每个 oid 仅 WARN 一次（防刷屏）；载荷长度与类型格式不符（流错位的信号）
 * 抛 {@link IllegalStateException} fail-fast。
 *
 * <p>线程约束：静态纯函数、线程安全；唯一可变状态是 WARN 去重集合（并发安全结构，仅作观测节流）。
 */
public final class BinaryValueDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryValueDecoder.class);

    // ---- PG catalog OID（pg_type.dat 硬编码，跨大版本稳定；依据 spec 附录 B 同源 PG 源码）----

    private static final int OID_BOOL = 16;
    private static final int OID_BYTEA = 17;
    private static final int OID_NAME = 19;
    private static final int OID_INT8 = 20;
    private static final int OID_INT2 = 21;
    private static final int OID_INT4 = 23;
    private static final int OID_TEXT = 25;
    private static final int OID_OID = 26;
    private static final int OID_XID = 28;
    private static final int OID_CID = 29;
    private static final int OID_JSON = 114;
    private static final int OID_FLOAT4 = 700;
    private static final int OID_FLOAT8 = 701;
    private static final int OID_BPCHAR = 1042;
    private static final int OID_VARCHAR = 1043;
    private static final int OID_DATE = 1082;
    private static final int OID_TIME = 1083;
    private static final int OID_TIMESTAMP = 1114;
    private static final int OID_TIMESTAMPTZ = 1184;
    private static final int OID_TIMETZ = 1266;
    private static final int OID_INTERVAL = 1186;
    private static final int OID_NUMERIC = 1700;
    private static final int OID_UUID = 2950;

    // ---- 内建数组 OID（pg_type.dat 硬编码；自定义数组/域数组仍走降级）----

    private static final int OID_BOOL_ARRAY = 1000;
    private static final int OID_BYTEA_ARRAY = 1001;
    private static final int OID_INT2_ARRAY = 1005;
    private static final int OID_INT4_ARRAY = 1007;
    private static final int OID_TEXT_ARRAY = 1009;
    private static final int OID_BPCHAR_ARRAY = 1014;
    private static final int OID_VARCHAR_ARRAY = 1015;
    private static final int OID_INT8_ARRAY = 1016;
    private static final int OID_FLOAT4_ARRAY = 1021;
    private static final int OID_FLOAT8_ARRAY = 1022;
    private static final int OID_TIMESTAMP_ARRAY = 1115;
    private static final int OID_DATE_ARRAY = 1182;
    private static final int OID_TIME_ARRAY = 1183;
    private static final int OID_TIMESTAMPTZ_ARRAY = 1185;
    private static final int OID_INTERVAL_ARRAY = 1187;
    private static final int OID_NUMERIC_ARRAY = 1231;
    private static final int OID_TIMETZ_ARRAY = 1270;
    private static final int OID_JSON_ARRAY = 199;
    private static final int OID_UUID_ARRAY = 2951;

    /**
     * 数组元素中的已知元素类型集（varlena 与定长统一——array_send 对**所有**元素一律发
     * {@code i32 长度前缀 + SendFunctionCall 输出字节}，NULL 统一为前缀 -1，定长与变长同构；
     * PG 18 实测锚定：date[] 定长元素若按无前缀读取会整体错位一位元素）。矩阵外的元素类型
     * （自定义/域）使整个数组降级。bytea 元素输出 "\x.." 含反斜杠，经 quoteIfNeeded 自动加引号
     * 并对 \ 与 " 双写（与 array_out 一致）。
     */
    private static final Set<Integer> KNOWN_ELEM_OIDS = Set.of(
            OID_BYTEA, OID_NAME, OID_TEXT, OID_JSON, OID_BPCHAR, OID_VARCHAR, OID_NUMERIC, OID_INTERVAL,
            OID_BOOL, OID_INT2, OID_INT4, OID_INT8, OID_OID, OID_XID, OID_CID, OID_TIMETZ,
            OID_FLOAT4, OID_FLOAT8, OID_DATE, OID_TIME, OID_TIMESTAMP, OID_TIMESTAMPTZ, OID_UUID);

    /** date 纪元 2000-01-01 相对 LocalDate 纪元（1970-01-01）的天数。 */
    private static final long PG_EPOCH_DAY = 10957L;

    /** numeric sign 字段取值：0x0000 正、0x4000 负、0xC000 NaN、0xD000 +Inf、0xF000 -Inf（numeric.c）。 */
    private static final int NUMERIC_POS = 0x0000;
    private static final int NUMERIC_NEG = 0x4000;
    private static final int NUMERIC_NAN = 0xC000;
    private static final int NUMERIC_PINF = 0xD000;
    private static final int NUMERIC_NINF = 0xF000;

    /** 已 WARN 过的未知 oid 集合（观测节流）：每 oid 一次，并发安全（渲染可多线程调用）。 */
    private static final Set<Integer> WARNED_UNKNOWN_OIDS = ConcurrentHashMap.newKeySet();

    private BinaryValueDecoder() {
    }

    /**
     * 解释单个二进制列载荷。
     *
     * <p>职责：按 typeId 分派到对应 typsend 格式的解码分支，产出与 text 模式输出对齐的可读字符串。
     * 边界：矩阵外的 oid 降级十六进制（WARN 一次/oid）；定长类型载荷长度不符抛
     * {@link IllegalStateException}（fail-fast——长度错位通常意味着流已错位，继续解只会扩散）。
     *
     * @param typeId Relation 列元数据的类型 OID
     * @param raw    'b' 种类的载荷字节（已剥 Int32 长度前缀）
     * @return 可读值字符串（与 {@link TupleValue#Text} 的 text 模式渲染同规则消费）
     */
    public static String decode(int typeId, byte[] raw) {
        ByteBufferReader r = new ByteBufferReader(ByteBuffer.wrap(raw));
        return switch (typeId) {
            case OID_BOOL -> decodeBool(r);
            case OID_BYTEA -> "\\x" + HexFormat.of().formatHex(raw);
            case OID_NAME, OID_TEXT, OID_JSON, OID_BPCHAR, OID_VARCHAR ->
                    new String(raw, StandardCharsets.UTF_8);
            case OID_INT2 -> Short.toString(r.readShort());
            case OID_INT4 -> Integer.toString(r.readInt());
            case OID_INT8 -> Long.toString(r.readLong());
            case OID_OID, OID_XID, OID_CID -> Long.toString(r.readUnsignedInt());
            case OID_FLOAT4 -> Float.toString(Float.intBitsToFloat(r.readInt()));
            case OID_FLOAT8 -> Double.toString(Double.longBitsToDouble(r.readLong()));
            case OID_DATE -> decodeDate(r);
            case OID_TIME -> decodeTime(r);
            case OID_TIMETZ -> decodeTimeTz(r);
            case OID_TIMESTAMP -> decodeTimestamp(r, ZoneOffset.UTC);
            case OID_TIMESTAMPTZ -> decodeTimestamp(r, ZoneId.systemDefault());
            case OID_NUMERIC -> decodeNumeric(r);
            case OID_UUID -> decodeUuid(r);
            case OID_INTERVAL -> decodeInterval(r);
            case OID_BOOL_ARRAY, OID_BYTEA_ARRAY, OID_INT2_ARRAY, OID_INT4_ARRAY, OID_TEXT_ARRAY,
                 OID_BPCHAR_ARRAY, OID_VARCHAR_ARRAY, OID_INT8_ARRAY, OID_FLOAT4_ARRAY,
                 OID_FLOAT8_ARRAY, OID_TIMESTAMP_ARRAY, OID_DATE_ARRAY, OID_TIME_ARRAY,
                 OID_TIMESTAMPTZ_ARRAY, OID_INTERVAL_ARRAY, OID_NUMERIC_ARRAY, OID_UUID_ARRAY,
                 OID_TIMETZ_ARRAY, OID_JSON_ARRAY -> decodeArray(typeId, r, raw);
            default -> unknown(typeId, raw);
        };
    }

    /** bool：1 字节非零为真 → PG 文本形态 "t"/"f"；长度≠1 抛 ISE。 */
    private static String decodeBool(ByteBufferReader r) {
        if (r.remaining() != 1) {
            throw malformed(OID_BOOL, r.remaining(), "1 字节");
        }
        return r.readByte() != 0 ? "t" : "f";
    }

    /** date：i32 天数（纪元 2000-01-01，可为负）→ LocalDate 的 ISO 文本（与 PG date 文本输出一致）。 */
    private static String decodeDate(ByteBufferReader r) {
        if (r.remaining() != 4) {
            throw malformed(OID_DATE, r.remaining(), "4 字节");
        }
        return LocalDate.ofEpochDay(PG_EPOCH_DAY + r.readInt()).toString();
    }

    /** time：i64 微秒（自当日零点）→ PG 文本形态（恒 HH:mm:ss，微秒非零时附加去尾零小数）。 */
    private static String decodeTime(ByteBufferReader r) {
        if (r.remaining() != 8) {
            throw malformed(OID_TIME, r.remaining(), "8 字节");
        }
        long micros = r.readLong();
        return pgTimePart(LocalTime.ofNanoOfDay(micros * 1_000L));
    }

    /** timetz：i64 微秒 + i32 秒级时区偏移（timetz_send 的 zone 是 int32，存储语义"西经为正"——文本输出时取负得东经偏移）→ "HH:mm:ss[.frac]±HH[:MM[:SS]]"（PG timetz 文本形态）。 */
    private static String decodeTimeTz(ByteBufferReader r) {
        if (r.remaining() != 12) {
            throw malformed(OID_TIMETZ, r.remaining(), "12 字节");
        }
        long micros = r.readLong();
        String time = pgTimePart(LocalTime.ofNanoOfDay(micros * 1_000L));
        long eastSeconds = -r.readInt();  // 西经为正 → 翻转为东经为正的常规表示
        long abs = Math.abs(eastSeconds);
        long hours = abs / 3600;
        long minutes = (abs % 3600) / 60;
        long seconds = abs % 60;
        String offset = seconds != 0
                ? "%02d:%02d:%02d".formatted(hours, minutes, seconds)
                : minutes != 0 ? "%02d:%02d".formatted(hours, minutes) : "%02d".formatted(hours);
        return time + (eastSeconds < 0 ? "-" : "+") + offset;
    }

    /**
     * timestamp/timestamptz：i64 微秒自 2000-01-01 → {@code "yyyy-MM-dd HH:mm:ss[.frac]"}（空格分隔，
     * 对齐 PG 文本输出；小数秒去尾零）。timestamp（无时区）的微秒是墙钟语义，按 UTC 换算即字面值；
     * timestamptz 的微秒是 UTC 绝对时刻，按 zone 参数渲染（默认调用点为系统默认时区）。
     */
    private static String decodeTimestamp(ByteBufferReader r, ZoneId zone) {
        if (r.remaining() != 8) {
            throw malformed(zone == ZoneOffset.UTC ? OID_TIMESTAMP : OID_TIMESTAMPTZ,
                    r.remaining(), "8 字节");
        }
        long micros = r.readLong();
        LocalDateTime ldt = LocalDateTime.ofInstant(ByteBufferReader.pgMicrosToInstant(micros), zone);
        return ldt.toLocalDate() + " " + pgTimePart(ldt.toLocalTime());
    }

    /**
     * PG 时间文本形态：恒 {@code HH:mm:ss}（秒为 0 不省略——与 java.time toString 的 ISO 规则不同），
     * 微秒非零时附加去尾零的小数（≤6 位，对齐 PG time/timestamp 输出）。小数取自 LocalTime 自身的
     * 纳秒——timestamptz 经时区换算后秒以下部分不变、秒以上可偏移（历史 LMT 秒级偏移场景）。
     */
    private static String pgTimePart(LocalTime t) {
        String base = "%02d:%02d:%02d".formatted(t.getHour(), t.getMinute(), t.getSecond());
        int frac = t.getNano() / 1_000;
        if (frac == 0) {
            return base;
        }
        String digits = "%06d".formatted(frac);
        int end = digits.length();
        while (end > 0 && digits.charAt(end - 1) == '0') {
            end--;
        }
        return base + "." + digits.substring(0, end);
    }

    /**
     * numeric：u16 ndigits + i16 weight + u16 sign + u16 dscale + ndigits 个 u16 digit（base-10000）。
     * 值 = Σ digit[i] × 10000^(weight−i)；sign 见类常量（NaN/±Inf 特殊形态先行返回）。
     * 整数部分取 digit[0..weight]（缺失补零组），小数部分取 digit[weight+1..] 每 digit 4 位、
     * 不足 dscale 补零/超出截断（dscale 是声明显示位数）；头声明长度与实际载荷不符抛 ISE。
     */
    private static String decodeNumeric(ByteBufferReader r) {
        if (r.remaining() < 8) {
            throw malformed(OID_NUMERIC, r.remaining(), "≥8 字节头");
        }
        int ndigits = r.readUnsignedShort();
        int weight = r.readShort();
        int sign = r.readUnsignedShort();
        int dscale = r.readUnsignedShort();
        if (r.remaining() != ndigits * 2) {
            throw malformed(OID_NUMERIC, r.remaining(), ndigits + " 个 digit");
        }
        if (sign == NUMERIC_NAN) {
            return "NaN";
        }
        if (sign == NUMERIC_PINF) {
            return "Infinity";
        }
        if (sign == NUMERIC_NINF) {
            return "-Infinity";
        }
        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = r.readUnsignedShort();
        }
        StringBuilder out = new StringBuilder(ndigits * 4 + 8);
        if (sign == NUMERIC_NEG) {
            out.append('-');
        }
        // 整数部分：digit[0..weight]，最高组不补零、其余组 4 位零填充，缺失组全零
        if (weight >= 0) {
            for (int i = 0; i <= weight; i++) {
                int d = i < ndigits ? digits[i] : 0;
                out.append(i == 0 ? Integer.toString(d) : pad4(d));
            }
        } else {
            out.append('0');
        }
        // 小数部分：自 digit[weight+1] 起每 digit 4 位；weight < -1 时先隔 -weight-1 组全零
        if (dscale > 0) {
            out.append('.');
            int idx = weight + 1;
            if (idx < 0) {
                for (int z = 0; z < -idx; z++) {
                    out.append("0000");
                }
                idx = 0;
            }
            int emitted = 0;
            while (emitted < dscale) {
                int d = idx >= 0 && idx < ndigits ? digits[idx] : 0;
                String group = pad4(d);
                int take = Math.min(4, dscale - emitted);
                out.append(group, 0, take);
                emitted += take;
                idx++;
            }
        }
        return out.toString();
    }

    /** digit 的 4 位十进制零填充（热路径手写，避开 String.format 开销）。 */
    private static String pad4(int digit) {
        if (digit < 10) {
            return "000" + digit;
        }
        if (digit < 100) {
            return "00" + digit;
        }
        if (digit < 1000) {
            return "0" + digit;
        }
        return Integer.toString(digit);
    }

    /** uuid：16 字节 → 8-4-4-4-12 小写连字符形态（与 PG uuid 文本输出一致）；长度≠16 抛 ISE。 */
    private static String decodeUuid(ByteBufferReader r) {
        if (r.remaining() != 16) {
            throw malformed(OID_UUID, r.remaining(), "16 字节");
        }
        byte[] b = r.readBytes(16);
        HexFormat hex = HexFormat.of();
        return hex.formatHex(b, 0, 4) + "-"
                + hex.formatHex(b, 4, 6) + "-"
                + hex.formatHex(b, 6, 8) + "-"
                + hex.formatHex(b, 8, 10) + "-"
                + hex.formatHex(b, 10, 16);
    }

    /** 长度错位的 fail-fast 消息。 */
    private static IllegalStateException malformed(int oid, int actual, String expected) {
        return new IllegalStateException(
                "binary 载荷长度与类型 oid=" + oid + " 的格式不符：实际 " + actual + " 字节，期望 " + expected
                        + "（流可能已错位）");
    }

    /**
     * interval：i64 微秒（time）+ i32 天（day）+ i32 月（month），此顺序（interval_send，PG 18 源码核对）。
     * 渲染对齐 interval_out 的 postgres 风格（EncodeInterval/AddPostgresIntPart，经 PG 18 真库校准）：
     * year/mon 由 month 分解（C 向零取整——负 month 各段自带负号，如 -18 月 → "-1 years -6 mons"）；
     * 单复数按"值 ≠ 1 带复数"（-1 同样带 s）；**is_before 逐字段传递**——正字段紧跟负字段之后输出
     * {@code +} 前缀（"-1 mons +2 days"，混合符号形态）；时间部分非零时以 {@code HH:mm:ss[.frac]}
     * 输出（负值前缀 "-"、is_before 且正时前缀 "+"，微秒去尾零），全部为零时输出 "00:00:00"。
     */
    private static String decodeInterval(ByteBufferReader r) {
        if (r.remaining() != 16) {
            throw malformed(OID_INTERVAL, r.remaining(), "16 字节");
        }
        long timeMicros = r.readLong();
        int day = r.readInt();
        int month = r.readInt();
        int year = month / 12;
        int mon = month % 12;
        StringBuilder out = new StringBuilder(32);
        // 状态对齐源码：state[0]=is_before（上一非零字段为负），state[1]=is_zero（尚无任何非零字段）
        boolean[] state = {false, true};
        appendIntervalPart(out, year, "year", state);
        appendIntervalPart(out, mon, "mon", state);
        appendIntervalPart(out, day, "day", state);
        // 时间部分：非零时输出；全部分量为零时输出 "00:00:00"（interval_out 对零 interval 的形态）
        if (timeMicros != 0 || state[1]) {
            boolean negative = timeMicros < 0;
            long abs = Math.abs(timeMicros);
            long hours = abs / 3_600_000_000L;
            long minutes = (abs % 3_600_000_000L) / 60_000_000L;
            long seconds = (abs % 60_000_000L) / 1_000_000L;
            long micros = abs % 1_000_000L;
            if (!state[1]) {
                out.append(' ');
            }
            if (negative) {
                out.append('-');
            } else if (state[0]) {
                out.append('+');  // 正时间段紧跟负分段之后（源码 minus/is_before 分支）
            }
            out.append("%02d:%02d:%02d".formatted(hours, minutes, seconds));
            if (micros != 0) {
                String digits = "%06d".formatted(micros);
                int end = digits.length();
                while (end > 0 && digits.charAt(end - 1) == '0') {
                    end--;
                }
                out.append('.').append(digits, 0, end);
            }
        }
        return out.toString();
    }

    /**
     * interval 单个整数分段的输出（AddPostgresIntPart 同构）：零值跳过；非首字段前置空格；
     * is_before 且本字段为正时前缀 {@code +}（负值由数字自带负号）；"值 ≠ 1" 恒带复数（-1 亦然）；
     * 本字段非零即翻转 state——is_before 记录本字段符号、is_zero 清位。
     */
    private static void appendIntervalPart(StringBuilder out, long value, String unit, boolean[] state) {
        if (value == 0) {
            return;
        }
        if (!state[1]) {
            out.append(' ');
        }
        if (state[0] && value > 0) {
            out.append('+');
        }
        out.append(value).append(' ').append(unit);
        if (value != 1) {
            out.append('s');
        }
        state[0] = value < 0;
        state[1] = false;
    }

    /**
     * 数组（array_send 格式）：i32 ndim + i32 hasnull + i32 elemOid + 每维 i32 dim + i32 lowerBound +
     * 按行序的元素序列。**每个元素一律 i32 长度前缀 + typsend 输出字节，NULL = 前缀 -1**（定长与
     * varlena 同构——bool 的 false 与 NULL 由此无歧义）。元素值经 {@link #decode} 递归解释（剥前缀后
     * 恰为元素类型的 typsend 载荷），再按 array_out 规则引号化；任一维 lowerBound ≠ 1 时输出
     * {@code [lb:ub]=} 前缀。elemOid 不在已知元素集（自定义/域数组）时整个数组降级十六进制。
     */
    private static String decodeArray(int typeId, ByteBufferReader r, byte[] raw) {
        if (r.remaining() < 12) {
            throw malformed(typeId, r.remaining(), "≥12 字节数组头");
        }
        int ndim = r.readInt();
        r.readInt();  // hasnull 标志：仅元数据（NULL 判定走元素级 -1 前缀），消费以推进读取位置
        int elemOid = r.readInt();
        if (ndim < 0 || ndim > 6 || r.remaining() < ndim * 8L) {
            throw malformed(typeId, r.remaining(), "ndim=" + ndim + " 的维数头");
        }
        if (!KNOWN_ELEM_OIDS.contains(elemOid)) {
            return unknownElem(elemOid, raw);
        }
        int[] dims = new int[ndim];
        int[] lbs = new int[ndim];
        for (int i = 0; i < ndim; i++) {
            dims[i] = r.readInt();
            lbs[i] = r.readInt();
        }
        StringBuilder out = new StringBuilder(estimateLength(dims));
        for (int i = 0; i < ndim; i++) {
            if (lbs[i] != 1) {  // 任一维非 1 才打前缀（array_out 同规则）
                for (int j = 0; j < ndim; j++) {
                    out.append('[').append(lbs[j]).append(':').append(lbs[j] + dims[j] - 1).append(']');
                }
                out.append('=');
                break;
            }
        }
        renderDim(r, dims, 0, elemOid, out);
        return out.toString();
    }

    /**
     * 数组某一维的渲染：depth == dims.length 时读单个元素（引号化后追加），否则输出一层花括号并
     * 递归下一维（元素间逗号分隔）。
     */
    private static void renderDim(ByteBufferReader r, int[] dims, int depth, int elemOid, StringBuilder out) {
        if (depth == dims.length) {
            renderElem(r, elemOid, out);
            return;
        }
        out.append('{');
        for (int i = 0; i < dims[depth]; i++) {
            if (i > 0) {
                out.append(',');
            }
            renderDim(r, dims, depth + 1, elemOid, out);
        }
        out.append('}');
    }

    /** 单个元素读取与渲染：i32 长度前缀（-1 = NULL）剥除后递归 decode + 引号化。 */
    private static void renderElem(ByteBufferReader r, int elemOid, StringBuilder out) {
        int len = r.readInt();
        if (len == -1) {
            out.append("NULL");
            return;
        }
        out.append(quoteIfNeeded(decode(elemOid, r.readBytes(len))));
    }

    /**
     * array_out 的元素引号化：空串、字面 "NULL"、或含 {@code { } , " \ } 任一字符、**任意位置**
     * 空白（PG isspace 判定——timestamp/interval 文本的内部空格命中，故其数组元素带引号）的输出
     * 加双引号，引号内 {@code "} 与 {@code \} 以反斜杠转义（{@code \"} / {@code \\}，PG 18 实测）；
     * 数值类输出不含特殊字符自然免引号。
     */
    private static String quoteIfNeeded(String s) {
        boolean need = s.isEmpty() || "NULL".equals(s);
        if (!need) {
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' || Character.isWhitespace(c)) {
                    need = true;
                    break;
                }
            }
        }
        if (!need) {
            return s;
        }
        StringBuilder quoted = new StringBuilder(s.length() + 4);
        quoted.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                quoted.append('\\');  // 反斜杠转义
            }
            quoted.append(c);
        }
        return quoted.append('"').toString();
    }

    /** 数组元素长度的粗略预估（StringBuilder 初始容量，避免多次扩容）。 */
    private static int estimateLength(int[] dims) {
        long total = 1;
        for (int dim : dims) {
            total *= Math.max(dim, 1);
        }
        return (int) Math.min(total * 8 + 16, 1 << 20);
    }

    /** 数组元素类型未知（自定义/域数组）：整个数组降级十六进制，每 elemOid WARN 一次。 */
    private static String unknownElem(int elemOid, byte[] raw) {
        if (WARNED_UNKNOWN_OIDS.add(-elemOid)) {  // 负号键与标量 unknown 共用去重集不冲突
            LOG.warn("未覆盖的数组元素类型 oid={}（自定义/域数组），该数组值降级为十六进制原文", elemOid);
        }
        return "0x" + HexFormat.of().formatHex(raw);
    }

    /** 未知 oid 降级：十六进制原文；每 oid WARN 一次（观测节流，防大事务刷屏）。 */
    private static String unknown(int typeId, byte[] raw) {
        if (WARNED_UNKNOWN_OIDS.add(typeId)) {
            LOG.warn("未覆盖的二进制类型 oid={}（enum/域/jsonb/组合类型等动态或复杂格式），"
                    + "该类型值降级为十六进制原文", typeId);
        }
        return "0x" + HexFormat.of().formatHex(raw);
    }
}
