package org.vastdata.vbstream.walsource.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 磁盘格式值 → PG text 渲染矩阵——把 v1 {@code TupleDecoder} 产出的解码对象（或本类
 * 自行从磁盘字节解的类型）渲染为与 pgoutput text 模式对齐的可读字符串，是双路对拍
 * （wal-source 直解路 vs engine 逻辑解码路）的值对齐基础。
 *
 * <p><b>输入契约</b>（{@link #render}，分派键 typeOid）：</p>
 * <ul>
 *   <li>v1 {@code TupleDecoder} 已解的 11 kind 传其 Java 对象：bool→Boolean、
 *       int2→Short、int4→Integer、int8/oid→Long（oid 无符号语义）、float4→Float、
 *       float8→Double、text/varchar/bpchar/name/json→String、bytea→byte[]、
 *       timestamp→String（TupleDecoder 的 ISO 形态，本类无损重排为 PG 文本形态）</li>
 *   <li>{@code TupleDecoder} 词典外的类型传<b>磁盘 datum 原始字节（小端）</b>：
 *       date=i32 天（epoch 2000-01-01）、time/timestamptz=i64 微秒、timestamp 亦接受
 *       i64 微秒形态、uuid=16 字节、numeric=<b>已剥 varlena 头的 varlena 内容</b>
 *       （首 u16 的 flag 位自证短/长/特殊格式，与 varlena 头形态无关）</li>
 *   <li>decoded == null 一律返回 null（NULL 语义交调用方）</li>
 * </ul>
 *
 * <p><b>渲染规则与实测锚</b>（样本 2026-10-07 docker PG 18.6 容器 {@code ::text} 实测
 * 钉死，全文见测试类 javadoc 样本表）：bool 取 bool_out 的 {@code t}/{@code f}
 * （<b>非</b> {@code ::text} cast 的 {@code true}——pgoutput text 模式即 bool_out）；
 * 整数十进制；浮点 Java 最短表示（1.5/NaN/Infinity 与 PG 逐字一致，科学计数法区段
 * 有 {@code 1e+20} vs {@code 1.0E20} 的已知格式差——与 engine {@code BinaryValueDecoder}
 * 同一取舍）；文本族原文透传；bytea {@code \x}+小写 hex；时间类恒 {@code HH:mm:ss}
 * （秒 0 不省略）+ 微秒<b>去尾零</b>小数（实测裁定：{@code .100000} 渲染 {@code .1}）；
 * timestamptz 按 JVM 默认时区渲染并带数字偏移（对齐 pgjdbc 把复制会话时区设为 JVM
 * 默认的 engine 侧 text 行为）；numeric 精度无损、永不科学计数法。</p>
 *
 * <p><b>矩阵外 oid</b>（enum/域/jsonb/组合类型等动态或复杂格式）降级 {@code 0x}+小写
 * 十六进制，每 oid 仅 WARN 一次（类级 logger，观测节流防大事务刷屏）。定长类型载荷
 * 长度不符抛 {@link IllegalStateException} fail-fast（磁盘走读错位的信号）。已知
 * 限制：BC 日期/时间戳（PG 的 {@code  BC} 纪年后缀）不在首发矩阵，走 java.time 的
 * ISO 纪年（零年 = 1 BC），与 PG 文本形态在远古值上分叉。</p>
 *
 * <p>线程约束：静态纯函数、并发安全；唯一可变状态是 WARN 去重集合（并发安全结构，
 * 仅作观测节流）。</p>
 */
public final class DiskValueRenderer {

    private static final Logger LOG = LoggerFactory.getLogger(DiskValueRenderer.class);

    // ---- PG catalog OID（pg_type.dat 硬编码，跨大版本稳定）----

    private static final int OID_BOOL = 16;
    private static final int OID_BYTEA = 17;
    private static final int OID_CHAR = 18;
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
    private static final int OID_NUMERIC = 1700;
    private static final int OID_UUID = 2950;

    /** date 纪元 2000-01-01 相对 LocalDate 纪元（1970-01-01）的天数。 */
    private static final long PG_EPOCH_DAY = 10957L;

    /** timestamp 家族的 PostgreSQL epoch（2000-01-01T00:00:00Z）折算微秒数。 */
    private static final long EPOCH_2000_MICROS = 946_684_800_000_000L;

    // ---- numeric 磁盘格式的头位（REL_18_STABLE numeric.c L156-253）----

    /** 首 u16 的高 2 位 flag 掩码。 */
    private static final int NUMERIC_SIGN_MASK = 0xC000;

    /** flag 取值：短格式（头 2 字节）。 */
    private static final int NUMERIC_FLAG_SHORT = 0x8000;

    /** 特殊值 flag（NaN/±Inf，整字相等判定：0xC000/0xD000/0xF000）。 */
    private static final int NUMERIC_FLAG_SPECIAL = 0xC000;

    /** 长格式的负号位（n_sign_dscale 高 2 位为 NEG 时置位）。 */
    private static final int NUMERIC_NEG = 0x4000;

    /** 短格式负号位（n_header bit13）。 */
    private static final int NUMERIC_SHORT_SIGN = 0x2000;

    /** 短格式 dscale 位段（bits 7-12，6 位 → 0..63）。 */
    private static final int NUMERIC_SHORT_DSCALE_MASK = 0x1F80;

    /** 短格式 dscale 位段右移量。 */
    private static final int NUMERIC_SHORT_DSCALE_SHIFT = 7;

    /** 短格式 weight 的符号位（bit6，置位即 7 位二补码负值）。 */
    private static final int NUMERIC_SHORT_WEIGHT_SIGN = 0x0040;

    /** 短格式 weight 的数值位（bits 0-5）。 */
    private static final int NUMERIC_SHORT_WEIGHT_MASK = 0x003F;

    /** 特殊值整字常量（NaN/+Inf/-Inf，make_result 直接写 2 字节头）。 */
    private static final int NUMERIC_NAN = 0xC000;
    private static final int NUMERIC_PINF = 0xD000;
    private static final int NUMERIC_NINF = 0xF000;

    /** 长格式 dscale 位段（n_sign_dscale 低 14 位）。 */
    private static final int NUMERIC_DSCALE_MASK = 0x3FFF;

    /** 已 WARN 过的未知 oid 集合（观测节流）：每 oid 一次，并发安全。 */
    private static final Set<Integer> WARNED_UNKNOWN_OIDS = ConcurrentHashMap.newKeySet();

    /**
     * 工具类防实例化。
     */
    private DiskValueRenderer() {
    }

    /**
     * 解码值 → PG text 表示（pgoutput text 模式对齐）。
     *
     * <p>关键步骤：null 直通；按 typeOid 分派到类型分支（输入形态契约见类 javadoc——
     * v1 已解类型传 Java 对象，词典外类型传磁盘 datum 小端字节）。边界与异常语义：
     * 矩阵外 oid 降级 {@code 0x}+hex（WARN 一次/oid）；定长字节载荷长度不符、
     * numeric 载荷奇数/头不符、输入对象类型与 oid 不匹配均抛 ISE fail-fast（磁盘
     * 走读错位的信号，继续渲染只会扩散）。线程约束：静态纯函数，并发安全。</p>
     *
     * @param decoded 解码值（TupleDecoder 对象或磁盘 datum 字节，契约见类 javadoc）
     * @param typeOid 列类型 oid（分派键）
     * @return PG text 形态字符串；decoded == null 时返回 null
     * @throws IllegalStateException 载荷长度/输入类型与 oid 格式不符
     */
    public static String render(Object decoded, long typeOid) {
        if (decoded == null) {
            return null;
        }
        return switch ((int) typeOid) {
            case OID_BOOL -> renderBool(decoded);
            case OID_INT2, OID_INT4, OID_INT8, OID_OID, OID_XID, OID_CID -> renderInt(decoded);
            case OID_FLOAT4 -> Float.toString(expectNumber(decoded, typeOid).floatValue());
            case OID_FLOAT8 -> Double.toString(expectNumber(decoded, typeOid).doubleValue());
            case OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_NAME, OID_CHAR, OID_JSON -> renderTextFamily(decoded);
            case OID_BYTEA -> "\\x" + HexFormat.of().formatHex(expectBytes(decoded, typeOid));
            case OID_DATE -> renderDate(expectBytes(decoded, typeOid));
            case OID_TIME -> renderTime(expectBytes(decoded, typeOid));
            case OID_TIMESTAMP -> renderTimestamp(decoded);
            case OID_TIMESTAMPTZ -> renderTimestamptz(expectBytes(decoded, typeOid));
            case OID_NUMERIC -> renderNumeric(expectBytes(decoded, typeOid));
            case OID_UUID -> renderUuid(expectBytes(decoded, typeOid));
            default -> unknown(typeOid, decoded);
        };
    }

    /**
     * 单列的 {@code name=value} 对拍形态（截断 64 规则在后续 OutputRenderer 层）。
     *
     * <p>关键步骤：dropped 列恒 {@code name=∅}（值不消费——存储恒 NULL，占位语义）；
     * NULL 值渲染 {@code name=NULL} 字面（区别于空串的 {@code name=}）；其余经
     * {@link #render} 取值。边界与异常语义：col 为 null 抛 NPE（列元数据是对拍形态的
     * 必要素材，缺失属装配错误）；attnum 仅用于诊断消息。线程约束：静态纯函数，
     * 并发安全。</p>
     *
     * @param attnum  物理列号（诊断用；权威值在 col 内）
     * @param decoded 解码值（可为 null = NULL 列）
     * @param col     列元数据（非 null）
     * @return {@code name=value} / {@code name=NULL} / {@code name=∅} 形态字符串
     * @throws NullPointerException col 为 null
     */
    public static String renderColumn(int attnum, Object decoded, ColumnMeta col) {
        if (col == null) {
            throw new NullPointerException("renderColumn requires column meta (attnum=" + attnum + ")");
        }
        if (col.dropped()) {
            return col.name() + "=∅";
        }
        String value = render(decoded, col.typeOid());
        return col.name() + "=" + (value == null ? "NULL" : value);
    }

    /**
     * bool：Boolean 或单字节磁盘形态 → bool_out 的 {@code t}/{@code f}。
     *
     * <p>实测锚：docker {@code SELECT 't'::bool::text} 为 {@code true}（cast 形态），
     * 而 pgoutput text 模式（bool_out）是 {@code t}——本渲染取后者，对齐 engine
     * {@code BinaryValueDecoder} 与逻辑解码 text 输出。边界：字节长度≠1 或输入类型
     * 不符抛 ISE。线程约束：纯函数。</p>
     *
     * @param decoded Boolean 或 byte[1]（磁盘 0/1 字节）
     * @return "t" / "f"
     * @throws IllegalStateException 输入形态不符
     */
    private static String renderBool(Object decoded) {
        boolean v;
        if (decoded instanceof Boolean b) {
            v = b;
        } else if (decoded instanceof byte[] b && b.length == 1) {
            v = b[0] != 0;
        } else {
            throw malformed(16, decoded);
        }
        return v ? "t" : "f";
    }

    /**
     * 整数族（int2/int4/int8/oid/xid/cid）：任意 Number → 十进制。
     *
     * <p>int8 边界（Long.MIN_VALUE）与 oid 的 u32 无符号语义（v1 已折算 Long）均由
     * Long.toString 覆盖。边界：非 Number 抛 ISE。线程约束：纯函数。</p>
     *
     * @param decoded Number（TupleDecoder 的 Short/Integer/Long）
     * @return 十进制字符串
     * @throws IllegalStateException 非 Number 输入
     */
    private static String renderInt(Object decoded) {
        return Long.toString(expectNumber(decoded, -1).longValue());
    }

    /**
     * 文本族（text/varchar/bpchar/name/char/json）：String 原文透传，byte[] 按 UTF-8 解。
     *
     * <p>已知分叉：v1 {@code TupleDecoder} 对 "char" 产出带引号的 {@code 'x'}（spike
     * 渲染形态），本层原样透传（"char" 是 catalog 内部类型，用户表罕见，pgoutput 的
     * char_out 为裸字符——对拍矩阵不含该类型）。线程约束：纯函数。</p>
     *
     * @param decoded String（TupleDecoder 产物）或 byte[]（varlena 载荷，剥头后）
     * @return 原文
     * @throws IllegalStateException 非 String/byte[] 输入
     */
    private static String renderTextFamily(Object decoded) {
        if (decoded instanceof String s) {
            return s;
        }
        if (decoded instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }
        throw malformed(-1, decoded);
    }

    /**
     * date：i32 天数（epoch 2000-01-01，小端）→ ISO 日期文本。
     *
     * <p>实测锚：docker {@code SELECT '2026-10-06'::date::text} = {@code 2026-10-06}、
     * 纪元日 {@code 2000-01-01}、{@code '1999-12-31'}（负天数）。已知限制：BC 纪年
     * PG 带 {@code  BC} 后缀，本层走 java.time ISO 纪年（零年=1 BC），远古值分叉。
     * 边界：长度≠4 抛 ISE。线程约束：纯函数。</p>
     *
     * @param raw 磁盘 datum（4 字节小端）
     * @return yyyy-MM-dd
     * @throws IllegalStateException 长度不符
     */
    private static String renderDate(byte[] raw) {
        if (raw.length != 4) {
            throw malformed(OID_DATE, raw.length, "4 字节");
        }
        int days = (int) u32le(raw, 0);
        return LocalDate.ofEpochDay(PG_EPOCH_DAY + days).toString();
    }

    /**
     * time：i64 微秒（自当日零点，小端）→ PG 文本形态。
     *
     * <p>实测锚：docker {@code '12:34:56.123456'::time::text} = {@code 12:34:56.123456}、
     * {@code '12:34:56.100000'} → {@code 12:34:56.1}（<b>尾零截断</b>——任务书疑点
     * 实测裁定）。恒 HH:mm:ss（秒 0 不省略）。边界：长度≠8 或微秒越出一日抛 ISE/
     * 异常 fail-fast。线程约束：纯函数。</p>
     *
     * @param raw 磁盘 datum（8 字节小端）
     * @return HH:mm:ss[.frac]
     * @throws IllegalStateException 长度不符
     */
    private static String renderTime(byte[] raw) {
        if (raw.length != 8) {
            throw malformed(OID_TIME, raw.length, "8 字节");
        }
        return pgTimePart(LocalTime.ofNanoOfDay(u64le(raw, 0) * 1_000L));
    }

    /**
     * timestamp（无时区）：双输入路径 → PG 文本形态 {@code yyyy-MM-dd HH:mm:ss[.frac]}。
     *
     * <p>关键步骤：String 路径 = v1 {@code TupleDecoder} 的 ISO 产物（LocalDateTime.toString
     * 形态——"T" 分隔、秒/小数可省略），解析后经 {@link #pgTimestamp} 重排（ISO 的
     * "…T12:34" 不丢秒）；byte[] 路径 = 磁盘 i64 微秒（epoch 2000 墙钟语义，小端），
     * 按 UTC 换算即字面值。实测锚：docker {@code '2026-10-06 12:34'::timestamp::text}
     * = {@code 2026-10-06 12:34:00}、{@code .100000} → {@code .1}。边界：ISO 串解析
     * 失败抛 ISE（包装 DateTimeParseException 保留原因）；字节长度≠8 抛 ISE。
     * 线程约束：纯函数。</p>
     *
     * @param decoded ISO 字符串（TupleDecoder 产物）或 byte[8]（磁盘 datum）
     * @return PG 文本形态
     * @throws IllegalStateException 输入形态不符或 ISO 解析失败
     */
    private static String renderTimestamp(Object decoded) {
        LocalDateTime ldt;
        if (decoded instanceof String iso) {
            try {
                ldt = LocalDateTime.parse(iso);
            } catch (DateTimeParseException e) {
                throw new IllegalStateException("timestamp 输入不是 TupleDecoder 的 ISO 形态: " + iso, e);
            }
        } else if (decoded instanceof byte[] raw && raw.length == 8) {
            ldt = microsToLocalDateTime(u64le(raw, 0), ZoneOffset.UTC);
        } else {
            throw malformed(OID_TIMESTAMP, decoded);
        }
        return pgTimestamp(ldt);
    }

    /**
     * timestamptz：i64 微秒（UTC 绝对时刻，epoch 2000，小端）→ JVM 默认时区渲染 +
     * 数字偏移后缀。
     *
     * <p>实测锚：docker GMT 会话 {@code '2026-10-06 12:34:56.123456+00'::timestamptz::text}
     * = {@code 2026-10-06 12:34:56.123456+00}（+08 输入折算为 {@code 04:34:56...+00}）；
     * 时区选择对齐 engine {@code BinaryValueDecoder}（pgjdbc 把复制会话时区设为 JVM
     * 默认——双路对拍的 engine 侧 text 输出即该时区，两侧同 JVM 时即逐字一致）。
     * 偏移格式：整小时 {@code +08}、半时 {@code +05:30}（与 ZoneOffset 文本同形），
     * 零偏移特例 {@code +00}（PG 不打 "Z"）。边界：长度≠8 抛 ISE。线程约束：纯函数。</p>
     *
     * @param raw 磁盘 datum（8 字节小端）
     * @return yyyy-MM-dd HH:mm:ss[.frac]±HH[:MM[:SS]]
     * @throws IllegalStateException 长度不符
     */
    private static String renderTimestamptz(byte[] raw) {
        if (raw.length != 8) {
            throw malformed(OID_TIMESTAMPTZ, raw.length, "8 字节");
        }
        long micros = u64le(raw, 0);
        LocalDateTime ldt = microsToLocalDateTime(micros, ZoneId.systemDefault());
        ZoneOffset off = offsetAt(micros);
        String offText = off.getTotalSeconds() == 0 ? "+00" : off.toString();
        return pgTimestamp(ldt) + offText;
    }

    /**
     * uuid：16 字节 → 8-4-4-4-12 小写连字符形态。
     *
     * <p>实测锚：docker {@code 'a0b1c2d3-…'::uuid::text} 原样小写连字符。边界：长度
     * ≠16 抛 ISE。线程约束：纯函数。</p>
     *
     * @param raw 磁盘 datum（16 字节，字节序即网络序）
     * @return 36 字符小写连字符形态
     * @throws IllegalStateException 长度不符
     */
    private static String renderUuid(byte[] raw) {
        if (raw.length != 16) {
            throw malformed(OID_UUID, raw.length, "16 字节");
        }
        HexFormat hex = HexFormat.of();
        return hex.formatHex(raw, 0, 4) + "-"
                + hex.formatHex(raw, 4, 6) + "-"
                + hex.formatHex(raw, 6, 8) + "-"
                + hex.formatHex(raw, 8, 10) + "-"
                + hex.formatHex(raw, 10, 16);
    }

    /**
     * numeric：磁盘 varlena 内容（已剥 varlena 头）→ 十进制字符串（精度无损）。
     *
     * <p>关键步骤：首 u16 的 flag 位（&amp;0xC000）三路分派（REL_18 numeric.c L104-253）：
     * ①特殊值（0xC000）整字判 NaN(0xC000)/+Inf(0xD000)/-Inf(0xF000)，其余保留位非零
     * 抛 ISE；②短格式（0x8000）头 2 字节——负号 0x2000、dscale=(h&amp;0x1F80)&gt;&gt;7、
     * weight 7 位二补码（0x0040 符号扩展）；③长格式（0x0000/0x4000）头 4 字节——
     * u16 sign_dscale（高 2 位符号 + 低 14 位 dscale）+ i16 weight（PG 会把 dscale&gt;63
     * 或 weight 越界 [-64,63] 的值存长格式，pg_upgrade 存量库亦有长格式同值字节）。
     * digits 逐 u16 小端 base-10000，值 = Σ digit[i]×10000^(weight−i)，首尾零组已剥
     * （零值无 digits）。渲染与 engine {@code decodeNumeric} 同算法：整数部分取
     * digit[0..weight]（最高组不补零）、小数部分自 digit[weight+1] 起每组 4 位、不足
     * dscale 补零/超出截断（dscale 是声明显示位数，1.10 的尾零由它保住）。边界：
     * 内容奇数字节（digit 半截）或头声明越界抛 ISE。线程约束：纯函数。</p>
     *
     * @param raw varlena 内容（varlena 头已剥；短格式最少 2 字节、长格式最少 4 字节）
     * @return 十进制字符串 / "NaN" / "Infinity" / "-Infinity"
     * @throws IllegalStateException 载荷与格式不符
     */
    private static String renderNumeric(byte[] raw) {
        if (raw.length < 2) {
            throw malformed(OID_NUMERIC, raw.length, "≥2 字节头");
        }
        int header = u16le(raw, 0);
        int flag = header & NUMERIC_SIGN_MASK;
        boolean negative;
        int dscale;
        int weight;
        int digitOff;
        if (flag == NUMERIC_FLAG_SPECIAL) {
            if (header == NUMERIC_NAN) {
                return "NaN";
            }
            if (header == NUMERIC_PINF) {
                return "Infinity";
            }
            if (header == NUMERIC_NINF) {
                return "-Infinity";
            }
            throw new IllegalStateException("numeric 特殊值保留位非零: 0x" + Integer.toHexString(header));
        } else if (flag == NUMERIC_FLAG_SHORT) {
            negative = (header & NUMERIC_SHORT_SIGN) != 0;
            dscale = (header & NUMERIC_SHORT_DSCALE_MASK) >>> NUMERIC_SHORT_DSCALE_SHIFT;
            weight = (header & NUMERIC_SHORT_WEIGHT_SIGN) != 0
                    ? (header & NUMERIC_SHORT_WEIGHT_MASK) - (NUMERIC_SHORT_WEIGHT_SIGN)
                    : (header & NUMERIC_SHORT_WEIGHT_MASK);
            digitOff = 2;
        } else {
            if (raw.length < 4) {
                throw malformed(OID_NUMERIC, raw.length, "≥4 字节头（长格式）");
            }
            negative = (header & NUMERIC_NEG) != 0;
            dscale = header & NUMERIC_DSCALE_MASK;
            weight = (short) u16le(raw, 2);   // i16 符号语义
            digitOff = 4;
        }
        if (((raw.length - digitOff) & 1) != 0) {
            throw malformed(OID_NUMERIC, raw.length - digitOff, "偶数字节的 digit 序列");
        }
        int ndigits = (raw.length - digitOff) / 2;
        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = u16le(raw, digitOff + i * 2);
        }
        StringBuilder out = new StringBuilder(ndigits * 4 + 8);
        if (negative) {
            out.append('-');
        }
        if (weight >= 0) {
            for (int i = 0; i <= weight; i++) {
                int d = i < ndigits ? digits[i] : 0;
                out.append(i == 0 ? Integer.toString(d) : pad4(d));
            }
        } else {
            out.append('0');
        }
        if (dscale > 0) {
            out.append('.');
            // 统一循环：idx < 0（weight+1 起）的组全为零 digit——与真实 digit 组同路消耗，
            // 使 emit 总数恰为 dscale（分开的"先补零组再 emitted=0 计数"会把补过的零组
            // 漏计、产出 68+70 位的多余尾零——1E-70 锚钉住此形态）
            int idx = weight + 1;
            int emitted = 0;
            while (emitted < dscale) {
                int d = idx >= 0 && idx < ndigits ? digits[idx] : 0;
                int take = Math.min(4, dscale - emitted);
                out.append(pad4(d), 0, take);
                emitted += take;
                idx++;
            }
        }
        return out.toString();
    }

    /**
     * PG 时间文本形态：恒 {@code HH:mm:ss}（秒为 0 不省略——java.time 的 ISO 规则会
     * 省略，须显式两位格式），微秒非零时附加去尾零小数（≤6 位）。实测锚：
     * docker {@code '12:34:56.100000'::time::text} = {@code 12:34:56.1}。
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
     * PG timestamp 文本形态：{@code yyyy-MM-dd HH:mm:ss[.frac]}（空格分隔，小数去尾零，
     * 秒不省略）。实测锚：docker {@code '2026-10-06 12:34:56.123456'::timestamp::text}
     * = {@code 2026-10-06 12:34:56.123456}。
     */
    private static String pgTimestamp(LocalDateTime ldt) {
        return ldt.toLocalDate() + " " + pgTimePart(ldt.toLocalTime());
    }

    /**
     * 磁盘微秒（epoch 2000 起点）→ 指定时区的 LocalDateTime。
     *
     * <p>floorDiv/floorMod 保负值（epoch 前）正确；epoch 折算常量与 v1 TupleDecoder
     * 同源。线程约束：纯函数。</p>
     */
    private static LocalDateTime microsToLocalDateTime(long micros, ZoneId zone) {
        long epochMicros = EPOCH_2000_MICROS + micros;
        Instant inst = Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * 1000L);
        return LocalDateTime.ofInstant(inst, zone);
    }

    /**
     * 磁盘微秒（epoch 2000 起点）时刻在 JVM 默认时区的偏移——供 timestamptz 的数字
     * 偏移后缀（与渲染用同一换算源，日期与偏移必然自洽）。
     */
    private static ZoneOffset offsetAt(long micros) {
        long epochMicros = EPOCH_2000_MICROS + micros;
        Instant inst = Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * 1000L);
        return inst.atZone(ZoneId.systemDefault()).getOffset();
    }

    /**
     * numeric digit 的 4 位十进制零填充（热路径手写，避开 String.format 开销）。
     */
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

    /**
     * 矩阵外 oid 降级：{@code 0x} + 小写十六进制原文；每 oid WARN 一次（观测节流，
     * 防大事务刷屏）。String 输入按 UTF-8 字节取 hex，其他对象取 String.valueOf 的
     * UTF-8 字节。
     */
    private static String unknown(long typeOid, Object decoded) {
        if (WARNED_UNKNOWN_OIDS.add((int) typeOid)) {
            LOG.warn("未覆盖的磁盘类型 oid={}（enum/域/jsonb/组合类型等动态或复杂格式），该类型值降级为十六进制原文", typeOid);
        }
        byte[] raw = decoded instanceof byte[] b ? b
                : (decoded instanceof String s ? s : String.valueOf(decoded)).getBytes(StandardCharsets.UTF_8);
        return "0x" + HexFormat.of().formatHex(raw);
    }

    /**
     * 要求 Number 输入（整数/浮点族），否则 ISE。
     */
    private static Number expectNumber(Object decoded, long typeOid) {
        if (decoded instanceof Number n) {
            return n;
        }
        throw malformed(typeOid, decoded);
    }

    /**
     * 要求 byte[] 输入（磁盘 datum 面），否则 ISE。
     */
    private static byte[] expectBytes(Object decoded, long typeOid) {
        if (decoded instanceof byte[] b) {
            return b;
        }
        throw malformed(typeOid, decoded);
    }

    /** 输入形态不符的 fail-fast 消息（oid=-1 表示多处共用的族级分支）。 */
    private static IllegalStateException malformed(long typeOid, Object decoded) {
        return new IllegalStateException("渲染输入与 oid=" + typeOid + " 的期望形态不符: "
                + (decoded == null ? "null" : decoded.getClass().getSimpleName()));
    }

    /** 定长载荷长度错位的 fail-fast 消息。 */
    private static IllegalStateException malformed(int oid, int actual, String expected) {
        return new IllegalStateException(
                "磁盘载荷长度与类型 oid=" + oid + " 的格式不符：实际 " + actual + " 字节，期望 " + expected
                        + "（走读可能已错位）");
    }

    /**
     * 就地读 little-endian u16（对齐 v1 TupleDecoder 的读取风格）。
     */
    private static int u16le(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     */
    private static long u32le(byte[] b, int o) {
        return (b[o] & 0xFFL) | ((b[o + 1] & 0xFFL) << 8) | ((b[o + 2] & 0xFFL) << 16) | ((b[o + 3] & 0xFFL) << 24);
    }

    /**
     * 就地读 little-endian u64。
     */
    private static long u64le(byte[] b, int o) {
        return u32le(b, o) | (u32le(b, o + 4) << 32);
    }
}
