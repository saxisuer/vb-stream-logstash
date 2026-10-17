package org.vastdata.vbstream.walsource.changes;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 渲染面选项（{@code vb.wal.render.*} 四键，2026-10-17 渲染面配置化）——浮点样式 /
 * 日期时间格式 / 值截断阈值的单一载体，经门面 {@code WalSource} 解析后穿线
 * {@code ChangeStream} → {@code XactGrouper} → {@code DiskValueRenderer}（值面）与
 * {@code OutputRenderer}（截断面）。
 *
 * <p><b>缺省纪律（对拍锚）</b>：四键缺省 = {@code pg}/{@code pg}/{@code pg}/{@code 64}
 * ——缺省档输出与服务器文本输出<b>逐字节同形</b>（双路对拍契约只锚缺省档：全部既有
 * parity IT 构造即缺省档，零回归）。非缺省档是<b>有意分叉</b>：宿主选择与 PG 生态文本
 * 形态分叉（下游若依赖 PG text 语义须自知）。</p>
 *
 * <p><b>键语义</b>：
 * <ul>
 *   <li>{@code vb.wal.render.float.style}：{@code pg}（缺省——服务端 float8out/float4out
 *       同形，科学计数法区段 {@code 1e+20} 形态）| {@code plain}（最短往返数字位<b>去指数
 *       展开</b>为定点数字串——{@code 100000000000000000000}；数字位与 pg 档同源，无假
 *       精度长尾）；</li>
 *   <li>{@code vb.wal.render.date.format}：{@code pg}（缺省——date/timestamp/timestamptz
 *       服务端同形）| {@code default}（内建推荐档 {@link #DEFAULT_DATE_PATTERN}）| Java
 *       {@link DateTimeFormatter} pattern 文本；</li>
 *   <li>{@code vb.wal.render.time.format}：同上作用于 time/timetz（date 与 time 的 pattern
 *       几何不同故分两键；内建 {@link #DEFAULT_TIME_PATTERN}）；</li>
 *   <li>{@code vb.wal.render.truncate}：值截断阈值（{@code OutputRenderer} 面），缺省
 *       {@code 64}（engine {@code ConsoleRenderer} 契约），{@code 0} = 不截断全量输出。</li>
 * </ul></p>
 *
 * <p>边界与异常语义：非法档位/pattern/负阈值构造期 IAE 附键名（fail-fast 与门面其余键
 * 同族）；空串值视为未设（与 collectConfig"空串=未设"语义对齐）。线程约束：record +
 * 不可变 formatter，无约束。</p>
 *
 * @param floatStyle     浮点文本样式（缺省 PG）
 * @param dateFormat     日期时间族格式化器（null = PG 服务器同形）
 * @param timeFormat     时间族格式化器（null = PG 服务器同形）
 * @param truncateLimit  值截断阈值（0 = 不截断）
 */
public record RenderOptions(FloatStyle floatStyle, DateTimeFormatter dateFormat,
                            DateTimeFormatter timeFormat, int truncateLimit) {

    /** 浮点文本样式：PG 服务端同形 vs 最短往返位去指数定点展开。 */
    public enum FloatStyle {

        /** 服务端 float8out/float4out 同形（科学计数法区段 {@code 1e+20} 形态——对拍锚）。 */
        PG,

        /** 最短往返数字位去指数展开（{@code 1e+20} → {@code 100000000000000000000}）。 */
        PLAIN
    }

    /** {@code vb.wal.render.float.style} 键。 */
    public static final String KEY_FLOAT_STYLE = "vb.wal.render.float.style";

    /** {@code vb.wal.render.date.format} 键（date/timestamp/timestamptz 族）。 */
    public static final String KEY_DATE_FORMAT = "vb.wal.render.date.format";

    /** {@code vb.wal.render.time.format} 键（time/timetz 族）。 */
    public static final String KEY_TIME_FORMAT = "vb.wal.render.time.format";

    /** {@code vb.wal.render.truncate} 键（值截断阈值）。 */
    public static final String KEY_TRUNCATE = "vb.wal.render.truncate";

    /** 内建推荐 date 档 pattern（{@code default} 令牌的目标——含秒的常规形态）。 */
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /** 内建推荐 time 档 pattern（{@code default} 令牌的目标）。 */
    public static final String DEFAULT_TIME_PATTERN = "HH:mm:ss";

    /** 缺省档：全部 PG 同形 + 截断 64（既有行为逐字节不变——对拍锚）。 */
    public static final RenderOptions PG = new RenderOptions(FloatStyle.PG, null, null, 64);

    /**
     * 配置解析入口（门面构造期调用一次）：四键各自缺省/白名单档位校验 + pattern 构造期
     * 解析（非法即 IAE——不把坏 pattern 推迟到首行渲染才爆）。
     *
     * @param cfg 配置源（null 值/空串均视为未设）
     * @return 渲染选项（缺省档即 {@link #PG} 等值）
     * @throws IllegalArgumentException 档位值域外 / pattern 非法 / 阈值非数字或负数（消息附键名）
     */
    public static RenderOptions fromProperties(Properties cfg) {
        FloatStyle floatStyle = parseFloatStyle(cfg.getProperty(KEY_FLOAT_STYLE));
        DateTimeFormatter dateFormat = parseFormatter(cfg.getProperty(KEY_DATE_FORMAT),
                KEY_DATE_FORMAT, DEFAULT_DATE_PATTERN);
        DateTimeFormatter timeFormat = parseFormatter(cfg.getProperty(KEY_TIME_FORMAT),
                KEY_TIME_FORMAT, DEFAULT_TIME_PATTERN);
        int truncate = parseTruncate(cfg.getProperty(KEY_TRUNCATE));
        return new RenderOptions(floatStyle, dateFormat, timeFormat, truncate);
    }

    /**
     * 浮点档位解析：null/空串 = PG；{@code pg}/{@code plain} 之外 IAE。
     *
     * @param raw 配置原值
     * @return 档位
     * @throws IllegalArgumentException 值域外（附键名）
     */
    private static FloatStyle parseFloatStyle(String raw) {
        if (raw == null || raw.isBlank()) {
            return FloatStyle.PG;
        }
        return switch (raw.trim()) {
            case "pg" -> FloatStyle.PG;
            case "plain" -> FloatStyle.PLAIN;
            default -> throw new IllegalArgumentException(KEY_FLOAT_STYLE + " 须为 pg/plain: " + raw);
        };
    }

    /**
     * 时间格式键解析：null/空串/{@code pg} = null（服务器同形）；{@code default} = 内建
     * 推荐 pattern；其余按 {@link DateTimeFormatter#ofPattern} 构造（非法 IAE 转译附键名）。
     *
     * @param raw           配置原值
     * @param key           键名（异常消息归因）
     * @param defaultPattern 内建推荐 pattern（default 令牌目标）
     * @return 格式化器；null = PG 服务器同形
     * @throws IllegalArgumentException pattern 非法（附键名）
     */
    private static DateTimeFormatter parseFormatter(String raw, String key, String defaultPattern) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String token = raw.trim();
        if ("pg".equals(token)) {
            return null;
        }
        String pattern = "default".equals(token) ? defaultPattern : token;
        try {
            return DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(key + " 的 pattern 非法: " + pattern, e);
        }
    }

    /**
     * 截断阈值解析：null/空串 = 64（engine 契约）；非负整数（0 = 不截断）；负数/非数字 IAE。
     *
     * @param raw 配置原值
     * @return 阈值
     * @throws IllegalArgumentException 负数或非数字（附键名）
     */
    private static int parseTruncate(String raw) {
        if (raw == null || raw.isBlank()) {
            return PG.truncateLimit;
        }
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(KEY_TRUNCATE + " 须为非负整数（0 = 不截断）: " + raw, e);
        }
        if (value < 0) {
            throw new IllegalArgumentException(KEY_TRUNCATE + " 须为非负整数（0 = 不截断）: " + raw);
        }
        return value;
    }
}
