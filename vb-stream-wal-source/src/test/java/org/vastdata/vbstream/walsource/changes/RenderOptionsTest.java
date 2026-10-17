package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 渲染面四键（{@code vb.wal.render.*}）的解析语义矩阵：缺省档逐字段等价
 * {@link RenderOptions#PG}（对拍锚不动）、非缺省档各分支取值、非法值构造期 IAE
 * fail-fast（附键名便于归因）。
 *
 * <p><b>缺省纪律（设计裁定）</b>：四键缺省 = {@code pg}/{@code pg}/{@code pg}/{@code 64}
 * ——缺省档输出与服务器文本输出逐字节同形（双路对拍契约只锚缺省档）；非缺省档是
 * <b>有意分叉</b>（宿主自担与 PG 生态文本形态的兼容面）。{@code default} 内建档 =
 * date 族 {@code yyyy-MM-dd HH:mm:ss} / time 族 {@code HH:mm:ss}（"给一个默认的
 * format 设置"需求的落点——免写 pattern 即得推荐格式）。</p>
 */
class RenderOptionsTest {

    /** 缺省：四键全缺 → 与 PG 常量逐字段等价（对拍锚零扰动的构造面证明）。 */
    @Test
    void absentKeysYieldPgDefaults() {
        RenderOptions opts = RenderOptions.fromProperties(new Properties());
        assertEquals(RenderOptions.FloatStyle.PG, opts.floatStyle(), "float 缺省 pg 档");
        assertNull(opts.dateFormat(), "date 缺省无自定义格式（服务器同形）");
        assertNull(opts.timeFormat(), "time 缺省无自定义格式");
        assertEquals(64, opts.truncateLimit(), "截断阈值缺省 64（engine ConsoleRenderer 契约）");
    }

    /** 显式 pg 档 + 缺省值显式书写 → 与缺键同值（"缺省 = 显式 pg"的等价面）。 */
    @Test
    void explicitPgTokensEqualDefaults() {
        Properties cfg = new Properties();
        cfg.setProperty(RenderOptions.KEY_FLOAT_STYLE, "pg");
        cfg.setProperty(RenderOptions.KEY_DATE_FORMAT, "pg");
        cfg.setProperty(RenderOptions.KEY_TIME_FORMAT, "pg");
        cfg.setProperty(RenderOptions.KEY_TRUNCATE, "64");
        assertEquals(RenderOptions.fromProperties(cfg), RenderOptions.PG,
                "显式缺省值与缺键产物逐字段等价");
    }

    /** plain 浮点档 + 自定义 pattern + 阈值 0（不截断）的全开档解析。 */
    @Test
    void plainFloatCustomPatternsAndUnlimitedTruncateParse() {
        Properties cfg = new Properties();
        cfg.setProperty(RenderOptions.KEY_FLOAT_STYLE, "plain");
        cfg.setProperty(RenderOptions.KEY_DATE_FORMAT, "yyyy/MM/dd HH:mm");
        cfg.setProperty(RenderOptions.KEY_TIME_FORMAT, "HHmmss");
        cfg.setProperty(RenderOptions.KEY_TRUNCATE, "0");
        RenderOptions opts = RenderOptions.fromProperties(cfg);
        assertEquals(RenderOptions.FloatStyle.PLAIN, opts.floatStyle(), "plain 档");
        assertEquals("2026/10/07 12:34",
                opts.dateFormat().format(java.time.LocalDateTime.of(2026, 10, 7, 12, 34)),
                "date pattern 经 DateTimeFormatter 解析（formatter 无 equals——以样本格式化结果断言）");
        assertEquals("123456", opts.timeFormat().format(java.time.LocalTime.of(12, 34, 56)),
                "time pattern");
        assertEquals(0, opts.truncateLimit(), "0 = 不截断全量输出");
    }

    /** default 内建档：免写 pattern 的推荐格式（date 族含秒、time 族纯时分秒）。 */
    @Test
    void defaultTokenMapsToBuiltInRecommendedPatterns() {
        Properties cfg = new Properties();
        cfg.setProperty(RenderOptions.KEY_DATE_FORMAT, "default");
        cfg.setProperty(RenderOptions.KEY_TIME_FORMAT, "default");
        RenderOptions opts = RenderOptions.fromProperties(cfg);
        assertEquals("2026-10-07 12:34:56",
                opts.dateFormat().format(java.time.LocalDateTime.of(2026, 10, 7, 12, 34, 56)),
                "default 档 date = " + RenderOptions.DEFAULT_DATE_PATTERN);
        assertEquals("12:34:56", opts.timeFormat().format(java.time.LocalTime.of(12, 34, 56)),
                "default 档 time = " + RenderOptions.DEFAULT_TIME_PATTERN);
    }

    /** 非法值面：未知 float 档位 / 非法 pattern / 负阈值各 IAE 附键名。 */
    @Test
    void invalidValuesFailFastWithKeyName() {
        Properties badFloat = new Properties();
        badFloat.setProperty(RenderOptions.KEY_FLOAT_STYLE, "decimal");
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
                () -> RenderOptions.fromProperties(badFloat));
        assertTrue(e1.getMessage().contains(RenderOptions.KEY_FLOAT_STYLE),
                "消息附键名: " + e1.getMessage());

        Properties badPattern = new Properties();
        badPattern.setProperty(RenderOptions.KEY_DATE_FORMAT, "yyyy-MM-dd'");   // 未闭合引号——非法 pattern
        IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class,
                () -> RenderOptions.fromProperties(badPattern));
        assertTrue(e2.getMessage().contains(RenderOptions.KEY_DATE_FORMAT),
                "非法 pattern 附键名: " + e2.getMessage());

        Properties badTruncate = new Properties();
        badTruncate.setProperty(RenderOptions.KEY_TRUNCATE, "-1");
        IllegalArgumentException e3 = assertThrows(IllegalArgumentException.class,
                () -> RenderOptions.fromProperties(badTruncate));
        assertTrue(e3.getMessage().contains(RenderOptions.KEY_TRUNCATE),
                "负阈值附键名: " + e3.getMessage());

        Properties notNumber = new Properties();
        notNumber.setProperty(RenderOptions.KEY_TRUNCATE, "big");
        assertThrows(IllegalArgumentException.class, () -> RenderOptions.fromProperties(notNumber),
                "非数字阈值 fail-fast（NumberFormatException 同属 IAE 家族语义）");
    }

    /** 空串视为未设（与门面 collectConfig"空串=未设"语义对齐）。 */
    @Test
    void blankValuesAreTreatedAsUnset() {
        Properties cfg = new Properties();
        cfg.setProperty(RenderOptions.KEY_FLOAT_STYLE, "");
        cfg.setProperty(RenderOptions.KEY_DATE_FORMAT, "");
        RenderOptions opts = RenderOptions.fromProperties(cfg);
        assertEquals(RenderOptions.PG, opts, "空串键等价缺键（缺省档）");
    }
}
