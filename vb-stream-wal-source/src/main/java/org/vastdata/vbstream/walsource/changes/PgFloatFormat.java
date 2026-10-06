package org.vastdata.vbstream.walsource.changes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * PG 风格浮点文本格式化器（Task 9）——Java float/double → PG 服务端
 * {@code float8out}/{@code float4out} 的逐字同形文本，双路对拍的浮点值对齐前提
 * （engine 逻辑解码 text 档的浮点值由服务端渲染，wal 直解路必须在本侧复刻同一形态，
 * 补上 {@code Double.toString} 的 {@code 1.0E20} vs PG {@code 1e+20} 分叉——Task 2
 * 疑虑 2 记档的遗留项）。
 *
 * <p><b>规则面（REL_18_STABLE 源码钉 + docker 实测锚，全文见 PgFloatFormatTest）</b>：
 * 数字位串取<b>最短往返表示</b>（PG 的 Ryū 同义——本文经 BigDecimal 逐精度试探取
 * 最小位数的往返十进制）；display exponent（值 = d.ddd × 10^exp 的 exp）落在定点门限
 * 内打定点、否则打科学计数法 {@code d[.ddd]e±XX}（指数恒带符号、至少两位、≥100 三位
 * 自然宽）。门限取 printf {@code %g} 缺省（源码注释原话 "thresholds for fixed-point
 * output are chosen to match printf defaults"）：
 * <b>float8（d2s.c）：{@code -4 <= exp < 15}；float4（f2s.c）：{@code -4 <= exp < 6}</b>
 * （float4 低一档，故 {@code 1e6::float4} 已是 {@code 1e+06} 而 float8 的
 * {@code 1000000} 仍定点）。零打 {@code 0}、负零保留符号 {@code -0}；NaN/±Infinity
 * 打 {@code NaN}/{@code Infinity}/{@code -Infinity}（float8out 的特值前置分支）。</p>
 *
 * <p>定点形态细则（对齐 d2s.c {@code to_chars_df}）：无尾随 {@code .0}（整数尾随零
 * 直接省略，{@code 1} 不打 {@code 1.0}）；纯小数在 exp&lt;0 时前置 {@code 0.}（如
 * {@code 0.0001}）。科学计数法形态：首位数字 + 可选 {@code .ddd} + {@code e} + 符号
 * + 两位以上指数（{@code 4.5e-08}/{@code 1e+20}/{@code 5e-324}）。</p>
 *
 * <p>线程约束：静态纯函数、并发安全（无共享可变状态）。JDK 17 的
 * {@code Double.toString} 不保证最短（JDK-4511638，19 才修）且形态分叉，故不得用于
 * 本面。</p>
 */
public final class PgFloatFormat {

    /** float8 定点门限上限（display exponent &lt; 15 走定点——d2s.c to_chars）。 */
    private static final int FLOAT8_FIXED_HIGH = 15;

    /** float4 定点门限上限（display exponent &lt; 6 走定点——f2s.c to_chars）。 */
    private static final int FLOAT4_FIXED_HIGH = 6;

    /** 定点门限下限（两侧同为 -4——%g 缺省）。 */
    private static final int FIXED_LOW = -4;

    /** 最短往返试探的位宽上限（IEEE-754 binary64 最短往返 ≤17 位、binary32 ≤9 位）。 */
    private static final int MAX_DOUBLE_DIGITS = 17;

    /**
     * 工具类防实例化。
     */
    private PgFloatFormat() {
    }

    /**
     * float8（double）→ PG {@code float8out} 逐字同形文本。
     *
     * <p>关键步骤：NaN/±Infinity 特值前置（float8out 分支）；零/负零按符号位打
     * {@code 0}/{@code -0}；其余取符号后经 {@link #formatUnsigned} 以门限
     * {@code [-4, 15)} 排版。边界与异常语义：无（任意 double 值均有确定形态）。
     * 线程约束：纯函数。</p>
     *
     * @param v 双精度值
     * @return PG 文本形态（如 {@code 1e+20} / {@code 0.0001} / {@code -0}）
     */
    public static String format(double v) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        if (v == Double.POSITIVE_INFINITY) {
            return "Infinity";
        }
        if (v == Double.NEGATIVE_INFINITY) {
            return "-Infinity";
        }
        if (v == 0d) {
            return Double.doubleToRawLongBits(v) < 0 ? "-0" : "0";
        }
        boolean negative = v < 0;
        double abs = Math.abs(v);
        return formatUnsigned(shortestDigits(abs, MAX_DOUBLE_DIGITS, false),
                negative, FLOAT8_FIXED_HIGH);
    }

    /**
     * float4（float）→ PG {@code float4out} 逐字同形文本。
     *
     * <p>关键步骤：同 {@link #format(double)} 的特值/零分支；最短往返按 float 位宽
     * （≤9 位、往返校验 {@code BigDecimal.floatValue()}）+ 门限 {@code [-4, 6)}。
     * 边界与异常语义：无。线程约束：纯函数。</p>
     *
     * @param v 单精度值
     * @return PG 文本形态（如 {@code 1e+06} / {@code 3.4028235e+38}）
     */
    public static String format(float v) {
        if (Float.isNaN(v)) {
            return "NaN";
        }
        if (v == Float.POSITIVE_INFINITY) {
            return "Infinity";
        }
        if (v == Float.NEGATIVE_INFINITY) {
            return "-Infinity";
        }
        if (v == 0f) {
            return Float.floatToRawIntBits(v) < 0 ? "-0" : "0";
        }
        boolean negative = v < 0;
        float abs = Math.abs(v);
        return formatUnsigned(shortestDigits(abs, 9, true), negative, FLOAT4_FIXED_HIGH);
    }

    /**
     * 最短往返十进制位串：自 1 位精度起以 HALF_EVEN 取最近十进制（BigDecimal 构造
     * 自浮点的<b>精确二进制值</b>——float 先经 double 无损展宽），首个满足回读相等
     * （float 则 {@code floatValue()}）的精度即最短（Ryū 同义；Ryū 保证存在性，故
     * 逐位试探必在位宽上限内命中，兜底返回精确展开不作额外防御）。
     *
     * <p>极小精度即可往返的值天然无尾随零（尾随零位降一位精度后值不变、与"首个命中"
     * 矛盾），与 Ryū 科学计数法分支的尾随零剥离殊途同归。线程约束：纯函数。</p>
     *
     * @param abs         绝对值（非零非特值）
     * @param maxDigits   位宽上限（double 17 / float 9）
     * @param asFloat     true = float4 面（往返校验走 floatValue）
     * @return 最短往返十进制（正的、无指数形态）
     */
    private static BigDecimal shortestDigits(double abs, int maxDigits, boolean asFloat) {
        for (int precision = 1; precision <= maxDigits; precision++) {
            BigDecimal bd = new BigDecimal(abs, new MathContext(precision, RoundingMode.HALF_EVEN));
            boolean roundTrips = asFloat ? bd.floatValue() == (float) abs : bd.doubleValue() == abs;
            if (roundTrips) {
                return bd;
            }
        }
        return new BigDecimal(abs);
    }

    /**
     * 最短位串 → PG 文本排版：display exponent（= precision - 1 - scale）落在
     * {@code [FIXED_LOW, fixedHigh)} 内打定点（整数尾随零/纯小数前置 {@code 0.}），
     * 否则科学计数法（首位 + {@code .ddd} + {@code e±XX}）。
     *
     * <p>线程约束：纯函数。</p>
     *
     * @param shortest  最短往返十进制（正值）
     * @param negative 负号标记
     * @param fixedHigh 定点门限上限（float8 15 / float4 6）
     * @return PG 文本形态
     */
    private static String formatUnsigned(BigDecimal shortest, boolean negative, int fixedHigh) {
        BigInteger unscaled = shortest.unscaledValue();
        String digits = unscaled.toString();
        int scale = shortest.scale();
        int exp = (digits.length() - 1) - scale;   // display exponent：首位数字的十进制幂
        StringBuilder out = new StringBuilder(digits.length() + 8);
        if (negative) {
            out.append('-');
        }
        if (exp >= FIXED_LOW && exp < fixedHigh) {
            int point = exp + 1;                   // 小数点前的数字位数
            if (point <= 0) {
                out.append("0.").append("0".repeat(-point)).append(digits);
            } else if (point >= digits.length()) {
                out.append(digits).append("0".repeat(point - digits.length()));
            } else {
                out.append(digits, 0, point).append('.').append(digits.substring(point));
            }
        } else {
            out.append(digits.charAt(0));
            if (digits.length() > 1) {
                out.append('.').append(digits.substring(1));
            }
            out.append('e').append(exp < 0 ? '-' : '+');
            int e = Math.abs(exp);
            if (e >= 100) {
                out.append(e);
            } else {
                out.append(e < 10 ? "0" : "").append(e);
            }
        }
        return out.toString();
    }
}
