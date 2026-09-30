package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MessagePreview} 日志预览纯函数单测:可打印 text 走截断形态(64 字符边界)、
 * 含非可打印字节走 bytea 十六进制形态(32 字节边界)——两形态的判定规则与截断标记
 * 是逻辑消息日志(spec §3.2 的 content 预览)的固定输出口径,错位会让日志留痕失去
 * 排障价值或泄露超长载荷。纯函数直驱,零 PG 零容器。
 */
class MessagePreviewTest {

    /** 生成 n 个重复字符的 ASCII 字节数组(全可打印,text 路径的标准载荷)。 */
    private static byte[] ascii(String s, int n) {
        byte[] out = new byte[n];
        Arrays.fill(out, (byte) s.charAt(0));
        return out;
    }

    /** 生成 n 个 0xAB 字节的载荷(0xAB 超出可打印 ASCII 区间,bytea 路径的标准载荷)。 */
    private static byte[] nonPrintable(int n) {
        byte[] out = new byte[n];
        Arrays.fill(out, (byte) 0xAB);
        return out;
    }

    /**
     * 纯 text 形态:全部字节落在可打印 ASCII 区间(0x20..0x7E)时按原字符输出;
     * 长度 ≤64 完整输出、恰好 64 不截断(边界含端)、65 起截前 64 并附
     * "...(65B)" 原长标记——与引擎 ConsoleRenderer 文本值渲染同款截断形态。
     */
    @Test
    void printableAsciiRendersAsTextWithSixtyFourCharBoundary() {
        assertEquals("hello", MessagePreview.preview("hello".getBytes(StandardCharsets.US_ASCII)),
                "短可打印载荷应原样输出");
        assertEquals("a".repeat(64), MessagePreview.preview(ascii("a", 64)),
                "恰好 64 字符应完整输出不截断");
        assertEquals("a".repeat(64) + "...(65B)", MessagePreview.preview(ascii("a", 65)),
                "65 字符应截前 64 并附原长标记");
    }

    /**
     * bytea 形态:载荷含任一非可打印字节(控制字符 0x00 或高位字节 0xAB)即整条走
     * 十六进制,输出前 32 字节的 hex;恰好 32 字节不截断、33 字节起附 "...(33B)" 原长
     * 标记——判定是整条口径(混合内容不逐段切换,日志行保持单形态可读)。
     */
    @Test
    void nonPrintableBytesRenderAsHexWithThirtyTwoByteBoundary() {
        assertEquals("ab", MessagePreview.preview(new byte[]{ (byte) 0xAB }),
                "单个非可打印字节即触发 hex 形态");
        assertEquals("00", MessagePreview.preview(new byte[]{ 0x00 }),
                "控制字符同样触发 hex 形态");
        String full = MessagePreview.preview(nonPrintable(32));
        assertTrue(full.startsWith("ab".repeat(32)) && full.length() == "ab".repeat(32).length(),
                "恰好 32 字节应完整输出 hex 不截断");
        assertEquals("ab".repeat(32) + "...(33B)", MessagePreview.preview(nonPrintable(33)),
                "33 字节应截前 32 字节 hex 并附原长标记");
    }

    /**
     * 边界长度与判定口径补钉:空载荷预览为空串(零字节 vacuously 可打印,走 text 路径);
     * 可打印区间的两端字符(空格 0x20 与 '~' 0x7E)走 text 而区间外紧邻字节(0x1F/0x7F)
     * 走 hex——判定边界必须精确落在 0x20..0x7E 闭区间上。
     */
    @Test
    void boundaryLengthsAndPrintableRangeEdges() {
        assertEquals("", MessagePreview.preview(new byte[0]), "空载荷预览应为空串");
        assertEquals("~ ~", MessagePreview.preview(new byte[]{ 0x7E, 0x20, 0x7E }),
                "区间两端(0x20 空格、0x7E '~')属可打印,走 text");
        assertEquals("1f", MessagePreview.preview(new byte[]{ 0x1F }),
                "0x1F(区间下端外紧邻)应走 hex");
        assertEquals("7f", MessagePreview.preview(new byte[]{ 0x7F }),
                "0x7F(区间上端外紧邻,DEL)应走 hex");
        byte[] mixed = new byte[64];
        Arrays.fill(mixed, (byte) 'x');
        mixed[63] = 0x00;   // 64 个字节里仅最后一个非可打印:整条切到 hex 形态
        assertEquals("78".repeat(32) + "...(64B)", MessagePreview.preview(mixed),
                "判定按整条:尾部单个非可打印字节即整条 hex(前 32 字节 + 原长标记)");
    }
}
