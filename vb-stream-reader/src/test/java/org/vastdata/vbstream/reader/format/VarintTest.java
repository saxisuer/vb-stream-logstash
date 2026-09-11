package org.vastdata.vbstream.reader.format;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link Varint}（LEB128）编解码单测：无符号/zigzag 的 roundtrip、边界值、已知字节形态
 * （锚定跨仓契约——vb-cdc-file-transform 的 cdc-file-format 同款编码）。
 */
class VarintTest {

    /**
     * 场景:无符号 roundtrip——0、单字节上限 127、双字节起点 128、int 域边界、long 上限,
     * 以及 7 位步进处的代表性值。
     */
    @Test
    void unsignedRoundTripAcrossBoundaries() throws IOException {
        long[] values = {0, 1, 127, 128, 255, 256, 16_383, 16_384, Integer.MAX_VALUE,
                (1L << 35) - 1, Long.MAX_VALUE};
        for (long v : values) {
            assertEquals(v, readBackUnsigned(v), "无符号 roundtrip: " + v);
        }
    }

    /**
     * 场景:已知字节形态——0 单字节 0x00、1 单字节 0x01、300 编码为小端序双字节
     * {@code 0xAC 0x02}(LEB128 规范例,锚定字节布局防实现漂移)。
     */
    @Test
    void knownByteLayouts() throws IOException {
        assertArrayEquals(new byte[]{0x00}, encodeUnsigned(0));
        assertArrayEquals(new byte[]{0x01}, encodeUnsigned(1));
        assertArrayEquals(new byte[]{(byte) 0xAC, 0x02}, encodeUnsigned(300));
    }

    /** 场景:zigzag roundtrip——0/±1/Long 最小最大(zigzag 的设计边界,负数映射到奇数域)。 */
    @Test
    void zigzagRoundTripIncludingExtremes() throws IOException {
        long[] values = {0, 1, -1, 63, -64, Long.MAX_VALUE, Long.MIN_VALUE};
        for (long v : values) {
            assertEquals(v, readBackZigzag(v), "zigzag roundtrip: " + v);
        }
    }

    /** 场景:zigzag 已知形态——0→0、-1→1、1→2(负数映射奇数、非负映射偶数)。 */
    @Test
    void zigzagKnownEncodings() throws IOException {
        assertArrayEquals(new byte[]{0x00}, encodeZigzag(0));
        assertArrayEquals(new byte[]{0x01}, encodeZigzag(-1));
        assertArrayEquals(new byte[]{0x02}, encodeZigzag(1));
    }

    /** 场景:无符号编码拒绝负值(IAE fail-fast——长度/个数域外的信号)。 */
    @Test
    void unsignedRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () ->
                Varint.writeUnsigned(new DataOutputStream(new ByteArrayOutputStream()), -1));
    }

    /** 场景:超过 10 字节的残缺流(继续置高位)读侧抛 IOException(数据损坏信号)。 */
    @Test
    void overlongStreamRejected() {
        byte[] overlong = new byte[11];
        java.util.Arrays.fill(overlong, (byte) 0x80);
        overlong[10] = 0x00;
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(overlong));
        assertThrows(IOException.class, () -> Varint.readUnsigned(in));
    }

    private static byte[] encodeUnsigned(long v) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        Varint.writeUnsigned(new DataOutputStream(buf), v);
        return buf.toByteArray();
    }

    private static byte[] encodeZigzag(long v) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        Varint.writeZigzag(new DataOutputStream(buf), v);
        return buf.toByteArray();
    }

    private static long readBackUnsigned(long v) throws IOException {
        return Varint.readUnsigned(new DataInputStream(new ByteArrayInputStream(encodeUnsigned(v))));
    }

    private static long readBackZigzag(long v) throws IOException {
        return Varint.readZigzag(new DataInputStream(new ByteArrayInputStream(encodeZigzag(v))));
    }
}
