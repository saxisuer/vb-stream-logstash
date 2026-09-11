package org.vastdata.vbstream.format;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * LEB128 变长整数编码，小端序字节组（VBFG 落地文件的基础编码）。
 * <ul>
 *   <li>{@link #writeUnsigned}：无符号，用于长度、个数等非负值</li>
 *   <li>{@link #writeZigzag}：zigzag + 无符号，用于有符号数值（txid/lsn/时间戳）</li>
 * </ul>
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 io.Varint（2026-09-11 快照）——
 * 格式契约跨仓必须逐字节一致，升级格式须两仓同步递增 {@code ChangeFileWriter.VERSION}。
 */
public final class Varint {

    private Varint() {
    }

    /** 责任：写无符号 varint。边界：负值抛 IAE（无符号编码域外）。 */
    public static void writeUnsigned(DataOutput out, long value) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException("无符号 varint 不接受负数: " + value);
        }
        writeRaw(out, value);
    }

    /** 裸 LEB128 写入，接受负 long 位型（zigzag 编码 Long.MIN_VALUE 会得到全 1 位型）。 */
    private static void writeRaw(DataOutput out, long value) throws IOException {
        while (true) {
            if ((value & ~0x7FL) == 0) {
                out.writeByte((int) value);
                return;
            }
            out.writeByte((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
    }

    /** 责任：读无符号 varint。边界：超过 64 位（10 字节）视为数据损坏抛 IOException。 */
    public static long readUnsigned(DataInput in) throws IOException {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            byte b = in.readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IOException("varint 超过 64 位，数据可能损坏");
    }

    /** zigzag 编码有符号值：非负映射为偶数、负值映射为奇数，小绝对值占用字节最少。 */
    public static void writeZigzag(DataOutput out, long value) throws IOException {
        writeRaw(out, (value << 1) ^ (value >> 63));
    }

    /** zigzag 解码（{@link #writeZigzag} 的逆）。 */
    public static long readZigzag(DataInput in) throws IOException {
        long encoded = readUnsigned(in);
        return (encoded >>> 1) ^ -(encoded & 1);
    }
}
