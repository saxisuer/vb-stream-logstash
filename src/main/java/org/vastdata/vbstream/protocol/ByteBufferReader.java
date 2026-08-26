package org.vastdata.vbstream.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** 按协议字节序（big-endian）逐字段读取 pgoutput 消息体的工具。非线程安全，每条消息新建。 */
public final class ByteBufferReader {

    /** PostgreSQL 时间纪元：2000-01-01 00:00:00 UTC 相对 Unix 纪元的秒数。 */
    private static final long PG_EPOCH_SECONDS = 946684800L;

    private final ByteBuffer buf;

    public ByteBufferReader(ByteBuffer buf) {
        this.buf = buf;
    }

    public int remaining() {
        return buf.remaining();
    }

    public byte readByte() {
        return buf.get();
    }

    public int readUnsignedByte() {
        return buf.get() & 0xFF;
    }

    public int readUnsignedShort() {
        return buf.getShort() & 0xFFFF;
    }

    public int readInt() {
        return buf.getInt();
    }

    /** 读无符号 32 位（xid 等），装入 long 避免负数。 */
    public long readUnsignedInt() {
        return buf.getInt() & 0xFFFFFFFFL;
    }

    public long readLong() {
        return buf.getLong();
    }

    /** 读 null 结尾 UTF-8 字符串（CString）。 */
    public String readString() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte b;
        while ((b = buf.get()) != 0) {
            out.write(b);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    public byte[] readBytes(int len) {
        byte[] arr = new byte[len];
        buf.get(arr);
        return arr;
    }

    /** PG 微秒时间戳 → Instant。 */
    public static Instant pgMicrosToInstant(long micros) {
        long seconds = PG_EPOCH_SECONDS + Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
