package org.vastdata.vbstream.reader.format;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 按列类型编码/解码单个值。null 不在此处理（由 EVENT 记录的 null 位图表达，只序列化非 null 值）。
 * 时间类型（TIMESTAMP/TIMESTAMPTZ/DATE/TIME/INTERVAL）的载荷布局与 STRING 同构
 * （类型差异只进 TABLE_DEF，不改变 EVENT 值编码）。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 io.Values（2026-09-11 快照）。
 */
public final class Values {

    private Values() {
    }

    /** 责任：按 {@link TypeCode} 编码单个非 null 值（STRING/时间走 UTF-8 长度前缀字节串）。 */
    public static void write(DataOutput out, TypeCode type, Object value) throws IOException {
        switch (type) {
            case BOOL -> out.writeBoolean((Boolean) value);
            case INT -> Varint.writeZigzag(out, ((Number) value).longValue());
            case FLOAT32 -> out.writeFloat((Float) value);
            case FLOAT64 -> out.writeDouble((Double) value);
            case STRING -> writeBytes(out, value.toString().getBytes(StandardCharsets.UTF_8));
            case BYTES -> writeBytes(out, (byte[]) value);
            case TIMESTAMP, TIMESTAMPTZ, DATE, TIME, INTERVAL ->
                    writeBytes(out, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    /** 责任：按 {@link TypeCode} 解码单个值（{@link #write} 的逆）。 */
    public static Object read(DataInput in, TypeCode type) throws IOException {
        return switch (type) {
            case BOOL -> in.readBoolean();
            case INT -> Varint.readZigzag(in);
            case FLOAT32 -> in.readFloat();
            case FLOAT64 -> in.readDouble();
            case STRING -> new String(readBytes(in), StandardCharsets.UTF_8);
            case BYTES -> readBytes(in);
            case TIMESTAMP, TIMESTAMPTZ, DATE, TIME, INTERVAL ->
                    new String(readBytes(in), StandardCharsets.UTF_8);
        };
    }

    private static void writeBytes(DataOutput out, byte[] bytes) throws IOException {
        Varint.writeUnsigned(out, bytes.length);
        out.write(bytes);
    }

    /** 责任：读长度前缀字节串。边界：长度超 int 域视为损坏抛 IOException。 */
    private static byte[] readBytes(DataInput in) throws IOException {
        long len = Varint.readUnsigned(in);
        if (len > Integer.MAX_VALUE) {
            throw new IOException("字节长度超限: " + len);
        }
        byte[] bytes = new byte[(int) len];
        in.readFully(bytes);
        return bytes;
    }
}
