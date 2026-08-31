package org.vastdata.vbstream.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** 测试用 pgoutput 消息样本构造器。写入均为 big-endian。 */
public final class MsgBuilder {

    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private final DataOutputStream out = new DataOutputStream(buf);

    public MsgBuilder type(char t) {
        buf.write(t);
        return this;
    }

    public MsgBuilder i8(int v) {
        buf.write(v);
        return this;
    }

    public MsgBuilder i16(int v) throws IOException {
        out.writeShort(v);
        return this;
    }

    public MsgBuilder i32(int v) throws IOException {
        out.writeInt(v);
        return this;
    }

    public MsgBuilder i64(long v) throws IOException {
        out.writeLong(v);
        return this;
    }

    public MsgBuilder str(String s) throws IOException {
        buf.write(s.getBytes(StandardCharsets.UTF_8));
        buf.write(0);
        return this;
    }

    public MsgBuilder bytes(byte[] arr) throws IOException {
        out.writeInt(arr.length);
        out.write(arr);
        return this;
    }

    public ByteBuffer build() throws IOException {
        out.flush();
        return ByteBuffer.wrap(buf.toByteArray());
    }
}
