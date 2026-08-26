package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ByteBufferReaderTest {

    @Test
    void readsBigEndianIntegers() {
        // 1 + 2 + 4 + 4 = 11 字节（原计划 allocate(10) 容量不足，putInt(4) 时 BufferOverflow）
        ByteBuffer buf = ByteBuffer.allocate(11)
                .put((byte) 1).putShort((short) 2).putInt(3).putInt(4);
        buf.flip();
        ByteBufferReader r = new ByteBufferReader(buf);
        assertEquals(1, r.readByte());
        assertEquals(2, r.readUnsignedShort());
        assertEquals(3, r.readInt());
        assertEquals(4L, r.readUnsignedInt()); // unsigned 32 位保进 long
    }

    @Test
    void readsCStringAsUtf8() {
        ByteBuffer buf = ByteBuffer.allocate(16).put("你好".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .put((byte) 0).put((byte) 'x');
        buf.flip();
        ByteBufferReader r = new ByteBufferReader(buf);
        assertEquals("你好", r.readString());
        assertEquals('x', r.readByte());
    }

    @Test
    void convertsPgEpochMicrosToInstant() {
        // PG epoch 2000-01-01 00:00:00 UTC = Unix 946684800 秒；1 秒 = 1e6 微秒
        Instant expected = Instant.ofEpochSecond(946684800L + 100, 500_000_000L);
        assertEquals(expected, ByteBufferReader.pgMicrosToInstant(100_500_000L));
    }
}
