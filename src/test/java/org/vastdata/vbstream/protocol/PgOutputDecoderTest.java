package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgOutputDecoderTest {

    @Test
    void unknownTypeByteFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('X').i32(1).build();
        UnknownMessageTypeException ex = assertThrows(UnknownMessageTypeException.class,
                () -> new PgOutputDecoder(StreamingMode.OFF).decode(payload));
        assertTrue(ex.getMessage().contains("0x58"), "异常应含字节十六进制值: " + ex.getMessage());
    }

    @Test
    void placeholderParserWired() throws IOException {
        // Task 3 阶段各族 parser 为占位实现；此用例锁定 dispatch 已接线（实现后此用例仍应通过）
        ByteBuffer payload = new MsgBuilder().type('B').i64(1).i64(2).i32(3).build();
        assertThrows(UnsupportedOperationException.class,
                () -> new PgOutputDecoder(StreamingMode.OFF).decode(payload));
    }
}
