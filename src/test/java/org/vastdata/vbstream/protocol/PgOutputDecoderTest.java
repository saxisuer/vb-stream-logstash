package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    void insertDispatchesToDmlParser() throws IOException {
        // 'I' 已由 Task 5 实现：dispatch 应正常解析出 Insert 消息（占位接线用例的最终形态）
        ByteBuffer payload = new MsgBuilder().type('I').i32(1).i8('N')
                .i16(1).i8('t').bytes("x".getBytes(java.nio.charset.StandardCharsets.UTF_8)).build();
        PgOutputMessage msg = new PgOutputDecoder(StreamingMode.OFF).decode(payload);
        assertInstanceOf(PgOutputMessage.Insert.class, msg);
    }
}
