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
        // 'I' 属 DmlParsers，Task 3 阶段为占位实现；Task 5 完成后请将此用例改为断言正常解析
        ByteBuffer payload = new MsgBuilder().type('I').i32(1).i8('N')
                .i16(1).i8('t').bytes("x".getBytes()).build();
        assertThrows(UnsupportedOperationException.class,
                () -> new PgOutputDecoder(StreamingMode.OFF).decode(payload));
    }
}
