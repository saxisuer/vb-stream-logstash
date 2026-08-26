package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamParsersTest {

    @Test
    void streamStartStopToggleInStreamState() throws IOException {
        PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.ON);
        decoder.decode(new MsgBuilder().type('S').i32(505).i8(1).build());
        // inStream 置位后，流块内的 I 消息会先读 Int32 xid 前缀
        Object inBlock = decoder.decode(new MsgBuilder().type('I')
                .i32(505).i32(16385).i8('N').i16(0).build());
        assertInstanceOf(PgOutputMessage.Insert.class, inBlock);
        assertTrue(((PgOutputMessage.Insert) inBlock).streamXid().isPresent());
        assertEquals(505L, ((PgOutputMessage.Insert) inBlock).streamXid().getAsLong());

        decoder.decode(new MsgBuilder().type('E').build());
        Object afterStop = decoder.decode(new MsgBuilder().type('I')
                .i32(16385).i8('N').i16(0).build());
        assertFalse(((PgOutputMessage.Insert) afterStop).streamXid().isPresent());
    }

    @Test
    void streamCommit() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('c')
                .i32(505).i8(0).i64(0x6000L).i64(0x7000L).i64(946684800_000_000L).build();
        PgOutputMessage.StreamCommit msg = (PgOutputMessage.StreamCommit)
                new PgOutputDecoder(StreamingMode.ON).decode(payload);
        assertEquals(505L, msg.xid());
        assertEquals(0x6000L, msg.commitLsn());
        assertEquals(0x7000L, msg.endLsn());
    }

    @Test
    void streamAbortWithoutParallelExtra() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A')
                .i32(505).i32(606).build();
        PgOutputMessage.StreamAbort msg = (PgOutputMessage.StreamAbort)
                new PgOutputDecoder(StreamingMode.ON).decode(payload);
        assertEquals(505L, msg.xid());
        assertEquals(606L, msg.subxid());
        assertFalse(msg.abortLsn().isPresent());
    }

    @Test
    void streamAbortWithParallelExtra() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A')
                .i32(505).i32(505).i64(0x8000L).i64(123_456L).build();
        PgOutputMessage.StreamAbort msg = (PgOutputMessage.StreamAbort)
                new PgOutputDecoder(StreamingMode.PARALLEL).decode(payload);
        assertEquals(0x8000L, msg.abortLsn().getAsLong());
        assertEquals(123_456L, msg.abortTimestamp().getAsLong());
    }

    @Test
    void streamAbortInParallelModeRequiresExtraBytes() throws IOException {
        // parallel 模式下不足 16 字节附加字段会 BufferUnderflow，验证按模式分支解析
        ByteBuffer payload = new MsgBuilder().type('A').i32(1).i32(1).build();
        assertThrows(java.nio.BufferUnderflowException.class,
                () -> new PgOutputDecoder(StreamingMode.PARALLEL).decode(payload));
    }

    @Test
    void streamStopIsFieldless() throws IOException {
        Object msg = new PgOutputDecoder(StreamingMode.ON)
                .decode(new MsgBuilder().type('E').build());
        assertInstanceOf(PgOutputMessage.StreamStop.class, msg);
    }
}
