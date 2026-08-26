package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TwoPhaseParsersTest {

    private final PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.PARALLEL);

    /** PG epoch + 42 秒的微秒形式（注意：输入语义是 epoch 起的微秒数，勿再叠 epoch） */
    private static final long MICROS = 42_000_000L;

    @Test
    void beginPrepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('b')
                .i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.BeginPrepare msg = (PgOutputMessage.BeginPrepare) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareLsn());
        assertEquals(0x200L, msg.endLsn());
        assertEquals(Instant.ofEpochSecond(946684800L + 42), msg.prepareTimestamp());
        assertEquals(909L, msg.xid());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void prepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('P')
                .i8(0).i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.Prepare msg = (PgOutputMessage.Prepare) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareLsn());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void commitPrepared() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('K')
                .i8(0).i64(0x300L).i64(0x400L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.CommitPrepared msg = (PgOutputMessage.CommitPrepared) decoder.decode(payload);
        assertEquals(0x300L, msg.commitLsn());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void rollbackPreparedHasTwoTimestamps() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('r')
                .i8(0).i64(0x100L).i64(0x500L).i64(MICROS).i64(MICROS + 10).i32(909).str("gid_1").build();
        PgOutputMessage.RollbackPrepared msg = (PgOutputMessage.RollbackPrepared) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareEndLsn());
        assertEquals(0x500L, msg.rollbackEndLsn());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void streamPrepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('p')
                .i8(0).i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        assertInstanceOf(PgOutputMessage.StreamPrepare.class, decoder.decode(payload));
    }
}
