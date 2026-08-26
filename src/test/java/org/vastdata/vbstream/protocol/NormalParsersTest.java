package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NormalParsersTest {

    private final PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.OFF);

    @Test
    void begin() throws IOException {
        // commit_ts 为 PG epoch(2000-01-01) 起的微秒数：2_500_000µs = epoch+2.5s（勿再叠加 epoch 秒数）
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(0x1000L).i64(2_500_000L).i32(777).build();
        PgOutputMessage.Begin msg = (PgOutputMessage.Begin) decoder.decode(payload);
        assertEquals(0x1000L, msg.finalLsn());
        assertEquals(Instant.ofEpochSecond(946684800L + 2, 500_000_000L), msg.commitTimestamp());
        assertEquals(777L, msg.xid());
    }

    @Test
    void commitConsumesLeadingFlagsByte() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('C')
                .i8(0).i64(0x2000L).i64(0x3000L).i64(1_000_000L).build();
        PgOutputMessage.Commit msg = (PgOutputMessage.Commit) decoder.decode(payload);
        assertEquals(0x2000L, msg.commitLsn());
        assertEquals(0x3000L, msg.endLsn());
    }

    @Test
    void origin() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('O').i64(0x4000L).str("origin_a").build();
        PgOutputMessage.Origin msg = (PgOutputMessage.Origin) decoder.decode(payload);
        assertEquals("origin_a", msg.originName());
    }

    @Test
    void relationWithTwoColumns() throws IOException {
        MsgBuilder m = new MsgBuilder().type('R')
                .i32(16385).str("public").str("t_demo").i8('d')
                .i16(2)
                .i8(1).str("id").i32(23).i32(-1)   // int4, key, typmod=-1
                .i8(0).str("name").i32(25).i32(-1); // text, 非key
        PgOutputMessage.Relation msg = (PgOutputMessage.Relation) decoder.decode(m.build());
        assertEquals(16385, msg.relationOid());
        assertEquals("t_demo", msg.table());
        assertEquals('d', msg.replicaIdentity());
        assertEquals(2, msg.columns().size());
        assertEquals(new Column("id", 23, -1, true), msg.columns().get(0));
        assertEquals(new Column("name", 25, -1, false), msg.columns().get(1));
    }

    @Test
    void typeMsg() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('Y').i32(16386).str("public").str("mytype").build();
        PgOutputMessage.Type msg = (PgOutputMessage.Type) decoder.decode(payload);
        assertEquals("mytype", msg.name());
    }

    @Test
    void leftoverBytesCauseMisalignment() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(1).i64(2).i32(3).i32(99).build(); // 故意多 4 字节
        assertThrows(ProtocolMisalignmentException.class, () -> decoder.decode(payload));
    }
}
