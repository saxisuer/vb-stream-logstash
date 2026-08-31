package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DmlParsersTest {

    private final PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.OFF);

    private static byte[] utf8(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void insertWithTextAndNullColumns() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(16385).i8('N')
                .i16(3)
                .i8('t').bytes(utf8("a"))
                .i8('n')
                .i8('t').bytes(utf8("b"))
                .build();
        PgOutputMessage.Insert msg = (PgOutputMessage.Insert) decoder.decode(payload);
        assertEquals(16385, msg.relationOid());
        assertEquals(3, msg.newTuple().columns().size());
        assertEquals(new TupleValue.Text("a"), msg.newTuple().columns().get(0));
        assertEquals(TupleValue.NULL, msg.newTuple().columns().get(1));
        assertEquals(new TupleValue.Text("b"), msg.newTuple().columns().get(2));
    }

    @Test
    void updateWithKeyPrefixThenNewTuple() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('K').i16(1).i8('t').bytes(utf8("1"))  // 旧 key
                .i8('N').i16(1).i8('t').bytes(utf8("2"))  // 新值
                .build();
        PgOutputMessage.Update msg = (PgOutputMessage.Update) decoder.decode(payload);
        assertTrue(msg.oldTuple().isPresent());
        assertEquals(1, msg.oldTuple().get().columns().size());
        assertEquals(new TupleValue.Text("2"), msg.newTuple().columns().get(0));
    }

    @Test
    void updateWithFullOldRow() throws IOException {
        // REPLICA IDENTITY FULL：'O' 携带完整旧行（非仅 key 列），与 'K' 同一解析路径
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('O').i16(2).i8('t').bytes(utf8("1")).i8('t').bytes(utf8("old"))
                .i8('N').i16(1).i8('t').bytes(utf8("new"))
                .build();
        PgOutputMessage.Update msg = (PgOutputMessage.Update) decoder.decode(payload);
        assertTrue(msg.oldTuple().isPresent());
        assertEquals(2, msg.oldTuple().get().columns().size());
        assertEquals(new TupleValue.Text("old"), msg.oldTuple().get().columns().get(1));
        assertEquals(new TupleValue.Text("new"), msg.newTuple().columns().get(0));
    }

    @Test
    void updateWithoutOldTuple() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385).i8('N').i16(1).i8('t').bytes(utf8("x")).build();
        PgOutputMessage.Update msg = (PgOutputMessage.Update) decoder.decode(payload);
        assertEquals(Optional.empty(), msg.oldTuple());
    }

    @Test
    void updateOldTupleFollowedByWrongTagFailsFast() throws IOException {
        // 'K'/'O' 之后必须是 'N'：再来一个 'O'（K/O 非法并存序列）不得静默跳过
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('K').i16(1).i8('t').bytes(utf8("1"))
                .i8('O')
                .build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }

    @Test
    void updateUnknownTagFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385).i8('X').build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }

    @Test
    void deleteWithKey() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('D')
                .i32(16385).i8('K').i16(1).i8('t').bytes(utf8("9")).build();
        PgOutputMessage.Delete msg = (PgOutputMessage.Delete) decoder.decode(payload);
        assertEquals(16385, msg.relationOid());
        assertEquals(new TupleValue.Text("9"), msg.oldTuple().columns().get(0));
    }

    @Test
    void deleteWithoutKeyOrOldTupleTagFailsFast() throws IOException {
        // 'D' 必有 'K' 或 'O'：'N' 不是合法的 delete tag
        ByteBuffer payload = new MsgBuilder().type('D')
                .i32(16385).i8('N').build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }

    @Test
    void truncateOptionsAndOids() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('T')
                .i32(2).i8(3).i32(100).i32(200).build(); // CASCADE|RESTART, 两张表
        PgOutputMessage.Truncate msg = (PgOutputMessage.Truncate) decoder.decode(payload);
        assertEquals(java.util.EnumSet.of(TruncateOption.CASCADE, TruncateOption.RESTART_IDENTITY), msg.options());
        assertArrayEquals(new int[]{100, 200}, msg.relationOids());
    }

    @Test
    void logicalMessageWithContentLength() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('M')
                .i8(1).i64(0x5000L).str("prefix").bytes(utf8("hello")).build();
        PgOutputMessage.LogicalMsg msg = (PgOutputMessage.LogicalMsg) decoder.decode(payload);
        assertTrue(msg.transactional());
        assertEquals("prefix", msg.prefix());
        assertArrayEquals(utf8("hello"), msg.content());

        // flags 只有 bit0 表示 transactional：其他位（如 bit1）不得误读为事务消息
        ByteBuffer nonTxPayload = new MsgBuilder().type('M')
                .i8(2).i64(0x5000L).str("prefix").bytes(utf8("hello")).build();
        PgOutputMessage.LogicalMsg nonTx = (PgOutputMessage.LogicalMsg) decoder.decode(nonTxPayload);
        assertFalse(nonTx.transactional());
    }

    @Test
    void unchangedToastAndBinaryValue() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(1).i8('N')
                .i16(2)
                .i8('u')
                .i8('b').bytes(new byte[]{1, 2})
                .build();
        PgOutputMessage.Insert msg = (PgOutputMessage.Insert) decoder.decode(payload);
        assertEquals(TupleValue.UNCHANGED_TOAST, msg.newTuple().columns().get(0));
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}), msg.newTuple().columns().get(1));
    }

    @Test
    void unknownTupleKindFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(1).i8('N').i16(1).i8('z').build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }
}
