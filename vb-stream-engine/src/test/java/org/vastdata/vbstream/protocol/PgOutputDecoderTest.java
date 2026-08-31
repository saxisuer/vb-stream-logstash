package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    // ---- decodeSingle（Task 3）：单条消息按显式 inStream 解析，回放场景免 S/E 包裹 ----

    /** 核心契约：inStream=true 消费 xid 前缀、false 不消费；'B' 等控制类类型直接拒绝。 */
    @Test
    void decodeSingleReadsPrefixWithoutState() {
        PgOutputDecoder d = new PgOutputDecoder(StreamingMode.ON);
        byte[] ins = concat(new byte[]{'I'}, i32(7), tupleN(textCol("v")));       // 无前缀体
        byte[] streamed = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v"))); // 前缀 xid=99
        assertEquals(OptionalLong.of(99), ((PgOutputMessage.Insert) d.decodeSingle(ByteBuffer.wrap(streamed), true)).streamXid());
        assertNotEquals(OptionalLong.of(99), ((PgOutputMessage.Insert) d.decodeSingle(ByteBuffer.wrap(ins), false)).streamXid());
        assertThrows(IllegalArgumentException.class, () -> d.decodeSingle(ByteBuffer.wrap(beginBytes()), false));
    }

    /** 前缀假设与字节不符必须 fail-fast：按无前缀解析带前缀体，oid 吃掉 xid 后 0x00 落在元组标记位即抛，不静默错读。 */
    @Test
    void decodeSingleWithMismatchedInStreamFailsFast() {
        PgOutputDecoder d = new PgOutputDecoder(StreamingMode.ON);
        byte[] streamed = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v")));
        assertThrows(UnknownMessageTypeException.class, () -> d.decodeSingle(ByteBuffer.wrap(streamed), false));
    }

    /** decodeSingle 只用入参不做实例状态写入门：其后 decode 的 S..E 流块状态机须从未污染的初始状态照常工作。 */
    @Test
    void decodeSingleDoesNotPolluteDecodeStreamState() {
        PgOutputDecoder d = new PgOutputDecoder(StreamingMode.ON);
        byte[] inBlockIns = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v")));
        byte[] topIns = concat(new byte[]{'I'}, i32(7), tupleN(textCol("v")));
        d.decodeSingle(ByteBuffer.wrap(inBlockIns), true); // 显式 true 仅作用于本次解析
        // 若 decodeSingle 误写实例 inStream，这条顶层 I 会按带前缀错读而抛异常
        assertFalse(((PgOutputMessage.Insert) d.decode(ByteBuffer.wrap(topIns))).streamXid().isPresent());
        // 连续 decode 的状态机照常：S 置位后块内消息自动带前缀，E 复位后顶层消息不带
        d.decode(ByteBuffer.wrap(concat(new byte[]{'S'}, i32(99), new byte[]{1})));
        assertTrue(((PgOutputMessage.Insert) d.decode(ByteBuffer.wrap(inBlockIns))).streamXid().isPresent());
        d.decode(ByteBuffer.wrap(new byte[]{'E'}));
        assertFalse(((PgOutputMessage.Insert) d.decode(ByteBuffer.wrap(topIns))).streamXid().isPresent());
    }

    // ---- 字节样本助手（照 StreamParsersTest 的内联构造手法，big-endian） ----

    /** 顺序拼接多段字节为一条消息样本。 */
    private static byte[] concat(byte[]... parts) {
        byte[] out = new byte[java.util.Arrays.stream(parts).mapToInt(p -> p.length).sum()];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    /** big-endian Int32（xid / relation oid / 文本长度等）。 */
    private static byte[] i32(int v) {
        return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
    }

    /** big-endian Int16（TupleData 列数）。 */
    private static byte[] i16(int v) {
        return new byte[]{(byte) (v >>> 8), (byte) v};
    }

    /** big-endian Int64（LSN / 微秒时间戳）。 */
    private static byte[] i64(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    /** 单个文本列值：'t' + I32 长度 + UTF-8 字节。 */
    private static byte[] textCol(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return concat(new byte[]{'t'}, i32(b.length), b);
    }

    /** 新元组段：'N' 标记 + I16 列数 + 各列字节。 */
    private static byte[] tupleN(byte[]... cols) {
        return concat(new byte[]{'N'}, i16(cols.length), concat(cols));
    }

    /** Begin 消息样本：'B' + I64 finalLsn + I64 微秒时间戳 + I32 xid（decodeSingle 应拒绝的控制类类型）。 */
    private static byte[] beginBytes() {
        return concat(new byte[]{'B'}, i64(0x1000L), i64(0L), i32(5));
    }
}
