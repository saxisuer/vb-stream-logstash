package org.vastdata.debezium.connector.postgresql.stream.protocol;

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

/**
 * PgOutputStreamDecoder 双入口（decode 流块状态机 / decodeSingle 显式 inStream）的
 * 行为契约测试：引擎 {@code org.vastdata.vbstream.protocol.PgOutputDecoderTest} 全部
 * 5 用例的 1:1 翻写（断言值不变，本地 byte[] 助手一并翻写），另按 Task 6 追加要求
 * 补 decoder 层 misalignment 直测 1 例（'B' 多留字节经 decode 抛
 * {@link ProtocolMisalignmentException}——Task 4 只在 parser 层以替身断言过该语义）。
 */
class PgOutputStreamDecoderTest {

    /**
     * 验证意图：未知类型字节 0x58（'X'）fail-fast 抛 {@link UnknownMessageTypeException}，
     * 异常消息携带十六进制字节值——未知类型往往正是错位的首发现场。
     */
    @Test
    void unknownTypeByteFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('X').i32(1).build();
        UnknownMessageTypeException ex = assertThrows(UnknownMessageTypeException.class,
                () -> new PgOutputStreamDecoder(StreamingMode.OFF).decode(payload));
        assertTrue(ex.getMessage().contains("0x58"), "异常应含字节十六进制值: " + ex.getMessage());
    }

    /**
     * 验证意图：'I' 经 dispatch 走到 DmlParsers 正常解析出 Insert——四族 parser 的
     * 接线终态（Task 6 收官时 19 类消息在 decode 路径全部可达）。
     */
    @Test
    void insertDispatchesToDmlParser() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I').i32(1).i8('N')
                .i16(1).i8('t').bytes("x".getBytes(StandardCharsets.UTF_8)).build();
        PgOutputMessage msg = new PgOutputStreamDecoder(StreamingMode.OFF).decode(payload);
        assertInstanceOf(PgOutputMessage.Insert.class, msg);
    }

    // ---- decodeSingle：单条消息按显式 inStream 解析，回放场景免 S/E 包裹 ----

    /**
     * 验证意图：decodeSingle 前缀契约核心——inStream=true 消费 xid 前缀、false 不消费；
     * 'B' 等控制类类型（无前缀语义）入口白名单直接拒绝（IllegalArgumentException）。
     */
    @Test
    void decodeSingleReadsPrefixWithoutState() {
        PgOutputStreamDecoder d = new PgOutputStreamDecoder(StreamingMode.ON);
        byte[] ins = concat(new byte[]{'I'}, i32(7), tupleN(textCol("v")));       // 无前缀体
        byte[] streamed = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v"))); // 前缀 xid=99
        assertEquals(OptionalLong.of(99), ((PgOutputMessage.Insert) d.decodeSingle(ByteBuffer.wrap(streamed), true)).streamXid());
        assertNotEquals(OptionalLong.of(99), ((PgOutputMessage.Insert) d.decodeSingle(ByteBuffer.wrap(ins), false)).streamXid());
        assertThrows(IllegalArgumentException.class, () -> d.decodeSingle(ByteBuffer.wrap(beginBytes()), false));
    }

    /**
     * 验证意图：前缀假设与字节不符必须 fail-fast——按无前缀解析带前缀体，oid 吃掉
     * xid 后前缀首字节 0x00 落在元组标记位即抛 {@link UnknownMessageTypeException}，
     * 不静默错读。
     */
    @Test
    void decodeSingleWithMismatchedInStreamFailsFast() {
        PgOutputStreamDecoder d = new PgOutputStreamDecoder(StreamingMode.ON);
        byte[] streamed = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v")));
        assertThrows(UnknownMessageTypeException.class, () -> d.decodeSingle(ByteBuffer.wrap(streamed), false));
    }

    /**
     * 验证意图：decodeSingle 只用入参不做实例状态写入门——其后 decode 的 S..E 流块
     * 状态机须从未污染的初始状态照常工作（若 decodeSingle 误写实例 inStream，
     * 紧随的顶层 I 会按带前缀错读而抛异常）。
     */
    @Test
    void decodeSingleDoesNotPolluteDecodeStreamState() {
        PgOutputStreamDecoder d = new PgOutputStreamDecoder(StreamingMode.ON);
        byte[] inBlockIns = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v")));
        byte[] topIns = concat(new byte[]{'I'}, i32(7), tupleN(textCol("v")));
        d.decodeSingle(ByteBuffer.wrap(inBlockIns), true); // 显式 true 仅作用于本次解析
        assertFalse(((PgOutputMessage.Insert) d.decode(ByteBuffer.wrap(topIns))).streamXid().isPresent());
        // 连续 decode 的状态机照常：S 置位后块内消息自动带前缀，E 复位后顶层消息不带
        d.decode(ByteBuffer.wrap(concat(new byte[]{'S'}, i32(99), new byte[]{1})));
        assertTrue(((PgOutputMessage.Insert) d.decode(ByteBuffer.wrap(inBlockIns))).streamXid().isPresent());
        d.decode(ByteBuffer.wrap(new byte[]{'E'}));
        assertFalse(((PgOutputMessage.Insert) d.decode(ByteBuffer.wrap(topIns))).streamXid().isPresent());
    }

    /**
     * 验证意图：decoder 层出口剩余字节检查的直测（Task 4 仅以 parser 层替身断言过）——
     * 'B' 消息体故意多留 4 字节，Begin 自身不报错（读到字段尽头即止），错位由 decode
     * 出口的 finish 检查暴露为 {@link ProtocolMisalignmentException}，消息含剩余量诊断。
     */
    @Test
    void misalignedLeftoverThroughDecodeFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(1).i64(2).i32(3).i32(99).build(); // 故意多 4 字节
        ProtocolMisalignmentException ex = assertThrows(ProtocolMisalignmentException.class,
                () -> new PgOutputStreamDecoder(StreamingMode.OFF).decode(payload));
        assertTrue(ex.getMessage().contains("剩余 4 字节"), "异常应含剩余字节数: " + ex.getMessage());
    }

    // ---- 字节样本助手（照引擎 PgOutputDecoderTest 的本地构造手法，big-endian） ----

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
