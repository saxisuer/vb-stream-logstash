package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.MsgBuilder;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SpoolFrame/PayloadUnit 单测：信封帧 round-trip（有/无 streamXid）、内容全零与超长 payload、
 * 非法帧三类 fail-fast（首 9 字节不足 / xidPresent 非 0/1 / 声明长度超界），
 * 以及 PayloadUnit 的消费契约——payload 可直接 decodeSingle（有前缀时 inStream=true 且前缀值 == streamXid）。
 * 字节级布局断言（big-endian、帧头 9/13 字节）直接编码 spec §4 的帧格式，防止实现与设计漂移。
 */
class SpoolFrameTest {

    /** 无 xid 帧头长度：I64 seq + I8 xidPresent。 */
    private static final int BASE_HEADER = 9;
    /** 有 xid 帧头长度：I64 seq + I8 xidPresent + I32 xid。 */
    private static final int XID_HEADER = 13;

    /**
     * 有 streamXid 的 round-trip：seq/xid/payload 三分量经 frame→unframe 完整复原（依赖 PayloadUnit 值相等语义）。
     * xid 取无符号 Int32 上限 4294967295——若实现误用带符号读法会得到 -1，立即暴露；
     * 同时断言帧总长（13+payload）与前 13 字节的 big-endian 逐字节布局。
     */
    @Test
    void roundTripPreservesUnitWithStreamXid() {
        byte[] payload = insertPayload(true);
        PayloadUnit unit = new PayloadUnit(payload, 0x0102030405060708L, OptionalLong.of(4294967295L));

        byte[] framed = SpoolFrame.frame(unit);
        assertEquals(XID_HEADER + payload.length, framed.length);
        // 布局断言：[0..7]=seq big-endian、[8]=xidPresent=1、[9..12]=0xFFFFFFFF、其后为 payload
        assertEquals((byte) 0x01, framed[0]);
        assertEquals((byte) 0x08, framed[7]);
        assertEquals((byte) 1, framed[8]);
        assertEquals((byte) 0xFF, framed[9]);
        assertEquals((byte) 0xFF, framed[12]);

        PayloadUnit back = SpoolFrame.unframe(framed);
        assertEquals(unit, back);
        assertArrayEquals(payload, back.payload());
        assertEquals(0x0102030405060708L, back.seq());
        assertEquals(OptionalLong.of(4294967295L), back.streamXid());
    }

    /**
     * 无 streamXid 的 round-trip：OptionalLong.empty() 单元帧头只有 9 字节（xidPresent=0、无 I32 字段），
     * 三分量同样完整复原——同一 unframe 入口对两种头长的分派正确性。
     */
    @Test
    void roundTripPreservesUnitWithoutStreamXid() {
        byte[] payload = insertPayload(false);
        PayloadUnit unit = new PayloadUnit(payload, 42L, OptionalLong.empty());

        byte[] framed = SpoolFrame.frame(unit);
        assertEquals(BASE_HEADER + payload.length, framed.length);
        assertEquals((byte) 0, framed[8]);

        assertEquals(unit, SpoolFrame.unframe(framed));
    }

    /**
     * 边界内容不敏感：内容全零（256 字节，检验帧层不对字节内容做任何特殊解释）与
     * 超长 payload（1 MiB 确定性图案）均须原样往返——溢写场景单条消息可达百 KB 级。
     */
    @Test
    void allZeroAndHugePayloadRoundTrip() {
        byte[] zeros = new byte[256];
        PayloadUnit zeroUnit = new PayloadUnit(zeros, 7L, OptionalLong.empty());
        assertEquals(zeroUnit, SpoolFrame.unframe(SpoolFrame.frame(zeroUnit)));

        byte[] huge = new byte[1024 * 1024];
        for (int i = 0; i < huge.length; i++) {
            huge[i] = (byte) i;
        }
        PayloadUnit hugeUnit = new PayloadUnit(huge, 8L, OptionalLong.of(12345L));
        assertEquals(hugeUnit, SpoolFrame.unframe(SpoolFrame.frame(hugeUnit)));
    }

    /** 首 9 字节不足（空帧、8 字节帧）必须抛 IllegalArgumentException——帧头都读不完，无从谈结构。 */
    @Test
    void truncatedHeaderRejected() {
        assertThrows(IllegalArgumentException.class, () -> SpoolFrame.unframe(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> SpoolFrame.unframe(new byte[8]));
    }

    /**
     * xidPresent 字节非 0/1 必须抛 IllegalArgumentException：把合法帧的 [8] 改成 2
     * （既非"无 xid"也非"有 xid"，无法安全分派头长），拒绝而非猜测解释。
     */
    @Test
    void illegalXidPresentRejected() {
        byte[] framed = SpoolFrame.frame(new PayloadUnit(insertPayload(false), 1L, OptionalLong.empty()));
        framed[8] = 2;
        assertThrows(IllegalArgumentException.class, () -> SpoolFrame.unframe(framed));
    }

    /**
     * 声明长度超界必须抛 IllegalArgumentException：xidPresent=1 声明了 4 字节 xid 字段，
     * 但帧总长不足 13 字节（截到 9/10/11/12 四档逐一验证）——声明的结构超出实际字节边界。
     */
    @Test
    void declaredXidBeyondFrameRejected() {
        byte[] framed = SpoolFrame.frame(new PayloadUnit(insertPayload(true), 1L, OptionalLong.of(99L)));
        for (int len = BASE_HEADER; len < XID_HEADER; len++) {
            byte[] truncated = new byte[len];
            System.arraycopy(framed, 0, truncated, 0, len);
            assertThrows(IllegalArgumentException.class, () -> SpoolFrame.unframe(truncated),
                    "帧长 " + len + " 应因声明长度超界被拒绝");
        }
    }

    /**
     * streamXid 超出无符号 Int32 值域（负数或 >4294967295）时 frame 必须 fail-fast 抛
     * IllegalArgumentException：I32 字段写不下会静默截断，回放时得到错误 xid 路由到错误桶——
     * 数据损坏比异常昂贵得多。合法上界 4294967295 不抛（round-trip 测试已证）。
     */
    @Test
    void frameRejectsXidOutsideUnsignedInt32Range() {
        assertThrows(IllegalArgumentException.class,
                () -> SpoolFrame.frame(new PayloadUnit(insertPayload(true), 1L, OptionalLong.of(-1L))));
        assertThrows(IllegalArgumentException.class,
                () -> SpoolFrame.frame(new PayloadUnit(insertPayload(true), 1L, OptionalLong.of(4294967296L))));
    }

    /**
     * PayloadUnit 消费契约（spec §4）：payload 是可直接回放的单条完整消息——
     * 无前缀单元 decodeSingle(buf,false)；有前缀单元 decodeSingle(buf,true) 且
     * 协议解析出的 streamXid == 单元的 streamXid（xid 用无符号上限值验证读法一致）。
     */
    @Test
    void payloadDecodableViaDecodeSingle() throws IOException {
        PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.OFF);

        PayloadUnit plain = new PayloadUnit(insertPayload(false), 1L, OptionalLong.empty());
        PgOutputMessage.Insert noXid = (PgOutputMessage.Insert) decoder.decodeSingle(
                ByteBuffer.wrap(plain.payload()), plain.streamXid().isPresent());
        assertEquals(OptionalLong.empty(), noXid.streamXid());
        assertEquals(16384, noXid.relationOid());

        long xid = 4294967295L;
        PayloadUnit prefixed = new PayloadUnit(insertPayload(true), 2L, OptionalLong.of(xid));
        PgOutputMessage.Insert withXid = (PgOutputMessage.Insert) decoder.decodeSingle(
                ByteBuffer.wrap(prefixed.payload()), prefixed.streamXid().isPresent());
        assertEquals(OptionalLong.of(xid), withXid.streamXid());
    }

    /**
     * 构造一条可直接回放的单条 INSERT 消息字节（类型字节 + 可选 Int32 xid 前缀 + I32 关系 oid +
     * 'N' 新元组标记 + TupleData 单文本列）。xid 前缀取无符号 Int32 上限的位型（写出为 -1），
     * 与 decodeSingle 的 readUnsignedInt 读法互为镜像。
     *
     * @param withXid true 时在类型字节后写 4 字节 xid 前缀（流式块内形态）
     * @return 完整消息字节（即 PayloadUnit.payload 的真实形态）
     */
    private static byte[] insertPayload(boolean withXid) {
        try {
            MsgBuilder b = new MsgBuilder().type('I');
            if (withXid) {
                b.i32((int) 4294967295L);
            }
            return b.i32(16384).i8('N').i16(1).i8('t').bytes("v".getBytes()).build().array();
        } catch (IOException e) {
            throw new IllegalStateException("测试消息构造不可能失败", e);
        }
    }
}
