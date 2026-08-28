package org.vastdata.vbstream.replication;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * SPILLED 单元信封帧的编/解码纯函数（spec §4）。字节布局（全部 big-endian）：
 * {@code [I64 seq][I8 xidPresent][I32 xid?][payload]}——无 xid 时帧头 9 字节，有 xid 时 13 字节，
 * payload 就是 PayloadUnit 的原始消息字节，原样追加在后面。
 *
 * <p>用途：SPILLED 桶落盘时堆里不保留任何按单元的元数据，seq 和 streamXid 都只随帧字节走，
 * 回读时复原。无状态、无副作用；unframe 碰到结构非法的帧抛 {@link IllegalArgumentException}——
 * 错位的数据宁可拒绝也不猜测解释。
 */
public final class SpoolFrame {

    /** 无 xid 帧头长度：I64 seq + I8 xidPresent = 9 字节。 */
    private static final int BASE_HEADER_BYTES = 9;

    /** 有 xid 帧头长度：I64 seq + I8 xidPresent + I32 xid = 13 字节。 */
    private static final int XID_HEADER_BYTES = 13;

    /** 无符号 Int32 上限：streamXid 值域约束（超出即 frame fail-fast，防止 I32 写入静默截断）。 */
    private static final long UNSIGNED_INT32_MAX = 4294967295L;

    /** 纯函数工具类，禁止实例化。 */
    private SpoolFrame() {
    }

    /**
     * 把单元编成信封帧字节。
     *
     * <p>步骤：按 streamXid 的有无决定帧头长度（9 或 13 字节）→ 按 BIG_ENDIAN 依次写 I64 seq、
     * I8 xidPresent（0/1）、（有 xid 时）I32 xid → 原样追加 payload。
     *
     * <p>边界：unit 为 null 抛 NPE；streamXid 超出无符号 Int32 的值域抛
     * {@link IllegalArgumentException}——I32 写不下会静默截断，回放时会路由到错误的桶，必须在
     * 编码时就拦住；空 payload 合法（帧层只搬字节，"payload 至少要有类型字节"的语义校验是
     * 消费方 decodeSingle 的事）。
     *
     * @param unit 待编码的桶存储单元
     * @return 完整帧字节（调用方独占，可直接写 Chronicle Queue）
     */
    public static byte[] frame(PayloadUnit unit) {
        Objects.requireNonNull(unit, "unit 不能为 null");
        boolean hasXid = unit.streamXid().isPresent();
        if (hasXid) {
            long xid = unit.streamXid().getAsLong();
            if (xid < 0 || xid > UNSIGNED_INT32_MAX) {
                throw new IllegalArgumentException(
                        "streamXid %d 超出无符号 Int32 值域 [0,%d]，无法无损编码进 I32 帧字段"
                                .formatted(xid, UNSIGNED_INT32_MAX));
            }
        }
        ByteBuffer buf = ByteBuffer
                .allocate((hasXid ? XID_HEADER_BYTES : BASE_HEADER_BYTES) + unit.payload().length)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putLong(unit.seq());
        buf.put((byte) (hasXid ? 1 : 0));
        if (hasXid) {
            buf.putInt((int) unit.streamXid().getAsLong());
        }
        buf.put(unit.payload());
        return buf.array();
    }

    /**
     * 把帧字节还原成单元（frame 的逆操作）。
     *
     * <p>步骤：先确认帧长够 9 字节的基础头 → 读出 I64 seq 和 I8 xidPresent，按它的值分派：
     * 为 0 时 payload 就是 9 字节之后的全部；为 1 时先确认帧长够 13 字节的头，再按无符号读法
     * （&amp; 0xFFFFFFFFL）取出 xid，payload 是 13 字节之后的全部。
     *
     * <p>边界：framed 为 null 抛 NPE；三类结构非法都抛 {@link IllegalArgumentException}——
     * 帧长不足 9（连头都读不完）、xidPresent 不是 0/1（没法决定头长）、xidPresent=1 声明了
     * 4 字节 xid 但帧不够长。长度都在读取前校验过，后面批量读不会以 BufferUnderflowException 逃出。
     *
     * @param framed frame 产出的完整帧字节
     * @return 复原的单元（payload 是新建数组，与 framed 不共享）
     */
    public static PayloadUnit unframe(byte[] framed) {
        Objects.requireNonNull(framed, "framed 不能为 null");
        if (framed.length < BASE_HEADER_BYTES) {
            throw new IllegalArgumentException(
                    "帧长 %d 不足最小头 %d 字节（I64 seq + I8 xidPresent）".formatted(framed.length, BASE_HEADER_BYTES));
        }
        ByteBuffer buf = ByteBuffer.wrap(framed).order(ByteOrder.BIG_ENDIAN);
        long seq = buf.getLong();
        byte xidPresent = buf.get();
        if (xidPresent == 0) {
            return new PayloadUnit(rest(buf, framed.length - BASE_HEADER_BYTES), seq, OptionalLong.empty());
        }
        if (xidPresent == 1) {
            if (framed.length < XID_HEADER_BYTES) {
                throw new IllegalArgumentException(
                        "xidPresent=1 声明 4 字节 xid 字段，但帧长 %d 不足 %d 字节头（声明长度超界）"
                                .formatted(framed.length, XID_HEADER_BYTES));
            }
            long xid = buf.getInt() & 0xFFFFFFFFL;    // 协议 xid 为无符号 Int32
            return new PayloadUnit(rest(buf, framed.length - XID_HEADER_BYTES), seq, OptionalLong.of(xid));
        }
        throw new IllegalArgumentException(
                "xidPresent 字节 0x%02X 非 0/1，头长无法分派".formatted(xidPresent));
    }

    /**
     * 从已定位到 payload 起点的 buffer 批量读出剩余字节。长度由调用方按帧总长预先算好并完成
     * 越界校验，故本方法内不会发生 underflow。
     *
     * @param buf     已读毕帧头的 buffer（position 位于 payload 起点）
     * @param length  payload 字节数（帧总长 - 头长）
     * @return 新建的 payload 数组
     */
    private static byte[] rest(ByteBuffer buf, int length) {
        byte[] payload = new byte[length];
        buf.get(payload);
        return payload;
    }
}
