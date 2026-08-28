package org.vastdata.vbstream.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

/**
 * pgoutput 消息解码器。构造时指定 StreamingMode；运行期维护最小流块状态 inStream：
 * 收到 'S'(StreamStart) 置位、'E'(StreamStop) 复位；inStream 期间 M/R/Y/I/U/D/T
 * 会前置 Int32 xid（spec 附录 A）。解码结束若有多余字节立即抛 ProtocolMisalignmentException。
 * 另提供 {@link #decodeSingle(ByteBuffer, boolean)}：按调用方显式给定的 inStream 解码单条
 * 消息（回放场景用，免 S/E 包裹重建流块上下文），不读写实例的流块状态。
 * 非线程安全；每个复制流一个实例，由读取该流的线程调用。
 */
public final class PgOutputDecoder {

    /** 逐消息 DEBUG（默认关闭）：类型字节 + 解析结果，排障协议问题时的第一现场。 */
    private static final Logger LOG = LoggerFactory.getLogger(PgOutputDecoder.class);

    private final StreamingMode streamingMode;
    private boolean inStream;

    /** 构造时仅绑定 StreamingMode（供 StreamAbort 按模式分支解析），流块状态从 false 起步。 */
    public PgOutputDecoder(StreamingMode streamingMode) {
        this.streamingMode = streamingMode;
    }

    /**
     * 解码一条来自复制流的顶层消息。
     * 关键步骤：读类型字节 → 以实例当前 {@code inStream} 分发解析（'S'/'E' 分支顺带置位/复位该字段）
     * → 出口统一做剩余字节检查与 DEBUG 日志。
     * 边界与异常语义：解析后仍有剩余字节抛 {@link ProtocolMisalignmentException}；未知类型字节抛
     * {@link UnknownMessageTypeException}；buffer 不足抛 {@link java.nio.BufferUnderflowException}。
     * 线程约束：非线程安全，须由持有本实例的单一流读取线程调用。
     */
    public PgOutputMessage decode(ByteBuffer payload) {
        ByteBufferReader r = new ByteBufferReader(payload);
        byte type = r.readByte();
        return finish(type, r, dispatch(type, r, inStream));
    }

    /**
     * 按显式指定的 inStream 解码**单条**消息，供回放场景使用——回放侧已知每条消息是否处于
     * 流式块内，无需用 'S'/'E' 包裹重建上下文。
     * 关键步骤：读类型字节 → 白名单校验（仅允许可带 xid 前缀的 7 类 M/R/Y/I/U/D/T，其余一律
     * {@link IllegalArgumentException}，含 'S'/'E' 与两阶段控制类型——它们没有可复用的前缀语义）
     * → 以**入参**（而非实例字段）作为 inStream 分发解析 → 出口与 {@link #decode} 共用剩余字节
     * 检查与日志。
     * 边界与异常语义：完全不读写实例的 {@code inStream} 字段，调用前后 decode 的流块状态不受影响；
     * 剩余字节非 0 抛 {@link ProtocolMisalignmentException}；前缀假设与字节布局不符时经由各 parser
     * 的 fail-fast 异常暴露。
     * 线程约束：同 decode，单一线程调用。
     */
    public PgOutputMessage decodeSingle(ByteBuffer payload, boolean inStream) {
        ByteBufferReader r = new ByteBufferReader(payload);
        byte type = r.readByte();
        switch (type) {
            case 'M', 'R', 'Y', 'I', 'U', 'D', 'T' -> { /* 仅此 7 类可带 Int32 xid 前缀 */ }
            default -> throw new IllegalArgumentException(
                    "decodeSingle 仅接受可带 xid 前缀的 7 类消息（M/R/Y/I/U/D/T），收到 '%s' (0x%02X)"
                            .formatted((char) type, type));
        }
        return finish(type, r, dispatch(type, r, inStream));
    }

    /**
     * 类型字节到各 parser 的分发表。
     * 关键步骤：按入参 inStream 决定 M/R/Y/I/U/D/T 是否先读 xid 前缀；'S'/'E' 分支写入的是
     * **实例字段**（this.inStream，与入参同名需限定）——这是 decode 路径的流块状态副作用，
     * decodeSingle 在入口白名单已拒绝这两类类型，不会触达。
     * 边界与异常语义：未知类型字节抛 {@link UnknownMessageTypeException}。
     */
    private PgOutputMessage dispatch(byte type, ByteBufferReader r, boolean inStream) {
        return switch (type) {
            case 'B' -> NormalParsers.begin(r);
            case 'C' -> NormalParsers.commit(r);
            case 'O' -> NormalParsers.origin(r);
            case 'R' -> NormalParsers.relation(r, streamXid(r, inStream));
            case 'Y' -> NormalParsers.type(r, streamXid(r, inStream));
            case 'I' -> DmlParsers.insert(r, streamXid(r, inStream));
            case 'U' -> DmlParsers.update(r, streamXid(r, inStream));
            case 'D' -> DmlParsers.delete(r, streamXid(r, inStream));
            case 'T' -> DmlParsers.truncate(r, streamXid(r, inStream));
            case 'M' -> DmlParsers.logicalMsg(r, streamXid(r, inStream));
            case 'S' -> {
                this.inStream = true;
                yield StreamParsers.start(r);
            }
            case 'E' -> {
                this.inStream = false;
                yield StreamParsers.stop(r);
            }
            case 'c' -> StreamParsers.commit(r);
            case 'A' -> StreamParsers.abort(r, streamingMode);
            case 'b' -> TwoPhaseParsers.beginPrepare(r);
            case 'P' -> TwoPhaseParsers.prepare(r);
            case 'K' -> TwoPhaseParsers.commitPrepared(r);
            case 'r' -> TwoPhaseParsers.rollbackPrepared(r);
            case 'p' -> TwoPhaseParsers.streamPrepare(r);
            default -> throw new UnknownMessageTypeException(type, r);
        };
    }

    /**
     * 两个解码入口共用的出口：解析完成后剩余字节非 0 立即抛
     * {@link ProtocolMisalignmentException}（防错位扩散），再打逐消息 DEBUG（默认关闭）。
     */
    private PgOutputMessage finish(byte type, ByteBufferReader r, PgOutputMessage msg) {
        if (r.remaining() != 0) {
            throw new ProtocolMisalignmentException(type, r.remaining());
        }
        LOG.debug("解码: '{}' {}", (char) type, msg);
        return msg;
    }

    /** 流式块内的 M/R/Y/I/U/D/T 前置 Int32 xid；顶层消息无此前缀。inStream 由调用方显式给定。 */
    private OptionalLong streamXid(ByteBufferReader r, boolean inStream) {
        return inStream ? OptionalLong.of(r.readUnsignedInt()) : OptionalLong.empty();
    }
}
