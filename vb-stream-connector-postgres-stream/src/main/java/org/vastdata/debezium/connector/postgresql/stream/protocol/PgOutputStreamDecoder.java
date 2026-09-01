package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

/**
 * pgoutput 消息解码器（协议层唯一公共入口，MS2 组装器的解码接缝）。构造时指定
 * {@link StreamingMode}；运行期维护最小流块状态 inStream：收到 'S'(StreamStart)
 * 置位、'E'(StreamStop) 复位；inStream 期间 M/R/Y/I/U/D/T 会前置 Int32 xid
 * （spec 附录 A）。解码结束若有多余字节立即抛 {@link ProtocolMisalignmentException}。
 * 另提供 {@link #decodeSingle(ByteBuffer, boolean)}：按调用方显式给定的 inStream
 * 解码单条消息（回放场景用，免 S/E 包裹重建流块上下文），不读写实例的流块状态。
 * 引擎 {@code org.vastdata.vbstream.protocol.PgOutputDecoder} 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），双入口契约与分发表逐行一致。
 * 非线程安全；每个复制流一个实例，由读取该流的线程调用。
 */
public final class PgOutputStreamDecoder {

    /** 逐消息 DEBUG（默认关闭）：类型字节 + 解析结果，排障协议问题时的第一现场。 */
    private static final Logger LOG = LoggerFactory.getLogger(PgOutputStreamDecoder.class);

    private final StreamingMode streamingMode;
    private boolean inStream;

    /**
     * 构造时仅绑定 StreamingMode（供 StreamAbort 按模式分支解析），流块状态从 false 起步。
     *
     * @param streamingMode 复制流的 streaming 档位，决定 'A' 消息附加字段的读取形态
     */
    public PgOutputStreamDecoder(StreamingMode streamingMode) {
        this.streamingMode = streamingMode;
    }

    /**
     * 解码一条来自复制流的顶层消息。
     * 关键步骤：读类型字节 → 以实例当前 {@code inStream} 分发解析（'S'/'E' 分支顺带
     * 置位/复位该字段）→ 出口统一做剩余字节检查与 DEBUG 日志。
     * 边界与异常语义：解析后仍有剩余字节抛 {@link ProtocolMisalignmentException}；
     * 未知类型字节抛 {@link UnknownMessageTypeException}；buffer 不足抛
     * {@link java.nio.BufferUnderflowException}。
     * 线程约束：非线程安全，须由持有本实例的单一流读取线程调用。
     *
     * @param payload 一条完整 pgoutput 消息体（已剥去复制协议封装），从其当前 position 读起
     * @return 解析出的强类型消息（19 类 {@link PgOutputMessage} 之一）
     */
    public PgOutputMessage decode(ByteBuffer payload) {
        WireReader r = new WireReader(payload);
        byte type = r.readByte();
        return finish(type, r, dispatch(type, r, inStream));
    }

    /**
     * 按显式指定的 inStream 解码**单条**消息，供回放场景使用——回放侧本来就知道每条
     * 消息在不在流式块内，不需要拿 'S'/'E' 把流块上下文重新包一遍。
     *
     * <p>步骤：读类型字节 → 白名单校验（只允许可带 xid 前缀的 7 类 M/R/Y/I/U/D/T，
     * 其余一律 {@link IllegalArgumentException}，包括 'S'/'E'/'c'/'A' 和两阶段控制
     * 类型——它们没有可复用的前缀语义）→ 用**入参**（而不是实例字段）作为 inStream
     * 分发解析 → 出口与 {@link #decode} 共用剩余字节检查和日志。
     *
     * <p>边界：完全不读写实例的 {@code inStream} 字段，调用它不会影响 decode 的流块
     * 状态；剩余字节非 0 抛 {@link ProtocolMisalignmentException}；前缀假设与字节
     * 布局不符时由各 parser 的 fail-fast 异常暴露。线程约束同 decode：单一线程调用。
     *
     * @param payload 一条完整 pgoutput 消息体（已剥去复制协议封装）
     * @param inStream 该消息是否处于流式块内（决定类型字节后有无 Int32 xid 前缀）
     * @return 解析出的强类型消息
     */
    public PgOutputMessage decodeSingle(ByteBuffer payload, boolean inStream) {
        WireReader r = new WireReader(payload);
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
     * 类型字节到各 parser 族的分发表。
     * 关键步骤：按入参 inStream 决定 M/R/Y/I/U/D/T 是否先读 xid 前缀（前缀位置在
     * 类型字节之后、消息体之前）；'S'/'E' 分支写入的是**实例字段**（this.inStream，
     * 与入参同名需限定）——这是 decode 路径的流块状态副作用，decodeSingle 在入口
     * 白名单已拒绝这两类类型，不会触达。
     * 边界与异常语义：未知类型字节抛 {@link UnknownMessageTypeException}。
     */
    private PgOutputMessage dispatch(byte type, WireReader r, boolean inStream) {
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
    private PgOutputMessage finish(byte type, WireReader r, PgOutputMessage msg) {
        if (r.remaining() != 0) {
            throw new ProtocolMisalignmentException(type, r.remaining());
        }
        LOG.debug("解码: '{}' {}", (char) type, msg);
        return msg;
    }

    /**
     * 流式块内的 M/R/Y/I/U/D/T 前置 Int32 xid；顶层消息无此前缀。
     * inStream 由调用方显式给定（decode 传实例字段、decodeSingle 传方法入参）。
     */
    private OptionalLong streamXid(WireReader r, boolean inStream) {
        return inStream ? OptionalLong.of(r.readUnsignedInt()) : OptionalLong.empty();
    }
}
