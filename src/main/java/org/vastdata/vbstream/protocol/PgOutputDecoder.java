package org.vastdata.vbstream.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

/**
 * pgoutput 消息解码器。构造时指定 StreamingMode；运行期维护最小流块状态 inStream：
 * 收到 'S'(StreamStart) 置位、'E'(StreamStop) 复位；inStream 期间 M/R/Y/I/U/D/T
 * 会前置 Int32 xid（spec 附录 A）。解码结束若有多余字节立即抛 ProtocolMisalignmentException。
 * 非线程安全；每个复制流一个实例，由读取该流的线程调用。
 */
public final class PgOutputDecoder {

    /** 逐消息 DEBUG（默认关闭）：类型字节 + 解析结果，排障协议问题时的第一现场。 */
    private static final Logger LOG = LoggerFactory.getLogger(PgOutputDecoder.class);

    private final StreamingMode streamingMode;
    private boolean inStream;

    public PgOutputDecoder(StreamingMode streamingMode) {
        this.streamingMode = streamingMode;
    }

    public PgOutputMessage decode(ByteBuffer payload) {
        ByteBufferReader r = new ByteBufferReader(payload);
        byte type = r.readByte();
        PgOutputMessage msg = dispatch(type, r);
        if (r.remaining() != 0) {
            throw new ProtocolMisalignmentException(type, r.remaining());
        }
        LOG.debug("解码: '{}' {}", (char) type, msg);
        return msg;
    }

    private PgOutputMessage dispatch(byte type, ByteBufferReader r) {
        return switch (type) {
            case 'B' -> NormalParsers.begin(r);
            case 'C' -> NormalParsers.commit(r);
            case 'O' -> NormalParsers.origin(r);
            case 'R' -> NormalParsers.relation(r, streamXid(r));
            case 'Y' -> NormalParsers.type(r, streamXid(r));
            case 'I' -> DmlParsers.insert(r, streamXid(r));
            case 'U' -> DmlParsers.update(r, streamXid(r));
            case 'D' -> DmlParsers.delete(r, streamXid(r));
            case 'T' -> DmlParsers.truncate(r, streamXid(r));
            case 'M' -> DmlParsers.logicalMsg(r, streamXid(r));
            case 'S' -> {
                inStream = true;
                yield StreamParsers.start(r);
            }
            case 'E' -> {
                inStream = false;
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

    /** 流式块内的 M/R/Y/I/U/D/T 前置 Int32 xid；顶层消息无此前缀。 */
    private OptionalLong streamXid(ByteBufferReader r) {
        return inStream ? OptionalLong.of(r.readUnsignedInt()) : OptionalLong.empty();
    }
}
