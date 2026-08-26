package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.OptionalLong;

/** 族 3：流式大事务控制消息。格式见 spec 附录 A。 */
final class StreamParsers {

    private StreamParsers() {
    }

    static PgOutputMessage.StreamStart start(ByteBufferReader r) {
        long xid = r.readUnsignedInt();
        boolean firstSegment = (r.readByte() & 0xFF) != 0; // I8，1 = 首个流段
        return new PgOutputMessage.StreamStart(xid, firstSegment);
    }

    static PgOutputMessage.StreamStop stop(ByteBufferReader r) {
        return new PgOutputMessage.StreamStop();
    }

    static PgOutputMessage.StreamCommit commit(ByteBufferReader r) {
        long xid = r.readUnsignedInt();
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        return new PgOutputMessage.StreamCommit(xid, commitLsn, endLsn, commitTs);
    }

    /** parallel 模式额外携带 Int64 abort_lsn + Int64 abort_time（微秒原值）。 */
    static PgOutputMessage.StreamAbort abort(ByteBufferReader r, StreamingMode mode) {
        long xid = r.readUnsignedInt();
        long subxid = r.readUnsignedInt();
        if (mode == StreamingMode.PARALLEL) {
            long abortLsn = r.readLong();
            long abortTime = r.readLong();
            return new PgOutputMessage.StreamAbort(xid, subxid, OptionalLong.of(abortLsn), OptionalLong.of(abortTime));
        }
        return new PgOutputMessage.StreamAbort(xid, subxid, OptionalLong.empty(), OptionalLong.empty());
    }
}
