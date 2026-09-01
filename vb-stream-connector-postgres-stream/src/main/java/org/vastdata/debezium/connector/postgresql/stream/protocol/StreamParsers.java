package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.time.Instant;
import java.util.OptionalLong;

/**
 * 族 3：流式大事务控制消息（'S' StreamStart / 'E' StreamStop / 'c' StreamCommit /
 * 'A' StreamAbort）的解析，decoder 剥离类型字节后从消息体首字段读起。
 * 引擎 {@code org.vastdata.vbstream.protocol.StreamParsers} 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），读取序列与分支语义逐行一致。
 * 格式见 spec 附录 A（字节格式表以计划文档转引为准）。纯函数无状态：所有方法
 * 包私有 static，仅由解码器在持有它的单一线程内调用。
 */
final class StreamParsers {

    private StreamParsers() {
    }

    /**
     * 解析 'S' StreamStart：I32 xid（无符号读入 long）+ I8 firstSegment（无符号读，
     * 非 0 即首个流段）。本消息自身无 xid 前缀（firstSegment 是无条件字段）——
     * 前缀语义由解码器对流块内的 M/R/Y/I/U/D/T 施加，与 StreamStart 无关。
     */
    static PgOutputMessage.StreamStart start(WireReader r) {
        long xid = r.readUnsignedInt();
        boolean firstSegment = r.readUnsignedByte() != 0; // I8，1 = 首个流段
        return new PgOutputMessage.StreamStart(xid, firstSegment);
    }

    /**
     * 解析 'E' StreamStop：无字段。消息体恰为空，任何残留字节由解码器出口的
     * 剩余字节检查暴露。
     */
    static PgOutputMessage.StreamStop stop(WireReader r) {
        return new PgOutputMessage.StreamStop();
    }

    /**
     * 解析 'c' StreamCommit：I32 xid + I8 flags（读掉不建模，漏读 1 字节即后续字段
     * 全部错位）+ I64 commitLsn + I64 endLsn + I64 commitTs（PG 纪元微秒换算）。
     */
    static PgOutputMessage.StreamCommit commit(WireReader r) {
        long xid = r.readUnsignedInt();
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = WireReader.pgMicrosToInstant(r.readLong());
        return new PgOutputMessage.StreamCommit(xid, commitLsn, endLsn, commitTs);
    }

    /**
     * 解析 'A' StreamAbort：I32 xid + I32 subxid，此后按构造解码器时的模式分支——
     * 仅 {@code mode == PARALLEL} 继续读 I64 abortLsn + I64 abortTime（微秒原值，
     * 刻意不转 Instant），否则两附加字段为 empty。错读/漏读 16 字节都会让消息流
     * 全部错位，非 parallel 模式下不足量由底层缓冲抛 BufferUnderflowException 暴露。
     */
    static PgOutputMessage.StreamAbort abort(WireReader r, StreamingMode mode) {
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
