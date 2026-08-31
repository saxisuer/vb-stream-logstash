package org.vastdata.vbstream.protocol;

import java.time.Instant;

/** 族 4：两阶段提交消息。格式见 spec 附录 A；I8(0) flags 字段消费不建模。 */
final class TwoPhaseParsers {

    private TwoPhaseParsers() {
    }

    static PgOutputMessage.BeginPrepare beginPrepare(ByteBufferReader r) {
        long prepareLsn = r.readLong();
        long endLsn = r.readLong();
        Instant prepareTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.BeginPrepare(prepareLsn, endLsn, prepareTs, xid, gid);
    }

    static PgOutputMessage.Prepare prepare(ByteBufferReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        return readPreparedTxn(r, PgOutputMessage.Prepare::new);
    }

    static PgOutputMessage.CommitPrepared commitPrepared(ByteBufferReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.CommitPrepared(commitLsn, endLsn, commitTs, xid, gid);
    }

    static PgOutputMessage.RollbackPrepared rollbackPrepared(ByteBufferReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long prepareEndLsn = r.readLong();
        long rollbackEndLsn = r.readLong();
        Instant prepareTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        Instant rollbackTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.RollbackPrepared(prepareEndLsn, rollbackEndLsn, prepareTs, rollbackTs, xid, gid);
    }

    static PgOutputMessage.StreamPrepare streamPrepare(ByteBufferReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        return readPreparedTxn(r, PgOutputMessage.StreamPrepare::new);
    }

    /** 'P' Prepare 与 'p' StreamPrepare 的消息体同构（I64/I64/I64/I32/Str），仅 record 类型不同，复用读取。 */
    private static <T> T readPreparedTxn(ByteBufferReader r, PreparedTxnCtor<T> ctor) {
        long prepareLsn = r.readLong();
        long endLsn = r.readLong();
        Instant prepareTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return ctor.create(prepareLsn, endLsn, prepareTs, xid, gid);
    }

    /** Prepare/StreamPrepare 共用的五字段构造器。 */
    @FunctionalInterface
    private interface PreparedTxnCtor<T> {
        T create(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid);
    }
}
