package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.time.Instant;

/**
 * 族 4：两阶段提交消息（'b' BeginPrepare / 'P' Prepare / 'K' CommitPrepared /
 * 'r' RollbackPrepared / 'p' StreamPrepare）的解析，decoder 剥离类型字节后从消息体
 * 首字段读起。引擎 {@code org.vastdata.vbstream.protocol.TwoPhaseParsers} 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），读取序列与复用手法逐行一致。
 * 格式见 spec 附录 A（字节格式表以计划文档转引为准）。纯函数无状态：所有方法
 * 包私有 static，仅由解码器在持有它的单一线程内调用。
 */
final class TwoPhaseParsers {

    private TwoPhaseParsers() {
    }

    /**
     * 解析 'b' BeginPrepare：I64 prepareLsn + I64 endLsn + I64 prepareTs（PG 纪元微秒
     * 换算）+ I32 xid（无符号读入 long）+ CString gid。注意本消息**无** flags 字节
     * （与 'P'/'K'/'r'/'p' 的差异点），直接从 prepareLsn 读起。
     */
    static PgOutputMessage.BeginPrepare beginPrepare(WireReader r) {
        long prepareLsn = r.readLong();
        long endLsn = r.readLong();
        Instant prepareTs = WireReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.BeginPrepare(prepareLsn, endLsn, prepareTs, xid, gid);
    }

    /**
     * 解析 'P' Prepare：首字节 I8(0) flags 读掉不建模（漏读 1 字节即后续字段全部
     * 错位），随后五字段与 'p' StreamPrepare 同构，经 {@link #readPreparedTxn} 复用读取。
     */
    static PgOutputMessage.Prepare prepare(WireReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        return readPreparedTxn(r, PgOutputMessage.Prepare::new);
    }

    /**
     * 解析 'K' CommitPrepared：首字节 I8(0) flags 读掉不建模，随后 I64 commitLsn +
     * I64 endLsn + I64 commitTs（PG 纪元微秒换算）+ I32 xid + CString gid
     * （gid 与 Prepare 阶段配对）。
     */
    static PgOutputMessage.CommitPrepared commitPrepared(WireReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = WireReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.CommitPrepared(commitLsn, endLsn, commitTs, xid, gid);
    }

    /**
     * 解析 'r' RollbackPrepared：首字节 I8(0) flags 读掉不建模，随后 I64 prepareEndLsn +
     * I64 rollbackEndLsn + **两个** I64 微秒时间戳（先 prepare 后 rollback，顺序不可换
     * ——唯一双时间戳消息）+ I32 xid + CString gid。
     */
    static PgOutputMessage.RollbackPrepared rollbackPrepared(WireReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long prepareEndLsn = r.readLong();
        long rollbackEndLsn = r.readLong();
        Instant prepareTs = WireReader.pgMicrosToInstant(r.readLong());
        Instant rollbackTs = WireReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.RollbackPrepared(prepareEndLsn, rollbackEndLsn, prepareTs, rollbackTs, xid, gid);
    }

    /**
     * 解析 'p' StreamPrepare：首字节 I8(0) flags 读掉不建模，随后与 'P' Prepare
     * 完全同构的五字段，经 {@link #readPreparedTxn} 复用读取。
     */
    static PgOutputMessage.StreamPrepare streamPrepare(WireReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        return readPreparedTxn(r, PgOutputMessage.StreamPrepare::new);
    }

    /**
     * 'P' Prepare 与 'p' StreamPrepare 的消息体同构（I64 prepareLsn + I64 endLsn +
     * I64 prepareTs + I32 xid + CString gid），仅构造的 record 类型不同——经构造器
     * 函数复用同一条读取路径，两处解析行为由同一份代码保证一致。
     */
    private static <T> T readPreparedTxn(WireReader r, PreparedTxnCtor<T> ctor) {
        long prepareLsn = r.readLong();
        long endLsn = r.readLong();
        Instant prepareTs = WireReader.pgMicrosToInstant(r.readLong());
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
