package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * 族 1/2：事务边界与元数据消息（'B'/'C'/'O'/'R'/'Y'）的解析，decoder 剥离类型字节后
 * 从消息体首字段读起。引擎 {@code org.vastdata.vbstream.protocol.NormalParsers} 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），读取序列与构造语义逐行一致。
 * 格式见 spec 附录 A（字节格式表以计划文档转引为准）。纯函数无状态：所有方法包私有
 * static，仅由解码器在持有它的单一线程内调用。
 */
final class NormalParsers {

    private NormalParsers() {
    }

    /**
     * 解析 'B' Begin：I64 finalLsn + I64 commitTs（PG 纪元微秒，经
     * {@link WireReader#pgMicrosToInstant} 换算）+ I32 xid（无符号读入 long，
     * 高位 xid 不变负）。越界读由 WireReader 抛 BufferUnderflowException。
     */
    static PgOutputMessage.Begin begin(WireReader r) {
        long finalLsn = r.readLong();
        Instant commitTs = WireReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        return new PgOutputMessage.Begin(finalLsn, commitTs, xid);
    }

    /**
     * 解析 'C' Commit：首字节 flags 读掉不建模（协议 currently-unused，漏读 1 字节即
     * 后续字段全部错位），随后 I64 commitLsn + I64 endLsn + I64 commitTs（PG 纪元微秒换算）。
     */
    static PgOutputMessage.Commit commit(WireReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = WireReader.pgMicrosToInstant(r.readLong());
        return new PgOutputMessage.Commit(commitLsn, endLsn, commitTs);
    }

    /**
     * 解析 'O' Origin：I64 originCommitLsn + CString originName（级联复制的源节点位点）。
     */
    static PgOutputMessage.Origin origin(WireReader r) {
        long lsn = r.readLong();
        String name = r.readString();
        return new PgOutputMessage.Origin(lsn, name);
    }

    /**
     * 解析 'R' Relation：I32 oid + CString schema + CString table + I8 replicaIdentity
     * （单字符，'d'/'i'/'f'/'n'）+ I16 列数 + 每列 [I8 flags（bit0=key）+ CString name
     * + I32 typeId + I32 typmod]。
     * 关键步骤：按列数预容后逐列循环收集，列 flags 的 bit0 揭示 partOfKey；
     * 出口 {@code List.copyOf} 落地 Relation javadoc 的不可变契约（record 自身不再复制）。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.Relation relation(WireReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        String schema = r.readString();
        String table = r.readString();
        char replident = (char) r.readByte();
        int ncols = r.readUnsignedShort();
        List<RelationColumn> cols = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            boolean partOfKey = (r.readByte() & 0x01) != 0; // bit0 = key 列
            String name = r.readString();
            int typeId = r.readInt();
            int typmod = r.readInt();
            cols.add(new RelationColumn(name, typeId, typmod, partOfKey));
        }
        return new PgOutputMessage.Relation(streamXid, oid, schema, table, replident, List.copyOf(cols));
    }

    /**
     * 解析 'Y' Type：I32 typeOid + CString schema + CString name（复合类型定义）。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.Type type(WireReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        String schema = r.readString();
        String name = r.readString();
        return new PgOutputMessage.Type(streamXid, oid, schema, name);
    }
}
