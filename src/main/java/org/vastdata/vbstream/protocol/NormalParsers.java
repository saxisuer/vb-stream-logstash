package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/** 族 1/2：事务边界与元数据消息。格式见 spec 附录 A。 */
final class NormalParsers {

    private NormalParsers() {
    }

    static PgOutputMessage.Begin begin(ByteBufferReader r) {
        long finalLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        return new PgOutputMessage.Begin(finalLsn, commitTs, xid);
    }

    static PgOutputMessage.Commit commit(ByteBufferReader r) {
        r.readByte(); // currently-unused flags，消费不建模；漏读 1 字节即后续字段全部错位
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        return new PgOutputMessage.Commit(commitLsn, endLsn, commitTs);
    }

    static PgOutputMessage.Origin origin(ByteBufferReader r) {
        long lsn = r.readLong();
        String name = r.readString();
        return new PgOutputMessage.Origin(lsn, name);
    }

    static PgOutputMessage.Relation relation(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        String schema = r.readString();
        String table = r.readString();
        char replident = (char) r.readByte();
        int ncols = r.readUnsignedShort();
        List<Column> cols = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            boolean partOfKey = (r.readByte() & 0x01) != 0; // bit0 = key 列
            String name = r.readString();
            int typeId = r.readInt();
            int typmod = r.readInt();
            cols.add(new Column(name, typeId, typmod, partOfKey));
        }
        return new PgOutputMessage.Relation(streamXid, oid, schema, table, replident, List.copyOf(cols));
    }

    static PgOutputMessage.Type type(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        String schema = r.readString();
        String name = r.readString();
        return new PgOutputMessage.Type(streamXid, oid, schema, name);
    }
}
