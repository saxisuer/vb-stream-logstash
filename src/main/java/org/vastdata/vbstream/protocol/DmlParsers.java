package org.vastdata.vbstream.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** 族 1：DML 消息与 TupleData。格式见 spec 附录 A。 */
final class DmlParsers {

    private DmlParsers() {
    }

    static PgOutputMessage.Insert insert(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        expectTupleTag(r, 'N');
        return new PgOutputMessage.Insert(streamXid, oid, tupleData(r));
    }

    static PgOutputMessage.Update update(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        Optional<TupleData> oldTuple = Optional.empty();
        byte tag = r.readByte();
        if (tag == 'K' || tag == 'O') {
            oldTuple = Optional.of(tupleData(r));
            expectTupleTag(r, 'N');
        } else if (tag != 'N') {
            throw new UnknownMessageTypeException(tag, r);
        }
        TupleData newTuple = tupleData(r);
        return new PgOutputMessage.Update(streamXid, oid, oldTuple, newTuple);
    }

    static PgOutputMessage.Delete delete(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        byte tag = r.readByte();
        if (tag != 'K' && tag != 'O') {
            throw new UnknownMessageTypeException(tag, r);
        }
        return new PgOutputMessage.Delete(streamXid, oid, tupleData(r));
    }

    static PgOutputMessage.Truncate truncate(ByteBufferReader r, OptionalLong streamXid) {
        int nrel = r.readInt();
        int bits = r.readUnsignedByte();
        EnumSet<TruncateOption> options = EnumSet.noneOf(TruncateOption.class);
        if ((bits & 0x01) != 0) {
            options.add(TruncateOption.CASCADE);      // bit0 = CASCADE
        }
        if ((bits & 0x02) != 0) {
            options.add(TruncateOption.RESTART_IDENTITY); // bit1 = RESTART IDENTITY
        }
        int[] oids = new int[nrel];
        for (int i = 0; i < nrel; i++) {
            oids[i] = r.readInt();
        }
        return new PgOutputMessage.Truncate(streamXid, options, oids);
    }

    static PgOutputMessage.LogicalMsg logicalMsg(ByteBufferReader r, OptionalLong streamXid) {
        boolean transactional = r.readByte() != 0;
        long lsn = r.readLong();
        String prefix = r.readString();
        int len = r.readInt();
        byte[] content = r.readBytes(len);
        return new PgOutputMessage.LogicalMsg(streamXid, transactional, lsn, prefix, content);
    }

    private static void expectTupleTag(ByteBufferReader r, char expected) {
        byte tag = r.readByte();
        if (tag != expected) {
            throw new UnknownMessageTypeException(tag, r);
        }
    }

    /** TupleData：I16 列数；每列 'n'/'u' 无负载，'t'/'b' 为 I32 长度 + 字节。 */
    static TupleData tupleData(ByteBufferReader r) {
        int ncols = r.readUnsignedShort();
        List<TupleValue> values = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            byte kind = r.readByte();
            values.add(switch (kind) {
                case 'n' -> TupleValue.NULL;
                case 'u' -> TupleValue.UNCHANGED_TOAST;
                case 't' -> new TupleValue.Text(new String(r.readBytes(r.readInt()), StandardCharsets.UTF_8));
                case 'b' -> new TupleValue.Binary(r.readBytes(r.readInt()));
                default -> throw new UnknownMessageTypeException(kind, r);
            });
        }
        return new TupleData(List.copyOf(values));
    }
}
