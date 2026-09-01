package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 族 1：DML 消息（'I' insert / 'U' update / 'D' delete / 'T' truncate / 'M' logicalMsg）
 * 与 TupleData 的解析，decoder 剥离类型字节后从消息体首字段读起。引擎
 * {@code org.vastdata.vbstream.protocol.DmlParsers} 的 1:1 重写（设计决策 D2：
 * 重写不 import 引擎类），读取序列与 fail-fast 语义逐行一致。
 * 格式见 spec 附录 A（字节格式表以计划文档转引为准）。纯函数无状态：所有方法
 * 包私有 static，仅由解码器在持有它的单一线程内调用。
 */
final class DmlParsers {

    private DmlParsers() {
    }

    /**
     * 解析 'I' insert：I32 relationOid + 'N' 标记 + TupleData（新行）。
     * 'N' 标记经 {@link #expectTupleTag} 校验，非法即 fail-fast。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.Insert insert(WireReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        expectTupleTag(r, 'N');
        return new PgOutputMessage.Insert(streamXid, oid, tupleData(r));
    }

    /**
     * 解析 'U' update 三形态：I32 relationOid 后首标记分支——①'K' key 元组
     * ②'O' 旧整行（REPLICA IDENTITY FULL），两者都再读 'N' + 新元组；③直接 'N'
     * 无旧元组（oldTuple=empty）。标记既非 'K'/'O' 也非 'N' 时 fail-fast。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.Update update(WireReader r, OptionalLong streamXid) {
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

    /**
     * 解析 'D' delete：I32 relationOid + 'K'（key）或 'O'（旧整行）标记 + TupleData。
     * 协议规定 delete 必带旧元组，'N' 等其他标记 fail-fast（insert 语义混进即拒绝）。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.Delete delete(WireReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        byte tag = r.readByte();
        if (tag != 'K' && tag != 'O') {
            throw new UnknownMessageTypeException(tag, r);
        }
        return new PgOutputMessage.Delete(streamXid, oid, tupleData(r));
    }

    /**
     * 解析 'T' truncate：I32 关系数 + I8 选项位（无符号读，bit0/bit1 拼进
     * {@link EnumSet}）+ N × I32 relationOid（一条语句可截断多张表）。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.Truncate truncate(WireReader r, OptionalLong streamXid) {
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

    /**
     * 解析 'M' logicalMsg：I8 flags（只有 bit0 = transactional，其余位隔离不读）
     * + I64 lsn + CString prefix + I32 长度 + content 字节（恰取长度前缀声明的量，
     * 多读少读都会让后续消息错位）。
     *
     * @param streamXid 流式块内为块的事务号，顶层消息传 empty——parser 不读不判断，原样入 record
     */
    static PgOutputMessage.LogicalMsg logicalMsg(WireReader r, OptionalLong streamXid) {
        boolean transactional = (r.readByte() & 0x01) != 0; // bit0 = transactional
        long lsn = r.readLong();
        String prefix = r.readString();
        int len = r.readInt();
        byte[] content = r.readBytes(len);
        return new PgOutputMessage.LogicalMsg(streamXid, transactional, lsn, prefix, content);
    }

    /**
     * 读 1 字节并校验其为期望的元组标记（'N'/'K'/'O'），不符即 fail-fast 抛
     * {@link UnknownMessageTypeException}——元组标记错位往往正是消息错位的首发现场。
     *
     * @param expected 期望的元组标记字符
     */
    private static void expectTupleTag(WireReader r, char expected) {
        byte tag = r.readByte();
        if (tag != expected) {
            throw new UnknownMessageTypeException(tag, r);
        }
    }

    /**
     * 解析 TupleData：I16 列数；每列 I8 种类字节分支——'n' null / 'u' TOAST 未变
     * （两者均无负载）/'t' 文本（I32 长度 + 字节按 UTF-8 解码）/'b' 二进制
     * （I32 长度 + 原始字节）；未知种类 fail-fast。出口 {@code List.copyOf} 落地
     * TupleData javadoc 的不可变契约（record 自身不再复制）。
     */
    static TupleData tupleData(WireReader r) {
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
