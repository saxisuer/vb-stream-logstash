package org.vastdata.debezium.connector.postgresql.stream;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

/**
 * 测试用 pgoutput 线格式字节构造器(raw 驱动组装器单测的底座):以静态方法构造<b>完整单条</b>
 * 消息的 {@code byte[]}(含首个类型字节与其后全部字段,布局按解码设计文档附录 A,与生产
 * 解析器 {@code NormalParsers}/{@code DmlParsers}/{@code StreamParsers}/{@code TwoPhaseParsers}
 * 逐一镜像——往返互证见 {@code PgWireTest})。引擎测试
 * {@code org.vastdata.vbstream.replication.PgWire} 的 1:1 重写(文字参照,非依赖)。
 * 全部整数字段 big-endian;字符串为 null 结尾 UTF-8(CString)。
 *
 * <p>占位约定(组装器单测只断言路由与组装语义,不断言服务端真实 LSN/时间戳):
 * <ul>
 *   <li>LSN 字段:每条消息内按出现顺序填 1、2、3……递增</li>
 *   <li>微秒时间戳字段:一律填 0——解码器换算后为 {@link #PG_EPOCH}
 *       (2000-01-01T00:00:00Z,PG 纪元),断言侧直接引用该常量</li>
 *   <li>{@code relation} 的列元数据:首列 partOfKey=true、typeId=23(int),其余列
 *       partOfKey=false、typeId=25(text),typmod 恒 -1——单测不断言列类型细节,仅保持布局合法</li>
 * </ul>
 *
 * <p>流式块内变体:协议规定块内消息在类型字节后前置 Int32 xid,用 {@link #streamed(long, byte[])}
 * 把任一顶层消息包装为块内形态({@code concat(type, i32(xid), body)})。
 *
 * <p>线程约束:无状态纯函数,线程安全。
 */
public final class PgWire {

    /** 微秒占位 0 经解码器换算得到的时间戳(PG 纪元 2000-01-01T00:00:00Z),断言侧引用。 */
    public static final Instant PG_EPOCH = Instant.ofEpochSecond(946684800L);

    /** 工具类禁止实例化。注意:{@link #streamAbort} 只产出非 parallel 形态(无 abort_lsn/abort_time 附加字段),使用方组装器须以非 PARALLEL 模式构造。 */
    private PgWire() {
    }

    /**
     * 责任:构造 Begin('B')——I64 finalLsn(占位 1)+ I64 提交时间戳微秒(占位 0)+ I32 xid。
     * 边界:xid 按无符号 Int32 写出(调用方测试值须落在 0..2^32-1)。
     */
    public static byte[] begin(long xid) {
        return new Wire().type('B').i64(1L).i64(0L).u32(xid).toByteArray();
    }

    /**
     * 责任:构造 Commit('C')——I8 currently-unused flags(0) + I64 commitLsn(占位 1)+
     * I64 endLsn(占位 2)+ I64 提交时间戳微秒(占位 0)。消息体无 xid 字段(协议如此)。
     */
    public static byte[] commit() {
        return new Wire().type('C').i8(0).i64(1L).i64(2L).i64(0L).toByteArray();
    }

    /**
     * 责任:构造 Insert('I')——I32 relationOid + 元组标记 'N' + TupleData 段。
     * 边界:tuple 为 {@link #tuple(String...)} 的产物(I16 列数 + 逐列种类字节)。
     */
    public static byte[] insert(int oid, byte[] tuple) {
        return new Wire().type('I').i32(oid).i8('N').raw(tuple).toByteArray();
    }

    /**
     * 责任:构造 Update('U')——I32 relationOid + 可选旧元组段 + 'N' + 新元组。
     * 关键步骤:oldTag 为 null 时直接 'N'(无旧镜像,REPLICA IDENTITY DEFAULT 的常态);
     * 为 'K'(键元组)/ 'O'(旧整行)时写标记 + oldTuple + 'N'。
     * 边界:oldTag 非 null 时 oldTuple 不得为 null(否则字节流缺 TupleData 段,解码错位)。
     */
    public static byte[] update(int oid, Character oldTag, byte[] oldTuple, byte[] newTuple) {
        Wire w = new Wire().type('U').i32(oid);
        if (oldTag != null) {
            w.i8(oldTag).raw(oldTuple);
        }
        return w.i8('N').raw(newTuple).toByteArray();
    }

    /**
     * 责任:构造 Delete('D')——I32 relationOid + 元组标记(必须 'K' 或 'O',与生产解析器
     * 的 fail-fast 一致)+ TupleData 段。
     */
    public static byte[] delete(int oid, char tag, byte[] tuple) {
        return new Wire().type('D').i32(oid).i8(tag).raw(tuple).toByteArray();
    }

    /**
     * 责任:构造 Truncate('T')——I32 表数 + I8 选项位(bit0=CASCADE、bit1=RESTART_IDENTITY,
     * 由调用方拼位)+ N 个 I32 oid。
     */
    public static byte[] truncate(int[] oids, byte options) {
        Wire w = new Wire().type('T').i32(oids.length).i8(options);
        for (int oid : oids) {
            w.i32(oid);
        }
        return w.toByteArray();
    }

    /**
     * 责任:构造 LogicalMsg('M')——I8 flags(bit0=transactional)+ I64 lsn(占位 1)+
     * prefix CString + I32 长度 + 内容字节。
     */
    public static byte[] logicalMsg(boolean transactional, String prefix, byte[] content) {
        return logicalMsg(transactional, 1L, prefix, content);
    }

    /**
     * 责任:构造显式 lsn 的 LogicalMsg('M')(MS3.5 护栏用例需要消息 LSN 与各控制消息的
     * 占位 LSN 拉开差距——无 pending 推进到 msgLsn、有 pending 压到 commitLsn 之下,
     * 占位 1 与 Commit 的占位 1 同值无法区分)。布局同三参重载,仅 lsn 由调用方给出。
     */
    public static byte[] logicalMsg(boolean transactional, long lsn, String prefix, byte[] content) {
        return new Wire().type('M').i8(transactional ? 0x01 : 0x00).i64(lsn)
                .str(prefix).i32(content.length).raw(content).toByteArray();
    }

    /**
     * 责任:构造 Relation('R')——I32 oid + schema CString(固定 "public")+ table CString +
     * I8 replicaIdentity(固定 'd')+ I16 列数 + 逐列 [I8 flags, name CString, I32 typeId, I32 typmod]。
     * 列元数据占位见类 javadoc(首列键列 int、其余 text)。
     */
    public static byte[] relation(int oid, String table, String... colNames) {
        Wire w = new Wire().type('R').i32(oid).str("public").str(table).i8('d').i16(colNames.length);
        for (int i = 0; i < colNames.length; i++) {
            w.i8(i == 0 ? 0x01 : 0x00).str(colNames[i]).i32(i == 0 ? 23 : 25).i32(-1);
        }
        return w.toByteArray();
    }

    /**
     * 责任:构造 Type('Y')——I32 typeOid + schema CString(固定 "pg_catalog")+ name CString。
     * 组装器对 'Y' 直接 DEBUG 丢弃,字节内容仅保持布局合法。
     */
    public static byte[] type(int typeOid, String name) {
        return new Wire().type('Y').i32(typeOid).str("pg_catalog").str(name).toByteArray();
    }

    /** 富 Relation 列描述(供 {@link PgWire#relation(int, String, String, char, Col...)}):名称/键旗标/类型 oid/typmod 全显式。 */
    public record Col(String name, boolean key, int typeId, int typmod) {
    }

    /**
     * 责任:构造<b>全显式</b>的 Relation('R')——schema/replicaIdentity/逐列
     * (flags bit0=key, name, typeId, typmod)均由调用方给出(RelationTableFactory 单测
     * 需要类型 oid/typmod 的真实变化面,简版 {@link #relation(int, String, String...)}的
     * 占位约定不够用)。
     * 边界:cols 为空产出零列表(布局合法,调用方自负语义)。
     */
    public static byte[] relation(int oid, String schema, String table, char replicaIdentity, Col... cols) {
        Wire w = new Wire().type('R').i32(oid).str(schema).str(table).i8(replicaIdentity).i16(cols.length);
        for (Col col : cols) {
            w.i8(col.key() ? 0x01 : 0x00).str(col.name()).i32(col.typeId()).i32(col.typmod());
        }
        return w.toByteArray();
    }

    /**
     * 责任:构造 Origin('O')——I64 originCommitLsn(占位 1)+ name CString。
     * 组装器对 'O' 直接 DEBUG 丢弃,字节内容仅保持布局合法。
     */
    public static byte[] origin(String name) {
        return new Wire().type('O').i64(1L).str(name).toByteArray();
    }

    /**
     * 责任:构造 StreamStart('S')——I32 顶层 xid + I8 firstSegment(1=首段,0=续段)。
     */
    public static byte[] streamStart(long xid, boolean firstSegment) {
        return new Wire().type('S').u32(xid).i8(firstSegment ? 1 : 0).toByteArray();
    }

    /** 责任:构造 StreamStop('E')——消息体无字段。 */
    public static byte[] streamStop() {
        return new Wire().type('E').toByteArray();
    }

    /**
     * 责任:构造 StreamCommit('c')——I32 顶层 xid + I8 currently-unused flags(0) +
     * I64 commitLsn(占位 1)+ I64 endLsn(占位 2)+ I64 提交时间戳微秒(占位 0)。
     * 注意 xid 之后那 1 字节 flags 是协议钉死的被消费字段,漏写即整条流错位。
     */
    public static byte[] streamCommit(long xid) {
        return new Wire().type('c').u32(xid).i8(0).i64(1L).i64(2L).i64(0L).toByteArray();
    }

    /**
     * 责任:构造 StreamAbort('A')的<b>非 parallel 形态</b>——I32 顶层 xid + I32 被回滚的
     * (子)事务 xid,无附加字段。
     * 边界:组装器的解析模式决定 abort 是否携带 abort_lsn/abort_time(PARALLEL 模式附加
     * I64+I64)——本方法只与非 PARALLEL 模式的组装器配对使用。
     */
    public static byte[] streamAbort(long xid, long subxid) {
        return new Wire().type('A').u32(xid).u32(subxid).toByteArray();
    }

    /**
     * 责任:构造 BeginPrepare('b')——I64 prepareLsn(占位 1)+ I64 endLsn(占位 2)+
     * I64 prepare 时间戳微秒(占位 0)+ I32 xid + gid CString。
     */
    public static byte[] beginPrepare(long xid, String gid) {
        return new Wire().type('b').i64(1L).i64(2L).i64(0L).u32(xid).str(gid).toByteArray();
    }

    /**
     * 责任:构造 Prepare('P')——I8 currently-unused flags(0) + 五字段
     * (I64/I64/I64 占位 1、2、0 + I32 xid + gid CString)。
     */
    public static byte[] prepare(long xid, String gid) {
        return new Wire().type('P').i8(0).i64(1L).i64(2L).i64(0L).u32(xid).str(gid).toByteArray();
    }

    /**
     * 责任:构造 CommitPrepared('K')——I8 currently-unused flags(0) +
     * I64 commitLsn(占位 1)+ I64 endLsn(占位 2)+ I64 提交时间戳微秒(占位 0)+
     * I32 xid + gid CString。
     */
    public static byte[] commitPrepared(long xid, String gid) {
        return new Wire().type('K').i8(0).i64(1L).i64(2L).i64(0L).u32(xid).str(gid).toByteArray();
    }

    /**
     * 责任:构造 RollbackPrepared('r')——I8 currently-unused flags(0) + I64 prepareEndLsn
     * (占位 1)+ I64 rollbackEndLsn(占位 2)+ <b>两个</b> I64 微秒时间戳(均占位 0)+
     * I32 xid + gid CString(唯一双时间戳消息)。
     */
    public static byte[] rollbackPrepared(long xid, String gid) {
        return new Wire().type('r').i8(0).i64(1L).i64(2L).i64(0L).i64(0L).u32(xid).str(gid).toByteArray();
    }

    /**
     * 责任:构造 StreamPrepare('p')——与 {@link #prepare} 消息体同构
     * (I8(0) + I64/I64/I64 占位 1、2、0 + I32 xid + gid CString),仅类型字节不同。
     */
    public static byte[] streamPrepare(long xid, String gid) {
        return new Wire().type('p').i8(0).i64(1L).i64(2L).i64(0L).u32(xid).str(gid).toByteArray();
    }

    /**
     * 责任:把任一已构造的顶层消息(I/U/D/T/M/R 等)包装为<b>流式块内形态</b>——
     * {@code concat(type, i32(xid), body)}:保留类型字节,在其后插入 Int32 xid 前缀,
     * 其余字段整体后移。
     * 边界:msg 至少含类型字节(空数组抛 ArrayIndexOutOfBoundsException);xid 写为
     * 无符号 Int32;对不含 xid 前缀语义的消息类型(如控制消息)包装后是非法流,调用方自负。
     */
    public static byte[] streamed(long xid, byte[] msg) {
        byte[] out = new byte[msg.length + 4];
        out[0] = msg[0];
        out[1] = (byte) (xid >>> 24);
        out[2] = (byte) (xid >>> 16);
        out[3] = (byte) (xid >>> 8);
        out[4] = (byte) xid;
        System.arraycopy(msg, 1, out, 5, msg.length - 1);
        return out;
    }

    /**
     * 责任:构造 TupleData 段——I16 列数 + 逐列种类字节段。
     * 边界:元素为 null 时写 {@link #nullCol()}('n'),否则写 {@link #textCol(String)}。
     */
    public static byte[] tuple(String... values) {
        Wire w = new Wire().i16(values.length);
        for (String v : values) {
            w.raw(v == null ? nullCol() : textCol(v));
        }
        return w.toByteArray();
    }

    /**
     * 责任:构造文本列值段——'t' + I32 字节长度 + UTF-8 字节。
     */
    public static byte[] textCol(String value) {
        byte[] b = value.getBytes(StandardCharsets.UTF_8);
        return new Wire().i8('t').i32(b.length).raw(b).toByteArray();
    }

    /** 责任:构造 NULL 列值段——'n'(无负载)。 */
    public static byte[] nullCol() {
        return new Wire().i8('n').toByteArray();
    }

    /**
     * 极简 big-endian 字节装配器(仅测试构造用,非线程安全问题不存在——方法内局部实例)。
     * 每个公开 PgWire 方法新建一个实例、链式写入后经 {@link #toByteArray()} 落定。
     */
    private static final class Wire {

        private byte[] data = new byte[32];
        private int len;

        /** 写类型字节(ASCII 字符)。 */
        Wire type(char t) {
            return i8(t);
        }

        /** 写 1 字节(低 8 位)。 */
        Wire i8(int v) {
            ensure(1);
            data[len++] = (byte) v;
            return this;
        }

        /** 写 2 字节 big-endian。 */
        Wire i16(int v) {
            ensure(2);
            data[len++] = (byte) (v >>> 8);
            data[len++] = (byte) v;
            return this;
        }

        /** 写 4 字节 big-endian(有符号语义,oid/长度等)。 */
        Wire i32(int v) {
            ensure(4);
            data[len++] = (byte) (v >>> 24);
            data[len++] = (byte) (v >>> 16);
            data[len++] = (byte) (v >>> 8);
            data[len++] = (byte) v;
            return this;
        }

        /** 写 4 字节 big-endian(无符号语义,xid:按位拆分保证 2^31..2^32-1 值正确落盘)。 */
        Wire u32(long v) {
            return i32((int) v);
        }

        /** 写 8 字节 big-endian(LSN/微秒时间戳占位)。 */
        Wire i64(long v) {
            i32((int) (v >>> 32));
            return i32((int) v);
        }

        /** 写 null 结尾 UTF-8 字符串(CString)。 */
        Wire str(String s) {
            return raw(s.getBytes(StandardCharsets.UTF_8)).i8(0);
        }

        /** 原样追加一段已构造字节(TupleData 段/内容字节复用)。 */
        Wire raw(byte[] arr) {
            ensure(arr.length);
            System.arraycopy(arr, 0, data, len, arr.length);
            len += arr.length;
            return this;
        }

        /** 落定为精确长度的副本(不暴露内部缓冲)。 */
        byte[] toByteArray() {
            return Arrays.copyOf(data, len);
        }

        /** 容量按需倍增,保证后续 n 字节写入不越界。 */
        private void ensure(int n) {
            if (len + n > data.length) {
                data = Arrays.copyOf(data, Math.max(data.length * 2, len + n));
            }
        }
    }
}
