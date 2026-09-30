package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputStreamDecoder;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TruncateOption;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PgWire 字节基建与生产解码器的往返冒烟:每个构造器产出的字节经
 * {@link PgOutputStreamDecoder#decode} 解回强类型 record,以值相等断言布局正确——
 * 组装器单测的全部输入都来自 PgWire,本类先把"字节构造器 ↔ 生产 parser 族"的镜像
 * 关系钉死(PgWire 是引擎测试同名类的 1:1 重写,这里以 connector 自己的解码器互证)。
 *
 * <p>占位约定沿用 PgWire 类 javadoc:LSN 按出现顺序填 1、2;微秒时间戳填 0
 * (解码后为 {@link PgWire#PG_EPOCH})。streamed 包装与 tuple 辅助一并覆盖
 * (流块内变体经先喂 'S' 置位解码器流块状态后解码,断言 streamXid 前缀生效)。
 */
class PgWireTest {

    /** 微秒占位 0 的解码结果(PG 纪元),全部时间戳断言统一引用。 */
    private static final Instant TS = PgWire.PG_EPOCH;

    /** 每用例新建(流块状态 inStream 是实例字段,用例间不复用)。 */
    private final PgOutputStreamDecoder decoder = new PgOutputStreamDecoder(StreamingMode.ON);

    /** 便捷:构造字节并立即经生产解码器解回强类型消息。 */
    private PgOutputMessage roundTrip(byte[] raw) {
        return decoder.decode(ByteBuffer.wrap(raw));
    }

    /** 责任:Begin/Commit 两控制消息的往返——LSN 占位序列与 PG_EPOCH 时间戳逐组件断言。 */
    @Test
    void beginAndCommitRoundTrip() {
        PgOutputMessage.Begin b = assertInstanceOf(PgOutputMessage.Begin.class, roundTrip(PgWire.begin(505L)));
        assertEquals(1L, b.finalLsn());
        assertEquals(TS, b.commitTimestamp());
        assertEquals(505L, b.xid());

        PgOutputMessage.Commit c = assertInstanceOf(PgOutputMessage.Commit.class, roundTrip(PgWire.commit()));
        assertEquals(1L, c.commitLsn());
        assertEquals(2L, c.endLsn());
        assertEquals(TS, c.commitTimestamp());
    }

    /** 责任:Relation 往返——oid/schema/table/replicaIdentity 与列元数据占位(首列键列 int、其余 text)。 */
    @Test
    void relationRoundTrip() {
        PgOutputMessage.Relation r = assertInstanceOf(PgOutputMessage.Relation.class,
                roundTrip(PgWire.relation(16384, "t", "id", "v")));
        assertEquals(16384, r.relationOid());
        assertEquals("public", r.schema());
        assertEquals("t", r.table());
        assertEquals('d', r.replicaIdentity());
        assertEquals(2, r.columns().size());
        assertTrue(r.columns().get(0).partOfKey());
        assertEquals(23, r.columns().get(0).typeId());
        assertEquals("id", r.columns().get(0).name());
        assertEquals(-1, r.columns().get(0).typeModifier());
        assertEquals("v", r.columns().get(1).name());
        assertEquals(25, r.columns().get(1).typeId());
        assertTrue(!r.columns().get(1).partOfKey());
    }

    /** 责任:I/U/D 三 DML 往返——元组列值经 tuple/textCol/nullCol 构造后解回 Text/Null 形态。 */
    @Test
    void dmlRoundTrip() {
        PgOutputMessage.Insert i = assertInstanceOf(PgOutputMessage.Insert.class,
                roundTrip(PgWire.insert(16384, PgWire.tuple("1", "a"))));
        assertEquals(16384, i.relationOid());
        assertEquals(new TupleValue.Text("1"), i.newTuple().columns().get(0));
        assertEquals(new TupleValue.Text("a"), i.newTuple().columns().get(1));

        PgOutputMessage.Update u = assertInstanceOf(PgOutputMessage.Update.class,
                roundTrip(PgWire.update(16384, null, null, PgWire.tuple("1", "b"))));
        assertTrue(u.oldTuple().isEmpty());   // 无旧镜像(REPLICA IDENTITY DEFAULT 常态)
        assertEquals(new TupleValue.Text("b"), u.newTuple().columns().get(1));

        PgOutputMessage.Update uKeyed = assertInstanceOf(PgOutputMessage.Update.class,
                roundTrip(PgWire.update(16384, 'K', PgWire.tuple("1", null), PgWire.tuple("1", "c"))));
        assertEquals(Optional.of(new TupleValue.Text("1")),
                uKeyed.oldTuple().map(t -> t.columns().get(0)));
        assertInstanceOf(TupleValue.Null.class, uKeyed.oldTuple().orElseThrow().columns().get(1));   // nullCol 分支

        PgOutputMessage.Delete d = assertInstanceOf(PgOutputMessage.Delete.class,
                roundTrip(PgWire.delete(16384, 'O', PgWire.tuple("1", "b"))));
        assertEquals(new TupleValue.Text("1"), d.oldTuple().columns().get(0));
    }

    /** 责任:Truncate 往返——多 oid 数组与选项位(CASCADE=bit0)值相等(record override 生效)。 */
    @Test
    void truncateRoundTrip() {
        PgOutputMessage.Truncate t = assertInstanceOf(PgOutputMessage.Truncate.class,
                roundTrip(PgWire.truncate(new int[]{16384, 16385}, (byte) 0x01)));
        assertArrayEquals(new int[]{16384, 16385}, t.relationOids());
        assertTrue(t.options().contains(TruncateOption.CASCADE));
    }

    /** 责任:LogicalMsg 往返——transactional/prefix/content 值相等(数组 override 生效)。 */
    @Test
    void logicalMsgRoundTrip() {
        PgOutputMessage.LogicalMsg m = assertInstanceOf(PgOutputMessage.LogicalMsg.class,
                roundTrip(PgWire.logicalMsg(true, "p", new byte[]{1, 2})));
        assertTrue(m.transactional());
        assertEquals("p", m.prefix());
        assertArrayEquals(new byte[]{1, 2}, m.content());
    }

    /** 责任:Type/Origin 往返——布局合法即可(组装器对二者直接丢弃,断言只证可解码)。 */
    @Test
    void typeAndOriginRoundTrip() {
        PgOutputMessage.Type y = assertInstanceOf(PgOutputMessage.Type.class, roundTrip(PgWire.type(25, "text")));
        assertEquals(25, y.typeOid());
        assertEquals("text", y.name());

        PgOutputMessage.Origin o = assertInstanceOf(PgOutputMessage.Origin.class, roundTrip(PgWire.origin("origin-1")));
        assertEquals(1L, o.originCommitLsn());
        assertEquals("origin-1", o.originName());
    }

    /** 责任:流式控制四消息(S/E/c/A)往返——注意 'c' 的 xid 后 I8 flags(0) 与 'A' 的非 parallel 形态(无附加字段)。 */
    @Test
    void streamControlRoundTrip() {
        PgOutputMessage.StreamStart s = assertInstanceOf(PgOutputMessage.StreamStart.class,
                roundTrip(PgWire.streamStart(7001L, true)));
        assertEquals(7001L, s.xid());
        assertTrue(s.firstSegment());

        assertInstanceOf(PgOutputMessage.StreamStop.class, roundTrip(PgWire.streamStop()));

        PgOutputMessage.StreamCommit c = assertInstanceOf(PgOutputMessage.StreamCommit.class,
                roundTrip(PgWire.streamCommit(7001L)));
        assertEquals(7001L, c.xid());
        assertEquals(1L, c.commitLsn());
        assertEquals(2L, c.endLsn());
        assertEquals(TS, c.commitTimestamp());

        PgOutputMessage.StreamAbort a = assertInstanceOf(PgOutputMessage.StreamAbort.class,
                roundTrip(PgWire.streamAbort(7001L, 7003L)));
        assertEquals(7001L, a.xid());
        assertEquals(7003L, a.subxid());
        assertTrue(a.abortLsn().isEmpty());        // 非 parallel 形态:无附加字段
        assertTrue(a.abortTimestamp().isEmpty());
    }

    /** 责任:两阶段五消息(b/P/K/r/p)往返——LSN 占位序列、双时间戳(唯一)与 gid/xid 逐组件断言。 */
    @Test
    void twoPhaseRoundTrip() {
        PgOutputMessage.BeginPrepare b = assertInstanceOf(PgOutputMessage.BeginPrepare.class,
                roundTrip(PgWire.beginPrepare(601L, "g")));
        assertEquals(1L, b.prepareLsn());
        assertEquals(2L, b.endLsn());
        assertEquals(TS, b.prepareTimestamp());
        assertEquals(601L, b.xid());
        assertEquals("g", b.gid());

        assertInstanceOf(PgOutputMessage.Prepare.class, roundTrip(PgWire.prepare(601L, "g")));
        assertInstanceOf(PgOutputMessage.CommitPrepared.class, roundTrip(PgWire.commitPrepared(601L, "g")));
        assertInstanceOf(PgOutputMessage.RollbackPrepared.class, roundTrip(PgWire.rollbackPrepared(601L, "g")));
        assertInstanceOf(PgOutputMessage.StreamPrepare.class, roundTrip(PgWire.streamPrepare(601L, "g")));
    }

    /**
     * 责任:streamed 包装的流块内变体——先喂 'S' 置位解码器 inStream,再解码被包装的
     * Insert/Relation,断言 Int32 xid 前缀被读出为 streamXid(非空)且其余字段不受影响。
     */
    @Test
    void streamedWrapperCarriesXidPrefix() {
        decoder.decode(ByteBuffer.wrap(PgWire.streamStart(7001L, true)));
        PgOutputMessage.Insert i = assertInstanceOf(PgOutputMessage.Insert.class,
                roundTrip(PgWire.streamed(7003L, PgWire.insert(16384, PgWire.tuple("1", "a")))));
        assertEquals(OptionalLong.of(7003L), i.streamXid());
        assertEquals(16384, i.relationOid());

        PgOutputMessage.Relation r = assertInstanceOf(PgOutputMessage.Relation.class,
                roundTrip(PgWire.streamed(7001L, PgWire.relation(16384, "t", "id", "v"))));
        assertEquals(OptionalLong.of(7001L), r.streamXid());
        assertEquals("t", r.table());
    }
}
