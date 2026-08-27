package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TruncateOption;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TransactionAssembler 状态机单测：直接以 protocol record 构造消息序列（组装器输入是已解析消息，
 * 无需字节级构造）。每用例断言输出 Transaction 的形态/顺序/内容与 fail-fast 行为。
 */
class TransactionAssemblerTest {

    private static final Instant TS = Instant.parse("2026-08-27T00:00:00Z");
    private static final int OID = 16384;

    /** 构造默认 oid 的 Relation 消息，供单表场景使用。 */
    private static PgOutputMessage.Relation relation() {
        return relation(OID);
    }

    /**
     * 按指定 oid 构造两列 (id int, v text) 的 Relation 消息，列序与 {@link #row} 对齐，
     * 供 Truncate 多表等需要多个不同 oid 的场景使用。表名默认 "t"，非默认 oid 用 "t"+oid 区分。
     */
    private static PgOutputMessage.Relation relation(int oid) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public",
                oid == OID ? "t" : "t" + oid, 'd',
                List.of(new Column("id", 23, -1, true), new Column("v", 25, -1, false)));
    }

    /** 构造一行文本元组 (id, v)。 */
    private static TupleData row(String id, String v) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(v)));
    }

    /** 流式块外的 Insert 消息。 */
    private static PgOutputMessage.Insert insert(String id, String v) {
        return new PgOutputMessage.Insert(OptionalLong.empty(), OID, row(id, v));
    }

    /** 流式块内的 Insert 消息（streamXid=产生该变更的（子）事务 xid）。Task 3 起使用。 */
    private static PgOutputMessage.Insert streamedInsert(long streamXid, String id, String v) {
        return new PgOutputMessage.Insert(OptionalLong.of(streamXid), OID, row(id, v));
    }

    /**
     * 依序把消息喂给新组装器（Relation 先经 registry.accept，其余经 assembler.accept——与 Main 的装配顺序一致），
     * 收集输出的 Transaction。
     */
    private static List<Transaction> run(PgOutputMessage... msgs) {
        RelationRegistry registry = new RelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add);
        for (PgOutputMessage m : msgs) {
            registry.accept(m);
            assembler.accept(m, registry);
        }
        return out;
    }

    @Test
    void assemblesNormalTransactionInOrder() {
        List<Transaction> out = run(
                new PgOutputMessage.Begin(0x100L, TS, 505L),
                relation(),
                insert("1", "a"),
                new PgOutputMessage.Update(OptionalLong.empty(), OID,
                        Optional.empty(), row("1", "b")),
                new PgOutputMessage.Delete(OptionalLong.empty(), OID, row("1", "b")),
                new PgOutputMessage.Commit(0x100L, 0x180L, TS));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(505L, t.xid());
        assertEquals(TransactionKind.NORMAL, t.kind());
        assertNull(t.gid());
        assertEquals(0x100L, t.commitLsn());
        assertEquals(0x180L, t.endLsn());
        assertEquals(TS, t.commitTimestamp());
        assertEquals(3, t.changes().size());
        RowChange c0 = (RowChange) t.changes().get(0);
        assertEquals(DmlKind.INSERT, c0.dml());
        assertEquals("t", c0.relation().table());          // Relation 快照嵌入
        assertEquals(row("1", "a"), c0.after().orElseThrow());
        assertEquals(DmlKind.UPDATE, ((RowChange) t.changes().get(1)).dml());
        assertEquals(DmlKind.DELETE, ((RowChange) t.changes().get(2)).dml());
    }

    @Test
    void consecutiveTransactionsEmitOneByOne() {
        List<Transaction> out = run(
                relation(),   // Relation 会话内一次到达、跨事务持续有效（registry 缓存）
                new PgOutputMessage.Begin(0x1L, TS, 1L),
                insert("1", "a"),
                new PgOutputMessage.Commit(0x1L, 0x2L, TS),
                new PgOutputMessage.Begin(0x3L, TS, 2L),
                insert("2", "b"),
                new PgOutputMessage.Commit(0x3L, 0x4L, TS));
        assertEquals(List.of(1L, 2L), out.stream().map(Transaction::xid).toList());
    }

    @Test
    void rejectsCommitWithoutBegin() {
        assertThrows(IllegalStateException.class, () ->
                run(new PgOutputMessage.Commit(1L, 2L, TS)));
    }

    @Test
    void rejectsDuplicateBegin() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Begin(1L, TS, 1L),
                new PgOutputMessage.Begin(2L, TS, 1L)));
    }

    @Test
    void rejectsChangeWithoutActiveBucket() {
        assertThrows(IllegalStateException.class, () -> run(
                relation(),
                insert("1", "a")));
    }

    @Test
    void rejectsUnknownRelationOid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Begin(1L, TS, 1L),
                insert("1", "a")));   // 未发 Relation：registry.require miss
    }

    @Test
    void truncateAssemblesRelationSnapshotsPerOid() {
        List<Transaction> out = run(
                new PgOutputMessage.Begin(0x1L, TS, 1L),
                relation(16384),
                relation(16385),
                new PgOutputMessage.Truncate(OptionalLong.empty(),
                        EnumSet.of(TruncateOption.CASCADE), new int[]{16384, 16385}),
                new PgOutputMessage.Commit(0x1L, 0x2L, TS));
        assertEquals(1, out.size());
        assertEquals(TransactionKind.NORMAL, out.get(0).kind());
        assertEquals(1, out.get(0).changes().size());
        TruncateChange tc = (TruncateChange) out.get(0).changes().get(0);
        assertEquals(List.of("t", "t16385"),   // 每个 oid 各自的快照，顺序与消息一致
                tc.relations().stream().map(PgOutputMessage.Relation::table).toList());
        assertTrue(tc.options().contains(TruncateOption.CASCADE));
        assertTrue(tc.streamXid().isEmpty());
    }

    @Test
    void truncateFailsOnUnknownOid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Begin(1L, TS, 1L),
                relation(16384),
                new PgOutputMessage.Truncate(OptionalLong.empty(),
                        EnumSet.noneOf(TruncateOption.class), new int[]{16384, 404})));
    }

    @Test
    void transactionalMsgGoesIntoBucket() {
        List<Transaction> out = run(
                new PgOutputMessage.Begin(1L, TS, 1L),
                new PgOutputMessage.LogicalMsg(OptionalLong.empty(), true, 0x1L, "p", new byte[]{1}),
                new PgOutputMessage.Commit(1L, 2L, TS));
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).changes().size());
        MsgChange mc = (MsgChange) out.get(0).changes().get(0);
        assertTrue(mc.transactional());
        assertEquals("p", mc.prefix());
    }

    @Test
    void nonTransactionalMsgWithoutBucketIsDropped() {
        List<Transaction> out = run(
                new PgOutputMessage.LogicalMsg(OptionalLong.empty(), false, 0x1L, "p", new byte[]{1}));
        assertTrue(out.isEmpty());   // 丢弃路径：不抛异常、不产生 Transaction
    }
}
