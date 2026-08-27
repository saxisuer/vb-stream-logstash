package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TransactionAssembler 状态机单测：直接以 protocol record 构造消息序列（组装器输入是已解析消息，
 * 无需字节级构造）。每用例断言输出 Transaction 的形态/顺序/内容与 fail-fast 行为。
 */
class TransactionAssemblerTest {

    private static final Instant TS = Instant.parse("2026-08-27T00:00:00Z");
    private static final int OID = 16384;

    /** 构造两列 (id int, v text) 的 Relation 消息，列序与 {@link #row} 对齐。 */
    private static PgOutputMessage.Relation relation() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t", 'd',
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
                        java.util.Optional.empty(), row("1", "b")),
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
}
