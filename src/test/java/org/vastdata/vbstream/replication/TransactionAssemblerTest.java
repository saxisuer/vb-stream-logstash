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
    /** 顶层流式事务 A 的 xid——双事务交错用例。 */
    private static final long TOP_A = 7001L;
    /** 顶层流式事务 B 的 xid——双事务交错用例。 */
    private static final long TOP_B = 7002L;
    /** 子事务 xid（TOP_A 的 sub）：验证流块内（子）事务归属与 StreamAbort 剔除。 */
    private static final long SUB = 7003L;
    /** 两阶段事务全局 id 夹具值。 */
    private static final String GID = "gid-1";

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

    /** 提取事务内全部行变更首列（id）的文本值序列，用于桶间不混/桶内保序的逐值断言。仅适用于全 INSERT 的 RowChange 事务（对 DELETE/Truncate/Msg 变更会抛 ClassCastException/NoSuchElementException）。 */
    private static List<String> idsOf(Transaction t) {
        return t.changes().stream()
                .map(ch -> ((TupleValue.Text) ((RowChange) ch).after().orElseThrow()
                        .columns().get(0)).value())
                .toList();
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

    @Test
    void assemblesSingleStreamedTransaction() {
        List<Transaction> out = run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                relation(),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamCommit(TOP_A, 0x500L, 0x580L, TS));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TOP_A, t.xid());
        assertEquals(TransactionKind.STREAMED, t.kind());
        assertNull(t.gid());
        assertEquals(2, t.changes().size());
        // streamXid 逐变更保留（子事务归属可追溯）
        assertEquals(OptionalLong.of(TOP_A), t.changes().get(0).streamXid());
        assertEquals(OptionalLong.of(SUB), t.changes().get(1).streamXid());
    }

    @Test
    void interleavedStreamingTransactionsEmitIndependently() {
        // spec §4.2 场景：两个并发大事务流段交错，多桶各自独立（B.1 全局内存阈值 + B.2 轮番驱逐）
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_B, true),
                streamedInsert(TOP_B, "9", "i"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_A, false),
                streamedInsert(TOP_A, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_B, false),
                streamedInsert(TOP_B, "8", "h"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamCommit(TOP_A, 0x1L, 0x2L, TS),
                new PgOutputMessage.StreamCommit(TOP_B, 0x3L, 0x4L, TS));
        assertEquals(2, out.size());
        assertEquals(TOP_A, out.get(0).xid());
        assertEquals(TransactionKind.STREAMED, out.get(0).kind());
        assertEquals(TOP_B, out.get(1).xid());
        // A 桶两段共 2 条、B 桶两段共 2 条——段间交错不丢不混
        assertEquals(2, out.get(0).changes().size());
        assertEquals(2, out.get(1).changes().size());
        // 桶间不混不丢、桶内保序：逐值断言四个 id（错换任意两段会打破预期序列）
        assertEquals(List.of("1", "2"), idsOf(out.get(0)));
        assertEquals(List.of("9", "8"), idsOf(out.get(1)));
        assertEquals(TransactionKind.STREAMED, out.get(1).kind());
    }

    @Test
    void smallNormalTransactionBetweenStreamSegmentsRoutesCorrectly() {
        // 流段间隙插入的普通小事务先行输出，流事务随后（currentStream 在 stream_stop 后让位）
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.Begin(99L, TS, 99L),
                insert("5", "x"),
                new PgOutputMessage.Commit(1L, 2L, TS),
                new PgOutputMessage.StreamStart(TOP_A, false),
                streamedInsert(TOP_A, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamCommit(TOP_A, 3L, 4L, TS));
        assertEquals(List.of(99L, TOP_A), out.stream().map(Transaction::xid).toList());
        assertEquals(TransactionKind.NORMAL, out.get(0).kind());
        assertEquals(TransactionKind.STREAMED, out.get(1).kind());
        assertEquals(2, out.get(1).changes().size());
    }

    @Test
    void streamAbortRemovesSubtransactionChanges() {
        // 子事务回滚：仅剔除 streamXid==sub 的已流式变更，其余保留
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                streamedInsert(SUB, "3", "c"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamAbort(TOP_A, SUB, OptionalLong.empty(), OptionalLong.empty()),
                new PgOutputMessage.StreamCommit(TOP_A, 1L, 2L, TS));
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).changes().size());
        assertEquals(OptionalLong.of(TOP_A), out.get(0).changes().get(0).streamXid());
    }

    @Test
    void streamAbortOfWholeTopTransactionDropsBucket() {
        // 整顶层回滚（decode 层先逐子后顶，最后一条 top==sub，spec B.4）：桶整体移除，StreamCommit 无从回调。
        // 同一实例驱动（不走 run 夹具）：验证的是"桶被移除"而非"桶从未存在"。
        RelationRegistry registry = new RelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add);
        PgOutputMessage[] seq = {
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamAbort(TOP_A, SUB, OptionalLong.empty(), OptionalLong.empty()),
                new PgOutputMessage.StreamAbort(TOP_A, TOP_A, OptionalLong.empty(), OptionalLong.empty())};
        for (PgOutputMessage m : seq) {
            registry.accept(m);
            assembler.accept(m, registry);
        }
        assertEquals(0, out.size());
        // 同一实例：桶已被移除 → 后续同 xid StreamCommit fail-fast（非静默）
        assertThrows(IllegalStateException.class, () ->
                assembler.accept(new PgOutputMessage.StreamCommit(TOP_A, 1L, 2L, TS), registry));
    }

    @Test
    void rejectsStreamContinueForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, false)));   // 首段标记 false 但无桶
    }

    @Test
    void rejectsDuplicateFirstSegment() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_A, true)));   // 同顶层事务再次 first=true
    }

    @Test
    void rejectsStreamStopWithoutStreamBlock() {
        assertThrows(IllegalStateException.class, () ->
                run(new PgOutputMessage.StreamStop()));
    }

    @Test
    void rejectsStreamCommitForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamCommit(404L, 1L, 2L, TS)));
    }

    @Test
    void rejectsStreamCommitWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                new PgOutputMessage.StreamCommit(TOP_A, 1L, 2L, TS)));   // 流块未闭合
    }

    @Test
    void rejectsStreamAbortWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                new PgOutputMessage.StreamAbort(TOP_A, TOP_A, OptionalLong.empty(), OptionalLong.empty())));
    }

    @Test
    void rejectsStreamAbortForUnknownTopXid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamAbort(404L, 405L, OptionalLong.empty(), OptionalLong.empty())));
    }

    @Test
    void twoPhaseCommitEmitsOnCommitPrepared() {
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.BeginPrepare(0x10L, 0x18L, TS, 601L, GID),
                insert("1", "a"),
                new PgOutputMessage.Prepare(0x10L, 0x18L, TS, 601L, GID),
                new PgOutputMessage.CommitPrepared(0x20L, 0x28L, TS, 601L, GID));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.TWO_PHASE, t.kind());
        assertEquals(GID, t.gid());
        assertEquals(601L, t.xid());
        assertEquals(0x20L, t.commitLsn());
        assertEquals(1, t.changes().size());
    }

    @Test
    void rollbackPreparedDiscardsSilently() {
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.BeginPrepare(0x10L, 0x18L, TS, 601L, GID),
                insert("1", "a"),
                new PgOutputMessage.Prepare(0x10L, 0x18L, TS, 601L, GID),
                new PgOutputMessage.RollbackPrepared(0x10L, 0x30L, TS, TS, 601L, GID));
        assertEquals(0, out.size());
    }

    @Test
    void streamedTwoPhaseEmitsOnCommitPrepared() {
        // 流式 2PC：StreamPrepare 前必有最后一个流段并已闭合（spec B.6），桶从 streamedByXid 转挂起池
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamPrepare(0x10L, 0x18L, TS, TOP_A, GID),
                new PgOutputMessage.CommitPrepared(0x20L, 0x28L, TS, TOP_A, GID));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.TWO_PHASE, t.kind());
        assertEquals(GID, t.gid());
        assertEquals(TOP_A, t.xid());
        assertEquals(1, t.changes().size());
    }

    @Test
    void rejectsCommitPreparedForUnknownGid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.CommitPrepared(1L, 2L, TS, 1L, "no-such-gid")));
    }

    @Test
    void rejectsDuplicatePrepareGid() {
        assertThrows(IllegalStateException.class, () -> run(
                relation(),
                new PgOutputMessage.BeginPrepare(1L, 2L, TS, 601L, GID),
                new PgOutputMessage.Prepare(1L, 2L, TS, 601L, GID),
                // 同 gid 第二次 Prepare：挂起池已存在 → fail-fast
                new PgOutputMessage.BeginPrepare(3L, 4L, TS, 602L, GID),
                new PgOutputMessage.Prepare(3L, 4L, TS, 602L, GID)));
    }

    @Test
    void rejectsPrepareWithoutBeginPrepare() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Prepare(1L, 2L, TS, 601L, GID)));
    }

    @Test
    void rejectsDuplicateBeginPrepare() {
        // 两阶段桶未闭合（无 Prepare）又来 BeginPrepare：b..P 串行不嵌套守卫
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.BeginPrepare(1L, 2L, TS, 601L, GID),
                new PgOutputMessage.BeginPrepare(3L, 4L, TS, 602L, "gid-2")));
    }

    @Test
    void rejectsPrepareMismatchedXidOrGid() {
        // Prepare 与活动两阶段桶的 xid/gid 不匹配（rejectsPrepareWithoutBeginPrepare 仅覆盖 null 短路，此处测另一半）
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.BeginPrepare(1L, 2L, TS, 601L, GID),
                new PgOutputMessage.Prepare(1L, 2L, TS, 602L, GID)));   // xid 不匹配
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.BeginPrepare(1L, 2L, TS, 601L, GID),
                new PgOutputMessage.Prepare(1L, 2L, TS, 601L, "gid-2")));   // gid 不匹配
    }

    @Test
    void rejectsRollbackPreparedForUnknownGid() {
        // 挂起池 miss：回滚路径同样 fail-fast，不静默吞掉未知 gid
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.RollbackPrepared(1L, 2L, TS, TS, 601L, "no-such-gid")));
    }

    @Test
    void rejectsStreamPrepareWithOpenStreamBlock() {
        // stream_prepare 前服务端必已发完流段并 stream_stop（spec B.6）：流块未闭合守卫
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                new PgOutputMessage.StreamPrepare(0x10L, 0x18L, TS, TOP_A, GID)));
    }

    @Test
    void rejectsStreamPrepareForUnknownXid() {
        // 无对应流桶的 StreamPrepare：挂起池不能凭空接纳未知 xid
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamPrepare(0x10L, 0x18L, TS, 404L, GID)));
    }
}
