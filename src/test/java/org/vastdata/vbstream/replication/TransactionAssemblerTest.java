package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.protocol.TruncateOption;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.protocol.UnknownMessageTypeException;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TransactionAssembler 状态机单测（raw 字节驱动版）：全部输入经 {@link PgWire} 构造线格式
 * 字节直接喂 {@code onRaw}，覆盖轻窥路由、控制消息 live 解码与 MEMORY 回放的全路径。
 * 每用例断言输出 Transaction 的形态/顺序/内容与 fail-fast 行为，语义与消息驱动版
 * （里程碑 1.5 的 33 例）逐用例等价——占位字段（LSN=1/2 递增、时间戳=PG 纪元）按 PgWire 约定断言。
 *
 * <p>移植说明（与旧版的两处有意差异，均由 raw 模型的设计决定）：
 * <ul>
 *   <li>Relation 缺失的 fail-fast（旧 {@code rejectsUnknownRelationOid}/{@code truncateFailsOnUnknownOid}）：
 *       旧版在消息到达时经 {@code registry.require} 即抛；raw 模型数据消息不解码直接入桶，
 *       校验移至提交期回放渲染（assembly-spill 设计 §4.3）——移植用例补一条 Commit 触发回放，
 *       异常类型与根因（"Relation 未先行到达"）不变</li>
 *   <li>流块内的 Relation：旧版以 record 构造可给出 streamXid=empty 的块内 Relation（字节层不存在
 *       此形态），移植用 {@link PgWire#streamed} 加 xid 前缀保持消息序位置不变</li>
 * </ul>
 *
 * <p>夹具约定：组装器以 {@link StreamingMode#ON} 构造（非 parallel——{@link PgWire#streamAbort}
 * 只产出无附加字段的形态）；spill 阈值 0 = 纯 MEMORY 逃生门（本阶段组装器亦未接线 spill）。
 */
class TransactionAssemblerTest {

    /** PgWire 微秒占位 0 的解码结果（PG 纪元），提交时间戳断言统一引用。 */
    private static final Instant TS = PgWire.PG_EPOCH;
    private static final int OID = 16384;
    /** 顶层流式事务 A 的 xid——双事务交错用例。 */
    private static final long TOP_A = 7001L;
    /** 顶层流式事务 B 的 xid——双事务交错用例。 */
    private static final long TOP_B = 7002L;
    /** 子事务 xid（TOP_A 的 sub）：验证流块内（子）事务归属与 StreamAbort 剔除。 */
    private static final long SUB = 7003L;
    /** 两阶段事务全局 id 夹具值。 */
    private static final String GID = "gid-1";
    /** 纯 MEMORY 配置（阈值 0 = 禁用 spill 的逃生门值）。 */
    private static final SpillConfig NO_SPILL = new SpillConfig(0, Path.of("unused"), LegacyRollCycles.MINUTELY);

    /** 构造默认 oid 的两列 (id, v) Relation 字节，供单表场景使用。 */
    private static byte[] relation() {
        return relation(OID);
    }

    /**
     * 按指定 oid 构造两列 (id int, v text) 的 Relation 字节，列序与 {@link #row} 对齐，
     * 供 Truncate 多表等需要多个不同 oid 的场景使用。表名默认 "t"，非默认 oid 用 "t"+oid 区分。
     */
    private static byte[] relation(int oid) {
        return PgWire.relation(oid, oid == OID ? "t" : "t" + oid, "id", "v");
    }

    /** 构造一行文本元组 (id, v) 的**断言侧** record（与解码产物做值相等比较）。 */
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

    /** 流式块外的 Insert 字节。 */
    private static byte[] insert(String id, String v) {
        return PgWire.insert(OID, PgWire.tuple(id, v));
    }

    /** 流式块内的 Insert 字节（streamXid=产生该变更的（子）事务 xid 前缀）。 */
    private static byte[] streamedInsert(long streamXid, String id, String v) {
        return PgWire.streamed(streamXid, insert(id, v));
    }

    /**
     * 依序把 raw 字节喂给新组装器（'R' 的 registry 路由在组装器内部发生），收集输出的 Transaction。
     * 组装器以 StreamingMode.ON 构造——与 {@link PgWire#streamAbort} 的非 parallel 形态配对。
     */
    private static List<Transaction> run(byte[]... msgs) {
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(
                out::add, StreamingMode.ON, new VersionedRelationRegistry(), NO_SPILL);
        for (byte[] m : msgs) {
            assembler.onRaw(m);
        }
        return out;
    }

    /** 冒烟 1（新增，正路径最小切片）：Begin→Relation→Insert→Commit 产出恰含 1 条 RowChange 的 NORMAL Transaction（轻窥路由 + live 解码 + 回放渲染全链路）。 */
    @Test
    void assemblesBeginInsertCommitIntoNormalTransaction() {
        List<Transaction> out = run(
                PgWire.begin(505L),
                relation(),
                insert("1", "a"),
                PgWire.commit());
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(505L, t.xid());
        assertEquals(TransactionKind.NORMAL, t.kind());
        assertNull(t.gid());
        assertEquals(1L, t.commitLsn());          // PgWire LSN 占位
        assertEquals(2L, t.endLsn());
        assertEquals(TS, t.commitTimestamp());     // 微秒占位 0 → PG 纪元
        assertEquals(1, t.changes().size());
        RowChange c0 = (RowChange) t.changes().get(0);
        assertEquals(DmlKind.INSERT, c0.dml());
        assertEquals("t", c0.relation().table());   // Relation 快照嵌入
        assertEquals(row("1", "a"), c0.after().orElseThrow());
        assertTrue(c0.streamXid().isEmpty());
    }

    /** 旧例 1：普通事务内 I/U/D 按序组装，Relation 快照嵌入，kind/xid/LSN/时间戳来自 Commit 解码。 */
    @Test
    void assemblesNormalTransactionInOrder() {
        List<Transaction> out = run(
                PgWire.begin(505L),
                relation(),
                insert("1", "a"),
                PgWire.update(OID, null, null, PgWire.tuple("1", "b")),   // 无旧镜像（REPLICA IDENTITY DEFAULT 常态）
                PgWire.delete(OID, 'O', PgWire.tuple("1", "b")),
                PgWire.commit());
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(505L, t.xid());
        assertEquals(TransactionKind.NORMAL, t.kind());
        assertNull(t.gid());
        assertEquals(1L, t.commitLsn());
        assertEquals(2L, t.endLsn());
        assertEquals(TS, t.commitTimestamp());
        assertEquals(3, t.changes().size());
        RowChange c0 = (RowChange) t.changes().get(0);
        assertEquals(DmlKind.INSERT, c0.dml());
        assertEquals("t", c0.relation().table());          // Relation 快照嵌入
        assertEquals(row("1", "a"), c0.after().orElseThrow());
        assertEquals(DmlKind.UPDATE, ((RowChange) t.changes().get(1)).dml());
        assertEquals(DmlKind.DELETE, ((RowChange) t.changes().get(2)).dml());
    }

    /** 旧例 2：连续普通事务逐个输出；Relation 会话内一次到达、跨事务持续有效（registry 版本日志）。 */
    @Test
    void consecutiveTransactionsEmitOneByOne() {
        List<Transaction> out = run(
                relation(),
                PgWire.begin(1L),
                insert("1", "a"),
                PgWire.commit(),
                PgWire.begin(2L),
                insert("2", "b"),
                PgWire.commit());
        assertEquals(List.of(1L, 2L), out.stream().map(Transaction::xid).toList());
    }

    /** 旧例 3 = 冒烟 2：Commit 无活动普通事务桶 fail-fast。 */
    @Test
    void rejectsCommitWithoutBegin() {
        assertThrows(IllegalStateException.class, () -> run(PgWire.commit()));
    }

    /** 旧例 4：Begin 到达但普通事务未闭合（Begin..Commit 不嵌套守卫）。 */
    @Test
    void rejectsDuplicateBegin() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.begin(1L),
                PgWire.begin(2L)));
    }

    /** 旧例 5：变更消息到达但无任何活动桶（路由期 fail-fast，异常描述带类型与 relationOid）。 */
    @Test
    void rejectsChangeWithoutActiveBucket() {
        assertThrows(IllegalStateException.class, () -> run(
                relation(),
                insert("1", "a")));
    }

    /**
     * 旧例 6（移植适配）：变更引用未先行到达的 Relation——raw 模型数据消息不解码直接入桶，
     * require(oid, seq) 校验发生在提交期回放渲染，故补 Commit 触发；异常类型与根因不变
     * （VersionedRelationRegistry.require 的"未先行到达"fail-fast）。
     */
    @Test
    void rejectsUnknownRelationOid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.begin(1L),
                insert("1", "a"),   // 未发 Relation：回放期 registry.require(oid, seq) miss
                PgWire.commit()));
    }

    /** 旧例 7：Truncate 多表——每个 oid 各自的 Relation 快照（顺序与消息一致）、选项位、块外 streamXid=empty。 */
    @Test
    void truncateAssemblesRelationSnapshotsPerOid() {
        List<Transaction> out = run(
                PgWire.begin(1L),
                relation(16384),
                relation(16385),
                PgWire.truncate(new int[]{16384, 16385}, (byte) 0x01),   // bit0 = CASCADE
                PgWire.commit());
        assertEquals(1, out.size());
        assertEquals(TransactionKind.NORMAL, out.get(0).kind());
        assertEquals(1, out.get(0).changes().size());
        TruncateChange tc = (TruncateChange) out.get(0).changes().get(0);
        assertEquals(List.of("t", "t16385"),   // 每个 oid 各自的快照，顺序与消息一致
                tc.relations().stream().map(r -> r.table()).toList());
        assertTrue(tc.options().contains(TruncateOption.CASCADE));
        assertTrue(tc.streamXid().isEmpty());
    }

    /** 旧例 8（移植适配，同旧例 6）：Truncate 引用未到达的 oid——回放期 require miss fail-fast。 */
    @Test
    void truncateFailsOnUnknownOid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.begin(1L),
                relation(16384),
                PgWire.truncate(new int[]{16384, 404}, (byte) 0x00),
                PgWire.commit()));
    }

    /** 旧例 9：事务性 LogicalMsg 入桶，随事务输出 MsgChange（transactional/prefix 保留）。 */
    @Test
    void transactionalMsgGoesIntoBucket() {
        List<Transaction> out = run(
                PgWire.begin(1L),
                PgWire.logicalMsg(true, "p", new byte[]{1}),
                PgWire.commit());
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).changes().size());
        MsgChange mc = (MsgChange) out.get(0).changes().get(0);
        assertTrue(mc.transactional());
        assertEquals("p", mc.prefix());
    }

    /** 旧例 10：非事务性 LogicalMsg 无任何活动桶 → WARN 丢弃（不抛异常、不产出 Transaction）。 */
    @Test
    void nonTransactionalMsgWithoutBucketIsDropped() {
        List<Transaction> out = run(
                PgWire.logicalMsg(false, "p", new byte[]{1}));
        assertTrue(out.isEmpty());   // 丢弃路径：不抛异常、不产生 Transaction
    }

    /**
     * 旧例 11（流内 Relation 适配，见类 javadoc）：单流式事务两段内变更归属与 streamXid 逐单元保留。
     * Relation 位于流块内（S 之后），按协议形态经 {@link PgWire#streamed} 加顶层 xid 前缀。
     */
    @Test
    void assemblesSingleStreamedTransaction() {
        List<Transaction> out = run(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamed(TOP_A, relation()),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                PgWire.streamStop(),
                PgWire.streamCommit(TOP_A));
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

    /** 旧例 12：spec §4.2 场景——两个并发大事务流段交错，多桶各自独立、桶间不混不丢、桶内保序。 */
    @Test
    void interleavedStreamingTransactionsEmitIndependently() {
        List<Transaction> out = run(
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                PgWire.streamStop(),
                PgWire.streamStart(TOP_B, true),
                streamedInsert(TOP_B, "9", "i"),
                PgWire.streamStop(),
                PgWire.streamStart(TOP_A, false),
                streamedInsert(TOP_A, "2", "b"),
                PgWire.streamStop(),
                PgWire.streamStart(TOP_B, false),
                streamedInsert(TOP_B, "8", "h"),
                PgWire.streamStop(),
                PgWire.streamCommit(TOP_A),
                PgWire.streamCommit(TOP_B));
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

    /** 旧例 13：流段间隙插入的普通小事务先行输出，流事务随后（currentStream 在 stream_stop 后让位）。 */
    @Test
    void smallNormalTransactionBetweenStreamSegmentsRoutesCorrectly() {
        List<Transaction> out = run(
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                PgWire.streamStop(),
                PgWire.begin(99L),
                insert("5", "x"),
                PgWire.commit(),
                PgWire.streamStart(TOP_A, false),
                streamedInsert(TOP_A, "2", "b"),
                PgWire.streamStop(),
                PgWire.streamCommit(TOP_A));
        assertEquals(List.of(99L, TOP_A), out.stream().map(Transaction::xid).toList());
        assertEquals(TransactionKind.NORMAL, out.get(0).kind());
        assertEquals(TransactionKind.STREAMED, out.get(1).kind());
        assertEquals(2, out.get(1).changes().size());
    }

    /** 旧例 14：子事务回滚——abortedSubxids 回放过滤仅剔除 streamXid==sub 的单元，其余保留。 */
    @Test
    void streamAbortRemovesSubtransactionChanges() {
        List<Transaction> out = run(
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                streamedInsert(SUB, "3", "c"),
                PgWire.streamStop(),
                PgWire.streamAbort(TOP_A, SUB),
                PgWire.streamCommit(TOP_A));
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).changes().size());
        assertEquals(OptionalLong.of(TOP_A), out.get(0).changes().get(0).streamXid());
    }

    /**
     * 旧例 15：整顶层回滚（decode 层先逐子后顶，最后一条 top==sub，spec B.4）——桶整体移除，
     * StreamCommit 无从回调。同一实例驱动（不走 run 夹具）：验证的是"桶被移除"而非"桶从未存在"。
     */
    @Test
    void streamAbortOfWholeTopTransactionDropsBucket() {
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(
                out::add, StreamingMode.ON, new VersionedRelationRegistry(), NO_SPILL);
        byte[][] seq = {
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                PgWire.streamStop(),
                PgWire.streamAbort(TOP_A, SUB),
                PgWire.streamAbort(TOP_A, TOP_A)};
        for (byte[] m : seq) {
            assembler.onRaw(m);
        }
        assertEquals(0, out.size());
        // 同一实例：桶已被移除 → 后续同 xid StreamCommit fail-fast（非静默）
        assertThrows(IllegalStateException.class, () -> assembler.onRaw(PgWire.streamCommit(TOP_A)));
    }

    /** 旧例 16：StreamStart(first=false) 但顶层事务无桶 fail-fast。 */
    @Test
    void rejectsStreamContinueForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamStart(TOP_A, false)));   // 首段标记 false 但无桶
    }

    /** 旧例 17：同顶层事务再次 first=true（桶已存在）fail-fast。 */
    @Test
    void rejectsDuplicateFirstSegment() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamStop(),
                PgWire.streamStart(TOP_A, true)));
    }

    /** 旧例 18：StreamStop 到达但无进行中的流块 fail-fast。 */
    @Test
    void rejectsStreamStopWithoutStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(PgWire.streamStop()));
    }

    /** 旧例 19：StreamCommit 对应流式事务桶不存在 fail-fast。 */
    @Test
    void rejectsStreamCommitForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> run(PgWire.streamCommit(404L)));
    }

    /** 旧例 20：StreamCommit 到达但流块未闭合 fail-fast。 */
    @Test
    void rejectsStreamCommitWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamCommit(TOP_A)));   // 流块未闭合
    }

    /** 旧例 21：StreamAbort 到达但流块未闭合 fail-fast。 */
    @Test
    void rejectsStreamAbortWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamAbort(TOP_A, TOP_A)));
    }

    /** 旧例 22：StreamAbort 对应顶层事务桶不存在 fail-fast。 */
    @Test
    void rejectsStreamAbortForUnknownTopXid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamAbort(404L, 405L)));
    }

    /** 旧例 23：2PC——b..P 入挂起池，K(gid) 回放封箱 TWO_PHASE 输出（gid/xid/LSN 断言）。 */
    @Test
    void twoPhaseCommitEmitsOnCommitPrepared() {
        List<Transaction> out = run(
                relation(),
                PgWire.beginPrepare(601L, GID),
                insert("1", "a"),
                PgWire.prepare(601L, GID),
                PgWire.commitPrepared(601L, GID));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.TWO_PHASE, t.kind());
        assertEquals(GID, t.gid());
        assertEquals(601L, t.xid());
        assertEquals(1L, t.commitLsn());
        assertEquals(1, t.changes().size());
    }

    /** 旧例 24：RollbackPrepared 静默丢弃挂起桶（不回调）。 */
    @Test
    void rollbackPreparedDiscardsSilently() {
        List<Transaction> out = run(
                relation(),
                PgWire.beginPrepare(601L, GID),
                insert("1", "a"),
                PgWire.prepare(601L, GID),
                PgWire.rollbackPrepared(601L, GID));
        assertEquals(0, out.size());
    }

    /** 旧例 25：流式 2PC——StreamPrepare 前必有最后一个流段并已闭合（spec B.6），桶从 streamedByXid 转挂起池，K 输出。 */
    @Test
    void streamedTwoPhaseEmitsOnCommitPrepared() {
        List<Transaction> out = run(
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                PgWire.streamStop(),
                PgWire.streamPrepare(TOP_A, GID),
                PgWire.commitPrepared(TOP_A, GID));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.TWO_PHASE, t.kind());
        assertEquals(GID, t.gid());
        assertEquals(TOP_A, t.xid());
        assertEquals(1, t.changes().size());
    }

    /** 旧例 26：CommitPrepared 对应 gid 不存在 fail-fast。 */
    @Test
    void rejectsCommitPreparedForUnknownGid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.commitPrepared(1L, "no-such-gid")));
    }

    /** 旧例 27：同 gid 第二次 Prepare（挂起池已存在）fail-fast。 */
    @Test
    void rejectsDuplicatePrepareGid() {
        assertThrows(IllegalStateException.class, () -> run(
                relation(),
                PgWire.beginPrepare(601L, GID),
                PgWire.prepare(601L, GID),
                // 同 gid 第二次 Prepare：挂起池已存在 → fail-fast
                PgWire.beginPrepare(602L, GID),
                PgWire.prepare(602L, GID)));
    }

    /** 旧例 28：无活动两阶段桶的 Prepare fail-fast。 */
    @Test
    void rejectsPrepareWithoutBeginPrepare() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.prepare(601L, GID)));
    }

    /** 旧例 29：两阶段桶未闭合（无 Prepare）又来 BeginPrepare fail-fast（b..P 串行不嵌套守卫）。 */
    @Test
    void rejectsDuplicateBeginPrepare() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.beginPrepare(601L, GID),
                PgWire.beginPrepare(602L, "gid-2")));
    }

    /** 旧例 30：Prepare 与活动两阶段桶的 xid/gid 不匹配 fail-fast（两条子句各一用例）。 */
    @Test
    void rejectsPrepareMismatchedXidOrGid() {
        // xid 不匹配
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.beginPrepare(601L, GID),
                PgWire.prepare(602L, GID)));
        // gid 不匹配
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.beginPrepare(601L, GID),
                PgWire.prepare(601L, "gid-2")));
    }

    /** 旧例 31：RollbackPrepared 对应 gid 不存在 fail-fast（回滚路径不静默吞未知 gid）。 */
    @Test
    void rejectsRollbackPreparedForUnknownGid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.rollbackPrepared(601L, "no-such-gid")));
    }

    /** 旧例 32：StreamPrepare 到达但流块未闭合 fail-fast（stream_prepare 前必已 stream_stop，spec B.6）。 */
    @Test
    void rejectsStreamPrepareWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamPrepare(TOP_A, GID)));
    }

    /** 旧例 33：无对应流桶的 StreamPrepare fail-fast（挂起池不能凭空接纳未知 xid）。 */
    @Test
    void rejectsStreamPrepareForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamPrepare(404L, GID)));
    }

    /** 新增（brief Step 4 "Type/Origin 透传"）：'Y'/'O' 不入桶、不影响组装输出——raw 模型下由组装器自行丢弃（旧版由 instanceof 链忽略）。 */
    @Test
    void typeAndOriginMessagesAreIgnored() {
        List<Transaction> out = run(
                PgWire.type(19, "bytea"),
                PgWire.origin("origin-1"),
                relation(),
                PgWire.begin(1L),
                insert("1", "a"),
                PgWire.type(25, "text"),
                PgWire.commit());
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).changes().size());   // Y/O 均未混入桶
    }

    /** 新增：未知类型字节经 live 解码 fail-fast（路由表 default 分支 → UnknownMessageTypeException）。 */
    @Test
    void rejectsUnknownMessageTypeByte() {
        assertThrows(UnknownMessageTypeException.class, () -> run(new byte[]{'X'}));
    }
}
