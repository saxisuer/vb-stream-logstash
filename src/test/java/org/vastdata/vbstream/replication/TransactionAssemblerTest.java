package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.protocol.TruncateOption;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.protocol.UnknownMessageTypeException;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TransactionAssembler 状态机单测（raw 字节驱动版）：全部输入经 {@link PgWire} 构造线格式
 * 字节直接喂 {@code onRaw}，覆盖轻窥路由、控制消息 live 解码与流式事件回放的全路径
 * （2.0 起输出经 {@link TransactionCollector} 重组回整块，既有断言零改动）。
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
 * <p>夹具约定（1.7 纯段记账）：组装器以 {@link StreamingMode#ON} 构造（非 parallel——
 * {@link PgWire#streamAbort} 只产出无附加字段的形态）；管道目录取类级共享的静态 {@code @TempDir}
 * （每次构造组装器 wipe-on-open 顺序清空，用例间不残留）。数据全部经 CQ 往返
 * （append→readRange），等价基线 = 既有期望值不变——这是 1.6 → 1.7a 的无损性验收本体
 * （1.6 的"双阈值对照"已无对照面，等价性移至 Task 6 的同步/异步对照）。
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

    /** 类级共享管道目录：静态 @TempDir 全类一份，用例间由 MessagePipe 的 wipe-on-open 顺序清空。 */
    @TempDir
    static Path PIPE_DIR;

    /** 组装器统一管道配置（目录取类级共享 @TempDir，滚动周期与生产默认同档）。 */
    private static PipeConfig pipeCfg() {
        return new PipeConfig(PIPE_DIR, LegacyRollCycles.MINUTELY);
    }

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
     * 依序把 raw 字节喂给新组装器（'R' 的 registry 路由在组装器内部发生），收集输出的 Transaction
     * （2.0 起组装器回调流式事件，经 {@link TransactionCollector} 重组回整块——既有
     * {@code List<Transaction>} 断言零改动的等价币）。
     * 组装器以 StreamingMode.ON 构造——与 {@link PgWire#streamAbort} 的非 parallel 形态配对。
     * try-with-resources 收敛管道（每个组装器独占一条 CQ，不关会泄漏 mmap 且阻塞 @TempDir 清理）。
     */
    private static List<Transaction> run(byte[]... msgs) {
        TransactionCollector out = new TransactionCollector();
        try (TransactionAssembler assembler = new TransactionAssembler(
                out, StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg())) {
            for (byte[] m : msgs) {
                assembler.onRaw(m);
            }
        }
        return out.transactions();
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
     * intAt 掩码钉子（Task 12 评审补例，防 7b263c5 修复回退）：流式前缀 xid 的 4 字节中
     * **任一字节 ≥ 0x80** 时（byte 有符号，Java 对负 byte 做 {@code |} 会符号位扩散到全部高位），
     * 组装器侧 {@code unsignedInt(raw,1)} 读出的单元 streamXid 错值 → abortedSubxids 过滤
     * 永不命中 → 子事务回滚剔除静默失效。既有夹具 xid（7001/7003…）恰好每字节 &lt;0x80，
     * 本例专取覆盖四个字节位的高字节形态钉死回归：
     * TOP=0xABCD1234（首字节 0xAB）、SUB_A=0x8F1234（第 3 字节 0x8F）、SUB_B=758=0x2F6
     * （末字节 0xF6，Task 12 集成实测踩中的真实形态）、SUB_C=0x90AB（第 2 字节 0x90）。
     * 关键步骤：TOP 写 2 行（保留），SUB_A/SUB_B 各写 1 行后双双 abort（剔除），第二流块
     * SUB_C 写 1 行后 abort（剔除）再补 TOP 尾行（保留），StreamCommit 收束。
     * 断言依据：恰 1 个 STREAMED 事务、存活 2 行恰为 TOP 的首尾两行（id 序列逐值钉死）、
     * 逐变更 streamXid 精确等于 TOP。回退到未掩码的 intAt 时 SUB 单元因前缀错值
     * （如 0x2F6 读成 0xFFFFFFF6）全部漏剔，变更数断言必红（已实测复核）。
     */
    @Test
    void streamAbortFiltersHighByteXidPrefixesExactly() {
        final long top = 0xABCD1234L;    // 首字节 0xAB
        final long subA = 0x8F1234L;     // 第 3 字节 0x8F
        final long subB = 758L;          // 末字节 0xF6（Task 12 集成实测形态）
        final long subC = 0x90ABL;       // 第 2 字节 0x90
        List<Transaction> out = run(
                relation(),
                PgWire.streamStart(top, true),
                streamedInsert(top, "1", "a"),
                streamedInsert(subA, "201", "b"),
                streamedInsert(subB, "202", "c"),
                PgWire.streamStop(),
                PgWire.streamAbort(top, subA),
                PgWire.streamAbort(top, subB),
                PgWire.streamStart(top, false),
                streamedInsert(subC, "203", "d"),
                PgWire.streamStop(),
                PgWire.streamAbort(top, subC),
                PgWire.streamStart(top, false),
                streamedInsert(top, "999", "tail"),
                PgWire.streamStop(),
                PgWire.streamCommit(top));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.STREAMED, t.kind());
        assertEquals(top, t.xid());
        // 仅 TOP 的三行存活：三个高字节子事务的单元被按正确 streamXid 精确剔除
        assertEquals(List.of("1", "999"), idsOf(t));
        for (TxChange change : t.changes()) {
            assertEquals(OptionalLong.of(top), change.streamXid());
        }
    }

    /**
     * 旧例 15：整顶层回滚（decode 层先逐子后顶，最后一条 top==sub，spec B.4）——桶整体移除，
     * StreamCommit 无从回调。同一实例驱动（不走 run 夹具）：验证的是"桶被移除"而非"桶从未存在"。
     */
    @Test
    void streamAbortOfWholeTopTransactionDropsBucket() {
        TransactionCollector out = new TransactionCollector();
        try (TransactionAssembler assembler = new TransactionAssembler(
                out, StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg())) {
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
            assertEquals(0, out.transactions().size());
            // 同一实例：桶已被移除 → 后续同 xid StreamCommit fail-fast（非静默）
            assertThrows(IllegalStateException.class, () -> assembler.onRaw(PgWire.streamCommit(TOP_A)));
        }
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

    /**
     * 终审 Fix C：流块嵌套违规——S..S 不夹 E（上一流块未闭合又来 StreamStart）fail-fast，
     * 与 'c'/'A'/'p'/'E' 处理器的"流块未闭合"守卫对齐（此前该形态会静默改写 currentStream，
     * 丢失原桶的流块上下文）。
     */
    @Test
    void rejectsStreamStartWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> run(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamStart(TOP_B, true)));   // 未闭合 TOP_A 流块又开新流块
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

    // --- 终审 Fix B：registry 版本日志剪枝接线（桶完结点驱动，同 oid 多版本场景） ---------------

    /**
     * 桶完结驱动 registry 剪枝：无存活桶时低水位取"无穷"——被新版本取代的旧版本在下一个桶完结点
     * 被剪掉（旧 asOf 查询 ISE 证明确实剪了，非空转），后续事务仍按新版本正确渲染。
     * 消息序（1.7 起 seq ≡ CQ index，绝对值随建队列时刻漂移、不可字面断言）：R(t_v1)、B、I、C
     * （完结点①：仅 v1 在册，floor 保留）；R(t_v2)、B、I、C（完结点②：无存活桶 → v1 剪除）；
     * B、I、C（按 v2 渲染）。同一 registry 实例贯穿全程（剪枝副作用可观测的前提）。
     * asOf 锚点取法：完结点①后无存活桶，{@code pipeWatermark()} = 最近 append index + 1——减一
     * 即 v1 时代的最后一条消息 index，先证其可解析（v1 在册），再在完结点②后断言同 asOf 抛 ISE
     * （剪枝确已发生，而非 asAt 值本身无效的空转）。
     */
    @Test
    void retiredBucketPrunesSupersededRegistryVersions() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        TransactionCollector collector = new TransactionCollector();
        try (TransactionAssembler assembler = new TransactionAssembler(
                collector, StreamingMode.ON, registry, pipeCfg())) {
            assembler.onRaw(PgWire.relation(OID, "t_v1", "id", "v"));
            assembler.onRaw(PgWire.begin(1L));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.commit());
            long v1EraSeq = assembler.pipeWatermark() - 1L;   // == 完结点①前最近 append（首条 Commit）的 index
            assertEquals("t_v1", registry.require(OID, v1EraSeq).table());   // 剪枝前该 asOf 可解析
            assembler.onRaw(PgWire.relation(OID, "t_v2", "id", "v"));
            assembler.onRaw(PgWire.begin(2L));
            assembler.onRaw(insert("2", "b"));
            assembler.onRaw(PgWire.commit());
            assertThrows(IllegalStateException.class, () -> registry.require(OID, v1EraSeq));   // v1 已剪
            assertEquals("t_v2", registry.require(OID).table());                        // 最新视图仍可答
            assembler.onRaw(PgWire.begin(3L));
            assembler.onRaw(insert("3", "c"));
            assembler.onRaw(PgWire.commit());                                           // 剪枝后新查询仍正确
        }
        List<Transaction> out = collector.transactions();
        assertEquals(3, out.size());
        assertEquals("t_v1", ((RowChange) out.get(0).changes().get(0)).relation().table());
        assertEquals("t_v2", ((RowChange) out.get(1).changes().get(0)).relation().table());
        assertEquals("t_v2", ((RowChange) out.get(2).changes().get(0)).relation().table());
    }

    /**
     * 2PC 挂起桶算存活（剪枝低水位候选）：挂起桶的旧单元依赖 v1——其 seq（R(t_v1) 的 index）
     * 早于桶 firstIndex（'R' 恒先于同表 DML 到达），其间他桶（普通事务 99）完结触发的剪枝必须
     * 保住 v1（floor 语义）；挂起桶最终 CommitPrepared 仍按 v1 正确渲染，其完结后 v1 才被剪。
     * 这是"以存活桶 firstIndex 为低水位"接线正确性的钉子用例（若按"丢弃 seq &lt; 低水位"的
     * 字面实现，v1 会在事务 99 的完结点被误剪，CommitPrepared 回放 ISE 崩溃）。
     * 消息序（1.7 起 seq ≡ CQ index）：R(t_v1)、b、I（挂起桶 firstIndex）、P、R(t_v2)、B、I、C
     * （剪枝点：低水位 = 挂起桶 firstIndex）、K（挂起桶按 v1 回放，完结后 v1 剪除）。
     * 挂起桶单元 seq 取法：剪枝点后挂起桶是唯一带单元的存活桶，{@code pipeWatermark()} 恰等于
     * 其 firstIndex（min 语义），即旧单元自身的 asOf。
     */
    @Test
    void pendingTwoPhaseBucketKeepsItsAsOfVersionAliveAcrossPruning() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        TransactionCollector collector = new TransactionCollector();
        try (TransactionAssembler assembler = new TransactionAssembler(
                collector, StreamingMode.ON, registry, pipeCfg())) {
            assembler.onRaw(PgWire.relation(OID, "t_v1", "id", "v"));
            assembler.onRaw(PgWire.beginPrepare(601L, GID));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.prepare(601L, GID));
            assembler.onRaw(PgWire.relation(OID, "t_v2", "id", "v"));
            assembler.onRaw(PgWire.begin(99L));
            assembler.onRaw(insert("9", "x"));
            assembler.onRaw(PgWire.commit());                                    // 剪枝点：v1 必须存活
            long pendingSeq = assembler.pipeWatermark();   // == 挂起桶 firstIndex（唯一带单元存活桶的 min）
            assertEquals("t_v1", registry.require(OID, pendingSeq).table());     // 挂起桶依赖版本可答
            assembler.onRaw(PgWire.commitPrepared(601L, GID));                   // 挂起桶回放：按 v1 渲染
            assertThrows(IllegalStateException.class, () -> registry.require(OID, pendingSeq));   // 完结后 v1 已剪
        }
        List<Transaction> out = collector.transactions();
        assertEquals(List.of(99L, 601L), out.stream().map(Transaction::xid).toList());
        assertEquals("t_v2", ((RowChange) out.get(0).changes().get(0)).relation().table());
        assertEquals("t_v1", ((RowChange) out.get(1).changes().get(0)).relation().table());
    }

    // --- 1.7 Task 6：桶状态机 + 交接（HANDED_OFF 桶对 CQ 删除低水位的保护，spec §9.2） ------------------

    /**
     * 1.7：在途交接桶约束 CQ 删除低水位。构造：异步组装器 + 阻塞 listener 定格第一个桶在
     * OUTPUTTING；随后交接第二个事务，断言 pipeWatermark() 不越过被阻塞桶的 firstIndex（两个桶
     * 都非 DONE，低水位被钉住）；放行后排干、close 退出，两事务均已输出。
     * 关键步骤：commit 触发第一次交接 → inCallback latch 确认 consumer 已进入回调并阻塞 →
     * 读 handedOff 记账首桶 firstIndex → 第二个事务交接（排队，同样非 DONE）→ 断言水位 ≤ 钉住值。
     * 边界：try/finally 而非 try-with-resources——latch 必须先放行再 close，否则 close 的
     * join(60s) 会被阻塞回调拖满超时；若水位保护失效（DONE 误判/维度缺失），断言消息带双值定位。
     * 线程约束：喂流与水位断言在测试线程（reader 角色）；consumer 线程定格在 listener 回调内，
     * 断言时刻不可能把任何一桶推到 DONE。
     */
    @Test
    void handedOffBucketConstrainsPipeWatermark() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch inCallback = new CountDownLatch(1);
        AtomicLong frontier = new AtomicLong();
        // 2.0 起回调面是流式事件（Begin/TxChange/End）——阻塞点任意事件即可，取首事件（Begin）
        TransactionAssembler assembler = new TransactionAssembler(e -> {
            inCallback.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg(),
                (msg, view) -> { }, frontier, () -> { });
        try {
            assembler.onRaw(relation());
            assembler.onRaw(PgWire.begin(101));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.commit());   // 第一个桶交接，consumer 进入回调并阻塞
            assertTrue(inCallback.await(5, TimeUnit.SECONDS));
            long blockedFirst = assembler.handedOffForTest().get(0).firstIndex;
            assembler.onRaw(PgWire.begin(102));
            assembler.onRaw(insert("2", "b"));
            assembler.onRaw(PgWire.commit());   // 第二个桶交接（排队，同样非 DONE）
            assertTrue(assembler.pipeWatermark() <= blockedFirst,
                    "在途桶应钉住删除低水位: wm=" + assembler.pipeWatermark() + " blockedFirst=" + blockedFirst);
        } finally {
            release.countDown();
            assembler.close();                  // 排干并退出（try/finally 而非 try-with-resources：latch 要先放行）
        }
    }
}
