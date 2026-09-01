package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;
import org.vastdata.debezium.connector.postgresql.stream.protocol.UnknownMessageTypeException;

import java.nio.file.Path;
import java.sql.Types;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * StreamedTransactionAssembler 状态机单测(raw 字节驱动版):全部输入经 {@link PgWire} 构造
 * 线格式字节直接喂 {@code onRaw},覆盖轻窥路由、控制消息 live 解码、桶记账交接与全部
 * ISE fail-fast——引擎 {@code TransactionAssemblerTest}(767 行)的逐用例翻译。
 *
 * <p><b>Task 4 形态裁定(同步 handoff 只到"冻结 + 记账")</b>:本任务组装器的
 * {@code dispatchHandedOff} 是空骨架(事件流 Begin → TxChange* → End 的发出属 Task 5 的
 * {@code TransactionConsumer.processBucket}),因此引擎用例里依赖 consumer 交付的<b>渲染/事件流
 * 断言</b>全部改锚桶级状态:kind/xid/gid/commitLsn/endLsn/commitTimestamp/unitCount
 * (= Begin.expectedChanges 的来源)/oidSet/relationSnapshot 冻结/segments 段数/
 * abortedSubxids/liveBuckets。<b>归 Task 5 的断言清单</b>(其 BucketReplayerTest /
 * StreamingDeliveryTest / TransactionRecorderTest 翻译落地时接管):用例
 * {@code assemblesBeginInsertCommitIntoNormalTransaction} / {@code assemblesNormalTransactionInOrder}
 * (RowChange 内容渲染)、{@code consecutiveTransactionsEmitOneByOne}(xid 序以交接序替代)、
 * {@code truncateAssemblesRelationSnapshotsPerOid}(relations 顺序与 options 渲染)、
 * {@code transactionalMsgGoesIntoBucket}(MsgChange 字段)、{@code assemblesSingleStreamedTransaction}
 * / {@code interleavedStreamingTransactionsEmitIndependently} /
 * {@code smallNormalTransactionBetweenStreamSegmentsRoutesCorrectly}(streamXid 与行值渲染)、
 * {@code streamAbortRemovesSubtransactionChanges} /
 * {@code streamAbortFiltersHighByteXidPrefixesExactly}(aborted 过滤后的实付行)、
 * {@code twoPhaseCommitEmitsOnCommitPrepared} / {@code streamedTwoPhaseEmitsOnCommitPrepared}
 * (TWO_PHASE 渲染)、{@code retiredBucketPrunesSupersededRegistryVersions} /
 * {@code pendingTwoPhaseBucketKeepsItsAsOfVersionAliveAcrossPruning}(版本渲染断言);
 * {@code rejectsUnknownRelationOid} / {@code truncateFailsOnUnknownOid} 在引擎是回放期
 * require miss,本形态以<b>快照侧</b>同因 ISE 锚定(报错时机提前到冻结后的 require,
 * 根因"Relation 未先行到达"不变),回放期形态归 Task 5。
 *
 * <p>夹具约定:组装器以 {@link StreamingMode#ON} 构造(非 parallel——
 * {@link PgWire#streamAbort} 只产出无附加字段的形态);管道目录取类级共享静态
 * {@code @TempDir}(每次构造组装器 wipe-on-open 顺序清空,用例间不残留);RelationResolver
 * 用假实现(直接包 wire Relation + 最小 Debezium Table,真实现 JDBC enrich 属 Task 7)。
 * 数据全部经 CQ 往返(append→段记账);Task 4 无 consumer → 桶滞留 HANDED_OFF、
 * {@code pipeWatermark()} 被交接桶 firstIndex 钉住(引擎同步形态里 consumer 已推 DONE 清出,
 * 锚点值随之不同——39/40 用例的 asOf 锚点取法按此调整,断言语义不变)。
 */
class StreamedTransactionAssemblerTest {

    /** PgWire 微秒占位 0 的解码结果(PG 纪元),提交时间戳断言统一引用。 */
    private static final Instant TS = PgWire.PG_EPOCH;
    private static final int OID = 16384;
    /** 顶层流式事务 A 的 xid——双事务交错用例。 */
    private static final long TOP_A = 7001L;
    /** 顶层流式事务 B 的 xid——双事务交错用例。 */
    private static final long TOP_B = 7002L;
    /** 子事务 xid(TOP_A 的 sub):验证流块内(子)事务归属与 StreamAbort 剔除记账。 */
    private static final long SUB = 7003L;
    /** 两阶段事务全局 id 夹具值。 */
    private static final String GID = "gid-1";

    /** 类级共享管道目录:静态 @TempDir 全类一份,用例间由 MessagePipe 的 wipe-on-open 顺序清空。 */
    @TempDir
    static Path PIPE_DIR;

    /**
     * 测试用 RelationResolver 假实现:wire Relation 原样包进 {@link ResolvedRelation},
     * Table 用 {@code Table.editor()} 造最小形态(TableId 取 wire 的 schema/table,
     * 列名沿用 wire 列序)——不连库;JDBC enrich 的真实现属 Task 7 的 RelationTableFactory。
     */
    private static final RelationResolver RESOLVER = (seq, wire) -> new ResolvedRelation(wire, tableOf(wire));

    /** 责任:按 wire Relation 造最小 Debezium Table——TableId 取 wire 的 schema/table(同名互证),列沿 wire 列序全 text。 */
    private static Table tableOf(PgOutputMessage.Relation wire) {
        var editor = Table.editor().tableId(new TableId(null, wire.schema(), wire.table()));
        for (var col : wire.columns()) {
            editor.addColumn(Column.editor().name(col.name()).jdbcType(Types.VARCHAR).type("text").create());
        }
        return editor.create();
    }

    /** 构造默认 oid 的两列 (id, v) Relation 字节,供单表场景使用。 */
    private static byte[] relation() {
        return relation(OID);
    }

    /**
     * 按指定 oid 构造两列 (id int, v text) 的 Relation 字节,列序与 {@link #insert} 对齐,
     * 供 Truncate 多表等需要多个不同 oid 的场景使用。表名默认 "t",非默认 oid 用 "t"+oid 区分。
     */
    private static byte[] relation(int oid) {
        return PgWire.relation(oid, oid == OID ? "t" : "t" + oid, "id", "v");
    }

    /** 流式块外的 Insert 字节。 */
    private static byte[] insert(String id, String v) {
        return PgWire.insert(OID, PgWire.tuple(id, v));
    }

    /** 流式块内的 Insert 字节(streamXid=产生该变更的(子)事务 xid 前缀)。 */
    private static byte[] streamedInsert(long streamXid, String id, String v) {
        return PgWire.streamed(streamXid, insert(id, v));
    }

    /** drive 夹具的产出:交接桶记账快照(handedOffForTest 的浅拷) + 残留 LIVE 桶数。 */
    private record Driven(List<TxBuffer> handedOff, long liveBuckets) { }

    /** 责任:构造同步形态组装器(StreamingMode.ON,与 PgWire.streamAbort 的非 parallel 形态配对)。 */
    private static StreamedTransactionAssembler newAssembler(StreamingTransactionListener listener) {
        return new StreamedTransactionAssembler(listener, StreamingMode.ON, new VersionedRelationRegistry(),
                RESOLVER, PIPE_DIR, LegacyRollCycles.MINUTELY);
    }

    /**
     * 依序把 raw 字节喂给新组装器('R' 的 registry 路由在组装器内部发生,经 RESOLVER 假实现
     * 包成 ResolvedRelation),收尾以 (handedOff 记账, LIVE 桶数) 交付断言——Task 4 裁定的
     * 桶级锚定面(引擎夹具返回 List<Transaction>,事件流交付归 Task 5)。
     * try-with-resources 收敛管道(每个组装器独占一条 CQ,不关会泄漏 mmap 且阻塞 @TempDir 清理)。
     */
    private static Driven drive(byte[]... msgs) {
        try (StreamedTransactionAssembler assembler = newAssembler(event -> { })) {
            for (byte[] m : msgs) {
                assembler.onRaw(m);
            }
            return new Driven(assembler.handedOffForTest(), assembler.liveBucketsForTest());
        }
    }

    /** 冒烟 1(正路径最小切片):Begin→Relation→Insert→Commit 交接出恰 1 个 NORMAL 桶——轻窥路由 + live 解码 + 冻结封箱全链路(RowChange 渲染归 Task 5)。 */
    @Test
    void assemblesBeginInsertCommitIntoNormalTransaction() {
        Driven out = drive(
                PgWire.begin(505L),
                relation(),
                insert("1", "a"),
                PgWire.commit());
        assertEquals(1, out.handedOff().size());
        assertEquals(0, out.liveBuckets());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(505L, bucket.xid);
        assertEquals(TransactionKind.NORMAL, bucket.kind);
        assertNull(bucket.gid);
        assertEquals(1L, bucket.commitLsn);          // PgWire LSN 占位
        assertEquals(2L, bucket.endLsn);
        assertEquals(TS, bucket.commitTimestamp);     // 微秒占位 0 → PG 纪元
        assertEquals(1L, bucket.unitCount);             // Begin.expectedChanges 的来源(过滤前)
        assertEquals(BucketState.HANDED_OFF, bucket.state);
        assertTrue(bucket.firstIndex >= 0 && bucket.lastIndex >= bucket.firstIndex);
        assertEquals("t", bucket.relationSnapshot.require(OID, bucket.lastIndex).wire().table());   // 快照冻结
        assertEquals("t", bucket.relationSnapshot.require(OID, bucket.lastIndex).table().id().table());   // Table 视图经 RESOLVER
    }

    /** 旧例 1:普通事务内 I/U/D 按序入桶(3 单元 1 连续段,控制消息断段),oidSet 圈定单 oid(渲染归 Task 5)。 */
    @Test
    void assemblesNormalTransactionInOrder() {
        Driven out = drive(
                PgWire.begin(505L),
                relation(),
                insert("1", "a"),
                PgWire.update(OID, null, null, PgWire.tuple("1", "b")),   // 无旧镜像(REPLICA IDENTITY DEFAULT 常态)
                PgWire.delete(OID, 'O', PgWire.tuple("1", "b")),
                PgWire.commit());
        assertEquals(1, out.handedOff().size());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(505L, bucket.xid);
        assertEquals(TransactionKind.NORMAL, bucket.kind);
        assertEquals(1L, bucket.commitLsn);
        assertEquals(2L, bucket.endLsn);
        assertEquals(TS, bucket.commitTimestamp);
        assertEquals(3L, bucket.unitCount);              // I/U/D 三单元
        assertEquals(1, bucket.segments.size());          // 连续追加:一段(控制消息在首尾,不插中间)
        assertEquals(java.util.Set.of(OID), bucket.oidSet);
    }

    /** 旧例 2:连续普通事务逐个交接(交接序即提交序);Relation 会话内一次到达、跨事务持续有效(registry 版本日志)。 */
    @Test
    void consecutiveTransactionsEmitOneByOne() {
        Driven out = drive(
                relation(),
                PgWire.begin(1L),
                insert("1", "a"),
                PgWire.commit(),
                PgWire.begin(2L),
                insert("2", "b"),
                PgWire.commit());
        assertEquals(2, out.handedOff().size());
        assertEquals(List.of(1L, 2L), out.handedOff().stream().map(b -> b.xid).toList());
        assertEquals(0, out.liveBuckets());
    }

    /** 旧例 3 = 冒烟 2:Commit 无活动普通事务桶 fail-fast。 */
    @Test
    void rejectsCommitWithoutBegin() {
        assertThrows(IllegalStateException.class, () -> drive(PgWire.commit()));
    }

    /** 旧例 4:Begin 到达但普通事务未闭合(Begin..Commit 不嵌套守卫)。 */
    @Test
    void rejectsDuplicateBegin() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.begin(1L),
                PgWire.begin(2L)));
    }

    /** 旧例 5:变更消息到达但无任何活动桶(路由期 fail-fast,异常描述带类型与 relationOid)。 */
    @Test
    void rejectsChangeWithoutActiveBucket() {
        assertThrows(IllegalStateException.class, () -> drive(
                relation(),
                insert("1", "a")));
    }

    /**
     * 旧例 6(移植适配,Task 4 再前移):变更引用未先行到达的 Relation——raw 模型数据消息
     * 不解码直接入桶,引擎的 require(oid, seq) 校验在提交期回放渲染;本任务无回放,锚点再
     * 前移到<b>快照侧</b>:registry.snapshot 省略未到达的 oid,冻结快照的 require 以同因
     * ("Relation 未先行到达")ISE 暴露。回放期形态归 Task 5(BucketReplayer)。
     */
    @Test
    void rejectsUnknownRelationOid() {
        Driven out = drive(
                PgWire.begin(1L),
                insert("1", "a"),   // 未发 Relation:快照省略该 oid
                PgWire.commit());
        TxBuffer bucket = out.handedOff().get(0);
        assertThrows(IllegalStateException.class,
                () -> bucket.relationSnapshot.require(OID, bucket.lastIndex));   // 快照侧 require miss
    }

    /** 旧例 7:Truncate 多表——oidSet 圈定全部 oid,快照逐 oid 冻结(relations 顺序与 options 渲染归 Task 5)。 */
    @Test
    void truncateAssemblesRelationSnapshotsPerOid() {
        Driven out = drive(
                PgWire.begin(1L),
                relation(16384),
                relation(16385),
                PgWire.truncate(new int[]{ 16384, 16385 }, (byte) 0x01),   // bit0 = CASCADE
                PgWire.commit());
        assertEquals(1, out.handedOff().size());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(TransactionKind.NORMAL, bucket.kind);
        assertEquals(1L, bucket.unitCount);
        assertEquals(java.util.Set.of(16384, 16385), bucket.oidSet);
        assertEquals("t", bucket.relationSnapshot.require(16384, bucket.lastIndex).wire().table());
        assertEquals("t16385", bucket.relationSnapshot.require(16385, bucket.lastIndex).wire().table());
    }

    /** 旧例 8(移植适配,同旧例 6):Truncate 引用未到达的 oid——快照侧 require miss fail-fast(回放期形态归 Task 5)。 */
    @Test
    void truncateFailsOnUnknownOid() {
        Driven out = drive(
                PgWire.begin(1L),
                relation(16384),
                PgWire.truncate(new int[]{ 16384, 404 }, (byte) 0x00),
                PgWire.commit());
        TxBuffer bucket = out.handedOff().get(0);
        assertThrows(IllegalStateException.class,
                () -> bucket.relationSnapshot.require(404, bucket.lastIndex));
    }

    /** 旧例 9:事务性 LogicalMsg 入桶随事务交接(unitCount 计入;MsgChange 字段渲染归 Task 5)。 */
    @Test
    void transactionalMsgGoesIntoBucket() {
        Driven out = drive(
                PgWire.begin(1L),
                PgWire.logicalMsg(true, "p", new byte[]{ 1 }),
                PgWire.commit());
        assertEquals(1, out.handedOff().size());
        assertEquals(1L, out.handedOff().get(0).unitCount);
        assertTrue(out.handedOff().get(0).oidSet.isEmpty());   // 'M' 无 oid
    }

    /** 旧例 10:非事务性 LogicalMsg 无任何活动桶 → WARN 丢弃(不抛异常、不交接桶)。 */
    @Test
    void nonTransactionalMsgWithoutBucketIsDropped() {
        Driven out = drive(
                PgWire.logicalMsg(false, "p", new byte[]{ 1 }));
        assertTrue(out.handedOff().isEmpty());   // 丢弃路径:不抛异常、不交接
        assertEquals(0, out.liveBuckets());
    }

    /**
     * 旧例 11(流内 Relation 适配,见类 javadoc):单流式事务两段内变更入桶、hasPrefix 桶级
     * 不量定型(streamXid 逐单元保留归 Task 5 的回放重窥)。Relation 位于流块内(S 之后),
     * 按协议形态经 {@link PgWire#streamed} 加顶层 xid 前缀。
     */
    @Test
    void assemblesSingleStreamedTransaction() {
        Driven out = drive(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamed(TOP_A, relation()),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                PgWire.streamStop(),
                PgWire.streamCommit(TOP_A));
        assertEquals(1, out.handedOff().size());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(TOP_A, bucket.xid);
        assertEquals(TransactionKind.STREAMED, bucket.kind);
        assertNull(bucket.gid);
        assertEquals(2L, bucket.unitCount);
        assertTrue(bucket.hasPrefix);            // 桶级不变量:流式桶单元恒带前缀
        assertTrue(bucket.prefixKnown);
    }

    /** 旧例 12:spec §4.2 场景——两个并发大事务流段交错,多桶各自独立、每桶恰两段(桶间不混不丢、桶内保序的渲染断言归 Task 5)。 */
    @Test
    void interleavedStreamingTransactionsEmitIndependently() {
        Driven out = drive(
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
        assertEquals(2, out.handedOff().size());
        assertEquals(TOP_A, out.handedOff().get(0).xid);
        assertEquals(TransactionKind.STREAMED, out.handedOff().get(0).kind);
        assertEquals(TOP_B, out.handedOff().get(1).xid);
        assertEquals(TransactionKind.STREAMED, out.handedOff().get(1).kind);
        // A 桶两段共 2 单元、B 桶两段共 2 单元——段间交错被控制消息断段,不混不丢
        assertEquals(2, out.handedOff().get(0).segments.size());
        assertEquals(2L, out.handedOff().get(0).unitCount);
        assertEquals(2, out.handedOff().get(1).segments.size());
        assertEquals(2L, out.handedOff().get(1).unitCount);
        assertEquals(0, out.liveBuckets());
    }

    /** 旧例 13:流段间隙插入的普通小事务先行交接(currentStream 在 stream_stop 后让位;交接序 = 提交序的渲染对照归 Task 5)。 */
    @Test
    void smallNormalTransactionBetweenStreamSegmentsRoutesCorrectly() {
        Driven out = drive(
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
        assertEquals(List.of(99L, TOP_A), out.handedOff().stream().map(b -> b.xid).toList());
        assertEquals(TransactionKind.NORMAL, out.handedOff().get(0).kind);
        assertEquals(TransactionKind.STREAMED, out.handedOff().get(1).kind);
        assertEquals(2L, out.handedOff().get(1).unitCount);
    }

    /** 旧例 14:子事务回滚——abortedSubxids 记账(sub),unitCount 保持过滤前值(回放期过滤与 emitted 断言归 Task 5)。 */
    @Test
    void streamAbortRemovesSubtransactionChanges() {
        Driven out = drive(
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                streamedInsert(SUB, "3", "c"),
                PgWire.streamStop(),
                PgWire.streamAbort(TOP_A, SUB),
                PgWire.streamCommit(TOP_A));
        assertEquals(1, out.handedOff().size());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(3L, bucket.unitCount);                                // aborted 过滤前
        assertEquals(java.util.Set.of(SUB), bucket.abortedSubxids);        // 回放期过滤依据已记账
    }

    /**
     * intAt 掩码钉子(引擎 Task 12 评审补例,防修复回退):流式前缀 xid 的 4 字节中
     * <b>任一字节 ≥ 0x80</b> 时(byte 有符号,Java 对负 byte 做 {@code |} 会符号位扩散到全部高位),
     * 解码侧 readUnsignedInt 读出的 subxid 错值 → abortedSubxids 记账错值 → 回放过滤永不命中。
     * 本形态锚定<b>记账侧</b>:三个高字节子事务的 subxid 以无符号语义精确入集合
     * (0xABCD1234 首字节 0xAB / 0x8F1234 第 3 字节 0x8F / 758=0x2F6 末字节 0xF6 /
     * 0x90AB 第 2 字节 0x90);回放期按重窥前缀过滤的实付断言归 Task 5
     * (回退到未掩码实现时集合断言同样必红——subA/subB/subC 会以负值形态漏配)。
     */
    @Test
    void streamAbortFiltersHighByteXidPrefixesExactly() {
        final long top = 0xABCD1234L;    // 首字节 0xAB
        final long subA = 0x8F1234L;     // 第 3 字节 0x8F
        final long subB = 758L;          // 末字节 0xF6(引擎 Task 12 集成实测形态)
        final long subC = 0x90ABL;       // 第 2 字节 0x90
        Driven out = drive(
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
        assertEquals(1, out.handedOff().size());
        TxBuffer t = out.handedOff().get(0);
        assertEquals(TransactionKind.STREAMED, t.kind);
        assertEquals(top, t.xid);
        assertEquals(5L, t.unitCount);   // 1(top) + subA + subB + subC + 1(top 尾行)
        assertEquals(java.util.Set.of(subA, subB, subC), t.abortedSubxids);   // 无符号精确记账
    }

    /**
     * 旧例 15:整顶层回滚(decode 层先逐子后顶,最后一条 top==sub,spec B.4)——桶整体移除、
     * 退出 LIVE 记账,StreamCommit 无从交接。同一实例驱动(不走 drive 夹具):验证的是
     * "桶被移除"而非"桶从未存在"(后续同 xid StreamCommit fail-fast)。
     */
    @Test
    void streamAbortOfWholeTopTransactionDropsBucket() {
        try (StreamedTransactionAssembler assembler = newAssembler(event -> { })) {
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
            assertEquals(0, assembler.handedOffForTest().size());
            assertEquals(0, assembler.liveBucketsForTest());   // 整桶丢弃:退出 LIVE 记账
            // 同一实例:桶已被移除 → 后续同 xid StreamCommit fail-fast(非静默)
            assertThrows(IllegalStateException.class, () -> assembler.onRaw(PgWire.streamCommit(TOP_A)));
        }
    }

    /** 旧例 16:StreamStart(first=false) 但顶层事务无桶 fail-fast。 */
    @Test
    void rejectsStreamContinueForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamStart(TOP_A, false)));   // 首段标记 false 但无桶
    }

    /** 旧例 17:同顶层事务再次 first=true(桶已存在)fail-fast。 */
    @Test
    void rejectsDuplicateFirstSegment() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamStop(),
                PgWire.streamStart(TOP_A, true)));
    }

    /** 旧例 18:StreamStop 到达但无进行中的流块 fail-fast。 */
    @Test
    void rejectsStreamStopWithoutStreamBlock() {
        assertThrows(IllegalStateException.class, () -> drive(PgWire.streamStop()));
    }

    /** 旧例 19:StreamCommit 对应流式事务桶不存在 fail-fast。 */
    @Test
    void rejectsStreamCommitForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> drive(PgWire.streamCommit(404L)));
    }

    /** 旧例 20:StreamCommit 到达但流块未闭合 fail-fast。 */
    @Test
    void rejectsStreamCommitWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamCommit(TOP_A)));   // 流块未闭合
    }

    /** 旧例 21:StreamAbort 到达但流块未闭合 fail-fast。 */
    @Test
    void rejectsStreamAbortWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamAbort(TOP_A, TOP_A)));
    }

    /**
     * 终审 Fix C:流块嵌套违规——S..S 不夹 E(上一流块未闭合又来 StreamStart)fail-fast,
     * 与 'c'/'A'/'p'/'E' 处理器的"流块未闭合"守卫对齐(此前该形态会静默改写 currentStream,
     * 丢失原桶的流块上下文)。
     */
    @Test
    void rejectsStreamStartWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamStart(TOP_B, true)));   // 未闭合 TOP_A 流块又开新流块
    }

    /** 旧例 22:StreamAbort 对应顶层事务桶不存在 fail-fast。 */
    @Test
    void rejectsStreamAbortForUnknownTopXid() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamAbort(404L, 405L)));
    }

    /** 旧例 23:2PC——b..P 入挂起池(LIVE 记账钉住),K(gid) 交接封箱 TWO_PHASE(渲染归 Task 5)。 */
    @Test
    void twoPhaseCommitEmitsOnCommitPrepared() {
        Driven out = drive(
                relation(),
                PgWire.beginPrepare(601L, GID),
                insert("1", "a"),
                PgWire.prepare(601L, GID),
                PgWire.commitPrepared(601L, GID));
        assertEquals(1, out.handedOff().size());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(TransactionKind.TWO_PHASE, bucket.kind);
        assertEquals(GID, bucket.gid);
        assertEquals(601L, bucket.xid);
        assertEquals(1L, bucket.commitLsn);   // 'K' 的 LSN 占位
        assertEquals(1L, bucket.unitCount);
    }

    /** 旧例 24:Prepare 后挂起(LIVE 存活,剪枝低水位候选);RollbackPrepared 静默丢弃(不交接、退出 LIVE)。 */
    @Test
    void rollbackPreparedDiscardsSilently() {
        Driven out = drive(
                relation(),
                PgWire.beginPrepare(601L, GID),
                insert("1", "a"),
                PgWire.prepare(601L, GID),
                PgWire.rollbackPrepared(601L, GID));
        assertEquals(0, out.handedOff().size());
        assertEquals(0, out.liveBuckets());   // 挂起桶被丢弃后 LIVE 清零
    }

    /** 旧例 25:流式 2PC——StreamPrepare 前必有最后一个流段并已闭合(spec B.6),桶转挂起池,K 交接(渲染归 Task 5)。 */
    @Test
    void streamedTwoPhaseEmitsOnCommitPrepared() {
        Driven out = drive(
                relation(),
                PgWire.streamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                PgWire.streamStop(),
                PgWire.streamPrepare(TOP_A, GID),
                PgWire.commitPrepared(TOP_A, GID));
        assertEquals(1, out.handedOff().size());
        TxBuffer bucket = out.handedOff().get(0);
        assertEquals(TransactionKind.TWO_PHASE, bucket.kind);
        assertEquals(GID, bucket.gid);
        assertEquals(TOP_A, bucket.xid);
        assertEquals(1L, bucket.unitCount);
        assertTrue(bucket.hasPrefix);
    }

    /** 旧例 26:CommitPrepared 对应 gid 不存在 fail-fast。 */
    @Test
    void rejectsCommitPreparedForUnknownGid() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.commitPrepared(1L, "no-such-gid")));
    }

    /** 旧例 27:同 gid 第二次 Prepare(挂起池已存在)fail-fast。 */
    @Test
    void rejectsDuplicatePrepareGid() {
        assertThrows(IllegalStateException.class, () -> drive(
                relation(),
                PgWire.beginPrepare(601L, GID),
                PgWire.prepare(601L, GID),
                // 同 gid 第二次 Prepare:挂起池已存在 → fail-fast
                PgWire.beginPrepare(602L, GID),
                PgWire.prepare(602L, GID)));
    }

    /** 旧例 28:无活动两阶段桶的 Prepare fail-fast。 */
    @Test
    void rejectsPrepareWithoutBeginPrepare() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.prepare(601L, GID)));
    }

    /** 旧例 29:两阶段桶未闭合(无 Prepare)又来 BeginPrepare fail-fast(b..P 串行不嵌套守卫)。 */
    @Test
    void rejectsDuplicateBeginPrepare() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.beginPrepare(601L, GID),
                PgWire.beginPrepare(602L, "gid-2")));
    }

    /** 旧例 30:Prepare 与活动两阶段桶的 xid/gid 不匹配 fail-fast(两条子句各一用例)。 */
    @Test
    void rejectsPrepareMismatchedXidOrGid() {
        // xid 不匹配
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.beginPrepare(601L, GID),
                PgWire.prepare(602L, GID)));
        // gid 不匹配
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.beginPrepare(601L, GID),
                PgWire.prepare(601L, "gid-2")));
    }

    /** 旧例 31:RollbackPrepared 对应 gid 不存在 fail-fast(回滚路径不静默吞未知 gid)。 */
    @Test
    void rejectsRollbackPreparedForUnknownGid() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.rollbackPrepared(601L, "no-such-gid")));
    }

    /** 旧例 32:StreamPrepare 到达但流块未闭合 fail-fast(stream_prepare 前必已 stream_stop,spec B.6)。 */
    @Test
    void rejectsStreamPrepareWithOpenStreamBlock() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamStart(TOP_A, true),
                PgWire.streamPrepare(TOP_A, GID)));
    }

    /** 旧例 33:无对应流桶的 StreamPrepare fail-fast(挂起池不能凭空接纳未知 xid)。 */
    @Test
    void rejectsStreamPrepareForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> drive(
                PgWire.streamPrepare(404L, GID)));
    }

    /** 新增(brief "Type/Origin 透传"):'Y'/'O' 不入桶、不影响组装——raw 模型下由组装器自行丢弃(旧版由 instanceof 链忽略)。 */
    @Test
    void typeAndOriginMessagesAreIgnored() {
        Driven out = drive(
                PgWire.type(19, "bytea"),
                PgWire.origin("origin-1"),
                relation(),
                PgWire.begin(1L),
                insert("1", "a"),
                PgWire.type(25, "text"),
                PgWire.commit());
        assertEquals(1, out.handedOff().size());
        assertEquals(1L, out.handedOff().get(0).unitCount);   // Y/O 均未混入桶
    }

    /** 新增:未知类型字节经 live 解码 fail-fast(路由表 default 分支 → UnknownMessageTypeException)。 */
    @Test
    void rejectsUnknownMessageTypeByte() {
        assertThrows(UnknownMessageTypeException.class, () -> drive(new byte[]{ 'X' }));
    }

    // --- registry 版本日志剪枝接线(桶完结点驱动,同 oid 多版本场景) ------------------------------

    /**
     * 桶完结驱动 registry 剪枝:无存活桶时低水位取"无穷"——被新版本取代的旧版本在下一个桶完结点
     * 被剪掉(旧 asOf 查询 ISE 证明确实剪了,非空转),后续事务仍按新版本冻结快照。
     * 消息序(seq ≡ CQ index,绝对值随建队列时刻漂移、不可字面断言):R(t_v1)、B、I、C(完结点①:
     * 仅 v1 在册,floor 保留);R(t_v2)、B、I、C(完结点②:无存活桶 → v1 剪除);B、I、C(快照按 v2 冻结)。
     * 同一 registry 实例贯穿全程(剪枝副作用可观测的前提)。
     * asOf 锚点取法(Task 4 偏差):本形态无 consumer,首桶滞留 HANDED_OFF 钉住
     * {@code pipeWatermark()}(引擎同步形态里 consumer 已把首桶推 DONE 清出,锚点为 Commit 的
     * index;这里被钉在首单元 index)——减一即 v1 时代的更早消息 index,先证其可解析(v1 在册),
     * 再在完结点②后断言同 asOf 抛 ISE(剪枝确已发生,而非 asOf 值本身无效的空转)。
     * 三个事务的 RowChange 版本渲染断言(t_v1/t_v2/t_v2)归 Task 5。
     */
    @Test
    void retiredBucketPrunesSupersededRegistryVersions() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        try (StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(
                event -> { }, StreamingMode.ON, registry, RESOLVER, PIPE_DIR, LegacyRollCycles.MINUTELY)) {
            assembler.onRaw(PgWire.relation(OID, "t_v1", "id", "v"));
            assembler.onRaw(PgWire.begin(1L));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.commit());
            long v1EraSeq = assembler.pipeWatermark() - 1L;   // 首桶滞留 HANDED_OFF → 水位=其 firstIndex,减一=v1 时代更早消息
            assertEquals("t_v1", registry.require(OID, v1EraSeq).wire().table());   // 剪枝前该 asOf 可解析
            assembler.onRaw(PgWire.relation(OID, "t_v2", "id", "v"));
            assembler.onRaw(PgWire.begin(2L));
            assembler.onRaw(insert("2", "b"));
            assembler.onRaw(PgWire.commit());
            assertThrows(IllegalStateException.class, () -> registry.require(OID, v1EraSeq));   // v1 已剪
            assertEquals("t_v2", registry.find(OID).orElseThrow().wire().table());             // 最新视图仍可答(引擎单参 require 在 connector 由 find 承担)
            assembler.onRaw(PgWire.begin(3L));
            assembler.onRaw(insert("3", "c"));
            assembler.onRaw(PgWire.commit());                                            // 剪枝后新快照仍正确
            assertEquals(3, assembler.handedOffForTest().size());
            // 第三个桶的冻结快照按 v2 圈定(渲染归 Task 5,此处锚快照侧)
            TxBuffer third = assembler.handedOffForTest().get(2);
            assertEquals("t_v2", third.relationSnapshot.require(OID, third.lastIndex).wire().table());
        }
    }

    /**
     * 2PC 挂起桶算存活(剪枝低水位候选):挂起桶的旧单元依赖 v1——其 seq(R(t_v1) 的 index)
     * 早于桶 firstIndex('R' 恒先于同表 DML 到达),其间他桶(普通事务 99)完结触发的剪枝必须
     * 保住 v1(floor 语义);挂起桶最终 CommitPrepared 时快照按 v1 冻结,其完结后 v1 才被剪。
     * 这是"以存活桶 firstIndex 为低水位"接线正确性的钉子用例(若按"丢弃 seq &lt; 低水位"的
     * 字面实现,v1 会在事务 99 的完结点被误剪,挂起桶快照圈定 miss)。
     * 消息序:R(t_v1)、b、I(挂起桶 firstIndex)、P、R(t_v2)、B、I、C(剪枝点:低水位 = 挂起桶
     * firstIndex)、K(挂起桶按 v1 冻结快照,完结后 v1 剪除)。
     * 挂起桶单元 seq 取法:剪枝点后挂起桶是唯一带单元的存活桶,但 Task 4 无 consumer →
     * 事务 99 的交接桶滞留 HANDED_OFF 也参与水位 min;挂起桶 firstIndex(2)更小,
     * {@code pipeWatermark()} 仍恰等于它——即旧单元自身的 asOf(与引擎同步形态同值)。
     * 两个事务的版本渲染断言(t_v2/t_v1)归 Task 5。
     */
    @Test
    void pendingTwoPhaseBucketKeepsItsAsOfVersionAliveAcrossPruning() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        try (StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(
                event -> { }, StreamingMode.ON, registry, RESOLVER, PIPE_DIR, LegacyRollCycles.MINUTELY)) {
            assembler.onRaw(PgWire.relation(OID, "t_v1", "id", "v"));
            assembler.onRaw(PgWire.beginPrepare(601L, GID));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.prepare(601L, GID));
            assembler.onRaw(PgWire.relation(OID, "t_v2", "id", "v"));
            assembler.onRaw(PgWire.begin(99L));
            assembler.onRaw(insert("9", "x"));
            assembler.onRaw(PgWire.commit());                                    // 剪枝点:v1 必须存活
            long pendingSeq = assembler.pipeWatermark();   // == 挂起桶 firstIndex(min 语义,见上)
            assertEquals("t_v1", registry.require(OID, pendingSeq).wire().table());     // 挂起桶依赖版本可答
            assembler.onRaw(PgWire.commitPrepared(601L, GID));                   // 挂起桶交接:快照按 v1 冻结
            assertThrows(IllegalStateException.class, () -> registry.require(OID, pendingSeq));   // 完结后 v1 已剪
            assertEquals(List.of(99L, 601L), assembler.handedOffForTest().stream().map(b -> b.xid).toList());
            TxBuffer pending = assembler.handedOffForTest().get(1);
            assertEquals("t_v1", pending.relationSnapshot.require(OID, pending.lastIndex).wire().table());   // 按 v1 冻结
        }
    }

    // --- 交接桶对 CQ 删除低水位的保护(spec §9.2 同源) -----------------------------------------

    /**
     * 非 DONE 交接桶钉住 CQ 删除低水位(Task 4 同步滞留形态):本任务无 consumer,交接桶
     * 恒滞留 HANDED_OFF——首个交接桶的 firstIndex 即水位上界,后续交接不顶开它;
     * 两个桶都非 DONE,水位被 min 钉住。
     * 引擎同名用例的异步阻塞形态(consumer 定格 OUTPUTTING)归 Task 6
     * (DecoupledEquivalenceTest / StreamingDeliveryTest 翻译);DONE 解钉路径归 Task 5。
     */
    @Test
    void handedOffBucketConstrainsPipeWatermark() throws Exception {
        try (StreamedTransactionAssembler assembler = newAssembler(event -> { })) {
            assembler.onRaw(relation());
            assembler.onRaw(PgWire.begin(101L));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.commit());   // 第一个桶交接,滞留 HANDED_OFF(无 consumer)
            assertEquals(1, assembler.handedOffForTest().size());
            long blockedFirst = assembler.handedOffForTest().get(0).firstIndex;
            assertEquals(blockedFirst, assembler.pipeWatermark());
            assembler.onRaw(PgWire.begin(102L));
            assembler.onRaw(insert("2", "b"));
            assembler.onRaw(PgWire.commit());   // 第二个桶交接,同样非 DONE
            assertTrue(assembler.pipeWatermark() <= blockedFirst,
                    "在途桶应钉住删除低水位: wm=" + assembler.pipeWatermark() + " blockedFirst=" + blockedFirst);
        }
    }

    /** 边界补钉:管道空载时(maxAppendedIndex=-1)水位为 0——空队列无物可删,天然安全(引擎 pipeWatermark 契约)。 */
    @Test
    void emptyPipeWatermarkIsZero() throws Exception {
        try (StreamedTransactionAssembler assembler = newAssembler(event -> { })) {
            assertEquals(0L, assembler.pipeWatermark());
            assertFalse(assembler.pipeWatermark() < 0L);
        }
    }
}
