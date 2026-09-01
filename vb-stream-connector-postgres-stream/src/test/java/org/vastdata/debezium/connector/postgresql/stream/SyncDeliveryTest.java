package org.vastdata.debezium.connector.postgresql.stream;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同步形态交付链路单测(Task 5 接上 Task 4 钩子后的首道防线):{@code StreamedTransactionAssembler}
 * 的 {@code dispatchHandedOff} 直调 {@code TransactionConsumer.processBucket},本类以完整事件流
 * 路径钉住交付契约——空/非空 listener 走 Begin → TxChange* → End、End 之后前沿才推进、桶
 * DONE、listener 抛出截断(End 永不发、前沿不推进)、listener 侧 asOf 表解析接缝
 * ({@link BucketTableResolver})的绑定与透传。引擎对位面散在 TransactionAssemblerTest 与
 * StreamingDeliveryTest(2.0 spec §5.2),本类按同步形态收拢。
 *
 * <p>夹具约定:组装器以 {@link StreamingMode#ON} 构造(与 PgWire.streamAbort 的非 parallel
 * 形态配对);管道目录取类级共享静态 {@code @TempDir}(wipe-on-open 顺序清空);RelationResolver
 * 用与 StreamedTransactionAssemblerTest 同款的假实现(wire + 最小 Table)。回调内读取组装器
 * 状态(桶/前沿)在同一线程内联发生(同步形态无跨线程),断言确定性;持组装器引用的数组
 * holder 仅为绕开"构造参数要先有 listener"的先有鸡先有蛋,不构成并发面。
 */
class SyncDeliveryTest {

    private static final int OID = 16384;
    /** PgWire LSN 占位:Commit 消息的 endLsn 恒为 2(前沿推进断言的期望值)。 */
    private static final long COMMIT_END_LSN = 2L;

    /** 类级共享管道目录:静态 @TempDir 全类一份,用例间由 MessagePipe 的 wipe-on-open 顺序清空。 */
    @TempDir
    static Path PIPE_DIR;

    /** 测试用 RelationResolver 假实现(Task 3 账本回收项起收拢进共享夹具 {@link TestRelations},此前为本类私有的逐字重复工厂)。 */
    private static final RelationResolver RESOLVER = TestRelations.RESOLVER;

    /** 构造默认 oid 的两列 (id, v) Relation 字节。 */
    private static byte[] relation() {
        return PgWire.relation(OID, "t", "id", "v");
    }

    /** 流式块外的 Insert 字节。 */
    private static byte[] insert(String id, String v) {
        return PgWire.insert(OID, PgWire.tuple(id, v));
    }

    /**
     * <b>Task 5 首个新用例(钩子被调的等价防线)</b>:空内容 listener(重组器)驱动
     * Begin → Relation → 3×Insert → Commit,断言完整事件流——Begin 头(expectedChanges=3,
     * 元数据齐全)→ 3 条 RowChange(行值/streamXid/relation 渲染)→ End 尾(emitted=3);
     * End 之后前沿推进到 Commit 的 endLsn、桶推进 DONE(Task 4 骨架形态下这些全不发生,
     * 本例即钩子真正接上的证明)。
     */
    @Test
    void emptyListenerReceivesFullEventFlowAndBucketDone() {
        TransactionRecorder recorder = new TransactionRecorder();
        TxBuffer[] bucketAtEnd = new TxBuffer[1];
        StreamedTransactionAssembler[] handle = new StreamedTransactionAssembler[1];
        try (StreamedTransactionAssembler assembler = newAssembler(event -> {
            recorder.onEvent(event);
            if (event instanceof TransactionEvent.End) {
                bucketAtEnd[0] = handle[0].handedOffForTest().get(0);   // End 返回前持桶引用,事后读终态
            }
        })) {
            handle[0] = assembler;
            assembler.onRaw(relation());
            assembler.onRaw(PgWire.begin(505L));
            for (int i = 1; i <= 3; i++) {
                assembler.onRaw(insert(Integer.toString(i), "v" + i));
            }
            assembler.onRaw(PgWire.commit());   // 交接 → dispatchHandedOff → processBucket 同步内联

            assertEquals(1, recorder.transactions().size());
            Transaction t = recorder.transactions().get(0);
            assertEquals(505L, t.xid());
            assertEquals(TransactionKind.NORMAL, t.kind());
            assertEquals(COMMIT_END_LSN, t.endLsn());
            assertEquals(3, t.changes().size());
            assertEquals(List.of("1", "2", "3"),
                    t.changes().stream()
                            .map(c -> ((org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue.Text)
                                    ((RowChange) c).after().orElseThrow().columns().get(0)).value())
                            .toList());
            assertTrue(t.changes().stream().allMatch(c -> c.streamXid().isEmpty()));
            assertEquals(COMMIT_END_LSN, assembler.outputFrontierForTest(), "End 之后前沿推进到 endLsn");
            assertSame(BucketState.DONE, bucketAtEnd[0].state, "End 返回后桶推进 DONE");
            assertTrue(assembler.handedOffForTest().isEmpty(), "DONE 桶在交接后的完结点被惰性清出");
        }
    }

    /** 空桶(Begin 后无变更即 Commit)产出 Begin → End(0),前沿照常推进、桶 DONE——协议合法路径。 */
    @Test
    void emptyBucketDeliversBeginAndEndZero() {
        TransactionRecorder recorder = new TransactionRecorder();
        try (StreamedTransactionAssembler assembler = newAssembler(recorder)) {
            assembler.onRaw(PgWire.begin(1L));
            assembler.onRaw(PgWire.commit());

            assertEquals(1, recorder.transactions().size());
            assertEquals(0, recorder.transactions().get(0).changes().size());
            assertEquals(COMMIT_END_LSN, assembler.outputFrontierForTest());
        }
    }

    /**
     * listener 抛出红线:第 2 条 TxChange 的回调抛标记异常 → 原样经 onRaw 上抛(fail-fast),
     * End 永不发(重组器只见 Begin + 1 条变更)、前沿不推进(仍为初值 0)、桶停在 OUTPUTTING
     * (DONE 仅在 End 之后)——三件套同证"End 返回 = 完整消费确认"的截断语义。
     */
    @Test
    void listenerThrowSuppressesEndAndFrontier() {
        class Boom extends RuntimeException { }
        TransactionRecorder recorder = new TransactionRecorder();
        StreamedTransactionAssembler assembler = newAssembler(event -> {
            if (event instanceof TxChange c && "2".equals(firstColumn(c))) {
                throw new Boom();
            }
            recorder.onEvent(event);
        });
        try {
            assembler.onRaw(relation());
            assembler.onRaw(PgWire.begin(505L));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(insert("2", "b"));
            assembler.onRaw(insert("3", "c"));
            assertThrows(Boom.class, () -> assembler.onRaw(PgWire.commit()));
            // End 未发:重组器停在开桶状态(1 条已交付变更,事务未封箱)
            assertEquals(0, recorder.transactions().size());
            assertEquals(0L, assembler.outputFrontierForTest(), "End 未达——前沿不得推进");
            assertEquals(1, assembler.handedOffForTest().size());
            assertSame(BucketState.OUTPUTTING, assembler.handedOffForTest().get(0).state,
                    "DONE 仅在 End 与前沿之后——截断桶停在 OUTPUTTING");
        } finally {
            assembler.close();
        }
    }

    /**
     * listener 侧 asOf 表解析接缝:{@link BucketTableResolver} 在 Begin 发出前绑定该桶快照
     * (绑定先于 Begin 可观测),TxChange 回调内按 (oid, seq) resolve 得到变更时刻的
     * {@link ResolvedRelation}(wire + Table 双形态)——Task 7 的 DispatcherTransactionListener
     * 即经此通道拿 Table(事件本身不携带快照)。
     */
    @Test
    void bucketTableResolverBindsSnapshotBeforeBeginAndResolvesAsOf() {
        List<String> timeline = new ArrayList<>();
        ResolvedRelation[] resolvedAtChange = new ResolvedRelation[1];
        RelationSnapshot[] bound = new RelationSnapshot[1];   // bind 侧快照引用,resolve 透传读它
        BucketTableResolver spy = new BucketTableResolver() {
            @Override
            public void bind(RelationSnapshot bucketSnapshot) {
                bound[0] = bucketSnapshot;
                timeline.add("bind");
            }

            @Override
            public ResolvedRelation resolve(int relationOid, long asOfSeq) {
                timeline.add("resolve");
                return bound[0].require(relationOid, asOfSeq);   // 透传:直接透传 ResolvedRelation 的假实现形态
            }
        };
        TransactionRecorder recorder = new TransactionRecorder();
        try (StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(
                event -> {
                    if (event instanceof TransactionEvent.Begin) {
                        timeline.add("begin");
                    } else if (event instanceof TransactionEvent.End) {
                        timeline.add("end");
                    } else if (event instanceof RowChange c) {
                        timeline.add("change");
                        resolvedAtChange[0] = spy.resolve(c.relation().relationOid(), c.seq());
                    }
                    recorder.onEvent(event);
                }, StreamingMode.ON, new VersionedRelationRegistry(), RESOLVER,
                PIPE_DIR, LegacyRollCycles.MINUTELY, (msg, view) -> { }, spy)) {
            assembler.onRaw(relation());
            assembler.onRaw(PgWire.begin(505L));
            assembler.onRaw(insert("1", "a"));
            assembler.onRaw(PgWire.commit());

            assertEquals(List.of("bind", "begin", "change", "resolve", "end"), timeline,
                    "绑定先于 Begin,resolve 发生在 TxChange 回调内、End 之前");
            assertEquals("t", resolvedAtChange[0].wire().table());
            assertEquals("t", resolvedAtChange[0].table().id().table());   // Table 形态随快照可达
            assertEquals(1, recorder.transactions().size());
        }
    }

    /** 构造同步形态组装器(默认 snapshotBacked 表解析接缝)。 */
    private static StreamedTransactionAssembler newAssembler(StreamingTransactionListener listener) {
        return new StreamedTransactionAssembler(listener, StreamingMode.ON, new VersionedRelationRegistry(),
                RESOLVER, PIPE_DIR, LegacyRollCycles.MINUTELY);
    }

    /** 取行变更 after 元组首列的文本值(夹具两列 (id, v) 的 id 列)。 */
    private static String firstColumn(TxChange c) {
        return ((org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue.Text)
                ((RowChange) c).after().orElseThrow().columns().get(0)).value();
    }
}
