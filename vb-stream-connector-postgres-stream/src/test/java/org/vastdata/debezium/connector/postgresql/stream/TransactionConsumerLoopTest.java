package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.file.Path;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code TransactionConsumer.run()} 消费循环专项单测(Task 6 落地 run 半程):<b>绕过组装器</b>
 * 直接以"预填交接队列 + 测试线程调 run()"驱动循环本体,逐机制验证——排队桶按 FIFO 排干、
 * 毒丸即退出信号(run 正常返回)、处理失败 fail-fast(onFailure 触发 + <b>队列不排干</b> +
 * 前沿不推进)。引擎对位面经异步组装器 + close 间接覆盖(引擎无 run 的直连单测),本类把
 * 循环协议钉在单测层——DecoupledEquivalenceTest 只触到 happy path 的排干,失败路径在这里
 * 先行钉死。夹具沿用 {@link BucketReplayerTest} 的"管道追加 + 手造冻结桶"形态(测试线程
 * 扮 consumer,run() 同线程同步执行,零并发面——线程语义(异步形态的 consumer 线程驱动)由
 * StreamingDeliveryTest/DecoupledEquivalenceTest 的跨线程用例覆盖)。
 */
class TransactionConsumerLoopTest {

    private static final int OID = 16384;
    /** PgWire 微秒占位 0 的解码结果(PG 纪元),冻结元数据断言/构造统一引用。 */
    private static final Instant TS = PgWire.PG_EPOCH;

    /** 每用例独立的管道目录(@TempDir):用例间零残留。 */
    @TempDir
    Path pipeDir;

    /** 构造两列 (id int 键列, v text) 的 wire Relation 样本(与 BucketReplayerTest 同款)。 */
    private static PgOutputMessage.Relation rel(String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", table,
                'd', List.of(new RelationColumn("id", 23, -1, true), new RelationColumn("v", 25, -1, false)));
    }

    /**
     * 责任:把 wire Relation 包成版本日志载荷(双形态)——Table 用 {@code Table.editor()} 造
     * 最小形态(TableId 取 wire 的 schema/table,列沿 wire 列序全 text),不连库
     * (与组装器测试的假 RESOLVER 同款)。
     */
    private static ResolvedRelation resolved(PgOutputMessage.Relation wire) {
        var editor = Table.editor().tableId(new TableId(null, wire.schema(), wire.table()));
        for (var col : wire.columns()) {
            editor.addColumn(io.debezium.relational.Column.editor()
                    .name(col.name()).jdbcType(Types.VARCHAR).type("text").create());
        }
        return new ResolvedRelation(wire, editor.create());
    }

    /**
     * 手造一个"已交接冻结"的普通事务桶:向管道追加一条 Insert 单元并记段,registry 预置版本
     * (accept seq 取单元 index-10——'R' 恒先于 DML 到达的相对化表达)后以 oidSet 圈定、截止
     * lastIndex 拷出快照冻结,封箱元数据按入参落定——组装器 handoff 的同款冻结语义,
     * 直接驱动消费器必须补上这一步。
     *
     * @param xid     事务 xid(断言侧区分桶用)
     * @param endLsn  封箱 endLsn(前沿累加的期望来源,两桶取不同值以断言单调 max)
     * @param registry 版本日志(预置版本 + 拷快照共用)
     * @param pipe    单元字节所在管道
     * @return 已冻结(HANDED_OFF)的桶
     */
    private static TxBuffer frozenBucket(long xid, long endLsn, VersionedRelationRegistry registry, MessagePipe pipe) {
        TxBuffer bucket = new TxBuffer(xid);
        bucket.hasPrefix = false;
        bucket.prefixKnown = true;
        long index = pipe.append(PgWire.insert(OID, PgWire.tuple(Long.toString(xid), "v")));
        bucket.segments.addLast(new long[]{ index, index });
        bucket.firstIndex = index;
        bucket.lastIndex = index;
        bucket.unitCount = 1;
        bucket.oidSet.add(OID);
        registry.accept(index - 10L, resolved(rel("t")));
        bucket.relationSnapshot = registry.snapshot(bucket.oidSet, bucket.lastIndex);
        bucket.kind = TransactionKind.NORMAL;
        bucket.commitLsn = endLsn - 1L;
        bucket.endLsn = endLsn;
        bucket.commitTimestamp = TS;
        bucket.state = BucketState.HANDED_OFF;
        bucket.handoffNanos = System.nanoTime();
        return bucket;
    }

    /**
     * 责任:正常排干路径——预填两个冻结桶 + 毒丸后,测试线程同步执行 {@code run()}:两桶按
     * FIFO 顺序各产出完整事件流(Begin → 1 条 TxChange → End,经 TransactionRecorder 重组对账),
     * 前沿以 endLsn 单调 max 累加到较大者,见到毒丸即返回(run 正常落回——测试能走到断言即证
     * 退出,无死循环)。
     * 边界:毒丸排在队列尾,FIFO 保证先排干全部真实桶;空轮询路径(poll 1s 空转)不经本用例
     * (需要真实等待,属跨线程用例的覆盖面)。
     */
    @Test
    void runDrainsQueuedBucketsInFifoOrderAndExitsOnPoison() {
        TransactionRecorder recorder = new TransactionRecorder();
        AtomicLong frontier = new AtomicLong();
        BlockingQueue<TxBuffer> queue = new LinkedBlockingQueue<>();
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            queue.add(frozenBucket(501L, 20L, registry, pipe));
            queue.add(frozenBucket(502L, 10L, registry, pipe));
            queue.add(TxBuffer.POISON);
            TransactionConsumer consumer = new TransactionConsumer(recorder, StreamingMode.ON, pipe,
                    queue, frontier, () -> { }, (m, v) -> { }, BucketTableResolver.snapshotBacked(),
                    new StreamThroughputMetrics());

            consumer.run();   // 测试线程同步执行:排干两桶 → 见毒丸 → 返回

            assertEquals(List.of(501L, 502L),
                    recorder.transactions().stream().map(Transaction::xid).toList(),
                    "排队桶按 FIFO 交接序排干");
            assertEquals(1, recorder.transactions().get(0).changes().size());
            assertEquals(1, recorder.transactions().get(1).changes().size());
            assertEquals(20L, frontier.get(), "前沿取两桶 endLsn 的单调 max");
        }
    }

    /**
     * 责任:失败语义(fail-fast 不排干)——队首桶的回放在 End 前抛出(快照缺该 oid 的版本,
     * require 即 ISE):run() 触发 onFailure(恰一次)后退出,<b>第二个排队桶不被排干</b>
     * (重组器零事务、前沿钉初值 0),队头仍滞留第二个桶。
     * 边界:onFailure 抛出自身异常属违约(逃生回调不得再抛),本用例的 onFailure 为纯计数,
     * 不覆盖该约束;捕捉面是 Throwable 级(任何回放异常都走同一路径,此处以 ISE 为代表切片)。
     */
    @Test
    void runFailsFastWithoutDrainingWhenBucketReplayThrows() {
        TransactionRecorder recorder = new TransactionRecorder();
        AtomicLong frontier = new AtomicLong();
        AtomicInteger failures = new AtomicInteger();
        BlockingQueue<TxBuffer> queue = new LinkedBlockingQueue<>();
        try (MessagePipe pipe = new MessagePipe(pipeDir, LegacyRollCycles.MINUTELY)) {
            VersionedRelationRegistry registry = new VersionedRelationRegistry();
            TxBuffer broken = frozenBucket(601L, 20L, registry, pipe);
            broken.relationSnapshot = registry.snapshot(java.util.Set.of(), broken.lastIndex);   // 空快照:require(OID) 必 ISE
            queue.add(broken);
            queue.add(frozenBucket(602L, 10L, registry, pipe));
            TransactionConsumer consumer = new TransactionConsumer(recorder, StreamingMode.ON, pipe,
                    queue, frontier, failures::incrementAndGet, (m, v) -> { }, BucketTableResolver.snapshotBacked(),
                    new StreamThroughputMetrics());

            consumer.run();   // 首桶 ISE → ERROR + onFailure + return(不排干)

            assertEquals(1, failures.get(), "onFailure 恰触发一次");
            assertTrue(recorder.transactions().isEmpty(), "End 未达——重组器零事务");
            assertEquals(0L, frontier.get(), "End 未达——前沿不得推进");
            TxBuffer remaining = queue.poll();
            assertNotNull(remaining, "队列不排干:第二个桶应滞留队头");
            assertEquals(602L, remaining.xid, "滞留的恰是第二个(未处理)桶");
        }
    }
}
