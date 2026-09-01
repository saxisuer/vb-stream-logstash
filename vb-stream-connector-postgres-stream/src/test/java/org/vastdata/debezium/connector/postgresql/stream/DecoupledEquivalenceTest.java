package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 解耦等价性与 D7 快速停机验收(Task 6):同一字节流分别过两种形态——同步消费(单线程直调
 * processBucket,既有用例的驱动形态)与真实双线程管道(异步构造器起 {@code transaction-consumer}
 * 线程 + close 排干)——断言两者<b>完整事件流</b>全等({@code List<TransactionEvent>} 含 Begin
 * 头与 End 尾元数据,比整块 Transaction 更严)+ 整块全等(经 {@link TransactionRecorder} 重组,
 * 双保险)。引擎 {@code DecoupledEquivalenceTest}(146 行)的翻译;<b>偏差</b>:引擎第三形态
 * (block 输出经 {@code StreamingToBlockAdapter} 重组)是引擎 OutputMode 逃生门,connector
 * MS2 无此契约,断言面收敛为同步/异步两形态。
 *
 * <p>另载 D7 新形态验收 {@code shutdownFastDoesNotWaitForPendingBuckets}(引擎无对应物,
 * MS2 连接器停机序的新增行为):入队大桶(未回放)后 shutdownFast 立即返回、listener 未收
 * End——与 {@code close()}(排干,join 60s)的语义分叉点。
 *
 * <p>夹具约定:LSN/时间戳按 PgWire 占位约定(全部提交消息 endLsn=2、PG 纪元);'A' 为非
 * parallel 形态,组装器以 {@link StreamingMode#ON} 构造;管道目录取用例级 {@code @TempDir}
 * (两个组装器顺序构造,MessagePipe 的 wipe-on-open 保证互不残留)。
 */
class DecoupledEquivalenceTest {

    private static final int OID = 16384;
    /** PgWire LSN 占位:全部提交消息的 endLsn 恒为 2(前沿终态期望值)。 */
    private static final long COMMIT_END_LSN = 2L;
    /** D7 用例的大桶行数:足以把"排干耗时"与"立即返回"在量级上区分开(断言窗远小于 close 的 join 60s)。 */
    private static final int LARGE_ROWS = 8_000;
    /** shutdownFast 立即返回的断言上界(ms):管道关闭的 IO 量级,与 close 的 join 60s 相差一个数量级以上。 */
    private static final long SHUTDOWN_FAST_BOUND_MILLIS = 5_000L;

    /** 每用例独立的管道目录:两个组装器顺序复用,wipe-on-open 清彼此残留。 */
    @TempDir
    Path dir;

    /** 测试用 RelationResolver 假实现(与 StreamedTransactionAssemblerTest 同款):wire + 最小 Debezium Table。 */
    private static final RelationResolver RESOLVER = (seq, wire) -> new ResolvedRelation(wire, tableOf(wire));

    /** 责任:按 wire Relation 造最小 Debezium Table——TableId 取 wire 的 schema/table(同名互证),列沿 wire 列序全 text。 */
    private static Table tableOf(PgOutputMessage.Relation wire) {
        var editor = Table.editor().tableId(new TableId(null, wire.schema(), wire.table()));
        for (var col : wire.columns()) {
            editor.addColumn(io.debezium.relational.Column.editor()
                    .name(col.name()).jdbcType(Types.VARCHAR).type("text").create());
        }
        return editor.create();
    }

    /**
     * 责任:生成一段多形态字节流(普通事务 + 两阶段 + 流式交错 + 子事务回滚,PgWire 构造,
     * 与 StreamedTransactionAssemblerTest 同风格;引擎同名夹具的 1:1 搬运)。
     * 关键步骤:Relation 预置 → 普通事务(B/I/C)→ 两阶段(b/I/P/K)→ 流式事务
     * (S/带前缀子事务单元/E/子事务回滚 A/c——被回滚子事务的单元在回放期被剔除,产出
     * 0 变更的 STREAMED 事务,事件流为 {@code Begin(expected=1) → End(0)},头尾元数据的
     * 全等因此覆盖 emitted&lt;expected 边界)。
     * 边界:LSN/时间戳按 PgWire 占位约定;'A' 为非 parallel 形态,驱动组装器须以
     * StreamingMode.ON 构造。纯函数,测试线程调用。
     */
    private static byte[][] mixedStream() {
        return new byte[][] {
                PgWire.relation(OID, "t", "id", "v"),
                PgWire.begin(101),
                PgWire.insert(OID, PgWire.tuple("1", "a")),
                PgWire.commit(),
                PgWire.beginPrepare(8001, "gid-1"),
                PgWire.insert(OID, PgWire.tuple("3", "c")),
                PgWire.prepare(8001, "gid-1"),
                PgWire.commitPrepared(8001, "gid-1"),
                PgWire.streamStart(7001, true),
                PgWire.streamed(7003, PgWire.insert(OID, PgWire.tuple("2", "b"))),
                PgWire.streamStop(),
                PgWire.streamAbort(7001, 7003),      // 子事务回滚:单元应在回放期被剔除
                PgWire.streamCommit(7001),
        };
    }

    /**
     * 责任:解耦等价本体——同一字节流依次驱动同步与异步两个组装器形态,close 后断言事件流
     * 全等、整块全等与前沿推进。
     * 关键步骤:① 同步形态(dispatch 直调 processBucket)收集事件流 + 收集器并行重组 →
     * ② 异步形态(public 九参构造器起 consumer 线程)同样双收集,close 走毒丸排干协议
     * (毒丸 → consumer 排干余桶 → join → 关管道——断言时输出确定)→ 逐级断言:完整事件流
     * 全等(Begin/End 头尾元数据进断言,比整块更严)、整块 Transaction 序列全等(双保险)、
     * 异步前沿 = 末个输出事务的 endLsn(End 返回后推进)。
     * 边界:异步 close 前队列里可能有未消费桶,join 保证断言时全部已输出(确定性);
     * 事件流收集列表(ArrayList)在 join 建立的 happens-before 之后于测试线程读取。
     * 线程约束:喂流与同步消费在测试线程;异步消费在 transaction-consumer 线程,
     * close 的 join 建立测试线程断言前的 happens-before。
     */
    @Test
    void asyncPipelineEqualsSynchronous() {
        byte[][] stream = mixedStream();
        // ① 同步形态:事件流直攒 + 收集器并行重组(同一 lambda 双写——事件流断言与整块断言共用驱动)
        List<TransactionEvent> syncEvents = new ArrayList<>();
        TransactionRecorder syncCollector = new TransactionRecorder();
        try (StreamedTransactionAssembler sync = new StreamedTransactionAssembler(
                dualCapture(syncEvents, syncCollector), StreamingMode.ON, new VersionedRelationRegistry(),
                RESOLVER, dir, LegacyRollCycles.MINUTELY)) {
            feed(sync, stream);
        }
        // ② 异步形态:真实双线程管道,同样双收集
        List<TransactionEvent> asyncEvents = new ArrayList<>();
        TransactionRecorder asyncCollector = new TransactionRecorder();
        AtomicLong frontier = new AtomicLong();
        try (StreamedTransactionAssembler async = new StreamedTransactionAssembler(dualCapture(asyncEvents, asyncCollector),
                StreamingMode.ON, new VersionedRelationRegistry(), RESOLVER, dir, LegacyRollCycles.MINUTELY,
                (msg, view) -> { }, frontier, () -> { })) {
            feed(async, stream);
        }   // close:毒丸 → consumer 排干余桶 → join → pipe 关闭——排干后输出确定
        // 断言一:完整事件流全等——Begin/End 头尾元数据进断言,比整块更严
        assertEquals(syncEvents, asyncEvents);
        // 断言二(双保险):整块 Transaction 序列全等——既有等价币经收集器存活
        List<Transaction> syncOut = syncCollector.transactions();
        List<Transaction> asyncOut = asyncCollector.transactions();
        assertEquals(syncOut, asyncOut);
        assertEquals(3, asyncOut.size(), "普通/两阶段/流式各一事务");
        // 断言三:异步前沿 = 末个输出事务的 endLsn(End 返回后推进)
        assertEquals(asyncOut.get(asyncOut.size() - 1).endLsn(), frontier.get());
        assertEquals(COMMIT_END_LSN, frontier.get());
    }

    /**
     * 责任:D7 快速停机验收本体——consumer 阻塞在前一桶的 Begin 回调、大桶已交接入队(未回放)
     * 时,shutdownFast 必须立即返回,且排队大桶不被排干(listener 未收其 Begin/任何 End、
     * 前沿钉 0)——与 close(排干协议,join 最长 60s)的语义分叉即本用例钉住的差异点。
     * 关键步骤:异步构造(驻留型 listener:首个 Begin 即 countDown 并泊住)→ 喂小事务并
     * await 确认 consumer 已进入回调 → 喂大桶事务(交接入队,consumer 阻塞中永不被取出)→
     * 计时调 shutdownFast → 断言耗时远小于 join 上界 + 三件套(Begin 恰 1/End 恒 0/前沿 0)。
     * 边界:泊留辅助 {@link #parkUntilReleased} 挺过 shutdownFast 的 interrupt(否则 consumer
     * 会在断言窗内继续走完回放,断言失确定性);finally 先放行再 close——close 的 join 等
     * consumer 终结(管道已关,余下回放走 fail-fast/截断路径),保证非守护线程在用例结束前
     * 退出、@TempDir 清理无句柄竞争。close 在 shutdownFast 之后调用是安全组合(毒丸重复入队
     * 无害、join 对已亡线程立即返回、管道 close 幂等)。
     * 线程约束:喂流与 shutdownFast 在测试线程(reader 角色);泊留在 consumer 线程;
     * latch 的 countDown→await 建立 consumer→测试线程的 happens-before,断言读数确定。
     */
    @Test
    void shutdownFastDoesNotWaitForPendingBuckets() throws Exception {
        CountDownLatch firstBegin = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger beginsSeen = new AtomicInteger();
        AtomicInteger endSeen = new AtomicInteger();
        AtomicLong frontier = new AtomicLong();
        StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(event -> {
            if (event instanceof TransactionEvent.Begin) {
                beginsSeen.incrementAndGet();
                firstBegin.countDown();
                parkUntilReleased(release);   // consumer 驻留在下游回调里——排队桶无从被取出
            } else if (event instanceof TransactionEvent.End) {
                endSeen.incrementAndGet();
            }
        }, StreamingMode.ON, new VersionedRelationRegistry(), RESOLVER, dir, LegacyRollCycles.MINUTELY,
                (msg, view) -> { }, frontier, () -> { });
        try {
            // 第一个小事务:consumer 取走即阻塞在 Begin 回调(此后交接的桶只能排队)
            assembler.onRaw(PgWire.relation(OID, "t", "id", "v"));
            assembler.onRaw(PgWire.begin(901));
            assembler.onRaw(PgWire.insert(OID, PgWire.tuple("1", "a")));
            assembler.onRaw(PgWire.commit());
            assertTrue(firstBegin.await(5, TimeUnit.SECONDS), "5s 内未进入 Begin 回调——异步交付未发生");
            // 大桶:交接入队、永不被取出(consumer 阻塞在前一桶的回调里)
            assembler.onRaw(PgWire.begin(902));
            for (int i = 1; i <= LARGE_ROWS; i++) {
                assembler.onRaw(PgWire.insert(OID, PgWire.tuple(Integer.toString(i), "x")));
            }
            assembler.onRaw(PgWire.commit());

            long startNanos = System.nanoTime();
            assembler.shutdownFast();   // 毒丸 + interrupt + 不 join + 关管道
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;

            assertTrue(elapsedMillis < SHUTDOWN_FAST_BOUND_MILLIS,
                    "shutdownFast 必须立即返回(实测 " + elapsedMillis + "ms)——close 的 join 要等 consumer 最长 60s");
            assertEquals(1, beginsSeen.get(), "排队大桶未被取出——其 Begin 不得送达");
            assertEquals(0, endSeen.get(), "listener 未收 End——桶未排干(D7 不等语义)");
            assertEquals(0L, frontier.get(), "End 未达——前沿不得推进");
        } finally {
            release.countDown();   // 先放行驻留的 consumer
            assembler.close();     // 再以 close 收尾:join 等 consumer 终结,非守护线程不外溢到用例之外
        }
    }

    /**
     * 责任:构造"事件流直攒 + 收集器重组"的双写 listener(同步/异步两侧共用同一捕获形态,
     * 保证两侧断言输入的同构性)。
     * 边界:events 列表只在 consumer(或同步调用)线程写、join 后测试线程读;collector 的
     * 流合法性校验(End 对账等)在写入路径内联生效,违约即抛 ISE 直传驱动方。
     */
    private static StreamingTransactionListener dualCapture(List<TransactionEvent> events, TransactionRecorder collector) {
        return event -> {
            events.add(event);
            collector.onEvent(event);
        };
    }

    /**
     * 责任:把一段录制字节流按序喂给组装器(两个形态共用的驱动步骤)。
     * 边界:纯遍历转发,onRaw 的全部 fail-fast 语义原样上抛(违约即用例失败)。
     * 线程:测试线程(异步形态的 reader 角色)。
     */
    private static void feed(StreamedTransactionAssembler assembler, byte[][] stream) {
        for (byte[] m : stream) {
            assembler.onRaw(m);
        }
    }

    /**
     * 责任:驻留辅助——把当前线程泊在 release 闭锁上直到放行。<b>中断不放行</b>:shutdownFast
     * 会对 consumer 线程发 interrupt(防其阻塞在 poll 的 1s 等待),而本用例要证明的恰是
     * "即使 consumer 被中断,排队桶也不被排干"——泊留必须挺过中断,否则断言窗内 consumer
     * 会继续走完回放,断言失确定性。
     * 关键步骤:循环 {@code await(50ms)},InterruptedException 只吞不退(不恢复中断标志——
     * 恢复会使下一次 await 立即抛出,退化成忙等烧 CPU);release countDown 后 await 返回
     * true 即返回。
     * 边界:50ms 步进兼防失控死挂(release 恒在 finally 放行,正常路径远早于超时返回);
     * 吞中断是<b>测试夹具的构造手段</b>(真实下游不应模仿),生产侧的中断语义以
     * {@code TransactionConsumer.run} 的 poll 路径为准。
     * 线程:consumer 线程(listener 回调内)。
     */
    private static void parkUntilReleased(CountDownLatch release) {
        while (true) {
            try {
                if (release.await(50, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                // 吞中断:shutdownFast 的 interrupt 不是放行信号(见方法 javadoc)
            }
        }
    }
}
