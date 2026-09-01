package org.vastdata.debezium.connector.postgresql.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * 事务消费器:从交接队列取冻结的桶,以三段事件流式交付({@code Begin} 头 → 逐
 * {@link TxChange} → {@code End} 尾,堆内 O(单条)),随后以 endLsn 单调累加输出前沿。
 * 引擎 {@code org.vastdata.vbstream.replication.TransactionConsumer}(184 行)的 1:1 重写
 * (文字参照,非依赖)——从组装器抽出单独成类,是为了同步测试形态
 * ({@code dispatchHandedOff} 直调 {@code processBucket})与真实 consumer 线程形态
 * (组装器异步构造起线程驱动 {@link #run()})共用同一段处理逻辑。
 *
 * <p><b>循环协议(run,Task 6 落地)</b>:交接队列 {@code poll(1s)}——null(暂无交接)走
 * 滞留检查后继续;取到 {@link TxBuffer#POISON} 退出;否则 processBucket。失败语义:处理中
 * 抛出的任何 Throwable 记 ERROR、触发 onFailure、退出循环<b>不排干</b>(fail-fast,与
 * 引擎"异常上抛终止会话"等价);捕捉 Throwable 防 consumer 静默死亡导致 reader 无限追加。
 * poll 被中断时恢复中断标志退出——正常停机走毒丸({@code close} 排干协议),中断路径只有
 * {@code shutdownFast}(D7 快速停机)与防御性外部中断会走。
 *
 * <p><b>与引擎的偏差</b>:引擎挂在 10s 统计 tick 上的周期统计行(consumer 统计 INFO +
 * {@code ThroughputMetrics} 三行)随 MS2 接缝整体删除(MS5 以监听器形态加回),<b>不复活</b>;
 * 滞留告警(队头桶交接距今超 60s)保留,检查节流沿用引擎统计 tick 的 10s 周期(告警风暴
 * 防护,与引擎告警节奏一致)。liveBucketCount(引擎统计行展示用)随之不收——它在引擎侧
 * 的唯一消费点就是被删的那行。
 *
 * <p>红线(与引擎逐条一致):<b>End 发出之后</b>才 {@code outputFrontier.accumulateAndGet(
 * endLsn, max)}——End 返回 = 下游确认完整消费,顺序不可倒;<b>先交付后计数</b>——listener
 * 自身抛出时该条不计入已输出;listener 抛出 → End 永不发 → 前沿不推进(异常原样上抛,
 * run 捕获走 onFailure、同步形态直传调用方)。
 *
 * <p>线程约束:run() 由 consumer 线程执行(同步形态无人调用);processBucket 的触碰面 =
 * 冻结桶 + pipe.readRange + listener 回调 + {@link BucketTableResolver} 绑定 + 前沿累加 +
 * 桶状态字段——全部在 consumer 线程或并发安全结构上(同步测试形态即调用方线程)。
 */
final class TransactionConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionConsumer.class);

    /** 滞留告警的检查节流周期(引擎统计 tick 同款节奏,统计行本身已删——见类 javadoc)。 */
    private static final long STALE_CHECK_NANOS = 10_000_000_000L;
    /** 队头交接桶的最老滞留告警阈值(60s)。 */
    private static final long STALE_WARN_NANOS = 60_000_000_000L;

    private final StreamingTransactionListener listener;
    private final BucketReplayer replayer;
    private final MessagePipe pipe;
    /** 交接队列(组装器 handoff 投入、run() poll 取出;FIFO 保证交接序即提交序)。 */
    private final BlockingQueue<TxBuffer> queue;
    /** 输出前沿(已输出事务 endLsn 的单调 max,反馈封顶用;并发安全——reader 线程每轮读)。 */
    private final AtomicLong outputFrontier;
    /** 回放失败的逃生回调(fail-fast 路径,如通知停机;consumer 线程调用,不得再抛)。 */
    private final Runnable onFailure;
    /** listener 侧 asOf 表解析接缝:每个桶回放前绑定其快照(见 {@link BucketTableResolver} javadoc)。 */
    private final BucketTableResolver tableResolver;

    /**
     * 构造消费器(不启动线程——线程的创建与启动归组装器的异步构造,同步形态则永远无人调 run)。
     *
     * @param listener        事务事件流回调(Begin/TxChange/End 按序流式交付,consumer 线程同步调用)
     * @param mode            流式模式(回放器自持 decoder 用,须与录制流一致)
     * @param pipe            单元字节所在管道(readRange 属 consumer 侧方法,跨线程分工见其 javadoc)
     * @param queue           交接队列(组装器 handoff 投入、本类 poll 取出;FIFO 保证交接序)
     * @param outputFrontier  输出前沿(已输出事务 endLsn 的单调 max,反馈封顶用;并发安全)
     * @param onFailure       回放失败时的逃生回调(fail-fast 路径,如通知停机;consumer 线程调用)
     * @param replayObserver  每个回放解码点回调(第二参为该桶的 RelationSnapshot 渲染视图)
     * @param tableResolver   listener 侧按 (oid, seq) 解析 asOf 表定义的接缝(每个桶回放前绑定快照)
     */
    TransactionConsumer(StreamingTransactionListener listener, StreamingMode mode, MessagePipe pipe,
                        BlockingQueue<TxBuffer> queue, AtomicLong outputFrontier, Runnable onFailure,
                        BiConsumer<PgOutputMessage, RelationLookup> replayObserver,
                        BucketTableResolver tableResolver) {
        this.listener = Objects.requireNonNull(listener, "listener");
        this.pipe = Objects.requireNonNull(pipe, "pipe");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.outputFrontier = Objects.requireNonNull(outputFrontier, "outputFrontier");
        this.onFailure = Objects.requireNonNull(onFailure, "onFailure");
        this.tableResolver = Objects.requireNonNull(tableResolver, "tableResolver");
        this.replayer = new BucketReplayer(mode, replayObserver);
    }

    /**
     * 责任:消费循环(consumer 线程入口,组装器异步构造起线程后立即进入)。关键步骤:
     * {@code poll(1s)} 取桶——null(暂无交接)做滞留检查后继续(1s 即检查节奏的采样上限);
     * 取到毒丸即退出(close 排干协议的退出信号——毒丸由 close/shutdownFast 在队列尾投入,
     * FIFO 保证其前的全部交接桶先被见到);取到真实桶则先做滞留检查再 processBucket。
     * 边界:poll 被中断时恢复中断标志并退出——正常停机走毒丸,此路径只有 shutdownFast
     * (D7 快速停机,不等排干)与防御性外部中断会走,排干语义由 close 协议另行承担;
     * processBucket 抛出的任何 Throwable 记 ERROR、触发 onFailure 后退出循环且不排干
     * (fail-fast,与引擎"异常上抛终止会话"等价;捕捉 Throwable 是防 consumer 静默死亡、
     * reader 无限追加)。线程:仅 consumer 线程执行(同步测试形态不调用本方法)。
     */
    @Override
    public void run() {
        long lastStaleCheck = System.nanoTime();
        while (true) {
            TxBuffer bucket;
            try {
                bucket = queue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("consumer 被中断,退出(排干未完成——正常排干由 close 协议负责;shutdownFast 快速停机即走此路径)");
                return;
            }
            if (bucket == null) {
                lastStaleCheck = maybeStaleWarn(lastStaleCheck);   // 空闲轮:滞留检查后继续
                continue;
            }
            if (bucket == TxBuffer.POISON) {
                return;
            }
            lastStaleCheck = maybeStaleWarn(lastStaleCheck);
            try {
                processBucket(bucket);
            } catch (Throwable t) {
                // 截断细节(已输出条数/firstIndex)已在 processBucket 的 ERROR 里,此处只留 xid 与退出语义
                LOG.error("事务输出失败,consumer 终止(fail-fast,队列不排干): xid={}", bucket.xid, t);
                onFailure.run();
                return;
            }
        }
    }

    /**
     * 责任:处理一个冻结桶(同步/异步共用),以三段事件流式交付。关键步骤:绑定 listener 侧
     * 表解析接缝(connector 偏差——事件不携带快照,Begin 前 bind 使 listener 在任意事件回调内
     * 都能按 (oid, seq) 取到本桶快照)→ state=OUTPUTTING → 发 Begin 头(expectedChanges 取桶
     * 记账 unitCount,aborted 过滤前)→ 逐单元回放(快照渲染视图)每条 TxChange 即时回调
     * (不构造整事务列表——堆内 O(单条))→ 发 End 尾(emitted 为过滤后实付数)→ <b>End 之后</b>
     * 前沿以 endLsn 单调累加(End 返回 = 下游确认完整消费,顺序不可倒)→ state=DONE。
     * 边界:空桶产出 Begin → End(0)(空事务合法);回放异常在 End 发出前原样上抛并记 ERROR
     * (带已输出/预期条数与 firstIndex——fail-fast 截断留痕,<b>End 永不发、前沿不推进、
     * state 停在 OUTPUTTING</b>);listener 自身抛出按先交付后计数不计入已输出。
     * 线程:consumer 线程或同步测试线程。
     */
    void processBucket(TxBuffer bucket) {
        tableResolver.bind(bucket.relationSnapshot);
        bucket.state = BucketState.OUTPUTTING;
        listener.onEvent(new TransactionEvent.Begin(bucket.xid, bucket.kind, bucket.gid,
                bucket.commitLsn, bucket.endLsn, bucket.commitTimestamp, bucket.unitCount));
        long[] emitted = {0L};   // 数组而非 long:lambda 递增 + 异常路径计数存活(fail-fast 截断日志用)
        try {
            replayer.replay(bucket, pipe, change -> {
                listener.onEvent(change);   // 先交付后计数:listener 自身抛出时该条不计入"已输出"(多报 1 修复)
                emitted[0]++;
            });
        } catch (Throwable t) {
            LOG.error("事务流式输出中断(已输出 {}/{} 条): xid={} firstIndex={}",
                    emitted[0], bucket.unitCount, bucket.xid, bucket.firstIndex, t);
            throw t;
        }
        listener.onEvent(new TransactionEvent.End(bucket.xid, emitted[0]));
        outputFrontier.accumulateAndGet(bucket.endLsn, Math::max);
        bucket.state = BucketState.DONE;
    }

    /**
     * 责任:滞留告警的节流检查(引擎滞留告警的保留半程):距上次检查不足周期即原样返回旧戳;
     * 到点则窥队头(FIFO 即最老交接桶),其 handoffNanos 距今超过 60s 阈值升 WARN——consumer
     * 或下游回调阻塞的信号(队列在积压而消费停滞)。
     * 边界:队列为空时无滞留告警;peek 是并发安全操作,读数弱一致即可接受(告警非精确记账)。
     * 线程:consumer 线程。
     *
     * @param lastCheck 上次检查的 nanoTime 戳
     * @return 本次检查时刻(未到周期则原样返回入参)
     */
    private long maybeStaleWarn(long lastCheck) {
        long now = System.nanoTime();
        if (now - lastCheck < STALE_CHECK_NANOS) {
            return lastCheck;
        }
        TxBuffer oldest = queue.peek();
        if (oldest != null && now - oldest.handoffNanos > STALE_WARN_NANOS) {
            LOG.warn("最老交接桶滞留 {}ms 超过 {}s 阈值: xid={} firstIndex={}(consumer 或下游回调阻塞)",
                    (now - oldest.handoffNanos) / 1_000_000L, STALE_WARN_NANOS / 1_000_000_000L,
                    oldest.xid, oldest.firstIndex);
        }
        return now;
    }
}
