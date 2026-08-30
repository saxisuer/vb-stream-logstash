package org.vastdata.vbstream.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * 事务消费器（1.7 设计 §4.4，2.0 流式输出形态）：从交接队列取冻结桶，回放逐条解码逐条以
 * {@link TransactionEvent} 流式回调输出（Begin 头 → 逐 TxChange → End 尾，堆内 O(单条)），
 * 上报 LSN 前沿。它从组装器抽出单独成类，是为了既有单测能以"同线程消费"驱动（直接调
 * {@link #processBucket}，锚定既有期望）与真实线程形态共用同一段处理逻辑。
 *
 * <p>循环协议：{@link #queue}.poll(1s)——null（暂时无交接）做周期统计后继续；取到
 * {@link TxBuffer#POISON} 退出；否则 processBucket。失败语义：处理中抛出的任何 Throwable 记
 * ERROR、触发 onFailure、退出循环**不排干**（fail-fast，与 1.6"异常上抛终止会话"等价）；
 * 捕捉 Throwable 防 consumer 静默死亡导致 reader 无限追加。
 *
 * <p>线程约束：run() 由 consumer 线程执行；processBucket 的触碰面 = 冻结桶 + pipe.readRange +
 * listener 回调 + 前沿累加 + 桶状态字段——全部在 consumer 线程或并发安全结构上。
 */
final class TransactionConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionConsumer.class);
    /** 统计/告警周期与最老滞留告警阈值（1.7 设计 §7；常量不做配置面）。 */
    private static final long STATS_INTERVAL_NANOS = 10_000_000_000L;
    private static final long STALE_WARN_NANOS = 60_000_000_000L;

    private final StreamingTransactionListener listener;
    private final BucketReplayer replayer;
    private final MessagePipe pipe;
    private final BlockingQueue<TxBuffer> queue;
    private final AtomicLong outputFrontier;
    private final AtomicInteger liveBucketCount;
    private final Runnable onFailure;

    /**
     * 构造消费器（不启动线程——线程的创建与启动归组装器，同步形态则永远无人调 run）。
     *
     * @param listener        完整事务回调（消费线程同步调用）
     * @param mode            流式模式（回放器自持 decoder 用，须与录制流一致）
     * @param pipe            单元字节所在管道（readRange 属 consumer 侧方法，跨线程分工见其 javadoc）
     * @param queue           交接队列（组装器 handoff 投入、本类 poll 取出；FIFO 保证交接序）
     * @param outputFrontier  输出前沿（已输出事务 endLsn 的单调 max，反馈封顶用；并发安全）
     * @param liveBucketCount reader 侧存活桶计数（统计展示用；并发安全）
     * @param onFailure       回放失败时的逃生回调（fail-fast 路径，如通知会话停机）
     * @param replayObserver 每个回放解码点回调（第二参为该桶的 RelationSnapshot 渲染视图）
     */
    TransactionConsumer(StreamingTransactionListener listener, StreamingMode mode, MessagePipe pipe,
                        BlockingQueue<TxBuffer> queue, AtomicLong outputFrontier,
                        AtomicInteger liveBucketCount, Runnable onFailure,
                        BiConsumer<PgOutputMessage, RelationLookup> replayObserver) {
        this.listener = Objects.requireNonNull(listener, "listener");
        this.pipe = Objects.requireNonNull(pipe, "pipe");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.outputFrontier = Objects.requireNonNull(outputFrontier, "outputFrontier");
        this.liveBucketCount = Objects.requireNonNull(liveBucketCount, "liveBucketCount");
        this.onFailure = Objects.requireNonNull(onFailure, "onFailure");
        this.replayer = new BucketReplayer(mode, replayObserver);
    }

    /**
     * 责任：消费循环（consumer 线程入口）。关键步骤：poll(1s) 取桶——null 走空闲采样（周期统计
     * 后继续，1s 即统计粒度的采样上限）；取到毒丸即退出（close 排干协议的退出信号，其后的入队
     * 元素不存在——poison 由 close 在排干位投入）；取到真实桶则以"即将处理"采样后 processBucket。
     * 边界：poll 被中断时恢复中断标志并退出（防御路径——正常停机走毒丸，不走中断）；
     * processBucket 抛出的任何 Throwable 记 ERROR、触发 onFailure 后退出循环且不排干
     * （fail-fast，与 1.6"异常上抛终止会话"等价；捕捉 Throwable 是防 consumer 静默死亡、
     * reader 无限追加）。线程：仅 consumer 线程执行（同步形态不调用本方法）。
     */
    @Override
    public void run() {
        long lastStats = System.nanoTime();
        while (true) {
            TxBuffer bucket;
            try {
                bucket = queue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("consumer 被中断，退出（排干未完成——由 close 协议负责，此路径仅防御）");
                return;
            }
            if (bucket == null) {
                lastStats = maybeStats(lastStats, false);   // 空闲采样窗：未在处理任何桶
                continue;
            }
            if (bucket == TxBuffer.POISON) {
                return;
            }
            lastStats = maybeStats(lastStats, true);        // 取到桶：本采样窗即将进入处理
            try {
                processBucket(bucket);
            } catch (Throwable t) {
                // 截断细节（已输出条数/firstIndex）已在 processBucket 的 ERROR 里，此处只留 xid 与退出语义
                LOG.error("事务输出失败，consumer 终止（fail-fast，队列不排干）: xid={}", bucket.xid, t);
                onFailure.run();
                return;
            }
        }
    }

    /**
     * 责任：处理一个冻结桶（同步/异步共用），以三段事件流式交付。关键步骤：state=OUTPUTTING →
     * 发 Begin 头（expectedChanges 取桶记账 unitCount，aborted 过滤前）→ 逐单元回放（快照渲染
     * 视图）每条 TxChange 即时回调（不再构造整事务列表——堆内 O(单条)）→ 发 End 尾
     * （emitted 为过滤后实付数）→ 前沿以 endLsn 单调累加 → state=DONE。
     * 边界：空桶产出 Begin → End(0)（空事务合法）；回放异常在 End 发出前原样上抛并记
     * ERROR（带已输出/预期条数——fail-fast 截断留痕），异步由 run 捕获、同步直传调用方
     * （既有用例的 fail-fast 断言路径）；End 返回后前沿才推进（End 返回 = 下游确认完整消费）。
     * 线程：consumer 线程或同步测试线程。
     */
    void processBucket(TxBuffer bucket) {
        bucket.state = BucketState.OUTPUTTING;
        listener.onEvent(new TransactionEvent.Begin(bucket.xid, bucket.kind, bucket.gid,
                bucket.commitLsn, bucket.endLsn, bucket.commitTimestamp, bucket.unitCount));
        long[] emitted = {0L};   // 数组而非 long：lambda 递增 + 异常路径计数存活（fail-fast 截断日志用）
        try {
            replayer.replay(bucket, pipe, change -> {
                listener.onEvent(change);   // 先交付后计数：listener 自身抛出时该条不计入"已输出"（多报 1 修复）
                emitted[0]++;
            });
        } catch (Throwable t) {
            LOG.error("事务流式输出中断（已输出 {}/{} 条）: xid={} firstIndex={}",
                    emitted[0], bucket.unitCount, bucket.xid, bucket.firstIndex, t);
            throw t;
        }
        listener.onEvent(new TransactionEvent.End(bucket.xid, emitted[0]));
        outputFrontier.accumulateAndGet(bucket.endLsn, Math::max);
        bucket.state = BucketState.DONE;
    }

    /**
     * 责任：周期统计（每 10s 一条 INFO）：LIVE 桶数（reader 侧计数）+ HANDED_OFF 桶数（队列
     * 深度，FIFO 内全部待回放）+ OUTPUTTING（本单 consumer 是否正在处理）+ 输出前沿；最老交接
     * 桶（队头 peek，FIFO 即最老）滞留超 60s 升 WARN（consumer 或下游回调阻塞的信号）。
     * 关键步骤：距上次统计不足周期即原样返回旧戳；到点则取队列快照维度打 INFO，再对队头
     * handoffNanos 做滞留判定。边界：队列为空时无滞留告警；返回本次统计时刻作下一轮基准。
     * 线程：consumer 线程（queue 的 size/peek 并发安全，读数弱一致即可接受——统计非精确记账）。
     *
     * @param lastStats   上次统计的 nanoTime 戳
     * @param outputting  本采样窗的忙碌位（取到桶即将处理 = true；空闲轮询 = false——单 consumer 的 0/1 表达）
     * @return 本次统计时刻（未到周期则原样返回入参）
     */
    private long maybeStats(long lastStats, boolean outputting) {
        long now = System.nanoTime();
        if (now - lastStats < STATS_INTERVAL_NANOS) {
            return lastStats;
        }
        LOG.info("consumer 统计: LIVE={} HANDED_OFF={} OUTPUTTING={} frontierLsn=0x{}",
                liveBucketCount.get(), queue.size(), outputting ? 1 : 0,
                Long.toHexString(outputFrontier.get()));
        TxBuffer oldest = queue.peek();
        if (oldest != null && now - oldest.handoffNanos > STALE_WARN_NANOS) {
            LOG.warn("最老交接桶滞留 {}ms 超过 {}s 阈值: xid={} firstIndex={}（consumer 或下游回调阻塞）",
                    (now - oldest.handoffNanos) / 1_000_000L, STALE_WARN_NANOS / 1_000_000_000L,
                    oldest.xid, oldest.firstIndex);
        }
        return now;
    }
}
