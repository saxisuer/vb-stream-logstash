package org.vastdata.debezium.connector.postgresql.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * 事务消费器:处理一个交接冻结的桶,以三段事件流式交付({@code Begin} 头 → 逐
 * {@link TxChange} → {@code End} 尾,堆内 O(单条)),随后以 endLsn 单调累加输出前沿。
 * 引擎 {@code org.vastdata.vbstream.replication.TransactionConsumer}(184 行)的
 * {@code processBucket} 半程 1:1 重写(文字参照,非依赖)——从组装器抽出单独成类,是为了
 * 同步测试形态({@code dispatchHandedOff} 直调本类)与 Task 6 的 consumer 线程形态共用同一段
 * 处理逻辑。
 *
 * <p><b>Task 5 形态裁定</b>:本任务只落 {@link #processBucket} 与其依赖——构造可建对象但不
 * start 线程;消费循环(交接队列 poll + 周期统计/滞留告警 + 毒丸退出)与 onFailure 逃生路径
 * 归 Task 6 的 {@code run()}(届时构造另收交接队列/liveBucketCount/onFailure 三参)。
 * 引擎对位的吞吐指标(metrics 穿参与 onTxOutput 回填)随 MS2 接缝整体删除,MS5 以监听器
 * 形态加回。
 *
 * <p>红线(与引擎逐条一致):<b>End 发出之后</b>才 {@code outputFrontier.accumulateAndGet(
 * endLsn, max)}——End 返回 = 下游确认完整消费,顺序不可倒;<b>先交付后计数</b>——listener
 * 自身抛出时该条不计入已输出;listener 抛出 → End 永不发 → 前沿不推进(异常原样上抛,
 * 异步形态由 run 捕获、同步形态直传调用方)。
 *
 * <p>线程约束:processBucket 的触碰面 = 冻结桶 + pipe.readRange + listener 回调 +
 * {@link BucketTableResolver} 绑定 + 前沿累加 + 桶状态字段——全部在 consumer 线程或并发安全
 * 结构上(Task 5 同步形态即调用方线程)。
 */
final class TransactionConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionConsumer.class);

    private final StreamingTransactionListener listener;
    private final BucketReplayer replayer;
    private final MessagePipe pipe;
    /** 输出前沿(已输出事务 endLsn 的单调 max,反馈封顶用;并发安全——reader 线程每轮读)。 */
    private final AtomicLong outputFrontier;
    /** listener 侧 asOf 表解析接缝:每个桶回放前绑定其快照(见 {@link BucketTableResolver} javadoc)。 */
    private final BucketTableResolver tableResolver;

    /**
     * 构造消费器(不启动线程——Task 5 只有同步形态,Task 6 的 run 循环与 consumer 线程另接)。
     *
     * @param listener        事务事件流回调(Begin/TxChange/End 按序流式交付,consumer 线程同步调用)
     * @param mode            流式模式(回放器自持 decoder 用,须与录制流一致)
     * @param pipe            单元字节所在管道(readRange 属 consumer 侧方法,跨线程分工见其 javadoc)
     * @param outputFrontier  输出前沿(已输出事务 endLsn 的单调 max,反馈封顶用;并发安全)
     * @param replayObserver  每个回放解码点回调(第二参为该桶的 RelationSnapshot 渲染视图)
     * @param tableResolver   listener 侧按 (oid, seq) 解析 asOf 表定义的接缝(每个桶回放前绑定快照)
     */
    TransactionConsumer(StreamingTransactionListener listener, StreamingMode mode, MessagePipe pipe,
                        AtomicLong outputFrontier,
                        BiConsumer<PgOutputMessage, RelationLookup> replayObserver,
                        BucketTableResolver tableResolver) {
        this.listener = Objects.requireNonNull(listener, "listener");
        this.pipe = Objects.requireNonNull(pipe, "pipe");
        this.outputFrontier = Objects.requireNonNull(outputFrontier, "outputFrontier");
        this.tableResolver = Objects.requireNonNull(tableResolver, "tableResolver");
        this.replayer = new BucketReplayer(mode, replayObserver);
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
}
