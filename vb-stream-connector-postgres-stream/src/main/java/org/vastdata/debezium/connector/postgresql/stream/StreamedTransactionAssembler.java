package org.vastdata.debezium.connector.postgresql.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputStreamDecoder;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import net.openhft.chronicle.queue.RollCycle;

/**
 * pgoutput 事务组装状态机:消费原始字节,把同一事务的变更以<b>纯 CQ index 段记账</b>攒进
 * 桶里,收到提交信号后<b>交接冻结桶</b>(快照随行)给消费侧回放成 {@link TransactionEvent}
 * 事件流逐条交给监听器。引擎 {@code org.vastdata.vbstream.replication.TransactionAssembler}
 * (765 行)同步路径的 1:1 重写(文字参照,非依赖)。
 *
 * <p>与消息驱动形态的核心区别:<b>数据消息先不解码</b>。Insert/Update/Delete/Truncate/Message
 * 五类消息的原始字节只在 {@link #onRaw} 首行追加一次管道({@link MessagePipe}),桶里只记
 * CQ index 的连续段(堆占用 = 段数 × long[2],不随单元数增长);控制消息和 Relation 才当场
 * 解码,驱动桶状态流转。解码推迟到提交那一刻——回滚的大事务从未被解码过,提交的事务也只解一次。
 *
 * <p>seq ≡ CQ index:<b>每条消息(含控制消息与 'R')先 append 取 index 作 seq,再做记账路由</b>
 * ——数据单元与 'R' 版本天然同序。Relation 以到达时的 seq 经 {@link RelationResolver} 解析成
 * {@link ResolvedRelation} 记入 {@link VersionedRelationRegistry} 版本日志(接缝偏差:引擎直接
 * 记 wire Relation,connector 的版本载荷是 wire + Debezium Table 双形态,Table 构建策略注入);
 * 交接时按桶的 oidSet 圈定、截止 lastIndex 拷出 {@link RelationSnapshot} 随桶冻结,回放按单元
 * 自己的 seq 取"变更那一刻"的表定义——事务中途若有并发 DDL,前后段的行仍按各自的表结构解释。
 * 版本日志在每次桶完结点按存活桶的最老 index 剪枝(2PC 挂起桶算存活;<b>不含</b>已交接桶——
 * 快照自足),防止长期膨胀。
 *
 * <p>桶模型(语义与引擎逐条等价):
 * <ul>
 *   <li>普通事务:单指针 {@code currentNormalTx}。Commit 消息不带 xid,且 walsender 按 LSN 序
 *       串行输出 Begin..Commit——同一时刻至多一个活动普通事务</li>
 *   <li>流式事务:{@code streamedByXid} 多桶并存(按 StreamStart 的顶层 xid 索引),
 *       {@code currentStream} 指向当前流块所属的桶。多个并发大事务的流段会交错,但流块本身不嵌套</li>
 *   <li>两阶段:活动期单指针 {@code currentPrepareTx},Prepare/StreamPrepare 之后转入
 *       {@code preparedByGid} 挂起池,等 CommitPrepared(输出)或 RollbackPrepared(丢弃)</li>
 *   <li>StreamAbort:整事务回滚(top==sub)直接丢桶;子事务回滚只把 subxid 记入
 *       {@code abortedSubxids},等回放时跳过对应单元——与旧版即时剔除的可观察行为等价</li>
 * </ul>
 *
 * <p>低水位(两个作用域,勿混):CQ 删除低水位 {@link #pipeWatermark()} = min(存活桶 firstIndex,
 * 非 DONE 交接桶 firstIndex, maxAppendedIndex+1),交 {@link MessagePipe#releaseBelow} 删除过老
 * 的滚动文件;registry 剪枝低水位 = 全部存活桶 firstIndex 的最小值,驱动
 * {@link VersionedRelationRegistry#pruneBelow}。两者都挂在桶完结点(交接/整桶丢弃)。
 *
 * <p><b>MS2 形态裁定</b>(Task 6 收口):构造分两形态——<b>同步</b>(既有单测的驱动形态:
 * dispatchHandedOff 在调用线程直调 {@code TransactionConsumer.processBucket},回放与回调对
 * onRaw 同步可见、fail-fast 异常直传调用方)与<b>异步</b>(连接器生产形态:构造即起非守护
 * {@code transaction-consumer} 线程消费交接队列,reader 记账与 consumer 回放双线程解耦,
 * 前沿与 onFailure 由调用方穿入)。停机两形态:{@link #close()}(毒丸排干——join 60s,
 * 已提交未输出的事务不丢,测试确定性断言用)与 {@link #shutdownFast()}(D7 快速停机——
 * 毒丸 + interrupt + 不 join,连接器 source 停机序用)。相对引擎的另三处接缝偏差:吞吐指标
 * MS2 接缝期曾整体删除,MS5 Task 3 以<b>构造参数传导</b>形态加回(指标实例由装配点 source
 * 的 execute 建后穿入本组装器,consumer/replayer 经构造链转传,四点插桩口径与引擎逐字节
 * 同构;便捷构造自建默认实例);'R' 路由经
 * {@link RelationResolver}(见上);listener 侧 asOf 表解析经 {@link BucketTableResolver}
 * (消费器每桶绑定快照,Task 7 注入真实现)。
 *
 * <p>内存量级:组装期记账恒 O(桶元数据)(段数 × long[2] + oid/aborted 集合);提交回放逐单元
 * 解码逐条交付(Task 5 起,堆内 O(单条))。
 *
 * <p>线程约束:<b>reader 侧</b>(onRaw 及其全部私有路由/记账/registry/低水位维护)设计为由
 * run 循环的单一线程调用(decoder 的流块状态与全部桶指针都要求单写者);consumer 侧
 * (readRange、冻结桶回放、listener 回调、前沿累加、桶状态后两态)由 {@link TransactionConsumer}
 * 在 consumer 线程执行(同步形态即调用线程)。跨线程共享面:交接队列(并发安全)、
 * {@code TxBuffer.state}(volatile)与 outputFrontier(AtomicLong)。
 */
public final class StreamedTransactionAssembler implements RawMessageListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamedTransactionAssembler.class);

    /** live 解码器(只用于控制消息与 'R'):维护流块状态 inStream,与 currentStream 指针同步变化。 */
    private final PgOutputStreamDecoder decoder;
    /** Relation 版本日志:'R' 到达即经 resolver 解析后记入(带 seq),交接时按桶 oidSet 拷出快照随行。 */
    private final VersionedRelationRegistry registry;
    /** 'R' → ResolvedRelation 的解析接缝(测试假实现 / Task 7 的 RelationTableFactory 真实现)。 */
    private final RelationResolver relationResolver;
    /** 主缓冲管道(构造时急切建立——管道是地基,构造即 wipe 目录):每条消息 append 一次,回放按段读回。 */
    private final MessagePipe pipe;
    /** 每个解码点(控制消息、'R'、回放单元)回调一次;第二参是渲染视图({@link RelationLookup}):
     *  live 解码点传 registry(最新版),回放点传桶快照(Task 5 起)。 */
    private final BiConsumer<PgOutputMessage, RelationLookup> decodedObserver;
    /** 已交接桶的 reader 侧记账(冻结后引用仍要保住 CQ 删除低水位,DONE 后由完结点惰性清理)。 */
    private final ArrayDeque<TxBuffer> handedOff = new ArrayDeque<>();
    /** reader 侧存活桶计数(LIVE 状态桶数,交接/整桶丢弃时递减)——consumer 周期统计与测试锚定用。 */
    private final AtomicInteger liveCount = new AtomicInteger();
    /** 输出前沿(已输出事务 endLsn 的单调 max,反馈封顶用):由 {@link #consumer} 在 End 后累加;
     *  同步形态组装器内部自持(调用方经测试观测口 {@link #outputFrontierForTest()} 读),
     *  异步形态由调用方穿入同一实例(session 反馈封顶读它)。 */
    private final AtomicLong outputFrontier;
    /** 消费器:交接桶的回放输出半程(冻结桶 + readRange + 回调 + 前沿),同步/异步两形态共用。
     *  listener、交接队列、前沿与 onFailure 在构造时交它持有(组装器自身不触碰回调与前沿)。 */
    private final TransactionConsumer consumer;
    /** consumer 线程(异步形态非 null,名 {@code transaction-consumer} 非守护——未排干的交接桶
     *  在 JVM 退出前必须有机会输出);同步形态为 null——dispatch 直调 processBucket。 */
    private final Thread consumerThread;
    /** 吞吐与分布指标(MS5):本组装器记 slot 读取/组装两计数(onRaw 入口与交接点),同一实例
     *  经构造链传导给 consumer(输出计数 + 两分布 + 10s 报告 tick)与 replayer(回放字节)
     *  ——四点共用,与引擎"组装器自建、全链单实例"同构;连接器侧改为装配点(source 的
     *  execute)创建后穿入,便捷构造自建默认实例。 */
    private final StreamThroughputMetrics throughputMetrics;
    /** 交接队列(reader 投入 → consumer 取出,异步形态):无界 LinkedBlockingQueue,
     *  FIFO 保证交接序即提交序;同步形态恒空(dispatch 直调 processBucket 不经队列)。 */
    private final BlockingQueue<TxBuffer> handoffQueue = new LinkedBlockingQueue<>();

    /** 最近一次 append 的 index(reader 记账,替代 pipe.lastAppendedIndex()——空队列时后者会抛;
     *  未 append 过为 -1)。watermark 的"已落盘内容全是垃圾"上界由此 +1 派生。 */
    private long maxAppendedIndex = -1L;
    /** 最近一次全局 append 的归属桶(null = 上一次 append 是控制消息/被丢弃消息,不属任何桶)。
     *  连续段判定依据:同桶相邻 append 顺延当前段,他人(或控制消息)插队就新开一段。 */
    private TxBuffer lastAppendOwner;

    /** 活动普通事务桶(Begin 置位,Commit 封箱清空;协议保证 Begin..Commit 串行不嵌套)。 */
    private TxBuffer currentNormalTx;
    /** 活动两阶段事务桶(BeginPrepare 置位,Prepare 转挂起池)。 */
    private TxBuffer currentPrepareTx;
    /** 当前流块上下文:stream_start..stream_stop 之间非 null,指向 streamedByXid 中的某个桶。 */
    private TxBuffer currentStream;
    /** 流式事务桶,按顶层 xid 索引(多桶并存,段间交错)。 */
    private final Map<Long, TxBuffer> streamedByXid = new HashMap<>();
    /** 两阶段挂起池,按 gid 索引(PREPARE 到 COMMIT/ROLLBACK PREPARED 之间,可能长期挂起)。 */
    private final Map<String, TxBuffer> preparedByGid = new HashMap<>();

    /**
     * 构造<b>异步形态</b>组装器(连接器生产形态,引擎 Main 用途的对位):建管道与消费器之外,
     * 另起 {@code transaction-consumer} 线程(非守护——未排干的交接桶在 JVM 退出前必须有机会
     * 输出)立即开始消费交接队列;输出前沿与失败逃生回调由调用方穿入(前沿供 session 的
     * LSN 反馈封顶读,onFailure 供停机联动)。停机走 {@link #close()}(毒丸排干)或
     * {@link #shutdownFast()}(D7 快速停机),两者均不可与 onRaw 并发调用。
     * 急切建立管道——{@link MessagePipe} 构造即清空目录建队列,失败原样上抛 fail-fast:
     * 管道是地基,建不起来就没有可运行形态(此时 consumer 线程尚未创建,无泄漏)。
     *
     * @param listener         事务事件流回调(Begin/TxChange/End 按序流式交付,consumer 线程同步调用)
     * @param mode             流式模式(仅影响 decoder 对 StreamAbort 附加字段的解析,须与
     *                         START_REPLICATION 的 streaming 参数一致,否则 abort 解析错位 fail-fast)
     * @param registry         Relation 版本日志('R' 路由与交接快照共用,本组装器独占写入)
     * @param relationResolver 'R' → ResolvedRelation 解析接缝(测试假实现 / Task 7 真实现)
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期(决定低水位删除的档位粒度)
     * @param decodedObserver  每个解码点回调(控制消息 + 'R' + 回放单元;Y/O 不解码不回调)
     * @param outputFrontier   输出前沿载体(调用方持有以便反馈封顶;本实例只做单调 max 累加)
     * @param onFailure        consumer 回放失败的逃生回调(consumer 线程调用,如通知停机;不得再抛)
     */
    public StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                        VersionedRelationRegistry registry, RelationResolver relationResolver,
                                        Path pipeDir, RollCycle pipeRollCycle,
                                        BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                        AtomicLong outputFrontier, Runnable onFailure) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                outputFrontier, onFailure, BucketTableResolver.snapshotBacked());
    }

    /**
     * 构造<b>异步形态</b>组装器(带指标注入,表解析走默认快照透缝):其余语义同九参公共异步
     * 构造,吞吐指标实例由调用方创建后穿入(装配点与 Task 4 的 bridge 读源共用同一实例)。
     * 注意:listener 侧表解析用<b>默认新实例</b>——listener 若持有自有 tableResolver(如
     * source 的 DispatcherTransactionListener 接线),须改走包私有 11 参构造穿同一实例,
     * 否则 consumer 绑定的与 listener 读的解析器不是同一实例(asOf 查询永远落空)。
     *
     * @param listener         事务事件流回调(Begin/TxChange/End 按序流式交付,consumer 线程同步调用)
     * @param mode             流式模式(仅影响 decoder 对 StreamAbort 附加字段的解析,须与
     *                         START_REPLICATION 的 streaming 参数一致,否则 abort 解析错位 fail-fast)
     * @param registry         Relation 版本日志('R' 路由与交接快照共用,本组装器独占写入)
     * @param relationResolver 'R' → ResolvedRelation 解析接缝(测试假实现 / Task 7 真实现)
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期(决定低水位删除的档位粒度)
     * @param decodedObserver  每个解码点回调(控制消息 + 'R' + 回放单元;Y/O 不解码不回调)
     * @param outputFrontier   输出前沿载体(调用方持有以便反馈封顶;本实例只做单调 max 累加)
     * @param onFailure        consumer 回放失败的逃生回调(consumer 线程调用,如通知停机;不得再抛)
     * @param metrics          吞吐与分布指标(装配点创建;四点插桩共用本实例——slot/组装在
     *                         本类,输出/分布/报告 tick 在 consumer,回放字节在 replayer)
     */
    public StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                        VersionedRelationRegistry registry, RelationResolver relationResolver,
                                        Path pipeDir, RollCycle pipeRollCycle,
                                        BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                        AtomicLong outputFrontier, Runnable onFailure,
                                        StreamThroughputMetrics metrics) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                outputFrontier, onFailure, BucketTableResolver.snapshotBacked(), metrics);
    }

    /**
     * 构造<b>异步形态</b>组装器(全量,包私有——{@link BucketTableResolver} 是包内接缝,留给
     * 同包装配点注入 listener 侧表解析的真实现,Task 7 的接线用):其余语义同九参公共异步构造,
     * 指标自建默认实例。
     *
     * @param listener         事务事件流回调(consumer 线程调用;listener 侧经 tableResolver 取 asOf 表)
     * @param mode             流式模式
     * @param registry         Relation 版本日志
     * @param relationResolver 'R' → ResolvedRelation 解析接缝
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期(决定低水位删除的档位粒度)
     * @param decodedObserver  每个解码点回调
     * @param outputFrontier   输出前沿载体(调用方持有;本实例只做单调 max 累加)
     * @param onFailure        回放失败的逃生回调(consumer 线程调用)
     * @param tableResolver    listener 侧按 (oid, seq) 解析 asOf 表定义的接缝(每个桶回放前由消费器绑定快照)
     */
    StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                 VersionedRelationRegistry registry, RelationResolver relationResolver,
                                 Path pipeDir, RollCycle pipeRollCycle,
                                 BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                 AtomicLong outputFrontier, Runnable onFailure,
                                 BucketTableResolver tableResolver) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                outputFrontier, onFailure, tableResolver, new StreamThroughputMetrics());
    }

    /**
     * 构造<b>异步形态</b>组装器(全量含指标,包私有):其余语义同上,吞吐指标实例显式穿入
     * (便捷构造链最终汇入本构造)。
     *
     * @param listener         事务事件流回调(consumer 线程调用)
     * @param mode             流式模式
     * @param registry         Relation 版本日志
     * @param relationResolver 'R' → ResolvedRelation 解析接缝
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期(决定低水位删除的档位粒度)
     * @param decodedObserver  每个解码点回调
     * @param outputFrontier   输出前沿载体(调用方持有;本实例只做单调 max 累加)
     * @param onFailure        回放失败的逃生回调(consumer 线程调用)
     * @param tableResolver    listener 侧按 (oid, seq) 解析 asOf 表定义的接缝(每个桶回放前由消费器绑定快照)
     * @param metrics          吞吐与分布指标(四点插桩共用的单实例)
     */
    StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                 VersionedRelationRegistry registry, RelationResolver relationResolver,
                                 Path pipeDir, RollCycle pipeRollCycle,
                                 BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                 AtomicLong outputFrontier, Runnable onFailure,
                                 BucketTableResolver tableResolver, StreamThroughputMetrics metrics) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                outputFrontier, onFailure, tableResolver, metrics, true);
    }

    /**
     * 构造<b>同步形态</b>组装器(默认表解析接缝):{@code dispatchHandedOff} 在调用线程直调
     * {@code TransactionConsumer.processBucket}——回放与回调对 onRaw 同步可见,fail-fast 异常
     * 直传调用方;输出前沿内部自持(测试经 {@link #outputFrontierForTest()} 读)、onFailure
     * 置空消费(同步形态异常直传调用方,无人可通知)。listener 侧 asOf 表解析走默认的快照
     * 透传接缝({@code BucketTableResolver.snapshotBacked()}),需要注入自有实现的同包调用方
     * (Task 7 起)用包私有构造。急切建立管道——{@link MessagePipe}
     * 构造即清空目录建队列,失败原样上抛 fail-fast:管道是地基,建不起来就没有可运行形态。
     *
     * @param listener         事务事件流回调(Begin/TxChange/End 按序流式交付,交接时同步调用)
     * @param mode             流式模式(仅影响 decoder 对 StreamAbort 附加字段的解析,须与
     *                         START_REPLICATION 的 streaming 参数一致,否则 abort 解析错位 fail-fast)
     * @param registry         Relation 版本日志('R' 路由与交接快照共用,本组装器独占写入)
     * @param relationResolver 'R' → ResolvedRelation 解析接缝(测试假实现 / Task 7 真实现)
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期(决定低水位删除的档位粒度)
     * @param decodedObserver  每个解码点回调(控制消息 + 'R' + 回放单元;Y/O 不解码不回调)
     */
    public StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                        VersionedRelationRegistry registry, RelationResolver relationResolver,
                                        Path pipeDir, RollCycle pipeRollCycle,
                                        BiConsumer<PgOutputMessage, RelationLookup> decodedObserver) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                BucketTableResolver.snapshotBacked());
    }

    /**
     * 构造<b>同步形态</b>组装器(全量,包私有——{@link BucketTableResolver} 是包内接缝,留给
     * 同包装配点注入 listener 侧表解析的真实现,Task 7 的接线用):其余语义同七参公共构造,
     * 指标自建默认实例。
     *
     * @param listener         事务事件流回调(交接时同步调用;listener 侧经 tableResolver 取 asOf 表)
     * @param mode             流式模式
     * @param registry         Relation 版本日志
     * @param relationResolver 'R' → ResolvedRelation 解析接缝
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期
     * @param decodedObserver  每个解码点回调
     * @param tableResolver    listener 侧按 (oid, seq) 解析 asOf 表定义的接缝(每个桶回放前由消费器绑定快照)
     */
    StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                 VersionedRelationRegistry registry, RelationResolver relationResolver,
                                 Path pipeDir, RollCycle pipeRollCycle,
                                 BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                 BucketTableResolver tableResolver) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                tableResolver, new StreamThroughputMetrics());
    }

    /**
     * 构造<b>同步形态</b>组装器(全量含指标,包私有):其余语义同上,吞吐指标实例显式穿入
     * (接线测试用它注入自有实例以断言四点插桩——与生产路径仅实例来源不同,口径无差)。
     *
     * @param listener         事务事件流回调(交接时同步调用;listener 侧经 tableResolver 取 asOf 表)
     * @param mode             流式模式
     * @param registry         Relation 版本日志
     * @param relationResolver 'R' → ResolvedRelation 解析接缝
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期
     * @param decodedObserver  每个解码点回调
     * @param tableResolver    listener 侧按 (oid, seq) 解析 asOf 表定义的接缝(每个桶回放前由消费器绑定快照)
     * @param metrics          吞吐与分布指标(四点插桩共用的单实例)
     */
    StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                 VersionedRelationRegistry registry, RelationResolver relationResolver,
                                 Path pipeDir, RollCycle pipeRollCycle,
                                 BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                 BucketTableResolver tableResolver, StreamThroughputMetrics metrics) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, decodedObserver,
                new AtomicLong(), () -> { }, tableResolver, metrics, false);
    }

    /**
     * 便捷构造:解码观察者置为空消费(不需要逐消息透出的场景),其余同全量构造。
     *
     * @param listener         事务事件流回调
     * @param mode             流式模式
     * @param registry         Relation 版本日志
     * @param relationResolver 'R' → ResolvedRelation 解析接缝
     * @param pipeDir          管道目录(瞬态工作区,打开即整体清空)
     * @param pipeRollCycle    管道滚动周期
     */
    public StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                        VersionedRelationRegistry registry, RelationResolver relationResolver,
                                        Path pipeDir, RollCycle pipeRollCycle) {
        this(listener, mode, registry, relationResolver, pipeDir, pipeRollCycle, (msg, view) -> { });
    }

    /**
     * 全量构造(两形态公共初始化,引擎私有全量构造的 1:1 对位):字段赋值 + 建管道 + 建消费器
     * (交接队列、前沿、onFailure、吞吐指标随消费器落位——指标再经消费器转传回放器,四点
     * 共用同一实例);async=true 时另起并启动非守护
     * {@code transaction-consumer} 线程立即消费交接队列。管道建立失败原样上抛
     * (fail-fast,同上——此时 consumer 线程尚未创建,无线程泄漏)。
     */
    private StreamedTransactionAssembler(StreamingTransactionListener listener, StreamingMode mode,
                                         VersionedRelationRegistry registry, RelationResolver relationResolver,
                                         Path pipeDir, RollCycle pipeRollCycle,
                                         BiConsumer<PgOutputMessage, RelationLookup> decodedObserver,
                                         AtomicLong outputFrontier, Runnable onFailure,
                                         BucketTableResolver tableResolver, StreamThroughputMetrics metrics,
                                         boolean async) {
        this.decoder = new PgOutputStreamDecoder(Objects.requireNonNull(mode, "mode"));
        this.registry = Objects.requireNonNull(registry, "registry");
        this.relationResolver = Objects.requireNonNull(relationResolver, "relationResolver");
        Objects.requireNonNull(pipeDir, "pipeDir");
        Objects.requireNonNull(pipeRollCycle, "pipeRollCycle");
        this.pipe = new MessagePipe(pipeDir, pipeRollCycle);
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
        this.outputFrontier = Objects.requireNonNull(outputFrontier, "outputFrontier");
        this.throughputMetrics = Objects.requireNonNull(metrics, "metrics");
        this.consumer = new TransactionConsumer(Objects.requireNonNull(listener, "listener"), mode,
                this.pipe, handoffQueue, this.outputFrontier, Objects.requireNonNull(onFailure, "onFailure"),
                this.decodedObserver, Objects.requireNonNull(tableResolver, "tableResolver"),
                this.throughputMetrics);
        if (async) {
            this.consumerThread = new Thread(consumer, "transaction-consumer");
            this.consumerThread.setDaemon(false);
            this.consumerThread.start();
        } else {
            this.consumerThread = null;
        }
    }

    /**
     * 消费一条完整的 pgoutput 消息原始字节,推进组装状态机。
     *
     * <p>先 append 进管道取 seq(每条消息一个——含控制消息与 Relation,seq 即 CQ index,
     * 数据单元与 'R' 版本天然同序),再按类型字节路由:
     * <ul>
     *   <li>'B'/'C'/'S'/'E'/'c'/'A'/'b'/'P'/'K'/'r'/'p':当场解码(decoder 顺带维护流块状态),
     *       按控制规则处理——全部 fail-fast 语义逐条保留</li>
     *   <li>'R':当场解码后经 {@link RelationResolver} 解析成 ResolvedRelation、以到达 seq 记入
     *       registry 版本日志,字节不入桶</li>
     *   <li>'I'/'U'/'D'/'T':不完整解码,校验桶级前缀不变量、窥 oid 后把 index 记入当前活动桶
     *       的连续段(没有活动桶则 fail-fast)</li>
     *   <li>'M':先窥 flags 的 bit0 判断事务性——事务性必须落在活动桶里(无桶 fail-fast);
     *       非事务性有桶随桶走、无桶则 INFO 即时留痕并经护栏推进前沿(MS3.5,见
     *       {@link #routeLogicalMsg},仅槽选项 messages=true 时有输入)</li>
     *   <li>'Y'/'O':DEBUG 记录后丢弃(沿用旧组装器的忽略语义)</li>
     *   <li>未知类型字节:交给 decoder 抛 UnknownMessageTypeException(fail-fast 由解码层承担)</li>
     * </ul>
     *
     * <p>边界:raw 为 null 抛 NPE;空数组违反调用契约(首字节访问抛数组越界);管道写失败
     * (磁盘满/IO)以 Chronicle 运行时异常上抛;桶缺失/重复/流块状态异常抛
     * {@link IllegalStateException};字节与协议不符经 decoder 抛协议异常——都原样上抛,
     * 终止读取线程。
     *
     * @param raw 完整单条消息字节(含类型字节,流式块内还含 Int32 xid 前缀)
     */
    @Override
    public void onRaw(byte[] raw) {
        Objects.requireNonNull(raw, "raw");
        throughputMetrics.onSlotMessage(raw);   // slot 读取记账:收到即记(含控制消息与 'R',字节=raw.length)
        long seq = pipe.append(raw);
        maxAppendedIndex = seq;
        char type = (char) raw[0];
        // 注:record pattern switch 是 Java 21 正式特性,本项目约束 Java 17;此处 switch 的是
        // 类型字节(char)而非 record,合法;解码产物仍走 instanceof 强转
        switch (type) {
            case 'I', 'U', 'D', 'T' -> routeData(raw, seq);
            case 'M' -> routeLogicalMsg(raw, seq);
            default -> routeControl(raw, type, seq);
        }
    }

    /**
     * 控制消息与非数据类的统一路由(onRaw 的 default 分支收拢):控制消息 live 解码后分发到
     * 桶状态规则,'R' 以到达 seq 经 resolver 解析后记入版本日志,'Y'/'O' 丢弃,未知类型经
     * decoder fail-fast。处理完把 {@code lastAppendOwner} 置 null——这类消息的 append 不属于
     * 任何桶,必须断开连续段(否则本桶下一次追加会把中间的控制消息 index 误并入段内,回放
     * 读回错位)。未知类型的 default 分支解码即抛,置空不可达,无副作用。
     */
    private void routeControl(byte[] raw, char type, long seq) {
        switch (type) {
            case 'B' -> begin((PgOutputMessage.Begin) decode(raw));
            case 'C' -> commit((PgOutputMessage.Commit) decode(raw));
            case 'S' -> streamStart((PgOutputMessage.StreamStart) decode(raw));
            case 'E' -> streamStop((PgOutputMessage.StreamStop) decode(raw));
            case 'c' -> streamCommit((PgOutputMessage.StreamCommit) decode(raw));
            case 'A' -> streamAbort((PgOutputMessage.StreamAbort) decode(raw));
            case 'b' -> beginPrepare((PgOutputMessage.BeginPrepare) decode(raw));
            case 'P' -> prepare((PgOutputMessage.Prepare) decode(raw));
            case 'K' -> commitPrepared((PgOutputMessage.CommitPrepared) decode(raw));
            case 'r' -> rollbackPrepared((PgOutputMessage.RollbackPrepared) decode(raw));
            case 'p' -> streamPrepare((PgOutputMessage.StreamPrepare) decode(raw));
            case 'R' -> {
                PgOutputMessage.Relation wire = (PgOutputMessage.Relation) decode(raw);
                registry.accept(seq, relationResolver.resolve(seq, wire));
            }
            case 'Y', 'O' -> LOG.debug("元数据消息 '{}' 不入桶,直接丢弃", type);
            default -> decode(raw);   // 未知类型:解码层 fail-fast(UnknownMessageTypeException)
        }
        lastAppendOwner = null;
    }

    /**
     * 关闭组装器:<b>排干协议</b>(异步形态,引擎 close 的 1:1 对位)——毒丸入队 → 等 consumer
     * 线程退出(join 60s 超时 WARN 放行——防回调卡死拖住停机,超时后放弃等待继续关管道)→
     * 关管道。毒丸排在队列尾,FIFO 保证 consumer 先排干此前交接的全部冻结桶再见到毒丸——
     * <b>已提交未输出的事务不丢</b>(排干语义的核心承诺,测试确定性断言依赖它:join 返回即
     * 全部输出可见)。同步形态无 consumer 线程,跳过前两步直接关管道(回放本就同步内联完成,
     * 无未排干存量)。
     *
     * <p>{@link MessagePipe#close} 内部已逐资源 WARN 吸收,这里只兜住意外逃逸的异常——
     * close 不应掩盖业务异常(最坏代价是句柄延迟回收)。在 run 线程收尾或调用方线程调用一次,
     * <b>不可与 onRaw 并发</b>(pipe 的 appender 是 reader 单写者资源,关它必须发生在 reader
     * 停止之后——调用方约束,Task 7 的 source 停机序以 session.close → reader.join 保证)。
     * join 被中断时恢复中断标志并放弃等待(不重试——停机路径不应被阻塞)。shutdownFast 之后
     * 再调 close 是安全组合:毒丸重复入队无害、join 对已亡线程立即返回、管道 close 幂等。
     */
    @Override
    public void close() {
        if (consumerThread != null) {
            handoffQueue.add(TxBuffer.POISON);
            try {
                consumerThread.join(60_000L);
                if (consumerThread.isAlive()) {
                    LOG.warn("consumer 线程 60s 内未退出(可能卡在 listener 回调),放弃等待直接关管道");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("等待 consumer 退出被中断,放弃等待直接关管道");
            }
        }
        try {
            pipe.close();
        } catch (RuntimeException e) {
            LOG.warn("管道关闭失败(忽略)", e);
        }
    }

    /**
     * 责任:<b>D7 快速停机</b>(MS2 新增形态,引擎无对应物——连接器 source 停机序用):
     * 毒丸入队 + 中断 consumer 线程 + <b>不 join</b>(不等在途回放/排队桶排干——这正是与
     * {@link #close()} 排干协议的分叉点)+ 直接关管道。
     * 关键步骤:毒丸入队(consumer 若能走到队列即见退出信号)→ {@code interrupt}(防 consumer
     * 阻塞在 poll 的 1s 等待——中断使下一次 poll 立即抛 InterruptedException 退出;若正阻塞在
     * listener 回调,中断只置标志,回调返回后的下一次 poll 即退)→ 关管道(资源即刻回收)。
     * 边界:同步形态无 consumer 线程,退化为直接关管道;重复调用安全(毒丸重复入队无害、
     * interrupt 对已亡线程无效果、pipe.close 幂等 WARN);关管道后仍在途的回放会因管道已关
     * 而失败,经 {@code TransactionConsumer.run} 的 fail-fast 路径(onFailure)退出——不排干
     * 属预期(D7 语义:停机快于输出,未输出事务由复制槽在重启后重发,at-least-once)。
     * <b>不可与 onRaw 并发调用</b>——与 close 同一调用方约束(Task 7 的 source 停机序
     * session.close → reader.join → assembler.shutdownFast 保证 reader 已停,此处不再碰
     * reader 侧结构)。
     * 线程:停机线程(与 close 同)。
     */
    public void shutdownFast() {
        if (consumerThread != null) {
            handoffQueue.add(TxBuffer.POISON);
            consumerThread.interrupt();
        }
        try {
            pipe.close();
        } catch (RuntimeException e) {
            LOG.warn("管道关闭失败(忽略)", e);
        }
    }

    /**
     * 当场解码一条消息并回调 decodedObserver(所有 live 解码点的统一出口;第二参传 registry——
     * live 视角的 RelationLookup,最新版视图);协议不符由 decoder 抛异常,不捕获。
     * 只在 reader 线程调用(decoder 的流块状态单写者)。
     */
    private PgOutputMessage decode(byte[] raw) {
        PgOutputMessage msg = decoder.decode(ByteBuffer.wrap(raw));
        decodedObserver.accept(msg, registry);
        return msg;
    }

    /**
     * 数据消息(I/U/D/T)入桶:不解码,校验桶级前缀不变量、窥 relation oid 后把 append 返回的
     * index 记入当前活动桶的连续段。活动桶按"流块上下文 → 两阶段 → 普通"的顺序取,一个都没有
     * 就 fail-fast(异常消息带 {@link #describeData} 生成的触发消息上下文)。字节本身已在
     * onRaw 首行 append 进管道,此处不再写 CQ。
     */
    private void routeData(byte[] raw, long seq) {
        TxBuffer bucket = activeBucket(() -> describeData(raw));
        appendUnit(bucket, raw, seq);
    }

    /**
     * LogicalMsg('M')的轻窥路由(MS3.5 起非事务游离分支从"WARN 丢弃"升级为"INFO 留痕 +
     * 护栏推进",spec §3.2/§3.3):
     * 读 flags 的 bit0 判断是否事务性(flags 在流式块内有 4 字节 xid 前缀,偏移 5;顶层偏移 1)。
     * 事务性消息必须落在活动桶里(没有桶就 fail-fast,与 DML 相同);非事务性消息有桶就随桶走
     * (将来 abort 剔除按 streamXid 判断,语义安全),没有桶则<b>即时留痕并推进前沿</b>——协议
     * 允许它游离在任何事务之外,不算异常,但它是唯一不带事务的输出信号,不推进则空闲库的
     * confirmed_flush 永远钉死(原始动机)。关键步骤:经 {@link RawPeeks} 窥 prefix/lsn/content
     * (prefix NUL 之后的 I32 长度 + 字节段,不整条解码)→ INFO 一行(content 走 {@link MessagePreview}
     * 预览)→ {@code outputFrontier} 以 {@link #safeMessageAdvance}(msgLsn, 交接记账)的返回值
     * 单调 max 累加——<b>全有或全无</b>(决策 L8):无 pending 桶推进到消息位;有 pending 桶
     * 返回 {@link Long#MIN_VALUE},max 累加下天然 no-op,前沿一个字节都不动(pending 事务的
     * 重发空间无条件保住,不再依赖 commitLsn/endLsn 字段级精确语义)。flags 偏移由
     * currentStream 是否在流块内决定,与 decoder 的
     * inStream 状态同步变化。
     * 边界:消息字节仍已在 onRaw 首行 append 进管道(<b>不动"先 append 再路由"红线</b>)——
     * 此分支无桶引用,落盘字节无人回读,随 wipe-on-open(重启)或低水位删除清空,不构成泄漏;
     * 该分支仅在槽选项 messages=true 时才可能有输入(PG 关闭时不发 'M')——组装器不感知配置,
     * 行为天然由上游门控。游离路径的 owner 置空属<b>防御性</b>(当前控制消息路径已统一置空;
     * 此路径到达时无任何活动桶,owner 至多指向已退役/已交接的桶——它们不会再成为追加目标,
     * 置空与否不影响段判定)。线程:reader 线程(读 handedOff 的即时 state 即"此刻仍在途")。
     */
    private void routeLogicalMsg(byte[] raw, long seq) {
        int flagsOffset = currentStream != null ? 5 : 1;   // 流内前缀 4 字节在前
        boolean transactional = (raw[flagsOffset] & 0x01) != 0;
        if (transactional || hasActiveBucket()) {
            TxBuffer bucket = activeBucket(() -> describeData(raw));
            appendUnit(bucket, raw, seq);
            return;
        }
        lastAppendOwner = null;   // 防御性置空(见方法 javadoc)
        long msgLsn = RawPeeks.longAt(raw, flagsOffset + 1);
        int prefixStart = flagsOffset + 1 + 8;   // flags(1) + lsn(8) 之后
        String prefix = RawPeeks.cstringAt(raw, prefixStart);
        int lenOffset = RawPeeks.cstringEnd(raw, prefixStart) + 1;   // prefix NUL 之后的 I32 长度字段
        byte[] content = RawPeeks.bytesAt(raw, lenOffset + 4, RawPeeks.intAt(raw, lenOffset));
        LOG.info("逻辑消息: prefix={}, lsn={}, 事务性={}, content={}", prefix, Long.toHexString(msgLsn),
                false, MessagePreview.preview(content));
        outputFrontier.accumulateAndGet(safeMessageAdvance(msgLsn, handedOff), Math::max);
    }

    /**
     * 数据消息入桶:判定流式前缀有无(inStream)→ 校验桶级 hasPrefix 不变量
     * (首个单元定型,此后混现即 ISE fail-fast——协议不允许,防御)→ 窥 oid 入 oidSet →
     * unitCount 自增({@code TransactionEvent.Begin#expectedChanges} 的记账来源)→
     * appendIndex 记段。字节本身已在 onRaw 首行 append 进管道(index 即 seq),此处不再写 CQ;
     * 前缀的具体 xid 值推迟到回放期重窥(Task 5 的回放器按 hasPrefix 决定 decodeSingle 的
     * inStream 实参并重窥作 streamXid)。只在 reader 线程调用。
     */
    private void appendUnit(TxBuffer bucket, byte[] raw, long seq) {
        boolean inStream = currentStream != null;
        if (!bucket.prefixKnown) {
            bucket.hasPrefix = inStream;
            bucket.prefixKnown = true;
        } else if (bucket.hasPrefix != inStream) {
            throw new IllegalStateException("桶内单元流式前缀混现: xid=" + bucket.xid
                    + " hasPrefix=" + bucket.hasPrefix + " 当前 inStream=" + inStream);
        }
        collectOids(bucket, raw);
        bucket.unitCount++;
        appendIndex(bucket, seq);
    }

    /**
     * 窥数据消息的 relation oid 记入桶的 oidSet(快照圈定用):I/U/D 在类型字节(及可选 4 字节前缀)
     * 后取 Int32 relationOid;T 读 I32 表数 + 选项字节后的 oid 数组;M 无 oid 跳过。偏移与
     * describeData 同源(协议线格式见解码设计 spec 附录)。
     */
    private void collectOids(TxBuffer bucket, byte[] raw) {
        int base = currentStream != null ? 5 : 1;
        switch (raw[0]) {
            case 'I', 'U', 'D' -> bucket.oidSet.add(RawPeeks.intAt(raw, base));
            case 'T' -> {
                int n = RawPeeks.intAt(raw, base);
                for (int i = 0; i < n; i++) {
                    bucket.oidSet.add(RawPeeks.intAt(raw, base + 5 + 4 * i));
                }
            }
            default -> { /* 'M' 无 oid */ }
        }
    }

    /**
     * 把数据单元的 CQ index 记入桶的连续段:上一次全局 append 的 owner 是本桶才顺延当前段,
     * 否则新开段 [index,index]——控制消息的 append 会把 owner 置 null,天然断段。
     * firstIndex/lastIndex 维护全局端点。只在 reader 线程调用。
     */
    private void appendIndex(TxBuffer bucket, long index) {
        if (lastAppendOwner == bucket && !bucket.segments.isEmpty()) {
            bucket.segments.peekLast()[1] = index;
        } else {
            bucket.segments.addLast(new long[]{ index, index });
        }
        if (bucket.firstIndex < 0) {
            bucket.firstIndex = index;
        }
        bucket.lastIndex = index;
        lastAppendOwner = bucket;
    }

    /**
     * 取当前应接收变更的活动桶。
     * 查找顺序:流块上下文优先,其次是活动的两阶段桶,最后是普通桶。三者都为空
     * 说明变更消息游离在任何事务之外——协议流异常,fail-fast(触发消息的描述惰性求值,
     * 只有走异常路径才付出构造开销)。
     */
    private TxBuffer activeBucket(Supplier<String> trigger) {
        if (currentStream != null) {
            return currentStream;
        }
        if (currentPrepareTx != null) {
            return currentPrepareTx;
        }
        if (currentNormalTx != null) {
            return currentNormalTx;
        }
        throw new IllegalStateException("变更消息到达但无任何活动事务桶: " + trigger.get());
    }

    /** 是否存在任何一个活动桶(流块/两阶段/普通三处指针)——logicalMsg 的丢弃分支等场景判断用。 */
    private boolean hasActiveBucket() {
        return currentStream != null || currentPrepareTx != null || currentNormalTx != null;
    }

    /**
     * 为 fail-fast 异常生成触发消息的描述(类型 + relationOid;LogicalMsg 用 prefix + lsn)。
     * 信息全部按固定偏移从原始字节里窥出,不做完整解码。只在异常路径调用;'M' 的 prefix
     * 扫描到第一个 NUL 结束(前缀按协议约定是短字符串)。
     */
    private String describeData(byte[] raw) {
        int base = currentStream != null ? 5 : 1;   // 类型字节 + 可选 Int32 前缀之后
        return switch (raw[0]) {
            case 'I' -> "Insert relationOid=" + RawPeeks.intAt(raw, base);
            case 'U' -> "Update relationOid=" + RawPeeks.intAt(raw, base);
            case 'D' -> "Delete relationOid=" + RawPeeks.intAt(raw, base);
            case 'T' -> {
                int n = RawPeeks.intAt(raw, base);
                int[] oids = new int[n];
                for (int i = 0; i < n; i++) {
                    oids[i] = RawPeeks.intAt(raw, base + 5 + 4 * i);   // I32 表数 + I8 选项位之后
                }
                yield "Truncate relationOids=" + Arrays.toString(oids);
            }
            case 'M' -> "LogicalMsg prefix=" + RawPeeks.cstringAt(raw, base + 1 + 8);   // flags(1) + lsn(8) 之后
            default -> "'" + (char) raw[0] + "'";
        };
    }

    /** Begin:开新普通事务桶;已有未闭合普通事务即 fail-fast(协议上 Begin..Commit 不嵌套)。 */
    private void begin(PgOutputMessage.Begin m) {
        if (currentNormalTx != null) {
            throw new IllegalStateException("Begin 到达但普通事务未闭合: xid=" + currentNormalTx.xid);
        }
        currentNormalTx = newBucket(m.xid());
    }

    /**
     * Commit(无 xid 字段):当前普通事务桶交接封箱 NORMAL 输出,清空指针;无桶即 fail-fast
     * (异常带 commitLsn 定位)。交接后桶完结(低水位维护 + 剪枝)。
     */
    private void commit(PgOutputMessage.Commit m) {
        if (currentNormalTx == null) {
            throw new IllegalStateException("Commit 到达但无活动普通事务: commitLsn=0x"
                    + Long.toHexString(m.commitLsn()));
        }
        TxBuffer bucket = currentNormalTx;
        currentNormalTx = null;
        handoff(bucket, TransactionKind.NORMAL, m.commitLsn(), m.endLsn(), m.commitTimestamp());
    }

    /**
     * StreamStart(xid, firstSegment):xid 恒为顶层 xid(spec B.3——ReorderBufferStreamTXN 断言
     * toptxn;firstSegment 表示该事务此前还没被流式过)。
     *
     * <p>currentStream 非 null 意味着上一个流块没闭合(缺 'E'),fail-fast——流块不嵌套,
     * 与 'c'/'A'/'p'/'E' 各处理器的守卫一致。firstSegment=true(首个流段)新建桶放入
     * streamedByXid,同 xid 已有桶则 fail-fast;false(后续流段)要求桶已存在,查不到则
     * fail-fast。两种情况都把 currentStream 切到该桶。
     */
    private void streamStart(PgOutputMessage.StreamStart m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamStart 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket;
        if (m.firstSegment()) {
            bucket = newBucket(m.xid());
            if (streamedByXid.putIfAbsent(m.xid(), bucket) != null) {
                throw new IllegalStateException("流式事务桶已存在: xid=" + m.xid());
            }
        } else {
            bucket = streamedByXid.get(m.xid());
            if (bucket == null) {
                throw new IllegalStateException("StreamStart(first=false) 但顶层事务无桶: xid=" + m.xid());
            }
        }
        currentStream = bucket;
    }

    /**
     * StreamStop:流块边界(消息不携带 xid——spec B.3)。currentStream 必须非 null(否则 fail-fast),
     * 置 null。流桶保留在 streamedByXid 中等待后续段或 StreamCommit/StreamAbort/StreamPrepare。
     */
    private void streamStop(PgOutputMessage.StreamStop m) {
        if (currentStream == null) {
            throw new IllegalStateException("StreamStop 到达但无进行中的流块");
        }
        currentStream = null;
    }

    /**
     * StreamCommit(xid):顶层事务全部流段已收齐,桶交接封箱 STREAMED 输出、移除桶;
     * 桶 miss 或仍有未闭合流块均 fail-fast(协议保证 stream_commit 必在流块外,spec B.3)。
     * 交接后桶完结(低水位维护 + 剪枝)。
     */
    private void streamCommit(PgOutputMessage.StreamCommit m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamCommit 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamCommit 对应流式事务桶不存在: xid=" + m.xid());
        }
        handoff(bucket, TransactionKind.STREAMED, m.commitLsn(), m.endLsn(), m.commitTimestamp());
    }

    /**
     * StreamAbort(top, sub):已流式事务的(子)事务回滚(spec B.4)。
     *
     * <p>top==sub(整顶层回滚,decode 层"先子后顶"的最后一条)→ 移除整个桶(管道里的对应条目
     * 成为垃圾,随低水位推进被删除);否则 sub 记入桶的 abortedSubxids,回放期过滤该(子)事务
     * 的单元(Message 单元前缀=顶层 xid,天然不命中 sub,不会误剔)。桶 miss 或流块未闭合均
     * fail-fast(abort 必在流块外)。
     */
    private void streamAbort(PgOutputMessage.StreamAbort m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamAbort 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.get(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamAbort 对应流式事务桶不存在: xid=" + m.xid());
        }
        if (m.xid() == m.subxid()) {
            streamedByXid.remove(m.xid());
            liveCount.decrementAndGet();   // 整桶丢弃:退出 LIVE 记账
            maintainWatermarks();          // 整桶丢弃:低水位候选推进(释放检查)
        } else {
            bucket.abortedSubxids.add(m.subxid());
        }
    }

    /** BeginPrepare:开活动两阶段桶(记 gid/xid);已有未闭合两阶段桶即 fail-fast(b..P 串行不嵌套)。 */
    private void beginPrepare(PgOutputMessage.BeginPrepare m) {
        if (currentPrepareTx != null) {
            throw new IllegalStateException("BeginPrepare 到达但两阶段事务未闭合: gid=" + currentPrepareTx.gid);
        }
        currentPrepareTx = newBucket(m.xid());
        currentPrepareTx.gid = m.gid();
    }

    /**
     * Prepare:活动两阶段桶转挂起池(gid 已存在 → fail-fast)。
     * 事务自此挂起,等待 CommitPrepared(输出)或 RollbackPrepared(丢弃),可能长期等待甚至跨重启
     * (持久化非目标)。
     */
    private void prepare(PgOutputMessage.Prepare m) {
        if (currentPrepareTx == null || currentPrepareTx.xid != m.xid()
                || !currentPrepareTx.gid.equals(m.gid())) {
            throw new IllegalStateException("Prepare 与活动两阶段事务不匹配: gid=" + m.gid() + " xid=" + m.xid());
        }
        TxBuffer bucket = currentPrepareTx;
        currentPrepareTx = null;
        if (preparedByGid.putIfAbsent(bucket.gid, bucket) != null) {
            throw new IllegalStateException("挂起池已存在同 gid 事务: " + bucket.gid);
        }
        LOG.debug("两阶段事务 PREPARE 入挂起池: gid={} storage={} pending={}",
                bucket.gid, storageOf(bucket), preparedByGid.size());
    }

    /**
     * CommitPrepared:挂起池取桶(miss → fail-fast)交接封箱 TWO_PHASE 输出(gid 随桶冻结;
     * 用户确认的输出时机)。交接后桶完结(低水位维护 + 剪枝)。
     */
    private void commitPrepared(PgOutputMessage.CommitPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("CommitPrepared 对应 gid 不存在: " + m.gid());
        }
        handoff(bucket, TransactionKind.TWO_PHASE, m.commitLsn(), m.endLsn(), m.commitTimestamp());
    }

    /** RollbackPrepared:挂起池取桶(miss → fail-fast)静默丢弃,不回调(用户确认的回滚语义);丢弃后低水位候选推进。 */
    private void rollbackPrepared(PgOutputMessage.RollbackPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("RollbackPrepared 对应 gid 不存在: " + m.gid());
        }
        LOG.warn("两阶段事务回滚,丢弃已缓冲变更: gid={} xid={} storage={}",
                m.gid(), bucket.xid, storageOf(bucket));
        liveCount.decrementAndGet();   // 整桶丢弃:退出 LIVE 记账
        maintainWatermarks();
    }

    /**
     * StreamPrepare(xid, gid):流式 2PC 的 prepare。流桶从 streamedByXid 移出(miss 或流块未闭合 →
     * fail-fast,stream_prepare 前服务端必已发完最后一个流段并 stream_stop,spec B.6),记 gid 后转挂起池。
     */
    private void streamPrepare(PgOutputMessage.StreamPrepare m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamPrepare 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamPrepare 对应流式事务桶不存在: xid=" + m.xid());
        }
        bucket.gid = m.gid();
        if (preparedByGid.putIfAbsent(bucket.gid, bucket) != null) {
            throw new IllegalStateException("挂起池已存在同 gid 事务: " + bucket.gid);
        }
        LOG.debug("流式两阶段事务 StreamPrepare 入挂起池: gid={} storage={} pending={}",
                bucket.gid, storageOf(bucket), preparedByGid.size());
    }

    /**
     * 交接:拷快照(oidSet 圈定,截止 lastIndex)→ 捕获封箱元数据 → state=HANDED_OFF → 入
     * handedOff 记账 → 退出 LIVE 记账 → {@link #dispatchHandedOff}(按形态分流:同步直调
     * processBucket 桶当即推 DONE;异步入交接队列由 consumer 线程回放)→ 维护低水位。
     * 立即返回——reader 路径从此不含回放。只在 reader 线程调用。
     */
    private void handoff(TxBuffer bucket, TransactionKind kind, long commitLsn, long endLsn,
            Instant commitTimestamp) {
        bucket.kind = kind;
        bucket.commitLsn = commitLsn;
        bucket.endLsn = endLsn;
        bucket.commitTimestamp = commitTimestamp;
        bucket.relationSnapshot = registry.snapshot(bucket.oidSet, bucket.lastIndex);
        bucket.state = BucketState.HANDED_OFF;
        bucket.handoffNanos = System.nanoTime();
        handedOff.add(bucket);
        liveCount.decrementAndGet();
        throughputMetrics.onTxHandedOff();   // 组装完成记账:提交交接的事务才计(回滚丢弃不计)
        dispatchHandedOff(bucket);   // Task 5:同步直调 consumer.processBucket;Task 6:分流入队
        maintainWatermarks();
    }

    /**
     * 交接分发(引擎 handoff 同位调用,按形态分流):同步形态(consumerThread 为 null)在调用
     * 线程直调 {@link TransactionConsumer#processBucket}——事件流(Begin → TxChange* → End)
     * 的发出、End 之后的前沿累加与桶状态后两态都对 onRaw 同步可见,回放失败异常直传调用方
     * (fail-fast,End 永不发、前沿不推进);异步形态交接入队({@link #handoffQueue}),FIFO
     * 保证交接序即提交序,回放交由 consumer 线程异步完成——reader 路径从此不含回放。
     * 调用点位于 liveCount 递减之后、maintainWatermarks 之前(与引擎 handoff 同位)——
     * 桶此刻已冻结,消费侧可安全只读;同步形态下 processBucket 把桶推到 DONE 后,
     * maintainWatermarks 即在同一个 handoff 里把它惰性清出交接记账(异步形态下清出延后到
     * 下一个完结点,期间该桶以非 DONE 状态钉住 CQ 删除低水位——防 consumer 未回放先删档)。
     *
     * @param bucket 刚冻结的交接桶(HANDED_OFF,封箱元数据与快照已就绪)
     */
    void dispatchHandedOff(TxBuffer bucket) {
        if (consumerThread == null) {
            consumer.processBucket(bucket);   // 同步消费(测试锚定路径):回放与回调在调用线程内联完成
        } else {
            handoffQueue.add(bucket);         // 异步形态:交接入队,consumer 线程取走回放
        }
    }

    /**
     * 桶完结点统一收尾(交接/整桶丢弃时调用):先从交接记账里清掉已 DONE 的桶
     * (consumer 写 state、reader 清引用——reader 只读 state 的 volatile 值,清理是惰性的;
     * 同步形态下 processBucket 在 dispatchHandedOff 内已把本桶推 DONE,本步当即清出它)→
     * CQ 删除低水位检查(滚动文件回收,非 DONE 交接桶参与钉住)→ registry 剪枝(版本日志收缩,
     * <b>不含</b>交接桶——快照自足,交接桶回放不再查 registry)。只在 reader 线程调用。
     */
    private void maintainWatermarks() {
        handedOff.removeIf(b -> b.state == BucketState.DONE);
        releasePiped();
        pruneRegistryVersions();
    }

    /**
     * 开新桶并计入 LIVE 记账(begin/streamStart 首段/beginPrepare 共用)。
     * 只在 reader 线程调用。
     */
    private TxBuffer newBucket(long xid) {
        liveCount.incrementAndGet();
        return new TxBuffer(xid);
    }

    /**
     * 已交接桶的记账快照(测试面):包私有机动,仅供同包单测断言状态机保护(如 firstIndex 钉住
     * 低水位)——handedOff 是 reader 私有结构,DONE 惰性清理发生在完结点,本方法返回调用时刻
     * 的浅拷贝快照(List.copyOf)。只在 reader(测试)线程调用。
     */
    List<TxBuffer> handedOffForTest() {
        return List.copyOf(handedOff);
    }

    /**
     * 当前 LIVE 存活桶数(测试面,Task 4 裁定的锚定点之一):交接/整桶丢弃递减、开新桶递增。
     * 包私有机动,不是公开 API。
     */
    int liveBucketsForTest() {
        return liveCount.get();
    }

    /**
     * 输出前沿当前值(测试面):已输出事务 endLsn 的单调 max,End 之后才推进——同步形态下
     * 前沿由组装器内部自持,测试以本观测口断言"End 未达则前沿不动 / End 之后推进到 endLsn";
     * 异步形态前沿由调用方穿入同一实例(本观测口与调用方持有的引用同值)。包私有机动,
     * 不是公开 API。
     */
    long outputFrontierForTest() {
        return outputFrontier.get();
    }

    /**
     * 按当前低水位 {@link #pipeWatermark()} 让管道删除不再会被回读的过老滚动文件。
     * 单个文件删除失败由 pipe 内部 WARN 吸收不上抛——残留文件只是占磁盘,不影响正确性,
     * 下次水位推进会重试。管道恒存在(构造即建立),无需 null 守卫。
     */
    private void releasePiped() {
        pipe.releaseBelow(pipeWatermark());
    }

    /**
     * Relation 版本日志剪枝:以<b>所有存活桶的最老 index(firstIndex)</b>为低水位,调
     * {@link VersionedRelationRegistry#pruneBelow}(与 {@link #releasePiped} 一样挂在桶完结点)。
     *
     * <p>四路存活桶全部参与,包括 2PC 挂起池——挂起桶将来<b>交接时拷快照</b>仍会按它旧单元的
     * index 圈定版本,它的 firstIndex 必须继续保住对应版本。一个带单元的存活桶都没有时低水位取
     * Long.MAX_VALUE:现存版本不会再有人按旧 asOf 查,可以剪到每个 oid 只剩最新一条。
     * <b>已交接桶不参与</b>:它的回放走桶内快照,registry 的任何剪枝都不影响已冻结的渲染
     * 输入——这是"快照随行"换来的解耦红利。
     *
     * <p>正确性依据:pruneBelow 保留"低水位时刻正在生效"的那个版本(它自身的 seq 可以早于
     * 低水位——Relation 消息总是先于同表第一个 DML 到达),而存活桶的任何一次快照圈定/回放
     * 查询都不早于低水位,查不到被剪掉的部分。空桶(firstIndex&lt;0)不参与取最小值;
     * 每个桶完结时执行一次,消息热路径上没有开销。
     */
    private void pruneRegistryVersions() {
        long minSeq = Math.min(floor(currentNormalTx), floor(currentPrepareTx));
        for (TxBuffer bucket : streamedByXid.values()) {
            minSeq = Math.min(minSeq, floor(bucket));
        }
        for (TxBuffer bucket : preparedByGid.values()) {
            minSeq = Math.min(minSeq, floor(bucket));
        }
        registry.pruneBelow(minSeq);
    }

    /**
     * 计算当前 CQ 删除低水位 = min(存活桶 firstIndex, 非 DONE 交接桶 firstIndex,
     * maxAppendedIndex+1)。低于该 index 的队列条目永远不会再被回读,所在的滚动文件可以安全
     * 删除(保留哪些档位见 {@link MessagePipe#releaseBelow});一个带段的桶都没有时取
     * "最近写入 index+1"——已落盘的内容全部是垃圾。
     *
     * <p>边界:存活但还没写过单元的桶(firstIndex&lt;0)不参与取最小值(其未来单元的 index
     * 必然大于当前全部已 append 条目,不构成约束);交接桶按 state 过滤——DONE(consumer 已
     * 输出完成)不再约束,HANDED_OFF/OUTPUTTING 仍会被 readRange 读回必须钉住(state 是
     * volatile,本方法在 reader 线程读到的是 consumer 写入的即时值,语义即"此刻仍在途";
     * 同步形态下正常路径交接即 DONE 即清出,只有 listener 回调中截断/阻塞的桶会滞留钉住);
     * 管道刚建立未 append 过时 maxAppendedIndex=-1,水位为 0(空队列无物可删,天然安全)。
     * 包私有机动:仅供同包单测/探针断言低水位推进,不是公开 API。只在 reader 线程调用。
     *
     * @return 低水位 CQ index(≥0,无 -1 哨兵——管道恒存在)
     */
    long pipeWatermark() {
        long lowest = maxAppendedIndex + 1;
        lowest = Math.min(lowest, floor(currentNormalTx));
        lowest = Math.min(lowest, floor(currentPrepareTx));
        for (TxBuffer bucket : streamedByXid.values()) {
            lowest = Math.min(lowest, floor(bucket));
        }
        for (TxBuffer bucket : preparedByGid.values()) {
            lowest = Math.min(lowest, floor(bucket));
        }
        for (TxBuffer bucket : handedOff) {
            if (bucket.state != BucketState.DONE && bucket.firstIndex >= 0) {
                lowest = Math.min(lowest, bucket.firstIndex);
            }
        }
        return lowest;
    }

    /** 存活桶的 firstIndex 低水位候选(CQ 删除与 registry 剪枝两个低水位共用);桶 null 或空桶返回 Long.MAX_VALUE。 */
    private static long floor(TxBuffer bucket) {
        return (bucket == null || bucket.firstIndex < 0) ? Long.MAX_VALUE : bucket.firstIndex;
    }

    /**
     * 责任:非事务逻辑消息('M')的前沿推进护栏纯函数(MS3.5 spec §3.3 + 决策 L8 <b>全有或全无</b>)
     * ——给定消息自身 LSN 与 reader 维护的交接记账,返回可写入 {@code outputFrontier} 的推进值:
     * <b>没有未输出(pending)桶 → msgLsn(全有);有 → 完全不推进(全无,返回
     * {@link Long#MIN_VALUE},经调用点 {@code accumulateAndGet(max)} 天然 no-op——前沿一个
     * 字节都不动)</b>。
     * 简化理由(L8,取代 T1 的部分推进公式):部分推进 {@code min(msgLsn, min(pending.
     * commitLsn))} 的收益只是"积压期间的边际 WAL 修剪"(消息位到最老 pending commit 位
     * 之间的窗口),代价则是整类 commitLsn/endLsn off-by-one 推导风险(见"历史决策"段);
     * 而护栏的主场景是空闲库心跳(无 pending),两方案在该场景行为完全一致——收益边际、
     * 风险整类,砍掉计算本身。积压期间失去的部分推进可自愈(pending 桶输出完毕后下一条
     * 消息即全量推进,重复面只多不小)。
     *
     * <p><b>历史决策段(保留——它解释了为什么最终连这个计算都不要了)</b>:T1 版本为
     * 部分推进 {@code min(msgLsn, min(pending.commitLsn))},其中"为何是 commitLsn 而
     * <b>非</b> endLsn"的 off-by-one 论证:confirmed_flush 的服务端语义是"commit 结束位
     * ≤ 确认值的事务视为已送达,重启跳过"。若护栏在某未输出桶的 <b>endLsn</b> 上取 min,
     * 确认值 == 其 endLsn → 该桶被跳过 → 已输出头部还在、未输出尾部<b>永久丢失</b>;取
     * commitLsn(commit 记录自身 LSN,恒 &lt; endLsn)保证该桶 commit 结束位 &gt; 确认值 →
     * 重启<b>整桶重发</b>(头部重复允许,at-least-once,尾部补齐)。这条推导链上的每一步
     * (哪个字段、开闭区间、边界字节)都是必须永远保持正确的隐患面——L8 以"有 pending 就
     * 一个字节都不推"把该风险类整体消除:确认值停在 pending 事务的 commit 位之前<b>任意
     * 距离</b>处都安全,不再依赖字段级精确语义。行为级硬验收(重启尾部不丢)不变。
     *
     * <p>pending 判定 = {@code state != DONE}(HANDED_OFF/OUTPUTTING 均算)。DONE 排除
     * 正确性:consumer 次序"先写前沿后标 DONE",state 是 volatile——读到 DONE 即前沿已
     * 覆盖其 endLsn;读到旧值只是多余冻结(只多重复不丢),两方向安全无需加锁。
     * 边界与例外:<b>在途(live)桶刻意不进参数</b>——其安全性由 WAL 序论证承担(spec
     * §3.4:在途桶的 commit 在 WAL 序上必然晚于消息 X,PG restart_lsn 回放整体重发覆盖);
     * 参数只取交接记账({@link #handedOff} 形态的 deque),调用方传入时照抄即可。
     * 线程约束:纯函数;调用点在 reader 线程(非事务 'M' 的即时推进分支,
     * {@link #routeLogicalMsg} 已接线),读 state 的即时 volatile 值即"此刻仍在途"的语义。
     *
     * @param msgLsn    非事务逻辑消息自身的 LSN(消息头 lsn 字段)
     * @param handedOff 交接记账(pending = state != DONE 的桶;空 deque 即"全发完了"场景)
     * @return 无 pending 桶时 msgLsn(安全推进);有 pending 桶时 {@link Long#MIN_VALUE}
     *         (调用点 accumulateAndGet(max) 天然 no-op,不推进)
     */
    static long safeMessageAdvance(long msgLsn, Deque<TxBuffer> handedOff) {
        for (TxBuffer bucket : handedOff) {
            if (bucket.state != BucketState.DONE) {
                return Long.MIN_VALUE;   // 全无:任一 pending 桶即冻结(max 累加下 no-op)
            }
        }
        return msgLsn;
    }

    /** 桶存储形态的日志描述(段数 + CQ index 端点)——prepare/回滚的日志留痕用。 */
    private static String storageOf(TxBuffer bucket) {
        return "PIPE[" + bucket.segments.size() + " segs " + bucket.firstIndex + ".." + bucket.lastIndex + "]";
    }
}
