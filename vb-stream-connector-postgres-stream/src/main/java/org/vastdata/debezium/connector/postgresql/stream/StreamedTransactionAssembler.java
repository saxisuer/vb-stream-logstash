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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p><b>MS2 Task 4 形态裁定</b>:本类只落<b>同步形态</b>的"冻结 + 记账"——交接分发是空骨架
 * ({@link #dispatchHandedOff},事件流 Begin → TxChange* → End 的发出属 Task 5 的
 * {@code TransactionConsumer.processBucket});Task 6 再加异步构造(consumer 线程与交接队列)
 * 与停机两形态。相对引擎的另两处接缝偏差:吞吐指标(引擎构造穿 ThroughputMetrics)整体删除,
 * MS5 以监听器形态加回;'R' 路由经 {@link RelationResolver}(见上)。
 *
 * <p>内存量级:组装期记账恒 O(桶元数据)(段数 × long[2] + oid/aborted 集合);提交回放逐单元
 * 解码逐条交付(Task 5 起,堆内 O(单条))。
 *
 * <p>线程约束:<b>reader 侧</b>(onRaw 及其全部私有路由/记账/registry/低水位维护)设计为由
 * run 循环的单一线程调用(decoder 的流块状态与全部桶指针都要求单写者);consumer 侧
 * (readRange、冻结桶回放、listener 回调、前沿累加、桶状态后两态)属 Task 5/6 的消费器,
 * 在 consumer 线程(同步形态即调用线程)执行。跨线程共享仅有:{@code TxBuffer.state}
 * (volatile)。
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
     * 构造<b>同步形态</b>组装器(Task 4 唯一形态):不开消费器——handoff 只完成"冻结 + 记账",
     * 事件流交付是 {@link #dispatchHandedOff} 的空骨架(Task 5 接 {@code TransactionConsumer.
     * processBucket},Task 6 再分流同步直调/异步入队)。急切建立管道——{@link MessagePipe}
     * 构造即清空目录建队列,失败原样上抛 fail-fast:管道是地基,建不起来就没有可运行形态。
     *
     * @param listener         事务事件流回调(Begin/TxChange/End 按序流式交付;本形态无人触发,
     *                         参数面按 Task 5 的消费器接线预留)
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
        this.decoder = new PgOutputStreamDecoder(Objects.requireNonNull(mode, "mode"));
        this.registry = Objects.requireNonNull(registry, "registry");
        this.relationResolver = Objects.requireNonNull(relationResolver, "relationResolver");
        Objects.requireNonNull(pipeDir, "pipeDir");
        Objects.requireNonNull(pipeRollCycle, "pipeRollCycle");
        this.pipe = new MessagePipe(pipeDir, pipeRollCycle);
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
        Objects.requireNonNull(listener, "listener");   // 参数面校验(本形态不持有,Task 5 的消费器接线消费)
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
     *       非事务性有桶随桶走、无桶 WARN 丢弃(协议允许,不算异常)</li>
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
     * 关闭组装器(Task 4 同步形态):无消费器与交接队列,直接关管道。
     * {@link MessagePipe#close} 内部已逐资源 WARN 吸收,这里只兜住意外逃逸的异常——
     * close 不应掩盖业务异常(最坏代价是句柄延迟回收)。在 run 线程收尾或调用方线程调用
     * 一次,不可与 onRaw 并发。Task 6 的异步形态在此之前追加毒丸排干协议
     * (毒丸入队 → join consumer 60s → 关管道——已提交未输出的事务不丢)。
     */
    @Override
    public void close() {
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
     * LogicalMsg('M')的轻窥路由,沿用引擎语义:
     * 读 flags 的 bit0 判断是否事务性(flags 在流式块内有 4 字节 xid 前缀,偏移 5;顶层偏移 1)。
     * 事务性消息必须落在活动桶里(没有桶就 fail-fast,与 DML 相同);非事务性消息有桶就随桶走
     * (将来 abort 剔除按 streamXid 判断,语义安全),没有桶则 WARN 后丢弃——协议允许它游离在
     * 任何事务之外,不算异常。flags 偏移由 currentStream 是否在流块内决定,与 decoder 的
     * inStream 状态同步变化。丢弃路径的 owner 置空属<b>防御性</b>(当前控制消息路径已统一置空;
     * 此路径到达时无任何活动桶,owner 至多指向已退役/已交接的桶——它们不会再成为追加目标,
     * 置空与否不影响段判定)。
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
        LOG.warn("非事务性消息游离于任何事务之外,丢弃: prefix={} lsn=0x{}",
                RawPeeks.cstringAt(raw, flagsOffset + 1 + 8), Long.toHexString(RawPeeks.longAt(raw, flagsOffset + 1)));
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
     * handedOff 记账 → 退出 LIVE 记账 → {@link #dispatchHandedOff}(Task 4 空骨架)→ 维护
     * 低水位。立即返回——reader 路径从此不含回放。只在 reader 线程调用。
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
        dispatchHandedOff(bucket);   // Task 5:同步直调 consumer.processBucket;Task 6:分流入队
        maintainWatermarks();
    }

    /**
     * 交接分发钩子(MS2 Task 4 裁定的语义骨架,包私有接缝):<b>本任务为空实现</b>——
     * 事件流(Begin → TxChange* → End)的发出属 Task 5 的 {@code TransactionConsumer.
     * processBucket}(引擎同步形态在 handoff 里直调它,回放与回调对 onRaw 同步可见);
     * Task 6 的异步形态再按 consumer 线程是否存在分流(直调 vs 交接队列入队)。
     * 调用点位于 liveCount 递减之后、maintainWatermarks 之前(与引擎 handoff 同位)——
     * 桶此刻已冻结,消费侧可安全只读。
     *
     * @param bucket 刚冻结的交接桶(HANDED_OFF,封箱元数据与快照已就绪)
     */
    void dispatchHandedOff(TxBuffer bucket) {
        // Task 4 骨架:不交付(见方法 javadoc)——引擎对位的 throughputMetrics.onTxHandedOff()
        // 组装完成记账亦随 metrics 接缝整体删除,MS5 以监听器形态加回
    }

    /**
     * 桶完结点统一收尾(交接/整桶丢弃时调用):先从交接记账里清掉已 DONE 的桶
     * (consumer 写 state、reader 清引用——reader 只读 state 的 volatile 值,清理是惰性的,
     * 恰好发生在下一个完结点;Task 4 无 consumer,DONE 不会出现,此步空转)→ CQ 删除低水位
     * 检查(滚动文件回收,非 DONE 交接桶参与钉住)→ registry 剪枝(版本日志收缩,
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
     * Task 4 无 consumer,交接桶恒 HANDED_OFF——滞留桶钉住水位是本形态的预期语义);
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

    /** 桶存储形态的日志描述(段数 + CQ index 端点)——prepare/回滚的日志留痕用。 */
    private static String storageOf(TxBuffer bucket) {
        return "PIPE[" + bucket.segments.size() + " segs " + bucket.firstIndex + ".." + bucket.lastIndex + "]";
    }
}
