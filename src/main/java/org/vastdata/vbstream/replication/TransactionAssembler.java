package org.vastdata.vbstream.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * pgoutput 事务组装状态机：消费原始字节，把同一事务的变更攒进桶里，收到提交信号后回放成完整的
 * {@link Transaction} 交给监听器（spec §4 / assembly-spill 设计 §2-§5）。
 *
 * <p>与消息驱动旧版的核心区别：**数据消息先不解码**。Insert/Update/Delete/Truncate/Message
 * 五类消息的原始字节直接包成 {@link PayloadUnit} 入桶；控制消息和 Relation 才当场解码，驱动
 * 桶状态流转。解码推迟到提交那一刻——回滚的大事务从未被解码过，提交的事务也只解一次。
 *
 * <p>桶模型（语义与旧版逐条等价，等价基线 = 移植后的 33 例既有单测）：
 * <ul>
 *   <li>普通事务：单指针 {@code currentNormalTx}。Commit 消息不带 xid，且 walsender 按 LSN 序
 *       串行输出 Begin..Commit——同一时刻至多一个活动普通事务</li>
 *   <li>流式事务：{@code streamedByXid} 多桶并存（按 StreamStart 的顶层 xid 索引），
 *       {@code currentStream} 指向当前流块所属的桶。多个并发大事务的流段会交错，但流块本身不嵌套</li>
 *   <li>两阶段：活动期单指针 {@code currentPrepareTx}，Prepare/StreamPrepare 之后转入
 *       {@code preparedByGid} 挂起池，等 CommitPrepared（输出）或 RollbackPrepared（丢弃）</li>
 *   <li>StreamAbort：整事务回滚（top==sub）直接丢桶；子事务回滚只把 subxid 记入
 *       {@code abortedSubxids}，等回放时跳过对应单元——与旧版即时剔除的可观察行为等价</li>
 * </ul>
 *
 * <p>seq（消息序号）：每条消息到达时领取一个单调递增序号（控制消息与 Relation 也领）。Relation
 * 以到达时的 seq 记入 {@link VersionedRelationRegistry} 版本日志；回放时按单元自己的 seq 取
 * "变更那一刻"的表定义——事务中途若有并发 DDL，前后段的行仍按各自的表结构解释（设计 §4.4）。
 * 版本日志在每次桶完结时按存活桶的最老 seq 剪枝（2PC 挂起桶算存活），防止长期膨胀。
 *
 * <p>混合缓冲（组装期内存有界）——桶有两种存储形态 {@link TxBuffer.Mode}：
 * <ul>
 *   <li>MEMORY：单元留在堆里，字节计入全局水位 {@code memoryBytes}</li>
 *   <li>SPILLED：单元加 {@link SpoolFrame} 信封帧写入 Chronicle Queue，堆里不为单个单元保留
 *       任何数据，只记 CQ index 的连续段（并发桶的写入在队列里交错，见
 *       {@link TxBuffer#spillSegments}）</li>
 * </ul>
 * 任何一次堆内写入让水位越过阈值，就把全部 MEMORY 桶整体转储（{@link #spillAll}，正在追加的桶
 * 也在转储之列）；之后水位仍不低于阈值时，新开的桶直接以 SPILLED 起步。两种形态的提交回放走
 * 同一个 {@link BucketReplayer}——同一字节流的输出严格相等（spill 无损）。桶完结时按低水位
 * {@link #spillWatermark()} 让 {@link MessageSpool#releaseBelow} 删除过老的滚动文件。
 * 队列惰性创建（首次转储时才建）；spill 关闭（阈值 ≤0）时全程纯内存、永不建队列。
 *
 * <p>注意"内存有界"只覆盖组装期：提交回放仍会把整桶单元临时物化回堆，峰值 O(事务大小)，
 * 流式输出属里程碑 2 范畴。
 *
 * <p>线程约束：非线程安全。全部方法设计为由 run 循环的单一线程调用（decoder 的流块状态与
 * 全部桶指针都要求单写者）；产出的 Transaction 不可变，可跨线程传递。
 */
public final class TransactionAssembler implements RawMessageListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionAssembler.class);

    private final TransactionListener listener;
    /** live 解码器（只用于控制消息与 'R'）：维护流块状态 inStream，与 currentStream 指针同步变化。 */
    private final PgOutputDecoder decoder;
    /** Relation 版本日志：'R' 到达即记入（带 seq），回放时按单元 seq 取当时的版本。 */
    private final VersionedRelationRegistry registry;
    /** spill 配置（阈值/目录/滚动周期）；关闭（阈值 ≤0）时溢写路径全部短路。 */
    private final SpillConfig spill;
    /** 每个解码点（控制消息、'R'、回放单元）回调一次——ConsoleListener 逐消息 DEBUG 挂在这里。 */
    private final Consumer<PgOutputMessage> decodedObserver;
    /** 桶回放器：提交路径把桶单元渲染为 TxChange（自带独立 decoder，不影响本类的 live 解码状态）。 */
    private final BucketReplayer replayer;

    /** 下一个待分配的消息序号（从 1 起单调递增；每条 onRaw 消耗一个）。 */
    private long nextSeq = 1L;

    /** 溢写池（惰性单例）：首次转储时才经 {@link #spool()} 建立；spill 关闭或从未越限则始终为 null。 */
    private MessageSpool spool;
    /** 全局堆内水位 = 所有 MEMORY 桶的 bytesTotal 之和（SPILLED 桶不占）；桶转储或完结时回退。 */
    private long memoryBytes;
    /** 最近一次向溢写池写入的桶（null = 还没写过）。连续段判定依据：同桶连续写入就顺延上一段，
     *  中间夹了别的桶就新开一段。 */
    private TxBuffer lastSpillAppender;

    /** 活动普通事务桶（Begin 置位，Commit 封箱清空；协议保证 Begin..Commit 串行不嵌套）。 */
    private TxBuffer currentNormalTx;
    /** 活动两阶段事务桶（BeginPrepare 置位，Prepare 转挂起池）。 */
    private TxBuffer currentPrepareTx;
    /** 当前流块上下文：stream_start..stream_stop 之间非 null，指向 streamedByXid 中的某个桶。 */
    private TxBuffer currentStream;
    /** 流式事务桶，按顶层 xid 索引（多桶并存，段间交错——spec §4.2）。 */
    private final Map<Long, TxBuffer> streamedByXid = new HashMap<>();
    /** 两阶段挂起池，按 gid 索引（PREPARE 到 COMMIT/ROLLBACK PREPARED 之间，可能长期挂起）。 */
    private final Map<String, TxBuffer> preparedByGid = new HashMap<>();

    /**
     * 组装桶（spec §4）：xid/gid 定位事务，{@link Mode} 决定单元存在哪里——MEMORY 放堆内
     * units 列表（bytesTotal 计入全局水位，转储后冻结不再累计）；SPILLED 已全部落盘
     * （units 始终为空，seq/streamXid 随信封帧字节走，回读时复原）。
     *
     * <p>SPILLED 桶的落盘条目按**连续段**记账（{@link #spillSegments}）：多个桶共用一个队列，
     * 写入会互相交错，同一桶的条目在队列里不保证连成一片。判定规则：本次写入与上一次全局写入
     * 是同一个桶，两条记录在队列里就相邻，顺延当前段；中间夹了别的桶的写入，就新开一段。
     * 回放时逐段读回。堆内只为每个段保留一个 long[2]（段数 = 交错次数），不随单元数增长。
     * firstIndex/lastIndex 是全部落盘条目的全局端点（算低水位和打日志用）。
     *
     * <p>abortedSubxids 收集被回滚子事务的 xid（桶的元数据，与存储形态无关，回放时过滤）；
     * minSeq 记桶内最老单元的 seq（同样与存储形态无关，桶完结时与全部存活桶取最小值，
     * 驱动 {@link VersionedRelationRegistry#pruneBelow} 剪枝）。
     *
     * <p>非线程安全（只在 run 线程里被触碰）。
     */
    private static final class TxBuffer {

        /** 桶存储形态：决定单元往哪写、提交回放从哪取。 */
        private enum Mode {
            /** 堆内存储：单元在 units 列表里，bytesTotal 计入全局 memoryBytes。 */
            MEMORY,
            /** 落盘存储：单元在溢写池的若干连续段（spillSegments）里，units 始终为空、不占堆水位。 */
            SPILLED
        }

        final long xid;
        String gid;
        Mode mode = Mode.MEMORY;
        /** 落盘条目的全局端点（CQ index，含端点）；firstIndex&lt;0 表示还没写过任何条目。 */
        long firstIndex = -1L;
        long lastIndex = -1L;
        /** 桶内最老单元的 seq（MEMORY/SPILLED 两种形态都在 storeUnit 记账；无单元时为 Long.MAX_VALUE，不参与低水位取最小值）。 */
        long minSeq = Long.MAX_VALUE;
        /** 落盘条目的连续段列表（[first,last] 闭区间，按写入顺序排列）；MEMORY 桶始终为空。 */
        final ArrayDeque<long[]> spillSegments = new ArrayDeque<>();
        final List<PayloadUnit> units = new ArrayList<>();
        /** MEMORY 期间累计的单元字节数（全局 memoryBytes 的分项；转储后冻结，只用于日志）。 */
        long bytesTotal;
        final Set<Long> abortedSubxids = new HashSet<>();

        TxBuffer(long xid) {
            this.xid = xid;
        }
    }

    /**
     * 构造组装器。
     *
     * @param listener        完整事务到达时的回调（同步调用，调用线程与本组装器的调用线程一致）
     * @param mode            流式模式（仅影响 decoder 对 StreamAbort 附加字段的解析，须与
     *                        START_REPLICATION 的 streaming 参数一致，否则 abort 解析错位 fail-fast）
     * @param registry        Relation 版本日志（'R' 路由与回放渲染共用，本组装器独占写入）
     * @param spill           spill 配置（spillEnabled()==false 为纯内存逃生门：溢写路径全短路、
     *                        spool 永不创建）
     * @param decodedObserver 每个解码点回调（控制消息 + 'R' + 回放单元；Y/O 不解码不回调）
     */
    public TransactionAssembler(TransactionListener listener, StreamingMode mode,
            VersionedRelationRegistry registry, SpillConfig spill,
            Consumer<PgOutputMessage> decodedObserver) {
        this.listener = Objects.requireNonNull(listener, "listener");
        this.decoder = new PgOutputDecoder(Objects.requireNonNull(mode, "mode"));
        this.registry = Objects.requireNonNull(registry, "registry");
        this.spill = Objects.requireNonNull(spill, "spill");
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
        this.replayer = new BucketReplayer(mode, this.registry, this.decodedObserver);
    }

    /**
     * 便捷构造：解码观察者置为空消费（不需要逐消息透出的场景）。
     *
     * @param listener 完整事务到达时的回调
     * @param mode     流式模式
     * @param registry Relation 版本日志
     * @param spill    spill 配置
     */
    public TransactionAssembler(TransactionListener listener, StreamingMode mode,
            VersionedRelationRegistry registry, SpillConfig spill) {
        this(listener, mode, registry, spill, msg -> { });
    }

    /**
     * 消费一条完整的 pgoutput 消息原始字节，推进组装状态机。
     *
     * <p>先分配 seq（每条消息一个，控制消息与 Relation 也算），再按类型字节路由：
     * <ul>
     *   <li>'B'/'C'/'S'/'E'/'c'/'A'/'b'/'P'/'K'/'r'/'p'：当场解码（decoder 顺带维护流块状态），
     *       按控制规则处理——旧版的全部 fail-fast 语义逐条保留</li>
     *   <li>'R'：当场解码后记入 registry 版本日志（带 seq），字节不入桶</li>
     *   <li>'I'/'U'/'D'/'T'：不完整解码，窥一眼流式前缀后包成 PayloadUnit 入当前活动桶
     *       （没有活动桶则 fail-fast）</li>
     *   <li>'M'：先窥 flags 的 bit0 判断事务性——事务性必须落在活动桶里（无桶 fail-fast）；
     *       非事务性有桶随桶走、无桶 WARN 丢弃（协议允许，不算异常）</li>
     *   <li>'Y'/'O'：DEBUG 记录后丢弃（沿用旧组装器的忽略语义）</li>
     *   <li>未知类型字节：交给 decoder 抛 UnknownMessageTypeException（fail-fast 由解码层承担）</li>
     * </ul>
     *
     * <p>边界：raw 为 null 抛 NPE；空数组违反调用契约（首字节访问抛数组越界）；桶缺失/重复/
     * 流块状态异常抛 {@link IllegalStateException}；字节与协议不符经 decoder 抛协议异常——
     * 都原样上抛，终止会话线程。
     *
     * @param raw 完整单条消息字节（含类型字节，流式块内还含 Int32 xid 前缀）
     */
    @Override
    public void onRaw(byte[] raw) {
        Objects.requireNonNull(raw, "raw");
        long seq = nextSeq++;
        char type = (char) raw[0];
        // 注：record pattern switch 是 Java 21 正式特性，本项目约束 Java 17；此处 switch 的是
        // 类型字节（char）而非 record，合法；解码产物仍走 instanceof 链
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
            case 'R' -> registry.accept(seq, (PgOutputMessage.Relation) decode(raw));
            case 'I', 'U', 'D', 'T' -> routeData(raw, seq);
            case 'M' -> routeLogicalMsg(raw, seq);
            case 'Y', 'O' -> LOG.debug("元数据消息 '{}' 不入桶，直接丢弃", type);
            default -> decode(raw);   // 未知类型：解码层 fail-fast（UnknownMessageTypeException）
        }
    }

    /**
     * 关闭溢写池（如果建立过）。
     *
     * <p>从未建池（spill 关闭或全程没越限）则什么都不做。关闭失败只记 WARN 不上抛——close 不应
     * 掩盖业务异常（{@link MessageSpool#close} 内部已逐段 WARN 吸收，这里只兜住意外逃逸的异常）。
     * 在 run 线程收尾或调用方线程调用一次，不可与 onRaw 并发。
     */
    @Override
    public void close() {
        if (spool != null) {
            try {
                spool.close();
            } catch (RuntimeException e) {
                LOG.warn("溢写池关闭失败（忽略）", e);
            }
        }
    }

    /** 当场解码一条消息并回调 decodedObserver（所有 live 解码点的统一出口）；协议不符由 decoder 抛异常，不捕获。 */
    private PgOutputMessage decode(byte[] raw) {
        PgOutputMessage msg = decoder.decode(ByteBuffer.wrap(raw));
        decodedObserver.accept(msg);
        return msg;
    }

    /**
     * 数据消息（I/U/D/T）入桶：不解码，只窥一眼流式前缀，包成 PayloadUnit 存入当前活动桶。
     * 活动桶按"流块上下文 → 两阶段 → 普通"的顺序取，一个都没有就 fail-fast（异常消息带
     * {@link #describeData} 生成的触发消息上下文）。MEMORY 记账还是 SPILLED 落盘由
     * {@link #storeUnit} 分流。
     */
    private void routeData(byte[] raw, long seq) {
        TxBuffer bucket = activeBucket(() -> describeData(raw));
        appendUnit(bucket, raw, seq);
    }

    /**
     * LogicalMsg（'M'）的轻窥路由，沿用旧版的语义：
     * 读 flags 的 bit0 判断是否事务性（flags 在流式块内有 4 字节 xid 前缀，偏移 5；顶层偏移 1）。
     * 事务性消息必须落在活动桶里（没有桶就 fail-fast，与 DML 相同）；非事务性消息有桶就随桶走
     * （将来 abort 剔除按 streamXid 判断，语义安全），没有桶则 WARN 后丢弃——协议允许它游离在
     * 任何事务之外，不算异常。flags 偏移由 currentStream 是否在流块内决定，与 decoder 的
     * inStream 状态同步变化。
     */
    private void routeLogicalMsg(byte[] raw, long seq) {
        int flagsOffset = currentStream != null ? 5 : 1;   // 流内前缀 4 字节在前
        boolean transactional = (raw[flagsOffset] & 0x01) != 0;
        if (transactional || hasActiveBucket()) {
            TxBuffer bucket = activeBucket(() -> describeData(raw));
            appendUnit(bucket, raw, seq);
            return;
        }
        LOG.warn("非事务性消息游离于任何事务之外，丢弃: prefix={} lsn=0x{}",
                cstringAt(raw, flagsOffset + 1 + 8), Long.toHexString(longAt(raw, flagsOffset + 1)));
    }

    /**
     * 把一条数据消息字节包装成 PayloadUnit 交给 {@link #storeUnit} 入桶。
     * 在流式块内时，从 raw[1..4] 按无符号读出前缀 xid 作为 streamXid——与 decodeSingle 的
     * 回放契约对齐：单元的 streamXid 有值，当且仅当 payload 带 4 字节前缀且两者相等。
     */
    private void appendUnit(TxBuffer bucket, byte[] raw, long seq) {
        OptionalLong streamXid = currentStream != null
                ? OptionalLong.of(unsignedInt(raw, 1))
                : OptionalLong.empty();
        storeUnit(bucket, new PayloadUnit(raw, seq, streamXid));
    }

    /**
     * 单元入桶的统一入口，按桶当前形态分流（spec §4.2）。
     *
     * <p>步骤：先记桶的最老 seq（minSeq 取最小值，两种形态都要记，供桶完结时的 registry 剪枝用）。
     * MEMORY 形态：加入 units、累计字节到桶和全局水位，水位越过阈值（spill 启用时）就触发
     * {@link #spillAll}——正在追加的这个桶也在转储之列，转储完它后续的写入自然走 SPILLED 分支。
     * SPILLED 形态：单元帧化后写入溢写池，返回的 index 经 {@link #appendSpillIndex} 记入桶的
     * 连续段（首次写入时建立 firstIndex）。
     *
     * <p>边界：队列写失败（磁盘满/IO 错误）以 Chronicle 的运行时异常原样上抛，不吞不重试，
     * 会话线程随之终止（spec §6）；spill 关闭时跳过水位检查（永远是 MEMORY，水位无上限）。
     * 只在 run 线程内调用（单写者，无并发）。
     */
    private void storeUnit(TxBuffer bucket, PayloadUnit unit) {
        bucket.minSeq = Math.min(bucket.minSeq, unit.seq());
        if (bucket.mode == TxBuffer.Mode.MEMORY) {
            bucket.units.add(unit);
            bucket.bytesTotal += unit.payload().length;
            memoryBytes += unit.payload().length;
            if (spill.spillEnabled() && memoryBytes > spill.thresholdBytes()) {
                spillAll();
            }
            return;
        }
        appendSpillIndex(bucket, spool().append(SpoolFrame.frame(unit)));
    }

    /**
     * 把一次成功写入返回的 CQ index 记入桶的落盘区间（SPILLED 桶唯一记 index 的地方）。
     *
     * <p>连续段判定：本次写入与上一次全局写入是同一个桶（{@code lastSpillAppender == bucket}），
     * 两条记录在队列里就是相邻的，把当前段的右端推到 index；中间夹了别的桶的写入（比如另一个桶
     * 在本桶两次写入之间做了转储或直写），就新开一段 {@code [index,index]}。随后维护全局端点
     * firstIndex/lastIndex，并把 lastSpillAppender 指向本桶。
     *
     * <p>注意判定依据是**写入顺序**而不是 index 的数值差——cycle 滚动处 index 不连续递增，
     * 不能拿来做算术比较。堆内只为每个段保留一个 long[2]。
     * 只在 run 线程内调用（写入和记账紧挨着发生，无并发窗口）。
     */
    private void appendSpillIndex(TxBuffer bucket, long index) {
        if (lastSpillAppender == bucket) {
            bucket.spillSegments.peekLast()[1] = index;     // 同桶相邻：顺延当前段
        } else {
            bucket.spillSegments.addLast(new long[]{index, index});   // 他人插队：起新段
        }
        if (bucket.firstIndex < 0) {
            bucket.firstIndex = index;
        }
        bucket.lastIndex = index;
        lastSpillAppender = bucket;
    }

    /**
     * 取当前应接收变更的活动桶。
     * 查找顺序（spec §4.3）：流块上下文优先，其次是活动的两阶段桶，最后是普通桶。三者都为空
     * 说明变更消息游离在任何事务之外——协议流异常，fail-fast（触发消息的描述惰性求值，
     * 只有走异常路径才付出构造开销）。
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

    /** 是否存在任何一个活动桶（流块/两阶段/普通三处指针）——logicalMsg 的丢弃分支等场景判断用。 */
    private boolean hasActiveBucket() {
        return currentStream != null || currentPrepareTx != null || currentNormalTx != null;
    }

    /**
     * 为 fail-fast 异常生成触发消息的描述（类型 + relationOid；LogicalMsg 用 prefix + lsn）。
     * 信息全部按固定偏移从原始字节里窥出，不做完整解码。只在异常路径调用；'M' 的 prefix
     * 扫描到第一个 NUL 结束（前缀按协议约定是短字符串）。
     */
    private String describeData(byte[] raw) {
        int base = currentStream != null ? 5 : 1;   // 类型字节 + 可选 Int32 前缀之后
        return switch (raw[0]) {
            case 'I' -> "Insert relationOid=" + intAt(raw, base);
            case 'U' -> "Update relationOid=" + intAt(raw, base);
            case 'D' -> "Delete relationOid=" + intAt(raw, base);
            case 'T' -> {
                int n = intAt(raw, base);
                int[] oids = new int[n];
                for (int i = 0; i < n; i++) {
                    oids[i] = intAt(raw, base + 5 + 4 * i);   // I32 表数 + I8 选项位之后
                }
                yield "Truncate relationOids=" + Arrays.toString(oids);
            }
            case 'M' -> "LogicalMsg prefix=" + cstringAt(raw, base + 1 + 8);   // flags(1) + lsn(8) 之后
            default -> "'" + (char) raw[0] + "'";
        };
    }

    /** Begin：开新普通事务桶；已有未闭合普通事务即 fail-fast（协议上 Begin..Commit 不嵌套）。 */
    private void begin(PgOutputMessage.Begin m) {
        if (currentNormalTx != null) {
            throw new IllegalStateException("Begin 到达但普通事务未闭合: xid=" + currentNormalTx.xid);
        }
        currentNormalTx = newBucket(m.xid());
    }

    /**
     * 开新桶，并按全局水位决定它从哪种形态起步（spec §4.2）。
     * spill 启用且水位已达到阈值（其它未完结的巨型桶把水位顶了上去）时，新桶直接以 SPILLED
     * 起步——先是空区间，首个单元写入时才建立 firstIndex；否则从 MEMORY 起步。
     * 写入侧的越限判定是严格大于（见 {@link #storeUnit}），水位恰好等于阈值时不会触发转储，
     * 这里取 ≥ 正好接住这个边界情形；spill 关闭时永远是 MEMORY。
     */
    private TxBuffer newBucket(long xid) {
        TxBuffer bucket = new TxBuffer(xid);
        if (spill.spillEnabled() && memoryBytes >= spill.thresholdBytes()) {
            bucket.mode = TxBuffer.Mode.SPILLED;
        }
        return bucket;
    }

    /**
     * Commit（无 xid 字段）：回放当前普通事务桶并封箱 NORMAL Transaction 回调，清空指针；
     * 无桶即 fail-fast（异常带 commitLsn 定位）。回放完成后桶完结（记账回退 + 低水位维护）。
     */
    private void commit(PgOutputMessage.Commit m) {
        if (currentNormalTx == null) {
            throw new IllegalStateException("Commit 到达但无活动普通事务: commitLsn=0x"
                    + Long.toHexString(m.commitLsn()));
        }
        TxBuffer bucket = currentNormalTx;
        currentNormalTx = null;
        List<TxChange> changes = replay(bucket);
        retireBucket(bucket);
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.NORMAL, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), changes));
    }

    /**
     * StreamStart(xid, firstSegment)：xid 恒为顶层 xid（spec B.3——ReorderBufferStreamTXN 断言
     * toptxn；firstSegment 表示该事务此前还没被流式过）。
     *
     * <p>currentStream 非 null 意味着上一个流块没闭合（缺 'E'），fail-fast——流块不嵌套，
     * 与 'c'/'A'/'p'/'E' 各处理器的守卫一致。firstSegment=true（首个流段）新建桶放入
     * streamedByXid，同 xid 已有桶则 fail-fast；false（后续流段）要求桶已存在，查不到则
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
     * StreamStop：流块边界（消息不携带 xid——spec B.3）。currentStream 必须非 null（否则 fail-fast），
     * 置 null。流桶保留在 streamedByXid 中等待后续段或 StreamCommit/StreamAbort/StreamPrepare。
     */
    private void streamStop(PgOutputMessage.StreamStop m) {
        if (currentStream == null) {
            throw new IllegalStateException("StreamStop 到达但无进行中的流块");
        }
        currentStream = null;
    }

    /**
     * StreamCommit(xid)：顶层事务全部流段已收齐，回放桶并封箱 STREAMED Transaction 回调、移除桶；
     * 桶 miss 或仍有未闭合流块均 fail-fast（协议保证 stream_commit 必在流块外，spec B.3）。
     * 回放完成后桶完结（记账回退 + 低水位维护）。
     */
    private void streamCommit(PgOutputMessage.StreamCommit m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamCommit 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamCommit 对应流式事务桶不存在: xid=" + m.xid());
        }
        List<TxChange> changes = replay(bucket);
        retireBucket(bucket);
        listener.onTransaction(new Transaction(m.xid(), TransactionKind.STREAMED, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), changes));
    }

    /**
     * StreamAbort(top, sub)：已流式事务的（子）事务回滚（spec B.4）。
     *
     * <p>top==sub（整顶层回滚，decode 层"先子后顶"的最后一条）→ 移除整个桶（存储随之丢弃）；
     * 否则 sub 记入桶的 abortedSubxids，回放期过滤该（子）事务的单元（Message 单元前缀=顶层 xid，
     * 天然不命中 sub，不会误剔）。桶 miss 或流块未闭合均 fail-fast（abort 必在流块外）。
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
            retireBucket(bucket);       // 整桶丢弃：低水位候选推进（释放检查）
        } else {
            bucket.abortedSubxids.add(m.subxid());
        }
    }

    /** BeginPrepare：开活动两阶段桶（记 gid/xid）；已有未闭合两阶段桶即 fail-fast（b..P 串行不嵌套）。 */
    private void beginPrepare(PgOutputMessage.BeginPrepare m) {
        if (currentPrepareTx != null) {
            throw new IllegalStateException("BeginPrepare 到达但两阶段事务未闭合: gid=" + currentPrepareTx.gid);
        }
        currentPrepareTx = newBucket(m.xid());
        currentPrepareTx.gid = m.gid();
    }

    /**
     * Prepare：活动两阶段桶转挂起池（gid 已存在 → fail-fast）。
     * 事务自此挂起，等待 CommitPrepared（输出）或 RollbackPrepared（丢弃），可能长期等待甚至跨重启
     * （持久化非目标，spec §7）。
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
     * CommitPrepared：挂起池取桶（miss → fail-fast）回放并封箱 TWO_PHASE Transaction 回调
     * （用户确认的输出时机）。回放完成后桶完结（记账回退 + 低水位维护）。
     */
    private void commitPrepared(PgOutputMessage.CommitPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("CommitPrepared 对应 gid 不存在: " + m.gid());
        }
        List<TxChange> changes = replay(bucket);
        retireBucket(bucket);
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.TWO_PHASE, bucket.gid,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), changes));
    }

    /** RollbackPrepared：挂起池取桶（miss → fail-fast）静默丢弃，不回调（用户确认的回滚语义）；丢弃后低水位候选推进。 */
    private void rollbackPrepared(PgOutputMessage.RollbackPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("RollbackPrepared 对应 gid 不存在: " + m.gid());
        }
        LOG.warn("两阶段事务回滚，丢弃已缓冲变更: gid={} xid={} storage={}",
                m.gid(), bucket.xid, storageOf(bucket));
        retireBucket(bucket);
    }

    /**
     * StreamPrepare(xid, gid)：流式 2PC 的 prepare。流桶从 streamedByXid 移出（miss 或流块未闭合 →
     * fail-fast，stream_prepare 前服务端必已发完最后一个流段并 stream_stop，spec B.6），记 gid 后转挂起池。
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
     * 回放一个桶的存储单元，得到 TxChange 序列（提交路径的核心；MEMORY/SPILLED 两种形态）。
     *
     * <p>SPILLED 桶先逐段经 {@link MessageSpool#readRange} 读回、{@link SpoolFrame#unframe}
     * 还原成单元列表——段按写入顺序拼接，别的桶交错写入的条目天然不在本桶的段里；
     * abortedSubxids 这类桶元数据不落盘，内存里原样可用。还原出的单元列表与 MEMORY 桶的
     * units 走**同一个** {@link BucketReplayer}（aborted 过滤、按 asOf 取 Relation 版本、
     * 解码与 observer 回调都在它那里）——两种形态的回放语义完全一致。
     *
     * <p>边界：SPILLED 起步但还没写过单元（firstIndex&lt;0）的桶此时单元必然为空，与 MEMORY
     * 桶一样直接用 units。Relation 未先行到达（require 查不到）、回放中字节与协议不符时抛
     * ISE/协议异常——回放失败意味着协议流异常，不允许输出半截事务（异常先于 listener 回调抛出）。
     * 在 run 线程内同步执行。
     */
    private List<TxChange> replay(TxBuffer bucket) {
        if (bucket.mode != TxBuffer.Mode.SPILLED || bucket.firstIndex < 0) {
            return replayer.replay(bucket.units, bucket.abortedSubxids);
        }
        List<PayloadUnit> units = new ArrayList<>();
        for (long[] segment : bucket.spillSegments) {
            spool.readRange(segment[0], segment[1],
                    (framed, ordinal) -> units.add(SpoolFrame.unframe(framed)));
        }
        return replayer.replay(units, bucket.abortedSubxids);
    }

    /**
     * 把当前全部 MEMORY 桶整体转储为 SPILLED（水位越限的响应，一次成批处理，spec §5）。
     *
     * <p>步骤：先经 {@link #spool()} 建池（失败就抛，桶状态不动）→ 收集全部存活桶（普通/两阶段
     * 活动、流式多桶、挂起池；currentStream 指向的桶已在 streamedByXid 里，不重复计）里还是
     * MEMORY 形态的 → 逐桶逐单元帧化写入队列（一个桶的单元连续写入，正好构成该桶的一个连续段）→
     * 置为 SPILLED、清空 units、全局水位扣回该桶的字节 → INFO 记一行（桶数/单元数/字节数）。
     * 正在追加的桶也在转储之列，转储后它的后续写入自然走 SPILLED 分支。
     *
     * <p>边界：队列写失败（磁盘满/IO）以 Chronicle 运行时异常上抛，不吞不重试——此时桶集合
     * 可能停在转储到一半的中间状态，会话线程随异常终止（溢写池是瞬态工作区，重启时整体清空，
     * 没有一致性风险）。方法的前置条件 memoryBytes&gt;threshold 保证了至少有一个非空的 MEMORY 桶，
     * 也就是至少会写入一个单元——"先 append 后查询 lastAppendedIndex" 的契约由此成立。
     * 由 {@link #storeUnit} 的水位检查在 run 线程内同步调用。
     */
    private void spillAll() {
        MessageSpool target = spool();
        long bytesSpilled = memoryBytes;
        int bucketsSpilled = 0;
        long unitsSpilled = 0;
        for (TxBuffer bucket : liveBuckets()) {
            if (bucket.mode != TxBuffer.Mode.MEMORY) {
                continue;
            }
            for (PayloadUnit unit : bucket.units) {
                appendSpillIndex(bucket, target.append(SpoolFrame.frame(unit)));
                unitsSpilled++;
            }
            bucket.units.clear();
            bucket.mode = TxBuffer.Mode.SPILLED;
            memoryBytes -= bucket.bytesTotal;
            bucketsSpilled++;
        }
        LOG.info("spillAll：{} 个 MEMORY 桶（{} 单元/{}B）整体转储溢写池（threshold={}B，水位归零）",
                bucketsSpilled, unitsSpilled, bytesSpilled, spill.thresholdBytes());
    }

    /**
     * 惰性获取溢写池单例：首次转储时建立（INFO 留痕），之后始终复用同一实例——appender 的
     * 单写者约束和 index 记账都依赖单例。
     * 目录清不掉或队列建不起来时，{@link MessageSpool} 的构造异常原样上抛（fail-fast，
     * spool 字段保持 null）。只在 spill 启用的写入路径上会被调用；spill 关闭时本方法不可达。
     */
    private MessageSpool spool() {
        if (spool == null) {
            LOG.info("首次 spill，建立溢写池：dir={} rollCycle={} threshold={}B",
                    spill.dir(), spill.rollCycle(), spill.thresholdBytes());
            spool = new MessageSpool(spill.dir(), spill.rollCycle());
        }
        return spool;
    }

    /**
     * 桶完结（已从所有桶集合中移除）后的统一收尾：堆内水位回退 + 溢写低水位维护 + registry 剪枝。
     *
     * <p>MEMORY 桶把 bytesTotal 从全局水位里扣掉（SPILLED 桶的堆内没有单元，字节已在转储时扣过）；
     * 随后 {@link #releaseSpooled} 按当前低水位触发滚动文件的删除检查；
     * {@link #pruneRegistryVersions} 按存活桶的最老 seq 收缩 Relation 版本日志，防止长期膨胀。
     * spool 没建立时溢写侧是空操作（registry 剪枝与 spool 无关，总会执行）。
     * 调用时机 = 桶已经移除之后（提交回放后 / StreamAbort 整桶丢弃 / RollbackPrepared 丢弃）。
     */
    private void retireBucket(TxBuffer bucket) {
        if (bucket.mode == TxBuffer.Mode.MEMORY) {
            memoryBytes -= bucket.bytesTotal;
        }
        if (lastSpillAppender == bucket) {
            lastSpillAppender = null;   // 退役桶不再 append，解除引用（连续段判定退回"起新段"）
        }
        releaseSpooled();
        pruneRegistryVersions();
    }

    /**
     * 按当前低水位 {@link #spillWatermark()} 让溢写池删除不再会被回读的过老滚动文件。
     * spool 没建立（从未转储或 spill 关闭）就直接返回；单个文件删除失败由 spool 内部 WARN
     * 吸收不上抛——残留文件只是占磁盘，不影响正确性，下次水位推进会重试。
     */
    private void releaseSpooled() {
        if (spool != null) {
            spool.releaseBelow(spillWatermark());
        }
    }

    /**
     * Relation 版本日志剪枝：以**所有存活桶的最老 seq**（取最小值）为低水位，调
     * {@link VersionedRelationRegistry#pruneBelow}（与 {@link #releaseSpooled} 一样挂在桶完结点）。
     *
     * <p>四路存活桶全部参与，包括 2PC 挂起池——挂起桶将来回放时仍会按它旧单元的 seq 做
     * asOf 查询，它的 minSeq 必须继续保住对应版本。一个带单元的存活桶都没有时低水位取
     * Long.MAX_VALUE：现存版本不会再有人按旧 asOf 查，可以剪到每个 oid 只剩最新一条。
     *
     * <p>正确性依据：pruneBelow 保留"低水位时刻正在生效"的那个版本（它自身的 seq 可以早于
     * 低水位——Relation 消息总是先于同表第一个 DML 到达），而存活桶的任何一次 asOf 查询都
     * 不早于低水位，查不到被剪掉的部分。空桶（minSeq 为 Long.MAX_VALUE）不参与取最小值；
     * 每个桶完结时执行一次，消息热路径上没有开销。
     */
    private void pruneRegistryVersions() {
        long minSeq = Math.min(bucketFloor(currentNormalTx), bucketFloor(currentPrepareTx));
        for (TxBuffer bucket : streamedByXid.values()) {
            minSeq = Math.min(minSeq, bucketFloor(bucket));
        }
        for (TxBuffer bucket : preparedByGid.values()) {
            minSeq = Math.min(minSeq, bucketFloor(bucket));
        }
        registry.pruneBelow(minSeq);
    }

    /** 取一个存活桶的 registry 低水位候选（桶内最老单元的 seq）；桶为 null 或还没有单元时不参与取最小值。 */
    private static long bucketFloor(TxBuffer bucket) {
        return bucket == null ? Long.MAX_VALUE : bucket.minSeq;
    }

    /**
     * 计算当前溢写低水位 = min(存活 SPILLED 桶的 firstIndex, 最近写入 index+1)。
     * 低于该 index 的队列条目永远不会再被回读，所在的滚动文件可以安全删除（保留哪些档位见
     * {@link MessageSpool#releaseBelow}）；一个存活的 SPILLED 桶都没有时取"最近写入 index+1"
     * ——已落盘的内容全部是垃圾。
     *
     * <p>边界：溢写池从未建立（没转储过或 spill 关闭）返回哨兵值 -1；SPILLED 起步但还没写过
     * 单元的桶（firstIndex&lt;0）不参与取最小值；spool 非 null 时必有成功写入（spillAll 的前置
     * 保证），lastAppendedIndex 不会抛异常。包私有机动：仅供同包单测断言低水位推进，不是公开 API。
     *
     * @return 低水位 CQ index；溢写池未建立时返回 -1
     */
    long spillWatermark() {
        if (spool == null) {
            return -1L;
        }
        long lowest = spool.lastAppendedIndex() + 1;
        lowest = Math.min(lowest, spilledFloor(currentNormalTx));
        lowest = Math.min(lowest, spilledFloor(currentPrepareTx));
        for (TxBuffer bucket : streamedByXid.values()) {
            lowest = Math.min(lowest, spilledFloor(bucket));
        }
        for (TxBuffer bucket : preparedByGid.values()) {
            lowest = Math.min(lowest, spilledFloor(bucket));
        }
        return lowest;
    }

    /** 取一个存活 SPILLED 桶的区间下界（低水位候选）；桶为 null、还是 MEMORY 形态、或尚未写入任何条目时返回 Long.MAX_VALUE（不参与取最小值）。 */
    private static long spilledFloor(TxBuffer bucket) {
        if (bucket != null && bucket.mode == TxBuffer.Mode.SPILLED && bucket.firstIndex >= 0) {
            return bucket.firstIndex;
        }
        return Long.MAX_VALUE;
    }

    /**
     * 收集当前存活的全部桶（普通活动、两阶段活动、流式多桶、挂起池四路）。
     * currentStream 指向的桶就在 streamedByXid 里，不再单独加（避免重复）。只有 spillAll
     * 遍历时用，顺序没有语义——各桶按自己的区间独立回读，跨桶 index 交错不影响正确性。
     */
    private List<TxBuffer> liveBuckets() {
        List<TxBuffer> buckets = new ArrayList<>();
        if (currentNormalTx != null) {
            buckets.add(currentNormalTx);
        }
        if (currentPrepareTx != null) {
            buckets.add(currentPrepareTx);
        }
        buckets.addAll(streamedByXid.values());
        buckets.addAll(preparedByGid.values());
        return buckets;
    }

    /** 桶存储形态的日志描述（MEMORY 显示单元数/字节数，SPILLED 显示 CQ 区间）——prepare/回滚的日志留痕用。 */
    private static String storageOf(TxBuffer bucket) {
        return bucket.mode == TxBuffer.Mode.SPILLED
                ? "SPILLED[" + bucket.firstIndex + ".." + bucket.lastIndex + "]"
                : "MEMORY[" + bucket.units.size() + " units/" + bucket.bytesTotal + "B]";
    }

    /**
     * big-endian 读 4 字节有符号整数（oid 等，仅 fail-fast 描述与窥探用）。
     * 每字节先 &amp; 0xFF 再移位拼接——byte 是有符号类型，任一字节 ≥ 0x80 时直接 {@code |}
     * 拼接会把符号位扩散到全部高位（Task 12 集成实测：流式前缀 xid 758 的低字节 0xF6 使
     * {@code 512 | 0xFFFFFFF6 = 0xFFFFFFF6}，单元归属错位致 StreamAbort 剔除静默失效）。
     */
    private static int intAt(byte[] raw, int offset) {
        return ((raw[offset] & 0xFF) << 24) | ((raw[offset + 1] & 0xFF) << 16)
                | ((raw[offset + 2] & 0xFF) << 8) | (raw[offset + 3] & 0xFF);
    }

    /** big-endian 读 4 字节无符号整数入 long（流式前缀 xid；与 ByteBufferReader.readUnsignedInt 同语义）。 */
    private static long unsignedInt(byte[] raw, int offset) {
        return intAt(raw, offset) & 0xFFFFFFFFL;
    }

    /** big-endian 读 8 字节 long（LogicalMsg 的 lsn 窥探）。 */
    private static long longAt(byte[] raw, int offset) {
        return (unsignedInt(raw, offset) << 32) | unsignedInt(raw, offset + 4);
    }

    /** 读 offset 起的 null 结尾 UTF-8 字符串（LogicalMsg 的 prefix 窥探，仅异常/告警路径调用）。 */
    private static String cstringAt(byte[] raw, int offset) {
        int end = offset;
        while (raw[end] != 0) {
            end++;
        }
        return new String(raw, offset, end - offset, StandardCharsets.UTF_8);
    }
}
