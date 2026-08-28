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
 * pgoutput **原始字节驱动**的事务组装状态机（spec §4 / assembly-spill 设计 §2-§5，MEMORY/SPILLED
 * 混合缓冲）：实现 {@link RawMessageListener}，按"轻窥路由 + 控制消息 live 解码"分流——数据消息
 * （I/U/D/T/M）不做完整解码，直接以 {@link PayloadUnit}（原始字节 + seq + 可选流式 xid）入桶；
 * 控制消息与 'R' 现场解码驱动桶状态机。收到提交信号（Commit/StreamCommit/CommitPrepared）后经
 * {@link BucketReplayer} **回放**桶内单元（decodeSingle + 按 asOf 版本渲染 Relation + aborted
 * 子事务过滤）封箱为不可变 {@link Transaction} 回调；回滚路径（RollbackPrepared/StreamAbort）
 * 丢弃或记账后不回调。
 *
 * <p>桶模型（与消息驱动版逐语义等价，等价基线 = 既有 33 例单测的移植）：
 * <ul>
 *   <li>普通事务：单指针 {@code currentNormalTx}——Commit 消息无 xid 字段，且 walsender 按
 *       LSN 序串行输出 Begin..Commit，同时至多一个活动普通事务</li>
 *   <li>流式事务：{@code streamedByXid} 多桶（key=StreamStart 的顶层 xid）+ {@code currentStream}
 *       流块上下文指针——多个并发大事务的流段会交错，流块本身不嵌套</li>
 *   <li>两阶段：活动期单指针 {@code currentPrepareTx}，Prepare/StreamPrepare 后转
 *       {@code preparedByGid} 挂起池等待 CommitPrepared（输出）/ RollbackPrepared（丢弃）</li>
 *   <li>StreamAbort：top==sub 整桶丢弃；否则记入 {@code abortedSubxids}，回放时过滤
 *       （数据保留至提交期一次性甄别，与消息驱动版的即时剔除可观察行为等价）</li>
 * </ul>
 *
 * <p>seq 分配：每条 onRaw（含控制消息与 'R'）取一次 {@code nextSeq++}（单调，从 1 起）；
 * 'R' 以到达时的 seq 记入 {@link VersionedRelationRegistry} 版本日志，数据单元回放时经
 * {@code registry.require(oid, unit.seq())} 取"变更时刻"的表定义（DDL 并发下的 asOf 正确性，
 * assembly-spill 设计 §4.4）。
 *
 * <p>混合缓冲（assembly-spill 设计 §2/§5）：桶存储双形态 {@link TxBuffer.Mode}——MEMORY 持内存
 * {@code List<PayloadUnit>}，SPILLED 持溢写池 CQ index 连续段（单元经 {@link SpoolFrame} 信封帧
 * 落盘，seq/streamXid 随字节走，堆内零逐单元元数据；并发桶的 append 在共享 appender 上交错，
 * 单桶条目按连续段记账、逐段回读，见 {@link TxBuffer#spillSegments}）。全局记账
 * {@code memoryBytes} = Σ 存活 MEMORY 桶 bytesTotal：任一 MEMORY 写入后越限（&gt; threshold）即
 * {@link #spillAll()} 把**所有** MEMORY 桶逐单元转储（正在追加的桶也在列）；开桶时水位已达阈值
 * （&gt;= threshold，来自其它未完结巨型桶）直接 SPILLED 起步。SPILLED 桶提交经
 * {@link MessageSpool#readRange} 回读解帧后走与 MEMORY 完全相同的 {@link BucketReplayer}——两种
 * 形态对同一字节流输出严格相等（spill 无损）。桶完结（提交回放后/abort 丢弃/回滚丢弃）维护低水位
 * {@link #spillWatermark()} = min(存活 SPILLED 桶 firstIndex, lastAppended+1)，交
 * {@link MessageSpool#releaseBelow} 删除过老滚动文件。spool 惰性创建（首次 spill 时）；
 * {@link SpillConfig#spillEnabled()}==false 时全路径短路（纯内存逃生门，spool 永不创建）。
 *
 * <p>线程约束：非线程安全。设计为在单一 run 循环线程内被调用（decoder 的 inStream 状态机与
 * 全部桶指针要求单写者）；输出的 Transaction 不可变，可跨线程传递。
 */
public final class TransactionAssembler implements RawMessageListener, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionAssembler.class);

    private final TransactionListener listener;
    /** live 解码器（控制消息 + 'R'）：自持流块状态机 inStream，与本类的 currentStream 指针同点同变。 */
    private final PgOutputDecoder decoder;
    /** Relation 版本日志：'R' 到达即记入（seq 戳），回放按单元 seq 取 asOf 版本。 */
    private final VersionedRelationRegistry registry;
    /** spill 配置：全局阈值/目录/滚动周期；{@link SpillConfig#spillEnabled()}==false 时全路径短路。 */
    private final SpillConfig spill;
    /** 每个解码点（控制消息、'R'、回放单元）回调——ConsoleListener 逐消息 DEBUG 的新挂点。 */
    private final Consumer<PgOutputMessage> decodedObserver;
    /** 桶回放器：提交路径把桶单元渲染为 TxChange（自持独立 decoder，decodeSingle 不触碰 inStream 实例状态）。 */
    private final BucketReplayer replayer;

    /** 下一个分配的消息序号（单调，从 1 起；每条 onRaw 消耗一次）。 */
    private long nextSeq = 1L;

    /** 溢写池（惰性单例）：首次 spill 时经 {@link #spool()} 建立；spill 禁用或未越限期间恒 null。 */
    private MessageSpool spool;
    /** 全局 MEMORY 记账 = Σ 存活 MEMORY 桶 bytesTotal（SPILLED 桶不占）；桶转储/完结时回退。 */
    private long memoryBytes;
    /** 最近一次向溢写池 append 的桶（null=尚未 append）：连续段判定——同桶相邻 append 顺延当前段，他人插队起新段。 */
    private TxBuffer lastSpillAppender;

    /** 活动普通事务桶（Begin 置位，Commit 封箱清空；协议保证 Begin..Commit 串行不嵌套）。 */
    private TxBuffer currentNormalTx;
    /** 活动两阶段事务桶（BeginPrepare 置位，Prepare 转挂起池）。 */
    private TxBuffer currentPrepareTx;
    /** 当前流块上下文：stream_start..stream_stop 之间非 null，指向 streamedByXid 中某桶。 */
    private TxBuffer currentStream;
    /** 流式事务桶，key=顶层 xid（多桶并存，段间交错——spec §4.2）。 */
    private final Map<Long, TxBuffer> streamedByXid = new HashMap<>();
    /** 两阶段挂起池，key=gid（PREPARE 至 COMMIT/ROLLBACK PREPARED 之间，可能长期挂起）。 */
    private final Map<String, TxBuffer> preparedByGid = new HashMap<>();

    /**
     * 组装桶（混合存储形态，spec §4）：xid/gid 定位事务，{@link Mode} 决定单元存储位置——
     * MEMORY 持内存 units（bytesTotal 参与全局水位记账，转储后冻结不再累计）；SPILLED 持溢写池
     * CQ index 区间（units 恒空，seq/streamXid 随 {@link SpoolFrame} 信封帧落盘、回读复原）。
     * SPILLED 桶的落盘条目按**连续段**（{@link #spillSegments}）记账：并发流式桶的追加会在共享
     * appender 上交错，单桶条目不保证整体连续（例如 spillAll 先转储 A 再转储 B，其后 A 的直写
     * 追加与 B 的 spillAll 条目相邻交错），故每遇"他人插队"起新段、回放逐段 readRange——堆内
     * 仍零逐单元元数据（每段仅一个 long[2]，段数 = 交错次数，非单元数）。firstIndex/lastIndex 为
     * 全部落盘条目的全局端点（水印与日志用）。abortedSubxids 收集被回滚子事务的 xid
     * （桶元数据，与存储形态无关，回放期过滤）。
     * 非线程安全（仅 run 线程触碰）。
     */
    private static final class TxBuffer {

        /** 桶存储形态：决定单元写入与提交回放的取数路径。 */
        private enum Mode {
            /** 堆内存储：units 持 PayloadUnit，bytesTotal 计入全局 memoryBytes。 */
            MEMORY,
            /** 溢写存储：单元在溢写池的若干连续段（spillSegments）中，units 恒空、不占堆记账。 */
            SPILLED
        }

        final long xid;
        String gid;
        Mode mode = Mode.MEMORY;
        /** SPILLED 落盘条目的全局端点（CQ index，含端点）；firstIndex&lt;0 表示尚无条目落盘（MEMORY 桶或空溢写桶）。 */
        long firstIndex = -1L;
        long lastIndex = -1L;
        /** SPILLED 的连续段列表（[first,last] 闭区间，追加序）；MEMORY 桶恒空。 */
        final ArrayDeque<long[]> spillSegments = new ArrayDeque<>();
        final List<PayloadUnit> units = new ArrayList<>();
        /** MEMORY 期间累计的单元字节数（全局 memoryBytes 的分项；转储后冻结，仅日志参考）。 */
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
     * 消费一条完整单条 pgoutput 消息的原始字节并推进组装状态机。
     *
     * <p>关键步骤：先分配 seq（每条消息一次，含控制消息与 'R'），再按类型字节路由——
     * <ul>
     *   <li>'B'/'C'/'S'/'E'/'c'/'A'/'b'/'P'/'K'/'r'/'p'：live 解码（decoder 顺带维护 inStream）
     *       后分发到对应控制规则（全部旧 fail-fast 语义逐条保留在各处理点）</li>
     *   <li>'R'：live 解码入 registry 版本日志（seq 戳），字节不入桶</li>
     *   <li>'I'/'U'/'D'/'T'：不做完整解码，窥流式前缀后以 PayloadUnit 入当前活动桶（miss fail-fast）</li>
     *   <li>'M'：先窥 flags bit0（流内偏移 5、顶层偏移 1）——事务性必须落在活动桶（无桶 fail-fast）；
     *       非事务性有活动桶随桶走、无桶 WARN 丢弃（协议允许，非异常）</li>
     *   <li>'Y'/'O'：DEBUG 记录后丢弃（旧组装器忽略语义）</li>
     *   <li>未知类型字节：经 decoder 抛 UnknownMessageTypeException（fail-fast 由解码层承担）</li>
     * </ul>
     *
     * <p>边界与异常语义：raw 为 null 抛 NPE；空数组违反调用契约（首字节访问抛数组越界）；
     * 桶缺失/重复/流块状态异常抛 {@link IllegalStateException}；字节与协议不符经 decoder 抛
     * 协议异常——均原样上抛终止会话线程。
     *
     * @param raw 完整单条消息字节（含类型字节与流式块内可选的 Int32 xid 前缀）
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
     * 责任：收敛溢写池资源（曾建立过 spool 时）。
     * 边界与异常语义：spool 从未建立（spill 禁用或全程未越限）为空操作；关闭失败仅 WARN 不上抛
     * （close 不应掩盖业务异常；{@link MessageSpool#close} 自身已逐段 WARN 吸收，此处兜底防
     * 意外异常逃逸）。线程：run 线程收尾或调用方线程调用一次，不可与 onRaw 并发。
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

    /**
     * 责任：live 解码一条消息并回调 decodedObserver（每个解码点的统一出口）。
     * 边界：字节与协议不符时由 decoder fail-fast 抛协议异常，不做任何捕获。
     */
    private PgOutputMessage decode(byte[] raw) {
        PgOutputMessage msg = decoder.decode(ByteBuffer.wrap(raw));
        decodedObserver.accept(msg);
        return msg;
    }

    /**
     * 责任：数据消息（I/U/D/T）入桶——不解码，仅窥流式前缀构造 PayloadUnit。
     * 关键步骤：取当前活动桶（流块上下文 → 两阶段 → 普通，miss fail-fast）→ 以
     * （raw, seq, currentStream 前缀 xid）经统一存储入口入桶（MEMORY 记账 / SPILLED 落盘由
     * {@link #storeUnit} 分流）。
     * 边界：无任何活动桶抛 ISE（异常消息经 {@link #describeData} 附触发消息上下文）。
     */
    private void routeData(byte[] raw, long seq) {
        TxBuffer bucket = activeBucket(() -> describeData(raw));
        appendUnit(bucket, raw, seq);
    }

    /**
     * 责任：LogicalMsg('M') 的轻窥路由——事务性与非事务性分流（旧语义逐条保留）。
     * 关键步骤：读 flags bit0（流内偏移 5：类型字节 + Int32 前缀之后；顶层偏移 1）→
     * 事务性：必须有活动桶（无桶 fail-fast，同 DML）→ 入桶；非事务性：有活动桶随桶走
     * （abort 剔除按 streamXid，语义安全），无桶 WARN 丢弃（协议允许其游离于任何事务之外）。
     * 边界：flags 偏移由 currentStream 判定，与 decoder 的 inStream 状态一致（同点同变）。
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
     * 责任：把一条数据消息字节包装为 PayloadUnit 并经统一存储入口入桶。
     * 关键步骤：流块内时前缀值取 raw[1..4] 无符号（与 decodeSingle 回放的解析契约一致：
     * 单元 streamXid 有值 ⇔ payload 带 4 字节前缀且值相等）→ 交 {@link #storeUnit} 按桶形态分流。
     */
    private void appendUnit(TxBuffer bucket, byte[] raw, long seq) {
        OptionalLong streamXid = currentStream != null
                ? OptionalLong.of(unsignedInt(raw, 1))
                : OptionalLong.empty();
        storeUnit(bucket, new PayloadUnit(raw, seq, streamXid));
    }

    /**
     * 责任：单元入桶的**统一存储入口**——按桶当前形态分流写入（spec §4.2）。
     * 关键步骤：MEMORY → units.add + 桶/全局字节记账，随后越限检查（严格 &gt;，spill 启用时）
     * 触发 {@link #spillAll}（当前正在追加的桶也在转储之列，转储后本桶后续写入走 SPILLED 分支）；
     * SPILLED → 信封帧化后 {@code spool.append} 落盘，index 经 {@link #appendSpillIndex} 记入
     * 桶的连续段（首个 append 建立 firstIndex）。
     * 边界与异常语义：CQ 写失败（磁盘满/IO）以 CQ 运行时异常自然上抛——不吞不重试（spec §6），
     * 会话线程随异常终止；spill 禁用时越限检查短路（恒 MEMORY，水位无上限）。
     * 线程：run 线程内（单写者，无并发）。
     */
    private void storeUnit(TxBuffer bucket, PayloadUnit unit) {
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
     * 责任：把一次成功 append 返回的 CQ index 记入桶的落盘区间（SPILLED 桶的唯一 index 记账点）。
     * 关键步骤：与上一次全局 append 同桶（{@code lastSpillAppender == bucket}）说明两Entry 在队列中
     * 相邻——顺延当前段；否则（他人插队，如另一桶的 spillAll/直写在本桶两次 append 之间发生）起新段
     * {@code [index,index]} → 维护全局端点 firstIndex/lastIndex → 前移 lastSpillAppender。
     * 边界：判定依据是**append 顺序**而非 index 数值差（cycle 滚动处 index 非连续递增，不可做算术判断）；
     * 段数 = 交错次数（每段一个 long[2]），堆内仍零逐单元元数据。
     * 线程：run 线程内（单写者，append 与记账原子相邻）。
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
     * 责任：取当前应接收变更的活动桶。
     * 查找顺序（spec §4.3）：流块上下文（最高优先）→ 活动两阶段桶 → 活动普通桶；
     * 三者皆空说明变更消息游离在任何事务外，协议流异常，fail-fast（trigger 惰性求值，
     * 仅异常路径承担描述开销）。
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

    /** 是否存在任一活动桶（流块/两阶段/普通三指针）——桶集合不变性的唯一判定入口，logicalMsg 丢弃路径等使用。 */
    private boolean hasActiveBucket() {
        return currentStream != null || currentPrepareTx != null || currentNormalTx != null;
    }

    /**
     * 责任：为 fail-fast 异常构造触发消息的描述（类型 + relationOid(s)，LogicalMsg 用 prefix + lsn），
     * 全部经固定偏移轻窥原始字节取得，不触发完整解码。
     * 边界：仅异常路径调用；'M' 的 prefix 扫描到首个 NUL，前缀按协议约定为短字符串。
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
     * 责任：开新桶并按全局水位决定初始存储形态（spec §4.2"开桶"）。
     * 关键步骤：spill 启用且 {@code memoryBytes >= threshold}（其它未完结巨型桶把水位顶到阈值）时
     * 直接 SPILLED 起步（空区间——首单元 append 时建立 firstIndex）；否则 MEMORY 起步。
     * 边界：写入侧越限判定是严格 &gt;（见 {@link #storeUnit}），水位恰好等于阈值时不触发转储——
     * 本处取 &gt;= 恰好接住该稳态边界；spill 禁用时恒 MEMORY。
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
     * StreamStart(xid, firstSegment)：xid 恒为顶层 xid（spec B.3——ReorderBufferStreamTXN 断言 toptxn，
     * firstSegment=!rbtxn_is_streamed(txn)）。
     *
     * <p>firstSegment=true（该顶层事务首段）→ 新建桶入 streamedByXid（已存在同 xid → fail-fast）；
     * false（后续段）→ 桶必须已存在（miss → fail-fast）。两种情况都切换 currentStream 到该桶。
     */
    private void streamStart(PgOutputMessage.StreamStart m) {
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
     * 责任：回放一个桶的存储单元为 TxChange 序列（提交路径的核心，MEMORY/SPILLED 双形态）——
     * SPILLED 桶先逐段经 {@link MessageSpool#readRange} 回读、{@link SpoolFrame#unframe} 复原为
     * 单元列表（段间按追加序拼接，交错他桶的条目天然不在本桶段内；abortedSubxids 等桶元数据
     * 不落盘、原样在用），随后与 MEMORY 桶走**同一** {@link BucketReplayer}（aborted 过滤、
     * asOf 版本渲染、decodeSingle + observer 全部由其承担）——两种形态回放语义严格一致。
     * 边界与异常语义：SPILLED 起步但尚无单元（firstIndex&lt;0）或 MEMORY 桶直接用 units（前者
     * 此时恒空）；Relation 未先行到达（require(oid, seq) miss）、段起点错位（已被低水位误删——
     * 不可能，水位保活）或字节与协议不符时抛 ISE/协议异常 fail-fast——回放失败即协议流异常，
     * 不允许半截事务输出（异常先于 listener 回调抛出）。
     * 线程：run 线程内同步执行。
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
     * 责任：把当前全部 MEMORY 桶整体转储为 SPILLED（越限响应，一次成批，spec §5）。
     * 关键步骤：先经 {@link #spool()} 建池（失败即抛，桶状态未动）→ 收集四路存活桶
     * （普通/两阶段活动 + 流式多桶 + 挂起池；currentStream 指向 streamedByXid 内桶不重复计）中
     * MEMORY 形态者 → 逐桶逐单元 frame→append 落盘（append 顺序逐桶成块，经
     * {@link #appendSpillIndex} 记为各桶的一个连续段）→ 置 SPILLED、清 units、memoryBytes 回退
     * 该桶 bytesTotal → INFO 一行（桶数/单元数/字节）。正在追加的桶也在转储之列：转储后其后续
     * 写入自然走 SPILLED 分支（若期间有他桶插队则起新段，见 appendSpillIndex）。
     * 边界与异常语义：CQ 写失败（磁盘满/IO）以 CQ 运行时异常自然上抛——不吞不重试（spec §6），
     * 此时桶集合可能停在部分转储的中间态，会话线程随异常终止（溢写池为瞬态工作区，重启整体清空，
     * 无一致性风险）；调用前置条件 memoryBytes&gt;threshold 保证至少存在一个非空 MEMORY 桶，
     * 即本方法至少落盘一个单元（{@code lastAppendedIndex} 的先 append 后查询契约由此成立）。
     * 线程：run 线程内由 {@link #storeUnit} 的越限检查同步调用（单写者）。
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
     * 责任：惰性取溢写池单例——首次 spill 时建立（INFO 留痕），之后复用同一实例
     * （appender 单写者与 index 记账依赖单例）。
     * 边界与异常语义：目录不可清空/队列建不起来时 {@link MessageSpool} 构造异常原样上抛
     * （fail-fast，spool 字段保持 null）；仅在 spill 启用的写入路径（spillAll / SPILLED 追加）可达，
     * {@code spillEnabled()==false} 时本方法不可被调用。
     * 线程：run 线程内（单写者）。
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
     * 责任：桶完结（移除出全部桶集合）后的统一收尾——MEMORY 记账回退 + 低水位维护。
     * 关键步骤：MEMORY 桶把 bytesTotal 从全局 memoryBytes 扣除（SPILLED 桶堆内无单元、不占记账，
     * bytesTotal 已在转储时回退过）→ {@link #releaseSpooled} 按当前低水位触发滚动文件删除检查。
     * 边界：spool 未建立时低水位维护为空操作；调用时机 = 桶已从对应集合并移除之后
     * （提交回放后 / StreamAbort 整桶丢弃 / RollbackPrepared 丢弃）。
     * 线程：run 线程内。
     */
    private void retireBucket(TxBuffer bucket) {
        if (bucket.mode == TxBuffer.Mode.MEMORY) {
            memoryBytes -= bucket.bytesTotal;
        }
        if (lastSpillAppender == bucket) {
            lastSpillAppender = null;   // 退役桶不再 append，解除引用（连续段判定退回"起新段"）
        }
        releaseSpooled();
    }

    /**
     * 责任：低水位维护——按 {@link #spillWatermark()} 让溢写池删除不再被回读的过老滚动文件。
     * 边界与异常语义：spool 未建立（从未 spill / 禁用）直接返回；单文件删除失败由 spool 内部
     * WARN 吸收不上抛（残留只占磁盘，正确性无损，下次推进重试）。
     * 线程：run 线程内。
     */
    private void releaseSpooled() {
        if (spool != null) {
            spool.releaseBelow(spillWatermark());
        }
    }

    /**
     * 责任：计算当前溢写低水位 = min(存活 SPILLED 桶 firstIndex, lastAppended+1)——低于该 index 的
     * 队列条目永不再被回读，所在滚动文件可安全删除（保留档位见 {@link MessageSpool#releaseBelow}）；
     * 无存活 SPILLED 桶时取 lastAppended+1（已落盘内容皆成垃圾）。
     * 边界与异常语义：spool 从未建立（spill 未发生或被禁用）返回哨兵 -1；SPILLED 起步但尚无单元的
     * 桶（firstIndex&lt;0）不参与 min；spool 非 null 时必有成功 append（spillAll 的前置保证），
     * lastAppendedIndex 不抛。
     * 线程：run 线程内；包私有仅供同包单测断言低水位推进（非公开 API）。
     *
     * @return 低水位 CQ index；溢写池未建立时 -1
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

    /**
     * 责任：取一个存活 SPILLED 桶的区间下界（参与低水位 min 的候选）。
     * 边界：桶为 null、MEMORY 形态、或 SPILLED 起步但尚无落盘单元（firstIndex&lt;0）时返回
     * {@link Long#MAX_VALUE}（不参与 min）。
     */
    private static long spilledFloor(TxBuffer bucket) {
        if (bucket != null && bucket.mode == TxBuffer.Mode.SPILLED && bucket.firstIndex >= 0) {
            return bucket.firstIndex;
        }
        return Long.MAX_VALUE;
    }

    /**
     * 责任：收集当前存活的全部桶（四路：普通活动、两阶段活动、流式多桶、挂起池）。
     * 说明：currentStream 指向 streamedByXid 中的桶，不单独收集（避免重复入列）；仅 spillAll
     * 遍历用，顺序无语义（各桶以自身 [first..last] 区间独立回读，跨桶 index 交错无关正确性）。
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

    /** 桶存储形态的日志描述（MEMORY 单元数/字节 或 SPILLED CQ 区间）——prepare/回滚留痕用。 */
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
