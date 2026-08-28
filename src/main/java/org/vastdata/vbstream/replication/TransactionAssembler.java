package org.vastdata.vbstream.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * pgoutput **原始字节驱动**的事务组装状态机（spec §4 / assembly-spill 设计 §2-§4，MEMORY-only
 * 第一阶段）：实现 {@link RawMessageListener}，按"轻窥路由 + 控制消息 live 解码"分流——数据消息
 * （I/U/D/T/M）不做完整解码，直接以 {@link PayloadUnit}（原始字节 + seq + 可选流式 xid）入桶；
 * 控制消息与 'R' 现场解码驱动桶状态机。收到提交信号（Commit/StreamCommit/CommitPrepared）后
 * **回放**桶内单元（decodeSingle + 按 asOf 版本渲染 Relation）封箱为不可变 {@link Transaction}
 * 回调；回滚路径（RollbackPrepared/StreamAbort）丢弃或记账后不回调。
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
 * <p>本阶段边界：桶仅持内存 {@code List<PayloadUnit>}（每桶记 bytesTotal）；{@link SpillConfig}
 * 进入构造器但 spill 未接线——spool 永不创建、{@link #close()} 空实现，溢写路径由后续任务接续。
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
    /** spill 配置：本阶段仅持有不使用（Task 10 接线全局水位与转储）。 */
    private final SpillConfig spill;
    /** 每个解码点（控制消息、'R'、回放单元）回调——ConsoleListener 逐消息 DEBUG 的新挂点。 */
    private final Consumer<PgOutputMessage> decodedObserver;

    /** 下一个分配的消息序号（单调，从 1 起；每条 onRaw 消耗一次）。 */
    private long nextSeq = 1L;

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
     * 组装桶（MEMORY 形态）：xid/gid 定位事务，units 持原始字节单元；bytesTotal 为桶内字节记账
     * （spill 阈值的分母，Task 10 启用）；abortedSubxids 收集被回滚子事务的 xid（回放期过滤）。
     * 非线程安全（仅 run 线程触碰）。
     */
    private static final class TxBuffer {
        final long xid;
        String gid;
        final List<PayloadUnit> units = new ArrayList<>();
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
     * @param spill           spill 配置（本阶段仅持有；Task 10 按 spillEnabled() 接线）
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

    /** 本阶段空实现（无 spool 资源）；Task 10 实装溢写池释放。 */
    @Override
    public void close() {
        // MEMORY-only：无资源需要收敛
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
     * （raw, seq, currentStream 前缀 xid）入桶并累计 bytesTotal。
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
     * 责任：把一条数据消息字节包装为 PayloadUnit 追加进桶并记账。
     * 关键步骤：流块内时前缀值取 raw[1..4] 无符号（与 decodeSingle 回放的解析契约一致：
     * 单元 streamXid 有值 ⇔ payload 带 4 字节前缀且值相等）→ units.add → bytesTotal 累加。
     */
    private void appendUnit(TxBuffer bucket, byte[] raw, long seq) {
        OptionalLong streamXid = currentStream != null
                ? OptionalLong.of(unsignedInt(raw, 1))
                : OptionalLong.empty();
        bucket.units.add(new PayloadUnit(raw, seq, streamXid));
        bucket.bytesTotal += raw.length;
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
        currentNormalTx = new TxBuffer(m.xid());
    }

    /**
     * Commit（无 xid 字段）：回放当前普通事务桶并封箱 NORMAL Transaction 回调，清空指针；
     * 无桶即 fail-fast（异常带 commitLsn 定位）。
     */
    private void commit(PgOutputMessage.Commit m) {
        if (currentNormalTx == null) {
            throw new IllegalStateException("Commit 到达但无活动普通事务: commitLsn=0x"
                    + Long.toHexString(m.commitLsn()));
        }
        TxBuffer bucket = currentNormalTx;
        currentNormalTx = null;
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.NORMAL, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), replay(bucket)));
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
            bucket = new TxBuffer(m.xid());
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
     */
    private void streamCommit(PgOutputMessage.StreamCommit m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamCommit 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamCommit 对应流式事务桶不存在: xid=" + m.xid());
        }
        listener.onTransaction(new Transaction(m.xid(), TransactionKind.STREAMED, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), replay(bucket)));
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
        } else {
            bucket.abortedSubxids.add(m.subxid());
        }
    }

    /** BeginPrepare：开活动两阶段桶（记 gid/xid）；已有未闭合两阶段桶即 fail-fast（b..P 串行不嵌套）。 */
    private void beginPrepare(PgOutputMessage.BeginPrepare m) {
        if (currentPrepareTx != null) {
            throw new IllegalStateException("BeginPrepare 到达但两阶段事务未闭合: gid=" + currentPrepareTx.gid);
        }
        currentPrepareTx = new TxBuffer(m.xid());
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
        LOG.debug("两阶段事务 PREPARE 入挂起池: gid={} changes={} pending={}",
                bucket.gid, bucket.units.size(), preparedByGid.size());
    }

    /** CommitPrepared：挂起池取桶（miss → fail-fast）回放并封箱 TWO_PHASE Transaction 回调（用户确认的输出时机）。 */
    private void commitPrepared(PgOutputMessage.CommitPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("CommitPrepared 对应 gid 不存在: " + m.gid());
        }
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.TWO_PHASE, bucket.gid,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), replay(bucket)));
    }

    /** RollbackPrepared：挂起池取桶（miss → fail-fast）静默丢弃，不回调（用户确认的回滚语义）。 */
    private void rollbackPrepared(PgOutputMessage.RollbackPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("RollbackPrepared 对应 gid 不存在: " + m.gid());
        }
        LOG.warn("两阶段事务回滚，丢弃已缓冲变更: gid={} xid={} changes={}",
                m.gid(), bucket.xid, bucket.units.size());
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
        LOG.debug("流式两阶段事务 StreamPrepare 入挂起池: gid={} changes={} pending={}",
                bucket.gid, bucket.units.size(), preparedByGid.size());
    }

    /**
     * 责任：回放一个桶的存储单元为 TxChange 序列（提交路径的核心，MEMORY 形态）。
     * 关键步骤：按入桶序遍历 units → streamXid 命中 abortedSubxids 的单元跳过（子事务回滚剔除）
     * → 其余经 {@link #replayUnit} 解码渲染 → 汇集为列表（空桶产出空列表，协议合法）。
     * 边界与异常语义：Relation 未先行到达（require(oid, seq) miss）或字节与协议不符时抛
     * ISE/协议异常 fail-fast——回放失败即协议流异常，不允许半截事务输出（异常先于 listener 回调抛出）。
     * 线程：run 线程内同步执行。
     */
    private List<TxChange> replay(TxBuffer bucket) {
        List<TxChange> changes = new ArrayList<>(bucket.units.size());
        for (PayloadUnit unit : bucket.units) {
            if (unit.streamXid().isPresent() && bucket.abortedSubxids.contains(unit.streamXid().getAsLong())) {
                continue;
            }
            changes.add(replayUnit(unit));
        }
        return changes;
    }

    /**
     * 责任：回放单个存储单元——decodeSingle 解码（inStream 由单元自身 streamXid 有无显式给定，
     * 免 S/E 包裹）后按消息类型构造 TxChange，Relation 一律 {@code registry.require(oid, unit.seq())}
     * 取变更时刻版本。
     * 关键步骤：解码并回调 decodedObserver → instanceof 链分发（Java 17 约束，不用 record pattern）
     * → I/U/D 构造 RowChange（before/after 按 DML 语义）、T 构造 TruncateChange（逐 oid 快照）、
     * M 构造 MsgChange。
     * 边界与异常语义：桶内只可能有 I/U/D/T/M（路由保证 'R'/'Y'/'O' 不入桶），其余类型到达即
     * ISE（防御性，正常不可达）。
     */
    private TxChange replayUnit(PayloadUnit unit) {
        PgOutputMessage msg = decoder.decodeSingle(
                ByteBuffer.wrap(unit.payload()), unit.streamXid().isPresent());
        decodedObserver.accept(msg);
        if (msg instanceof PgOutputMessage.Insert m) {
            return new RowChange(DmlKind.INSERT, registry.require(m.relationOid(), unit.seq()),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Update m) {
            return new RowChange(DmlKind.UPDATE, registry.require(m.relationOid(), unit.seq()),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Delete m) {
            return new RowChange(DmlKind.DELETE, registry.require(m.relationOid(), unit.seq()),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(oid -> registry.require(oid, unit.seq()))
                    .toList();
            return new TruncateChange(snapshots, m.options(), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.LogicalMsg m) {
            return new MsgChange(m.transactional(), m.prefix(), m.content(), m.streamXid());
        }
        throw new IllegalStateException("桶内出现不可回放的消息类型: " + msg.getClass().getSimpleName());
    }

    /** big-endian 读 4 字节有符号整数（oid 等，仅 fail-fast 描述与窥探用）。 */
    private static int intAt(byte[] raw, int offset) {
        return (raw[offset] << 24) | (raw[offset + 1] << 16) | (raw[offset + 2] << 8) | raw[offset + 3];
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
