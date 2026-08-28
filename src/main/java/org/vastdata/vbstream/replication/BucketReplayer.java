package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 桶回放器（assembly-spill 设计 §2/§4.4）：把一个事务桶的存储单元（{@link PayloadUnit} 原始字节）
 * 逐条解码、渲染成 {@link TxChange} 序列。它是提交路径（Commit/StreamCommit/CommitPrepared）的
 * 核心；从 {@link TransactionAssembler} 抽出来单独成类，是为了能绕过组装器直接手造单元做测试。
 *
 * <p>每个单元三步：
 * <ul>
 *   <li>aborted 过滤：单元的 {@code streamXid} 命中 abortedSubxids（StreamAbort 记下的被回滚
 *       子事务）就直接跳过——不解码、不回调 observer。LogicalMsg 单元的前缀是顶层 xid，
 *       不会撞上子事务的 subxid，不会被误删</li>
 *   <li>解码：用 {@link PgOutputDecoder#decodeSingle(ByteBuffer, boolean)}，inStream 由单元自身
 *       的 {@code streamXid} 有无直接给定——不用拿 'S'/'E' 把流块上下文包一遍。解完回调
 *       decodedObserver（回放也是解码点，逐消息 DEBUG 在这里透出）</li>
 *   <li>构造：I/U/D 生成 {@link RowChange}，T 生成 {@link TruncateChange}（逐个 oid 取快照），
 *       M 生成 {@link MsgChange}；Relation 一律用 {@code registry.require(oid, unit.seq())} 取
 *       **变更时刻**的版本——DDL 并发时旧单元不能按新表结构解释（设计 §4.4）</li>
 * </ul>
 *
 * <p>边界：单元类型不是 I/U/D/T/M 抛 {@link IllegalStateException}——组装器的路由保证桶里只会
 * 有这五类，这里是防御性的 fail-fast；Relation 未先行到达（require 查不到）或字节与协议不符，
 * 抛 ISE/协议异常——回放失败即协议流异常，不允许输出半截事务（异常在组装器回调 listener 之前
 * 抛出，由调用方保证）。空的单元列表产出空列表（空桶提交是合法的）。
 *
 * <p>线程约束：非线程安全，在组装器的 run 线程内调用。自带一个独立的
 * {@link PgOutputDecoder}（decodeSingle 不碰实例的流块状态，与组装器的 live 解码互不干扰）；
 * 产出的 TxChange 不可变，可跨线程传递。
 */
final class BucketReplayer {

    private final PgOutputDecoder decoder;
    /** Relation 版本日志：回放按单元 seq 取 asOf 版本（与组装器共用同一实例，组装器写、回放读）。 */
    private final VersionedRelationRegistry registry;
    /** 每个回放解码点回调（与组装器 live 解码共用同一 observer，语义一致）。 */
    private final Consumer<PgOutputMessage> decodedObserver;

    /**
     * 构造回放器。
     *
     * @param mode            流式模式（构造自持 decoder 用；白名单类型的解析不分支于模式档位，
     *                        仅为与组装器的 live decoder 同构保留）
     * @param registry        Relation 版本日志（require(oid, seq) 取变更时刻版本）
     * @param decodedObserver 每个回放解码点回调（被 aborted 过滤跳过的单元不触发）
     */
    BucketReplayer(StreamingMode mode, VersionedRelationRegistry registry,
            Consumer<PgOutputMessage> decodedObserver) {
        this.decoder = new PgOutputDecoder(Objects.requireNonNull(mode, "mode"));
        this.registry = Objects.requireNonNull(registry, "registry");
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
    }

    /**
     * 按入桶顺序把一组存储单元回放成 TxChange 序列（MEMORY 形态的回放入口）。
     *
     * <p>遍历 units：streamXid 命中 abortedSubxids 的单元跳过（子事务回滚剔除，不解码也不
     * 回调）；其余交给 {@link #replayUnit} 解码渲染，结果按序收集。空输入产出空列表
     * （空桶提交是合法的）。
     *
     * <p>边界：units/abortedSubxids 为 null 抛 NPE；Relation 查不到或字节与协议不符时抛
     * ISE/协议异常（在组装器回调 listener 之前抛出，不输出半截事务）。在组装器的 run 线程内
     * 同步执行。
     *
     * @param units          桶内单元（入桶顺序即回放顺序）
     * @param abortedSubxids 被回滚的子事务 xid 集合（StreamAbort 记下的；空集 = 不剔除任何单元）
     * @return 回放产物（与保留的单元一一对应、保序）
     */
    List<TxChange> replay(Iterable<PayloadUnit> units, Set<Long> abortedSubxids) {
        Objects.requireNonNull(units, "units");
        Objects.requireNonNull(abortedSubxids, "abortedSubxids");
        List<TxChange> changes = new ArrayList<>();
        for (PayloadUnit unit : units) {
            if (unit.streamXid().isPresent() && abortedSubxids.contains(unit.streamXid().getAsLong())) {
                continue;
            }
            changes.add(replayUnit(unit));
        }
        return changes;
    }

    /**
     * 回放单个存储单元：先做类型守卫，再解码、回调 decodedObserver，最后按消息类型构造 TxChange。
     *
     * <p>类型守卫：不是 I/U/D/T/M 就抛 ISE——组装器的路由保证不可达，属防御性检查。解码用
     * decodeSingle，inStream 由单元 streamXid 的有无直接给定。构造时 Relation 一律用
     * {@code registry.require(oid, unit.seq())} 取变更时刻的版本；消息分发走 instanceof 链
     * （Java 17 没有 record pattern switch）。
     *
     * <p>边界：payload 为空数组违反契约（首字节访问抛数组越界）；守卫通过后 require 查不到或
     * 协议错位的异常原样上抛；instanceof 链末端保留一道 ISE（双保险）。
     */
    private TxChange replayUnit(PayloadUnit unit) {
        char type = (char) unit.payload()[0];
        switch (type) {
            case 'I', 'U', 'D', 'T', 'M' -> { /* 可回放的数据消息——组装器路由只可能落这五类 */ }
            default -> throw new IllegalStateException(
                    "桶内单元非可回放的数据消息类型: '" + type + "'（期望 I/U/D/T/M）");
        }
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
}
