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
 * 桶回放器（assembly-spill 设计 §2/§4.4）：把一个事务桶的存储单元（{@link PayloadUnit} 原始字节
 * 序列）回放渲染为 {@link TxChange} 序列——提交路径（Commit/StreamCommit/CommitPrepared）的核心，
 * 从 {@link TransactionAssembler} 抽取以获得机制级可测性（不经组装器直接手造单元驱动）。
 *
 * <p>逐单元三步：
 * <ul>
 *   <li>aborted 过滤：{@code streamXid} 命中 abortedSubxids（StreamAbort 记账的被回滚子事务）
 *       的单元直接跳过——不解码、不回调 observer（Message 单元前缀=顶层 xid，天然不命中 sub，
 *       不会误剔）</li>
 *   <li>解码：{@link PgOutputDecoder#decodeSingle(ByteBuffer, boolean)} 按单元自身
 *       {@code streamXid} 有无显式给定 inStream（免 S/E 包裹重建流块上下文），随后回调
 *       decodedObserver（回放也是解码点，ConsoleListener 逐消息透出）</li>
 *   <li>构造：I/U/D → {@link RowChange}（before/after 按 DML 语义）、T → {@link TruncateChange}
 *       （逐 oid 快照）、M → {@link MsgChange}；Relation 一律
 *       {@code registry.require(oid, unit.seq())} 取**变更时刻**版本（DDL 并发下 asOf 正确性，
 *       设计 §4.4——旧单元不能按新 schema 渲染）</li>
 * </ul>
 *
 * <p>边界与异常语义：单元类型字节非 I/U/D/T/M 抛 {@link IllegalStateException}（组装器路由保证
 * 桶内只可能落这五类，此处为防御性 fail-fast——'R'/'Y' 不入桶、控制消息更不可能）；Relation
 * 未先行到达（require miss）或字节与协议不符抛 ISE/协议异常——回放失败即协议流异常，不允许
 * 半截事务输出（异常先于组装器的 listener 回调抛出，由调用方保证）。空单元列表产出空列表
 * （空桶提交，协议合法）。
 *
 * <p>线程约束：非线程安全。设计为在组装器的单一 run 循环线程内被调用；自持独立
 * {@link PgOutputDecoder} 实例（decodeSingle 不读写实例 inStream 状态、白名单类型不分支于
 * StreamingMode，与组装器的 live 解码实例互不干扰）；产物 TxChange 不可变可跨线程传递。
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
     * 责任：按入桶序回放一组存储单元为 TxChange 序列（MEMORY 形态的回放入口）。
     * 关键步骤：遍历 units → streamXid 命中 abortedSubxids 的单元跳过（子事务回滚剔除，不解码
     * 不回调）→ 其余经 {@link #replayUnit} 解码渲染 → 汇集为列表（空输入产出空列表，协议合法）。
     * 边界与异常语义：units/abortedSubxids 为 null 抛 NPE；Relation miss 或字节与协议不符时抛
     * ISE/协议异常 fail-fast（先于组装器的 listener 回调，半截事务不输出）。
     * 线程：组装器 run 线程内同步执行。
     *
     * @param units          桶内单元（入桶序即回放射序）
     * @param abortedSubxids 被回滚的子事务 xid 集合（StreamAbort 记账；空集 = 无剔除）
     * @return 回放产物（与保留单元一一对应、保序；不可变数据的可变列表，调用方封箱后不再改动）
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
     * 责任：回放单个存储单元——类型字节守卫后 decodeSingle 解码（inStream 由单元自身 streamXid
     * 有无显式给定，免 S/E 包裹）并回调 decodedObserver，再按消息类型构造 TxChange，Relation 一律
     * {@code registry.require(oid, unit.seq())} 取变更时刻版本。
     * 关键步骤：类型守卫（非 I/U/D/T/M 即 ISE，防御性——组装器路由保证不可达）→ 解码 + observer →
     * instanceof 链分发（Java 17 约束，不用 record pattern）→ I/U/D 构造 RowChange（before/after
     * 按 DML 语义）、T 构造 TruncateChange（逐 oid 快照）、M 构造 MsgChange。
     * 边界与异常语义：payload 空数组违反契约（首字节访问抛数组越界）；类型守卫之后的
     * require miss / 协议错位异常原样上抛；instanceof 链末端对不可回放类型仍保留 ISE（双保险）。
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
