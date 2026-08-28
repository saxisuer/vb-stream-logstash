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
import java.util.OptionalLong;
import java.util.function.Consumer;

/**
 * 桶回放器（assembly-spill 设计 §2/§4.4，1.7 改管道驱动）：把一个事务桶（{@link TxBuffer} 的
 * CQ index 连续段）从 {@link MessagePipe} 逐段读回、逐条解码渲染成 {@link TxChange} 序列。
 * 它是提交路径（Commit/StreamCommit/CommitPrepared）的核心；从 {@link TransactionAssembler}
 * 抽出来单独成类，是为了能绕过组装器直接手造桶/管道做测试。
 *
 * <p>与 1.6 的差别：单元不再以 {@code PayloadUnit} 列表传入，而是"桶的段 + 管道"按 index 读回
 * （readRange 保证段内全部是本桶单元——段是追加期按"上一次 append 归属"划出来的）；Relation 的
 * asOf 解析经 {@link RelationResolver} 解耦——单线程形态传 {@code registry::require}，
 * 解耦形态（Task 6 起）传桶交接快照，回放器自身不再持有 registry。
 *
 * <p>每个单元三步：
 * <ul>
 *   <li>aborted 过滤：桶级 hasPrefix 时重窥 raw[1..4] 得 streamXid，命中 abortedSubxids
 *       （StreamAbort 记下的被回滚子事务）就直接跳过——不解码、不回调 observer。LogicalMsg
 *       单元的前缀是顶层 xid，不会撞上子事务的 subxid，不会被误删</li>
 *   <li>解码：用 {@link PgOutputDecoder#decodeSingle(ByteBuffer, boolean)}，inStream 由桶级
 *       hasPrefix 直接给定——不用拿 'S'/'E' 把流块上下文包一遍。解完回调 decodedObserver
 *       （回放也是解码点，逐消息 DEBUG 在这里透出）</li>
 *   <li>构造：I/U/D 生成 {@link RowChange}，T 生成 {@link TruncateChange}（逐个 oid 取快照），
 *       M 生成 {@link MsgChange}；Relation 一律用 {@code resolver.require(oid, index)} 取
 *       **变更时刻**的版本（index 即单元 seq）——DDL 并发时旧单元不能按新表结构解释（设计 §4.4）</li>
 * </ul>
 *
 * <p>边界：单元类型不是 I/U/D/T/M 抛 {@link IllegalStateException}——组装器的路由保证管道里
 * 只有本桶的数据单元落在桶的段内，这里是防御性的 fail-fast；Relation 未先行到达（require 查不到）
 * 或字节与协议不符，抛 ISE/协议异常——回放失败即协议流异常，不允许输出半截事务（异常在组装器
 * 回调 listener 之前抛出，由调用方保证）。空桶（无段）产出空列表（空桶提交是合法的）。
 *
 * <p>线程约束：单线程内同步执行（本任务形态在组装器的 run 线程内调用 pipe.readRange——reader
 * 侧线程调 consumer 侧方法属合法过渡，Task 6 起移交真正的 consumer 线程）。自带一个独立的
 * {@link PgOutputDecoder}（decodeSingle 不碰实例的流块状态，与组装器的 live 解码互不干扰）；
 * 产出的 TxChange 不可变，可跨线程传递。
 */
final class BucketReplayer {

    /** Relation asOf 解析器：1.7 起回放与 registry 解耦——单线程形态传 registry::require，解耦形态传桶快照。 */
    @FunctionalInterface
    interface RelationResolver {
        PgOutputMessage.Relation require(int relationOid, long asOfSeq);
    }

    private final PgOutputDecoder decoder;
    /** 每个回放解码点回调（与组装器 live 解码共用同一 observer，语义一致）。 */
    private final Consumer<PgOutputMessage> decodedObserver;

    /**
     * 构造回放器。
     *
     * @param mode            流式模式（构造自持 decoder 用；白名单类型的解析不分支于模式档位，
     *                        仅为与组装器的 live decoder 同构保留）
     * @param decodedObserver 每个回放解码点回调（被 aborted 过滤跳过的单元不触发）
     */
    BucketReplayer(StreamingMode mode, Consumer<PgOutputMessage> decodedObserver) {
        this.decoder = new PgOutputDecoder(Objects.requireNonNull(mode, "mode"));
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
    }

    /**
     * 回放一个桶：逐段 readRange（readRange 保证段内全部是本桶单元），逐单元三步——
     * aborted 过滤（hasPrefix 时重窥 raw[1..4] 得 streamXid，命中 abortedSubxids 跳过）→
     * decodeSingle(payload, bucket.hasPrefix) → 构造 TxChange（Relation 经 resolver.require(oid, index)，
     * index 即单元 seq）。空桶产出空列表。
     *
     * <p>边界：bucket/pipe/resolver 为 null 抛 NPE；Relation 查不到或字节与协议不符时抛
     * ISE/协议异常（在组装器回调 listener 之前抛出，不输出半截事务）。单线程内同步执行。
     *
     * @param bucket   待回放的桶（段列表与 abortedSubxids 元数据）
     * @param pipe     单元字节所在的管道（按段读回，payload 为副本）
     * @param resolver Relation asOf 解析器（index 作 asOf seq）
     * @return 回放产物（与保留的单元一一对应、按段序保序）
     */
    List<TxChange> replay(TxBuffer bucket, MessagePipe pipe, RelationResolver resolver) {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(pipe, "pipe");
        Objects.requireNonNull(resolver, "resolver");
        List<TxChange> changes = new ArrayList<>();
        for (long[] segment : bucket.segments) {
            pipe.readRange(segment[0], segment[1], (index, payload) -> {
                OptionalLong streamXid = bucket.hasPrefix
                        ? OptionalLong.of(RawPeeks.unsignedInt(payload, 1))
                        : OptionalLong.empty();
                if (streamXid.isPresent() && bucket.abortedSubxids.contains(streamXid.getAsLong())) {
                    return;
                }
                changes.add(replayUnit(payload, index, streamXid, resolver));
            });
        }
        return changes;
    }

    /**
     * 回放单个存储单元：先做类型守卫，再解码、回调 decodedObserver，最后按消息类型构造 TxChange。
     *
     * <p>类型守卫：不是 I/U/D/T/M 就抛 ISE——组装器的路由保证不可达，属防御性检查。解码用
     * decodeSingle，inStream 由桶级 hasPrefix 决定（经 streamXid 的有无表达）。构造时 Relation
     * 一律用 {@code resolver.require(oid, seq)} 取变更时刻的版本；消息分发走 instanceof 链
     * （Java 17 没有 record pattern switch）。
     *
     * <p>边界：payload 为空数组违反契约（首字节访问抛数组越界）；守卫通过后 require 查不到或
     * 协议错位的异常原样上抛；instanceof 链末端保留一道 ISE（双保险）。
     */
    private TxChange replayUnit(byte[] payload, long seq, OptionalLong streamXid, RelationResolver resolver) {
        char type = (char) payload[0];
        switch (type) {
            case 'I', 'U', 'D', 'T', 'M' -> { /* 可回放的数据消息——组装器路由只可能落这五类 */ }
            default -> throw new IllegalStateException(
                    "桶内单元非可回放的数据消息类型: '" + type + "'（期望 I/U/D/T/M）");
        }
        PgOutputMessage msg = decoder.decodeSingle(ByteBuffer.wrap(payload), streamXid.isPresent());
        decodedObserver.accept(msg);
        if (msg instanceof PgOutputMessage.Insert m) {
            return new RowChange(DmlKind.INSERT, resolver.require(m.relationOid(), seq),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Update m) {
            return new RowChange(DmlKind.UPDATE, resolver.require(m.relationOid(), seq),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Delete m) {
            return new RowChange(DmlKind.DELETE, resolver.require(m.relationOid(), seq),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(oid -> resolver.require(oid, seq))
                    .toList();
            return new TruncateChange(snapshots, m.options(), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.LogicalMsg m) {
            return new MsgChange(m.transactional(), m.prefix(), m.content(), m.streamXid());
        }
        throw new IllegalStateException("桶内出现不可回放的消息类型: " + msg.getClass().getSimpleName());
    }
}
