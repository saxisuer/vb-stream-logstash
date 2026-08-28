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
import java.util.function.BiConsumer;

/**
 * 桶回放器（assembly-spill 设计 §2/§4.4，1.7 解耦形态）：把一个交接冻结的事务桶
 * （{@link TxBuffer} 的 CQ index 连续段 + 随行 {@link RelationSnapshot}）从 {@link MessagePipe}
 * 逐段读回、逐条解码渲染成 {@link TxChange} 序列。它是提交路径（Commit/StreamCommit/
 * CommitPrepared 交接后的回放半程）的核心；从 {@link TransactionAssembler} 抽出来单独成类，
 * 是为了能绕过组装器直接手造桶/管道做测试。
 *
 * <p>Relation 的 asOf 解析自 1.7 Task 6 起完全走桶内快照：回放器不持有 registry——resolver 与
 * 逐消息渲染视图都取自 {@code bucket.relationSnapshot}（组装器交接时以 oidSet 圈定、截止
 * lastIndex 拷出），consumer 线程与 reader 的版本日志零共享、零竞争。
 *
 * <p>每个单元三步：
 * <ul>
 *   <li>aborted 过滤：桶级 hasPrefix 时重窥 raw[1..4] 得 streamXid，命中 abortedSubxids
 *       （StreamAbort 记下的被回滚子事务）就直接跳过——不解码、不回调 observer。LogicalMsg
 *       单元的前缀是顶层 xid，不会撞上子事务的 subxid，不会被误删</li>
 *   <li>解码：用 {@link PgOutputDecoder#decodeSingle(ByteBuffer, boolean)}，inStream 由桶级
 *       hasPrefix 直接给定——不用拿 'S'/'E' 把流块上下文包一遍。解完回调 decodedObserver
 *       （回放也是解码点，逐消息 DEBUG 在这里透出；第二参传桶快照作渲染视图）</li>
 *   <li>构造：I/U/D 生成 {@link RowChange}，T 生成 {@link TruncateChange}（逐个 oid 取快照），
 *       M 生成 {@link MsgChange}；Relation 一律用 {@code snapshot.require(oid, index)} 取
 *       **变更时刻**的版本（index 即单元 seq）——DDL 并发时旧单元不能按新表结构解释（设计 §4.4）</li>
 * </ul>
 *
 * <p>边界：桶未交接（relationSnapshot 为 null）抛 {@link IllegalStateException}——回放的
 * 前置条件是快照随行，缺失即调用序编程错误；单元类型不是 I/U/D/T/M 抛 ISE——组装器的路由保证
 * 管道里只有本桶的数据单元落在桶的段内，这里是防御性的 fail-fast；Relation 未先行到达
 * （快照 require 查不到）或字节与协议不符，抛 ISE/协议异常——回放失败即协议流异常，不允许输出
 * 半截事务（异常在组装器回调 listener 之前抛出，由调用方保证）。空桶（无段）产出空列表
 * （空桶提交是合法的）。
 *
 * <p>线程约束：单线程内同步执行——由 {@link TransactionConsumer} 在 consumer 线程调用
 * （同步测试形态下即调用方线程）。自带一个独立的 {@link PgOutputDecoder}（decodeSingle 不碰
 * 实例的流块状态，与组装器 reader 线程的 live 解码互不干扰）；产出的 TxChange 不可变，可跨线程传递。
 */
final class BucketReplayer {

    private final PgOutputDecoder decoder;
    /** 每个回放解码点回调（与组装器 live 解码共用同一 observer，语义一致；第二参为桶快照渲染视图）。 */
    private final BiConsumer<PgOutputMessage, RelationLookup> decodedObserver;

    /**
     * 构造回放器。
     *
     * @param mode            流式模式（构造自持 decoder 用；白名单类型的解析不分支于模式档位，
     *                        仅为与组装器的 live decoder 同构保留）
     * @param decodedObserver 每个回放解码点回调（被 aborted 过滤跳过的单元不触发；第二参传
     *                        该桶的 RelationSnapshot 作逐消息渲染视图）
     */
    BucketReplayer(StreamingMode mode, BiConsumer<PgOutputMessage, RelationLookup> decodedObserver) {
        this.decoder = new PgOutputDecoder(Objects.requireNonNull(mode, "mode"));
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
    }

    /**
     * 回放一个交接冻结的桶：逐段 readRange（readRange 保证段内全部是本桶单元），逐单元三步——
     * aborted 过滤（hasPrefix 时重窥 raw[1..4] 得 streamXid，命中 abortedSubxids 跳过）→
     * decodeSingle(payload, bucket.hasPrefix) → 构造 TxChange（Relation 经桶快照
     * require(oid, index)，index 即单元 seq）。空桶产出空列表。
     *
     * <p>边界：bucket/pipe 为 null 抛 NPE；桶未交接（快照缺失）抛 ISE；Relation 查不到或字节与
     * 协议不符时抛 ISE/协议异常（在回调 listener 之前抛出，不输出半截事务）。单线程内同步执行。
     *
     * @param bucket 已交接冻结的桶（段列表、abortedSubxids 与 relationSnapshot）
     * @param pipe   单元字节所在的管道（按段读回，payload 为副本）
     * @return 回放产物（与保留的单元一一对应、按段序保序）
     */
    List<TxChange> replay(TxBuffer bucket, MessagePipe pipe) {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(pipe, "pipe");
        RelationSnapshot snapshot = bucket.relationSnapshot;
        if (snapshot == null) {
            throw new IllegalStateException("桶未交接（relationSnapshot 缺失），回放前置条件违反: xid=" + bucket.xid);
        }
        List<TxChange> changes = new ArrayList<>();
        for (long[] segment : bucket.segments) {
            pipe.readRange(segment[0], segment[1], (index, payload) -> {
                OptionalLong streamXid = bucket.hasPrefix
                        ? OptionalLong.of(RawPeeks.unsignedInt(payload, 1))
                        : OptionalLong.empty();
                if (streamXid.isPresent() && bucket.abortedSubxids.contains(streamXid.getAsLong())) {
                    return;
                }
                changes.add(replayUnit(payload, index, streamXid, snapshot));
            });
        }
        return changes;
    }

    /**
     * 回放单个存储单元：先做类型守卫，再解码、回调 decodedObserver（第二参传桶快照作渲染视图），
     * 最后按消息类型构造 TxChange。
     *
     * <p>类型守卫：不是 I/U/D/T/M 就抛 ISE——组装器的路由保证不可达，属防御性检查。解码用
     * decodeSingle，inStream 由桶级 hasPrefix 决定（经 streamXid 的有无表达）。构造时 Relation
     * 一律用 {@code snapshot.require(oid, seq)} 取变更时刻的版本；消息分发走 instanceof 链
     * （Java 17 没有 record pattern switch）。
     *
     * <p>边界：payload 为空数组违反契约（首字节访问抛数组越界）；守卫通过后 require 查不到或
     * 协议错位的异常原样上抛；instanceof 链末端保留一道 ISE（双保险）。
     */
    private TxChange replayUnit(byte[] payload, long seq, OptionalLong streamXid, RelationSnapshot snapshot) {
        char type = (char) payload[0];
        switch (type) {
            case 'I', 'U', 'D', 'T', 'M' -> { /* 可回放的数据消息——组装器路由只可能落这五类 */ }
            default -> throw new IllegalStateException(
                    "桶内单元非可回放的数据消息类型: '" + type + "'（期望 I/U/D/T/M）");
        }
        PgOutputMessage msg = decoder.decodeSingle(ByteBuffer.wrap(payload), streamXid.isPresent());
        decodedObserver.accept(msg, snapshot);
        if (msg instanceof PgOutputMessage.Insert m) {
            return new RowChange(DmlKind.INSERT, snapshot.require(m.relationOid(), seq),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Update m) {
            return new RowChange(DmlKind.UPDATE, snapshot.require(m.relationOid(), seq),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Delete m) {
            return new RowChange(DmlKind.DELETE, snapshot.require(m.relationOid(), seq),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(oid -> snapshot.require(oid, seq))
                    .toList();
            return new TruncateChange(snapshots, m.options(), m.streamXid());
        }
        if (msg instanceof PgOutputMessage.LogicalMsg m) {
            return new MsgChange(m.transactional(), m.prefix(), m.content(), m.streamXid());
        }
        throw new IllegalStateException("桶内出现不可回放的消息类型: " + msg.getClass().getSimpleName());
    }
}
