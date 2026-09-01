package org.vastdata.debezium.connector.postgresql.stream;

import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputStreamDecoder;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 桶回放器:把一个交接冻结的事务桶({@link TxBuffer} 的 CQ index 连续段 + 随行
 * {@link RelationSnapshot})从 {@link MessagePipe} 逐段读回、逐条解码渲染成 {@link TxChange}
 * 序列。它是提交路径(Commit/StreamCommit/CommitPrepared 交接后的回放半程)的核心;
 * 从 {@link StreamedTransactionAssembler} 分离单独成类(引擎同款拆分),是为了能绕过组装器
 * 直接手造桶/管道做测试。引擎 {@code org.vastdata.vbstream.replication.BucketReplayer}
 * (171 行)的 1:1 重写(文字参照,非依赖)。
 *
 * <p>Relation 的 asOf 解析完全走桶内快照:回放器不持有 registry——渲染视图取自
 * {@code bucket.relationSnapshot}(组装器交接时以 oidSet 圈定、截止 lastIndex 拷出),
 * consumer 线程与 reader 的版本日志零共享、零竞争(引擎 1.7 设计 §4.3 的语义沿用)。
 * connector 偏差:快照载荷是 {@link ResolvedRelation}(wire + Debezium Table 双形态),
 * {@link RowChange} 只嵌入其 wire 形态——Table 形态由下游 listener 经
 * {@link BucketTableResolver} 按 (oid, seq) 解析(见其 javadoc)。
 *
 * <p>每个单元三步:
 * <ul>
 *   <li>aborted 过滤:桶级 hasPrefix 时重窥 raw[1..4] 得 streamXid,命中 abortedSubxids
 *       (StreamAbort 记下的被回滚子事务)就直接跳过——不解码、不回调 observer。LogicalMsg
 *       单元的前缀是顶层 xid,不会撞上子事务的 subxid,不会被误删</li>
 *   <li>解码:用 {@link PgOutputStreamDecoder#decodeSingle(ByteBuffer, boolean)},inStream 由桶级
 *       hasPrefix 直接给定——不用拿 'S'/'E' 把流块上下文包一遍。解完回调 decodedObserver
 *       (回放也是解码点,逐消息观测在这里透出;第二参传桶快照作渲染视图)</li>
 *   <li>构造:I/U/D 生成 {@link RowChange},T 生成 {@link TruncateChange}(逐个 oid 取快照),
 *       M 生成 {@link MsgChange};Relation 一律用 {@code snapshot.require(oid, seq).wire()} 取
 *       <b>变更时刻</b>的版本(seq 即单元 index)——DDL 并发时旧单元不能按新表结构解释</li>
 * </ul>
 *
 * <p>边界:桶未交接(relationSnapshot 为 null)抛 {@link IllegalStateException}——回放的
 * 前置条件是快照随行,缺失即调用序编程错误;单元类型不是 I/U/D/T/M 抛 ISE——组装器的路由保证
 * 管道里只有本桶的数据单元落在桶的段内,这里是防御性的 fail-fast;Relation 未先行到达
 * (快照 require 查不到)或字节与协议不符,抛 ISE/协议异常——回放失败即协议流异常,异常经
 * sink 调用链原样上抛(流式语义下已交付的 TxChange 不撤回,事务尾 End 由调用方保证
 * 永不发出)。空桶(无段)零交付(空桶提交是合法的,Begin → End(0))。
 *
 * <p>线程约束:单线程内同步执行——由 {@code TransactionConsumer} 在 consumer 线程调用
 * (同步测试形态下即调用方线程)。自带一个独立的 {@link PgOutputStreamDecoder}
 * (decodeSingle 不碰实例的流块状态,与组装器 reader 线程的 live 解码互不干扰);产出的
 * TxChange 不可变,可跨线程传递。
 */
final class BucketReplayer {

    private final PgOutputStreamDecoder decoder;
    /** 每个回放解码点回调(与组装器 live 解码共用同一 observer,语义一致;第二参为桶快照渲染视图)。 */
    private final BiConsumer<PgOutputMessage, RelationLookup> decodedObserver;

    /**
     * 构造回放器。
     *
     * @param mode            流式模式(自持 decoder 用;白名单类型的解析不分支于模式档位,
     *                        仅为与组装器的 live decoder 同构保留)
     * @param decodedObserver 每个回放解码点回调(被 aborted 过滤跳过的单元不触发;第二参传
     *                        该桶的 RelationSnapshot 作逐消息渲染视图)
     */
    BucketReplayer(StreamingMode mode, BiConsumer<PgOutputMessage, RelationLookup> decodedObserver) {
        this.decoder = new PgOutputStreamDecoder(Objects.requireNonNull(mode, "mode"));
        this.decodedObserver = Objects.requireNonNull(decodedObserver, "decodedObserver");
    }

    /**
     * 回放一个交接冻结的桶(交付化:不构造列表——逐条经 sink 交付,堆内 O(单条)):
     * 逐段 readRange(readRange 保证段内全部是本桶单元),逐单元三步——aborted 过滤
     * (hasPrefix 时重窥 raw[1..4] 得 streamXid,命中 abortedSubxids 跳过、不交付)→
     * decodeSingle(payload, bucket.hasPrefix) → 构造 TxChange(Relation 经桶快照
     * require(oid, seq) 的 wire 形态,index 即单元 seq)即交 sink。空桶零交付。
     *
     * <p>边界:bucket/pipe/sink 为 null 抛 NPE;桶未交接(快照缺失)抛 ISE;Relation 查不到或
     * 字节与协议不符时抛 ISE/协议异常(经 sink 调用链原样上抛,已交付条目不撤回)。
     * 单线程内同步执行。
     *
     * @param bucket 已交接冻结的桶(段列表、abortedSubxids 与 relationSnapshot)
     * @param pipe   单元字节所在的管道(按段读回,payload 为副本)
     * @param sink   交付目标(每条保留单元回调一次,按段序保序;计数归调用方)
     * @return 交付数(aborted 过滤后)
     */
    long replay(TxBuffer bucket, MessagePipe pipe, Consumer<TxChange> sink) {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(pipe, "pipe");
        Objects.requireNonNull(sink, "sink");
        RelationSnapshot snapshot = bucket.relationSnapshot;
        if (snapshot == null) {
            throw new IllegalStateException("桶未交接(relationSnapshot 缺失),回放前置条件违反: xid=" + bucket.xid);
        }
        long[] emitted = {0L};
        for (long[] segment : bucket.segments) {
            pipe.readRange(segment[0], segment[1], (index, payload) -> {
                OptionalLong streamXid = bucket.hasPrefix
                        ? OptionalLong.of(RawPeeks.unsignedInt(payload, 1))
                        : OptionalLong.empty();
                if (streamXid.isPresent() && bucket.abortedSubxids.contains(streamXid.getAsLong())) {
                    return;
                }
                sink.accept(replayUnit(payload, index, streamXid, snapshot));
                emitted[0]++;
            });
        }
        return emitted[0];
    }

    /**
     * 回放单个存储单元:先做类型守卫,再解码、回调 decodedObserver(第二参传桶快照作渲染视图),
     * 最后按消息类型构造 TxChange(相对引擎多携带 seq 偏差组件——见 {@link TxChange#seq()})。
     *
     * <p>类型守卫:不是 I/U/D/T/M 就抛 ISE——组装器的路由保证不可达,属防御性检查。解码用
     * decodeSingle,inStream 由桶级 hasPrefix 决定(经 streamXid 的有无表达)。构造时 Relation
     * 一律用 {@code snapshot.require(oid, seq).wire()} 取变更时刻的版本;消息分发走 instanceof
     * 链(Java 17 没有 record pattern switch)。
     *
     * <p>边界:payload 为空数组违反契约(首字节访问抛数组越界);守卫通过后 require 查不到或
     * 协议错位的异常原样上抛;instanceof 链末端保留一道 ISE(双保险)。
     */
    private TxChange replayUnit(byte[] payload, long seq, OptionalLong streamXid, RelationSnapshot snapshot) {
        char type = (char) payload[0];
        switch (type) {
            case 'I', 'U', 'D', 'T', 'M' -> { /* 可回放的数据消息——组装器路由只可能落这五类 */ }
            default -> throw new IllegalStateException(
                    "桶内单元非可回放的数据消息类型: '" + type + "'(期望 I/U/D/T/M)");
        }
        PgOutputMessage msg = decoder.decodeSingle(ByteBuffer.wrap(payload), streamXid.isPresent());
        decodedObserver.accept(msg, snapshot);
        if (msg instanceof PgOutputMessage.Insert m) {
            return new RowChange(DmlKind.INSERT, snapshot.require(m.relationOid(), seq).wire(),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid(), seq);
        }
        if (msg instanceof PgOutputMessage.Update m) {
            return new RowChange(DmlKind.UPDATE, snapshot.require(m.relationOid(), seq).wire(),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid(), seq);
        }
        if (msg instanceof PgOutputMessage.Delete m) {
            return new RowChange(DmlKind.DELETE, snapshot.require(m.relationOid(), seq).wire(),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid(), seq);
        }
        if (msg instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(oid -> snapshot.require(oid, seq).wire())
                    .toList();
            return new TruncateChange(snapshots, m.options(), m.streamXid(), seq);
        }
        if (msg instanceof PgOutputMessage.LogicalMsg m) {
            return new MsgChange(m.transactional(), m.prefix(), m.content(), m.streamXid(), seq);
        }
        throw new IllegalStateException("桶内出现不可回放的消息类型: " + msg.getClass().getSimpleName());
    }
}
