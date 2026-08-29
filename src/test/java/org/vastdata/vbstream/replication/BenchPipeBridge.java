package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * 跨包测试桥（1.7 Task 9，接替被 Task 5 删除的 {@code BenchSpillBridge}）：把
 * {@code org.vastdata.vbstream.replication} 的包私有管道机制（{@link MessagePipe} /
 * {@link TxBuffer} / {@link BucketReplayer}）以最小面透出给
 * {@code org.vastdata.vbstream.bench} 的 JMH 基准。存在动机与
 * {@link PipeWatermarkProbe} 相同——冻结桶经管道 readRange 回放是解耦设计的核心性能面，
 * 但不宜为基准把内部状态机升为公开 API；在测试源码目录以同包桥接类解决。
 * 仅测试代码可用，不属于主代码契约。
 *
 * <p>回放语义与 {@code TransactionConsumer#processBucket} 的**回放半程**同构：逐段
 * {@code readRange}（携带 index 作 seq）→ aborted 过滤（基准桶恒空集）→ decodeSingle →
 * 按桶内 {@link RelationSnapshot} 的 asOf 渲染为 TxChange 交 sink（2.0 流式交付形态，
 * 桥侧 {@code replayCounting()} 以零分配 sink 计数交付条数）；Begin/End 头尾事件、listener
 * 回调与前沿累加不在基准面（它们是消费循环的输出侧，非回放成本）。{@link #dump} 的记账循环则与
 * {@code TransactionAssembler#onRaw} 的 reader 侧同构：每条消息先 append 取 index 作 seq、
 * 'R' 以到达 seq 记入版本日志、数据单元窥 oid 后把 index 记入连续段——差异仅一处：真实组装器
 * 按事务边界分桶，本桥把全部匹配捕获条件的单元记进**单个基准桶**（协议的桶级 hasPrefix 不变量
 * 要求桶内单元的流式前缀形态唯一，故以 {@code captureInStream} 显式圈定，不匹配的单元照常
 * append 进管道成为段间断开的垃圾字节——与真实流中非本桶消息的行为一致）。
 */
public final class BenchPipeBridge {

    private BenchPipeBridge() {
    }

    /**
     * 已转储基准桶的句柄：持管道、冻结桶与回放器。桶的全部单元在 {@link #dump} 里顺序落盘，
     * 段的分裂只源于中间插队的非捕获消息（控制消息/'R'/异形态数据单元）——与真实组装器的
     * 段连续性规则（上一次 append 归属才顺延）逐条一致。
     */
    public static final class PipedBucket implements AutoCloseable {

        private final MessagePipe pipe;
        private final BucketReplayer replayer;
        private final TxBuffer bucket;

        PipedBucket(MessagePipe pipe, BucketReplayer replayer, TxBuffer bucket) {
            this.pipe = pipe;
            this.replayer = replayer;
            this.bucket = bucket;
        }

        /**
         * 责任：回放本桶并返回交付条数（管道回放口径的计时体，与
         * {@code TransactionConsumer#processBucket} 的回放半程同构——2.0 sink 形态）。
         * 关键步骤：逐段 readRange 回读副本（index 随行作 seq）→ aborted 过滤（基准桶恒空集，
         * 不触发）→ decodeSingle → 按桶内 RelationSnapshot 的 asOf 渲染为 TxChange 交 sink。
         * 计时体零分配 sink（{@code c -> {}}）：TxChange 的逐条分配仍是回放路径的真实成本
         * （实测 TxChange 分配未被逃逸分析消除），消失的只是基准旧口径里消费器不再做的整桶 List
         * 攒集——这正是 2.0 前后 gc 对照的口径差（docs/benchmarks-baseline.md 2.0 段）。
         * 边界与异常语义：空桶（未捕获任何单元）返回 0；readRange 起点错位 / Relation
         * miss / 协议错位按底层 fail-fast 上抛；快照在 dump 尾部预构（registry.snapshot），
         * 本方法零 registry 访问。
         *
         * @return 交付条数（aborted 过滤后；基准桶与捕获单元数一致）
         */
        public long replayCounting() {
            return replayer.replay(bucket, pipe, c -> { });
        }

        /**
         * 责任：向管道追加一条原始消息字节并返回其 CQ index（pipe.append 吞吐口径的计时体，
         * 与桶区间无关——追加不改变已记区间的回读边界；返回 index 单调，供结果表换算累计条数）。
         * 边界：raw 为 null 抛 NPE；写失败按 Chronicle 运行时异常上抛。
         *
         * @param raw 一条完整 pgoutput 消息字节（本方法只搬字节不解释）
         * @return 本条在队列中的 index
         */
        public long append(byte[] raw) {
            return pipe.append(raw);
        }

        /**
         * 责任：桶内捕获单元数（段端点差累加）——Setup 侧的口径自检与结果表注记用
         * （语料重录后单元配比漂移时，实际单元数以此为准而非 javadoc 里的历史值）。
         * 边界：空桶返回 0。
         *
         * @return 捕获进桶的数据单元数
         */
        public long unitCount() {
            long units = 0L;
            for (long[] segment : bucket.segments) {
                units += segment[1] - segment[0] + 1L;
            }
            return units;
        }

        /**
         * 责任：桶全部段的 index 跨度（lastIndex - firstIndex + 1）——回放时 readRange 走过的
         * 条目数口径（含段间垃圾条目，反映真实管道读放大）。
         * 边界：空桶返回 0。
         *
         * @return 管道条目跨度（含非捕获的插队条目）
         */
        public long indexSpan() {
            return bucket.firstIndex < 0 ? 0L : bucket.lastIndex - bucket.firstIndex + 1L;
        }

        /** 关闭底层管道（释放 mmap；删除目录由调用方 teardown 负责）。 */
        @Override
        public void close() {
            pipe.close();
        }
    }

    /**
     * 责任：把一组原始消息按组装器 reader 侧同构的记账循环转储进新建管道，产出可回放/可追加的
     * 基准桶句柄。
     * 关键步骤：建管道（构造即清空目录）→ 逐条 append 取 index 作 seq 并按类型路由：'R' 以
     * 到达 seq 解码记入版本日志；'S'/'E' 维护流块状态（与 decoder 的 inStream 同点同变，'R'
     * 解码的 inStream 实参同源于它）；I/U/D/T/M 窥 oid 入 oidSet——**仅捕获 inStream 与
     * {@code captureInStream} 一致的单元**入段（桶级 hasPrefix 不变量），不匹配的照常 append
     * 但不归桶（段断开）；其余控制消息只 append（段断开）→ 尾部以
     * {@code registry.snapshot(oidSet, lastIndex)} 预构快照冻结进桶 → 封句柄。
     * 边界与异常语义：rawMsgs/registry 为 null 抛 NPE、空列表合法（产出空桶，replay 恒空，
     * 句柄退化成裸管道供 append 口径）；registry 须含捕获单元引用的全部 Relation 版本（'R'
     * 先于同表 DML 到达由协议保证），否则 replay 时 fail-fast；CQ 写失败按底层异常上抛。
     *
     * @param rawMsgs        待转储的原始消息序列（入参序即落盘序；字节引用可跨条复用——CQ 写入即拷贝）
     * @param registry       Relation 版本日志（'R' 消息在循环内记入，调用方无需预灌）
     * @param captureInStream 桶捕获的流式形态：true 只捕获流块内（带 4 字节前缀）的单元，
     *                       false 只捕获块外单元
     * @param dir            管道目录（瞬态工作区，构造即清空）
     * @param rollCycle      滚动周期（与组装器默认一致取 LegacyRollCycles.MINUTELY）
     * @return 可回放/可追加的基准桶句柄
     */
    public static PipedBucket dump(List<byte[]> rawMsgs, VersionedRelationRegistry registry,
            boolean captureInStream, Path dir, RollCycle rollCycle) {
        Objects.requireNonNull(rawMsgs, "rawMsgs 不能为 null");
        Objects.requireNonNull(registry, "registry 不能为 null");
        MessagePipe pipe = new MessagePipe(dir, rollCycle);
        PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.PARALLEL);
        TxBuffer bucket = new TxBuffer(0L);
        boolean inStream = false;
        for (byte[] raw : rawMsgs) {
            long seq = pipe.append(raw);
            char type = (char) raw[0];
            switch (type) {
                case 'R' -> {
                    registry.accept(seq,
                            (PgOutputMessage.Relation) decoder.decodeSingle(ByteBuffer.wrap(raw), inStream));
                }
                case 'I', 'U', 'D', 'T', 'M' -> {
                    if (inStream != captureInStream) {
                        continue;   // 异形态单元：照常落盘但不归桶（不记段，成段间垃圾字节）
                    }
                    if (!bucket.prefixKnown) {
                        bucket.hasPrefix = inStream; // 首个捕获单元定型桶级不变量
                        bucket.prefixKnown = true;
                    }
                    collectOids(bucket, raw, inStream);
                    appendIndex(bucket, seq);
                }
                case 'S' -> inStream = true;
                case 'E' -> inStream = false;
                default -> { /* 其余控制消息：append 属垃圾字节，段连续性由 appendIndex 的紧邻判定自然断开 */ }
            }
        }
        bucket.relationSnapshot = registry.snapshot(bucket.oidSet, bucket.lastIndex);
        BucketReplayer replayer = new BucketReplayer(StreamingMode.PARALLEL, (msg, view) -> { });
        return new PipedBucket(pipe, replayer, bucket);
    }

    /**
     * 责任：透出 {@link MessagePipe#deletableFiles}（包私有纯函数）供 JMH 隔离计量
     * （1.7.1 Task 3 归因表 ⑧ 的收窄口径）：组装器每个桶完结点都经
     * {@code releaseBelow → deletableFiles} 做一次全目录扫描，该项在归因表中只有
     * 6~16% 的采样区间——本桥让基准能在真实管道目录上单次计时该函数，把区间收窄成点估计。
     * 边界与异常语义：与被透出函数完全一致（目录不存在/列举失败返回空列表，本身不删除任何文件）。
     *
     * @param rc          滚动周期（提供文件名格式与 cycle 换算参数）
     * @param dir         滚动文件所在目录（真实管道目录）
     * @param neededCycle 仍需保留的 cycle 号（它本身和减一档都保留）
     * @return 可删除文件的列表（可能为空，顺序不保证）
     */
    public static List<Path> deletableFiles(RollCycle rc, Path dir, long neededCycle) {
        return MessagePipe.deletableFiles(rc, dir, neededCycle);
    }

    /**
     * 责任：窥数据消息的 relation oid 记入桶的 oidSet（快照圈定用），逻辑与
     * {@code TransactionAssembler#collectOids} 同构：I/U/D 在类型字节（及可选 4 字节前缀）后取
     * Int32 relationOid；T 读 I32 表数 + 选项字节后的 oid 数组；M 无 oid 跳过。
     * 边界：偏移越界按数组越界上抛（语料损坏，fail-fast 优于静默错记）。
     *
     * @param bucket   记账目标桶
     * @param raw      数据消息原始字节
     * @param inStream 本条消息是否处于流块内（决定 oid 偏移基址）
     */
    private static void collectOids(TxBuffer bucket, byte[] raw, boolean inStream) {
        int base = inStream ? 5 : 1;
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
     * 责任：把捕获单元的 CQ index 记入桶的连续段，逻辑与
     * {@code TransactionAssembler#appendIndex} 同构：上一条全局 append 就是本桶的捕获单元
     * （等价判定：index 恰为 lastIndex+1，即两者之间无任何插队 append）才顺延当前段，否则新开
     * 段 [index,index]；firstIndex/lastIndex 维护全局端点。
     * 边界：段端点单调由 append 的 index 单调保证；首条捕获单元（lastIndex&lt;0）必新开段。
     *
     * @param bucket 记账目标桶
     * @param index  本条 append 返回的 CQ index
     */
    private static void appendIndex(TxBuffer bucket, long index) {
        if (bucket.lastIndex >= 0 && index == bucket.lastIndex + 1L) {
            bucket.segments.peekLast()[1] = index;   // 紧邻上一捕获条目：顺延当前段
        } else {
            bucket.segments.addLast(new long[]{index, index});
        }
        if (bucket.firstIndex < 0) {
            bucket.firstIndex = index;
        }
        bucket.lastIndex = index;
    }
}
