package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 跨包测试桥（Task 13）：把 {@code org.vastdata.vbstream.replication} 的包私有溢写机制
 * （{@link MessageSpool} / {@link BucketReplayer}）以最小面透出给
 * {@code org.vastdata.vbstream.bench} 的 JMH 基准。存在动机与 {@link SpillWatermarkProbe}
 * 相同——桶的 MEMORY/SPILLED 双形态回放是 spill 设计的核心性能面，但不宜为基准把内部状态机
 * 升为公开 API；在测试源码目录以同包桥接类解决。仅测试代码可用，不属于主代码契约。
 *
 * <p>回放语义与 {@code TransactionAssembler} 提交路径逐行同构：MEMORY 直接把堆内单元喂
 * {@link BucketReplayer}；SPILLED 逐段 {@link MessageSpool#readRange} 回读 +
 * {@link SpoolFrame#unframe} 复原后走同一回放器。基准据此测得的差异即"溢写回放代价"。
 */
public final class BenchSpillBridge {

    private BenchSpillBridge() {
    }

    /**
     * 已溢写桶的句柄：持有溢写池与该桶在池中的连续落盘区间（外加裸 spool 供 append 吞吐基准）。
     * 单桶顺序转储恰为一个连续段 [firstIndex..lastIndex]（组装器的段分裂只源于他桶插队，
     * 基准为单写单桶，不构造交错）。
     */
    public static final class SpilledBucket implements AutoCloseable {

        private final MessageSpool spool;
        private final BucketReplayer replayer;
        /** 落盘区间端点；空桶（未 append 过）为 -1，replay 直接返回空列表。 */
        private final long firstIndex;
        private final long lastIndex;

        SpilledBucket(MessageSpool spool, BucketReplayer replayer, long firstIndex, long lastIndex) {
            this.spool = spool;
            this.replayer = replayer;
            this.firstIndex = firstIndex;
            this.lastIndex = lastIndex;
        }

        /**
         * 责任：回放本桶（SPILLED 形态的提交路径计时体）。
         * 关键步骤：逐段 readRange 回读帧字节 → SpoolFrame.unframe 复原为单元 → BucketReplayer
         * 渲染为 TxChange（与组装器 replay(TxBuffer) 的 SPILLED 分支同构）。
         * 边界与异常语义：空桶（firstIndex&lt;0）返回空列表；回读起点错位/Relation miss/协议
         * 错位按底层 fail-fast 上抛；abortedSubxids 恒为空集（基准语料不做子事务剔除对照）。
         *
         * @return 回放产物（与落盘单元一一对应、保序）
         */
        public List<TxChange> replay() {
            if (firstIndex < 0) {
                return List.of();
            }
            List<PayloadUnit> units = new ArrayList<>();
            spool.readRange(firstIndex, lastIndex, (framed, ordinal) -> units.add(SpoolFrame.unframe(framed)));
            return replayer.replay(units, Set.of());
        }

        /**
         * 责任：向溢写池追加一条已帧化字节并返回其 CQ index（spool.append 吞吐基准的计时体，
         * 与桶区间无关——追加不改变已记区间的回读边界）。
         *
         * @param framed SpoolFrame.frame 产出的完整帧字节
         * @return 本条在队列中的 index
         */
        public long append(byte[] framed) {
            return spool.append(framed);
        }

        /** 关闭底层溢写池（释放 mmap；删除目录由调用方 teardown 负责）。 */
        @Override
        public void close() {
            spool.close();
        }
    }

    /**
     * 责任：把一组单元整体转储进新建溢写池（SPILLED 桶的预构造，等价组装器 spillAll 对
     * 单桶的转储动作）。
     * 关键步骤：建池（构造即清空目录）→ 逐单元 frame→append（返回的 index 记区间端点，
     * 顺序追加天然连续）→ 以 (PARALLEL, registry, 空观察者) 建回放器封为句柄。
     * 边界与异常语义：units 为 null 抛 NPE、空列表合法（产出空桶，replay 恒空）；
     * registry 必须已含单元引用的全部 Relation 版本（require(oid, seq) asOf 语义），否则
     * replay 时 fail-fast；CQ 写失败按底层异常上抛。
     *
     * @param units    待转储的桶单元（入参序即落盘序）
     * @param registry Relation 版本日志（回放渲染用，调用方预先灌好）
     * @param dir      溢写池目录（瞬态工作区，构造即清空）
     * @param rollCycle 滚动周期（与组装器默认一致取 LegacyRollCycles.MINUTELY）
     * @return 可回放/可追加的溢写桶句柄
     */
    public static SpilledBucket spill(List<PayloadUnit> units, VersionedRelationRegistry registry,
            Path dir, RollCycle rollCycle) {
        Objects.requireNonNull(units, "units 不能为 null");
        MessageSpool spool = new MessageSpool(dir, rollCycle);
        BucketReplayer replayer = new BucketReplayer(StreamingMode.PARALLEL, registry, msg -> { });
        long first = -1L;
        long last = -1L;
        for (PayloadUnit unit : units) {
            last = spool.append(SpoolFrame.frame(unit));
            if (first < 0) {
                first = last;
            }
        }
        return new SpilledBucket(spool, replayer, first, last);
    }

    /**
     * 责任：MEMORY 形态回放（对照组计时体）——与 SPILLED 走同一 BucketReplayer，
     * 差异仅在单元来源（堆内直接引用 vs 回读副本）。
     * 边界与异常语义：units/registry 为 null 抛 NPE；abortedSubxids 恒空集（同 {@link #spill}）。
     *
     * @param units    桶单元（堆内）
     * @param registry Relation 版本日志
     * @return 回放产物（与单元一一对应、保序）
     */
    public static List<TxChange> replayMemory(List<PayloadUnit> units, VersionedRelationRegistry registry) {
        return new BucketReplayer(StreamingMode.PARALLEL, registry, msg -> { }).replay(units, Set.of());
    }
}
