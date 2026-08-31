package org.vastdata.vbstream.bench;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.BenchPipeBridge;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * 组装开销归因基准（1.7.1 设计 §3 腿 2）：把组装路径上"窥探与 append 之外"的**每次交接级**
 * 开销逐项隔离计量——每交接 RelationSnapshot 拷贝、交接队列 add+poll 一对；1.7.1 Task 3 追加
 * {@code deletableFiles} 目录扫描口径（归因表 ⑧ 原只有 6~16% 采样区间，经本口径收窄成点估计，
 * 供"单项 ≥15% 且不动架构"修复门判定）。
 * 与 RoutePeekBenchmark（窥探 ns 级已入档，≈9 ns/条）、PipePathBenchmark（append 两口径）
 * 拼出 assembleWholeCorpus 总成本的分解视图，是 1.7.1 归因表的数据源。
 * 口径注意：交接级两项预期都在百 ns 级——若实测如此，即从嫌疑清单排除（归因表的"排除法"
 * 同样入档）；扫描口径预期在 µs 级（opendir/readdir/stat + 每调用新建 DateTimeFormatter）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AssemblyAttributionBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(AssemblyAttributionBenchmark.class);

    /**
     * 快照 state：预灌 2 oid × 各 2 版本的版本日志（对齐语料每轮约 4 条 'R' 的量级，含一次
     * 同 oid 重发模拟 DDL）——snapshot 的成本形状由（oid 数 × 版本数）决定，与语料真实
     * 'R' 内容无关，手工构造即可复现成本形态。
     */
    @State(Scope.Thread)
    public static class SnapshotState {

        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        Set<Integer> oidSet = Set.of(16384, 16390);
        long maxSeq = 40L;

        /**
         * 责任：灌 4 个版本（2 oid × 2 版本，seq 递增）。
         * 边界：Relation record 组件形态以 protocol 包实际定义为准（已核对：
         * (OptionalLong streamXid, int relationOid, String schema, String table,
         * char replicaIdentity, List&lt;Column&gt; columns)）。
         */
        @Setup(Level.Trial)
        public void setup() {
            registry.accept(10L, relation(16384, "t1_v1"));
            registry.accept(20L, relation(16390, "t2_v1"));
            registry.accept(30L, relation(16384, "t1_v2"));
            registry.accept(40L, relation(16390, "t2_v2"));
        }

        /**
         * 责任：手工构造一条最小 Relation 版本（schema/table 区分版本、空列——snapshot 只按
         * (oid, seq) 拷引用，列集空不改变拷贝成本形状）。
         * 边界：组件顺序与 PgOutputMessage.Relation record 定义逐一对齐。
         *
         * @param oid   表 oid（与 SnapshotState.oidSet 对应）
         * @param table 表名（区分 v1/v2 版本，仅助读）
         * @return 可安全共享的不可变 Relation record
         */
        private static PgOutputMessage.Relation relation(int oid, String table) {
            return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table, 'd', java.util.List.of());
        }
    }

    /**
     * 队列 state：与组装器交接队列同型（LinkedBlockingQueue）+ 一个冻结负载引用。
     * 负载用 Object 而非 TxBuffer（包私有不可及）：队列机制成本与负载类型无关。
     */
    @State(Scope.Thread)
    public static class QueueState {
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        Object payload = new Object();
    }

    /**
     * 计时体：一次交接快照拷贝（reader 提交路径的 snapshot(oidSet, lastIndex)）。
     * 返回快照对象防死码消除。
     */
    @Benchmark
    public Object snapshotCopyPerHandoff(SnapshotState s) {
        return s.registry.snapshot(s.oidSet, s.maxSeq);
    }

    /**
     * 计时体：交接一对（reader 侧 add + consumer 侧 poll）。
     * 返回 polled 对象防死码消除。
     */
    @Benchmark
    public Object handoffQueueOfferPoll(QueueState s) {
        s.queue.add(s.payload);
        return s.queue.poll();
    }

    /**
     * 扫描 state：**真实管道目录**（整份语料经 {@link BenchPipeBridge} 落盘一轮，目录内容与
     * 组装器实跑同形——metadata.cq4t + 当前 cycle 的 .cq4 滚动文件），neededCycle 取自当前
     * 追加前沿的 index（与目录内滚动文件同 cycle → 删集恒空，只扫不删——正是
     * assembleWholeCorpus 每轮 13 个桶完结点上 {@code releaseBelow} 的真实形态：单 cycle 内
     * 扫描费照付、删集恒空）。
     * 口径自检：Setup 记录目录条目数入日志（readdir/stat 次数的直接决定量），复测时可核对
     * 目录形状未漂移。
     */
    @State(Scope.Thread)
    public static class ScanState {

        /** 与组装器默认一致的滚动周期（文件名格式与 cycle 换算的参数源）。 */
        private final RollCycle rollCycle = LegacyRollCycles.MINUTELY;

        /** 真实管道句柄（Setup 落盘语料用，计时体不触碰）。 */
        private BenchPipeBridge.PipedBucket pipe;

        /** 管道目录（deletableFiles 的扫描对象）。 */
        private Path pipeDir;

        /** 扫描入参：当前追加前沿所在 cycle（与目录内滚动文件同 cycle，删集恒空）。 */
        private long neededCycle;

        /**
         * 责任：建真实管道并把整份语料落盘一轮（目录获得与组装器实跑同形的条目集）→ 追加一条
         * 取当前前沿 index → 换算 neededCycle → 记录目录条目数（口径自检日志）。
         * 边界：语料缺失按 BenchCorpus 带指引异常失败；CQ 写失败按底层异常上抛（基准立即失败）。
         */
        @Setup(Level.Trial)
        public void setup() throws Exception {
            List<byte[]> corpus = BenchCorpus.load();
            pipeDir = Files.createTempDirectory("bench-pipe-scan");
            pipe = BenchPipeBridge.dump(corpus, new VersionedRelationRegistry(), true,
                    pipeDir, rollCycle);
            long frontier = pipe.append(corpus.get(0));
            neededCycle = rollCycle.toCycle(frontier);
            try (Stream<Path> entries = Files.list(pipeDir)) {
                long dirEntries = entries.count();
                LOG.info("deletableFilesScan 口径自检：目录 {} 条目数 {}，neededCycle {}",
                        pipeDir, dirEntries, neededCycle);
            }
        }

        /**
         * 责任：teardown——关管道后递归删临时目录。
         * 边界：删除失败按 IOException 上抛（mmap 未释放类问题应显式暴露）。
         */
        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            pipe.close();
            CorpusLoader.deleteRecursively(pipeDir);
        }
    }

    /**
     * 计时体：单次 {@code deletableFiles} 目录扫描（opendir + readdir + 逐条目 stat + 新建
     * DateTimeFormatter + 解析滚动文件名）——组装器每个桶完结点经 releaseBelow 照付一次的
     * 全额成本，语料一轮付 13 次。
     * 返回可删列表（恒空，防死码消除）。
     * 口径（方法级覆盖类级 ns）：µs/次——归因换算 ×13 次/轮 ÷ 1,395 µs/轮基线即占
     * assembleWholeCorpus 的百分比，是 1.7.1 归因表 ⑧ 的点估计数据源。
     */
    @Benchmark
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<Path> deletableFilesScan(ScanState s) {
        return BenchPipeBridge.deletableFiles(s.rollCycle, s.pipeDir, s.neededCycle);
    }
}
