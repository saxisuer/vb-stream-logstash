package org.vastdata.vbstream.bench;

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
import org.vastdata.vbstream.replication.BenchPipeBridge;
import org.vastdata.vbstream.replication.TxChange;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 管道路径基准（1.7 Task 9，原 SpillPathBenchmark 换口径重建）：预构造一个 N=2000 单元的
 * 冻结桶（语料回卷填充，经 {@link BenchPipeBridge} 走与组装器 reader 侧同构的 append 记账
 * 循环落盘），计时**回放**（逐段 readRange 回读副本 + decodeSingle + 快照 asOf 渲染——即
 * {@code TransactionConsumer.processBucket} 的回放半程，consumer 线程的真实工作负载）；
 * 另附 {@code pipe.append} 裸吞吐（reader 线程每条消息的主成本之一，无帧化——一条 CQ 记录
 * 即一条完整消息，1.7 起帧头退役）。
 *
 * <p>与 1.6 口径的对照关系（{@code docs/benchmarks-baseline.md}）：1.6 的
 * {@code appendOneFrame} 在 SPILLED 溢写池上带 9 字节帧头，本基准的 {@code appendOneMessage}
 * 即同一 reader 路径成本的 1.7 形态（成本类同，仅少帧化一步）；1.6 的
 * MEMORY/SPILLED 回放对照已随 MEMORY 桶退役——回放口径（readRange+replay）为 1.7 新增，
 * 与 1.6 SPILLED 回放（9.185 ms/2000 单元）可作跨版本粗对照（载荷构成不同，见结果表注）。
 *
 * <p>外层类不标 {@code @State}（两个基准方法各挂自己的嵌套 state）：replayBucket(ReplayState)
 * 与 appendOneMessage(AppendState) 无共享参数维度。fork/warmup/measurement 由命令行给
 * （冒烟档 {@code -f 1 -w 1s -r 2s}）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PipePathBenchmark {

    /** 预构造桶的目标单元数（流式数据单元循环填充；实际单元数以语料为准，Setup 后可经 unitCount 自检）。 */
    private static final int BUCKET_UNITS = 2000;

    /** 语料回卷轮数：当前语料每轮含 20 条流式块内数据单元（≈328KB），100 轮凑满 2000 单元（≈32.8MB）。 */
    private static final int CORPUS_REPLAYS = 100;

    /**
     * 回放路径 state：Setup 里把语料回卷 {@link #CORPUS_REPLAYS} 轮后经 {@link BenchPipeBridge}
     * 转储成单个冻结桶（捕获流式块内单元——大载荷形态，与真实"流式大事务经管道回放"的主负载
     * 一致），benchmark 方法只做回放计时——转储（append ≈8400 条 ≈33MB）是一次性成本，
     * 不混入计时体。段结构随语料自然分裂（控制消息/'R'/块外单元插队即断段），非人造单段。
     */
    @State(Scope.Thread)
    public static class ReplayState {

        /** 已转储的基准桶句柄（持管道 + 冻结桶 + 回放器）。 */
        private BenchPipeBridge.PipedBucket piped;

        /** 临时管道目录（@TempDir 语义：Setup 建 / TearDown 删）。 */
        private Path pipeDir;

        /**
         * 责任：加载语料 → 回卷 CORPUS_REPLAYS 轮 → 经桥转储成冻结桶（registry 由桥在循环内灌好）。
         * 边界：语料缺失按 BenchCorpus 带指引异常失败；回卷后流式单元数不足/超出目标属口径漂移
         * （语料重录导致），仅影响结果表注的单元数与字节数，不影响基准可跑性；转储失败按 CQ
         * 异常上抛（本基准立即失败）。
         */
        @Setup(Level.Trial)
        public void setup() throws Exception {
            List<byte[]> corpus = BenchCorpus.load();
            List<byte[]> cycled = new ArrayList<>(corpus.size() * CORPUS_REPLAYS);
            for (int i = 0; i < CORPUS_REPLAYS; i++) {
                cycled.addAll(corpus);
            }
            pipeDir = Files.createTempDirectory("bench-pipe-replay");
            piped = BenchPipeBridge.dump(cycled, new VersionedRelationRegistry(), true,
                    pipeDir, LegacyRollCycles.MINUTELY);
        }

        /**
         * 责任：teardown——关管道（释放 mmap，先于目录删除）后递归删临时目录。
         * 边界：删除失败按 IOException 上抛（mmap 未释放类问题应显式暴露而非静默漏盘）。
         */
        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            piped.close();
            CorpusLoader.deleteRecursively(pipeDir);
        }
    }

    /**
     * append 吞吐 state：独立于回放参数——只持一个空桶句柄（即裸管道，无任何捕获单元）与一条
     * 最小真实数据消息（Setup 一次取出，计时体只剩 pipe.append 本身：writeBytes + index 取回，
     * 无帧化/无窥探）。
     */
    @State(Scope.Thread)
    public static class AppendState {

        /** 空桶句柄（裸管道；未捕获任何单元，replay 恒空不会被调用）。 */
        private BenchPipeBridge.PipedBucket pipe;

        /** 最小的真实数据消息字节（21B 主键 DELETE，Setup 一次取出，计时体复用同一数组）。 */
        private byte[] smallest;

        /** 临时管道目录（@TempDir 语义：Setup 建 / TearDown 删）。 */
        private Path dir;

        /**
         * 责任：建裸管道 + 从语料取最小数据消息（取最小是为了把磁盘增速压到冒烟档可承受的
         * 水平——测的是 append 机制吞吐，载荷大小在结果表中单列口径）。
         * 边界：语料无数据消息不可达（录制侧健康断言保证含 I/U/D）。
         */
        @Setup(Level.Trial)
        public void setup() throws Exception {
            dir = Files.createTempDirectory("bench-pipe-append");
            pipe = BenchPipeBridge.dump(List.of(), new VersionedRelationRegistry(), true,
                    dir, LegacyRollCycles.MINUTELY);
            smallest = smallestDataMessage(BenchCorpus.load());
        }

        /**
         * 责任：teardown——关管道后递归删目录（append 基准累计写入的消息全在其中）。
         * 边界：删除失败按 IOException 上抛。
         */
        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            pipe.close();
            CorpusLoader.deleteRecursively(dir);
        }
    }

    /**
     * 计时体：回放预构造的 2000 单元冻结桶（逐段 readRange + 解码 + 快照 asOf 渲染）。
     * 返回回放产物（防死码消除；2000 条 TxChange 的分配正是回放路径的真实成本）。
     */
    @Benchmark
    public List<TxChange> replayBucket(ReplayState state) {
        return state.piped.replay();
    }

    /**
     * 计时体（Throughput 覆盖类级 AverageTime）：向管道追加一条原始消息字节（无帧化——
     * 一条 CQ 记录即一条完整消息）。
     * 返回本条 CQ index（防死码消除；index 单调，同时给结果表换算累计条数/字节用）。
     * 口径：ops = 条（消息载荷长度见结果表注；reader 线程每条消息还要付路由窥探与桶记账，
     * 那部分由 RoutePeekBenchmark/AssembleMemoryBenchmark 覆盖）。
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public long appendOneMessage(AppendState state) {
        return state.pipe.append(state.smallest);
    }

    /**
     * 责任：取语料中最小的数据消息字节（append 吞吐基准的载荷源——最小载荷把磁盘增速压到
     * 冒烟档可承受，同时仍是真实协议形态而非合成字节）。
     * 边界：无数据消息抛 IllegalStateException（录制侧健康断言保证含 I/U/D，理论不可达）。
     *
     * @param corpus 语料消息字节列表
     * @return 最小的数据消息（I/U/D/T/M 之一）原始字节
     */
    private static byte[] smallestDataMessage(List<byte[]> corpus) {
        byte[] smallest = null;
        for (byte[] raw : corpus) {
            char type = (char) raw[0];
            if ((type == 'I' || type == 'U' || type == 'D' || type == 'T' || type == 'M')
                    && (smallest == null || raw.length < smallest.length)) {
                smallest = raw;
            }
        }
        if (smallest == null) {
            throw new IllegalStateException("语料中无数据消息（I/U/D/T/M），无法构造 append 载荷");
        }
        return smallest;
    }
}
