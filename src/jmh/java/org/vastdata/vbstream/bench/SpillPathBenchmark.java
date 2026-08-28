package org.vastdata.vbstream.bench;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.replication.BenchSpillBridge;
import org.vastdata.vbstream.replication.PayloadUnit;
import org.vastdata.vbstream.replication.SpoolFrame;
import org.vastdata.vbstream.replication.TxChange;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

/**
 * 溢写路径基准（Task 13）：预构造一个 N=2000 单元的桶（语料数据消息循环填充），按
 * {@code @Param} 分 MEMORY / SPILLED 两形态对**同一批单元**计时回放（经
 * {@link BenchSpillBridge} 走与 TransactionAssembler 提交路径同构的双分支——MEMORY 堆内
 * 直接引用 vs SPILLED readRange 回读副本 + unframe），差值即溢写回放的纯代价；另附
 * {@code spool.append} 单帧吞吐（与路径参数无关，故用独立的 {@link AppendState} 而非挂在
 * {@code @Param} 下重复两遍）。
 *
 * <p>外层类不标 {@code @State}（两个基准方法各挂自己的嵌套 state）：replayBucket(ReplayState)
 * 随 @Param 参数化两形态；appendOneFrame(AppendState) 无参数维度。fork/warmup/measurement
 * 由命令行给（冒烟档 {@code -f 1 -w 1s -r 2s}）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SpillPathBenchmark {

    /** 预构造桶的单元数（循环填充语料数据消息；约 15MB——远大于操作系统页缓存的单页，回读路径真实分页）。 */
    private static final int BUCKET_UNITS = 2000;

    /**
     * 回放路径 state：按 {@code path} 参数在 Setup 里预构造两种形态的桶（同一批单元），
     * benchmark 方法只做回放计时——构造（spill 转储 15MB）是一次性成本，不混入计时体。
     */
    @State(Scope.Thread)
    public static class ReplayState {

        /** 桶存储形态：MEMORY（堆内 List&lt;PayloadUnit&gt;）或 SPILLED（溢写池连续区间）。 */
        @Param({"MEMORY", "SPILLED"})
        public String path;

        /** Relation 版本日志（Setup 从语料 'R' 消息灌好，回放 asOf 渲染用）。 */
        private VersionedRelationRegistry registry;

        /** MEMORY 形态的桶单元（SPILLED 形态下同样持有——同批单元，保证两形态输入一致）。 */
        private List<PayloadUnit> bucket;

        /** SPILLED 形态句柄（MEMORY 形态为 null）。 */
        private BenchSpillBridge.SpilledBucket spilled;

        /** 临时溢写目录（@TempDir 语义：Setup 建 / TearDown 删）。 */
        private Path spillDir;

        /**
         * 责任：加载语料 → 灌 registry + 抽数据单元 → 循环填充 N 单元桶 → 按 path 预构造
         * （SPILLED 时整桶 frame→append 转储进新建溢写池）。
         * 边界：语料缺失按 BenchCorpus 带指引异常失败；语料无数据单元不可达（录制侧健康
         * 断言保证含 I/U/D）；spill 转储失败按 CQ 异常上抛（本基准立即失败）。
         */
        @Setup(Level.Trial)
        public void setup() throws Exception {
            Extraction extracted = extract(BenchCorpus.load());
            registry = extracted.registry();
            List<PayloadUnit> units = extracted.units();
            bucket = new ArrayList<>(BUCKET_UNITS);
            for (int i = 0; i < BUCKET_UNITS; i++) {
                bucket.add(units.get(i % units.size()));
            }
            spillDir = Files.createTempDirectory("bench-spill-replay");
            if ("SPILLED".equals(path)) {
                spilled = BenchSpillBridge.spill(bucket, registry, spillDir, LegacyRollCycles.MINUTELY);
            }
        }

        /**
         * 责任：teardown——关溢写池（释放 mmap，先于目录删除）后递归删临时目录。
         * 边界：MEMORY 形态 spilled 为 null 只删目录；删除失败按 IOException 上抛。
         */
        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            if (spilled != null) {
                spilled.close();
            }
            CorpusLoader.deleteRecursively(spillDir);
        }
    }

    /**
     * append 吞吐 state：独立于回放路径参数——只持一个空桶句柄（即裸溢写池）与一帧预帧化的
     * 最小真实消息（帧化在 Setup 完成，计时体只剩 spool.append 本身：writeBytes + index 取回）。
     */
    @State(Scope.Thread)
    public static class AppendState {

        /** 空桶句柄（裸溢写池；未 append 过桶单元，firstIndex=-1，replay 恒空不会被调用）。 */
        private BenchSpillBridge.SpilledBucket spool;

        /** 预帧化的最小数据消息帧（Setup 一次帧化，计时体复用同一字节数组）。 */
        private byte[] framedSmall;

        /** 临时溢写目录（@TempDir 语义：Setup 建 / TearDown 删）。 */
        private Path dir;

        /**
         * 责任：建溢写池 + 从语料取最小数据消息帧化（取最小是为了把字节量压到冒烟档可承受的
         * 磁盘增速——测的是 append 机制吞吐，载荷大小在结果表中单列口径）。
         * 边界：语料无数据消息不可达（同 ReplayState）；帧化失败按 SpillFrame 异常上抛。
         */
        @Setup(Level.Trial)
        public void setup() throws Exception {
            dir = Files.createTempDirectory("bench-spill-append");
            spool = BenchSpillBridge.spill(List.of(), new VersionedRelationRegistry(),
                    dir, LegacyRollCycles.MINUTELY);
            framedSmall = SpoolFrame.frame(smallestUnit(BenchCorpus.load()));
        }

        /**
         * 责任：teardown——关池后递归删目录（append 基准累计写入的帧全在其中）。
         * 边界：删除失败按 IOException 上抛。
         */
        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            spool.close();
            CorpusLoader.deleteRecursively(dir);
        }
    }

    /**
     * 计时体：回放预构造的 2000 单元桶（MEMORY/SPILLED 由 @Param 分流，输入单元同批）。
     * 返回回放产物（防死码消除；2000 条 TxChange 的分配正是回放路径的真实成本）。
     */
    @Benchmark
    public List<TxChange> replayBucket(ReplayState state) {
        return "SPILLED".equals(state.path)
                ? state.spilled.replay()
                : BenchSpillBridge.replayMemory(state.bucket, state.registry);
    }

    /**
     * 计时体（Throughput 覆盖类级 AverageTime）：向溢写池追加一帧预帧化字节。
     * 返回本条 CQ index（防死码消除；index 单调，同时给结果表换算累计条数/字节用）。
     * 口径：ops = 帧（含 9 字节帧头；载荷长度见结果表注）。
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public long appendOneFrame(AppendState state) {
        return state.spool.append(state.framedSmall);
    }

    // ---- 语料 → (registry, 数据单元) 抽取（与 TransactionAssembler.onRaw 的路由窥探同构） ----

    /**
     * 一次抽取的产物。
     *
     * @param registry 按语料 'R' 消息（携带原流 seq）灌好的版本日志
     * @param units    语料中的数据消息单元（I/U/D/T/M，seq 为其在语料中的 1 起序号，
     *                 流式块内的单元携带 4 字节前缀对应的 streamXid）
     */
    private record Extraction(VersionedRelationRegistry registry, List<PayloadUnit> units) {
    }

    /**
     * 责任：把语料抽为（registry, 数据单元）——镜像 {@code TransactionAssembler.onRaw} 的路由：
     * 'R' 以到达序号 seq 灌 {@link VersionedRelationRegistry}；'I'/'U'/'D'/'T'/'M' 包装为
     * {@link PayloadUnit}（流式块内时前缀取 raw[1..4] 无符号值）；'S'/'E' 维护本地块状态
     * （与 decoder 的 inStream 同点同变，用 {@link PgOutputDecoder#decodeSingle} 免整流重放）。
     * 边界与异常语义：语料中不可回放消息不入单元集；'R' 解码失败按协议异常上抛（语料损坏）。
     *
     * @param corpus 语料消息字节列表
     * @return 抽取产物（registry 与 units 供桶构造与回放）
     */
    private static Extraction extract(List<byte[]> corpus) {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        List<PayloadUnit> units = new ArrayList<>();
        PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.PARALLEL);
        boolean inStream = false;
        long seq = 0;
        for (byte[] raw : corpus) {
            seq++;
            switch ((char) raw[0]) {
                case 'R' -> registry.accept(seq,
                        (PgOutputMessage.Relation) decoder.decodeSingle(ByteBuffer.wrap(raw), inStream));
                case 'I', 'U', 'D', 'T', 'M' -> units.add(new PayloadUnit(raw, seq,
                        inStream ? OptionalLong.of(unsignedIntAt(raw, 1)) : OptionalLong.empty()));
                case 'S' -> inStream = true;
                case 'E' -> inStream = false;
                default -> { /* 控制消息（B/C/c/A/…）不入桶不灌 registry；'Y'/'O' 语料中不存在 */ }
            }
        }
        return new Extraction(registry, units);
    }

    /**
     * 责任：取语料中最小的数据消息单元（append 吞吐基准的帧化源——最小载荷把磁盘增速压到
     * 冒烟档可承受，同时仍是真实协议形态而非合成字节）。同步维护流式块状态，保证单元的
     * streamXid 有无与其 payload 前缀有无一致（PayloadUnit 消费契约）。
     * 边界：无数据消息抛 IllegalStateException（录制侧健康断言保证含 I/U/D，理论不可达）。
     */
    private static PayloadUnit smallestUnit(List<byte[]> corpus) {
        PayloadUnit smallest = null;
        boolean inStream = false;
        long seq = 0;
        for (byte[] raw : corpus) {
            seq++;
            switch ((char) raw[0]) {
                case 'I', 'U', 'D', 'T', 'M' -> {
                    if (smallest == null || raw.length < smallest.payload().length) {
                        smallest = new PayloadUnit(raw, seq,
                                inStream ? OptionalLong.of(unsignedIntAt(raw, 1)) : OptionalLong.empty());
                    }
                }
                case 'S' -> inStream = true;
                case 'E' -> inStream = false;
                default -> { }
            }
        }
        if (smallest == null) {
            throw new IllegalStateException("语料中无数据消息（I/U/D/T/M），无法帧化 append 载荷");
        }
        return smallest;
    }

    /** big-endian 无符号读 4 字节入 long（流式前缀 xid 窥探，与组装器 unsignedInt 同语义）。 */
    private static long unsignedIntAt(byte[] raw, int offset) {
        return ((raw[offset] & 0xFFL) << 24) | ((raw[offset + 1] & 0xFFL) << 16)
                | ((raw[offset + 2] & 0xFFL) << 8) | (raw[offset + 3] & 0xFFL);
    }
}
