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
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.replication.SpillConfig;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 纯内存组装基准（Task 13）：threshold=∞（spill 启用但永不可越——与线上默认配置同路径，
 * 含水位记账与越限检查本身的成本）的 {@link TransactionAssembler} 逐条吃进**整份语料**，
 * 测"一个完整语料轮次的组装总成本"（ms/轮，含路由窥探、桶记账、提交期回放解码与 Relation
 * asOf 渲染；listener/observer 均为 no-op）。与 {@link SpillPathBenchmark} 的 SPILLED
 * 回放对照即溢写代价；与 {@link DecodeBenchmark} 对照可见组装开销中解码的占比。
 *
 * <p>口径与约束：同一组装器实例跨调用反复重放同一语料——语料收尾时全部桶已闭合
 * （Commit/StreamCommit/StreamAbort 终结），重放起点状态干净，这是合法且贴近长连接会话的
 * 形态；VersionedRelationRegistry 的版本日志随轮次增长（每轮约 4 条 'R'），asOf 二分在
 * 数万版本内不构成本基准的主导项。临时 spill 目录为 @TempDir 语义：Setup 建、TearDown 删
 * （threshold=∞ 下 spool 永不创建、目录恒空，仅满足 SpillConfig 的路径契约）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AssembleMemoryBenchmark {

    /** @Setup 加载的语料。 */
    List<byte[]> corpus;

    /** 被测组装器（threshold=∞；跨调用复用，重放轮次共享 Relation 版本日志）。 */
    TransactionAssembler assembler;

    /** 临时 spill 目录（@TempDir 语义：Setup 建 / TearDown 删）。 */
    Path spillDir;

    /**
     * 责任：加载语料、建临时目录、组装 no-op listener/observer 的 threshold=∞ 组装器。
     * 边界：语料缺失按 BenchCorpus 带指引异常失败；临时目录建失败按 IOException 上抛。
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        corpus = BenchCorpus.load();
        spillDir = Files.createTempDirectory("bench-assemble-memory");
        assembler = new TransactionAssembler(tx -> { }, StreamingMode.PARALLEL,
                new VersionedRelationRegistry(),
                new SpillConfig(Long.MAX_VALUE, spillDir, LegacyRollCycles.MINUTELY),
                msg -> { });
    }

    /**
     * 责任：teardown——关组装器（释放可能建立的 spool；本基准下恒无）后递归删临时目录。
     * 边界：删除失败按 IOException 上抛（mmap 未释放类问题应显式暴露而非静默漏盘）。
     */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        assembler.close();
        CorpusLoader.deleteRecursively(spillDir);
    }

    /**
     * 计时体：整份语料逐条喂组装器（一轮 = 84 条消息的全部路由/记账/回放成本）。
     * 返回消息条数（防死码消除）。
     */
    @Benchmark
    public int assembleWholeCorpus() {
        for (byte[] raw : corpus) {
            assembler.onRaw(raw);
        }
        return corpus.size();
    }
}
