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
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 同步形态组装基准（1.7 换口径，类名沿用 1.6 的 AssembleMemory 以保持基线序列可对照）：
 * **同步** {@link TransactionAssembler}（不开 consumer 线程——交接在调用线程直调
 * processBucket）逐条吃进**整份语料**，测"一个完整语料轮次的组装总成本"（ms/轮，含
 * pipe.append、路由窥探与 oid 窥探、桶段记账、交接快照拷贝、回放解码与 Relation 快照 asOf
 * 渲染；listener/observer 均为 no-op）。与 {@link PipePathBenchmark} 的回放口径对照可见
 * 回放半程在总成本中的占比；与 {@link DecodeBenchmark} 对照可见组装开销中解码的占比。
 * 1.7 解耦后线上真实形态是异步（reader 记账 + consumer 回放分线程），本基准的同步形态把
 * 两半程合并在单线程计量——两边之和即端到端成本，异步拆分本身只挪线程不改总量。
 *
 * <p>口径与约束：同一组装器实例跨调用反复重放同一语料——语料收尾时全部桶已闭合
 * （Commit/StreamCommit/StreamAbort 终结），重放起点状态干净，这是合法且贴近长连接会话的
 * 形态；VersionedRelationRegistry 的版本日志在桶完结点被低水位剪枝（每轮约 4 条 'R'，剪后
 * 只随不同表 oid 数增长），asOf 二分不构成本基准的主导项。临时管道目录为 @TempDir 语义：
 * Setup 建、TearDown 删（目录经组装器构造时 wipe-on-open，恒为本次运行的队列文件）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AssembleMemoryBenchmark {

    /** @Setup 加载的语料。 */
    List<byte[]> corpus;

    /** 被测组装器（同步形态；跨调用复用，重放轮次共享 Relation 版本日志与同一条管道）。 */
    TransactionAssembler assembler;

    /** 临时管道目录（@TempDir 语义：Setup 建 / TearDown 删）。 */
    Path pipeDir;

    /**
     * 责任：加载语料、建临时目录、组装 no-op listener/observer 的同步形态组装器（构造即建管道
     * 并起零线程——同步形态无 consumer）。
     * 边界：语料缺失按 BenchCorpus 带指引异常失败；管道建立失败（磁盘/IO）按 Chronicle 异常
     * 上抛；临时目录建失败按 IOException 上抛。
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        corpus = BenchCorpus.load();
        pipeDir = Files.createTempDirectory("bench-assemble-sync");
        assembler = new TransactionAssembler(tx -> { }, StreamingMode.PARALLEL,
                new VersionedRelationRegistry(),
                new PipeConfig(pipeDir, LegacyRollCycles.MINUTELY),
                (msg, view) -> { });
    }

    /**
     * 责任：teardown——关组装器（同步形态直接关管道，无 consumer 线程可排干）后递归删临时目录。
     * 边界：删除失败按 IOException 上抛（mmap 未释放类问题应显式暴露而非静默漏盘）。
     */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        assembler.close();
        CorpusLoader.deleteRecursively(pipeDir);
    }

    /**
     * 计时体：整份语料逐条喂组装器（一轮 = 84 条消息的全部 append/路由/记账/交接/回放成本）。
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
