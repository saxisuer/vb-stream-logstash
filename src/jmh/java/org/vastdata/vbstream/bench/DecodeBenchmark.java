package org.vastdata.vbstream.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 协议解码基准（Task 13）：整语料按序推过一个顺序态 {@link PgOutputDecoder}（PARALLEL，
 * 与线上会话同构），测"一条 pgoutput 消息的完整解码"平均耗时（µs/条）——含 tuple 逐列
 * 解析与剩余字节校验，是 peek 路由（{@link RoutePeekBenchmark}）的对照上限。
 *
 * <p>口径与约束：
 * <ul>
 *   <li>游标在语料上循环推进（一条调用一条消息），decoder 的 inStream 状态机随真实流序
 *       演进——'S'/'E' 置位/复位与录制时一致；语料收尾于 Commit（块外），回卷到首条 'B'
 *       时状态正确归零，循环重放不产生错位</li>
 *   <li>MB/s 口径由本结果换算：语料总字节 / (µs/条 × 条数)，见 docs/benchmarks-baseline.md</li>
 *   <li>fork/warmup/measurement 由命令行参数给（冒烟档 {@code -f 1 -w 1s -r 2s}），类上不硬编码</li>
 *   <li>线程：单基准线程（decoder 非线程安全，@State(Scope.Thread) 每线程独立实例）</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class DecodeBenchmark {

    /** @Setup 加载的语料（84 条真实录制消息，见 BenchCorpus）。 */
    List<byte[]> corpus;

    /** 顺序态解码器：跨调用保流块状态（inStream），正是被测对象的生命周期形态。 */
    PgOutputDecoder decoder;

    /** 循环游标（字段保跨调用位置；无符号回卷见 benchmark 方法内取模）。 */
    int cursor;

    /**
     * 责任：加载语料并新建顺序态解码器（trial 级——语料只读共享，decoder 状态即被测状态机）。
     * 边界：语料缺失/损坏按 BenchCorpus/CorpusLoader 的带指引异常让本基准直接失败。
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        corpus = BenchCorpus.load();
        decoder = new PgOutputDecoder(StreamingMode.PARALLEL);
    }

    /**
     * 计时体：取下一条语料消息做完整解码。
     * 返回解码产物（防死码消除；record 不可变无逃逸开销差异，与线上消费形态一致）。
     */
    @Benchmark
    public PgOutputMessage decodeOne() {
        byte[] raw = corpus.get(cursor = (cursor + 1) % corpus.size());
        return decoder.decode(ByteBuffer.wrap(raw));
    }
}
