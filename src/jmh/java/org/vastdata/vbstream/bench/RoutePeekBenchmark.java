package org.vastdata.vbstream.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 轻窥路由基准（Task 13）：与 {@link DecodeBenchmark} 同一语料、同一循环游标形态，但不做
 * 完整解码——只读类型字节 + 流式块内的 Int32 xid 前缀（与 {@code TransactionAssembler.onRaw}
 * 路由数据消息时的窥探同构：'S'/'E' 维护块状态，块内 M/R/Y/I/U/D/T 才有 4 字节前缀）。
 * 测得的 ns/条对照 DecodeBenchmark 的 µs/条，即"原始字节驱动组装"省下的解码成本比例
 * （组装器把解码推迟到提交期回放，路由期只付本基准的代价）。
 *
 * <p>口径与约束：结果累进一个 long 哈希并在返回值透出（防死码消除）；xid 以无符号 4 字节
 * big-endian 读法并入哈希（与组装器 unsignedInt 窥探一致）。fork/warmup/measurement 由
 * 命令行参数给；单基准线程。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class RoutePeekBenchmark {

    /** @Setup 加载的语料（与 DecodeBenchmark 同一份，保证两基准口径可直接相除）。 */
    List<byte[]> corpus;

    /** 循环游标（跨调用保位置，与 DecodeBenchmark 的推进方式一致）。 */
    int cursor;

    /** 流式块状态（'S' 置位 / 'E' 复位，与 decoder 的 inStream 同点同变）。 */
    boolean inStream;

    /** 防死码消除的累进哈希（类型字节与窥得的 xid 全部并入）。 */
    long sink;

    /**
     * 责任：加载语料（trial 级；窥探状态从块外起步，与真实流起点一致）。
     * 边界：语料缺失/损坏按 BenchCorpus 的带指引异常让本基准直接失败。
     */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        corpus = BenchCorpus.load();
        inStream = false;
    }

    /**
     * 计体：对下一条语料消息做路由级窥探（类型字节 + 可选 xid 前缀），不做完整解码。
     * 关键步骤：读 raw[0] → 'S'/'E' 维护块状态 → 块内七类前缀消息读 raw[1..4] 无符号值 →
     * 类型与（若读到）xid 一并并入累进哈希返回。
     * 返回累进哈希（防死码消除）。
     */
    @Benchmark
    public long peekRoute() {
        byte[] raw = corpus.get(cursor = (cursor + 1) % corpus.size());
        int type = raw[0];
        long xid = 0;
        if (type == 'S') {
            inStream = true;
        } else if (type == 'E') {
            inStream = false;
        } else if (inStream) {
            switch (type) {
                case 'M', 'R', 'Y', 'I', 'U', 'D', 'T' ->
                        xid = ((raw[1] & 0xFFL) << 24) | ((raw[2] & 0xFFL) << 16)
                                | ((raw[3] & 0xFFL) << 8) | (raw[4] & 0xFFL);
                default -> { /* 块外类型消息：无前缀可窥 */ }
            }
        }
        return sink = sink * 31L + type + xid;
    }
}
