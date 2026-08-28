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
 * 轻窥路由基准（1.7 补 oid 窥探口径）：与 {@link DecodeBenchmark} 同一语料、同一循环游标
 * 形态，但不做完整解码。两个口径：
 *
 * <ul>
 *   <li>{@code peekRoute}（1.6 起沿用）：只读类型字节 + 流式块内的 Int32 xid 前缀——1.6 路由
 *       窥探的同构口径，基线序列可直接对照；</li>
 *   <li>{@code peekRouteWithOid}（1.7 新增）：另窥数据消息的 relation oid（I/U/D 单 Int32、
 *       T 读表数 + oid 数组、M 无）——与 1.7 组装器追加期 {@code collectOids} 同构
 *       （oidSet 供交接快照圈定），即 reader 记账路径在 xid 前缀之外新增的窥探分量。</li>
 * </ul>
 *
 * <p>口径与约束：结果累进一个 long 哈希并在返回值透出（防死码消除）；xid 以无符号 4 字节
 * big-endian 读法并入哈希（与组装器 unsignedInt 窥探一致）；'S'/'E' 维护块状态，块内七类
 * 前缀消息（M/R/Y/I/U/D/T）才有 4 字节前缀。两个基准方法各自独立 fork/迭代，state 按
 * （方法 × fork）各建一份，游标互不干扰。fork/warmup/measurement 由命令行参数给；单基准线程。
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

    /** 防死码消除的累进哈希（类型字节与窥得的 xid/oid 全部并入）。 */
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
     * 计时体（1.6 沿用口径）：对下一条语料消息做路由级窥探（类型字节 + 可选 xid 前缀），不做
     * 完整解码。关键步骤：读 raw[0] → 'S'/'E' 维护块状态 → 块内七类前缀消息读 raw[1..4]
     * 无符号值 → 类型与（若读到）xid 一并并入累进哈希返回。
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

    /**
     * 计时体（1.7 新增口径）：在 {@link #peekRoute} 的基础上对数据消息（I/U/D/T/M）另窥
     * relation oid 并入哈希——与组装器追加期 collectOids 同构：I/U/D 在类型字节（及块内
     * 4 字节前缀）后取 Int32；T 读 I32 表数 + 1 字节选项位后的 oid 数组（逐个并入）；
     * M 无 oid 跳过。xid 前缀照旧窥（oid 偏移由块内有无决定）。
     * 返回累进哈希（防死码消除）。
     */
    @Benchmark
    public long peekRouteWithOid() {
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
        long hash = sink = sink * 31L + type + xid;
        int base = inStream ? 5 : 1;   // 类型字节 + 可选 Int32 前缀之后（collectOids 同构）
        switch (type) {
            case 'I', 'U', 'D' -> hash = sink = sink * 31L + peekInt(raw, base);
            case 'T' -> {
                int n = peekInt(raw, base);
                for (int i = 0; i < n; i++) {
                    hash = sink = sink * 31L + peekInt(raw, base + 5 + 4 * i);   // I32 表数 + I8 选项位之后
                }
            }
            default -> { /* 'M' 无 oid；控制消息不入本口径 */ }
        }
        return hash;
    }

    /**
     * 责任：big-endian 读 4 字节有符号整数（oid 窥探，与组装器 RawPeeks.intAt 同语义：
     * 每字节先 &amp;0xFF 再移位拼接——byte 有符号，直接 | 会把符号位扩散到高位）。
     * 边界：偏移越界按数组越界上抛（语料损坏，fail-fast 优于静默错窥）。
     *
     * @param raw    消息原始字节
     * @param offset 读取起点
     * @return 该处 4 字节的 int 值（并入哈希用，符号位保留与否不影响哈希散布）
     */
    private static int peekInt(byte[] raw, int offset) {
        return ((raw[offset] & 0xFF) << 24) | ((raw[offset + 1] & 0xFF) << 16)
                | ((raw[offset + 2] & 0xFF) << 8) | (raw[offset + 3] & 0xFF);
    }
}
