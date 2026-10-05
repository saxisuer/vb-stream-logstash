package org.vastdata.vbstream.walsource.receive;

import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * WAL 流走读计数器：四项 LongAdder 计数 + rmid/info 形态普查（census）。
 *
 * <p>计数项语义：{@code records}=已交付解析记录数；{@code resyncs}=pageaddr 锚定失配后
 * 受控再同步次数（spike 发现 23b 的正式观测面）；{@code contrecords}=跨页记录缝合时
 * 消费的续体页头数；{@code orphanSkips}=流首/再同步点撞上的孤立续体页跳过数
 * （spike 发现 11）。census 以 {@code "rmid/" + hex(info&0xF0)} 为键记各形态条数，
 * 对齐 spike 的 record census 输出面（wal-direct-decode-spike.md 发现 10 的普查延续）。</p>
 *
 * <p>线程约束：设计为接收线程单写者独占（Task 8 的 readPending 循环），census 用
 * 普通 HashMap 即可；{@link #censusSnapshot()} 返回不可变副本供任意线程读取。
 * LongAdder 字段为 public final——热路径原地自增免方法调用开销，快照读取走
 * {@code sum()}。</p>
 */
public final class WalStreamMetrics {

    /** 已交付 sink 的解析记录数。 */
    public final LongAdder records = new LongAdder();

    /** pageaddr 锚定失配后的受控再同步次数（每次丢弃错位字节并重锚计 1）。 */
    public final LongAdder resyncs = new LongAdder();

    /** 跨页记录缝合消费的续体页头数（每越过一个续体页计 1）。 */
    public final LongAdder contrecords = new LongAdder();

    /** 孤立续体页（记录头在窗口之前）跳过数。 */
    public final LongAdder orphanSkips = new LongAdder();

    /** rmid/info 形态普查：键 {@code "rmid/hex(info&0xF0)"}，值为条数（单写者线程独占）。 */
    private final HashMap<String, Long> census = new HashMap<>();

    /**
     * 记一条已交付记录：records 计数并归入 census 形态桶。
     *
     * <p>关键步骤：records 自增后按 {@code info & 0xF0} 折叠操作码高半字节（低 nibble
     * 保留位不参与分桶），与 spike census 键形一致。线程约束：仅接收线程调用。</p>
     *
     * @param rec 已解析交付的记录
     */
    public void countRecord(WalRecord rec) {
        records.increment();
        census.merge(rec.rmid() + "/" + Integer.toHexString(rec.info() & 0xF0), 1L, Long::sum);
    }

    /**
     * census 只读快照。
     *
     * <p>返回不可变副本（{@code Map.copyOf}），后续写入不影响已取快照；任意线程可读。
     * 快照时点 census 无并发写（单写者约束），无需加锁。</p>
     *
     * @return 键值不可变副本（键 {@code "rmid/hex(info&0xF0)"}，值条数）
     */
    public Map<String, Long> censusSnapshot() {
        return Map.copyOf(census);
    }
}
