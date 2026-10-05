package org.vastdata.vbstream.walsource.receive;

import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * WAL 流走读计数器：六项 LongAdder 计数 + rmid/info 形态普查（census）。
 *
 * <p>计数项语义：{@code records}=已交付解析记录数；{@code resyncs}=pageaddr 锚定失配后
 * 受控再同步次数（spike 发现 23b 的正式观测面）；{@code contrecords}=跨页记录缝合时
 * 消费的续体页头数；{@code orphanSkips}=流首/再同步点撞上的孤立续体页跳过数
 * （spike 发现 11）；{@code carryDrops}=feed 衔接校验失配的 carry 丢弃次数（与 resyncs
 * 分列：衔接层丢弃 vs 页头锚定层重锚）；{@code reconnects}=接收器断流重连次数（Task 8
 * 起——每次 SQLException 触发的重连调度自增 1，由 WalStreamReceiver 维护，walker 不写）。
 * census 以 {@code "rmid/" + hex(info&0xF0)} 为键记各形态条数，
 * 对齐 spike 的 record census 输出面（wal-direct-decode-spike.md 发现 10 的普查延续）。</p>
 *
 * <p>线程约束：计数与 census 写入仅接收线程（Task 8 的 readPending 循环，单写者）；
 * {@link #censusSnapshot()} 自 Task 9 起被冒烟 Main 的周期行活轮询——快照与写入并发
 * （多读单写），census 载体须为 {@link ConcurrentHashMap}（弱一致遍历不抛 CME）。
 * {@link #censusSnapshot()} 返回 {@code Map.copyOf} 不可变副本供任意线程安全读取。
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

    /** carry 衔接失配丢弃次数（chunkStart 与游标不连续 → 丢弃未消费 carry；与 resyncs
     *  分列——resyncs 记页头锚定层的重锚，carryDrops 记 feed 衔接层的丢弃）。 */
    public final LongAdder carryDrops = new LongAdder();

    /** 断流重连次数（每次 SQLException 触发的重连调度自增 1；接收器单写，walker 不写）——
     *  断流原地重连 IT 的行为证据面（重连后位点不回退须伴随本计数 &gt; 0）。 */
    public final LongAdder reconnects = new LongAdder();

    /** rmid/info 形态普查：键 {@code "rmid/hex(info&0xF0)"}，值为条数。写入单写者（接收
     *  线程），读取经 {@link #censusSnapshot()} 弱一致快照——多读单写下须并发安全载体
     *  （HashMap 在并发遍历下会 CME 杀死读者，Task 9 审查 High 修复）。 */
    private final ConcurrentHashMap<String, Long> census = new ConcurrentHashMap<>();

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
