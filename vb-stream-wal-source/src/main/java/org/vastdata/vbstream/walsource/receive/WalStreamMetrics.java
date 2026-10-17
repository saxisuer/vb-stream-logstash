package org.vastdata.vbstream.walsource.receive;

import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.util.HashMap;
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
 * 对齐 spike 的 record census 输出面（wal-direct-decode-spike.md 发现 10 的普查延续）
 * ——2026-10-17 性能优化：键经懒填充键表（{@code censusKeys[256][16]}，封闭键空间）
 * 预计算、值为 LongAdder，热路径零字符串拼接零装箱；键形与快照输出不变。</p>
 *
 * <p>线程约束：计数与 census 写入仅接收线程（Task 8 的 readPending 循环，单写者）；
 * {@link #censusSnapshot()} 自 Task 9 起被冒烟 Main 的周期行活轮询——快照与写入并发
 * （多读单写），census 载体须为 {@link ConcurrentHashMap}（弱一致遍历不抛 CME）。
 * {@link #censusSnapshot()} 返回 {@code Map.copyOf} 不可变副本供任意线程安全读取。
 * LongAdder 字段为 public final——热路径原地自增免方法调用开销，快照读取走
 * {@code sum()}。<strong>只读契约（终审 M2）：public LongAdder 字段为接收线程热路径
 * 自增设计，api 门面（{@code WalSource.metrics()} 等）返回的实例对调用方仅约定只读
 * ——外部 {@code increment()/reset()} 会污染协议计数锚（IT 以 reconnects/resyncs 断言
 * 行为证据），属契约违约。</strong></p>
 */
public final class WalStreamMetrics {

    /** 已交付 sink 的解析记录数。 */
    public final LongAdder records = new LongAdder();

    /** pageaddr 锚定失配后的受控再同步次数（每次丢弃错位字节并重锚计 1）。 */
    public final LongAdder resyncs = new LongAdder();

    /** <strong>丢字节</strong>再同步次数（resyncs 中 dropped&gt;0 的子集——错位记录被
     *  越过的数据丢失面；dropped==0 的重锚是服务端空页跳过的良性形态，Task 13 实证
     *  物理流可跳过未初始化页，对拍完整性由内容全等兜底）。 */
    public final LongAdder lossyResyncs = new LongAdder();

    /** 跨页记录缝合消费的续体页头数（每越过一个续体页计 1）。 */
    public final LongAdder contrecords = new LongAdder();

    /** 孤立续体页（记录头在窗口之前）跳过数。 */
    public final LongAdder orphanSkips = new LongAdder();

    /** <strong>整零页</strong>跳过数（Task 8 零页容忍档——OG file 通道专用：pg_receivexlog
     *  落盘段对服务端跳过的未初始化页与段尾零区以零填充补位，tail 侧 walker 在页界遇
     *  整页全零且页头魔数不符时跳过整页并计本数，而非走再同步/ISE。与 resyncs 分列：
     *  零页跳过是确定性的补位形态（无错位、无丢字节），不应污染锚定失配的观测面。
     *  仅 {@code WalLayout.tolerateAllZeroPages()} 档激活（V17/V18 恒 0）。 */
    public final LongAdder zeroPageSkips = new LongAdder();

    /** carry 衔接失配丢弃次数（chunkStart 与游标不连续 → 丢弃未消费 carry；与 resyncs
     *  分列——resyncs 记页头锚定层的重锚，carryDrops 记 feed 衔接层的丢弃）。 */
    public final LongAdder carryDrops = new LongAdder();

    /** 断流重连次数（每次 SQLException 触发的重连调度自增 1；接收器单写，walker 不写）——
     *  断流原地重连 IT 的行为证据面（重连后位点不回退须伴随本计数 &gt; 0）。 */
    public final LongAdder reconnects = new LongAdder();

    /** rmid/info 形态普查：键 {@code "rmid/hex(info&0xF0)"}，值为 LongAdder（增量无
     *  装箱）。写入单写者（接收线程），读取经 {@link #censusSnapshot()} 弱一致快照
     *  ——多读单写下须并发安全载体（HashMap 在并发遍历下会 CME 杀死读者，Task 9
     *  审查 High 修复）。2026-10-17 性能优化：值从 {@code Long} 改 LongAdder，热路径
     *  {@code computeIfAbsent(...).increment()} 零装箱。 */
    private final ConcurrentHashMap<String, LongAdder> census = new ConcurrentHashMap<>();

    /** census 懒填充键表 {@code [rmid][高 nibble]}——键空间封闭（rmid 是 u8 值域
     *  0-255、{@code (info&0xF0)>>4} ∈ 0-15，恰 256×16 槽），由 {@link #countRecord}
     *  单写者（接收线程）在首次命中时拼接填充，此后热路径命中槽位零字符串拼接。
     *  数组界内保证来源：WalRecord.rmid/info 均由记录头 u8 解码而来，构造期越界值
     *  不可达；非单写者并发填充会重复拼接（幂等同串）但本类不提供该契约。 */
    private final String[][] censusKeys = new String[256][16];

    /**
     * 诊断用字符串形态（六计数概览）——IT/冒烟的指标行打印面（Task 13）。
     *
     * @return 形如 {@code records=.. resyncs=.. contrecords=.. orphanSkips=.. carryDrops=.. reconnects=..}
     */
    @Override
    public String toString() {
        return "records=" + records.sum() + " resyncs=" + resyncs.sum()
                + " lossyResyncs=" + lossyResyncs.sum()
                + " contrecords=" + contrecords.sum() + " orphanSkips=" + orphanSkips.sum()
                + " zeroPageSkips=" + zeroPageSkips.sum()
                + " carryDrops=" + carryDrops.sum() + " reconnects=" + reconnects.sum();
    }

    /**
     * 记一条已交付记录：records 计数并归入 census 形态桶。
     *
     * <p>关键步骤：records 自增后按 {@code info & 0xF0} 折叠操作码高半字节（低 nibble
     * 保留位不参与分桶），与 spike census 键形一致。热路径优化（2026-10-17）：先查
     * {@code censusKeys[rmid][高nibble]} 懒填充键表，仅首次命中拼接字符串，此后零
     * 拼接零装箱（LongAdder increment）。线程约束：仅接收线程调用（键表填充是单写者
     * 前提——非本线程并发填充不在契约内）。</p>
     *
     * @param rec 已解析交付的记录
     */
    public void countRecord(WalRecord rec) {
        records.increment();
        int rmid = rec.rmid();
        int nib = rec.info() & 0xF0;
        String key = censusKeys[rmid][nib >> 4];
        if (key == null) {
            key = rmid + "/" + Integer.toHexString(nib);
            censusKeys[rmid][nib >> 4] = key;
        }
        census.computeIfAbsent(key, k -> new LongAdder()).increment();
    }

    /**
     * census 只读快照。
     *
     * <p>返回不可变副本（{@code Map.copyOf}），后续写入不影响已取快照；快照与写入
     * <strong>可能并发</strong>（冒烟 Main 的周期统计行/IT 指标行在别的线程活轮询
     * ——接收线程仍在写 census），CHM 载体 + 弱一致遍历拷贝即线程安全，无需加锁；
     * 任意线程可读。值语义：LongAdder 经 {@code sum()} 折叠为 Long，与改造前
     * 装箱 Long 计数对外逐字同形（键形/值域/不可变性均不变）。</p>
     *
     * @return 键值不可变副本（键 {@code "rmid/hex(info&0xF0)"}，值条数）
     */
    public Map<String, Long> censusSnapshot() {
        Map<String, Long> out = new HashMap<>(census.size());
        census.forEach((k, v) -> out.put(k, v.sum()));
        return Map.copyOf(out);
    }
}
