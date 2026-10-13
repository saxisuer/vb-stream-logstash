package org.vastdata.vbstream.walsource.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.changes.ChangeStream;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.receive.WalStreamWalker;
import org.vastdata.vbstream.walsource.replay.CatalogReplay;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * vb-stream-wal-source 直解路的离线语料回放基准（wal-bench）——VastBase G100 真库
 * 采集的一段物理 WAL（{@link WalCorpus}，表全生命周期在窗内）单遍回放计时，核心结论
 * 为 <b>MB/s</b>（语料字节数 ÷ 单遍耗时）：1 op = walker 吃完整份语料，Throughput
 * ops/s × 语料 MiB 即 MB/s；records/s = ops/s × 语料记录数。两级被测面：
 *
 * <ol>
 *   <li><b>L1 记录层</b>（{@link #recordLayerParse}）：{@link WalStreamWalker} 走读
 *   （页/记录导航 + pageaddr 锚定 + contrecord 缝合）+ {@code WalRecordParser} 头区/
 *   块链解析——直接解析路的下界成本。</li>
 *   <li><b>L2 全链</b>（{@link #fullChainDecode}）：L1 之上叠 {@link CatalogReplay}
 *   四字典纯流内重建（无 JDBC 引导——语料窗含 CREATE TABLE 起始，词典从 pg_class/
 *   pg_attribute/pg_namespace/pg_partition 行事件积累；SelfHealer null = 纯回放禁
 *   回查档）+ {@link ChangeStream} DML 解码（XactGrouper 事务组装 / TOAST 重组 /
 *   磁盘值渲染，listener 计数丢弃）——与生产装配同构，仅缺网络接收与检查点。</li>
 * </ol>
 *
 * <p><strong>状态策略</strong>：语料 trial 级只读共享；walker/词典/组装器全部
 * <b>方法体内新建</b>——walker 的页锚定状态与字典的 ctid 重放状态跨遍历不可复用
 * （续传重放窗口非幂等），每次调用即一次全新回放。单遍耗时（数百 ms 至秒级）远大于
 * 状态构造（μs 级），方法内构造的开销占比可忽略。</p>
 *
 * <p>运行方式与结果表见 docs/benchmarks-baseline.md 的 wal-source 段（勿对带着语料跑
 * {@code mvn clean}——语料在 target/corpus 下）。</p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class VastCorpusBenchmark {

    /** 喂入 chunk 尺寸（字节）——64KB 与生产接收路 pgjdbc/直连通道的块量级对齐。 */
    static final int CHUNK_SIZE = 1 << 16;

    /**
     * 语料载体（trial 级装载，全基准只读共享）。
     */
    @State(Scope.Thread)
    public static class CorpusState {

        WalCorpus corpus;

        /**
         * trial 装载：读入语料与元信息（缺失即 ISE fail-fast 附采集指引——见
         * {@link WalCorpus#load()}）。
         */
        @Setup(Level.Trial)
        public void load() {
            corpus = WalCorpus.load();
        }
    }

    /**
     * L1 记录层：单遍走读 + 解析语料，返回解析记录数（JMH 返回值防死码消除——采集侧
     * 记录数即期望值，差值提示回放缺陷）。
     *
     * @param s 语料状态
     * @return 本遍解析记录数
     */
    @Benchmark
    public long recordLayerParse(CorpusState s) {
        WalCorpus corpus = s.corpus;
        AtomicLong records = new AtomicLong();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutVVastbase.INSTANCE,
                new WalStreamMetrics(), rec -> records.incrementAndGet());
        feedAll(walker, corpus);
        return records.get();
    }

    /**
     * L2 全链：L1 之上叠四字典纯流内重放 + DML 解码渲染（listener 丢弃计数），返回
     * 解析记录数与发射行数之和（两计数都进返回值防死码消除）。
     *
     * <p>装配次序与生产 WalSource sink 分发同构：每条记录先
     * {@link CatalogReplay#applyCatalogRecord}（字典施加）后
     * {@link ChangeStream#onRecord}（行解码）——as-of 次序正确性前提。whitelist 空 =
     * 用户表全放行；SQL 会话 null = TOAST 回查禁用的纯回放档（缺 chunk 降级
     * toast-unavailable，不 fail）。</p>
     *
     * @param s 语料状态
     * @return 解析记录数 + 发射行数
     */
    @Benchmark
    public long fullChainDecode(CorpusState s) {
        WalCorpus corpus = s.corpus;
        WalLayoutVVastbase layout = WalLayoutVVastbase.INSTANCE;
        CatalogStores stores = new CatalogStores();
        stores.pgAttrRelfilenode(corpus.pgAttrFilenode());
        stores.pgClassRelfilenode(corpus.pgClassFilenode());
        stores.pgNspRelnode(corpus.pgNspFilenode());
        stores.pgPartitionRelfilenode(corpus.pgPartFilenode());
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout));
        BenchCatalogSnapshot snapshot = new BenchCatalogSnapshot(stores);
        AtomicLong emittedRows = new AtomicLong();
        ChangeStream changes = new ChangeStream(snapshot, null, layout, null,
                new CountingListener(emittedRows));
        AtomicLong records = new AtomicLong();
        WalStreamWalker walker = new WalStreamWalker(layout, new WalStreamMetrics(), rec -> {
            records.incrementAndGet();
            replay.applyCatalogRecord(rec, stores);
            changes.onRecord(rec);
            snapshot.advanceTo((rec.lsn() + rec.totLen() + 7) & ~7L);
        });
        feedAll(walker, corpus);
        return records.get() + emittedRows.get();
    }

    /**
     * 把语料按 {@value #CHUNK_SIZE} 字节 chunk 逐块喂入 walker（chunkEndLsn 以语料
     * 起始 LSN 推绝对地址——与生产 feed 契约同形：锚 = 末字节之后的绝对 LSN）。
     *
     * <p>chunk 切片逐块拷贝（{@code Arrays.copyOfRange}）——真实接收路每块也是新数组，
     * 拷贝成本计入基准属口径内。线程约束：单线程（基准线程）。</p>
     *
     * @param walker 走读器（调用方全新构造）
     * @param corpus 语料
     */
    static void feedAll(WalStreamWalker walker, WalCorpus corpus) {
        byte[] data = corpus.data();
        long start = corpus.startLsn();
        for (int off = 0; off < data.length; off += CHUNK_SIZE) {
            int len = Math.min(CHUNK_SIZE, data.length - off);
            walker.feed(start + off + len, Arrays.copyOfRange(data, off, off + len));
        }
    }

    /**
     * 计数丢弃 listener（L2 下游替身）——Begin/End/Aborted no-op，RowChange 计数：
     * 渲染后的行值经计数器消费即弃（堆峰 O(1)，隔离下游 IO 成本——本基准量的是
     * 解析路而非落地路）。
     */
    private static final class CountingListener implements ChangeOutputListener {

        private final AtomicLong rows;

        /**
         * 装配计数 listener。
         *
         * @param rows 行计数器（基准线程外可读）
         */
        CountingListener(AtomicLong rows) {
            this.rows = rows;
        }

        /**
         * 事务头 no-op（expectedChanges 记账面不消费）。
         *
         * @param begin 事务头事件
         */
        @Override
        public void onBegin(BatchBegin begin) {
            // 下游替身不消费
        }

        /**
         * 行事件计数后丢弃。
         *
         * @param row 已渲染的行变更
         */
        @Override
        public void onRow(RowChange row) {
            rows.incrementAndGet();
        }

        /**
         * 事务尾 no-op。
         *
         * @param end 事务尾事件
         */
        @Override
        public void onEnd(BatchEnd end) {
            // 下游替身不消费
        }

        /**
         * 回滚事务 no-op（零输出契约）。
         *
         * @param aborted 回滚事件
         */
        @Override
        public void onAborted(BatchAborted aborted) {
            // 下游替身不消费
        }
    }
}
