package org.vastdata.debezium.connector.postgresql.stream;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 吞吐指标接线测试（MS5 Task 3，引擎 {@code ThroughputMetricsWiringTest} 的逐字翻译
 * ——观测目标从引擎组装器自建实例改为本侧构造注入实例，埋点点位与口径逐字节同构）：
 * PgWire 手造字节驱动同步形态组装器走完整业务路径，断言六项计数、事务大小分布与
 * rec 峰值在五个埋点点位全部生效——任何一处单行插桩漏挂（raw 消息入口 / handoff /
 * processBucket 尾 / 回放器逐单元 / 回放 sink 逐条 rec 秒桶）都会在此露馅。
 *
 * <p>两场景：①完整普通事务（Relation + Begin + 2×Insert + Commit）——slot 记 5 条
 * 消息与字节和，组装/输出各 1 tx，输出 records=2、字节=两条 Insert 载荷和，事务大小
 * 分布单样本 2 rec；②两阶段回滚——桶整体丢弃，组装与输出全零，但 slot 照记（字节
 * 确实从槽收到了）。
 *
 * <p>夹具沿 {@code StreamedTransactionAssemblerTest}：类级共享 @TempDir 管道目录
 * （wipe-on-open 顺序清空）、StreamingMode.ON、{@link TransactionRecorder} 作输出端、
 * {@link TestRelations} 假 resolver；指标实例由本测试自建经九参同步构造注入（生产路径
 * 由 source 的 execute 建同款实例注入，口径无差）。
 */
class StreamThroughputMetricsWiringTest {

    private static final int OID = 16384;

    /** 类级共享管道目录：全类一份，用例间由 MessagePipe 的 wipe-on-open 顺序清空（@TempDir NEVER 见模块测试规约）。 */
    @TempDir(cleanup = CleanupMode.NEVER)
    static Path PIPE_DIR;

    /**
     * 完整普通事务全链路五埋点核对：slot 记 5 条消息与字节和（含 Relation 与控制消息）、
     * handoff 记组装 1 tx；输出侧记 1 tx / 2 rec、字节仅两条 Insert 载荷（Relation 与
     * 控制消息不回读）、rec 峰值非 n/a（第五埋点在回放 sink 逐条驱动）。事务大小分布
     * 恰一个样本 2 rec。
     * 峰值断言只验"非 n/a"：真实时钟下 2 条记录可能落同秒或跨秒，具体数值不确定。
     */
    @Test
    void normalTransactionRecordsAllSixCountersAndRecPeak() {
        TransactionRecorder out = new TransactionRecorder();
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(System.nanoTime());
        try (StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(out, StreamingMode.ON,
                new VersionedRelationRegistry(), TestRelations.RESOLVER, PIPE_DIR,
                LegacyRollCycles.MINUTELY, (msg, view) -> { },
                BucketTableResolver.snapshotBacked(), metrics)) {
            byte[] rel = PgWire.relation(OID, "t", "id", "v");
            byte[] begin = PgWire.begin(5001L);
            byte[] ins1 = PgWire.insert(OID, PgWire.tuple("1", "a"));
            byte[] ins2 = PgWire.insert(OID, PgWire.tuple("2", "b"));
            byte[] commit = PgWire.commit();
            assembler.onRaw(rel);
            assembler.onRaw(begin);
            assembler.onRaw(ins1);
            assembler.onRaw(ins2);
            assembler.onRaw(commit);

            StreamThroughputMetrics.Totals t = metrics.totals();
            assertEquals(5, t.slotMessages(), "slot 应记全部 5 条消息(含 Relation 与控制消息)");
            assertEquals(rel.length + begin.length + ins1.length + ins2.length + commit.length,
                    t.slotBytes(), "slot 字节应为 5 条消息长度和");
            assertEquals(1, t.assembledTxs(), "handoff 应记组装 1 tx");
            assertEquals(1, t.outputTxs(), "processBucket 尾应记输出 1 tx");
            assertEquals(2, t.outputRecords(), "输出 records 应为实付 2 条 TxChange");
            assertEquals(ins1.length + ins2.length, t.outputBytes(),
                    "输出字节应为回放重读的两条 Insert 载荷和(Relation/控制消息不回读)");

            List<String> lines = metrics.reportLines(System.nanoTime() + 10_000_000_000L);
            assertTrue(lines.get(1).contains("p90=2 rec"), "事务大小分布应见单样本 2 rec: " + lines.get(1));
            assertTrue(!lines.get(2).contains("n/a rec/s"),
                    "rec 峰值非 n/a 即证明第五埋点(回放 sink 逐条)已接线: " + lines.get(2));
        }
    }

    /**
     * 两阶段回滚的口径边界：BeginPrepare + Insert + Prepare + RollbackPrepared——桶整体
     * 丢弃（不交接、不回放），组装与输出计数及两分布全零（报告行 n/a）；slot 照记
     * 5 条消息——回滚的字节确实从槽读到了，读取吞吐不该假装没发生。
     */
    @Test
    void twoPhaseRollbackSkipsOutputButStillCountsSlotRead() {
        TransactionRecorder out = new TransactionRecorder();
        StreamThroughputMetrics metrics = new StreamThroughputMetrics(System.nanoTime());
        try (StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(out, StreamingMode.ON,
                new VersionedRelationRegistry(), TestRelations.RESOLVER, PIPE_DIR,
                LegacyRollCycles.MINUTELY, (msg, view) -> { },
                BucketTableResolver.snapshotBacked(), metrics)) {
            byte[] rel = PgWire.relation(OID, "t", "id", "v");
            byte[] beginPrepare = PgWire.beginPrepare(6001L, "gid-rollback");
            byte[] ins = PgWire.insert(OID, PgWire.tuple("1", "a"));
            byte[] prepare = PgWire.prepare(6001L, "gid-rollback");
            byte[] rollback = PgWire.rollbackPrepared(6001L, "gid-rollback");
            assembler.onRaw(rel);
            assembler.onRaw(beginPrepare);
            assembler.onRaw(ins);
            assembler.onRaw(prepare);
            assembler.onRaw(rollback);

            StreamThroughputMetrics.Totals t = metrics.totals();
            assertEquals(5, t.slotMessages(), "slot 照记 5 条消息(含 Relation)");
            assertEquals(rel.length + beginPrepare.length + ins.length + prepare.length + rollback.length,
                    t.slotBytes());
            assertEquals(0, t.assembledTxs(), "回滚桶不交接,组装计数为零");
            assertEquals(0, t.outputTxs());
            assertEquals(0, t.outputRecords());
            assertEquals(0, t.outputBytes());

            List<String> lines = metrics.reportLines(System.nanoTime() + 10_000_000_000L);
            assertEquals("分布: 回放耗时 n/a | 事务大小 n/a", lines.get(1), "零样本分布应打 n/a");
        }
    }
}
