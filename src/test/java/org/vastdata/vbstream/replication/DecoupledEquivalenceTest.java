package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 解耦等价性验收（1.7 设计 §9.1）：同一字节流分别过同步消费（单线程直调 processBucket——既有
 * 33+ 用例的驱动形态，锚定 1.6 期望）与真实双线程管道（异步构造器 + close 排干），断言
 * Transaction 序列完全相等。
 *
 * <p>夹具约定（PgWire 实际签名对齐）：commit/streamCommit 不带 LSN 参数（占位 1/2 内建）、
 * relation 无 schema 参（固定 "public"）、streamAbort 为非 parallel 形态——组装器以
 * {@link StreamingMode#ON} 构造。管道目录取用例级 {@code @TempDir}（两个组装器顺序构造，
 * MessagePipe 的 wipe-on-open 保证互不残留）。
 */
class DecoupledEquivalenceTest {

    /** 每用例独立的管道目录：同步/异步两个组装器顺序复用，wipe-on-open 清彼此残留。 */
    @TempDir
    Path dir;

    /**
     * 责任：生成一段多形态字节流（普通事务 + 两阶段 + 流式交错 + 子事务回滚，PgWire 构造，
     * 与 TransactionAssemblerTest 同风格）。
     * 关键步骤：Relation 预置 → 普通事务（B/I/C）→ 两阶段（b/I/P/K，第三条提交分支）→
     * 流式事务（S/带前缀子事务单元/E/子事务回滚 A/c——被回滚子事务的单元在回放期被剔除，
     * 产出 0 变更的 STREAMED 事务）。
     * 边界：LSN/时间戳按 PgWire 占位约定（commitLsn=1、endLsn=2、PG 纪元）；'A' 为非
     * parallel 形态，驱动组装器须以 StreamingMode.ON 构造。纯函数，测试线程调用。
     */
    private static byte[][] mixedStream() {
        return new byte[][] {
                PgWire.relation(16384, "t", "id", "v"),
                PgWire.begin(101),
                PgWire.insert(16384, PgWire.tuple("1", "a")),
                PgWire.commit(),
                PgWire.beginPrepare(8001, "gid-1"),
                PgWire.insert(16384, PgWire.tuple("3", "c")),
                PgWire.prepare(8001, "gid-1"),
                PgWire.commitPrepared(8001, "gid-1"),
                PgWire.streamStart(7001, true),
                PgWire.streamed(7003, PgWire.insert(16384, PgWire.tuple("2", "b"))),
                PgWire.streamStop(),
                PgWire.streamAbort(7001, 7003),      // 子事务回滚：单元应在回放期被剔除
                PgWire.streamCommit(7001),
        };
    }

    /**
     * 责任：解耦等价本体——同步形态（包私有构造器，handoff 直调 processBucket）与异步形态
     * （public 构造器起 consumer 线程）喂同一字节流，close 后断言输出 Transaction 序列全等、
     * 输出前沿 = 末个输出事务的 endLsn。
     * 关键步骤：先驱动同步组装器（try-with-resources 收敛）→ 再驱动异步组装器 → close 触发
     * 排干协议（毒丸 → consumer 排干余桶 → join → pipe 关闭），排干后输出确定 → 逐字段全等断言。
     * 边界：异步 close 前队列里可能有未消费桶，join 保证断言时全部已输出（确定性）；前沿以
     * endLsn 单调 max 累加，末事务 endLsn 即其上界。
     * 线程约束：喂流与同步消费在测试线程；异步消费在 transaction-consumer 线程，close 的
     * join 建立测试线程断言前的 happens-before。
     */
    @Test
    void asyncPipelineEqualsSynchronous() {
        // 2.0 起组装器回调流式事件——两侧经 TransactionCollector 重组回整块（等价币），
        // assertEquals(syncOut, asyncOut) 断言零改动（Task 3 再升级为事件流全等）
        TransactionCollector syncCollector = new TransactionCollector();
        try (TransactionAssembler sync = new TransactionAssembler(
                syncCollector, StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg())) {
            for (byte[] m : mixedStream()) {
                sync.onRaw(m);
            }
        }
        List<Transaction> syncOut = syncCollector.transactions();
        TransactionCollector asyncCollector = new TransactionCollector();
        AtomicLong frontier = new AtomicLong();
        try (TransactionAssembler async = new TransactionAssembler(asyncCollector, StreamingMode.ON,
                new VersionedRelationRegistry(), pipeCfg(),
                (msg, view) -> { }, frontier, () -> { })) {
            for (byte[] m : mixedStream()) {
                async.onRaw(m);
            }
        }   // close：毒丸 → consumer 排干余桶 → join → pipe 关闭——排干后输出确定
        List<Transaction> asyncOut = asyncCollector.transactions();
        assertEquals(syncOut, asyncOut);
        assertEquals(asyncOut.get(asyncOut.size() - 1).endLsn(), frontier.get());   // 前沿 = 末个输出事务 endLsn
    }

    /** 组装器统一管道配置（用例级 @TempDir，滚动周期与生产默认同档）。 */
    private PipeConfig pipeCfg() {
        return new PipeConfig(dir, LegacyRollCycles.MINUTELY);
    }
}
