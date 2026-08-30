package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 解耦等价性与输出形态等价验收（1.7 设计 §9.1 + 2.0 spec §5.1）：同一字节流分别过
 * 三种形态——同步消费（单线程直调 processBucket——既有 33+ 用例的驱动形态，锚定 1.6 期望）、
 * 真实双线程管道（异步构造器 + close 排干）、block 输出形态（异步构造器 +
 * {@link StreamingToBlockAdapter} 重组整块）——断言三者产物全等。
 *
 * <p>三重断言（2.0 升级）：① 同步/异步**完整事件流**全等（{@code List<TransactionEvent>}
 * 含 Begin 头与 End 尾元数据——比整块 Transaction 更严，头尾进断言）；② 整块
 * {@code List<Transaction>} 全等（经 {@link TransactionRecorder} 重组，1.7 既有断言的
 * 双保险）；③ block 适配器转发序列与收集器重组结果全等（流式/block 两模式输出语义一致性）。
 *
 * <p>夹具约定（PgWire 实际签名对齐）：commit/streamCommit 不带 LSN 参数（占位 1/2 内建）、
 * relation 无 schema 参（固定 "public"）、streamAbort 为非 parallel 形态——组装器以
 * {@link StreamingMode#ON} 构造。管道目录取用例级 {@code @TempDir}（三个组装器顺序构造，
 * MessagePipe 的 wipe-on-open 保证互不残留）。
 */
class DecoupledEquivalenceTest {

    /** 每用例独立的管道目录：三个组装器顺序复用，wipe-on-open 清彼此残留。 */
    @TempDir
    Path dir;

    /**
     * 责任：生成一段多形态字节流（普通事务 + 两阶段 + 流式交错 + 子事务回滚，PgWire 构造，
     * 与 TransactionAssemblerTest 同风格）。
     * 关键步骤：Relation 预置 → 普通事务（B/I/C）→ 两阶段（b/I/P/K，第三条提交分支）→
     * 流式事务（S/带前缀子事务单元/E/子事务回滚 A/c——被回滚子事务的单元在回放期被剔除，
     * 产出 0 变更的 STREAMED 事务，其事件流为 {@code Begin(expected=1) → End(0)}，头尾元数据
     * 的全等因此覆盖 emitted&lt;expected 边界）。
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
     * 责任：解耦等价本体——同一字节流依次驱动三个组装器形态，close 后断言三重全等与前沿推进。
     * 关键步骤：① 同步形态（包私有构造器，handoff 直调 processBucket）收集事件流 + 收集器重组
     * → ② 异步形态（public 构造器起 consumer 线程）同样双收集 → ③ block 形态（异步构造器 +
     * StreamingToBlockAdapter 攒整块转发）→ 逐级断言：事件流全等、整块全等（双保险）、block 产物
     * 与收集器重组全等、两个异步形态的前沿 = 末个输出事务的 endLsn（End 返回后推进，两模式
     * 语义一致）。
     * 边界：异步 close 前队列里可能有未消费桶，join 保证断言时全部已输出（确定性）；事件流
     * 收集列表（ArrayList）在 join 建立的 happens-before 之后于测试线程读取；前沿以 endLsn
     * 单调 max 累加，末事务 endLsn 即其上界。
     * 线程约束：喂流与同步消费在测试线程；异步/block 消费在 transaction-consumer 线程，
     * 两次 close 的 join 建立测试线程断言前的 happens-before。
     */
    @Test
    void asyncPipelineEqualsSynchronous() {
        byte[][] stream = mixedStream();
        // ① 同步形态：事件流直攒 + 收集器并行重组（同一 lambda 双写——事件流断言与整块断言共用驱动）
        List<TransactionEvent> syncEvents = new ArrayList<>();
        TransactionRecorder syncCollector = new TransactionRecorder();
        try (TransactionAssembler sync = new TransactionAssembler(
                dualCapture(syncEvents, syncCollector), StreamingMode.ON,
                new VersionedRelationRegistry(), pipeCfg())) {
            feed(sync, stream);
        }
        // ② 异步形态：真实双线程管道，同样双收集
        List<TransactionEvent> asyncEvents = new ArrayList<>();
        TransactionRecorder asyncCollector = new TransactionRecorder();
        AtomicLong frontier = new AtomicLong();
        try (TransactionAssembler async = new TransactionAssembler(dualCapture(asyncEvents, asyncCollector),
                StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg(),
                (msg, view) -> { }, frontier, () -> { })) {
            feed(async, stream);
        }   // close：毒丸 → consumer 排干余桶 → join → pipe 关闭——排干后输出确定
        // ③ block 形态：同一字节流再过一异步组装器，StreamingToBlockAdapter 攒整块转发（1.7 语义逃生门）
        List<Transaction> blockOut = new ArrayList<>();
        AtomicLong blockFrontier = new AtomicLong();
        try (TransactionAssembler block = new TransactionAssembler(new StreamingToBlockAdapter(blockOut::add),
                StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg(),
                (msg, view) -> { }, blockFrontier, () -> { })) {
            feed(block, stream);
        }
        // 断言一（2.0 升级）：完整事件流全等——Begin/End 头尾元数据进断言，比整块更严
        assertEquals(syncEvents, asyncEvents);
        // 断言二（双保险）：整块 Transaction 序列全等——1.7 既有等价币经收集器存活
        List<Transaction> syncOut = syncCollector.transactions();
        List<Transaction> asyncOut = asyncCollector.transactions();
        assertEquals(syncOut, asyncOut);
        // 断言三（spec §5.1）：block 适配器转发序列与收集器重组结果全等——两模式输出语义一致
        assertEquals(syncOut, blockOut);
        assertEquals(asyncOut.get(asyncOut.size() - 1).endLsn(), frontier.get());   // 前沿 = 末个输出事务 endLsn
        assertEquals(blockOut.get(blockOut.size() - 1).endLsn(), blockFrontier.get());   // block 模式前沿语义同
    }

    /**
     * 责任：构造"事件流直攒 + 收集器重组"的双写 listener（同步/异步两侧共用同一捕获形态，
     * 保证两侧断言输入的同构性）。
     * 边界：events 列表只在 consumer（或同步调用）线程写、join 后测试线程读；collector 的
     * 流合法性校验（End 对账等）在写入路径内联生效，违约即抛 ISE 直传驱动方。
     */
    private static StreamingTransactionListener dualCapture(List<TransactionEvent> events, TransactionRecorder collector) {
        return event -> {
            events.add(event);
            collector.onEvent(event);
        };
    }

    /**
     * 责任：把一段录制字节流按序喂给组装器（三个形态共用的驱动步骤）。
     * 边界：纯遍历转发，onRaw 的全部 fail-fast 语义原样上抛（违约即用例失败）。
     * 线程：测试线程（异步形态的 reader 角色）。
     */
    private static void feed(TransactionAssembler assembler, byte[][] stream) {
        for (byte[] m : stream) {
            assembler.onRaw(m);
        }
    }

    /** 组装器统一管道配置（用例级 @TempDir，滚动周期与生产默认同档）。 */
    private PipeConfig pipeCfg() {
        return new PipeConfig(dir, LegacyRollCycles.MINUTELY);
    }
}
