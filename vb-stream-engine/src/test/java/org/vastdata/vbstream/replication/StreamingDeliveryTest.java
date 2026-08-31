package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流式交付时序证明（2.0 spec §5.2 的核心验收）：变更事件在回放<b>进行中</b>即已到达 listener、
 * 先于事务完成——直接证明"边回放边输出"。1.7 整块形态下本断言不可能成立：listener 在
 * {@code onTransaction} 返回前什么都收不到，"收到"与"事务完成"不可分；2.0 单回调流式契约
 * 把两者拆开，本用例即钉住这个差异。
 *
 * <p>夹具约定（PgWire 占位对齐 {@link DecoupledEquivalenceTest}）：单事务 3 条 Insert（普通
 * NORMAL 路径），异步构造器起真实 {@code transaction-consumer} 线程；listener 在第 1 条
 * TxChange 后闭锁阻塞——断言窗内 End 与后续变更永不可达。"事务未完成"以两级证态断言：
 * 输出前沿恒为初值 0（前沿只在 End 之后推进）+ 桶状态停在 OUTPUTTING（DONE 仅在 End 与前沿
 * 之后写入，经 {@code handedOffForTest()} 包私有探针读取）。brief 骨架里的
 * {@code List<Transaction> reassembled} 对照形态按其注释省略（"用独立收集器对照可省"——阻塞
 * listener 之下无人重组，前沿/桶状态是更直接的"未完成"证态）。
 *
 * <p>线程约束：喂流在测试线程（reader 角色）；阻塞回调在 consumer 线程；latch 的
 * countDown→await 建立 consumer→测试线程的 happens-before，断言读到的计数与桶状态是确定性的。
 */
class StreamingDeliveryTest {

    /** 每用例独立的管道目录（异步构造器建管道即 wipe，测试收尾 close 排干）。 */
    @TempDir
    Path dir;

    /**
     * 责任：流式时序证明本体——consumer 仍阻塞在第 1 条变更回调内时，断言三件套全部成立：
     * 变更已出（恰 1 条）、End 未出、事务未完成（前沿 0 + 桶 OUTPUTTING）。
     * 关键步骤：异步构造器（阻塞 listener）→ 测试线程喂 Relation/Begin/3×Insert/Commit
     * （Commit 触发交接，consumer 线程回放：Begin 头 → 第 1 条 TxChange 即 countDown 并阻塞）
     * → await firstChange(5s) 确认已进入回调 → 断言窗内三件套（阻塞未放行，End/后续变更
     * 不可达，读数确定）。
     * 边界：await 超时即用例失败（consumer 未在 5s 内交付首条变更——流式契约违约的信号）；
     * finally 先放行再 close——consumer 立即排干余下变更与 End 后见到毒丸退出，join 秒回
     * （不放行则 join 要吃满 consumer 侧超时，60s join 超时 WARN 放行路径仅作兜底）。
     * 线程约束：断言在测试线程；计数器用原子类型 + latch happens-before 保证可见性。
     */
    @Test
    void changeEventsArriveBeforeTransactionCompletes() throws Exception {
        CountDownLatch firstChange = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger changesSeen = new AtomicInteger();
        AtomicInteger endSeen = new AtomicInteger();
        AtomicLong frontier = new AtomicLong();
        // 计数单位核对（Task 2 迁移教训）：changesSeen/endSeen 均以"事件"为单位（TxChange/End
        // 各计一条），与 1.7 的"事务"计数单位不同——分支按事件形态收窄，不存在编译不可见漂移
        StreamingTransactionListener blocking = event -> {
            if (event instanceof TxChange) {
                if (changesSeen.incrementAndGet() == 1) {
                    firstChange.countDown();
                }
                awaitRelease(release);   // 阻塞在每条变更里——断言窗内 End/后续变更永不可达
            } else if (event instanceof TransactionEvent.End) {
                endSeen.incrementAndGet();
            }
        };
        TransactionAssembler assembler = new TransactionAssembler(blocking, StreamingMode.ON,
                new VersionedRelationRegistry(), pipeCfg(),
                (msg, view) -> { }, frontier, () -> { });
        try {
            assembler.onRaw(PgWire.relation(16384, "t", "id", "v"));
            assembler.onRaw(PgWire.begin(301));
            for (int i = 1; i <= 3; i++) {
                assembler.onRaw(PgWire.insert(16384, PgWire.tuple(Integer.toString(i), "v" + i)));
            }
            assembler.onRaw(PgWire.commit());   // 交接 → consumer 线程回放：第 1 条 TxChange 即阻塞
            assertTrue(firstChange.await(5, TimeUnit.SECONDS), "5s 内未收到第一条变更事件——流式交付未发生");
            // 三件套：变更已出（恰 1 条——consumer 仍卡在第 1 条回调内）
            assertEquals(1, changesSeen.get(), "变更事件应恰送达 1 条（后续 2 条被回调阻塞挡住）");
            // End 未出（End 必须晚于全部变更交付）
            assertEquals(0, endSeen.get(), "End 不应在回放进行中到达");
            // 事务未完成：前沿只在 End 后推进（End 返回 = 下游确认完整消费）
            assertEquals(0L, frontier.get(), "事务未完成——输出前沿不得推进");
            List<TxBuffer> handedOff = assembler.handedOffForTest();
            assertEquals(1, handedOff.size(), "单事务应恰有 1 个交接桶");
            assertSame(BucketState.OUTPUTTING, handedOff.get(0).state,
                    "桶应停在 OUTPUTTING——DONE 仅在 End 与前沿推进之后");
        } finally {
            release.countDown();   // 先放行再 close：consumer 即刻排干余下变更与 End，毒丸退出（join 秒回）
            assembler.close();
        }
    }

    /**
     * 责任：阻塞辅助——await release 闭锁（带超时兜底）。release 在断言完成前永不 countDown，
     * 对断言窗而言是事实上的永久阻塞（"End/后续变更永不可达"的构造手段）；超时值仅防测试
     * 失控死挂（finally 必放行，正常路径远早于超时返回）。
     * 边界：被中断时恢复中断标志并返回（测试线程不会中断 consumer，防御路径）。
     * 线程：consumer 线程（listener 回调内）。
     */
    private static void awaitRelease(CountDownLatch release) {
        try {
            release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 组装器统一管道配置（用例级 @TempDir，滚动周期与生产默认同档）。 */
    private PipeConfig pipeCfg() {
        return new PipeConfig(dir, LegacyRollCycles.MINUTELY);
    }
}
