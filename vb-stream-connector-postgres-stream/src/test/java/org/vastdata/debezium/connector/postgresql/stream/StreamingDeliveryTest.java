package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流式交付时序证明(引擎 2.0 spec §5.2 的核心验收,同步形态版):变更事件在回放<b>进行中</b>
 * 即已到达 listener、先于事务完成——直接证明"边回放边输出"。整块形态下本断言不可能成立:
 * listener 在整块回调返回前什么都收不到,"收到"与"事务完成"不可分;单回调流式契约把两者
 * 拆开,本用例即钉住这个差异。引擎 {@code StreamingDeliveryTest}(119 行)的翻译。
 *
 * <p><b>同步形态适配</b>(Task 5):等价证明以<b>回调内取景</b>——第 1 条 TxChange 的回调
 * 执行时(此刻回放仍在进行)内联快照三件套(End 计数/输出前沿/桶状态),快照值即"回放
 * 进行中"的确定读数(同一线程,零并发);喂流继续完成后另证终态(End 已出、前沿已推进、
 * 桶 DONE)。<b>跨线程闭锁形态</b>(Task 6 回补,引擎原版形态):异步构造器起真实
 * {@code transaction-consumer} 线程,listener 在第 1 条 TxChange 后闭锁阻塞——断言窗内
 * End 与后续变更永不可达,latch 的 countDown→await 建立 consumer→测试线程的
 * happens-before,跨线程时序由此直接证明。
 *
 * <p>夹具约定:单事务 3 条 Insert(NORMAL 路径),组装器以 {@link StreamingMode#ON} 构造,
 * 管道目录取用例级 @TempDir;回调内读组装器状态(前沿/交接记账)在同线程内联发生,断言确定性。
 */
class StreamingDeliveryTest {

    /** PgWire LSN 占位:Commit 消息的 endLsn 恒为 2(前沿终态期望值)。 */
    private static final long COMMIT_END_LSN = 2L;
    private static final int OID = 16384;

    /** 每用例独立的管道目录(构造组装器建管道即 wipe)。 */
    @TempDir
    Path dir;

    /** 测试用 RelationResolver 假实现(与 StreamedTransactionAssemblerTest 同款):wire + 最小 Debezium Table。 */
    private static final RelationResolver RESOLVER = (seq, wire) -> new ResolvedRelation(wire, tableOf(wire));

    /** 责任:按 wire Relation 造最小 Debezium Table——TableId 取 wire 的 schema/table(同名互证),列沿 wire 列序全 text。 */
    private static Table tableOf(PgOutputMessage.Relation wire) {
        var editor = Table.editor().tableId(new TableId(null, wire.schema(), wire.table()));
        for (var col : wire.columns()) {
            editor.addColumn(io.debezium.relational.Column.editor()
                    .name(col.name()).jdbcType(Types.VARCHAR).type("text").create());
        }
        return editor.create();
    }

    /**
     * 责任:流式时序证明本体——第 1 条 TxChange 回调执行时(回放进行中)取景三件套并断言其
     * "未完成"读数,喂流完成后再断言终态。
     * 关键步骤:记录型 listener 在第 1 条 TxChange 处快照(End 计数=0、输出前沿=0、交接桶
     * 状态=OUTPUTTING)→ 继续喂完(Commit 已在快照前发出,回放自然走完)→ 断言终态
     * (End=1、前沿=endLsn、桶 DONE)。
     * 边界:快照取不到(首条变更未达)时用例失败——流式契约违约的信号;取景发生在 End 之前
     * 由回调时序保证(End 只在全部变更交付后发出)。
     * 线程约束:全程单线程(同步形态),取景与断言无并发面。
     */
    @Test
    void changeEventsArriveBeforeTransactionCompletes() {
        TransactionRecorder recorder = new TransactionRecorder();
        List<String> events = new ArrayList<>();
        List<String> eventsAtFirstChange = new ArrayList<>();
        long[] frontierAtFirstChange = { -1L };
        BucketState[] stateAtFirstChange = new BucketState[1];
        StreamedTransactionAssembler[] handle = new StreamedTransactionAssembler[1];
        try (StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(event -> {
            recorder.onEvent(event);
            if (event instanceof TxChange) {
                events.add("change");
                if (events.size() == 1) {   // 首条变更:回放进行中的取景窗
                    eventsAtFirstChange.addAll(events);
                    frontierAtFirstChange[0] = handle[0].outputFrontierForTest();
                    stateAtFirstChange[0] = handle[0].handedOffForTest().get(0).state;
                }
            } else if (event instanceof TransactionEvent.End) {
                events.add("end");
            }
        }, StreamingMode.ON, new VersionedRelationRegistry(),
                RESOLVER, dir, LegacyRollCycles.MINUTELY)) {
            handle[0] = assembler;
            assembler.onRaw(PgWire.relation(OID, "t", "id", "v"));
            assembler.onRaw(PgWire.begin(301));
            for (int i = 1; i <= 3; i++) {
                assembler.onRaw(PgWire.insert(OID, PgWire.tuple(Integer.toString(i), "v" + i)));
            }
            assembler.onRaw(PgWire.commit());   // 交接 → 同步回放:Begin 头 → 逐条 TxChange(首条处取景)→ End 尾

            // 取景窗断言(回放进行中的"未完成"三件套)
            assertEquals(List.of("change"), eventsAtFirstChange,
                    "首条变更到达时,End 与后续变更均未交付——变更先于事务完成");
            assertEquals(0L, frontierAtFirstChange[0], "回放进行中——输出前沿不得推进(End 未达)");
            assertSame(BucketState.OUTPUTTING, stateAtFirstChange[0],
                    "回放进行中——桶停在 OUTPUTTING(DONE 仅在 End 与前沿之后)");
            // 终态断言
            assertEquals(List.of("change", "change", "change", "end"), events,
                    "End 必须晚于全部变更交付");
            assertEquals(COMMIT_END_LSN, assembler.outputFrontierForTest());
            assertEquals(1, recorder.transactions().size());
            assertEquals(3, recorder.transactions().get(0).changes().size());
            assertTrue(assembler.handedOffForTest().isEmpty(), "DONE 桶已惰性清出交接记账");
        }
    }

    /**
     * 责任:流式时序证明的<b>跨线程闭锁形态</b>(Task 6 回补,引擎原版形态)——异步构造器
     * 起真实 {@code transaction-consumer} 线程后,consumer 仍阻塞在第 1 条变更回调内时,
     * 断言三件套全部成立:变更已出(恰 1 条)、End 未出、事务未完成(前沿 0 + 桶 OUTPUTTING)。
     * 关键步骤:异步构造(九参公共构造器,前沿由本用例穿入)→ 测试线程喂
     * Relation/Begin/3×Insert/Commit(Commit 触发交接,consumer 线程回放:Begin 头 →
     * 第 1 条 TxChange 即 countDown 并阻塞)→ await firstChange(5s) 确认已进入回调 →
     * 断言窗内三件套(阻塞未放行,End/后续变更不可达,latch happens-before 保证读数确定)。
     * 边界:await 超时即用例失败(consumer 未在 5s 内交付首条变更——流式契约违约的信号);
     * finally 先放行再 close——consumer 立即排干余下变更与 End 后见到毒丸退出,join 秒回
     * (不放行则 join 要吃满 60s 超时 WARN 兜底路径);泊留辅助沿用引擎形态
     * (await 10s 超时 + 中断恢复标志——与 DecoupledEquivalenceTest 的吞中断泊留形态不同,
     * 本用例无人 interrupt consumer,无需挺过中断)。
     * 线程约束:喂流在测试线程(reader 角色);阻塞回调在 consumer 线程;计数器用原子类型
     * + latch happens-before 保证可见性;{@code handedOffForTest} 在 reader(测试)线程读取
     * ——交接记账由本线程写入,零竞争。
     */
    @Test
    void changeEventsArriveBeforeTransactionCompletesOnConsumerThread() throws Exception {
        CountDownLatch firstChange = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger changesSeen = new AtomicInteger();
        AtomicInteger endSeen = new AtomicInteger();
        AtomicLong frontier = new AtomicLong();
        // 计数单位核对:changesSeen/endSeen 均以"事件"为单位(TxChange/End 各计一条)
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
        StreamedTransactionAssembler assembler = new StreamedTransactionAssembler(blocking, StreamingMode.ON,
                new VersionedRelationRegistry(), RESOLVER, dir, LegacyRollCycles.MINUTELY,
                (msg, view) -> { }, frontier, () -> { });
        try {
            assembler.onRaw(PgWire.relation(OID, "t", "id", "v"));
            assembler.onRaw(PgWire.begin(301));
            for (int i = 1; i <= 3; i++) {
                assembler.onRaw(PgWire.insert(OID, PgWire.tuple(Integer.toString(i), "v" + i)));
            }
            assembler.onRaw(PgWire.commit());   // 交接 → consumer 线程回放:第 1 条 TxChange 即阻塞
            assertTrue(firstChange.await(5, TimeUnit.SECONDS), "5s 内未收到第一条变更事件——流式交付未发生");
            // 三件套:变更已出(恰 1 条——consumer 仍卡在第 1 条回调内)
            assertEquals(1, changesSeen.get(), "变更事件应恰送达 1 条(后续 2 条被回调阻塞挡住)");
            // End 未出(End 必须晚于全部变更交付)
            assertEquals(0, endSeen.get(), "End 不应在回放进行中到达");
            // 事务未完成:前沿只在 End 后推进(End 返回 = 下游确认完整消费)
            assertEquals(0L, frontier.get(), "事务未完成——输出前沿不得推进");
            List<TxBuffer> handedOff = assembler.handedOffForTest();
            assertEquals(1, handedOff.size(), "单事务应恰有 1 个交接桶");
            assertSame(BucketState.OUTPUTTING, handedOff.get(0).state,
                    "桶应停在 OUTPUTTING——DONE 仅在 End 与前沿推进之后");
        } finally {
            release.countDown();   // 先放行再 close:consumer 即刻排干余下变更与 End,毒丸退出(join 秒回)
            assembler.close();
        }
    }

    /**
     * 责任:阻塞辅助——await release 闭锁(带超时兜底)。release 在断言完成前永不 countDown,
     * 对断言窗而言是事实上的永久阻塞("End/后续变更永不可达"的构造手段);超时值仅防测试
     * 失控死挂(finally 必放行,正常路径远早于超时返回)。
     * 边界:被中断时恢复中断标志并返回(本用例无人 interrupt consumer,防御路径)。
     * 线程:consumer 线程(listener 回调内)。
     */
    private static void awaitRelease(CountDownLatch release) {
        try {
            release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
