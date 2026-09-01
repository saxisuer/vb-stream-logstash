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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 流式交付时序证明(引擎 2.0 spec §5.2 的核心验收,同步形态版):变更事件在回放<b>进行中</b>
 * 即已到达 listener、先于事务完成——直接证明"边回放边输出"。整块形态下本断言不可能成立:
 * listener 在整块回调返回前什么都收不到,"收到"与"事务完成"不可分;单回调流式契约把两者
 * 拆开,本用例即钉住这个差异。引擎 {@code StreamingDeliveryTest}(119 行)的翻译。
 *
 * <p><b>同步形态适配</b>:引擎版以异步构造器起真实 consumer 线程、listener 在第 1 条
 * TxChange 后闭锁阻塞,断言窗内 End 与后续变更永不可达;Task 5 只有同步形态(dispatch 在
 * 调用线程内联),等价证明改为<b>回调内取景</b>——第 1 条 TxChange 的回调执行时(此刻回放
 * 仍在进行)内联快照三件套(End 计数/输出前沿/桶状态),快照值即"回放进行中"的确定读数
 * (同一线程,零并发);喂流继续完成后另证终态(End 已出、前沿已推进、桶 DONE)。
 * Task 6 落异步形态后再按引擎原版补跨线程闭锁形态。
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
}
