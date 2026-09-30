package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link TransactionRecorder} 单测:事件流重组器(测试等价币)的正常重组、流合法性 fail-fast、
 * aborted 过滤下 emitted &lt; expected 的合法性(契约边界)。引擎
 * {@code TransactionRecorderTest}(74 行)的 1:1 翻译——对账时机统一在 End 处理
 * (emitted &gt; expected 与 emitted != 实收条数两检查都在 onEvent 的 End 分支抛出,
 * {@code transactions()} 访问器恒不抛——测试写法随之固定)。
 *
 * <p>夹具偏差:connector 的 {@link RowChange} 比引擎多一个 seq 偏差组件
 * (见 {@link TxChange#seq()}),本类 RowChange 夹具统一以 0L 占位——重组与对账逻辑
 * 不读 seq,占位值不影响断言语义。
 */
class TransactionRecorderTest {

    /** 构造事件流头(expected 可与后续 End 的 emitted 不同——aborted 过滤形态的夹具基础)。 */
    private static TransactionEvent.Begin begin(long xid, long expected) {
        return new TransactionEvent.Begin(xid, TransactionKind.NORMAL, null, 1L, 2L, Instant.EPOCH, expected);
    }

    /** 构造零列 Relation 夹具(RowChange 的组件要求非 null 即可,重组测试不渲染内容)。 */
    private static PgOutputMessage.Relation rel() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), 1, "public", "t", 'd', List.of());
    }

    /** 构造极简行变更夹具(dml 与 seq 由参数给定,元组全空——重组测试只关心事件形态与计数)。 */
    private static RowChange row(DmlKind dml, long seq) {
        return new RowChange(dml, rel(), Optional.empty(), Optional.empty(), OptionalLong.empty(), seq);
    }

    /** 正常流:Begin(3) + 2 变更 + End(2)(aborted 过滤形态,emitted&lt;expected 合法)→ 重组出 1 个 2 变更事务。 */
    @Test
    void reassemblesTransactionAndAllowsEmittedBelowExpected() {
        TransactionRecorder c = new TransactionRecorder();
        c.onEvent(begin(1L, 3L));
        c.onEvent(row(DmlKind.INSERT, 10L));
        c.onEvent(row(DmlKind.DELETE, 11L));
        c.onEvent(new TransactionEvent.End(1L, 2L));
        assertEquals(1, c.transactions().size());
        assertEquals(2, c.transactions().get(0).changes().size());
    }

    /** End 无 Begin(首个事件即尾)→ ISE fail-fast。 */
    @Test
    void rejectsEndWithoutBegin() {
        TransactionRecorder c = new TransactionRecorder();
        assertThrows(IllegalStateException.class, () -> c.onEvent(new TransactionEvent.End(1L, 0L)));
    }

    /** Begin 内嵌 Begin(上一事务未封箱)→ ISE fail-fast。 */
    @Test
    void rejectsNestedBegin() {
        TransactionRecorder c = new TransactionRecorder();
        c.onEvent(begin(1L, 0L));
        assertThrows(IllegalStateException.class, () -> c.onEvent(begin(2L, 0L)));
    }

    /** End 对账失败:emitted &gt; expected(记账异常)与 emitted != 实收条数(声称与实收不符)均 ISE。 */
    @Test
    void rejectsEmittedAboveExpectedAndCountMismatch() {
        TransactionRecorder c = new TransactionRecorder();
        c.onEvent(begin(1L, 1L));
        assertThrows(IllegalStateException.class, () -> c.onEvent(new TransactionEvent.End(1L, 2L)));
        TransactionRecorder c2 = new TransactionRecorder();      // End 的 emitted 与实收条数对账
        c2.onEvent(begin(1L, 2L));
        assertThrows(IllegalStateException.class,
                () -> c2.onEvent(new TransactionEvent.End(1L, 1L)));   // 声称 1 实收 0:End 处理时抛 ISE
    }
}
