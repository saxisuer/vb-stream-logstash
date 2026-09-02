package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleData;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleValue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TransactionEvent 事件族形状单测:Begin/End record 值语义、TxChange 的 IS-A 关系
 * (sealed permits 成员可用多态接收)、以及 connector 相对引擎的<b>已文档化偏差</b>——
 * 三种变更 record 各带 {@code long seq} 组件(消息的 CQ index,下游按 (oid, seq) 从桶
 * 快照解析 asOf Table 用),经 {@link TxChange#seq()} 多态可读。
 * 引擎 {@code TransactionEventTest} 的 1:1 翻译 + seq 偏差钉子。
 */
class TransactionEventTest {

    /** 最小 wire Relation 夹具(事件族只关心形状,不关心列内容)。 */
    private static PgOutputMessage.Relation relation() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), 1, "public", "t", 'd', List.of());
    }

    @Test
    void beginEndAreValueRecords() {
        TransactionEvent.Begin b = new TransactionEvent.Begin(101L, TransactionKind.NORMAL,
                null, 1L, 2L, Instant.EPOCH, 3L);
        assertEquals(101L, b.xid());
        assertEquals(3L, b.expectedChanges());
        assertEquals(new TransactionEvent.End(101L, 3L), new TransactionEvent.End(101L, 3L));
    }

    /**
     * 责任:TxChange 是事件族成员——三种变更实现都能以 TransactionEvent 多态接收
     * (permits 编译期保证,此处运行期再钉一次),且 seq 组件经接口方法多态可读
     * (偏差钉子:引擎事件族无此组件,connector 为 asOf Table 解析而加)。
     */
    @Test
    void txChangesAreTransactionEventsWithSeq() {
        TransactionEvent e1 = new RowChange(DmlKind.INSERT, relation(),
                Optional.empty(), Optional.of(new TupleData(List.of(new TupleValue.Text("1")))),
                OptionalLong.empty(), 42L);
        TransactionEvent e2 = new TruncateChange(List.of(relation()), java.util.Set.of(), OptionalLong.empty(), 43L);
        TransactionEvent e3 = new MsgChange(true, "p", new byte[0], OptionalLong.empty(), 44L);
        assertTrue(e1 instanceof TxChange && e2 instanceof TxChange && e3 instanceof TxChange);
        assertEquals(42L, ((TxChange) e1).seq());
        assertEquals(43L, ((TxChange) e2).seq());
        assertEquals(44L, ((TxChange) e3).seq());
    }

    /** 责任:MsgChange 的数组组件值相等(content 与 seq 都参与 equals——偏差组件不得被遗漏)。 */
    @Test
    void msgChangeHasValueEqualityIncludingSeq() {
        MsgChange a = new MsgChange(true, "p", new byte[]{1}, OptionalLong.empty(), 7L);
        assertEquals(a, new MsgChange(true, "p", new byte[]{1}, OptionalLong.empty(), 7L));
        assertNotEquals(a, new MsgChange(true, "p", new byte[]{1}, OptionalLong.empty(), 8L));
    }

    /** 责任:RowChange 的 null 宽容归一(before/after null 统一 empty,引擎同款)。 */
    @Test
    void rowChangeNormalizesNullTuples() {
        RowChange c = new RowChange(DmlKind.INSERT, relation(), null, null, OptionalLong.empty(), 1L);
        assertTrue(c.before().isEmpty() && c.after().isEmpty());
    }
}
