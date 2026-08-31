package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TransactionEvent 事件族形状单测（2.0 spec §2）：Begin/End record 值语义、TxChange 的
 * IS-A 关系（sealed permits 成员可用多态接收）、gid 非 2PC 为 null 的约定与 Transaction 一致。
 */
class TransactionEventTest {

    @Test
    void beginEndAreValueRecords() {
        TransactionEvent.Begin b = new TransactionEvent.Begin(101L, TransactionKind.NORMAL,
                null, 1L, 2L, Instant.EPOCH, 3L);
        assertEquals(101L, b.xid());
        assertEquals(3L, b.expectedChanges());
        assertEquals(new TransactionEvent.End(101L, 3L), new TransactionEvent.End(101L, 3L));
    }

    /** TxChange 是事件族成员：三种变更实现都能以 TransactionEvent 多态接收（permits 编译期保证，此处运行期再钉一次）。 */
    @Test
    void txChangesAreTransactionEvents() {
        TransactionEvent e1 = new RowChange(DmlKind.INSERT,
                new PgOutputMessage.Relation(OptionalLong.empty(), 1, "public", "t", 'd', List.of()),
                Optional.empty(), Optional.empty(), OptionalLong.empty());
        TransactionEvent e2 = new TruncateChange(List.of(), java.util.Set.of(), OptionalLong.empty());
        TransactionEvent e3 = new MsgChange(true, "p", new byte[0], OptionalLong.empty());
        assertTrue(e1 instanceof TxChange && e2 instanceof TxChange && e3 instanceof TxChange);
    }
}
