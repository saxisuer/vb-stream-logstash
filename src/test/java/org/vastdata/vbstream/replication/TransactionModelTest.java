package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 数据模型契约测试：构造可访问、防御性拷贝、数组值相等。 */
class TransactionModelTest {

    private static final Instant TS = Instant.parse("2026-08-27T00:00:00Z");

    /** 构造一个两列（id int4 主键 / v text）的 Relation 元数据快照，oid 可变以便区分不同表。 */
    private static PgOutputMessage.Relation relation(int oid) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", "t", 'd',
                List.of(new Column("id", 23, -1, true), new Column("v", 25, -1, false)));
    }

    /** 构造一行 (id, v) 两列的文本元组，用于填充 RowChange 的 before/after。 */
    private static TupleData row(String id, String v) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(v)));
    }

    @Test
    void transactionDefensivelyCopiesChanges() {
        // 验证紧凑构造器的 List.copyOf：外部改动源 List 不得影响 Transaction（不可变值对象语义）
        var changes = new java.util.ArrayList<TxChange>();
        changes.add(new RowChange(DmlKind.INSERT, relation(1),
                Optional.empty(), Optional.of(row("1", "a")), OptionalLong.empty()));
        Transaction txn = new Transaction(505L, TransactionKind.NORMAL, null,
                0x100L, 0x180L, TS, changes);
        changes.clear();
        assertEquals(1, txn.changes().size());
        assertThrows(UnsupportedOperationException.class, () -> txn.changes().add(
                new MsgChange(true, "p", new byte[0], OptionalLong.empty())));
    }

    @Test
    void msgChangeHasValueEqualsForByteArray() {
        // byte[] 组件必须值相等（record 默认对数组退化为引用相等，同 PgOutputMessage.LogicalMsg 的处理）
        MsgChange a = new MsgChange(true, "p", new byte[]{1, 2}, OptionalLong.empty());
        MsgChange b = new MsgChange(true, "p", new byte[]{1, 2}, OptionalLong.empty());
        assertEquals(a, b);
        assertNotEquals(a, new MsgChange(true, "p", new byte[]{1, 3}, OptionalLong.empty()));
    }

    @Test
    void truncateChangeCollectsRelationSnapshots() {
        TruncateChange tc = new TruncateChange(
                List.of(relation(1), relation(2)), EnumSet.of(TruncateOption.CASCADE), OptionalLong.empty());
        assertEquals(2, tc.relations().size());
        assertEquals(java.util.Set.of(TruncateOption.CASCADE), tc.options());
    }
}
