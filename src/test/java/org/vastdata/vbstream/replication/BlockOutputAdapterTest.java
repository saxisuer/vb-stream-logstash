package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * BlockOutputAdapter 单测：同一事件流经适配器与 TransactionCollector 的整块产物全等
 * （block 模式等价验收）；流不合法时（End 无 Begin）原样 ISE 不转发（block 模式原子性——
 * 中途失败下游零输出）。夹具 Begin/TxChange 构造与 TransactionCollectorTest 同款。
 */
class BlockOutputAdapterTest {

    /** 构造 NORMAL 形态事件流头（expected=3、End emitted=2——emitted&lt;expected 合法形态）。 */
    private static TransactionEvent.Begin begin(long xid, long expected) {
        return new TransactionEvent.Begin(xid, TransactionKind.NORMAL, null, 1L, 2L, Instant.EPOCH, expected);
    }

    /** 构造零列 Relation 夹具（RowChange 组件非 null 即可，等价断言不渲染内容）。 */
    private static PgOutputMessage.Relation rel() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), 1, "public", "t", 'd', List.of());
    }

    /** 构造 INSERT 形态的 RowChange 事件（before/after 双空——值语义 record 全等断言不受影响）。 */
    private static RowChange change() {
        return new RowChange(DmlKind.INSERT, rel(),
                Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    /** 同一事件序列（Begin(3)+2×TxChange+End(2)）分别喂 adapter 与 collector，整块产物 List.equals 全等。 */
    @Test
    void adapterForwardsSameTransactionsAsCollectorReassembles() {
        List<Transaction> viaAdapter = new ArrayList<>();
        BlockOutputAdapter adapter = new BlockOutputAdapter(viaAdapter::add);
        TransactionCollector collector = new TransactionCollector();
        List<TransactionEvent> events = List.of(
                begin(1L, 3L),
                change(),
                change(),
                new TransactionEvent.End(1L, 2L));
        for (TransactionEvent event : events) {
            adapter.onEvent(event);
            collector.onEvent(event);
        }
        assertEquals(collector.transactions(), viaAdapter);   // block 重组 == 收集器重组（同事件流的两种整块表达）
    }

    /** 流不合法（End 无 Begin）原样 ISE 上抛且零转发——block 模式原子交付语义。 */
    @Test
    void illegalStreamPropagatesWithoutForwarding() {
        List<Transaction> out = new ArrayList<>();
        BlockOutputAdapter adapter = new BlockOutputAdapter(out::add);
        assertThrows(IllegalStateException.class, () -> adapter.onEvent(new TransactionEvent.End(1L, 0L)));
        assertEquals(List.of(), out);       // 零转发——block 模式原子性
    }
}
