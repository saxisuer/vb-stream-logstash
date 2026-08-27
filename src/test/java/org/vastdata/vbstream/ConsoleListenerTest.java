package org.vastdata.vbstream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.DmlKind;
import org.vastdata.vbstream.replication.RelationRegistry;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionKind;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ConsoleListener 事务块渲染格式测试：logback ListAppender 直接挂在 CDC logger 上捕获输出行。 */
class ConsoleListenerTest {

    private final Logger cdc = (Logger) org.slf4j.LoggerFactory.getLogger("org.vastdata.vbstream.cdc");
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    /** 挂载捕获器：start 后追加到 CDC logger，此后该 logger 的所有发射事件进入 list（级别过滤取决于 root 配置）。 */
    @BeforeEach
    void attach() {
        appender.start();
        cdc.addAppender(appender);
    }

    /** 摘除捕获器：避免影响其他用例/测试类的 CDC logger 状态（logger 是 logback 上下文级单例）。 */
    @AfterEach
    void detach() {
        cdc.detachAppender(appender);
    }

    /** 构造两列 (id int, payload text) 的 Relation 快照，列序与 {@link #row} 的元组对齐。 */
    private static PgOutputMessage.Relation relation() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), 16384, "public", "t_stream", 'd',
                List.of(new Column("id", 23, -1, true), new Column("payload", 25, -1, false)));
    }

    /** 构造一行文本元组 (id, payload)，值均为 Text 形态。 */
    private static TupleData row(String id, String payload) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(payload)));
    }

    /** 事务块渲染：头行（xid/kind/gid/变更数）+ 逐变更行（序号 + DML + 表 + 列值）+ 尾行，共 3 条 INFO。 */
    @Test
    void rendersTransactionBlockHeaderBodyFooter() {
        RowChange insert = new RowChange(DmlKind.INSERT, relation(),
                Optional.empty(),
                Optional.of(row("1", "aaa")),
                OptionalLong.empty());
        Transaction txn = new Transaction(505L, TransactionKind.STREAMED, null,
                0x1BD9E70L, 0x1BD9E80L, Instant.parse("2026-08-27T08:00:00Z"), List.of(insert));

        new ConsoleListener().onTransaction(txn);

        assertEquals(3, appender.list.size());
        String header = appender.list.get(0).getFormattedMessage();
        assertTrue(header.startsWith("TXN-BEGIN xid=505 kind=STREAMED gid=null"), "头行不符: " + header);
        assertTrue(header.contains("changes=1"), "头行变更数不符: " + header);
        String body = appender.list.get(1).getFormattedMessage();
        assertTrue(body.contains("[1] INSERT public.t_stream"), "变更行不符: " + body);
        assertTrue(body.contains("id=1"), "列渲染不符: " + body);
        assertEquals("TXN-END   xid=505", appender.list.get(2).getFormattedMessage());
    }

    /**
     * 级别分工（spec §5）：逐消息 onMessage 降为 DEBUG、事务块保持 INFO。
     * ListAppender 不过滤级别，捕获内容取决于 CDC logger 有效级别（logback-test.xml 为 root INFO，
     * DEBUG 事件不发射）；两种级别配置下都成立的稳定断言：onMessage 后无任何 INFO 级行，
     * onTransaction 后出现 INFO 级 TXN-BEGIN。
     */
    @Test
    void onMessageLogsAtDebugWhileTransactionLogsAtInfo() {
        ConsoleListener listener = new ConsoleListener();
        listener.onMessage(new PgOutputMessage.Begin(1L, Instant.EPOCH, 505L), new RelationRegistry());
        assertTrue(appender.list.stream().noneMatch(e -> e.getLevel() == Level.INFO),
                "逐消息渲染不应再以 INFO 输出: " + appender.list);

        RowChange insert = new RowChange(DmlKind.INSERT, relation(),
                Optional.empty(), Optional.of(row("1", "a")), OptionalLong.empty());
        Transaction txn = new Transaction(505L, TransactionKind.NORMAL, null,
                1L, 2L, Instant.parse("2026-08-27T08:00:00Z"), List.of(insert));
        listener.onTransaction(txn);
        assertTrue(appender.list.stream()
                        .anyMatch(e -> e.getLevel() == Level.INFO
                                && e.getFormattedMessage().startsWith("TXN-BEGIN")),
                "事务块应以 INFO 输出: " + appender.list);
    }
}
