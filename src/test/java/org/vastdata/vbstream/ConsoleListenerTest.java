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

    /**
     * 级别分工修订（spec §5）：事务生命周期控制消息（流式分段边界 + 两阶段信号，共 9 种）升 INFO——
     * 其中 StreamAbort/RollbackPrepared 不产生组装后事务块，若维持 DEBUG 会在 INFO 级吞掉唯一的事务级线索；
     * 行级数据与元数据（Insert/Begin/Relation 等）维持 DEBUG（提交路径已由事务块 INFO 覆盖）。
     * 断言：9 条生命周期消息各产生且仅产生一条 INFO（内容与逐消息渲染一致），
     * 随后的 Insert/Begin/Relation 不产生任何 INFO。
     */
    @Test
    void transactionLifecycleMessagesEmitInfoWhileRowDataStaysDebug() {
        ConsoleListener listener = new ConsoleListener();
        RelationRegistry registry = new RelationRegistry();

        // 流式生命周期（分段边界与终局信号）
        listener.onMessage(new PgOutputMessage.StreamStart(505L, true), registry);
        listener.onMessage(new PgOutputMessage.StreamStop(), registry);
        listener.onMessage(new PgOutputMessage.StreamCommit(505L, 0x10L, 0x18L, Instant.EPOCH), registry);
        listener.onMessage(new PgOutputMessage.StreamAbort(505L, 505L, OptionalLong.empty(), OptionalLong.empty()), registry);
        // 两阶段生命周期
        listener.onMessage(new PgOutputMessage.BeginPrepare(0x10L, 0x18L, Instant.EPOCH, 506L, "gid-a"), registry);
        listener.onMessage(new PgOutputMessage.Prepare(0x10L, 0x18L, Instant.EPOCH, 506L, "gid-a"), registry);
        listener.onMessage(new PgOutputMessage.CommitPrepared(0x20L, 0x28L, Instant.EPOCH, 506L, "gid-a"), registry);
        listener.onMessage(new PgOutputMessage.RollbackPrepared(0x10L, 0x30L, Instant.EPOCH, Instant.EPOCH, 507L, "gid-b"), registry);
        listener.onMessage(new PgOutputMessage.StreamPrepare(0x10L, 0x18L, Instant.EPOCH, 508L, "gid-c"), registry);

        List<String> infoLines = appender.list.stream()
                .filter(e -> e.getLevel() == Level.INFO)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
        assertEquals(9, infoLines.size(), "9 条生命周期消息应各产生一条 INFO: " + infoLines);
        assertTrue(infoLines.get(0).startsWith("STREAM-START"), "首条应为 STREAM-START: " + infoLines.get(0));
        assertTrue(infoLines.get(1).startsWith("STREAM-STOP"), "次条应为 STREAM-STOP: " + infoLines.get(1));
        assertTrue(infoLines.get(3).startsWith("STREAM-ABORT"), "第 4 条应为 STREAM-ABORT: " + infoLines.get(3));
        assertTrue(infoLines.get(7).startsWith("ROLLBACK-PREPARED"), "第 8 条应为 ROLLBACK-PREPARED: " + infoLines.get(7));

        // 对照组：行级数据与元数据维持 DEBUG，不得产生 INFO
        listener.onMessage(new PgOutputMessage.Insert(OptionalLong.empty(), 16384, row("1", "aaa")), registry);
        listener.onMessage(new PgOutputMessage.Begin(1L, Instant.EPOCH, 505L), registry);
        listener.onMessage(relation(), registry);
        long infoCount = appender.list.stream().filter(e -> e.getLevel() == Level.INFO).count();
        assertEquals(9, infoCount, "行级/元数据消息不应追加 INFO: " + appender.list);
    }
}
