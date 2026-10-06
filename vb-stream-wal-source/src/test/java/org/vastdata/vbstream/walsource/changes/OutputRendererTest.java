package org.vastdata.vbstream.walsource.changes;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OutputRenderer} 的失败先行测试——logback {@link ListAppender} 挂 CDC 专用
 * logger（{@code org.vastdata.vbstream.walsource.cdc}）捕获格式化消息，与任务书 §契约
 * 样板<b>逐字符</b>断言：头行形态（kind 枚举名 / gid null 字面 / 无补零 hex /
 * changes 取 End 终值）、行前缀与 tuple 形态（截断 64 / NULL / dropped ∅ 占位 /
 * 前像子集渲染）、尾行 TXN-END 三空格、abort 零输出、多事务并发 pending 与
 * 按提交序 flush、2PC kind=TWO_PHASE gid 非 null。
 *
 * <p>值面前提（Task 4/5 形态）：RowChange 的 before/after 值已是
 * {@code XactGrouper.renderRow} 经 {@link DiskValueRenderer} 渲染后的 String
 * （NULL 列与 dropped 列为 null）——本层只截断与占位，不再二次渲染。</p>
 */
class OutputRendererTest {

    /** CDC 数据通道 logger 名（与 OutputRenderer 的 CDC 常量同串——断言面隐式钉死该名）。 */
    private static final String CDC_LOGGER_NAME = "org.vastdata.vbstream.walsource.cdc";

    private static Logger cdcLogger;
    private static ListAppender<ILoggingEvent> appender;

    /** 挂捕获 appender 并把 CDC logger 钉 INFO（输出契约级别——断言面同时校验级别）。 */
    @BeforeAll
    static void attachCaptureAppender() {
        cdcLogger = (Logger) LoggerFactory.getLogger(CDC_LOGGER_NAME);
        appender = new ListAppender<>();
        appender.start();
        cdcLogger.addAppender(appender);
        cdcLogger.setLevel(Level.INFO);
    }

    /** 摘 appender（防泄漏到后续测试类的 logger 配置面）。 */
    @AfterAll
    static void detachCaptureAppender() {
        cdcLogger.detachAppender(appender);
    }

    /** 每用例清空捕获列表（logger/appender 类级共享，事件列表用例级隔离）。 */
    @BeforeEach
    void clearCapturedEvents() {
        appender.list.clear();
    }

    /** 样板事务（任务书 §契约逐字符）：NORMAL/gid null/无补零 hex/三行 DML/尾行三空格。 */
    @Test
    void sampleTransactionRendersCharacterExactContractFormat() {
        OutputRenderer renderer = new OutputRenderer();
        TableMeta t1 = t1Meta();
        renderer.onBegin(new ChangeOutputListener.BatchBegin(773L, false, null, 0x46F1234L, 0x46F1300L,
                Instant.parse("2026-10-06T05:11:22.123456Z"), 0L));
        renderer.onRow(new ChangeOutputListener.RowChange(t1, ChangeOutputListener.DmlKind.INSERT,
                null, Map.of("id", "1", "s", "hello")));
        renderer.onRow(new ChangeOutputListener.RowChange(t1, ChangeOutputListener.DmlKind.UPDATE,
                Map.of("id", "1"), Map.of("id", "1", "s", "world")));
        renderer.onRow(new ChangeOutputListener.RowChange(t1, ChangeOutputListener.DmlKind.DELETE,
                Map.of("id", "1"), null));
        renderer.onEnd(new ChangeOutputListener.BatchEnd(773L, 3L, 3L));

        assertEquals(List.of(
                "TXN-BEGIN xid=773 kind=NORMAL gid=null commitLsn=0x46f1234 commitTs=2026-10-06T05:11:22.123456Z changes=3",
                "  [1] INSERT public.t1 BEFORE=- AFTER=[id=1, s=hello]",
                "  [2] UPDATE public.t1 BEFORE=[id=1] AFTER=[id=1, s=world]",
                "  [3] DELETE public.t1 BEFORE=[id=1] AFTER=-",
                "TXN-END   xid=773"), capturedMessages());
        assertEquals(Level.INFO, appender.list.get(0).getLevel(), "输出级别契约 = INFO");
        assertEquals(CDC_LOGGER_NAME, appender.list.get(0).getLoggerName(), "输出通道契约 = CDC 专用 logger");
    }

    /** 头行 changes 取 BatchEnd 终值（Begin 即时到达的时点值 3 被无视，End 终值 5 胜出）。 */
    @Test
    void headerChangesUsesEndExpectedChangesNotBeginValue() {
        OutputRenderer renderer = new OutputRenderer();
        renderer.onBegin(new ChangeOutputListener.BatchBegin(10L, false, null, 0xAL, 0x10L,
                Instant.parse("2026-10-06T00:00:00Z"), 3L));
        renderer.onRow(new ChangeOutputListener.RowChange(t1Meta(), ChangeOutputListener.DmlKind.INSERT,
                null, Map.of("id", "1", "s", "a")));
        renderer.onEnd(new ChangeOutputListener.BatchEnd(10L, 1L, 5L));

        assertEquals(List.of(
                "TXN-BEGIN xid=10 kind=NORMAL gid=null commitLsn=0xa commitTs=2026-10-06T00:00:00Z changes=5",
                "  [1] INSERT public.t1 BEFORE=- AFTER=[id=1, s=a]",
                "TXN-END   xid=10"), capturedMessages());
    }

    /** 值截断 64（超长附原长字节数）、NULL 列字面、dropped 占位列名原样 + ∅ 值占位。 */
    @Test
    void valueOver64CharsTruncatedAndNullAndDroppedPlaceholdersRendered() {
        OutputRenderer renderer = new OutputRenderer();
        String wide = "x".repeat(70);
        TableMeta meta = new TableMeta(24600L, "public", "t1", List.of(
                new ColumnMeta(1, "id", 23, false),
                new ColumnMeta(2, "s", 25, false),
                new ColumnMeta(3, "........pg.dropped.3........", 25, true)));
        renderer.onBegin(new ChangeOutputListener.BatchBegin(20L, false, null, 0xBL, 0x20L,
                Instant.parse("2026-10-06T00:00:00Z"), 1L));
        Map<String, Object> after = new HashMap<>();
        after.put("id", null);
        after.put("s", wide);
        after.put("........pg.dropped.3........", null);
        renderer.onRow(new ChangeOutputListener.RowChange(meta, ChangeOutputListener.DmlKind.INSERT,
                null, after));
        renderer.onEnd(new ChangeOutputListener.BatchEnd(20L, 1L, 1L));

        assertEquals(List.of(
                "TXN-BEGIN xid=20 kind=NORMAL gid=null commitLsn=0xb commitTs=2026-10-06T00:00:00Z changes=1",
                "  [1] INSERT public.t1 BEFORE=- AFTER=[id=NULL, s=" + "x".repeat(64) + "...(70B)"
                        + ", ........pg.dropped.3........=∅]",
                "TXN-END   xid=20"), capturedMessages());
    }

    /** 2PC：kind=TWO_PHASE 枚举名、gid 非 null 透传；零行事务仍发头尾骨架（changes=0）。 */
    @Test
    void twoPhaseTransactionRendersKindAndGidWithZeroRowSkeleton() {
        OutputRenderer renderer = new OutputRenderer();
        renderer.onBegin(new ChangeOutputListener.BatchBegin(900L, true, "gx_773", 0x123L, 0x200L,
                Instant.parse("2026-10-06T05:11:22.123456Z"), 0L));
        renderer.onEnd(new ChangeOutputListener.BatchEnd(900L, 0L, 0L));

        assertEquals(List.of(
                "TXN-BEGIN xid=900 kind=TWO_PHASE gid=gx_773 commitLsn=0x123 commitTs=2026-10-06T05:11:22.123456Z changes=0",
                "TXN-END   xid=900"), capturedMessages());
    }

    /** abort 丢弃 pending：Begin + 行到达后 onAborted——零输出（无头无行无尾）。 */
    @Test
    void abortedTransactionDiscardsPendingWithZeroOutput() {
        OutputRenderer renderer = new OutputRenderer();
        renderer.onBegin(new ChangeOutputListener.BatchBegin(30L, false, null, 0xCL, 0x30L,
                Instant.parse("2026-10-06T00:00:00Z"), 2L));
        renderer.onRow(new ChangeOutputListener.RowChange(t1Meta(), ChangeOutputListener.DmlKind.INSERT,
                null, Map.of("id", "1", "s", "doomed")));
        renderer.onRow(new ChangeOutputListener.RowChange(t1Meta(), ChangeOutputListener.DmlKind.INSERT,
                null, Map.of("id", "2", "s", "doomed2")));
        renderer.onAborted(new ChangeOutputListener.BatchAborted(30L));

        assertTrue(capturedMessages().isEmpty(), "aborted 事务零输出");
    }

    /** 多事务并发 pending：交错 Begin/Row 归属最近 Begin，按 End 序 flush、各桶行不串。 */
    @Test
    void interleavedPendingTransactionsFlushIndependentlyInEndOrder() {
        OutputRenderer renderer = new OutputRenderer();
        TableMeta t1 = t1Meta();
        renderer.onBegin(beginOf(100L));
        renderer.onRow(new ChangeOutputListener.RowChange(t1, ChangeOutputListener.DmlKind.INSERT,
                null, Map.of("id", "1", "s", "a1")));
        renderer.onBegin(beginOf(200L));
        renderer.onRow(new ChangeOutputListener.RowChange(t1, ChangeOutputListener.DmlKind.INSERT,
                null, Map.of("id", "7", "s", "b1")));
        renderer.onEnd(new ChangeOutputListener.BatchEnd(200L, 1L, 1L));
        renderer.onEnd(new ChangeOutputListener.BatchEnd(100L, 1L, 1L));

        assertEquals(List.of(
                "TXN-BEGIN xid=200 kind=NORMAL gid=null commitLsn=0x64 commitTs=2026-10-06T00:00:00Z changes=1",
                "  [1] INSERT public.t1 BEFORE=- AFTER=[id=7, s=b1]",
                "TXN-END   xid=200",
                "TXN-BEGIN xid=100 kind=NORMAL gid=null commitLsn=0x64 commitTs=2026-10-06T00:00:00Z changes=1",
                "  [1] INSERT public.t1 BEFORE=- AFTER=[id=1, s=a1]",
                "TXN-END   xid=100"), capturedMessages());
    }

    /** onRow 先于任何 onBegin（契约违背）→ ISE fail-fast，不产出半截行。 */
    @Test
    void rowBeforeAnyBeginFailsFast() {
        OutputRenderer renderer = new OutputRenderer();
        ChangeOutputListener.RowChange row = new ChangeOutputListener.RowChange(t1Meta(),
                ChangeOutputListener.DmlKind.INSERT, null, Map.of("id", "1", "s", "orphan"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> renderer.onRow(row));
        assertTrue(capturedMessages().isEmpty());
    }

    /** 样板表元数据：public.t1（id int4 / s text）。 */
    private static TableMeta t1Meta() {
        return new TableMeta(24600L, "public", "t1", List.of(
                new ColumnMeta(1, "id", 23, false),
                new ColumnMeta(2, "s", 25, false)));
    }

    /** NORMAL 形态 Begin 事件（xid 变参，其余取可复现的定位值）。 */
    private static ChangeOutputListener.BatchBegin beginOf(long xid) {
        return new ChangeOutputListener.BatchBegin(xid, false, null, 0x64L, 0x100L,
                Instant.parse("2026-10-06T00:00:00Z"), 1L);
    }

    /** 捕获事件的格式化消息面（渲染产物的逐字符断言载体）。 */
    private static List<String> capturedMessages() {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }
}
