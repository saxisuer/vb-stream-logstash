package org.vastdata.vbstream.walsource.changes;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ChangeStream} 门面的端到端失败先行测试（Task 7 接线锚）——stub catalog 快照
 * 之上，WalBytes/TupleBytes 手造"catalog INS + 用户表 DML + XACT commit"合成序列，
 * 经 {@link ChangeStream#onRecord} 单入口喂入，断言事件面 / OutputRenderer 日志面 /
 * 词典外 oid（date/numeric/uuid）经渲染矩阵的值面 / 行内 external 指针的 TOAST 接线 /
 * 白名单 / DML 观测计数。
 *
 * <p>与 {@code XactGrouperTest} 的分界：那边直测组装状态机的行为语义（子事务归并/
 * 2PC/缓存失效），这边验证<b>门面装配契约</b>——构造内建 TableFilter/ToastAssembler/
 * XactGrouper 三件套、单委派入口、观测计数面，以及 Task 5 提醒的硬前置补面（词典外
 * oid 的 kinds 增补 + external 行内检测走 resolveExternal）。</p>
 */
class ChangeStreamTest {

    /** 测试基准定位器三件组（与 XactGrouperTest 同位形）。 */
    private static final long SPC = 1663L;
    private static final long DB = 16385L;

    /** 用户表：oid == relfilenode == 24600，public.t_stream（id int4 / big text）。 */
    private static final long USER_REL = 24600L;

    /** 词典外 oid 表：oid == relfilenode == 24601，public.t_typed（id/date/numeric/uuid/tail）。 */
    private static final long TYPED_REL = 24601L;

    /** toast 关系：oid == relfilenode == 33657（relkind 't'）。 */
    private static final long TOAST_REL = 33657L;

    /** pg_attribute 的固定 oid（catalog INS 用例的字典表 heap 记录目标）。 */
    private static final long PG_ATTRIBUTE_RELNODE = 1249L;

    /** numeric 短格式磁盘内容："12.345"（header 0x8180 + digit 12/3450，Task 2 矩阵锚）。 */
    private static final byte[] NUMERIC_12_345 = {(byte) 0x80, (byte) 0x81, 0x0C, 0x00, 0x7A, 0x0D};

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    private ListAppender<ILoggingEvent> cdcAppender;
    private Logger cdcLogger;

    /**
     * 词典外 oid 用例的表列（id/date/numeric/uuid/tail——tail 在最外验证定宽新 kind
     * 的消耗面正确：前四列任一长度/对齐错位都会让 tail 错读）。
     */
    @BeforeEach
    void attachCdcCapture() {
        cdcLogger = (Logger) LoggerFactory.getLogger("org.vastdata.vbstream.walsource.cdc");
        cdcAppender = new ListAppender<>();
        cdcAppender.start();
        cdcLogger.addAppender(cdcAppender);
    }

    /**
     * 摘除 CDC 捕获器（防跨用例泄漏）。
     */
    @AfterEach
    void detachCdcCapture() {
        cdcLogger.detachAppender(cdcAppender);
    }

    /** 任务书 ①：catalog INS + DML + XACT commit 合成序列 → 事件面 Begin/Row/End 齐。 */
    @Test
    void endToEndCatalogInsDmlCommitEmitsBatch() {
        Fixture fx = fixture(null, null);
        fx.catalogAttrInsert(555);                       // catalog INS：字典表 heap 记录（无行语义）
        fx.insert(100, 1, "alice");
        fx.insert(100, 2, "bob");
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, big=alice}",
                "ROW INSERT public.t_stream before=null after={id=2, big=bob}",
                "END xid=100 emitted=2 exp=2"), fx.listener.events);
    }

    /** Task 5 硬前置①：词典外 oid（date/numeric/uuid）行解码经渲染矩阵产出 PG text 值。 */
    @Test
    void offDictionaryOidColumnsDecodeThroughRendererMatrix() {
        Fixture fx = fixture(null, null);
        int days = (int) (LocalDate.of(2026, 10, 7).toEpochDay() - 10_957L);   // 自 2000-01-01 的天数
        byte[] uuid = new byte[16];
        for (int i = 0; i < 16; i++) {
            uuid[i] = (byte) i;
        }
        byte[] payload = TupleBytes.of("int4", "date", "numeric", "uuid", "text")
                .i32(7).date(days).numericContent(NUMERIC_12_345).uuidBytes(uuid).text("tail-ok")
                .payload();
        fx.feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 100)
                .block(0, SPC, DB, TYPED_REL, 0).data(payload).main(new byte[3]).build());
        fx.commit(100);

        ChangeOutputListener.RowChange row = fx.listener.rows.get(0);
        assertEquals("7", row.after().get("id"), "int4 经矩阵渲染为十进制（renderRow 产物全 String）");
        assertEquals("2026-10-07", row.after().get("d"), "date i32 天 → ISO 日期");
        assertEquals("12.345", row.after().get("n"), "numeric 短格式 → 精度无损十进制");
        assertEquals("00010203-0405-0607-0809-0a0b0c0d0e0f", row.after().get("u"), "uuid 16B → 连字符形态");
        assertEquals("tail-ok", row.after().get("tail"), "尾列 text 不受前四列消耗面影响");
    }

    /** Task 5 硬前置②：行内 0x01/0x12 external 指针 → 已归集 chunk 拼装原值（Task 3 stub 接线）。 */
    @Test
    void externalPointerInlineResolvedFromCollectedChunks() {
        Fixture fx = fixture(null, null);
        fx.toastChunks(900, 4242L, "abcdefghij".getBytes(java.nio.charset.StandardCharsets.UTF_8), 5);
        byte[] payload = TupleBytes.of("int4", "text")
                .i32(1).externalPointer(14, 10, 4242L, TOAST_REL)
                .payload();
        fx.feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 100)
                .block(0, SPC, DB, USER_REL, 0).data(payload).main(new byte[3]).build());
        fx.commit(100);

        ChangeOutputListener.RowChange row = fx.listener.rows.get(0);
        assertEquals("1", row.after().get("id"), "指针列后的游标推进正确（id 在前不受影响）");
        assertEquals("abcdefghij", row.after().get("big"), "external 指针经归集 chunk 拼装为原值");
    }

    /** 行内压缩 varlena（tag 0x02，pglz）：压缩后 &lt;2KB 的宽文本不走 external——解压接线。 */
    @Test
    void inlineCompressedVarlenaDecodedThroughPglz() {
        Fixture fx = fixture(null, null);
        byte[] payload = TupleBytes.of("int4", "text")
                .i32(1).compressedText("hello wal source!")
                .payload();
        fx.feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 100)
                .block(0, SPC, DB, USER_REL, 0).data(payload).main(new byte[3]).build());
        fx.commit(100);

        ChangeOutputListener.RowChange row = fx.listener.rows.get(0);
        assertEquals("hello wal source!", row.after().get("big"),
                "tag 0x02 行内压缩经 tcinfo + pglz 解压为原值");
    }

    /** 任务书①的输出面变体：真 OutputRenderer 经 CDC logger 落 TXN-BEGIN/行/TXN-END 三段。 */
    @Test
    void realOutputRendererLogsTxnBlockThroughCdcLogger() {
        Fixture fx = fixture(null, new OutputRenderer());
        fx.insert(100, 1, "alice");
        fx.insert(100, 2, "bob");
        fx.commit(100);

        List<String> lines = cdcAppender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertEquals(4, lines.size(), "头行 + 两行 + 尾行");
        assertTrue(lines.get(0).startsWith("TXN-BEGIN xid=100 kind=NORMAL gid=null "), "头行形态");
        assertTrue(lines.get(0).endsWith("changes=2"), "changes=End 终值（pending 缓冲契约）");
        assertTrue(lines.get(1).contains("INSERT public.t_stream BEFORE=- AFTER=[id=1, big=alice]"), "行 1");
        assertTrue(lines.get(2).contains("INSERT public.t_stream BEFORE=- AFTER=[id=2, big=bob]"), "行 2");
        assertTrue(lines.get(3).startsWith("TXN-END   xid=100"), "尾行形态");
    }

    /** DML 观测计数：发射桶/行随提交累积（Main 周期 smoke 行的数据源）。 */
    @Test
    void emittedCountersExposeDmlFace() {
        Fixture fx = fixture(null, null);
        assertEquals(0, fx.stream.emittedBuckets());
        assertEquals(0, fx.stream.emittedRows());
        fx.insert(100, 1, "a");
        fx.insert(100, 2, "b");
        fx.commit(100);
        fx.insert(200, 3, "c");
        fx.commit(200);
        fx.insert(300, 4, "doomed");
        fx.commit(301);                                  // 无关终态：不产桶

        assertEquals(2, fx.stream.emittedBuckets(), "两笔提交事务各一桶");
        assertEquals(3, fx.stream.emittedRows(), "2 + 1 行");
    }

    /** 白名单：表不命中时零事件（TableFilter 三闸经门面透传）。 */
    @Test
    void whitelistFiltersOutUnlistedTable() {
        Fixture fx = fixture(Set.of("public.other"), null);
        fx.insert(100, 1, "alice");
        fx.commit(100);
        assertEquals(List.of(), fx.listener.events, "白名单外表零事件");
    }

    // ---- 测试基建 ----

    /**
     * 逐用例装配：stub 快照 + ChangeStream 门面（sqlConn=null 即 probe 禁用——离线形态）
     * + 可选 OutputRenderer（null 时用录音 listener）。
     */
    private Fixture fixture(Set<String> whitelist, OutputRenderer renderer) {
        StubSnapshot snapshot = new StubSnapshot();
        snapshot.table(USER_REL, "public", "t_stream", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "big", 25, false));
        snapshot.table(TYPED_REL, "public", "t_typed", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "d", 1082, false),
                new CatalogSnapshot.Column(3, "n", 1700, false),
                new CatalogSnapshot.Column(4, "u", 2950, false),
                new CatalogSnapshot.Column(5, "tail", 25, false));
        snapshot.table(TOAST_REL, "pg_toast", "pg_toast_24600", "t");
        snapshot.table(PG_ATTRIBUTE_RELNODE, "pg_catalog", "pg_attribute", "r");
        RecordingListener listener = new RecordingListener();
        ChangeOutputListener out = renderer != null ? renderer : listener;
        ChangeStream stream = new ChangeStream(snapshot, null, layout, whitelist, out);
        return new Fixture(snapshot, stream, listener);
    }

    /**
     * 单用例装配体——持快照/门面/录音 listener 与 LSN 游标。
     */
    private static final class Fixture {
        final StubSnapshot snapshot;
        final ChangeStream stream;
        final RecordingListener listener;
        private long lsn = 0x100000L;

        Fixture(StubSnapshot snapshot, ChangeStream stream, RecordingListener listener) {
            this.snapshot = snapshot;
            this.stream = stream;
            this.listener = listener;
        }

        /** 喂一条记录字节并推进 LSN（经门面单入口）。 */
        WalRecord feed(byte[] bytes) {
            WalRecord r = WalRecordParser.parse(bytes, lsn, WalLayoutV18.INSTANCE);
            stream.onRecord(r);
            lsn += ((long) r.totLen() + 7) / 8 * 8;
            return r;
        }

        /** 一条用户表 INSERT（id + big text）。 */
        WalRecord insert(int xid, int id, String big) {
            byte[] payload = TupleBytes.of("int4", "text").i32(id).text(big).payload();
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, xid)
                    .block(0, SPC, DB, USER_REL, 0).data(payload).main(new byte[3]).build());
        }

        /** 一条 catalog INS（pg_attribute 上的 heap 记录——字典表形态，无行语义）。 */
        WalRecord catalogAttrInsert(int xid) {
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, xid)
                    .block(0, SPC, DB, PG_ATTRIBUTE_RELNODE, 0).data(new byte[] {1}).main(new byte[3]).build());
        }

        /** 一条 COMMIT。 */
        WalRecord commit(int xid) {
            return feed(WalBytes.xactRecord(HeapOps.XLOG_XACT_COMMIT, xid, 2_000_000_000_000L, 0));
        }

        /** toast 关系上的 chunk multi-insert（与 XactGrouperTest 同形）。 */
        void toastChunks(int xid, long valueId, byte[] value, int chunkSize) {
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            List<byte[]> payloads = new ArrayList<>();
            for (int off = 0, seq = 0; off < value.length; off += chunkSize, seq++) {
                int len = Math.min(chunkSize, value.length - off);
                byte[] chunk = new byte[len];
                System.arraycopy(value, off, chunk, 0, len);
                payloads.add(TupleBytes.of("oid", "int4", "bytea")
                        .oid(valueId).i32(seq).bytes(chunk).payload());
            }
            m.write(0);
            m.write(0);
            put16(m, payloads.size());
            for (int i = 0; i < payloads.size(); i++) {
                put16(m, i + 1);
            }
            for (byte[] p : payloads) {
                put16(data, p.length);
                data.writeBytes(p);
            }
            feed(WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, xid)
                    .block(0, SPC, DB, TOAST_REL, 0).data(data.toByteArray())
                    .main(m.toByteArray()).build());
        }
    }

    /**
     * 事件录音 listener——扁平字符串面 + 强类型 RowChange 面（值断言）。
     */
    private static final class RecordingListener implements ChangeOutputListener {
        final List<String> events = new ArrayList<>();
        final List<RowChange> rows = new ArrayList<>();

        @Override
        public void onBegin(BatchBegin begin) {
            events.add("BEGIN xid=" + begin.xid() + " 2p=" + begin.twoPhase() + " gid=" + begin.gid()
                    + " exp=" + begin.expectedChanges());
        }

        @Override
        public void onRow(RowChange row) {
            rows.add(row);
            events.add("ROW " + row.dml() + " " + row.table().schema() + "." + row.table().table()
                    + " before=" + row.before() + " after=" + row.after());
        }

        @Override
        public void onEnd(BatchEnd end) {
            events.add("END xid=" + end.xid() + " emitted=" + end.emittedChanges()
                    + " exp=" + end.expectedChanges());
        }

        @Override
        public void onAborted(BatchAborted aborted) {
            events.add("ABORTED xid=" + aborted.xid());
        }
    }

    /**
     * 最小 catalog 快照 stub（XactGrouperTest 同形——oid 即键、relfilenode 身份映射）。
     */
    private static final class StubSnapshot implements CatalogSnapshot {
        final Map<Long, String> relkindByOid = new HashMap<>();
        final Map<Long, String> schemaByOid = new HashMap<>();
        final Map<Long, String> nameByOid = new HashMap<>();
        final Map<Long, List<Column>> columnsByOid = new HashMap<>();

        void table(long oid, String schema, String name, String relkind, Column... cols) {
            relkindByOid.put(oid, relkind);
            schemaByOid.put(oid, schema);
            nameByOid.put(oid, name);
            columnsByOid.put(oid, List.of(cols));
        }

        @Override
        public long lsn() {
            return 0;
        }

        @Override
        public List<Column> columnsOf(long relOid) {
            return columnsByOid.getOrDefault(relOid, List.of());
        }

        @Override
        public OptionalLong relfilenodeOf(long relOid) {
            return relkindByOid.containsKey(relOid) ? OptionalLong.of(relOid) : OptionalLong.empty();
        }

        @Override
        public OptionalLong toastOf(long relOid) {
            return relkindByOid.containsKey(relOid) ? OptionalLong.of(0) : OptionalLong.empty();
        }

        @Override
        public Optional<String> schemaOf(long relOid) {
            return Optional.ofNullable(schemaByOid.get(relOid));
        }

        @Override
        public Optional<String> relkindOf(long relOid) {
            return Optional.ofNullable(relkindByOid.get(relOid));
        }

        @Override
        public Optional<String> nameOf(long relOid) {
            return Optional.ofNullable(nameByOid.get(relOid));
        }

        @Override
        public OptionalLong relOidOf(long relNodeOrOid) {
            return relkindByOid.containsKey(relNodeOrOid) ? OptionalLong.of(relNodeOrOid) : OptionalLong.empty();
        }
    }

    /** 向流写 little-endian u16。 */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }
}
