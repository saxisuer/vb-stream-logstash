package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link XactGrouper} 事务组装状态机的失败先行测试——WalBytes/TupleBytes 手造
 * heap 与 XACT 记录序列（stub catalog 快照），经 RecordingListener 断言事件面：
 * 提交时批量发射的事件序 / 交错两桶不串 / ABORT 零 Begin-End / 子事务回滚扣减
 * （expected=过滤前）/ 2PC 挂起-确认-弃三态 / TOPLEVEL_XID 与 ASSIGNMENT 双路归并 /
 * UPDATE-DELETE 前像 / FPW 镜像路径 / TOAST chunk 采集与终态淘汰 / TableMeta 缓存的
 * DDL 失效。
 *
 * <p>发射语义锚（控制器 2026-10 裁定修正）：桶内累积、<b>COMMIT/COMMIT_PREPARED 时
 * 按序批量发射</b>（Begin → 过滤后行 → End）——即时回调 onRow 与 aborted 过滤不可
 * 同时成立（已发射的行无法撤回）；任务书用例①的"事件序 Begin/3×Row/End"由批量
 * 回放产生同序。expectedChanges=桶记账（aborted 过滤前），emittedChanges=过滤后
 * 实付；BatchBegin 携带终值（发射点=提交时点，全量已知）。</p>
 *
 * <p>XACT 布局锚（REL_18_STABLE 实源钉）：commit/abort 最小 main = 裸 i64
 * xact_time；COMMIT/ABORT_PREPARED 的 info 置 HAS_INFO(0x80)，main 走 xinfo 块链，
 * <b>归属键 = twophase chunk xid</b>（记录头 xid 是执行命令的新事务）；PREPARE main =
 * 72B 两阶段文件头（gidlen u16@54、gid@72 按 gidlen 截取非 NUL 结尾）。</p>
 */
class XactGrouperTest {

    /** 测试基准定位器三件组（与 HeapViewsTest 同位形）。 */
    private static final long SPC = 1663L;
    private static final long DB = 16385L;

    /** 用户表：oid == relfilenode == 24600，public.t_stream（id int4 / name text）。 */
    private static final long USER_REL = 24600L;

    /** toast 关系：oid == relfilenode == 33657（relkind 't'）。 */
    private static final long TOAST_REL = 33657L;

    /** pg_attribute 的固定 oid（catalog 字典三表之一——缓存失效触发键）。 */
    private static final long PG_ATTRIBUTE_RELNODE = 1249L;

    /** 提交时间戳（自 2000-01-01 起微秒）——取整秒倍数使 Instant 断言免重复实现换算。 */
    private static final long COMMIT_MICROS = 2_000_000_000_000L;

    /** COMMIT_MICROS 对应的 Instant（946684800s = 2000-01-01T00:00:00Z + 2e6 s）。 */
    private static final Instant COMMIT_TS = Instant.ofEpochSecond(946_684_800L + 2_000_000L);

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 任务书 ①：单事务 INSERT×3 → COMMIT——事件序 Begin/3×Row/End、expected==emitted==3、commit 字段齐。 */
    @Test
    void singleTxnThreeInsertsEmitBeginRowsEndInOrder() {
        Fixture fx = fixture();
        fx.insert(100, 0, 1, "alice");
        fx.insert(100, 0, 2, "bob");
        fx.insert(100, 0, 3, "carol");
        WalRecord commit = fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=alice}",
                "ROW INSERT public.t_stream before=null after={id=2, name=bob}",
                "ROW INSERT public.t_stream before=null after={id=3, name=carol}",
                "END xid=100 emitted=3 exp=3"), fx.listener.events);

        ChangeOutputListener.BatchBegin begin = fx.listener.begins.get(0);
        assertEquals(commit.lsn(), begin.commitLsn(), "commitLsn = 提交记录 LSN");
        assertEquals(commit.lsn() + align8(commit.totLen()), begin.endLsn(), "endLsn = LSN+MAXALIGN(totLen)");
        assertEquals(COMMIT_TS, begin.commitTs());
    }

    /** 任务书 ②：交错两桶不串——按提交序各发各的批量，行归属正确。 */
    @Test
    void interleavedTransactionsEmitIndependentBatches() {
        Fixture fx = fixture();
        fx.insert(100, 0, 1, "a1");
        fx.insert(200, 0, 7, "b1");
        fx.insert(100, 0, 2, "a2");
        fx.insert(200, 0, 8, "b2");
        fx.commit(200);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=200 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=7, name=b1}",
                "ROW INSERT public.t_stream before=null after={id=8, name=b2}",
                "END xid=200 emitted=2 exp=2",
                "BEGIN xid=100 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=a1}",
                "ROW INSERT public.t_stream before=null after={id=2, name=a2}",
                "END xid=100 emitted=2 exp=2"), fx.listener.events);
    }

    /** 任务书 ③：ABORT → onAborted 单事件，零 Begin/Row/End。 */
    @Test
    void abortedTransactionEmitsAbortedWithoutBeginOrEnd() {
        Fixture fx = fixture();
        fx.insert(300, 0, 1, "doomed");
        fx.abort(300, 0);

        assertEquals(List.of("ABORTED xid=300"), fx.listener.events);
    }

    /** 任务书 ④：SAVEPOINT 子行扣减——expected=过滤前 3、emitted=2，子 xid 的行被剔除。 */
    @Test
    void savepointSubRowsDeductedExpectedStaysPreFilter() {
        Fixture fx = fixture();
        fx.insert(5, 0, 1, "top-a");
        fx.insert(7, 5, 2, "sub-b");       // 子事务行（TOPLEVEL_XID 标记归并到 5）
        fx.abort(7, 5);                    // ROLLBACK TO SAVEPOINT：子 xid ABORT + toplevel 标记
        fx.insert(5, 0, 3, "top-c");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=top-a}",
                "ROW INSERT public.t_stream before=null after={id=3, name=top-c}",
                "END xid=5 emitted=2 exp=3"), fx.listener.events);
    }

    /** 任务书 ⑤：PREPARE 挂起零发射 → COMMIT_PREPARED 发射（gid 解出、twoPhase、xid=被准备事务）。 */
    @Test
    void preparePendsThenCommitPreparedEmitsWithGid() {
        Fixture fx = fixture();
        fx.insert(700, 0, 1, "p1");
        fx.insert(700, 0, 2, "p2");
        fx.feed(WalBytes.prepareRecord(700, COMMIT_MICROS, "gt-42"));
        assertEquals(List.of(), fx.listener.events, "PREPARE 挂起期零发射");

        fx.feed(WalBytes.xactRecordWithGid(HeapOps.XLOG_XACT_COMMIT_PREPARED, 888, COMMIT_MICROS, 700, "gt-42"));

        assertEquals(List.of(
                "BEGIN xid=700 2p=true gid=gt-42 exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=p1}",
                "ROW INSERT public.t_stream before=null after={id=2, name=p2}",
                "END xid=700 emitted=2 exp=2"), fx.listener.events);
    }

    /** 任务书 ⑥：ABORT_PREPARED 弃挂起桶——onAborted 单事件，零 Begin/End。 */
    @Test
    void abortPreparedDiscardsPendingBucket() {
        Fixture fx = fixture();
        fx.insert(700, 0, 1, "p1");
        fx.feed(WalBytes.prepareRecord(700, COMMIT_MICROS, "gt-42"));
        fx.feed(WalBytes.xactRecordWithGid(HeapOps.XLOG_XACT_ABORT_PREPARED, 889, COMMIT_MICROS, 700, "gt-42"));

        assertEquals(List.of("ABORTED xid=700"), fx.listener.events);
    }

    /** 任务书 ⑦：TOPLEVEL_XID 标记归并——多个子 xid 的行并进同一顶层桶，单批量发射。 */
    @Test
    void toplevelXidMarkerMergesSubxactRows() {
        Fixture fx = fixture();
        fx.insert(7, 5, 1, "s1");
        fx.insert(8, 5, 2, "s2");
        fx.insert(5, 0, 3, "top");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=3",
                "ROW INSERT public.t_stream before=null after={id=1, name=s1}",
                "ROW INSERT public.t_stream before=null after={id=2, name=s2}",
                "ROW INSERT public.t_stream before=null after={id=3, name=top}",
                "END xid=5 emitted=3 exp=3"), fx.listener.events);
    }

    /** ASSIGNMENT 兜底归并：无 TOPLEVEL 标记的子 xid 行经 assignment 映射并进顶层桶。 */
    @Test
    void assignmentRecordMergesSubxactRowsFallback() {
        Fixture fx = fixture();
        fx.feed(WalBytes.assignmentRecord(5, 7));
        fx.insert(7, 0, 1, "via-assignment");
        fx.insert(5, 0, 2, "top");
        fx.commit(5);

        assertEquals(List.of(
                "BEGIN xid=5 2p=false gid=null exp=2",
                "ROW INSERT public.t_stream before=null after={id=1, name=via-assignment}",
                "ROW INSERT public.t_stream before=null after={id=2, name=top}",
                "END xid=5 emitted=2 exp=2"), fx.listener.events);
    }

    /** UPDATE/DELETE 前像：CONTAINS_OLD 置位时 before 来自 main data 尾；无标志 before=null。 */
    @Test
    void updateAndDeleteCarryBeforeImageFromMainTail() {
        Fixture fx = fixture();
        fx.update(100, 0, 1, "alice", 1, "aliced");
        fx.delete(100, 0, 2, "bob");
        fx.deleteNoOld(100, 0, 3);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=3",
                "ROW UPDATE public.t_stream before={id=1, name=alice} after={id=1, name=aliced}",
                "ROW DELETE public.t_stream before={id=2, name=bob} after=null",
                "ROW DELETE public.t_stream before=null after=null",
                "END xid=100 emitted=3 exp=3"), fx.listener.events);
    }

    /** FPW 镜像路径：块只有 image 无 data 时，行经 PageImages 重建页 + 行指针解出。 */
    @Test
    void insertWithFullPageImageDecodesFromRebuiltPage() {
        Fixture fx = fixture();
        byte[] tuple = TupleBytes.of("int4", "text").i32(9).text("fpi").tuple();
        byte[] page = new byte[8192];
        System.arraycopy(tuple, 0, page, 64, tuple.length);
        int itemId = 64 | (1 << 15) | (tuple.length << 17);   // lp_off | LP_NORMAL<<15 | lp_len<<17
        put32(page, 24, itemId);
        byte[] image = new byte[64 + tuple.length];
        System.arraycopy(page, 0, image, 0, image.length);
        byte[] main = new byte[3];                              // xl_heap_insert 最小 main（offnum=1, flags=0）
        main[0] = 1;
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 100)
                .block(0, SPC, DB, USER_REL, 0).image(image, 0, 0).main(main).build();
        fx.feed(raw);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=1",
                "ROW INSERT public.t_stream before=null after={id=9, name=fpi}",
                "END xid=100 emitted=1 exp=1"), fx.listener.events);
    }

    /** TOAST chunk 采集 + 终态淘汰：multi-insert 喂 chunk → resolveExternal 可拼装；任意终态后归集面全清。 */
    @Test
    void toastChunksCollectedThroughGrouperAndClearedAtTerminal() {
        Fixture fx = fixture();
        fx.toastChunks(900, 4242L, "abcdefghij".getBytes(StandardCharsets.UTF_8), 5);

        String before = fx.toast.resolveExternal(pointer(14, 10, 4242L, TOAST_REL), 0);
        assertEquals("abcdefghij", before, "终态前 chunk 已归集可拼装");

        fx.commit(901);                                         // 无桶事务的终态同样触发淘汰
        assertEquals("toast-unavailable",
                fx.toast.resolveExternal(pointer(14, 10, 4242L, TOAST_REL), 0),
                "终态后归集面已清、无 probe 回查即降级");
    }

    /** TableMeta 缓存失效：pg_attribute 上的 heap 记录到达即全清缓存，DDL 后新列即时可见。 */
    @Test
    void catalogHeapRecordInvalidatesTableMetaCache() {
        Fixture fx = fixture();
        fx.insert(100, 0, 1, "two-cols");
        fx.commit(100);
        assertEquals(1, fx.listener.begins.size());

        fx.snapshot.columnsByOid.put(USER_REL, List.of(
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false),
                new CatalogSnapshot.Column(3, "note", 25, false)));   // 模拟 DDL 重放已施加（ADD COLUMN）
        byte[] main = new byte[3];
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 555)
                .block(0, SPC, DB, PG_ATTRIBUTE_RELNODE, 0).data(new byte[] {1}).main(main).build();
        fx.feed(raw);                                           // 字典表 heap 记录 → 缓存全清

        fx.insert(200, 0, 2, "three-cols");                     // 旧元组两列，note 补 null（ADD COLUMN 前形态）
        fx.commit(200);

        assertEquals("ROW INSERT public.t_stream before=null after={id=2, name=three-cols, note=null}",
                fx.listener.events.stream().filter(e -> e.startsWith("ROW") && e.contains("three-cols"))
                        .findFirst().orElseThrow());
    }

    // ---- 测试基建 ----

    /**
     * 逐用例装配：stub 快照 + 真 ToastAssembler（probe=null）+ 录音 listener + LSN
     * 推进的喂入面。
     */
    private Fixture fixture() {
        StubSnapshot snapshot = new StubSnapshot();
        snapshot.table(USER_REL, "public", "t_stream", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false));
        snapshot.table(TOAST_REL, "pg_toast", "pg_toast_24600", "t");
        snapshot.table(PG_ATTRIBUTE_RELNODE, "pg_catalog", "pg_attribute", "r");
        RecordingListener listener = new RecordingListener();
        ToastAssembler toast = new ToastAssembler(null);
        XactGrouper grouper = new XactGrouper(snapshot, null, layout, toast, listener);
        return new Fixture(snapshot, toast, grouper, listener);
    }

    /**
     * 单用例装配体——持四组件与 LSN 游标（记录按 MAXALIGN 步进，commitLsn/endLsn
     * 断言与真实 WAL 序一致）。
     */
    private static final class Fixture {
        final StubSnapshot snapshot;
        final ToastAssembler toast;
        final XactGrouper grouper;
        final RecordingListener listener;
        private long lsn = 0x100000L;

        Fixture(StubSnapshot snapshot, ToastAssembler toast, XactGrouper grouper, RecordingListener listener) {
            this.snapshot = snapshot;
            this.toast = toast;
            this.grouper = grouper;
            this.listener = listener;
        }

        /** 喂一条记录字节并推进 LSN。 */
        WalRecord feed(byte[] bytes) {
            WalRecord r = WalRecordParser.parse(bytes, lsn, layout());
            grouper.onRecord(r);
            lsn += align8(r.totLen());
            return r;
        }

        /** 一条用户表 INSERT（toplevel=0 即无标记）。 */
        WalRecord insert(int xid, int toplevel, int id, String name) {
            byte[] payload = TupleBytes.of("int4", "text").i32(id).text(name).payload();
            byte[] main = new byte[3];
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0).data(payload).main(main).build());
        }

        /** 一条 UPDATE（CONTAINS_OLD 前像在 main 尾、新元组在块 data）。 */
        WalRecord update(int xid, int toplevel, int oldId, String oldName, int newId, String newName) {
            byte[] before = TupleBytes.of("int4", "text").i32(oldId).text(oldName).payload();
            byte[] after = TupleBytes.of("int4", "text").i32(newId).text(newName).payload();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0x11223344L);                              // old_xmax u32@0
            put16(m, 5);                                        // old_offnum u16@4
            m.write(0);                                         // old_infobits u8@6
            m.write(0x04 | 0x08);                               // flags u8@7 = XLH_UPDATE_CONTAINS_OLD
            put32(m, 0x55667788L);                              // new_xmax u32@8
            put16(m, 9);                                        // new_offnum u16@12
            m.writeBytes(before);                               // 前像载荷殿后（14B 结构之后）
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0).data(after)
                    .main(m.toByteArray()).build());
        }

        /** 一条 DELETE（CONTAINS_OLD 前像在 main 尾）。 */
        WalRecord delete(int xid, int toplevel, int id, String name) {
            byte[] before = TupleBytes.of("int4", "text").i32(id).text(name).payload();
            return deleteRaw(xid, toplevel, before, 0x02 | 0x04);
        }

        /** 一条无前像 DELETE（默认 replica identity 形态——flags 无 CONTAINS_OLD）。 */
        WalRecord deleteNoOld(int xid, int toplevel, int id) {
            return deleteRaw(xid, toplevel, null, 0);
        }

        /** DELETE 记录装配（8B 结构 + 可选前像）。 */
        private WalRecord deleteRaw(int xid, int toplevel, byte[] before, int flags) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put32(m, 0xAABBCCDDL);                              // xmax u32@0
            put16(m, 4);                                        // offnum u16@4
            m.write(0);                                         // infobits u8@6
            m.write(flags);                                     // flags u8@7
            if (before != null) {
                m.writeBytes(before);                           // 前像载荷殿后（8B 结构之后）
            }
            return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, xid)
                    .toplevel(toplevel).block(0, SPC, DB, USER_REL, 0)
                    .main(m.toByteArray()).build());
        }

        /** 一条 COMMIT（toplevel=0 即无标记）。 */
        WalRecord commit(int xid) {
            return feed(WalBytes.xactRecord(HeapOps.XLOG_XACT_COMMIT, xid, COMMIT_MICROS, 0));
        }

        /** 一条 ABORT（toplevel 非 0 即子事务回滚形态）。 */
        WalRecord abort(int xid, int toplevel) {
            return feed(WalBytes.xactRecord(HeapOps.XLOG_XACT_ABORT, xid, COMMIT_MICROS, toplevel));
        }

        /** toast 关系上的 chunk multi-insert（entry = [datalen u16][三列 payload]）。 */
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
            m.write(0);                                         // flags u8@0
            m.write(0);                                         // C padding@1
            put16(m, payloads.size());                          // ntuples u16@2
            for (int i = 0; i < payloads.size(); i++) {
                put16(m, i + 1);                                // offsets u16[]@4
            }
            for (byte[] p : payloads) {
                put16(data, p.length);                          // entry 的 datalen 前缀
                data.writeBytes(p);
            }
            feed(WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, xid)
                    .block(0, SPC, DB, TOAST_REL, 0).data(data.toByteArray())
                    .main(m.toByteArray()).build());
        }

        private WalLayout layout() {
            return WalLayoutV18.INSTANCE;
        }
    }

    /**
     * 事件录音 listener——扁平字符串面（断言事件序/内容）+ BatchBegin 强类型面
     * （commitLsn/endLsn/commitTs 断言）。
     */
    private static final class RecordingListener implements ChangeOutputListener {
        final List<String> events = new ArrayList<>();
        final List<BatchBegin> begins = new ArrayList<>();

        @Override
        public void onBegin(BatchBegin begin) {
            begins.add(begin);
            events.add("BEGIN xid=" + begin.xid() + " 2p=" + begin.twoPhase() + " gid=" + begin.gid()
                    + " exp=" + begin.expectedChanges());
        }

        @Override
        public void onRow(RowChange row) {
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
     * 最小 catalog 快照 stub——四查询 + 归一入口按三张 map 应答（oid 即键，relfilenode
     * 缺省身份映射，可经 {@code relnodeToOid} 显式分叉）；列字典可变（DDL 失效用例
     * 中途改写）。
     */
    private static final class StubSnapshot implements CatalogSnapshot {
        final Map<Long, String> relkindByOid = new HashMap<>();
        final Map<Long, String> schemaByOid = new HashMap<>();
        final Map<Long, String> nameByOid = new HashMap<>();
        final Map<Long, List<Column>> columnsByOid = new HashMap<>();

        /** 登记一张表（relfilenode 取身份映射）。 */
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
            if (relkindByOid.containsKey(relNodeOrOid)) {
                return OptionalLong.of(relNodeOrOid);           // oid 直认
            }
            return OptionalLong.empty();                        // 无 relfilenode 分叉登记
        }
    }

    /** MAXALIGN 到 8。 */
    private static long align8(int len) {
        return ((long) len + 7) / 8 * 8;
    }

    /** 18B external 指针（ToastAssemblerTest 同布局：01 12 | rawsize | extinfo | valueid | toastrelid）。 */
    private static byte[] pointer(long rawsize, long extsize, long valueid, long toastrelid) {
        byte[] p = new byte[18];
        p[0] = 0x01;
        p[1] = 0x12;
        put32(p, 2, rawsize);
        put32(p, 6, extsize);
        put32(p, 10, valueid);
        put32(p, 14, toastrelid);
        return p;
    }

    /** 就地写 little-endian u32。 */
    private static void put32(byte[] target, int offset, long v) {
        target[offset] = (byte) (v & 0xFF);
        target[offset + 1] = (byte) ((v >>> 8) & 0xFF);
        target[offset + 2] = (byte) ((v >>> 16) & 0xFF);
        target[offset + 3] = (byte) ((v >>> 24) & 0xFF);
    }

    /** 向流写 little-endian u16。 */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }

    /** 向流写 little-endian u32。 */
    private static void put32(ByteArrayOutputStream out, long v) {
        out.write((int) (v & 0xFF));
        out.write((int) (v >>> 8) & 0xFF);
        out.write((int) (v >>> 16) & 0xFF);
        out.write((int) (v >>> 24) & 0xFF);
    }
}
