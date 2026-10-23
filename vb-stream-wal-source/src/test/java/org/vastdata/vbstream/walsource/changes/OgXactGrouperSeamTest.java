package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link XactGrouper} 的 openGauss 档 XACT 接缝验收（M2 T2）——手造 OG 布局记录
 * （heap INSERT 带块引用 + 四 XACT 记录族）喂 OG layout 装配的组装器，断言
 * 状态机消费面：发射/弃桶/挂起与<b>子桶合并</b>（OG 流无 252 TOPLEVEL 标记，
 * top 先写形态下子行落独立子桶——终态记录 main 的 subxacts 数组是唯一归并源，
 * 见 XactGrouper 类 javadoc OG 节）。
 *
 * <p>v2 changes 包既有用例零改动的前提检查同源：本测试是<b>新增文件</b>，vanilla
 * 路径行为由既有 {@code XactGrouperTest} 20 用例钉死（本类的 OG 接缝在 vanilla
 * layout 下全部短路）。</p>
 *
 * <p>断言场景：①子事务提交归并——COMMIT 的 subxacts 合并子桶 + 行序按 WAL 真序
 * （lsn 稳定排序）；②子回滚映射缺位（子桶整弃 + onAborted）；③子回滚映射在场
 * （ASSIGNMENT 归并——abortedSubxids 过滤面，与 vanilla 子回滚同形）；④PREPARE
 * 挂起 + COMMIT_PREPARED 按 u64 前缀归属键发射 twoPhase + gid；⑤ABORT_PREPARED
 * 弃挂起桶；⑥⑦<b>INIT_PAGE + CONTAINS_OLD 的 UPDATE/DELETE 旧元组偏移角落</b>
 * （vastbase T7 补钉，T6 延后项——OG 13/11+8 与 VastBase 21/19 常态 + INIT_PAGE
 * 29/27 双档，{@code XactGrouper.ogInitShift} 的 +8 面）。</p>
 */
class OgXactGrouperSeamTest {

    /** 测试基准定位器三件组。 */
    private static final long SPC = 1663L;

    private static final long DB = 24581L;

    /** 用户表：oid == relfilenode == 24610，public.t_og（id int4 / name text）。 */
    private static final long USER_REL = 24610L;

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    /**
     * 场景①（top 先写形态）：COMMIT 的 subxacts 合并子桶，行序按 WAL 真序恢复——
     * top 行、子行、再 top 行交错，合并后发射序 = lsn 序而非桶到达序。
     */
    @Test
    void ogCommitMergesSubBucketAndEmitsRowsInLsnOrder() {
        Fixture fx = fixture();
        fx.insert(8001, 1, "top-first");
        fx.insert(8002, 2, "sub-row");     // 子行——无 252 标记无 ASSIGNMENT，落独立桶
        fx.insert(8001, 3, "top-last");
        fx.commit(8001, 8002);                 // COMMIT main 携带 subxacts=[8002]

        assertEquals(List.of(
                "BEGIN xid=8001 2p=false gid=null exp=3",
                "ROW INSERT public.t_og before=null after={id=1, name=top-first}",
                "ROW INSERT public.t_og before=null after={id=2, name=sub-row}",
                "ROW INSERT public.t_og before=null after={id=3, name=top-last}",
                "END xid=8001 emitted=3 exp=3"), fx.listener.events);
    }

    /**
     * 场景②（子回滚映射缺位）：ABORT_WITH_XID 的记录头 xid 即子 xid——子桶整弃
     * （onAborted 单事件），随后顶层 COMMIT 只发射 top 行（子行不复活）。
     */
    @Test
    void ogSubAbortWithoutMappingDropsSubBucketAlone() {
        Fixture fx = fixture();
        fx.insert(8011, 1, "top");
        fx.insert(8012, 2, "sub-doomed");
        fx.abortWithXid(8012);
        fx.commit(8011, 8012);                 // 已回滚子事务仍在 commit 子列表

        assertEquals(List.of(
                "ABORTED xid=8012",
                "BEGIN xid=8011 2p=false gid=null exp=1",
                "ROW INSERT public.t_og before=null after={id=1, name=top}",
                "END xid=8011 emitted=1 exp=1"), fx.listener.events);
    }

    /**
     * 场景③（子回滚映射在场）：ASSIGNMENT 先建 sub→top 映射，子行直落顶层桶；
     * 子回滚走 vanilla 的 abortedSubxids 过滤面（无 onAborted 事件、expected 含
     * 被扣减行、emitted 为实付）。
     */
    @Test
    void ogSubAbortWithAssignmentFiltersLikeVanilla() {
        Fixture fx = fixture();
        fx.insert(8021, 1, "top");
        fx.assignment(8021, 8022);
        fx.insert(8022, 2, "sub-doomed");   // 映射在场——直落 8021 桶（origin=8022）
        fx.abortWithXid(8022);
        fx.commit(8021, 8022);

        assertEquals(List.of(
                "BEGIN xid=8021 2p=false gid=null exp=2",
                "ROW INSERT public.t_og before=null after={id=1, name=top}",
                "END xid=8021 emitted=1 exp=2"), fx.listener.events);
    }

    /**
     * 场景④：PREPARE 挂起（gid 记桶、零发射）→ COMMIT_PREPARED 按 main 的 u64
     * 前缀归属键发射 twoPhase=true + gid（记录头 xid=0——OG 实测执行命令的新事务
     * 无 xid，见 xact-anchor.txt lsn=0x29272E0 锚）。
     */
    @Test
    void ogPrepareSuspendsThenCommitPreparedEmitsWithGid() {
        Fixture fx = fixture();
        fx.insert(8031, 1, "p1");
        fx.insert(8031, 2, "p2");
        fx.prepare(8031, "og_seam_g1");
        assertEquals(List.of(), fx.listener.events, "挂起期零发射");

        fx.commitPrepared(8031);
        assertEquals(List.of(
                "BEGIN xid=8031 2p=true gid=og_seam_g1 exp=2",
                "ROW INSERT public.t_og before=null after={id=1, name=p1}",
                "ROW INSERT public.t_og before=null after={id=2, name=p2}",
                "END xid=8031 emitted=2 exp=2"), fx.listener.events);
    }

    /**
     * 场景⑤：ABORT_PREPARED 弃挂起桶（onAborted 单事件，零 Begin/End）。
     */
    @Test
    void ogAbortPreparedDropsPendingBucket() {
        Fixture fx = fixture();
        fx.insert(8041, 1, "doomed");
        fx.prepare(8041, "og_seam_g2");
        fx.abortPrepared(8041);
        assertEquals(List.of("ABORTED xid=8041"), fx.listener.events);
    }

    /**
     * 场景⑥（vastbase T7 补钉，T6 延后项——INIT_PAGE 角落）：INIT_PAGE 位 + CONTAINS_OLD
     * 的 UPDATE/DELETE 记录，旧行元组偏移经 {@code XactGrouper.ogInitShift} 正确 +8
     * ——OG 布局（13/11 + 8 = 21/19）的手造字节锚。前像解码自 main 尾（错位会落进
     * pd_xid_base/LogCSN 段解出垃圾 natts——值断言即红）。
     */
    @Test
    void ogInitPageUpdateAndDeleteShiftOldTupleOffsetByEight() {
        Fixture fx = fixture();
        fx.initPageUpdate(8051, 1, "old-u", 2, "new-u");
        fx.initPageDelete(8051, 3, "old-d");
        fx.commit(8051);

        assertEquals(List.of(
                "BEGIN xid=8051 2p=false gid=null exp=2",
                "ROW UPDATE public.t_og before={id=1, name=old-u} after={id=2, name=new-u}",
                "ROW DELETE public.t_og before={id=3, name=old-d} after=null",
                "END xid=8051 emitted=2 exp=2"), fx.listener.events);
    }

    /**
     * 场景⑦（vastbase T7 补钉，T6 延后项——VastBase 形态）：同上 INIT_PAGE 角落 +
     * <b>常态对照</b>，VastBase 布局（{@code WalLayoutVVastbase}——sizeOfHeapUpdate=21/
     * Delete=19，INIT_PAGE 再 +8 = 29/27，与 OG INIT_PAGE 同型）——常态 21/19 与
     * INIT_PAGE 29/27 双档的前像偏移一次钉死（常态面 T6 真库对拍已端到端覆盖，此处
     * 离线锚 INIT_PAGE 推导形态）。
     */
    @Test
    void vastbaseInitPageUpdateAndDeleteShiftOldTupleOffsetByEight() {
        Fixture fx = fixture(WalLayoutVVastbase.INSTANCE);
        fx.vastbaseUpdate(8061, 1, "old-u", 2, "new-u");
        fx.vastbaseDelete(8061, 3, "old-d");
        fx.initPageUpdate(8061, 4, "old-iu", 5, "new-iu");
        fx.initPageDelete(8061, 6, "old-id");
        fx.commit(8061);

        assertEquals(List.of(
                "BEGIN xid=8061 2p=false gid=null exp=4",
                "ROW UPDATE public.t_og before={id=1, name=old-u} after={id=2, name=new-u}",
                "ROW DELETE public.t_og before={id=3, name=old-d} after=null",
                "ROW UPDATE public.t_og before={id=4, name=old-iu} after={id=5, name=new-iu}",
                "ROW DELETE public.t_og before={id=6, name=old-id} after=null",
                "END xid=8061 emitted=4 exp=4"), fx.listener.events);
    }

    // ---- 测试基建 ----

    /**
     * 逐用例装配（OG 布局档）：stub 快照（单表）+ OG layout 组装器 + 录音 listener。
     */
    private Fixture fixture() {
        return fixture(og);
    }

    /**
     * 逐用例装配（布局参数化——OG 与 VastBase 双档共用骨架；XACT 记录族两档同构，
     * 布局差异仅在 heap UPDATE/DELETE 的 main 前缀宽度）。
     *
     * @param layout 布局描述符（{@code WalLayoutVOpenGauss} 或 {@code WalLayoutVVastbase}）
     */
    private Fixture fixture(WalLayout layout) {
        StubSnapshot snapshot = new StubSnapshot();
        snapshot.table(USER_REL, "public", "t_og", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false));
        RecordingListener listener = new RecordingListener();
        XactGrouper grouper = new XactGrouper(snapshot, null, layout, new ToastAssembler(null), listener);
        return new Fixture(grouper, listener, layout);
    }

    /**
     * OG 布局记录喂入面——LSN 游标按 MAXALIGN 步进（行序断言的 lsn 真序源）。
     * 布局参数化（vastbase T7）：解析与组装共用同一 {@link WalLayout}，OG 与 VastBase
     * 双档仅 heap UPDATE/DELETE 的 main 前缀宽度分叉（XACT 族两档同构）。
     */
    private static final class Fixture {
        final XactGrouper grouper;
        final RecordingListener listener;
        private final WalLayout layout;
        private long lsn = 0x400000L;

        Fixture(XactGrouper grouper, RecordingListener listener, WalLayout layout) {
            this.grouper = grouper;
            this.listener = listener;
            this.layout = layout;
            this.xidTail = layout.sizeOfHeapUpdate() > 13;   // VastBase 21/19（OG 13/11）
        }

        /** heap UPDATE/DELETE main 是否带 VastBase 档的 xid u64 殿后（OG 常态无此尾）。 */
        private final boolean xidTail;

        /** 一条用户表 heap INSERT：块引用带定位器 14B + blkno + last_lsn，新元组在块 data。 */
        void insert(long xid, int rowId, String name) {
            byte[] payload = TupleBytes.of("int4", "text").i32(rowId).text(name).payload();
            byte[] main = new byte[11];   // SizeOfOldHeapInsert 3 + LogCSN 8
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            headers.write(0);             // 块 id 0
            headers.write(0x20);          // BKPBLOCK_HAS_DATA, fork=0
            put16(headers, payload.length);
            put32(headers, SPC);
            put32(headers, DB);
            put32(headers, USER_REL);
            put16(headers, 0);            // extra_flag u16
            put32(headers, 0);            // blkno
            put64(headers, 0);            // last_lsn u64
            headers.write(255);           // DATA_SHORT
            headers.write(main.length);
            byte[] hdr = headers.toByteArray();
            byte[] rec = new byte[32 + hdr.length + payload.length + main.length];
            put32(rec, 0, rec.length);
            put64(rec, 8, xid);
            rec[24] = (byte) HeapOps.XLOG_HEAP_INSERT;
            rec[25] = (byte) HeapOps.RM_HEAP_ID;
            System.arraycopy(hdr, 0, rec, 32, hdr.length);
            System.arraycopy(payload, 0, rec, 32 + hdr.length, payload.length);
            System.arraycopy(main, 0, rec, 32 + hdr.length + payload.length, main.length);
            feed(rec);
        }

        /** 一条 OG COMMIT：main = xl_xact_commit 48B + subxacts u64[]（nsub=len）+ RecentXmin 8。 */
        void commit(long top, long... subs) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, 1_000_000L);        // xact_time
            put64(m, 42L);               // csn
            put64(m, 0L);                // xinfo
            put32(m, 0);                 // nrels
            put32(m, subs.length);       // nsubxacts
            put32(m, 0);                 // nmsgs
            put32(m, 24581);             // dbId
            put32(m, 1663);              // tsId
            put32(m, 0);                 // nlibrary
            for (long sub : subs) {
                put64(m, sub);
            }
            put64(m, 0);                 // RecentXmin 尾
            feed(xact(HeapOps.XLOG_XACT_COMMIT | HeapOps.OG_XLR_REL_COMPRESS, top, m.toByteArray()));
        }

        /** 一条 ABORT_WITH_XID（logical 档回滚恒为此形态）：20B 头 + subxacts + curId 尾。 */
        void abortWithXid(long xid, long... subs) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, 1_000_000L);        // xact_time
            put32(m, 0);                 // nrels
            put32(m, subs.length);       // nsubxacts
            put32(m, 0);                 // nlibrary
            for (long sub : subs) {
                put64(m, sub);
            }
            put64(m, xid);               // curId 尾缀（== 记录头 xid）
            feed(xact(HeapOps.OG_XLOG_XACT_ABORT_WITH_XID | HeapOps.OG_XLR_REL_COMPRESS,
                    xid, m.toByteArray()));
        }

        /** 一条 ASSIGNMENT：xtop u64@0 / n i32@8 / 4B 对齐垫 / subs u64[]@16。 */
        void assignment(long top, long... subs) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, top);
            put32(m, subs.length);
            put32(m, 0);                  // u64 对齐垫（offsetof(xsub)=16）
            for (long sub : subs) {
                put64(m, sub);
            }
            feed(xact(HeapOps.XLOG_XACT_ASSIGNMENT, subs.length == 0 ? 0 : subs[0], m.toByteArray()));
        }

        /** 一条 PREPARE：272B 新头形（magic COMPRESSION）+ gid@53 + nsubxacts=0。 */
        void prepare(long xid, String gid) {
            byte[] m = new byte[272];
            put32(m, 0, 0x57F94534L);
            put32(m, 4, m.length);
            put64(m, 8, xid);
            put64(m, 24, 1_000_000L);
            put32(m, 36, 0);
            byte[] gidBytes = gid.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(gidBytes, 0, m, 53, gidBytes.length);
            feed(xact(HeapOps.XLOG_XACT_PREPARE, xid, m));
        }

        /** 一条 COMMIT_PREPARED：u64 前缀 xid@0 + commit 结构 +8（nsub=0）。 */
        void commitPrepared(long preparedXid) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, preparedXid);       // 2pc xid u64 前缀
            put64(m, 1_000_000L);        // xact_time@8
            put64(m, 43L);               // csn@16
            put64(m, 0L);                // xinfo@24
            put32(m, 0);                 // nrels@32
            put32(m, 0);                 // nsubxacts@36
            put32(m, 0);                 // nmsgs@40
            put32(m, 24581);             // dbId@44
            put32(m, 1663);              // tsId@48
            put32(m, 0);                 // nlibrary@52
            put64(m, 0);                 // RecentXmin 尾
            feed(xact(HeapOps.XLOG_XACT_COMMIT_PREPARED | HeapOps.OG_XLR_REL_COMPRESS, 0,
                    m.toByteArray()));
        }

        /** 一条 ABORT_PREPARED：u64 前缀 xid@0 + abort 结构 +8（nsub=0）。 */
        void abortPrepared(long preparedXid) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, preparedXid);
            put64(m, 1_000_000L);        // xact_time@8
            put32(m, 0);                 // nrels@16
            put32(m, 0);                 // nsubxacts@20
            put32(m, 0);                 // nlibrary@24
            feed(xact(HeapOps.XLOG_XACT_ABORT_PREPARED | HeapOps.OG_XLR_REL_COMPRESS, 0,
                    m.toByteArray()));
        }

        /** 喂一条记录字节并推进 LSN 游标。 */
        private void feed(byte[] bytes) {
            WalRecord r = WalRecordParser.parse(bytes, lsn, layout);
            grouper.onRecord(r);
            lsn += ((long) r.totLen() + 7) / 8 * 8;
        }

        /**
         * 一条 INIT_PAGE + CONTAINS_OLD 的 UPDATE（OG 系角落形态，vastbase T7 补钉）：
         * main = pd_xid_base u64 前置 + SizeOfOldHeapUpdate 5B（old@8/new@10/flags@12）
         * + LogCSN u64 +（VastBase 档再 + xid u64 殿后）+ 旧元组殿后——旧元组起点
         * main+13+8（OG）/ main+21+8（VastBase），即 ogInitShift +8 的双档锚；
         * 新元组在块 data。
         *
         * @param xid     事务 id
         * @param oldId   旧元组 id 值（前像断言面）
         * @param oldName 旧元组 name 值
         * @param newId   新元组 id 值
         * @param newName 新元组 name 值
         */
        void initPageUpdate(long xid, int oldId, String oldName, int newId, String newName) {
            byte[] after = TupleBytes.of("int4", "text").i32(newId).text(newName).payload();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, 7777L);                    // pd_xid_base u64（INIT_PAGE 前置）
            put16(m, 5);                        // old_offnum u16@8
            put16(m, 9);                        // new_offnum u16@10
            m.write(0x04 | 0x08);               // flags u8@12 = XLH_UPDATE_CONTAINS_OLD
            put64(m, 8888L);                    // LogCSN u64@13
            if (xidTail) {
                put64(m, xid);                  // VastBase 档 xid u64 殿后
            }
            byte[] before = TupleBytes.of("int4", "text").i32(oldId).text(oldName).payload();
            m.writeBytes(before);               // 旧元组 @21(OG)/@29(VastBase)
            feed(heapRecord(HeapOps.XLOG_HEAP_UPDATE | HeapOps.XLOG_HEAP_INIT_PAGE, xid, after,
                    m.toByteArray()));
        }

        /**
         * 一条 INIT_PAGE + CONTAINS_OLD 的 DELETE（OG 系角落形态）：main = pd_xid_base
         * u64 前置 + SizeOfOldHeapDelete 3B（offnum@8/flags@10）+ LogCSN u64 +（VastBase
         * 档再 + xid u64 殿后）+ 旧元组殿后——旧元组起点 main+11+8（OG）/ main+19+8
         * （VastBase）；块仅定位器（无 data）。
         *
         * @param xid  事务 id
         * @param id   旧元组 id 值（前像断言面）
         * @param name 旧元组 name 值
         */
        void initPageDelete(long xid, int id, String name) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, 7777L);                    // pd_xid_base u64（INIT_PAGE 前置）
            put16(m, 4);                        // offnum u16@8
            m.write(0x02 | 0x04);               // flags u8@10 = XLH_DELETE_CONTAINS_OLD
            put64(m, 8888L);                    // LogCSN u64@11
            if (xidTail) {
                put64(m, xid);                  // VastBase 档 xid u64 殿后
            }
            byte[] before = TupleBytes.of("int4", "text").i32(id).text(name).payload();
            m.writeBytes(before);               // 旧元组 @19(OG)/@27(VastBase)
            feed(heapRecord(HeapOps.XLOG_HEAP_DELETE | HeapOps.XLOG_HEAP_INIT_PAGE, xid, null,
                    m.toByteArray()));
        }

        /**
         * 一条 VastBase 常态 UPDATE（T6 实测锚的离线化——sizeOfHeapUpdate=21）：main =
         * SizeOfOldHeapUpdate 5B + LogCSN u64 + <b>xid u64 殿后</b>（无条件在场）+ 旧元组
         * 殿后——旧元组起点 main+21。
         *
         * @param xid     事务 id
         * @param oldId   旧元组 id 值
         * @param oldName 旧元组 name 值
         * @param newId   新元组 id 值
         * @param newName 新元组 name 值
         */
        void vastbaseUpdate(long xid, int oldId, String oldName, int newId, String newName) {
            byte[] after = TupleBytes.of("int4", "text").i32(newId).text(newName).payload();
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put16(m, 5);                        // old_offnum u16@0
            put16(m, 9);                        // new_offnum u16@2
            m.write(0x04 | 0x08);               // flags u8@4 = XLH_UPDATE_CONTAINS_OLD
            put64(m, 8888L);                    // LogCSN u64@5
            put64(m, xid);                      // xid u64@13（VastBase 殿后常态）
            byte[] before = TupleBytes.of("int4", "text").i32(oldId).text(oldName).payload();
            m.writeBytes(before);               // 旧元组 @21
            feed(heapRecord(HeapOps.XLOG_HEAP_UPDATE, xid, after, m.toByteArray()));
        }

        /**
         * 一条 VastBase 常态 DELETE（sizeOfHeapDelete=19）：main = SizeOfOldHeapDelete 3B
         * + LogCSN u64 + xid u64 殿后 + 旧元组殿后——旧元组起点 main+19。
         *
         * @param xid  事务 id
         * @param id   旧元组 id 值
         * @param name 旧元组 name 值
         */
        void vastbaseDelete(long xid, int id, String name) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put16(m, 4);                        // offnum u16@0
            m.write(0x02 | 0x04);               // flags u8@2 = XLH_DELETE_CONTAINS_OLD
            put64(m, 8888L);                    // LogCSN u64@3
            put64(m, xid);                      // xid u64@11（VastBase 殿后常态）
            byte[] before = TupleBytes.of("int4", "text").i32(id).text(name).payload();
            m.writeBytes(before);               // 旧元组 @19
            feed(heapRecord(HeapOps.XLOG_HEAP_DELETE, xid, null, m.toByteArray()));
        }

        /**
         * heap 记录拼装（OG/VastBase 共用骨架——两档记录头/块引用同构）：块 id 0 携
         * 定位器 14B + blkno + last_lsn，data 非 null 时置 HAS_DATA 前置 dataLen，尾部
         * DATA_SHORT + main（main ≤255B 由调用方场景保证）。
         *
         * @param info 记录 info 字节（opcode 可含 INIT_PAGE 位）
         * @param xid  记录头 xid
         * @param data 块 data 载荷（UPDATE 的新元组；null = DELETE 的无 data 块）
         * @param main main 区字节（xlrec + 可选旧元组殿后）
         * @return 完整记录字节
         */
        private byte[] heapRecord(int info, long xid, byte[] data, byte[] main) {
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            headers.write(0);                   // 块 id 0
            headers.write(data != null ? 0x20 : 0);   // BKPBLOCK_HAS_DATA（fork=0）
            put16(headers, data != null ? data.length : 0);
            put32(headers, SPC);
            put32(headers, DB);
            put32(headers, USER_REL);
            put16(headers, 0);                  // extra_flag u16
            put32(headers, 0);                  // blkno
            put64(headers, 0);                  // last_lsn u64
            headers.write(255);                 // DATA_SHORT
            headers.write(main.length);
            byte[] hdr = headers.toByteArray();
            int dataLen = data != null ? data.length : 0;
            byte[] rec = new byte[32 + hdr.length + dataLen + main.length];
            put32(rec, 0, rec.length);
            put64(rec, 8, xid);
            rec[24] = (byte) info;
            rec[25] = (byte) HeapOps.RM_HEAP_ID;
            System.arraycopy(hdr, 0, rec, 32, hdr.length);
            if (data != null) {
                System.arraycopy(data, 0, rec, 32 + hdr.length, data.length);
            }
            System.arraycopy(main, 0, rec, 32 + hdr.length + dataLen, main.length);
            return rec;
        }

        /** OG XACT 记录拼装（main ≤255B 用 DATA_SHORT，超长用 DATA_LONG）。 */
        private static byte[] xact(int info, long xid, byte[] main) {
            boolean shortHeader = main.length <= 255;
            int headerLen = shortHeader ? 2 : 5;
            byte[] rec = new byte[32 + headerLen + main.length];
            put32(rec, 0, rec.length);
            put64(rec, 8, xid);
            rec[24] = (byte) info;
            rec[25] = (byte) HeapOps.RM_XACT_ID;
            if (shortHeader) {
                rec[32] = (byte) 255;
                rec[33] = (byte) main.length;
            } else {
                rec[32] = (byte) 254;
                put32(rec, 33, main.length);
            }
            System.arraycopy(main, 0, rec, 32 + headerLen, main.length);
            return rec;
        }
    }

    /**
     * 事件录音 listener——扁平字符串面（与 XactGrouperTest 同形，断言事件序/内容）。
     */
    private static final class RecordingListener implements ChangeOutputListener {
        final List<String> events = new ArrayList<>();

        @Override
        public void onBegin(BatchBegin begin) {
            events.add("BEGIN xid=" + begin.xid() + " 2p=" + begin.twoPhase() + " gid=" + begin.gid()
                    + " exp=" + begin.expectedChanges());
        }

        @Override
        public void onRow(RowChange row) {
            events.add("ROW " + row.op() + " " + row.table().schema() + "." + row.table().table()
                    + " before=" + row.before() + " after=" + row.after());
        }

        /** TRUNCATE 事件录音：表集映 schema.table 的列表文本 + 选项集（与 onRow 同缝）。 */
        @Override
        public void onTruncate(TruncateEvent event) {
            events.add("TRUNCATE " + event.tables().stream()
                    .map(t -> t.schema() + "." + t.table()).toList() + " options=" + event.options());
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
     * 最小 catalog 快照 stub（XactGrouperTest 同形裁剪：单表 + 身份映射）。
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
            return relkindByOid.containsKey(relNodeOrOid)
                    ? OptionalLong.of(relNodeOrOid) : OptionalLong.empty();
        }
    }

    /** 向流写 little-endian u16。 */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }

    /** 向流写 little-endian u32。 */
    private static void put32(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 4; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /** 向流写 little-endian u64。 */
    private static void put64(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /** 就地写 little-endian u32。 */
    private static void put32(byte[] b, int offset, long v) {
        for (int i = 0; i < 4; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /** 就地写 little-endian u64。 */
    private static void put64(byte[] b, int offset, long v) {
        for (int i = 0; i < 8; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }
}
