package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HeapViews 六个 heap 家族记录视图的失败先行测试（WalBytes 手造字节 → WalRecordParser
 * 走读 → 视图工厂断言）。
 *
 * <p>断言面来自任务书 Step 1：update 记录断言 old/new offnum 与 flags=0x60（前缀/后缀
 * 截断位）+ oldBlockNo 按 fork==0 非 b0 块选择（VM fork 块不干扰）；multi_insert 记录
 * 断言 ntuples=3 与 offsets 读取（INIT_PAGE 形态 offsets 省略、offnum=i+1）；prune 记录
 * （freeze 1 plan + 2 redirected 对 + 3 dead）断言三段列表（spike 发现 18 布局）；toplevel
 * 标记无关性。另补 insert/delete/inplace 三视图、HOT 同页回退与 opcode 错配 fail-fast。</p>
 */
class HeapViewsTest {

    /** 测试基准定位器三件组（spc=pg_default/db=postgres 的典型 oid 位形）。 */
    private static final long SPC = 1663L;
    private static final long DB = 16385L;
    private static final long REL = 24600L;

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 用例 1：UPDATE——old/new offnum、flags=0x60（PREFIX|SUFFIX 截断位）、oldBlockNo 选 fork==0 的非 b0 块。 */
    @Test
    void updateRecordParsesOffsetsTruncationFlagsAndOldBlock() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 0x11223344L);   // old_xmax u32@0
        put16(m, 5);             // old_offnum u16@4
        m.write(0x03);           // old_infobits_set u8@6
        m.write(0x60);           // flags u8@7 = PREFIX_FROM_OLD|SUFFIX_FROM_OLD
        put32(m, 0x55667788L);   // new_xmax u32@8
        put16(m, 9);             // new_offnum u16@12
        // 块序：id0=新页 heap 块、id1=旧页 heap 块（跨页）、id2=VM fork 块——oldBlockNo 必须选 id1 而非按序取 id1 一概而论
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 4242)
                .block(0, SPC, DB, REL, 3).data(new byte[] {1})
                .block(0, SPC, DB, REL, 7)
                .block(1, SPC, DB, REL, 99).data(new byte[] {2})
                .main(m.toByteArray())
                .build();
        HeapViews.HeapUpdateView v = HeapViews.HeapUpdateView.parse(
                WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(5, v.oldOffnum());
        assertEquals(9, v.newOffnum());
        assertEquals(0x60, v.flags());
        assertTrue((v.flags() & HeapOps.XLH_UPDATE_TRUNCATION) != 0, "0x60 必须命中截断位组");
        assertEquals(7, v.oldBlockNo());   // fork==1 的 VM 块 99 不得被选中
    }

    /** 用例 1b：HOT UPDATE——无第二个 fork==0 块时 oldBlockNo 回退到新页块号（同页更新）。 */
    @Test
    void hotUpdateWithoutSecondHeapBlockFallsBackToNewBlock() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 0L);
        put16(m, 5);
        m.write(0);
        m.write(0x04);           // CONTAINS_OLD_KEY
        put32(m, 0L);
        put16(m, 9);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_HOT_UPDATE, 4242)
                .block(0, SPC, DB, REL, 3).data(new byte[] {1})
                .main(m.toByteArray())
                .build();
        HeapViews.HeapUpdateView v = HeapViews.HeapUpdateView.parse(
                WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(3, v.oldBlockNo());
        assertEquals(0x04, v.flags());
    }

    /** 用例 2：multi_insert（非 INIT_PAGE）——ntuples=3 + offsets 数组自 mainOff+4 逐 u16 读取。 */
    @Test
    void multiInsertReadsNtuplesAndOffsetsFromMain() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        m.write(0);              // flags u8@0
        m.write(0);              // C padding（spike 发现 3：uint16 ntuples 落 @2）
        put16(m, 3);             // ntuples u16@2
        put16(m, 11);            // offsets[] @4 起，逐 u16
        put16(m, 12);
        put16(m, 13);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, 4242)
                .block(0, SPC, DB, REL, 0).data(new byte[8])
                .main(m.toByteArray())
                .build();
        HeapViews.MultiInsertView v = HeapViews.MultiInsertView.parse(
                WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(3, v.ntuples());
        assertEquals(11, v.offsetAt(0));
        assertEquals(12, v.offsetAt(1));
        assertEquals(13, v.offsetAt(2));
        assertTrue(v.offsetsOff() > 0, "非 INIT_PAGE 形态 offsets 区必须有效定位");
    }

    /** 用例 2b：multi_insert INIT_PAGE（info|0x80）——offsets 省略：offsetsOff=-1、offnum=i+1。 */
    @Test
    void multiInsertWithInitPageOmitsOffsetsAndNumbersSequentially() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        m.write(0);
        m.write(0);
        put16(m, 3);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP2_ID,
                        HeapOps.XLOG_HEAP2_MULTI_INSERT | HeapOps.XLOG_HEAP_INIT_PAGE, 4242)
                .block(0, SPC, DB, REL, 0).data(new byte[8])
                .main(m.toByteArray())
                .build();
        HeapViews.MultiInsertView v = HeapViews.MultiInsertView.parse(
                WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(3, v.ntuples());
        assertEquals(-1, v.offsetsOff());
        assertEquals(1, v.offsetAt(0));
        assertEquals(2, v.offsetAt(1));
        assertEquals(3, v.offsetAt(2));
    }

    /** 用例 3：prune（freeze 1 plan + 2 redirected 对 + 3 dead）——三段列表 + freeze/conflict 在工厂内消化。 */
    @Test
    void pruneWithConflictFreezeAndSegmentsWalksThreeLists() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        m.write(1);              // reason u8@0
        m.write(0x08 | 0x10 | 0x20 | 0x40);   // flags u8@1：conflict horizon + freeze + redirected + dead
        put32(m, 987654321L);    // snapshot_conflict_horizon（main 区 unaligned 跟随，视图不暴露）
        ByteArrayOutputStream d = new ByteArrayOutputStream();
        put16(d, 1);             // xlhp_freeze_plans：nplans u16
        d.write(0);              // 2B padding
        d.write(0);
        d.writeBytes(new byte[12]);   // 1 × xlhp_freeze_plan（12B，视图不消费内容）
        put16(d, 2);             // redirected：n=2 对
        put16(d, 1);
        put16(d, 21);
        put16(d, 2);
        put16(d, 22);
        put16(d, 3);             // nowdead：n=3
        put16(d, 31);
        put16(d, 32);
        put16(d, 33);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_PRUNE_VACUUM_SCAN, 0)
                .block(0, SPC, DB, REL, 5).data(d.toByteArray())
                .main(m.toByteArray())
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0, layout);
        HeapViews.PruneView v = HeapViews.PruneView.parse(r, layout);
        assertEquals(0x78, v.flags());
        // freeze 段在工厂内跳过：dataOff = 块0 data 起点 + nplans u16 + 2B pad + 1 plan × 12B
        assertEquals(r.blocks().get(0).dataOff() + 4 + 12, v.dataOff());
        assertEquals(List.of(1, 21, 2, 22), v.redirectedPairs());
        assertEquals(List.of(31, 32, 33), v.nowdead());
        assertEquals(List.of(), v.nowunused());   // 0x80 位未置，段缺位
    }

    /** 用例 3b：prune 无 freeze——段起点即块0 data 起点；nowunused 段走读越过前两段。 */
    @Test
    void pruneWithoutFreezeReadsNowunusedAfterEarlierSegments() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        m.write(0);
        m.write(0x20 | 0x40 | 0x80);   // redirected + dead + unused，无 conflict/freeze
        ByteArrayOutputStream d = new ByteArrayOutputStream();
        put16(d, 1);             // redirected：n=1 对
        put16(d, 7);
        put16(d, 17);
        put16(d, 2);             // nowdead：n=2
        put16(d, 41);
        put16(d, 42);
        put16(d, 2);             // nowunused：n=2
        put16(d, 51);
        put16(d, 52);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_PRUNE_ON_ACCESS, 0)
                .block(0, SPC, DB, REL, 5).data(d.toByteArray())
                .main(m.toByteArray())
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0, layout);
        HeapViews.PruneView v = HeapViews.PruneView.parse(r, layout);
        assertEquals(r.blocks().get(0).dataOff(), v.dataOff());
        assertEquals(List.of(7, 17), v.redirectedPairs());
        assertEquals(List.of(41, 42), v.nowdead());
        assertEquals(List.of(51, 52), v.nowunused());
    }

    /** 用例 3c：prune 无 redirected 段（flags 仅 dead|unused）——nowunused 走读越过 nowdead、不落回段首。 */
    @Test
    void pruneWithoutRedirectedStillSkipsNowdeadBeforeNowunused() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        m.write(0);
        m.write(0x40 | 0x80);   // dead + unused，无 redirected/freeze/conflict
        ByteArrayOutputStream d = new ByteArrayOutputStream();
        put16(d, 2);             // nowdead：n=2
        put16(d, 61);
        put16(d, 62);
        put16(d, 1);             // nowunused：n=1
        put16(d, 71);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_PRUNE_VACUUM_CLEANUP, 0)
                .block(0, SPC, DB, REL, 5).data(d.toByteArray())
                .main(m.toByteArray())
                .build();
        HeapViews.PruneView v = HeapViews.PruneView.parse(WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(List.of(), v.redirectedPairs());
        assertEquals(List.of(61, 62), v.nowdead());
        assertEquals(List.of(71), v.nowunused());
    }

    /** 用例 4：TOPLEVEL_XID 标记无关性——视图字段全部来自 main/块区，与 252 标记无关。 */
    @Test
    void toplevelMarkerIsIrrelevantToHeapViews() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 0L);
        put16(m, 5);
        m.write(0);
        m.write(0x0C);           // CONTAINS_OLD（TUPLE|KEY）
        put32(m, 0L);
        put16(m, 9);
        HeapViews.HeapUpdateView plain = parseUpdate(WalBytes.record(
                HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 777), m.toByteArray());
        HeapViews.HeapUpdateView withTop = parseUpdate(WalBytes.record(
                HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 777).toplevel(4242), m.toByteArray());
        assertEquals(plain, withTop);   // record 等值：四组件全等
        assertEquals(0x0C & HeapOps.XLH_UPDATE_CONTAINS_OLD, 0x0C & withTop.flags());
    }

    /** 用例 5：inplace——offnum 取自 main@0，后续 16B 头（dbId/tsId/nmsgs/msgs）不参与视图。 */
    @Test
    void inplaceParsesOffnumIgnoringTrailingHeader() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put16(m, 6);             // offnum u16@0
        m.write(0);              // pad2
        m.write(0);
        put32(m, 16385L);        // dbId u32@4
        put32(m, 1663L);         // tsId u32@8
        m.write(1);              // relcacheInitFileInval bool@12
        m.write(0);              // pad3
        m.write(0);
        m.write(0);
        put32(m, 0L);            // nmsgs int@16
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INPLACE, 4242)
                .block(0, SPC, DB, REL, 2).data(new byte[16])
                .main(m.toByteArray())
                .build();
        assertEquals(6, HeapViews.HeapInplaceView.parse(
                WalRecordParser.parse(raw, 0, layout), layout).offnum());
    }

    /** 用例 6：insert——offnum u16@0 + flags u8@2（xl_heap_insert 两字段结构）。 */
    @Test
    void insertParsesOffnumAndFlags() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put16(m, 4);             // offnum u16@0
        m.write(0x08);           // flags u8@2 = CONTAINS_NEW_TUPLE
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 4242)
                .block(0, SPC, DB, REL, 0).data(new byte[5])
                .main(m.toByteArray())
                .build();
        HeapViews.HeapInsertView v = HeapViews.HeapInsertView.parse(
                WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(4, v.offnum());
        assertEquals(0x08, v.flags());
    }

    /** 用例 7：delete——offnum u16@4 + flags u8@7（xmax 前缀结构），CONTAINS_OLD 位组可判。 */
    @Test
    void deleteParsesOffnumAndOldTupleFlags() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 4242L);         // xmax u32@0
        put16(m, 7);             // offnum u16@4
        m.write(0x01);           // infobits_set u8@6
        m.write(0x06);           // flags u8@7 = CONTAINS_OLD_TUPLE|OLD_KEY
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, 4242)
                .block(0, SPC, DB, REL, 0).data(new byte[5])
                .main(m.toByteArray())
                .build();
        HeapViews.HeapDeleteView v = HeapViews.HeapDeleteView.parse(
                WalRecordParser.parse(raw, 0, layout), layout);
        assertEquals(7, v.offnum());
        assertEquals(0x06, v.flags());
        assertTrue((v.flags() & HeapOps.XLH_DELETE_CONTAINS_OLD) != 0);
    }

    /** opcode 错配 fail-fast：DELETE 记录喂给 update 视图工厂，ISE 点明期望与实际 opcode。 */
    @Test
    void updateViewOnDeleteOpcodeMismatchFails() {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 4242L);
        put16(m, 7);
        m.write(0x01);
        m.write(0x06);
        byte[] raw = WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, 4242)
                .block(0, SPC, DB, REL, 0)
                .main(m.toByteArray())
                .build();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HeapViews.HeapUpdateView.parse(WalRecordParser.parse(raw, 0, layout), layout));
        assertTrue(e.getMessage().contains("opcode"));
    }

    /**
     * 解析一条 UPDATE 形态记录为 update 视图（toplevel 无关性用例的重复拼装收敛点）。
     *
     * @param head  已声明 rmid/info/xid 的 DSL 起点
     * @param main  main data 字节（14B xl_heap_update 结构）
     * @return 解析出的 update 视图
     */
    private HeapViews.HeapUpdateView parseUpdate(WalBytes head, byte[] main) {
        byte[] raw = head
                .block(0, SPC, DB, REL, 3).data(new byte[] {1})
                .main(main)
                .build();
        return HeapViews.HeapUpdateView.parse(WalRecordParser.parse(raw, 0, layout), layout);
    }

    /**
     * 向流写 little-endian u16（测试侧独立小端写，与 DSL/被测各写一份互证端序）。
     *
     * @param out 目标流
     * @param v   16 位值（仅低 16 位有效）
     */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }

    /**
     * 向流写 little-endian u32。
     *
     * @param out 目标流
     * @param v   32 位值
     */
    private static void put32(ByteArrayOutputStream out, long v) {
        out.write((int) (v & 0xFF));
        out.write((int) ((v >>> 8) & 0xFF));
        out.write((int) ((v >>> 16) & 0xFF));
        out.write((int) ((v >>> 24) & 0xFF));
    }
}
