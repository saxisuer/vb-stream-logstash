package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HeapViews.MultiInsertView} openGauss 分支的失败先行测试（M2 Task 3——M1 的
 * ISE 位反转）——og-plan 真实语料锚（mi-a/mi-b 窗口，锚 LSN/字段值/SHA-256 见
 * {@code src/test/resources/ogcorpus/mi-anchor.txt}）+ 手造 OG 记录双源。
 *
 * <p>断言面三层：</p>
 * <ol>
 *   <li>语料锚：三形态全覆盖——INIT_PAGE 短行多 entry（新表整页重初始化、offsets
 *       省略、offnum 隐含 i+1）/ 非 INIT_PAGE 带 offsets 数组（跨批续写 offnum 几何：
 *       宽行批首记录自前批末页 offnum 61 续起）/ 宽行少 entry 与批末单 entry——
 *       逐记录断言 ntuples/offsetAt/块 0 data 逐 entry 走读（2 对齐 + 7+datalen
 *       步进，经生产 {@link TupleDecoder} 解码 id/v 值面）；</li>
 *   <li>手造 OG 记录：INIT_PAGE 的 pd_xid_base u64 前缀平移、offsets 数组读取、
 *       LogCSN 尾缀容忍（不要求、不消费）；</li>
 *   <li>拒面：isCompressed≠0（页字典压缩 M2 范围外）、offsets 数组越出 main、
 *       main 过短均 ISE fail-fast。</li>
 * </ol>
 */
class OgMultiInsertViewTest {

    /** 窗口 A 基准 LSN（页对齐）：[0x2934000,0x293E000)——短行批全量 + 宽行批头两条。 */
    private static final long WINDOW_A_BASE = 0x2934000L;

    /** 窗口 A 终点 LSN（页对齐，排他）。 */
    private static final long WINDOW_A_END = 0x293E000L;

    /** 窗口 B 基准 LSN（页对齐）：[0x294A000,0x294D000)——宽行批末 + 尾批。 */
    private static final long WINDOW_B_BASE = 0x294A000L;

    /** 窗口 B 终点 LSN（页对齐，排他）。 */
    private static final long WINDOW_B_END = 0x294D000L;

    // ---- 语料锚 LSN（mi-anchor.txt，2026-10-12 og-plan 实测） ----

    private static final long LSN_INIT_FIRST = 0x2935DC8L;
    private static final long LSN_INIT_S2 = 0x2936BE8L;
    private static final long LSN_INIT_TAIL_SHORT = 0x2939858L;
    private static final long LSN_NONINIT_WIDE = 0x2939E00L;
    private static final long LSN_INIT_WIDE1 = 0x293B3B8L;
    private static final long LSN_INIT_WIDE_LAST = 0x294B6F0L;
    private static final long LSN_NONINIT_TAIL = 0x294BF00L;

    /** 语料事务 xid（mi-anchor.txt：短行批 13895 / 宽行批 13896 / 尾批 13897）。 */
    private static final long XID_SHORT_COPY = 13895L;
    private static final long XID_WIDE_COPY = 13896L;
    private static final long XID_TAIL_COPY = 13897L;

    /** 语料表 t_mi 的 relfilenode（块 0 定位器锚——非本表记录混入即红）。 */
    private static final long T_MI_RELNODE = 32793L;

    /** 两窗口的 MULTI_INSERT 记录普查（任何布局回归改变计数即红）。 */
    private static final int WINDOW_A_MI_COUNT = 7;
    private static final int WINDOW_B_MI_COUNT = 2;

    /** 短行首 entry 的 datalen（'miA1'：t_hoff=24 无位图，datalen = 24+4(id)+5(1B varlena)-23）。 */
    private static final int SHORT_ROW_FIRST_DATALEN = 10;

    /** 页头 info 位（xlog_basic.h——迷你走读与 OgXactViewTest 同源）。 */
    private static final int XLP_FIRST_IS_CONTRECORD = 0x0001;

    private static final int XLP_LONG_HEADER = 0x0002;

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    private final TupleDecoder decoder = new TupleDecoder(og);

    /** t_mi(id int, v text) 的解码词典——entry 值面断言用。 */
    private static final String[] T_MI_KINDS = {"int4", "text"};

    /**
     * 窗口 A 语料锚：INIT_PAGE 三形态（首记录/中间页/批末）+ 非 INIT_PAGE 宽行续写
     * （offsets 数组在场、跨批 offnum 几何）逐记录核对。
     *
     * <p>关键步骤：迷你段走读过滤 heap2 MULTI_INSERT → 普查计数 → 逐锚解析断言
     * ntuples/offsetAt/entry 走读（生产解码器值面）。INIT_PAGE 形态 offsets 省略
     * （offsetsOff=-1、offnum 隐含 i+1）；非 INIT 形态 offsets 逐项读出。</p>
     */
    @Test
    void corpusWindowAAnchorsInitPageAndNonInitVariants() throws IOException {
        Map<Long, WalRecord> byLsn = multiInsertRecordsOf(loadCorpus("mi-a.bin"), WINDOW_A_BASE, WINDOW_A_END);
        assertEquals(WINDOW_A_MI_COUNT, byLsn.size(),
                "窗口 A MULTI_INSERT 普查面（mi-anchor.txt 7 条）");

        // 短行首记录：新表页 0 整页重初始化（INIT_PAGE）——offsets 省略、185 entry 连续
        WalRecord first = anchored(byLsn, LSN_INIT_FIRST);
        assertEquals(XID_SHORT_COPY, first.xid() & 0xFFFFFFFFL);
        HeapViews.MultiInsertView vFirst = HeapViews.MultiInsertView.parse(first, og);
        assertEquals(185, vFirst.ntuples());
        assertEquals(-1, vFirst.offsetsOff(), "INIT_PAGE 形态 offsets 须省略");
        assertEquals(1, vFirst.offsetAt(0), "INIT_PAGE offnum 隐含 i+1（首行 1 号）");
        assertEquals(185, vFirst.offsetAt(184), "INIT_PAGE offnum 隐含 i+1（末行 185 号）");
        assertEquals(0, first.blocks().get(0).blockNo());
        assertEquals(T_MI_RELNODE, first.blocks().get(0).relNode());
        List<Object[]> entries = walkEntries(first);
        assertEquals(185, entries.size(), "块 0 data 逐 entry 走读计数（2 对齐 7+datalen 步进）");
        assertArrayEquals(new Object[] {1, "miA1"}, entries.get(0), "首 entry 值面");
        assertArrayEquals(new Object[] {185, "miA185"}, entries.get(184), "末 entry 值面");
        assertEquals(SHORT_ROW_FIRST_DATALEN, firstEntryDatalen(first),
                "datalen 只计 tail（t_len-23）：'miA1' = id 4 + 1B varlena 5 + 头 24 - 23");

        // 短行中间页记录：同 INIT_PAGE 形态、id 段续接（跨页批次逐页一记录）
        HeapViews.MultiInsertView vS2 = HeapViews.MultiInsertView.parse(anchored(byLsn, LSN_INIT_S2), og);
        assertEquals(185, vS2.ntuples());
        List<Object[]> entriesS2 = walkEntries(anchored(byLsn, LSN_INIT_S2));
        assertArrayEquals(new Object[] {186, "miA186"}, entriesS2.get(0), "跨页续页首 entry");

        // 短行批末记录：60 entry 收尾（741..800）
        HeapViews.MultiInsertView vTail = HeapViews.MultiInsertView.parse(anchored(byLsn, LSN_INIT_TAIL_SHORT), og);
        assertEquals(60, vTail.ntuples());
        List<Object[]> entriesTail = walkEntries(anchored(byLsn, LSN_INIT_TAIL_SHORT));
        assertEquals(60, entriesTail.size());
        assertArrayEquals(new Object[] {800, "miA800"}, entriesTail.get(59), "短行批末 entry");

        // 宽行批首记录：非 INIT_PAGE——前批末页（60 行）后续写，offsets=[61,62,63] 在场
        WalRecord wide = anchored(byLsn, LSN_NONINIT_WIDE);
        assertEquals(XID_WIDE_COPY, wide.xid() & 0xFFFFFFFFL);
        HeapViews.MultiInsertView vWide = HeapViews.MultiInsertView.parse(wide, og);
        assertEquals(3, vWide.ntuples());
        assertEquals(61, vWide.offsetAt(0), "跨批续写 offnum 几何：页 4 已有 60 短行 → 61 续起");
        assertEquals(62, vWide.offsetAt(1));
        assertEquals(63, vWide.offsetAt(2));
        assertEquals(4, wide.blocks().get(0).blockNo(), "续写仍在页 4");
        List<Object[]> entriesWide = walkEntries(wide);
        assertEquals(3, entriesWide.size());
        assertEquals(1001, entriesWide.get(0)[0]);
        assertEquals("W".repeat(1800), entriesWide.get(0)[1], "宽行 v 值面（1800B 行内 varlena）");
        assertEquals(1003, entriesWide.get(2)[0]);

        // 宽行批首个 INIT_PAGE 记录：新页整页重初始化、4 个宽 entry
        WalRecord wide1 = anchored(byLsn, LSN_INIT_WIDE1);
        HeapViews.MultiInsertView vWide1 = HeapViews.MultiInsertView.parse(wide1, og);
        assertEquals(4, vWide1.ntuples());
        assertEquals(-1, vWide1.offsetsOff());
        assertEquals(5, wide1.blocks().get(0).blockNo(), "宽行批自页 5 起新页");
        List<Object[]> entriesWide1 = walkEntries(wide1);
        assertArrayEquals(new Object[] {1004, "W".repeat(1800)}, entriesWide1.get(0));
        assertArrayEquals(new Object[] {1007, "W".repeat(1800)}, entriesWide1.get(3));
    }

    /**
     * 窗口 B 语料锚：宽行批末单 entry 记录（LAST_IN_MULTI 形态）+ 尾批非 INIT_PAGE
     * offsets 数组第二锚（offnum 2..11 续写几何）。
     */
    @Test
    void corpusWindowBAnchorsWideLastAndTailContinuation() throws IOException {
        Map<Long, WalRecord> byLsn = multiInsertRecordsOf(loadCorpus("mi-b.bin"), WINDOW_B_BASE, WINDOW_B_END);
        assertEquals(WINDOW_B_MI_COUNT, byLsn.size(),
                "窗口 B MULTI_INSERT 普查面（mi-anchor.txt 2 条）");

        // 宽行批末记录：单 entry（id=1040）、INIT_PAGE（页 14 新页首写即批末）
        WalRecord wideLast = anchored(byLsn, LSN_INIT_WIDE_LAST);
        assertEquals(XID_WIDE_COPY, wideLast.xid() & 0xFFFFFFFFL);
        HeapViews.MultiInsertView vLast = HeapViews.MultiInsertView.parse(wideLast, og);
        assertEquals(1, vLast.ntuples());
        assertEquals(-1, vLast.offsetsOff());
        List<Object[]> entriesLast = walkEntries(wideLast);
        assertEquals(1, entriesLast.size());
        assertArrayEquals(new Object[] {1040, "W".repeat(1800)}, entriesLast.get(0));

        // 尾批：非 INIT_PAGE、10 entry、offsets=[2..11]（页 14 仅 1 宽行 → 自 offnum 2 续起）
        WalRecord tail = anchored(byLsn, LSN_NONINIT_TAIL);
        assertEquals(XID_TAIL_COPY, tail.xid() & 0xFFFFFFFFL);
        HeapViews.MultiInsertView vTailRec = HeapViews.MultiInsertView.parse(tail, og);
        assertEquals(10, vTailRec.ntuples());
        for (int i = 0; i < 10; i++) {
            assertEquals(i + 2, vTailRec.offsetAt(i), "尾批 offsets 逐项（2..11 连续）");
        }
        assertEquals(14, tail.blocks().get(0).blockNo(), "尾批续写页 14");
        List<Object[]> entriesT = walkEntries(tail);
        assertEquals(10, entriesT.size());
        assertArrayEquals(new Object[] {2001, "miT2001"}, entriesT.get(0));
        assertArrayEquals(new Object[] {2010, "miT2010"}, entriesT.get(9));
    }

    /**
     * 手造 OG 记录：INIT_PAGE 的 pd_xid_base u64 前缀平移（字段整体 +8）与非 INIT_PAGE
     * 的 offsets 数组读取 + LogCSN 尾缀（在场容忍、缺省亦可——视图不消费）。
     */
    @Test
    void handBuiltOgRecordsParseHeaderVariants() {
        // 非 INIT_PAGE：flags 1 + isCompressed 1 + ntuples 2 + offsets 2*2 + LogCSN 8
        byte[] main = new byte[4 + 2 * 2 + 8];
        main[0] = 0x10;                       // flags = CONTAINS_NEW_TUPLE
        main[1] = 0;                          // isCompressed = 0
        put16(main, 2, 2);                    // ntuples = 2
        put16(main, 4, 5);                    // offsets[0] = 5
        put16(main, 6, 6);                    // offsets[1] = 6
        put64(main, 8, 12345L);               // LogCSN（值不消费）
        WalRecord rec = parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT, 7001, main));
        HeapViews.MultiInsertView v = HeapViews.MultiInsertView.parse(rec, og);
        assertEquals(2, v.ntuples());
        assertEquals(5, v.offsetAt(0));
        assertEquals(6, v.offsetAt(1));

        // LogCSN 缺省形态：main 止于 offsets 末（4+2n）同样可解
        HeapViews.MultiInsertView vNoCsn =
                HeapViews.MultiInsertView.parse(parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT, 7001,
                        new byte[] {0, 0, 1, 0, 9, 0})), og);
        assertEquals(1, vNoCsn.ntuples());
        assertEquals(9, vNoCsn.offsetAt(0));

        // INIT_PAGE：pd_xid_base u64 前置 + 头 4B（offsets 省略）+ LogCSN 8 = 20B
        byte[] mainInit = new byte[8 + 4 + 8];
        put64(mainInit, 0, 777L);             // pd_xid_base（值不消费）
        mainInit[8] = 0x10;
        put16(mainInit, 10, 2);               // ntuples = 2
        put64(mainInit, 12, 6789L);           // LogCSN
        HeapViews.MultiInsertView vInit = HeapViews.MultiInsertView.parse(
                parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT | HeapOps.XLOG_HEAP_INIT_PAGE, 7002, mainInit)), og);
        assertEquals(2, vInit.ntuples(), "INIT_PAGE ntuples 须越过 pd_xid_base 前缀读 @2");
        assertEquals(-1, vInit.offsetsOff());
        assertEquals(1, vInit.offsetAt(0), "INIT_PAGE offnum 隐含 i+1");
        assertEquals(2, vInit.offsetAt(1));
    }

    /**
     * 拒面：isCompressed≠0（页字典压缩区 M2 范围外）、offsets 数组越出 main、
     * main 过短（常态 <4 / INIT_PAGE <12）均 ISE fail-fast。
     */
    @Test
    void handBuiltOgRejectionFaces() {
        // isCompressed≠0：header 与 offsets 之后有页字典压缩区（未支持形态）
        byte[] compressed = new byte[4 + 8];
        compressed[1] = 1;                    // isCompressed = true
        put16(compressed, 2, 0);
        IllegalStateException eDict = assertThrows(IllegalStateException.class,
                () -> HeapViews.MultiInsertView.parse(
                        parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT, 7003, compressed)), og));
        assertTrue(eDict.getMessage().contains("isCompressed"), eDict.getMessage());

        // INIT_PAGE + isCompressed 双位：前缀平移后仍须拦下
        byte[] compressedInit = new byte[8 + 4 + 8];
        compressedInit[9] = 1;
        IllegalStateException eDictInit = assertThrows(IllegalStateException.class,
                () -> HeapViews.MultiInsertView.parse(
                        parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT | HeapOps.XLOG_HEAP_INIT_PAGE,
                                7003, compressedInit)), og));
        assertTrue(eDictInit.getMessage().contains("isCompressed"), eDictInit.getMessage());

        // offsets 越界：ntuples=2 但 main 仅头 4B（无 offsets 数组）
        IllegalStateException eOffsets = assertThrows(IllegalStateException.class,
                () -> HeapViews.MultiInsertView.parse(
                        parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT, 7004, new byte[] {0, 0, 2, 0})), og));
        assertTrue(eOffsets.getMessage().contains("offsets"), eOffsets.getMessage());

        // main 过短：常态 <4 与 INIT_PAGE <12（8 前缀 + 头 4）
        assertThrows(IllegalStateException.class,
                () -> HeapViews.MultiInsertView.parse(
                        parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT, 7005, new byte[3])), og));
        assertThrows(IllegalStateException.class,
                () -> HeapViews.MultiInsertView.parse(
                        parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT | HeapOps.XLOG_HEAP_INIT_PAGE,
                                7005, new byte[8 + 3])), og));

        // vanilla 档不受 OG 分支影响：同 main（isCompressed 位实为 vanilla padding）照常解析
        HeapViews.MultiInsertView vanilla = HeapViews.MultiInsertView.parse(
                parse(ogHeap2(HeapOps.XLOG_HEAP2_MULTI_INSERT, 7006,
                        new byte[] {0, 1, 1, 0, 3, 0})), WalLayoutV18.INSTANCE);
        assertEquals(1, vanilla.ntuples());
        assertEquals(3, vanilla.offsetAt(0));
    }

    // ---- 测试基建 ----

    /**
     * 迷你段走读 + heap2 MULTI_INSERT 过滤（形态与 OgXactViewTest 同源：页头跳过/
     * 续体缝合/MAXALIGN 推进；窗口尾截断记录跳过——窗口是资源切片，末记录可被截）。
     *
     * @param seg        窗口字节（起点页对齐）
     * @param base       窗口基准 LSN
     * @param windowEnd  窗口终点 LSN（排他）
     * @return lsn → 走读记录（仅 heap2 MULTI_INSERT）
     */
    private Map<Long, WalRecord> multiInsertRecordsOf(byte[] seg, long base, long windowEnd) {
        Map<Long, WalRecord> out = new HashMap<>();
        for (SegRecord sr : walkSegment(seg, base, windowEnd)) {
            if (sr.rec()[25] == HeapOps.RM_HEAP2_ID
                    && (sr.rec()[24] & HeapOps.XLOG_XACT_OPMASK) == HeapOps.XLOG_HEAP2_MULTI_INSERT) {
                out.put(sr.lsn(), WalRecordParser.parse(sr.rec(), sr.lsn(), og));
            }
        }
        assertFalse(out.isEmpty(), "窗口内无 MULTI_INSERT 记录——语料漂移或走读断裂（基准 "
                + Long.toHexString(base) + "）");
        return out;
    }

    /**
     * 迷你段走读：逐页校验 magic → 记录链 MAXALIGN 推进 + 跨页续体缝合；记录起点 +
     * totLen 越出窗口终点即停（窗口切片尾部的截断记录不进产出）。
     *
     * @param seg        窗口字节
     * @param base       窗口基准 LSN
     * @param windowEnd  窗口终点 LSN（排他）
     * @return 逐记录 (lsn, 已缝合记录字节)
     */
    private static List<SegRecord> walkSegment(byte[] seg, long base, long windowEnd) {
        List<SegRecord> out = new ArrayList<>();
        int pageSize = 8192;
        for (int page = 0; page + pageSize <= seg.length; page += pageSize) {
            assertEquals(0xD074, leU16(seg, page) & 0xFFFF, "页魔数错配于窗口偏移 " + page);
            int xlpInfo = leU16(seg, page + 2);
            long pageAddr = leU64(seg, page + 8);
            assertEquals(base + page, pageAddr, "pageaddr 与窗口基准漂移于窗口偏移 " + page);
            int remLen = leU32(seg, page + 16);
            int hdr = (xlpInfo & XLP_LONG_HEADER) != 0 ? 40 : 24;
            int pos = page + hdr;
            boolean first = true;
            while (pos + 4 <= page + pageSize && leU32(seg, pos) != 0) {
                if (first && (xlpInfo & XLP_FIRST_IS_CONTRECORD) != 0) {
                    pos += (remLen + 7) & ~7;
                    first = false;
                    continue;
                }
                int totLen = leU32(seg, pos);
                assertTrue(totLen >= 32, "totLen 短于记录头——链断裂");
                if (pageAddr + (pos - page) + totLen > windowEnd) {
                    return out;   // 窗口尾截断记录：不可缝也不可达其后
                }
                long lsn = pageAddr + (pos - page);
                out.add(new SegRecord(lsn, stitchRecord(seg, page, pos, totLen)));
                first = false;
                pos += (totLen + 7) & ~7;
            }
        }
        return out;
    }

    /**
     * 取一条记录的 totLen 字节（跨页记录缝合续体，跳过中间页短页头）。
     *
     * @param seg    窗口字节
     * @param page   记录起始所在页偏移
     * @param pos    记录起点偏移
     * @param totLen 记录总长
     * @return 恰长记录字节
     */
    private static byte[] stitchRecord(byte[] seg, int page, int pos, int totLen) {
        byte[] rec = new byte[totLen];
        int copied = Math.min(totLen, page + 8192 - pos);
        System.arraycopy(seg, pos, rec, 0, copied);
        int cur = page;
        while (copied < totLen) {
            cur += 8192;
            assertTrue(leU16(seg, cur) == 0xD074 && (leU16(seg, cur + 2) & XLP_FIRST_IS_CONTRECORD) != 0,
                    "续体页标志缺失于窗口偏移 " + cur);
            int take = Math.min(totLen - copied, 8192 - 24);
            System.arraycopy(seg, cur + 24, rec, copied, take);
            copied += take;
        }
        return rec;
    }

    /**
     * 块 0 data 逐 entry 走读（真实几何：2 对齐起点 + 7+datalen 步进——datalen 只计
     * tail 字节，mi-anchor.txt 实测锚），经生产解码器解出值行。
     *
     * @param r 走读完成的 MULTI_INSERT 记录（块 0 带 data）
     * @return 逐 entry 值行（词典 {@link #T_MI_KINDS}）
     */
    private List<Object[]> walkEntries(WalRecord r) {
        BlockRef b0 = r.blocks().get(0);
        assertTrue(b0.hasData(), "锚记录块 0 须带 data（logical 档 REGBUF_KEEP_DATA）");
        byte[] raw = r.raw();
        int end = b0.dataOff() + b0.dataLen();
        List<Object[]> out = new ArrayList<>();
        int cur = b0.dataOff();
        while (cur < end) {
            cur = (cur + 1) & ~1;   // entry 起点 2 对齐（SHORTALIGN）
            out.add(decoder.decodeEntry(raw, cur, T_MI_KINDS));
            cur += 7 + leU16(raw, cur);
        }
        return out;
    }

    /**
     * 首个 entry 的 datalen 字段值（走读几何自检——datalen 只计 tail 字节）。
     *
     * @param r 走读完成的记录（块 0 带 data）
     * @return 首 entry 的 datalen（u16）
     */
    private static int firstEntryDatalen(WalRecord r) {
        BlockRef b0 = r.blocks().get(0);
        int cur = (b0.dataOff() + 1) & ~1;   // entry 起点 2 对齐
        return leU16(r.raw(), cur);
    }

    /**
     * 按 LSN 取锚记录，缺失即失败。
     *
     * @param byLsn 走读索引
     * @param lsn   锚 LSN
     * @return 解析结果（必存在）
     */
    private static WalRecord anchored(Map<Long, WalRecord> byLsn, long lsn) {
        WalRecord r = byLsn.get(lsn);
        assertNotNull(r, "锚记录缺失于 LSN " + Long.toHexString(lsn));
        return r;
    }

    /**
     * 手造 OG heap2 记录字节：32B 记录头（totLen/term/xid u64/prev/info/rmid）+
     * 终止头（main ≤255B 用 DATA_SHORT，更长用 DATA_LONG——MULTI_INSERT main 恒短）+
     * main（无块引用——视图解析不消费块链）。
     *
     * @param info info 字节（opcode + 可选 INIT_PAGE 位）
     * @param xid  记录头 xid
     * @param main main data 字节
     * @return 记录字节数组
     */
    private static byte[] ogHeap2(int info, long xid, byte[] main) {
        boolean shortHeader = main.length <= 255;
        int headerLen = shortHeader ? 2 : 5;
        byte[] rec = new byte[32 + headerLen + main.length];
        put32(rec, 0, rec.length);
        put32(rec, 4, 1);                  // term（单机实测恒 0x1）
        put64(rec, 8, xid);                // xid u64@8
        put32(rec, 28, 0);                 // crc（走读不校验）
        rec[24] = (byte) info;
        rec[25] = (byte) HeapOps.RM_HEAP2_ID;
        if (shortHeader) {
            rec[32] = (byte) 255;          // DATA_SHORT
            rec[33] = (byte) main.length;
        } else {
            rec[32] = (byte) 254;          // DATA_LONG
            put32(rec, 33, main.length);
        }
        System.arraycopy(main, 0, rec, 32 + headerLen, main.length);
        return rec;
    }

    /**
     * 走读一条手造记录（lsn 取哨兵值）。
     *
     * @param bytes 记录字节
     * @return 走读结果
     */
    private WalRecord parse(byte[] bytes) {
        return WalRecordParser.parse(bytes, 0x200L, og);
    }

    /**
     * 加载 ogcorpus 语料窗口。
     *
     * @param name 资源名（mi-a.bin / mi-b.bin）
     * @return 窗口字节
     * @throws IOException 资源缺失或读取失败（语料是断言面组成部分，不降级）
     */
    private static byte[] loadCorpus(String name) throws IOException {
        try (InputStream in = OgMultiInsertViewTest.class.getResourceAsStream("/ogcorpus/" + name)) {
            assertNotNull(in, "语料资源缺失：ogcorpus/" + name);
            ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
            in.transferTo(out);
            return out.toByteArray();
        }
    }

    /**
     * 就地写 little-endian u16。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      16 位值
     */
    private static void put16(byte[] b, int offset, int v) {
        b[offset] = (byte) v;
        b[offset + 1] = (byte) (v >>> 8);
    }

    /**
     * 就地写 little-endian u32。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      32 位值
     */
    private static void put32(byte[] b, int offset, long v) {
        for (int i = 0; i < 4; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /**
     * 就地写 little-endian u64。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      64 位值
     */
    private static void put64(byte[] b, int offset, long v) {
        for (int i = 0; i < 8; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 16 位值
     */
    private static int leU16(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 32 位值
     */
    private static int leU32(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8)
                | ((b[offset + 2] & 0xFF) << 16) | ((b[offset + 3] & 0xFF) << 24);
    }

    /**
     * 就地读 little-endian u64。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 64 位值
     */
    private static long leU64(byte[] b, int offset) {
        return (leU32(b, offset) & 0xFFFFFFFFL) | (leU32(b, offset + 4) & 0xFFFFFFFFL) << 32;
    }

    /**
     * 迷你段走读的产出单元：记录起始 LSN + 已缝合记录字节。
     *
     * @param lsn 记录起始 LSN
     * @param rec 记录 totLen 字节
     */
    private record SegRecord(long lsn, byte[] rec) {
    }
}
