package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.replay.CatalogReplay;
import org.vastdata.vbstream.walsource.replay.CatalogRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 5.0.0 heap2 页清理记录（XLOG_HEAP2_CLEAN=0x10，9.2 系老格式）的
 * {@link HeapViews.PruneView} 解析与 {@link CatalogReplay#applyPrune} 施加验收测试。
 *
 * <p>OG 与 vanilla PG 14+ 的 PRUNE 族<strong>opcode 撞号</strong>（OG CLEAN=0x10 与
 * vanilla PRUNE_ON_ACCESS=0x10 同值、OG CLEANUP_INFO=0x30 与 vanilla
 * PRUNE_VACUUM_CLEANUP 同值），判定只能经 layout 门控——本测试的 OG 档全部经
 * {@link WalLayoutVOpenGauss#INSTANCE} 走 OG 分支。断言面三层：</p>
 * <ol>
 *   <li><strong>真实语料锚</strong>（og-plan 容器 VACUUM t_corpus 实测字节，LSN
 *       0x2367EF8 / 180B）：main=12B（latestRemovedXid <strong>u64</strong> + nredirected
 *       u16@8 + ndead u16@10——OG TransactionId 是 uint64，c.h L450），块0 data =
 *       20 对 redirected（HOT 链根迁移）+ 11 dead + 1 unused（<strong>nunused 由
 *       data 余长推导</strong>，redo_heapam.cpp HeapXlogCleanOperatorPage 同式）；</li>
 *   <li>手造字节：段几何/空 clean（块无 data 的 FPI 形态）/NO_REPAIR_PAGE 位
 *       （info=0x90）宽容/非 CLEAN opcode（0x30 CLEANUP_INFO）拒绝；</li>
 *   <li>CatalogReplay 施加：redirect 重定位行/tail/tracked + dead/unused 移除
 *       （复用 CatalogReplayTest 用例 6 断言风格）。</li>
 * </ol>
 */
class OgPruneViewTest {

    /** 语料块定位器三件组：t_corpus（og-plan 容器，与 Task 2 语料同表）。 */
    private static final long SPACENODE = 1663L;
    private static final long DBNODE = 14806L;
    private static final long RELNODE = 16395L;

    /** 真实 CLEAN 记录锚 LSN：VACUUM t_corpus 期 lazy_vacuum_page 写入（2026-10-12
     *  og-plan 容器实测采集，窗口 [0x2361A78,0x2368350] 走读定位）。 */
    private static final long LSN_REAL_CLEAN = 0x2367EF8L;

    /**
     * 真实 CLEAN 记录字节（180B totLen）：info=0x10（XLOG_HEAP2_CLEAN，repair
     * fragmentation 形态）、rmid=9；块0 = t_corpus blkno 0 + data 104B（20 对
     * redirected + 11 dead + 1 unused）；main 12B = latestRemovedXid u64=12984 +
     * nredirected=20 + ndead=11。预期段值见
     * {@link #parsesRealVacuumCleanRecordFromCorpus()} 注释。
     */
    private static final String REAL_CLEAN_HEX =
            "b4000000010000800000000000000000987e3602000000001009000009cd663e0"
                    + "02068007f060000d63900000b400000000000000000c86c360200000000"
                    + "ff0c01002b0004002c0005002d0006002e0007002f0008003000090031"
                    + "000a0032000b0033000c0034000d0035000e0036000f00370010003800"
                    + "1100390012003a0013003b0014003c0015003d0016003e000200210022"
                    + "0023002400250026002700280029002a000300b83200000000000014000b00";

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    /**
     * 真实语料 CLEAN 记录解析：20 对 redirected（from=[1,4..22] → to=[43..62]）、
     * 11 dead（[2] ∪ [33..42]）、1 unused（[3]，data 余长推导）。
     *
     * <p>关键步骤：真实字节经 OG 布局走读（头 32B/块引用几何/终止头全链正确才能
     * 收口 mainOff+mainLen==totLen）→ PruneView OG 分支解析 → 三段列表逐项断言。
     * 该记录是 OG CLEAN 格式的唯一字节实测锚（task 3 采集）：若 OG 分支按 vanilla
     * PRUNE 语义解析（flags 读 main@1），flags 恒 0 产空表——本用例即红。</p>
     */
    @Test
    void parsesRealVacuumCleanRecordFromCorpus() {
        WalRecord r = WalRecordParser.parse(hex(REAL_CLEAN_HEX), LSN_REAL_CLEAN, og);
        assertEquals(HeapOps.RM_HEAP2_ID, r.rmid());
        assertEquals(0x10, r.info() & 0x70, "opcode 应为 XLOG_HEAP2_CLEAN");
        assertEquals(12, r.mainLen(), "xl_heap_clean = u64 xid + 2×u16（SizeOfHeapClean=12）");
        assertEquals(RELNODE, r.blocks().get(0).relNode());
        assertEquals(0, r.blocks().get(0).blockNo());

        HeapViews.PruneView v = HeapViews.PruneView.parse(r, og);
        assertEquals(List.of(1, 43, 4, 44, 5, 45, 6, 46, 7, 47, 8, 48, 9, 49,
                        10, 50, 11, 51, 12, 52, 13, 53, 14, 54, 15, 55, 16, 56,
                        17, 57, 18, 58, 19, 59, 20, 60, 21, 61, 22, 62),
                v.redirectedPairs(), "20 对 HOT 链根 redirect");
        assertEquals(List.of(2, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42), v.nowdead());
        assertEquals(List.of(3), v.nowunused(), "nunused 由块 data 余长推导：(104-80-22)/2");
    }

    /**
     * 手造 CLEAN 记录段几何：1 对 redirect + 2 dead + 1 unused；flags() 为段存在性
     * 合成位（OG 记录无 XLHP flags 字节）。
     */
    @Test
    void handBuiltCleanRecordWalksSegmentsAndDerivesNunused() {
        ByteArrayOutputStream main = new ByteArrayOutputStream();
        put64(main, 4242L);         // latestRemovedXid（OG TransactionId = uint64）
        put16(main, 1);             // nredirected
        put16(main, 2);             // ndead
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        put16(data, 1);             // redirected: from
        put16(data, 5);             //             to
        put16(data, 7);             // dead: 7
        put16(data, 8);             // dead: 8
        put16(data, 9);             // unused: 9（余长推导）
        WalRecord r = WalRecordParser.parse(
                ogClean(0x10, main.toByteArray(), data.toByteArray()), 0x1000L, og);
        HeapViews.PruneView v = HeapViews.PruneView.parse(r, og);
        assertEquals(List.of(1, 5), v.redirectedPairs());
        assertEquals(List.of(7, 8), v.nowdead());
        assertEquals(List.of(9), v.nowunused());
        assertTrue((v.flags() & HeapOps.XLHP_HAS_REDIRECTIONS) != 0, "flags 为段存在性合成位");
        assertTrue((v.flags() & HeapOps.XLHP_HAS_DEAD_ITEMS) != 0);
        assertTrue((v.flags() & HeapOps.XLHP_HAS_NOW_UNUSED_ITEMS) != 0);
        assertEquals(r.blocks().get(0).dataOff(), v.dataOff());
    }

    /**
     * 空 CLEAN（三段计数全 0）：log_heap_clean 对空数组仍注册块但 rdata_len=0 →
     * needs_data=false（xloginsert.cpp：needs_data = !needs_backup），块0 无 data。
     * 解析合法产空三表（与 vanilla 空 prune 的 dataOff=-1 形态同语义）。
     */
    @Test
    void emptyCleanRecordWithoutBlockDataYieldsEmptySegments() {
        ByteArrayOutputStream main = new ByteArrayOutputStream();
        put64(main, 0L);
        put16(main, 0);             // nredirected = 0
        put16(main, 0);             // ndead = 0
        WalRecord r = WalRecordParser.parse(
                ogClean(0x10, main.toByteArray(), null), 0x1000L, og);
        assertEquals(0, r.blocks().get(0).dataLen(), "空 clean 块无 data（FPI/空数组形态）");
        HeapViews.PruneView v = assertDoesNotThrow(() -> HeapViews.PruneView.parse(r, og));
        assertEquals(List.of(), v.redirectedPairs());
        assertEquals(List.of(), v.nowdead());
        assertEquals(List.of(), v.nowunused());
    }

    /**
     * NO_REPAIR_PAGE 位（info=0x90 = CLEAN|0x80，heap_page_prune 访问期修剪形态）：
     * 位在 info 高位不进 opcode 位段，解析须宽容——htup.h L746 + heapam.cpp
     * log_heap_clean 的 {@code info |= XLOG_HEAP2_NO_REPAIR_PAGE} 分支。
     */
    @Test
    void noRepairPageInfoBitIsTolerated() {
        ByteArrayOutputStream main = new ByteArrayOutputStream();
        put64(main, 0L);
        put16(main, 0);
        put16(main, 1);             // ndead=1
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        put16(data, 4);
        WalRecord r = WalRecordParser.parse(
                ogClean(0x90, main.toByteArray(), data.toByteArray()), 0x1000L, og);
        HeapViews.PruneView v = HeapViews.PruneView.parse(r, og);
        assertEquals(List.of(4), v.nowdead());
        assertEquals(List.of(), v.redirectedPairs());
    }

    /**
     * OG 档非 CLEAN 的 heap2 opcode 拒绝：XLOG_HEAP2_CLEANUP_INFO（0x30，无块引用的
     * standby 冲突记录）与 vanilla PRUNE_VACUUM_CLEANUP 数值撞号——PruneView OG 分支
     * 须拒（main 语义完全不同）；且 CatalogReplay.applyPrune 对其 no-op 不抛
     * （施加门先于解析按 layout 收窄到 0x10）。
     */
    @Test
    void cleanupInfoOpcodeRejectedAndPrunedByApplyGate() {
        ByteArrayOutputStream main = new ByteArrayOutputStream();
        put64(main, 99L);
        put16(main, 0);
        put16(main, 0);
        byte[] cleanupInfo = ogClean(0x30, main.toByteArray(), null);
        WalRecord r = WalRecordParser.parse(cleanupInfo, 0x1000L, og);
        assertThrows(IllegalStateException.class, () -> HeapViews.PruneView.parse(r, og));

        CatalogReplay replay = new CatalogReplay(og, new TupleDecoder(og));
        CatalogStores stores = new CatalogStores();
        stores.pgClassRelfilenode(RELNODE);
        assertDoesNotThrow(() -> replay.applyPrune(r, stores), "非 CLEAN opcode 须在门前 no-op");
    }

    /**
     * CatalogReplay 施加面（OG layout）：1 对 redirect 重定位行/tail/tracked，
     * 2 dead + 1 unused 移除行——断言风格复用 CatalogReplayTest 用例 6。
     */
    @Test
    void applyOgCleanRedirectsRowsAndDropsDead() {
        CatalogReplay replay = new CatalogReplay(og, new TupleDecoder(og));
        CatalogStores stores = new CatalogStores();
        stores.pgClassRelfilenode(RELNODE);
        long a = CatalogReplay.ctidKey(0, 1);
        long dead1 = CatalogReplay.ctidKey(0, 7);
        long dead2 = CatalogReplay.ctidKey(0, 8);
        long unused = CatalogReplay.ctidKey(0, 9);
        CatalogRow.ClassRow rowA = new CatalogRow.ClassRow(101, "ra", 11, 12, 0, 10, 0, 300, 0, "r");
        stores.classRows().put(a, rowA);
        stores.classRows().put(dead1, new CatalogRow.ClassRow(201, "d7", 11, 12, 0, 10, 0, 0, 0, "r"));
        stores.classRows().put(dead2, new CatalogRow.ClassRow(202, "d8", 11, 12, 0, 10, 0, 0, 0, "r"));
        stores.classRows().put(unused, new CatalogRow.ClassRow(203, "u9", 11, 12, 0, 10, 0, 0, 0, "r"));
        stores.rawClassTails().put(a, new byte[]{1, 2, 3});
        stores.trackedTableCtid(a);

        ByteArrayOutputStream main = new ByteArrayOutputStream();
        put64(main, 4242L);
        put16(main, 1);             // nredirected
        put16(main, 2);             // ndead
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        put16(data, 1);             // redirect 1 -> 5
        put16(data, 5);
        put16(data, 7);             // dead
        put16(data, 8);
        put16(data, 9);             // unused
        WalRecord r = WalRecordParser.parse(
                ogClean(0x10, main.toByteArray(), data.toByteArray()), 0x1000L, og);
        replay.applyPrune(r, stores);

        assertEquals(rowA, stores.classRows().get(CatalogReplay.ctidKey(0, 5)), "redirect 1->5 重定位 rowA");
        assertNull(stores.classRows().get(a));
        assertNull(stores.classRows().get(dead1), "dead 行必须移除");
        assertNull(stores.classRows().get(dead2));
        assertNull(stores.classRows().get(unused), "unused 行必须移除");
        assertEquals(CatalogReplay.ctidKey(0, 5), stores.trackedTableCtid(), "tracked 随 redirect 移动");
        assertTrue(stores.rawClassTails().containsKey(CatalogReplay.ctidKey(0, 5)), "tail 随 redirect 重定位");
    }

    /**
     * 十六进制串 → 字节（真实语料锚装载）。
     *
     * @param s 偶数长度小写/大写十六进制串
     * @return 等长字节数组
     */
    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    /**
     * 手造 OG heap2 CLEAN 记录：32B 记录头（info/rmid 可配）+ 单块引用
     * （完整定位器 12B + extra_flag u16 + blkno u32 + last_lsn u64，data 可缺省）
     * + DATA_SHORT 终止头 + 载荷（块 data + main）——块头几何与
     * {@code OgWalRecordParserTest.OgRecordBuilder} 同源（双源互证）。
     *
     * @param info      记录 info 字节（0x10=CLEAN / 0x90=CLEAN|NO_REPAIR_PAGE /
     *                  0x30=CLEANUP_INFO 拒绝面）
     * @param main      main data（xl_heap_clean 结构 12B）
     * @param blockData 块0 data（段数组）；null = 无 data（空 clean/FPI 形态）
     * @return 记录字节数组（totLen 即长度，无 MAXALIGN 垫）
     */
    private static byte[] ogClean(int info, byte[] main, byte[] blockData) {
        boolean hasData = blockData != null;
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        headers.write(0);                                    // block id 0
        headers.write(hasData ? 0x20 : 0x00);               // HAS_DATA 位（OG 0x20）
        put16(headers, hasData ? blockData.length : 0);     // data_length
        put32(headers, SPACENODE);
        put32(headers, DBNODE);
        put32(headers, RELNODE);
        put16(headers, 0);                                   // extra_flag
        put32(headers, 0);                                   // blkno
        put64(headers, 0);                                   // last_lsn
        if (hasData) {
            payload.writeBytes(blockData);
        }
        headers.write(255);                                  // DATA_SHORT 终止头
        headers.write(main.length);
        payload.writeBytes(main);
        byte[] hdr = headers.toByteArray();
        byte[] pay = payload.toByteArray();
        byte[] rec = new byte[32 + hdr.length + pay.length];
        put32(rec, 0, rec.length);
        put32(rec, 4, 0x80000001L);                          // term（单机实测恒值）
        put32(rec, 8, 777);                                  // xid
        rec[24] = (byte) info;
        rec[25] = HeapOps.RM_HEAP2_ID;
        System.arraycopy(hdr, 0, rec, 32, hdr.length);
        System.arraycopy(pay, 0, rec, 32 + hdr.length, pay.length);
        return rec;
    }

    /**
     * 向流写 little-endian u16。
     *
     * @param out 目标流
     * @param v   16 位值
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
        for (int i = 0; i < 4; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /**
     * 向流写 little-endian u64。
     *
     * @param out 目标流
     * @param v   64 位值
     */
    private static void put64(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /**
     * 就地写 little-endian u32（记录头用）。
     *
     * @param rec    目标数组
     * @param offset 起始偏移
     * @param v      32 位值
     */
    private static void put32(byte[] rec, int offset, long v) {
        for (int i = 0; i < 4; i++) {
            rec[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }
}
