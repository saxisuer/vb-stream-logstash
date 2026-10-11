package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 5.0.0 真实 WAL 语料驱动的 {@link WalRecordParser} 验收测试。
 *
 * <p>语料为 og-plan 容器（opengauss:5.0.0，wal_level=logical）第 2 号段文件
 * [0x352000,0x35A000) 页对齐窗口（32KB / 4 页），写入序列与锚 LSN 见同目录
 * anchor.txt：INSERT+INIT_PAGE(0x80)/INSERT(0x00)/HOT_UPDATE(0x40, REPLICA
 * IDENTITY FULL 旧行随 main)/DELETE(0x10, 旧行随 main)。断言面四层：</p>
 * <ol>
 *   <li>窗口内全部记录（含 2 条 FPI 与 2 条跨页续体）经 OG 布局走读全部通过
 *   {@code mainOff+mainLen==totLen} 不变量——即块引用链形态（定位器 12B +
 *   extra_flag u16 + blkno u32 + last_lsn u64）与头字段偏移（xid@8/info@24/
 *   rmid@25）整体正确；</li>
 *   <li>四条锚记录按 LSN 精确定位，块定位器/块载荷/旧行前缀逐项核对（值来自
 *   anchor.txt，采集期实测写入）；</li>
 *   <li>heap 前缀锚：{@code sizeOfHeapUpdate()}=13（SizeOfOldHeapUpdate 5 +
 *   LogCSN 8）后恰为旧行 xl_heap_header；{@code sizeOfHeapDelete()}=11 同构；
 *   {@code sizeOfHeapInplace()}=2（heap_inplace_update 不记 LogCSN）；</li>
 *   <li>块引用 id 扩展位 fail-fast：手造 id 置 0x80（bucket/segpage）/0x40（TDE）
 *   的记录在 OG 档抛 ISE（消息含 LSN 与违例位值），V17/V18 档保持既有
 *   "unknown block id" 拒绝面不受扰。</li>
 * </ol>
 */
class OgWalRecordParserTest {

    /** 锚 LSN：INSERT+INIT_PAGE（首行 'b'x100，xid=12980）——anchor.txt 实测。
     *  记录 LSN = pageaddr + 页内全偏移（含 24B 页头）——fix round 1 修正（首版
     *  误减页头宽 24B，经 prev 链 + 服务端 insert location 双重证伪）。 */
    private static final long LSN_INSERT_INIT_PAGE = 0x2359550L;

    /** 锚 LSN：普通 INSERT（'x'x64，xid=12981）。 */
    private static final long LSN_INSERT_PLAIN = 0x2359778L;

    /** 锚 LSN：HOT_UPDATE（新行 'c'x100 在块载荷、旧行 'b'x100 在 main，xid=12982）。 */
    private static final long LSN_HOT_UPDATE = 0x2359908L;

    /** 锚 LSN：DELETE（旧行 'x'x64 在 main，xid=12983）。 */
    private static final long LSN_DELETE = 0x2359AD8L;

    /** 锚 LSN：两条 XLOG_HEAP_INPLACE（catalog 原地改写，main 恒 2B）。 */
    private static final long[] LSN_INPLACE = {0x23587D8L, 0x2358918L};

    /** 语料表 t_corpus 的块定位器三件组（anchor.txt：spc/db/relfilenode）。 */
    private static final long SPACENODE = 1663L;
    private static final long DBNODE = 14806L;
    private static final long RELNODE = 16395L;

    /** 语料写入事务 xid（anchor.txt：12980-12983）。 */
    private static final int XID_INSERT1 = 12980;
    private static final int XID_INSERT2 = 12981;
    private static final int XID_UPDATE = 12982;
    private static final int XID_DELETE = 12983;

    /** 页头 info 位 XLP_FIRST_IS_CONTRECORD（xlog_basic.h）——页首块为前页记录续体。 */
    private static final int XLP_FIRST_IS_CONTRECORD = 0x0001;

    /** 页头 info 位 XLP_LONG_HEADER——段首长页头（40B）。 */
    private static final int XLP_LONG_HEADER = 0x0002;

    /** 走读收敛下限：窗口实测 283 条（anchor.txt），断言 ≥200 防语料漂移假绿。 */
    private static final int MIN_RECORDS = 200;

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    /**
     * 真实语料全窗走读 + 四条锚记录逐项验收 + prev 链自洽性钉死。
     *
     * <p>关键步骤：加载语料 → 迷你段走读（页头跳过/续体缝合/MAXALIGN 链推进，
     * 形态与生产 walker 同源但零状态）→ 逐条 {@code WalRecordParser.parse}（任何
     * 记录布局错配即 ISE 直接红）→ prev 链断言（记录 xl_prev@16 == 前一记录
     * 起始 LSN——LSN 口径"pageaddr+页内全偏移含页头"的外部权威锚，跨页记录
     * 除外：OG 对跨页记录写续体页边界为 prev，属源面行为非走读错位）→
     * 锚记录按 LSN 精确断言（rmid/xid/opcode/定位器/块载荷值域/旧行前缀与内容）。</p>
     */
    @Test
    void parsesRealOgCorpusAnchoredRecords() throws IOException {
        byte[] seg = loadCorpus();
        List<SegRecord> walk = walkSegment(seg, og);
        assertTrue(walk.size() >= MIN_RECORDS,
                "窗口记录数 " + walk.size() + " 低于下限——语料漂移或走读断裂");

        Map<Long, WalRecord> byLsn = new HashMap<>();
        for (SegRecord sr : walk) {
            byLsn.put(sr.lsn, WalRecordParser.parse(sr.rec, sr.lsn, og));   // 任何布局错配在此 ISE
        }

        // prev 链：xl_prev == 前一记录起始 LSN（跨页记录与 prev=0 除外）——LSN 口径
        // 的外部权威锚（fix round 1：首版口径漏算页头 24B 被 prev 链整体证伪）
        int pageSize = og.walBlockSize();
        int chainChecked = 0;
        for (int i = 1; i < walk.size(); i++) {
            SegRecord cur = walk.get(i);
            long prev = leU64(cur.rec, 16);
            boolean spansPage = cur.lsn + ((cur.rec.length + 7) & ~7)
                    > (cur.lsn & ~(pageSize - 1L)) + pageSize;
            if (prev == 0 || spansPage) {
                continue;
            }
            assertEquals(walk.get(i - 1).lsn, prev,
                    "prev 链断裂于 lsn=" + Long.toHexString(cur.lsn));
            chainChecked++;
        }
        assertTrue(chainChecked >= MIN_RECORDS, "prev 链覆盖记录数异常偏低：" + chainChecked);

        // ① INSERT+INIT_PAGE：'b'x100 新元组在块 0 载荷，定位器三件组 + fork/blkno 核对
        WalRecord insert1 = anchored(byLsn, LSN_INSERT_INIT_PAGE);
        assertEquals(10, insert1.rmid());
        assertEquals(0x00, insert1.info() & 0x70);
        assertEquals(0x80, insert1.info() & 0x80, "首写新页应带 INIT_PAGE 位");
        assertEquals(XID_INSERT1, insert1.xid());
        BlockRef b1 = heapMainBlock(insert1);
        assertEquals(SPACENODE, b1.spc());
        assertEquals(DBNODE, b1.db());
        assertEquals(RELNODE, b1.relNode());
        assertEquals(0, b1.fork());
        assertEquals(0, b1.blockNo());
        assertTrue(contains(rawOf(insert1, b1), times((byte) 'b', 100)), "块载荷应含 'b'x100");
        // main = pd_xid_base u64 + SizeOfOldHeapInsert 3B + LogCSN 8B = 19
        assertEquals(19, insert1.mainLen());

        // ② 普通 INSERT：'x'x64 新元组在块 0 载荷
        WalRecord insert2 = anchored(byLsn, LSN_INSERT_PLAIN);
        assertEquals(10, insert2.rmid());
        assertEquals(0x00, insert2.info() & 0x70);
        assertEquals(XID_INSERT2, insert2.xid());
        assertTrue(contains(rawOf(insert2, heapMainBlock(insert2)), times((byte) 'x', 64)),
                "块载荷应含 'x'x64");
        assertEquals(11, insert2.mainLen(), "SizeOfOldHeapInsert 3B + LogCSN 8B");

        // ③ HOT_UPDATE：新行 'c'x100 在块载荷；旧行在 main 的 sizeOfHeapUpdate 前缀之后，
        //    含 'b'x100（REPLICA IDENTITY FULL）——13 前缀锚的字节级验证
        WalRecord update = anchored(byLsn, LSN_HOT_UPDATE);
        assertEquals(10, update.rmid());
        assertEquals(0x40, update.info() & 0x70);
        assertEquals(XID_UPDATE, update.xid());
        assertTrue(contains(rawOf(update, heapMainBlock(update)), times((byte) 'c', 100)),
                "新元组 'c'x100 应在块载荷");
        assertEquals(124, update.mainLen());
        int oldTupOff = update.mainOff() + og.sizeOfHeapUpdate();
        byte[] oldTuple = Arrays.copyOfRange(update.raw(), oldTupOff,
                update.mainOff() + update.mainLen());
        // 旧行 = xl_heap_header 5B + 对齐垫 1B + id int4 + varlena 1B + 'b'x100 = 111B
        assertEquals(111, oldTuple.length);
        assertEquals(24, oldTuple[4] & 0xFF, "xl_heap_header.t_hoff 应为 24");
        assertTrue(contains(oldTuple, times((byte) 'b', 100)), "旧行应含 'b'x100");
        assertEquals(0x04, update.raw()[update.mainOff() + 4] & 0x04,
                "xl_heap_update.flags 应含 XLH_UPDATE_CONTAINS_OLD_TUPLE");

        // ④ DELETE：旧行在 main 的 sizeOfHeapDelete 前缀之后，含 'x'x64——11 前缀锚
        WalRecord delete = anchored(byLsn, LSN_DELETE);
        assertEquals(10, delete.rmid());
        assertEquals(0x10, delete.info() & 0x70);
        assertEquals(XID_DELETE, delete.xid());
        assertEquals(86, delete.mainLen());
        int delOldOff = delete.mainOff() + og.sizeOfHeapDelete();
        byte[] delTuple = Arrays.copyOfRange(delete.raw(), delOldOff,
                delete.mainOff() + delete.mainLen());
        assertEquals(75, delTuple.length, "旧行 = 5B 头 + 对齐垫 1B + id 4B + varlena 1B + 'x'x64");
        assertTrue(contains(delTuple, times((byte) 'x', 64)), "旧行应含 'x'x64");
        assertEquals(0x04, delete.raw()[delete.mainOff() + 2] & 0x04,
                "xl_heap_delete.flags 应含 XLH_DELETE_CONTAINS_OLD_TUPLE");

        // ⑤ INPLACE：main 恒 2B（SizeOfHeapInplace 锚——heap_inplace_update 不记 LogCSN）
        for (long lsn : LSN_INPLACE) {
            WalRecord inplace = anchored(byLsn, lsn);
            assertEquals(0x70, inplace.info() & 0x70);
            assertEquals(2, inplace.mainLen(), "xl_heap_inplace {offnum u16} 后无附加块");
        }
    }

    /**
     * heap 前缀锚常量面：13 = SizeOfOldHeapUpdate(5) + LogCSN(8)；11 =
     * SizeOfOldHeapDelete(3) + LogCSN(8)；2 = SizeOfHeapInplace（无 CSN 块）。
     *
     * <p>与语料测试分离锚常量本身——常量错（如误锚 5/3 或 25/17）在此直接红，
     * 消息即锚定义，不必从载荷断言反推。</p>
     */
    @Test
    void heapPrefixSizesAreAnchoredToLogicalInfoShape() {
        assertEquals(13, og.sizeOfHeapUpdate(), "SizeOfOldHeapUpdate 5 + LogCSN 8");
        assertEquals(11, og.sizeOfHeapDelete(), "SizeOfOldHeapDelete 3 + LogCSN 8");
        assertEquals(2, og.sizeOfHeapInplace(), "SizeOfHeapInplace 2（heap_inplace 不记 CSN）");
    }

    /**
     * 块引用 id 扩展位 fail-fast：OG 档置 0x80（BKID_HAS_BUCKET_OR_SEGPAGE）或
     * 0x40（BKID_HAS_TDE_PAGE）即 ISE，消息携带记录 LSN 与违例位值。
     *
     * <p>同时核对 V18 档对同字节保持既有 "unknown block id" 拒绝面（扩展位分支
     * 不得改变 vanilla 行为——V17/V18 零字节变化约束）。</p>
     */
    @Test
    void blockRefExtensionBitsFailFast() {
        long lsn = 0x1000L;
        byte[] withBucket = ogRecord(0x80 | 0).build();
        IllegalStateException e80 = assertThrows(IllegalStateException.class,
                () -> WalRecordParser.parse(withBucket, lsn, og));
        assertTrue(e80.getMessage().contains("0x80"), "消息应含违例位值 0x80: " + e80.getMessage());
        assertTrue(e80.getMessage().contains(Long.toString(lsn)), "消息应含记录 LSN");

        byte[] withTde = ogRecord(0x40 | 0).build();
        IllegalStateException e40 = assertThrows(IllegalStateException.class,
                () -> WalRecordParser.parse(withTde, lsn, og));
        assertTrue(e40.getMessage().contains("0x40"), "消息应含违例位值 0x40: " + e40.getMessage());

        // vanilla 档对高位 id 保持既有 "unknown block id" 面——扩展位分支不得对
        // V17/V18 生效（零字节变化约束；用 V18 形制的记录直击该分支排序）
        byte[] vanillaShaped = WalBytes.record(10, 0x00, 1)
                .block(0, SPACENODE, DBNODE, RELNODE, 7)
                .data(new byte[4])
                .main(new byte[4])
                .build();
        vanillaShaped[WalLayoutV18.INSTANCE.recordHeaderSize()] = (byte) 0xC2;   // 高位 id
        IllegalStateException eLegacy = assertThrows(IllegalStateException.class,
                () -> WalRecordParser.parse(vanillaShaped, lsn, WalLayoutV18.INSTANCE));
        assertTrue(eLegacy.getMessage().contains("unknown block id"));
    }

    /**
     * 手造 OG 记录走读：完整定位器块 + SAME_REL 块 + main data 的字段/偏移核对。
     *
     * <p>区别于语料测试的黑盒断言，本用例逐字段核对 OG 块头几何：完整块 =
     * id u8 + fork_flags u8 + data_length u16 + 定位器 12B + extra_flag u16 +
     * blkno u32 + last_lsn u64；SAME_REL 块省定位器与 extra_flag（blkno 与
     * last_lsn 仍随）——两块的 spc/db/relNode 复用语义同 vanilla。</p>
     */
    @Test
    void handBuiltOgRecordWalksBlockRefGeometry() {
        byte[] blockData = {1, 2, 3, 4};
        byte[] mainData = new byte[10];
        byte[] rec = ogRecord(0)
                .withBlock(blockData)
                .withSameRelBlock()
                .withMain(mainData)
                .build();
        WalRecord r = WalRecordParser.parse(rec, 0, og);
        assertEquals(10, r.rmid());
        assertEquals(0x00, r.info());
        assertEquals(777, r.xid());
        assertEquals(2, r.blocks().size());
        BlockRef b0 = r.blocks().get(0);
        assertEquals(SPACENODE, b0.spc());
        assertEquals(DBNODE, b0.db());
        assertEquals(RELNODE, b0.relNode());
        assertEquals(9, b0.blockNo());
        assertTrue(b0.hasData());
        assertEquals(blockData.length, b0.dataLen());
        BlockRef b1 = r.blocks().get(1);
        assertEquals(b0.spc(), b1.spc());
        assertEquals(b0.db(), b1.db());
        assertEquals(b0.relNode(), b1.relNode());
        assertEquals(3, b1.blockNo());
        assertEquals(rec.length, r.mainOff() + r.mainLen(), "main 区恰收口 totLen");
        // OG 记录头字段：totLen@0、term@4、xid@8、info@24、rmid@25（xlog_basic.h）
        assertEquals(rec.length, leU32(rec, 0));
        assertEquals(0x80000001, leU32(rec, 4));
        assertEquals(777, leU32(rec, 8));
        assertEquals(0x00, rec[24] & 0xFF);
        assertEquals(10, rec[25] & 0xFF);
    }

    /**
     * 加载 ogcorpus 语料二进制。
     *
     * <p>来源与锚见同目录 anchor.txt；流式读尽（32768B）后返回——资源缺失/
     * 读失败让测试直接红（语料是断言面的组成部分，不做降级）。</p>
     *
     * @return 段窗口字节（4 个 8KB 页）
     * @throws IOException 资源缺失或读取失败
     */
    private static byte[] loadCorpus() throws IOException {
        try (InputStream in = OgWalRecordParserTest.class.getResourceAsStream(
                "/ogcorpus/wal-000000010000000000000002.bin")) {
            assertNotNull(in, "语料资源缺失：ogcorpus/wal-000000010000000000000002.bin");
            ByteArrayOutputStream out = new ByteArrayOutputStream(32768);
            in.transferTo(out);
            return out.toByteArray();
        }
    }

    /**
     * 迷你段走读：页头跳过 + 记录链 MAXALIGN 推进 + 跨页续体缝合，产出逐记录
     * (lsn, 记录字节) 列表。
     *
     * <p>关键步骤：逐页校验 magic 与 pageaddr（错配即断言失败）；页首
     * XLP_FIRST_IS_CONTRECORD 置位时按 rem_len 跳过前页记录的续体块（窗口起点
     * 不保证记录边界）；记录链按 {@code MAXALIGN(totLen)} 推进；记录跨页时从
     * 后续页的短页头之后缝合续体字节（续体页同样置 FIRST_IS_CONTRECORD——其
     * rem_len 不再消费）；页尾全零即本页记录终结。窗口末记录完整包含（语料采集
     * 保证），越界即断言失败。</p>
     *
     * @param seg    段窗口字节（起点须页对齐）
     * @param layout 版本布局描述符（页头尺寸/magic/记录头长）
     * @return 逐记录 (lsn, totLen 字节——跨页记录已缝合，尾部不带 MAXALIGN 垫)
     */
    private static List<SegRecord> walkSegment(byte[] seg, WalLayout layout) {
        List<SegRecord> out = new ArrayList<>();
        int pageSize = layout.walBlockSize();
        for (int page = 0; page + pageSize <= seg.length; page += pageSize) {
            assertEquals(layout.pageMagic(), leU16(seg, page), "页魔数错配于窗口偏移 " + page);
            int xlpInfo = leU16(seg, page + 2);
            long pageAddr = leU64(seg, page + 8);
            assertEquals(0, pageAddr & (pageSize - 1), "pageaddr 须页对齐");
            int remLen = leU32(seg, page + 16);
            int hdr = (xlpInfo & XLP_LONG_HEADER) != 0
                    ? layout.longPageHeaderSize() : layout.shortPageHeaderSize();
            int pos = page + hdr;
            boolean first = true;
            while (pos + 4 <= page + pageSize && leU32(seg, pos) != 0) {
                if (first && (xlpInfo & XLP_FIRST_IS_CONTRECORD) != 0) {
                    pos += (remLen + 7) & ~7;   // 窗口起点前的记录续体——跳过
                    first = false;
                    continue;
                }
                int totLen = leU32(seg, pos);
                assertTrue(totLen >= layout.recordHeaderSize(), "totLen 短于记录头——链断裂");
                // 记录 LSN = pageaddr + 页内全偏移（含页头字节）——WAL 的 LSN 语义是
                // 字节地址而非"页头后数据区偏移"（fix round 1：首版误减 hdr 使全部
                // 锚恒低一个页头宽 24B，被 prev 链 280/282 证 + 服务端 insert
                // location 两点精确命中证伪）
                out.add(new SegRecord(pageAddr + (pos - page),
                        stitchRecord(seg, page, pos, totLen, layout)));
                first = false;
                pos += (totLen + 7) & ~7;
            }
        }
        return out;
    }

    /**
     * 取一条记录的 totLen 字节——跨页记录缝合续体（跳过中间页的短页头）。
     *
     * <p>关键步骤：先取本页剩余；不足 totLen 时逐后续页推进——每页须置
     * XLP_FIRST_IS_CONTRECORD（断言），自其短页头后接续取。窗口末记录完整包含
     * （语料采集保证），越界即断言失败。</p>
     *
     * @param seg      段窗口字节
     * @param page     记录起始所在页的窗口偏移
     * @param pos      记录起点窗口偏移
     * @param totLen   记录总长
     * @param layout   版本布局描述符（页大小/页头尺寸）
     * @return 记录 totLen 字节（恰长，无尾垫）
     */
    private static byte[] stitchRecord(byte[] seg, int page, int pos, int totLen, WalLayout layout) {
        int pageSize = layout.walBlockSize();
        byte[] rec = new byte[totLen];
        int copied = Math.min(totLen, page + pageSize - pos);
        System.arraycopy(seg, pos, rec, 0, copied);
        int cur = page;
        while (copied < totLen) {
            cur += pageSize;
            assertTrue(leU16(seg, cur) == layout.pageMagic()
                            && (leU16(seg, cur + 2) & XLP_FIRST_IS_CONTRECORD) != 0,
                    "续体页标志缺失于窗口偏移 " + cur);
            int hdr = layout.shortPageHeaderSize();
            int take = Math.min(totLen - copied, pageSize - hdr);
            System.arraycopy(seg, cur + hdr, rec, copied, take);
            copied += take;
        }
        return rec;
    }

    /**
     * 迷你段走读的产出单元：记录起始 LSN + 已缝合的记录字节。
     *
     * @param lsn 记录起始 LSN（pageaddr + 页内偏移）
     * @param rec 记录 totLen 字节（跨页已缝合）
     */
    private record SegRecord(long lsn, byte[] rec) {
    }

    /**
     * 按 LSN 取锚记录，缺失即失败（锚漂移说明语料或走读错位）。
     *
     * @param byLsn 走读结果索引
     * @param lsn   锚 LSN
     * @return 解析结果（必存在）
     */
    private static WalRecord anchored(Map<Long, WalRecord> byLsn, long lsn) {
        WalRecord r = byLsn.get(lsn);
        assertNotNull(r, "锚记录缺失于 LSN " + Long.toHexString(lsn));
        return r;
    }

    /**
     * 取记录的 fork=0 首块（heap 主 fork 块——heap 记录的元组载荷所在）。
     *
     * @param r heap 记录
     * @return 主 fork 块引用
     */
    private static BlockRef heapMainBlock(WalRecord r) {
        return r.blocks().stream().filter(b -> b.fork() == 0).findFirst()
                .orElseThrow(() -> new AssertionError("heap 记录无主 fork 块"));
    }

    /**
     * 取块载荷字节（块 data 区切片）。
     *
     * @param r 走读完成的记录
     * @param b 目标块
     * @return dataOff..dataOff+dataLen 切片
     */
    private static byte[] rawOf(WalRecord r, BlockRef b) {
        return Arrays.copyOfRange(r.raw(), b.dataOff(), b.dataOff() + b.dataLen());
    }

    /**
     * 生成 n 连重复字节的模式串（载荷存在性断言用）。
     *
     * @param b 字节值
     * @param n 重复次数
     * @return 模式串
     */
    private static byte[] times(byte b, int n) {
        byte[] out = new byte[n];
        Arrays.fill(out, b);
        return out;
    }

    /**
     * 子串存在性断言辅助（harness 自持，避免测试面引 Hamcrest）。
     *
     * @param hay    被检字节
     * @param needle 模式
     * @return 存在 true
     */
    private static boolean contains(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (hay[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
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
     * 手造 OG 记录的字节拼装器——OG 块头几何与测试断言各写一份（双源互证）。
     *
     * <p>记录头 32B（xlog_basic.h XLogRecord：totLen@0/term@4/xid@8/pad@12/prev@16/
     * info@24/rmid@25/bucket@26/crc@28）+ 块头链（完整定位器块 / SAME_REL 块）+
     * DATA_SHORT 终止头（main 非空时）+ 载荷区。完整块头 = id u8 + fork_flags u8 +
     * data_length u16 + 定位器 12B + extra_flag u16 + blkno u32 + last_lsn u64
     * （openGauss v5.0.0 xlogreader.cpp DecodeXLogRecord 消费序）；SAME_REL 块省
     * 定位器与 extra_flag。id 的高位扩展位（0x80/0x40）由首参直接传入。</p>
     */
    private static final class OgRecordBuilder {

        private final int blockIdWithExtBits;
        private byte[] blockData = {};
        private boolean withSameRel;
        private byte[] mainData = {};

        /**
         * 记下首块的 id 字节（可含 0x80/0x40 扩展位）。
         *
         * @param blockIdWithExtBits 首块 id（含扩展位时由 parser fail-fast）
         */
        private OgRecordBuilder(int blockIdWithExtBits) {
            this.blockIdWithExtBits = blockIdWithExtBits;
        }

        /**
         * 声明首块的 data 载荷（fork_flags 置 HAS_DATA）。
         *
         * @param data 载荷字节
         * @return 本实例（链式）
         */
        private OgRecordBuilder withBlock(byte[] data) {
            this.blockData = data;
            return this;
        }

        /**
         * 追加一个 SAME_REL 块（blockno=3，无定位器与 extra_flag）。
         *
         * @return 本实例（链式）
         */
        private OgRecordBuilder withSameRelBlock() {
            this.withSameRel = true;
            return this;
        }

        /**
         * 声明 main data（发射 DATA_SHORT 终止头）。
         *
         * @param data main 载荷
         * @return 本实例（链式）
         */
        private OgRecordBuilder withMain(byte[] data) {
            this.mainData = data;
            return this;
        }

        /**
         * 拼出完整 OG 记录字节（totLen 即数组长度，无 MAXALIGN 尾垫）。
         *
         * @return 记录字节数组
         */
        private byte[] build() {
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            // 首块：完整定位器
            headers.write(blockIdWithExtBits);
            headers.write(0x20);   // BKPBLOCK_HAS_DATA, fork=0
            put16(headers, blockData.length);
            put32(headers, SPACENODE);
            put32(headers, DBNODE);
            put32(headers, RELNODE);
            put16(headers, 0);     // extra_flag u16
            put32(headers, 9);     // blkno
            put64(headers, 0);     // last_lsn u64
            payload.writeBytes(blockData);
            if (withSameRel) {
                headers.write(1);  // 第二块 id
                headers.write(0x80 | 0x20);   // SAME_REL | HAS_DATA
                put16(headers, 2);
                put32(headers, 3); // blkno
                put64(headers, 0); // last_lsn u64
                payload.writeBytes(new byte[] {9, 9});
            }
            if (mainData.length > 0) {
                headers.write(255);
                headers.write(mainData.length);
                payload.writeBytes(mainData);
            }
            byte[] hdr = headers.toByteArray();
            byte[] pay = payload.toByteArray();
            byte[] rec = new byte[32 + hdr.length + pay.length];
            put32(rec, 0, rec.length);
            put32(rec, 4, 0x80000001L);   // term（单机实测恒值）
            put32(rec, 8, 777);           // xid
            rec[24] = 0x00;               // info = XLOG_HEAP_INSERT
            rec[25] = 10;                 // rmid = RM_HEAP_ID
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

    /**
     * 手造单块 OG 记录便捷入口（扩展位测试用——id 可置 0x80/0x40）。
     *
     * @param blockIdWithExt 首块 id（可含扩展位）
     * @return 拼装器实例
     */
    private static OgRecordBuilder ogRecord(int blockIdWithExt) {
        return new OgRecordBuilder(blockIdWithExt);
    }
}
