package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WalRecordParser 记录/block 头走读 + WalBytes 手造字节 DSL 的失败先行测试。
 *
 * <p>断言面来自任务书 Step 1 四用例（单 block+main 的 dataOff/mainOff 自洽与
 * mainOff+mainLen==totLen 不变量 / 仅 image 无终止头的 datatotal 终止 / TOPLEVEL_XID
 * 标记读出与 effXid 归并 / SAME_REL 复用前定位器），另含 WalBytes 自身行为
 * （终止头自动选档、载荷序、记录头定偏移落位）与 parser 的失败分支（未知 block id、
 * 无前定位器的 SAME_REL、totLen 与实际字节数不符的不变量破坏）。</p>
 */
class WalRecordParserTest {

    /** rmgrlist.h 0-based 枚举序：RM_HEAP_ID=10（heap）。 */
    private static final int RM_HEAP_ID = 10;

    /** rmgrlist.h 0-based 枚举序：RM_XLOG_ID=0（XLOG，FPI 形态模拟用）。 */
    private static final int RM_XLOG_ID = 0;

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 用例 1：单 block（heap insert 形态）+ main data——块区/主区偏移自洽，不变量钉死。 */
    @Test
    void singleBlockWithMainDataParsesBlockRegionAndInvariant() {
        byte[] blockData = {(byte) 0xAA, 0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] mainData = new byte[20];
        Arrays.fill(mainData, (byte) 0x5A);
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 4242)
                .block(0, 1663L, 16385L, 24600L, 7)
                .data(blockData)
                .main(mainData)
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0x046F00000L, layout);
        assertEquals(1, r.blocks().size());
        BlockRef b = r.blocks().get(0);
        assertEquals(0, b.fork());
        assertEquals(7, b.blockNo());
        assertEquals(1663L, b.spc());
        assertEquals(16385L, b.db());
        assertEquals(24600L, b.relNode());
        assertTrue(b.hasData());
        assertEquals(blockData.length, b.dataLen());
        assertFalse(b.hasImage());
        assertEquals(-1, b.imageOff());
        // 块内 data 指回 raw 的载荷区且逐字节即手造值
        assertArrayEquals(blockData, Arrays.copyOfRange(raw, b.dataOff(), b.dataOff() + b.dataLen()));
        assertEquals(4242, r.xid());
        assertEquals(0, r.toplevelXid());
        assertEquals(4242L, r.effXid());
        assertEquals(RM_HEAP_ID, r.rmid());
        assertEquals(0x00, r.info());
        assertEquals(0x046F00000L, r.lsn());
        assertEquals(raw.length, r.totLen());
        // 不变量：main 区自头区/块载荷之后延展到 totLen 恰好收口（spike 发现 6）
        assertEquals(raw.length, r.mainOff() + r.mainLen());
        assertArrayEquals(mainData, Arrays.copyOfRange(raw, r.mainOff(), r.mainOff() + r.mainLen()));
    }

    /** 用例 2：仅 image 无 main data（XLOG_FPI 形态）——无终止头，终止判据=datatotal（spike 发现 2）。 */
    @Test
    void imageOnlyRecordHasNoTerminatorAndStopsOnDatatotal() {
        byte[] pageImage = new byte[300];
        Arrays.fill(pageImage, (byte) 0xC3);
        // HAS_HOLE 且未压缩：holeLen 不写头，parser 由 XLOG_BLCKSZ - imageLen 推导
        byte[] raw = WalBytes.record(RM_XLOG_ID, 0x10, 0)
                .block(0, 1663L, 16385L, 24600L, 0)
                .image(pageImage, 40, 0x01)
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0x10L, layout);
        assertEquals(0, r.mainLen());
        assertEquals(r.totLen(), r.mainOff());   // main 区退化为零长、恰贴 totLen
        BlockRef b = r.blocks().get(0);
        assertTrue(b.hasImage());
        assertEquals(pageImage.length, b.imageLen());
        assertEquals(40, b.holeOffset());
        assertEquals(8192 - pageImage.length, b.holeLen());   // 未压缩洞长 = 页大小 - 镜像长
        assertArrayEquals(pageImage, Arrays.copyOfRange(raw, b.imageOff(), b.imageOff() + b.imageLen()));
        assertFalse(b.hasData());
        assertEquals(-1, b.dataOff());
        assertEquals(0, r.xid());
        assertEquals(0L, r.effXid());            // 无事务记录 effXid 归零
    }

    /** 用例 3：TOPLEVEL_XID(252) 标记——toplevelXid 读出、effXid 归并到顶层（spike 发现 25）。 */
    @Test
    void toplevelXidMarkerIsReadAndEffXidMerges() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 777)   // 777=子事务自身 xid
                .toplevel(4242)
                .block(0, 1663L, 16385L, 24600L, 3)
                .data(new byte[4])
                .main(new byte[16])
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0, layout);
        assertEquals(4242, r.toplevelXid());
        assertEquals(777, r.xid());              // 记录头 xid 仍保留子事务号
        assertEquals(4242L, r.effXid());         // 事务归属归并到顶层
    }

    /** 用例 4：SAME_REL——第二 block 不带定位器字节，spc/db/relNode 复用前一 locator。 */
    @Test
    void sameRelSecondBlockReusesPreviousLocator() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 4242)
                .block(0, 1663L, 16385L, 24600L, 1)
                .data(new byte[] {1})
                .sameRel(0, 2)
                .data(new byte[] {2, 2})
                .main(new byte[8])
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0, layout);
        assertEquals(2, r.blocks().size());
        BlockRef b0 = r.blocks().get(0);
        BlockRef b1 = r.blocks().get(1);
        assertEquals(b0.spc(), b1.spc());
        assertEquals(b0.db(), b1.db());
        assertEquals(b0.relNode(), b1.relNode());
        assertEquals(2, b1.blockNo());
        assertEquals(raw.length, r.mainOff() + r.mainLen());
    }

    /** WalBytes 行为：main 终止头自动选档——≤255 用 DATA_SHORT（u8 长度），否则 DATA_LONG（u32）。 */
    @Test
    void mainAutoSelectsDataShortAndDataLongTerminator() {
        byte[] shortRaw = WalBytes.record(RM_HEAP_ID, 0x00, 1)
                .main(new byte[100])
                .build();
        WalRecord rs = WalRecordParser.parse(shortRaw, 0, layout);
        assertEquals(100, rs.mainLen());
        assertEquals((byte) 0xFF, shortRaw[rs.mainOff() - 2]);      // DATA_SHORT id
        assertEquals(100, shortRaw[rs.mainOff() - 1] & 0xFF);       // u8 载荷长度

        byte[] longRaw = WalBytes.record(RM_HEAP_ID, 0x00, 1)
                .main(new byte[300])
                .build();
        WalRecord rl = WalRecordParser.parse(longRaw, 0, layout);
        assertEquals(300, rl.mainLen());
        assertEquals((byte) 0xFE, longRaw[rl.mainOff() - 5]);       // DATA_LONG id
        assertEquals(300, readU32(longRaw, rl.mainOff() - 4));      // u32 载荷长度
    }

    /** WalBytes 行为：载荷区逐块 [image][data] 按块序（spike 发现 6），main data 殿后收尾。 */
    @Test
    void payloadRegionOrdersImagesThenDataPerBlockInIdOrder() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 9)
                .block(0, 1663L, 16385L, 24600L, 0)
                .image(new byte[10], 0, 0x00)
                .data(new byte[6])
                .sameRel(0, 1)
                .image(new byte[8], 0, 0x00)
                .data(new byte[4])
                .main(new byte[5])
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0, layout);
        BlockRef b0 = r.blocks().get(0);
        BlockRef b1 = r.blocks().get(1);
        assertTrue(b0.imageOff() < b0.dataOff(), "块0 镜像先于块0 data");
        assertTrue(b0.dataOff() < b1.imageOff(), "块0 data 先于块1 镜像");
        assertTrue(b1.imageOff() < b1.dataOff(), "块1 镜像先于块1 data");
        assertTrue(b1.dataOff() < r.mainOff(), "main data 殿后");
        assertEquals(raw.length, r.mainOff() + r.mainLen());
    }

    /** 页内记录 MAXALIGN 尾垫：raw 可长于 totLen，走读只认 totLen、不受尾部补齐干扰。 */
    @Test
    void trailingMaxalignPaddingBeyondTotLenIsTolerated() {
        byte[] built = WalBytes.record(RM_HEAP_ID, 0x00, 5)
                .block(0, 1663L, 16385L, 24600L, 4)
                .data(new byte[4])
                .main(new byte[8])
                .build();
        byte[] padded = Arrays.copyOf(built, built.length + 7);
        Arrays.fill(padded, built.length, padded.length, (byte) 0);
        WalRecord r = WalRecordParser.parse(padded, 0, layout);
        assertEquals(built.length, r.totLen());
        assertEquals(built.length, r.mainOff() + r.mainLen());
    }

    /** SAME_REL 前无任何完整定位器：走读 fail-fast 抛 ISE，消息点明根因。 */
    @Test
    void sameRelWithoutPreviousLocatorFails() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 1)
                .sameRel(0, 0)
                .data(new byte[4])
                .build();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> WalRecordParser.parse(raw, 0, layout));
        assertTrue(e.getMessage().contains("SAME_REL"));
    }

    /** WalBytes 行为：记录头字段落 XLogRecord 定偏移——totLen@0、xid@4、info@16、rmid@17（小端）。 */
    @Test
    void headerFieldsLandAtFixedRecordHeaderOffsets() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x50, 70000).build();
        assertEquals(raw.length, readU32(raw, 0));
        assertEquals(70000, readU32(raw, 4));       // 超 u16 的 xid 钉住 4 字节小端宽度
        assertEquals(0x50, raw[16] & 0xFF);
        assertEquals(RM_HEAP_ID, raw[17] & 0xFF);
    }

    /** 压缩镜像（HAS_HOLE+压缩位）：显式 holeLen u16 随镜像头写出并被读回。 */
    @Test
    void compressedImageCarriesExplicitHoleLen() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x80, 4242)
                .block(0, 1663L, 16385L, 24600L, 2)
                .image(new byte[100], 24, 0x01 | 0x04)   // HAS_HOLE | pglz 压缩
                .holeLen(4096)
                .build();
        BlockRef b = WalRecordParser.parse(raw, 0, layout).blocks().get(0);
        assertEquals(4096, b.holeLen());
        assertEquals(100, b.imageLen());
        assertEquals(24, b.holeOffset());
    }

    /** 未知 block id（协议未定义的中间值）：走读立即抛 ISE，消息携带 id 与偏移。 */
    @Test
    void unknownBlockIdFails() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 1)
                .block(0, 1663L, 16385L, 24600L, 0)
                .data(new byte[4])
                .build();
        raw[layout.recordHeaderSize()] = (byte) 200;   // 首个 block id 0 篡改为未知值
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> WalRecordParser.parse(raw, 0, layout));
        assertTrue(e.getMessage().contains("unknown block id"));
    }

    /** totLen 声称多于实际字节：mainOff+mainLen==totLen 不变量破坏，ISE 报三元组定位。 */
    @Test
    void totLenBeyondActualBytesFailsLayoutInvariant() {
        byte[] raw = WalBytes.record(RM_HEAP_ID, 0x00, 1)
                .main(new byte[8])
                .build();
        raw[0] += 1;   // totLen 低位 +1（34→35），实际字节不变
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> WalRecordParser.parse(raw, 0, layout));
        assertTrue(e.getMessage().contains("mainOff"));
    }

    /**
     * 就地读 little-endian u32（测试侧独立小端读——与 DSL/被测各写一份，互证端序）。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 32 位无符号值（int 载体）
     */
    private static int readU32(byte[] b, int offset) {
        return (b[offset] & 0xFF)
                | ((b[offset + 1] & 0xFF) << 8)
                | ((b[offset + 2] & 0xFF) << 16)
                | ((b[offset + 3] & 0xFF) << 24);
    }
}
