package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PageImages（FPW 页镜像重建 + ItemId line pointer 提取）的失败先行测试。
 *
 * <p>断言面来自任务书 Step 1：带 hole 镜像（head 72B + tail 16B，holeLen=8192-88）
 * 重建后 {@code pd_lower@12}/{@code pd_upper@14} 可读；自造 ItemId {@code 0x00809d00}
 * 断言 {@code lp_off=7424}（经公开行为 {@code linePointerOffset} 返回值验收）；
 * 非 LP_NORMAL（flags 0/2）抛 ISE。另覆盖无 hole 整页拷贝、压缩镜像 ISE、
 * 无镜像块 ISE 三个边界。</p>
 *
 * <p>镜像字节经 {@link WalBytes} DSL 造记录后由 {@link WalRecordParser} 走读回填
 * imageOff——rebuild 消费的是 WalRecord+BlockRef，测试只拼"head+tail 拼好的
 * bytes"（imgLen=bytes.length，即 72+16=88），hole 信息走镜像头参数
 * （holeOffset=72、bimg_info=HAS_HOLE），rebuild 按 HAS_HOLE 拆两段回填。</p>
 */
class PageImagesTest {

    /** rmgrlist.h 0-based 枚举序：RM_XLOG_ID=0（XLOG，FPI 形态模拟用）。 */
    private static final int RM_XLOG_ID = 0;

    /** 标准页大小（XLOG_BLCKSZ / BLCKSZ，与 WalLayoutV18 一致）。 */
    private static final int PAGE_SIZE = 8192;

    /** 页头长（SizeOfPageHeaderData）：pd_lower@12/pd_upper@14、ItemId 数组自 24 起。 */
    private static final int PAGE_HEADER_SIZE = 24;

    /** 测试镜像头段长：页头 24B + 12 个 ItemId（4B 每个）= 72B，即 holeOffset。 */
    private static final int HEAD_LEN = 72;

    /** 测试镜像尾段长：洞后残余页尾（imgLen = 72+16 = 88，holeLen = 8192-88）。 */
    private static final int TAIL_LEN = 16;

    /** bimg_info：镜像有洞位（BKPIMAGE_HAS_HOLE，xlogrecord.h——测试侧独立转录）。 */
    private static final int BKPIMAGE_HAS_HOLE = 0x01;

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /**
     * 用例 1：带 hole 镜像重建——head 落页首、tail 落页尾、洞区全零、页头字段可读。
     *
     * <p>head 72B 内含真实页头位形（pd_lower=80@12、pd_upper=8160@14），重建后
     * 两字段按定偏移读出即证明 head 段落位页首；tail 的 16B 显式字节与洞区
     * （72..8176）全零一并钉死拼接几何（发现 15：镜像即变更后整页）。</p>
     */
    @Test
    void holeImageRebuildsFullPageWithHeaderTailAndZeroedHole() {
        byte[] page = rebuildFirstBlock(splicedHoleImage(), HEAD_LEN, BKPIMAGE_HAS_HOLE, 0);
        assertEquals(PAGE_SIZE, page.length);
        assertEquals(80, u16(page, 12));        // pd_lower 仍在页头定偏移 12 可读
        assertEquals(8160, u16(page, 14));      // pd_upper 仍在页头定偏移 14 可读
        for (int i = HEAD_LEN; i < PAGE_SIZE - TAIL_LEN; i++) {
            assertEquals(0, page[i], "hole byte at " + i + " must stay zero");
        }
        assertArrayEquals(tailPattern(), Arrays.copyOfRange(page, PAGE_SIZE - TAIL_LEN, PAGE_SIZE));
    }

    /**
     * 用例 2：linePointerOffset 对 LP_NORMAL ItemId 返回 lp_off——自造
     * {@code 0x00809d00}（lp_off=7424、lp_len=64、flags=1）经重建后的页提取，
     * offnum 1 命中页偏移 24 的 ItemId（发现 4：位段序 lp_off:15/lp_flags:2@15/lp_len:15@17）。
     */
    @Test
    void linePointerOffsetExtractsLpOffFromNormalItemId() {
        byte[] page = rebuildFirstBlock(splicedHoleImage(), HEAD_LEN, BKPIMAGE_HAS_HOLE, 0);
        assertEquals(7424, PageImages.linePointerOffset(page, 1));
    }

    /**
     * 用例 3：非 LP_NORMAL line pointer 抛 ISE——offnum 2 是 flags=2
     * （LP_REDIRECT）、offnum 3 是 flags=0（LP_UNUSED），两者都不是可解引用的
     * 元组指针（LP_NORMAL==1，发现 4）。
     */
    @Test
    void nonNormalLinePointerThrowsIllegalState() {
        byte[] page = rebuildFirstBlock(splicedHoleImage(), HEAD_LEN, BKPIMAGE_HAS_HOLE, 0);
        assertThrows(IllegalStateException.class, () -> PageImages.linePointerOffset(page, 2));
        assertThrows(IllegalStateException.class, () -> PageImages.linePointerOffset(page, 3));
    }

    /** 用例 4：无 hole 镜像（bimg_info=0）——整段拷贝即整页，逐字节与镜像相等。 */
    @Test
    void noHoleImageRebuildsAsWholePageCopy() {
        byte[] image = new byte[PAGE_SIZE];
        image[12] = 0x50;                           // pd_lower 低字节，可读性自证
        put32(image, PAGE_HEADER_SIZE, 0x00809d00); // offnum 1：LP_NORMAL lp_off 7424
        byte[] page = rebuildFirstBlock(image, 0, 0x00, 0);
        assertArrayEquals(image, page);
        assertEquals(7424, PageImages.linePointerOffset(page, 1));
    }

    /** 用例 5：压缩镜像（pglz 0x04 / zstd 0x10 置位）——rebuild 一律抛 ISE（v1 不支持解压）。 */
    @Test
    void compressedImageThrowsIllegalState() {
        assertThrows(IllegalStateException.class,
                () -> rebuildFirstBlock(splicedHoleImage(), HEAD_LEN, BKPIMAGE_HAS_HOLE | 0x04, 8104));
        assertThrows(IllegalStateException.class,
                () -> rebuildFirstBlock(new byte[100], 0, 0x10, 0));
    }

    /** 用例 6：无镜像块（仅 data）——rebuild 前置校验抛 ISE，不让 imageOff=-1 裸越界。 */
    @Test
    void blockWithoutImageThrowsIllegalState() {
        byte[] raw = WalBytes.record(RM_XLOG_ID, 0x00, 0)
                .block(0, 1663L, 16385L, 24600L, 0)
                .data(new byte[8])
                .build();
        WalRecord r = WalRecordParser.parse(raw, 0, layout);
        BlockRef b = r.blocks().get(0);
        assertThrows(IllegalStateException.class, () -> PageImages.rebuild(r, b));
    }

    // ---- 测试基建 --------------------------------------------------------------

    /**
     * 造记录走读后对第一个 block 引用执行 rebuild——本测试类全部镜像记录单 block，
     * 统一走此入口把"造记录 → rebuild"样板折叠。
     *
     * @param image      拼好的镜像字节
     * @param holeOffset 镜像头洞偏移
     * @param bimgInfo   镜像标志字节
     * @param holeLen    显式洞长（未压缩传 0）
     * @return 重建的完整页
     */
    private byte[] rebuildFirstBlock(byte[] image, int holeOffset, int bimgInfo, int holeLen) {
        WalRecord r = parseImageRecord(image, holeOffset, bimgInfo, holeLen);
        return PageImages.rebuild(r, r.blocks().get(0));
    }

    /**
     * 造一条单 block + 页镜像的记录并走读：镜像字节即"head+tail 拼好的 bytes"
     * （imgLen=bytes.length），holeOffset/holeLen 经镜像头参数与显式洞长进入。
     *
     * @param image      拼好的镜像字节（无 hole 存储形态）
     * @param holeOffset 镜像头洞偏移
     * @param bimgInfo   镜像标志字节
     * @param holeLen    显式洞长（仅压缩且 HAS_HOLE 时写头；未压缩传 0 走页大小推导）
     * @return 走读完成的记录（imageOff 已回填）
     */
    private WalRecord parseImageRecord(byte[] image, int holeOffset, int bimgInfo, int holeLen) {
        WalBytes builder = WalBytes.record(RM_XLOG_ID, 0x10, 0)
                .block(0, 1663L, 16385L, 24600L, 0)
                .image(image, holeOffset, bimgInfo);
        if (holeLen != 0) {
            builder.holeLen(holeLen);
        }
        return WalRecordParser.parse(builder.build(), 0, layout);
    }

    /**
     * 带 hole 镜像的存储形态：head 72B + tail 16B 拼好的 88B 字节流
     * （imgLen=88，holeLen 由 parser 按 8192-88 推导为 8104）。
     *
     * @return 88B 镜像字节
     */
    private byte[] splicedHoleImage() {
        byte[] image = new byte[HEAD_LEN + TAIL_LEN];
        byte[] head = headWithHeaderAndItemIds();
        System.arraycopy(head, 0, image, 0, HEAD_LEN);
        System.arraycopy(tailPattern(), 0, image, HEAD_LEN, TAIL_LEN);
        return image;
    }

    /**
     * 自造 72B 镜像头段：真实页头位形（pd_lsn 8B 零、pd_lower=80@12、
     * pd_upper=8160@14、pd_special@16、pd_pagesize_version@18）+ 12 个 ItemId
     * （offnum 1 = 0x00809d00 LP_NORMAL lp_off 7424、offnum 2 = 0x00011234
     * LP_REDIRECT、offnum 3 = 0x00005678 LP_UNUSED、其余 LP_NORMAL 填充）。
     *
     * @return 72B 头段字节
     */
    private byte[] headWithHeaderAndItemIds() {
        byte[] head = new byte[HEAD_LEN];
        put16(head, 12, 80);                    // pd_lower
        put16(head, 14, 8160);                  // pd_upper
        put16(head, 16, 8192);                  // pd_special
        put16(head, 18, 8192 | 4);              // pd_pagesize_version（8192 | layout版本 4）
        put32(head, PAGE_HEADER_SIZE, 0x00809d00);      // offnum 1：lp_off 7424 / flags 1 / lp_len 64
        put32(head, PAGE_HEADER_SIZE + 4, 0x00011234);  // offnum 2：flags=2 LP_REDIRECT
        put32(head, PAGE_HEADER_SIZE + 8, 0x00005678);  // offnum 3：flags=0 LP_UNUSED
        for (int i = 3; i < 12; i++) {
            put32(head, PAGE_HEADER_SIZE + i * 4, 0x00809d00);
        }
        return head;
    }

    /**
     * 16B 尾段的可识别填充（与洞区零字节、头段区分）。
     *
     * @return 16B 字节，逐位 0xA5 ^ 下标
     */
    private byte[] tailPattern() {
        byte[] tail = new byte[TAIL_LEN];
        for (int i = 0; i < TAIL_LEN; i++) {
            tail[i] = (byte) (0xA5 ^ i);
        }
        return tail;
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8);
    }

    /**
     * 就地写 little-endian u16。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      16 位值
     */
    private static void put16(byte[] b, int offset, int v) {
        b[offset] = (byte) (v & 0xFF);
        b[offset + 1] = (byte) ((v >>> 8) & 0xFF);
    }

    /**
     * 就地写 little-endian u32。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      32 位值
     */
    private static void put32(byte[] b, int offset, int v) {
        b[offset] = (byte) (v & 0xFF);
        b[offset + 1] = (byte) ((v >>> 8) & 0xFF);
        b[offset + 2] = (byte) ((v >>> 16) & 0xFF);
        b[offset + 3] = (byte) ((v >>> 24) & 0xFF);
    }
}
