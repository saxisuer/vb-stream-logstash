package org.vastdata.vbstream.walsource.layout;

import java.util.Arrays;

/**
 * FPW（full-page write）页镜像重建与 ItemId line pointer 提取（纯函数零状态）。
 *
 * <p>PG 在页首次修改前（checkpoint 后首写）把整页镜像写进 WAL；带洞存储时镜像
 * 是"head + tail"两段拼接（页中 {@code pd_lower..pd_upper} 之间的洞被抠掉不写，
 * 重建时补零），无洞时即整页连续拷贝。重建出的页是<strong>变更后状态</strong>
 * （spike 发现 15 勘误：redo 对带镜像块走 restore 跳过 apply）——记录自身的
 * offnum 对应的新行已在页内，旧行/前像同样可回读。</p>
 *
 * <p>Task 10 重放引擎消费本类：先 {@link #rebuild(WalRecord, BlockRef)} 还原整页，
 * 再 {@link #linePointerOffset(byte[], int)} 按行号取元组页内偏移，交给
 * {@link TupleDecoder} 的页元组形态解码。压缩镜像（pglz/lz4/zstd）v1 不支持，
 * 一律 ISE fail-fast。</p>
 */
public final class PageImages {

    /** bimg_info：镜像有洞位（BKPIMAGE_HAS_HOLE，xlogrecord.h REL_18_STABLE）。 */
    private static final int BKPIMAGE_HAS_HOLE = 0x01;

    /** bimg_info：压缩算法位掩码（BKPIMAGE_COMPRESS_*：pglz|lz4|zstd = 0x04|0x08|0x10）。 */
    private static final int BKPIMAGE_COMPRESS_MASK = 0x04 | 0x08 | 0x10;

    /** 页头长（SizeOfPageHeaderData，bufpage.h）：ItemId 数组自页偏移 24 起。 */
    private static final int SIZE_OF_PAGE_HEADER_DATA = 24;

    /** 单个 ItemId（line pointer）占 4 字节。 */
    private static final int ITEMID_SIZE = 4;

    /** ItemId lp_flags 的正常态（LP_NORMAL，bufpage.h）——指向堆元组的可解引用指针。 */
    private static final int LP_NORMAL = 1;

    /**
     * 工具类防实例化。
     */
    private PageImages() {
    }

    /**
     * 重建 block 引用携带的 FPW 页镜像为完整页字节数组。
     *
     * <p>关键分支（spike {@code rebuildPage} 移植）：压缩位任一置位先抛 ISE
     * （v1 不带解压）；{@code HAS_HOLE} 时镜像存储形态为 head（页首..holeOffset）
     * + tail（holeOffset 起的剩余 imageLen-holeOffset 字节，落页尾
     * {@code holeOffset+holeLen} 起）——head 与 tail 各一次 arraycopy，洞区由
     * 新建数组的零初值自然补零，重建页长 = {@code imageLen + holeLen}（未压缩
     * 即页大小，因 parser 已按 {@code walBlockSize - imageLen} 推导洞长）；
     * 无 hole 时整段 {@code imageLen} 字节连续拷贝即整页。</p>
     *
     * <p>边界与异常语义：{@code b} 不带镜像（hasImage=false，imageOff=-1）前置
     * ISE，不让 -1 偏移裸越界；镜像越界（imageOff+imageLen 超 raw 长）由数组
     * 越界裸抛（前置约定 parser 已走读自洽）。线程约束：静态纯函数、零共享状态，
     * 任意线程并发安全。</p>
     *
     * @param r 镜像所属的走读完成记录（取 {@link WalRecord#raw()} 载荷）
     * @param b 目标 block 引用（imageOff/imageLen/holeOffset/holeLen/bimgInfo 已回填）
     * @return 重建的完整页（洞区为零字节）
     * @throws IllegalStateException 压缩镜像不支持、或块不带镜像
     */
    public static byte[] rebuild(WalRecord r, BlockRef b) {
        if (!b.hasImage()) {
            throw new IllegalStateException(String.format(
                    "block %d has no page image (hasImage=false)", b.blockNo()));
        }
        if ((b.bimgInfo() & BKPIMAGE_COMPRESS_MASK) != 0) {
            throw new IllegalStateException(String.format(
                    "compressed FPW image unsupported in v1 (bimg_info=0x%02x, block %d)",
                    b.bimgInfo(), b.blockNo()));
        }
        byte[] raw = r.raw();
        if ((b.bimgInfo() & BKPIMAGE_HAS_HOLE) != 0) {
            int headLen = b.holeOffset();
            int tailLen = b.imageLen() - headLen;
            byte[] page = new byte[b.imageLen() + b.holeLen()];
            System.arraycopy(raw, b.imageOff(), page, 0, headLen);
            System.arraycopy(raw, b.imageOff() + headLen, page, headLen + b.holeLen(), tailLen);
            return page;
        }
        return Arrays.copyOfRange(raw, b.imageOff(), b.imageOff() + b.imageLen());
    }

    /**
     * 从重建页提取行号（offnum，1-based）对应 ItemId 的元组页内偏移 lp_off。
     *
     * <p>ItemId 位段（spike 发现 4，bufpage.h ItemIdData）：u32 little-endian 中
     * {@code lp_off:15}（低位，元组页内偏移）、{@code lp_flags:2}（bit 15-16）、
     * {@code lp_len:15}（bit 17-31，元组长度）。ItemId 数组自页偏移
     * {@code SizeOfPageHeaderData=24} 起、每项 4 字节，offnum n 落位于
     * {@code 24 + (n-1)*4}。仅 {@code LP_NORMAL==1}（正常态，指向堆元组）可
     * 解引用——LP_UNUSED(0)/LP_DEAD/LP_REDIRECT 等其余取值抛 ISE（消息带
     * offnum/flags/itemId 三元组定位）。</p>
     *
     * <p>边界与异常语义：offnum &lt; 1 抛 ISE（行号是 1-based，0 是协议错用）；
     * offnum 超出页内 ItemId 数组范围由数组越界裸抛（调用方按 pd_lower 自律）。
     * 线程约束：静态纯函数、零共享状态，任意线程并发安全。</p>
     *
     * @param page   重建页字节（{@link #rebuild(WalRecord, BlockRef)} 产物）
     * @param offnum 行号（1-based，与 heap 记录载荷中的 offsets 同源）
     * @return 该行元组在页内的起始偏移（lp_off）
     * @throws IllegalStateException offnum 非法或 line pointer 非 LP_NORMAL
     */
    public static int linePointerOffset(byte[] page, int offnum) {
        if (offnum < 1) {
            throw new IllegalStateException("offnum must be 1-based, got " + offnum);
        }
        int itemId = u32(page, SIZE_OF_PAGE_HEADER_DATA + (offnum - 1) * ITEMID_SIZE);
        int flags = (itemId >> 15) & 0x3;
        if (flags != LP_NORMAL) {
            throw new IllegalStateException(String.format(
                    "line pointer %d not LP_NORMAL (flags=%d, itemId=0x%08x)", offnum, flags, itemId));
        }
        return itemId & 0x7FFF;
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b      源数组（长度须覆盖 offset+4）
     * @param offset 起始偏移
     * @return 32 位值（int 载体，位段提取仅用移位与掩码、不涉符号语义）
     */
    private static int u32(byte[] b, int offset) {
        return (b[offset] & 0xFF)
                | ((b[offset + 1] & 0xFF) << 8)
                | ((b[offset + 2] & 0xFF) << 16)
                | ((b[offset + 3] & 0xFF) << 24);
    }
}
