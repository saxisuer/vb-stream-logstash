package org.vastdata.vbstream.walsource.layout;

import java.util.ArrayList;
import java.util.List;

/**
 * WAL 单条记录的走读解析器（记录头 + block 头链 + main data 定位，纯函数零状态）。
 *
 * <p>移植自 spike {@code WalParseSpike.parseRecord}，硬编码尺寸改为 {@link WalLayout}
 * 注入（记录头长作头区起点、页大小作未压缩洞长推导）；block id / fork_flags 位等
 * 协议稳定常量自持。两阶段走读：</p>
 * <ol>
 *   <li>头区走读：自 {@code recordHeaderSize()} 起，逐 id 推进——block 头
 *   （id ≤ 32：id u8 / fork_flags u8 / data_length u16 内嵌 [+ 镜像头 5B，压缩且
 *   有洞再补 holeLen u16] [!SAME_REL 定位器 spc/db/relNode 三 u32] / blockno u32）、
 *   253 origin（u32 跳过不存）、252 TOPLEVEL_XID（u32 读出，spike 发现 25）、
 *   255/254 main 终止头（u8/u32 长度）。终止判据双通道：撞终止头 break，或
 *   剩余字节数恰等于已声明载荷合计 datatotal——无 main data 的记录没有终止头
 *   （spike 发现 2）。</li>
 *   <li>载荷区回填：自头区终点起按块 id 序逐块排布 [image][data]（spike 发现 6），
 *   回填 imageOff/dataOff 绝对偏移（无对应载荷为 -1），main data 殿后。</li>
 * </ol>
 *
 * <p>不变量收口 {@code mainOff + mainLen == totLen}：不等即布局错配（版本漂移或
 * 字节损坏），ISE fail-fast 并报 mainOff/mainLen/totLen/rmid/info 五元组定位。
 * raw 允许长于 totLen（页内记录 MAXALIGN 尾垫），走读只认 totLen。</p>
 */
public final class WalRecordParser {

    /** fork_flags：携带页镜像位（BKPBLOCK_HAS_IMAGE，xlogrecord.h）。 */
    private static final int BKPBLOCK_HAS_IMAGE = 0x10;

    /** fork_flags：携带块内 data 位（BKPBLOCK_HAS_DATA）。 */
    private static final int BKPBLOCK_HAS_DATA = 0x20;

    /** fork_flags：定位器同前块位（BKPBLOCK_SAME_REL）——置位时省 12B 定位器。 */
    private static final int BKPBLOCK_SAME_REL = 0x80;

    /** bimg_info：镜像有洞位（BKPIMAGE_HAS_HOLE）。 */
    private static final int BKPIMAGE_HAS_HOLE = 0x01;

    /** bimg_info：压缩算法位掩码（pglz|lz4|zstd）——有效时洞长以显式 u16 存储。 */
    private static final int BKPIMAGE_COMPRESS_MASK = 0x04 | 0x08 | 0x10;

    /** main data 短终止头 id（XLR_BLOCK_ID_DATA_SHORT，u8 长度跟随）。 */
    private static final int XLR_BLOCK_ID_DATA_SHORT = 255;

    /** main data 长终止头 id（XLR_BLOCK_ID_DATA_LONG，u32 长度跟随）。 */
    private static final int XLR_BLOCK_ID_DATA_LONG = 254;

    /** origin 标记 id（XLR_BLOCK_ID_ORIGIN，u32 origin id 跟随——跳过不存）。 */
    private static final int XLR_BLOCK_ID_ORIGIN = 253;

    /** 顶层事务标记 id（XLR_BLOCK_ID_TOPLEVEL_XID，u32 xid 跟随，spike 发现 25）。 */
    private static final int XLR_BLOCK_ID_TOPLEVEL_XID = 252;

    /** block 头 id 上界（XLR_MAX_BLOCK_ID=32；之上是 252-255 的标记/终止头区段）。 */
    private static final int MAX_BLOCK_ID = 32;

    /** XLogRecord 定偏移：totLen u32@0（REL_18 及全部现代版本同位）。 */
    private static final int TOTLEN_OFFSET = 0;

    /** XLogRecord 定偏移：xid u32@4。 */
    private static final int XID_OFFSET = 4;

    /** XLogRecord 定偏移：info u8@16（xl_prev 8B 之后）。 */
    private static final int INFO_OFFSET = 16;

    /** XLogRecord 定偏移：rmid u8@17。 */
    private static final int RMID_OFFSET = 17;

    /**
     * 工具类防实例化。
     */
    private WalRecordParser() {
    }

    /**
     * 走读一条完整 WAL 记录。
     *
     * <p>关键分支：头区循环按 id 分派（block 头内嵌 data_length、可选镜像头与
     * SAME_REL 省定位器、252/253 标记跳读、255/254 终止头定 mainLen）；终止判据
     * 双通道（终止头 break / 剩余==datatotal break，spike 发现 2）；载荷区按块序
     * [image][data] 回填偏移后以 {@code mainOff+mainLen==totLen} 不变量收口。</p>
     *
     * <p>边界与异常语义：前置约定 {@code rec.length >= totLen}（不足时数组越界裸抛）；
     * 未知 block id、SAME_REL 无前定位器、不变量破坏均抛 ISE（消息带定位上下文）。
     * 线程约束：静态纯函数、零共享状态，任意线程并发安全。</p>
     *
     * @param rec    记录字节（含 24B 头；尾部允许 MAXALIGN 垫字节）
     * @param lsn    记录起始 LSN（透传进结果，供游标/确认位关联）
     * @param layout 版本布局描述符（注入记录头长与页大小）
     * @return 走读结果（blocks 已不可变化、载荷偏移已回填）
     * @throws IllegalStateException 布局错配或协议违约（见上）
     */
    public static WalRecord parse(byte[] rec, long lsn, WalLayout layout) {
        int totLen = u32(rec, TOTLEN_OFFSET);
        int pos = layout.recordHeaderSize();
        List<BlockRef> blocks = new ArrayList<>();
        long[] lastLoc = null;   // 最近一次完整定位器 (spc, db, relNode)——SAME_REL 复用源
        int mainLen = 0;
        int toplevelXid = 0;
        int datatotal = 0;       // 头区已声明的 image+data 载荷字节合计
        while (pos < totLen) {
            // 头区终点判据之一：剩余字节恰为已声明载荷——无 main data 的记录
            // 没有终止头（spike 发现 2，如 XLOG_FPI）
            if (totLen - pos == datatotal) {
                break;
            }
            int id = rec[pos] & 0xFF;
            if (id == XLR_BLOCK_ID_DATA_SHORT) {
                mainLen = rec[pos + 1] & 0xFF;
                pos += 2;
                break;
            } else if (id == XLR_BLOCK_ID_DATA_LONG) {
                mainLen = u32(rec, pos + 1);
                pos += 5;
                break;
            } else if (id == XLR_BLOCK_ID_ORIGIN) {
                pos += 5;   // id u8 + origin u32，当前任务不消费
                continue;
            } else if (id == XLR_BLOCK_ID_TOPLEVEL_XID) {
                // 子事务记录在此携带顶层 xid（spike 发现 25）——effXid 归并依据
                toplevelXid = u32(rec, pos + 1);
                pos += 5;
                continue;
            } else if (id <= MAX_BLOCK_ID) {
                int forkFlags = rec[pos + 1] & 0xFF;
                int dataLen = u16(rec, pos + 2);
                pos += 4;
                boolean hasImage = (forkFlags & BKPBLOCK_HAS_IMAGE) != 0;
                boolean hasData = (forkFlags & BKPBLOCK_HAS_DATA) != 0;
                int imageLen = 0;
                int bimgInfo = 0;
                int holeOffset = 0;
                int holeLen = 0;
                if (hasImage) {
                    imageLen = u16(rec, pos);
                    holeOffset = u16(rec, pos + 2);
                    bimgInfo = rec[pos + 4] & 0xFF;
                    pos += 5;
                    boolean compressed = (bimgInfo & BKPIMAGE_COMPRESS_MASK) != 0;
                    if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0 && compressed) {
                        holeLen = u16(rec, pos);   // 压缩镜像的洞长不可推导，显式 u16
                        pos += 2;
                    } else if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0) {
                        // 未压缩时镜像存的就是去洞后的长度，洞长可由页大小反推
                        holeLen = layout.walBlockSize() - imageLen;
                    }
                }
                long spc;
                long db;
                long relNode;
                if ((forkFlags & BKPBLOCK_SAME_REL) == 0) {
                    spc = u32(rec, pos) & 0xFFFFFFFFL;
                    db = u32(rec, pos + 4) & 0xFFFFFFFFL;
                    relNode = u32(rec, pos + 8) & 0xFFFFFFFFL;
                    pos += 12;
                    lastLoc = new long[] {spc, db, relNode};
                } else {
                    if (lastLoc == null) {
                        throw new IllegalStateException(
                                "SAME_REL with no previous locator at offset " + pos);
                    }
                    spc = lastLoc[0];
                    db = lastLoc[1];
                    relNode = lastLoc[2];
                }
                int blockNo = u32(rec, pos);
                pos += 4;
                datatotal += (hasImage ? imageLen : 0) + dataLen;
                // 载荷偏移暂 0，载荷区阶段统一回填
                blocks.add(new BlockRef(forkFlags & 0x0F, blockNo, spc, db, relNode,
                        hasImage, 0, imageLen, bimgInfo, holeOffset, holeLen,
                        hasData, 0, dataLen));
            } else {
                throw new IllegalStateException(
                        "unknown block id " + id + " at offset " + pos);
            }
        }
        // 载荷区阶段：按块 id 序逐块 [image][data] 排布并回填绝对偏移，main data 殿后
        int cur = pos;
        List<BlockRef> patched = new ArrayList<>(blocks.size());
        for (BlockRef b : blocks) {
            int imageOff = b.hasImage() ? cur : -1;
            cur += b.hasImage() ? b.imageLen() : 0;
            int dataOff = b.hasData() ? cur : -1;
            cur += b.dataLen();
            patched.add(new BlockRef(b.fork(), b.blockNo(), b.spc(), b.db(), b.relNode(),
                    b.hasImage(), imageOff, b.imageLen(), b.bimgInfo(), b.holeOffset(), b.holeLen(),
                    b.hasData(), dataOff, b.dataLen()));
        }
        int mainOff = cur;
        if (mainOff + mainLen != totLen) {
            throw new IllegalStateException(String.format(
                    "layout mismatch: mainOff=%d mainLen=%d totLen=%d rmid=%d info=%x",
                    mainOff, mainLen, totLen, rec[RMID_OFFSET] & 0xFF, rec[INFO_OFFSET] & 0xFF));
        }
        return new WalRecord(lsn, totLen, u32(rec, XID_OFFSET), toplevelXid,
                rec[RMID_OFFSET] & 0xFF, rec[INFO_OFFSET] & 0xFF, patched, mainOff, mainLen, rec);
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b      源数组（长度须覆盖 offset+2）
     * @param offset 起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b      源数组（长度须覆盖 offset+4）
     * @param offset 起始偏移
     * @return 32 位值（int 载体，调用方按需掩码转无符号 long）
     */
    private static int u32(byte[] b, int offset) {
        return (b[offset] & 0xFF)
                | ((b[offset + 1] & 0xFF) << 8)
                | ((b[offset + 2] & 0xFF) << 16)
                | ((b[offset + 3] & 0xFF) << 24);
    }
}
