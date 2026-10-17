package org.vastdata.vbstream.walsource.layout;

import java.util.ArrayList;
import java.util.List;

/**
 * WAL 单条记录的走读解析器（记录头 + block 头链 + main data 定位，纯函数零状态）。
 *
 * <p>移植自 spike {@code WalParseSpike.parseRecord}，硬编码尺寸改为 {@link WalLayout}
 * 注入：记录头长作头区起点、头字段偏移（xid/info/rmid）、定位器伴随区宽度、blkno 后
 * 尾随宽度与镜像头形制均经 layout 取值——openGauss 档（记录头 32B、xid@8/info@24/
 * rmid@25、定位器 12B+extra_flag u16、blkno 后 last_lsn u64、镜像头 4B 仅洞字段、
 * 块 id 扩展位 0x80/0x40 fail-fast）与 V17/V18 现代档（24B 头、12B 定位器、无尾随、
 * 5B 镜像头带 bimg_info）共用同一走读骨架，V17/V18 路径取值与原内联常量逐字节相等。
 * block id / fork_flags 位等协议稳定常量自持。两阶段走读：</p>
 * <ol>
 *   <li>头区走读：自 {@code recordHeaderSize()} 起，逐 id 推进——block 头
 *   （id ≤ 32：id u8 / fork_flags u8 / data_length u16 内嵌 [+ 镜像头——形制经
 *   {@code imageHeaderIsHoleOnly()} 分派] [!SAME_REL 定位器伴随区，宽
 *   {@code relNodeEncodedSize()}] / blockno u32 [+ 尾随 last_lsn，宽
 *   {@code blockRefLastLsnBytes()}]）、253 origin（跳过不存）、252 TOPLEVEL_XID
 *   （u32 读出，spike 发现 25）、255/254 main 终止头（u8/u32 长度）。
 *   {@code rejectBlockIdExtensionBits()} 为真时 id 高位（0x80 bucket/段页、
 *   0x40 TDE）先于块分派 fail-fast（openGauss 扩展记录形态，防静默错位）。
 *   终止判据双通道：撞终止头 break，或剩余字节数恰等于已声明载荷合计
 *   datatotal——无 main data 的记录没有终止头（spike 发现 2）。</li>
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

    /** fork_flags：定位器同前块位（BKPBLOCK_SAME_REL）——置位时省定位器伴随区。 */
    private static final int BKPBLOCK_SAME_REL = 0x80;

    /** 块 id 高位扩展位掩码（openGauss xlog_basic.h：0x80=BKID_HAS_BUCKET_OR_SEGPAGE、
     *  0x40=BKID_HAS_TDE_PAGE）——V17/V18 的 id ≤ 32 天然不命中，仅 OG 档校验。 */
    private static final int BLOCK_ID_EXTENSION_MASK = 0xC0;

    /** bimg_info：镜像有洞位（BKPIMAGE_HAS_HOLE）。 */
    private static final int BKPIMAGE_HAS_HOLE = 0x01;

    /** bimg_info：压缩算法位掩码（pglz|lz4|zstd）——有效时洞长以显式 u16 存储。 */
    private static final int BKPIMAGE_COMPRESS_MASK = 0x04 | 0x08 | 0x10;

    /** main data 短终止头 id（XLR_BLOCK_ID_DATA_SHORT，u8 长度跟随）。 */
    private static final int XLR_BLOCK_ID_DATA_SHORT = 255;

    /** main data 长终止头 id（XLR_BLOCK_ID_DATA_LONG，u32 长度跟随）。 */
    private static final int XLR_BLOCK_ID_DATA_LONG = 254;

    /** origin 标记 id（XLR_BLOCK_ID_ORIGIN，跟随 id 的会话/origin 域跳过不存）。 */
    private static final int XLR_BLOCK_ID_ORIGIN = 253;

    /** 顶层事务标记 id（XLR_BLOCK_ID_TOPLEVEL_XID，u32 xid 跟随，spike 发现 25）。 */
    private static final int XLR_BLOCK_ID_TOPLEVEL_XID = 252;

    /** block 头 id 上界（XLR_MAX_BLOCK_ID=32；之上是 252-255 的标记/终止头区段）。 */
    private static final int MAX_BLOCK_ID = 32;

    /** XLogRecord 定偏移：totLen u32@0（REL_18 与 openGauss 同位）。 */
    private static final int TOTLEN_OFFSET = 0;

    /**
     * 工具类防实例化。
     */
    private WalRecordParser() {
    }

    /**
     * 走读一条完整 WAL 记录（记录独占一个数组、起点即下标 0 的便捷形态）。
     *
     * <p>实现为 {@link #parse(byte[], int, long, WalLayout)} 四参偏移重载的
     * off=0 委托——坐标系语义、走读分支与异常面完全同源（详见四参版 javadoc）；
     * recOff=0 时返回结果的 mainOff/imageOff/dataOff 即 rec 内偏移、raw 即 rec，
     * 与历史行为逐字节一致。</p>
     *
     * @param rec    记录字节（含记录头——V17/V18 24B / OG 32B；记录可短于数组
     *               长度——尾部允许 MAXALIGN 垫字节，走读只认 totLen）
     * @param lsn    记录起始 LSN（透传进结果，供游标/确认位关联；异常消息引用）
     * @param layout 版本布局描述符（注入头偏移/定位器几何/镜像头形制/页大小）
     * @return 走读结果（blocks 已不可变化、载荷偏移已回填）
     * @throws IllegalStateException 布局错配或协议违约（见四参版）
     */
    public static WalRecord parse(byte[] rec, long lsn, WalLayout layout) {
        return parse(rec, 0, lsn, layout);
    }

    /**
     * 走读一条完整 WAL 记录——记录位于宿主缓冲 {@code buf} 的 {@code recOff} 偏移处。
     *
     * <p>关键分支与三参版完全同构（头区循环按 id 分派、OG 扩展位 fail-fast、终止判据
     * 双通道、[image][data] 载荷回填、不变量收口），区别仅在坐标系：头区与载荷区的
     * 全部读点以 {@code recOff} 为基准换算，返回结果中的 {@code mainOff} 与各
     * BlockRef 的 {@code imageOff()/dataOff()} 均为 **buf 内绝对偏移**、{@code raw()}
     * 即 buf 本身——消费方按记录自带偏移直接索引宿主缓冲，免缝合拷贝（walker 零拷贝
     * 优化引入）。</p>
     *
     * <p>边界与异常语义：前置约定 {@code buf.length >= recOff + totLen}（不足时数组
     * 越界裸抛）；未知 block id、扩展位命中（OG）、SAME_REL 无前定位器、不变量破坏
     * 均抛 ISE（消息带定位上下文，offset 字段为 buf 内绝对偏移）。线程约束：静态
     * 纯函数、零共享状态，任意线程并发安全。</p>
     *
     * @param buf    宿主缓冲（记录可位于任意偏移；调用方持有、约定只读）
     * @param recOff 记录头在 buf 中的起始偏移（非负；记录字节即
     *               {@code buf[recOff .. recOff + totLen)}）
     * @param lsn    记录起始 LSN（透传进结果，供游标/确认位关联；异常消息引用）
     * @param layout 版本布局描述符（注入头偏移/定位器几何/镜像头形制/页大小）
     * @return 走读结果（blocks 已不可变化、载荷偏移已回填为 buf 内绝对偏移、raw 即 buf）
     * @throws IllegalStateException 布局错配或协议违约（见上）
     */
    public static WalRecord parse(byte[] buf, int recOff, long lsn, WalLayout layout) {
        int base = recOff;
        int totLen = u32(buf, base + TOTLEN_OFFSET);
        int xidOff = base + layout.recordXidOffset();
        int infoOff = base + layout.recordInfoOffset();
        int rmidOff = base + layout.recordRmidOffset();
        int relSize = layout.relNodeEncodedSize();
        int lastLsnBytes = layout.blockRefLastLsnBytes();
        int pos = base + layout.recordHeaderSize();
        List<BlockRef> blocks = new ArrayList<>();
        long[] lastLoc = null;   // 最近一次完整定位器 (spc, db, relNode)——SAME_REL 复用源
        int mainLen = 0;
        int toplevelXid = 0;
        int datatotal = 0;       // 头区已声明的 image+data 载荷字节合计
        while (pos < base + totLen) {
            // 头区终点判据之一：剩余字节恰为已声明载荷——无 main data 的记录
            // 没有终止头（spike 发现 2，如 XLOG_FPI）
            if (base + totLen - pos == datatotal) {
                break;
            }
            int id = buf[pos] & 0xFF;
            if (id == XLR_BLOCK_ID_DATA_SHORT) {
                mainLen = buf[pos + 1] & 0xFF;
                pos += 2;
                break;
            } else if (id == XLR_BLOCK_ID_DATA_LONG) {
                mainLen = u32(buf, pos + 1);
                pos += 5;
                break;
            } else if (id == XLR_BLOCK_ID_ORIGIN) {
                pos += 5;   // id u8 + 跟随域，当前任务不消费
                continue;
            } else if (id == XLR_BLOCK_ID_TOPLEVEL_XID) {
                // 子事务记录在此携带顶层 xid（spike 发现 25）——effXid 归并依据
                toplevelXid = u32(buf, pos + 1);
                pos += 5;
                continue;
            } else if (layout.rejectBlockIdExtensionBits()
                    && (id & BLOCK_ID_EXTENSION_MASK) != 0) {
                // openGauss 扩展块形态（bucket/段页 0x80、TDE 0x40——定位器/块头
                // 结构随位变化）未支持前 fail-fast，防按普通块头走读静默错位
                throw new IllegalStateException(String.format(
                        "block id 0x%02X carries unsupported extension bits "
                                + "(0x80=bucket/segpage, 0x40=TDE page) at lsn=%d offset=%d",
                        id, lsn, pos));
            } else if (id <= MAX_BLOCK_ID) {
                int forkFlags = buf[pos + 1] & 0xFF;
                int dataLen = u16(buf, pos + 2);
                pos += 4;
                boolean hasImage = (forkFlags & BKPBLOCK_HAS_IMAGE) != 0;
                boolean hasData = (forkFlags & BKPBLOCK_HAS_DATA) != 0;
                int imageLen = 0;
                int bimgInfo = 0;
                int holeOffset = 0;
                int holeLen = 0;
                if (hasImage) {
                    if (layout.imageHeaderIsHoleOnly()) {
                        // openGauss 形制：镜像头仅 hole_offset/hole_length 两 u16，
                        // 无 bimg_info 与压缩面——镜像恒为整页去洞
                        holeOffset = u16(buf, pos);
                        holeLen = u16(buf, pos + 2);
                        pos += 4;
                        imageLen = layout.walBlockSize() - holeLen;
                    } else {
                        imageLen = u16(buf, pos);
                        holeOffset = u16(buf, pos + 2);
                        bimgInfo = buf[pos + 4] & 0xFF;
                        pos += 5;
                        boolean compressed = (bimgInfo & BKPIMAGE_COMPRESS_MASK) != 0;
                        if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0 && compressed) {
                            holeLen = u16(buf, pos);   // 压缩镜像的洞长不可推导，显式 u16
                            pos += 2;
                        } else if ((bimgInfo & BKPIMAGE_HAS_HOLE) != 0) {
                            // 未压缩时镜像存的就是去洞后的长度，洞长可由页大小反推
                            holeLen = layout.walBlockSize() - imageLen;
                        }
                    }
                }
                long spc;
                long db;
                long relNode;
                if ((forkFlags & BKPBLOCK_SAME_REL) == 0) {
                    spc = u32(buf, pos) & 0xFFFFFFFFL;
                    db = u32(buf, pos + 4) & 0xFFFFFFFFL;
                    relNode = u32(buf, pos + 8) & 0xFFFFFFFFL;
                    // 定位器伴随区整体跳过：V17/V18 止于 12B 三 u32；OG 再含
                    // extra_flag u16（12+2）——伴随字段不参与 watched 匹配判据
                    pos += relSize;
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
                int blockNo = u32(buf, pos);
                pos += 4;
                // OG 每块引用 blkno 后恒随 last_lsn u64（页 LSN 链回溯），当前不
                // 消费——按 layout 声明宽度跳过（V17/V18 为 0，字节行为不变）
                pos += lastLsnBytes;
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
        if (mainOff + mainLen != base + totLen) {
            throw new IllegalStateException(String.format(
                    "layout mismatch: mainOff=%d mainLen=%d totLen=%d rmid=%d info=%x",
                    mainOff, mainLen, totLen, buf[rmidOff] & 0xFF, buf[infoOff] & 0xFF));
        }
        return new WalRecord(lsn, totLen, u32(buf, xidOff), toplevelXid,
                buf[rmidOff] & 0xFF, buf[infoOff] & 0xFF, patched, mainOff, mainLen, buf);
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
