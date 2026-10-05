package org.vastdata.vbstream.walsource.layout;

/**
 * WAL 记录中单个 block 引用（registered block）走读后的强类型视图。
 *
 * <p>由 {@link WalRecordParser} 两阶段产出：头区走读阶段定身份字段
 * （fork/blockNo/定位器三件组/hasImage/hasData 及镜像头明细），载荷区阶段回填
 * {@code imageOff}/{@code dataOff} 两个绝对偏移（指向 {@link WalRecord#raw()}）。
 * 载荷区布局为逐块 [image][data] 按块 id 序（spike 发现 6）。</p>
 *
 * @param fork      fork 号（0=main，低位 4 bit；BKPBLOCK_FORK_MASK）
 * @param blockNo   块号（u32，定位器之后的独立字段）
 * @param spc       表空间 oid（SAME_REL 时复用前一定位器值）
 * @param db        数据库 oid（同上）
 * @param relNode   relation node（同上）
 * @param hasImage  是否携带页镜像（fork_flags 的 BKPBLOCK_HAS_IMAGE 位）
 * @param imageOff  镜像在 raw 中的起始偏移；无镜像为 -1
 * @param imageLen  镜像字节数（含洞时的存储长度，即写盘长度）；无镜像为 0
 * @param bimgInfo  镜像标志字节（HAS_HOLE/APPLY/pglz/lz4/zstd 位）；无镜像为 0
 * @param holeOffset 镜像头声明的洞偏移；无镜像为 0
 * @param holeLen   洞长：压缩镜像自头区显式 u16 读出，未压缩镜像由
 *                  {@code walBlockSize - imageLen} 推导；无镜像为 0
 * @param hasData   是否携带块内 data（fork_flags 的 BKPBLOCK_HAS_DATA 位）
 * @param dataOff   块内 data 在 raw 中的起始偏移；无 data 为 -1
 * @param dataLen   块内 data 字节数（头区内嵌的 u16 data_length）；无 data 为 0
 */
public record BlockRef(
        int fork,
        int blockNo,
        long spc,
        long db,
        long relNode,
        boolean hasImage,
        int imageOff,
        int imageLen,
        int bimgInfo,
        int holeOffset,
        int holeLen,
        boolean hasData,
        int dataOff,
        int dataLen
) {
}
