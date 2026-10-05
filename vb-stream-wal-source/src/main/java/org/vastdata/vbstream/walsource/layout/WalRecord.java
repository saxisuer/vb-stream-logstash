package org.vastdata.vbstream.walsource.layout;

import java.util.List;

/**
 * 一条 WAL 记录走读完成的解析结果——头区身份字段 + block 引用链 + main data 定位
 * + 原始字节引用。由 {@link WalRecordParser#parse(byte[], long, WalLayout)} 产出，
 * 后续 heap 视图/catalog 重放任务在此之上按 rmid/info 分发消费。
 *
 * @param lsn         本记录起始 LSN（由页遍历器传入，非记录体内容）
 * @param totLen      记录总长（含 24B 头，u32 totLen@0）——走读边界即此值，
 *                    raw 尾部可有 MAXALIGN 垫字节不被消费
 * @param xid         记录头 xid（u32@4）；无事务记录（如 FPI、检查点）为 0
 * @param toplevelXid 顶层事务 id（头区 252 标记读出，spike 发现 25）；无标记为 0
 * @param rmid        资源管理器 id（u8@17，rmgrlist.h 0-based 枚举序）
 * @param info        记录 info 字节（u8@16，低 nibble 保留、高位为操作码/标志）
 * @param blocks      block 引用链（头区出现序 = 块 id 序），载荷偏移已回填
 * @param mainOff     main data 在 raw 中的起始偏移（块载荷之后）；不变量
 *                    {@code mainOff + mainLen == totLen}（spike 发现 6）
 * @param mainLen     main data 字节数（终止头 DATA_SHORT/LONG 声明；无 main 为 0）
 * @param raw         完整记录字节（含头；数组由调用方持有，本类不拷贝、约定只读）
 */
public record WalRecord(
        long lsn,
        int totLen,
        int xid,
        int toplevelXid,
        int rmid,
        int info,
        List<BlockRef> blocks,
        int mainOff,
        int mainLen,
        byte[] raw
) {

    /**
     * 紧凑构造器：blocks 收敛为不可变 List——解析结果跨线程传递（reader/consumer
     * 交接）时防外部改写；raw 保留原引用（零拷贝，契约只读）。
     */
    public WalRecord {
        blocks = List.copyOf(blocks);
    }

    /**
     * 本记录归属的事务：子事务记录带 252 标记时归并到顶层 xid，否则即记录头 xid
     * （spike 发现 25——子事务归并与回滚过滤的事务键）。
     *
     * <p>返回 long 无符号语义：xid 是 u32，int 载体经掩码还原 0-4294967295 全域。</p>
     *
     * @return 归并后的有效事务 id（无事务记录为 0）
     */
    public long effXid() {
        return (toplevelXid != 0 ? toplevelXid : xid) & 0xFFFFFFFFL;
    }
}
