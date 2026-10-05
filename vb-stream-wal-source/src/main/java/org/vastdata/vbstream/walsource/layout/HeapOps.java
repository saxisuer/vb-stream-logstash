package org.vastdata.vbstream.walsource.layout;

/**
 * heap 家族记录的 rmid / opcode / XLH_* 标志位常量——照抄 spike 常量区
 * （transcribed from REL_18_STABLE：src/include/access/rmgrlist.h 的 0-based 枚举序
 * 与 src/include/access/heapam_xlog.h 的 opcode / 标志位定义）。
 *
 * <p>info 字节的低 nibble 保留，opcode 取 {@code info & XLOG_XACT_OPMASK}（0x70，heap
 * 与 xact 同值）；heap 家族另有 {@link #XLOG_HEAP_INIT_PAGE}（0x80）作为 info 附加位
 * 与 opcode 同字节共存（multi_insert 的 offsets 省略判据）。尺寸类常量
 * （SizeOfHeapUpdate=14 / SizeOfHeapDelete=8）不入本类——它们随 PG 大版本漂移，
 * 由 {@link WalLayout} 注入。</p>
 */
public final class HeapOps {

    /** rmgrlist.h 0-based 枚举序：RM_XACT_ID（事务提交/回滚）。 */
    public static final int RM_XACT_ID = 1;

    /** rmgrlist.h 0-based 枚举序：RM_HEAP2_ID（multi_insert / prune 等 heap 二级操作）。 */
    public static final int RM_HEAP2_ID = 9;

    /** rmgrlist.h 0-based 枚举序：RM_HEAP_ID（insert / update / delete / inplace）。 */
    public static final int RM_HEAP_ID = 10;

    /** heapam_xlog.h：XLOG_HEAP_INSERT（opcode 位段 0x00）。 */
    public static final int XLOG_HEAP_INSERT = 0x00;

    /** heapam_xlog.h：XLOG_HEAP_DELETE（opcode 位段 0x10）。 */
    public static final int XLOG_HEAP_DELETE = 0x10;

    /** heapam_xlog.h：XLOG_HEAP_UPDATE（opcode 位段 0x20，跨页更新）。 */
    public static final int XLOG_HEAP_UPDATE = 0x20;

    /** heapam_xlog.h：XLOG_HEAP_HOT_UPDATE（opcode 位段 0x40，同页 HOT 更新）。 */
    public static final int XLOG_HEAP_HOT_UPDATE = 0x40;

    /** heapam_xlog.h：XLOG_HEAP_INPLACE（opcode 位段 0x70，原地改写）。 */
    public static final int XLOG_HEAP_INPLACE = 0x70;

    /** heapam_xlog.h：XLOG_HEAP2_MULTI_INSERT（RM_HEAP2 下 opcode 位段 0x50）。 */
    public static final int XLOG_HEAP2_MULTI_INSERT = 0x50;

    /** heapam_xlog.h：info 附加位——整页重初始化（multi_insert 时 offsets 数组省略）。 */
    public static final int XLOG_HEAP_INIT_PAGE = 0x80;

    /** xact.h：XLOG_XACT_COMMIT（opcode 位段 0x00，RM_XACT_ID 下）。 */
    public static final int XLOG_XACT_COMMIT = 0x00;

    /** xact.h：XLOG_XACT_ABORT（opcode 位段 0x20，RM_XACT_ID 下）。 */
    public static final int XLOG_XACT_ABORT = 0x20;

    /** opcode 掩码（heap 与 xact 同值 0x70；info 低 nibble 保留）。 */
    public static final int XLOG_XACT_OPMASK = 0x70;

    /** heapam_xlog.h：xl_heap_update 含旧 tuple 全行或旧键（CONTAINS_OLD_TUPLE|OLD_KEY = 0x04|0x08）。 */
    public static final int XLH_UPDATE_CONTAINS_OLD = 0x04 | 0x08;

    /** heapam_xlog.h：xl_heap_update 新 tuple 前缀/后缀截断（PREFIX_FROM_OLD|SUFFIX_FROM_OLD = 0x20|0x40）。 */
    public static final int XLH_UPDATE_TRUNCATION = 0x20 | 0x40;

    /** heapam_xlog.h：xl_heap_delete 含旧 tuple 全行或旧键（CONTAINS_OLD_TUPLE|OLD_KEY = 0x02|0x04）。 */
    public static final int XLH_DELETE_CONTAINS_OLD = 0x02 | 0x04;

    /** heapam_xlog.h：XLOG_HEAP2_PRUNE_ON_ACCESS（opcode 位段 0x10，访问期修剪）。 */
    public static final int XLOG_HEAP2_PRUNE_ON_ACCESS = 0x10;

    /** heapam_xlog.h：XLOG_HEAP2_PRUNE_VACUUM_SCAN（opcode 位段 0x20，VACUUM 扫描期修剪）。 */
    public static final int XLOG_HEAP2_PRUNE_VACUUM_SCAN = 0x20;

    /** heapam_xlog.h：XLOG_HEAP2_PRUNE_VACUUM_CLEANUP（opcode 位段 0x30，VACUUM 收尾期修剪）。 */
    public static final int XLOG_HEAP2_PRUNE_VACUUM_CLEANUP = 0x30;

    /** heapam_xlog.h XLHP_*：prune 含快照冲突水位（main 区 flags 后 unaligned 跟随 4B XID，视图不消费）。 */
    public static final int XLHP_HAS_CONFLICT_HORIZON = 0x08;

    /** heapam_xlog.h XLHP_*：prune 块0 data 首段含 freeze plans（nplans u16 + 2B pad + plans×12B）。 */
    public static final int XLHP_HAS_FREEZE_PLANS = 0x10;

    /** heapam_xlog.h XLHP_*：prune 含 redirected 段（n 对 (from,to) 行指针重定向）。 */
    public static final int XLHP_HAS_REDIRECTIONS = 0x20;

    /** heapam_xlog.h XLHP_*：prune 含 nowdead 段（刚被杀死的行指针）。 */
    public static final int XLHP_HAS_DEAD_ITEMS = 0x40;

    /** heapam_xlog.h XLHP_*：prune 含 nowunused 段（可复用的行指针）。 */
    public static final int XLHP_HAS_NOW_UNUSED_ITEMS = 0x80;

    /**
     * 工具类防实例化。
     */
    private HeapOps() {
    }
}
