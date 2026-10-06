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

    /** xact.h：XLOG_XACT_PREPARE（opcode 位段 0x10，RM_XACT_ID 下——两阶段准备，
     * main data 是 72B 两阶段状态文件头 + gid，见 XactGrouper 的布局注记）。 */
    public static final int XLOG_XACT_PREPARE = 0x10;

    /** xact.h：XLOG_XACT_ABORT（opcode 位段 0x20，RM_XACT_ID 下）。 */
    public static final int XLOG_XACT_ABORT = 0x20;

    /** xact.h：XLOG_XACT_COMMIT_PREPARED（opcode 位段 0x30——两阶段提交确认，
     * 归属键是 main data 的 twophase chunk xid 而非记录头 xid，见 XactGrouper）。 */
    public static final int XLOG_XACT_COMMIT_PREPARED = 0x30;

    /** xact.h：XLOG_XACT_ABORT_PREPARED（opcode 位段 0x40——两阶段回滚，归属键同上）。 */
    public static final int XLOG_XACT_ABORT_PREPARED = 0x40;

    /** xact.h：XLOG_XACT_ASSIGNMENT（opcode 位段 0x50——子事务归并映射兜底，
     * main = xl_xact_assignment {xtop u32, nsubxacts i32, subxacts u32[]}）。 */
    public static final int XLOG_XACT_ASSIGNMENT = 0x50;

    /** opcode 掩码（heap 与 xact 同值 0x70；info 低 nibble 保留）。 */
    public static final int XLOG_XACT_OPMASK = 0x70;

    /** xact.h：XLOG_XACT_HAS_INFO（info 附加位 0x80——main 区在 xact_time 后携带
     * xinfo u32 与块链；commit/abort/两阶段确认记录共用）。 */
    public static final int XLOG_XACT_HAS_INFO = 0x80;

    /** heapam_xlog.h：xl_heap_update 含旧 tuple 全行或旧键（CONTAINS_OLD_TUPLE|OLD_KEY = 0x04|0x08）。 */
    public static final int XLH_UPDATE_CONTAINS_OLD = 0x04 | 0x08;

    /** heapam_xlog.h：XLH_UPDATE_CONTAINS_OLD_TUPLE（1&lt;&lt;2 = 0x04——REPLICA IDENTITY FULL
     * 的整行旧元组，ExtractReplicaIdentity 全列形态；截断重建的合法拼装源，Task 8.5）。 */
    public static final int XLH_UPDATE_CONTAINS_OLD_TUPLE = 0x04;

    /** heapam_xlog.h：XLH_UPDATE_CONTAINS_OLD_KEY（1&lt;&lt;3 = 0x08——DEFAULT/INDEX 身份的
     * 旧键元组，非键列全 null（ExtractReplicaIdentity heap_form_tuple 形态）——字节拼装
     * 会产出错值，截断重建的拒绝面，Task 8.5）。 */
    public static final int XLH_UPDATE_CONTAINS_OLD_KEY = 0x08;

    /** heapam_xlog.h：xl_heap_update 新 tuple 前缀/后缀截断（PREFIX_FROM_OLD|SUFFIX_FROM_OLD = 0x20|0x40）。 */
    public static final int XLH_UPDATE_TRUNCATION = 0x20 | 0x40;

    /** heapam_xlog.h：XLH_UPDATE_PREFIX_FROM_OLD（1&lt;&lt;5 = 0x20——新元组数据区与旧元组
     * 公共前缀省略，块 data 首部携带 prefix u16；Task 8.5 拼装坐标之一）。 */
    public static final int XLH_UPDATE_PREFIX_FROM_OLD = 0x20;

    /** heapam_xlog.h：XLH_UPDATE_SUFFIX_FROM_OLD（1&lt;&lt;6 = 0x40——新元组数据区与旧元组
     * 公共后缀省略，块 data 首部携带 suffix u16；Task 8.5 拼装坐标之一）。 */
    public static final int XLH_UPDATE_SUFFIX_FROM_OLD = 0x40;

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
