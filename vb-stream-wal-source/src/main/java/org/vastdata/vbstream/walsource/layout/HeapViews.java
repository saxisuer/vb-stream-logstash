package org.vastdata.vbstream.walsource.layout;

import java.util.ArrayList;
import java.util.List;

/**
 * heap 家族六种记录的强类型视图 + 解析工厂——重放引擎（后续任务）在
 * {@link WalRecordParser} 走读结果之上按 rmid/opcode 分发到本类各视图。
 *
 * <p>六视图全部经各自 {@code static XxxView parse(WalRecord, WalLayout)} 工厂产出：
 * main 区结构字段按 heapam_xlog.h（REL_18_STABLE）的成员偏移读取，随版本漂移的
 * 结构尺寸（{@code sizeOfHeapUpdate} 等）由 {@link WalLayout} 注入而非硬编码；
 * opcode/rmid 错配一律启动期 ISE fail-fast（消息带期望与实际值）。工厂为静态纯函数
 * 零共享状态，任意线程并发安全。</p>
 *
 * <p>各视图结构出处（heapam_xlog.h，REL_18_STABLE）：</p>
 * <ul>
 *   <li>xl_heap_insert：offnum u16@0 + flags u8@2（SizeOfHeapInsert=3）</li>
 *   <li>xl_heap_update：old_xmax u32@0 + old_offnum u16@4 + old_infobits u8@6 +
 *       flags u8@7 + new_xmax u32@8 + new_offnum u16@12（SizeOfHeapUpdate=14，
 *       new_offnum 为末成员，偏移 = 14-2 由 layout 推导）</li>
 *   <li>xl_heap_delete：xmax u32@0 + offnum u16@4 + infobits u8@6 + flags u8@7
 *       （SizeOfHeapDelete=8，offnum/flags 偏移由 8-4 / 8-1 推导——与 update 共享
 *       同形前缀）</li>
 *   <li>xl_heap_inplace：offnum u16@0 + 后续 16B 头（dbId/tsId/nmsgs）与 msgs[]
 *       柔性数组，本视图不消费</li>
 *   <li>xl_heap_multi_insert：flags u8@0 + C padding@1 + ntuples u16@2 +
 *       offsets u16[]@4（INIT_PAGE 时省略，spike 发现 3）</li>
 *   <li>xl_heap_prune：main 区 reason u8@0 + flags u8@1（+冲突水位 4B 可选），
 *       块0 data 区按 XLHP_* 位序分段（freeze/redirected/nowdead/nowunused，
 *       spike 发现 18）</li>
 * </ul>
 */
public final class HeapViews {

    /**
     * 工具类防实例化：六视图经各自 parse 工厂产出。
     */
    private HeapViews() {
    }

    /**
     * XLOG_HEAP_INSERT 记录视图：新 tuple 在页内的行指针序号 + 记录标志位。
     *
     * @param offnum 新 tuple 的页内偏移号（u16，main@0）
     * @param flags  XLH_INSERT_* 标志（u8，main@2；CONTAINS_NEW_TUPLE 等）
     */
    public record HeapInsertView(int offnum, int flags) {

        /**
         * 解析一条 heap INSERT 记录。
         *
         * <p>关键步骤：校验 rmid==RM_HEAP_ID 且 opcode==XLOG_HEAP_INSERT（错配 ISE）；
         * main 区须至少 SizeOfHeapInsert=3 字节；offnum 读 main@0（u16 小端）、
         * flags 读 main@2（u8）。边界与异常语义：main 过短抛 ISE；线程约束：
         * 静态纯函数，并发安全。</p>
         *
         * @param r      走读完成的记录（main 区即 xl_heap_insert 结构）
         * @param layout 版本布局描述符（本视图结构为版本稳定小结构，暂不取值；签名统一）
         * @return 视图（offnum + flags）
         * @throws IllegalStateException rmid/opcode 错配或 main 过短
         */
        public static HeapInsertView parse(WalRecord r, WalLayout layout) {
            requireOp(r, HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT);
            requireMainLen(r, 3, "xl_heap_insert");
            byte[] raw = r.raw();
            int off = r.mainOff();
            return new HeapInsertView(u16(raw, off), raw[off + 2] & 0xFF);
        }
    }

    /**
     * XLOG_HEAP_UPDATE / XLOG_HEAP_HOT_UPDATE 记录视图：旧/新 tuple 行指针序号 +
     * 记录标志位 + 旧 tuple 所在页块号。
     *
     * @param oldOffnum  旧 tuple 的页内偏移号（u16，main@4）
     * @param newOffnum  新 tuple 的页内偏移号（u16，main@12 = SizeOfHeapUpdate-2）
     * @param flags      XLH_UPDATE_* 标志（u8，main@7；CONTAINS_OLD / TRUNCATION 位组）
     * @param oldBlockNo 旧 tuple 所在块号：取首个 fork==0 且块号不等于新页（块 id 0）
     *                   的块——记录还可携带 VM fork 块（fork==1），按 fork 过滤而非
     *                   按块序号取（spike VM 块教训）；无此块（HOT 同页更新）回退
     *                   新页块号
     */
    public record HeapUpdateView(int oldOffnum, int newOffnum, int flags, int oldBlockNo) {

        /**
         * 解析一条 heap UPDATE / HOT UPDATE 记录。
         *
         * <p>关键步骤：校验 rmid==RM_HEAP_ID 且 opcode ∈ {UPDATE, HOT_UPDATE}；main 区
         * 须至少 layout.sizeOfHeapUpdate()（14）字节；old_offnum 读 main@4、flags 读
         * main@7（共享 xl_heap_delete 同形前缀）、new_offnum 读
         * main@sizeOfHeapUpdate-2（末成员，偏移经 layout 推导）；oldBlockNo 按块链
         * 过滤 fork==0 且 blockNo != 块0.blockNo 的首块，无则回退块0.blockNo。边界与
         * 异常语义：错配/main 过短/无块引用均 ISE；线程约束：静态纯函数，并发安全。</p>
         *
         * @param r      走读完成的记录（main 区即 xl_heap_update 结构）
         * @param layout 版本布局描述符（注入 SizeOfHeapUpdate 锚定 new_offnum 偏移）
         * @return 视图（old/new offnum + flags + oldBlockNo）
         * @throws IllegalStateException rmid/opcode 错配、main 过短或无块引用
         */
        public static HeapUpdateView parse(WalRecord r, WalLayout layout) {
            int op = r.info() & HeapOps.XLOG_XACT_OPMASK;
            if (r.rmid() != HeapOps.RM_HEAP_ID
                    || (op != HeapOps.XLOG_HEAP_UPDATE && op != HeapOps.XLOG_HEAP_HOT_UPDATE)) {
                throw new IllegalStateException(String.format(
                        "heap update view expects rmid=%d opcode in {0x%x, 0x%x}, got rmid=%d info=0x%02x",
                        HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, HeapOps.XLOG_HEAP_HOT_UPDATE,
                        r.rmid(), r.info()));
            }
            requireMainLen(r, layout.sizeOfHeapUpdate(), "xl_heap_update");
            if (r.blocks().isEmpty()) {
                throw new IllegalStateException("heap update record without block references");
            }
            byte[] raw = r.raw();
            int off = r.mainOff();
            int oldOffnum = u16(raw, off + 4);
            int flags = raw[off + 7] & 0xFF;
            int newOffnum = u16(raw, off + layout.sizeOfHeapUpdate() - 2);
            BlockRef newPage = r.blocks().get(0);   // 块 id 0 = 新页 heap 块（HEAP_UPDATE_BLKREF_HEAP_NEW）
            int oldBlockNo = newPage.blockNo();
            for (BlockRef b : r.blocks()) {
                // 旧页 heap 块按 fork 过滤选取——记录可混入 VM fork 块（spike VM 块教训）
                if (b.fork() == 0 && b.blockNo() != newPage.blockNo()) {
                    oldBlockNo = b.blockNo();
                    break;
                }
            }
            return new HeapUpdateView(oldOffnum, newOffnum, flags, oldBlockNo);
        }
    }

    /**
     * XLOG_HEAP_DELETE 记录视图：被删 tuple 行指针序号 + 记录标志位。
     *
     * @param offnum 被删 tuple 的页内偏移号（u16，main@4 = SizeOfHeapDelete-4）
     * @param flags  XLH_DELETE_* 标志（u8，main@7 = SizeOfHeapDelete-1；CONTAINS_OLD 位组）
     */
    public record HeapDeleteView(int offnum, int flags) {

        /**
         * 解析一条 heap DELETE 记录。
         *
         * <p>关键步骤：校验 rmid==RM_HEAP_ID 且 opcode==XLOG_HEAP_DELETE；main 区须
         * 至少 layout.sizeOfHeapDelete()（8）字节；offnum 读
         * main@sizeOfHeapDelete-4（xmax u32 前缀之后）、flags 读
         * main@sizeOfHeapDelete-1（结构末字节）。边界与异常语义：错配/main 过短
         * ISE；线程约束：静态纯函数，并发安全。</p>
         *
         * @param r      走读完成的记录（main 区即 xl_heap_delete 结构）
         * @param layout 版本布局描述符（注入 SizeOfHeapDelete 锚定两字段偏移）
         * @return 视图（offnum + flags）
         * @throws IllegalStateException rmid/opcode 错配或 main 过短
         */
        public static HeapDeleteView parse(WalRecord r, WalLayout layout) {
            requireOp(r, HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE);
            requireMainLen(r, layout.sizeOfHeapDelete(), "xl_heap_delete");
            byte[] raw = r.raw();
            int off = r.mainOff();
            int size = layout.sizeOfHeapDelete();
            return new HeapDeleteView(u16(raw, off + size - 4), raw[off + size - 1] & 0xFF);
        }
    }

    /**
     * XLOG_HEAP_INPLACE 记录视图：被原地改写的 tuple 行指针序号。
     *
     * <p>main 区后续的 16B 头（dbId/tsId/relcacheInitFileInval/nmsgs，见
     * layout.sizeOfHeapInplace()=20 的固定前缀）与共享失效消息 msgs[] 由后续任务
     * 消费，本视图只取 offnum（spike replayInplace 同面）。</p>
     *
     * @param offnum 被改写 tuple 的页内偏移号（u16，main@0）
     */
    public record HeapInplaceView(int offnum) {

        /**
         * 解析一条 heap INPLACE 记录。
         *
         * <p>关键步骤：校验 rmid==RM_HEAP_ID 且 opcode==XLOG_HEAP_INPLACE；main 区
         * 须至少 2 字节（offnum u16）；offnum 读 main@0。边界与异常语义：错配/main
         * 过短 ISE；线程约束：静态纯函数，并发安全。</p>
         *
         * @param r      走读完成的记录（main 区即 xl_heap_inplace 结构前缀）
         * @param layout 版本布局描述符（本视图仅取版本稳定首成员；签名统一保留）
         * @return 视图（offnum）
         * @throws IllegalStateException rmid/opcode 错配或 main 过短
         */
        public static HeapInplaceView parse(WalRecord r, WalLayout layout) {
            requireOp(r, HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INPLACE);
            requireMainLen(r, 2, "xl_heap_inplace");
            return new HeapInplaceView(u16(r.raw(), r.mainOff()));
        }
    }

    /**
     * XLOG_HEAP2_MULTI_INSERT 记录视图：批量插入的 tuple 数与 offsets 数组定位。
     *
     * <p>main 区布局：flags u8@0 + C padding@1 + ntuples u16@2（spike 发现 3——
     * uint16 成员落在 @2，XLogRegisterData 按原样注册含 padding 的结构）+ offsets
     * u16[]@4。INIT_PAGE（info 的 0x80 位）时 PG 省略 offsets 数组（整页重初始化、
     * offnum 天然连续）——此时工厂置 {@code offsetsOff=-1}，{@link #offsetAt(int)}
     * 以 {@code i+1} 应答；否则 offsetsOff=mainOff+4，逐 u16 读取。</p>
     *
     * @param ntuples    本记录批量插入的 tuple 数（u16，main@2）
     * @param offsetsOff offsets 数组在 raw 中的绝对偏移；INIT_PAGE 形态为 -1
     * @param rec        源记录引用（offsetAt 逐条回读 raw，契约只读）
     */
    public record MultiInsertView(int ntuples, int offsetsOff, WalRecord rec) {

        /**
         * 解析一条 heap2 MULTI_INSERT 记录。
         *
         * <p>关键步骤：校验 rmid==RM_HEAP2_ID 且 opcode==XLOG_HEAP2_MULTI_INSERT；
         * main 区须至少 4 字节（flags + padding + ntuples）；ntuples 读 main@2（u16）；
         * 按 info 的 XLOG_HEAP_INIT_PAGE 位决定 offsets 形态——置位时 offsetsOff=-1
         * （offnum 隐含为 i+1），否则 mainOff+4。边界与异常语义：错配/main 过短 ISE；
         * 线程约束：静态纯函数 + 持只读记录引用，并发安全。</p>
         *
         * @param r      走读完成的记录（main 区即 xl_heap_multi_insert 头）
         * @param layout 版本布局描述符（本视图结构为版本稳定小结构，暂不取值；签名统一）
         * @return 视图（ntuples + offsetsOff + 源记录引用）
         * @throws IllegalStateException rmid/opcode 错配或 main 过短
         */
        public static MultiInsertView parse(WalRecord r, WalLayout layout) {
            requireOp(r, HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT);
            requireMainLen(r, 4, "xl_heap_multi_insert");
            int ntuples = u16(r.raw(), r.mainOff() + 2);
            boolean initPage = (r.info() & HeapOps.XLOG_HEAP_INIT_PAGE) != 0;
            int offsetsOff = initPage ? -1 : r.mainOff() + 4;
            return new MultiInsertView(ntuples, offsetsOff, r);
        }

        /**
         * 第 i 个（0-based）插入 tuple 的页内偏移号。
         *
         * <p>关键步骤：INIT_PAGE 形态（offsetsOff&lt;0）直接应答 i+1——整页重初始化后
         * 行指针按序分配；否则读 offsets 数组第 i 项（u16 小端，offsetsOff+2i）。
         * 边界与异常语义：i 越界（&lt;0 或 ≥ntuples）抛 ISE；线程约束：纯读，并发安全。</p>
         *
         * @param i tuple 序号（0-based）
         * @return 页内偏移号
         * @throws IllegalStateException i 越界
         */
        public int offsetAt(int i) {
            if (i < 0 || i >= ntuples) {
                throw new IllegalStateException("tuple index " + i + " out of 0.." + (ntuples - 1));
            }
            return offsetsOff < 0 ? i + 1 : u16(rec.raw(), offsetsOff + 2 * i);
        }
    }

    /**
     * XLOG_HEAP2_PRUNE_{ON_ACCESS,VACUUM_SCAN,VACUUM_CLEANUP} 记录视图：页修剪的
     * 标志位与三段行指针分段（spike 发现 18 布局）。
     *
     * <p>main 区：reason u8@0 + flags u8@1（&amp;0xFF 读——Java byte 符号扩展教训，
     * 高位段 0x40/0x80 不掩码会变负数）+ 可选冲突水位 XID 4B（flags&amp;0x08 时
     * unaligned 跟随，视图不消费，冲突消解属备库回放）。块0 data 区按 XLHP_* 位序
     * 分段：freeze plans（flags&amp;0x10：nplans u16 + 2B pad + plans×12B——含
     * TransactionId 需 4 字节对齐故居首）→ redirected（n 对 from/to）→ nowdead →
     * nowunused；工厂消化 freeze 段跳过，{@code dataOff} 即首段起点（无任何段时
     * 亦为 freeze 后的理论起点）。</p>
     *
     * @param flags   XLHP_* 标志（u8，main@1）
     * @param dataOff 块0 data 区中 redirected/nowdead/nowunused 首段在 raw 中的
     *                绝对偏移（freeze 段已跳过）
     * @param rec     源记录引用（三访问器逐段回读 raw，契约只读）
     */
    public record PruneView(int flags, int dataOff, WalRecord rec) {

        /**
         * 解析一条 heap2 PRUNE 记录（三种 prune opcode 等价，spike 走读同判）。
         *
         * <p>关键步骤：校验 rmid==RM_HEAP2_ID 且 opcode ∈ {0x10, 0x20, 0x30}；main 区
         * 须至少 2 字节（reason + flags）；块 id 0 须携带 data（分段载体）；flags 读
         * main@1（&amp;0xFF 掩码）；freeze 段（flags&amp;0x10）在块0 data 起点上跳过
         * 4 + nplans×12 字节得 dataOff（冲突水位在 main 区，不影响块区走读）。
         * 边界与异常语义：错配/main 过短/无块 data 均 ISE；线程约束：静态纯函数 +
         * 只读记录引用，并发安全。</p>
         *
         * @param r      走读完成的记录（main 区即 xl_heap_prune 头）
         * @param layout 版本布局描述符（本视图分段布局为 PG 16+ 稳定形态，暂不取值；签名统一）
         * @return 视图（flags + dataOff + 源记录引用）
         * @throws IllegalStateException rmid/opcode 错配、main 过短或块0 无 data
         */
        public static PruneView parse(WalRecord r, WalLayout layout) {
            int op = r.info() & HeapOps.XLOG_XACT_OPMASK;
            if (r.rmid() != HeapOps.RM_HEAP2_ID
                    || (op != HeapOps.XLOG_HEAP2_PRUNE_ON_ACCESS
                    && op != HeapOps.XLOG_HEAP2_PRUNE_VACUUM_SCAN
                    && op != HeapOps.XLOG_HEAP2_PRUNE_VACUUM_CLEANUP)) {
                throw new IllegalStateException(String.format(
                        "prune view expects rmid=%d opcode in {0x10, 0x20, 0x30}, got rmid=%d info=0x%02x",
                        HeapOps.RM_HEAP2_ID, r.rmid(), r.info()));
            }
            requireMainLen(r, 2, "xl_heap_prune");
            if (r.blocks().isEmpty() || !r.blocks().get(0).hasData()) {
                throw new IllegalStateException("prune record without block data");
            }
            byte[] raw = r.raw();
            // &0xFF 掩码：高位段标志（0x40/0x80）经有符号 byte 会符号扩展为负（spike 教训）
            int flags = raw[r.mainOff() + 1] & 0xFF;
            int cur = r.blocks().get(0).dataOff();
            if ((flags & HeapOps.XLHP_HAS_FREEZE_PLANS) != 0) {
                // nplans u16 + 2B padding + nplans × xlhp_freeze_plan(12B)
                cur += 4 + u16(raw, cur) * 12;
            }
            return new PruneView(flags, cur, r);
        }

        /**
         * redirected 段：被重定向的行指针对（HOT 链根迁移）。
         *
         * <p>关键步骤：flags 未置 REDIRECTIONS 位则空表；否则自 dataOff 读 n u16，
         * 展平收集 2n 个 u16——相邻两项为一对 (from, to)。边界与异常语义：段字节
         * 越界 raw 边界则数组越界裸抛（上游走读不变量已保证 totLen 自洽）；线程
         * 约束：纯读，并发安全。</p>
         *
         * @return 展平的 (from, to) 对列表（长度 2n；无段为空表）
         */
        public List<Integer> redirectedPairs() {
            if ((flags & HeapOps.XLHP_HAS_REDIRECTIONS) == 0) {
                return List.of();
            }
            return readItems(dataOff, 2);
        }

        /**
         * nowdead 段：刚被标记死亡（LP_DEAD）的行指针。
         *
         * <p>关键步骤：flags 未置 DEAD_ITEMS 位则空表；否则走读越过 redirected 段
         * （若存在）后读 n u16 与 n 个 u16。边界与异常语义：同 {@link #redirectedPairs()}。</p>
         *
         * @return 偏移号列表（长度 n；无段为空表）
         */
        public List<Integer> nowdead() {
            if ((flags & HeapOps.XLHP_HAS_DEAD_ITEMS) == 0) {
                return List.of();
            }
            return readItems(nowdeadStart(), 1);
        }

        /**
         * nowunused 段：已回收可复用（LP_UNUSED）的行指针。
         *
         * <p>关键步骤：flags 未置 NOW_UNUSED_ITEMS 位则空表；否则走读越过 redirected
         * 与 nowdead 段（若存在）后读 n u16 与 n 个 u16。边界与异常语义：同
         * {@link #redirectedPairs()}。</p>
         *
         * @return 偏移号列表（长度 n；无段为空表）
         */
        public List<Integer> nowunused() {
            if ((flags & HeapOps.XLHP_HAS_NOW_UNUSED_ITEMS) == 0) {
                return List.of();
            }
            return readItems(nowunusedStart(), 1);
        }

        /**
         * nowdead 段起点：走读越过 redirected 段（若存在）——n u16 + 4n 数据字节。
         *
         * @return nowdead 段起点（redirected 缺位即 dataOff）
         */
        private int nowdeadStart() {
            int pos = dataOff;
            if ((flags & HeapOps.XLHP_HAS_REDIRECTIONS) != 0) {
                pos += 2 + 4 * u16(rec.raw(), pos);
            }
            return pos;
        }

        /**
         * nowunused 段起点：再走读越过 nowdead 段（若存在）——n u16 + 2n 数据字节。
         *
         * @return nowunused 段起点（nowdead 亦缺位即 nowdeadStart()）
         */
        private int nowunusedStart() {
            int pos = nowdeadStart();
            if ((flags & HeapOps.XLHP_HAS_DEAD_ITEMS) != 0) {
                pos += 2 + 2 * u16(rec.raw(), pos);
            }
            return pos;
        }

        /**
         * 自 pos 起 reads n=count u16 展平收集（n 个计数单位 × countPerItem 个 u16）。
         *
         * @param pos           段起点（指向计数 u16）
         * @param countPerItem  每个计数单位对应的 u16 个数
         * @return 展平值列表
         */
        private List<Integer> readItems(int pos, int countPerItem) {
            byte[] raw = rec.raw();
            int n = u16(raw, pos);
            List<Integer> out = new ArrayList<>(n * countPerItem);
            for (int i = 0; i < n * countPerItem; i++) {
                out.add(u16(raw, pos + 2 + 2 * i));
            }
            return out;
        }
    }

    /**
     * 校验记录的 rmid 与 opcode（info &amp; 0x70）恰好符合期望，否则 ISE。
     *
     * @param r            走读完成的记录
     * @param expectedRmid 期望的 rmid
     * @param expectedOp   期望的 opcode 位段值
     * @throws IllegalStateException 任一项不符
     */
    private static void requireOp(WalRecord r, int expectedRmid, int expectedOp) {
        if (r.rmid() != expectedRmid || (r.info() & HeapOps.XLOG_XACT_OPMASK) != expectedOp) {
            throw new IllegalStateException(String.format(
                    "opcode mismatch: expect rmid=%d opcode 0x%02x, got rmid=%d info=0x%02x",
                    expectedRmid, expectedOp, r.rmid(), r.info()));
        }
    }

    /**
     * 校验 main 区长度下限（结构尺寸约束），不足即 ISE。
     *
     * @param r      走读完成的记录
     * @param minLen 最小 main 字节数
     * @param what   结构名（进异常消息定位）
     * @throws IllegalStateException main 区过短
     */
    private static void requireMainLen(WalRecord r, int minLen, String what) {
        if (r.mainLen() < minLen) {
            throw new IllegalStateException(String.format(
                    "%s main data too short: %d < %d", what, r.mainLen(), minLen));
        }
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
}
