package org.vastdata.vbstream.walsource.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.BlockRef;
import org.vastdata.vbstream.walsource.layout.DecodeKinds;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.HeapViews;
import org.vastdata.vbstream.walsource.layout.PageImages;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * WAL 事务组装状态机（v2 MS2）——把流经的 heap/XACT 记录组装为
 * {@link ChangeOutputListener} 的批量事务事件：heap I/U/D 行入桶累积（解码 +
 * {@link DiskValueRenderer} 渲染在到达时完成），XACT 终态记录驱动发射/弃桶/挂起。
 *
 * <p><b>发射语义（控制器 2026-10 裁定修正，与 spec §4 "即时发射"的偏差记档）</b>：
 * 行在桶内累积、<b>COMMIT / COMMIT_PREPARED 时按序批量发射</b>（Begin → 过滤后行
 * → End）。理由：aborted 子事务过滤与逐行即时回调不可同时成立——子回滚（ABORT 记录
 * 携带子 xid）到达时桶内该子 xid 的行可能已即时发出，无法撤回；spec 的"即时发射"
 * 措辞是架构对齐论证（WAL 本身即落盘缓冲），实际发射点=提交时，与 engine 流式
 * 输出"提交后才回放"同构，输出缓冲语义不变。事件序 Begin → Row×n → End 与逐行即时形态
 * 逐序一致。expectedChanges=桶记账（aborted 过滤前）；emittedChanges=过滤后实付；
 * BatchBegin 在批量发射首端携带的就是记账终值（发射点=提交时点，全量已知——契约
 * javadoc 的"Begin 时点值"在本形态下即终值）。</p>
 *
 * <p><b>子事务归并</b>：heap 记录的记录头 xid 是产生行的（子）事务，TOPLEVEL_XID
 * 标记（spike 发现 25）优先归并到顶层；无标记时 ASSIGNMENT(0x50) 记录的 subxid→top
 * 映射兜底。aborted 过滤按行的<b>来源 xid</b>（桶内私有记账，不泄漏进
 * {@code RowChange}——预检 #5 裁定）：子回滚把子 xid 记入桶的 abortedSubxids，
 * 发射期扣减该来源的行；顶层 ABORT 则整桶弃（onAborted，零 Begin/End）。</p>
 *
 * <p><b>2PC</b>：PREPARE 把桶标记挂起（记 gid，零发射——行值已在到达期物化）；
 * COMMIT_PREPARED 按 main data 的 <b>twophase chunk xid</b>（非记录头 xid——
 * COMMIT PREPARED 在新事务里执行，记录头是新 xid；REL_18 xact.c xact_redo_commit
 * 即用 parsed.twophase_xid）发射 twoPhase=true + gid；ABORT_PREPARED 弃桶。挂起桶
 * 跨重启安全：检查点 LSN 必然落后 PREPARE 位点，重放重建桶并补发终态。</p>
 *
 * <p><b>XACT main 布局钉</b>（REL_18_STABLE src/include/access/xact.h +
 * src/backend/access/rmgrdesc/xact.c ParseCommitRecord / ParsePrepareRecord，2026-10
 * curl 实源钉）：commit/abort 最小 main = 裸 {@code xact_time i64}@0；info 置
 * HAS_INFO(0x80) 时 xinfo u32@8 + 块链（dbinfo 8B / subxacts 4+4n / relfilelocators
 * 4+12n / stats 4+16n / invals 4+16n / twophase xid u32 + gid NUL 结尾）；PREPARE main =
 * 72B 两阶段状态文件头（xid u32@8、prepared_at i64@16、gidlen u16@54、gid@72 按
 * gidlen 截取，<b>非 NUL 结尾</b>——twophase.c PrepareRedoAdd 的 strncpy 同面）。</p>
 *
 * <p><b>TableMeta 缓存</b>（控制器裁定，Task 4 提醒）：{@link TableFilter#resolve}
 * 每行 7 趟线性扫——本层按 relfilenode 缓存解析结果；失效策略取最简可靠形态：
 * <b>pg_class/pg_attribute/pg_namespace 三张字典表（固定 oid 1259/1249/2615）上的
 * heap 记录到达即全清缓存</b>（用户表形态/列序只经这三张表的变更改变；toast chunk
 * 与索引记录不清——高频且不影响形态）。oid 计数器 wraparound 使 relfilenode 撞键的
 * 理论残留与 v1 块匹配同口径。缺省身份映射（relfilenode==oid）下三表可直认。</p>
 *
 * <p><b>TOAST 接线</b>：toast 关系（relkind 't'）上的 INSERT/MULTI_INSERT 解码为
 * 三列 chunk 行喂 {@link ToastAssembler#onChunkRow}；每个事务终态记录（COMMIT/ABORT/
 * PREPARE/两阶段确认）后调 {@link ToastAssembler#clear()}（控制器裁定全清——valueid
 * 全局唯一依据与交错风险面见其 javadoc）。<b>行内 external 接线（Task 7 硬前置②）
 * </b>：本类构造的 {@link TupleDecoder} 注入 {@code toast::resolveExternal}——用户表
 * 元组内首字节 0x01 的 external 短指针经已归集 chunk 拼装/解压/回查兜底重建原值
 * （缺值降级 {@code toast-unavailable} 不 fail 流）；toast=null（采集禁用）时保持
 * v1 拒绝面。行内 compressed varlena（tag 0x02）仍 ISE——非 TOAST 重组面。</p>
 *
 * <p><b>类型词典</b>：列 kind 由 typeOid 经 {@link DecodeKinds#forTypeOid} 单源派生
 * ——Task 7 硬前置①扩容后与渲染矩阵 oid 集合对齐（含 date/time/timetz/
 * timestamptz/numeric/uuid 的 datum 字节形态），enum/域/jsonb/组合类型等矩阵外
 * oid 解码即 ISE（走读对位无法维持——双路对拍 MS3+ 的扩面输入）。UPDATE 前缀/
 * 后缀截断（XLH_UPDATE_TRUNCATION）的新元组重建同为 v2 未落面（spike 遗留，
 * 撞上即解码失败 fail-fast 而非静默错位）。</p>
 *
 * <p>线程约束：<b>单写者</b>（wal-receiver 线程顺序喂 onRecord，与 v1 replay 组件
 * 同假设）；listener 回调与 TOAST 组装同线程发生。跨重启安全依赖调用方保证：本类
 * 无自身持久化面。</p>
 */
public final class XactGrouper {

    private static final Logger LOG = LoggerFactory.getLogger(XactGrouper.class);

    /** PostgreSQL epoch（2000-01-01T00:00:00Z）折算微秒——XACT 时间戳的换算源。 */
    private static final long EPOCH_2000_MICROS = 946_684_800_000_000L;

    /** xact.h：XACT_XINFO_HAS_DBINFO（1<<0——dbinfo 块 8B）。 */
    private static final int XINFO_HAS_DBINFO = 1 << 0;

    /** xact.h：XACT_XINFO_HAS_SUBXACTS（1<<1——subxacts 块 4+4n）。 */
    private static final int XINFO_HAS_SUBXACTS = 1 << 1;

    /** xact.h：XACT_XINFO_HAS_RELFILELOCATORS（1<<2——relfilelocators 块 4+12n）。 */
    private static final int XINFO_HAS_RELFILELOCATORS = 1 << 2;

    /** xact.h：XACT_XINFO_HAS_INVALS（1<<3——invals 块 4+16n）。 */
    private static final int XINFO_HAS_INVALS = 1 << 3;

    /** xact.h：XACT_XINFO_HAS_TWOPHASE（1<<4——twophase 块 xid u32）。 */
    private static final int XINFO_HAS_TWOPHASE = 1 << 4;

    /** xact.h：XACT_XINFO_HAS_DROPPED_STATS（1<<8——stats 块 4+16n）。 */
    private static final int XINFO_HAS_DROPPED_STATS = 1 << 8;

    /** xact.h：XACT_XINFO_HAS_GID（1<<7——gid NUL 结尾字符串随 twophase 块）。 */
    private static final int XINFO_HAS_GID = 1 << 7;

    /** PREPARE main 的两阶段文件头尺寸（offsetof 链详见类 javadoc 布局钉）。 */
    private static final int SIZE_OF_XACT_PREPARE = 72;

    /** 字典三表的固定 oid（pg_attribute/pg_class/pg_namespace——缓存失效触发键）。 */
    private static final Set<Long> SHAPE_CATALOG_OIDS = Set.of(1249L, 1259L, 2615L);

    /** toast chunk 表的三列解码词典（chunk_id oid / chunk_seq int4 / chunk_data bytea）。 */
    private static final String[] TOAST_CHUNK_KINDS = {"oid", "int4", "bytea"};

    /** 未决 2PC 挂起桶的 WARN 护栏阈值（正常业务挂起窗口秒级，持续增长 = 终态泄漏信号）。 */
    private static final int PREPARED_PENDING_WARN_THRESHOLD = 1000;

    private final CatalogSnapshot snapshot;
    private final TableFilter filter;
    private final WalLayout layout;
    private final TupleDecoder decoder;
    private final ToastAssembler toast;
    private final ChangeOutputListener out;

    /** 待决桶：顶层事务 id → 桶（行到达期累积，终态记录发射/弃）。 */
    private final Map<Long, Bucket> buckets = new HashMap<>();

    /** ASSIGNMENT 兜底映射：子 xid → 顶层 xid（终态时按顶层剪枝，防会话期无界增长）。 */
    private final Map<Long, Long> subToTop = new HashMap<>();

    /** TableFilter 解析缓存：relfilenode → 解析结果（miss 亦缓存；字典表 heap 记录全清）。 */
    private final Map<Long, Resolved> metaCache = new HashMap<>();

    /** toast 关系判定缓存：relfilenode → 是否 relkind 't'。 */
    private final Map<Long, Boolean> toastRels = new HashMap<>();

    /** 未决 2PC 挂起桶计数（PREPARE 递增、终态弃桶递减——WARN 护栏的观测面）。 */
    private int preparedPending;

    /** 护栏 WARN 已发标记（计数节流：超阈值只告警一次，回落不重置）。 */
    private boolean preparedOverflowWarned;

    /** 已发射事务桶计数（emitBucket 递增——Main 周期 smoke 行的 DML 观测面，volatile 跨线程读）。 */
    private volatile long emittedBuckets;

    /** 已发射行计数（过滤后实付口径，emitBucket 累加 emitted——volatile 跨线程读）。 */
    private volatile long emittedRows;

    /**
     * 装配组装器。
     *
     * <p>注意 {@link DiskValueRenderer} 是静态形态（Task 2 交付，控制器裁定）——不经
     * 构造参注入，渲染经静态调用发生。whitelist 语义透传 {@link TableFilter}（null =
     * 用户表全放行）。</p>
     *
     * @param snapshot  catalog as-of 快照（表身份/列序查询面）
     * @param whitelist 表白名单（{@code schema.table} 全名集；null/空 = 全放行）
     * @param layout    版本布局描述符（SizeOfHeapUpdate/Delete 等 heap 结构尺寸）
     * @param toast     TOAST 重组器（chunk 采集 + 终态淘汰；null = 采集禁用——纯无宽值形态）
     * @param out       变更事件输出契约实现（发射线程 = 本类单写者线程）
     */
    public XactGrouper(CatalogSnapshot snapshot, Set<String> whitelist, WalLayout layout,
                       ToastAssembler toast, ChangeOutputListener out) {
        this.snapshot = snapshot;
        this.filter = new TableFilter(snapshot, whitelist);
        this.layout = layout;
        this.decoder = new TupleDecoder(layout, toast);
        this.toast = toast;
        this.out = out;
    }

    /**
     * 喂一条走读完成的 WAL 记录——按 rmid 分发：XACT 记录驱动事务终态，heap/heap2
     * 记录解码行入桶（或 toast chunk 采集），其余 rmid（PRUNE 已含在 heap2 内过滤、
     * XLOG 等）静默忽略。
     *
     * <p>关键步骤：rmid 三路分发；XACT 下按 opcode 六态（COMMIT/ABORT/PREPARE/
     * COMMIT_PREPARED/ABORT_PREPARED/ASSIGNMENT，INVALIDATIONS 等其余 opcode 忽略）；
     * heap 下先经 TableFilter 解析（缓存），命中用户表才解码。边界与异常语义：无块
     * 引用或无事务归属（xid=0）的 heap 记录跳过；解码/走读错位抛 ISE fail-fast。
     * 线程约束：单写者（wal-receiver 线程）。</p>
     *
     * @param rec 走读完成的记录（raw 契约只读）
     */
    public void onRecord(WalRecord rec) {
        if (rec.rmid() == HeapOps.RM_XACT_ID) {
            dispatchXact(rec);
        } else if (rec.rmid() == HeapOps.RM_HEAP_ID) {
            dispatchHeap(rec);
        } else if (rec.rmid() == HeapOps.RM_HEAP2_ID) {
            dispatchHeap2(rec);
        }
    }

    /**
     * XACT 记录六态分发（opcode 见类 javadoc 布局钉）。
     *
     * @param rec XACT 记录
     */
    private void dispatchXact(WalRecord rec) {
        int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
        switch (op) {
            case HeapOps.XLOG_XACT_COMMIT -> onCommit(rec, false);
            case HeapOps.XLOG_XACT_ABORT -> onAbort(rec);
            case HeapOps.XLOG_XACT_PREPARE -> onPrepare(rec);
            case HeapOps.XLOG_XACT_COMMIT_PREPARED -> onCommit(rec, true);
            case HeapOps.XLOG_XACT_ABORT_PREPARED -> onAbortPrepared(rec);
            case HeapOps.XLOG_XACT_ASSIGNMENT -> onAssignment(rec);
            default -> { /* INVALIDATIONS(0x60) 等与事务组装无关 */ }
        }
    }

    /**
     * COMMIT / COMMIT_PREPARED：归属桶按序批量发射（Begin → 过滤后行 → End）后弃桶。
     *
     * <p>关键步骤：解析 main（commitTs/gid/twophase xid——归属键优先 twophase chunk
     * 的 xid，缺省记录头 xid）；取桶（无桶 = 事务无用户表行，零发射）；发射
     * （twoPhase/gid 由本记录携带优先，缺省回落桶上 PREPARE 记下的 gid）；终态面
     * 清理——toast 归集全清 + subToTop 剪枝。边界与异常语义：无桶事务静默跳过
     * （catalog-only / 全过滤事务不产生事件）；main 过短抛 ISE。线程约束：单写者。</p>
     *
     * @param rec      提交记录
     * @param twoPhase true = COMMIT_PREPARED 形态
     */
    private void onCommit(WalRecord rec, boolean twoPhase) {
        CommitView c = parseCommitMain(rec);
        long xid = c.twophaseXid() != 0 ? c.twophaseXid() : rec.xid() & 0xFFFFFFFFL;
        Bucket b = removeBucket(xid);
        if (b != null) {
            String gid = c.gid() != null ? c.gid() : b.gid;
            emitBucket(b, twoPhase, gid, rec.lsn(), endLsnOf(rec), c.commitTs());
        }
        terminalCleanup(xid);
    }

    /**
     * ABORT：顶层回滚整桶弃（onAborted 单事件）；子回滚（TOPLEVEL 标记或 assignment
     * 映射指出归属顶层≠自身）把子 xid 记入桶的 abortedSubxids，发射期扣减。
     *
     * <p>关键步骤：xid = 记录头 xid（子回滚时即子 xid）；effectiveTop 归并判定——
     * top≠xid 为子回滚（非事务终态，不触发 toast 清理）；否则整桶弃 + onAborted
     * （无桶 = 无用户表行，仍属终态但不发事件——BatchAborted 只对有行事务有意义，
     * 逐回滚通知会淹没 catalog-only 噪声）。线程约束：单写者。</p>
     *
     * @param rec 中止记录
     */
    private void onAbort(WalRecord rec) {
        long xid = rec.xid() & 0xFFFFFFFFL;
        long top = effectiveTop(rec, xid);
        if (top != xid) {
            Bucket b = buckets.get(top);
            if (b != null) {
                b.abortedSubxids.add(xid);
            }
            return;
        }
        Bucket b = removeBucket(xid);
        if (b != null) {
            out.onAborted(new ChangeOutputListener.BatchAborted(xid));
        }
        terminalCleanup(xid);
    }

    /**
     * PREPARE：桶标记挂起（记 gid，零发射）——行值已在到达期物化，挂起仅悬置终态。
     *
     * <p>关键步骤：main 按 72B 文件头解 xid@8 / prepared_at@16 / gidlen@54 / gid@72；
     * gid 取 gidlen 字节并<b>剥尾 NUL</b>（真实 gidlen = strlen+1，twophase.c
     * SaveTransactionState 同面）；有桶则记 gid 并递增未决计数（超过护栏阈值 WARN 一次
     * ——终态悬置泄漏的观测信号）。边界与异常语义：main &lt; 72 或 gidlen 越界抛
     * ISE；无桶（无用户表行）仅跳过。线程约束：单写者。</p>
     *
     * @param rec 准备记录
     */
    private void onPrepare(WalRecord rec) {
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        if (rec.mainLen() < SIZE_OF_XACT_PREPARE) {
            throw new IllegalStateException("xl_xact_prepare main too short: " + rec.mainLen()
                    + " < " + SIZE_OF_XACT_PREPARE);
        }
        long xid = u32(raw, off + 8);
        int gidlen = u16(raw, off + 54);
        if (SIZE_OF_XACT_PREPARE + gidlen > rec.mainLen()) {
            throw new IllegalStateException("prepare gid 越界: gidlen=" + gidlen
                    + ", mainLen=" + rec.mainLen());
        }
        // 真实 gidlen = strlen+1 含尾 NUL（twophase.c SaveTransactionState 的
        // hdr->gidlen = strlen(gid) + 1）——剥尾 NUL 才得原 gid；无 NUL 形态（gidlen 即
        // strlen 的手造/异源字节）不受影响
        int gidStrLen = gidlen;
        if (gidStrLen > 0 && raw[off + SIZE_OF_XACT_PREPARE + gidStrLen - 1] == 0) {
            gidStrLen--;
        }
        String gid = new String(raw, off + SIZE_OF_XACT_PREPARE, gidStrLen, StandardCharsets.UTF_8);
        Bucket b = buckets.get(xid);
        if (b != null) {
            b.gid = gid;
            preparedPending++;
            if (preparedPending > PREPARED_PENDING_WARN_THRESHOLD && !preparedOverflowWarned) {
                preparedOverflowWarned = true;
                LOG.warn("未决 2PC 挂起桶数 {} 超过 {}——PREPARE 终态悬置泄漏的观测信号"
                        + "（正常业务下挂起窗口为秒级，持续增长指向终态记录丢失/消费停滞）",
                        preparedPending, PREPARED_PENDING_WARN_THRESHOLD);
            }
            LOG.info("两阶段事务挂起: xid={}, gid={}, 行数={}", xid, gid, b.rows.size());
        }
        terminalCleanup(xid);
    }

    /**
     * ABORT_PREPARED：弃挂起桶（onAborted 单事件）——归属键同 COMMIT_PREPARED
     * （main 的 twophase chunk xid 优先）。
     *
     * @param rec 两阶段回滚记录
     */
    private void onAbortPrepared(WalRecord rec) {
        CommitView c = parseCommitMain(rec);
        long xid = c.twophaseXid() != 0 ? c.twophaseXid() : rec.xid() & 0xFFFFFFFFL;
        Bucket b = removeBucket(xid);
        if (b != null) {
            out.onAborted(new ChangeOutputListener.BatchAborted(xid));
        }
        terminalCleanup(xid);
    }

    /**
     * 终态取桶的公共面：摘桶 + 未决 2PC 计数递减（挂起后被确认/回滚）+ 桶缺位的
     * DEBUG 观测（无用户表行的事务属常态——catalog-only/全过滤，不打 INFO 防刷屏，
     * DEBUG 供消费停滞排查时定位"记录到达但无桶"形态）。
     *
     * @param xid 终态归属事务 id
     * @return 摘出的桶；无桶为 null
     */
    private Bucket removeBucket(long xid) {
        Bucket b = buckets.remove(xid);
        if (b == null) {
            LOG.debug("终态记录无待决桶（事务无用户表行或已终结）: xid={}", xid);
        } else if (b.gid != null) {
            preparedPending--;
        }
        return b;
    }

    /**
     * ASSIGNMENT：子 xid → 顶层 xid 映射兜底（TOPLEVEL_XID 标记缺位时的归并通道，
     * spike 发现 25）——main = xtop u32@0 / nsubxacts i32@4 / subxacts u32[]@8。
     *
     * @param rec 归并记录
     */
    private void onAssignment(WalRecord rec) {
        if (rec.mainLen() < 8) {
            throw new IllegalStateException("xl_xact_assignment main too short: " + rec.mainLen());
        }
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        long top = u32(raw, off);
        int n = (int) u32(raw, off + 4);
        if (8 + 4L * n > rec.mainLen()) {
            throw new IllegalStateException("assignment subxacts 越界: n=" + n + ", mainLen=" + rec.mainLen());
        }
        for (int i = 0; i < n; i++) {
            subToTop.put(u32(raw, off + 8 + 4 * i), top);
        }
    }

    /**
     * heap（RM_HEAP）记录：INSERT/UPDATE/DELETE 解码行入桶；INPLACE 是 catalog 专用
     * 形态（用户表不走），静默忽略。
     *
     * <p>关键步骤：块链首块取 relfilenode → TableFilter 解析（缓存）；命中用户表则
     * 按 opcode 解码（新元组 data 路径优先 / FPW 镜像路径，前像从 main 尾 CONTAINS_OLD
     * 载荷）；miss 时若为 toast 关系转 chunk 采集。线程约束：单写者。</p>
     *
     * @param rec heap 记录
     */
    private void dispatchHeap(WalRecord rec) {
        if (rec.blocks().isEmpty()) {
            return;
        }
        int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
        if (op == HeapOps.XLOG_HEAP_INPLACE) {
            return;   // catalog 专用（pg_class 统计面等），用户表不变更语义
        }
        BlockRef blk = rec.blocks().get(0);
        long relNode = blk.relNode();
        Resolved res = resolveCached(relNode);
        if (res == null || res.meta() == null) {
            collectToastChunks(rec, blk, relNode);
            return;
        }
        long origin = rec.xid() & 0xFFFFFFFFL;
        if (origin == 0) {
            return;
        }
        long top = effectiveTop(rec, origin);
        switch (op) {
            case HeapOps.XLOG_HEAP_INSERT -> {
                HeapViews.HeapInsertView v = HeapViews.HeapInsertView.parse(rec, layout);
                appendRow(top, origin, res, ChangeOutputListener.DmlKind.INSERT,
                        null, tupleFromBlock(rec, blk, v.offnum(), res.kinds()));
            }
            case HeapOps.XLOG_HEAP_UPDATE, HeapOps.XLOG_HEAP_HOT_UPDATE -> {
                HeapViews.HeapUpdateView v = HeapViews.HeapUpdateView.parse(rec, layout);
                Object[] before = (v.flags() & HeapOps.XLH_UPDATE_CONTAINS_OLD) != 0
                        ? decoder.decodePayload(rec.raw(), rec.mainOff() + layout.sizeOfHeapUpdate(), res.kinds())
                        : null;
                appendRow(top, origin, res, ChangeOutputListener.DmlKind.UPDATE,
                        before, tupleFromBlock(rec, blk, v.newOffnum(), res.kinds()));
            }
            case HeapOps.XLOG_HEAP_DELETE -> {
                HeapViews.HeapDeleteView v = HeapViews.HeapDeleteView.parse(rec, layout);
                Object[] before = (v.flags() & HeapOps.XLH_DELETE_CONTAINS_OLD) != 0
                        ? decoder.decodePayload(rec.raw(), rec.mainOff() + layout.sizeOfHeapDelete(), res.kinds())
                        : null;
                appendRow(top, origin, res, ChangeOutputListener.DmlKind.DELETE, before, null);
            }
            default -> { /* 未预期 opcode 静默忽略 */ }
        }
    }

    /**
     * heap2（RM_HEAP2）记录：仅 MULTI_INSERT 是行承载面（PRUNE 三态等与行发射无关，
     * 属页维护）——命中用户表逐 entry 解码，toast 关系转 chunk 采集。
     *
     * @param rec heap2 记录
     */
    private void dispatchHeap2(WalRecord rec) {
        int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
        if (op != HeapOps.XLOG_HEAP2_MULTI_INSERT || rec.blocks().isEmpty()) {
            return;
        }
        BlockRef blk = rec.blocks().get(0);
        long relNode = blk.relNode();
        Resolved res = resolveCached(relNode);
        if (res == null || res.meta() == null) {
            collectToastChunks(rec, blk, relNode);
            return;
        }
        long origin = rec.xid() & 0xFFFFFFFFL;
        if (origin == 0) {
            return;
        }
        long top = effectiveTop(rec, origin);
        HeapViews.MultiInsertView v = HeapViews.MultiInsertView.parse(rec, layout);
        if (blk.hasData()) {
            byte[] raw = rec.raw();
            int off = blk.dataOff();
            for (int i = 0; i < v.ntuples(); i++) {
                if (off + 2 > blk.dataOff() + blk.dataLen()) {
                    throw new IllegalStateException("multi_insert entry 越界: i=" + i);
                }
                int datalen = u16(raw, off);
                if (off + 2 + datalen > blk.dataOff() + blk.dataLen()) {
                    throw new IllegalStateException("multi_insert entry 越界: i=" + i + ", datalen=" + datalen);
                }
                appendRow(top, origin, res, ChangeOutputListener.DmlKind.INSERT,
                        null, decoder.decodeEntry(raw, off, res.kinds()));
                off += 2 + datalen;
            }
        } else {
            byte[] page = requireImage(rec, blk);
            for (int i = 0; i < v.ntuples(); i++) {
                int lp = PageImages.linePointerOffset(page, v.offsetAt(i));
                appendRow(top, origin, res, ChangeOutputListener.DmlKind.INSERT,
                        null, decoder.decodePageTuple(page, lp, res.kinds()));
            }
        }
    }

    /**
     * toast 关系上的行记录采集为 chunk（INSERT 单行 / MULTI_INSERT 逐 entry，三列
     * 词典 oid/int4/bytea）——chunk_data 是 plain varlena（chunk ≤1996B 恒行内），
     * external/compressed 拒绝面不触发。非 toast 关系、非行承载 opcode 或无块 data
     * （INIT_PAGE 镜像形态）静默跳过——缺 chunk 落回查兜底。
     *
     * @param rec     heap/heap2 记录
     * @param blk     首块（relfilenode 归集键）
     * @param relNode relfilenode
     */
    private void collectToastChunks(WalRecord rec, BlockRef blk, long relNode) {
        if (toast == null || !isToastRelation(relNode)) {
            return;
        }
        int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
        if (op != HeapOps.XLOG_HEAP_INSERT && op != HeapOps.XLOG_HEAP2_MULTI_INSERT) {
            return;   // chunk 删除（DELETE/PRUNE）不采集——归集面由终态全清承担
        }
        if (!blk.hasData()) {
            return;   // INIT_PAGE+镜像无 data 形态：chunk 载荷在镜像内，采集面跳过
            //（对齐 dispatchHeap2 的 hasData 判定；缺 chunk 落 resolveExternal 回查兜底）
        }
        if (op == HeapOps.XLOG_HEAP_INSERT) {
            Object[] row = decoder.decodePayload(rec.raw(), blk.dataOff(), TOAST_CHUNK_KINDS);
            toast.onChunkRow(relNode, row);
            return;
        }
        HeapViews.MultiInsertView v = HeapViews.MultiInsertView.parse(rec, layout);
        byte[] raw = rec.raw();
        int off = blk.dataOff();
        for (int i = 0; i < v.ntuples(); i++) {
            if (off + 2 > blk.dataOff() + blk.dataLen()) {
                throw new IllegalStateException("toast multi_insert entry 越界: i=" + i);
            }
            int datalen = u16(raw, off);
            if (off + 2 + datalen > blk.dataOff() + blk.dataLen()) {
                throw new IllegalStateException("toast multi_insert entry 越界: i=" + i + ", datalen=" + datalen);
            }
            toast.onChunkRow(relNode, decoder.decodeEntry(raw, off, TOAST_CHUNK_KINDS));
            off += 2 + datalen;
        }
    }

    /**
     * 解析关系到表身份 + 列词典（缓存面）——命中直接应答；miss 时先探字典三表
     * （pg_class/pg_attribute/pg_namespace 上的 heap 记录 = DDL 类变更信号 →
     * <b>全清缓存</b>，策略依据见类 javadoc），再走 {@link TableFilter} 完整解析
     * （miss 亦缓存，防 toast/索引关系逐记录重扫）。
     *
     * <p>边界与异常语义：字典三表返回 null（调用方按跳过处理，不缓存——三表记录
     * 本身不产生行）。线程约束：单写者。</p>
     *
     * @param relNode relfilenode（heap 块形态）
     * @return 解析结果（meta=null 表示非用户表）；字典三表命中为 null
     */
    private Resolved resolveCached(long relNode) {
        Resolved hit = metaCache.get(relNode);
        if (hit != null) {
            return hit;
        }
        OptionalLong oid = snapshot.relOidOf(relNode);
        if (oid.isPresent() && SHAPE_CATALOG_OIDS.contains(oid.getAsLong())) {
            metaCache.clear();
            LOG.debug("字典表 heap 记录到达，TableMeta 缓存全清: oid={}", oid.getAsLong());
            return null;
        }
        TableMeta meta = filter.resolve(relNode).orElse(null);
        Resolved res = new Resolved(meta, meta == null ? null : kindsOf(meta));
        metaCache.put(relNode, res);
        return res;
    }

    /**
     * relfilenode 是否 toast 关系（relkind 't'）——判定结果缓存（toast chunk 流量
     * 大，逐记录 relOidOf+relkindOf 两趟扫不可接受）。
     *
     * @param relNode relfilenode
     * @return true = toast 关系
     */
    private boolean isToastRelation(long relNode) {
        Boolean hit = toastRels.get(relNode);
        if (hit != null) {
            return hit;
        }
        boolean isToast = false;
        OptionalLong oid = snapshot.relOidOf(relNode);
        if (oid.isPresent()) {
            isToast = snapshot.relkindOf(oid.getAsLong()).map("t"::equals).orElse(false);
        }
        toastRels.put(relNode, isToast);
        return isToast;
    }

    /**
     * 解码新元组：块 data 路径优先（WAL 载荷形态 {@code [xl_heap_header 5B][...]}），
     * 无 data 走 FPW 镜像路径（PageImages 重建页 + 行指针 + 页内完整 tuple 解码）。
     *
     * @param rec    记录
     * @param blk    新元组所在块（heap 家族块 id 0；UPDATE 为新页）
     * @param offnum 元组行号（data 缺位时的镜像路径锚）
     * @param kinds  列词典
     * @return 值数组（长度 natts）
     * @throws IllegalStateException 块既无 data 又无镜像（不可解码形态）
     */
    private Object[] tupleFromBlock(WalRecord rec, BlockRef blk, int offnum, String[] kinds) {
        if (blk.hasData()) {
            return decoder.decodePayload(rec.raw(), blk.dataOff(), kinds);
        }
        byte[] page = requireImage(rec, blk);
        int lp = PageImages.linePointerOffset(page, offnum);
        return decoder.decodePageTuple(page, lp, kinds);
    }

    /**
     * 重建块的 FPW 页镜像（无 data 路径的解码前提）。
     *
     * @param rec 记录
     * @param blk 目标块
     * @return 重建页（洞区零字节）
     * @throws IllegalStateException 块不带镜像
     */
    private byte[] requireImage(WalRecord rec, BlockRef blk) {
        if (!blk.hasImage()) {
            throw new IllegalStateException(String.format(
                    "heap 记录块既无 data 又无镜像，不可解码: rmid=%d info=0x%02x block=%d",
                    rec.rmid(), rec.info(), blk.blockNo()));
        }
        return PageImages.rebuild(rec, blk);
    }

    /**
     * 行入桶：值渲染（静态 {@link DiskValueRenderer}）+ RowChange 组装 + 桶记账
     * expectedChanges++（aborted 过滤前口径）。
     *
     * <p>关键步骤：前/后像按 dml 裁剪（INSERT 前 null / DELETE 后 null / UPDATE
     * 双像——replica identity 面由 flags 已定）；旧元组 atts 少于列词典时尾列补
     * null（ADD COLUMN 前的存量形态）。线程约束：单写者。</p>
     *
     * @param top    归并后的顶层事务 id（桶键）
     * @param origin 行来源 xid（记录头 xid——aborted 过滤键）
     * @param res    表解析结果（meta + kinds）
     * @param dml    行操作种类
     * @param before 前像值数组（无为 null）
     * @param after  后像值数组（无为 null）
     */
    private void appendRow(long top, long origin, Resolved res,
                           ChangeOutputListener.DmlKind dml, Object[] before, Object[] after) {
        TableMeta meta = res.meta();
        Map<String, Object> beforeMap = before == null ? null : renderRow(meta, before);
        Map<String, Object> afterMap = after == null ? null : renderRow(meta, after);
        buckets.computeIfAbsent(top, Bucket::new)
                .rows.add(new RowEntry(origin, new ChangeOutputListener.RowChange(meta, dml, beforeMap, afterMap)));
    }

    /**
     * 值数组 → 列名→渲染值 map（列序保持 attnum 序——LinkedHashMap）：dropped 列恒
     * null（占位语义），其余经静态 {@link DiskValueRenderer#render} 取 PG text 形态。
     *
     * @param meta 表身份 + 列序
     * @param vals 解码值数组（可短于列数——尾列补 null）
     * @return 有序值 map
     */
    private static Map<String, Object> renderRow(TableMeta meta, Object[] vals) {
        Map<String, Object> m = new LinkedHashMap<>();
        List<ColumnMeta> cols = meta.columns();
        for (int i = 0; i < cols.size(); i++) {
            ColumnMeta col = cols.get(i);
            Object decoded = i < vals.length ? vals[i] : null;
            m.put(col.name(), col.dropped() ? null : DiskValueRenderer.render(decoded, col.typeOid()));
        }
        return m;
    }

    /**
     * 桶批量发射：Begin（expected=桶记账终值）→ 过滤后行（来源 xid ∈ abortedSubxids
     * 的行扣减）→ End（emitted=过滤后实付，expected 同 Begin 终值）。
     *
     * <p>边界与异常语义：全过滤（emitted=0）仍发 Begin/End 骨架——桶存在即事务
     * 曾有用户表行，计数面下游可对齐。线程约束：单写者（回调与喂入同线程）。</p>
     *
     * @param b         待发射桶
     * @param twoPhase  两阶段形态标记
     * @param gid       两阶段全局事务名（非两阶段 null）
     * @param commitLsn 提交记录 LSN
     * @param endLsn    提交记录末尾 LSN（LSN+MAXALIGN(totLen)）
     * @param commitTs  提交时间戳
     */
    private void emitBucket(Bucket b, boolean twoPhase, String gid, long commitLsn, long endLsn, Instant commitTs) {
        long expected = b.rows.size();
        out.onBegin(new ChangeOutputListener.BatchBegin(b.xid, twoPhase, gid, commitLsn, endLsn, commitTs, expected));
        long emitted = 0;
        for (RowEntry e : b.rows) {
            if (b.abortedSubxids.contains(e.originXid())) {
                continue;
            }
            out.onRow(e.row());
            emitted++;
        }
        out.onEnd(new ChangeOutputListener.BatchEnd(b.xid, emitted, expected));
        emittedBuckets++;
        emittedRows += emitted;
    }

    /**
     * DML 观测面：已发射事务桶数（提交时批量发射的 Begin-End 批次数）。
     *
     * <p>单写者递增（发射线程）、volatile 读——Main 周期 smoke 行与宿主轮询安全。
     * 回滚/无行事务不计。</p>
     *
     * @return 已发射桶数（会话累计）
     */
    public long emittedBuckets() {
        return emittedBuckets;
    }

    /**
     * DML 观测面：已发射行数（aborted 过滤后的实付口径）。
     *
     * <p>单写者递增、volatile 读；与 {@link #emittedBuckets()} 同批更新。</p>
     *
     * @return 已发射行数（会话累计）
     */
    public long emittedRows() {
        return emittedRows;
    }

    /**
     * 事务终态后的公共清理面：TOAST chunk 归集全清（控制器裁定）+ subToTop 按顶层
     * 剪枝（该事务的归并映射随终态失效，防会话期无界增长）。
     *
     * @param top 终态事务的顶层 xid
     */
    private void terminalCleanup(long top) {
        if (toast != null) {
            toast.clear();
        }
        Iterator<Long> it = subToTop.values().iterator();
        while (it.hasNext()) {
            if (it.next() == top) {
                it.remove();
            }
        }
    }

    /**
     * 记录的事务归属：TOPLEVEL_XID 标记优先（spike 发现 25），缺位走 ASSIGNMENT
     * 映射兜底，再缺位即自身（顶层记录）。
     *
     * @param rec 记录
     * @param xid 记录头 xid（候选子 xid）
     * @return 归并后的顶层事务 id
     */
    private long effectiveTop(WalRecord rec, long xid) {
        if (rec.toplevelXid() != 0) {
            return rec.toplevelXid() & 0xFFFFFFFFL;
        }
        Long mapped = subToTop.get(xid);
        return mapped == null ? xid : mapped;
    }

    /**
     * 解析 commit/abort/两阶段确认记录的 main data——xact_time 即 Instant、块链走读
     * 至 twophase chunk（xid + gid），中间块（dbinfo/subxacts/relfilelocators/
     * stats/invals）只跳过不消费。
     *
     * <p>关键步骤：i64@0 → commitTs；HAS_INFO 位（info 0x80）解 xinfo u32 并按位序
     * 走块（尺寸锚见类 javadoc）；TWOPHASE 位解 xid u32，GID 位解 NUL 结尾字符串。
     * 边界与异常语义：main &lt; 8 抛 ISE；块链越界由数组越界裸抛（上游记录走读
     * 自洽前提，同 v1 风格）。线程约束：纯读。</p>
     *
     * @param rec commit/abort 形态记录
     * @return 解析面（commitTs / twophaseXid[无=0] / gid[无=null]）
     */
    private CommitView parseCommitMain(WalRecord rec) {
        if (rec.mainLen() < 8) {
            throw new IllegalStateException("xact commit/abort main too short: " + rec.mainLen());
        }
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        Instant commitTs = instantOfPgMicros(i64(raw, off));
        long xinfo = 0;
        int cur = off + 8;
        if ((rec.info() & HeapOps.XLOG_XACT_HAS_INFO) != 0) {
            xinfo = u32(raw, cur);
            cur += 4;
        }
        if ((xinfo & XINFO_HAS_DBINFO) != 0) {
            cur += 8;
        }
        if ((xinfo & XINFO_HAS_SUBXACTS) != 0) {
            cur += 4 + 4 * u32(raw, cur);
        }
        if ((xinfo & XINFO_HAS_RELFILELOCATORS) != 0) {
            cur += 4 + 12 * u32(raw, cur);
        }
        if ((xinfo & XINFO_HAS_DROPPED_STATS) != 0) {
            cur += 4 + 16 * u32(raw, cur);
        }
        if ((xinfo & XINFO_HAS_INVALS) != 0) {
            cur += 4 + 16 * u32(raw, cur);
        }
        long twophaseXid = 0;
        String gid = null;
        if ((xinfo & XINFO_HAS_TWOPHASE) != 0) {
            twophaseXid = u32(raw, cur);
            cur += 4;
            if ((xinfo & XINFO_HAS_GID) != 0) {
                gid = nulTerminated(raw, cur);
            }
        }
        return new CommitView(commitTs, twophaseXid, gid);
    }

    /**
     * 表的列词典派生：typeOid → {@link TupleDecoder} kind 映射（词汇面 =
     * {@link DecodeKinds#forTypeOid} 渲染矩阵全集——Task 7 硬前置①扩容后单源派生，
     * 不在本类复刻 switch；dropped 列 → "dropped" 零消耗占位）。
     *
     * <p>边界与异常语义：词典外 oid（enum/域/jsonb/组合类型等动态格式）抛 ISE
     * fail-fast（包装 {@link DecodeKinds#forTypeOid} 的拒绝并补列名上下文）：继续
     * 解码只会列错位，静默产出错值。线程约束：纯函数。</p>
     *
     * @param meta 表身份 + 列序
     * @return 逐列 kind 词典
     * @throws IllegalStateException 列类型超出解码词典面
     */
    private static String[] kindsOf(TableMeta meta) {
        List<ColumnMeta> cols = meta.columns();
        String[] kinds = new String[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            ColumnMeta col = cols.get(i);
            if (col.dropped()) {
                kinds[i] = "dropped";
                continue;
            }
            try {
                kinds[i] = DecodeKinds.forTypeOid(col.typeOid());
            } catch (IllegalStateException e) {
                throw new IllegalStateException("列 " + col.name() + " 的类型 oid=" + col.typeOid()
                        + " 超出解码词典面（enum/域/jsonb 等动态格式属矩阵外），继续解码只会列错位", e);
            }
        }
        return kinds;
    }

    /**
     * 记录末尾 LSN：LSN + MAXALIGN(totLen)（WAL 记录按 MAXALIGN 占位——输出前沿
     * 推进锚与 v1 catalog 施加前沿同口径）。
     *
     * @param rec 记录
     * @return 末尾 LSN
     */
    private static long endLsnOf(WalRecord rec) {
        return rec.lsn() + (((long) rec.totLen() + 7) / 8 * 8);
    }

    /**
     * PG 微秒时间戳（epoch 2000）→ Instant（floorDiv/floorMod 保负值正确——BC 侧）。
     *
     * @param micros 自 2000-01-01T00:00:00Z 起微秒
     * @return Instant
     */
    private static Instant instantOfPgMicros(long micros) {
        long epochMicros = EPOCH_2000_MICROS + micros;
        return Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * 1000L);
    }

    /**
     * 自 pos 起 读 NUL 结尾字符串（gid）——NUL 缺失时读到 main 边界由调用方数组
     * 越界裸抛（走读自洽前提）。
     *
     * @param raw 记录字节
     * @param pos 起点
     * @return UTF-8 字符串
     */
    private static String nulTerminated(byte[] raw, int pos) {
        int end = pos;
        while (raw[end] != 0) {
            end++;
        }
        return new String(raw, pos, end - pos, StandardCharsets.UTF_8);
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32（无符号 long 语义）。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 32 位值
     */
    private static long u32(byte[] b, int o) {
        return (b[o] & 0xFFL) | ((b[o + 1] & 0xFFL) << 8) | ((b[o + 2] & 0xFFL) << 16) | ((b[o + 3] & 0xFFL) << 24);
    }

    /**
     * 就地读 little-endian i64（XACT 时间戳）。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 64 位值
     */
    private static long i64(byte[] b, int o) {
        return u32(b, o) | (u32(b, o + 4) << 32);
    }

    /**
     * commit/abort 记录 main 的解析面。
     *
     * @param commitTs    提交/中止时间戳（xact_time@0）
     * @param twophaseXid 两阶段归属 xid（twophase chunk；无块为 0）
     * @param gid         两阶段全局事务名（NUL 结尾；无块为 null）
     */
    private record CommitView(Instant commitTs, long twophaseXid, String gid) {
    }

    /**
     * TableFilter 解析缓存条目：表身份 + 派生列词典（miss 时两者皆 null——缓存
     * "已判非用户表"语义，防 toast/索引关系逐记录重扫）。
     *
     * @param meta  表身份（非用户表为 null）
     * @param kinds 逐列解码词典（非用户表为 null）
     */
    private record Resolved(TableMeta meta, String[] kinds) {
    }

    /**
     * 待决事务桶：行到达期累积（RowEntry 携带来源 xid 供 aborted 过滤），终态记录
     * 发射/弃。挂起（PREPARE）= 桶留存 + gid 悬置。
     */
    private static final class Bucket {
        final long xid;
        final List<RowEntry> rows = new ArrayList<>();
        final Set<Long> abortedSubxids = new HashSet<>();
        String gid;

        /**
         * 以桶键（顶层事务 id）建桶。
         *
         * @param xid 顶层事务 id
         */
        Bucket(long xid) {
            this.xid = xid;
        }
    }

    /**
     * 桶内行条目：RowChange + 来源 xid（记录头 xid——子事务行即子 xid，aborted
     * 过滤的私有记账键，不泄漏进输出契约——预检 #5 裁定）。
     *
     * @param originXid 行来源事务 id
     * @param row       行变更事件
     */
    private record RowEntry(long originXid, ChangeOutputListener.RowChange row) {
    }
}
