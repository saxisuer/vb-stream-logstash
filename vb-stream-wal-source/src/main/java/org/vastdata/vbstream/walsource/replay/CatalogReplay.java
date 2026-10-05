package org.vastdata.vbstream.walsource.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.BlockRef;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.HeapViews;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.PageImages;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;

/**
 * catalog ctid 重放引擎（纯逻辑，spike WalParseSpike 的 heapEvents / replayPrune /
 * replayInplace / replayCatalogs 正式化）——把 heap 家族 WAL 记录提取为
 * {@link HeapEvent} 行事件并施加到 {@link CatalogStores}，维护 ctid 键控行字典、
 * raw tail 存储与 tracked 双 ctid。
 *
 * <p>对 spike 的两处裁定差异（spec §6）：① {@code pg_read_binary_file} 页读兜底
 * <strong>整体删除</strong>——截断更新优先值编码重建（prefix ≤ 88 由已知
 * {@link CatalogRow.ClassRow} 重编码，发现 24），越界或有 rawTail 走 splice，两者
 * 皆无时（healer 已注入且为 pg_class 面）走 ②自愈、否则 skip + 计数
 * skippedTruncated（窗口外噪声不抛）；② tracked 断链自愈（Task 12 落地，Task 13
 * 收敛）——未知 oldCtid 的截断更新经 {@link SelfHealer} 按<strong>记录 new 位 ctid
 * 寻址探测目录末态行</strong>，行仍居该位即为本记录施加后的精确状态，整行采纳并修复
 * tracked（Task 12 的候选枚举 + 中段值校验形态经对抗性 IT 实证存在时序身份混窗，
 * 已废——见 SelfHealer javadoc）；healer 未注入（null）保留纯 skip 行为。</p>
 *
 * <p>线程约束：实例持 layout + decoder（decoder 无共享可变状态），但通过
 * {@link CatalogStores} 的重放入口按<strong>单线程</strong>假设运行（spec §3 单线程
 * 直通）；纯提取面（{@link #heapEvents} 四参档）为实例态纯函数，任意线程并发安全。
 * block 匹配按 relNode 单值（spike 按 spc/db/relNode 三件组；本签名无 spc/db——
 * 同库引导下等价，跨库 relfilenode 碰撞为理论残留，Task 11 接线时可收紧）。</p>
 */
public final class CatalogReplay {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogReplay.class);

    /** heapam_xlog.h：xl_heap_update 新 tuple 前缀截断位（XLH_UPDATE_PREFIX_FROM_OLD）。 */
    private static final int XLH_UPDATE_PREFIX_FROM_OLD = 0x20;

    /** heapam_xlog.h：xl_heap_update 新 tuple 后缀截断位（XLH_UPDATE_SUFFIX_FROM_OLD）。 */
    private static final int XLH_UPDATE_SUFFIX_FROM_OLD = 0x40;

    /** HeapTupleHeader 的 t_bits 偏移（offsetof=23，截断重建的位图区起点）。 */
    private static final int TUPLE_BITS_OFFSET = 23;

    // 派生档九槽位（与 pgClassKinds() 词典列序一致，位号同 CatalogRow.ClassRow 的 IX_* 私有镜像——两处须同步；
    // 词典前八槽 V17/V18 同序，reltoastrelid 槽位随版本漂移经 layout.classToastRelidIndex() 取）
    /** 词典槽位：oid（列 1）。 */
    private static final int IX_CLASS_OID = 0;

    /** 词典槽位：relname（列 2）。 */
    private static final int IX_CLASS_RELNAME = 1;

    /** 词典槽位：relnamespace（列 3）。 */
    private static final int IX_CLASS_RELNAMESPACE = 2;

    /** 词典槽位：reltype（列 4）。 */
    private static final int IX_CLASS_RELTYPE = 3;

    /** 词典槽位：reloftype（列 5）。 */
    private static final int IX_CLASS_RELOFTYPE = 4;

    /** 词典槽位：relowner（列 6）。 */
    private static final int IX_CLASS_RELOWNER = 5;

    /** 词典槽位：relam（列 7）。 */
    private static final int IX_CLASS_RELAM = 6;

    /** 词典槽位：relfilenode（列 8，数据区偏移 88）。 */
    private static final int IX_CLASS_RELFILENODE = 7;

    // attr 面精确采纳的五槽位（与 pgAttributeKinds() 词典列序一致，位号同 CatalogRow.AttrRow 的 IX_* 私有镜像——两处须同步；
    // 前五槽 V17/V18 同序，attisdropped 槽位随版本漂移经 layout.attrDroppedIndex() 取）
    /** 词典槽位：attrelid（列 1）。 */
    private static final int IX_ATTR_ATTRELID = 0;

    /** 词典槽位：attname（列 2）。 */
    private static final int IX_ATTR_ATTNAME = 1;

    /** 词典槽位：atttypid（列 3）。 */
    private static final int IX_ATTR_ATTTYPEID = 2;

    /** 词典槽位：attnum（列 5）。 */
    private static final int IX_ATTR_ATTNUM = 4;

    private final WalLayout layout;
    private final TupleDecoder decoder;

    /** 截断自愈校验器（null = 禁用：未知 oldCtid 保留 skip + 计数行为）。 */
    private final SelfHealer healer;

    /** per-oid 连续精确采纳拒绝计数（Task 13 审查裁定 3 的 stale 轻量复活）——单写者。 */
    private final Map<Long, Integer> consecutiveAdoptSkips = new HashMap<>();

    /** 连续采纳拒绝的 stale 登记门槛（控制器裁定 3）。 */
    private static final int ADOPT_SKIP_STALE_THRESHOLD = 3;

    /**
     * 构造重放引擎（自愈禁用档）。
     *
     * @param layout  版本布局描述符（截断阈值 88、INPLACE 偏移 88/112、两目录词典均取自它）
     * @param decoder 磁盘格式 tuple 解码器（实例无状态，可与他处共享）
     */
    public CatalogReplay(WalLayout layout, TupleDecoder decoder) {
        this(layout, decoder, null);
    }

    /**
     * 构造重放引擎（全参档）。
     *
     * @param layout  版本布局描述符（截断阈值 88、INPLACE 偏移 88/112、两目录词典均取自它）
     * @param decoder 磁盘格式 tuple 解码器（实例无状态，可与他处共享）
     * @param healer  截断自愈探测面（null 保留 skip 行为；非 null 时未知 oldCtid 的
     *                截断更新走 ctid 寻址精确采纳——pg_class 面与 pg_attribute 面）
     */
    public CatalogReplay(WalLayout layout, TupleDecoder decoder, SelfHealer healer) {
        this.layout = layout;
        this.decoder = decoder;
        this.healer = healer;
    }

    /**
     * 物理 ctid 折叠为 map 键：{@code (block << 16) | offnum}（spike 同式）——
     * offnum 为 u16（行号上限 65535 恰满 16 bit，无重叠）。
     *
     * @param block  块号
     * @param offnum 行号（1-based）
     * @return ctid 键
     */
    public static long ctidKey(int block, int offnum) {
        return ((long) block << 16) | offnum;
    }

    /**
     * 提取一条 WAL 记录在 watched relfilenode 上的 heap 级行事件（任务书签名档：
     * 无 pg_class 行查找、无指标挂点——截断更新仅 rawTail splice 可用，旧行值与
     * tail 皆无即静默 skip）。
     *
     * <p>语义与全量档 {@link #heapEvents(WalRecord, long, String[], Map, LongFunction,
     * CatalogStores.CatalogMetrics)} 一致（含 rawTailStore 维护），见其 javadoc。</p>
     *
     * @param r                 走读完成的记录
     * @param watchedRelfilenode watched 关系的 relfilenode（0 恒不匹配）
     * @param kinds             逐列解码词典（如 layout 的两目录词典）
     * @param rawTailStore      raw tail 存储（ctid 键 → 自 offset 23 起字节；本方法
     *                          增量维护：INS/UPD 落新键、DEL/UPD 删旧键；null 则不维护）
     * @return 事件列表（记录不属于该关系或全 skip 为空表）
     */
    public List<HeapEvent> heapEvents(WalRecord r, long watchedRelfilenode, String[] kinds, Map<Long, ?> rawTailStore) {
        return heapEvents(r, watchedRelfilenode, kinds, rawTailStore, null, null, null);
    }

    /**
     * 提取 heap 级行事件（全量档，自愈禁用）：语义与七参档
     * {@link #heapEvents(WalRecord, long, String[], Map, LongFunction,
     * CatalogStores.CatalogMetrics, CatalogStores)} 完全一致（healStores=null，
     * 旧行值与 tail 皆无即 skip），见其 javadoc。
     *
     * @param r                 走读完成的记录
     * @param watchedRelfilenode watched 关系 relfilenode（0 恒不匹配）
     * @param kinds             逐列解码词典
     * @param rawTailStore      raw tail 存储（维护契约见四参档；null 不维护）
     * @param classRowLookup    pg_class 行字典查找（null 则截断仅 rawTail splice）
     * @param metrics           指标容器（skip 计数；null 不计）
     * @return 事件列表
     */
    public List<HeapEvent> heapEvents(WalRecord r, long watchedRelfilenode, String[] kinds,
            Map<Long, ?> rawTailStore, LongFunction<CatalogRow.ClassRow> classRowLookup,
            CatalogStores.CatalogMetrics metrics) {
        return heapEvents(r, watchedRelfilenode, kinds, rawTailStore, classRowLookup, metrics, null);
    }

    /**
     * 提取 heap 级行事件（全量档 + 自愈档）——INS / DEL / UPD（含截断重建）/
     * MULTI_INSERT，data 与 FPW image 双路径（发现 14：catalogs 在 FPI 时不记
     * tuple data；发现 15：镜像是变更后页状态，offnum 行直接在页内）。
     *
     * <p>关键步骤：rmid 筛（heap/heap2）→ 块链按 relNode 匹配 watched（fork=0）→
     * 按 opcode 分支：INS data 路径 [xl_heap_header 5B][tail] 解码并落 tail、image
     * 路径重建页后按行号取 ItemId 解码（无 tail）；DEL 仅事件 + 删 tail；UPD 以
     * HeapUpdateView 取 old/new 行号（旧页块按 fork 过滤选取，spike VM 块教训），
     * 截断位组置位时先试值编码（classRowLookup 命中且 prefix ≤ 88）、再试 rawTail
     * splice、皆无时若 healStores 非 null（pg_class 面 + healer 已注入）走
     * {@code selfHealTruncated} ctid 寻址精确采纳（spec §6② 的收敛形态：按记录
     * new 位探测 JDBC 末态行整行采纳——末态回填；单次路径内完成——本方法
     * 非幂等，不重复调用），否则 skip（metrics 计数 skippedTruncated，UPD 必删旧
     * tail——spike 教训）；MULTI_INSERT 逐 entry（2 对齐起点、datalen 只计 tail
     * 字节，spike 锚）或 image 路径按 offsets（INIT_PAGE 时隐含 i+1，发现 3）。
     * INPLACE 不在本方法面（返回空）——原地改写需要行字典就地更新与 tail 失效，
     * 由 {@link #applyInplace} 承载。</p>
     *
     * <p>边界与异常语义：opcode/结构错配由 HeapViews 各工厂 ISE fail-fast；块无
     * data 且无 image（协议违约）静默无事件；截断 skip 不抛（自愈判否同不抛——
     * 连续失败只计数/标 stale）。线程约束：纯提取 + 对 rawTailStore/classRowLookup
     * /healStores 的单写者假设（stores 生命周期内仅重放线程调用）。</p>
     *
     * @param r                 走读完成的记录
     * @param watchedRelfilenode watched 关系 relfilenode（0 恒不匹配）
     * @param kinds             逐列解码词典
     * @param rawTailStore      raw tail 存储（维护契约见四参档；null 不维护）
     * @param classRowLookup    pg_class 行字典查找（值编码重建的旧行面；<strong>非
     *                          null 即约定 kinds 为 pgClassKinds()</strong>，null 则
     *                          截断仅 rawTail splice）
     * @param metrics           指标容器（skip 计数；null 不计）
     * @param healStores        自愈状态容器（tracked 双 ctid/候选行字典/staleOids；
     *                          <strong>非 null 即约定 watchedRelfilenode 为
     *                          pg_class 且 kinds 为 pg_class 词典</strong>——仅
     *                          {@link #applyCatalogRecord} 的 pg_class 腿传入；
     *                          null 则旧行值与 tail 皆无即 skip）
     * @return 事件列表
     */
    public List<HeapEvent> heapEvents(WalRecord r, long watchedRelfilenode, String[] kinds,
            Map<Long, ?> rawTailStore, LongFunction<CatalogRow.ClassRow> classRowLookup,
            CatalogStores.CatalogMetrics metrics, CatalogStores healStores) {
        List<HeapEvent> out = new ArrayList<>();
        if (r.rmid() != HeapOps.RM_HEAP_ID && r.rmid() != HeapOps.RM_HEAP2_ID) {
            return out;
        }
        BlockRef b0 = findHeapBlock(r, watchedRelfilenode);
        if (b0 == null) {
            return out;
        }
        Map<Long, byte[]> tails = tails(rawTailStore);
        int op = r.info() & HeapOps.XLOG_XACT_OPMASK;
        if (r.rmid() == HeapOps.RM_HEAP_ID && op == HeapOps.XLOG_HEAP_INSERT) {
            HeapViews.HeapInsertView view = HeapViews.HeapInsertView.parse(r, layout);
            long key = ctidKey(b0.blockNo(), view.offnum());
            if (b0.hasData()) {
                out.add(new HeapEvent(HeapEvent.INS, 0, key,
                        decoder.decodePayload(r.raw(), b0.dataOff(), kinds)));
                if (tails != null) {
                    tails.put(key, tailOf(r.raw(), b0.dataOff(), b0.dataLen()));
                }
            } else if (b0.hasImage()) {
                byte[] page = PageImages.rebuild(r, b0);
                int lpOff = PageImages.linePointerOffset(page, view.offnum());
                out.add(new HeapEvent(HeapEvent.INS, 0, key,
                        decoder.decodePageTuple(page, lpOff, kinds)));
            }
        } else if (r.rmid() == HeapOps.RM_HEAP_ID && op == HeapOps.XLOG_HEAP_DELETE) {
            HeapViews.HeapDeleteView view = HeapViews.HeapDeleteView.parse(r, layout);
            long key = ctidKey(b0.blockNo(), view.offnum());
            if (tails != null) {
                tails.remove(key);
            }
            out.add(new HeapEvent(HeapEvent.DEL, key, 0, null));
        } else if (r.rmid() == HeapOps.RM_HEAP_ID
                && (op == HeapOps.XLOG_HEAP_UPDATE || op == HeapOps.XLOG_HEAP_HOT_UPDATE)) {
            HeapViews.HeapUpdateView view = HeapViews.HeapUpdateView.parse(r, layout);
            long oldCtid = ctidKey(view.oldBlockNo(), view.oldOffnum());
            long newCtid = ctidKey(b0.blockNo(), view.newOffnum());
            if (b0.hasData()) {
                Object[] row;
                byte[] tail;
                if ((view.flags() & HeapOps.XLH_UPDATE_TRUNCATION) != 0) {
                    Reconstruction rc = reconstructTruncatedUpdate(
                            r, b0, oldCtid, newCtid, kinds, tails, classRowLookup, metrics, healStores);
                    if (rc == null) {
                        return out;    // 旧行值与 raw tail 皆无：窗口外噪声，skip 不抛
                    }
                    row = rc.row();
                    tail = rc.tail();
                } else {
                    tail = tailOf(r.raw(), b0.dataOff(), b0.dataLen());
                    row = decoder.decodePayload(r.raw(), b0.dataOff(), kinds);
                }
                if (tails != null) {
                    tails.remove(oldCtid);   // UPD 必删旧（spike 教训）
                    if (tail != null) {
                        tails.put(newCtid, tail);    // 派生档 tail=null：无从重建，不落伪 tail（后续截断走值编码/自愈）
                    }
                }
                out.add(new HeapEvent(HeapEvent.UPD, oldCtid, newCtid, row));
            } else if (b0.hasImage()) {
                byte[] page = PageImages.rebuild(r, b0);
                int lpOff = PageImages.linePointerOffset(page, view.newOffnum());
                if (tails != null) {
                    tails.remove(oldCtid);
                }
                out.add(new HeapEvent(HeapEvent.UPD, oldCtid, newCtid,
                        decoder.decodePageTuple(page, lpOff, kinds)));
            }
        } else if (r.rmid() == HeapOps.RM_HEAP2_ID && op == HeapOps.XLOG_HEAP2_MULTI_INSERT) {
            HeapViews.MultiInsertView view = HeapViews.MultiInsertView.parse(r, layout);
            if (b0.hasData()) {
                int cur = b0.dataOff();
                for (int i = 0; i < view.ntuples(); i++) {
                    cur = (cur + 1) & ~1;    // entry 起点 2 对齐（spike 走读锚）
                    int datalen = u16(r.raw(), cur);
                    int offnum = view.offsetAt(i);
                    long key = ctidKey(b0.blockNo(), offnum);
                    out.add(new HeapEvent(HeapEvent.INS, 0, key,
                            decoder.decodeEntry(r.raw(), cur, kinds)));
                    if (tails != null) {
                        // datalen 只计 header 之后的 tail 字节（spike 实测锚）
                        tails.put(key, Arrays.copyOfRange(r.raw(), cur + 7, cur + 7 + datalen));
                    }
                    cur += 7 + datalen;
                }
            } else if (b0.hasImage()) {
                byte[] page = PageImages.rebuild(r, b0);
                for (int i = 0; i < view.ntuples(); i++) {
                    int offnum = view.offsetAt(i);
                    int lpOff = PageImages.linePointerOffset(page, offnum);
                    out.add(new HeapEvent(HeapEvent.INS, 0, ctidKey(b0.blockNo(), offnum),
                            decoder.decodePageTuple(page, lpOff, kinds)));
                }
            }
        }
        // XLOG_HEAP_INPLACE 与其它 opcode：无行事件（原地改写由 applyInplace 承载）
        return out;
    }

    /**
     * 施加一条 heap2 PRUNE 记录（三种 prune opcode 等价，发现 18）到两目录的
     * ctid 键控状态：redirected 段重定位行/tail 并跟随 tracked，nowdead/nowunused
     * 段移除行/tail——不重放 PRUNE 会在 autovacuum 压实页后丢失 tracked 行位
     * （spike S6 实证）。
     *
     * <p>关键步骤：rmid/opcode 筛 → PruneView 解析（freeze 段已在视图层跳过——
     * nplans u16 + 2B pad + plans×12B 居首）→ 对 pg_attribute / pg_class 各自按
     * relNode 匹配块后逐段施加。tracked 命中 dead/unused 打 WARN（行位丢失是
     * 异常态：该行已被物理删除）。边界与异常语义：非 prune 记录 no-op；记录块
     * 不属任一 watched 目录 no-op；段字节越界由上游走读不变量排除（裸抛）。
     * 线程约束：单写者（stores 生命周期内仅重放线程）。</p>
     *
     * @param r      走读完成的记录
     * @param stores 重放状态容器
     */
    public void applyPrune(WalRecord r, CatalogStores stores) {
        int op = r.info() & HeapOps.XLOG_XACT_OPMASK;
        if (r.rmid() != HeapOps.RM_HEAP2_ID
                || (op != HeapOps.XLOG_HEAP2_PRUNE_ON_ACCESS
                && op != HeapOps.XLOG_HEAP2_PRUNE_VACUUM_SCAN
                && op != HeapOps.XLOG_HEAP2_PRUNE_VACUUM_CLEANUP)) {
            return;
        }
        HeapViews.PruneView view = HeapViews.PruneView.parse(r, layout);
        pruneCatalog(view, stores.pgAttrRelfilenode(), stores.attrRows(), stores.rawAttrTails(), stores, false);
        pruneCatalog(view, stores.pgClassRelfilenode(), stores.classRows(), stores.rawClassTails(), stores, true);
    }

    /**
     * 施加 XLOG_HEAP_INPLACE 到 tracked pg_class 行（TRUNCATE/ANALYZE 路径，
     * 发现 17/19：PG 18 原地改写新 relfilenode——ctid 不动、块 data 是自 t_hoff 起
     * 的新数据区、头/位图不动，列数据区偏移定宽故 88/112 直读）。
     *
     * <p>关键步骤：rmid/opcode 筛 → 按 pgClassRelfilenode 匹配块且须有 data →
     * offnum 定 ctid 键 → 行字典命中（未跟踪行 no-op）→ 按布局偏移读新
     * relfilenode/reltoastrelid 重建 ClassRow（其余字段不动）→ raw tail 置失效
     * （删除：改写后的 tail 已陈旧，后续截断重建走值编码）→ 指标计数。
     * 边界与异常语义：非 inplace/块不属 pg_class/行未跟踪均 no-op；线程约束：
     * 单写者。</p>
     *
     * @param r      走读完成的记录
     * @param stores 重放状态容器
     */
    public void applyInplace(WalRecord r, CatalogStores stores) {
        if (r.rmid() != HeapOps.RM_HEAP_ID
                || (r.info() & HeapOps.XLOG_XACT_OPMASK) != HeapOps.XLOG_HEAP_INPLACE) {
            return;
        }
        BlockRef b0 = findHeapBlock(r, stores.pgClassRelfilenode());
        if (b0 == null || !b0.hasData()) {
            return;
        }
        HeapViews.HeapInplaceView view = HeapViews.HeapInplaceView.parse(r, layout);
        long key = ctidKey(b0.blockNo(), view.offnum());
        CatalogRow.ClassRow cr = stores.classRows().get(key);
        if (cr == null) {
            return;    // 未跟踪行（非 watched 关系的 pg_class 行）：no-op
        }
        long newFileno = u32(r.raw(), b0.dataOff() + layout.pgClassRelfilenodeDataOffset()) & 0xFFFFFFFFL;
        long newToast = u32(r.raw(), b0.dataOff() + layout.pgClassReltoastrelidDataOffset()) & 0xFFFFFFFFL;
        stores.classRows().put(key, new CatalogRow.ClassRow(cr.relOid(), cr.relname(), cr.relnamespace(),
                cr.reltype(), cr.reloftype(), cr.relowner(), cr.relam(), newFileno, newToast));
        stores.rawClassTails().remove(key);    // tail 已陈旧：置失效
        stores.metrics().inc(CatalogStores.CatalogMetrics.INPLACE_UPDATES);
        LOG.debug("pg_class INPLACE oid={} relfilenode {} -> {} reltoastrelid {} -> {}",
                cr.relOid(), cr.relfilenode(), newFileno, cr.reltoastrelid(), newToast);
    }

    /**
     * 单记录全量重放入口（spike {@code replayCatalogs} 移植）——施加次序固定：
     * PRUNE → INPLACE → pg_attribute 行事件 → pg_class 行事件（含 tracked 跟随
     * 与 toast 收养）。Task 11 的 synchronizer.apply 直接委托本方法。
     *
     * <p>关键步骤（pg_class 事件施加）：DEL 删行；INS/UPD 落新键行模型（UPD 先删
     * 旧键——heapEvents 已维护 tail，此处维护行字典）；UPD 时 tracked 双 ctid
     * 跟随；INS 且新行 oid 等于 tracked 表的 reltoastrelid 时收养为 trackedToast
     * （toast 关系重建以新 pg_class 行到达）。pg_class 腿以 stores 作 healStores
     * 传入 heapEvents——healer 已注入时未知 oldCtid 的截断更新在提取内完成自愈
     * （spec §6②，含 tracked 修复）。边界与异常语义：非 heap 家族记录仅
     * prune/inplace 两个 no-op 筛；replayed 指标按施加事件数计。线程约束：单写者。</p>
     *
     * @param r      走读完成的记录
     * @param stores 重放状态容器
     */
    public void applyCatalogRecord(WalRecord r, CatalogStores stores) {
        applyPrune(r, stores);
        applyInplace(r, stores);
        List<HeapEvent> attrEvents = heapEvents(r, stores.pgAttrRelfilenode(),
                layout.pgAttributeKinds(), stores.rawAttrTails(), null, stores.metrics());
        for (HeapEvent ev : attrEvents) {
            if (ev.op() == HeapEvent.DEL) {
                stores.attrRows().remove(ev.oldCtid());
                stores.metrics().inc(CatalogStores.CatalogMetrics.REPLAYED);
            } else {
                if (ev.op() == HeapEvent.UPD) {
                    stores.attrRows().remove(ev.oldCtid());
                }
                CatalogRow.AttrRow row = CatalogRow.AttrRow.fromDecoded(ev.row(), layout);
                if (row.attnum() > 0) {
                    // 字典面契约与引导同源：attnum>0（系统列行不进字典——流内建表时
                    // pg_attribute 的负 attnum INS 与种子查询的过滤口径一致，Task 13）
                    stores.attrRows().put(ev.newCtid(), row);
                    stores.metrics().inc(CatalogStores.CatalogMetrics.REPLAYED);
                } else if (row.attnum() == 0 || row.attnum() < -6) {
                    // 解码异常指示：合法系统列 attnum ∈ [-6,-1]，越界值意味着词典/字节错位
                    LOG.warn("pg_attribute 行解码异常 attnum={}（attrelid={} attname={}）——疑似词典错位",
                            row.attnum(), row.attrelid(), row.attname());
                }
            }
        }
        List<HeapEvent> classEvents = heapEvents(r, stores.pgClassRelfilenode(),
                layout.pgClassKinds(), stores.rawClassTails(), stores.classRows()::get,
                stores.metrics(), stores);
        for (HeapEvent ev : classEvents) {
            if (ev.op() == HeapEvent.DEL) {
                stores.classRows().remove(ev.oldCtid());
            } else {
                if (ev.op() == HeapEvent.UPD) {
                    stores.classRows().remove(ev.oldCtid());
                }
                CatalogRow.ClassRow cr = CatalogRow.ClassRow.fromDecoded(ev.row(), layout);
                stores.classRows().put(ev.newCtid(), cr);
                if (ev.op() == HeapEvent.UPD) {
                    stores.followTracked(ev.oldCtid(), ev.newCtid());
                }
                // tracked 归位兜底（Task 13，审查 Med-1 补判据）：链经 splice/值编码事件
                // 重建（不走自愈的 repairTracked）时 tracked 位可能仍指陈旧位——归位判据
                // 覆盖三形态：位上无行（断链）、位上行 oid 不符（他关系占位）、<strong>位上
                // 是同 oid 旧副本</strong>（tracked 位 ctid ≠ 本事件新位——正常 UPD 的
                // followTracked 已先行归位，此形态仅出现在链重建后的首条事件）；新行 oid
                // 须为 interest 最小 oid（v1 tracked 表判据，与引导同规则）
                CatalogRow.ClassRow trackedRow = stores.classRows().get(stores.trackedTableCtid());
                if (minInterestOid(stores) == cr.relOid()
                        && (trackedRow == null || trackedRow.relOid() != cr.relOid()
                        || stores.trackedTableCtid() != ev.newCtid())) {
                    stores.trackedTableCtid(ev.newCtid());
                }
                if (ev.op() == HeapEvent.INS && cr.relOid() != 0) {
                    // toast 关系重建：新 pg_class 行的 oid 恰为 tracked 表的 reltoastrelid
                    CatalogRow.ClassRow table = stores.classRows().get(stores.trackedTableCtid());
                    if (table != null && cr.relOid() == table.reltoastrelid()) {
                        stores.trackedToastCtid(ev.newCtid());
                    }
                }
            }
            stores.metrics().inc(CatalogStores.CatalogMetrics.REPLAYED);
        }
    }

    /**
     * pg_class 截断更新的值编码重建（spike {@code reconstructClassTruncated} 移植，
     * 发现 24 + 审查 High-1 读区回填）——prefix 落在读区（≤{@code 读区末尾}）时由
     * 已知旧行 {@link CatalogRow.ClassRow#encodeReadRegion()} 重编码前缀，中段取自
     * 记录；后缀零填充<strong>仅在读区之外合法</strong>，与读区（V18 [0,116) / V17
     * [0,112)，由 layout 的 toast 偏移锚定）的重叠段按
     * 旧行读区字节回填（suffix 截断省略的尾段与旧元组逐字节相同是其定义，回填即
     * 精确值——此前盲零填充在 RENAME 形态把 relnamespace..relam 清零）。
     *
     * <p>关键步骤：截断头走读（双 u16 前缀 + xl_heap_header 5B）→ prefix 越界
     * （&gt;{@code pgClassRelfilenodeDataOffset()}）返回 null（走 rawTail splice）→
     * prefix=0 整段取记录（只截后缀）否则 [位图][值编码 prefix][中段] 拼装 →
     * <strong>后缀处理</strong>：零填充区起点（数据区位）&ge; 读区末尾
     * （reltoastrelid 末尾 = {@code pgClassReltoastrelidDataOffset()}+4）才允许纯零
     * 填充；否则先零填充占位、再把 [零填充起点, 读区末尾) ∩ [0, 数据区末尾) 段从
     * 旧行读区编码回填 → 记录携带的头字段（infomask/infomask2/t_hoff）驱动解码。
     * 边界与异常语义：返回 null 表示不支持该 prefix 深度（非异常，落 rawTail
     * splice/自愈兜底）；读区回填要求旧行值模型可得（本方法签名保证非 null——
     * 旧行不可得的形态由 {@link #reconstructTruncatedUpdate} 直接走 tail/自愈）；
     * 解码失败（词典/长度错配）ISE 裸抛；线程约束：纯函数。</p>
     *
     * @param r      走读完成的 UPDATE 记录
     * @param b0     新页块引用（须携带 data）
     * @param oldRow 旧行值模型（来自 pg_class 行字典）
     * @return 重建产物；prefix &gt; 88 为 null
     */
    Reconstruction reconstructClassTruncated(WalRecord r, BlockRef b0, CatalogRow.ClassRow oldRow) {
        int[] p = truncParams(r, b0);
        int prefix = p[0];
        int suffix = p[1];
        int cur = p[2];
        if (prefix > layout.pgClassRelfilenodeDataOffset()) {
            return null;
        }
        byte[] raw = r.raw();
        int infomask2 = u16(raw, cur);
        int infomask = u16(raw, cur + 2);
        int tHoff = raw[cur + 4] & 0xFF;
        cur += 5;
        int bitmapLen = tHoff - TUPLE_BITS_OFFSET;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (prefix == 0) {
            out.write(raw, cur, b0.dataLen() - (cur - b0.dataOff()));    // 整段（仅截后缀）
        } else {
            out.write(raw, cur, bitmapLen);    // 位图 + 垫齐
            cur += bitmapLen;
            out.write(oldRow.encodeReadRegion(layout), 0, prefix);
            out.write(raw, cur, b0.dataLen() - (cur - b0.dataOff()));    // 中段
        }
        out.write(new byte[suffix], 0, suffix);    // 后缀零填充占位（读区重叠段下方回填）
        byte[] assembled = out.toByteArray();
        // 读区回填（审查 High-1）：零填充仅在读区之外合法——suffix 起点 < 读区末尾时，
        // 重叠段从旧行读区编码取精确值（后缀与旧元组逐字节相同是 suffix 截断的定义）。
        // 两分支的 assembled 均为 [位图][数据区] 形态，数据区偏移 = bitmapLen。
        int dataLen = assembled.length - bitmapLen;
        int zeroStart = dataLen - suffix;
        int readEnd = layout.pgClassReltoastrelidDataOffset() + 4;    // 最后被读列末尾
        if (zeroStart < 0) {
            return null;    // 防御：suffix 超数据区长（畸形记录）——落 tail/自愈兜底
        }
        if (zeroStart < readEnd) {
            byte[] readRegion = oldRow.encodeReadRegion(layout);
            int backfillEnd = Math.min(dataLen, readEnd);
            for (int i = zeroStart; i < backfillEnd; i++) {
                assembled[bitmapLen + i] = readRegion[i];
            }
        }
        return finishReconstruction(assembled, tHoff, infomask, infomask2, layout.pgClassKinds());
    }

    /**
     * 通用截断更新的 rawTail splice 重建（spike {@code reconstructTruncated} 移植）
     * ——chunk 布局（heapam.c）：[prefix u16?][suffix u16?][xl_heap_header 5B]，随后
     * prefix=0 时 [整 tail 减后缀]、否则 [位图+垫 (t_hoff-23)B][数据区自
     * t_hoff+prefix 起]；重建拼装 = 位图 + 旧 tail 前 prefix 字节 + 记录中段 +
     * 旧 tail 末尾 suffix 字节。
     *
     * <p>边界与异常语义：oldTail 长度不足由数组越界裸抛（上游键入的 tail 与记录
     * 同源，自洽前提）；解码失败 ISE；线程约束：纯函数。</p>
     *
     * @param r       走读完成的 UPDATE 记录
     * @param b0      新页块引用（须携带 data）
     * @param oldTail 旧行 raw tail（自 offset 23 起字节）
     * @param kinds   逐列解码词典
     * @return 重建产物（tail + 解码值行）
     */
    Reconstruction reconstructTruncated(WalRecord r, BlockRef b0, byte[] oldTail, String[] kinds) {
        int[] p = truncParams(r, b0);
        int prefix = p[0];
        int suffix = p[1];
        int cur = p[2];
        byte[] raw = r.raw();
        int infomask2 = u16(raw, cur);
        int infomask = u16(raw, cur + 2);
        int tHoff = raw[cur + 4] & 0xFF;
        cur += 5;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (prefix == 0) {
            out.write(raw, cur, b0.dataLen() - (cur - b0.dataOff()));
            out.write(oldTail, oldTail.length - suffix, suffix);
        } else {
            int bitmapLen = tHoff - TUPLE_BITS_OFFSET;
            out.write(raw, cur, bitmapLen);
            cur += bitmapLen;
            out.write(oldTail, bitmapLen, prefix);    // 旧 tail 的数据区前缀
            out.write(raw, cur, b0.dataLen() - (cur - b0.dataOff()));
            out.write(oldTail, oldTail.length - suffix, suffix);
        }
        return finishReconstruction(out.toByteArray(), tHoff, infomask, infomask2, kinds);
    }

    /**
     * 截断 UPDATE 的重建决策（spec §6 三级：值编码 → rawTail splice → 自愈/skip）。
     *
     * <p>关键步骤：值编码优先——classRowLookup 非空且 prefix ≤ 88 时查旧行，命中
     * 即走 {@link #reconstructClassTruncated}（prefix 越界防御性再判一次）；未命中
     * 或未配 lookup 则取 rawTailStore 的旧 tail 走 splice；两者皆无时若 healer 与
     * healStores 均可用（pg_class 面）走 {@code selfHealTruncated} ctid 寻址精确
     * 采纳（spec §6② 的收敛形态——末态回填而非历史重建），仍无则返回 null（调用方
     * skip）并对 metrics 计数
     * skippedTruncated（spike 差异①：页读兜底已删）。边界与异常语义：tail/lookup
     * 任一非 null 即重建；自愈判否不抛（连续失败只计数/标 stale）；线程约束：单写者。</p>
     *
     * @param r               走读完成的 UPDATE 记录
     * @param b0              新页块引用
     * @param oldCtid         旧行 ctid 键（查找面）
     * @param newCtid         新行 ctid 键（自愈校验的末态行位对照面）
     * @param kinds           逐列解码词典
     * @param tails           raw tail 存储（可为 null）
     * @param classRowLookup  pg_class 行字典查找（可为 null；非 null 约定 kinds 为 pg_class 词典）
     * @param metrics         指标容器（可为 null）
     * @param healStores      自愈状态容器（可为 null；非 null 约定为 pg_class 面）
     * @return 重建产物；skip 为 null
     */
    private Reconstruction reconstructTruncatedUpdate(WalRecord r, BlockRef b0, long oldCtid, long newCtid,
            String[] kinds, Map<Long, byte[]> tails, LongFunction<CatalogRow.ClassRow> classRowLookup,
            CatalogStores.CatalogMetrics metrics, CatalogStores healStores) {
        if (classRowLookup != null) {
            CatalogRow.ClassRow oldRow = classRowLookup.apply(oldCtid);
            if (oldRow != null) {
                Reconstruction rc = reconstructClassTruncated(r, b0, oldRow);
                if (rc != null) {
                    return rc;
                }
            }
        }
        byte[] oldTail = tails == null ? null : tails.get(oldCtid);
        if (oldTail == null) {
            if (healer != null && healer.enabled()) {
                if (healStores != null) {
                    // pg_class 面：ctid 寻址精确采纳（spec §6② 收敛形态）
                    Reconstruction healed = selfHealTruncated(r, b0, newCtid, healStores);
                    if (healed != null) {
                        return healed;
                    }
                } else if (classRowLookup == null) {
                    // pg_attribute 面（Task 13）：ctid 寻址精确采纳——更新必移行位，
                    // "末态仍居记录 new 位"的行即该记录施加后的精确状态，整行采纳
                    Reconstruction adopted = attrExactAdopt(r, newCtid, metrics);
                    if (adopted != null) {
                        return adopted;
                    }
                }
            }
            if (metrics != null) {
                metrics.inc(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED);
            }
            LOG.debug("skip truncated update of untracked ctid={} (no old row value, no raw tail)", oldCtid);
            return null;
        }
        return reconstructTruncated(r, b0, oldTail, kinds);
    }

        /**
     * 未知 oldCtid 的 pg_class 截断更新自愈（spec §6② 的 Task 13 收敛形态）——
     * <strong>ctid 寻址精确采纳</strong>：更新必移行位，"JDBC 末态仍居记录 new 位"的行
     * 即本记录施加后的精确状态，整行采纳（{@link #ctidExactAdoptClass}）。
     *
     * <p><strong>对 Task 12 候选枚举 + 中段值校验形态的裁定（对抗性 IT 实证）</strong>：
     * 候选前缀源（字典行值）在断链窗口内是<strong>历史快照</strong>，与记录中段拼装会
     * 产出时代错位的混合行（实测：pre-RENAME 的 relname 混入 toast 关系的 relfilenode）；
     * 且"末态 ctid == new 位"可被恰巧途经该位的他关系行满足——中段值校验的鉴别力在
     * 高频迁移风暴下不足。ctid 精确采纳的行值全部来自探测行自身（身份自述），不存在
     * 拼装；该位无行（行已再迁移，记录形态过时）返回 null（skip 计数），链由该 oid 的
     * <strong>末条记录在追平后</strong>的精确采纳收敛。线程约束：单写者（与 heapEvents
     * 同缝，本方法在提取单次路径内完成——facade 非幂等，不重复调用）。</p>
     *
     * @param r      走读完成的 UPDATE 记录（LSN 定位日志面）
     * @param b0     新页块引用（保留签名对齐——精确采纳不消费块细节）
     * @param newCtid 记录新行 ctid 键
     * @param stores 自愈状态容器（tracked/staleOids/指标）
     * @return 采纳产物；该位无行 null（调用方走 skip 计数）
     */
    private Reconstruction selfHealTruncated(WalRecord r, BlockRef b0, long newCtid, CatalogStores stores) {
        return ctidExactAdoptClass(r, newCtid, stores);
    }

        /**
     * pg_class 截断更新的 <strong>ctid 寻址精确采纳</strong>（Task 13）——按"末态仍居
     * 记录 new 位 ⟹ 该行即本记录施加后的精确状态"（更新移位、INPLACE 不移位但
     * 重放收敛）整行采纳 JDBC 末态值（九槽全来自探测行，与字典候选的时序错位解耦）。
     *
     * <p>关键步骤：new 位渲染 "(block,off)" → probe 点查 → 命中则九槽值行组装 +
     * repairTracked（probed 行 oid 作归属判据）+ selfHealed 计数 + WARN 返回派生档
     * 产物（tail=null）。边界与异常语义：<strong>末态回填语义（审查 Med-2）</strong>
     * ——采纳值取自探测时刻的目录末态，丢页/断链窗口内的中间代际不可恢复，v1 仅承诺
     * catalog 末态正确（对拍面即末态全等）；该位无行返回 null（调用方走 skip 计数 +
     * stale 记账——行已再迁移的记录形态本就过时）；行位复用窗口（毫秒级理论残留）
     * 由对拍暴露。
     * 线程约束：单写者（重放线程）。</p>
     *
     * @param r       走读完成的 UPDATE 记录（LSN 定位日志面）
     * @param newCtid 记录新行 ctid 键
     * @param stores  状态容器（tracked 修复面）
     * @return 采纳产物（tail=null + 九槽值行）；该位无行 null
     */
    private Reconstruction ctidExactAdoptClass(WalRecord r, long newCtid, CatalogStores stores) {
        String ctidText = "(" + (newCtid >>> 16) + "," + (newCtid & 0xFFFF) + ")";
        JdbcProbe.ProbedRow end = healer.probeClassByCtid(ctidText);
        if (end == null) {
            noteAdoptRejection(r, newCtid, stores);
            return null;
        }
        CatalogRow.ClassRow row = end.row();
        Object[] vals = new Object[layout.pgClassKinds().length];
        vals[IX_CLASS_OID] = row.relOid();
        vals[IX_CLASS_RELNAME] = row.relname();
        vals[IX_CLASS_RELNAMESPACE] = row.relnamespace();
        vals[IX_CLASS_RELTYPE] = row.reltype();
        vals[IX_CLASS_RELOFTYPE] = row.reloftype();
        vals[IX_CLASS_RELOWNER] = row.relowner();
        vals[IX_CLASS_RELAM] = row.relam();
        vals[IX_CLASS_RELFILENODE] = row.relfilenode();
        vals[layout.classToastRelidIndex()] = row.reltoastrelid();
        repairTracked(stores, row, newCtid);
        noteAdoption(row.relOid());
        stores.metrics().inc(CatalogStores.CatalogMetrics.SELF_HEALED);
        LOG.warn("class 精确采纳: ctid={} oid={}（末态仍居记录 new 位, lsn={}）", ctidText, row.relOid(), Lsn.format(r.lsn()));
        return new Reconstruction(null, vals);
    }

    /**
     * 精确采纳拒绝记账（stale 轻量复活，审查裁定 3）：拒绝记录无身份信息（该位无行），
     * 归因于 v1 tracked 表面 oid（tracked 位上行 oid，位空回落 interest 最小 oid——
     * 断链自愈域即该表面）；同 oid 连续拒绝 ≥ {@value ADOPT_SKIP_STALE_THRESHOLD} 次向
     * {@link CatalogStores#staleOids()} 登记 + WARN（含 oid 与计数）——语义为"该关系
     * 的链恢复持续失败，<strong>待下轮引导重新种子</strong>（= 重启路径；进程内热重
     * 引导 v1 不做）"。
     *
     * <p>边界与异常语义：无归因面（interest 空）不计数；登记幂等（Set 去重），计数
     * 继续累计供 WARN 观测。线程约束：单写者（重放线程）。</p>
     *
     * @param r       走读完成的 UPDATE 记录（LSN 定位日志面）
     * @param newCtid 被拒的记录新行 ctid 键
     * @param stores  状态容器（stale 登记面）
     */
    private void noteAdoptRejection(WalRecord r, long newCtid, CatalogStores stores) {
        CatalogRow.ClassRow trackedRow = stores.classRows().get(stores.trackedTableCtid());
        long oid = trackedRow != null ? trackedRow.relOid() : minInterestOid(stores);
        if (oid == Long.MAX_VALUE) {
            return;    // 无归因面（interest 空 + tracked 位空）：不计数
        }
        int times = consecutiveAdoptSkips.merge(oid, 1, Integer::sum);
        if (times >= ADOPT_SKIP_STALE_THRESHOLD) {
            stores.staleOids().add(oid);    // 幂等（Set 去重）——待下轮引导（重启路径）
            // WARN 节流：过门槛即打（含 oid 与计数），其后每 100 次补一行——风暴期拒绝
            // 可达数千次/重复执行，逐条刷屏会淹没问题日志
            if (times == ADOPT_SKIP_STALE_THRESHOLD || times % 100 == 0) {
                LOG.warn("精确采纳连续拒绝 oid={} 第 {} 次（new 位 {} 无行）——登记 stale 待下轮引导（进程内热重引导 v1 不做）, lsn={}",
                        oid, times, "(" + (newCtid >>> 16) + "," + (newCtid & 0xFFFF) + ")", Lsn.format(r.lsn()));
            }
        }
    }

    /**
     * 精确采纳成功记账：清零该 oid 的连续拒绝计数（链已恢复，stale 语义解除——
     * staleOids 标记的撤回由 {@link #repairTracked} 承担）。
     *
     * @param oid 采纳行 oid
     */
    private void noteAdoption(long oid) {
        consecutiveAdoptSkips.remove(oid);
    }

    /**
     * pg_attribute 截断更新的 <strong>ctid 寻址精确采纳</strong>（Task 13）——attr 面
     * 无值编码重建（AttrRow 仅五字段投影，无法重编码全前缀）、种子行无 raw tail 时，
     * 按"末态仍居记录 new 位 ⟹ 该行即本记录施加后的精确状态"（更新移位、INPLACE
     * 不移位但重放收敛）整行采纳 JDBC 末态值。
     *
     * <p>关键步骤：new 位渲染 "(block,off)" → probe 点查 → 命中则组装五槽位稀疏值行
     * （仅填 {@link CatalogRow.AttrRow} 消费的词典槽位）返回 tail=null 派生档产物 +
     * selfHealed 计数 + WARN。边界与异常语义：该位无行（行已再迁移，记录形态过时）
     * 返回 null（调用方走 skip 计数）；行位复用窗口（行迁走后新行复占同位）为毫秒级
     * 理论残留，采纳错行会被后续事件/对拍暴露。线程约束：单写者（重放线程）。</p>
     *
     * @param r       走读完成的 UPDATE 记录（LSN 定位日志面）
     * @param newCtid 记录新行 ctid 键
     * @param metrics 指标容器（可为 null）
     * @return 采纳产物（tail=null + 五槽值行）；拒绝 null
     */
    private Reconstruction attrExactAdopt(WalRecord r, long newCtid, CatalogStores.CatalogMetrics metrics) {
        String ctidText = "(" + (newCtid >>> 16) + "," + (newCtid & 0xFFFF) + ")";
        CatalogRow.AttrRow row = healer.probeAttrByCtid(ctidText);
        if (row == null) {
            return null;
        }
        Object[] vals = new Object[layout.pgAttributeKinds().length];
        vals[IX_ATTR_ATTRELID] = row.attrelid();
        vals[IX_ATTR_ATTNAME] = row.attname();
        vals[IX_ATTR_ATTTYPEID] = row.atttypid();
        vals[IX_ATTR_ATTNUM] = (short) row.attnum();
        vals[layout.attrDroppedIndex()] = row.attisdropped();
        if (metrics != null) {
            metrics.inc(CatalogStores.CatalogMetrics.SELF_HEALED);
        }
        LOG.warn("attr 精确采纳: ctid={} attrelid={} attnum={}（末态仍居记录 new 位, lsn={}）",
                ctidText, row.attrelid(), row.attnum(), Lsn.format(r.lsn()));
        return new Reconstruction(null, vals);
    }

                /**
     * 采纳后的 tracked 修复与陈旧副本清扫（Task 13 ctid 精确采纳形态）——精确采纳
     * 不知旧行位，按 <strong>oid 扫除</strong>陈旧副本（行 + tail：行已物理离开旧位，
     * 残留会同 oid 双键），再修 tracked：tracked 表位上的行 oid == 采纳 oid → 表位；
     * tracked toast 位行 oid 或表行 reltoastrelid == 采纳 oid → toast 位；表行位断链
     * （字典无行）且采纳 oid 是 interest 最小 oid → 保守归表位（v1 tracked 面单表）；
     * 均不中不动 tracked。
     *
     * <p>边界与异常语义：清扫先于归属判据（tracked 位上的同 oid 陈旧行被清后走
     * interest 保守档——语义等价且更简）；采纳同时撤回该 oid 的 stale 标记。线程
     * 约束：单写者。</p>
     *
     * @param stores  状态容器
     * @param adopted 采纳的探测行值模型（oid 归属判据）
     * @param newCtid 记录新行 ctid 键（tracked 修复目标位，清扫的豁免位）
     */
    private void repairTracked(CatalogStores stores, CatalogRow.ClassRow adopted, long newCtid) {
        long oid = adopted.relOid();
        // 同 oid 陈旧副本清扫（含 tail）——精确采纳行落 newCtid，其余同 oid 位皆已物理离开
        List<Long> staleCtids = new ArrayList<>();
        stores.classRows().forEach((k, v) -> {
            if (v.relOid() == oid && k != newCtid) {
                staleCtids.add(k);
            }
        });
        for (long k : staleCtids) {
            stores.classRows().remove(k);
            stores.rawClassTails().remove(k);
        }
        stores.staleOids().remove(oid);    // 链已修复：撤回既往 stale 标记（下轮引导不再被裁剪面引用）
        CatalogRow.ClassRow tableRow = stores.classRows().get(stores.trackedTableCtid());
        CatalogRow.ClassRow toastRow = stores.classRows().get(stores.trackedToastCtid());
        if (tableRow != null && tableRow.relOid() == oid) {
            stores.trackedTableCtid(newCtid);
        } else if ((toastRow != null && toastRow.relOid() == oid)
                || (tableRow != null && tableRow.reltoastrelid() == oid)) {
            stores.trackedToastCtid(newCtid);
        } else if (tableRow == null && minInterestOid(stores) == oid) {
            // 断链修复档：表行副本已失（PRUNE 清除），interest 最小 oid 即 v1 tracked 表面
            stores.trackedTableCtid(newCtid);
        }
    }

    /**
     * interest 集最小 oid——v1 tracked 表面判据（与引导 locateTracked 的"取最小存在
     * oid"规则同源：interest 集中 tracked 面单表，动态登记的新关系 oid 较大不抢位）。
     *
     * @param stores 状态容器
     * @return 最小 oid；interest 空 Long.MAX_VALUE（恒不命中任何真实 oid）
     */
    private static long minInterestOid(CatalogStores stores) {
        long min = Long.MAX_VALUE;
        for (Long oid : stores.interestRelOids()) {
            min = Math.min(min, oid);
        }
        return min;
    }

    /**
     * 截断头走读：flags（main@7）的双截断位决定 [prefix u16?][suffix u16?] 前缀，
     * 返回三元组（prefix、suffix、xl_heap_header 起点的绝对偏移）。
     *
     * @param r  走读完成的 UPDATE 记录
     * @param b0 新页块引用（data 区即截断 chunk）
     * @return {prefix, suffix, headerOff}
     */
    private int[] truncParams(WalRecord r, BlockRef b0) {
        int flags = r.raw()[r.mainOff() + 7] & 0xFF;
        int cur = b0.dataOff();
        int prefix = 0;
        int suffix = 0;
        if ((flags & XLH_UPDATE_PREFIX_FROM_OLD) != 0) {
            prefix = u16(r.raw(), cur);
            cur += 2;
        }
        if ((flags & XLH_UPDATE_SUFFIX_FROM_OLD) != 0) {
            suffix = u16(r.raw(), cur);
            cur += 2;
        }
        return new int[]{prefix, suffix, cur};
    }

    /**
     * 重建收尾：把重建 tail（自 offset 23 起字节）与记录携带的头字段拼回
     * {@link TupleDecoder#decodePayload} 的输入形态（[xl_heap_header 5B][tail]）
     * 解码出值行。
     *
     * @param tail      重建 tail
     * @param tHoff     t_hoff（记录携带）
     * @param infomask  t_infomask（记录携带）
     * @param infomask2 t_infomask2（记录携带）
     * @param kinds     逐列解码词典
     * @return 重建产物
     */
    private Reconstruction finishReconstruction(byte[] tail, int tHoff, int infomask, int infomask2, String[] kinds) {
        byte[] payload = new byte[5 + tail.length];
        payload[0] = (byte) infomask2;
        payload[1] = (byte) (infomask2 >>> 8);
        payload[2] = (byte) infomask;
        payload[3] = (byte) (infomask >>> 8);
        payload[4] = (byte) tHoff;
        System.arraycopy(tail, 0, payload, 5, tail.length);
        return new Reconstruction(tail, decoder.decodePayload(payload, 0, kinds));
    }

    /**
     * 对一张 watched 目录施加 PruneView：redirect 重定位（行 + tail）、nowdead/
     * nowunused 移除。tracked 跟随（redirect）与 tracked 链断 WARN（dead/unused）
     * <strong>仅 pg_class 分支</strong>（isClass=true）——tracked 双 ctid 是 pg_class
     * 行位，而 ctidKey 无关系判别、两目录块号键空间完全重叠，attr 分支数值命中
     * tracked 时不得搬移/告警（spike remapCtid 的 attr 分支同判）。
     *
     * <p>关键步骤：按 relNode 匹配块（不属则 no-op）→ redirected 段逐对 (from,to)
     * 折键重定位（行命中计 pruneRedirects；tail 无行也可单独存在，随迁；isClass
     * 时 tracked 跟随）→ dead/unused 段逐行移除（行命中计 pruneDropped；isClass
     * 且 tracked 命中打 WARN）。边界与异常语义：段空（flags 未置位）由视图访问器
     * 回空表自然跳过。线程约束：单写者。</p>
     *
     * @param view        prune 视图（freeze 段已跳过）
     * @param relfilenode 目录 relfilenode（0 不匹配）
     * @param rows        行字典（ctid 键控）
     * @param tails       raw tail 存储
     * @param stores      状态容器（tracked 跟随 + 指标）
     * @param isClass     是否 pg_class 目录（tracked 面的施加判据）
     * @param <T>         行模型类型（AttrRow / ClassRow）
     */
    private <T> void pruneCatalog(HeapViews.PruneView view, long relfilenode,
            Map<Long, T> rows, Map<Long, byte[]> tails, CatalogStores stores, boolean isClass) {
        BlockRef b = findHeapBlock(view.rec(), relfilenode);
        if (b == null) {
            return;
        }
        int blockNo = b.blockNo();
        List<Integer> pairs = view.redirectedPairs();
        for (int i = 0; i + 1 < pairs.size(); i += 2) {
            long from = ctidKey(blockNo, pairs.get(i));
            long to = ctidKey(blockNo, pairs.get(i + 1));
            T row = rows.remove(from);
            if (row != null) {
                rows.put(to, row);
                stores.metrics().inc(CatalogStores.CatalogMetrics.PRUNE_REDIRECTS);
            }
            byte[] tail = tails.remove(from);
            if (tail != null) {
                tails.put(to, tail);
            }
            if (isClass) {
                stores.followTracked(from, to);
            }
        }
        for (int off : view.nowdead()) {
            dropPruned(blockNo, off, rows, tails, stores, isClass);
        }
        for (int off : view.nowunused()) {
            dropPruned(blockNo, off, rows, tails, stores, isClass);
        }
    }

    /**
     * PRUNE dead/unused 段的单行移除：行与 tail 删键；pg_class 分支且 tracked 命中
     * （行位被物理删除，链断）打 WARN——attr 分支数值命中不告警（键空间重叠，
     * 见 {@link #pruneCatalog}）。
     *
     * @param blockNo 块号
     * @param offnum  行号
     * @param rows    行字典
     * @param tails   raw tail 存储
     * @param stores  状态容器（tracked + 指标）
     * @param isClass 是否 pg_class 目录
     * @param <T>     行模型类型
     */
    private <T> void dropPruned(int blockNo, int offnum, Map<Long, T> rows,
            Map<Long, byte[]> tails, CatalogStores stores, boolean isClass) {
        long key = ctidKey(blockNo, offnum);
        T row = rows.remove(key);
        if (row != null) {
            stores.metrics().inc(CatalogStores.CatalogMetrics.PRUNE_DROPPED);
        }
        tails.remove(key);
        if (isClass && (stores.trackedTableCtid() == key || stores.trackedToastCtid() == key)) {
            LOG.warn("tracked ctid {} (block {}, offnum {}) pruned dead/unused — ctid chain broken", key, blockNo, offnum);
        }
    }

    /**
     * 块链按 relNode 匹配 watched 关系的 heap 主 fork 块（fork=0；spike
     * {@code findHeapBlock} 的单值简化——同库引导下等价，见类 javadoc）。
     *
     * @param r          走读完成的记录
     * @param relfilenode watched relfilenode（0 恒不匹配）
     * @return 首个匹配块；无匹配 null
     */
    private BlockRef findHeapBlock(WalRecord r, long relfilenode) {
        if (relfilenode == 0) {
            return null;
        }
        for (BlockRef b : r.blocks()) {
            if (b.fork() == 0 && b.relNode() == relfilenode) {
                return b;
            }
        }
        return null;
    }

    /**
     * data 路径单 tuple 的 raw tail：block data = [xl_heap_header 5B][tail]，
     * 总长 dataLen（spike {@code tailOf}）。
     *
     * @param raw     记录字节
     * @param off     载荷起点（xl_heap_header 处）
     * @param dataLen 块 data 总长
     * @return tail 字节副本
     */
    private static byte[] tailOf(byte[] raw, int off, int dataLen) {
        return Arrays.copyOfRange(raw, off + 5, off + dataLen);
    }

    /**
     * 通配 rawTailStore 收敛为字节值 map（签名档 {@code Map<Long,?>} 的内部视图；
     * 契约由调用方保证——实参一律 {@code Map<Long, byte[]>}）。
     *
     * @param store 通配引用（可为 null）
     * @return 收敛视图（共享同一 map；null 透传）
     */
    @SuppressWarnings("unchecked")
    private static Map<Long, byte[]> tails(Map<Long, ?> store) {
        return (Map<Long, byte[]>) store;
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b 源数组（长度须覆盖 o+2）
     * @param o 起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b 源数组（长度须覆盖 o+4）
     * @param o 起始偏移
     * @return 32 位值（int 载体，无符号语义经调用方掩码）
     */
    private static int u32(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8) | ((b[o + 2] & 0xFF) << 16) | ((b[o + 3] & 0xFF) << 24);
    }
}
