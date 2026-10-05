package org.vastdata.vbstream.walsource.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.BlockRef;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.HeapViews;
import org.vastdata.vbstream.walsource.layout.PageImages;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
 * 皆无 skip + 计数 skippedTruncated（窗口外噪声不抛）；② tracked 断链自愈
 * <strong>不做</strong>（Task 12 专题：中段新值对 JDBC 末态校验）——本任务 unknown
 * oldCtid 直接 skip + 计数。</p>
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

    private final WalLayout layout;
    private final TupleDecoder decoder;

    /**
     * 构造重放引擎。
     *
     * @param layout  版本布局描述符（截断阈值 88、INPLACE 偏移 88/112、两目录词典均取自它）
     * @param decoder 磁盘格式 tuple 解码器（实例无状态，可与他处共享）
     */
    public CatalogReplay(WalLayout layout, TupleDecoder decoder) {
        this.layout = layout;
        this.decoder = decoder;
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
        return heapEvents(r, watchedRelfilenode, kinds, rawTailStore, null, null);
    }

    /**
     * 提取 heap 级行事件（全量档）——INS / DEL / UPD（含截断重建）/ MULTI_INSERT，
     * data 与 FPW image 双路径（发现 14：catalogs 在 FPI 时不记 tuple data；发现 15：
     * 镜像是变更后页状态，offnum 行直接在页内）。
     *
     * <p>关键步骤：rmid 筛（heap/heap2）→ 块链按 relNode 匹配 watched（fork=0）→
     * 按 opcode 分支：INS data 路径 [xl_heap_header 5B][tail] 解码并落 tail、image
     * 路径重建页后按行号取 ItemId 解码（无 tail）；DEL 仅事件 + 删 tail；UPD 以
     * HeapUpdateView 取 old/new 行号（旧页块按 fork 过滤选取，spike VM 块教训），
     * 截断位组置位时先试值编码（classRowLookup 命中且 prefix ≤ 88）、再试 rawTail
     * splice、皆无 skip（metrics 计数 skippedTruncated，UPD 必删旧 tail——spike
     * 教训）；MULTI_INSERT 逐 entry（2 对齐起点、datalen 只计 tail 字节，spike 锚）
     * 或 image 路径按 offsets（INIT_PAGE 时隐含 i+1，发现 3）。INPLACE 不在本方法
     * 面（返回空）——原地改写需要行字典就地更新与 tail 失效，由
     * {@link #applyInplace} 承载。</p>
     *
     * <p>边界与异常语义：opcode/结构错配由 HeapViews 各工厂 ISE fail-fast；块无
     * data 且无 image（协议违约）静默无事件；截断 skip 不抛。线程约束：纯提取 +
     * 对 rawTailStore/classRowLookup 的单写者假设（stores 生命周期内仅重放线程调用）。</p>
     *
     * @param r                 走读完成的记录
     * @param watchedRelfilenode watched 关系 relfilenode（0 恒不匹配）
     * @param kinds             逐列解码词典
     * @param rawTailStore      raw tail 存储（维护契约见四参档；null 不维护）
     * @param classRowLookup    pg_class 行字典查找（值编码重建的旧行面；<strong>非
     *                          null 即约定 kinds 为 pgClassKinds()</strong>，null 则
     *                          截断仅 rawTail splice）
     * @param metrics           指标容器（skip 计数；null 不计）
     * @return 事件列表
     */
    public List<HeapEvent> heapEvents(WalRecord r, long watchedRelfilenode, String[] kinds,
            Map<Long, ?> rawTailStore, LongFunction<CatalogRow.ClassRow> classRowLookup,
            CatalogStores.CatalogMetrics metrics) {
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
                            r, b0, oldCtid, kinds, tails, classRowLookup, metrics);
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
                    tails.put(newCtid, tail);
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
        pruneCatalog(view, stores.pgAttrRelfilenode(), stores.attrRows(), stores.rawAttrTails(), stores);
        pruneCatalog(view, stores.pgClassRelfilenode(), stores.classRows(), stores.rawClassTails(), stores);
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
     * （toast 关系重建以新 pg_class 行到达）。边界与异常语义：非 heap 家族记录仅
     * prune/inplace 两个 no-op 筛；replayed 指标按施加事件数计。线程约束：单写者。</p>
     *
     * @param r      走读完成的记录
     * @param stores 重放状态容器
     */
    public void applyCatalogRecord(WalRecord r, CatalogStores stores) {
        applyPrune(r, stores);
        applyInplace(r, stores);
        List<HeapEvent> attrEvents = heapEvents(r, stores.pgAttrRelfilenode(),
                layout.pgAttributeKinds(), stores.rawAttrTails(), null, null);
        for (HeapEvent ev : attrEvents) {
            if (ev.op() == HeapEvent.DEL) {
                stores.attrRows().remove(ev.oldCtid());
            } else {
                if (ev.op() == HeapEvent.UPD) {
                    stores.attrRows().remove(ev.oldCtid());
                }
                stores.attrRows().put(ev.newCtid(), CatalogRow.AttrRow.fromDecoded(ev.row()));
            }
            stores.metrics().inc(CatalogStores.CatalogMetrics.REPLAYED);
        }
        List<HeapEvent> classEvents = heapEvents(r, stores.pgClassRelfilenode(),
                layout.pgClassKinds(), stores.rawClassTails(), stores.classRows()::get,
                stores.metrics());
        for (HeapEvent ev : classEvents) {
            if (ev.op() == HeapEvent.DEL) {
                stores.classRows().remove(ev.oldCtid());
            } else {
                if (ev.op() == HeapEvent.UPD) {
                    stores.classRows().remove(ev.oldCtid());
                }
                CatalogRow.ClassRow cr = CatalogRow.ClassRow.fromDecoded(ev.row());
                stores.classRows().put(ev.newCtid(), cr);
                if (ev.op() == HeapEvent.UPD) {
                    stores.followTracked(ev.oldCtid(), ev.newCtid());
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
     * 发现 24）——prefix 落在列 1-7 定宽区（≤88B）时由已知旧行
     * {@link CatalogRow.ClassRow#encodeFirstSevenCols()} 重编码前缀，中段取自记录、
     * 后缀零填充（后缀列必为定宽不读值或位图 null 的 varlena，spike 论证）。
     *
     * <p>关键步骤：截断头走读（双 u16 前缀 + xl_heap_header 5B）→ prefix 越界
     * （&gt;{@code pgClassRelfilenodeDataOffset()}）返回 null（走 rawTail splice）→
     * prefix=0 整段取记录（只截后缀）否则 [位图][值编码 prefix][中段] 拼装 →
     * 记录携带的头字段（infomask/infomask2/t_hoff）驱动解码。边界与异常语义：
     * 返回 null 表示不支持该 prefix 深度（非异常）；解码失败（词典/长度错配）ISE
     * 裸抛；线程约束：纯函数。</p>
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
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (prefix == 0) {
            out.write(raw, cur, b0.dataLen() - (cur - b0.dataOff()));    // 整段（仅截后缀）
        } else {
            int bitmapLen = tHoff - TUPLE_BITS_OFFSET;
            out.write(raw, cur, bitmapLen);    // 位图 + 垫齐
            cur += bitmapLen;
            out.write(oldRow.encodeFirstSevenCols(), 0, prefix);
            out.write(raw, cur, b0.dataLen() - (cur - b0.dataOff()));    // 中段
        }
        out.write(new byte[suffix], 0, suffix);    // 后缀零填充
        return finishReconstruction(out.toByteArray(), tHoff, infomask, infomask2, layout.pgClassKinds());
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
     * 截断 UPDATE 的重建决策（spec §6 两级：值编码 → rawTail splice → skip）。
     *
     * <p>关键步骤：值编码优先——classRowLookup 非空且 prefix ≤ 88 时查旧行，命中
     * 即走 {@link #reconstructClassTruncated}（prefix 越界防御性再判一次）；未命中
     * 或未配 lookup 则取 rawTailStore 的旧 tail 走 splice；两者皆无返回 null（调用
     * 方 skip）并对 metrics 计数 skippedTruncated（spike 差异①：页读兜底已删）。
     * 边界与异常语义：tail/lookup 任一非 null 即重建，不抛；线程约束：单写者。</p>
     *
     * @param r               走读完成的 UPDATE 记录
     * @param b0              新页块引用
     * @param oldCtid         旧行 ctid 键（查找面）
     * @param kinds           逐列解码词典
     * @param tails           raw tail 存储（可为 null）
     * @param classRowLookup  pg_class 行字典查找（可为 null；非 null 约定 kinds 为 pg_class 词典）
     * @param metrics         指标容器（可为 null）
     * @return 重建产物；skip 为 null
     */
    private Reconstruction reconstructTruncatedUpdate(WalRecord r, BlockRef b0, long oldCtid, String[] kinds,
            Map<Long, byte[]> tails, LongFunction<CatalogRow.ClassRow> classRowLookup,
            CatalogStores.CatalogMetrics metrics) {
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
            if (metrics != null) {
                metrics.inc(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED);
            }
            LOG.debug("skip truncated update of untracked ctid={} (no old row value, no raw tail)", oldCtid);
            return null;
        }
        return reconstructTruncated(r, b0, oldTail, kinds);
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
     * 对一张 watched 目录施加 PruneView：redirect 重定位（行 + tail + tracked
     * 跟随）、nowdead/nowunused 移除。
     *
     * <p>关键步骤：按 relNode 匹配块（不属则 no-op）→ redirected 段逐对 (from,to)
     * 折键重定位（行命中计 pruneRedirects；tail 无行也可单独存在，随迁）→
     * dead/unused 段逐行移除（行命中计 pruneDropped；tracked 命中打 WARN）。
     * 边界与异常语义：段空（flags 未置位）由视图访问器回空表自然跳过。
     * 线程约束：单写者。</p>
     *
     * @param view      prune 视图（freeze 段已跳过）
     * @param relfilenode 目录 relfilenode（0 不匹配）
     * @param rows      行字典（ctid 键控）
     * @param tails     raw tail 存储
     * @param stores    状态容器（tracked 跟随 + 指标）
     * @param <T>       行模型类型（AttrRow / ClassRow）
     */
    private <T> void pruneCatalog(HeapViews.PruneView view, long relfilenode,
            Map<Long, T> rows, Map<Long, byte[]> tails, CatalogStores stores) {
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
            stores.followTracked(from, to);
        }
        for (int off : view.nowdead()) {
            dropPruned(blockNo, off, rows, tails, stores);
        }
        for (int off : view.nowunused()) {
            dropPruned(blockNo, off, rows, tails, stores);
        }
    }

    /**
     * PRUNE dead/unused 段的单行移除：行与 tail 删键；tracked 命中（行位被物理
     * 删除，链断）打 WARN。
     *
     * @param blockNo 块号
     * @param offnum  行号
     * @param rows    行字典
     * @param tails   raw tail 存储
     * @param stores  状态容器（tracked + 指标）
     * @param <T>     行模型类型
     */
    private <T> void dropPruned(int blockNo, int offnum, Map<Long, T> rows,
            Map<Long, byte[]> tails, CatalogStores stores) {
        long key = ctidKey(blockNo, offnum);
        T row = rows.remove(key);
        if (row != null) {
            stores.metrics().inc(CatalogStores.CatalogMetrics.PRUNE_DROPPED);
        }
        tails.remove(key);
        if (stores.trackedTableCtid() == key || stores.trackedToastCtid() == key) {
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
