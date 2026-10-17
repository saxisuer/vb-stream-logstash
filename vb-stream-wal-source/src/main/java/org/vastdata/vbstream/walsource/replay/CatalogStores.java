package org.vastdata.vbstream.walsource.replay;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * catalog ctid 重放的状态容器——pg_attribute / pg_class / pg_namespace 三张 watched
 * 目录的行字典与 raw tail 存储（按物理 ctid 键控），加 tracked 双 ctid（用户表与
 * 其 toast 关系在 pg_class 中的行位，0=无）与引导/自愈所需的关系登记面。
 *
 * <p>控制器裁定：本类随 Task 10（重放核心）交付，Task 11（CatalogSynchronizer/
 * 引导）只消费；v2（Task 4）扩第三张 watched 目录 pg_namespace（schema 名解析的
 * 字典源）。生命周期约定：Task 11 的 {@code CatalogBootstrap} 在 REPEATABLE READ
 * 单事务内灌入全行含 ctid 并回填三 relfilenode，此后由重放引擎增量维护。
 * <strong>线程约束：单写者 + 弱一致并发读</strong>——spec §3 执行模型为单线程直通
 * （接收→解析→重放→周期落盘），put/remove/跟随类复合迁移（先 remove 后 put 的
 * ctid 搬迁）仍按单写者假设（读线程在搬迁间隙可能瞬时看不到该行，属"尽力一致"
 * 口径内的窗口）；六个行字典/tail 存储用 {@link ConcurrentHashMap}——跨线程快照
 * 查询（{@code CatalogSynchronizer.snapshot()} 的迭代）与重放线程写入并发时弱一致
 * 迭代、不抛 CME；tracked 双 ctid/interest/stale 等标量与小集合仅装配/引导线程
 * 触碰，保持普通字段/HashSet；持久化与指标快照走 {@link CatalogMetrics#snapshot()}
 * 的防御拷贝。</p>
 *
 * <p><strong>二级索引面（性能优化 #1，2026-10-17）</strong>：五张 CHM 索引
 * （{@code classOidIndex}/{@code classFilenodeIndex}/{@code nspOidIndex}/
 * {@code attrRelIndex}/{@code partitionFilenodeIndex}）把 as-of 查询从行字典值域
 * 线性扫（宽 schema 万级行）降 O(1)。<b>维护纪律</b>：① 运行期增量写点一律走
 * {@link #putAttrRow}/{@link #removeAttrRow} 等八包装方法（主 map + 索引 +
 * {@link #shapeEpoch()} 同步维护）——直接操作 {@link #attrRows()} 等主 map 会留下
 * 陈旧索引；bulk 装载（引导种子/{@code StoredState#restoreInto}）例外——种子循环
 * 直接 put 主 map、完成后恰一次 {@link #rebuildIndexes()} 全量重建；② 两 filenode
 * 索引 <b>0 键不入</b>（映射目录行 relfilenode 列值恒 0，入表会把 relOidOf(0) 从
 * "任意命中"变"恒命中"）；③ 同 oid/relfilenode 陈旧 ctid 副本共存窗口内，线性扫
 * 返回"CHM 迭代序任意首配"、索引返回"最后 put 者"——后者即最新代际，是更正确的
 * 选择（<b>记档的语义收紧</b>）；repairTracked/PRUNE 收敛后两者一致，parity IT 钉
 * 行为不变；④ 索引与 epoch 均为<strong>单写者维护 + 弱一致读</strong>（与主 map 同
 * 口径，不抛 CME）；attrRelIndex 的值是 {@link CopyOnWriteArrayList}——DDL 稀有故
 * 写侧拷贝可接受，读侧对快照迭代无 CME。</p>
 */
public final class CatalogStores {

    private final Map<Long, CatalogRow.AttrRow> attrRows = new ConcurrentHashMap<>();
    private final Map<Long, CatalogRow.ClassRow> classRows = new ConcurrentHashMap<>();
    private final Map<Long, CatalogRow.NspRow> nspRows = new ConcurrentHashMap<>();
    private final Map<Long, CatalogRow.PartitionRow> partitionRows = new ConcurrentHashMap<>();
    private final Map<Long, byte[]> rawAttrTails = new ConcurrentHashMap<>();
    private final Map<Long, byte[]> rawClassTails = new ConcurrentHashMap<>();
    private final Map<Long, byte[]> rawNspTails = new ConcurrentHashMap<>();
    private final Map<Long, byte[]> rawPartitionTails = new ConcurrentHashMap<>();
    // interest 集 CHM：重放线程的 ctid 寻址精确采纳与消费方的中途登记（新关系出现时）并发——弱一致不抛 CME
    private final Set<Long> interestRelOids = ConcurrentHashMap.newKeySet();
    private final Set<Long> staleOids = new HashSet<>();
    private final CatalogMetrics metrics = new CatalogMetrics();

    // ---- 五二级索引（性能优化 #1，2026-10-17；维护纪律与多行同键语义见类 javadoc 索引节） ----

    /** pg_class 按关系 oid 的索引（键 relOid → 行模型）——relfilenodeOf/toastOf/
     *  relkindOf/nameOf/schemaOf 首段与 relOidOf 的 oid 直认腿；CHM 弱一致读。 */
    private final Map<Long, CatalogRow.ClassRow> classOidIndex = new ConcurrentHashMap<>();

    /** pg_class 按物理段号的索引（键 relfilenode → 行模型，<b>0 键不入</b>——映射目录
     *  行 relfilenode 列值恒 0）——relOidOf 反查腿。 */
    private final Map<Long, CatalogRow.ClassRow> classFilenodeIndex = new ConcurrentHashMap<>();

    /** pg_namespace 按模式 oid 的索引（键 nspOid → 行模型）——schemaOf 第二段。 */
    private final Map<Long, CatalogRow.NspRow> nspOidIndex = new ConcurrentHashMap<>();

    /** pg_attribute 按所属关系 oid 的索引（键 attrelid → 行列表，含 dropped 占位行与
     *  同键陈旧副本）——columnsOf；值 COW 列表：DDL 稀有故写侧拷贝可接受，读侧快照
     *  迭代无 CME。 */
    private final Map<Long, CopyOnWriteArrayList<CatalogRow.AttrRow>> attrRelIndex = new ConcurrentHashMap<>();

    /** pg_partition 按分区物理段号的索引（键 relfilenode → 行模型，<b>0 键不入</b>——
     *  母表 'r' 行 relfilenode 恒 0）——partitionOf（'p' 判定保留在查询侧）。 */
    private final Map<Long, CatalogRow.PartitionRow> partitionFilenodeIndex = new ConcurrentHashMap<>();

    /** 字典形态代际戳（volatile 弱一致读）：每次行变更（put/remove 命中）与每次
     *  {@link #rebuildIndexes()} 自增——Task 6 以它作消费方缓存的代际键（本字段先行
     *  落地，暂无消费方）。 */
    private volatile long shapeEpoch;

    private long trackedTableCtid;
    private long trackedToastCtid;
    private long pgAttrRelfilenode;
    private long pgClassRelfilenode;
    private long pgNspRelnode;
    private long pgPartitionRelfilenode;

    /**
     * pg_attribute 行字典（ctid 键 → 行模型）——活引用，重放引擎与引导直接读写。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, CatalogRow.AttrRow> attrRows() {
        return attrRows;
    }

    /**
     * pg_class 行字典（ctid 键 → 行模型）——活引用。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, CatalogRow.ClassRow> classRows() {
        return classRows;
    }

    /**
     * pg_attribute 的 raw tail 存储（ctid 键 → 自 heap-tuple offset 23 起字节）——
     * 截断更新 splice 重建的旧值面；活引用。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, byte[]> rawAttrTails() {
        return rawAttrTails;
    }

    /**
     * pg_class 的 raw tail 存储——语义同 {@link #rawAttrTails()}；INPLACE 改写后
     * 对应条目置失效（删除，后续截断走值编码）；活引用。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, byte[]> rawClassTails() {
        return rawClassTails;
    }

    /**
     * pg_namespace 行字典（ctid 键 → 行模型，v2 扩链第三 watched 目录）——schema
     * 名解析的字典源（relnamespace → nspname）；活引用。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, CatalogRow.NspRow> nspRows() {
        return nspRows;
    }

    /**
     * pg_namespace 的 raw tail 存储——语义同 {@link #rawAttrTails()}（nsp 面无
     * INPLACE，失效面不存在）；活引用。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, byte[]> rawNspTails() {
        return rawNspTails;
    }

    /**
     * pg_partition 行字典（ctid 键 → 行模型，openGauss M3 扩链第四 watched 目录）
     * ——OG 原生分区路由字典（分区 relfilenode → parentid → 母表 oid 映射链源）；
     * 活引用。vanilla 档恒空（引导不回填 {@link #pgPartitionRelfilenode()}，第四腿
     * 不激活）。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, CatalogRow.PartitionRow> partitionRows() {
        return partitionRows;
    }

    /**
     * pg_partition 的 raw tail 存储——语义同 {@link #rawAttrTails()}（截断更新
     * splice 重建的旧值面；INPLACE 后置失效）；活引用。
     *
     * @return 键控字典（非拷贝）
     */
    public Map<Long, byte[]> rawPartitionTails() {
        return rawPartitionTails;
    }

    // ---- 二级索引维护面（八包装方法 + bulk 重建 + 五查询；单写者维护，弱一致读） ----

    /**
     * pg_attribute 行的增量写点（重放引擎 attr 族的唯一正门）：主 map put + attrelid
     * 索引登记 + 代际戳自增；同 ctid 覆写旧行时先摘旧行的索引条目（值不同则列表去
     * 一，值相等则摘旧登新计数不变）。
     *
     * <p>边界与异常语义：row 为 null 时主 map 落 null 键值（与裸 put 同口径，调用方
     * 契约保证非 null）。线程约束：<strong>单写者</strong>（重放线程）维护；读侧
     * （COW 列表快照迭代）弱一致不抛 CME。</p>
     *
     * @param ctid 行位键
     * @param row  行模型
     */
    public void putAttrRow(long ctid, CatalogRow.AttrRow row) {
        CatalogRow.AttrRow old = attrRows.put(ctid, row);
        if (old != null) {
            removeAttrIndexEntry(old);
        }
        attrRelIndex.computeIfAbsent(row.attrelid(), k -> new CopyOnWriteArrayList<>()).add(row);
        shapeEpoch++;
    }

    /**
     * pg_attribute 行的增量删点：主 map remove + 旧行的 attrelid 索引条目摘除
     * （无行 no-op——不动索引与代际戳）。
     *
     * <p>线程约束：单写者维护；读侧弱一致。</p>
     *
     * @param ctid 行位键
     */
    public void removeAttrRow(long ctid) {
        CatalogRow.AttrRow old = attrRows.remove(ctid);
        if (old == null) {
            return;
        }
        removeAttrIndexEntry(old);
        shapeEpoch++;
    }

    /**
     * attrelid 索引的条目摘除（put 覆写/remove 共用）：列表按值去一（record equals），
     * 摘空后连键回收。
     *
     * @param old 离场行模型
     */
    private void removeAttrIndexEntry(CatalogRow.AttrRow old) {
        CopyOnWriteArrayList<CatalogRow.AttrRow> list = attrRelIndex.get(old.attrelid());
        if (list == null) {
            return;
        }
        list.remove(old);
        if (list.isEmpty()) {
            attrRelIndex.remove(old.attrelid(), list);
        }
    }

    /**
     * pg_class 行的增量写点（重放引擎 class 族含 INPLACE/PRUNE redirect 的唯一正门）：
     * 主 map put + oid/filenode 两索引 upsert（<b>最后 put 者胜</b>——同 oid 陈旧副本
     * 共存窗口内索引取最新代际，记档的语义收紧）+ 旧行与新行键不重叠的旧索引条目
     * 条件移除 + 代际戳自增。
     *
     * <p>关键步骤：先登记新行（relfilenode 0 不入 filenode 索引），再对旧行非重叠键
     * 做"索引现值 == old 才移除"的条件删除（两参 remove）——防误删同键后写行。
     * 边界与异常语义：row 非 null 由调用方契约保证。线程约束：单写者维护；读侧弱一致。</p>
     *
     * @param ctid 行位键
     * @param row  行模型
     */
    public void putClassRow(long ctid, CatalogRow.ClassRow row) {
        CatalogRow.ClassRow old = classRows.put(ctid, row);
        classOidIndex.put(row.relOid(), row);
        if (row.relfilenode() != 0) {
            classFilenodeIndex.put(row.relfilenode(), row);
        }
        if (old != null) {
            if (old.relOid() != row.relOid()) {
                classOidIndex.remove(old.relOid(), old);
            }
            if (old.relfilenode() != 0 && old.relfilenode() != row.relfilenode()) {
                classFilenodeIndex.remove(old.relfilenode(), old);
            }
        }
        shapeEpoch++;
    }

    /**
     * pg_class 行的增量删点：主 map remove + 两索引按"索引现值 == old 才移除"条件
     * 删除（同 oid/relfilenode 的后写行在场时不误删）+ 代际戳自增（无行 no-op）。
     *
     * <p>线程约束：单写者维护；读侧弱一致。</p>
     *
     * @param ctid 行位键
     */
    public void removeClassRow(long ctid) {
        CatalogRow.ClassRow old = classRows.remove(ctid);
        if (old == null) {
            return;
        }
        classOidIndex.remove(old.relOid(), old);
        if (old.relfilenode() != 0) {
            classFilenodeIndex.remove(old.relfilenode(), old);
        }
        shapeEpoch++;
    }

    /**
     * pg_namespace 行的增量写点：主 map put + nspOid 索引 upsert（最后 put 者胜）+
     * 旧行非重叠键条件移除 + 代际戳自增。线程约束：单写者维护；读侧弱一致。
     *
     * @param ctid 行位键
     * @param row  行模型
     */
    public void putNspRow(long ctid, CatalogRow.NspRow row) {
        CatalogRow.NspRow old = nspRows.put(ctid, row);
        nspOidIndex.put(row.nspOid(), row);
        if (old != null && old.nspOid() != row.nspOid()) {
            nspOidIndex.remove(old.nspOid(), old);
        }
        shapeEpoch++;
    }

    /**
     * pg_namespace 行的增量删点：主 map remove + nspOid 索引条件删除（现值 == old
     * 才移除）+ 代际戳自增（无行 no-op）。线程约束：单写者维护；读侧弱一致。
     *
     * @param ctid 行位键
     */
    public void removeNspRow(long ctid) {
        CatalogRow.NspRow old = nspRows.remove(ctid);
        if (old == null) {
            return;
        }
        nspOidIndex.remove(old.nspOid(), old);
        shapeEpoch++;
    }

    /**
     * pg_partition 行的增量写点（重放引擎 partition 族含 INPLACE 的唯一正门）：主
     * map put + filenode 索引 upsert（<b>0 键不入</b>——母表 'r' 行 relfilenode 恒 0）
     * + 旧行非重叠键条件移除 + 代际戳自增。线程约束：单写者维护；读侧弱一致。
     *
     * @param ctid 行位键
     * @param row  行模型
     */
    public void putPartitionRow(long ctid, CatalogRow.PartitionRow row) {
        CatalogRow.PartitionRow old = partitionRows.put(ctid, row);
        if (row.relfilenode() != 0) {
            partitionFilenodeIndex.put(row.relfilenode(), row);
        }
        if (old != null && old.relfilenode() != 0 && old.relfilenode() != row.relfilenode()) {
            partitionFilenodeIndex.remove(old.relfilenode(), old);
        }
        shapeEpoch++;
    }

    /**
     * pg_partition 行的增量删点：主 map remove + filenode 索引条件删除（现值 == old
     * 才移除）+ 代际戳自增（无行 no-op）。线程约束：单写者维护；读侧弱一致。
     *
     * @param ctid 行位键
     */
    public void removePartitionRow(long ctid) {
        CatalogRow.PartitionRow old = partitionRows.remove(ctid);
        if (old == null) {
            return;
        }
        if (old.relfilenode() != 0) {
            partitionFilenodeIndex.remove(old.relfilenode(), old);
        }
        shapeEpoch++;
    }

    /**
     * 五索引清空并自四张主字典全量重建 + 代际戳自增——bulk 装载（{@code
     * CatalogBootstrap} 种子完成点 / {@code StoredState#restoreInto} 四 putAll 后）
     * 恰一次调用；重建走主 map 迭代，同 oid/relfilenode 多副本时"最后 put 者胜"由
     * 迭代次序承载（CHM 迭代序不定，副本间任取其一——与增量维护的语义收紧同向，
     * parity 面两者收敛后一致）。
     *
     * <p>边界与异常语义：filenode 0 行不入两 filenode 索引（与增量纪律一致）；attr
     * 行按 attrelid 分组进 COW 列表。线程约束：<strong>单写者</strong>（装配/重放
     * 线程）调用；重建窗口内读侧弱一致（可能读到部分重建的索引——"尽力一致"口径）。</p>
     */
    public void rebuildIndexes() {
        classOidIndex.clear();
        classFilenodeIndex.clear();
        nspOidIndex.clear();
        attrRelIndex.clear();
        partitionFilenodeIndex.clear();
        for (CatalogRow.ClassRow row : classRows.values()) {
            classOidIndex.put(row.relOid(), row);
            if (row.relfilenode() != 0) {
                classFilenodeIndex.put(row.relfilenode(), row);
            }
        }
        for (CatalogRow.NspRow row : nspRows.values()) {
            nspOidIndex.put(row.nspOid(), row);
        }
        for (CatalogRow.AttrRow row : attrRows.values()) {
            attrRelIndex.computeIfAbsent(row.attrelid(), k -> new CopyOnWriteArrayList<>()).add(row);
        }
        for (CatalogRow.PartitionRow row : partitionRows.values()) {
            if (row.relfilenode() != 0) {
                partitionFilenodeIndex.put(row.relfilenode(), row);
            }
        }
        shapeEpoch++;
    }

    /**
     * pg_class 按关系 oid 的索引查询——relfilenodeOf/toastOf/relkindOf/nameOf/
     * schemaOf 首段与 relOidOf 的 oid 直认腿。
     *
     * @param relOid 关系 oid
     * @return 行模型；不在字典 null（同 oid 陈旧副本窗口内取最后 put 者）
     */
    public CatalogRow.ClassRow classByOid(long relOid) {
        return classOidIndex.get(relOid);
    }

    /**
     * pg_class 按物理段号的索引查询——relOidOf 反查腿（0 键不入索引，恒 null）。
     *
     * @param relfilenode 物理段号
     * @return 行模型；无匹配 null
     */
    public CatalogRow.ClassRow classByFilenode(long relfilenode) {
        return classFilenodeIndex.get(relfilenode);
    }

    /**
     * pg_attribute 按所属关系 oid 的索引查询——columnsOf 源（含 dropped 占位行；
     * 排序保留在查询侧）。
     *
     * @param relOid 关系 oid
     * @return 行列表（COW 快照语义，弱一致迭代不抛 CME）；无列关系 null
     */
    public List<CatalogRow.AttrRow> attrsByRel(long relOid) {
        return attrRelIndex.get(relOid);
    }

    /**
     * pg_namespace 按模式 oid 的索引查询——schemaOf 第二段。
     *
     * @param nspOid 模式 oid
     * @return 行模型；不在字典 null
     */
    public CatalogRow.NspRow nspByOid(long nspOid) {
        return nspOidIndex.get(nspOid);
    }

    /**
     * pg_partition 按分区物理段号的索引查询——partitionOf 源（'p' 判定保留在查询侧；
     * 母表 'r' 行 relfilenode 0 不入索引）。
     *
     * @param relfilenode 分区物理段号
     * @return 行模型（母表/分区行皆可能）；无匹配 null
     */
    public CatalogRow.PartitionRow partitionByFilenode(long relfilenode) {
        return partitionFilenodeIndex.get(relfilenode);
    }

    /**
     * 字典形态代际戳——每次行变更（put/remove 命中行）与每次 {@link #rebuildIndexes()}
     * 单调自增；Task 6 以它作消费方缓存的代际键（本方法先行落地，暂无消费方）。
     *
     * @return 当前代际（初始 0，单调不减；volatile 弱一致读）
     */
    public long shapeEpoch() {
        return shapeEpoch;
    }

    /**
     * 用户表在 pg_class 中的行位（tracked ctid 键）。
     *
     * @return ctid 键（未跟踪/未引导为 0）
     */
    public long trackedTableCtid() {
        return trackedTableCtid;
    }

    /**
     * 设置用户表 tracked ctid（引导定位或 UPD/redirect 跟随）。
     *
     * @param ctid 新行位键
     */
    public void trackedTableCtid(long ctid) {
        this.trackedTableCtid = ctid;
    }

    /**
     * 用户表的 toast 关系在 pg_class 中的行位。
     *
     * @return ctid 键（无 toast/未引导为 0）
     */
    public long trackedToastCtid() {
        return trackedToastCtid;
    }

    /**
     * 设置 toast 关系 tracked ctid（toast 收养/跟随）。
     *
     * @param ctid 新行位键
     */
    public void trackedToastCtid(long ctid) {
        this.trackedToastCtid = ctid;
    }

    /**
     * tracked 双 ctid 的重定位跟随（UPD / PRUNE redirect 共用）：任一 tracked 命中
     * 旧键即改指新键，另一保持不动。
     *
     * @param from 旧 ctid 键
     * @param to   新 ctid 键
     */
    public void followTracked(long from, long to) {
        if (trackedTableCtid == from) {
            trackedTableCtid = to;
        }
        if (trackedToastCtid == from) {
            trackedToastCtid = to;
        }
    }

    /**
     * pg_attribute 的 relfilenode（块匹配面，引导回填）。
     *
     * @return relfilenode（未引导为 0——重放引擎按 0 不匹配处理）
     */
    public long pgAttrRelfilenode() {
        return pgAttrRelfilenode;
    }

    /**
     * 回填 pg_attribute relfilenode。
     *
     * @param relfilenode 引导查询值
     */
    public void pgAttrRelfilenode(long relfilenode) {
        this.pgAttrRelfilenode = relfilenode;
    }

    /**
     * pg_class 的 relfilenode（块匹配面 + INPLACE watched 判据，引导回填）。
     *
     * @return relfilenode（未引导为 0）
     */
    public long pgClassRelfilenode() {
        return pgClassRelfilenode;
    }

    /**
     * 回填 pg_class relfilenode。
     *
     * @param relfilenode 引导查询值
     */
    public void pgClassRelfilenode(long relfilenode) {
        this.pgClassRelfilenode = relfilenode;
    }

    /**
     * pg_namespace 的 relfilenode（块匹配面，引导回填，v2 扩链）。
     *
     * @return relfilenode（未引导为 0——重放引擎按 0 不匹配处理）
     */
    public long pgNspRelnode() {
        return pgNspRelnode;
    }

    /**
     * 回填 pg_namespace relfilenode。
     *
     * @param relfilenode 引导查询值
     */
    public void pgNspRelnode(long relfilenode) {
        this.pgNspRelnode = relfilenode;
    }

    /**
     * pg_partition 的 relfilenode（块匹配面，引导回填，openGauss M3 扩链）——<b>须按
     * 物理段号注册</b>（T0 坑 2：pg_partition 自身 relfilenode=14318 ≠ oid 9016，
     * 与 M2 T7 修复 C 的映射目录段号直认机制同源注意）。
     *
     * @return relfilenode（未引导/vanilla 档为 0——重放引擎按 0 不匹配处理，第四腿不激活）
     */
    public long pgPartitionRelfilenode() {
        return pgPartitionRelfilenode;
    }

    /**
     * 回填 pg_partition relfilenode（仅 OG 档引导调用）。
     *
     * @param relfilenode 引导查询值（{@code pg_relation_filenode('pg_partition'::regclass)}）
     */
    public void pgPartitionRelfilenode(long relfilenode) {
        this.pgPartitionRelfilenode = relfilenode;
    }

    /**
     * 兴趣关系 oid 登记（caller 指定需要列字典/toast 映射的关系）——活引用；
     * 引导据此裁剪查询、快照据此取列、ctid 寻址精确采纳据此圈定 oid 域。
     *
     * <p><strong>中途登记（Task 13）</strong>：CHM 键集，流运行期消费方可随时登记
     * 新出现的 interest oid（重放线程迭代弱一致不抛 CME）——对抗性场景中新关系创建
     * 后其 pg_class 行即进入 ctid 寻址精确采纳域。</p>
     *
     * @return oid 集合（非拷贝，弱一致并发）
     */
    public Set<Long> interestRelOids() {
        return interestRelOids;
    }

    /**
     * 自愈失败标记 stale 的关系 oid 集合（Task 12 写入、下轮引导清除）——活引用。
     *
     * @return oid 集合（非拷贝）
     */
    public Set<Long> staleOids() {
        return staleOids;
    }

    /**
     * 重放指标容器。
     *
     * @return 指标（本 stores 独享）
     */
    public CatalogMetrics metrics() {
        return metrics;
    }

    /**
     * 重放指标——单写者计数（重放线程 {@code inc}）+ 跨线程只读快照（Task 15 的
     * 指标面消费 {@link #snapshot()}）。counters 用 {@link ConcurrentHashMap}：
     * {@code get}/{@code snapshot} 与重放线程的 {@code inc} 并发时弱一致（读到
     * 稍旧的计数）但不抛 CME；无需 LongAdder（计数面无竞争写）。
     *
     * <p>键为静态常量字符串；未触及的键在 {@link #snapshot()} 中不出现（调用方用
     * {@code getOrDefault} 取 0 默认）。</p>
     */
    public static final class CatalogMetrics {

        /** 计数键：已施加的行事件数（INS/UPD/DEL 合计，两目录合计）。 */
        public static final String REPLAYED = "replayed";

        /** 计数键：截断更新因旧行值与 raw tail 皆无而 skip 的次数（窗口外噪声）。 */
        public static final String SKIPPED_TRUNCATED = "skippedTruncated";

        /** 计数键：PRUNE redirect 成功重定位的行数（含 tracked 跟随）。 */
        public static final String PRUNE_REDIRECTS = "pruneRedirects";

        /** 计数键：PRUNE nowdead/nowunused 移除的行数。 */
        public static final String PRUNE_DROPPED = "pruneDropped";

        /** 计数键：INPLACE 就地更新（TRUNCATE/ANALYZE 改 relfilenode/toast）的行数。 */
        public static final String INPLACE_UPDATES = "inplaceUpdates";

        /** 计数键：截断更新经末态回填自愈采纳（记录 new 位探测 JDBC 末态行整行采纳——
         *  旧"候选枚举 + 中段值校验"形态已废弃，见 {@code SelfHealer} 裁定注记）的次数。 */
        public static final String SELF_HEALED = "selfHealed";

        /** 计数键：pg_namespace 面 PRUNE redirect 缺行放弃物化的次数（v2 无 nsp 探测
         *  通道的观测面——非零提示 nsp 链断，schema 字典可能静默缺表）。 */
        public static final String NSP_REDIRECT_MISSES = "nspRedirectMisses";

        /** 计数键：pg_partition 面 PRUNE redirect 缺行放弃物化的次数（openGauss M3
         *  扩链第四腿——无 ctid 探测通道的观测面，语义同 nspRedirectMisses：非零
         *  提示分区字典链断，分区路由可能静默缺行）。 */
        public static final String PARTITION_REDIRECT_MISSES = "partitionRedirectMisses";

        private final Map<String, Long> counters = new ConcurrentHashMap<>();

        /**
         * 指定键计数 +1（不存在即从 1 起）。
         *
         * @param key 计数键常量
         */
        public void inc(String key) {
            counters.merge(key, 1L, Long::sum);
        }

        /**
         * 读指定键的当前计数。
         *
         * @param key 计数键常量
         * @return 计数值（未触及为 0）
         */
        public long get(String key) {
            return counters.getOrDefault(key, 0L);
        }

        /**
         * 全量快照（防御拷贝）——持久化/指标暴露面用，与后续 inc 互不影响。
         *
         * @return 键值不可变拷贝
         */
        public Map<String, Long> snapshot() {
            return Map.copyOf(counters);
        }
    }
}
