package org.vastdata.vbstream.walsource.replay;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * catalog ctid 重放的状态容器——pg_attribute / pg_class 两张 watched 目录的行字典
 * 与 raw tail 存储（按物理 ctid 键控），加 tracked 双 ctid（用户表与其 toast 关系
 * 在 pg_class 中的行位，0=无）与引导/自愈所需的关系登记面。
 *
 * <p>控制器裁定：本类随 Task 10（重放核心）交付，Task 11（CatalogSynchronizer/
 * 引导）只消费。生命周期约定：Task 11 的 {@code CatalogBootstrap} 在 REPEATABLE READ
 * 单事务内灌入全行含 ctid 并回填两 relfilenode，此后由重放引擎增量维护。
 * <strong>线程约束：非线程安全</strong>——spec §3 执行模型为单线程直通
 * （接收→解析→重放→周期落盘），全部集合按单写者假设使用；持久化快照走
 * {@link CatalogMetrics#snapshot()} 的防御拷贝。</p>
 */
public final class CatalogStores {

    private final Map<Long, CatalogRow.AttrRow> attrRows = new HashMap<>();
    private final Map<Long, CatalogRow.ClassRow> classRows = new HashMap<>();
    private final Map<Long, byte[]> rawAttrTails = new HashMap<>();
    private final Map<Long, byte[]> rawClassTails = new HashMap<>();
    private final Set<Long> interestRelOids = new HashSet<>();
    private final Set<Long> staleOids = new HashSet<>();
    private final CatalogMetrics metrics = new CatalogMetrics();
    private long trackedTableCtid;
    private long trackedToastCtid;
    private long pgAttrRelfilenode;
    private long pgClassRelfilenode;

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
     * 兴趣关系 oid 登记（caller 指定需要列字典/toast 映射的关系）——活引用；
     * 引导据此裁剪查询、快照据此取列。
     *
     * @return oid 集合（非拷贝）
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
     * 重放指标——单线程 HashMap 计数 + 防御拷贝快照（无需 LongAdder/CHM，spec §3
     * 单线程直通；Task 15 的指标只读面消费 {@link #snapshot()}）。
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

        private final Map<String, Long> counters = new HashMap<>();

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
