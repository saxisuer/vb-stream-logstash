package org.vastdata.vbstream.walsource.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.WalLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * 用户表过滤器 + {@link TableMeta} 组装器（v2 MS2）——把 heap 块反查出的关系
 * （relfilenode，或元数据面直知的 oid）经 v1 catalog as-of 快照解析为变更流表身份：
 * 三道闸全过才放行——①relkind ∈ {'r'（普通表）, 'p'（分区表）}（toast 't'/索引
 * 'i'/视图 'v'/序列 'S' 等一律排除）；②非系统模式（pg_catalog/pg_toast/
 * information_schema 下的表不进变更流——watched 目录自身所在模式）；③白名单
 * （空集 = 用户表全放行；非空 = 恰 {@code schema.table} 命中才放行）。
 *
 * <p><b>openGauss 档分区判别（M2 T5，经 layout 档位门控）</b>：{@code layout}
 * 为 openGauss 方言时第一闸改 parttype 语义——relkind 收窄为仅 'r'（OG 无
 * relkind 'p' 值：分区母表 relkind 仍 'r'，分区语义由独有列 pg_class.parttype
 * 承担——og-plan m2pg[PG 档] live 实查，值域恰 {'n','p'}，2026-10-12），并叠
 * parttype 闸：'n'（普通表）与 'p'（分区母表——接口语义 parttype∈{'n','p'} 放行；
 * 路由实测母表<b>不承载行</b>，放行无行流副作用）放行、未转录值 miss（防御面）、
 * {@link CatalogSnapshot#parttypeOf} 不可判（empty）fail-open 放行（与 V17/V18
 * 无此闸的行为面一致）。</p>
 *
 * <p><b>openGauss 档分区身份链（M3 T2，T0 锚②语义重排；T5 属主优先修复）</b>：
 * OG 原生分区的 heap 记录路由到<b>分区自己的 relfilenode</b>（pg_partition 'p'
 * 行——不在 pg_class 三表字典，og-plan m2pg 四锚实测）；OG 档 resolve 先过第四
 * 字典属主判定——{@link CatalogSnapshot#partitionOf} 命中即走母表身份链（T5 前
 * 是"三表 miss 后第四步"，EXCHANGE 换绑段号与普通表 oid 碰撞的误属主实测后升为
 * 属主优先、命中不回退三表面）：反查分区行 → parentid 母表链 → 按<b>母表身份</b>
 * 放行与组装（TableMeta = schema.母表名 + 母表 pg_class oid + 母表列集
 * ——列挂母表 oid，M2 T5 事实；基准路 pgoutput R/I 帧同形，锚②
 * {@code PARTITION_NAME=mother}：wal 路渲分区名对拍必 diff）。<b>白名单基准形态
 * = 母表名匹配</b>（写母表名即覆盖其全部分区）；<b>分区名条目 = 兼容增强</b>——
 * 运行期双查（先母表全名、未中再分区全名，schema 取母表解析值）：解析期预展开
 * （把分区名静态展开进白名单集）需要构造期字典且 ADD/DROP PARTITION 后即过期，
 * 双查的匹配面随第四字典 as-of 走、每行一次 Set 查询成本可忽略——口径落定记档。
 * <b>链断 fail-safe 有痕跳过</b>（沿 nsp redirect miss 先例）：第四字典有分区行
 * 但母表链解析不出（parentid 悬空/字典半损）——WARN 一次（全局节流）+
 * {@link #partitionChainMisses()} 计数，行照常跳过不砖流。</p>
 *
 * <p>放行时组装 {@link TableMeta}：schema 经 relnamespace→pg_namespace.nspname
 * 解析、columns 取 {@link CatalogSnapshot#columnsOf} 的有序含 dropped 占位投影。
 * miss 面（relkind/系统模式/白名单/字典无行）一律 {@link Optional#empty()}——
 * 调用方（v2 桶发射）据此静默跳过该块，不抛不告警（噪声表属常态；分区链断的
 * 有痕面除外，见上）。</p>
 *
 * <p>线程约束：持快照与白名单两不可变引用，查询面委派快照的弱一致语义
 * （见 {@link CatalogSnapshot}）；链断计数（LongAdder）与 WARN 节流位
 * （AtomicBoolean）并发安全——任意线程并发调用安全。</p>
 */
public final class TableFilter {

    private static final Logger LOG = LoggerFactory.getLogger(TableFilter.class);

    /** 系统模式排除集：watched 目录（pg_catalog）与其 toast（pg_toast）、SQL 标准视图
     * （information_schema）所在模式——这些 'r' 表是目录/基础设施，不属变更流语义。 */
    private static final Set<String> SYSTEM_SCHEMAS = Set.of("pg_catalog", "pg_toast", "information_schema");

    private final CatalogSnapshot snapshot;

    private final Set<String> whitelist;

    /** OG 方言档门控（构造期定格）：true 时第一闸走 parttype 语义（见类 javadoc）。 */
    private final boolean ogDialect;

    /** 分区链断的跳过计数（沿 nsp redirect miss 先例——逐次递增，观测面）。 */
    private final LongAdder partitionChainMisses = new LongAdder();

    /** 链断 WARN 全局节流位（整实例恰一次——后续链断走 DEBUG + 计数）。 */
    private final AtomicBoolean partitionChainWarned = new AtomicBoolean();

    /**
     * 装配过滤器（v2 兼容形态：vanilla 语义，不消费 parttype）。
     *
     * @param snapshot  catalog as-of 快照（表身份/列序查询面）
     * @param whitelist 白名单（{@code schema.table} 全名集；null 或空集 = 用户表全放行；
     *                   非 null 时经 {@link Set#copyOf} 防御拷贝——调用方后续改动不影响本实例）
     */
    public TableFilter(CatalogSnapshot snapshot, Set<String> whitelist) {
        this(snapshot, whitelist, null);
    }

    /**
     * 装配过滤器（版本感知形态）。
     *
     * <p>关键步骤：白名单防御拷贝；{@code layout} 非 null 且
     * {@link WalLayout#openGaussDialect()} 为 true 时定格 OG 档（第一闸改 parttype
     * 语义——relkind 收窄 'r' + parttype∈{'n','p'} 闸），否则 vanilla 语义
     * （relkind 'r'/'p'，零变化）。边界与异常语义：layout 为 null 合法（等价两参
     * 构造器——v2 兼容面）。</p>
     *
     * @param snapshot  catalog as-of 快照（表身份/列序查询面）
     * @param whitelist 白名单（同两参构造器）
     * @param layout    版本布局描述符（OG 档门控来源；null = vanilla 语义）
     */
    public TableFilter(CatalogSnapshot snapshot, Set<String> whitelist, WalLayout layout) {
        this.snapshot = snapshot;
        this.whitelist = whitelist == null ? Set.of() : Set.copyOf(whitelist);
        this.ogDialect = layout != null && layout.openGaussDialect();
    }

    /**
     * 解析一个关系到变更流表身份：relfilenode 反查优先（heap 块形态——块头只携带
     * relfilenode），miss 再按 oid 直认（元数据形态）；三道闸（relkind[OG 档含
     * parttype]/系统模式/白名单）全过返回组装好的 {@link TableMeta}。
     *
     * <p>关键步骤：①OG 档先过<b>第四字典属主判定</b>——{@code partitionOf} 命中即走
     * {@link #resolvePartitionAnchored} 母表身份链（<b>不回退三表面</b>：第四字典
     * 'p' 行是分区段号的权威属主，而 EXCHANGE PARTITION 换绑后段号数值可与某普通表
     * oid 碰撞[OG 建表 relfilenode==oid、换绑双向对调段号]，三表的 oid 直认兜底在该
     * 面必产出错误属主——M3 T5 修复，OgPartitionIT 实测 + TableFilterTest ⑯ 锚；
     * 链断 fail-safe 跳过同样不回退——段号属主已定）；②未命中第四字典走三表解析
     * （{@link CatalogSnapshot#relOidOf(long)} 双入口归一 + 三道闸）；vanilla 档
     * 无第四字典面（ogDialect 门控——零变化锚）。边界与异常语义：任一闸不过或字典
     * 缺行返回 empty（不抛——miss 是常态而非异常；分区链断除外：WARN 一次 +
     * 计数的有痕面）；relnamespace 无对应 nsp 行（字典半损）视同 miss。线程约束：
     * 并发安全（见类 javadoc）。</p>
     *
     * @param relNodeOrOid relfilenode（heap 块反查形态）或关系 oid（元数据直查形态）
     * @return 表身份 + 列序；miss 为 empty
     */
    public Optional<TableMeta> resolve(long relNodeOrOid) {
        if (!ogDialect) {
            return resolveThreeTable(relNodeOrOid);
        }
        Optional<CatalogSnapshot.PartitionIdentity> part = snapshot.partitionOf(relNodeOrOid);
        if (part.isPresent()) {
            return resolvePartitionAnchored(part.get(), relNodeOrOid);
        }
        return resolveThreeTable(relNodeOrOid);
    }

    /**
     * 分区链断的跳过计数观测面（沿 nsp redirect miss 先例）——链断逐次递增、
     * volatile 读；正常 miss（第四字典无行/白名单不中）不计数。
     *
     * @return 本实例会话累计链断跳过次数
     */
    public long partitionChainMisses() {
        return partitionChainMisses.sum();
    }

    /**
     * 三表（pg_class/pg_attribute/pg_namespace）解析面——v2 既有路径逐字节不变：
     * relOidOf 归一 → relkind[OG 档含 parttype]/系统模式/白名单 → TableMeta 组装。
     *
     * <p>边界与异常语义同 {@link #resolve}（vanilla 档即本方法的全部语义）。
     * 线程约束：并发安全（见类 javadoc）。</p>
     *
     * @param relNodeOrOid relfilenode 或关系 oid
     * @return 表身份 + 列序；三表面 miss 为 empty
     */
    private Optional<TableMeta> resolveThreeTable(long relNodeOrOid) {
        OptionalLong oidOpt = snapshot.relOidOf(relNodeOrOid);
        if (oidOpt.isEmpty()) {
            return Optional.empty();
        }
        long oid = oidOpt.getAsLong();

        Optional<String> relkind = snapshot.relkindOf(oid);
        if (relkind.isEmpty() || !isUserTableKind(relkind.get(), oid)) {
            return Optional.empty();
        }
        Optional<String> schema = snapshot.schemaOf(oid);
        if (schema.isEmpty() || SYSTEM_SCHEMAS.contains(schema.get())) {
            return Optional.empty();
        }
        Optional<String> name = snapshot.nameOf(oid);
        if (name.isEmpty()) {
            return Optional.empty();
        }
        if (!whitelist.isEmpty() && !whitelist.contains(schema.get() + "." + name.get())) {
            return Optional.empty();
        }
        return Optional.of(assembleMeta(oid, schema.get(), name.get()));
    }

    /**
     * OG 档分区身份链（M3 T2 第四步，T0 锚②；T5 起由 {@link #resolve} 的第四字典
     * 属主判定先行调用）：分区身份（第四字典反查所得）→ parentid 母表链 → 按<b>母表
     * 身份</b>过闸与组装。
     *
     * <p>关键步骤：①母表闸——relkindOf(parentid) 缺行 = <b>链断</b>（parentid
     * 悬空——WARN 一次 + 计数）；形态闸不过（非用户表 kind）为正常 miss；②schema
     * 解析缺行（nsp 半损）= 链断、系统模式为正常 miss；nameOf 缺行 = 链断；③白名单
     * <b>双匹配</b>（母表全名 OR 分区全名——schema 取母表解析值；母表名 = 基准
     * 形态，分区名 = 兼容增强）；④TableMeta = 母表 oid/母表名/母表列集
     * （columnsOf(parentid)）。边界与异常语义：链断与正常 miss 同返 empty
     * （fail-safe 跳过不抛——差面只在内嵌痕迹/计数；<b>调用方不再回退三表解析</b>，
     * 段号属主已由第四字典判定，见 {@link #resolve} 的 oid 碰撞锚）；列集空（母表
     * 无 attr 行的字典半损）照常组装（空列的 TableMeta 由解码层自然兜住，与三表面
     * 同口径）。线程约束：并发安全（计数/WARN 节流原子面）。</p>
     *
     * @param identity             第四字典命中的分区身份（'p' 行——relname/parentid）
     * @param partitionRelfilenode 分区物理段号（heap 记录路由锚——链断痕迹面）
     * @return <b>母表身份</b>的表身份 + 母表列序；miss/链断为 empty
     */
    private Optional<TableMeta> resolvePartitionAnchored(CatalogSnapshot.PartitionIdentity identity,
            long partitionRelfilenode) {
        long parentOid = identity.parentid();

        Optional<String> relkind = snapshot.relkindOf(parentOid);
        if (relkind.isEmpty()) {
            notePartitionChainMiss(partitionRelfilenode, identity, "母表不在 pg_class 字典（parentid 悬空）");
            return Optional.empty();
        }
        if (!isUserTableKind(relkind.get(), parentOid)) {
            return Optional.empty();   // 母表行在但形态/列闸不过：正常 miss
        }
        Optional<String> schema = snapshot.schemaOf(parentOid);
        if (schema.isEmpty()) {
            notePartitionChainMiss(partitionRelfilenode, identity, "母表 relnamespace 无 nsp 行（字典半损）");
            return Optional.empty();
        }
        if (SYSTEM_SCHEMAS.contains(schema.get())) {
            return Optional.empty();
        }
        Optional<String> name = snapshot.nameOf(parentOid);
        if (name.isEmpty()) {
            notePartitionChainMiss(partitionRelfilenode, identity, "母表 relname 缺行（字典半损）");
            return Optional.empty();
        }
        if (!whitelist.isEmpty()) {
            String motherFullName = schema.get() + "." + name.get();
            String partitionFullName = schema.get() + "." + identity.relname();
            if (!whitelist.contains(motherFullName) && !whitelist.contains(partitionFullName)) {
                return Optional.empty();
            }
        }
        return Optional.of(assembleMeta(parentOid, schema.get(), name.get()));
    }

    /**
     * TableMeta 组装公共面（三表/分区链同构）：columnsOf 的有序含 dropped 占位
     * 投影转 {@link ColumnMeta} 列表。
     *
     * @param oid    关系 oid（分区链形态 = 母表 oid）
     * @param schema 模式名
     * @param name   表名（分区链形态 = 母表名）
     * @return 组装好的 TableMeta
     */
    private TableMeta assembleMeta(long oid, String schema, String name) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (CatalogSnapshot.Column col : snapshot.columnsOf(oid)) {
            columns.add(new ColumnMeta(col.attnum(), col.name(), col.typeOid(), col.dropped()));
        }
        return new TableMeta(oid, schema, name, columns);
    }

    /**
     * 分区链断的有痕跳过（沿 nsp redirect miss 先例）：递增
     * {@code partitionChainMisses} 计数 + <b>整实例恰一次 WARN</b>（含
     * relfilenode/分区名/parentid 上下文与原因——常见形态：第四字典与 pg_class
     * 字典半损/窗口期 DDL），后续链断走 DEBUG（防刷屏）。
     *
     * <p>线程约束：LongAdder/AtomicBoolean 原子面，并发安全（WARN"恰一次"跨线程
     * 由 CAS 保证）。</p>
     *
     * @param relfilenode 链断的分区段号
     * @param identity    第四字典命中的分区身份
     * @param why         链断原因（诊断上下文）
     */
    private void notePartitionChainMiss(long relfilenode, CatalogSnapshot.PartitionIdentity identity, String why) {
        partitionChainMisses.increment();
        if (partitionChainWarned.compareAndSet(false, true)) {
            LOG.warn("openGauss 分区身份链断裂——分区行 fail-safe 跳过: relfilenode={}, partition={},"
                    + " parentid={}, 原因={}（后续链断只递增 partitionChainMisses 计数不再告警；"
                    + "常见形态：第四字典与 pg_class 字典半损/窗口期 DDL，重启全新引导自愈）",
                    relfilenode, identity.relname(), identity.parentid(), why);
        } else {
            LOG.debug("分区身份链断裂跳过: relfilenode={}, partition={}, parentid={}, 原因={}",
                    relfilenode, identity.relname(), identity.parentid(), why);
        }
    }

    /**
     * relkind 放行判定（版本感知）：vanilla 档 'r'（普通表）或 'p'（分区表）——其余
     * relkind（toast 't'、索引 'i'/'I'、视图 'v'、物化视图 'm'、序列 'S'、组合类型
     * 'c'、外部表 'f' 等）都不是行变更语义的承载面；OG 档收窄为仅 'r'（OG 无
     * relkind 'p' 值——分区母表 relkind 仍 'r'）并叠 parttype 闸（'n'/'p' 放行、
     * 未转录值 miss、不可判 fail-open，见 {@link #isAdmittedOgParttype}）。
     *
     * @param relkind relkind 单字符（pg_class.relkind）
     * @param oid     关系 oid（OG 档 parttype 闸的查询键）
     * @return 用户表形态 true
     */
    private boolean isUserTableKind(String relkind, long oid) {
        if (!ogDialect) {
            return "r".equals(relkind) || "p".equals(relkind);
        }
        if (!"r".equals(relkind)) {
            return false;
        }
        Optional<String> parttype = snapshot.parttypeOf(oid);
        return parttype.isEmpty() || isAdmittedOgParttype(parttype.get());
    }

    /**
     * OG 档 parttype 放行判定：'n'（普通表）与 'p'（分区母表）放行——接口语义
     * parttype∈{'n','p'}（og-plan 实测值域恰此两值）；其余非空值 miss（未转录的
     * 分区路由形态防御面）；空串视同不可判 fail-open 放行（与 parttypeOf empty
     * 同向——读区越界零填充/NUL 的既有防御口径，见 CatalogReplay 护栏）。
     *
     * <p>线程约束：纯函数（实例字段只读）。</p>
     *
     * @param parttype parttype 值（空串 = 不可判）
     * @return 'n'/'p'/空串 true；其余 false
     */
    private static boolean isAdmittedOgParttype(String parttype) {
        return parttype.isEmpty() || parttype.charAt(0) == 'n' || parttype.charAt(0) == 'p';
    }
}
