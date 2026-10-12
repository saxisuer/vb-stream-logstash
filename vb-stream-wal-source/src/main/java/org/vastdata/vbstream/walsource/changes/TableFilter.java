package org.vastdata.vbstream.walsource.changes;

import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.WalLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

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
 * <p><b>白名单物理表匹配规则（spec §3，路由实测锚定）</b>：白名单匹配对象 =
 * <b>物理承载行的表</b>——og-plan m2pg 实测（range t5_rng p1=32809/p2=32810 +
 * list t5_lst pa=32821/pb=32822 四锚，rmid=10 heap INSERT 块引用）：INSERT 的
 * heap 记录全部路由到<b>分区自己的 relfilenode</b>（pg_partition 'p' 行），母表
 * relfilenode 全窗零命中——母表不在匹配面；分区子表按物理表独立匹配。V17/V18 的
 * 分区子表是 pg_class 'r' 行（独立 relname），白名单写子表全名即命中。OG 的分区
 * 仅在 pg_partition（无 pg_class/pg_attribute 行，列字典挂母表 oid）——不在 v1
 * 三表字典解析面，resolve 对分区 relfilenode 天然 fail-safe miss（静默跳过），
 * "分区子表独立匹配白名单"在 OG 档的完整落地需 pg_partition 字典（M3/后续记档）。
 * V17/V18 路径（含两参兼容构造器）零变化。</p>
 *
 * <p>放行时组装 {@link TableMeta}：schema 经 relnamespace→pg_namespace.nspname
 * 解析、columns 取 {@link CatalogSnapshot#columnsOf} 的有序含 dropped 占位投影。
 * miss 面（relkind/系统模式/白名单/字典无行）一律 {@link Optional#empty()}——
 * 调用方（v2 桶发射）据此静默跳过该块，不抛不告警（噪声表属常态）。</p>
 *
 * <p>线程约束：持快照与白名单两不可变引用，查询面委派快照的弱一致语义
 * （见 {@link CatalogSnapshot}）——任意线程并发调用安全。</p>
 */
public final class TableFilter {

    /** 系统模式排除集：watched 目录（pg_catalog）与其 toast（pg_toast）、SQL 标准视图
     * （information_schema）所在模式——这些 'r' 表是目录/基础设施，不属变更流语义。 */
    private static final Set<String> SYSTEM_SCHEMAS = Set.of("pg_catalog", "pg_toast", "information_schema");

    private final CatalogSnapshot snapshot;

    private final Set<String> whitelist;

    /** OG 方言档门控（构造期定格）：true 时第一闸走 parttype 语义（见类 javadoc）。 */
    private final boolean ogDialect;

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
     * <p>关键步骤：①{@link CatalogSnapshot#relOidOf(long)} 双入口归一到关系 oid，
     * 字典无行（未引导/已删除/未登记）即 empty——OG 档的分区 relfilenode 即落此面
     * （不在 pg_class，fail-safe miss）；②relkind（OG 档再叠 parttype）/系统模式/
     * 白名单逐闸过滤；③组装 TableMeta（schema/table/列序投影）。边界与异常语义：
     * 任一闸不过或字典缺行返回 empty（不抛——miss 是常态而非异常）；relnamespace
     * 无对应 nsp 行（字典半损）视同 miss。线程约束：并发安全（见类 javadoc）。</p>
     *
     * @param relNodeOrOid relfilenode（heap 块反查形态）或关系 oid（元数据直查形态）
     * @return 表身份 + 列序；miss 为 empty
     */
    public Optional<TableMeta> resolve(long relNodeOrOid) {
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
        List<ColumnMeta> columns = new ArrayList<>();
        for (CatalogSnapshot.Column col : snapshot.columnsOf(oid)) {
            columns.add(new ColumnMeta(col.attnum(), col.name(), col.typeOid(), col.dropped()));
        }
        return Optional.of(new TableMeta(oid, schema.get(), name.get(), columns));
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
