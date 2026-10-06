package org.vastdata.vbstream.walsource.changes;

import org.vastdata.vbstream.walsource.api.CatalogSnapshot;

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

    /**
     * 装配过滤器。
     *
     * @param snapshot  catalog as-of 快照（表身份/列序查询面）
     * @param whitelist 白名单（{@code schema.table} 全名集；null 或空集 = 用户表全放行；
     *                   非 null 时经 {@link Set#copyOf} 防御拷贝——调用方后续改动不影响本实例）
     */
    public TableFilter(CatalogSnapshot snapshot, Set<String> whitelist) {
        this.snapshot = snapshot;
        this.whitelist = whitelist == null ? Set.of() : Set.copyOf(whitelist);
    }

    /**
     * 解析一个关系到变更流表身份：relfilenode 反查优先（heap 块形态——块头只携带
     * relfilenode），miss 再按 oid 直认（元数据形态）；三道闸（relkind/系统模式/
     * 白名单）全过返回组装好的 {@link TableMeta}。
     *
     * <p>关键步骤：①{@link CatalogSnapshot#relOidOf(long)} 双入口归一到关系 oid，
     * 字典无行（未引导/已删除/未登记）即 empty；②relkind/系统模式/白名单逐闸过滤；
     * ③组装 TableMeta（schema/table/列序投影）。边界与异常语义：任一闸不过或字典
     * 缺行返回 empty（不抛——miss 是常态而非异常）；relnamespace 无对应 nsp 行
     * （字典半损）视同 miss。线程约束：并发安全（见类 javadoc）。</p>
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
        if (relkind.isEmpty() || !isUserTableKind(relkind.get())) {
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
     * relkind 放行判定：'r'（普通表）或 'p'（分区表）——其余 relkind（toast 't'、
     * 索引 'i'/'I'、视图 'v'、物化视图 'm'、序列 'S'、组合类型 'c'、外部表 'f' 等）
     * 都不是行变更语义的承载面。
     *
     * @param relkind relkind 单字符（pg_class.relkind）
     * @return 用户表形态 true
     */
    private static boolean isUserTableKind(String relkind) {
        return "r".equals(relkind) || "p".equals(relkind);
    }
}
