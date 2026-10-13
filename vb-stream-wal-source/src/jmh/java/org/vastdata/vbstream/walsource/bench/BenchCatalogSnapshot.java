package org.vastdata.vbstream.walsource.bench;

import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.replay.CatalogRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 基准离线档的 {@link CatalogSnapshot} 实现（wal-bench）——把 {@link CatalogStores}
 * 直暴露为 as-of 查询面，查询语义按 {@code CatalogSynchronizer.SnapshotView} 复刻
 * （该视图类是同步器私有内类、其构造缝包私有，离线基准无法复用，故在此按同语义
 * 新写）：列字典按 attrelid 值域过滤 + attnum 升序（含 dropped 占位）、class 行首配
 * 即止、relOidOf 双入口（relfilenode 反查 → oid 直认，前置四 watched 段号直认——
 * VastBase/OG 的映射目录 relfilenode 列值恒 0，无直认则三表 heap 记录恒 miss）、
 * partitionOf 第四字典 'p' 行投影。
 *
 * <p>与生产视图的唯一形态差：前沿 {@link #lsn()} 由基准回放循环经 {@link #advanceTo}
 * 单写推进（生产视图由同步器 apply 维护）——消费方（XactGrouper 的表身份解析）不读
 * 前沿，该值仅保持契约完备。线程约束：单写者（基准回放线程）+ 弱一致并发读（stores
 * 载体为 ConcurrentHashMap，与生产视图同口径）。</p>
 */
public final class BenchCatalogSnapshot implements CatalogSnapshot {

    /** pg_attribute 的固定 oid（映射目录段号直认面，与同步器同值）。 */
    private static final long PG_ATTRIBUTE_OID = 1249L;

    /** pg_class 的固定 oid（同上）。 */
    private static final long PG_CLASS_OID = 1259L;

    /** pg_partition 的固定 oid（第四 watched 目录直认面，与同步器同值）。 */
    private static final long PG_PARTITION_OID = 9016L;

    private final CatalogStores stores;

    /** 已施加前沿镜像（最近一次 advanceTo 的记录 MAXALIGN 末尾；volatile 跨线程读）。 */
    private volatile long frontier;

    /**
     * 装配快照视图。
     *
     * @param stores 重放状态容器（活引用——查询随字典重放推进）
     */
    public BenchCatalogSnapshot(CatalogStores stores) {
        this.stores = stores;
    }

    /**
     * 前沿推进（基准回放循环每记录后调用）——记录末尾 MAXALIGN 值由调用方算好传入。
     *
     * @param recordEndAligned 记录 MAXALIGN 末尾 LSN
     */
    public void advanceTo(long recordEndAligned) {
        frontier = Math.max(frontier, recordEndAligned);
    }

    /**
     * 已施加前沿（字典内容的 as-of 位点）。
     *
     * @return 打包 LSN（未推进为 0）
     */
    @Override
    public long lsn() {
        return frontier;
    }

    /**
     * 列字典查询：attrRows 值域过滤 attrelid 后按 attnum 升序投影，含 dropped 占位行；
     * 未知关系空表（语义同生产 SnapshotView）。
     *
     * @param relOid 关系 oid
     * @return 列列表（attnum 升序）
     */
    @Override
    public List<Column> columnsOf(long relOid) {
        List<Column> out = new ArrayList<>();
        for (CatalogRow.AttrRow row : stores.attrRows().values()) {
            if (row.attrelid() == relOid) {
                out.add(new Column(row.attnum(), row.attname(), row.atttypid(), row.attisdropped()));
            }
        }
        out.sort(Comparator.comparingInt(Column::attnum));
        return out;
    }

    /**
     * relfilenode 查询：classRows 值域首配 relOid（oid 目录内唯一）。
     *
     * @param relOid 关系 oid
     * @return 当前 relfilenode；不在字典 empty
     */
    @Override
    public OptionalLong relfilenodeOf(long relOid) {
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relOid() == relOid) {
                return OptionalLong.of(row.relfilenode());
            }
        }
        return OptionalLong.empty();
    }

    /**
     * toast 关系查询：与 {@link #relfilenodeOf(long)} 同源行取 reltoastrelid。
     *
     * @param relOid 关系 oid
     * @return toast 关系 oid（无 toast 为 of(0)）；不在字典 empty
     */
    @Override
    public OptionalLong toastOf(long relOid) {
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relOid() == relOid) {
                return OptionalLong.of(row.reltoastrelid());
            }
        }
        return OptionalLong.empty();
    }

    /**
     * 模式名查询：class 行取 relnamespace 再经 nsp 行字典解析 nspname（两段线性扫描）。
     *
     * @param relOid 关系 oid
     * @return 模式名；关系/nsp 行不在字典 empty
     */
    @Override
    public Optional<String> schemaOf(long relOid) {
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relOid() == relOid) {
                for (CatalogRow.NspRow nsp : stores.nspRows().values()) {
                    if (nsp.nspOid() == row.relnamespace()) {
                        return Optional.of(nsp.nspname());
                    }
                }
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * relkind 查询：class 行投影直供（裸单字符）。
     *
     * @param relOid 关系 oid
     * @return relkind；不在字典 empty
     */
    @Override
    public Optional<String> relkindOf(long relOid) {
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relOid() == relOid) {
                return Optional.ofNullable(row.relkind());
            }
        }
        return Optional.empty();
    }

    /**
     * 表名查询：class 行投影直供。
     *
     * @param relOid 关系 oid
     * @return relname；不在字典 empty
     */
    @Override
    public Optional<String> nameOf(long relOid) {
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relOid() == relOid) {
                return Optional.of(row.relname());
            }
        }
        return Optional.empty();
    }

    /**
     * relfilenode/oid 双入口归一：先与四 watched 段号比对直认固定 oid（VastBase 的
     * pg_class/pg_attribute/pg_partition 是 relation-mapped——pg_class 行 relfilenode
     * 列值 0，值域反查对物理段号恒 miss，直认是字典形态变更信号的唯一入口），再
     * relfilenode 值域反查，最后 oid 直认。
     *
     * @param relNodeOrOid relfilenode 或关系 oid
     * @return 关系 oid；无匹配 empty
     */
    @Override
    public OptionalLong relOidOf(long relNodeOrOid) {
        if (relNodeOrOid != 0) {
            if (relNodeOrOid == stores.pgAttrRelfilenode()) {
                return OptionalLong.of(PG_ATTRIBUTE_OID);
            }
            if (relNodeOrOid == stores.pgClassRelfilenode()) {
                return OptionalLong.of(PG_CLASS_OID);
            }
            if (relNodeOrOid == stores.pgPartitionRelfilenode()) {
                return OptionalLong.of(PG_PARTITION_OID);
            }
        }
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relfilenode() == relNodeOrOid) {
                return OptionalLong.of(row.relOid());
            }
        }
        for (CatalogRow.ClassRow row : stores.classRows().values()) {
            if (row.relOid() == relNodeOrOid) {
                return OptionalLong.of(relNodeOrOid);
            }
        }
        return OptionalLong.empty();
    }

    /**
     * 分区身份链查询：partitionRows 值域首配 relfilenode 且行身份 'p'（母表 'r' 行
     * relfilenode 恒 0 不入路由面），投影分区名 + 母表 oid。
     *
     * @param relfilenode 分区物理段号
     * @return 分区身份；无 'p' 行匹配 empty
     */
    @Override
    public Optional<PartitionIdentity> partitionOf(long relfilenode) {
        for (CatalogRow.PartitionRow row : stores.partitionRows().values()) {
            if (row.relfilenode() == relfilenode && "p".equals(row.parttype())) {
                return Optional.of(new PartitionIdentity(row.relname(), row.parentid()));
            }
        }
        return Optional.empty();
    }
}
