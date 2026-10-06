package org.vastdata.vbstream.walsource.changes;

import java.util.List;

/**
 * 一张用户表的变更流元数据（v2 MS2）——{@link TableFilter} 自 v1 catalog 字典
 * （pg_class/pg_attribute/pg_namespace 的 as-of 快照）解析出的表身份面 + 列序面，
 * 随 {@code RowChange} 逐行携带（值渲染不回查字典，行自描述）。
 *
 * <p>{@link #columns} 是 {@code CatalogSnapshot.columnsOf} 的有序含 dropped 占位
 * 直供投影：attnum 升序、DROP COLUMN 留下的存储占位列保位（值面恒渲染
 * {@code ∅}）——列序与 tuple 值数组的对位基准。record 不可变（columns 经
 * {@link List#copyOf} 防御拷贝），并发安全。</p>
 *
 * @param relOid  关系 oid（pg_class.oid——TRUNCATE/重写后不变的身份键）
 * @param schema  模式名（pg_namespace.nspname，经 relnamespace 解析，如 "public"）
 * @param table   表名（pg_class.relname）
 * @param columns 列元数据（attnum 升序，含 dropped 占位）
 */
public record TableMeta(long relOid, String schema, String table, List<ColumnMeta> columns) {

    /**
     * 紧凑构造：列列表防御拷贝（组件不可变语义——调用方后续改源列表不影响本记录）。
     */
    public TableMeta {
        columns = List.copyOf(columns);
    }
}
