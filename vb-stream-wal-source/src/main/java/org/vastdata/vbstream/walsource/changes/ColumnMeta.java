package org.vastdata.vbstream.walsource.changes;

/**
 * 用户表单列元数据——v2 值面（{@code DiskValueRenderer}）与后续 TableMeta/对拍契约的
 * 列描述原子。
 *
 * <p>来源是 v1 catalog 字典（{@code pg_attribute} 重放快照）的逐列投影：attnum 定位
 * tuple 值数组下标、name 供 {@code name=value} 对拍形态、typeOid 是
 * {@code DiskValueRenderer.render} 的分派键、dropped 标记 attisdropped 列（存储恒
 * NULL、零字节消耗——渲染恒 {@code ∅} 占位）。record 不可变，并发安全。</p>
 *
 * @param attnum  物理列号（pg_attribute.attnum，1 起；dropped 重建列会跳号）
 * @param name    列名（attname；dropped 列在 PG 侧形如 {@code ........pg.dropped.N........}）
 * @param typeOid 列类型 oid（atttypid，{@code DiskValueRenderer} 的分派键）
 * @param dropped attisdropped——ALTER TABLE ... DROP COLUMN 留下的占位列
 */
public record ColumnMeta(int attnum, String name, long typeOid, boolean dropped) {
}
