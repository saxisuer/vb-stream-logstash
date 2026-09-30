package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.util.List;

/**
 * 一行的列值序列——Insert.newTuple / Update 新旧元组 / Delete.oldTuple 的载荷。
 * 引擎同名 record 的 1:1 重写。列值与 Relation.columns 按位置一一对应
 * （列布局消息先于 DML 到达，下游按列序对齐渲染）。
 *
 * @param columns 每列一个 {@link TupleValue}（空值用 TupleValue.NULL、TOAST 未变用
 *                TupleValue.UNCHANGED_TOAST，元素不为 null）；不可变 List 由 parser
 *                构造侧 {@code List.copyOf} 保证，record 自身不再复制
 */
public record TupleData(List<TupleValue> columns) {}
