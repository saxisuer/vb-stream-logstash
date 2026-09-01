package org.vastdata.debezium.connector.postgresql.stream.protocol;

/**
 * Relation 消息的单列元数据。引擎 {@code Column} 的更名重写
 * （连接器包内 Column 语义易与 Debezium 同名类混淆，故更名 RelationColumn），组件一致。
 *
 * @param name 列名（CString 解码）
 * @param typeId 列类型的 oid（映射 PG 类型系统）
 * @param typeModifier 列类型修饰符 atttypmod（PG 18 起随 pgoutput Relation 消息下发）
 * @param partOfKey 列 flags 的 bit0——该列是否属于 replica identity 键（UPDATE/DELETE
 *                  旧元组按此挑选），其余 flag 位协议未定义
 */
public record RelationColumn(String name, int typeId, int typeModifier, boolean partOfKey) {}
