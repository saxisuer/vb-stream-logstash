package org.vastdata.vbstream.format;

/** TRUNCATE 事件。读取端已把 tableDefId 解析为完整表定义。 */
public record TruncateRecord(TableDef tableDef) implements Record {
}
