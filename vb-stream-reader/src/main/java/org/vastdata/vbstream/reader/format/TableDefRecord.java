package org.vastdata.vbstream.reader.format;

/** 表定义记录：文件内首次出现某表（或列结构变化）时写入。 */
public record TableDefRecord(TableDef tableDef) implements Record {
}
