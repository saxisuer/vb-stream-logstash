package org.vastdata.vbstream.format.binary;

import org.vastdata.vbstream.format.TableDef;

/** 表定义记录：文件内首次出现某表（或列结构变化）时写入。 */
public record TableDefRecord(TableDef tableDef) implements Record {
}
