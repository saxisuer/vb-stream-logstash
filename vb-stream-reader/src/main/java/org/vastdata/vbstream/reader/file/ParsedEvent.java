package org.vastdata.vbstream.reader.file;

import org.vastdata.vbstream.format.Op;
import org.vastdata.vbstream.format.TableDef;

/**
 * envelope 解析结果：写 VBFG 文件所需的表定义 + 操作 + 对齐列值。
 * values 数组组件显式值相等语义（record 默认对数组退化引用相等）。
 */
record ParsedEvent(TableDef tableDef, Op op, Object[] values, boolean truncate,
                   long lsn, long txid, long timestampMs) {

    ParsedEvent {
        values = values.clone();
    }

    @Override
    public Object[] values() {
        return values.clone();
    }
}
