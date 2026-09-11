package org.vastdata.vbstream.format;

/**
 * DML 事件。values 与所属 TableDef 的 columns 一一对应，null 表示该列为 NULL。
 * values 数组组件显式值相等语义（record 默认对数组退化引用相等）。
 */
public record EventRecord(int tableDefId, Op op, Object[] values) implements Record {

    public EventRecord {
        values = values.clone();
    }

    @Override
    public Object[] values() {
        return values.clone();
    }
}
