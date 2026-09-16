package org.vastdata.vbstream.format;

/**
 * envelope 解析结果:写落地文件所需的表定义 + 操作 + 对齐列值(binary 与 sql 两种格式的
 * 共享 IR——Values 编码与 SqlRenderer 渲染都以本 record 为输入)。
 * values 数组组件显式值相等语义(record 默认对数组退化引用相等)。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 ParsedEvent(2026-09-16 快照,原在
 * reader file 包,拆分三模块时下沉基座)。
 */
public record ParsedEvent(TableDef tableDef, Op op, Object[] values, boolean truncate,
                          long lsn, long txid, long timestampMs) {

    public ParsedEvent {
        values = values.clone();
    }

    @Override
    public Object[] values() {
        return values.clone();
    }
}
