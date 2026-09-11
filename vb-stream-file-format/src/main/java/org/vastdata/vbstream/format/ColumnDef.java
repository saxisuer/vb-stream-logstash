package org.vastdata.vbstream.format;

/**
 * 列定义。类型在 TABLE_DEF 中声明一次，事件记录只编码值。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 ColumnDef（2026-09-11 快照）。
 */
public record ColumnDef(String name, TypeCode type) {
}
