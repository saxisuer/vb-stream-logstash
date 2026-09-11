package org.vastdata.vbstream.reader.format;

import java.util.List;
import java.util.Objects;

/**
 * 表定义。同一文件内首次出现时写入 TABLE_DEF 记录，后续事件通过 id 引用；
 * id 由写入器按文件分配（defId 是文件内作用域），列定义变化（DDL）分配新的 id。
 *
 * <p>{@code equals}/{@code hashCode} 刻意<b>忽略 id</b>（比较业务定义）——写入器靠此做
 * 文件内 TABLE_DEF 去重：同一表结构只写一条，结构变化才写新的一条。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 TableDef（2026-09-11 快照）。
 */
public record TableDef(int id, String db, String schema, String table,
                       List<String> keyColumns, List<ColumnDef> columns) {

    public TableDef {
        columns = List.copyOf(columns);
        keyColumns = List.copyOf(keyColumns);
    }

    /** 不带 id 的构造（id 分配前使用），相等性比较忽略 id。 */
    public static TableDef of(String db, String schema, String table,
                              List<String> keyColumns, List<ColumnDef> columns) {
        return new TableDef(0, db, schema, table, keyColumns, columns);
    }

    /** 业务身份与结构是否一致（忽略 id）。 */
    public boolean sameDefinition(TableDef other) {
        return db.equals(other.db) && schema.equals(other.schema) && table.equals(other.table)
                && keyColumns.equals(other.keyColumns) && columns.equals(other.columns);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TableDef other && sameDefinition(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, schema, table, keyColumns, columns);
    }
}
