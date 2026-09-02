package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * {@link RelationResolver} 的真实现:'R' wire Relation → {@link ResolvedRelation}(wire +
 * Debezium {@link Table} 双形态)。模板是 vanilla {@code PgOutputMessageDecoder.
 * handleRelationMessage} + {@code resolveRelationFromMetadata}(DBZ 3.6.1.Final sources
 * 实证口径,javadoc 记档):
 *
 * <ul>
 *   <li><b>列序 = wire 列序</b>(协议列序是元组位序的真源,Table 列按到达顺序建)——
 *       vanilla 逐 wire 列建 ColumnMetaData 后依序 resolveRelationFromMetadata 同构</li>
 *   <li><b>类型五件套</b>经 {@link RelationMetadataSource#type(int, int)}(生产实现 =
 *       TypeRegistry.get(oid) + typmod 精度换算);ColumnEditor 签名注意
 *       {@code type(String, String)} 双参(名 + 带修饰表达式),无 typeName 方法</li>
 *   <li><b>可选性/默认值 JDBC enrich</b>:名称对位查 {@code tableColumns};JDBC 未登记的
 *       列 optional 回落 true(vanilla 口径——宁可选勿漏 NULL)</li>
 *   <li><b>主键 = JDBC 读出的 PK 名</b>(含空回落唯一索引),<b>retainAll 到 wire 列集</b>
 *       ——vanilla 的"来自未来的 PK"防御(读元数据晚于 'R' 生成时,后加的 PK 列不进本版本)。
 *       brief 的"PK=wire flags bit0"未采纳:旗标是 replica identity 标记,REPLICA IDENTITY
 *       FULL 下全列置位,当 PK 会生成全列键,与 Debezium 键语义不符(差异上报)</li>
 *   <li>重名列防御:同名小写撞车抛 DebeziumException(vanilla 逐字保留——大小写仅差
 *       的列会造成数据错位)</li>
 * </ul>
 *
 * <p>线程约束:仅 reader 线程调用('R' 到达点;JDBC enrich 持 main 连接,见
 * {@link RelationMetadataSource});实例无状态可复用。
 */
public final class RelationTableFactory implements RelationResolver {

    /** 元数据接缝(类型五件套 + JDBC enrich;生产实现连库,测试假实现直供)。 */
    private final RelationMetadataSource metadata;

    /**
     * 构造工厂。
     *
     * @param metadata 元数据接缝(生产实现经 {@link RelationMetadataSource#jdbc} 适配)
     */
    public RelationTableFactory(RelationMetadataSource metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    /**
     * 责任:把一条 'R' 消息解析成版本日志载荷。关键步骤:TableId(catalog null,schema/table
     * 取 wire)→ JDBC enrich 两查(列元数据 + 主键名,SQLException 包装 DebeziumException
     * fail-fast)→ 逐 wire 列建 ColumnEditor(类型五件套 + 可选性/默认值,重名防御)→
     * PK retainAll(wire 列集)→ TableEditor 建表,连同 wire 原样封进 ResolvedRelation。
     * 边界:wire 为 null 抛 NPE(接缝契约);JDBC 查询失败经 DebeziumException 终止
     * reader 线程('R' 无法解析则后续 DML 必错位, fail-fast 优于带病运行);
     * wire 列数与 JDBC 列数不一致属常态(元数据时刻漂移),按名对位 enrich、
     * miss 列回落 optional=true。
     *
     * @param seq  该 'R' 消息序号(诊断归因,不参与解析)
     * @param wire live 解码的 wire Relation
     * @return 双形态表定义(两组件均不可变)
     */
    @Override
    public ResolvedRelation resolve(long seq, PgOutputMessage.Relation wire) {
        Objects.requireNonNull(wire, "wire");
        TableId tableId = new TableId(null, wire.schema(), wire.table());

        Map<String, Optional<String>> columnDefaults;
        Map<String, Boolean> columnOptionality;
        List<String> primaryKeyColumns;
        try {
            List<Column> readColumns = metadata.tableColumns(tableId);
            columnDefaults = new HashMap<>();
            columnOptionality = new HashMap<>();
            for (Column column : readColumns) {
                columnDefaults.put(column.name(), column.defaultValueExpression());
                columnOptionality.put(column.name(), column.isOptional());
            }
            primaryKeyColumns = new ArrayList<>(metadata.primaryKeyNames(tableId));
        }
        catch (SQLException e) {
            throw new DebeziumException("读取表元数据失败(oid=" + wire.relationOid() + ", seq=" + seq + "): " + tableId, e);
        }

        Set<String> columnNames = new HashSet<>();
        Set<String> seenLowercaseNames = new HashSet<>();
        List<Column> columns = new ArrayList<>(wire.columns().size());
        for (var wireColumn : wire.columns()) {
            String columnName = wireColumn.name();
            if (!seenLowercaseNames.add(columnName.toLowerCase())) {
                throw new DebeziumException(String.format(
                        "表 '%s' 存在仅大小写不同的重名列 '%s':列名错位会造成数据污染,请改名后重试",
                        tableId, columnName));
            }
            RelationMetadataSource.ColumnType type = metadata.type(wireColumn.typeId(), wireColumn.typeModifier());
            ColumnEditor editor = Column.editor()
                    .name(columnName)
                    .jdbcType(type.jdbcType())
                    .nativeType(type.rootOid())
                    .optional(columnOptionality.getOrDefault(columnName, true))
                    .type(type.typeName(), type.typeExpression())
                    .length(type.length())
                    .scale(type.scale());
            Optional<String> defaultValue = columnDefaults.get(columnName);
            if (defaultValue != null) {
                editor.defaultValueExpression(defaultValue.orElse(null));
            }
            columns.add(editor.create());
            columnNames.add(columnName);
        }

        // "来自未来的 PK"防御(vanilla retainAll 口径):读元数据晚于 'R' 生成时,
        // 后加的 PK 列不属本版本,保留会生成悬空键
        primaryKeyColumns.retainAll(columnNames);

        TableEditor tableEditor = Table.editor()
                .tableId(tableId)
                .addColumns(columns)
                .setPrimaryKeyNames(primaryKeyColumns);
        return new ResolvedRelation(wire, tableEditor.create());
    }
}
