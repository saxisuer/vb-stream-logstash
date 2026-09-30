package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;

import java.sql.Types;

/**
 * 测试共享的假 {@link RelationResolver} 夹具(Task 3 账本回收项):把此前在
 * {@code StreamedTransactionAssemblerTest}/{@code SyncDeliveryTest}/{@code StreamingDeliveryTest}/
 * {@code DecoupledEquivalenceTest} 四处逐字重复的 "wire + 最小 Debezium Table" 工厂收拢为一处。
 * 不连库、不做 JDBC enrich——列沿 wire 列序全 {@code text}(jdbcType=VARCHAR),
 * TableId 取 wire 的 schema/table(同名互证);JDBC enrich 的真实现属
 * {@code RelationTableFactory}(Task 7,主代码),与本夹具互为对照面。
 *
 * <p>线程约束:纯函数工厂,线程安全;产物 {@link ResolvedRelation} 不可变可跨线程持有。
 */
final class TestRelations {

    /**
     * 共享假解析器:{@code (seq, wire) -> ResolvedRelation(wire, tableOf(wire))}——
     * 组装器/消费器/回放各测试的默认 {@link RelationResolver} 注入形态。
     */
    static final RelationResolver RESOLVER = (seq, wire) -> new ResolvedRelation(wire, tableOf(wire));

    /** 工具类禁止实例化。 */
    private TestRelations() {
    }

    /**
     * 责任:按 wire Relation 造最小 Debezium {@link Table}——TableId 取 wire 的
     * schema/table(同名互证),列沿 wire 列序逐列全 {@code text}(jdbcType=VARCHAR),
     * 不设主键(组装器路径不消费键信息;键语义归 RelationTableFactory 的 JDBC enrich)。
     * 边界:wire 为 null 时 TableId/列访问抛 NPE(测试样本恒非 null)。
     *
     * @param wire 已解码的 wire Relation(协议列序真源)
     * @return 最小 Table(仅 TableId + 列名/列型,无键/无默认值)
     */
    static Table tableOf(PgOutputMessage.Relation wire) {
        var editor = Table.editor().tableId(new TableId(null, wire.schema(), wire.table()));
        for (var col : wire.columns()) {
            editor.addColumn(Column.editor()
                    .name(col.name()).jdbcType(Types.VARCHAR).type("text").create());
        }
        return editor.create();
    }
}
