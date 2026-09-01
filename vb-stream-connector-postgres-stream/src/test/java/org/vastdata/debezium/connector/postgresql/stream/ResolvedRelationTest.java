package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;

import java.sql.Types;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link ResolvedRelation} 组件语义单测:wire / table 双形态组件的恒等交付、构造期拒 null、
 * 跨两形态的值相等。本任务是数据形态的定义点(真 Table 的 JDBC enrich 在 Task 7 的
 * {@code RelationTableFactory}),故此处以 {@code Table.editor()} 造最小假 Table(TableId +
 * 单列,不连库)。引擎无对应测试(引擎 registry 只存 wire Relation,无双形态泛化)。
 */
class ResolvedRelationTest {

    /**
     * 构造单列 wire Relation 样本。
     * 步骤:固定 schema=public、replicaIdentity='d'、一列 (name="c", typeId=25 text, typmod=-1,
     * partOfKey=true),仅 oid 与表名随参数变化。
     * 边界:无——纯样本工厂,不触达被测状态;streamXid 恒 empty(组件语义与流式前缀无关)。
     */
    private static PgOutputMessage.Relation rel(int oid, String name) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", name,
                'd', List.of(new RelationColumn("c", 25, -1, true)));
    }

    /**
     * 构造最小假 Debezium Table:TableId(public.name) + 单列 c(text)。
     * 步骤:Table.editor() 起手,tableId 定位,addColumn 补一列(jdbcType=VARCHAR、typeName=text),
     * create() 收口。列集与 wire 侧 rel 的单列同名同序,模拟 Task 7 真实 enrich 的最小形态。
     * 边界:catalog 传 null(PG 连接器惯例,TableId 内部归一为空串);不连库。
     */
    private static Table table(String name) {
        return Table.editor()
                .tableId(new TableId(null, "public", name))
                .addColumn(Column.editor().name("c").jdbcType(Types.VARCHAR).type("text").create())
                .create();
    }

    /**
     * 组件恒等交付:wire() / table() 返回构造时传入的确切实例(引用恒等,不是拷贝)——
     * registry 与快照按引用浅拷共享版本载荷的前提(record 不可变,共享安全)。
     * 关键步骤:同一 wire / table 实例构造 → assertSame 双组件 → 顺带断言组件内容可读
     * (wire 侧表名与 oid、Table 侧 TableId)。
     * 边界:无失败分支——恒等断言即全部。
     */
    @Test
    void accessorsExposeExactWireAndTableInstances() {
        PgOutputMessage.Relation wire = rel(7, "t_a");
        Table view = table("t_a");
        ResolvedRelation resolved = new ResolvedRelation(wire, view);
        assertSame(wire, resolved.wire());
        assertSame(view, resolved.table());
        assertEquals("t_a", resolved.wire().table());
        assertEquals(7, resolved.wire().relationOid());
        assertNotNull(resolved.table().id());
        assertEquals("t_a", resolved.table().id().table());
    }

    /**
     * 构造期 fail-fast 拒 null:任一组件为 null 立即 NPE(装配错误就地暴露,
     * 而不是留到回放渲染期解引用时炸在消费者线程)。
     * 关键步骤:分别以 null wire / null table 构造,各断言 NPE。
     * 边界:两分支都测——缺任一形态都是错误装配。
     */
    @Test
    void nullComponentsFailFastAtConstruction() {
        Table view = table("t_a");
        PgOutputMessage.Relation wire = rel(7, "t_a");
        assertThrows(NullPointerException.class, () -> new ResolvedRelation(null, view));
        assertThrows(NullPointerException.class, () -> new ResolvedRelation(wire, null));
    }

    /**
     * 值相等横跨双形态:wire(record 值相等) + Table(TableImpl 值相等)都相等才相等,
     * 任一形态不同即不等——版本日志同 seq 幂等去重之外的内容比较语义由此定义。
     * 关键步骤:两组各自新建的等值实例构造两个 ResolvedRelation → assertEquals 且 hashCode 一致;
     * 再分别只动 wire 表名 / 只动 Table 的 TableId → assertNotEquals。
     * 边界:等值分支用"新建实例"而非同实例,确保走的是值相等而非引用相等。
     */
    @Test
    void valueEqualitySpansBothForms() {
        ResolvedRelation a = new ResolvedRelation(rel(7, "t_a"), table("t_a"));
        ResolvedRelation b = new ResolvedRelation(rel(7, "t_a"), table("t_a"));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new ResolvedRelation(rel(7, "t_b"), table("t_a")));   // wire 不同
        assertNotEquals(a, new ResolvedRelation(rel(7, "t_a"), table("t_b")));   // Table 不同
    }
}
