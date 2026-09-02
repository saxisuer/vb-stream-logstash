package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputStreamDecoder;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RelationTableFactory 单测(离线,'R' wire 解码 + 假 JDBC 元数据;真库归 Task 8):
 * 'R' 字节经生产解码器解码成 wire Relation,工厂在假 {@link RelationMetadataSource} 之上
 * 建 Debezium {@link Table}——断言列序(=元组位序真源)、类型五件套(typeName/typeExpression/
 * jdbcType/nativeOid/length-scale 经接缝)、可选性与默认值的 JDBC enrich 口径、
 * PK 的 JDBC 真源 + "来自未来的 PK"剔除(vanilla retainAll 口径),以及元数据查询失败的
 * fail-fast 包装。
 *
 * <p>夹具约定:假元数据源以 Map/字段直供(typeId → ColumnType、TableId → 列/PK),
 * 不连库;PgWire 富 Relation 重载构造类型 oid/typmod 变化面。无并发面。
 */
class RelationTableFactoryTest {

    /** 测试表 oid。 */
    private static final int OID = 16385;

    /** varchar oid(PG 真值 1043)。 */
    private static final int VARCHAR_OID = 1043;

    /**
     * 假元数据源:type 按映射表直供(未命中抛 AssertionError——类型断言不该 miss);
     * tableColumns/primaryKeyNames 按字段直供(可按用例改写)。
     */
    private static final class FakeMetadataSource implements RelationMetadataSource {
        final Map<Integer, RelationMetadataSource.ColumnType> types = new HashMap<>();
        final Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        final Map<TableId, List<String>> pkByTable = new HashMap<>();
        boolean failQueries;

        @Override
        public RelationMetadataSource.ColumnType type(int oid, int typeModifier) {
            RelationMetadataSource.ColumnType t = types.get(oid);
            if (t == null) {
                throw new AssertionError("测试未登记类型 oid=" + oid);
            }
            return t;
        }

        @Override
        public List<Column> tableColumns(TableId tableId) throws SQLException {
            if (failQueries) {
                throw new SQLException("boom");
            }
            return columnsByTable.getOrDefault(tableId, List.of());
        }

        @Override
        public List<String> primaryKeyNames(TableId tableId) throws SQLException {
            if (failQueries) {
                throw new SQLException("boom");
            }
            return pkByTable.getOrDefault(tableId, List.of());
        }
    }

    /**
     * 责任:登记三类型(int4/text/varchar[typmod 由接缝消费侧体现])并构造工厂 +
     * 解码 'R' 字节成 wire Relation——三列 (id int4, v varchar(20), note text)。
     */
    private static PgOutputMessage.Relation decodeRelation() {
        byte[] raw = PgWire.relation(OID, "public", "t_rich", 'd',
                new PgWire.Col("id", true, 23, -1),
                new PgWire.Col("v", false, VARCHAR_OID, 20),
                new PgWire.Col("note", false, 25, -1));
        return (PgOutputMessage.Relation) new PgOutputStreamDecoder(MODE).decode(ByteBuffer.wrap(raw));
    }

    /** 责任:登记 int4/text/varchar 的类型五件套(与生产 ColumnMetaData 逻辑同构的期望值)。 */
    private static void registerTypes(FakeMetadataSource source) {
        source.types.put(23, new RelationMetadataSource.ColumnType("int4", "int4", Types.INTEGER, 23, -1, 0));
        source.types.put(25, new RelationMetadataSource.ColumnType("text", "text", Types.VARCHAR, 25, -1, 0));
        source.types.put(VARCHAR_OID, new RelationMetadataSource.ColumnType("varchar", "varchar(20,0)", Types.VARCHAR, VARCHAR_OID, 20, 0));
    }

    /** StreamingMode 夹具常量(解码器构造参数,与列断言无关)。 */
    private static final org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode MODE =
            org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode.ON;

    /**
     * 全属性建表:TableId 取 wire 的 schema/table(catalog 为 null——vanilla
     * {@code new TableId(null, schemaName, tableName)} 同款);列序 = wire 列序;
     * jdbcType/nativeOid/typeName/typeExpression 来自类型接缝;可选性与默认值来自 JDBC
     * enrich(JDBC 未登记的列回落 optional=true——vanilla 缺省口径)。
     */
    @Test
    void buildsTableWithWireOrderAndJdbcEnrichment() {
        FakeMetadataSource source = new FakeMetadataSource();
        registerTypes(source);
        TableId tableId = new TableId(null, "public", "t_rich");
        var id = Column.editor().name("id").jdbcType(Types.INTEGER).type("int4").optional(false).create();
        var v = Column.editor().name("v").jdbcType(Types.VARCHAR).type("varchar").optional(true).create();
        source.columnsByTable.put(tableId, new ArrayList<>(List.of(id, v)));
        source.pkByTable.put(tableId, List.of("id"));

        PgOutputMessage.Relation wire = decodeRelation();
        ResolvedRelation resolved = new RelationTableFactory(source).resolve(7L, wire);

        assertSame(wire, resolved.wire(), "wire 形态原样入版本日志载荷");
        Table table = resolved.table();
        assertEquals(tableId, table.id());
        assertEquals(List.of("id", "v", "note"), table.columns().stream().map(Column::name).toList(),
                "列序 = wire 列序(元组位序真源)");
        assertEquals(Types.INTEGER, table.columns().get(0).jdbcType());
        assertEquals(23, table.columns().get(0).nativeType());
        assertEquals("int4", table.columns().get(0).typeName());
        assertEquals("varchar(20,0)", table.columns().get(1).typeExpression(),
                "typeExpression 带修饰(类型接缝产出)");
        assertFalse(table.columns().get(0).isOptional(), "JDBC enrich:id 非空");
        assertTrue(table.columns().get(2).isOptional(), "JDBC 未登记 note → optional 缺省 true(vanilla 口径)");
        assertTrue(table.columns().get(1).isOptional());
        assertEquals(List.of("id"), table.primaryKeyColumnNames());
    }

    /** 默认值 enrich:JDBC 登记了默认值的列带上 defaultValueExpression,未登记的没有。 */
    @Test
    void enrichesColumnDefaultsFromJdbc() {
        FakeMetadataSource source = new FakeMetadataSource();
        registerTypes(source);
        TableId tableId = new TableId(null, "public", "t_rich");
        var id = Column.editor().name("id").jdbcType(Types.INTEGER).type("int4").optional(false).create();
        var v = Column.editor().name("v").jdbcType(Types.VARCHAR).type("varchar").optional(true)
                .defaultValueExpression("'-'").create();
        source.columnsByTable.put(tableId, List.of(id, v));
        source.pkByTable.put(tableId, List.of("id"));

        Table table = new RelationTableFactory(source).resolve(1L, decodeRelation()).table();
        assertEquals(Optional.of("'-'"), table.columns().get(1).defaultValueExpression(), "JDBC 默认值带入");
        assertTrue(table.columns().get(0).defaultValueExpression().isEmpty(), "未登记默认值的列保持空");
        assertTrue(table.columns().get(2).defaultValueExpression().isEmpty());
    }

    /**
     * "来自未来的 PK"剔除(vanilla retainAll 口径):JDBC 读到的 PK 含 wire 已不存在的列
     * (DDL 竞态:读元数据晚于 'R' 生成)时被剔除;wire 仍存在的 PK 保留。
     */
    @Test
    void removesPrimaryKeyColumnsMissingFromWireRelation() {
        FakeMetadataSource source = new FakeMetadataSource();
        registerTypes(source);
        TableId tableId = new TableId(null, "public", "t_rich");
        source.columnsByTable.put(tableId, List.of());
        source.pkByTable.put(tableId, List.of("id", "dropped_future_pk"));

        Table table = new RelationTableFactory(source).resolve(1L, decodeRelation()).table();
        assertEquals(List.of("id"), table.primaryKeyColumnNames(),
                "wire 未出现的 PK 列被剔除,不生成悬空键");
    }

    /** 无 PK:JDBC 未读到主键时 Table 无键列(keySchema 缺位的合法形态,下游按无键事件处理)。 */
    @Test
    void buildsTableWithoutPrimaryKeyWhenJdbcHasNone() {
        FakeMetadataSource source = new FakeMetadataSource();
        registerTypes(source);
        source.columnsByTable.put(new TableId(null, "public", "t_rich"), List.of());

        Table table = new RelationTableFactory(source).resolve(1L, decodeRelation()).table();
        assertTrue(table.primaryKeyColumnNames().isEmpty());
    }

    /** fail-fast:JDBC 元数据查询抛 SQLException 时以 DebeziumException 包装上抛(终止 reader 线程)。 */
    @Test
    void wrapsMetadataQueryFailureAsDebeziumException() {
        FakeMetadataSource source = new FakeMetadataSource();
        registerTypes(source);
        source.failQueries = true;

        DebeziumException e = assertThrows(DebeziumException.class,
                () -> new RelationTableFactory(source).resolve(1L, decodeRelation()));
        assertEquals("boom", e.getCause().getMessage());
    }

    /**
     * 带引号标识符列名(DBZ-298 修复回归):pgoutput 对引号创建的列原样发送列名(引号保留),
     * 而 JDBC 元数据名不带引号——工厂须先去引号再比对:①建出的 Table 列名去引号;
     * ②enrich 按名命中(可选性取 JDBC 而非缺省 true);③PK retainAll 不误剔键列
     * (不去引号时 "my id" 与 my id 不等,键被剔、key schema 错——本用例即该 bug 的锚)。
     */
    @Test
    void unquotesQuotedColumnNamesBeforeJdbcEnrichmentAndPkRetention() {
        FakeMetadataSource source = new FakeMetadataSource();
        registerTypes(source);
        TableId tableId = new TableId(null, "public", "t_q");
        // JDBC 侧登记<b>不带引号</b>的名字(系统目录的真实形态);引号列非空、带默认值
        var quotedPk = Column.editor().name("my id").jdbcType(Types.INTEGER).type("int4").optional(false).create();
        var quotedV = Column.editor().name("my v").jdbcType(Types.VARCHAR).type("varchar").optional(true)
                .defaultValueExpression("'d'").create();
        source.columnsByTable.put(tableId, List.of(quotedPk, quotedV));
        source.pkByTable.put(tableId, List.of("my id"));

        byte[] raw = PgWire.relation(OID, "public", "t_q", 'd',
                new PgWire.Col("\"my id\"", true, 23, -1),
                new PgWire.Col("\"my v\"", false, 25, -1));
        PgOutputMessage.Relation wire = (PgOutputMessage.Relation) new PgOutputStreamDecoder(MODE)
                .decode(ByteBuffer.wrap(raw));
        Table table = new RelationTableFactory(source).resolve(1L, wire).table();

        assertEquals(List.of("my id", "my v"),
                table.columns().stream().map(Column::name).toList(),
                "Table 列名去引号落地(与 JDBC 元数据同名互证)");
        assertFalse(table.columns().get(0).isOptional(), "enrich 按去引号名命中:可选性取 JDBC");
        assertEquals(Optional.of("'d'"), table.columns().get(1).defaultValueExpression(), "默认值同理命中");
        assertEquals(List.of("my id"), table.primaryKeyColumnNames(),
                "PK retainAll 不误剔引号键列(key schema 正确性锚点)");
    }
}
