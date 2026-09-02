package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * 'R' 解析所需元数据的接缝(MS2 设计新增,引擎无对应物):{@link RelationTableFactory}
 * 建表需要两类外部事实——类型五件套(oid/typmod → typeName/typeExpression/jdbcType/
 * nativeOid/length/scale,真源是连库 {@link TypeRegistry})与 JDBC enrich(列可选性/
 * 默认值/主键名,真源是 main 连接的系统目录查询)。两者都只能连库构造(TypeRegistry 的
 * 构造即查询 pg_catalog),本接口把它们收窄成可注入的纯查询面,使工厂行为可离线单测;
 * 生产实现由 {@link #jdbc(PostgresConnection, TypeRegistry, ColumnNameFilter)} 适配。
 *
 * <p>线程约束:接口本身无状态;生产实现内部持 JDBC 连接与注册表,<b>仅 reader 线程调用</b>
 * ('R' 的 enrich 点;R3 审计答案——MS2 期 main JDBC 连接读者线程独占),实现无需并发原语。
 */
public interface RelationMetadataSource {

    /**
     * 责任:解析 (oid, typmod) → 列类型五件套。
     * 边界:未注册 oid 的行为由实现方声明(生产实现回落 UNKNOWN 类型,vanilla 同款)。
     *
     * @param oid          wire 列类型 oid('R' 消息携带)
     * @param typeModifier wire 列 typmod(varchar 长度/numeric 精度等的修饰来源)
     * @return 类型五件套(不可变)
     */
    ColumnType type(int oid, int typeModifier);

    /**
     * 责任:读某表全部列的 JDBC 元数据(可选性/默认值的 enrich 源)。
     * 边界:表不存在返回空列表(vanilla getTableColumnsForDecoder 同语义);抛
     * SQLException 由调用方(RelationTableFactory)包装 fail-fast。
     *
     * @param tableId 目标表(wire 的 schema/table 构成)
     * @return JDBC 侧列元数据(可能为空)
     */
    List<Column> tableColumns(TableId tableId) throws SQLException;

    /**
     * 责任:读某表的主键列名(有序)。
     * 边界:无主键返回空列表(调用方按无键表处理);抛 SQLException 同上 fail-fast。
     *
     * @param tableId 目标表
     * @return 主键列名序列(可能为空)
     */
    List<String> primaryKeyNames(TableId tableId) throws SQLException;

    /** 列类型五件套:RelationTableFactory 建 ColumnEditor 所需的类型事实全集。 */
    record ColumnType(String typeName, String typeExpression, int jdbcType, int rootOid, int length, int scale) {
    }

    /**
     * 责任:构造生产实现——TypeRegistry 提供类型五件套(长度/精度换算复刻 vanilla
     * {@code ColumnMetaData} 的 typmod 分支),main 连接提供 JDBC enrich(可选性/默认值
     * 经 {@code getTableColumnsForDecoder},主键经 {@code readPrimaryKeyNames},
     * 空主键回落唯一索引并 WARN——vanilla handleRelationMessage 同款次序)。
     * 边界:查询失败原样上抛 SQLException(由调用方包装);columnFilter 参与列元数据
     * 读取(vanilla 把它传给 getTableColumnsForDecoder 以校验包含列)。
     * 线程:产物持连接与注册表,仅 reader 线程调用。
     *
     * @param connection   main JDBC 连接(reader 线程独占)
     * @param typeRegistry 连库类型注册表(Task.start 的共享实例)
     * @param columnFilter 列包含过滤器(连接器配置)
     * @return 生产元数据源
     */
    static RelationMetadataSource jdbc(PostgresConnection connection, TypeRegistry typeRegistry,
                                       ColumnNameFilter columnFilter) {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(typeRegistry, "typeRegistry");
        return new RelationMetadataSource() {

            private static final Logger LOG = LoggerFactory.getLogger("org.vastdata.debezium.connector.postgresql.stream.RelationMetadataSource.jdbc");

            /** 责任:类型五件套换算(vanilla ColumnMetaData 构造体的等价逻辑:typmod 有值且 TypeInfo 可得走精度/标度换算,否则默认值)。 */
            @Override
            public ColumnType type(int oid, int typeModifier) {
                PostgresType type = typeRegistry.get(oid);
                PostgresType root = type.getRootType();
                int length;
                int scale;
                if (TypeRegistry.NO_TYPE_MODIFIER != typeModifier && type.getTypeInfo() != null) {
                    length = type.getTypeInfo().getPrecision(type.getOid(), typeModifier);
                    scale = type.getTypeInfo().getScale(type.getOid(), typeModifier);
                }
                else {
                    length = type.getDefaultLength();
                    scale = type.getDefaultScale();
                }
                String typeExpression = type.getName();
                if (!(length == type.getDefaultLength() && scale == 0)) {
                    typeExpression += "(" + length + "," + scale + ")";
                }
                return new ColumnType(type.getName(), typeExpression, root.getJdbcId(), root.getOid(), length, scale);
            }

            /** 责任:main 连接读列元数据(vanilla getTableColumnsForDecoder 委派)。 */
            @Override
            public List<Column> tableColumns(TableId tableId) throws SQLException {
                return connection.getTableColumnsForDecoder(tableId, columnFilter);
            }

            /** 责任:主键名(空时 WARN 回落唯一索引——vanilla handleRelationMessage 同款)。 */
            @Override
            public List<String> primaryKeyNames(TableId tableId) throws SQLException {
                DatabaseMetaData databaseMetadata = connection.connection().getMetaData();
                List<String> primaryKeyColumns = connection.readPrimaryKeyNames(databaseMetadata, tableId);
                if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
                    LOG.warn("Primary keys are not defined for table '{}', defaulting to unique indices", tableId.table());
                    return connection.readTableUniqueIndices(databaseMetadata, tableId);
                }
                return primaryKeyColumns;
            }
        };
    }
}
