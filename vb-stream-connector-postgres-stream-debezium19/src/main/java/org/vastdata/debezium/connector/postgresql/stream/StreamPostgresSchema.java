package org.vastdata.debezium.connector.postgresql.stream;

import org.vastdata.debezium.connector.postgresql.PostgresConnectorConfig;
import org.vastdata.debezium.connector.postgresql.PostgresSchema;
import org.vastdata.debezium.connector.postgresql.PostgresValueConverter;
import org.vastdata.debezium.connector.postgresql.TypeRegistry;
import org.vastdata.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * 本连接器的 schema 组件:{@link PostgresSchema} 的公开子类——父构造器是
 * <b>protected</b>(DBZ 1.9.7.Final 实测),包外无法实例化,本类的唯一职责是把该构造
 * 暴露给 {@code PostgresStreamConnectorTask} 的装配点(参数面照 vanilla
 * {@code PostgresConnectorTask} 的 {@code new PostgresSchema(connectorConfig,
 * typeRegistry, defaultValueConverter, topicSelector, valueConverter)}——1.9.7 无
 * builder,五个实参经 Task 装配点产出:topicSelector 由
 * {@code PostgresTopicSelector.create(config)} 建,typeRegistry/defaultValueConverter
 * 由 main 连接产出)。
 *
 * <p>继承的行为面(MS2 消费点,1.9.7 实测均在):{@link #applySchemaChangesForTable(int, Table)}
 * (DispatcherTransactionListener 在 TxChange 时按 asOf 版本安装)与
 * {@link #tableFor(int)}(oid → 已装版本,listener 的重装短路判据)。
 *
 * <p>线程约束:沿用父类单写者假设——单写者 = consumer 线程(版本安装只发生在
 * TxChange 回调),reader 线程不触碰本实例('R' 的表定义进
 * {@link VersionedRelationRegistry},不进 schema)。
 */
public class StreamPostgresSchema extends PostgresSchema {

    /**
     * 构造 schema 组件(委派父构造器,装配参数语义见 vanilla PostgresSchema)。
     *
     * @param connectorConfig       连接器配置(表过滤器/列过滤器/键映射等的真源)
     * @param typeRegistry          连库类型注册表(main 连接产出)
     * @param defaultValueConverter 默认值转换器(main 连接产出)
     * @param topicSelector         主题选择器(2.0 前体系,
     *                              {@code PostgresTopicSelector.create(config)} 产出)
     * @param valueConverter        PG 值转换器(charset + typeRegistry 产出)
     */
    public StreamPostgresSchema(PostgresConnectorConfig connectorConfig,
                                TypeRegistry typeRegistry,
                                PostgresDefaultValueConverter defaultValueConverter,
                                TopicSelector<TableId> topicSelector,
                                PostgresValueConverter valueConverter) {
        super(connectorConfig, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
    }
}
