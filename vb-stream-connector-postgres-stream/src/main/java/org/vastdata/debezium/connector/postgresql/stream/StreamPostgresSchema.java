package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.relational.TableId;

/**
 * 本连接器的 schema 组件:{@link PostgresSchema} 的公开子类——父构造器是
 * <b>protected</b>(DBZ 3.6.1.Final 实测),包外无法实例化,本类的唯一职责是把该构造
 * 暴露给 {@code PostgresStreamConnectorTask} 的装配点(参数面照 vanilla
 * {@code PostgresConnectorTask}:143 的 {@code new PostgresSchema(taskContext,
 * defaultValueConverter, topicNamingStrategy, valueConverter, customConverterRegistry)})。
 *
 * <p>继承的行为面(MS2 消费点):{@link #applySchemaChangesForTable(int, Table)}
 * (Task 7 的 DispatcherTransactionListener 在 TxChange 时按 asOf 版本安装——
 * 内部走 {@code refresh(table)} 重建 TableSchema,DDL 稀疏故重建开销可接受)与
 * {@link #tableFor(int)}(oid → 已装版本,listener 的重装短路判据)。
 * 泛型注意:父构造器要 {@code CdcSourceTaskContext<PostgresConnectorConfig>}——
 * 任务侧的 taskContext 以父类型参数构造(实例配置仍是
 * {@link PostgresStreamConnectorConfig})。
 *
 * <p>线程约束:沿用父类 {@code @NotThreadSafe}——单写者 = consumer 线程
 * (版本安装只发生在 TxChange 回调;R1 账本),reader 线程不触碰本实例
 * ('R' 的表定义进 {@link VersionedRelationRegistry},不进 schema)。
 */
public class StreamPostgresSchema extends PostgresSchema {

    /**
     * 构造 schema 组件(委派父构造器,装配参数语义见 vanilla PostgresSchema)。
     *
     * @param taskContext           任务上下文(构造链只读其 config——表过滤器/列过滤器/键映射等)
     * @param defaultValueConverter 默认值转换器(main 连接产出)
     * @param topicNamingStrategy   主题命名策略
     * @param valueConverter        PG 值转换器(charset + typeRegistry 产出)
     * @param customConverterRegistry 自定义转换器注册表(服务注册表产出)
     */
    public StreamPostgresSchema(CdcSourceTaskContext<PostgresConnectorConfig> taskContext,
                                PostgresDefaultValueConverter defaultValueConverter,
                                TopicNamingStrategy<TableId> topicNamingStrategy,
                                PostgresValueConverter valueConverter,
                                CustomConverterRegistry customConverterRegistry) {
        super(taskContext, defaultValueConverter, topicNamingStrategy, valueConverter, customConverterRegistry);
    }
}
