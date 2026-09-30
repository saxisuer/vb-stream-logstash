/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.vastdata.debezium.connector.postgresql;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Postgres 连接器的 change event 发射器：在 {@link EventDispatcher} 基础上钉死
 * {@link PostgresPartition} 分区类型与 PG 的心跳工厂形态，数据变更/事务边界/心跳经基类
 * {@code dispatchDataChangeEvent}/{@code dispatchTransactionStartedEvent} 等发射。
 * 复刻自 io.debezium.connector.postgresql.PostgresEventDispatcher（debezium-connector-postgres
 * 1.9.7.Final sources，2026-09-30 裁剪复刻）。裁剪说明：删除逻辑解码消息（logical decoding message）
 * 发射面——{@code dispatchLogicalDecodingMessage(...)}/{@code enqueueLogicalDecodingMessage(...)} 两方法、
 * 字段 {@code logicalDecodingMessageMonitor}/{@code messageFilter} 与构造体内对应初始化，及相关 import
 * （{@code LogicalDecodingMessage}/{@code LogicalDecodingMessageMonitor}/{@code LogicalDecodingMessageFilter}
 * 均为排除集/根包 vanilla 类型）；依据 MS3.5 裁定逻辑消息解析不发射、dispatcher 该路径运行期不可达
 * （vanilla 81 行小文件，裁后约半）。三个构造器签名逐字保留（11 参巨构造是
 * {@code .stream/DispatcherTransactionListener} 的装配面）。
 *
 * @author Lairen Hightower
 */
public class PostgresEventDispatcher<T extends DataCollectionId> extends EventDispatcher<PostgresPartition, T> {

    public PostgresEventDispatcher(PostgresConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                                   DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<T> filter,
                                   ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider, SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, null, metadataProvider,
                new HeartbeatFactory<>(connectorConfig, topicSelector, schemaNameAdjuster), schemaNameAdjuster, null);
    }

    public PostgresEventDispatcher(PostgresConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                                   DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<T> filter,
                                   ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider,
                                   HeartbeatFactory<T> heartbeatFactory, SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, null, metadataProvider,
                heartbeatFactory, schemaNameAdjuster, null);
    }

    public PostgresEventDispatcher(PostgresConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                                   DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<T> filter,
                                   ChangeEventCreator changeEventCreator, InconsistentSchemaHandler<PostgresPartition, T> inconsistentSchemaHandler,
                                   EventMetadataProvider metadataProvider, HeartbeatFactory<T> heartbeatFactory, SchemaNameAdjuster schemaNameAdjuster,
                                   JdbcConnection jdbcConnection) {
        super(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, inconsistentSchemaHandler, metadataProvider,
                heartbeatFactory, schemaNameAdjuster);
    }
}
