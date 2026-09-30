package org.vastdata.debezium.connector.postgresql.stream;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.SchemaNameAdjuster;

/**
 * 流式连接器的 Connect 任务(1.9.7 真装配):泛型对齐 PG 连接器的分区/offset 体系
 * ({@link PostgresPartition} + {@link PostgresOffsetContext}),start(Configuration)
 * 按模板 <b>DBZ 1.9.7.Final {@code PostgresConnectorTask.start}</b> 的官方装配骨架改写
 * (替换点:StreamPostgresSchema / StreamEventMetadataProvider / lambda 形态的
 * partitionProvider / StreamChangeEventSourceFactory / StreamChangeEventSourceMetricsFactory),
 * doPoll 从 {@link ChangeEventQueue} 取已发射记录,doStop 关 main 连接/schema/queue。
 * 与 vanilla 1.9.7 的结构差异(记档,行为语义见各组件 javadoc):
 * <ul>
 *   <li><b>coordinator 用基类 8 参构造</b>而非 PG 专属子类——javap 实证
 *       {@code PostgresChangeEventSourceCoordinator} 的第 5 参是具体类
 *       {@code PostgresChangeEventSourceFactory}(非接口),包外工厂无法穿入;PG 子类
 *       的唯一增量行为 executeCatchUpStreaming(exported 快照的追赶流)对本连接器
 *       不可达(快照恒 skipped),且基类 1.9.7 的 executeChangeEventSources 直走
 *       doSnapshot→streamEvents、无 catch-up 调用面——与 3.6.1 版同构(也用基类)</li>
 *   <li><b>无复制槽装配段</b>:vanilla 的 createReplicationConnection/createReplicationSlot/
 *       槽状态读取/getReplicationSlotState 整段不复刻——建槽走自建
 *       {@code ReplicationSession.ensureSlot}(幂等建槽带 two_phase,R5 预检在内),
 *       槽创建不走官方 {@code createReplicationSlot()}</li>
 *   <li><b>无 snapshotter 装配面</b>:vanilla 的 getSnapshotter/init(OffsetState,SlotState)
 *       段删除——快照恒 skipped(snapshot.mode 仅 never 三层防线),基类 coordinator
 *       不收 snapshotter 参数,我们的 snapshot 源也不消费它</li>
 *   <li><b>taskContext 用裸 CdcSourceTaskContext</b>(1.9.7 三参构造 connectorType/
 *       connectorName/capturedCollections——vanilla 的 PostgresTaskContext 构造器
 *       protected 包外不可用;capturedCollections 取 {@code Collections::emptySet},
 *       vanilla 同款——本连接器表集合经流内 'R' 动态到达,启动期无静态集合可报)</li>
 *   <li><b>无 bean/notification/schema-factory/signal/snapshotter-service 面</b>:
 *       1.9.7 无 3.x 的 bean registry / NotificationService / SignalProcessor /
 *       SnapshotterService / QueueProviderService 体系,相应装配段整体不存在,
 *       队列经 Builder 直建(1.9.7 的 Builder 无 queueProvider)</li>
 *   <li><b>values.as.string 双轨切换保留</b>(schema 侧换
 *       {@code StringValueConverter},监督壳 execute 的值侧换
 *       {@code StringColumnValueMapper}——两处同开关切换)</li>
 *   <li><b>心跳错误处理按 1.9.7 官方面收敛</b>:57P01→DebeziumException、
 *       57P03→RetriableException(3.6.1 版另有的 42P01 分支是 3.x 增补,javap 实证
 *       1.9.7 vanilla 无此分支)</li>
 * </ul>
 * offset 提交回灌:1.9.7 的 BaseSourceTask.commit() 自带
 * "重读框架 offset → coordinator.commitOffset(lastOffset) → 流式源 commitOffset"整链
 * (coordinator 字段基类私有),任务侧无需(也无法)覆写 3.6.1 版的 performCommit。
 *
 * <p>线程约束:start/doStop 由 Connect runtime 串行调用(BaseSourceTask 的
 * stateLock);commit 在 Connect 提交线程(基类实现);运行期线程拓扑归
 * {@link PostgresStreamStreamingChangeEventSource} 的监督壳(reader/consumer)与
 * coordinator 的 change-event-source-coordinator 线程。
 */
public class PostgresStreamConnectorTask extends BaseSourceTask<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamConnectorTask.class);

    /** 日志上下文名(MDC 归因,queue 的 loggingContextSupplier 同源)。 */
    private static final String CONTEXT_NAME = "postgres-stream-connector-task";

    private volatile CdcSourceTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile StreamPostgresSchema schema;

    private Partition.Provider<PostgresPartition> partitionProvider = null;
    private OffsetContext.Loader<PostgresOffsetContext> offsetContextLoader = null;

    /**
     * 真装配(vanilla 1.9.7 PostgresConnectorTask.start 的同序替换版)。次序:config 解析
     * → topicSelector/schemaNameAdjuster → charset 临时连接(CONNECTION_GENERAL 裸连)→
     * main 连接(valueConverterBuilder + autoCommit false)→ typeRegistry/defaultValueConverter
     * → StreamPostgresSchema[5 参] → CdcSourceTaskContext[3 参] → partition/offset 装载
     * → queue(Builder,无 queueProvider)→ PostgresErrorHandler[2 参] →
     * StreamEventMetadataProvider → PostgresEventDispatcher[11 参,HeartbeatFactory 真实例
     * (构造体裸调 createHeartbeat 无 null 护卫)] → metricsBridge 单例两路分叉 →
     * ChangeEventSourceCoordinator[基类 8 参] → start → 返回。
     * 边界:任一步失败原样上抛(Connect 任务启动失败;1.9.7 vanilla 对 charset 失败不
     * 包 RetriableException,照官方形态直抛)。
     *
     * @param config 任务配置
     * @return 已启动的协调器(快照恒 skipped 后进入流式监督壳)
     */
    @Override
    protected ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> start(Configuration config) {
        final PostgresStreamConnectorConfig connectorConfig = new PostgresStreamConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();

        final Charset databaseCharset;
        try (PostgresConnection tempConnection = new PostgresConnection(connectorConfig.getJdbcConfig(),
                PostgresConnection.CONNECTION_GENERAL)) {
            databaseCharset = tempConnection.getDatabaseCharset();
        }

        final PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder = typeRegistry ->
                PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);
        // 全局 JDBC 连接:'R' 元数据 enrich 与初始 offset 上下文的来源,execute 装配后
        // reader 线程独占(R3);1.9.7 无连接工厂/bean registry 体系,直建直持
        jdbcConnection = new PostgresConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder,
                PostgresConnection.CONNECTION_GENERAL);
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final PostgresDefaultValueConverter defaultValueConverter = jdbcConnection.getDefaultValueConverter();
        // schema 侧转换器二选一(values.as.string):全串模式换 StringValueConverter——所有列
        // Connect schema 恒 STRING + converter 恒等,值侧配合监督壳 execute 的
        // StringColumnValueMapper 原文透传;连接的 valueConverterBuilder 保持 vanilla——
        // JDBC 连接只服务元数据 enrich('R')与初始 offset,快照恒 never 不经连接读值,无需改形
        final PostgresValueConverter valueConverter =
                connectorConfig.valuesAsString()
                        ? StringValueConverter.of(connectorConfig, databaseCharset, typeRegistry)
                        : valueConverterBuilder.build(typeRegistry);

        schema = new StreamPostgresSchema(connectorConfig, typeRegistry, defaultValueConverter,
                topicSelector, valueConverter);

        // 任务上下文(1.9.7 裸三参形态):connectorType 取本连接器 getContextName() 覆写值
        // (Module.CONTEXT_NAME,MDC/指标 ObjectName 域),connectorName 取逻辑名;
        // capturedCollections 静态空集(vanilla 同款——表集合经流内 'R' 动态到达)
        taskContext = new CdcSourceTaskContext(connectorConfig.getContextName(),
                connectorConfig.getLogicalName(), Collections::emptySet);

        // vanilla 的 new PostgresPartition.Provider(...) 是包私有类,包外不可构造——
        // 以等价 lambda 提供分区(上报差异);1.9.7 的 PostgresPartition 仅由逻辑名构成
        // (3.6.1 版另有 database 组件,本版构造器单参)
        this.partitionProvider = () -> Collections.singleton(
                new PostgresPartition(connectorConfig.getLogicalName()));
        this.offsetContextLoader = new PostgresOffsetContext.Loader(connectorConfig);
        final Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets =
                getPreviousOffsets(this.partitionProvider, this.offsetContextLoader);
        final Clock clock = Clock.system();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

        try {
            this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new PostgresErrorHandler(connectorConfig, queue);

            final StreamEventMetadataProvider metadataProvider = new StreamEventMetadataProvider();

            final PostgresEventDispatcher<TableId> dispatcher = new PostgresEventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    // EventDispatcher#ignoreMissingSchema 是实例方法(不可作未绑定方法引用,
                    // 与 InconsistentSchemaHandler 的 3 参形状不匹配)——vanilla 1.9.7 的
                    // updateSchema 是包私有 static,包外不可引——以等值 lambda 复刻其
                    // "缺 schema 即跳过"语义(3.6.1 版同款,上报差异)
                    (partition, dataCollectionId, changeRecordEmitter) -> Optional.empty(),
                    metadataProvider,
                    // HeartbeatFactory 必须传真例:dispatcher 构造体裸调 createHeartbeat
                    // 无 null 护卫;连接供应商与错误处理按 vanilla 1.9.7 形态
                    // (57P01 admin_shutdown→不可重试;57P03 cannot_connect_now→可重试;
                    // 其余 SQLSTATE 交给调用方默认面)
                    new HeartbeatFactory<>(connectorConfig, topicSelector, schemaNameAdjuster,
                            () -> new PostgresConnection(connectorConfig.getJdbcConfig(),
                                    PostgresConnection.CONNECTION_GENERAL),
                            exception -> {
                                String sqlErrorId = exception.getSQLState();
                                switch (sqlErrorId) {
                                    case "57P01":
                                        // Postgres error admin_shutdown
                                        throw new DebeziumException(
                                                "Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    case "57P03":
                                        // Postgres error cannot_connect_now
                                        throw new RetriableException(
                                                "Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    default:
                                        break;
                                }
                            }),
                    schemaNameAdjuster,
                    jdbcConnection);

            // 管线指标桥(MS5 Task 4):单一实例两路分叉——metrics 工厂(streaming 指标读)与
            // 源工厂(流式源 execute 填充读源)持有同一 bridge,MBean 面与运行管线单向衔接
            StreamMetricsBridge metricsBridge = new StreamMetricsBridge();

            ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator =
                    new ChangeEventSourceCoordinator<>(
                            previousOffsets,
                            errorHandler,
                            PostgresStreamConnector.class,
                            connectorConfig,
                            new StreamChangeEventSourceFactory(
                                    connectorConfig, dispatcher, errorHandler, clock, schema, jdbcConnection,
                                    typeRegistry, metricsBridge),
                            new StreamChangeEventSourceMetricsFactory<>(metricsBridge),
                            dispatcher,
                            schema);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    /**
     * 从变更事件队列取一批已发射记录(vanilla 1.9.7 同款:queue.poll 逐条取
     * {@link DataChangeEvent#getRecord()};1.9.7 无 3.x 的 pollRecords 基类助手)。
     *
     * @return 本批记录(可能为空)
     */
    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        return queue.poll().stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    /**
     * 停机收敛(vanilla 1.9.7 doStop 的裁剪版,复制流/组装器的收敛归协调器停机 → 流式源
     * stopStreaming 的次序,不在此重复):main 连接 → schema(1.9.7 无 bean registry 无
     * bean 专属连接可关;ChangeEventQueue 无 close 面,vanilla 同样不关)。
     * 边界:字段 null(启动中途失败)安全跳过——BaseSourceTask.stop() 对任何状态都会
     * 调本方法,零装配状态必须无异常。
     */
    @Override
    protected void doStop() {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (schema != null) {
            schema.close();
        }
    }

    /**
     * 返回任务版本号(Connect runtime 元数据;kafka 3.1.0 的 Task.version() 是抽象方法)。
     *
     * @return {@link Module#version()},永不抛错
     */
    @Override
    public String version() {
        return Module.version();
    }

    /**
     * 返回任务可用的全部配置字段(基类用于配置完整性校验)。
     *
     * @return {@link PostgresStreamConnectorConfig#ALL_FIELDS}(含 7 个本模块配置项)
     */
    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresStreamConnectorConfig.ALL_FIELDS;
    }
}
