package org.vastdata.debezium.connector.postgresql.stream;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProviderService;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * 流式连接器的 Connect 任务(MS2 真装配):泛型对齐 PG 连接器的分区/offset 体系
 * ({@link PostgresPartition} + {@link PostgresOffsetContext}),start(Configuration)
 * 按模板 <b>DBZ 3.6.1.Final {@code PostgresConnectorTask}:101-284</b> 装配全链路
 * (替换点:StreamPostgresSchema / StreamEventMetadataProvider / lambda 形态的
 * partitionProvider / StreamChangeEventSourceFactory),doPoll 从
 * {@link ChangeEventQueue} 取已发射记录,doStop 关 main 连接/schema/queue;
 * commit 面经 performCommit → coordinator.commitOffset → 流式源的单调水位。
 * 与 vanilla 的结构差异(记档,行为语义见各组件 javadoc):无复制槽预检/守门
 * (wal_level 校验、slot 状态读取、publication 自动建——Task 8 IT 起按需补)、
 * 无 schema history 校验(PG 非 historized)、快照恒 skipped(MS5 换真)。
 *
 * <p>线程约束:start/doStop/commit 由 Connect runtime 串行调用(BaseSourceTask 的
 * start/stop/commitRecord 互斥锁);运行期线程拓扑归
 * {@link PostgresStreamStreamingChangeEventSource} 的监督壳(reader/consumer)与
 * coordinator 的 change-event-source-coordinator 线程。
 */
public class PostgresStreamConnectorTask extends BaseSourceTask<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamConnectorTask.class);

    /** 日志上下文名(MDC 归因,queue 的 loggingContextSupplier 同源)。 */
    private static final String CONTEXT_NAME = "postgres-stream-connector-task";

    private volatile CdcSourceTaskContext<PostgresConnectorConfig> taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile StreamPostgresSchema schema;

    private Partition.Provider<PostgresPartition> partitionProvider = null;
    private OffsetContext.Loader<PostgresOffsetContext> offsetContextLoader = null;

    private final ReentrantLock commitLock = new ReentrantLock();

    /**
     * 构造任务上下文:原始配置 + 本连接器 config 包装 + 空自定义指标标签。
     * 返回值不可为 null——基类 start(Map) 立即解引用;类型参数取父类
     * {@code PostgresConnectorConfig}({@link StreamPostgresSchema} 的 protected 构造
     * 链要求,实例配置仍是 {@link PostgresStreamConnectorConfig},start 另建局部实例)。
     *
     * @param config 任务的完整配置(应为已通过 ALL_FIELDS 校验的原始配置)
     * @return 携带本连接器 config 的非 null 上下文
     */
    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
        PostgresConnectorConfig connectorConfig = new PostgresStreamConnectorConfig(config);
        taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        return taskContext;
    }

    /**
     * 真装配(vanilla :101-284 的同序替换版)。次序:config 解析(charset 临时连接 →
     * 共享 TypeRegistry[1 参] → 连接工厂/main 连接[autoCommit false])→ 服务注册 →
     * StreamPostgresSchema → partition/offset 装载 → queue(Builder)→ PostgresErrorHandler
     * → StreamEventMetadataProvider → SignalProcessor → PostgresEventDispatcher[12 参] →
     * NotificationService → ChangeEventSourceCoordinator[基类 11 参] → start → 返回。
     * 边界:任一步失败原样上抛(Connect 任务启动失败;临时连接取 charset 失败转
     * RetriableException——vanilla 同款可重试语义)。
     *
     * @param config 任务配置
     * @return 已启动的协调器(快照恒 skipped 后进入流式监督壳)
     */
    @Override
    protected ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> start(Configuration config) {
        final PostgresStreamConnectorConfig connectorConfig = new PostgresStreamConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(
                CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        final Charset databaseCharset;
        try (PostgresConnection tempConnection = new PostgresConnection(connectorConfig.getJdbcConfig(),
                PostgresConnection.CONNECTION_GENERAL)) {
            databaseCharset = tempConnection.getDatabaseCharset();
        }
        catch (DebeziumException e) {
            throw new RetriableException("Couldn't obtain encoding for database", e);
        }

        final TypeRegistry sharedTypeRegistry = PostgresConnection.createTypeRegistry(connectorConfig.getJdbcConfig());

        final PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder = typeRegistry ->
                io.debezium.connector.postgresql.PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);

        MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory =
                new DefaultMainConnectionProvidingConnectionFactory<>(
                        () -> new PostgresConnection(connectorConfig.getJdbcConfig(), sharedTypeRegistry,
                                valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL));
        // 全局 JDBC 连接:'R' 元数据 enrich 与初始 offset 上下文的来源,execute 装配后
        // reader 线程独占(R3)
        jdbcConnection = connectionFactory.mainConnection();
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final PostgresDefaultValueConverter defaultValueConverter = jdbcConnection.getDefaultValueConverter();
        final io.debezium.connector.postgresql.PostgresValueConverter valueConverter =
                valueConverterBuilder.build(typeRegistry);

        // 服务提供方注册(vanilla 同款集合;QueueProviderService 是 queue 装配的前置)
        registerServiceProviders(connectorConfig.getServiceRegistry());

        CustomConverterRegistry customConverterRegistry = connectorConfig.getServiceRegistry()
                .tryGetService(CustomConverterRegistry.class);

        schema = new StreamPostgresSchema(taskContext, defaultValueConverter, topicNamingStrategy,
                valueConverter, customConverterRegistry);
        // vanilla :144 的 new PostgresPartition.Provider(...) 是包私有类,包外不可构造——
        // 以等价 lambda 提供分区(上报差异);分区键 = 逻辑名 + database 配置项
        this.partitionProvider = () -> Collections.singleton(new PostgresPartition(
                connectorConfig.getLogicalName(), config.getString(RelationalDatabaseConnectorConfig.DATABASE_NAME.name())));
        this.offsetContextLoader = new PostgresOffsetContext.Loader(connectorConfig);
        final Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets =
                getPreviousOffsets(this.partitionProvider, this.offsetContextLoader);
        final io.debezium.util.Clock clock = io.debezium.util.Clock.system();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry()
                .tryGetService(SnapshotterService.class);

        io.debezium.util.LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
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
                    .queueProvider(connectorConfig.getServiceRegistry()
                            .tryGetService(QueueProviderService.class).getQueueProvider())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new PostgresErrorHandler(connectorConfig, queue, null);

            final StreamEventMetadataProvider metadataProvider = new StreamEventMetadataProvider();

            SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor = new SignalProcessor<>(
                    PostgresStreamConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffsets);

            final PostgresEventDispatcher<TableId> dispatcher = new PostgresEventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    // EventDispatcher#ignoreMissingSchema 是实例方法(不可作未绑定方法引用,
                    // 与 InconsistentSchemaHandler 的 3 参形状不匹配)——以等值 lambda 复刻其
                    // "缺 schema 即跳过"语义(vanilla :230 用包私有的 updateSchema,包外不可引,
                    // 差异上报)
                    (partition, dataCollectionId, changeRecordEmitter) -> Optional.empty(),
                    metadataProvider,
                    new HeartbeatFactory<>().getScheduledHeartbeat(
                            connectorConfig,
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
                                    case "42P01":
                                        // Postgres error undefined_table
                                        throw new DebeziumException(
                                                "Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    default:
                                        break;
                                }
                            }, queue),
                    schemaNameAdjuster,
                    signalProcessor,
                    connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService = new NotificationService<>(
                    getNotificationChannels(), connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator =
                    new ChangeEventSourceCoordinator<>(
                            previousOffsets,
                            errorHandler,
                            PostgresStreamConnector.class,
                            connectorConfig,
                            new StreamChangeEventSourceFactory(
                                    connectorConfig, dispatcher, errorHandler, clock, schema, jdbcConnection, typeRegistry),
                            new DefaultChangeEventSourceMetricsFactory<>(),
                            dispatcher,
                            schema,
                            signalProcessor,
                            notificationService,
                            snapshotterService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    /**
     * 从变更事件队列取一批已发射记录(基类 pollRecords 负责 record 化与批量语义)。
     *
     * @return 本批记录(可能为空)
     */
    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        return pollRecords(queue);
    }

    /**
     * 流式源的错误处理器(queue.poll 感知失败后停任务的通道);start 未完成时为 empty。
     *
     * @return errorHandler 的 Optional 包装
     */
    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    /**
     * 停机收敛(vanilla doStop 的裁剪版,复制流/组装器的收敛归协调器停机 → 流式源
     * stopStreaming 的次序,不在此重复):main 连接 → schema → queue。
     * 边界:字段 null(启动中途失败)安全跳过。
     */
    @Override
    protected void doStop() {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (schema != null) {
            schema.close();
        }
        if (queue != null) {
            queue.close();
        }
    }

    /**
     * 返回任务版本号(Connect runtime 元数据)。
     *
     * @return {@link Module#version()},永不抛错
     */
    @Override
    public String version() {
        return Module.version();
    }

    /**
     * 返回连接器逻辑名(日志/MDC 上下文归因)。
     *
     * @return 常量 {@link Module#NAME}
     */
    @Override
    public String connectorName() {
        return Module.NAME;
    }

    /**
     * 返回任务可用的全部配置字段(基类用于配置完整性校验)。
     *
     * @return {@link PostgresStreamConnectorConfig#ALL_FIELDS}(含 5 个本模块配置项)
     */
    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresStreamConnectorConfig.ALL_FIELDS;
    }

    /**
     * 请求提交位(vanilla 同款):置 shouldPerformCommit,基类 poll 循环择机调
     * {@link #performCommit()}。
     */
    @Override
    public void commit() throws InterruptedException {
        shouldPerformCommit.set(true);
    }

    /**
     * 执行 offset 提交回灌(vanilla :464-492 同款):重读框架存量 offset,逐分区经
     * coordinator.commitOffset 交给流式源(单调水位记录;服务端确认由输出前沿封顶
     * 承担,见流式源 commitOffset 的口径注记)。边界:与停机并发(tryLock 失败)时
     * WARN 放弃本次提交。
     */
    @Override
    public void performCommit() {
        boolean locked = commitLock.tryLock();

        if (locked) {
            try {
                if (coordinator != null) {
                    Offsets<PostgresPartition, PostgresOffsetContext> offsets =
                            this.getPreviousOffsets(this.partitionProvider, this.offsetContextLoader);
                    if (offsets.getOffsets() != null) {
                        offsets.getOffsets()
                                .entrySet()
                                .stream()
                                .filter(e -> e.getValue() != null)
                                .forEach(entry -> {
                                    Map<String, String> partition = entry.getKey().getSourcePartition();
                                    Map<String, ?> lastOffset = entry.getValue().getOffset();
                                    coordinator.commitOffset(partition, lastOffset);
                                });
                    }
                }
            }
            finally {
                commitLock.unlock();
            }
        }
        else {
            LOGGER.warn("Couldn't commit processed log positions with the source database due to a concurrent connector shutdown or restart");
        }
    }
}
