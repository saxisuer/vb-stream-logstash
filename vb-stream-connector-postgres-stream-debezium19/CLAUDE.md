# vb-stream-connector-postgres-stream-debezium19 — CLAUDE.md

## 模块定位（1.9.7 宿主嵌入版）

`vb-stream-connector-postgres-stream`（Debezium 3.6.1 版）面向**内置 Debezium 1.9.7.Final 的宿主产品**的同款连接器：**整模块复制 + 接缝层按 1.9.7 API 改写**（用户裁定方案 A，2026-09-30）。四条裁定（spec §1）：D1 嵌入内置 1.9.7 的宿主（API 以宿主全家桶为准，connect-api 走 Kafka 3.1 线）；D2 功能全量对齐 3.6.1 模块的 MS1-MS6（仅 1.9.7 无对应物处替代或裁剪）；D3 宿主 JVM Java 17+（`release 17` 与 record/text block 形态 1:1 保留）；D4 **同包名** `org.vastdata.debezium.connector.postgresql.stream`、版本后缀模块名、**两模块永不同 classpath**。

设计 spec：`docs/superpowers/specs/2026-09-30-debezium19-connector-design.md`；逐任务实施报告：`.superpowers/sdd/2026-09-30-debezium19-connector/task-{1..12}-report.md`。

## 源码三桶（60 主源码文件 + 测试 61 文件）

分桶依据 = 逐文件对 3.6.1 模块的 Debezium import 统计与逐文件字节比对（`cmp`，2026-09-30 实测）。

### 桶 A：字节等同复制（44 文件）

- `protocol/` 子包全部 14 文件（`PgOutputStreamDecoder`/`PgOutputMessage`/`WireReader`/四个 parser 族/值类型/异常类——零 Debezium import，见 `protocol/CLAUDE.md`）
- 包根 28 文件：`StreamedTransactionAssembler`、`MessagePipe`、`ReplicationSession`、`StreamThroughputMetrics`、`StreamMetricsBridge`、`BucketReplayer`、`TransactionConsumer`、`RelationSnapshot`、`VersionedRelationRegistry`、`TxChange`/`TxBuffer`/`RowChange`/`MsgChange`/`TruncateChange`、事务事件模型、`RawMessageListener`/`RawPeeks`/`MessagePreview`、关系解析四件、`ColumnValueMapper`、`DmlKind`、`Module`、`ResolvedRelation`（带 1 个稳定 Debezium import `TableId`）等
- **核对放行 2 件**（spec 原划桶 B，经 1.9.7 依赖面逐项 javap 核对后**字节等同、零改动放行**，实归桶 A）：`RelationTableFactory`（DebeziumException/relational 四件/`Strings.unquoteIdentifierPart` 逐项同形）、`StringColumnValueMapper`（`new String(getUnavailableValuePlaceholder())` 恰命中 1.9.7 `byte[]` 返回型的 `String(byte[])` 重载，与 vanilla 1.9.7 父类同解码路径）
- 3.6.1 模块的 `package-info.java`（包级 javadoc）未随迁（移植清单边界，非 API 载体）

### 桶 B：按 1.9.7 API 改写（16 文件，接缝层）

| 文件 | 改写要点（对齐基准 = 1.9.7 官方 `PostgresConnectorTask`/`PostgresStreamingChangeEventSource` 源码） |
|---|---|
| `PostgresStreamConnectorTask` | 装配骨架全换 1.9.7 官方形态：无 bean registry/notification/schema-factory/signal/snapshotter-service/连接工厂体系；coordinator 用**基类 8 参构造**（PG 专属子类第 5 参是具体 vanilla 工厂类不可穿入，catch-up 行为对快照恒 skipped 不可达）；doPoll 走 `queue.poll()`+`getRecord()`；doStop 关连接+schema（ChangeEventQueue 无 close） |
| `PostgresStreamConnectorConfig` | `SNAPSHOT_MODE_NEVER` 同名替换（仅 never）；1.9.7 无公开 `getSnapshotMode()`——补 `snapshotMode()` 同语义重写；自建七配置面逐字零改动 |
| `PostgresStreamConnector` | 1.9.7 基类无 `getConfigFields()` 钩子（去 @Override）；validate 离线门槛 = 连接相关五字段零问题 |
| `StreamPostgresSchema` | `PostgresSchema` 5 参构造 + `TopicSelector<TableId>`（`PostgresTopicSelector.create`，2.0 前体系） |
| `PostgresStreamStreamingChangeEventSource` | 接口面 1.9.7 形态（`execute` 补 throws/心跳 `dispatchHeartbeatEvent`/`commitOffset` 单参/空接口无 close）——监督壳行为（D7 shutdownFast 不排干）零改动 |
| `StreamChangeEventSourceFactory` | 工厂接口 1.9.7 签名（streaming 零参/snapshot 单参 listener）；显式覆写 incremental 返回 empty（R2 裁定钉在装配点） |
| `DispatcherTransactionListener` | `PostgresEventDispatcher` 11 参巨构造；`dispatchTransactionStarted/CommittedEvent` 去时间戳参；`updateWalPosition` 去 Operation 位；TRUNCATE 门控双键机制 |
| `RowChangeEmitter` / `TruncateEmitter` | 基类 `RelationalChangeRecordEmitter` 三参构造（构造签名保持含 connectorConfig 的装配面兼容） |
| `StreamEventMetadataProvider` | `DataCollectionId` 包名 `io.debezium.schema`（未拆 spi）；两级 ts 回落照旧（`ts_ms` 常量在父类 `AbstractSourceInfo`） |
| `StreamStreamingChangeEventSourceMetrics(MXBean)` | 1.9.7 `StreamingChangeEventSourceMetricsMXBean` traits 聚合方法集；`DefaultStreaming` 3 参构造（无 CapturedTablesSupplier） |
| `StreamChangeEventSourceMetricsFactory` | `getStreamingMetrics` 四参→三参 |
| `RelationMetadataSource` | `getTableColumnsForDecoder` 1.9.7 不存在——vanilla `getTableColumnsFromDatabase` 同构循环改写 |
| `TypeRegistryColumnValueMapper` | TOAST 哨兵构造 5 参追加 `hasMetadata=false` |
| `StringValueConverter` | 构造参 11 `byte[]`（1.9.7 无 `UnchangedToastedPlaceholder` 类）；`of()` 实参改 `getUnavailableValuePlaceholder()` |

### 桶 C：配置资源

`META-INF/services/`（SourceConnector SPI 清单）、`src/main/assembly/plugin.xml`（与 3.6.1 版唯一差异 = include 的本模块坐标；R4 排除边界逐字同款）、`logback-test.xml`。

### 双线同步约定（spec §7）

**桶 A 内核（protocol/组装/管道/会话/指标）的任何后续改动须双模块同步落地**（文件字节等同，同步 = 复制零改动）；**接缝层（桶 B）豁免**——本就因 Debezium 版本而异，各随各的 API 面。改 3.6.1 模块内核时同步本模块，反之亦然。

## 1.9.7 API 差异清单（实证，javap/sources 求证记录）

**1.9.7 不存在**（3.6.1 依赖面，接缝改写的根源）：`io.debezium.bean.StandardBeanNames`、`pipeline.notification.NotificationService`、`pipeline.source.SnapshottingTask`、`snapshot.SnapshotterService`、`schema.SchemaFactory`、`pipeline.signal.actions.snapshotting.SnapshotConfiguration`、`jdbc.DefaultMainConnectionProvidingConnectionFactory`/`MainConnectionProvidingConnectionFactory`、`connector.base.QueueProviderService`、`connector.common.DebeziumHeaderProducer`、`io.debezium.spi.schema.DataCollectionId`（在 `io.debezium.schema`，spi 拆分是 2.x 后）、`io.debezium.spi.topic.TopicNamingStrategy`（主题体系是 `TopicSelector<TableId>`，2.0 才切 TopicNamingStrategy）、**`io.debezium.engine.format.Connect`（2.0 才引入——Task 8 勘误：spec §2 事实 3 的"`DebeziumEngine.create(Connect.class)` 可用"不成立，embedded 装配走 `EmbeddedEngine.create()`，记录类型裸 `SourceRecord` 无 `RecordChangeEvent` 包装）**、`ChangeEventQueue` 的 `close()`/Builder 的 `queueProvider`、`BaseSourceTask` 的 `preStart`/`pollRecords`/`performCommit` 钩子。

**1.9.7 已有**（勿当缺失处理）：`heartbeat.HeartbeatFactory`（官方 Task 装配即用；dispatcher 构造体裸调 `createHeartbeat` **无 null 护卫**——必须传真例）、`ChangeEventQueue.Builder`（含 maxQueueSizeInBytes）、`PgConnectionSupplier`（`PostgresStreamingChangeEventSource` 内 static interface）、`SignalProcessor`（基础形态）。

**构造器/体系形态差异**：`PostgresEventDispatcher<TableId>` 11 参巨构造（含 `PostgresChangeRecordEmitter::updateSchema`——**包私有 static 包外不可引**，用等值 lambda——与 HeartbeatFactory）；`PostgresSchema` 5 参构造；`PostgresChangeEventSourceCoordinator` 10 参且第 5 参是具体 vanilla 工厂类（本模块用基类 8 参）；`PostgresValueConverter.of(config, charset, typeRegistry)` 静态工厂（构造参 11 是 `byte[]`）；`PgOutputReplicationMessage` 1.9.7 已是 `(Operation, String, Instant, Long, List<Column> old, List<Column> new)` 构造；`ChangeEventSourceFactory<P,O>` 的 `getStreamingChangeEventSource()` **零参**、`getSnapshotChangeEventSource(listener)` **单参**；metrics 内部为 `ConnectionMeter`/`StreamingMeter` traits 聚合（构造 3 参，`getCapturedTables()` 走 taskContext 动态读源）；`CdcSourceTaskContext` 非泛型 3 参构造（`PostgresTaskContext` 构造器 protected 包外不可用）；`PostgresPartition` 构造器单参（逻辑名）。

**行为面差异**（IT 断言适配的根因，均非本连接器缺陷）：TRUNCATE 门控双键 + `plugin.name` 第三键（见 README 配置面表）；TOAST 占位键 `toasted.value.placeholder` 先读带默认；启动校验拒绝经 `validateAndRecord` **记 ERROR 日志**后抛**无 cause** 笼统异常；`Loader.load` 对 `ts_usec` 裸拆箱；`EmbeddedEngine.stop()` 默认 5 分钟 interrupt 等待（系统属性压短）；1.9.7 心跳错误处理仅 57P01/57P03 两分支（42P01 是 3.x 增补）。

## 自建 IT 基座 AbstractStreamIT

1.9.7 官方 `AbstractConnectorTest` 是 **JUnit 4 编译**（`org.junit.Before/After`），JUnit 6 Jupiter 无法继承（vintage 线不可用）；本仓 27 IT 对基座调用面仅 4 方法 + CompletionCallback。自建 Jupiter 基座（`it/AbstractStreamIT.java`）：

- 内装 1.9.7 **同步** `EmbeddedEngine.create()` 引擎 + `ChangeConsumer` 入队基座无界记录队列 + 生命周期闩/hook；start 自动注入引擎五件套（name/connector.class/offset.storage+文件（按测试类名分立）/offset.flush.interval.ms=100 压默认 60s）
- 对外方法面与 3.6.1 基座同名对齐：`start(Class, Configuration)` / `stopConnector()` / `consumeRecords(int[, Consumer])`（超时 partial-return 语义同 3.6.1 字节码实证 + engineFailure 快速失败通道）/ `consumeRecordsByTopic(int)` / `initializeConnectorTestFramework` / `drainArrivedRecords`
- **停机/失败语义三分**（Task 8 审查修复）：`stopConnector()` 对从未 start 的用例纯 no-op（否则闩永不开挂 60s）；`awaitEngine()` 纯等待（60s 卡死探测与失败信号解耦）；`expectEngineFailure()` 预声明豁免连带抛出 + `engineFailure()` 暴露原始异常（预期失败 IT 的干净收敛通道，`SlotTwoPhaseMismatchIT`/`DefaultsAndMetricsIT` 消费）；`AbstractStreamITContractTest` 两契约用例零 Docker 钉行为
- **基座接缝的等价翻译**（断言面零损失的改写，各 IT javadoc 记档）：`ReaderUnblockedIT`/`FrontierCapIT`/`LogicalMsgIT` 的阻塞消费者经**类内自建阻塞引擎**装（基座 start 无消费者注入口——1.9.7 同步引擎 run() 在引擎线程串行 poll→handleBatch，消费者阻塞即输出路径停摆）；`ReaderThroughputIT` 自持 35s 窗口（基座截止写死 60s）；重启场景两次引擎 start 各用**管道独立子目录** engine-a/engine-b（Windows 下 Chronicle mmap 句柄异步释放撞同目录 wipe-on-open，POSIX 等价——D7 弃桶观察面不变）

## 测试矩阵与容器版本

`mvn test` 单命令 275 用例（surefire 显式含 `**/*IT.java`）：

| 类别 | 数量 | 说明 |
|---|---|---|
| 离线单测（36 类） | 245 | 桶 A 复制件多数**字节等同直过**（含 `RelationTableFactoryTest`/`TypeRegistryColumnValueMapperTest` 整类零改）；接缝类测试按 1.9.7 构造面翻译（配置基础面统一补 `database.server.name` + `snapshot.mode=never`） |
| 语义 IT（14 类） | 27 | `SmokeIT` 冒烟 1 + Task 9 七组 12（流式大事务/回滚过滤/DDL asOf/重启三情况/解耦三件）+ Task 10 六组 14（Truncate/全串/逻辑消息/two_phase 四场景/槽预检/缺省指标）——断言面与 3.6.1 版逐字同锚 |
| 基座契约 | 2 | `AbstractStreamITContractTest`（零 Docker） |
| Connect 验收 | 1 | `ConnectPluginIT`（真 Kafka Connect 容器，@BeforeAll 产物结构断言前置，缺产物 fail-fast 提示先 `mvn -pl vb-stream-connector-postgres-stream-debezium19 package -DskipTests`） |

**容器版本**：`StreamPgTestEnv` **postgres:18** 单例（与 3.6.1 模块逐字同配方）；`ConnectPluginIT` 用 **confluentinc/cp-kafka:7.1.0 + cp-kafka-connect:7.1.0**（= Kafka 3.1.0，Debezium 1.9.7 官方矩阵）——broker 为自建 `ZkKafkaContainer`（testcontainers 2.x kafka 模块恒以 KRaft 起 Confluent 容器，7.1 拒绝 KRaft 参数面），connect 为 7.1.0 基座叠 **eclipse-temurin:17-jre 覆盖层**（镜像原 JVM Zulu 11 与本模块 release 17 字节码硬冲突；Confluent 启动链与 Connect 3.1.0 runtime 原样保留）。

**测试环境坑位**（pom 钉版与绕道，均为 test 面不动运行期类路径）：
- testcontainers 2.0.5 的 shaded jackson-databind 引用未 shade 的 jackson-annotation 2.14+ 常量，而 connect-runtime 3.1.0 近路钉 2.12.3——pom test scope 显式钉 `jackson-annotations:2.20`（对齐引擎模块实测组合）
- test 类路径 commons-lang3 被 1.9.7 传递钉 3.8.1，testcontainers 2.x 的 commons-compress 1.28 tar 写路径需 3.14+ 的 `ArrayFill`——`ConnectPluginIT` 三处绕道：手写单文件 ustar tar 构建覆盖层镜像、插件目录 `withFileSystemBind`（不走 tar）、broker 启动脚本整体内联 CMD（Task 11 §2.3；若日后 pom 钉 commons-lang3 ≥3.14 可回收为 3.6.1 版原形态）
- `@TempDir` 一律 `cleanup = CleanupMode.NEVER`（Windows mmap 句柄；同 3.6.1 模块规约）；`SmokeIT` 的 NEVER 目录跨次累积（%TEMP% 交 OS 清理）

## 其余

- 模块 pom：1.9.7 专属版本属性全部留模块内（根 pom 零双 debezium 版本属性）；pgjdbc 显式 pin 根属性 42.7.13（压 1.9.7 传递的 42.3.x）；slf4j-api 2.0.17 provided 压制策略与 3.6.1 模块同款；测试域 `debezium-embedded` **无 tests classifier**（自建基座不需官方测试类）
- 开发规约（方法名英文 camelCase/import 简名/slf4j/全函数 javadoc）与根 CLAUDE.md 同规；本模块所有新写文件已按规约落地
- 用户面文档（定位/配置面/打包安装/at-least-once 语义/已知限制/与 3.6.1 模块差异表）见模块 `README.md`——两处文档的差异表条目须同步维护
