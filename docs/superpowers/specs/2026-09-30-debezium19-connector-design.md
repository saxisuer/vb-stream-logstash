# vb-stream-connector-postgres-stream-debezium19 设计

日期：2026-09-30
状态：已确认（用户裁定四项 + 设计五节全确认）
关联：`docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md`（3.6.1 版模块原始设计）、MS1-MS6 各审计文档

## 1. 背景与目标

现有 `vb-stream-connector-postgres-stream` 基于 Debezium 3.6.1.Final。目标环境是**内置 Debezium 1.9.7.Final 的宿主产品**，需要功能全量对齐（MS1-MS6：流式解码/组装/回放/事务元数据/two_phase/MBean 指标/assembly 打包/27 IT）的同款连接器，运行其上。

约束（用户裁定，2026-09-30）：

| # | 裁定 | 影响 |
|---|---|---|
| D1 | 嵌入内置 1.9.7 的宿主产品 | API 以宿主全家桶为准；connect-api 跟随 1.9.7 的 Kafka 3.1 线而非 4.3 |
| D2 | 功能全量对齐 MS1-MS6 | two_phase/metrics/打包/27 IT 全保留；仅 1.9.7 无对应物处替代或裁剪（见 §5） |
| D3 | 宿主 JVM 为 Java 17+ | 编译目标保持 `release 17`，record/text block 等现有形态 1:1 保留 |
| D4 | 版本后缀模块名 + 同包名 | 模块 `vb-stream-connector-postgres-stream-debezium19`，包名保持 `org.vastdata.debezium.connector.postgresql.stream` 不变；两模块永不同 classpath（各嵌各的宿主/插件包） |

移植策略（用户选定方案 A）：**整模块复制 + 接缝层改写**——现有 3.6.1 模块一行不动；新模块独立演进；桶 A 内核（低频改动区）双线同步，接缝层本就因版本而异。

## 2. 已验证的 1.9.7 API 事实（设计依据）

以下均核对自 GitHub tag `v1.9.7.Final` 源码：

1. `PostgresConnectorConfig.SnapshotMode` 枚举为 `ALWAYS / INITIAL / NEVER / INITIAL_ONLY / EXPORTED(deprecated) / CUSTOM`——**无 `NO_DATA`**（2.6 才加入）；snapshotter 体系是 PG 专属 `io.debezium.connector.postgresql.spi.Snapshotter`，非 2.x 的通用 `SnapshotterService`。
2. `debezium-embedded` 的 `AbstractConnectorTest` 是 **JUnit 4 编译**（`org.junit.Before/After/Rule`）——JUnit 6 Jupiter 体系无法继承（vintage 线不可用）。
3. `io.debezium.engine.DebeziumEngine.create(Class<? extends SerializationFormat<T>>)` 方法在 1.9.7 存在，**但 `io.debezium.engine.format.Connect` 类是 2.0 才引入的**——`Connect.class` 装配形态在 1.9.7 不可用（设计期误判，落地期 Task 8 实证勘误）：自建基座实际走 `EmbeddedEngine.create()`（1.9.7 embedded 即 `DebeziumEngine<SourceRecord>` 同步实现），消费面拿到的是**裸 `SourceRecord`**（无 `RecordChangeEvent` 包装）。
4. 现有 27 IT 对官方基座的调用面仅 4 方法（`start` 50 处 / `stopConnector` 15 / `consumeRecords` 3 / `consumeRecordsByTopic` 2）+ CompletionCallback 接线 3 文件——自建薄基座可行。

3.6.1 依赖面中 **1.9.7 不存在**的 API（接缝层改写的根源）：`io.debezium.bean.StandardBeanNames`、`pipeline.notification.NotificationService`、`pipeline.source.SnapshottingTask`、`snapshot.SnapshotterService`、`schema.SchemaFactory`、`pipeline.signal.actions.snapshotting.SnapshotConfiguration`、`jdbc.DefaultMainConnectionProvidingConnectionFactory`/`MainConnectionProvidingConnectionFactory`、`connector.base.QueueProviderService`、`connector.common.DebeziumHeaderProducer`。**1.9.7 已有**（勿当缺失处理）：`heartbeat.HeartbeatFactory`（官方 Task 装配即用，5 参构造）、`ChangeEventQueue.Builder`（含 maxQueueSizeInBytes）、`PgConnectionSupplier`（`PostgresStreamingChangeEventSource` 内 static interface）、`SignalProcessor`（基础形态）。

构造器/体系形态差异（已核对 v1.9.7.Final 官方源码）：
- `PostgresEventDispatcher<TableId>` 11 参巨构造（含 `PostgresChangeRecordEmitter::updateSchema` 与 HeartbeatFactory）
- `PostgresSchema` 5 参构造（`connectorConfig, typeRegistry, defaultValueConverter, topicSelector, valueConverter`）
- PG 专属 `PostgresChangeEventSourceCoordinator`（非裸 `ChangeEventSourceCoordinator`，12 参）
- `PostgresValueConverter.of(config, charset, typeRegistry)` 静态工厂
- **主题体系是 `TopicSelector<TableId>`（`PostgresTopicSelector.create(config)`），2.0 才切 `TopicNamingStrategy`**——`StreamPostgresSchema`/Task 装配须按 TopicSelector 形态
- `PgOutputReplicationMessage` 1.9.7 已是 `(Operation, String table, Instant, Long txId, List<Column> old, List<Column> new)` 构造——`RowChangeEmitter` 改写面比预期小
- `ChangeEventSourceFactory<P,O>` 接口：`getStreamingChangeEventSource()` **零参**、`getSnapshotChangeEventSource(listener)` 单参（3.x 均带 partition/offset 参）
- metrics 体系结构不同：1.9.7 `DefaultStreamingChangeEventSourceMetrics` 内部为 `ConnectionMeter`/`StreamingMeter` 聚合（3.x 一整块字段），构造器签名不同

## 3. 模块定位与依赖边界

- 聚合 parent 追加 `<module>vb-stream-connector-postgres-stream-debezium19</module>`，根 pom **只加这一行**；1.9.7 专属版本属性全部放模块 pom（避免根 pom 出现双 debezium 版本属性的混乱）。
- 版本来源：
  - 复用根属性：`postgresql.version=42.7.13`（压 1.9.7 传递的 42.3.x，复制会话 API 与本仓其余模块同版本）、`chronicle-queue`、`HdrHistogram`、`junit-jupiter 6.x`、`testcontainers 2.x`、`logback`、surefire 配置。
  - 模块内覆盖：`debezium.version19=1.9.7.Final`；`kafka 3.1.0`（connect-api provided / 测试域 connect-runtime + connect-json，1.9.7 官方 Kafka 矩阵）；slf4j-api 2.0.17 provided 同款压制策略（1.9.7 传递 1.7.36，2.x 二进制兼容 1.7 编译面，ServiceLoader 绑定恢复 logback 1.5——与 3.6.1 模块 2026-09-01 用户裁决同款）。
- 测试域 Debezium 坐标仅 `io.debezium:debezium-embedded:1.9.7.Final`（test scope，传递 debezium-api 供自建基座使用）；**不需要任何 tests classifier**（3.x 基座的 connector-common/util:tests 坐标在 1.9.7 场景全部移除）。
- 编译边界同现有模块：不依赖 vb-stream-engine，协议层只面向 Debezium / Kafka Connect API。

## 4. 源码三桶（文件级，共 60 主源码文件）

分桶依据：逐文件 Debezium/Kafka import 统计（2026-09-30 实测）。

### 桶 A：原样复制（零 Debezium import 41 文件 + `ResolvedRelation`，共 42）

- `protocol` 子包全部 15 文件（`PgOutputStreamDecoder`/`PgOutputMessage`/`WireReader`/`NormalParsers`/`DmlParsers`/`TwoPhaseParsers`/`StreamParsers`/`TupleData`/`TupleValue`/`RelationColumn`/`StreamingMode`/`TruncateOption`/两个异常类/`package-info`）
- 核心层 26 文件：`StreamedTransactionAssembler`、`MessagePipe`、`ReplicationSession`、`StreamThroughputMetrics`、`StreamMetricsBridge`、`BucketReplayer`、`TransactionConsumer`、`RelationSnapshot`、`VersionedRelationRegistry`、`TxChange`/`TxBuffer`/`RowChange`/`MsgChange`/`TruncateChange`、事务事件模型（`TransactionEvent`/`TransactionKind`/`StreamingTransactionListener`）、`RawMessageListener`/`RawPeeks`/`MessagePreview`、关系解析（`RelationResolver`/`RelationLookup`/`BucketTableResolver`/`BucketState`）、`ColumnValueMapper`、`DmlKind`、`Module`、`package-info`
- `ResolvedRelation`（带 1 个 Debezium import，`TableId` 级别，1.9.7 存在）——复制后核对即过

复制后动作：包声明零改动（同包名），仅补模块级 CLAUDE.md。

### 桶 B：按 1.9.7 API 改写（接缝层，18 文件）

改写的对齐基准 = **1.9.7 官方 `PostgresConnectorTask` / `PostgresStreamingChangeEventSource` 源码**——装配骨架从官方 1.9.7 形态出发，替换为我们自建的流式源与组装器。

| 文件 | Debezium import | 改写要点 |
|---|---|---|
| `PostgresStreamConnectorTask` | 42 | 最大头：bean 体系/notification/heartbeat-factory/snapshotter-service/schema-factory/snapshotting-task 装配全换 1.9.7 官方 Task 形态；`ChangeEventSourceCoordinator`/`ChangeEventQueue` 按旧构造器 |
| `StreamChangeEventSourceFactory` | 18 | 工厂接口 1.9.7 签名（snapshot/streaming 两 source 的创建参数差异） |
| `PostgresStreamStreamingChangeEventSource` | 12 | `ChangeEventSource`/`Offsets`/分区接口 1.9.7 形态；监督壳行为（D7 shutdownFast）不变 |
| `DispatcherTransactionListener` | 11 | `PostgresEventDispatcher` 1.9.7 巨构造器 + dispatch 系列方法签名 |
| `StreamStreamingChangeEventSourceMetrics` | 10 | `StreamingChangeEventSourceMetricsMXBean` 1.9.7 方法集（`getLastEvent` 返回 `String` 而非 `Instant` 等差异） |
| `StreamPostgresSchema` | 9 | `PostgresSchema` 1.9.7 构造器（无 builder） |
| `StreamEventMetadataProvider` | 8 | `OffsetContext` getter 面 1.9.7 差异 |
| `StreamChangeEventSourceMetricsFactory` | 8 | `DefaultStreamingChangeEventSourceMetrics`/`SnapshotChangeEventSourceMetrics` 旧构造器 |
| `TruncateEmitter` | 7 | dispatcher/Envelope 面 1.9.7 签名（行为语义不变：默认跳过 t、`none` 才逐表发） |
| `RowChangeEmitter` | 7 | `PgOutputReplicationMessage` 1.9.7 已是 `List<Column>` 构造（§2 已核实）——仅核对该类对 column 面的其余调用签名 |
| `RelationTableFactory` | 7 | 'R' 消息 enrich 的 `PostgresConnection` 1.9.7 构造 |
| `PostgresStreamConnector` | 7 | `validate` 按 1.9.7 `Configuration.validate` 能力；`version()` 从 Module 读 |
| `RelationMetadataSource` | 6 | 同上 `PostgresConnection` 构造 |
| `PostgresStreamConnectorConfig` | 6 | snapshot 档位替换（见 §5.1）；`values.as.string`/`slot.*`/`pipe.*` 自建配置面原样 |
| `TypeRegistryColumnValueMapper` | 5 | `PostgresType` API 差异核对（预期小改） |
| `StreamStreamingChangeEventSourceMetricsMXBean` | 1 | 接口 extends 的 1.9.7 MXBean 方法集 |
| `StringColumnValueMapper` | 1 | 预期小改（pgoutput 文本透传路径） |
| `StringValueConverter` | 12 | import 全是 kafka connect data 层（`SchemaBuilder`/`Struct`），**预期原样**——1.9.7 的 connect-api 3.1 该面稳定，复制后核对 |

### 桶 C：配置资源

`META-INF/services/`（Connector SPI）、`plugin.xml`（assembly descriptor，改 finalName）、logback-test.xml、`module CLAUDE.md`/`README.md` 新写。

## 5. 关键适配裁定

### 5.1 `snapshot.mode=no_data` → `never`

1.9.7 无 `NO_DATA`（§2 事实 1）。等价性论证：本连接器的表结构自举走流内 'R' 消息 JDBC enrich（`RelationTableFactory`），从不依赖快照导出数据或 schema history 回放——`never` 档（"不快照、直接流式"）与 `no_data` 在本架构下行为等价。MS5 的"仅 no_data、其余启动期拒绝"翻译为"**仅 never**，其余档（always/initial/initial_only/exported/custom）启动期拒绝"。REST validate 真实生效 + 任务构造器 fail-fast 兜底的双层钉法照搬。

### 5.2 two_phase 全量保留

槽选项（自建 `ReplicationSession` 拼 `two_phase=true`）、19 消息协议解码（`TwoPhaseParsers`）、组装器 `preparedByGid` 挂起池全在桶 A 自管层；Debezium 侧只是发射通道，不依赖 3.x 的 two_phase 原生支持。MS4 四场景 IT、R5 存量槽 two_phase 预检（42710 复用检查）照搬。PG 15+ 前置不变（测试容器继续 postgres:18）。

### 5.3 notification 裁剪（唯一真裁剪项）

1.9.7 无 notification 体系，`PostgresStreamConnectorTask` 装配中对应接线点不存在。对宿主产品的影响：无 JMX/REST 通知通道——连接器状态可观测性由 MBean 指标（§5.4）与日志承担。

### 5.4 MBean 指标（MS5 五点插桩 + MBean 面）移植

`StreamThroughputMetrics`（桶 A）与五点插桩不动；`StreamMetricsBridge` 预计算挂统计 tick 不动；仅 `StreamStreamingChangeEventSourceMetrics`/`StreamStreamingChangeEventSourceMetricsMXBean`/`StreamChangeEventSourceMetricsFactory` 按 1.9.7 metrics 接口方法集改写（JMX 读零锁的桥接模式不变）。

### 5.5 其余功能面原样

`values.as.string` 全串开关（`StringValueConverter`/`StringColumnValueMapper` + Task 两处同开关切换）、Truncate 发射门控、'M' 逻辑消息 `slot.messages` 门控、aborted 子事务过滤、`provide.transaction.metadata` 默认 true 注入、at-least-once 语义（LSN 确认按输出前沿封顶）——全部位于自管层或与 1.9.7 兼容的 API 面，照搬。

### 5.6 signal / 增量快照

R2 已裁定 v1 不接 signal-based 增量快照（3.6.1 侧同样未接）——零损失，1.9.7 侧不适用项。

## 6. 测试策略

### 6.1 自建薄测试基座 `AbstractStreamIT`（关键决策）

1.9.7 官方 `AbstractConnectorTest` 为 JUnit 4 编译（§2 事实 2），JUnit 6 无法继承；调用面仅 4 方法 + CompletionCallback（§2 事实 4）。自建 Jupiter 基座：

- 内装 `DebeziumEngine.create(Connect.class)`（1.9.7 工厂形态，§2 事实 3）+ `ChangeConsumer` 记录收集 + 生命周期 latch/hook（参照 vb-stream-reader `EngineLifecycle` 模式）
- 对外暴露与 3.6.1 基座同名的方法：`start(Class, Configuration)` / `stopConnector()` / `consumeRecords(int[, Consumer])` / `consumeRecordsByTopic(int)` / CompletionCallback 捕获通道
- 27 IT 的翻译 = 改 import + 个别断言语义微调，不重写用例逻辑

### 6.2 测试矩阵

| 类别 | 数量 | 处理 |
|---|---|---|
| protocol 字节级单测 | ~15 | 原样复制（`MsgBuilder` 等辅助同搬） |
| 组装/回放/接缝单测 | ~16 | 桶 B 对应类的构造器调用点按 1.9.7 形态调整 |
| it 包 27 用例 | 27 | 基座换 `AbstractStreamIT`；`StreamPgTestEnv`（postgres:18 单例）配方照抄 |
| `ConnectPluginIT` | 1 | 容器 cp-kafka/cp-kafka-connect **8.3.0 → 7.1.x**（Confluent 7.1 = Kafka 3.1.0）；REST 建连接器 → topic 收数断言流程不变；@BeforeAll 产物结构断言前置（finalName 改 `-debezium19-plugin`） |

### 6.3 风险与回退

| 风险 | 等级 | 回退 |
|---|---|---|
| kafka 3.1 connect-runtime 在 Java 17 运行（官方背书 11） | 低 | 测试域 connect 升 3.3（API 兼容，1.9.7 连接器在 3.3 Connect 上官方可用） |
| pgjdbc 42.7.13 × 1.9.7 `PostgresConnection` 二进制兼容 | 低-中 | 首批 IT 即覆盖；若断裂按宿主产品实际 pgjdbc 版本 pin（42.3+ 的复制 API 面完整） |
| JUnit 6 与 1.9.7 传递测试依赖共存 | 已消解 | 不再继承 Debezium 基类（自建基座红利），仅 assertj/awaitility 由本仓显式管版本 |
| 1.9.7 `PostgresSchema`/`TypeRegistry` 对 PG 18 系统表查询兼容 | 低 | JDBC enrich 面为稳定系统表；IT postgres:18 全矩阵验证 |

## 7. 打包与文档

- assembly `plugin.xml` 同款复制：连接器自身 jar 落插件根 + runtime 依赖落 `lib/`；provided（connect-api/slf4j-api）与 test 依赖 scope 过滤排除；**debezium-connector-postgres 1.9.7 落 lib/**（独立 Connect 安装场景自带；嵌入宿主时若宿主已带同版本 Debezium，类重复且同版本无害——文档说明 first-win 语义）。finalName `vb-stream-connector-postgres-stream-debezium19-plugin`。
- 模块 README：定位（宿主 = 内置 Debezium 1.9.7 产品）、配置面、与 3.6.1 模块的**差异表**（no_data→never、notification 裁剪、IT 基座自建、Kafka 3.1 线、容器 7.1.x、双线同步约定）。
- 模块 CLAUDE.md：三桶结构、1.9.7 API 差异清单（§2）、自建基座说明。
- 根 CLAUDE.md 补模块索引行。
- **双线同步约定**：桶 A（protocol/组装/管道/会话/指标内核）的任何后续改动须双模块同步落地；接缝层豁免（本就因版本而异）。

## 8. 里程碑切分建议（供实施计划参考）

| MS | 内容 | 验收 |
|---|---|---|
| D19-MS1 | 模块骨架 + pom + 桶 A 37 文件复制 + protocol/组装单测复制 | 模块 `mvn test` 桶 A 单测全绿 |
| D19-MS2 | 接缝层改写：Config/Connector/Schema/Relation enrich（Task 可启动） | 离线单测 + 首个端到端 IT（流式大事务进记录） |
| D19-MS3 | Dispatcher/回放/监督壳/metrics 三件 | 核心五 IT 翻译绿（aborted 过滤/asOf/重启三情况） |
| D19-MS4 | 全量 IT 27 用例 + `ConnectPluginIT`（cp 7.1） | `mvn test` 全绿，对齐 3.6.1 模块 272 用例规模 |
| D19-MS5 | assembly 打包 + README/CLAUDE.md + 根文档 | 产物结构断言 + 文档差异表 |

依赖序：D19-MS1 → MS2 → MS3 → MS4 → MS5 线性；MS3 内 metrics 可与 dispatcher 并行。
