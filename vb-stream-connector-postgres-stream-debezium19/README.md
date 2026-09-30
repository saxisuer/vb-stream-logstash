# vb-stream-connector-postgres-stream-debezium19 — Debezium 1.9.7 流式连接器

## 定位与架构

流式专用的 PostgreSQL 逻辑解码 Kafka Connect 连接器插件（`connector.class = org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector`，与 3.6.1 模块同包名同类名）：适配 pgoutput **stream 模式**（`proto_version=4` + `streaming` + `two_phase`），进行中的大事务边收边发而非提交后整体回放。目标宿主 = **内置 Debezium 1.9.7.Final 的产品**（其 Kafka Connect API 线为 3.1.0，非 4.x）——本模块即 `vb-stream-connector-postgres-stream`（Debezium 3.6.1 版）面向该宿主的同款移植：**整模块复制 + 接缝层按 1.9.7 API 改写**（设计 spec `docs/superpowers/specs/2026-09-30-debezium19-connector-design.md`）。

架构、端到端数据流、组件分层与 at-least-once 语义与 3.6.1 模块 **1:1 相同**（流式解码/CQ 主缓冲管道/双线程解耦组装回放/事务元数据/two_phase/MBean 指标全保留）——内核层（protocol 解码、组装、管道、复制会话、指标）44 个主源码文件与 3.6.1 模块**字节等同**，仅 16 个 Debezium 接缝文件按 1.9.7 API 改写。逐组件机制见 3.6.1 模块 README 与包内 `src/main/java/.../stream/CLAUDE.md`；本模块与 3.6.1 模块的差异与双线同步约定见下文专节。**两模块永不同 classpath**（各嵌各的宿主/插件包，同包名不构成冲突）。

## 配置面

自建七项配置与 3.6.1 模块**逐字相同**（`slot.streaming` / `slot.two.phase` / `pipe.dir` / `pipe.roll.cycle` / `slot.feedback.interval.ms` / `slot.messages` / `values.as.string`——语义与约束见 3.6.1 模块 README 配置面表格，配置语义真源是 `PostgresStreamConnectorConfig` 的 javadoc）。两项默认面钉死中 `provide.transaction.metadata=true` 不变；`snapshot.mode` 钉死值为 **`never`**（差异见下表）。另有三处 **1.9.7 继承面的配置键差异**，从 3.6.1 迁移配置时必须对照：

| 配置项 | 3.6.1 模块 | 本模块（1.9.7 面） | 机理 |
|---|---|---|---|
| 逻辑名键 | `topic.prefix` | **`database.server.name`** | `topic.prefix` 是 2.0+ 键，1.9.7 的 `RelationalBaseSourceConnector` 不识别，缺失即启动失败（NPE 于父构造器的 transactionTopic 模板替换） |
| `snapshot.mode` 钉死值 | `no_data`（唯一合法值） | **`never`**（唯一合法值） | 1.9.7 的 `SnapshotMode` 枚举无 `no_data`（2.6 才加入）。本连接器表结构自举走流内 'R' 消息 JDBC enrich、从不依赖快照导出，`never`（不快照、直接流式）与 `no_data` 在本架构下行为等价（spec §5.1）。其余档（initial/always/initial_only/exported/custom）REST validate 与任务构造器两级启动期拒绝 |
| TRUNCATE 放行 | `skipped.operations=none` 单键 | **三键**：`skipped.operations=none` + `truncate.handling.mode=include` + `plugin.name=pgoutput` | 1.9.7 保留废弃键 `truncate.handling.mode`（默认 skip）且 PG 连接器覆写 `getSkippedOperations()` 把它并入跳过集——只配 `none` 会静默零发射；`include` 档的继承校验只对支持 TRUNCATE 的解码器放行，而 `plugin.name` 父默认是 decoderbufs（2.0+ 才改默认 pgoutput）——本连接器线协议恒 pgoutput（`ReplicationSession` 槽选项硬编码），显式声明零行为副作用。**默认跳过 TRUNCATE 的行为两线一致** |
| TOAST 未变占位键 | `unavailable.value.placeholder` | **`toasted.value.placeholder`** | 1.9.7 的取值器先读 `toasted.value.placeholder`（**带默认 `__debezium_unavailable_value`，恒非空**）再回落 `unavailable.value.placeholder`（无默认）——沿用旧键会被前者默认遮蔽而**静默失效** |

## 打包与安装

```bash
mvn -pl vb-stream-connector-postgres-stream-debezium19 clean package -DskipTests
```

产物（maven-assembly `plugin` 目录清单，`finalName` 带模块后缀）：

```
target/
├── vb-stream-connector-postgres-stream-debezium19-plugin/   # 安装即拷此目录
│   ├── vb-stream-connector-postgres-stream-debezium19-1.0-SNAPSHOT.jar  # 连接器自身 jar（带 SourceConnector ServiceLoader 清单）
│   └── lib/                                                 # 32 个 runtime 依赖：debezium-connector-postgres/debezium-core/debezium-api 1.9.7.Final、
│                                                            #   postgresql-42.7.13、chronicle-*、HdrHistogram 等
└── vb-stream-connector-postgres-stream-debezium19-plugin.zip  # 同构分发物
```

Connect runtime 已提供的坐标显式排除在清单外（与 3.6.1 模块同款 R4 边界）：`connect-api`、`kafka-clients`、`slf4j-api` 及其独占子件 `zstd-jni`/`lz4-java`/`snappy-java`/`jakarta.ws.rs-api`。**debezium-connector-postgres 1.9.7 落 lib/**：独立 Connect 安装场景自带；嵌入宿主时若宿主已带同版本 Debezium，类重复且同版本无害（插件类加载器 first-win，spec §7）。

安装：把 plugin 目录放进 worker 的 `plugin.path` 后 REST 建连接器（**扁平 config map**，`PUT /connectors/{name}/config` 形态；注意逻辑名键是 `database.server.name`）：

```bash
curl -X PUT http://connect:8083/connectors/pg-stream-19/config \
  -H 'Content-Type: application/json' -d '{
  "connector.class": "org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.dbname": "postgres",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.server.name": "pgstream19",
  "slot.name": "vb_stream_slot_19",
  "publication.name": "vb_pub",
  "pipe.dir": "/var/lib/kafka-connect-pipe/pg-stream-19"
}'
```

`snapshot.mode` 与 `provide.transaction.metadata` 有意不设——默认注入（never/true）即预期形态。部署注记：①`pipe.dir` 用绝对路径；②连接器内 Chronicle Queue 的 mmap 需开放 JDK 内部包（`KAFKA_OPTS` 注入与 3.6.1 模块相同的 `--add-opens` 五件清单）。端到端验收在档：`ConnectPluginIT`——真 Kafka Connect 容器（Confluent 7.1.0 = Kafka 3.1.0，Debezium 1.9.7 官方 Kafka 矩阵）装 assembly 插件，REST 建连接器 → PG 写入 → topic 收数断言；容器形态为**自建 ZooKeeper 单容器 + cp-kafka-connect 7.1.0 基座叠加 eclipse-temurin:17-jre 覆盖层**（镜像 JVM 原为 Zulu 11，与本模块 release 17 字节码硬冲突，覆盖层保留 Confluent 启动链与 Connect 3.1.0 runtime 原样，见模块 CLAUDE.md）。

## at-least-once 与停机语义

与 3.6.1 模块**逐条相同**（内核字节等同，语义载体不经过 Debezium 接缝层）：

- **输出前沿锚定事务尾（End）**：一个事务的全部记录发出、事务边界 offset 提交后才推进前沿；LSN 反馈按 `min(已收到, 前沿)` 封顶——未完整输出的事务钉住槽的 `confirmed_flush`，服务端必为其保留 WAL。
- **crash = 整事务重发、不去重**：输出中途失败（End 未达）前沿不推进，重启后 PG 从确认位点整桶重发；重复收敛交给下游（事务元数据 BEGIN/END + 记录 source 块 LSN/事务 id 幂等去重）。
- **优雅停机不排干（D7 shutdownFast）**：任务停止立即断复制流、不回放积压交接桶——快速让位优先于多输出，未输出事务由槽重发补齐。
- **重启续传锚槽 confirmed_flush**（≤ 输出前沿）：offset 落后重复段取并集；管道目录重启自动清空属预期（瞬态工作区，真源是复制槽）。
- **回滚语义**：aborted 子事务回放期剔除不进 Kafka；整事务回滚与 ROLLBACK PREPARED 只留日志痕迹零发射。
- **consumer 慢/停摆不回压 reader**：代价转移到磁盘（管道目录 + WAL 保留增长），`max_slot_wal_keep_size` 兜底；观测面经 JMX MBean（五速率/lagBytes/挂起 prepared 数/管道磁盘占用，`StreamMetricsBridge` 挂统计 tick 预计算，JMX 读零锁）。

## 开发与测试

- **模块边界**：零 `org.vastdata.vbstream` import（与 3.6.1 模块同款结构性保证）；不依赖 3.6.1 模块——两模块是独立平行的同包名实现
- **测试形态**：`mvn test` 单命令全跑，**275 用例** = 离线单测 245（36 类：protocol 字节级 + 组装/回放/接缝/三件套，多数与 3.6.1 版字节等同直过）+ `it` 包 30（27 语义 IT + 基座契约 2 + `ConnectPluginIT` 1）
- **自建 IT 基座 `AbstractStreamIT`**：1.9.7 官方 `AbstractConnectorTest` 是 JUnit 4 编译（`org.junit.Before/After`），JUnit 6 Jupiter 无法继承——基座内装 `EmbeddedEngine.create()` 同步引擎 + 记录收集 + 生命周期闩，对外暴露与 3.6.1 基座同名的方法面（start/stopConnector/consumeRecords×2/consumeRecordsByTopic/awaitEngine/assertNoEngineFailure），27 个语义 IT 的断言面与 3.6.1 版逐字同锚（细节见模块 CLAUDE.md）
- **容器**：`StreamPgTestEnv` postgres:18 单例（与 3.6.1 模块同配方）；`ConnectPluginIT` 用 cp-kafka/cp-kafka-connect 7.1.0 + temurin-17 覆盖层
- **代码内文档真源**：模块 `CLAUDE.md`（三桶结构/1.9.7 API 差异清单/基座说明）与包内 `src/main/java/.../stream/CLAUDE.md`、`stream/protocol/CLAUDE.md`（与 3.6.1 模块同文，文件头附本模块归属注记）

## 与 3.6.1 模块的差异与同步约定

内核行为两线一致；差异全部落在 Debezium 接缝面与配套（宿主/测试基建）。实证条目：

| # | 差异点 | 3.6.1 模块 | 本模块（1.9.7） |
|---|---|---|---|
| 1 | `snapshot.mode` 钉死值 | 仅 `no_data` | 仅 `never`——1.9.7 枚举无 `no_data`；表结构自举走流内 'R' enrich，两档行为等价（spec §5.1） |
| 2 | notification 通道 | Debezium 3.x NotificationService 装配 | **裁剪**——1.9.7 无此体系，连接器状态可观测性由 MBean 指标 + 日志承担（spec §5.3，唯一真裁剪项） |
| 3 | Kafka Connect API 线 | 4.3（connect-api provided） | **3.1.0**（provided；1.9.7 官方 Kafka 矩阵，宿主内置 1.9.7 全家桶） |
| 4 | 逻辑名配置键 | `topic.prefix` | `database.server.name`（见配置面表） |
| 5 | IT 基座 | 官方 `AbstractAsyncEngineConnectorTest` | **自建 `AbstractStreamIT`**（1.9.7 官方基座 JUnit 4 编译无法在 JUnit 6 继承；停机/失败语义三分：未启动 no-op / `awaitEngine` 纯等待 / `expectEngineFailure` 预期失败豁免 + `engineFailure()` 取原始异常） |
| 6 | TRUNCATE 放行键 | `skipped.operations=none` 单键 | 三键（+ `truncate.handling.mode=include` + `plugin.name=pgoutput`，见配置面表；默认跳过行为一致） |
| 7 | TOAST 未变占位键 | `unavailable.value.placeholder` | `toasted.value.placeholder`（见配置面表） |
| 8 | 启动校验拒绝的异常面 | 异常链含具体拒绝文案 | 1.9.7 `BaseSourceTask.start` 把校验问题**记 ERROR 日志**后抛**无 cause** 的笼统 `ConnectException`——定位拒绝原因须看日志（`DefaultsAndMetricsIT` 场景② 双面断言：链面钉失败发生点 + ListAppender 捕 ERROR 行钉 "never only" 文案） |
| 9 | embedded 引擎停机等待 | 异步引擎 `shutdownNow` 即时中断 | 1.9.7 `EmbeddedEngine.stop()` 在 interrupt 前先等引擎完成闩，系统属性 `debezium.embedded.shutdown.pause.before.interrupt.ms` **默认 5 分钟**——crash 注入类 IT 须压短（`LogicalMsgIT` `@BeforeAll` 压 1s、`@AfterAll` 清除） |
| 10 | embedded 引擎装配/记录类型 | `DebeziumEngine.create(Connect.class)`、`RecordChangeEvent<SourceRecord>` 包装 | **`EmbeddedEngine.create()`**（1.9.7 无 `io.debezium.engine.format.Connect` 类，2.0 才引入）、记录类型**裸 `SourceRecord`** |
| 11 | `ConnectPluginIT` 容器形态 | cp-kafka/cp-kafka-connect 8.3.0（AK 4.3，KRaft 容器组，镜像 JVM 可跑 17 字节码） | **自建 ZooKeeper 单容器（`ZkKafkaContainer`）+ cp-kafka-connect 7.1.0 基座叠 eclipse-temurin:17-jre 覆盖层**——真 Connect 3.1.0 runtime × Java 17（镜像原 JVM Zulu 11 与 release 17 字节码硬冲突；testcontainers 2.x kafka 模块恒以 KRaft 起 Confluent 容器、7.1 拒绝 KRaft 参数面） |

**双线同步约定**（spec §7）：桶 A 内核——`protocol/` 子包、组装（`StreamedTransactionAssembler`/桶/交接）、管道（`MessagePipe`）、复制会话（`ReplicationSession`）、指标（`StreamThroughputMetrics`/`StreamMetricsBridge`）——的任何后续改动须**双模块同步落地**（这些文件本就字节等同，同步 = 复制后零改动）；接缝层（桶 B 16 文件，Task/Config/Schema/dispatcher/metrics 三件等）**豁免**——本就因 Debezium 版本而异，各随各的 API 面。

## Known limitations

- **无 notification 通道**：1.9.7 无 notification 体系，无 JMX/REST 通知——可观测性由 MBean 指标与日志承担（spec §5.3）。
- **增量快照不接**（R2 裁定同 3.6.1，2026-09-05）：v1 不接 signal-based 增量快照（`StreamChangeEventSourceFactory` 显式覆写 `getIncrementalSnapshotChangeEventSource` 返回 empty 钉住裁定），见 `docs/superpowers/specs/2026-09-05-ms5-r2-incremental-snapshot-audit.md`。
- **数组列 fail-fast**（仅类型化路径；`values.as.string=true` 下消解）、**未知类型照 vanilla 静默 null**、**LogicalMsg 解析但不发射**、**无快照数据抽取**——四项与 3.6.1 模块同款，详见 R1/R3 审计文档「已知限制与延期」节。
- **1.9.7 继承面的离线构造坑**（影响测试/离线直构，非运行期）：`PostgresOffsetContext.Loader.load` 对 `ts_usec` 裸拆箱（空 map 即 NPE）、`snapshot.mode=custom` 无 `snapshot.custom.class` 时父校验器 NPE（vanilla 缺陷）——离线构造须种 `ts_usec`，custom 档须占位 class。
- **`SmokeIT` 的 `@TempDir(cleanup = NEVER)` 跨次累积目录**（Windows mmap 句柄异步释放，删除会撞文件锁；临时目录交 OS 清理 %TEMP%，属模块测试规约而非产物缺陷，POSIX 不受影响）。
