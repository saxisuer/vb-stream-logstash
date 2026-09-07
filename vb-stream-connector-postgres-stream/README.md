# vb-stream-connector-postgres-stream — Debezium 流式连接器

## 定位与架构

流式专用的 PostgreSQL 逻辑解码 Kafka Connect 连接器插件（`connector.class = org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector`）：适配 pgoutput **stream 模式**（`proto_version=4` + `streaming` + `two_phase`），进行中的大事务边收边发而非提交后整体回放；不做快照数据抽取（`snapshot.mode` 仅 `no_data`，该职能属 vanilla `postgresql-connector`——两者可并存于同一 Connect 集群）。架构上是引擎 `vb-stream-engine` 解耦形态的 1:1 接线（零引擎 import，代码文字参照非依赖）：reader 线程（`vb-pgoutput-reader`）raw drain 并把每条消息落 Chronicle Queue 主缓冲管道、桶只记 index 段（组装期堆内零字节引用），提交事务交接给 consumer 线程（`transaction-consumer`）逐条回放解码、按变更时刻的表结构版本（asOf）渲染、经 Debezium dispatcher 发进 Kafka——大事务回放再慢也不阻塞读取，代价转移到磁盘（管道目录与 PG 侧 WAL 保留增长，`max_slot_wal_keep_size` 兜底）。组件图与线程约束详见包内 `src/main/java/.../stream/CLAUDE.md`；端到端数据流与逐层讲解见下节。

## 端到端数据流与组件分层

数据流自上而下贯穿三级线程上下文（Connect runtime → reader/consumer 双线程 → Kafka 出口）：

```
Kafka Connect runtime
  │ PostgresStreamConnectorTask.start()        ← 全链路装配（vanilla PostgresConnectorTask:101-284 同序替换）
  ▼
PostgresStreamStreamingChangeEventSource.execute()   ← 监督壳（coordinator 线程）
  │  装配 ReplicationSession + StreamedTransactionAssembler（构造即起 consumer 线程）
  │  起 vb-pgoutput-reader 线程跑 session.run(assembler, frontier::get)；200ms 心跳监督
  ▼
PostgreSQL（walsender：pgoutput v4 + streaming + two_phase）
  │ CopyData/'w' 帧（pgjdbc 已剥复制协议封装）
  ▼ ┌────────────────── reader 线程 ──────────────────┐
ReplicationSession.run()
  │  每轮五步：isClosed 守卫 → readPending drain（取尽缓冲，空轮才睡 100ms）
  │          → 确认值 = min(已收到, 输出前沿) → 满间隔才 forceUpdateStatus → 续转
  │ RawMessageListener.onRaw(byte[])
StreamedTransactionAssembler.onRaw()
  │  每条消息先 pipe.append 落盘取 CQ index 作 seq
  │  控制消息（B/C/S/E/c/A/b/P/K/r/p）+ 'R'：当场解码，驱动桶状态机
  │  I/U/D/T/M：不解码——只窥 oid，把 index 记入桶的连续段
  │  Commit/StreamCommit/CommitPrepared = 交接：拷 Relation 快照冻结桶 → 入队 → 立即返回
  ▼ （交接队列，FIFO = 提交序）
  │ ┌────────────────── consumer 线程 ────────────────┐
TransactionConsumer
  │  BucketReplayer 逐段 readRange（数据落盘后仅此一读）→ decodeSingle → 按桶快照 asOf 渲染
  │  逐条回调 DispatcherTransactionListener.onEvent(Begin → TxChange* → End)
  │    Begin：锚定事务边界 offset + 发事务块 BEGIN
  │    RowChange：resolve asOf Table → 安装 schema → dispatchDataChangeEvent
  │    End：发事务块 COMMIT → 返回 = 确认完整消费 → 前沿 AtomicLong ← endLsn
  │ └─────────────────────────────────────────────────┘
  ▼
Debezium PostgresEventDispatcher → ChangeEventQueue → Task.doPoll() → Kafka topic
```

各层职责与关键机制：

### 协议层——`protocol/` 子包

- pgoutput 19 种消息的纯函数解码（sealed interface `PgOutputMessage` + 19 个 record；无 IO、零 Debezium import，可独立移植）
- `PgOutputStreamDecoder` 双入口：`decode()` 顶层消息入口（内建流块状态机 `inStream`，出口强校验剩余字节 = 0 防错位扩散）；`decodeSingle()` 回放专用入口（白名单只收 M/R/Y/I/D/T 数据消息——回放时桶内消息本身处于流式块语境，免 'S'/'E' 重建上下文）
- 字节格式第一手总表与两处实测勘误（StreamAbort 类型字节是**大写 'A'**；StreamCommit 在 xid 后有一个被消费不建模的 flags(0) 字节）见 `protocol/CLAUDE.md`

### 复制会话——`ReplicationSession`

- 两条连接（普通 SQL + `replication=database`），生命周期 open → ensureSlot → start → run → close；幂等建槽带 two_phase，撞 SQLState 42710（槽已存在）时转存量槽 two_phase 预检（R5），不匹配启动期拒绝
- run 循环是吞吐命门：`readPending` **drain**（每轮取尽缓冲——每轮取一条 + 固定睡 100ms 会把读取钉死在 ~10 msg/s）；LSN 反馈确认值 = **min(已收到, 输出前沿)**——前沿只在事务 End 之后推进，crash 时未输出事务 PG 必然重发，这是 at-least-once 的实现机制

### 组装域（reader 线程）——`StreamedTransactionAssembler`

- 桶（`TxBuffer`）模型：普通事务单指针（协议保证 Begin..Commit 串行不嵌套）；流式事务按顶层 xid 多桶并存（并发大事务流段交错）；两阶段 `preparedByGid` 挂起池（PREPARE 后可能长期挂起，等 COMMIT PREPARED 输出 / ROLLBACK PREPARED 丢弃）
- **组装期不解码数据**：I/U/D/T/M 只窥 relation oid 记 CQ index 连续段（堆占用 = 段数 × long[2]，不随单元数增长）——回滚的大事务从未被解码过
- **同事务 DDL 正确性**：'R'（表元数据）按到达 seq 记入 `VersionedRelationRegistry` 版本日志；交接时按桶 oidSet 圈定、截止 lastIndex 拷出 `RelationSnapshot` 随桶冻结——回放时每个单元按**自己的 seq** 取"变更那一刻"的表定义，事务中途 DDL 前后段的行各按各的结构解释
- 两个低水位（勿混）：CQ 删除低水位（删过老滚动文件）与 registry 剪枝低水位（收缩版本日志），都挂在桶完结点，消息热路径零开销

### 回放域（consumer 线程）——`TransactionConsumer` + `BucketReplayer`

交接队列取冻结桶 → 发 Begin 头 → 逐段 `readRange` → `decodeSingle` → asOf 渲染 → **逐条即时回调（回放期堆峰 O(单条)，不攒 List）** → aborted 子事务过滤 → 发 End 尾 → 前沿 ← endLsn。**End 返回 = 下游确认完整消费**，是整条管线的背压点。

### Debezium 接线层

- 三件套：`PostgresStreamConnector`（ServiceLoader 入口）/ `PostgresStreamConnectorConfig`（配置面真源）/ `PostgresStreamConnectorTask`（`start()` 是 vanilla `PostgresConnectorTask:101-284` 的同序替换装配，替换点仅 schema / 元数据提供者 / 源工厂三处）
- 监督壳 `PostgresStreamStreamingChangeEventSource`：与 vanilla 的本质差异——消息处理不在 coordinator 线程内联，本类只做装配、心跳与停机次序；失败汇聚带"停机期失败忽略"守卫（D7 快速停机会砸中在途回放，属正常收敛，不上报为 FAILED）
- `DispatcherTransactionListener`：流式事件 → dispatcher 的翻译器，几处踩坑语义——事务 id 必须纯数字且与元数据提供者同源（否则 TransactionMonitor 给每事务补发空 BEGIN/END 对）；source 块 txId 恒取**顶层 xid**（流式单元携带的是子事务 xid，aborted 过滤正依赖该语义区分）；TRUNCATE 按 `skipped.operations` 门控（默认 "t" 跳过）
- 自死锁防御：初始 offset 读取的 `txid_current()` 会给 autoCommit=false 的 main 连接分配 XID，**读后必须立即 commit**——否则另一条连接上的 CREATE SLOT 为等解码一致点会死等（IT 实测）

### 指标面

- `StreamThroughputMetrics` 四点插桩（reader 记 slot 读取、组装器记交接、consumer 记输出与分布），10s INFO 三行（吞吐/分布/峰值），与引擎 `ThroughputMetrics` 同口径
- `StreamStreamingChangeEventSourceMetrics` 经 Debezium metrics 体系暴露 JMX：五速率 / lagBytes / 挂起 prepared 数 / 管道磁盘占用；`StreamMetricsBridge` 挂统计 tick **预计算**，JMX 读零锁零计算零 IO

### 贯穿全局的三条设计主线

1. **读取/组装/回放解耦**——reader 永不等待 consumer，consumer 慢/停摆不回压读取，代价转移到磁盘（语义见下文 at-least-once 节）
2. **End 锚定的输出前沿**——一个 `AtomicLong` 串起整个 at-least-once：LSN 确认按前沿封顶，前沿只在 End 后推进，crash 时未输出事务必然重发，头行重复允许、尾部永不丢
3. **快照随行**——交接时把表结构版本快照冻结进桶，回放自足不查 registry：换来 registry 可激进剪枝、同事务 DDL 正确、交接桶与版本日志完全解耦

## 配置面

父类 `io.debezium.connector.postgresql.PostgresConnectorConfig` 的完整配置面（`database.*` 四件套、`topic.prefix`、`slot.name`、`publication.name`、`skipped.operations`、转换器等）全部可用；本连接器追加六个专属项并钉死两项默认面。**配置语义真源是 `PostgresStreamConnectorConfig` 的 javadoc**，下表与其逐一对照（改配置先改 javadoc，再同步此处）：

| 配置项 | 默认 | 语义与约束 |
|---|---|---|
| `slot.streaming` | `on` | 流式档位：`off`（提交后整体回放）/ `on`（进行中大事务边收边发）/ `parallel`（流式+并行），大小写宽容。`parallel` 必须搭配 `slot.two.phase=true`，否则启动期校验拒绝（PG 侧 parallel 流式解码以 two_phase 为前置） |
| `slot.two.phase` | `true` | 建槽带 `two_phase` 选项（建槽后不可更改；存量槽 two_phase 不匹配时启动期拒绝并附 DROP SLOT 迁移指引）。`parallel` 档的前置（PG 15+） |
| `pipe.dir` | `pg-stream-pipe-queue` | Chronicle Queue 管道工作目录（瞬态工作区，**重启自动清空属预期**——真源是复制槽，PG 从确认位点重发未输出事务）。相对路径按 worker 进程工作目录解析，**真 Connect 部署建议显式配绝对路径**（容器/服务形态下 worker CWD 不确定） |
| `pipe.roll.cycle` | `MINUTELY` | 管道滚动周期，`LegacyRollCycles` 枚举名（大小写宽容）；未知值启动期校验拒绝并附可用值清单（残余到建管道才炸会拖垮 reader 线程） |
| `slot.feedback.interval.ms` | `10000`（=10 秒） | 复制会话 LSN 反馈节流周期（毫秒，正整数；确认值经输出前沿封顶）。整除换算为秒——亚秒值（如 500）截断为 0 即每轮都反馈，不会静默翻倍 |
| `slot.messages` | `false` | 'M' 逻辑消息门控（PG 14+）：true 时槽选项追加 `messages=true`，逻辑消息逐条解析记录（INFO 两时点：非事务 reader 即时/事务性 consumer 回放期）且非事务消息经护栏参与输出前沿安全推进（全有或全无：无未输出桶才推进到消息位，有则完全静止）；**不发射下游**。false 时槽选项与行为完全同未开档 |

两项默认面钉死（非新键，同名替换/注入）：

| 配置项 | 钉死值 | 语义 |
|---|---|---|
| `snapshot.mode` | `no_data`（唯一合法值） | 本连接器流式-only、不做快照数据抽取：缺省由 `taskConfigs` 注入 no_data；`initial`/`always`/`when_needed` 等其余值经 REST validate 与任务构造器**两级启动期拒绝**（fail-fast，不残余到运行期） |
| `provide.transaction.metadata` | `true` | 事务元数据常开：数据 topic 之外向 `<topic.prefix>.transaction` 发 BEGIN/END 事务边界记录——at-least-once 语义下下游按事务幂等收敛的依据（见下节） |

## 打包与安装

```bash
mvn -pl vb-stream-connector-postgres-stream clean package -DskipTests
```

产物（maven-assembly `plugin` 目录清单，绑 `package` 阶段）：

```
target/
├── vb-stream-connector-postgres-stream-plugin/        # 安装即拷此目录
│   ├── vb-stream-connector-postgres-stream-1.0-SNAPSHOT.jar   # 连接器自身 jar（带 SourceConnector ServiceLoader 清单）
│   └── lib/                                           # 全部 runtime 依赖 jar（pgjdbc/chronicle-queue/debezium 等，个数以实际构建为准）
└── vb-stream-connector-postgres-stream-plugin.zip     # 同构分发物（数十 MB 级）
```

Connect runtime 已提供的坐标**显式排除**在清单外（plugin.path 隔离类加载器下插件自包含、两连接器并存不互扰——重复类会让 Connect 启动即炸）：`connect-api`、`kafka-clients`、`slf4j-api` 及其独占子件 `zstd-jni`/`lz4-java`/`snappy-java`/`jakarta.ws.rs-api`；test 依赖被 scope 过滤天然排除。

安装：把 plugin 目录（解 zip 或拷目录）放进 worker 的 `plugin.path`（每子目录一个隔离插件位）后重启/触发插件扫描，REST 建连接器——**请求体直接是扁平 config map**（`PUT /connectors/{name}/config` 的形态；`{"name":..,"config":{..}}` 包装是 `POST /connectors` 的形态，Connect 4.3 在 PUT 端点按 `Map<String,String>` 反序列化，包装体即 500）：

```bash
curl -X PUT http://connect:8083/connectors/pg-stream-1/config \
  -H 'Content-Type: application/json' -d '{
  "connector.class": "org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.dbname": "postgres",
  "database.user": "postgres",
  "database.password": "postgres",
  "topic.prefix": "pgstream",
  "slot.name": "vb_stream_slot",
  "publication.name": "vb_pub",
  "pipe.dir": "/var/lib/kafka-connect-pipe/pg-stream-1"
}'
```

`snapshot.mode` 与 `provide.transaction.metadata` 有意不设——默认注入（no_data/true）即预期形态。部署注记两件：①`pipe.dir` 用绝对路径（默认相对路径按 worker CWD 解析，跨容器/服务形态不确定，本例即显式指定）；②连接器内 Chronicle Queue 的 mmap 需开放 JDK 内部包——Kafka/Connect 启动环境给 JVM 加 `--add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED --add-opens java.base/sun.nio.fs=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED`（Confluent 镜像经 `KAFKA_OPTS` 注入）。端到端验收在档：`ConnectPluginIT`（Testcontainers 起真 Kafka Connect 装此插件，REST 建连接器 → PG 写入 → topic 收数断言）。

## at-least-once 与停机语义

- **输出前沿锚定事务尾（End）**：一个事务的全部记录发出、事务边界 offset 提交后才推进前沿；LSN 反馈按 `min(已收到, 前沿)` 封顶——未完整输出的事务钉住槽的 `confirmed_flush`，服务端必为其保留 WAL。
- **crash = 整事务重发、不去重**：输出中途失败（End 未达）前沿不推进，重启后 PG 从确认位点整桶重发——下游可能重复见到已输出事务的部分/全部记录，**本连接器不丢不重承诺是 at-least-once**，重复收敛交给下游：事务元数据 topic 的 BEGIN/END（`provide.transaction.metadata=true` 默认开）+ 记录 source 块的 LSN/事务 id 供幂等去重。
- **优雅停机不排干（D7 shutdownFast）**：任务停止时立即断复制流、不回放积压的交接桶——快速让位优先于多输出（未输出事务由槽重发补齐，与 crash 语义同族收敛）。
- **重启续传锚槽 confirmed_flush**（≤ 输出前沿）：offset 落后于槽确认位时重复段取并集，不丢不静默吞；管道目录重启自动清空属预期（瞬态工作区，真源是复制槽）。
- **回滚语义**：aborted 子事务（SAVEPOINT 回滚）的变更在回放期剔除、不进 Kafka；整事务回滚与 ROLLBACK PREPARED 只留日志痕迹零发射。
- **consumer 慢/停摆不回压 reader**：代价转移到磁盘（管道目录 + WAL 保留增长），`max_slot_wal_keep_size` 兜底；lagBytes 等观测面经 JMX MBean 暴露（`StreamStreamingChangeEventSourceMetrics`：五速率/lagBytes/挂起 prepared 数/管道磁盘占用）。

## 开发与测试

- **模块边界**（pom 结构性保证）：零 `org.vastdata.vbstream` import——协议层是引擎 `vb-stream-engine` protocol 包的 1:1 手写重写（文字参照非依赖）；反向复用 Debezium 3.6.1 的 Config/Schema/Emitter/offset 体系，`connect-api` 为 `provided`（运行期由 Connect runtime 提供）
- **测试形态**（`mvn test` 单命令全跑；surefire 显式补 `**/*IT.java` include——默认模式不含该命名）：离线单测（协议字节级 `MsgBuilder` 手造字节 + 组装/回放状态机假件驱动，零 PG）+ `it` 包集成测试（embedded engine 基座 + Testcontainers 真 PG 18：流式大事务进 Kafka/aborted 子事务过滤/同事务 DDL asOf/重启三情况/两阶段四场景/存量槽 two_phase 拒绝/缺省配置注入）；`ConnectPluginIT` 独立不挂基座——真 Kafka Connect 容器装 assembly 插件跑端到端验收
- **代码内文档真源**：包内 `src/main/java/.../stream/CLAUDE.md`（组件图、线程审计 R1/R3、配置面与停机次序）与 `protocol/CLAUDE.md`（19 消息字节格式速查与勘误）

## Known limitations

- **数组列 fail-fast**（非 vanilla 的静默 null）：consumer 线程不持 JDBC 连接（R3 线程约束），数组列解析抛 `DebeziumException` 显式停机 + 槽重发——判定比静默丢值安全，维持现状；支持路径与论证见 R1/R3 审计「已知限制与延期」第 1 条。
- **未知类型照 vanilla 静默 null**：`include.unknown.datatypes`（默认 false）为 false 时未知类型返回 null 不抛，true 时照发原串。
- **LogicalMsg 解析但不发射**：`slot.messages=true` 只开解析记录与前沿安全推进，逻辑消息不进 Kafka topic（发射仍延期，专属 topic `.message` 后缀等设计要点在 R1/R3 审计第 3 条）。
- **无快照数据抽取**：`snapshot.mode` 仅 `no_data`，初始存量/按需快照请用 vanilla `postgresql-connector`。
- **增量快照不接**（MS6 裁定，2026-09-05）：v1 不接 signal-based 增量快照——vanilla 形态的三个交错面（第二 dispatch 线程/main 连接时序独占消失/offset 与前沿交互）各打破一处既有契约，接入需先满足前置条件五项路线图，见 `docs/superpowers/specs/2026-09-05-ms5-r2-incremental-snapshot-audit.md`。
- 其余已知限制与延期（Truncate 选项位协议层超集、`skipped.operations` 偏差等）统一记档于 `docs/superpowers/specs/2026-09-02-ms2-r1-r3-audit.md`「已知限制与延期」节。
