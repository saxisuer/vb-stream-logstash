# vb-stream-connector-postgres-stream — Debezium 流式连接器

## 定位与架构

流式专用的 PostgreSQL 逻辑解码 Kafka Connect 连接器插件（`connector.class = org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector`）：适配 pgoutput **stream 模式**（`proto_version=4` + `streaming` + `two_phase`），进行中的大事务边收边发而非提交后整体回放；不做快照数据抽取（`snapshot.mode` 仅 `no_data`，该职能属 vanilla `postgresql-connector`——两者可并存于同一 Connect 集群）。架构上是引擎 `vb-stream-engine` 解耦形态的 1:1 接线（零引擎 import，代码文字参照非依赖）：reader 线程（`vb-pgoutput-reader`）raw drain 并把每条消息落 Chronicle Queue 主缓冲管道、桶只记 index 段（组装期堆内零字节引用），提交事务交接给 consumer 线程（`transaction-consumer`）逐条回放解码、按变更时刻的表结构版本（asOf）渲染、经 Debezium dispatcher 发进 Kafka——大事务回放再慢也不阻塞读取，代价转移到磁盘（管道目录与 PG 侧 WAL 保留增长，`max_slot_wal_keep_size` 兜底）。组件图与线程约束详见包内 `src/main/java/.../stream/CLAUDE.md`。

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

## Known limitations

- **数组列 fail-fast**（非 vanilla 的静默 null）：consumer 线程不持 JDBC 连接（R3 线程约束），数组列解析抛 `DebeziumException` 显式停机 + 槽重发——判定比静默丢值安全，维持现状；支持路径与论证见 R1/R3 审计「已知限制与延期」第 1 条。
- **未知类型照 vanilla 静默 null**：`include.unknown.datatypes`（默认 false）为 false 时未知类型返回 null 不抛，true 时照发原串。
- **LogicalMsg 解析但不发射**：`slot.messages=true` 只开解析记录与前沿安全推进，逻辑消息不进 Kafka topic（发射仍延期，专属 topic `.message` 后缀等设计要点在 R1/R3 审计第 3 条）。
- **无快照数据抽取**：`snapshot.mode` 仅 `no_data`，初始存量/按需快照请用 vanilla `postgresql-connector`。
- **增量快照不接**（MS6 裁定，2026-09-05）：v1 不接 signal-based 增量快照——vanilla 形态的三个交错面（第二 dispatch 线程/main 连接时序独占消失/offset 与前沿交互）各打破一处既有契约，接入需先满足前置条件五项路线图，见 `docs/superpowers/specs/2026-09-05-ms5-r2-incremental-snapshot-audit.md`。
- 其余已知限制与延期（Truncate 选项位协议层超集、`skipped.operations` 偏差等）统一记档于 `docs/superpowers/specs/2026-09-02-ms2-r1-r3-audit.md`「已知限制与延期」节。
