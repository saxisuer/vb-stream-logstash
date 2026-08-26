# pgoutput 流式解码器设计（里程碑 1）

日期：2026-08-26
状态：已与用户逐节确认

## 1. 背景与目标

vb-stream-logstash 的最终目标是适配 PostgreSQL 最新逻辑解码 stream 模式的 CDC 管道。本里程碑（里程碑 1）聚焦：**通过 pgjdbc 的 ReplicationConnection 建立复制流，自研解析 pgoutput 二进制协议，完整适配四种事务场景**：

1. 普通事务（proto_version=1 起即有）
2. 流式大事务（proto_version=2，`streaming=on`）
3. 两阶段提交（proto_version=3，`two_phase=on`）
4. 并行流式（proto_version=4，`streaming=parallel`）

交付物：一个可运行的 `Main`，连接 PG 18 复制流，把解析出的所有消息以可读格式打印到控制台；配套单测（纯 JVM）与集成测试（Testcontainers）。

## 2. 已确认的决策

| 决策点 | 结论 |
|---|---|
| 输出目的地 | 仅控制台打印；Chronicle Queue 接入留到里程碑 2 |
| 集成测试基础设施 | Testcontainers 独立实例（自行配置 wal_level、账号、max_prepared_transactions 等），与 src/docker compose 环境解耦 |
| 场景验证策略 | 单流全开：`proto_version=4` + `streaming=parallel` + `two_phase=on`，四个测试用例分别构造四种事务 |
| 断线重连 | 不含。连接断开即退出（exit 1），复制槽保留位点，重启 Main 续传 |
| 代码组织 | 方案 A：三层结构 protocol / replication / Main |

Main 运行时的默认连接目标为 src/docker 的 compose 环境（localhost:55432，postgres/postgres，db postgres，publication vb_pub），全部可被 system property 覆盖。

## 3. 架构与数据流

```
                          控制台（本里程碑）
                              ▲
PG 18 ──复制流──▶ replication/PgReplicationSession ──▶ Main(ConsoleListener)
 (pgoutput        (建槽/开流/LSN反馈/Relation缓存)        │
  v4 全开)                     │ ByteBuffer payload        ▼
                              ▼                     里程碑 2 可换
                    protocol/PgOutputDecoder ──▶ PgOutputMessage   Chronicle listener
                    （纯函数，无 IO，可单测）        （不可变 record）
```

| 模块 | 职责 | 明确不做 |
|---|---|---|
| `protocol/` | `ByteBuffer → PgOutputMessage`，覆盖全部消息类型 | 任何 IO、任何状态 |
| `replication/` | 幂等建槽、复制连接、开流参数、Relation/Type 元数据缓存、LSN 周期反馈、消息循环 | 解析字节、业务处理 |
| `Main` | 读配置（system property）、组装、打印、Ctrl+C 优雅关闭 | 协议/连接细节 |

包路径：`org.vastdata.vbstream`。

## 4. 协议数据模型（protocol/ 包）

> 本节字段与字节格式已按 PostgreSQL 18 官方文档 54.9（Logical Replication Message Formats）与 PG 18 源码 `src/include/replication/logicalproto.h` 双源核对。

`sealed interface PgOutputMessage`，四个消息族：

```java
// 族 1：普通事务（v1）
record BeginMessage(long finalLsn, Instant commitTimestamp, long xid)
record CommitMessage(long commitLsn, long endLsn, Instant commitTimestamp)   // I8(0) flags 解析时消费，不建模
record OriginMessage(long originCommitLsn, String originName)               // 级联复制场景出现
record InsertMessage(OptionalLong streamXid, int relationOid, TupleData newTuple)
record UpdateMessage(OptionalLong streamXid, int relationOid,
                     Optional<TupleData> oldTuple, TupleData newTuple)
record DeleteMessage(OptionalLong streamXid, int relationOid, TupleData oldTuple)
record TruncateMessage(OptionalLong streamXid, EnumSet<TruncateOption> options, int[] relationOids)
record LogicalMessage(OptionalLong streamXid, boolean transactional, long lsn,
                      String prefix, byte[] content)

// 族 2：元数据（消息流中随时插入）
record RelationMessage(OptionalLong streamXid, int relationOid, String schema, String table,
                       char replicaIdentity, List<Column> columns)
record Column(String name, int typeId, int typeModifier, boolean partOfKey) // typmod 为 PG 18 新增字段；flag 语义=属于复制键
record TypeMessage(OptionalLong streamXid, int typeOid, String schema, String name)

// 族 3：流式大事务（v2，streaming=on/parallel；类型字节 'S'/'E'/'c'/'A'）
record StreamStartMessage(long xid, boolean firstSegment)
record StreamStopMessage()
record StreamCommitMessage(long xid, long commitLsn, long endLsn, Instant commitTimestamp)
record StreamAbortMessage(long xid, long subxid, OptionalLong abortLsn, OptionalLong abortTimestamp)
// abortLsn/abortTimestamp 仅 streaming=parallel 时随消息附加（源码 write_abort_info=true），否则为 empty

// 族 4：两阶段（v3，two_phase=on；类型字节 'b'/'P'/'K'/'r'/'p'）
record BeginPrepareMessage(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid)
record PrepareMessage(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid)
record CommitPreparedMessage(long commitLsn, long endLsn, Instant commitTimestamp, long xid, String gid)
record RollbackPreparedMessage(long prepareEndLsn, long rollbackEndLsn, Instant prepareTimestamp,
                               Instant rollbackTimestamp, long xid, String gid)
record StreamPrepareMessage(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid)
```

**streamXid 的含义（关键协议事实）**：M/R/Y/I/U/D/T 这些消息在**流式块内**（Stream Start 与 Stream Stop 之间）时会**前置一个 Int32 的 xid 字段**，顶层（非流式）消息没有该前缀。因此解码器必须跟踪流块状态。

列值模型（`sealed interface TupleValue`）：

- `NullValue` — 'n'
- `UnchangedToastValue` — 'u'（TOAST 未变列，流式大事务高频出现，**必须显式建模**）
- `TextValue(String)` — 't'（本里程碑列值统一用 text 格式）
- `BinaryValue(byte[])` — 'b'（占位，不展开解码）

Tuple 前缀语义：`'N'` 新值 / `'K'` 复制键 / `'O'` 旧完整行，分别进入 `Insert/Update/Delete` 的对应字段；`UpdateMessage.oldTuple` 为 `Optional`（仅 REPLICA IDENTITY FULL / 含旧值场景出现）。

**实现纪律**：

1. 每条消息的字段序列严格按附录 A 的字节格式表实现（来源：官方文档 54.9 + PG 18 源码 logicalproto.h 双源核对）
2. `PgOutputDecoder` 构造时携带 `StreamingMode`（OFF/ON/PARALLEL）；运行期维护**最小流块状态** `inStream`（收到 Stream Start 置位、Stream Stop 复位）——`inStream=true` 时 M/R/Y/I/U/D/T 先读 Int32 xid 前缀；`streaming=parallel` 时 Stream Abort 额外读 Int64 abort_lsn + Int64 abort_time。除此之外无其他状态
3. 解码器遇未知类型字节或字段错位必须 fail-fast 抛 `UnknownMessageTypeException`（带字节值与 hex 上下文），绝不静默跳过——错读一个字节会导致后续全部消息错位
4. 各消息中的 `Int8(0)` currently-unused flags 字段（Commit/Prepare/CommitPrepared/RollbackPrepared/StreamPrepare/StreamCommit 均有）解析时**必须消费但不建模**

## 附录 A：pgoutput 消息字节格式表（PG 18）

类型字节后按序读取；Int 均为 big-endian；String 为 null 结尾 UTF-8（CString）；时间戳为距 PG epoch（2000-01-01，Unix epoch + 946684800 秒）的微秒数。

| 类型字节 | 消息 | 字段序列（类型字节之后） |
|---|---|---|
| 'B' | Begin | I64 final_lsn; I64 commit_ts; I32 xid |
| 'C' | Commit | I8 flags(0); I64 commit_lsn; I64 end_lsn; I64 commit_ts |
| 'O' | Origin | I64 origin_commit_lsn; Str origin_name |
| 'R' | Relation | [流内: I32 xid]; I32 oid; Str schema; Str table; I8 replident; I16 ncols; cols×{ I8 flags(1=key); Str name; I32 type_oid; I32 typmod } |
| 'Y' | Type | [流内: I32 xid]; I32 type_oid; Str schema; Str name |
| 'I' | Insert | [流内: I32 xid]; I32 oid; 'N'; TupleData |
| 'U' | Update | [流内: I32 xid]; I32 oid; [ 'K' TupleData \| 'O' TupleData ]; 'N'; TupleData |
| 'D' | Delete | [流内: I32 xid]; I32 oid; ('K'\|'O'); TupleData |
| 'T' | Truncate | [流内: I32 xid]; I32 nrel; I8 opts(1=CASCADE,2=RESTART); nrel×I32 oid |
| 'M' | Message | [流内: I32 xid]; I8 flags(1=transactional); I64 lsn; Str prefix; I32 content_len; bytes |
| 'S' | Stream Start | I32 xid; I8 first_segment |
| 'E' | Stream Stop | （无字段） |
| 'c' | Stream Commit | I32 xid; I8 flags(0); I64 commit_lsn; I64 end_lsn; I64 commit_ts |
| 'A' | Stream Abort | I32 xid; I32 subxid; [仅 parallel: I64 abort_lsn; I64 abort_time] |
| 'b' | Begin Prepare | I64 prepare_lsn; I64 end_lsn; I64 prepare_ts; I32 xid; Str gid |
| 'P' | Prepare | I8 flags(0); I64 prepare_lsn; I64 end_lsn; I64 prepare_ts; I32 xid; Str gid |
| 'K' | Commit Prepared | I8 flags(0); I64 commit_lsn; I64 end_lsn; I64 commit_ts; I32 xid; Str gid |
| 'r' | Rollback Prepared | I8 flags(0); I64 prepare_end_lsn; I64 rollback_end_lsn; I64 prepare_ts; I64 rollback_ts; I32 xid; Str gid |
| 'p' | Stream Prepare | I8 flags(0); I64 prepare_lsn; I64 end_lsn; I64 prepare_ts; I32 xid; Str gid |

TupleData：`I16 ncols;` 每列 `'n'`（NULL，无负载）`| 'u'`（TOAST 未变，无负载）`| 't' I32 len bytes`（text）`| 'b' I32 len bytes`（binary）。

## 5. 会话层（replication/ 包）

**`ReplicationConfig`**（record）：

- 连接：host、port、database、user、password
- 复制：slotName、publicationNames（逗号分隔）、protoVersion（默认 4）、streamingMode（默认 `parallel`）、twoPhase（默认 true）、feedbackIntervalSeconds（默认 10）
- 工厂方法从 system property 读取，前缀 `vb.pg.`（如 `-Dvb.pg.host=...`、`-Dvb.pg.slot=...`）

**`PgReplicationSession`** 生命周期：

1. `open()` — `jdbc:postgresql://host:port/db?replication=database` 建复制连接
2. `ensureSlot()` — 幂等建槽：`SELECT pg_create_logical_replication_slot(:slot, 'pgoutput', false, true)`（末参 two_phase=true）；槽已存在（duplicate slot）则 warn 后复用
3. `start()` — `connection.unwrap(PGConnection.class).getReplicationAPI().createReplicationStream()` 携 `withSlotOption("proto_version","4")`、`withSlotOption("publication_names", ...)`、`withSlotOption("streaming","parallel")`、`withSlotOption("two_phase","on")`、`withStatusInterval(...)` 后 `start()`
4. `run(listener)` — 循环：`stream.read()` 阻塞取消息 → payload 交按 `config.streamingMode` 构造的 `PgOutputDecoder.decode()` → 回调 `listener.onMessage(msg)`；每条消息处理后 `setAppliedLSN/setFlushedLSN(stream.getLastReceiveLSN())`，按 `feedbackInterval` 周期 `forceStatusUpdate()`
5. `close()` — 关流、关复制连接（shutdown hook 调用）

**`RelationRegistry`**：`oid → RelationMessage` 缓存（`ConcurrentHashMap`）。Relation 消息在流式块内会按块重复下发，统一进缓存；`TypeMessage` 同理按 oid 缓存。listener 打印 I/U/D 时查表把 oid 翻译为 `schema.table` 并对齐列名。缓存查找 miss 属于协议异常（Relation 必先于 DML 出现），fail-fast。

**Listener 契约**：`interface PgOutputListener { void onMessage(PgOutputMessage msg, RelationRegistry registry); }`——里程碑 2 换 Chronicle writer 时实现同一接口即可，协议层与会话层零改动。

## 6. Main

- 读取 `ReplicationConfig`（默认值对准 compose 环境）→ 打印启动摘要
- `session.open() → ensureSlot() → start() → run(new ConsoleListener(...))`
- `ConsoleListener`：每条消息一行可读输出：`时间戳 [消息族标签] 解读内容`（DML 打印 `schema.table`、列名=值、事务号、LSN）
- `Runtime.addShutdownHook` 调 `session.close()`；主线程 `CountDownLatch.await()`；Ctrl+C 优雅退出

## 7. 错误处理

| 情形 | 行为 |
|---|---|
| 未知消息类型字节 / 字段错位 | `UnknownMessageTypeException` fail-fast（带字节值与 hex 上下文） |
| 连接中断（SQLException/IOException） | 包装为 `PgStreamException`；Main 打印「槽 X 已保留，最后 LSN Y，重启即续传」→ exit 1 |
| 建槽冲突（槽已存在） | warn + 复用现有槽 |
| 配置校验失败 | 打印 usage → exit 2 |

## 8. 测试矩阵

### 8.1 单元测试（protocol/，纯 JVM、无 PG）

- 测试内小工具手工拼 `ByteBuffer` 样本（`buildMessage(type, fields...)` 风格）
- 断言：解析出的 record 各字段值正确，**且 ByteBuffer 剩余字节恰好为 0**（抓错位最锋利的断言）
- 覆盖：四族消息正常样本；列值四种变体（null / unchanged-toast / text / binary）；parallel 模式附加 flags 变体；消息体内含空字符串/CJK 字符的边界样本

### 8.2 集成测试（Testcontainers，postgres:18）

容器启动参数：`wal_level=logical`、`max_replication_slots=16`、`max_wal_senders=16`、`max_prepared_transactions=16`（2PC 必需，默认 0）、`logical_decoding_work_mem=64kB`（压低阈值让中等事务即可触发流式）。

单例 static 容器（随机端口）+ 测试内两个连接：普通 JDBC 造数据、复制流消费（独立线程 + `CountDownLatch` 等待期望消息）。每用例独立槽名（UUID），`@AfterEach` drop slot。

| # | 用例 | 构造 | 断言 |
|---|---|---|---|
| 1 | 普通事务 | 小事务 INSERT/UPDATE/DELETE/COMMIT | `B→R→I/U/D→C` 完整序列，行数据正确 |
| 2 | 流式大事务 | 单事务插入数千行（超 64kB work_mem） | `'S'(firstSegment=true)→R→I×N→'E'` 块重复出现，最终 `'c'`(StreamCommit)，其后无 `'C'`；流块内 R/I 带 streamXid |
| 3 | 两阶段提交 | `PREPARE TRANSACTION 'gid'` → `COMMIT PREPARED` | `'b'→变更→'P'`，随后（流式 2PC 场景为 `'p'` StreamPrepare）`'K'` CommitPrepared，gid 匹配 |
| 4 | 两阶段回滚 | `PREPARE TRANSACTION 'gid'` → `ROLLBACK PREPARED` | `'b'→'P'` 后 `'r'` RollbackPrepared，gid 匹配 |
| 5 | 并行流式 | streaming=parallel 下重复用例 2 | 含附加 flags 的消息全部正确解析、序列无错位 |
| 6 | LSN 反馈 | 处理若干消息后 `forceStatusUpdate()` | `pg_replication_slots.confirmed_flush_lsn` 前进 |
| 7 | Truncate | `TRUNCATE`（含 CASCADE / RESTART IDENTITY 变体） | `T` 消息选项位与 oid 列表正确 |

新增测试依赖：`org.testcontainers:postgresql`、`org.testcontainers:junit-jupiter`（版本实现时经 Maven Central metadata 查证）。

## 9. 非目标（本里程碑明确不做）

- Chronicle Queue 写入（里程碑 2，通过实现 `PgOutputListener` 接入）
- 断线自动重连 / 指数退避（后续里程碑；槽位天然支持续传）
- binary 列值（publication `binary=true`）的语义解码——模型占位即可
- 与 Logstash 的集成
- 多 publication、DDL 变更、pg_logical_emit_message 的深度处理（LogicalMessage 仅解析打印）

## 10. 多 agent 协作提示

协议层按消息族天然分片（族 1+2 / 族 3 / 族 4），各 agent 只写自己的消息 record + 解析分支 + 单测，互不触碰同一文件；会话层、Main、集成测试为独立任务。实施计划（writing-plans 阶段产出）将据此拆分任务与依赖顺序。
