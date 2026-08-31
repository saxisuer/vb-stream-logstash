# 2026-09-01 · vb-stream-logstash 多模块化 + debezium-connector-postgres-stream 设计

## 1. 背景与目标

vb-stream-logstash 已在自有冒烟入口(Main)完成 PostgreSQL 逻辑解码 stream 模式的完整适配(里程碑 2.0:双线程解耦、Chronicle Queue 主缓冲、流式输出契约、吞吐指标、177 测试)。本设计将同一能力以 **Debezium 连接器**形态产品化:进入 Kafka Connect 生态,复用 Debezium 的快照/offset 存储/事务元数据/指标框架。

**Debezium 现状调研结论**(2026-09-01,三路并行探索 Debezium main @ `bffe031a25`,3.7.0-SNAPSHOT,pgjdbc 42.7.13 与本项目同版本):

- 对流式事务支持为**零**:`PgOutputMessageDecoder.defaultOptions` 硬编码 `proto_version=1`,无 `streaming`/`two_phase` 选项;`MessageType.forType` 遇 'S'/'E'/'c'/'a'/两阶段消息直接抛 `IllegalArgumentException`
- 客户端无进行中事务缓冲(大事务由 PG 服务端 reorder buffer 缓冲,Commit 后整段下发);唯一客户端缓冲是纯内存有界 `ChangeEventQueue`(满则阻塞读循环)
- `PostgresSchema` 是 relationId 单版本 HashMap,同事务 DDL 正确性靠"单线程时序副作用"
- 现成挂点:`PostgresStreamingChangeEventSource` 已是 `readPending()` drain 轮询;offset 的 `lsn_commit` 已锚 COMMIT 边界;`TransactionMonitor` 事务元数据齐备;**chronicle-queue 已在 debezium-bom 管理**(2026.2,`debezium-storage-chronicle-queue` 模块先例)——CQ 依赖零许可/版本障碍
- `slot.stream.params` 用户注入通道存在,但解码器默认值在用户参数之后应用(`Properties.setProperty` 后者胜),`proto_version` 会被硬编码 1 反向覆盖——现有链路无法开启流式

## 2. 决策记录(已与用户逐项确认)

| # | 决策 | 内容 |
|---|---|---|
| D1 | 仓库结构 | vb-stream-logstash 改多模块:现有代码入 `vb-stream-engine`,新连接器为 `vb-stream-connector-postgres-stream`,**同仓**(不放 Debezium fork) |
| D2 | 复用模型 | **只复用思想,Debezium 风格重写**:connector 模块不 import 引擎任何类,同仓两套同逻辑代码各自演进(接受双维护代价) |
| D3 | 插件形态 | 新 Connector(包名 `org.vastdata.debezium.connector.postgresql.stream`,与 `io.debezium.*` 隔离,插件并存零类冲突);Maven 依赖 `debezium-connector-postgres` 复用其 Config/Schema/Emitter/快照/offset 体系,零侵入原模块 |
| D4 | 范围 | 全量一次做齐:含 two_phase、streaming=parallel、吞吐指标 |
| D5 | 线程模型 | 双自建线程(reader/consumer)+ coordinator 监督——最贴近引擎 Main 的结构;集成面(isRunning/心跳/停机握手)自行接线 |
| D6 | Debezium 依赖 | Maven Central 稳定版(最新 3.x,实施时定号;调研基线为 3.7.0-SNAPSHOT main,所引 API 均为长期稳定面) |
| D7 | 停机语义 | **不排干 CQ**:半事务(End 未达)前沿不推进,PG 重启重发整事务,已发射残段由下游靠事务元数据(BEGIN 无 END)过滤——at-least-once,快速停机 |

## 3. 模块结构

```
vb-stream-logstash/                          ← parent pom(packaging=pom,坐标不变 org.vastdata:vb-stream-logstash:1.0-SNAPSHOT)
├─ pom.xml                                   ← 聚合 + 共享属性/插件管理(Java 17、surefire argLine 等)
├─ vb-stream-engine/                         ← 现有代码全量迁入(git mv 保历史)
│   ├─ pom.xml                               ← artifactId vb-stream-engine
│   └─ src/{main,test,jmh}/java/...          ← protocol / replication / Main / ConsoleRenderer / it / bench 原样
├─ vb-stream-connector-postgres-stream/      ← 新模块(本设计主体)
│   ├─ pom.xml                               ← 依赖 io.debezium:debezium-connector-postgres(D6)+ chronicle-queue(BOM 同源或显式 2026.x);test 域另依赖 debezium-embedded 驱动 IT(见 §9)
│   └─ src/{main,test}/java/org/vastdata/debezium/connector/postgresql/stream/...
├─ src/docker/                               ← 留根,两模块共用冒烟 PG(postgres:18)
├─ docs/                                     ← 留根共享
└─ CLAUDE.md                                 ← 根,更新源码结构说明;两模块各自维护包级 CLAUDE.md
```

迁移验收:引擎 177 测试全绿、原 Main 冒烟路径可跑(classpath 变为 `vb-stream-engine/target/classes`)、`-Pjmh` 档不破。

## 4. 架构总览

```
Kafka Connect worker(worker 线程 poll ChangeEventQueue;offset 按 poll 批次提交)
▲
│ ChangeEventQueue(有界内存队列,Debezium 既有——这里是"阻塞"背压点,但只作用于 consumer)
│
change-event-source-coordinator(Debezium 框架线程,监督壳)
│  execute(): 起 reader/consumer 两线程 → await 停机信号 → 编排停机次序(见 §6)
│
├─ vb-pgoutput-reader(自建)
│    RawReplicationSession(自建 pgjdbc 复制会话,proto_version=4 + streaming + two_phase)
│    readPending drain 轮询(空轮睡 poll.interval.ms)→ raw 字节 append 进 CQ
│    桶记账(I/U/D/T 只窥前缀与 oid 记 index 段,解码推迟)
│    'R' 时 JDBC 元数据查询构 Debezium Table 入版本日志(仅 DDL 时,罕见)
│    空轮发心跳;LSN 确认 = min(已收到, 输出前沿)
│
│              [Chronicle Queue 管道——reader 永不因下游慢而停,代价转移到磁盘]
│
└─ vb-transaction-consumer(自建)
     交接队列取桶 → 逐段 readRange 回读 → 解码单条 → asOf 版本 Table 渲染
     → dispatcher.dispatchDataChangeEvent → ChangeEventQueue → Connect poll
     End 处理完毕推进前沿(offsetContext.updateCommitPosition + 前沿 AtomicLong ← endLsn)

快照阶段(复用 PostgresSnapshotChangeEventSource,JDBC 一致性快照)→ 交接流式起点
```

## 5. 组件设计

### 5.1 连接器装配

`PostgresStreamConnector` / `PostgresStreamConnectorTask`。配置类继承 `PostgresConnectorConfig`(复用全部既有配置面:JDBC、publication、heartbeat、snapshot、max.queue.size 等),追加流式专属项:

| 配置 | 默认 | 语义 |
|---|---|---|
| `slot.streaming` | `on` | `on`/`parallel`/`off`;`parallel` 要求 `slot.two.phase=true` 否则启动 fail-fast;`off` 时大事务走服务端缓冲、提交后整段下发,桶管道照常工作(兼容模式) |
| `slot.two.phase` | `true` | 建槽即带 `two_phase`(PG 不允许后改);已有不带此参数的槽启动期报错 |
| `pipe.dir` | `pg-stream-pipe-queue` | CQ 目录,瞬态工作区,wipe-on-open,gitignore |
| `pipe.roll.cycle` | `MINUTELY` | 滚动周期(LegacyRollCycles 枚举名) |

`provide.transaction.metadata` 默认覆写为 **true**(D7 停机语义下,下游过滤半事务依赖它;与 Debezium 原生默认 false 不同,文档标注)。

### 5.2 复制会话(自建,弃用 PostgresReplicationConnection)

**弃用依据**(四条,实施与评审时不得回退为"复用它"):

1. 解码器路由是封闭枚举(`LogicalDecoder` 仅 PGOUTPUT/DECODERBUFS)、构造期绑定(`PostgresReplicationConnection` 构造函数内 `plugin.messageDecoder(...)`),Builder 链不暴露注入点——插入自定义解码器必须改 connector-postgres 源码,违背 D3 零侵入
2. `readPending` 回调链内嵌 `messageDecoder.processMessage(buffer,...)`,ByteBuffer 生命周期仅限回调内——raw 字节拿不出来,透传解码器又撞回 1
3. 槽选项控制权在解码器 `defaultOptions` 且应用次序在用户参数之后(`Properties.setProperty` 后者胜)——`proto_version=4 + streaming + two_phase` 的控制权不在自己手里
4. flush 语义走 `LsnFlushMode` 门控(锚"Connect 已提交 offset"),与我们的 `min(已收到,前沿)` 周期回传 + End 锚定不同构

自建范围:`RawReplicationSession` 直连 pgjdbc `ReplicationConnection`,槽选项拼装、`readPending()` drain 轮询、keep-alive/statusInterval 线程、`setFlushedLSN/setAppliedLSN` 只前进不后退——模板即引擎 `PgReplicationSession` 已验证逻辑(D2:重写,非移植)。JDBC 侧(`PostgresConnection`:元数据/快照)照旧复用。

### 5.3 CQ 管道与桶记账

- `MessagePipe`:CQ 封装,一条记录 = 一条完整 raw 消息;wipe-on-open(残留陈旧 index 会让回读错位);配置见 5.1
- 桶记账(reader 线程,组装期堆内零字节引用):每条消息先 `pipe.append` 取 CQ index 作 seq → 按类型字节路由:控制消息 live 解码;I/U/D/T 只窥前缀与 oid 记 index 段入桶;`abortedSubxids` 集合(StreamAbort 标记的子事务,回放期剔除)
- 交接(Commit / StreamCommit / CommitPrepared 到达):拷 Relation 版本快照 → 冻结桶 → 入交接队列 → 立即返回,reader 不停

### 5.4 版本化 Schema 注册表

新 `VersionedSchemaRegistry`:relationId → 有序版本列表(每版本 = Debezium `Table`,含 JDBC enrich 的列精度/可空性等——pgoutput 'R' 消息只有列名+oid,记录保真度要求 JDBC 补齐,查询发生在 reader 线程 'R' 时,DDL 罕见可接受)。交接时按桶做 asOf 快照;回放按 asOf 取版本,同事务 DDL 前后段各自正确。低水位 `pruneBelow` 按存活桶 firstIndex(两阶段挂起桶算存活、已交接桶不算)。**原 `PostgresSchema` 仅供快照阶段使用**,流式阶段走版本日志,双轨不干扰。

### 5.5 回放与发射

consumer 线程:交接队列取桶 → 逐段 `readRange` 回读 → 解码单条 → asOf Table 渲染 → `dispatcher.dispatchDataChangeEvent(partition, tableId, emitter)` 逐条发射 → End 处理完毕推进前沿。回放路径无跨单元累积容器,堆峰 O(单条)。

### 5.6 offset/LSN 语义

- **per-record offset 统一事务边界**:回放发射的每条记录 source offset map 写所属事务 end LSN(`lsn` = `lsn_commit` 同值,`xmin` 周期值)——事务内所有记录共享同值,Connect 无论哪个批次提交 offset 都落在事务边界,**不存在事务中段位点**。由此弃用 vanilla 的 `WalPositionLocator` 重启搜索与 `lsn_events_processed` 同 LSN 计数跳过(其存在前提"未提交事务的事件可能已被发出"在桶模型下消失),重启不做事务内去重
- **前沿双层**:consumer End 处理完 → `updateCommitPosition(endLsn)` + 前沿 AtomicLong ← endLsn;reader 周期回传 confirmed = `min(已收到,前沿)`;Connect `performCommit → commitOffset → flushLsn` 回调链保留,flush 走自建会话(只前进不后退),值与前沿一致
- **重启三情况**:①半事务(End 未达):前沿没推进 → PG 重发整事务,下游过滤 BEGIN-无-END 残段;②Connect offset 落后前沿:重发已输出事务 → 头部重复,at-least-once 文档化(不去重);③一致:无缝续传
- 心跳:空轮经 dispatcher 发(调用线程审计见 §8-R1);xmin 周期拉取照旧(reader 线程 JDBC 查 slot xmin)

### 5.7 two_phase 与 streaming=parallel

- `preparedByGid` 挂起池:Prepare('P')→ 桶入池;CommitPrepared('K')→ 交接回放;RollbackPrepared('r')→ 弃桶
- 挂起桶算存活:`pruneBelow` 不越过;长期挂起 → WAL 保留增长,consumer 周期 WARN + `max_slot_wal_keep_size` 兜底
- 建槽即带 `two_phase=true`(自建会话 ensureSlot 负责,含 streaming 选项)
- 重启 prepared:confirmed_flush 未过 CommitPrepared 位点 → 重放范围重新收到 PREPARE + CommitPrepared,语义自洽
- `streaming=parallel`:要求 two_phase + proto v4,启动校验 fail-fast;客户端消息形态不变(流式块内 xid 前缀解析统一覆盖 on/parallel)

### 5.8 事务元数据

`TransactionMonitor` 复用:Begin → `dispatchTransactionStartedEvent`,End → `dispatchTransactionCommittedEvent`,逐条数据经 `dataEvent` 内嵌事务块——与 Debezium 原生事务 topic 结构逐字段一致。默认开启(5.1)。

### 5.9 指标

- 复刻引擎 `ThroughputMetrics` 全套语义:三段速率(spot 读取/组装/输出)+ 回放耗时/事务大小分位数 + 八项会话峰值行;10s 周期 INFO 日志行与引擎同构(便于与现有基线对照)
- 关键运行态暴露进 Debezium MBean 体系:三段速率、`lastReceived − frontier` 滞后、挂起 prepared 数、CQ 目录磁盘占用(命名实施时对齐 `PostgresStreamingMetrics` 风格)
- 记账分工:reader 记读取/组装,consumer 记输出/分布/峰值,tick 挂 consumer

### 5.10 快照衔接

快照阶段整套复用 `PostgresSnapshotChangeEventSource`(coordinator 框架编排)。流式起点沿用 vanilla 三优先级:新建槽 → 槽创建一致性位点;已有槽 → `slotLastFlushedLsn`;兜底 → `currentXLogLocation`(由自建会话 ensureSlot/start 承接)。重启场景按 §5.6 走 confirmed_flush。预快照 catch-up(`streamingStoppingLsn`)接口约定照搬。

## 6. 错误处理与停机

- 回放失败(decode 等)→ fail-fast:经 `ChangeSourceContext` 上报,任务失败、保留槽位,由 Connect 框架重试(Debezium 惯例)
- 复制流中断 → 同路径(vanilla 靠 Connect 重启任务,不自建倒计时)
- **停机次序(D7,不排干)**:coordinator 收到停机 → 停复制会话(reader 退出)→ consumer 停止(不等桶回放完)→ 关管道(下次启动 wipe-on-open)
- consumer 慢 → CQ 目录与 PG WAL 保留增长:周期 WARN + `max_slot_wal_keep_size` 兜底,不加客户端硬上限

## 7. 堆有界性承诺(与引擎 2.0 同构)

组装期堆内零字节引用(数据只在 `pipe.append` 落盘一次,桶只记 index 段 + oid/aborted 集合);回放期堆峰 O(单条);随会话增长的堆结构仅:`abortedSubxids`(桶完结释放)、`preparedByGid`(未决 2PC 数,协议固有)、交接队列与 `handedOff` 记账(待输出桶数 × 元数据,DONE 惰性清理)、registry 版本日志(剪枝后随不同表 oid 数线性)。consumer 慢/停摆不回压 reader,代价转移到磁盘。

## 8. 风险清单(实施计划逐项落任务)

| # | 风险 | 处置 |
|---|---|---|
| R1 | dispatcher 线程安全:dispatch 从 consumer 线程发起,心跳/信号等旁路事件调用线程需改道或验证无共享可变态 | MS2 专项审计 |
| R2 | 增量快照交错:信号表读取/chunk 发射与流式 dispatch 共享 dispatcher 的线程交错 | MS5 独立验证任务 |
| R3 | 'R' 的 JDBC 元数据查询在 reader 线程,与快照/信号共享连接需串行化 | 同步锁或独立短连接(MS2) |
| R4 | Connect 插件隔离:两连接器并存时新插件打包需内嵌 connector-postgres 及其依赖 | MS6 打包验证 |
| R5 | `two_phase` 建槽参数不可后改——存量槽迁移报错文案 | MS4 |

## 9. 测试策略(三层)

1. **字节级单测**(不起 PG):MsgBuilder/PgWire 式手造字节辅助按 Debezium 侧重写(现有先例仅反射+ByteBuffer 孤例,建正式 fixture);协议解码(流式块 xid 前缀、两阶段消息)、桶记账/交接/aborted 剔除/registry asOf/prune 离线可测
2. **Testcontainers 集成测试**——以 **Debezium Embedded Engine 驱动**(参照 vanilla PG IT 范式:`debezium-embedded` 的 `DebeziumEngine<SourceRecord>` 在测试 JVM 内起连接器,`AbstractConnectorTest` 系基类 + `consumeRecordsByTopic(N).allRecordsInOrder()` 断言;容器 postgres:18,`logical_decoding_work_mem=64kB` + `max_prepared_transactions=16`)。场景库从引擎 it 整体翻译重写:
   - 双连接并发流式大事务多桶交错 + StreamAbort 子事务剔除
   - 大事务内同事务 DDL,前后段 asOf 渲染
   - 流式大事务回滚后低水位推进 + 陈旧滚动文件删档
   - reader 不阻塞验收(consumer 阻塞期间 reader 持续接收)
   - frontier 封顶验收(未输出事务钉住 confirmed_flush,输出后越过)
   - 读取节拍回归(防"每轮一条+固定 sleep"退化)
   - 半事务停机语义:不排干停机 → 重启重发 → 事务元数据 END 补齐
   - two_phase:挂起→交接/弃桶/重启续传 prepared
   - 快照 → 流式无缝衔接
   - **重启类场景统一走 embedded engine 的停止/重启**:engine 配文件 offset storage(`offset.storage.file.filename`),停引擎→断言半事务残段→重起→断言 PG 重发补齐与事务元数据 END——重启三情况、半事务停机语义、two_phase prepared 续传共用此骨架
   - 流式数据构造经验照搬(不可压缩载荷 ~16KB md5 串、阈值是全局 `rb->size`、分批跨秒写入)
3. **基准对照**(可选):引擎端到端吞吐基线(窄行 ~34 万条/s / 宽行 ~320MB/s)作回归参照

## 10. 里程碑(每期 commit + push)

| 期 | 内容 | 验收 |
|---|---|---|
| MS0 | 仓库多模块化:parent pom + `vb-stream-engine` 迁移(git mv 保历史)+ connector 空模块骨架 | 引擎 177 测试全绿;Main 冒烟可跑;`-Pjmh` 不破 |
| MS1 | connector 协议层:Maven 模块、Connector/Task/Config、字节级解码器(含流式/两阶段) | 单测全绿,不起 PG |
| MS2 | 管道与会话:自建复制会话(raw drain + LSN 回传)、CQ 管道、桶记账、双线程、回放 → dispatcher、End 锚定 offset;R1/R3 审计 | 流式大事务端到端进 Kafka 记录 |
| MS3 | 语义闭环:重启三情况、frontier 封顶、半事务停机语义、reader 不阻塞、aborted 过滤、DDL asOf | 对应 IT 全绿 |
| MS4 | two_phase + parallel:挂起池、建槽参数、重启 prepared 续传、parallel 校验;R5 | 两阶段 IT 全绿 |
| MS5 | 集成面:快照衔接、事务元数据默认开启、指标(日志行 + MBean)、R2 增量快照专项 | 快照→流式 IT + 审计结论入档 |
| MS6 | 打包与文档:插件打包(R4)、配置文档、at-least-once/停机语义文档 | 插件可装进 Connect 运行 |

## 11. 实施期决定项(不阻塞本设计)

- Debezium 稳定版具体版本号(MS1 定,Central 最新 3.x)
- MBean 指标命名细节(MS5)
- 插件打包形态(fat-jar 内嵌 vs plugin 目录清单)(MS6)
