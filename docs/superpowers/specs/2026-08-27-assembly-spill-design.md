# 事务组装缓冲溢写 Chronicle Queue 设计（里程碑 1.6）

## 1. 背景与目标

里程碑 1.5 的 `TransactionAssembler` 把事务变更缓冲为堆内 `TxChange` 对象列表。流式大事务（PG 侧 reorder buffer 驱逐后逐段下发）的变更量没有上限——组装器堆占用随事务大小线性增长，一个 100GB 事务就能拖垮进程。

**目标：组装缓冲内存有界化。** 小事务纯内存组装（快路径零额外成本）；大事务越过阈值后溢写（spill）到 Chronicle Queue（CQ），堆里只留记账元数据；两种模式输出严格等价。

### 1.1 调研结论：不需要 protostuff（或任何新序列化组件）

本设计起点是"引入 protostuff 做序列化"，调研（基于项目在用的 chronicle-queue 2026.6 / chronicle-wire 2026.8 jar 的 javap 实证）推翻了这一前提：

- **protostuff-runtime 与 chronicle-wire 泛型反射都不支持 Java record**（需可变字段 + 空构造器；protostuff GitHub issue #313，1.8.0 起项目休眠无修复指望；protostuff `Schema.mergeFrom(Input, T)` 返回 void，填充模式天生排斥不可变对象）
- **`ExcerptAppender.writeBytes(BytesStore)` 是 CQ 现成入口**——纯字节写入，最快路径
- 关键巧合：`PgOutputMessage` 本就是 pgoutput 线格式的 1:1 解析结果，**"反序列化器"就是现有的 `PgOutputDecoder`**。溢写原始字节 = 永远不需要"对象→字节"编码器，无编解码漂移风险，保真度由构造保证

若未来 Logstash 集成需要自定义线格式，届时再评估 protostuff/手写 codec，与本里程碑解耦。

## 2. 架构总览：混合模式与单一存储格式

**内存模式存的不是解码后的 `TxChange` 对象，而是每条消息的原始字节（`byte[]` 列表）。** 这是整个混合设计的支点：

- **切换 = 纯字节转储**：内存桶越限 → 已有 `byte[]` 逐条 `writeBytes` 进 CQ、后续消息直写 CQ。自始至终只有一种存储格式（pgoutput 原始字节），不存在"数据转移时的表示形态变化"
- **内存模式占用更小**：原始字节比解码对象致密（解码形态每列一个 `TupleValue` 对象 + String，对象头/引用开销远超原始字节）；阈值按"缓冲的原始字节数"记账，语义与 PG reorder buffer 记账（`rb->size` 按 TOAST 压缩后字节）对齐
- **解码推迟到提交时**：小事务从内存 `byte[]` 解码、大事务从 CQ tailer 回读解码——两种模式走完全相同的解码路径（`PgOutputDecoder` 只认字节）。附带红利：回滚的大事务从未被解码（省 CPU），提交前只解码一次
- **代价**：路由需要"轻窥"（peek）——只读类型字节 + 流块内 xid 前缀即可路由，不完整解码；run 循环契约变化（见 §4.5）

```
                        ┌────────────────────────────────────────────┐
                        │              run 循环线程                   │
  readPending ──byte[]──▶ peek 路由 ──┬─ 'R'  → 立即解码 → registry    │
                        │             │         （版本日志, seq 戳）   │
                        │             ├─ 开桶/关桶/abort 记账          │
                        │             ├─ MEMORY 桶: List<byte[]> ─────┼─▶ 阈值内
                        │             │   （全局字节记账）              │
                        │             └─ 越限 → dump 转储 ─▶ CQ append │
                        │                                （后续直写） │
                        │  提交信号:                                    │
                        │    MEMORY → 逐 byte[] 解码 ┐                 │
                        │    SPILLED → tailer 回读 ──┴─▶ decode →      │
                        │        TxChange(按 asOf 版本) → Transaction  │
                        └────────────────────────────────────────────┘
```

**阈值策略**：全局记账（所有 MEMORY 桶原始字节总和，跨桶共享，与 PG 全局 `rb->size` 语义一致）。越过阈值时把所有 MEMORY 态桶一次性转储（简单可预测，切换每桶一次）；之后水位仍高时新桶直接以 SPILLED 起步。

## 3. 组件（新增 2 + 改造 2）

| 组件 | 角色 |
|---|---|
| `MessageSpool`（新） | CQ 单例管理者：`append(bytes)→index`、`dump(List<byte[]>)→(first,last)`、`readRange(first,last)` 回读、全局单调序号分配、低水位删滚动文件、close 顺序收敛。持有 ChronicleQueue/Appender/Tailer 生命周期 |
| `MessagePeek`（新，纯函数） | 只读类型字节 + 流块内 xid 前缀（duplicate buffer 不消耗原字节），产出路由最小信息（类型/xid/subxid）。自带 inStream 状态机（S/E 驱动，与 decoder 同规则） |
| `RelationRegistry` 改造 | 升级为**版本日志**：oid → 按到达序号的 `(seq, Relation)` 列表；`lookup(oid, asOfSeq)` 二分取"当时版本"；低于全局最低未决序号可剪枝（DDL 稀少，日志极短） |
| `TransactionAssembler` 改造 | 桶改持 `List<byte[]>`（MEMORY）或 `(firstIndex,lastIndex)`（SPILLED）+ 字节记账；新增全局水位与 spillAll 触发；提交路径改为"回放→全量解码→组 TxChange→封箱" |

内存侧每桶只剩：xid、gid（2PC）、kind、mode、`List<byte[]>` 或 index 区间、abortedSubxids 集合、字节计数——几十字节到 KB 级。100GB 事务的堆占用也是平的。

## 4. 线上不变量与路由

### 4.1 回放可行性的根基（复用 spec 已验证结论）

普通事务 `B..C` 与非流式 2PC `b..P` 在线上**串行独占**（walsender 按 LSN 序输出，同时至多一个活动普通事务，Commit 无 xid 字段），区间内无外来消息可混入；**只有并发流式事务的流块会交错**，而流块内消息全部带 xid 前缀（spec §4.2 已源码验证）。

因此：回放 `[first..last]` → peek 路由（与 live 同一套代码）→ 外来 xid 跳过 → 可精确重建该桶。**live 路由与回放路由是同一套代码**，规则由线上不变量保证在子序列上同样成立。

### 4.2 live 数据流（每消息）

```
readPending → byte[] 拷贝 → seq = spool.nextSeq() → peek 路由
  ├─ 'R' Relation   → 立即全量解码 → registry.accept(seq, rel)（字节不入桶）
  ├─ 'B'/'S'/'b'    → 开桶（内存水位已高则 SPILLED 起步）
  ├─ DML/'T'/'M'/'Y' → append 进归属桶（流块内按 peek 的 xid 选桶），记账 → 越限则 spillAll
  ├─ 'A' StreamAbort → 桶记 abortedSubxids（回放时过滤，不删数据）
  ├─ 'C'/'c'/'K' → 提交路径；'P' → 挂起池；'r' → 丢弃桶；'E' → 关流块
  └─ LSN 反馈逻辑不变（session 层，与组装解耦）
```

### 4.3 提交路径

```
关桶 → 回放：MEMORY 逐 byte[] / SPILLED tailer 读 [first..last]
  → 每条全量 decode → 流块内按 xid 前缀滤外来/滤 abortedSubxids
  → RowChange.relation = registry.lookup(oid, asOf=该消息 seq)
  → 组 TxChange 序列 → Transaction 封箱 → listener 回调
  → 低水位推进 → 触发滚动文件删除检查
```

### 4.4 Relation 快照正确性（asOf 二分）

事务进行中若并发连接做了 DDL（如 `ALTER TABLE ADD COLUMN`），协议会在流里重发新版 Relation。回放构造 `RowChange` 时**不能用当前 registry**（可能已是新版，列数与事务前段的 TupleData 错位）。`lookup(oid, asOfSeq)` 按消息自身序号取"变更时刻版本"，事务前后段各自对齐——这是本设计正确性的机制保障，集成测试有专项场景（§9.2）。

Relation 字节不进桶（registry 版本日志已持对象），桶存储更省。

### 4.5 run 循环接缝改造

`PgReplicationSession.run` 的回调契约从 `onMessage(已解码消息, registry)` 改为交付**原始字节**（`onRaw(bytes)` 语义）；Relation 的即时解码移到组装侧；LSN 反馈、readPending 轮询、100ms 节奏均不变。decoder 的 inStream 状态机移到回放侧使用（每个桶回放一个 decoder 实例，桶区间天然以开桶控制消息起始、以关桶控制消息终止，状态机自洽）。

## 5. CQ 生命周期与清理策略

- **单例队列**：`SingleChronicleQueue`（专属 spill 目录），roll cycle 默认 MINUTELY。appender 单写者（run 线程）；回放也在 run 线程同步执行（提交回调本就在 run 线程），单 tailer 即可
- **低水位线** = 所有 SPILLED 桶的 `min(firstIndex)`；无 spilled 桶时为 +∞（全部已完成内容皆垃圾）。MEMORY 桶数据在堆里、不引用 CQ 索引，与水位无关
- **删除粒度 = 整个滚动文件**：水位线所在 cycle 的前一档以下全部可删（永不碰当前 cycle 与上一档）；文件名解析 cycle 号比对纯函数化（便于单测注入文件名列表）；每次删除 WARN 留痕；失败仅 WARN，下次推进重试
- **不跨重启续用**：spill 队列是瞬态工作区，真源是复制槽——重启后 PG 从槽确认位点重发未完事务。打开时发现残留目录直接整体清空重建，杜绝陈旧状态复活
- **与里程碑 2 的边界**：本 spill 队列（组装工作内存）≠ 未来的输出持久化队列（CDC 数据流），届时是两个独立队列实例；本里程碑只建前者

## 6. 错误处理（fail-fast 语义与现状对齐）

| 场景 | 行为 |
|---|---|
| peek/decode 协议错位/未知类型 | 现有异常族上抛，run 循环退出 |
| CQ 写失败（磁盘满/IO） | ERROR + 上抛，会话收敛（不可恢复，不吞） |
| 回放中桶状态/解码异常 | `IllegalStateException`/协议异常 fail-fast（协议流不应出现） |
| 删文件失败 | WARN 不致命，低水位推进时重试 |

## 7. 配置（`SpillConfig` record，`vb.spill.*`，与 `ReplicationConfig` 同风格）

| 属性 | 默认 | 语义 |
|---|---|---|
| `vb.spill.thresholdBytes` | 64MB | 全局 MEMORY 桶字节和阈值；**≤0 = 禁用 spill**（纯内存，保留里程碑 1.5 行为作逃生门与对照基线） |
| `vb.spill.dir` | `spill-queue` | 队列目录（测试用 temp 目录） |
| `vb.spill.rollCycle` | MINUTELY | 滚动周期 |

## 8. 性能基准（JMH）

### 8.1 基准矩阵（全部无需 Docker）

| 基准 | 度量 | 回答的问题 |
|---|---|---|
| `decode` 逐消息类型 | ops/s + MB/s | 19 种消息各自解码单价，找热点类型 |
| `peek` vs `decode` | ops/s 比值 | 路由窥探是否真"轻"（预期差一个数量级） |
| `assemble` live 路径 | msgs/s | 内存快路径每消息开销（peek+路由+入桶） |
| `commit` 回放：MEMORY vs SPILLED | µs/事务 | 混合两模式提交成本差（堆迭代 vs CQ mmap 回读+重解码） |
| `spool.append` | ops/s + MB/s | CQ writeBytes 落盘单价 |

### 8.2 语料：录制真实流

集成测试跑 `src/main/resources/sql/` 6 个场景脚本，录制原始 pgoutput 字节流为语料文件（几百 KB，提交进 `src/test/resources/bench-corpus/`）。基准回放语料——数字来自真实线格式（含 TOAST、流式 xid 前缀、可/不可压缩载荷混合），基准运行零 Docker 依赖、可复现。另配合成极端样本（不可压缩大元组）补边界。

### 8.3 工程接入

- JMH 1.37，test scope + `jmh` Maven profile；基准类放 `src/test/java/.../bench/`，命名 `*Benchmark`（Surefire 默认 include 不匹配，`mvn test` 不误跑）
- 运行：`mvn -Pjmh test-compile` 后 `java -cp ... org.openjdk.jmh.Main <regex>`；短参数冒烟档默认（`-f 1 -w 1s -r 2s`），全档手动
- 基线数字记入 `docs/benchmarks-baseline.md`（语料、参数、环境、结果表），作为后续回归对照；**不进 CI 硬门**（JMH 进 CI 慢且抖）
- 验收前跑一轮基线：量化确认纯内存快路径零 CQ 开销

边界：JMH 只覆盖计算与本地 IO 成本；端到端 walsender 吞吐受 PG 侧制约，属集成测试范畴。

## 9. 测试

### 9.1 单测（纯 JVM）

- `MessagePeek`：19 种消息 peek 正确性 + 流块状态机（S/E 交界、xid 前缀有无）
- registry 版本日志：多版本 bisect、asOf 边界（恰在版本切换 seq 上）、剪枝安全性
- `MessageSpool`：temp 目录 append/readRange 往返、dump 转储、删除比对纯函数
- 组装器混合模式（构造 pgoutput 字节序列，同 parser 测试手法）：小事务全程 MEMORY、越限转储、越限后新桶 SPILLED 起步、abortedSubxids 回放过滤、2PC 挂起跨转储

### 9.2 集成测试（Testcontainers，沿用 SessionHarness 模式）

- **等价性场景（核心验收）**：同一 SQL 序列大阈值/小阈值（128KB，配 CLAUDE.md 不可压缩 `string_agg(md5)` 手法）各跑一遍，输出 `Transaction` 完全等价——证明 spill 路径无损
- 流式大事务 + spill：交错流段 xid 过滤回放正确
- 并发 DDL：事务窗口内另一连接 `ALTER TABLE ADD COLUMN`，前后段 TupleData 按 asOf 版本对齐（§4.4 专项）
- 大事务回滚：桶丢弃、低水位推进、文件删除被触发
- 既有 5 场景全量回归（组装行为不变式）

## 10. 非目标

- CQ 作为**输出**持久化队列（里程碑 2 原方向，届时独立队列实例）
- 断线重连/错误恢复语义
- Logstash 集成及其线格式（含 protostuff 引入的重新评估）
- JMH 基准进 CI

## 11. 交付物

- `MessageSpool`、`MessagePeek`、`SpillConfig`（新）；`RelationRegistry`、`TransactionAssembler`、`PgReplicationSession`/`PgOutputListener`（改造）
- JMH profile + 基准类 + 语料录制 + `docs/benchmarks-baseline.md`
- 单测 + 集成场景（含等价性验收）
- 本设计文档
