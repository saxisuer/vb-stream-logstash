# CLAUDE.md 细化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按已批准的设计（`docs/superpowers/specs/2026-08-28-claude-md-refinement-design.md`）细化四份 CLAUDE.md：根文档补架构总览、replication 包补事务模型 record 族专节、it 包与 src/jmh 各新建文档。

**Architecture:** 纯文档变更，零代码改动。就近分层——全局信息进根 CLAUDE.md，测试细节进 `it` 包，基准细节进 `src/jmh`。所有内容已对照源码核实（2026-08-28，main @ 7847b0c 之后含 javadoc 可读性重写），本计划内嵌各文档的完整目标内容，执行者直接落盘即可，无需再从代码提炼。

**Tech Stack:** Markdown（GitHub 方言）；文档风格与既有 CLAUDE.md 一致——全中文密集、粗体术语、`{}` 内为说明不翻译代码标识符。

**共同规约（每个任务适用）：**

- 每个 Edit 的 `old_string` 必须先 Read 目标文件确认逐字匹配（本计划的锚点文本基于 main@7847b0c，若文件已变动以实际为准）
- 纯 .md 变更无编译影响，验证 = 引用实体核对（每个任务附核对清单）+ `git diff --stat` 目测
- commit 后 push；push 被 reject 时执行 `git pull --rebase && git push`（跨多机开发，远端可能有新提交）

---

### Task 1: 根 CLAUDE.md——新增「架构总览」节 + 改写「源码结构」bullet

**Files:**
- Modify: `CLAUDE.md`（两处：① 在 `## 常用命令` 前插入新节；② 替换 `- **源码结构**：...` bullet）

- [ ] **Step 1: Read `CLAUDE.md` 全文**，确认两处锚点存在：
  - 锚点 A：`## 常用命令`（用于前置插入）
  - 锚点 B：`- 源码结构：\`org.vastdata.vbstream.protocol\`（协议解析，纯函数）、` 开头的整条 bullet（位于 `## 运行 Main` 节的 bullet 列表中、spill 有界性 bullet 之后）

- [ ] **Step 2: 插入「架构总览」节**

用 Edit 把 `## 常用命令` 整行替换为下面的内容（即新节 + 原标题）：

````markdown
## 架构总览

端到端数据流（raw 接缝，里程碑 1.6 形态）——各组件机制详见对应包内 CLAUDE.md：

```
PostgreSQL 18（walsender 逻辑解码：pgoutput v4 + streaming + two_phase）
  │ CopyBoth 消息（pgjdbc 已剥复制协议头）
  ▼
PgReplicationSession.run()  ←100ms readPending 非阻塞轮询；周期回传 LSN 确认
  │ RawMessageListener.onRaw(byte[])：单条完整消息的独占数组（类型字节 + 流式块内可选 xid 前缀）
  ▼
TransactionAssembler  ←全局 seq 分配 → 按类型字节路由：控制消息与 'R' live 解码；
  │                   I/U/D/T/M 只窥 xid 前缀构造 PayloadUnit 入桶（解码推迟到提交期）
  ├─ MEMORY 桶（默认）──字节和越 vb.spill.thresholdBytes──▶ spillAll() 全量转储
  │                                                        └▶ MessageSpool（Chronicle Queue 溢写池）
  ├─ VersionedRelationRegistry：oid → (seq, Relation) 版本日志（DDL 重发同 oid 新版即追加）
  ▼ 提交期（Commit / StreamCommit / CommitPrepared）
BucketReplayer  ←aborted 子事务过滤 → decodeSingle → 按单元 seq 取 asOf 版本 Relation 渲染
  │ TransactionListener.onTransaction(Transaction)：不可变原子事务块
  ▼
ConsoleListener（CDC 专用 logger org.vastdata.vbstream.cdc，INFO）
```

三层模块职责：

| 层 | 位置 | 职责 | 细节文档 |
|---|---|---|---|
| 协议解析 | `org.vastdata.vbstream.protocol` | pgoutput 消息字节 → 强类型 record，纯函数无 IO | `src/main/java/.../protocol/CLAUDE.md` |
| 会话与组装 | `org.vastdata.vbstream.replication` | 双 JDBC 连接、raw 字节交付接缝、事务组装（MEMORY/SPILLED 混合桶 + 溢写） | `src/main/java/.../replication/CLAUDE.md` |
| 入口与输出 | `org.vastdata.vbstream`（顶层） | `Main` 装配、`ConsoleListener` 控制台输出 | 本节 |

- **`Main`**：冒烟入口。校验配置（缺失 exit 2 打用法）→ session open/ensureSlot/start → reader 线程（`pgoutput-reader`）内 try-with-resources 建组装器（独享 `VersionedRelationRegistry` 与 `SpillConfig`；`ConsoleListener` 一个实例兼任事务回调与解码点 observer——组装器是唯一解码者）→ 主线程 await 停机信号（Ctrl+C 触发 shutdown hook）→ 会话关闭使 run 退出、组装器随之收尾 spill 池。启动失败 exit 1；复制流中断保留槽位并倒计时停机（重启续传）
- **`ConsoleListener`**：双角色 listener。`onTransaction`：TXN-BEGIN/END 头尾 + 逐变更行，基于 `TxChange` 内嵌 Relation 快照渲染（不依赖 registry），INFO；`onMessage`：9 种事务生命周期控制消息（流式 5 + 两阶段 4）升 INFO，行级/元数据 DEBUG（默认关闭）——INFO 级保证任何事务形态至少留一行痕迹。值渲染：text 截 64 字符、binary 十六进制、TOAST 未变显式标注

## 常用命令
````

- [ ] **Step 3: 替换「源码结构」bullet**

用 Edit 把锚点 B 的整条 bullet（`- 源码结构：...` 到行尾）替换为：

```markdown
- **源码结构**（各源码根一行；包内细节见各模块级 CLAUDE.md，层间关系见上文"架构总览"）：
    - `src/main/java`：`protocol`（协议解析，纯函数）、`replication`（会话 + raw 接缝 + 事务组装与溢写）、顶层 `Main`/`ConsoleListener`
    - `src/test/java`：`protocol`/`replication` 包字节级单测（`MsgBuilder`/`PgWire` 手造字节辅助）、`it` 包集成测试 9 组（Testcontainers，见其 CLAUDE.md）、`bench` 包语料基建（JMH 语料来源）
    - `src/jmh/java`：四基准（`-Pjmh` 档才参与编译，默认构建零 JMH 依赖，见其 CLAUDE.md）
```

- [ ] **Step 4: 验证**

核对清单（对源码/既有文档抽查）：
- 图中方法名 `onRaw`/`onTransaction`、类名 `PgReplicationSession`/`TransactionAssembler`/`BucketReplayer`/`MessageSpool`/`VersionedRelationRegistry`/`ConsoleListener` 与 `src/main/java` 一致
- `Main` 描述与 `src/main/java/org/vastdata/vbstream/Main.java` javadoc 一致（exit 2/exit 1/槽位保留语义）
- `ConsoleListener` 的"9 种控制消息"与 `ConsoleListener.isTxLifecycle` 一致
- 旧「源码结构」bullet 已不存在；`mvn` 命令等既有内容未被破坏（`git diff CLAUDE.md` 只含两处改动）

- [ ] **Step 5: Commit + push**

```bash
git add CLAUDE.md
git commit -m "docs(claude): 根 CLAUDE.md 增架构总览（数据流图/三层职责/Main+ConsoleListener）与源码结构导览"
git push
```

---

### Task 2: replication/CLAUDE.md——新增「事务模型 record 族（输出侧）」节

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/CLAUDE.md`（在 TransactionAssembler 节末尾与 `## SpillConfig` 之间插入新节）

- [ ] **Step 1: Read 目标文件**，确认锚点：`## SpillConfig（record，不可变）` 标题行（唯一出现）

- [ ] **Step 2: 插入新节**

用 Edit 把 `## SpillConfig（record，不可变）` 整行替换为下面的内容（新节 + 原标题）：

```markdown
## 事务模型 record 族（输出侧，组装器的回调产物）

提交路径回放出的不可变值对象族：`Transaction` 是对外的原子单元，`TxChange` sealed 族是其内容。全部 record + 紧凑构造器归一（无 null 组件、集合防御性拷贝），可跨线程传递。各类完整 javadoc 见源文件。

- **`TransactionListener.onTransaction(Transaction)`**：事务消费契约（@FunctionalInterface）。调用线程 = run 循环线程（同步执行，回调耗时直接拖慢消息循环与 LSN 反馈，实现方应快速返回或转交）；ROLLBACK 路径不回调
- **`Transaction(xid, kind, gid, commitLsn, endLsn, commitTimestamp, changes)`**：一个已确认提交的完整事务。xid 来源随 kind 而定（NORMAL←Begin、STREAMED←StreamStart、TWO_PHASE←BeginPrepare/StreamPrepare）；gid 非 null **当且仅当** kind=TWO_PHASE；changes 按协议到达顺序，紧凑构造器 `List.copyOf` 防御性拷贝（null 或含 null 元素抛 NPE）
- **`TransactionKind`**（枚举）：NORMAL（变更整体缓冲，Commit 后一次输出）/ STREAMED（越过 logical_decoding_work_mem 被驱逐流式，StreamCommit 后一次输出）/ TWO_PHASE（PREPARE 后挂起，COMMIT PREPARED 才输出，ROLLBACK PREPARED 丢弃）
- **`TxChange`（sealed interface，permits RowChange/TruncateChange/MsgChange）**：事务内一条变更的基接口。公共组件 `streamXid`（OptionalLong）：流式块内非空——DML/Truncate 的 xid 前缀 = 产生变更的**（子）事务** xid、Message 的前缀 = 顶层 xid；非流式块内恒 empty。供 StreamAbort(sub) 按子事务剔除与下游追溯归属
- **`RowChange(dml, relation, before, after, streamXid)`**：行变更。`relation` 是**变更时刻的 Relation 快照嵌入**（非 registry 引用——下游自包含，DDL 后旧行不按新 schema 错解）；before/after 统一 Optional：INSERT 仅 after、DELETE 仅 before、UPDATE 的 before 取决于 replica identity（紧凑构造器把 null 归一为 empty）
- **`TruncateChange(relations, options, streamXid)`**：一条 TRUNCATE 语句可截多表——一次变更携带全部受影响表的快照（顺序与协议 relationOids 一致）；options 经 Set.copyOf 不可变化
- **`MsgChange(transactional, prefix, content, streamXid)`**：`pg_logical_emit_message` 的事务内逻辑消息（非事务性即时消息在组装器 WARN 丢弃，不入 Transaction）；content 为 byte[] 组件，显式 override equals/hashCode 为**值相等**（record 默认对数组退化为引用相等）
- **`DmlKind`**（枚举）：INSERT/UPDATE/DELETE，与 RowChange 的 before/after 语义一一对应

## SpillConfig（record，不可变）
```

- [ ] **Step 3: 验证**

核对清单：
- 8 个类型名与 `src/main/java/org/vastdata/vbstream/replication/` 下文件一一对应
- `Transaction` 组件顺序 `(xid, kind, gid, commitLsn, endLsn, commitTimestamp, changes)` 与源码 record 头一致
- `TxChange` permits 列表、`RowChange` 组件顺序与源码一致
- 插入后 `## SpillConfig` 节内容完整未损

- [ ] **Step 4: Commit + push**

```bash
git add src/main/java/org/vastdata/vbstream/replication/CLAUDE.md
git commit -m "docs(claude): replication 包补事务模型 record 族专节（Transaction/TxChange 等 8 输出侧类型）"
git push
```

---

### Task 3: it 包新建 CLAUDE.md

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/it/CLAUDE.md`

- [ ] **Step 1: Write 新文件**，内容如下：

````markdown
# it/ 集成测试——Testcontainers 真 PG 18 端到端

全部测试类跑真库：共享单例容器 `PgTestEnv.PG`（postgres:18，`wal_level=logical`、`logical_decoding_work_mem=64kB`、`max_prepared_transactions=16`、`max_slot_wal_keep_size=1GB`），**需要本机 Docker**；`mvn test` 单命令即含本包（与单测同轮）。

## 基建与通用模式

- **`PgTestEnv`**：类加载即启动的单例容器 + 静态工具（`newSqlConnection` / `newConfig(slot, pub)`——固定 proto 4 + PARALLEL + two_phase + 反馈 2s / `execSql` / `dropSlotQuietly`）。容器**跨测试类共享**，故各测试类用独立槽名，`@BeforeEach` 清残留槽（上次异常退出留同名槽会从旧 confirmed_flush_lsn 续传、静默吞掉先于建流写入的事务）、`@AfterEach` drop（先杀 walsender 再删）
- **`SessionHarness`**：会话包装 + 双轨录制（raw 字节与解码消息两列表同序一一对应）。停止条件是 countDown latch——**确定性全量断言必须先 `close()` 再读列表**，故本包多用显式 try/finally 而非 try-with-resources（harness 出块后仍可引用）。机制细节见 replication 包 CLAUDE.md 的 SessionHarness 节
- **录制→离线回放模式**（组装器类测试通用）：真库录制 raw 字节 → close → `rawMessages()` 回放给 `TransactionAssembler`（确定性纯状态机，离线回放与在线组装一致）——随机数据只进录制侧，不进双配置对照断言

## 9 组测试各自验证什么

| 测试类 | 验证场景 |
|---|---|
| `NormalTransactionTest` | 普通事务消息子序列（首个 Begin..Commit 精确四元 + 三事务计数）；LSN 反馈两段式断言：`pg_stat_replication.flush_lsn` 先采纳 → 槽 `confirmed_flush_lsn` 在解码推进时跳位 |
| `StreamedTransactionTest` | 500 行×8KB 单事务触发流式分段（StreamStart firstSegment、分段结构）；parallel 模式 StreamAbort 携带附加字段且后续无错位 |
| `TwoPhaseTransactionTest` | PREPARE→COMMIT PREPARED（b/变更/P/K 按 gid 匹配）；PREPARE→ROLLBACK PREPARED（r）；大事务 PREPARE 以 StreamPrepare 分段收尾 |
| `DataTypeTest` | 19 列常见类型（时间/数字/字符串/bool/uuid/jsonb/bytea）文本协议解码端到端一致性——以 PG 自身 JDBC getString 输出为 oracle（同一套类型输出函数），不硬编码期望值 |
| `TruncateTest` | TRUNCATE 选项位（CASCADE/RESTART_IDENTITY）与多表 oid 列表解码 |
| `RawSessionContractTest` | raw 接缝契约三角：raw 与解码消息逐条等长、每条 raw 首字节是 19 种合法类型字符之一、全新 `DecodedMessageBridge` 重放 raw 流得 record 值相等序列 |
| `TransactionAssemblyTest` | 组装器五场景：普通多语句事务 / 流式+子事务回滚剔除 / 2PC 提交与回滚 / 双连接并发大事务多桶交错 / 多类型值 round-trip |
| `AssemblySpillTest` | 溢写四场景（双阈值等价 / 交错大事务 / 事务内 DDL asOf 渲染 / 回滚后低水位推进删档），场景明细见根 CLAUDE.md"集成测试"条 |
| `BenchCorpusRecordTest` | JMH 语料生成器：6 场景脚本真库录制 → `corpus.bin` + SHA-256 指纹边车；指纹一致时**只做健康断言、不启容器**（PgTestEnv 引用收缩在录制分支内，常规 mvn test 秒级过）。改 `src/main/resources/sql/` 脚本或 DDL 即触发重录并**改写源码树**（产物须提交回库） |

## 领域注意（不在本包重复）

构造流式数据的载荷规则（TOAST 压缩后记账、不可压缩载荷写法）与双连接交错原理见根 CLAUDE.md"领域要点"。
````

- [ ] **Step 2: 验证**

核对清单：
- 9 个测试类名与 `src/test/java/org/vastdata/vbstream/it/` 目录文件一一对应（无多无漏）
- 表内场景与各类 `@Test` 方法语义相符（抽查：`NormalTransactionTest.feedbackIsAdoptedByServerAndConfirmedFlushAdvances`、`TwoPhaseTransactionTest.largePreparedTransactionEndsWithStreamPrepare`、`AssemblySpillTest` 四方法名）
- 容器参数与 `PgTestEnv.PG` 的 `withCommand` 一致

- [ ] **Step 3: Commit + push**

```bash
git add src/test/java/org/vastdata/vbstream/it/CLAUDE.md
git commit -m "docs(claude): it 包新建 CLAUDE.md——9 组集成测试场景与基建导览"
git push
```

---

### Task 4: src/jmh 新建 CLAUDE.md

**Files:**
- Create: `src/jmh/CLAUDE.md`

- [ ] **Step 1: Write 新文件**，内容如下：

````markdown
# jmh/ 基准源码根——四基准离线回放

独立源码根的动机：JMH 依赖（jmh-core + annprocess）与基准编译只在 `-Pjmh` profile 参与，默认构建（含 `mvn test`）零 JMH 依赖；源码根经 build-helper 挂为 test 源码目录（能复用 src/test 基建），挂载与注解处理细节见 pom.xml 的 `jmh` profile 注释。

## 语料依赖链

```
it.BenchCorpusRecordTest（真库录制，src/test）
  → src/test/resources/bench-corpus/corpus.bin（提交进库；6 场景脚本叠加，84 条真实消息）
  → bench.BenchCorpus.load()（统一取用点，缺失抛带修复指引的 ISE）
  → bench.CorpusLoader（[I32 len][bytes] 长度前缀流读写器，big-endian）
```

录制内容覆盖：类型边界值 INSERT/UPDATE/DELETE、TOAST 未变标志、REPLICA IDENTITY FULL 的全列 old tuple 与 Relation 重发、流式大事务分段提交、StreamAbort。改动 `src/main/resources/sql/` 场景脚本或建表 DDL 会使 SHA-256 指纹失配、`BenchCorpusRecordTest` 自动重录（需 Docker，产物提交回库）。

## 四基准各测哪条路径

| 基准 | 测什么 | 口径 |
|---|---|---|
| `DecodeBenchmark` | 一条消息的**完整解码**（tuple 逐列 + 剩余字节校验），顺序态 decoder（PARALLEL，inStream 随真实流序演进） | µs/条（可换算 MB/s） |
| `RoutePeekBenchmark` | 组装器**路由窥探**（只读类型字节 + 流式块内 Int32 xid 前缀，不解码——与 onRaw 路由同构） | ns/条——与 Decode 相除即"推迟解码"省下的比例 |
| `AssembleMemoryBenchmark` | **整语料一轮完整组装**（threshold=∞ 含水位记账与越限检查；路由窥探 + 桶记账 + 提交回放解码 + asOf 渲染，listener/observer 均 no-op） | ms/轮 |
| `SpillPathBenchmark` | `@Param` MEMORY/SPILLED **同批 2000 单元回放对照**（差值 = 溢写回放纯代价，经 BenchSpillBridge 走组装器同构双分支）；另附 `spool.append` 单帧吞吐 | ms/次 |

口径间关系：Decode 是 RoutePeek 的对照上限；AssembleMemory 与 SpillPathBenchmark 的 SPILLED 回放对照即溢写代价；AssembleMemory 与 Decode 对照可见组装开销中解码的占比。

## 运行与基线

运行命令（`--add-opens` 须经 `-jvmArgsAppend` 自带）、冒烟档参数与**基线数字**见 `docs/benchmarks-baseline.md`——改基准或改组装器/解码器热点路径后须重跑对照并把数字更新入档。
````

- [ ] **Step 2: 验证**

核对清单：
- 四基准类名与 `src/jmh/java/org/vastdata/vbstream/bench/` 一致
- "84 条真实消息"与 `DecodeBenchmark` javadoc 一致；"2000 单元"与 `SpillPathBenchmark.BUCKET_UNITS` 一致
- `BenchCorpus.CORPUS_FILE` 路径与文档一致（`src/test/resources/bench-corpus/corpus.bin`）
- `docs/benchmarks-baseline.md` 存在

- [ ] **Step 3: Commit + push**

```bash
git add src/jmh/CLAUDE.md
git commit -m "docs(claude): src/jmh 新建 CLAUDE.md——四基准口径、语料依赖链与 baseline 文档分工"
git push
```

---

### Task 5: 收尾——交叉验证与设计文档勾稽

**Files:**
- 无新改动（验证任务）

- [ ] **Step 1: 交叉核对四份文档间的引用**

- 根 CLAUDE.md"源码结构"提到的三个 CLAUDE.md 均实际存在（protocol/replication 既有、it 与 jmh 新建）
- it 与 jmh 文档内引用的"根 CLAUDE.md"小节名真实存在（"领域要点"、"集成测试"条）
- `git diff main~4 --stat`（或逐 commit diff）确认改动只含 4 个 .md 文件

- [ ] **Step 2: 验收对照设计文档**

打开 `docs/superpowers/specs/2026-08-28-claude-md-refinement-design.md` 的"验收标准"逐条核对：
- 新会话仅读根 CLAUDE.md 可复述数据流与三层职责 ✓（Task 1）
- replication 包输出侧 8 类型均有语义描述 ✓（Task 2）
- it 9 组、jmh 4 基准各有"验证什么"描述 ✓（Task 3/4）
- 根文档增幅可控（架构总览约 40 行）✓

- [ ] **Step 3: 最终 push 确认**

```bash
git status   # 应 clean
git log --oneline -6   # 四个 docs commit 在列
```
