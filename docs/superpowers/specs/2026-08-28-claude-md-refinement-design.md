# CLAUDE.md 细化设计（2026-08-28）

## 背景与动机

用户认为根 CLAUDE.md 与包内 CLAUDE.md 描述"过于简单"。经探索核实：

- 两个既有包内文档（`protocol/CLAUDE.md` 70 行、`replication/CLAUDE.md` 101 行）已达字段/机制级详细，**不是**主要缺口
- 真实缺口有三：
    1. **根 CLAUDE.md 无架构导览**——"源码结构"仅一行；无端到端数据流、无模块依赖；顶层 `Main`/`ConsoleListener` 无描述
    2. **replication 包输出侧类型无专节**——`Transaction`/`TransactionKind`/`TxChange`/`RowChange`/`MsgChange`/`TruncateChange`/`DmlKind`/`TransactionListener` 8 个类型只在组装器上下文顺带提及，未达 protocol 包对 `PgOutputMessage` 的覆盖标准
    3. **测试与基准结构无文档**——`src/test`（单测 / it 集成测试 9 组 / 语料基建）与 `src/jmh`（四基准）的结构散落在根 CLAUDE.md 的密集段落里

## 决策记录

| 决策点 | 结论 | 依据 |
|---|---|---|
| 细化方向 | 架构导览 + 补全包内缺口 + 测试与基准结构（不做全面翻修） | 用户三选 |
| 放置策略 | **就近分层**：全局信息进根，测试细节进 `it` 包 CLAUDE.md，基准细节进 `src/jmh` CLAUDE.md | 根 CLAUDE.md 每会话全量进 context；包内文档按需加载 |
| 细化深度 | **A 导览式**：写到"不读代码能建立心智模型"为止，细节归 javadoc | 与 B 手册式（重复易过期）和 C 最小增量（覆盖不全）权衡 |

## 变更清单

### ① 根 CLAUDE.md——新增「架构总览」节（置于"项目概述"与"常用命令"之间）

- 端到端数据流 ASCII 图：`PG walsender → PgReplicationSession(run 100ms 轮询, raw byte[]) → TransactionAssembler(seq 路由/桶缓冲/溢写) → BucketReplayer(提交期回放) → TransactionListener → ConsoleListener`；旁注 LSN 确认回传与 spill 旁路（MEMORY 桶 → 64MiB 阈值 → MessageSpool 溢写池）
- 三层模块职责表：`protocol`（纯函数解码，无 IO）/ `replication`（会话 + raw 接缝 + 组装 + 溢写）/ 顶层 `Main` + `ConsoleListener`
- "源码结构"一节改写为源码根导览表（`src/main`、`src/test`、`src/jmh` 各一行）+ 指向各包内 CLAUDE.md 的指引

### ② `replication/CLAUDE.md`——新增「事务模型 record 族（输出侧）」节

插入位置：TransactionAssembler 节之后。8 个类型逐组件语义 + 不可变性/约束，风格对齐 protocol 包 `PgOutputMessage` 节。

### ③ `src/test/java/org/vastdata/vbstream/it/CLAUDE.md`（新建）

- 9 组集成测试各验证什么场景（两三句/组）
- 基建：`PgTestEnv`（容器生命周期）、`SessionHarness`（测试使用视角，与 replication 包文档的机制视角互补）
- 运行前提（Docker）、语料指纹重录机制
- 领域深坑（TOAST 记账等）引用根 CLAUDE.md，不重复

### ④ `src/jmh/CLAUDE.md`（新建）

- 四基准各测哪条路径（`DecodeBenchmark`/`RoutePeekBenchmark`/`AssembleMemoryBenchmark`/`SpillPathBenchmark`）
- 语料依赖链：`BenchCorpus`/`CorpusLoader`（test 源码根）← `BenchCorpusRecordTest` 录制
- 与 `docs/benchmarks-baseline.md` 分工：CLAUDE.md = 是什么/改哪；baseline = 怎么跑/数字对照
- 独立源码根动机一句话（详情在 pom 注释）

## 共同约束

- 全中文密集风格、粗体术语，与既有文档一致
- 只新增/改写标注的节，**不动其他既有内容**
- 一切描述以实际代码为准（`Main`/`ConsoleListener`/it 各测试类/jmh 四基准先读再写，不臆断）

## 验收标准

- 新会话仅读根 CLAUDE.md + 架构总览即可画出数据流与三层职责，无需翻代码
- `replication` 包 41 个 main 源文件中所有 public 输出侧类型在包内文档均有语义描述
- it 包 9 组测试、jmh 4 基准各有"验证什么"的一句话描述
- 根 CLAUDE.md 增幅可控（架构总览节约 30-40 行）；既有内容零删改（"源码结构"一节除外，为改写）
