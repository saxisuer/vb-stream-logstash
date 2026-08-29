# 组装成本归因与微优化设计（里程碑 1.7.1）

## 1. 背景与目标

1.7 把组装存储层换血为"纯 CQ index 段记账"后，`AssembleMemoryBenchmark.assembleWholeCorpus` 实测 **1.395 ms/轮（≈16.6 µs/条）**，对 1.6 纯内存基线 **0.081 ms/轮（≈0.96 µs/条）** 差约 17 倍（`docs/benchmarks-baseline.md`）。但孤立口径 `PipePathBenchmark.appendOneMessage` 仅 **0.23 µs/条**——append 只能解释差距的 ~1.5%，**~98% 未归因**。曾考虑的"1.6 式混合缓冲（超阈值才溢写）"直译形态收益 ≈ 零（append 次数不变，只挪时间点，还把 O(事务大小) 的转储突发停顿带回 reader 提交点）；真正省 append 的"小事务跳过 CQ"形态则引回 1.6 被删的根本原因（交接桶堆引用脱离阈值管辖，consumer 慢则无界）并需复活 nextSeq/信封帧/双形态回放整套装置。

**本里程碑目标：先把差距逐段归因入档，再只做不动架构的便宜修复，为"是否开 1.7.2 混合存储"提供数据决策依据。** 不预设收窄幅度——归因结论本身就是核心交付。

**完成判据**：baseline 文档新增 1.7.1 归因表（各段 µs/条 + 占比 + 误差域 + 修复前后对照）与一段明确的 1.7.2 建议（存储路径占比 X% → 混合存储预期收益上限即 X%，值得/不值得）；`mvn clean test` 143 用例零回归（生产代码预期最多动 `MessagePipe.append` 一处）。

## 2. 非目标

- 不动架构：seq ≡ CQ index、纯段记账、控制消息 append（seq 时间线配重）全部保留
- 不做混合存储——归因即便证明存储路径占大头，也只写入 1.7.2 建议，本里程碑不实施
- 不加任何依赖（不用 async-profiler 等）
- 控制消息免 append 明确排除：动了 seq ≡ index 不变量，与混合存储同属 1.7.2 议题

## 3. 三腿归因方法论

**腿 1——`-prof stack` 采样**：JMH 内置 stack profiler（macOS 可用，零依赖）跑 `assembleWholeCorpus`，得各段（live 解码/append/窥探与段记账/快照/交接队列/回放 readRange+解码）粗占比。粒度粗（采样 ~5-10% 误差），表内标注误差域，结论需与腿 2/3 互证。

**腿 2——组件微基准**（归因缺口逐个补齐）：
- **冷页 append**（头号嫌疑）：现有 `appendOneMessage` 是**热页**口径（单管道连续写，mmap 页全热），而组装路径在真实运行中会不断触碰新 mmap 页——每 4KB 一次软缺页（µs 级）足以解释 17× 的大头。新增冷页口径：新鲜页上按页边界跨步 append
- `BytesStore.wrap` 每消息分配 vs 复用（终审标记过的每消息分配）
- 每交接 `RelationSnapshot` 拷贝（新 `AssemblyAttributionBenchmark`）
- 交接队列 put/poll、段记账 + oidSet 窥探（同上，隔离口径）

**腿 3——差分检验**：冷/热管道组装对照（`@Setup(Level.INVOCATION)` 每调用新管道——JMH 对重状态 setup 有统计局限，文档注明；对照口径之差不依赖绝对值，结论仍可靠）。

## 4. 修复菜单与启用准则

**准则：单项占比 ≥15% 且修复不动架构才修。** 已知候选：

| 候选 | 前提 | 预期形态 |
|---|---|---|
| `BytesStore` 复用 | wrap 分配占可见比例 | `MessagePipe` 持可复用 store/bytes 视图，`append` 热路径零分配 |
| mmap 页预触碰 | 冷页假设成立且 CQ 侧有便宜开关 | 视归因结论设计，可能落空（写明即可） |

修复实施后必须复测全口径入档（修复前后对照表）。

## 5. 交付物

- `docs/benchmarks-baseline.md` 1.7.1 段：差距分解表 + 修复对照 + **1.7.2 建议**（一段话给出存储路径占比与混合存储收益上限的换算）
- 新增 `AssemblyAttributionBenchmark` + `PipePathBenchmark` 冷页口径（jmh 档，默认构建零参与）
- 生产代码最多一处修复（`MessagePipe.append`），其余全为基准与文档

## 6. 风险

- 归因可能证明大头就在存储路径（冷缺页等）→ 本里程碑交付决策依据而非修复，属设计内结局，不是失败
- stack profiler 误差 → 三腿互证；差分口径的统计局限 → 只用对照之差，不用绝对值
- 冷页假设不成立 → 归因表本身仍有价值（排除法收窄未知），修复菜单按实际占比启用
