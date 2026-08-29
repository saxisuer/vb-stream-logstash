# 里程碑 2.1 设计：命名辨识度改造（主代码全面命名审计）

- 日期：2026-08-30
- 状态：已与用户逐节确认（审计落点 §1、边界与验收 §2 均定案）
- 前置：里程碑 2.0（流式输出契约）已完成，`mvn test` 158 用例全绿

## 1. 背景与动机

用户最初以"lambda 写得太多、可读性差"提出 2.1 想法，经盘点更正为**命名辨识度**问题：

- 主代码 `->` 共约 86 处，其中约 70% 是 switch 表达式箭头（`case 'B' -> begin(...)`）而非 lambda；真 lambda 仅约 25 处且多为良性（回调接缝、Optional 链、线程接线）——lambda 并非实际痛点。
- 真正的痛点在**命名**：输出契约族（流式/块式两契约 + 适配器 + 收集器）名字辨识度差，只看名字分不出谁是契约、谁是实现、谁转发谁留存。

## 2. 目标与非目标

**目标**：主代码（`src/main/java`）全面命名审计，只改辨识度不足的名字，让输出契约族等混淆点"看名知义"。

**非目标**（明确排除，防范围蔓延）：

- **lambda 不动**——回调接缝（`Consumer`/`BiConsumer`/`Supplier`）、可变捕获（`long[] emitted`）、Optional/stream 链、switch 箭头全部保持现状
- **领域行话保留**——`TxBuffer`（桶）、`MessagePipe`（管道）、`handoff`（交接）、`frontier`（前沿）、`pipeWatermark`（低水位）等与历史 spec 词汇一一对应的术语不改
- **不做结构变更**——`TransactionCollector` 不挪 test 源集（只改名）；不拆类、不移动方法
- **外部面不动**（见 §5）

## 3. 审计结论

### 3.1 A 组 · 改名清单（输出契约族，方案 A"形态对称派"）

`Streaming`/`Block` 是项目从配置（`vb.output.mode=streaming|block`）到枚举（`OutputMode.STREAMING/BLOCK`）的通用形态词汇，契约名直接携带形态，族内对称：

| 现名 | 新名 | 理由 |
|---|---|---|
| `TransactionListener` | `StreamingTransactionListener` | 与 `BlockTransactionListener` 对称，一看便知是流式契约（`onEvent` 收 `TransactionEvent` 流） |
| `BlockTransactionListener` | 不变 | 已带形态 |
| `BlockOutputAdapter` | `StreamingToBlockAdapter` | 方向自述：implements `StreamingTransactionListener`，把流式事件攒回整块再转发（1.7 原子交付逃生门） |
| `TransactionCollector` | `TransactionRecorder` | 与 Adapter 对立明确：**录制留存**（重组事件流为 `List<Transaction>` 供测试断言）vs **适配转发**（交付即弃）；住在 main 源集不变 |

### 3.2 B 组 · 审计发现的拍板点（已定案）

- `ConsoleListener` → `ConsoleRenderer`：它实现三个接口（流式渲染 + 块渲染 + 逐消息观察），是实现类却顶着 `Listener` 后缀，与两个契约接口混在同一命名层。改后层次分明——`StreamingTransactionListener`/`BlockTransactionListener` 是契约（接口），`ConsoleRenderer` 是实现。这是全库唯一一处"实现冒充契约命名"。

### 3.3 C 组 · 保留名单（审计结论：不动）

| 分组 | 类型 | 理由 |
|---|---|---|
| 行话类 | `TxBuffer`、`MessagePipe`、`TransactionConsumer`、`BucketReplayer`、`TransactionAssembler`，字段 `handedOff`/`lastAppendOwner`，方法 `pipeWatermark()`/`floor()` | 与 spec 的"桶/管道/交接/前沿/低水位"词汇一一对应（用户拍板行话保留） |
| protocol 包全部 | `NormalParsers`/`DmlParsers`/`StreamParsers`/`TwoPhaseParsers`、消息 record 族、`UnknownMessageTypeException`/`ProtocolMisalignmentException` 等 | 按消息类型分文件，自解释 |
| Relation 四件套 | `RelationRegistry`（父）、`VersionedRelationRegistry`（子）、`RelationLookup`（接口）、`RelationSnapshot`（值对象） | 继承与实现关系从名字可读 |
| 接缝与辅助 | `RawMessageListener`、`PgOutputListener`、`DecodedMessageBridge`、`RawPeeks`、`PgReplicationSession`、`ReplicationConfig`、`PipeConfig` | 达意 |
| 值对象与枚举 | `TransactionEvent` 族、`TxChange` 族、`Transaction`、`OutputMode`、`TransactionKind`、`DmlKind` | 清晰 |
| 入口 | `Main` | 冒烟入口，惯例名 |

## 4. 执行方式

- **IDE rename refactoring**（引用完整 + 文件名同步改），javadoc 内 `{@link}` 引用随动
- `src/jmh/java` 对被改名四类**零引用**（已核实），JMH 档不受影响
- `src/test/java` 的引用随 rename 自动更新（测试代码引用改、命名风格本身不在审计范围）；**以旧类名命名的测试类随之重命名**，否则 §7.3 的 grep 零残留会被类名子串误中：`ConsoleListenerTest` → `ConsoleRendererTest`、`BlockOutputAdapterTest` → `StreamingToBlockAdapterTest`、`TransactionCollectorTest` → `TransactionRecorderTest`
- **提交分组**（跨机开发，每组 commit + push）：
  1. 输出契约族改名（§3.1 四项，一次原子提交）
  2. `ConsoleListener` → `ConsoleRenderer`（§3.2）
  3. 文档同步（三份 CLAUDE.md + README）

## 5. 外部面一律不动

以下均为已文档化的外部契约，改名只添噪音：

- 配置键：`vb.output.mode` / `vb.pg.*` / `vb.pipe.*`
- 线程名：`transaction-consumer` / `pgoutput-reader`（日志可见面，且对应类名不变）
- logger 名：`org.vastdata.vbstream.cdc` 及全部日志文本
- 控制台输出格式：TXN-BEGIN / 逐行变更 / TXN-END——逐字节不变

## 6. 文档同步范围

| 文档 | 处理 |
|---|---|
| 三份 CLAUDE.md（根 / protocol / replication）+ README | 更新为新名（活文档） |
| 历史 spec（`docs/superpowers/specs/*.md`） | **不追改**——历史记录反映当时命名，强行追改反而失真 |
| `docs/benchmarks-baseline.md` | **不追改**（历史性能档案，同上） |

预期注记：改造后 CLAUDE.md 用新名、历史 spec 保留旧名，两套词汇并存属**预期行为**——历史文档本就反映当时状态。

## 7. 验收标准

1. `mvn clean test` 全绿（clean 防增量编译假绿——已知陷阱）
2. 行为零变化：输出格式逐字节不变（既有 `DecoupledEquivalenceTest` 等等价验收覆盖，无需新增测试）
3. 全库 grep 四个旧名（`TransactionListener` 精确词边界、`BlockOutputAdapter`、`TransactionCollector`、`ConsoleListener`）零残留——范围含代码 + 三份 CLAUDE.md + README；历史 spec 与 baseline 文档除外
4. javadoc 规约不降级：每函数逻辑描述完整，方法签名变更处（如 `Main` 接线、`TransactionAssembler` 构造参数 javadoc 中的 `listener` 描述）同步更新

## 8. 风险与注记

- **词汇断线**：历史 spec（1.7/2.0 设计）以 `TransactionListener`/`BlockOutputAdapter`/`TransactionCollector` 记述，本改造后需按 §3.1 对照表回溯——对照表本身即为此留档
- **rename 波及面**：`TransactionListener` 引用最广（`Main`、`TransactionAssembler`/`TransactionConsumer` 签名、测试接线），IDE rename 一次性处理；改名后 `StreamingToBlockAdapter implements StreamingTransactionListener` 的组合读起来方向自述
- 本里程碑不触碰任何运行时行为，风险上限 = 编译期/引用错误，由验收 1、3 兜底
