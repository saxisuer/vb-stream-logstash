# 事务组装与原子输出设计（里程碑 1.5）

日期：2026-08-27
状态：已与用户确认（2PC 输出时机经 AskUserQuestion 拍板）

## 1. 背景与目标

里程碑 1 交付了逐消息解码（`PgOutputListener.onMessage` 逐条回调）。本里程碑在其上叠加**事务组装**：把属于同一事务的变更缓冲、组装为一个不可变的 `Transaction` 对象，**收到对应提交信号后才整体输出**——保证下游（控制台/里程碑 2 的 Chronicle Queue）看到的事务是原子的、完整的、不含未提交数据的。

三个提交信号对应三种事务形态：

| 形态 | 提交信号 | 说明 |
|---|---|---|
| 普通事务 | `Commit('C', xid)` | Begin..Commit 序列 |
| 流式大事务 | `StreamCommit('c', xid)` | 流段序列 + 最终 StreamCommit |
| 两阶段（含流式 2PC） | `CommitPrepared('K', gid)` | Prepare 后挂起，COMMIT PREPARED 才输出；ROLLBACK PREPARED 丢弃（**用户已确认**） |

## 2. 架构与数据流

```
PgReplicationSession.run（逐消息契约，零改动）
        │ PgOutputMessage
        ▼
TransactionAssembler.accept(msg, registry)        ← 本里程碑新增（纯内存状态机）
        │ 缓冲组装（按 xid / gid 分桶 + 2PC 挂起池）
        │ 完整事务时
        ▼
TransactionListener.onTransaction(Transaction)    ← 新契约
        │
        ├── ConsoleListener（事务形态输出，CDC logger）
        └── 里程碑 2：Chronicle Queue 写入器（消费同一 Transaction）

RelationRegistry 持续更新（assembler 转发/复用），Relation 快照在组装时嵌入变更。
```

分层约束：`TransactionAssembler` 无 IO、无线程（在 run 线程内被调用，回调即 run 线程）；协议层（protocol 包）零改动。

## 3. 数据模型（replication 包新文件）

```java
/** 事务消费契约：组装完成的原子单元到达即回调（调用线程 = run 循环线程）。 */
@FunctionalInterface
public interface TransactionListener {
    void onTransaction(Transaction transaction);
}

/** 一个已确认提交的完整事务。 */
public record Transaction(long xid, TransactionKind kind, String gid,
                          long commitLsn, long endLsn, Instant commitTimestamp,
                          List<TxChange> changes) {}

public enum TransactionKind { NORMAL, STREAMED, TWO_PHASE }

/** gid 语义：两阶段事务非 null（来自 BeginPrepare/Prepare），其余 null。 */

public sealed interface TxChange permits RowChange, TruncateChange, MsgChange {}

/**
 * 行变更。Relation 为变更时刻的元数据快照（嵌入而非引用 registry——表定义变化时
 * 协议会重发 Relation，逐变更快照天然对齐；下游自包含，无需 registry）。
 */
public record RowChange(DmlKind dml, PgOutputMessage.Relation relation,
                        Optional<TupleData> before, TupleData after,
                        OptionalLong streamXid) {}

public enum DmlKind { INSERT, UPDATE, DELETE }

/** TRUNCATE 可涉及多表：一次操作一条 TxChange，携带全部受影响表的 Relation 快照。 */
public record TruncateChange(List<PgOutputMessage.Relation> relations,
                              EnumSet<TruncateOption> options, OptionalLong streamXid) {}

/** pg_logical_emit_message 产生的事务内逻辑消息。 */
public record MsgChange(boolean transactional, String prefix, byte[] content,
                        OptionalLong streamXid) {}
```

- `before/after` 语义沿用协议：INSERT 仅 after；UPDATE before 可选（replica identity 决定），after 必有；DELETE 仅 before（即被删行/键）。
- `streamXid`：变更所属的（子）事务 xid，流式场景非空；普通事务为 empty。供下游追溯子事务归属。
- Relation/Type 消息**不作为事务内容**——它们是元数据；Type 消息仅由 registry 消化，不进 Transaction。

## 4. 组装器状态机

**类**：`TransactionAssembler`（public final，replication 包）
**API**：

```java
public TransactionAssembler(TransactionListener listener)
/** 喂入一条消息；registry 用于把变更的 relationOid 解析为 Relation 快照。 */
public void accept(PgOutputMessage message, RelationRegistry registry)
```

内部结构：

```java
private final TransactionListener listener;
private final Map<Long, TxBuffer> pendingByXid = new HashMap<>();   // 普通事务（key=Begin.xid）
private TxBuffer streamedTx;                                        // 当前流式事务（单活动流式事务假设见下）
private final Map<String, TxBuffer> preparedByGid = new HashMap<>(); // 2PC 挂起池（key=gid）

private static final class TxBuffer {
    long xid; String gid; boolean twoPhase; boolean streamed;
    long beginLsn; Instant beginTs;              // Begin/BeginPrepare 携带的时间戳
    List<TxChange> changes = new ArrayList<>();  // TxChange 自带 streamXid，无需额外包装
}
```

### 4.1 消息驱动规则（全路径）

| 消息 | 动作 |
|---|---|
| `Begin(xid, finalLsn, ts)` | 新建 TxBuffer 入 `pendingByXid`（已存在同 xid → fail-fast） |
| `Insert/Update/Delete(oid, ...)` | 活动桶 = streamedTx（若流式进行中）否则最近 Begin 桶——**由消息到达时的活动事务上下文决定**；构造 RowChange（relation 取 `registry.require(oid)` 快照，miss 即 fail-fast）入桶 |
| `Truncate(oids...)` | 同上，每 oid 一次 `registry.require` 快照，构造 TruncateChange 入桶 |
| `LogicalMsg` | 构造 MsgChange 入当前活动桶 |
| `Commit(xid, ...)` | 从 `pendingByXid` 取桶（miss → fail-fast），封箱为 `Transaction(NORMAL, gid=null, ...)` 回调，移除桶 |
| `StreamStart(xid, firstSegment)` | `firstSegment=true` → 新建 streamedTx（xid 即**顶层 xid**，PG 源码 pgoutput_stream_start 仅顶层首段置 first_segment）；`false` → 断言 streamedTx 存在（子事务段，继续用当前桶） |
| `StreamStop` | 无状态动作（流段边界标记，可校验 streamedTx 存在） |
| `StreamAbort(top, sub)` | 从 streamedTx 的 changes 中**移除所有 `streamXid==sub` 的变更**（子事务回滚的已流式数据不得下发）；top 与 streamedTx.xid 不符 → fail-fast |
| `StreamCommit(xid, ...)` | streamedTx 封箱为 `Transaction(STREAMED, ...)` 回调，清空 streamedTx |
| `BeginPrepare(gid, xid, ...)` | 新建 twoPhase 桶（活跃，记 gid） |
| `Prepare(gid, ...)` | 活动 2PC 桶转入 `preparedByGid`（gid 已存在 → fail-fast） |
| `StreamPrepare(gid, ...)` | streamedTx 转入 `preparedByGid`（gid 记入桶；桶标记 TWO_PHASE） |
| `CommitPrepared(gid, ...)` | `preparedByGid` 取桶（miss → fail-fast），封箱 `Transaction(TWO_PHASE, gid, ...)` 回调 |
| `RollbackPrepared(gid, ...)` | `preparedByGid` 取桶并**丢弃**（不回调） |
| `Relation` | `registry.accept`（由调用方 Main 转发，或 assembler 内部转发——**设计定：assembler 不接管 registry 更新，调用方负责先 registry.accept 再 assembler.accept**，职责单一） |
| `Type` | 忽略（registry 职责） |

### 4.2 单活动流式事务假设

同一 walsender 的 reorder buffer 按序流式发送**单个** in-progress 大事务（流段之间可插入其他小事务的 Begin..Commit 整体序列，但不会交错两个流式事务）——因此 `streamedTx` 单槽即可。交错到达的普通事务按 xid 分桶，互不干扰；挂起池中的 prepared 事务同理。

### 4.3 2PC 活动桶与普通事务的区分

`BeginPrepare` 后至 `Prepare` 前的变更也走"当前活动桶"逻辑。桶查找顺序：streamedTx（流式中）→ 活动 2PC 桶 → 最近 Begin 桶。活动 2PC 桶与普通 Begin 桶用同一 Map 亦可（key=xid，桶带 twoPhase/gid 标记），`Prepare(gid,xid)` 时按 xid 取出转挂起池——简化实现，行为一致。

### 4.4 协议异常（fail-fast）

- Commit/Prepare/CommitPrepared/RollbackPrepared/StreamCommit 对应桶或 gid miss
- Begin/BeginPrepare/StreamStart(firstSegment) 重复开桶
- 变更消息到达但无任何活动桶
- registry.require(oid) miss

均抛 `IllegalStateException`（与 RelationRegistry.require 同风格，消息带 xid/gid/oid 上下文）。

## 5. ConsoleListener / Main 改造

- `Main`：`session.run((msg, registry) -> { registry.accept(msg); assembler.accept(msg, registry); })`；assembler 构造时注入事务回调
- `ConsoleListener`：保留逐消息渲染方法（复用现有 render/tupleOf），**新增事务块输出**（`onTransaction`）：
  ```
  TXN-BEGIN xid=505 kind=STREAMED gid=null commitLsn=0/1BD9E70 commitTs=2026-.. changes=402
    [1] INSERT public.t_stream BEFORE=- AFTER=[id=1, payload=xxx...]
    [2] UPDATE ...
  TXN-END   xid=505
  ```
  头尾各一行 CDC logger INFO，变更行复用现有渲染。逐消息的 `onMessage` 输出保留（受日志级别控制：事务块 INFO、逐消息 DEBUG——**Main 默认走事务形态**）。

## 6. 测试

### 6.1 单测（纯 JVM，`TransactionAssemblerTest`）

用 protocol 层的 record 直接构造消息序列（无需 MsgBuilder 字节级——组装器输入是已解析消息）：

1. 普通事务：B→R→I→U→D→C 输出一个 NORMAL Transaction，changes 顺序与输入一致，Relation 快照正确
2. 多普通事务交错不发生（协议保证顺序），但连续事务逐个输出
3. 流式事务：StreamStart(first=true)→流内 I(streamXid=sub)→E→c 输出 STREAMED
4. **StreamAbort 剔除**：流桶含 sub 甲乙两组变更 → StreamAbort(top, sub甲) → StreamCommit 输出仅含乙组
5. 流段间隙交错：StreamStart→I→E→B(小事务)→I→C→StreamStart(first=false)→I→E→c：小事务先行输出，流事务随后输出
6. 2PC 提交：b→I→P→K 输出 TWO_PHASE（gid 匹配）
7. 2PC 回滚：b→I→P→r 不回调
8. 流式 2PC：StreamStart→I→E→p(gid)→K(gid) 输出 TWO_PHASE
9. 负例：无桶 Commit / 无活动桶的 Insert / 重复 Begin 同 xid / 未知 gid 的 K → IllegalStateException

### 6.2 集成测试（Testcontainers，`TransactionAssemblyIT` 风格，沿用 SessionHarness）

真库三场景断言 `Transaction` 完整性：

| # | 场景 | 构造 | 断言 |
|---|---|---|---|
| 1 | 普通多语句事务 | 显式 BEGIN; INSERT×2; UPDATE; DELETE; COMMIT（**单连接单事务**） | 一个 NORMAL Transaction，4 changes，kind/dml 序列与数据值（列名来自快照）正确 |
| 2 | 流式 + 子事务回滚 | 大事务（>64kB work_mem）内 SAVEPOINT 插入 + ROLLBACK TO + 继续插入，COMMIT | 一个 STREAMED Transaction；被回滚子事务的变更**不在** changes 中；changes 总数 = 存活行数 |
| 3 | 2PC 双路 | PREPARE→COMMIT PREPARED 与 PREPARE→ROLLBACK PREPARED 各一次 | 前者输出 TWO_PHASE（gid 匹配、changes 完整）；后者无输出 |

注：场景 2 依赖 `t_stream` 类大 payload 构造（沿用 `StreamedTransactionTest` 经验：`logical_decoding_work_mem=64kB` 下 8KB×500 触发流式）。

## 7. 非目标

- Chronicle Queue 写入（里程碑 2，直接消费 Transaction）
- 事务输出的进一步格式化/JSON 化（按需后补）
- Origin 消息进事务（级联复制才出现，透传忽略）
- 跨重启的 prepared 挂起池持久化（崩溃后槽位重放会重收 p→K，见 §1 决策依据）

## 8. 交付物

`replication` 包新文件：`Transaction`、`TransactionKind`、`TxChange`、`RowChange`、`DmlKind`、`TruncateChange`、`MsgChange`、`TransactionListener`、`TransactionAssembler`；`ConsoleListener`/`Main` 改造；单测 + 集成测试。全量测试绿 + push。
