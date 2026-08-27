# 事务组装与原子输出设计（里程碑 1.5）

日期：2026-08-27
状态：已与用户确认（2PC 输出时机经 AskUserQuestion 拍板）；**2026-08-27 修订**：§4.2 经 PG 18 源码定位验证，原"单活动流式事务假设"**被推翻**，流式桶改为按顶层 xid 多桶并存（§4.1/§4.2/§4.4/§6 同步修订，源码摘录见附录 B）

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
private TxBuffer currentNormalTx;                                     // 活动普通事务（Begin 置位、Commit 封箱清空——Commit 消息无 xid，协议保证 Begin..Commit 串行不嵌套）
private final Map<Long, TxBuffer> streamedByXid = new HashMap<>();   // 流式事务桶（key=StreamStart.xid=顶层 xid，多桶并存——依据见 §4.2）
private TxBuffer currentStream;                                       // 当前流块上下文：stream_start..stream_stop 之间非 null（§4.2 已证流块不重叠）
private TxBuffer currentPrepareTx;                                    // 活动两阶段事务（BeginPrepare 置位、Prepare 转挂起池）
private final Map<String, TxBuffer> preparedByGid = new HashMap<>(); // 2PC 挂起池（key=gid）

private static final class TxBuffer {
    final long xid; String gid;                  // gid 仅两阶段桶非 null；事务形态由所在容器隐含（普通指针/流式 Map/挂起池）
    final List<TxChange> changes = new ArrayList<>();  // TxChange 自带 streamXid，无需额外包装
}
```

### 4.1 消息驱动规则（全路径）

| 消息 | 动作 |
|---|---|
| `Begin(xid, finalLsn, ts)` | 新建 TxBuffer 置为 `currentNormalTx`（已有未闭合普通事务 → fail-fast） |
| `Insert/Update/Delete(oid, ...)` | 活动桶 = `currentStream`（流块内）否则按 §4.3 顺序找普通/2PC 桶；构造 RowChange（relation 取 `registry.require(oid)` 快照，miss 即 fail-fast）入桶 |
| `Truncate(oids...)` | 同上，每 oid 一次 `registry.require` 快照，构造 TruncateChange 入桶 |
| `LogicalMsg` | 构造 MsgChange 入当前活动桶 |
| `Commit(...)` | **消息无 xid 字段**——封箱当前普通事务桶 `currentNormalTx`（无桶 → fail-fast）为 `Transaction(NORMAL, gid=null, ...)` 回调并清空指针；普通/2PC 活动桶实现为单指针（协议保证 Begin..Commit / BeginPrepare..Prepare 串行不嵌套），对应地 §4 内部结构 `pendingByXid` 由两指针替代 |
| `StreamStart(xid, firstSegment)` | xid 恒为**顶层 xid**（源码证据 B.3）；`firstSegment=true`（该顶层事务首段）→ 新建桶入 `streamedByXid`（已存在同 xid → fail-fast）；`false`（后续段）→ `streamedByXid` 必须已有该桶（miss → fail-fast）；两种情况都置 `currentStream=该桶` |
| `StreamStop` | `currentStream` 必须非 null（否则 fail-fast），置 null——流块边界（消息本身不携带 xid，源码 B.3） |
| `StreamAbort(top, sub)` | 桶 = `streamedByXid[top]`（miss → fail-fast；到达时 currentStream 必为 null，见 B.4）；`top==sub`（整顶层回滚，B.4）→ **移除整个桶**；否则从桶的 changes 中**移除所有 `streamXid==sub` 的变更**（子事务回滚的已流式数据不得下发） |
| `StreamCommit(xid, ...)` | 桶 = `streamedByXid.remove(xid)`（miss → fail-fast；currentStream 必为 null），封箱为 `Transaction(STREAMED, ...)` 回调 |
| `BeginPrepare(gid, xid, ...)` | 新建 twoPhase 桶（活跃，记 gid） |
| `Prepare(gid, ...)` | 活动 2PC 桶转入 `preparedByGid`（gid 已存在 → fail-fast） |
| `StreamPrepare(xid, gid, ...)` | 桶 = `streamedByXid.remove(xid)`（miss → fail-fast；currentStream 必为 null——stream_prepare 前服务端已发完最后一个流段并 stream_stop，见 B.6 注），记 gid、标记 TWO_PHASE，转入 `preparedByGid`（gid 重复 → fail-fast） |
| `CommitPrepared(gid, ...)` | `preparedByGid` 取桶（miss → fail-fast），封箱 `Transaction(TWO_PHASE, gid, ...)` 回调 |
| `RollbackPrepared(gid, ...)` | `preparedByGid` 取桶并**丢弃**（不回调） |
| `Relation` | `registry.accept`（由调用方 Main 转发，或 assembler 内部转发——**设计定：assembler 不接管 registry 更新，调用方负责先 registry.accept 再 assembler.accept**，职责单一） |
| `Type` | 忽略（registry 职责） |

### 4.2 多活动流式事务（PG 18 源码验证结论，推翻原"单活动"假设）

**原假设**（同一 walsender 同一时刻只流式一个 in-progress 大事务，`streamedTx` 单槽即可）**不成立**。源码定位（`REL_18_STABLE`，摘录见附录 B）：

1. **内存阈值是全局的，不是单事务**：`ReorderBufferCheckMemoryLimit` 比较 `rb->size`（reorder buffer 内**所有** in-progress 事务的总内存）与 `logical_decoding_work_mem`（B.1）；文件头注释亦明确 "we track memory used at the reorder buffer level (i.e. total amount of memory)"。
2. **驱逐循环逐个挑最大可流式顶层事务**：超限后 while 循环内反复调用 `ReorderBufferLargestStreamableTopTXN`——它遍历 `rb->txns_by_base_snapshot_lsn` 中**所有**有 base snapshot 的顶层事务，挑 `total_size` 最大者调 `ReorderBufferStreamTXN` 流式（B.1/B.2）。
3. **推论——两个并发大事务的流段交错**：A、B 共同把 `rb->size` 推超限时，先驱逐大者（如 A），仍超则驱逐 B；随后解码继续、A 再积累、再驱逐……下游收到：

   ```
   [S(A,first=1) … E] [S(B,first=1) … E] [S(A,first=0) … E] [S(B,first=0) … E] … [c(A)] [c(B)]
   ```

   **多个流式事务桶并存、段间交错**，故必须 `Map<topXid, TxBuffer>` 多桶。

4. **流块本身不重叠**（多桶可行性的协议基础）：`pgoutput_stream_start` 有 `Assert(!data->in_streaming)`（"we can't nest streaming of transactions"）——任一 stream_start..stream_stop 块在协议上是严格串行的，块内消息全部属于该顶层事务（k-way merge 该 toptxn 及其子事务），绝无两个流块嵌套或交叠（B.3）。因此"当前流上下文"单指针 + `stream_start.xid`（顶层）寻桶即可正确归属每条消息。
5. **stream_start 恒携带顶层 xid，firstSegment ⇔ 该顶层事务首次被流式**：`logicalrep_write_stream_start(ctx->out, txn->xid, !rbtxn_is_streamed(txn))`——txn 是 `ReorderBufferStreamTXN` 断言过的顶层事务，`RBTXN_IS_STREAMED` 标记在该事务首次流式结束（`ProcessTXN` 尾部 `ReorderBufferMaybeMarkTXNStreamed`）时置位（B.3/B.6）。firstSegment=true 是开新桶的可靠锚点。
6. **段内消息的 xid 前缀标识（子）事务**：流块内 DML/Truncate 的 Int32 xid 前缀 = `change->txn->xid`（产生该变更的**（子）事务**，B.5）；Message 的前缀 = 顶层 xid（B.5 注）。前缀仅用于 `StreamAbort(sub)` 时剔除对应子事务变更（已记入 `TxChange.streamXid`），不参与桶归属。
7. **StreamAbort 的 (top, sub) 语义**：`logicalrep_write_stream_abort(out, toptxn->xid, txn->xid, ...)`，txn 为被回滚的（子）事务；decode 层按"先子后顶"逐个发出——整顶层回滚时最后一条是 `top==sub`（B.4）。此即规则表中 `top==sub → 移除整个桶` 的依据。

### 4.3 2PC 活动桶与普通事务的区分

`BeginPrepare` 后至 `Prepare` 前的变更也走"当前活动桶"逻辑。桶查找顺序：`currentStream`（流块内，最高优先）→ `currentPrepareTx`（活动 2PC）→ `currentNormalTx`（活动普通）。活动 2PC 桶与普通桶各用**单指针**而非 Map（walsender 按事务边界串行输出，同时至多一个未闭合），`Prepare(gid,xid)` 时直接把 `currentPrepareTx` 转入挂起池。

### 4.4 协议异常（fail-fast）

- Commit/Prepare/CommitPrepared/RollbackPrepared/StreamCommit/StreamPrepare/StreamAbort 对应桶或 gid miss
- Begin/BeginPrepare/StreamStart(firstSegment=true) 重复开桶
- StreamStart(firstSegment=false) 但 `streamedByXid` 无该顶层 xid 的桶
- StreamStop 到达但 `currentStream` 为 null；StreamCommit/StreamAbort/StreamPrepare 到达但 `currentStream` 非 null（协议保证三者必在流块外，B.3/B.4 的 `Assert(!data->in_streaming)`）
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
  头尾各一行 CDC logger INFO，变更行复用现有渲染。逐消息的 `onMessage` 输出保留（受日志级别控制：事务块 INFO、逐消息 DEBUG——**Main 默认走事务形态**）。（Main 装配中显式调用 onMessage——置于 isDebugEnabled 守卫内，避免 DEBUG 关闭时实参急切渲染的热路径开销）

## 6. 测试

### 6.1 单测（纯 JVM，`TransactionAssemblerTest`）

用 protocol 层的 record 直接构造消息序列（无需 MsgBuilder 字节级——组装器输入是已解析消息）：

1. 普通事务：B→R→I→U→D→C 输出一个 NORMAL Transaction，changes 顺序与输入一致，Relation 快照正确
2. 多普通事务交错不发生（协议保证顺序），但连续事务逐个输出
3. 流式事务：StreamStart(first=true)→流内 I(streamXid=sub)→E→c 输出 STREAMED
4. **StreamAbort 剔除**：流桶含 sub 甲乙两组变更 → StreamAbort(top, sub甲) → StreamCommit 输出仅含乙组；另一用例 StreamAbort(top, top)（整顶层回滚）→ 后续 c(top) fail-fast（桶已移除）
5. **双流式事务交错**（§4.2 场景）：S(A,first=1)→I→E→S(B,first=1)→I→E→S(A,first=0)→I→E→S(B,first=0)→I→E→c(A)→c(B)：先输出 A（3 条变更、顺序正确），后输出 B
6. 流段间隙小事务：S(A,first=1)→I→E→B(小事务)→I→C→S(A,first=0)→I→E→c：小事务先行输出，流事务随后输出（currentStream 间隙正确路由）
7. 2PC 提交：b→I→P→K 输出 TWO_PHASE（gid 匹配）
8. 2PC 回滚：b→I→P→r 不回调
9. 流式 2PC：StreamStart→I→E→p(gid)→K(gid) 输出 TWO_PHASE
10. 负例：无桶 Commit / 无活动桶的 Insert / 重复 Begin 同 xid / 未知 gid 的 K / StreamStart(first=false) 未知 xid / 无流块的 StreamStop → IllegalStateException
11. （质量审查后补）Truncate 多表快照组装（relations 逐 oid 快照 + 未知 oid fail-fast）与 LogicalMsg 路由（事务性入桶 / 非事务性无桶 WARN 丢弃不抛）——TruncateChange/MsgChange 生产行为的组装器级覆盖

### 6.2 集成测试（Testcontainers，`TransactionAssemblyIT` 风格，沿用 SessionHarness）

真库四场景断言 `Transaction` 完整性：

| # | 场景 | 构造 | 断言 |
|---|---|---|---|
| 1 | 普通多语句事务 | 显式 BEGIN; INSERT×2; UPDATE; DELETE; COMMIT（**单连接单事务**） | 一个 NORMAL Transaction，4 changes，kind/dml 序列与数据值（列名来自快照）正确 |
| 2 | 流式 + 子事务回滚 | 大事务（>64kB work_mem）内 SAVEPOINT 插入 + ROLLBACK TO + 继续插入，COMMIT | 一个 STREAMED Transaction；被回滚子事务的变更**不在** changes 中；changes 总数 = 存活行数 |
| 3 | 2PC 双路 | PREPARE→COMMIT PREPARED 与 PREPARE→ROLLBACK PREPARED 各一次 | 前者输出 TWO_PHASE（gid 匹配、changes 完整）；后者无输出 |
| 4 | **双连接并发大事务（§4.2 交错实证）** | 两条 JDBC 连接各自 BEGIN，**交替**写入 8KB 行、各 10 行（单事务 80KB 已独立越过 64kB work_mem，保证两者都必然被驱逐流式），先 COMMIT A 后 COMMIT B | 恰好输出 2 个 STREAMED Transaction（xid 各异、changes 各 10 条且行数据正确）；断言不依赖段间交错程度——两事务交替积累、被 `LargestStreamableTopTXN` 轮番驱逐时多桶路由即被真实路径覆盖 |

注：场景 2/4 依赖 `t_stream` 类大 payload 构造（沿用 `StreamedTransactionTest` 经验：`logical_decoding_work_mem=64kB` 下 8KB×500 触发流式；场景 4 因阈值是全局 rb->size，两事务合计超限即可触发）。

## 7. 非目标

- Chronicle Queue 写入（里程碑 2，直接消费 Transaction）
- 事务输出的进一步格式化/JSON 化（按需后补）
- Origin 消息进事务（级联复制才出现，透传忽略）
- 跨重启的 prepared 挂起池持久化（崩溃后槽位重放会重收 p→K，见 §1 决策依据）

## 8. 交付物

`replication` 包新文件：`Transaction`、`TransactionKind`、`TxChange`、`RowChange`、`DmlKind`、`TruncateChange`、`MsgChange`、`TransactionListener`、`TransactionAssembler`；`ConsoleListener`/`Main` 改造；单测 + 集成测试。全量测试绿 + push。

## 附录 B：PG 18 源码摘录（§4.2 验证依据）

来源：`postgres/postgres` 分支 `REL_18_STABLE`（2026-08-27 抓取）：
`src/backend/replication/logical/reorderbuffer.c`、`src/backend/replication/pgoutput/pgoutput.c`（PG 18 起 pgoutput.c 位于独立子目录）。摘录经裁剪，语义完整。

### B.1 ReorderBufferCheckMemoryLimit——阈值是全局 rb->size，驱逐循环挑最大顶层事务

```c
static void
ReorderBufferCheckMemoryLimit(ReorderBuffer *rb)
{
	ReorderBufferTXN *txn;

	/* Bail out if ... we haven't exceeded the memory limit. */
	if (debug_logical_replication_streaming == DEBUG_LOGICAL_REP_STREAMING_BUFFERED &&
		rb->size < logical_decoding_work_mem * (Size) 1024)
		return;

	while (rb->size >= logical_decoding_work_mem * (Size) 1024 || ...)
	{
		if (ReorderBufferCanStartStreaming(rb) &&
			(txn = ReorderBufferLargestStreamableTopTXN(rb)) != NULL)
		{
			Assert(txn && rbtxn_is_toptxn(txn));
			...
			ReorderBufferStreamTXN(rb, txn);      /* 流式驱逐 */
		}
		else
		{
			txn = ReorderBufferLargestTXN(rb);
			...
			ReorderBufferSerializeTXN(rb, txn);   /* 落盘驱逐 */
		}
	}
}
```

文件头注释（内存记账语义）："To limit the amount of memory used by decoded changes, we track memory used **at the reorder buffer level (i.e. total amount of memory)**, and for each transaction. When the total amount of used memory exceeds the limit, the transaction consuming the most memory is then serialized to disk."

### B.2 ReorderBufferLargestStreamableTopTXN——候选池是所有顶层事务

```c
/*
 * Find the largest streamable (and non-aborted) toplevel transaction to evict
 * (by streaming). ... we can simply iterate over the limited number of
 * toplevel transactions that have a base snapshot.
 */
static ReorderBufferTXN *
ReorderBufferLargestStreamableTopTXN(ReorderBuffer *rb)
{
	dlist_iter	iter;
	Size		largest_size = 0;
	ReorderBufferTXN *largest = NULL;

	/* Find the largest top-level transaction having a base snapshot. */
	dlist_foreach(iter, &rb->txns_by_base_snapshot_lsn)
	{
		ReorderBufferTXN *txn = dlist_container(...);
		/* Don't consider these kinds of transactions for eviction. */
		if (rbtxn_has_partial_change(txn) ||
			!rbtxn_has_streamable_change(txn) ||
			rbtxn_is_aborted(txn))
			continue;
		if ((largest == NULL || txn->total_size > largest_size) &&
			(txn->total_size > 0))
		{
			largest = txn;
			largest_size = txn->total_size;
		}
	}
	return largest;
}
```

### B.3 pgoutput_stream_start / stream_stop——顶层 xid、first_segment 判定、流块不嵌套

```c
static void
pgoutput_stream_start(struct LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn)
{
	PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;

	/* we can't nest streaming of transactions */
	Assert(!data->in_streaming);

	/* If we already sent the first stream for this transaction then don't
	 * send the origin id in the subsequent streams. */
	if (rbtxn_is_streamed(txn))
		send_replication_origin = false;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_stream_start(ctx->out, txn->xid, !rbtxn_is_streamed(txn));
	...
	data->in_streaming = true;
}

static void
pgoutput_stream_stop(struct LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn)
{
	...
	Assert(data->in_streaming);
	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_stop(ctx->out);   /* 不携带 xid */
	OutputPluginWrite(ctx, true);
	data->in_streaming = false;
}
```

`txn` 恒为顶层事务：`ReorderBufferStreamTXN` 入口 `Assert(rbtxn_is_toptxn(txn))`，`rb->stream_start(rb, txn, ...)` 在 `ReorderBufferProcessTXN` 内以该 toptxn 调用（reorderbuffer.c）。

### B.4 pgoutput_stream_abort——(top, sub) 参数与流块外约束

```c
static void
pgoutput_stream_abort(struct LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn,
					  XLogRecPtr abort_lsn)
{
	ReorderBufferTXN *toptxn;
	PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;
	bool		write_abort_info = (data->streaming == LOGICALREP_STREAM_PARALLEL);

	/* The abort should happen outside streaming block... */
	Assert(!data->in_streaming);

	/* determine the toplevel transaction */
	toptxn = rbtxn_get_toptxn(txn);      /* 顶层自身时返回自身 */
	Assert(rbtxn_is_streamed(toptxn));

	logicalrep_write_stream_abort(ctx->out, toptxn->xid, txn->xid, abort_lsn,
								  txn->xact_time.abort_time, write_abort_info);
	...
}
```

decode 侧对回滚按"先子后顶"逐 xid 调 `ReorderBufferAbort`（其函数注释 "Needs to be first called for subtransactions and then for the toplevel xid"）→ 整顶层回滚时最后一条 StreamAbort 满足 `top==sub`。

### B.5 pgoutput_change / pgoutput_message——流式消息的 xid 前缀

```c
static void
pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	...
	/* Remember the xid for the change in streaming mode. We need to send xid
	 * with each change in the streaming mode so that subscriber can make
	 * their association and on aborts, it can discard the corresponding changes. */
	if (data->in_streaming)
		xid = change->txn->xid;          /* DML 前缀 = 产生变更的（子）事务 */
	...
	logicalrep_write_insert(ctx->out, xid, targetrel, new_slot, ...);
	...
}

static void
pgoutput_message(...)                    /* message 前缀 = 顶层 xid（回调收到的 txn 是 toptxn） */
{
	...
	if (data->in_streaming)
		xid = txn->xid;
	...
}
```

### B.6 ReorderBufferStreamCommit——stream_commit/stream_prepare 前必先发最后一个流段

```c
static void
ReorderBufferStreamCommit(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	/* we should only call this for previously streamed transactions */
	Assert(rbtxn_is_streamed(txn));

	ReorderBufferStreamTXN(rb, txn);      /* 先流式剩余变更（内部发 stream_start..stream_stop） */

	if (rbtxn_is_prepared(txn))
	{
		...
		rb->stream_prepare(rb, txn, txn->final_lsn);   /* 再发 stream_prepare */
		...
	}
	else
	{
		rb->stream_commit(rb, txn, txn->final_lsn);    /* 再发 stream_commit */
		...
	}
}
```

首次流式结束时 `ReorderBufferProcessTXN` 尾部调 `ReorderBufferMaybeMarkTXNStreamed` 置 `RBTXN_IS_STREAMED`（顶层恒标记）——故同事务第二个流段的 `pgoutput_stream_start` 必见 `rbtxn_is_streamed(txn)=true`，firstSegment=false。
