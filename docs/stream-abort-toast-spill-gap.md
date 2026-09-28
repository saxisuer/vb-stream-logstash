# PG 逻辑解码流式子事务回滚通知（StreamAbort）缺失问题报告

> 版本锚定：2026-09-28 实测定位（postgres:17 与 postgres:18 双版本复现，REL_17_STABLE 源码核对）。
> 定位过程：四轮构造实验 + 录制消息序列取证 + `log_min_messages=debug2` 日志 + `decode.c`/`reorderbuffer.c` 源码链核对。
> 配套测试：`vb-stream-engine/src/test/java/org/vastdata/vbstream/it/Pg17DefaultWorkMemTest`（手动档）。

## 摘要

`logical_decoding_work_mem` 为出厂默认 **64MB** 时，**行宽超过 TOAST 阈值（约 2KB）的大事务**在
流式驱逐（streaming）路径上会被静默降级为 **spill-to-disk**（落盘）路径；spilled 子事务在
`ROLLBACK TO SAVEPOINT` 后的补发流被 concurrent-abort 检测中断，导致：

1. **StreamAbort 消息永不发射**——下游收不到子事务回滚的丢弃通知；
2. **被回滚子事务的行"部分下发、无 discard 信号"**（实测 2744/4800 行到达，其余静默消失）。

对依赖 StreamAbort 做子事务剔除的下游（含本引擎的 `abortedSubxids` 机制、Debezium、vanilla
逻辑复制 apply worker）构成**脏数据风险**：已回滚的行会被当存活行输出/应用。

**规避核心**：让行不 TOAST（行总尺寸 < 2KB），驱逐即回到 streaming 分支，StreamAbort 正常发射（已实测验证）。

## 一、问题表象

以下数据均为 `logical_decoding_work_mem=64MB`（默认、不传参）、`streaming=parallel` 模式下真库实测。

### 16KB TOAST 载荷（问题形态）

事务结构：主段 4800 行（~78MB）→ `SAVEPOINT sp1` → 子段 4800 行（~78MB）→ `ROLLBACK TO sp1`
→ 驱动段 4200 行（~68MB）→ 尾行 → `COMMIT`。

| 观察项 | 实测值 | 期望（协议语义） |
|---|---|---|
| StreamStart / StreamStop | 4 段（语句边界的补发流） | 越限驱逐段 |
| **StreamAbort** | **0（缺失）** | 1（对被回滚子事务） |
| StreamCommit | 1 ✓ | 1 |
| 被回滚子事务（subxid 744）行到达量 | **2744 / 4800** | 全量先发 + abort 通知剔除，或零发 |
| 主段 / 驱动段 / 尾行 | 全量到达 ✓ | 全量 |
| 服务端 debug2 日志 | `spill 27434 changes in XID 744 to disk` 等 6 条 spill | 应无（或少量）spill |

该形态**跨 PG 17 与 PG 18 稳定复现**（两版本被回滚子事务行到达量均精确为 2744），与写入节奏
（逐行 75ms / 分批 200 行 + 250ms）、是否等待 walsender 追平（`pg_stat_replication.sent_lsn`
追平子段末位点后再回滚）无关。

### 1KB 不 TOAST 载荷（正常形态）

同样的三段事务结构（~200MB、20 万行），仅行载荷改为 1KB（不 TOAST）：

| 观察项 | 实测值 |
|---|---|
| StreamStart / StreamStop | 4 段（streaming 驱逐） |
| **StreamAbort** | **1 ✓（携带 parallel 附加字段 abortLsn/abortTimestamp）** |
| StreamCommit | 1 ✓ |
| spill 日志 | **0 条** |
| 主段 / 驱动段 | 全量到达 ✓ |
| 被回滚子事务行 | 部分到达（先发后弃，具体量随时序浮动）——剔除语义正确 |

### 对照：64kB 压低阈值档（现有常规测试基线）

`logical_decoding_work_mem=64kB` + 16KB TOAST 行 + 逐行慢写（75ms/行）：StreamAbort 正常
（`Pg17CompatTest.streamedSubtransactionAbortCarriesParallelFields` 每轮回归为绿）。

## 二、根因分析（源码链）

源码核对版本：[postgres/postgres REL_17_STABLE](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/replication/logical/reorderbuffer.c)（PG 18 行为一致；项目内摘录另见 `docs/superpowers/specs/2026-08-26-pgoutput-stream-decoder-design.md` 附录 B）。

链路分五步：

1. **TOAST 制造 partial 标**：行宽超过约 2KB（`TOAST_TUPLE_THRESHOLD`）的变长列被 TOAST 外存，
   WAL 中每行 = 若干 toast chunk 记录 + 1 条主表记录。toast chunk 入队时
   `ReorderBufferProcessPartialChange` 给 top 事务置 `RBTXN_HAS_PARTIAL_CHANGE`，主表记录
   入队时清除。**多值批量 INSERT 的 change 流中 toast chunk 占绝大多数**（16KB 行约 80%），
   越限检查点大概率落在"TOAST 装配中段"（partial 置位）。

2. **越限驱逐的分支选择**：`ReorderBufferCheckMemoryLimit`（每个 change 入队后检查）越过
   `logical_decoding_work_mem` 时二选一——`ReorderBufferLargestStreamableTopTXN` 挑选可流式
   驱逐的 top 事务，条件含 `!(rbtxn_has_partial_change(txn))`；**带 partial 标的事务不可流式
   驱逐，只能走 `ReorderBufferSerializeTXN`（spill 到盘）**。64MB 阈值下一次 spill 即清空
   积压，越限点稀疏且大概率落在 partial 置位段——**驱逐整体降级为 spill**（实测 6 条 spill
   日志、StreamStart 仅剩语句边界 partial 清除时 `ReorderBufferProcessPartialChange` 对已
   serialized 事务的补发流）。

3. **spill 不打流式标**：`RBTXN_IS_STREAMED` 只在流式驱逐段收尾的 `ReorderBufferTruncateTXN`
   打标（子事务须在段内有 entry 且 top 已 streamed）；spill 路径只置 `RBTXN_IS_SERIALIZED`。

4. **补发流撞上 concurrent-abort 中断**：`ROLLBACK TO SAVEPOINT` 在 clog 标记子事务 aborted
   （无独立 WAL abort 记录即时生效）。spilled 数据的补发流（从盘 RestoreChanges 后 stream
   发送）发送中被回滚子事务的行时，`SetupCheckXidLive` + catalog 访问检测到 aborted，抛
   `ERRCODE_TRANSACTION_ROLLBACK`，`ReorderBufferProcessTXN` 的 PG_CATCH 捕获后置
   `concurrent_abort = true` 并 `ReorderBufferResetTXN` **中断本段**——已发出的行无后续
   通知，未发出的行连同盘上剩余被丢弃（`ReorderBufferQueueChange` 对 concurrent_abort
   事务直接丢 change）。

5. **StreamAbort 发射链断裂**：`rb->stream_abort` 的发射点唯一——`ReorderBufferAbort`，由
   `DecodeAbort`（解码子事务的 `xl_xact_abort` 记录）触发，门槛是 `rbtxn_is_streamed(子事务)`。
   被中断的补发流未能完成"干净打过 streamed 标"的驱逐段，abort 通知永不发射。

64kB 档为何正常：阈值极小使越限检查**高频**发生（每几行一次），change 流中"主表记录入队"
时刻（partial 恰被清除）必然出现越限检查点，驱逐稳定走 streaming 分支——子事务正常打标，
abort 通知链完整。

## 三、触发场景（精确条件）

三个条件**同时**成立时触发：

| # | 条件 | 说明 |
|---|---|---|
| 1 | `logical_decoding_work_mem` 为默认或较大值（如 64MB） | 驱逐稀疏，越限点大概率落在 TOAST 装配中段；压到 kB 级则驱逐高频、回到 streaming |
| 2 | 行宽超过 TOAST 阈值（约 2KB）且批量写入 | change 流中 toast chunk 占比高，驱逐时刻事务恒/大概率带 partial 标 |
| 3 | 大事务内 `SAVEPOINT` + 写入 + `ROLLBACK TO SAVEPOINT` | 子事务数据已（部分）下发后回滚，才需要 StreamAbort 通知下游剔除 |

典型受害负载：**CDC 场景的大批量写入事务（行宽 >2KB 的 text/bytea/jsonb 列）中使用保存点做
部分回滚**。反之，以下场景不受影响：行不 TOAST；子事务数据量很小（从未被驱逐下发，PG 静默
丢弃、无一致性问题）；阈值压低（64kB 档实测正常）；纯小事务。

## 四、规避方案

按可靠性排序：

1. **行宽控制在 TOAST 阈值内（行总尺寸 < 2KB）**——根治方向，本轮已实测验证（1KB 行：spill
   为 0、StreamAbort 正常）。对宽表可拆列、压缩进单行、或把大字段拆到独立表/独立事务。
2. **压低 `logical_decoding_work_mem`**（如 64kB~几 MB）——驱逐高频化使检查点必然覆盖
   partial 清除时刻，回到 streaming 分支（64kB 档常规回归长期为绿即此机制）。代价：驱逐/
   落盘更频繁的服务端开销；该缓解在"低阈值 + 批量写"组合下未单独压测，建议先在预发验证。
3. **业务侧避开组合**：大事务内不用 `SAVEPOINT` 回滚——把需要部分回滚的逻辑挪进小事务，
   或先写临时表确认后再入正式表。
4. **下游兜底（引擎侧，待办）**：vb-stream 引擎的 `abortedSubxids` 剔除完全依赖 StreamAbort
   消息，当前无兜底。可评估的缓解：commit 前后按表内实际状态对账，或在文档化前提下接受
   at-least-once 语义中的该脏数据窗口（crash 重发会带来重复，但不会自动修复此脏行）。

## 五、测试环境

| 项 | 值 |
|---|---|
| 宿主机 | Windows 11 Pro（10.0.26200），Docker Desktop（Linux 引擎） |
| 数据库容器 | Testcontainers 拉起 `postgres:17`（复现）/ `postgres:18`（对照，同样复现） |
| 客户端驱动 | pgjdbc 42.7.13，复制连接 `jdbc:postgresql://<host>:<port>/<db>?replication=database&assumeMinServerVersion=9.4` |
| 复制流参数 | `proto_version=4`、`publication_names=<pub>`、`streaming=parallel`、`two_phase=on`（binary=off） |
| 建槽 | `SELECT pg_create_logical_replication_slot('<slot>', 'pgoutput', false, true)` |
| 测试载体 | `Pg17DefaultWorkMemTest`（`-Dvb.it.pg17.defaultmem=true` 手动档，单次 ~80s） |

## 六、数据库参数

复现容器（`Pg17DefaultMemTestEnv`）的完整参数——**唯一关键项是 `logical_decoding_work_mem`
不传参（出厂默认 64MB）**：

```ini
wal_level              = logical   # 逻辑解码必需
max_replication_slots  = 16
max_wal_senders        = 16
max_prepared_transactions = 16     # two_phase 槽所需
max_slot_wal_keep_size = 1GB
# logical_decoding_work_mem 不设置 —— 出厂默认 64MB 即复现条件
```

排障期临时加过 `log_min_messages=debug2`（用于抓 `spill N changes in XID ... to disk` 轨迹，
正式测试中已移除）。对照基线（64kB 档）：同参数但 `logical_decoding_work_mem=64kB`。

## 七、表结构与测试数据

```sql
CREATE TABLE t_v17_def(id int PRIMARY KEY, payload text);
CREATE PUBLICATION pub_v17_def FOR TABLE t_v17_def;
```

行载荷表达式（问题形态 16KB / 正常形态 1KB，均为不可压缩随机文本——reorder buffer 按
TOAST 压缩后实际字节记账，规则图案会被 pglz 压缩永不越限）：

```sql
-- 16KB（TOAST 外存，问题形态）：512 个不同 md5 拼接
(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 512))
-- 1KB（行内存储，正常形态）：32 个不同 md5 拼接
(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 32))
```

事务写入节奏（分批多值 INSERT，批间 250ms 给 walsender 解码窗口——单语句快写的大事务
不触发进行中驱逐）：

```sql
BEGIN;
-- 主段：分批 INSERT（合计越过 64MB，触发流式）
SAVEPOINT sp1;
-- 子段：分批 INSERT（被回滚的数据）
ROLLBACK TO SAVEPOINT sp1;
-- 驱动段：分批 INSERT（触发回滚标记后的下一次越限驱逐）
INSERT INTO t_v17_def VALUES (<id>, 'tail');
COMMIT;
```

## 八、复现方式

```bash
mvn test -pl vb-stream-engine -Dtest=Pg17DefaultWorkMemTest -Dvb.it.pg17.defaultmem=true
```

- 正常路径（1KB 载荷，现行断言面）：绿——StreamStart/StreamAbort/StreamCommit 齐全 + 按
  id 区间到达量（主段/驱动段全量、被回滚子段部分到达）。
- 问题形态（16KB 载荷）：把测试内 `RAND_PAYLOAD` 的 `generate_series(1, 32)` 改回 `512`、
  行数参数缩至 24 批 × 200 行即可复现断言②失败（StreamAbort 缺失）与 2744/4800 的到达量。

## 九、对本项目引擎的影响

`TransactionAssembler` 的子事务剔除（`abortedSubxids` → 回放期过滤）**完全依赖 StreamAbort
消息**。本问题形态下消息缺失 ⇒ 已回滚子事务的下发行（实测最多可达其全部行数的过半）会被
引擎当存活行进入输出流——**已回滚的数据出现在下游**，且 at-least-once 的 crash 重发机制
不会修复它。生产接入前建议：核对业务行宽分布；宽行 + 保存点回滚的业务流按第四节规避；
引擎侧兜底列为待评估事项。

## 十、参考

- 源码：REL_17_STABLE `src/backend/replication/logical/reorderbuffer.c`（`ReorderBufferCheckMemoryLimit`
  分流、`ReorderBufferMaybeMarkTXNStreamed` 打标、PG_CATCH concurrent-abort 路径）与
  `decode.c`（`DecodeAbort` 发射链）；项目内摘录：`docs/superpowers/specs/2026-08-26-pgoutput-stream-decoder-design.md` 附录 B
- 相邻已知 bug：[BUG #19616: pgoutput sends stream abort ('A') to clients that did not enable streaming](https://postgr.es/m/19616-f6153af509910853@postgresql.org)（官方邮件档案短链）——与本报告方向相反的"误发"：子事务从未真正流式下发却被打上 streamed 标、对未启用 streaming 的客户端也发 StreamAbort；由 v18 引入的 abort-discard 路径（072ee847ad4）导致，修复 commit [`aa4c52b8`](https://github.com/postgres/postgres/commit/aa4c52b808f76870f80189757af7217358544d60)（Backpatch 18）把打标收紧为"top 已 streamed 才给子事务打标"。同属 `RBTXN_IS_STREAMED` 标记语义区域，佐证该机制的边界条件在后续版本仍在收敛
- 官方文档：[Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)（流式与 abort 通知的既有语义）
- 配套测试与基座：`Pg17DefaultWorkMemTest` / `Pg17DefaultMemTestEnv`（it 包，机制注释完整）
