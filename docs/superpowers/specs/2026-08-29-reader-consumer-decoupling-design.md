# 读取与组装输出解耦设计（里程碑 1.7）

## 1. 背景与目标

里程碑 1.6 的端到端链路跑在**单一 reader 线程**上：`PgReplicationSession.run()` 的 `readPending()` → `onRaw()` → 组装 → 提交期回放 → `onTransaction()` 输出，全部同步串联（`PgReplicationSession.java` 的消息循环、`TransactionAssembler` 的提交分支）。后果：

- **提交点即停顿点**：大事务的回放（SPILLED 桶逐段回读 + 全量解码 + 渲染）耗时 O(事务大小)，期间 `readPending()` 一次不调，后续消息堆在 TCP 层（背压不丢数据，但服务端停发）
- **LSN 反馈停摆**：`forceUpdateStatus()` 在 `onRaw` 返回后才执行——回放超过 `wal_sender_timeout`（默认 60s）时服务端会认为 walsender 客户端失联而断开复制连接，这是当前形态最实际的隐患

**目标：读取路径与组装-输出路径解耦。** `pgoutput-reader` 线程只做"记账 + 写 Chronicle Queue（CQ）+ 反馈 LSN"，提交事务的回放与输出交给独立的消费器线程；CQ 从 1.6 的溢写池升级为 reader 与 consumer 之间的**主缓冲管道**。

**完成判据**：`mvn test` 全绿（既有用例期望值不变 + 新增用例）；解耦 IT（consumer 长睡眠期间 reader 持续接收）与 frontier IT（未输出事务不推进 confirmed_flush）通过；同一 raw 流同步/异步管道输出全等。

### 1.1 方案演进（为何是这个形态）

设计过程中评估过的三个候选：

| 候选 | 形态 | 结论 |
|---|---|---|
| 哑泵（dumb pump） | reader 零协议知识只 append；consumer 顺序回读 CQ 驱动完整组装器 | **否**：数据消息读两次（追进度遍 + 回放遍），且追进度遍与大事务回放在同一 consumer 线程上互相排队，输出延迟双重拉高。记账本可发生在 reader 收到消息的瞬间（字节已在内存，零额外读） |
| 仅输出卸载 | 1.6 组装器原地不动，只把回放交给 worker | **否**：MEMORY 桶交接后堆占用脱离阈值约束（consumer 慢则无界）；交接前强制转储又等于回到"全部进 CQ"还多一次提交期突发停顿 |
| **reader 记账 + 消费器角色（本设计）** | reader 保留 1.6 状态机做记账（数据全量 append 进 CQ，桶只记 index 段）；提交时交接冻结桶，consumer 只回放输出 | **采纳**：数据仅回放时读一次；组装期堆内零字节引用（只有元数据）；从 1.6 增量演进，状态机不动 |

## 2. 架构总览

```
PostgreSQL walsender
  │ CopyData/'w' WAL 帧
  ▼
reader 线程（pgoutput-reader，沿用）
  PgReplicationSession.run()            ←100ms readPending 轮询不变
  │ TransactionAssembler.onRaw()        ←1.6 状态机原班逻辑，仍在 reader 线程
  │   ├─ 每条消息先 pipe.append(raw)    ←返回的 CQ index 即 seq（§4.1）
  │   ├─ 控制消息/'R'：live 解码 → 桶记账 + 版本日志（照旧，不回读 CQ）
  │   ├─ I/U/D/T/M：窥 streamXid + oid → 桶记 CQ index 连续段 + oidSet
  │   └─ Commit/StreamCommit/CommitPrepared：handoff(桶) → 入交接队列 → 立即返回
  │ 反馈 LSN = min(已收到, 输出前沿 AtomicLong)        ←§6 frontier cap
  ▼
Chronicle Queue（MessagePipe：一条记录 = 一条完整消息，wipe-on-open 保留）
  ▼
consumer 线程（transaction-consumer，TransactionAssembler 内部）
  交接队列取桶 → state=OUTPUTTING → 逐段 readRange（数据仅此一读）
  → decodeSingle → 按桶内快照 asOf 渲染 → Transaction → ConsoleListener
  → 前沿 AtomicLong ← endLsn → state=DONE
```

要点：

- **记账发生在 reader 收到消息的瞬间**（live 解码控制消息 + 窥数据消息前缀），不经过 CQ 回读；CQ 里的数据字节只在 consumer 回放时读一次
- **控制消息也 append 进 CQ**（bookkeeping 仍用内存里的 live 解码结果，不回读）——目的唯一：建立 seq 时间线（§4.1）。垃圾字节代价每事务几十字节
- **桶纯 index 段记账**：1.6 的 MEMORY/SPILLED 双形态、`spillAll`、`memoryBytes` 水平、`vb.spill.thresholdBytes` 整套退役。解耦后 MEMORY 快路径的堆上限依赖"组装与回放同线程"，交接给慢 consumer 时堆会无界；CQ 就是缓冲，堆内只有元数据，组装期有界性不降反升
- **失败语义与 1.6 对齐**：任何一侧 fail-fast 都终止整个进程（冒烟阶段语义），槽保留、重启续传

## 3. 桶状态机与两个低水位

### 3.1 桶四态

```
组装中 ──Commit/StreamCommit/CommitPrepared──▶ 组装完成 ──consumer 出队──▶ 输出中 ──回调返回──▶ 输出完成
(LIVE)                                        (HANDED_OFF)             (OUTPUTTING)          (DONE)
```

- **写侧归属**：reader 写状态到 HANDED_OFF 为止（交接即冻结——此后桶的段信息、xid、oidSet 等字段不再变化）；consumer 接手写 OUTPUTTING / DONE。唯一跨线程可变字段是 `volatile state`
- **交接后的桶集合只由 reader 动**：consumer 只改桶状态、不动集合；reader 算低水位时顺带把 DONE 桶踢掉（惰性清理，无锁）
- 2PC 挂起桶（PREPARE 至 COMMIT/ROLLBACK PREPARED 之间）属 **LIVE**——reader 仍持有，同时约束两个低水位
- 白赚收益：状态计数即积压指标（HANDED_OFF 堆积 = consumer 跟不上；OUTPUTTING 恒 ≤1，单消费者）

### 3.2 两个低水位（作用域不同，勿混）

| 低水位 | 计算 | 驱动 |
|---|---|---|
| **CQ 删除低水位** | min(所有**状态 ≠ DONE** 桶的 firstIndex)——LIVE / HANDED_OFF / OUTPUTTING 三态都算 | `MessagePipe.releaseBelow` 删过老滚动文件 |
| **registry 剪枝低水位** | min(**LIVE** 桶的 firstIndex)（2PC 挂起桶算 LIVE） | `VersionedRelationRegistry.pruneBelow` |

差异的依据：consumer 回放用的是交接时拷走的 Relation 版本快照（§4.3），**不碰 reader 的 registry**——"组装完成"之后的桶不约束剪枝；但它们的 CQ 段仍可能被回放，必须约束文件删除。不做这个区分，慢 consumer 会撞上文件删除（`readRange` 起点错位 ISE）。

## 4. 组件

### 4.1 TxBuffer（桶，改造）与 seq ≡ CQ index

冻结时携带的全部字段（1.6 的 `Mode`/`units`/`bytesTotal` 删除）：

| 字段 | 来源 | 用途 |
|---|---|---|
| `state`（volatile 枚举） | reader 写到 HANDED_OFF，consumer 写后两态 | 低水位过滤 + 惰性清理 |
| `xid` / `gid` | 记账期 | 封箱 Transaction |
| `kind` / `commitLsn` / `endLsn` / `commitTimestamp` | 提交控制消息 live 解码时捕获 | Transaction 元数据 + 前沿上报 |
| `segments`（`long[2]` 列表） | 追加期连续段记账 | readRange 端点 |
| `firstIndex` / `lastIndex` | 首末单元 CQ index | 删除低水位 / 快照截止（maxSeq） |
| `abortedSubxids`（Set\<Long\>） | StreamAbort 记账 | 回放期子事务过滤 |
| `oidSet`（Set\<Long\>） | 追加期窥 I/U/D/T 的 relation oid（T 为多 oid） | 交接时圈定快照范围 |
| `relationSnapshot` | **交接时**由 reader 从 registry 拷贝 | consumer 渲染（§4.3） |

**seq ≡ CQ index**：每条消息（含控制消息）append 后拿到的 index 就是它的 seq。数据单元与 'R' 版本天然同序，asOf 二分查找的正确性由构造保证；`firstIndex` 兼任 1.6 的 `minSeq`。`nextSeq` 计数器退役。

**段连续性规则**：上一次 append（含控制消息）是本桶数据消息才顺延当前段，否则新开段——控制消息插入即断段。一段 `[first, last]` 内全部是同桶数据单元（构造保证，readRange 无需甄别）。

### 4.2 MessagePipe（原 MessageSpool 改名）

- `append(byte[] payload) → index`（**reader 线程**）；`readRange(first, last, BiConsumer<Long, byte[]>)`（**consumer 线程**）——签名改为携带每条自己的 index（作 seq，asOf 用）。信封帧全删（`SpoolFrame` 退役），一条 CQ 记录 = 一条完整消息，回读时重窥类型字节/流式前缀恢复 streamXid（1.6 `BucketReplayer` 消费契约本就如此）
- 结构不变：单 appender + 单 tailer（tailer 仅被 consumer 的 readRange 用，`moveToIndex` 定位）；**跨线程分工变更**：append 与 readRange 分属两线程——CQ 官方支持（appender/tailer 各自单线程使用即可），类 javadoc 重写线程约束
- `wipe-on-open`（真源是复制槽）、`releaseBelow` 保守删档、`lastAppendedIndex`、close 顺序（tailer → appender → queue）原样保留；readRange 起点错位 ISE 保留——状态机低水位失效时的最后防线

### 4.3 RelationSnapshot（新，不可变值对象）

`oid → (seq, Relation) 版本列表` 的不可变快照，由 reader 在交接瞬间经 `VersionedRelationRegistry.snapshot(oidSet, ≤ lastIndex)` 拷出（registry 上新增方法）。向 `BucketReplayer` 提供与 registry 同形的 `require(oid, asOf)`（二分，未命中 ISE fail-fast 不变）。通常每 oid 一版，快照几十字节。**这是 consumer 不共享 reader registry 的关键**——registry 保持单写者（reader），跨线程零并发改造。

### 4.4 TransactionAssembler（双角色）与 TransactionConsumer（新，包私有）

**reader 面（现有 `onRaw`，改动点）**：

- 每条消息先 `pipe.append`（拿 index 作 seq），再做原有记账路由；数据消息多一步 oid 窥探（类型字节及可选流式 xid 前缀之后的 relationOid：I/U/D 单 oid、T 为 oid 数组、M 无）入桶的 `oidSet`
- **提交三分支（1.6 的 ：439/:497/:563）改为 `handoff(bucket)`**：拷快照 → 冻结 → `state=HANDED_OFF` → 入 `handedOff` 列表（reader 私有）→ 入交接队列 → **立即返回**。内联 `replay()` 从 reader 路径消失
- 桶完结点照旧算两个低水位（§3.2）

**consumer 角色 = `TransactionConsumer`（独立类，可单测）**：

- 循环：交接队列 `take()` → `state=OUTPUTTING` → 逐段 `readRange` → `BucketReplayer`（用桶内快照）→ 封箱 `Transaction` → `listener.onTransaction` → 前沿以 endLsn 单调累加 → `state=DONE`
- 交接队列 `LinkedBlockingQueue<TxBuffer>`（无界——元素只有元数据，真缓冲是 CQ）
- 消费异常：ERROR + 失败回调（`stop::countDown`）+ 退出不排干（fail-fast）；捕捉 `Throwable` 防 consumer 静默死亡导致 reader 无限追加
- assembler 负责装配默认线程形态（内部创建 `transaction-consumer` 线程）；`close()`（reader 在 try-with-resources 调）：投毒丸 → consumer 排干余下桶（全部到 DONE）→ join → `pipe.close()`。**停机时已提交未输出的事务不丢**
- 线程约束重写：`onRaw`/`close` 单写者（reader）；consumer 只触碰冻结桶 + 交接队列 + pipe tailer + 前沿 AtomicLong——共享面精确枚举

`decodedObserver`（ConsoleListener 逐消息挂点）变为两线程调用（reader 的控制消息 live 解码 + consumer 的回放解码）——ConsoleListener 是无状态 slf4j 日志，线程安全，javadoc 注明即可。

### 4.5 PgReplicationSession（最小改动）

`run` 增加重载 `run(listener, LongSupplier outputFrontier)`：反馈处 `setAppliedLSN/setFlushedLSN(min(received, frontier))`，frontier 为 0 视为无 cap（首个事务输出前与 1.6 行为一致）。轮询节奏、isClosed 守卫、断连语义全部不动。

### 4.6 Main 装配与停机次序

```java
AtomicLong frontier = new AtomicLong();
try (session) {
    try (assembler = new TransactionAssembler(console, mode, registry,
            pipeConfig, decodedObserver, frontier, 失败回调 = stop::countDown)) {
        reader 线程: session.run(assembler, () -> frontier.get());
    }   // ← reader 退出后此处排干 consumer
}
```

- **正常停机**（Ctrl+C）：stop latch → `session.close()`（reader ≤100ms 退出，不再有新 append/交接）→ assembler.close 排干 → pipe 关闭 → 进程退出。槽保留，重启从 cap 后的 confirmed_flush 重发
- **consumer 失败**：ERROR + countDown + consumer 退出（不排干）；Main 走停机路径，close 感知 consumer 已死则跳过排干直接关 pipe
- **reader 失败**（断连）：与 1.6 相同（保槽、ERROR、倒计时停机），差异仅在于 close 时先把已交接事务排干输出

## 5. LSN 反馈语义：按输出前沿封顶

解耦后"已收到"与"已输出"之间隔着 CQ 积压。反馈锚定**输出前沿**：consumer 每输出完一个事务，把 endLsn 单调累加进 `AtomicLong`；reader 反馈 `min(received, frontier)`。

- **crash 丢失窗口为零**：未输出事务必然被 PG 重发（at-leat-once；console 可能重复输出已见事务，不做去重，文档化）
- reader 永不被阻塞（只读一个原子变量）；status 包照发，consumer 停摆不会触发 `wal_sender_timeout` 断连（服务端只要求 status 到达，不要求 LSN 前进）
- 代价：consumer 停摆/极慢时 confirmed_flush 不前进，PG 侧 WAL 保留增长（`max_slot_wal_keep_size=2GB` 兜底）+ 我方 CQ 磁盘增长——均靠监控 WARN 提前告警（§7）

## 6. 错误处理矩阵

| 故障 | 行为 | 备注 |
|---|---|---|
| consumer 回放/解码异常 | ERROR + `stop::countDown` + consumer 退出（不排干） | 与 1.6 "异常上抛终止会话"等价 |
| reader 断连 | 与 1.6 相同：保槽、ERROR、倒计时停机；close 先排干已交接事务 | 已收未输出的事务不丢 |
| CQ append 失败（磁盘满/IO） | CQ 运行时异常沿 onRaw 上抛 → reader 失败路径 | **新引入故障面**：CQ 成主缓冲后磁盘满 = 全局停机信号，fail-fast 是设计行为 |
| readRange 起点错位 ISE | fail-fast 崩溃 | 状态机低水位失效的最后防线（理论上不可达） |
| kill -9 / 进程崩溃 | 重启：CQ wipe-on-open 清空，PG 从 confirmed_flush（frontier 封顶值）重发未输出事务 | at-least-once |
| consumer 极慢/停摆 | reader 照常读照常反馈（值被 cap 住）；CQ 磁盘与 WAL 保留增长 | WARN 告警 + max_slot_wal_keep_size 兜底 |
| 2PC 长期挂起 | 桶保持 LIVE，约束两个低水位 | 1.6 语义原样 |

## 7. 可观测性

consumer 线程周期性（10s 固定周期）一行 INFO：各状态桶计数（LIVE / HANDED_OFF / OUTPUTTING）+ 最老 HANDED_OFF 桶滞留时长；滞留超 60s 升 WARN。frontier 值并入 reader 现有反馈 DEBUG 行。两个常量不做配置面（YAGNI，需要时再提）。

## 8. 配置与删除清单

- `vb.pipe.dir`（默认 `pipe-queue`）/ `vb.pipe.rollCycle`（默认 `MINUTELY`，`LegacyRollCycles` 枚举名，大小写宽容）；`SpillConfig → PipeConfig`
- `vb.spill.*` 三项退役（thresholdBytes / dir / rollCycle）；纯内存逃生门删除——CQ 是本架构地基，无"绕过 CQ"形态（那等于回到 1.6 同步阻塞）
- 删除：`TxBuffer.Mode` 与 hybrid 机制、`spillAll`、`memoryBytes`、`PayloadUnit`、`SpoolFrame`（及其单测——经 CQ 往返的等价用例改以 raw 字节驱动重建）

## 9. 测试策略

**关键设计决定：consumer 逻辑独立成 `TransactionConsumer`，既有 33+ 组装器单测以"同线程消费"模式驱动**（喂完 `onRaw` 后手动调 `processNext()`），断言期望值原样存活——它们就是 1.6 输出的固化基线；确定性、无锁、无 latch。

1. **等价性验收**：同一录制 raw 流（bench 语料 / SessionHarness 双轨录制）分别过**同步消费**与**真实双线程**管道，断言 Transaction 序列全等——同步路径锚定 1.6 期望，异步路径锚定同步路径
2. **新增单测**：控制消息断段规则；HANDED_OFF 桶对 releaseBelow 的保护；快照 asOf（事务内 DDL 前后段按各自版本渲染，BucketReplayer 级手造）；readRange 携带 index；前沿单调累加与 cap 边界（0 = 无 cap）
3. **新增 IT（Testcontainers）**：
   - **解耦目标验证（头名测试）**：listener 在 `onTransaction` 内 sleep 数秒，期间持续写入——断言 reader 的接收计数持续增长（读不被输出阻塞）
   - **frontier cap 验证**：阻塞 consumer → 提交事务 → 等反馈周期 → 查 slot `confirmed_flush_lsn` 未越过该事务 endLsn → 放行 → 断言推进越过（两段式断言，仿既有 feedback 测试）
   - 既有 9 组场景（流式交错 / 2PC / 事务内 DDL / 回滚后删档）改经解耦管道跑通
4. **JMH**：pipe.append（reader 路径）、readRange 回放口径、RoutePeek 增 oid 窥探分量；`docs/benchmarks-baseline.md` 补 1.7 段作回归对照

## 10. 非目标

- 输出仍是 ConsoleListener——不做 Logstash 集成、不做下游投递
- 单消费者线程——不做并行输出/多管道（全局 LSN 序）
- 事务仍是整体块输出——回放期 O(事务大小) 瞬态堆不变，流式输出属里程碑 2
- 不做背压硬上限——reader 永不因 consumer 慢而阻塞（这是目标本身）；磁盘与 WAL 保留就是缓冲，靠 WARN + `max_slot_wal_keep_size` 兜底
- CQ 不跨重启复用——wipe-on-open 保留，真源是复制槽
- at-least-once 语义只文档化，不做去重

## 11. 交付物

- `replication` 包：`MessagePipe`（改名 + 双线程改造）、`RelationSnapshot`、`TransactionConsumer`、`TxBuffer` 状态机化、`TransactionAssembler` 双角色化、`PipeConfig`；删除 `PayloadUnit`/`SpoolFrame`/`SpillConfig`
- `PgReplicationSession.run` 重载（frontier cap）；`Main` 装配更新
- 测试：既有组装器单测改造为同步消费驱动 + 新单测 + 解耦/frontier 两组 IT + 等价性验收
- 文档：根/`replication` CLAUDE.md、README、`docs/benchmarks-baseline.md`（1.7 段）同步
