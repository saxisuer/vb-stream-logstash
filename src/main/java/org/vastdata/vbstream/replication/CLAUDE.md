# replication/ 模块——复制会话、raw 接缝与解耦事务组装（IO + 组装层）

职责：管理两条 JDBC 连接（普通 SQL + 复制专用），把复制流按**原始字节**交付上层（raw 接缝，里程碑 1.6 起）；本包同时承载 raw 驱动的 `TransactionAssembler`（1.7 起 reader/consumer 双线程解耦：**reader 记账 + Chronicle Queue 主缓冲管道 + 独立消费器回放输出**）。上游 `Main` 依赖 `PgReplicationSession` + `TransactionAssembler`（raw 接缝直连）；集成测试 `SessionHarness` 双轨录制（raw + 经 `DecodedMessageBridge` 的解码消息）。

## 1.7 数据流（解耦形态，设计文档 `docs/superpowers/specs/2026-08-29-reader-consumer-decoupling-design.md`）

```
reader 线程（pgoutput-reader，沿用 1.6 名）
  PgReplicationSession.run(listener, frontier)   ←100ms readPending 轮询；反馈 = min(收到, 前沿)
  │ TransactionAssembler.onRaw(raw)
  │   ├─ 每条消息先 pipe.append(raw)            ←返回的 CQ index 即 seq（seq ≡ index）
  │   ├─ 控制消息/'R'：live 解码 → 桶状态机/'R' 记版本日志（不回读 CQ）
  │   ├─ I/U/D/T/M：窥前缀定性 + 窥 oid 入 oidSet → 桶记 CQ index 连续段
  │   └─ Commit/StreamCommit/CommitPrepared：handoff(桶)（拷快照→冻结→入队）→ 立即返回
  ▼
Chronicle Queue（MessagePipe：一条记录 = 一条完整消息，wipe-on-open）
  ▼
consumer 线程（transaction-consumer，TransactionAssembler 内部创建）
  TransactionConsumer：交接队列 take 桶 → 逐段 readRange → BucketReplayer（桶内快照渲染）
  → 封箱 Transaction → listener.onTransaction → 前沿 AtomicLong ← endLsn → state=DONE
```

记账发生在 reader 收到消息的瞬间（live 解码控制消息 + 窥数据消息前缀），CQ 里的数据字节只在 consumer 回放时读一次；提交点不含回放，reader 永不被输出耗时阻塞，LSN 反馈不因大事务回放停摆（1.7 的两个动因）。

## PgReplicationSession（核心，AutoCloseable）

生命周期严格按 **open → ensureSlot → start → run → close**；`open` 失败会自行回收半开连接（防泄漏）。

- **`open()`**：建两条连接——普通 `config.jdbcUrl()` 与 `config.replicationUrl()`（带 `replication=database`）。复制连接要求见 ReplicationConfig。
- **`ensureSlot()`**：`SELECT pg_create_logical_replication_slot(槽名, 'pgoutput', false, twoPhase)` **幂等建槽**；捕获 SQLState `42710`（duplicate_object）视为"已存在，直接复用"并打 WARN（提示槽的 two_phase 属性需与配置一致，不一致将由 start 时服务端报错），其余异常上抛。
- **`start()`**：经 `PGConnection.getReplicationAPI()` 建 `PGReplicationStream`，slot options **四项**：`proto_version`、`publication_names`、`streaming`（OFF→"off"/ON→"on"/PARALLEL→"parallel"）、`two_phase`（on/off）；另经 `withStartPosition(INVALID_LSN)` 从槽当前确认点续传、`withStatusInterval` 设状态回传周期。
- **`run(RawMessageListener)` / `run(RawMessageListener, LongSupplier outputFrontier)`**：**轮询式消息循环（100ms readPending 非阻塞轮询），由调用方线程执行**（Main/harness 中是名为 `pgoutput-reader` 的线程）。双参重载（1.7）把 LSN 确认**按输出前沿封顶**：每轮反馈 `capFeedback(received, frontier) = frontier ≤ 0 ? received : min(received, frontier)`——frontier=0 视为无 cap（首个事务输出前与 1.6 行为一致）；单参重载即恒不封顶的兼容形态。**会话只做字节交付**：每条消息的完整字节（含类型字节与流式块内可选 Int32 xid 前缀）拷入**独占新建数组**回调 `listener.onRaw(raw)`（调用方可无复制长期持有）；解码与 Relation 缓存完全移出 session（由组装器或桥承担），自身不触碰协议层。frontier 只在 reader 线程每轮读一次（AtomicLong 读，永不被 consumer 阻塞）。声明 `throws SQLException, IOException`：
  1. 每轮先查 `stream.isClosed()`（断连快速感知，抛描述性 `SQLException`）；`stream.readPending()` 非阻塞取消息（null=暂无消息属正常，sleep 100ms 后继续；非 null 但 remaining()==0 的载荷防御性跳过不回调）
  2. 回调 `listener.onRaw(raw)`（同步，回调耗时直接拖慢消息循环——1.7 起回放已不在回调里，onRaw 只做记账）
  3. 每轮 `setAppliedLSN/setFlushedLSN(capFeedback(...))`；每满一个反馈周期 `forceUpdateStatus()` 上报确认位点
  - **为什么轮询而非阻塞 `read()`**（实测 pgjdbc 42.7.13 + PG 18）：阻塞 read 空闲期不按 statusInterval 醒来，status 依赖服务端 keepalive（~wal_sender_timeout/2，默认约 30s）才触发；轮询使 status 周期独立于消息到达（反馈间隔=feedbackIntervalSeconds，`pg_stat_replication.flush_lsn` 及时反映客户端进度），且断连感知更快（isClosed 每轮检查）
  - **反馈封顶的语义（设计 §5）**：确认锚定**输出前沿**（consumer 已输出事务的最大 endLsn）而非读取进度——crash 时未输出事务必然被 PG 重发（at-least-once，console 可能重复输出已见事务，不去重）；consumer 停摆不会触发 `wal_sender_timeout` 断连（服务端只要求 status 到达，不要求 LSN 前进），代价是 PG 侧 WAL 保留与 CQ 磁盘增长（`max_slot_wal_keep_size` 兜底 + consumer 周期 WARN 告警）
  - **confirmed_flush_lsn 的服务端行为（Diag 实证，勿再当 bug 排查）**：standby status 到达后先被采纳进 `pg_stat_replication.flush_lsn`；槽的 `confirmed_flush_lsn` 由 walsender 在解码推进时（candidate 机制）落库——空闲期不推进，但确认不丢失，下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点。集成验证见 `NormalTransactionTest.feedbackIsAdoptedByServerAndConfirmedFlushAdvances` 与 `FrontierCapTest`（封顶/解封两段式）
- **`close()`**：顺序 流 → 复制连接 → SQL 连接；关闭后 run 循环在下一轮 `isClosed()` 检查（≤100ms）抛 SQLException 退出。各步失败仅 WARN 不上抛。
- **`lastReceiveLsn()`**：供异常退出时打印续传位点；流未启动返回 INVALID_LSN。**`config()`**：暴露配置（harness 日志用）。

日志：生命周期 INFO（连接建立/槽已创建/流已启动/会话已关闭）、槽复用与关闭失败 WARN、LSN 反馈 DEBUG。

## 交付接缝：RawMessageListener（现行）与 PgOutputListener（旧，保留）

双契约并存，新增的是并行接缝而非替换：

- **`RawMessageListener.onRaw(byte[] raw)`**：raw 字节消费者（现行接缝）。`raw` 是完整单条 pgoutput 消息字节——含首个类型字节与其后全部字段（流式块内消息含可选 Int32 xid 前缀），解码器/组装器可直接消费。回调线程 = 调用 run() 的线程（同步）；抛出的异常经 run 循环原样上抛终止会话线程。`TransactionAssembler` 实现此接口。
- **`PgOutputListener.onMessage(message, lookup)`**：解码后消息契约（里程碑 1 形态），保留——里程碑 2 的输出队列与既有解码测试仍走此契约。**参型自 `RelationRegistry` 放宽为 `RelationLookup`**（1.7 设计 §4.3）：live 解码点传 registry（reader 线程最新视图）、回放解码点传桶内不可变快照，实现方对 Relation 来源无关，跨线程竞争由接口形态消除。
- **`DecodedMessageBridge`**：raw → 解码契约的适配器。自持一套 `PgOutputDecoder`（含流块状态机 inStream）与 `RelationRegistry`，`onRaw` → `decode` → `registry.accept` → `target.onMessage`，与接缝改造前 run 循环内置链路**逐字节等价**（`RawSessionContractTest` 验证）。同一桥实例只能重放**一条**按序消息流（inStream 状态）；构造需传与 START_REPLICATION 一致的 StreamingMode（否则 StreamAbort 附加字段解析错位 fail-fast）。`registry()` 暴露桥持有的 Relation 缓存（ConcurrentHashMap，跨线程查询安全）。非线程安全。

## ReplicationConfig（record，不可变）

11 个分量的配置模型；`fromSystemProperties()` 以 `vb.pg.*` 前缀读取系统属性，默认值对准 `src/docker` compose 环境（localhost:55432 / postgres 库 / 槽 vb_cdc_slot / publication vb_pub / proto 4 / streaming parallel / twoPhase true / 反馈 10s）。

- **`replicationUrl()`**：`jdbcUrl() + "?replication=database&assumeMinServerVersion=9.4"`——pgjdbc 规定 replication 连接必须同时带 `assumeMinServerVersion>=9.4` 才会把 replication 参数放进启动包，否则 `START_REPLICATION` 被服务端按普通 SQL 解析报语法错（真实 PG 18 首跑踩过）
- **`streamingParam()`**：StreamingMode → START_REPLICATION 参数值字符串

## RelationRegistry 与 VersionedRelationRegistry（oid → Relation）

协议保证 Relation 消息先于同表 DML 到达（含流式块内的重复下发）。

- **`RelationRegistry`**（父类，实现 `RelationLookup`）：`accept(message)` 只认 Relation 消息、按 oid 覆盖式 put（DDL 后服务端重发新版本即翻新）；`find(oid)` 返回 Optional（宽松视图，miss 返回 empty）；`require(oid)` 未命中抛 `IllegalStateException`（fail-fast——缓存 miss 意味着协议流异常）。`ConcurrentHashMap`，为 listener 回调跨线程查询设计；`DecodedMessageBridge` 持有的是它。
- **`VersionedRelationRegistry`**（子类，组装器 reader 侧用）：oid → `(seq, Relation)` **版本日志**——存在动机：DDL 会让服务端在流中重发同 oid 新版 Relation，回放旧单元必须按"变更时刻"版本渲染，取最新版会把旧行按新 schema 错解。要点：
  - `accept(seq, rel)`：按 oid 追加版本、维持 seq 升序；同 seq 重复投递幂等跳过
  - `require(oid, asOfSeq)`：**二分取 ≤ asOfSeq 的最新版**；无版本或全部晚于 asOfSeq 抛 ISE（沿用"Relation 未先行到达"fail-fast）
  - `pruneBelow(minSeq)`：以"最低仍会被查询的 seq"为低水位剪枝——各 oid 保留 asOf=minSeq 时刻**生效**的版本（floor，其自身 seq 可早于 minSeq：'R' 恒先于首个 DML，存活桶旧单元会解析到低水位之前记入的版本——字面"丢弃 seq < minSeq"实现会在并发 DDL 流形下误剪崩回放）及其后全部；minSeq 之后无任何版本时整列保留（每 oid 至少留最新一条由此自然成立）。**已接线非死代码**：组装器在桶完结点以全部**存活**桶（LIVE，含 2PC 挂起）firstIndex 最小值调用——**不含已交接桶**（快照自足，设计 §3.2），版本日志低水位剪枝后有界
  - `snapshot(oids, maxSeq)`（1.7）：把指定 oid 集合在 maxSeq 时刻已生效的版本前缀拷成不可变 `RelationSnapshot`，供桶交接时随行冻结（见下）
  - 继承的 `accept(PgOutputMessage)`/`find`/`require(oid)` 委托最新版本（旧接缝行为不变；旧接缝以合成 seq 记入时间线末尾，与带 seq 接缝不应混用）
  - **线程约束不同于父类**：单写者假设（reader 线程串行调用全部方法），用 HashMap 而非 ConcurrentHashMap；跨线程查询场景请用父类或 `RelationSnapshot`

## RelationLookup 与 RelationSnapshot（consumer 不共享 registry 的两块拼图，设计 §4.3）

- **`RelationLookup`**（公共接口）：`Optional<Relation> find(int oid)`——宽松查询视图（miss 返回 empty，供渲染降级 "oid:N"）。存在动机：逐消息渲染的 Relation 来源随线程不同（live 解码点 = reader 线程的版本日志最新视图；回放解码点 = consumer 线程的桶内不可变快照），本接口是两者的公共形态，使 `PgOutputListener`/`ConsoleListener.onMessage` 不依赖具体 registry 实现——若回放侧闭包引用 reader 的 HashMap registry 即构成数据竞争。
- **`RelationSnapshot`**（包私有，不可变）：`oid → (seq, Relation) 版本前缀` 的快照，由 reader 在交接瞬间经 `VersionedRelationRegistry.snapshot(oidSet, ≤ lastIndex)` 拷出（浅拷，Relation record 引用安全共享；通常每 oid 一版，几十字节）。`require(oid, asOfSeq)` 二分取 ≤ asOfSeq 的最新版（语义与版本日志对齐，被快照省略的 oid 以"未先行到达"fail-fast，报错时机与 1.6 直查 registry 一致）；`find` 是快照内最新版宽松视图（实现 `RelationLookup`）。**这是 consumer 不共享 reader registry 的关键**——registry 保持单写者（reader），跨线程零并发改造。

## MessagePipe（包私有，Chronicle Queue 主缓冲管道，AutoCloseable）

reader 与 consumer 之间的**主缓冲**（1.7 设计 §4.2，原 `MessageSpool` 演进：信封帧全删，一条 CQ 记录 = 一条完整 pgoutput 消息，含控制消息——目的唯一：建立 seq 时间线）。CQ 生命周期管理者，字节语义归调用方（本类只搬 byte[]）。

- `append(byte[] payload) → index`（**reader 线程**）：原样落盘并返回该条 CQ index（单调递增，即该消息的 seq，可作回读区间端点与低水位入参）；写失败以 CQ 运行时异常上抛（不吞不重试——磁盘满 = 全局停机信号，fail-fast 是设计行为）
- `readRange(firstIndex, lastIndex, BiConsumer<Long, byte[]>)`（**consumer 线程**）：按 index 升序回读闭区间逐条回调——回调参数携带该条**自己的真实 CQ index**（作 seq，asOf 用）与队列内存不共享的字节副本；区间起点错位（已被误删/从未存在）抛 ISE fail-fast；区间不存在且无后续条目时空手而归不抛
- `releaseBelow(lowestNeededIndex)`：删除严格低于 needed cycle - 1 的滚动文件（保留 needed 与 needed-1 档，保守删除）；每删一个 WARN 留痕、单文件失败 WARN 重试（残留只占磁盘不影响正确性）；候选集计算 `deletableFiles` 纯函数化（供单测注入文件名）
- `lastAppendedIndex()` / `close()`（tailer → appender → queue 逆序，失败 WARN 吸收）
- **wipe-on-open（真源是复制槽）**：构造时先递归清空目录内容再建 `SingleChronicleQueue`——重启后 PG 从确认位点重发，残留旧管道数据有害（陈旧 index 会让回读错位），**重启自动清空属预期行为**；推论：管道目录在进程内独占，同 JVM 第二实例指向同一目录会清掉前者的队列文件
- **线程约束（跨线程分工，CQ 官方支持的用法）**：append/lastAppendedIndex/releaseBelow/close 由 reader 线程调用，readRange 由 consumer 线程调用；appender 与 tailer 均为构造时创建的单实例资源，各自单线程使用，不得交叉线程调用

日志：建/关管道 INFO、删档留痕与各步失败 WARN。

## TxBuffer（桶）与桶状态机

组装桶（设计 §4.1）：**纯 CQ index 段记账**——桶内不持有任何 payload 字节，堆占用只有元数据（`ArrayDeque<long[]>` 段列表 + oid/aborted 集合 + 封箱元数据）。数据消息字节只在 reader 追加时写一次 CQ、consumer 回放时读一次。

**四态生命周期（volatile `state`，写侧按状态分段）**：

```
LIVE ──Commit/StreamCommit/CommitPrepared──▶ HANDED_OFF ──consumer 出队──▶ OUTPUTTING ──回调返回──▶ DONE
(reader 写)                                  (reader 写，交接即冻结)       (consumer 写)            (consumer 写)
```

- **交接即冻结**：handoff 瞬间 reader 把封箱元数据（kind/commitLsn/endLsn/commitTimestamp）与 `relationSnapshot` 写入冻结字段、state 推进 HANDED_OFF，此后除 `state` 外全部字段终生不变——consumer 线程只读消费。`state` 是唯一跨线程可变字段（volatile）：reader 写 LIVE→HANDED_OFF，consumer 写后两态
- 2PC 挂起桶（PREPARE 至 COMMIT/ROLLBACK PREPARED 之间）属 **LIVE**——reader 仍持有，同时约束两个低水位
- 白赚收益：状态计数即积压指标（HANDED_OFF 堆积 = consumer 跟不上；OUTPUTTING 恒 ≤1，单消费者）——`TransactionConsumer` 周期统计透出

**seq ≡ CQ index**：每条消息（含控制消息）append 后拿到的 index 就是它的 seq。数据单元与 'R' 版本天然同序，asOf 二分查找的正确性由构造保证；`firstIndex` 兼任 1.6 的 `minSeq`（`nextSeq` 计数器退役）。

**段连续性规则**：上一次全局 append（含控制消息）是本桶数据消息才顺延当前段，否则新开段——控制消息插入即断段。一段 `[first, last]` 内全部是同桶数据单元（构造保证，readRange 无需甄别）。

**hasPrefix（桶级不变量）**：流式桶的单元恒在流块内收到（payload 带 4 字节 xid 前缀）、普通/两阶段桶恒在块外（无前缀）；首单元追加时按流块上下文定型，此后混现即 ISE fail-fast（协议不允许，防御）。回放据此决定 `decodeSingle` 的 inStream 实参，前缀的**值**回放时重窥 `raw[1..4]` 作 streamXid（子事务过滤用）——**有无**无法从裸 payload 判定，这正是桶级不变量存在的原因。

**两个低水位（作用域不同，勿混，设计 §3.2）**：

| 低水位 | 计算 | 驱动 |
|---|---|---|
| **CQ 删除低水位**（`pipeWatermark()`） | min(所有**状态 ≠ DONE** 桶的 firstIndex——LIVE/HANDED_OFF/OUTPUTTING 三态都算， maxAppendedIndex+1) | `MessagePipe.releaseBelow` 删过老滚动文件 |
| **registry 剪枝低水位** | min(**LIVE 存活**桶的 firstIndex)（2PC 挂起桶算 LIVE） | `VersionedRelationRegistry.pruneBelow` |

差异依据：consumer 回放用交接时拷走的版本快照，**不碰 reader 的 registry**——"已交接"之后的桶不约束剪枝；但它们的 CQ 段仍会被回放，必须约束文件删除。不做区分，慢 consumer 会撞上文件删除（readRange 起点错位 ISE）。两个低水位都挂在桶完结点（交接/整桶丢弃）；DONE 桶由 reader 在下一个完结点惰性清出交接记账。

## TransactionAssembler（raw 驱动的事务组装状态机，AutoCloseable，双角色）

实现 `RawMessageListener`。核心思路：**数据消息不解码直接记账**——原始字节 append 进管道，桶只记 CQ index 段；控制消息和 Relation 当场解码驱动状态机；解码推迟到 consumer 回放（回滚的大事务从未被解码过，提交的事务也只解一次）。

- **路由（每条 onRaw，reader 线程）**：先 `pipe.append(raw)` 取 seq（每条消息一个——含控制消息与 'R'），再按类型字节分流：
  - 控制消息（B/C/S/E/c/A/b/P/K/r/p）与 'R'：**live 解码**（自持 decoder 顺带维护 inStream 流块状态机，与 `currentStream` 指针同点同变）后分发到桶状态规则；'R' 以到达 seq 记入版本日志，字节不占桶（其 append 仍是 seq 时间线的一环）
  - I/U/D/T/M：**窥探不入桶不解码**——校验桶级 hasPrefix 不变量（首单元定性）、窥 relation oid 入 oidSet（I/U/D 单 oid、T 为 oid 数组、M 无）、把 index 记入当前活动桶的连续段（没有活动桶 fail-fast）；'M' 先窥 flags bit0 分事务性/非事务性（非事务性无桶 WARN 丢弃）
  - Y/O：DEBUG 记录后丢弃；未知类型经 decoder 抛 UnknownMessageTypeException（解码层 fail-fast）
- **桶模型**：普通事务单指针 `currentNormalTx`（Begin..Commit 串行不嵌套）；流式多桶 `streamedByXid`（key=StreamStart 顶层 xid）+ 流块上下文 `currentStream`（段间交错、流块不嵌套）；两阶段 `currentPrepareTx` 活动 + `preparedByGid` 挂起池（PREPARE 至 COMMIT/ROLLBACK PREPARED，可能长期挂起）。**StreamAbort**：top==sub 整桶丢弃（管道条目成垃圾，随低水位删除）；否则记入桶的 `abortedSubxids`，**回放期过滤**。桶缺失/重复/流块状态异常一律 ISE fail-fast
- **提交路径 = `handoff(bucket)`**（设计 §4.4）：拷快照（`registry.snapshot(oidSet, lastIndex)`）→ 捕获封箱元数据 → `state=HANDED_OFF` → 入 `handedOff` 记账（低水位钉住）→ 入交接队列（异步）或直调 `consumer.processBucket`（同步测试形态）→ 维护两个低水位 → **立即返回**。回滚路径（RollbackPrepared/StreamAbort 整桶）丢弃不回调，完结点同样维护低水位
- **close() = 毒丸排干协议**（异步形态）：毒丸（`TxBuffer.POISON`）入队 → join consumer（60s 超时 WARN 放行）→ 关管道。FIFO 保证 consumer 先排干此前交接的全部冻结桶再见到毒丸——**停机时已提交未输出的事务不丢**。同步形态无 consumer 线程，直接关管道
- **线程约束（红线）**：`onRaw`/`close` 单写者（reader 线程——decoder 的流块状态、全部桶指针、registry、pipe appender 都要求）；consumer 只触碰冻结桶 + 交接队列 + pipe tailer + 前沿 AtomicLong。不可与 onRaw 并发调 close
- 日志：非事务性 M 丢弃、RollbackPrepared 丢弃 WARN；Y/O 丢弃与 PREPARE 入挂起池 DEBUG

构造两形态：**异步**（Main 用：`(listener, mode, registry, pipeConfig, decodedObserver, outputFrontier, onFailure)`——构造即建管道并起非守护 `transaction-consumer` 线程）与**同步**（测试锚定 1.6 期望：`(listener, mode, registry, pipeConfig, decodedObserver)`——handoff 在调用线程直调 processBucket，无线程拆分；另有 observer 缺省便捷构造）。`decodedObserver` 是 `BiConsumer<PgOutputMessage, RelationLookup>`（ConsoleListener 逐消息挂点），live 解码点（reader）传 registry、回放解码点（consumer）传桶快照。

## TransactionConsumer（包私有，消费器循环）

从组装器抽出单独成类，为的是既有单测能以"同线程消费"驱动（直接调 `processBucket`，锚定 1.6 期望）与真实线程形态共用同一段处理逻辑（设计 §9）。

- **循环协议（`run()`，consumer 线程）**：交接队列 `poll(1s)`——null（暂无交接）做周期统计后继续；取到毒丸退出；否则 processBucket。poll 被中断恢复中断标志退出（防御路径）
- **processBucket（单桶处理半程，同步/异步共用）**：`state=OUTPUTTING` → `BucketReplayer.replay(bucket, pipe)` → 封箱 `Transaction` 回调 listener → 前沿以 endLsn 单调累加（`accumulateAndGet(max)`）→ `state=DONE`。空桶产出空 changes；回放异常原样上抛——异步由 run 捕获、同步直传调用方（既有用例的 fail-fast 断言路径）
- **失败语义**：处理中抛出的任何 Throwable 记 ERROR、触发 onFailure（如 `stop::countDown`）、退出循环**不排干**（fail-fast，与 1.6"异常上抛终止会话"等价）；捕捉 Throwable 防 consumer 静默死亡导致 reader 无限追加
- **交接队列** `LinkedBlockingQueue<TxBuffer>`（无界——元素只有元数据，真缓冲是 CQ）
- **周期统计**（10s 固定周期，常量不做配置面）：各状态桶计数（LIVE/HANDED_OFF/OUTPUTTING）+ 输出前沿一行 INFO；最老 HANDED_OFF 桶滞留超 60s 升 WARN（consumer 或下游回调阻塞的信号）
- **线程约束**：run() 仅 consumer 线程；processBucket 的触碰面 = 冻结桶 + pipe.readRange + listener 回调 + 前沿累加 + 桶状态字段——全部在 consumer 线程或并发安全结构上

## BucketReplayer（包私有，桶回放器）

把一个交接冻结的桶回放渲染为 `TxChange` 序列——consumer 的核心。自 1.7 起完全走桶内快照：不持有 registry，resolver 与逐消息渲染视图都取自 `bucket.relationSnapshot`，consumer 线程与 reader 的版本日志零共享。逐单元三步：**aborted 过滤**（hasPrefix 时重窥 raw[1..4] 得 streamXid，命中 abortedSubxids 跳过——不解码不回调；LogicalMsg 单元前缀=顶层 xid，不会撞上子事务 subxid）→ **解码**（自持独立 decoder 的 `decodeSingle(payload, hasPrefix)`，显式给定 inStream 免 'S'/'E' 包裹重建流块上下文，不触碰实例状态——与组装器 live 解码实例互不干扰）→ **构造**（I/U/D→RowChange、T→TruncateChange、M→MsgChange；Relation 一律 `snapshot.require(oid, index)` 取**变更时刻**版本，index 即单元 seq）。每个解码点回调 decodedObserver（第二参传桶快照作渲染视图）。空桶产出空列表（协议合法）；非 I/U/D/T/M 单元防御性 ISE；桶未交接（快照缺失）抛 ISE——回放前置条件违反。单线程内同步执行（consumer 线程或同步测试线程）；产出的 TxChange 不可变，可跨线程传递。

## PipeConfig（record，不可变）

解耦管道配置，`fromSystemProperties()` 读 `vb.pipe.*`（默认：dir `pipe-queue` / rollCycle `MINUTELY`——chronicle-queue 2026.6 中该枚举在 `LegacyRollCycles`，大小写宽容）。**没有"禁用"逃生门**——管道是解耦架构的地基，绕过管道等于回到 1.6 同步阻塞形态（`vb.spill.thresholdBytes` 随 spill 机制整体退役）。属性非法（未知 rollCycle）启动期抛 IllegalArgumentException fail-fast。无日志。

## RawPeeks（包私有，纯函数）

raw 字节窥探辅助：`intAt`（big-endian 有符号 I32，oid 窥探——每字节先 &0xFF 再移位拼接，byte 有符号直接 | 会把符号位扩散）、`unsignedInt`（无符号 I32 入 long，流式前缀 xid）、`longAt`（I64，LogicalMsg lsn）、`cstringAt`（null 结尾 UTF-8，prefix 短字符串）。组装器路由与回放器 streamXid 重窥共用。

## 事务模型 record 族（输出侧，组装器的回调产物）

提交路径回放出的不可变值对象族：`Transaction` 是对外的原子单元，`TxChange` sealed 族是其内容。全部为值语义 record：集合组件经紧凑构造器防御性拷贝（null 或含 null 元素抛 NPE）、Optional 组件 null 归一 empty。两个例外：`Transaction.gid` 非 TWO_PHASE 时刻意为 null；`MsgChange` 无紧凑构造器（content 为解码器独占新建数组、共享引用不复制——"构造后不得改写"约定）。不可变、可跨线程传递。各类完整 javadoc 见源文件。

- **`TransactionListener.onTransaction(Transaction)`**：事务消费契约（@FunctionalInterface）。**调用线程 = transaction-consumer**（异步形态；同步形态即调用线程）——回调耗时只拖慢输出不阻塞读取，实现方可安心做慢 IO；ROLLBACK 路径不回调
- **`Transaction(xid, kind, gid, commitLsn, endLsn, commitTimestamp, changes)`**：一个已确认提交的完整事务。xid 来源随 kind 而定（NORMAL←Begin、STREAMED←StreamStart、TWO_PHASE←BeginPrepare/StreamPrepare）；gid 非 null **当且仅当** kind=TWO_PHASE；changes 按协议到达顺序，紧凑构造器 `List.copyOf` 防御性拷贝
- **`TransactionKind`**（枚举）：NORMAL（变更整体缓冲，Commit 后一次输出）/ STREAMED（越过 logical_decoding_work_mem 被驱逐流式，StreamCommit 后一次输出）/ TWO_PHASE（PREPARE 后挂起，COMMIT PREPARED 才输出，ROLLBACK PREPARED 丢弃）
- **`TxChange`（sealed interface，permits RowChange/TruncateChange/MsgChange）**：事务内一条变更的基接口。公共组件 `streamXid`（OptionalLong）：流式块内非空——DML/Truncate 的 xid 前缀 = 产生变更的**（子）事务** xid、Message 的前缀 = 顶层 xid；非流式块内恒 empty。供回放期按子事务剔除与下游追溯归属
- **`RowChange(dml, relation, before, after, streamXid)`**：行变更。`relation` 是**变更时刻的 Relation 快照嵌入**（经桶快照 asOf 取版后随变更冻结——下游自包含，DDL 后旧行不按新 schema 错解）；before/after 统一 Optional：INSERT 仅 after、DELETE 仅 before、UPDATE 的 before 取决于 replica identity
- **`TruncateChange(relations, options, streamXid)`**：一条 TRUNCATE 语句可截多表——一次变更携带全部受影响表的快照（顺序与协议 relationOids 一致）；relations/options 均经 List.copyOf/Set.copyOf 防御性拷贝
- **`MsgChange(transactional, prefix, content, streamXid)`**：`pg_logical_emit_message` 的事务内逻辑消息（非事务性即时消息在组装器 WARN 丢弃，不入 Transaction）；content 为 byte[] 组件，显式 override equals/hashCode 为**值相等**（record 默认对数组退化为引用相等）
- **`DmlKind`**（枚举）：INSERT/UPDATE/DELETE，与 RowChange 的 before/after 语义一一对应

## SessionHarness（测试侧，src/test/java/org/vastdata/vbstream/it/）

集成测试的会话包装：守护线程 `pgoutput-reader` 跑 `session.run`，**双轨录制**——`rawMessages()`（原始字节，先入列）与 `messages()`（经 `DecodedMessageBridge` 解码，后入列），两列表同序一一对应（每条 raw 恰是对位解码消息的完整字节）；`DecoupledPipelineTest` 即取 `rawMessages()` 录制喂异步组装器做解耦管道验收。停止条件仅 countDown latch——确定性全量断言必须**先 close() 再读**；无解码异常时 close 后两列表等长（解码异常时 raw 侧多一条）。`awaitTermination` 超时消息用类型直方图防大载荷爆炸。

## 线程模型小结（1.7 双线程形态）

**reader 线程（pgoutput-reader）**：session.run 循环 → 组装器 onRaw 全部路由/记账 → registry（单写者）→ pipe append/releaseBelow → 交接入队 → 反馈封顶读取前沿。**consumer 线程（transaction-consumer，异步形态）**：交接队列 take → pipe.readRange（tailer）→ 冻结桶回放（快照渲染）→ listener 回调 → 前沿累加 → 桶状态后两态。

**跨线程共享面精确枚举**（除此之外各线程私有）：

1. 交接队列 `LinkedBlockingQueue<TxBuffer>`（并发安全，FIFO 保证交接序即提交序）
2. `TxBuffer.state`（volatile，写侧按状态分段：reader 前段 LIVE→HANDED_OFF、consumer 后段 OUTPUTTING/DONE；其余字段交接即冻结、任意线程只读）
3. 输出前沿 `AtomicLong`（consumer 单调 max 累加，reader 每轮读一次做反馈封顶）
4. `MessagePipe` 的 appender（reader 独占）与 tailer（consumer 独占）——各自单线程使用，跨线程可见性由 CQ 的单 appender/多 tailer 内存模型保证

同步测试形态（`TransactionConsumer.processBucket` 直调）把 consumer 半程折叠回调用线程——既有 33+ 组装器单测以此锚定 1.6 期望，`DecoupledEquivalenceTest` 断言同一字节流同步/异步两形态输出全等。`VersionedRelationRegistry` 是 reader 侧单写者设计；`DecodedMessageBridge` 非线程安全（inStream）；`ConsoleListener` 无状态且 slf4j 线程安全（双线程回调安全）；输出的 `Transaction`/`TxChange` 不可变，可跨线程传递。
