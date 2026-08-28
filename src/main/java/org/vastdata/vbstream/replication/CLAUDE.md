# replication/ 模块——复制会话、raw 接缝与事务组装（IO + 组装层）

职责：管理两条 JDBC 连接（普通 SQL + 复制专用），把复制流按**原始字节**交付上层（raw 接缝，里程碑 1.6 起）；本包同时承载 raw 驱动的 `TransactionAssembler`（MEMORY/SPILLED 混合缓冲）与溢写基础设施（`MessageSpool`/`SpoolFrame`/`PayloadUnit`/`SpillConfig`）。上游 `Main` 依赖 `PgReplicationSession` + `TransactionAssembler`（raw 接缝直连）；集成测试 `SessionHarness` 双轨录制（raw + 经 `DecodedMessageBridge` 的解码消息）。

## PgReplicationSession（核心，AutoCloseable）

生命周期严格按 **open → ensureSlot → start → run → close**；`open` 失败会自行回收半开连接（防泄漏）。

- **`open()`**：建两条连接——普通 `config.jdbcUrl()` 与 `config.replicationUrl()`（带 `replication=database`）。复制连接要求见 ReplicationConfig。
- **`ensureSlot()`**：`SELECT pg_create_logical_replication_slot(槽名, 'pgoutput', false, twoPhase)` **幂等建槽**；捕获 SQLState `42710`（duplicate_object）视为"已存在，直接复用"并打 WARN（提示槽的 two_phase 属性需与配置一致，不一致将由 start 时服务端报错），其余异常上抛。
- **`start()`**：经 `PGConnection.getReplicationAPI()` 建 `PGReplicationStream`，slot options **四项**：`proto_version`、`publication_names`、`streaming`（OFF→"off"/ON→"on"/PARALLEL→"parallel"）、`two_phase`（on/off）；另经 `withStartPosition(INVALID_LSN)` 从槽当前确认点续传、`withStatusInterval` 设状态回传周期。
- **`run(RawMessageListener)`**：**轮询式消息循环（100ms readPending 非阻塞轮询），由调用方线程执行**（Main/harness 中是名为 `pgoutput-reader` 的线程：harness 里为守护线程，Main 里为普通线程、靠 shutdown hook + CountDownLatch 收敛）。**会话只做字节交付**：每条消息的完整字节（含类型字节与流式块内可选 Int32 xid 前缀）拷入**独占新建数组**回调 `listener.onRaw(raw)`（调用方可无复制长期持有）；解码与 Relation 缓存完全移出 session（由组装器或桥承担），自身不触碰协议层。声明 `throws SQLException, IOException`：
  1. 每轮先查 `stream.isClosed()`（断连快速感知，抛描述性 `SQLException`）；`stream.readPending()` 非阻塞取消息（null=暂无消息属正常，sleep 100ms 后继续；非 null 但 remaining()==0 的载荷防御性跳过不回调）
  2. 回调 `listener.onRaw(raw)`（同步，回调耗时直接拖慢消息循环与 LSN 反馈）
  3. 每轮 `setAppliedLSN/setFlushedLSN(getLastReceiveLSN())`；每满一个反馈周期 `forceUpdateStatus()` 上报确认位点
  - **为什么轮询而非阻塞 `read()`**（实测 pgjdbc 42.7.13 + PG 18）：阻塞 read 空闲期不按 statusInterval 醒来，status 依赖服务端 keepalive（~wal_sender_timeout/2，默认约 30s）才触发；轮询使 status 周期独立于消息到达（反馈间隔=feedbackIntervalSeconds，`pg_stat_replication.flush_lsn` 及时反映客户端进度），且断连感知更快（isClosed 每轮检查）
  - **confirmed_flush_lsn 的服务端行为（Diag 实证，勿再当 bug 排查）**：standby status 到达后先被采纳进 `pg_stat_replication.flush_lsn`；槽的 `confirmed_flush_lsn` 由 walsender 在解码推进时（candidate 机制）落库——空闲期不推进，但确认不丢失，下一次任何 WAL 活动会使其一步跳到客户端已确认的最新位点。集成验证见 `NormalTransactionTest.feedbackIsAdoptedByServerAndConfirmedFlushAdvances`（两段式断言）
- **`close()`**：顺序 流 → 复制连接 → SQL 连接；关闭后 run 循环在下一轮 `isClosed()` 检查（≤100ms）抛 SQLException 退出。各步失败仅 WARN 不上抛。
- **`lastReceiveLsn()`**：供异常退出时打印续传位点；流未启动返回 INVALID_LSN。**`config()`**：暴露配置（harness 日志用）。

日志：生命周期 INFO（连接建立/槽已创建/流已启动/会话已关闭）、槽复用与关闭失败 WARN、LSN 反馈 DEBUG。

## 交付接缝：RawMessageListener（现行）与 PgOutputListener（旧，保留）

双契约并存，新增的是并行接缝而非替换（assembly-spill 设计 §4.5/§11）：

- **`RawMessageListener.onRaw(byte[] raw)`**：raw 字节消费者（现行接缝）。`raw` 是完整单条 pgoutput 消息字节——含首个类型字节与其后全部字段（流式块内消息含可选 Int32 xid 前缀），解码器/组装器可直接消费。回调线程 = 调用 run() 的线程（同步）；抛出的异常经 run 循环原样上抛终止会话线程。`TransactionAssembler` 实现此接口。
- **`PgOutputListener.onMessage(message, registry)`**：解码后消息契约（里程碑 1 形态），**保留不改**——里程碑 2 的输出队列与既有解码测试仍走此契约。
- **`DecodedMessageBridge`**：raw → 解码契约的适配器。自持一套 `PgOutputDecoder`（含流块状态机 inStream）与 `RelationRegistry`，`onRaw` → `decode` → `registry.accept` → `target.onMessage`，与接缝改造前 run 循环内置链路**逐字节等价**（`RawSessionContractTest` 验证：同一 raw 流从头重放得到一致消息序列）。同一桥实例只能重放**一条**按序消息流（inStream 状态）；构造需传与 START_REPLICATION 一致的 StreamingMode（否则 StreamAbort 附加字段解析错位 fail-fast）。`registry()` 暴露桥持有的 Relation 缓存。非线程安全。

## ReplicationConfig（record，不可变）

11 个分量的配置模型；`fromSystemProperties()` 以 `vb.pg.*` 前缀读取系统属性，默认值对准 `src/docker` compose 环境（localhost:55432 / postgres 库 / 槽 vb_cdc_slot / publication vb_pub / proto 4 / streaming parallel / twoPhase true / 反馈 10s）。

- **`replicationUrl()`**：`jdbcUrl() + "?replication=database&assumeMinServerVersion=9.4"`——pgjdbc 规定 replication 连接必须同时带 `assumeMinServerVersion>=9.4` 才会把 replication 参数放进启动包，否则 `START_REPLICATION` 被服务端按普通 SQL 解析报语法错（真实 PG 18 首跑踩过）
- **`streamingParam()`**：StreamingMode → START_REPLICATION 参数值字符串

## RelationRegistry 与 VersionedRelationRegistry（oid → Relation）

协议保证 Relation 消息先于同表 DML 到达（含流式块内的重复下发）。

- **`RelationRegistry`**（父类）：`accept(message)` 只认 Relation 消息、按 oid 覆盖式 put（DDL 后服务端重发新版本即翻新）；`find(oid)` 返回 Optional；`require(oid)` 未命中抛 `IllegalStateException`（fail-fast——缓存 miss 意味着协议流异常）。`ConcurrentHashMap`，为 listener 回调跨线程查询设计；`DecodedMessageBridge` 持有的是它。
- **`VersionedRelationRegistry`**（子类，组装器用）：oid → `(seq, Relation)` **版本日志**——存在动机：DDL 会让服务端在流中重发同 oid 新版 Relation，溢写回放旧单元必须按"变更时刻"版本渲染，取最新版会把旧行按新 schema 错解。要点：
  - `accept(seq, rel)`：按 oid 追加版本、维持 seq 升序；同 seq 重复投递幂等跳过
  - `require(oid, asOfSeq)`：**二分取 ≤ asOfSeq 的最新版**；无版本或全部晚于 asOfSeq 抛 ISE（沿用"Relation 未先行到达"fail-fast）
  - `pruneBelow(minSeq)`：以"最低仍会被查询的 seq"为低水位剪枝——各 oid 保留 asOf=minSeq 时刻**生效**的版本（floor，其自身 seq 可早于 minSeq：'R' 恒先于首个 DML，存活桶旧单元会解析到低水位之前记入的版本——字面"丢弃 seq < minSeq"实现会在并发 DDL 流形下误剪崩回放）及其后全部；minSeq 之后无任何版本时整列保留（每 oid 至少留最新一条由此自然成立）。**已接线非死代码**：组装器在桶完结点（`retireBucket`）以全部存活桶 minSeq 的最小值调用（2PC 挂起桶算存活），版本日志低水位剪枝后有界
  - 继承的 `accept(PgOutputMessage)`/`find`/`require(oid)` 委托最新版本（旧接缝行为不变；旧接缝以合成 seq 记入时间线末尾，与带 seq 接缝不应混用）
  - **线程约束不同于父类**：单写者假设（组装器 run 线程串行调用全部方法），用 HashMap 而非 ConcurrentHashMap；跨线程查询场景请用父类

## TransactionAssembler（raw 驱动的事务组装状态机，AutoCloseable）

实现 `RawMessageListener`，把原始字节流组装为不可变 `Transaction` 回调（`TransactionListener.onTransaction`）。核心思路：**数据消息不解码直接入桶，解码推迟到提交期回放**（spec §4 / assembly-spill 设计 §2-§5）。

- **路由（每条 onRaw）**：先分配全局单调 seq（`nextSeq++`，从 1 起，含控制消息与 'R'），再按类型字节分流：
  - 控制消息（B/C/S/E/c/A/b/P/K/r/p）与 'R'：**live 解码**（自持 decoder 顺带维护 inStream 流块状态机，与 `currentStream` 指针同点同变）后分发到桶状态规则；'R' 以到达 seq 记入版本日志，字节不入桶
  - I/U/D/T/M：**轻窥不入桶不解码**——只窥流式前缀（流块内 raw[1..4] 无符号 xid）构造 `PayloadUnit` 入当前活动桶；'M' 先窥 flags bit0 分事务性/非事务性（非事务性无桶 WARN 丢弃）
  - Y/O：DEBUG 记录后丢弃；未知类型经 decoder 抛 UnknownMessageTypeException（解码层 fail-fast）
- **桶模型**：普通事务单指针 `currentNormalTx`（Begin..Commit 串行不嵌套）；流式多桶 `streamedByXid`（key=StreamStart 顶层 xid）+ 流块上下文 `currentStream`（段间交错、流块不嵌套）；两阶段 `currentPrepareTx` 活动 + `preparedByGid` 挂起池（PREPARE 至 COMMIT/ROLLBACK PREPARED，可能长期挂起）。**StreamAbort**：top==sub 整桶丢弃（存储随之释放）；否则记入桶的 `abortedSubxids`，**回放期过滤**（数据保留到提交期一次性甄别）。桶缺失/重复/流块状态异常一律 ISE fail-fast
- **混合缓冲**：桶存储双形态 `Mode`——**MEMORY** 持内存 `List<PayloadUnit>`；**SPILLED** 持溢写池 CQ index **连续段**（单元经 `SpoolFrame` 信封帧落盘，堆内零逐单元元数据；并发桶 append 在共享 appender 上交错——同桶相邻 append 顺延当前段、他人插队起新段，回放逐段 readRange）。全局记账 `memoryBytes` = Σ 存活 MEMORY 桶 bytesTotal：任一 MEMORY 写入后越限（> threshold）即 **`spillAll()`** 把所有 MEMORY 桶逐单元转储（正在追加的桶也在列，INFO 留痕）；开桶时水位已达阈值（>= threshold）直接 SPILLED 起步。`spillEnabled()==false`（thresholdBytes ≤ 0）时全路径短路——纯内存逃生门，spool 永不创建
- **提交路径**：Commit/StreamCommit/CommitPrepared → `replay(bucket)`：SPILLED 桶先逐段 `MessageSpool.readRange` + `SpoolFrame.unframe` 复原单元，随后与 MEMORY 走**同一** `BucketReplayer`（两种形态输出严格相等，spill 无损）→ 封箱 Transaction 回调 → 桶完结收尾（`retireBucket`：记账回退 + 低水位维护）。回滚路径（RollbackPrepared/StreamAbort 整桶）丢弃不回调
- **低水位**：`spillWatermark()` = min(存活 SPILLED 桶 firstIndex, lastAppended+1)，交 `MessageSpool.releaseBelow` 删除过老滚动文件（spool 未建立返回 -1 哨兵）；同一完结点并行维护 **registry 剪枝低水位** = 全部存活桶 minSeq（桶内最老单元 seq，MEMORY/SPILLED 两路都在 storeUnit 记账）的最小值，驱动 `VersionedRelationRegistry.pruneBelow`（终审 Fix B 接线）
- **有界性范围（终审修正）**："内存有界"仅指**组装期**桶缓冲——提交期回放把整桶单元物化回堆（SPILLED 回读的原始字节 + 解码出的 TxChange 双份瞬态并存，峰值 O(事务大小)），流式输出属里程碑 2；仍随事务/会话增长的堆结构：`abortedSubxids`（每回滚子事务一个 Long，随桶完结释放）、`preparedByGid`（未决 2PC 数，协议固有）、registry 版本日志（随新表/DDL 线性——低水位剪枝后仅随不同表 oid 数线性）
- **close()**：收敛溢写池（曾建立过时）；失败仅 WARN
- **线程约束**：非线程安全——单写者假设，全部在 run 循环线程内（decoder inStream、桶指针、appender 均要求）；输出 Transaction 不可变可跨线程
- 日志：spillAll 转储与首次建池 INFO；非事务性 M 丢弃、RollbackPrepared 丢弃 WARN；Y/O 丢弃与 PREPARE 入挂起池 DEBUG

## SpillConfig（record，不可变）

溢写配置，`fromSystemProperties()` 读 `vb.spill.*`（默认：thresholdBytes 67108864 即 64MiB / dir `spill-queue` / rollCycle MINUTELY——chronicle-queue 2026.6 中该枚举在 `LegacyRollCycles`，大小写宽容）。`spillEnabled()` = 阈值 > 0；**≤0 为显式纯内存逃生门**（保留里程碑 1.5 行为作对照基线与故障回退，dir/rollCycle 不参与任何 IO）。属性非法（非数字阈值/未知 rollCycle）启动期抛 NumberFormatException/IllegalArgumentException fail-fast。无日志。

## PayloadUnit（record，桶存储单元）

`(byte[] payload, long seq, OptionalLong streamXid)`：payload 为完整单条消息字节（含类型字节与可选 xid 前缀）——桶里存的不是解码对象而是原始字节，使 MEMORY→SPILLED 切换成为纯字节转储。seq 供版本日志 asOf 取版；streamXid 有值即"流块内"（payload 带 4 字节前缀），值域无符号 Int32。equals/hashCode 显式按**数组值语义** override（record 默认对数组退化为引用相等，round-trip 断言会假阴性）。payload 引用共享不复制（热路径避免双倍拷贝，构造后不得改写）。消费契约：无前缀单元 `decodeSingle(wrap(payload), false)`、有前缀单元 `decodeSingle(wrap(payload), true)` 且前缀值 == streamXid。

## SpoolFrame（纯函数，信封帧/解帧）

`[I64 seq][I8 xidPresent][I32 xid?][payload]`（big-endian）——**变长帧头**：无 xid 9 字节、有 xid 13 字节（xidPresent 决定）。`frame(PayloadUnit)`/`unframe(byte[])` 无状态无副作用；streamXid 超出无符号 Int32 值域、帧长不足、xidPresent 非 0/1 均抛 IllegalArgumentException fail-fast（错位数据宁可拒绝不可猜测）。无日志。

## MessageSpool（包私有，Chronicle Queue 溢写池，AutoCloseable）

CQ 生命周期管理者，帧语义归调用方（本类只搬 byte[]）。**瞬态工作区语义**：构造时先递归清空目录内容再建 `SingleChronicleQueue`（wipe-on-open）——真源是复制槽，重启后 PG 从确认位点重发，残留旧 spill 数据有害（陈旧 index 会让回读错位），**重启自动清空属预期行为**。

- `append(framed)` → CQ index（单调，作回读端点与低水位入参）；写失败以 CQ 运行时异常上抛（不吞不重试）
- `readRange(first, last, consumer)`：按 index 升序回读闭区间逐条回调（0 起序号）；区间起点错位（已被误删/从未存在）抛 ISE fail-fast；区间不存在且无后续条目时空手而归不抛
- `releaseBelow(lowestNeededIndex)`：删除严格低于 needed cycle - 1 的滚动文件（保留 needed 与 needed-1 档，保守删除）；每删一个 WARN 留痕、单文件失败 WARN 重试（残留只占磁盘不影响正确性）；候选集计算 `deletableFiles` 纯函数化（供单测注入文件名）
- `lastAppendedIndex()` / `close()`（tailer → appender → queue 逆序，失败 WARN 吸收）
- **线程约束**：非线程安全——appender/tailer 均为构造时创建的单实例，单写者（run 线程）顺序调用全部方法
- 日志：建池/关池 INFO、删档留痕与各步失败 WARN

## BucketReplayer（包私有，桶回放器）

把一个桶的存储单元回放渲染为 `TxChange` 序列——提交路径核心，从组装器抽出以获机制级可测性（可手造单元直接驱动）。逐单元三步：**aborted 过滤**（streamXid 命中 abortedSubxids 的单元跳过，不解码不回调）→ **解码**（自持独立 decoder 的 `decodeSingle(payload, streamXid.isPresent())`，显式给定 inStream 免 S/E 包裹重建流块上下文，不触碰实例 inStream 状态——与组装器 live 解码实例互不干扰）→ **构造**（I/U/D→RowChange、T→TruncateChange、M→MsgChange；Relation 一律 `registry.require(oid, unit.seq())` 取**变更时刻**版本）。每个解码点回调 decodedObserver（与组装器 live 解码共用同一 observer）。空桶产出空列表（协议合法）；非 I/U/D/T/M 单元防御性 ISE。线程：run 线程内同步执行，非线程安全。

## SessionHarness（测试侧，src/test/java/org/vastdata/vbstream/it/）

集成测试的会话包装：守护线程 `pgoutput-reader` 跑 `session.run`，**双轨录制**——`rawMessages()`（原始字节，先入列）与 `messages()`（经 `DecodedMessageBridge` 解码，后入列），两列表同序一一对应（每条 raw 恰是对位解码消息的完整字节）；`AssemblySpillTest` 即取 `rawMessages()` 录制喂两个不同阈值的组装器做等价验收。停止条件仅 countDown latch——确定性全量断言必须**先 close() 再读**；无解码异常时 close 后两列表等长（解码异常时 raw 侧多一条）。`awaitTermination` 超时消息用类型直方图防大载荷爆炸。

## 线程模型小结

单一 run 循环线程贯穿 session → 组装器 → 溢写池（appender/tailer 单写者，`lastSpillAppender` 连续段判定依赖单写者的"append 与记账原子相邻"）；`VersionedRelationRegistry` 是组装器独占的单写者设计（区别于父类 `RelationRegistry` 的跨线程 ConcurrentHashMap）；`DecodedMessageBridge` 非线程安全（inStream）；全部回调（onRaw/onMessage/onTransaction/decodedObserver）在 run 线程内同步执行；输出的 `Transaction`/`TxChange` 不可变，可跨线程传递。
