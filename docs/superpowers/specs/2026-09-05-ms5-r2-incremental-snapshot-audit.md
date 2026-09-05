# MS5 R2 增量快照交错审计(只审不接,2026-09-05)

对应设计 `2026-09-04-ms5-integration-surface-design.md` 的 R2(增量快照接入的交错面)——本审计
**只审不接**:接入决策与实现留 MS6,本档钉死"若接,vanilla 形态会在哪些点打破本连接器的
既有契约"与前置条件清单。行文与结论形态参照 R1/R3 审计
(`2026-09-02-ms2-r1-r3-audit.md`)。

vanilla 引证基线:debezium-connector-postgres 与 debezium-connector-common **3.6.1.Final
sources jar**(3.6.1 起 `io.debezium.pipeline.*` 增量快照/信号类已从旧 debezium-core 拆入
**debezium-connector-common**;下述 `Abstract*`/`Signal*`/`EventDispatcher`/
`ChangeEventSourceCoordinator` 均出自该 jar,`Postgres*` 出自 connector-postgres jar,行号为
sources jar 实测)。本连接器侧引证为 MS5 Task 5 收官时源码(`vb-stream-connector-postgres-stream`)。

结论先行:**vanilla 增量快照不能以原生形态直接接入**——三个交错面(第二 dispatch 线程 /
main 连接时序独占消失 / offset 与前沿交互)各打破一处既有契约,但全部有明确的串行化收敛
形态(核心护栏与 MS3.5 `safeMessageAdvance` 同族:全有或全无)。**建议 MS6 接入
signal-based 形态**,以前置条件五项为准入门槛;**不建议接 read-only 变体**(心跳写路径
违约 + vanilla 侧 experimental 定位)。逐项依据如下。

---

## 0. vanilla 增量快照的触发拓扑(审计对象)

3.6.1 的触发面有三(线程归属是全部交错面的根源):

| # | 触发面 | 线程 | 依据 |
|---|---|---|---|
| V1 | 外部信号通道(file/Kafka/JMX)的 execute-snapshot 等动作 | **SignalProcessor 独立单线程调度器** | `ChangeEventSourceCoordinator` :171-186 注册动作并 `signalProcessor.start()`(:185 注释自证 "this will run on a separate thread");`SignalProcessor` :83 建调度器、:114-118 `scheduleAtFixedRate(process)`;process() :147-156 在该线程读通道并 `action.arrived`(:312) |
| V2 | source 信号表 DML 经 WAL 到达 | **流式消息处理线程**(本连接器=consumer) | `EventDispatcher.dispatchDataChangeEvent` :313-319:`isASignalEventToProcess` → `sourceSignalChannel.process(value)` + `signalProcessor.processSourceSignal(partition)`;`ExecuteSnapshot.arrived` :53→:78 `addDataCollectionNamesToSnapshot` → `readChunk` |
| V3 | 心跳推进窗口(read-only 变体) | 心跳发射线程(vanilla=流式线程;本连接器=监督壳) | vanilla `PostgresStreamingChangeEventSource` :270 无消息迭代发心跳;本连接器监督壳 `PostgresStreamStreamingChangeEventSource.supervise` :248-265 每 200ms 发;read-only 变体 `processHeartbeat` :152-162 → `readUntilNewTransactionChange` :179-202 → `sendWindowEvents`+`readChunk` |

三个触发面最终都汇入 `AbstractIncrementalSnapshotChangeEventSource.readChunk`(:244-367)
——**整条调用串内发生**:chunk SELECT 与 max-PK 查询(:280、:655-662,经 main JDBC 连接)、
watermark 信号表 INSERT/commit(`SignalBasedIncrementalSnapshotChangeEventSource`
:111-127/:130-141)、window 缓冲写(`window` LinkedHashMap :87,put :679/clear)、
offsetContext 写(`sendWindowEvents` :170-178 → `offsetContext.incrementalSnapshotEvents`
:172;`sendEvent` :180-186 → `context.sendEvent` :181 + `offsetContext.event` :182 +
`dispatcher.dispatchSnapshotEvent` :183)、schema 刷新(Postgres 子类
`refreshTableSchema` :55-59 → `schema.refreshFromIncrementalSnapshot`,写共享 schema)。
**`readChunk` 在哪个线程跑,哪条上述写路径就在那个线程**——这就是 R1 被打破的机制本体。

---

## 1. 交错面①——第二 dispatch 线程(R1"dispatch 仅 consumer 线程"被打破点逐项)

R1 结论(2026-09-02 审计):dispatch 全部状态写仅 consumer 线程(`DispatcherTransactionListener`
javadoc :57-62 钉死线程约束,schema/offset 单写者假设的落点)。vanilla 形态接入后的打破点:

| # | 打破点 | 违约的 R1 行 | 依据 |
|---|---|---|---|
| ①-a | **V1 信号执行器线程成为 dispatch 写者**:file/Kafka/JMX 信号 → `readChunk` → `dispatchSnapshotEvent`(经 `IncrementalSnapshotChangeRecordReceiver` 直入 `ChangeEventQueue`,EventDispatcher :690;`schema.schemaFor` :229 读共享 schema)+ progressListener/notificationService 回调 | 行 1-3(dispatch 出口) | `ChangeEventSourceCoordinator` :185;`SignalProcessor` :147-156/:302-313;`AbstractIncrementalSnapshotChangeEventSource` :183 |
| ①-b | **schema 第二写者**:`readChunk` → `refreshTableSchema` 写共享 `StreamPostgresSchema`,与 consumer 的 `resolveAndInstall` 版本安装(DispatcherTransactionListener :242-248)并发 | 行 4(schema 单写者) | vanilla Postgres 子类 :55-59;本连接器 :242-248 |
| ①-c | **offset 第二写者**:`sendWindowEvents`/`sendEvent`/`postSnapshotCompletion` 写 `PostgresOffsetContext`(非线程安全),与 consumer 的 `updateCommitPosition`/`updateWalPosition`(DispatcherTransactionListener :176/:198)并发 | 行 5(offset 单写者) | `AbstractIncrementalSnapshotChangeEventSource` :172/:176/:182;vanilla `PostgresOffsetContext.event` :289-291 |
| ①-d | **window 无并发原语**:dedup(`processMessage` → `deduplicateWindow`,`SignalBasedIncrementalSnapshotChangeEventSource` :98-108)在 dispatch 调用线程读,`readChunk` 的 put/clear 在触发线程写——`LinkedHashMap` 跨线程读写丢项/死循环风险。vanilla 的 `SignalProcessor` Semaphore(:67/:181-198)只互斥**信号处理**,dedup 不在信号量内 | (新增面,R1 表未列——R2 特有) | `AbstractIncrementalSnapshotChangeEventSource` :87/:679;`SignalBasedIncrementalSnapshotChangeEventSource` :104-107 |
| ①-e | **监督壳心跳升格违约(read-only 变体)**:V3 触发面下 `processHeartbeat` → `readChunk` 全套写路径落在监督线程——R1 行 7 现为"跨线程**只读** offset,已知无害(默认关)"接入后变**写**路径,无害论证失效 | 行 7 | `PostgresReadOnlyIncrementalSnapshotChangeEventSource` :152-162/:179-202;R1 审计「已知无害项」节 |

不构成违约的注记项:

- **signal-based 形态的心跳(V3)是 no-op**:接口 default 空实现
  (`IncrementalSnapshotChangeEventSource` :41-42),监督壳心跳面不变。
- **StreamEventMetadataProvider 不破**:无状态(StreamEventMetadataProvider :27),
  任意线程;chunk 记录 source 块无 txId,`getTransactionId` 返回 null 属安全回落(:90-94)。
  snapshot 事件不经 TransactionMonitor(`dispatchSnapshotEvent` :223-257 直走 receiver,
  不触 `transactionMonitor.dataEvent` :324 的路径)——事务块语义不受 chunk 记录干扰。
- **V2(source 通道)触发面线程不破**:信号表 DML 经 WAL → 本连接器唯一 dispatch 点在
  consumer(DispatcherTransactionListener :200-201)→ EventDispatcher 内嵌检测 :313-319 →
  `processSourceSignal` → `readChunk`——仍是单 dispatch 线程。**但** readChunk 同步阻塞
  consumer:交接桶积压、前沿静止(交错面③)、chunk JDBC 查询撞 main 连接(交错面②)。

---

## 2. 交错面②——main 连接:R3 时序独占前提消失

R3 结论(2026-09-02 审计):main 连接装配后由 reader 线程独占('R' enrich,
`RelationMetadataSource.jdbc(mainConnection, ...)`,PostgresStreamStreamingChangeEventSource
:192-193)——**时序独占证明以"快照恒 skipped"为前提**(R3 审计原文明示"MS5 预警:真快照
接入后……必须串行化")。增量快照接入即该前提消失:

- **vanilla 把 mainConnection 直接交给增量快照源**:`PostgresChangeEventSourceFactory`
  :91-117(`connectionFactory.mainConnection()`,read-only :100 / signal-based :117)。
  `readChunk` 在这条连接上的接触面:`preReadChunk` isValid/connect(Abstract :787-798)、
  **无条件 commit**(:260)、max-PK 查询(:280)、chunk SELECT(:655-662)、watermark
  INSERT + commit(SignalBased :118-126)、schema `refreshFromIncrementalSnapshot`
  (Postgres 子类 :55-59)、`readSchemaForTable`(:820-841)。
- **并发对象**:reader 线程的 'R' enrich(DDL 稀疏但高频于 chunk 期间——信号表/目标表的
  'R' 恰在 watermark 事务附近到达)。pgjdbc 单连接不支持并发语句;且 `readChunk` 的
  commit(:260)会把 reader 在途的 'R' 查询事务(autoCommit=false 连接)提前收敛。
- **线程收拢救不了这一面**:即使交错面①的全部触发面收拢到 consumer(前置条件 1),
  consumer 线程的 chunk 查询仍与 reader 线程的 'R' enrich **跨线程共享同一条连接**——
  串行化必须落在连接级(互斥)或连接隔离(快照专用连接),不是线程归属能解决的。
- **vanilla 自身的同类暴露注记**:vanilla 单线程流式循环也在用 main 连接
  (`probeConnectionIfNeeded` :448-453 SELECT 1 + commit :282、xmin 刷新 :294-315),
  V1 信号执行器线程的 readChunk 与之并发——vanilla 同款潜在竞态,靠"未配
  `signal.data.collection` 时增量源根本不创建"(factory :111-112)压低暴露。本连接器因
  reader/consumer 分离、'R' enrich 是**常驻**接触面(非 vanilla 的周期探测),该并发不可
  忽略,必须显式机制。

---

## 3. 交错面③——offset 与输出前沿的交互

本连接器契约:per-record offset 统一事务边界(Begin 时 `updateCommitPosition(endLsn,
endLsn)`,本事务记录 lsn/lsn_proc/lsn_commit 同值,DispatcherTransactionListener :172-179);
**前沿锚 End**(End 发出后才 `outputFrontier.accumulateAndGet(endLsn, max)`,
TransactionConsumer :36-38/:172),LSN 反馈按前沿封顶(`ReplicationSession.capFeedback`
:359-361,min(已收到, 前沿))。交互逐项:

- **(a) chunk 记录的 offset 不越前沿——LSN 面天然兼容**:vanilla chunk 路径对 offset 的
  写是 `event`(:182 → `PostgresOffsetContext.event` :289-291,**只改 sourceInfo 的
  table/timestamp**,不触 lastCommitLsn/lastCompletelyProcessedLsn)——chunk 记录携带的
  LSN 维持上一次 Begin 锚定值(≤ 已推进前沿)。对照:vanilla 自己的 `commitOffset` 以
  `commitLsn ?: changeLsn` 直推 `flushLsn`(vanilla 流式源 :501-503/:518-530,独立
  lsn-flush 线程)——**本连接器不复刻该直推**(commitOffset 只记单调水位,服务端确认由
  前沿封顶承担,PostgresStreamStreamingChangeEventSource :343-366),chunk 期"纯快照记录
  无事务提交"的场景不会把未消费 WAL 推给槽。此兼容性须**钉为不变量**:未来任何"按框架
  offset 收紧 flush"的演进不得越过前沿封顶。
- **(b) offset map 的第二写面**:非 snapshot 态的 `getOffset()` 会嵌入
  `incrementalSnapshotContext.store(...)`(vanilla PostgresOffsetContext :103)——chunk
  状态(数据集合/chunk 游标/window)持久进框架 offset,该写与 consumer 的 offset 写并发
  (交错面①-c 同根)。
- **(c) 事务中交错禁止**:chunk 记录不得插入某桶回放的 Begin..End 之间——流式契约
  "本事务每条记录 lsn=lsn_commit=endLsn"(DispatcherTransactionListener :26-28)会被
  chunk 的 sourceInfo 写污染(snapshot 标志位/表位),事务内记录的 source 块形态失去
  单调一致性。收敛形态与 MS3.5 护栏同族:**无未输出桶才开窗**(全有或全无,
  `StreamedTransactionAssembler.safeMessageAdvance` javadoc :545 同款)。
- **(d) chunk 期间前沿被钉住→slot 滞后(预期行为,须观测)**:readChunk 若在 consumer
  线程内联(V2 收敛形态),chunk 期间 consumer 不回放交接桶 → End 不发 → 前沿静止 →
  confirmed_flush 滞后 → 服务端 WAL 保留与 CQ 管道目录同步增长(reader 不回压,代价转移
  磁盘——consumer 慢/停摆既有家族,`max_slot_wal_keep_size=2GB` 兜底 + 周期 WARN)。
  lagBytes 观测面已就绪(MS5 `StreamMetricsBridge`,读源
  `session.lastReceiveLsn() - frontier`,PostgresStreamStreamingChangeEventSource :236),
  接入时须验证 chunk 期该指标如实上升。
- **(e) 重启交错**:增量快照状态经 (b) 持久;本连接器重启锚槽 confirmed_flush(≤ 前沿)
  → WAL 从 confirmed_flush 重发 → 重发流式事务与 chunk 重读交错,window dedup(V2 面,
  consumer 线程)消化重叠——vanilla 同款语义,落在本连接器"重启重复取并集"的既有口径内
  (`RestartSemanticsIT` 形态)。风险仅当 chunk 状态恢复与重发交错**跨线程**发生(回到
  交错面①-d 的单线程化前提)。

---

## 4. 结论:接入前置条件清单 + MS6 建议

**前置条件(全部满足才开工,均为机制项非研究项)**:

1. **readChunk 单线程化(R1)**:全部触发面收拢到 consumer 线程——V2 天然在 consumer
   (交错面①注记);V1 外部信号通道二选一:文档面禁用(`signal.data.collection` 未配时
   增量源不创建,factory :111-112 同款闸门)或把 SignalProcessor 动作改"置标志、consumer
   空转周期执行"(与 R1 审计「已知无害项」的心跳收敛建议同款形态);**禁止 read-only
   变体**(V3 把监督壳心跳变写路径,交错面①-e)。
2. **main 连接串行化(R3)**:chunk SELECT/max-PK/watermark INSERT/schema refresh 与
   reader 的 'R' enrich 跨线程共享 main 连接——连接级互斥或快照专用连接;另须消化
   `readChunk` 的无条件 commit(:260)与 reader 在途 'R' 查询事务的互踩。
3. **事务中交错禁止(前沿契约)**:无未输出桶才开窗(全有或全无,MS3.5 护栏同族);
   chunk 路径对 offsetContext 的全部写(event/incrementalSnapshotEvents/
   postSnapshotCompletion/store)与 consumer 写同线程——由前置条件 1 自然蕴含,列为
   验收断言而非独立机制。
4. **chunk 期滞后观测**:lagBytes MBean 面覆盖 chunk 期如实上升;文档面记 WAL 保留/CQ
   目录增长的兜底口径(consumer 慢家族既有承诺)。
5. **signal 表可见性**:signal 表须入 publication 才能经 WAL 到达 source 通道(vanilla
   前提);其 DML(含 watermark INSERT)会作为普通 CDC 事务进管道并输出——topic 过滤或
   文档面接受,接入设计时二选一定死。

**MS6 建议:接(signal-based 形态)**。理由:①接口面零新增依赖——挂点已就绪
(`ChangeEventSourceCoordinator` :366-368 的 `setIncrementalSnapshotChangeEventSource`,
本连接器工厂只差覆写 `getIncrementalSnapshotChangeEventSource`,现 javadoc :30-31 明示
"默认 Optional.empty 不接";SignalProcessor 装配已在,PostgresStreamConnectorTask
:216-220/:259);②交错面全部有明确收敛形态(前置条件 1-5,核心护栏与 MS3.5 已实证的
全有或全无同族);③运维刚需——按需补快照是 Debezium 生态的主流运维手段,缺失它连接器
不可用于生产运维面。**不接 read-only 变体**(前置条件 1 的硬排除:心跳写路径违约 +
vanilla `isReadOnlyConnection` 前提下的 experimental 形态)。工作量主体:前置条件 1+2 的
串行化设计与三项 IT(chunk 期前沿静止/lagBytes 上升、重启 chunk 重读 dedup 取并集、
signal 表 DML 可见性)。
