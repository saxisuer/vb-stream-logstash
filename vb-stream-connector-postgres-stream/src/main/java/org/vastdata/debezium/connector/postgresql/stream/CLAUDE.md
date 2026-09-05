# org.vastdata.debezium.connector.postgresql.stream — Debezium 流式连接器(MS2 形态)

零 `org.vastdata.vbstream` import(模块边界 D2,与引擎文字参照非依赖)。MS1 落协议层
(`protocol/` 子包,见其 CLAUDE.md)与三件套骨架;**MS2 落管道与接线**;**MS3 落 Truncate
发射**(`DispatcherTransactionListener` 的 TruncateChange 分支——`skipped.operations` 门控
照 vanilla 3.6.1:默认 "t" 跳过(连接器默认不发)、`none` 才逐表 `dispatchDataChangeEvent`,
每表一条 op="t"/key=null/无 before/after 的普通 data topic 记录(`TruncateEmitter`),
协议选项位发射时丢弃对齐;门控只吞 'T' 数据消息,BEGIN/COMMIT 照常驱动事务块——被
门控的事务留一对空 BEGIN/END,vanilla 同款);LogicalMsg/指标/快照后续里程碑。
**MS3 三情况 IT**:`StreamAbortFilterIT`(SAVEPOINT 回滚行不进 Kafka、存活行完整、END 计数=
实付)、`InTxnDdlAsOfIT`(同事务 DDL 前后段各按变更时刻表结构渲染、列数分界正确)、
`RestartSemanticsIT` 三用例(半事务停机 D7 shutdownFast→槽重发补齐;offset 落后重启
重复段取并集;无缝续传)。**MS4 两阶段 + R5 预检**:`TwoPhaseIT` 四场景(PREPARE 挂起
零发射/ROLLBACK PREPARED 弃桶/parallel 档 StreamPrepare 大事务全量落 Kafka/prepared
挂起期停机重启续传——`preparedByGid` 挂起池按 gid 幂等吸收重发)、`SlotTwoPhaseMismatchIT`
存量槽 two_phase 不匹配启动期拒绝(真 42710 + 真目录行;失败信号经基座 CompletionCallback
捕获、异常链含 DROP SLOT 迁移指引、槽不删)——单测假件锚分支语义,IT 补真库面。
**MS5 集成面收官**——①配置默认值注入与校验:`snapshot.mode` 同名替换父 Field
(`PostgresStreamConnectorConfig.SNAPSHOT_MODE_NO_DATA`,默认 no_data、仅 no_data——其余值
REST validate 与任务构造器两级拒绝)且 `provide.transaction.metadata` 默认 true
(`PostgresStreamConnector.taskConfigs` 注入,缺省配置面零 op="r" 且事务元数据常开);
②管线指标插桩:`StreamThroughputMetrics` 与引擎 `ThroughputMetrics` 逐段同构,四点插桩
(reader 记 slot 读取/组装器记交接/consumer 记输出与分布),10s INFO 三行
(吞吐/分布/峰值)与引擎同口径;③MBean 面:`StreamStreamingChangeEventSourceMetrics` 经
Debezium metrics 体系暴露五速率 + lagBytes(`session.lastReceiveLsn()-前沿`)+ 挂起
prepared 数 + 管道磁盘占用——`StreamMetricsBridge` 于 execute 填四读源并挂统计 tick
预计算,JMX 读零锁零计算零 IO;④R2 增量快照交错审计只审不接(档
`docs/superpowers/specs/2026-09-05-ms5-r2-incremental-snapshot-audit.md`——vanilla 形态
三个交错面逐项打破点 + 接入前置条件五项;结论:建议 MS6 接 signal-based 形态、
不接 read-only 变体)。
**MS6 打包与文档收官**——①maven-assembly plugin 目录清单打包(descriptor `src/main/assembly/plugin.xml` + pom `finalName` 钉死产物路径:连接器自身 jar 落插件根(带 SourceConnector ServiceLoader 清单)+ runtime 依赖落 `lib/`;connect-api/kafka-clients/slf4j-api 及其独占子件四件(zstd-jni/lz4-java/snappy-java/jakarta.ws.rs-api)显式排除——excludes 不下传传递闭包,独占子件须点名,R4 两连接器并存的清单边界),产物 `target/vb-stream-connector-postgres-stream-plugin/` 目录 + 同名 zip;②`ConnectPluginIT` 真 Kafka Connect 验收(cp-kafka 8.3.0 + cp-kafka-connect 8.3.0=AK 4.3.0 容器组,plugin.path 挂插件目录副本——镜像 JVM Java 25,Chronicle mmap 的 --add-opens 经 KAFKA_OPTS 注入;REST `PUT /connectors/{name}/config` **扁平 config map** 请求体建连接器——包装体在 Connect 4.3 该端点报 500;产物结构断言前置 @BeforeAll,缺产物 fail-fast 文案指向先 package);③R2 裁定记档 v1 不接 signal-based 增量快照(审计文档结论节裁定行);④模块 README(定位/配置面/打包安装/at-least-once 语义/已知限制五节一档全,配置表逐项对照 `PostgresStreamConnectorConfig` javadoc 防两处漂移)。
**已知限制与延期**记档于 R1/R3 审计文档「已知限制与延期」节
(数组列 fail-fast 不静默 null、未知类型静默 null、LogicalMsg 延期设计要点、Truncate
选项位超集)。

## MS2 组件图(自建会话 + 双线程管道 + Debezium 接线)

```
PostgresStreamConnectorTask.start(Configuration)        ← Connect 任务装配(vanilla :101-284 同序替换:
  │  charset/TypeRegistry/连接工厂/main 连接 → 服务注册 → 命名豆注册(vanilla :151-159,
  │  SnapshotterServiceProvider 查找前置)→ StreamPostgresSchema → partition/offset 装载
  │  → ChangeEventQueue → PostgresErrorHandler → SignalProcessor → PostgresEventDispatcher
  │  → NotificationService → ChangeEventSourceCoordinator.start
  ▼ streaming 阶段(coordinator 线程进入监督壳)
PostgresStreamStreamingChangeEventSource.execute        ← 监督壳:装配 + 200ms 心跳周期 + 停机次序
  ├─ ReplicationSession(open→ensureSlot→start→run)      ← reader 线程(vb-pgoutput-reader):raw drain +
  │     LSN 反馈按输出前沿封顶(min(已收到,前沿))         ensureSlot 幂等建槽带 two_phase
  │                                                       (42710 复用前 R5 预检目录 two_phase
  │                                                        匹配,不匹配启动期拒绝)
  ├─ StreamedTransactionAssembler(异步形态)             ← 构造即起 consumer 线程(transaction-consumer)
  │     ├─ MessagePipe(Chronicle Queue,wipe-on-open)
  │     ├─ 桶记账:I/U/D 窥前缀记 index 段,控制消息 live 解码
  │     ├─ VersionedRelationRegistry(oid→(seq,Relation) 版本日志,pruneBelow 低水位剪枝)
  │     └─ RelationTableFactory/RelationMetadataSource   ← 'R' enrich:JDBC 元数据补列精度/PK(占 main 连接,R3)
  ▼ 交接(Commit/StreamCommit/CommitPrepared)= 拷快照冻结桶入队,reader 不停
TransactionConsumer(consumer 线程)
  ├─ BucketReplayer(逐段 readRange→decodeSingle→asOf 渲染)
  ├─ DispatcherTransactionListener(Begin/RowChange/End → dispatcher;offset 事务边界锚定)
  │     事务 id 必须纯数字(与 StreamEventMetadataProvider.getTransactionId 同源,否则
  │     TransactionMonitor 补发空 BEGIN/END 对——Task 8 IT 实测)
  └─ End 处理完毕 → 前沿 AtomicLong ← endLsn(reader 反馈封顶的读源)
```

- **offset/LSN 语义(§5.6)**:per-record offset 统一事务边界——Begin 时
  `updateCommitPosition(endLsn, endLsn)` 双写,此后本事务记录 `lsn`/`lsn_proc`/`lsn_commit`
  同值;重启续传锚槽 confirmed_flush(输出前沿封顶),`WalPositionLocator` 搜索与
  `lsn_events_processed` 计数跳过不复刻。
- **initialContext 读后必 commit**:`txid_current()` 给 autoCommit=false 的 main 连接分配
  XID,不收敛则另一连接上的 CREATE SLOT 等解码一致点等它——连接器自死锁
  (`SkippedSnapshotSource`/`initialOffsetWithCommit`,Task 8 IT 实测)。
- **停机(D7)**:`stopStreaming` = session.close → reader.join(5s) → `assembler.shutdownFast()`
  (不排干,未输出事务由槽重发);任务 doStop = bean registry 连接 → main 连接 → schema → queue。
- **R1/R3 线程审计**:全部 dispatch/offset/schema 写仅 consumer 线程;心跳仅监督线程
  (跨线程读 effectiveOffset 属已知无害项,心跳缺省关);main 连接 reader 独占(时序证明
  以快照恒 skipped 为前提)——结论档 `docs/superpowers/specs/2026-09-02-ms2-r1-r3-audit.md`。
- 配置面六项:`slot.streaming`(OFF/ON/PARALLEL,parallel 强制 two_phase)、`slot.two.phase`、
  `pipe.dir`、`pipe.roll.cycle`(LegacyRollCycles 名)、`slot.feedback.interval.ms`(整除换算秒,
  亚秒值截 0 = 每轮反馈)、`slot.messages`(MS3.5,默认 false——true 才在槽选项加 messages=true,
  'M' 逻辑消息解析记录(INFO 两时点)且非事务消息经护栏即时推进前沿——全有或全无(L8):无未输出桶才推进到消息位,有则完全静止,不发射下游)。
  MS5 另钉死两项默认面(非新键):`snapshot.mode` 同名替换为仅 no_data(默认注入,
  initial 等其余值启动期拒绝)、`provide.transaction.metadata` 默认 true——
  见 `PostgresStreamConnectorConfig` javadoc 与 `DefaultsAndMetricsIT`。

## src/test/java — 测试形态

- 离线单测(零 PG):`protocol` 字节级 + 组装/回放/接缝单测 + 三件套骨架单测 + 解耦等价
  (`DecoupledEquivalenceTest`/`StreamingDeliveryTest`/`SyncDeliveryTest`)。
- `it/` 子包:embedded engine + Testcontainers 真 PG 的集成测试(`StreamPgTestEnv` 单例
  postgres:18 容器、`StreamITBase` 继承 Debezium `AbstractAsyncEngineConnectorTest`);
  **surefire includes 显式含 `**/*IT.java`**(默认模式不含,见模块 pom)。坑位档:
  - 基座消费断言路径(VerifyRecord)在 JDK 17 链接期引用 Confluent 类(不在 Central)——
    以免校验四参重载替身(`StreamITBase.consumeRecordsUnchecked`);
  - embedded tests jar 根部自带 logback-test.xml(只给 io.debezium 开 INFO)——测试资源
    自带一份覆盖(logback-test.xml 于 src/test/resources);
  - `AbstractConnectorTest` 需要 assertj(基座 start() 直调,需显式测试依赖)。
  `ConnectPluginIT` 独立于此形态(不挂基座:真 Kafka Connect 容器装 assembly 插件跑,验收面与坑位见上 MS6 段)。
  IT 明细与断言清单见 `.superpowers/sdd/2026-09-01-ms2-pipeline-and-session/task-8-report.md`。
