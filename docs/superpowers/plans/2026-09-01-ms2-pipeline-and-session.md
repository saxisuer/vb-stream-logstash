# MS2 管道与会话 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `vb-stream-connector-postgres-stream` 落成流式管道与 Debezium 接线:自建复制会话(raw drain + LSN 前沿封顶回传)、CQ 管道、桶记账组装器、双线程解耦、回放经 Debezium dispatcher 发射 Kafka Connect 记录、End 锚定 offset——验收 = 流式大事务端到端进 Kafka 记录(embedded engine IT,真 PG)。

**Architecture:** 引擎 replication 包按 MS1 模式 1:1 重写(会话/管道/桶/组装器/回放/事件族,行为红线清单见 Global Constraints);Debezium 接线层新写(StreamingChangeEventSource 作 coordinator 线程上的监督壳,自起 reader/consumer 双线程;事件→dispatcher 映射;offset 事务边界语义)。依赖 Debezium **3.6.1.Final**(签名经 sources jar 逐行核实,与 3.7 clone 的差异坑清单见 Global Constraints)。

**Tech Stack:** Java 17、chronicle-queue(引擎同源)、pgjdbc 42.7.13(显式 pin)、Debezium 3.6.1.Final、Testcontainers postgres:18、debezium-embedded(async 引擎 + tests jar)。

**Spec:** `docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md` §5.2-§5.6、§6-§8(R1/R3)、§10 MS2。

## Global Constraints

- **零 `import org.vastdata.vbstream`**(D2);引擎源码只读参照,行为权威
- **行为红线**(翻译最易走样处,审查硬口径):seq≡CQ index 且控制消息先 append 再路由;`lastAppendOwner=null` 断段;hasPrefix 桶级不变量(混现 ISE);两个低水位作用域差异(registry 剪枝不含已交接桶 / CQ 删档含非 DONE 桶);快照在 handoff 瞬间冻结;**End 返回后才推进前沿**;**先交付后计数**;drain 空轮才睡(100ms);`capFeedback` 前沿 0=无 cap;毒丸=xid -1;join 60s;StreamAbort 的 PARALLEL 附加字段与 decoder mode 一致;'M' flags 偏移 流内 5/顶层 1;MessagePipe 无信封帧(一条 CQ 记录=一条完整消息原样字节);wipe-on-open;首条 index 必须等于 firstIndex 否则 ISE
- **D7 停机不排干**:连接器停机走 `shutdownFast()`(毒丸+interrupt consumer+关管道,不 join 等回放);测试确定性断言用 drain 形态 `close()`(等价引擎)——两个方法都要
- **Debezium 3.6.1 坑清单**(以 3.6.1 为准,勿抄 3.7 clone):ChangeEventQueue.Builder **无** `pollDispatchInterval`;`PostgresConnection.createTypeRegistry(JdbcConfiguration)` **1 参**;取历史 offset 用 `getPreviousOffsets`(非 getSinglePartitionPreviousOffsets);offset 初始加载方法名 `PostgresOffsetContext.initialContext(...)`;**无** `setLastCommitLsn`——用 `updateCommitPosition(Lsn, Lsn)`;`updateWalPosition` 7 参形态;3.6.1 offset **无** `lsn_events_processed`;`PostgresEventMetadataProvider` 包私有须自实现;`PostgresChangeRecordEmitter::updateSchema` 包私有用 `EventDispatcher.ignoreMissingSchema`;`PostgresSchema` protected ctor 须子类化;PostgresEventDispatcher 12 参构造
- **pgjdbc 对齐(MS1 终审交接项)**:connector pom 显式 `org.postgresql:postgresql:${postgresql.version}`(根属性 42.7.13,压掉 Debezium 传递的 42.7.11);chronicle-queue 显式声明(带 chronicle-analytics exclusion,照引擎 pom 注释)
- javadoc 全覆盖(含测试辅助);slf4j 禁 System.out;JUnit 静态导入断言
- 每任务 commit + push;`Co-Authored-By: Claude <noreply@anthropic.com>`;验证用 `clean`;单模块命令 `-pl vb-stream-connector-postgres-stream`
- IT 需 Docker;流式数据构造照引擎经验(不可压缩载荷 `(SELECT string_agg(md5(random()::text),'') FROM generate_series(1,512))`≈16KB、全局 rb->size 阈值、分批跨秒写入)

**参照速查**:**ENG** = `vb-stream-engine/src/main/java/org/vastdata/vbstream/replication`,**ENG-T** = 对应 test,**ENG-IT** = `vb-stream-engine/src/test/java/org/vastdata/vbstream/it`;**NEW** = `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream`,**NEW-T** = 对应 test;**DBZ361** = 本地 sources jar(`/Users/saxisuer/Documents/Repository/io/debezium/<artifact>/3.6.1.Final/<artifact>-3.6.1.Final-sources.jar`,zip 解包读)。

---

### Task 0: 依赖与配置补齐(pgjdbc/chronicle/embedded-IT 依赖)

**Files:**
- Modify: `vb-stream-connector-postgres-stream/pom.xml`

**Interfaces:**
- Produces: 编译期 chronicle-queue、pgjdbc 42.7.13;测试期 debezium-embedded(+tests classifier)、debezium-util(tests)、connect-runtime/connect-json、testcontainers-postgresql、awaitility

- [ ] **Step 1: 编译域两条依赖**(junit 之后追加)

```xml
        <!-- pgjdbc:显式 pin 根属性 42.7.13(MS1 终审交接项——压掉 Debezium 传递的 42.7.11,
             复制会话 API 与引擎同版本;两模块若共类路径也不会分叉) -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>${postgresql.version}</version>
        </dependency>

        <!-- Chronicle Queue:CQ 主缓冲管道(MS2 MessagePipe)。排除 chronicle-analytics
             (core 反射缺类回落 MuteAnalytics,官方 DISCLAIMER 认可的关闭方式,照引擎 pom 同款) -->
        <dependency>
            <groupId>net.openhft</groupId>
            <artifactId>chronicle-queue</artifactId>
            <version>${chronicle-queue.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>net.openhft</groupId>
                    <artifactId>chronicle-analytics</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```

- [ ] **Step 2: 测试域 IT 依赖**(3.6.1.Final 实测坐标)

```xml
        <!-- embedded engine IT(async 引擎是 3.x 唯一实现;tests classifier 提供测试基类) -->
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-embedded</artifactId>
            <version>${debezium.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-embedded</artifactId>
            <version>${debezium.version}</version>
            <classifier>tests</classifier>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-util</artifactId>
            <version>${debezium.version}</version>
            <classifier>tests</classifier>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>connect-runtime</artifactId>
            <version>${kafka.connect-api.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>connect-json</artifactId>
            <version>${kafka.connect-api.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>4.3.0</version>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 3: 验证**:`mvn clean test`(三段 SUCCESS,引擎 177+connector 56 不回归)+ `mvn -pl vb-stream-connector-postgres-stream dependency:tree` 确认 postgresql:42.7.13 与 chronicle-queue 生效、无 vb-stream-engine
- [ ] **Step 4: Commit + push**(`build: MS2 依赖补齐——pgjdbc 42.7.13 pin + chronicle-queue + embedded-IT 测试域`)

---

### Task 1: 复制会话 ReplicationSession + RawMessageListener

**Files:**
- Create: `NEW/RawMessageListener.java`、`NEW/ReplicationSession.java`
- Test: `NEW-T/ReplicationSessionTest.java`(翻译 ENG-T/PgReplicationSessionTest.java 140 行,不含需真库的用例——分流到 Task 8 IT)

**Interfaces:**
- Produces(Task 4/7 消费):`RawMessageListener.onRaw(byte[])`(独占数组承诺);`ReplicationSession`(双 JDBC 连接;`open()`/`ensureSlot()`/`start(...)`/`run(listener, LongSupplier outputFrontier)`/`close()`/`lastReceiveLsn()`;包私有静态 `capFeedback(long received, long frontier)` 与 `drainPending`)

**参照**:ENG/PgReplicationSession.java(225 行)逐行;**红线**:run 循环五步序(isClosed 守卫→drainPending→capFeedback+setAppliedLSN/setFlushedLSN→满 feedbackIntervalSeconds 才 forceUpdateStatus→空轮 sleep 100ms);槽选项恰 4 项(proto_version/publication_names/streaming/two_phase);ensureSlot 第 4 参 twoPhase、SQLState 42710 复用;replicationUrl 必带 `replication=database&assumeMinServerVersion=9.4`;close 次序 流→复制连接→SQL 连接。配置来源改为从 `PostgresStreamConnectorConfig` 读(host/port/db/user/password/slot/publication/protoVersion/streamingMode/twoPhase + 新增 feedback 配置),`ReplicationConfig` record 不再需要——直接构造会话(构造参数自定,javadoc 注明)。
**配置项追加**:`slot.feedback.interval.ms`(默认 10000,`Field::isPositiveInteger`)入 Config。

- [ ] **Step 1-5**:TDD(翻译用例:capFeedback 三态/槽选项拼装/replicationUrl 形态/42710 复用语义 mock 化或纯函数化——需真库的用例移 Task 8)→ commit `feat: MS2 复制会话——raw drain 轮询 + LSN 前沿封顶回传(引擎 PgReplicationSession 1:1)`

---

### Task 2: MessagePipe + 管道配置

**Files:**
- Create: `NEW/MessagePipe.java`(包内可见性同引擎)、`NEW/PipeConfig` 并入 Config(已存在 pipe.dir/pipe.roll.cycle 字段,补 `parseRollCycle` 大小写宽容 + 未知 IAE 校验器)
- Test: `NEW-T/MessagePipeTest.java`(翻译 ENG-T 157 行全用例:往返/wipe-on-open/错位 ISE/releaseBelow 删档纯函数/档位节流)

**参照**:ENG/MessagePipe.java(351 行)逐行;红线见 Global Constraints。CQ 目录 = Config.pipeDir()(相对工作目录),wipe-on-open。

- [ ] **Step 1-5**:TDD → commit `feat: MS2 CQ 管道——MessagePipe 无信封帧 + wipe-on-open + 档位节流删档(引擎 1:1)`

---

### Task 3: 版本化 Relation 注册表(wire + Debezium Table 双形态)

**Files:**
- Create: `NEW/VersionedRelationRegistry.java`、`NEW/RelationSnapshot.java`(包私有)、`NEW/RelationLookup.java`、`NEW/ResolvedRelation.java`(新:record 持 wire `Relation` + Debezium `Table`)
- Test: 翻译 ENG-T/VersionedRelationRegistryTest.java(84 行)+ RelationSnapshotTest(62 行)+ RelationRegistryTest(43 行,RelationRegistry 父类若快照路径不需要可并入)

**Interfaces:**
- Produces(Task 4 组装器/Task 7 装配消费):`VersionedRelationRegistry.accept(long seq, ResolvedRelation)` / `require(oid, asOfSeq)` / `pruneBelow(minSeq)` / `snapshot(oidSet, maxSeq)→RelationSnapshot`;`RelationSnapshot.require(oid, asOfSeq)→ResolvedRelation` / `find(oid)`

**参照**:ENG/VersionedRelationRegistry.java(227 行)+ RelationSnapshot(104 行)逐行(floor 语义二分、前缀快照浅拷、prune 保留生效版)。**新增部分**:`ResolvedRelation` 的 Table 构建在 Task 7('R' 处理时 reader 线程做 JDBC enrich);本任务 registry 泛型即 ResolvedRelation,单测用假 Table(Table.editor() 造最小 Table 即可,不连库)。

- [ ] **Step 1-5**:TDD → commit `feat: MS2 版本化注册表——oid→(seq, ResolvedRelation) 版本日志 + asOf 前缀快照(引擎 1:1,泛型扩为 wire+Table 双形态)`

---

### Task 4: 桶记账 + 组装器(同步形态)+ 测试字节基建扩展

**Files:**
- Create: `NEW/TxBuffer.java`(包私有)、`NEW/BucketState.java`、`NEW/RawPeeks.java`、`NEW/StreamedTransactionAssembler.java`
- Modify: `NEW-T/protocol/MsgBuilder.java` 或新建 `NEW-T/PgWire.java`(翻译 ENG-T/PgWire.java 322 行:19 种消息构造器 + `streamed(xid,msg)` 前缀包装 + tuple/textCol/nullCol)
- Test: `NEW-T/StreamedTransactionAssemblerTest.java`(翻译 ENG-T/TransactionAssemblerTest.java **767 行全用例**——五场景 + 全部 ISE fail-fast;经 Task 8 的 ThroughputMetrics 不存在,构造签名不含 metrics 参数)

**Interfaces:**
- Consumes: Task 1-3 + MS1 协议层
- Produces: `StreamedTransactionAssembler`(同步构造:listener/mode/registry/pipeConfig/decodedObserver;异步构造 Task 6)。事件族 **本任务一并落**:`TransactionEvent`(Begin 七组件/End)、`TxChange` sealed + `RowChange(dml, relation, before, after, streamXid, long seq)`/`TruncateChange/MsgChange`(**+seq 组件**——connector 侧 asOf Table 解析需要,相对引擎的已文档化偏差)、`TransactionKind`、`DmlKind`、`StreamingTransactionListener`(connector 内部契约,不导出引擎语义)

**参照**:ENG/TransactionAssembler.java(765 行)逐行;红线清单全量适用。metrics 接缝:引擎构造穿 ThroughputMetrics(MS5)——connector 侧**删除该参数**,调用点(assembler/consumer/replayer)一并去掉;MS5 再以监听器形态加回。**Relation 解析接缝**:引擎 'R' 路由直接 `registry.accept(seq, wireRelation)`;connector 侧 registry 存 `ResolvedRelation`,故本任务定义函数式接缝 `NEW/RelationResolver.java`(`ResolvedRelation resolve(long seq, Relation wire)`),组装器构造参数注入——测试用假实现(直接包 wire Relation + Table.editor() 造最小 Table),Task 7 的 `RelationTableFactory` 是真实现(JDBC enrich)。
**PgWire 约束**:`streamAbort(xid,subxid)` 只产非 parallel 形态——组装器测试须以非 PARALLEL 构造(引擎同款约束,注释写明)。

- [ ] **Step 1-5**:TDD(PgWire 先行,组装器测试按引擎分场景逐组翻译)→ commit `feat: MS2 桶记账组装器(同步)+ 事件族 + PgWire 字节基建(引擎 1:1,+seq 组件偏差)`

---

### Task 5: 回放与消费者(processBucket)+ TransactionRecorder

**Files:**
- Create: `NEW/BucketReplayer.java`、`NEW/TransactionConsumer.java`、`NEW/BucketTableResolver.java`(新:listener 侧按 (oid,seq) 从桶快照解析 asOf Table 的接缝,供 Task 7 注入真实实现;本任务以直接透传 ResolvedRelation 的假实现测试)
- Test: 翻译 ENG-T/BucketReplayerTest(245 行)、StreamingDeliveryTest(119 行)、TransactionRecorderTest(74 行)+ 新建 `NEW-T/TransactionRecorder.java`(测试等价币,翻译 ENG/TransactionRecorder 63 行)与 `NEW-T/Transaction.java`(测试值对象,29 行)

**参照**:ENG/BucketReplayer(171 行)+ TransactionConsumer(184 行)逐行;红线:**End 发出后**才 `outputFrontier.accumulateAndGet(endLsn, max)`;先交付后计数;listener 抛出→End 永不发→前沿不推进;consumer 循环 poll(1s)+10s 统计行(简化:保留滞留 WARN,metrics 三行去掉)+ 毒丸退出。`TransactionConsumer.processBucket` 为同步/异步共用(引擎同款)。

- [ ] **Step 1-5**:TDD → commit `feat: MS2 回放与消费者——BucketReplayer asOf 渲染 + processBucket End 锚定前沿(引擎 1:1)`

---

### Task 6: 双线程异步形态 + 停机两形态

**Files:**
- Modify: `NEW/StreamedTransactionAssembler.java`(异步构造:非守护 `transaction-consumer` 线程;`close()`=毒丸+join 60s 排干;`shutdownFast()`=毒丸+interrupt+不 join(D7))
- Test: 翻译 ENG-T/DecoupledEquivalenceTest(146 行:同一字节流同步/异步事件流全等)+ 新增 `shutdownFastDoesNotWaitForPendingBuckets`(D7 验收:入队大桶后 shutdownFast 立即返回,listener 未收 End)

**参照**:ENG/TransactionAssembler 构造 L177-197 + close L277-296;线程模型照 ENG(名字 `vb-pgoutput-reader` 由 Task 7 的 source 起,consumer 线程名保持 `transaction-consumer`)。

- [ ] **Step 1-5**:TDD → commit `feat: MS2 双线程异步形态 + 停机两形态(close 排干供测试/shutdownFast 供连接器 D7)`

---

### Task 7: Debezium 接线(装配 + 事件映射 + offset 语义)

**Files:**
- Create: `NEW/StreamPostgresSchema.java`(extends PostgresSchema,暴露 protected ctor)、`NEW/StreamEventMetadataProvider.java`(implements EventMetadataProvider,4 方法)、`NEW/StreamChangeEventSourceFactory.java`(snapshot→skipped 占位[MS5 换真快照]、incremental→empty、streaming→下行)、`NEW/PostgresStreamStreamingChangeEventSource.java`、`NEW/DispatcherTransactionListener.java`(事件→dispatcher 映射)、`NEW/RowChangeEmitter.java`(ChangeRecordEmitter<P> 实现)、`NEW/RelationTableFactory.java`('R'→ResolvedRelation:wire 解码 + JDBC enrich 建 Table)
- Modify: `NEW/PostgresStreamConnectorTask.java`(start 从返 null 换真装配)
- Test: offset 事务边界单测(`DispatcherTransactionListener` 直接测:Begin 后 offsetContext.getOffset() 的 lsn=lsn_commit=endLsn、End 语义)+ RelationTableFactory 单测(PgWire relation 字节 + mock JDBC 元数据;真库归 Task 8)。**Task.start 真装配不自测**(需真库),由 Task 8 IT 全链路覆盖

**Interfaces(3.6.1.Final 实测,照抄勿改):**
- Task.start 装配序(模板 DBZ361 `PostgresConnectorTask.java:101-284`,替换点标 ⚠):config → charset(临时连接 try-with-resources)→ `PostgresConnection.createTypeRegistry(jdbcConfig)`(1 参 ⚠)→ connectionFactory/mainConnection(setAutoCommit false)→ `StreamPostgresSchema`(子类 ⚠)→ partitionProvider(`new PostgresPartition.Provider(...)`)/offsetLoader(`new PostgresOffsetContext.Loader(...)`)→ `getPreviousOffsets(provider, loader)`(⚠ 名字)→ queue(`ChangeEventQueue.Builder` **无 pollDispatchInterval** ⚠,pollInterval/maxQueueSize/maxBatchSize 照 Config)→ `PostgresErrorHandler` → `StreamEventMetadataProvider`(自实现 ⚠)→ `SignalProcessor`(可 new 最小实例,7 参照 vanilla :217)→ `new PostgresEventDispatcher<>(...)` 12 参(inconsistentSchemaHandler 用 `EventDispatcher::ignoreMissingSchema` ⚠)→ `NotificationService` → `new ChangeEventSourceCoordinator<>(11 参基类,`StreamChangeEventSourceFactory`、`DefaultChangeEventSourceMetricsFactory<>())` → `coordinator.start(taskContext, queue, metadataProvider)` → return
- `PostgresStreamStreamingChangeEventSource.execute(context, partition, offsetContext)`:coordinator 线程上的监督壳——建 `RelationTableFactory`(持 main JDBC 连接)→ 建异步 `StreamedTransactionAssembler`(listener=`DispatcherTransactionListener`,frontier=AtomicLong)→ 起 `vb-pgoutput-reader` 线程跑 `session.run(assembler, frontier::get)` → `while (context.isRunning() && !failed) sleep(200)` → 停机次序:`session.close()` → reader.join(5s) → `assembler.shutdownFast()`(D7);reader/consumer 失败 → `errorHandler.setProducerThrowable(e)` 并置 failed
- `DispatcherTransactionListener`(consumer 线程回调):Begin → `offsetContext.updateCommitPosition(Lsn.valueOf(endLsn), Lsn.valueOf(endLsn))`(事务边界 offset:此后本事务每条记录 getOffset() 的 lsn/lsn_commit 皆 endLsn)+ `dispatcher.dispatchTransactionStartedEvent(partition, "xid-"+xid, offsetContext, commitTs)`;TxChange → 解析 asOof Table(`bucketSnapshot.require(oid, seq).table()`)→ **版本安装**:若该 Table != schema 当前已装版本则 `streamSchema.applySchemaChangeForTable(oid, table)`(单写者=consumer 线程,DDL 稀疏故重建开销可接受)→ `dispatcher.dispatchDataChangeEvent(partition, tableId, new RowChangeEmitter(...))`;End → `dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, commitTs)`
- `RowChangeEmitter.emitChangeRecords(TableSchema, Receiver)`:Operation 映射 INSERT/UPDATE/DELETE;值映射 `TupleValue`→Text 传 String / Binary 传 byte[] / Null 传 null / **UnchangedToast**:UPDATE 有 before 值沿用同列 before,否则照 vanilla PostgresChangeRecordEmitter 的未变更 TOAST 处理(DBZ361 sources `PostgresChangeRecordEmitter.java` 为实现期参照,以它为准并在 javadoc 记口径);Truncate/LogicalMsg 变更:MS2 跳过发射(WARN DEBUG 级)——Truncate 变更族 MS3 补,不阻塞验收
- `RelationTableFactory.relation(seq, wireRelation)→ResolvedRelation`:列按 wire(名称/oid/typmod 顺序=元组位序真源),PK=wire flags bit0 列名;类型名/jdbcType 经 `TypeRegistry.get(oid)`;可选性/默认值 JDBC enrich(`getTableColumnsForDecoder`+`readPrimaryKeyNames`,reader 线程持 main 连接=R3 审计答案:MS2 期 main JDBC 连接读者线程独占)
- `commitOffset(Map,Map)` 实现(照 vanilla :498-562 简化):取 offset 的 lsn_commit → `session.flushLsnBelow` 只前进不后退
- 心跳(R1 答案):监督壳周期(空转时)`dispatcher.dispatchHeartbeatEventAlsoToIncrementalSnapshot(partition, offsetContext)`;**dispatchDataChangeEvent/Transaction* 全部仅 consumer 线程调用**;xmin fetch MS2 不启(默认 0)

- [ ] **Step 1-5**:TDD(可离线部分)+ 装配编译 → commit `feat: MS2 Debezium 接线——Task.start 真装配 + 事件→dispatcher 映射 + 事务边界 offset + End 锚定 flush`

---

### Task 8: 端到端 IT(embedded engine + 真 PG)+ R1/R3 审计落档

**Files:**
- Create: `NEW-T/it/StreamPgTestEnv.java`(翻译 ENG-IT/PgTestEnv:postgres:18 容器 + 同款 command 参数 + 槽/publication/LSN 查询工具)、`NEW-T/it/StreamITBase.java`(extends `io.debezium.embedded.async.AbstractAsyncEngineConnectorTest`;`initializeConnectorTestFramework()`;start(connector.class, config) 模式——harness 自动补 offset.storage 文件与 name)
- Create: `NEW-T/it/EndToEndStreamedTxIT.java`(验收主角)、`NEW-T/it/ReaderUnblockedIT.java`、`NEW-T/it/FrontierCapIT.java`、`NEW-T/it/ReaderThroughputIT.java`
- Create: `NEW/CLAUDE.md` 更新(MS2 组件节)+ 根 CLAUDE.md connector 行追加;审计结论文档 `docs/superpowers/specs/2026-09-01-ms2-r1-r3-audit.md`(R1:dispatch 单写者清单;R3:main 连接 reader 独占证明)

**验收场景(EndToEndStreamedTxIT)**:postgres:18 + `logical_decoding_work_mem=64kB`;表 + publication;`snapshot.mode=never` + `slot.streaming=parallel` + `slot.two.phase=true` + `provide.transaction.metadata=true`;单条连接开事务插 6 行×16KB 不可压缩载荷(行间 sleep 触发进行中驱逐)→ COMMIT → `consumeRecordsByTopic(N)` 断言:6 条 INSERT 记录进 topic、值结构列正确(text 列)、事务 topic 收 BEGIN/END、`source.lsn`=事务 endLsn 且组内一致;再验一个普通小事务。**ReaderUnblockedIT/FrontierCapIT/ReaderThroughputIT** 按 ENG-IT 同名测试翻译(断言面换成:记录数增长/confirmed_flush 钉住/节拍 35s)。

- [ ] **Step 1-5**:IT 先红后绿 → commit `feat: MS2 端到端验收——流式大事务进 Kafka 记录 + reader 不阻塞 + frontier 封顶 IT + R1/R3 审计入档`

---

## 验收汇总(对照 spec §10 MS2)

- [ ] `mvn clean test` 全绿(引擎 177 + connector 56+新增;IT 需 Docker)
- [ ] EndToEndStreamedTxIT:流式大事务(parallel 模式)6×16KB 记录完整进 Kafka,事务元数据 BEGIN/END 齐,per-record offset=事务 endLsn
- [ ] ReaderUnblockedIT(consumer 阻塞期间记录持续接收)+ FrontierCapIT(未输出事务钉住 confirmed_flush)+ ReaderThroughputIT(节拍回归)
- [ ] 零引擎 import;pgjdbc 42.7.13 生效
- [ ] R1(dispatch 单写者:数据/事务事件仅 consumer 线程;心跳仅监督线程)/R3(main JDBC 连接 reader 独占)审计结论文档在档
- [ ] D7:shutdownFast 不等回放(IT 或单测钉住);测试确定性断言走 close() 排干

## 备注

- typmod"PG 18 新增"注释核查属引擎模块文档修正(MS1 终审交接),不占 MS2 任务位——单独小提交跟进
- Truncate/LogicalMsg 变更不发射(MS3 补);ThroughputMetrics MS5;快照 MS5;增量快照 MS5 专项
