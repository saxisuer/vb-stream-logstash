# MS5 实现计划：snapshot.mode 校验 + 事务元数据默认开 + 指标（日志行 + MBean）+ R2 审计

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落 MS5 四件——snapshot.mode 仅 no_data（校验+默认注入）、provide.transaction.metadata 默认 true、ThroughputMetrics 同构复刻（10s INFO 日志行 + MBean 暴露）、R2 增量快照交错审计文档。

**Architecture:** 配置面走"taskConfigs 注入默认 + Field 替换 + 构造器 fail-fast"三层（Debezium Field.Set.with 同名不替换是已核实坑）；指标复刻引擎 `ThroughputMetrics` 文字参照重写进连接器（模块边界 D2 零 vbstream import），四点插桩（reader 记读取/组装、consumer 记输出/分布/峰值/tick）；MBean 经 `DefaultStreamingChangeEventSourceMetrics` 子类 + 自建工厂换装 `Task.start` 的 `DefaultChangeEventSourceMetricsFactory`，运行态经 volatile bridge 从管道线程安全送达。

**Tech Stack:** Java 17 + Maven、Debezium 3.6.1（metrics 体系在 debezium-connector-common `io.debezium.pipeline.metrics`）、HdrHistogram 2.2.2（连接器 pom 需新增依赖）、Testcontainers。

**Spec:** `docs/superpowers/specs/2026-09-04-ms5-integration-surface-design.md`

## Global Constraints

- 日志一律 slf4j、禁止 System.out/System.err；消息用 `{}` 占位符
- **每个函数（含私有方法与测试辅助方法）必须有 javadoc 逻辑描述**（职责/关键步骤/边界/线程约束）
- 模块边界 D2：连接器代码零 `org.vastdata.vbstream` import（文字参照重写，非依赖）
- 每任务完成即 `git commit` 并 `git push origin worktree-ms35-logical-msg-guard`；commit message 尾加 `Co-Authored-By: Claude <noreply@anthropic.com>`
- 单测命令：`mvn test -pl vb-stream-connector-postgres-stream -Dtest=类名`；IT 需本机 Docker
- 工作目录：当前 worktree 根（路径相对它）

---

### Task 1: 配置面——snapshot.mode 仅 no_data + 事务元数据默认开

**Files:**
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/PostgresStreamConnectorConfig.java`（ALL_FIELDS 构造 + 新 Field + 校验器 + 构造器 fail-fast）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/PostgresStreamConnector.java`（taskConfigs 默认注入 + config() 展示默认）
- Test: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/PostgresStreamConnectorConfigTest.java`（增用例）；`PostgresStreamConnectorTest.java`（增 taskConfigs 注入用例如该文件已有基建）

**Interfaces:**
- Consumes: 父类 `PostgresConnectorConfig.SNAPSHOT_MODE`（名 "snapshot.mode"，默认 initial）、`SnapshotMode.NO_DATA` 枚举值；`CommonConnectorConfig` 的 "provide.transaction.metadata"（默认 false）
- Produces: `PostgresStreamConnectorConfig.SNAPSHOT_MODE_NO_DATA`（名仍为 "snapshot.mode"、默认 "no_data"、带 `validateSnapshotMode` 校验器的新 Field）；静态校验器 `static int validateSnapshotMode(Configuration, Field, Field.ValidationOutput)`；`PostgresStreamConnector.taskConfigs` 注入行为

**关键坑（已核实，照此实现）**：Debezium `Field.Set.with(Field...)` 内部是 `LinkedHashSet.add`——同名字段**保留旧 Field 弃新 Field**，同名覆盖必须先 `filtered` 挖掉再 `with` 补：

```java
/** snapshot.mode 的本连接器声明:同名替换父 Field(filtered 挖旧再补新——Set.with 同名保旧弃新),默认 no_data + 仅 no_data 校验。 */
public static final Field SNAPSHOT_MODE_NO_DATA = Field.create("snapshot.mode")
        .withDisplayName("Snapshot mode (no_data only)")
        .withType(Type.STRING)
        .withDefault("no_data")
        .withValidation(PostgresStreamConnectorConfig::validateSnapshotMode);
```

ALL_FIELDS 改为：

```java
public static final Field.Set ALL_FIELDS = PostgresConnectorConfig.ALL_FIELDS
        .filtered(f -> !"snapshot.mode".equals(f.name()))
        .with(SNAPSHOT_MODE_NO_DATA, SLOT_STREAMING, SLOT_TWO_PHASE, PIPE_DIR, PIPE_ROLL_CYCLE,
                SLOT_FEEDBACK_INTERVAL_MS, SLOT_MESSAGES);
```

校验器（形态仿既有 `validateSlotStreaming`）：

```java
/**
 * snapshot.mode 的仅-no_data 校验器(Field.ValidationOutput 三元契约):值为 no_data
 * (大小写宽容)返回 0;其余值(initial/always/when_needed/initial_only/custom 等)向
 * problems 记 1 条说明定位的消息并返回 1——本连接器不做快照数据抽取,该职能属
 * vanilla postgresql-connector。
 */
static int validateSnapshotMode(Configuration config, Field field, Field.ValidationOutput problems) {
    String value = config.getString(field);
    if (!"no_data".equalsIgnoreCase(value)) {
        problems.accept(field, value,
                "Invalid value '" + value + "': this connector supports snapshot.mode=no_data only "
                        + "(streaming-only connector, no snapshot data extraction — use the vanilla postgresql-connector for snapshots)");
        return 1;
    }
    return 0;
}
```

构造器 fail-fast（兜底：REST 校验被绕过/直构造路径）——`PostgresStreamConnectorConfig` 构造器 super 后追加：

```java
if (getSnapshotMode() != SnapshotMode.NO_DATA) {
    throw new org.apache.kafka.connect.errors.ConnectException(
            "snapshot.mode='" + getSnapshotMode() + "' is not supported: this connector supports "
                    + "snapshot.mode=no_data only (streaming-only, no snapshot data extraction)");
}
```

注意：`getSnapshotMode()` 读父 Field 默认 initial，故"缺省 = no_data"必须由注入保证（下一段）；直构造未注入缺省配置时构造器抛错属 fail-fast 预期。

`PostgresStreamConnector` 增 taskConfigs 覆盖（默认注入点——Connect 与 embedded engine 都经此取任务配置）：

```java
/**
 * 责任:任务配置的默认值注入——snapshot.mode 与 provide.transaction.metadata 缺省时
 * 显式置入本连接器默认(no_data / true)再委派父类。为什么不用 Field 默认值覆盖:父类
 * 静态 Field 的默认(initial / false)在运行期读取方(getSnapshotMode 等)经<b>父 Field
 * 引用</b>回落,子类同名字段替换不改变父引用的回落值;taskConfigs 是两框架
 * (Connect runtime 与 embedded engine)共同的配置必经点,在此注入使默认值对父类
 * 读取方同样生效。
 * 边界:用户显式配置的值原样透传(不覆盖);注入键恰两个,其余配置零触碰。
 */
@Override
public List<Map<String, String>> taskConfigs(Map<String, String> config) {
    Map<String, String> effective = new java.util.HashMap<>(config);
    effective.putIfAbsent("snapshot.mode", "no_data");
    effective.putIfAbsent("provide.transaction.metadata", "true");
    return super.taskConfigs(effective);
}
```

`config()` 的 ConfigDef 展示面：对 snapshot.mode 与 provide.transaction.metadata 的 define 覆盖默认值展示（`def.define("snapshot.mode", ConfigDef.Type.STRING, "no_data", ConfigDef.Importance.MEDIUM, "Streaming-only connector: snapshot.mode=no_data is the only supported value.")` 形态；provide.transaction.metadata 同理 Type.BOOLEAN 默认 TRUE、Importance.MEDIUM）。

- [ ] **Step 1: 写失败单测**（PostgresStreamConnectorConfigTest 增三条 + Connector taskConfigs 两条）

```java
/** snapshot.mode=no_data(显式小写)构造与校验双过——支持面。 */
@Test
void snapshotModeNoDataIsAccepted() {
    Configuration config = StreamPgTestEnv 非也——离线用 Configuration.create().with(...) 最小面(参照本测试类既有离线用例的组装形态);
    // 断言: validate 零问题; 构造成功且 getSnapshotMode()==NO_DATA
}

/** snapshot.mode=initial:validate 记 1 条问题(文案含 "no_data only")且构造抛 ConnectException。 */
@Test
void snapshotModeInitialIsRejected() { ... }

/** 缺省(不设 snapshot.mode):经 taskConfigs 注入后构造成功且 NO_DATA;注入前直接构造抛 ConnectException(父默认 initial 的 fail-fast 兜底)。 */
@Test
void snapshotModeDefaultsToNoDataViaTaskConfigsInjection() { ... }

/** taskConfigs 注入面:缺省两键被置入(no_data/true),显式值不被覆盖。 */
@Test
void taskConfigsInjectsDefaultsWithoutOverridingExplicitValues() { ... }

/** provide.transaction.metadata 缺省经注入为 true(事务元数据默认开)。 */
@Test
void transactionMetadataDefaultsToTrueViaInjection() { ... }
```

（离线 Configuration 最小面参照本测试类既有用例——先读该文件再写，别引 StreamPgTestEnv。）

- [ ] **Step 2: 跑测确认失败**（编译错或断言失败均可）——`mvn test -pl vb-stream-connector-postgres-stream -Dtest=PostgresStreamConnectorConfigTest,PostgresStreamConnectorTest`
- [ ] **Step 3: 按上列代码实现**
- [ ] **Step 4: 跑测全绿**（注意存量用例可能因 ALL_FIELDS 替换受影响——如有快照模式相关存量断言，按新语义修正并在报告注明）
- [ ] **Step 5: Commit & push**——`feat(ms5): snapshot.mode 仅 no_data(校验+注入默认) + 事务元数据默认开`

---

### Task 2: StreamThroughputMetrics 复刻 + 离线单测

**Files:**
- Modify: `vb-stream-connector-postgres-stream/pom.xml`（新增 HdrHistogram 依赖，坐标/版本与 vb-stream-engine/pom.xml:44-46 同款 `org.hdrhistogram:HdrHistogram:${hdrhistogram.version}`——注意 Maven 坐标小写、Java 包名大写 `org.HdrHistogram`，import 别写反）
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/StreamThroughputMetrics.java`
- Test: Create `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/StreamThroughputMetricsTest.java`

**Interfaces:**
- Consumes: 参照源 `vb-stream-engine/src/main/java/org/vastdata/vbstream/replication/ThroughputMetrics.java`（356 行，文字参照重写零 import）；参照测试 `vb-stream-engine/src/test/java/org/vastdata/vbstream/replication/ThroughputMetricsTest.java`
- Produces: 类 `StreamThroughputMetrics`，API 与引擎同构：构造 `StreamThroughputMetrics(long baselineNanos[, LongSupplier clock])`、`void onSlotMessage(byte[] raw)`、`void onTxHandedOff()`、`void onReplayedUnit(int payloadLength)`、`void onTxOutput(long durationNanos, long unitCount, long emittedRecords)`、`List<String> reportLines(long nowNanos)`、`record Totals(long slotBytes, long slotMessages, long assembledTxs, long outputBytes, long outputRecords, long outputTxs)`、`Totals totals()`（新增公开只读——Task 4 MBean 的窗口差分读源）

- [ ] **Step 1: 翻译引擎 ThroughputMetricsTest 为 StreamThroughputMetricsTest（先写,红）**——逐用例文字参照重写（包名/类名替换，断言值不变），类 javadoc 注明"引擎 ThroughputMetricsTest 的文字参照重写(口径与语义逐条同构)"
- [ ] **Step 2: 跑测确认编译失败**（类不存在）
- [ ] **Step 3: 实现 StreamThroughputMetrics**——通读引擎源后逐段文字参照重写：六计数 LongAdder、回放耗时/事务大小 2 位精度 SingleWriterRecorder（上限钳制 1h/10 亿单元）、秒桶峰值（SecPeak 内嵌类）、reportLines 三行（吞吐/分布/峰值）窗口差分口径、formatNanos/grouped 格式化。差异仅三处：包名与类名；logger 归属本包；新增 `totals()` 只读访问器（返回当前六计数快照,任意线程可读——LongAdder sum 本身线程安全）
- [ ] **Step 4: 跑测全绿**
- [ ] **Step 5: Commit & push**——`feat(ms5): StreamThroughputMetrics 同构复刻——三段速率/分布分位/八项峰值,10s INFO 日志行口径与引擎一致`

---

### Task 3: 指标插桩接线（四点）+ 接线单测

**Files:**
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/StreamedTransactionAssembler.java`（消息入口 onSlotMessage + 交接点 onTxHandedOff；构造器增 `StreamThroughputMetrics metrics` 参数）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/BucketReplayer.java`（readRange 回读处 onReplayedUnit；传入途径：构造器或方法参数,与现有接线形态一致——先读两文件定）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/TransactionConsumer.java`（processBucket 计时 + onTxOutput + 10s tick reportLines）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/PostgresStreamStreamingChangeEventSource.java`（execute 装配点建 metrics 并注入）
- Test: Create `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/StreamThroughputMetricsWiringTest.java`（参照 `vb-stream-engine/.../ThroughputMetricsWiringTest.java` 文字重写）

**Interfaces:**
- Consumes: Task 2 的 `StreamThroughputMetrics` API
- Produces: 插桩后组件构造签名变化（assembler 增 metrics 参）——同 task 内全部调用点同步改；`PostgresStreamStreamingChangeEventSource` 新增只读字段访问 `StreamThroughputMetrics throughputMetrics()`（Task 4 bridge 的读源之一；execute 前为 null）

插桩映射（引擎点 → 连接器点，全部沿用引擎口径）：

| 引擎点 | 连接器点 | 口径 |
|---|---|---|
| `TransactionAssembler.onMessage:226` onSlotMessage | `StreamedTransactionAssembler` raw 消息入口 | 收到即记（含控制消息与 'R'，字节=raw.length） |
| `TransactionAssembler:640` onTxHandedOff | `StreamedTransactionAssembler` 交接处（拷快照入队点） | 提交交接的事务才计，回滚丢弃不计 |
| `BucketReplayer:113` onReplayedUnit | `BucketReplayer` readRange 回调内 | 回读即记（aborted 过滤前） |
| `TransactionConsumer:146` onTxOutput + `:173` tick | `TransactionConsumer` processBucket 尾 + 循环内 10s tick | durationNanos=事务回放耗时,emitted=过滤后实付;tick 打 INFO 三行 |

装配（`PostgresStreamStreamingChangeEventSource.execute`，建 assembler 前）：

```java
StreamThroughputMetrics metrics = new StreamThroughputMetrics(System.nanoTime());
```

并作为新构造参传入 assembler（consumer/replayer 由 assembler 内部创建,metrics 随之传导——传参形态先读现有构造链再定,保持与现有依赖注入风格一致）。类增 getter `throughputMetrics()` 返回已建实例（字段 volatile,execute 前 null）。

- [ ] **Step 1: 翻译引擎 WiringTest 为 StreamThroughputMetricsWiringTest（先写,红）**——引擎该测试验证全链路插桩正确性（接线观测面）,翻译时装配形态按连接器构造链调整
- [ ] **Step 2: 跑测确认失败**
- [ ] **Step 3: 按映射表实现插桩 + 构造签名传导**（javadoc 同步更新受影响构造器）
- [ ] **Step 4: 跑本测试 + 存量 `StreamedTransactionAssemblerTest`/`BucketReplayerTest`/`DecoupledEquivalenceTest`/`StreamingDeliveryTest`（构造签名变化波及面）全绿**
- [ ] **Step 5: Commit & push**——`feat(ms5): 指标四点插桩——reader 记读取/组装,consumer 记输出/分布/峰值 + 10s tick`

---

### Task 4: MBean 面——bridge + metrics 类 + 工厂换装 + 访问器

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/StreamMetricsBridge.java`
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/StreamStreamingChangeEventSourceMetrics.java`
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/StreamChangeEventSourceMetricsFactory.java`
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/PostgresStreamConnectorTask.java`（`new DefaultChangeEventSourceMetricsFactory<>()` 换 `new StreamChangeEventSourceMetricsFactory(bridge)`；bridge 实例在 start 建立并传入 factory 与流式源——流式源构造/或经 factory 装配后由 execute 填充,选一种并保持单向依赖）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/ReplicationSession.java`（新增 `long lastReceiveLsn()` 只读访问器——run 循环维护的最近收到 LSN,volatile 写 reader 线程/任意线程读）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/StreamedTransactionAssembler.java`（新增 `int pendingPreparedCount()`——`preparedByGid.size()` 读需与写同锁:方法体 `synchronized (preparedByGid)` 或复用既有写路径锁,先读该类并发面再定）
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/MessagePipe.java`（新增 `long diskUsageBytes()`——`Files.walk` 遍历 dir 求和常规文件 size,IO 异常记 WARN 返回 -1）
- Test: Create `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/StreamStreamingChangeEventSourceMetricsTest.java`

**Interfaces:**
- Consumes: Debezium `DefaultChangeEventSourceMetricsFactory<P>` / `DefaultStreamingChangeEventSourceMetrics<P>`（io.debezium.pipeline.metrics,debezium-connector-common 3.6.1——工厂方法签名 `<T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<P> getStreamingMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics, EventMetadataProvider eventMetadataProvider, CapturedTablesSupplier capturedTablesSupplier)`）；Task 2/3 的 metrics 与 throughputMetrics()；Task 4 自建访问器
- Produces: `StreamMetricsBridge`（volatile 供应商槽:`setSuppliers(LongSupplier lagBytes, ...)` 由流式源 execute 填充,getters 未装配时返回 0/-1）；MBean 属性 getter（MXBean 惯例 get 前缀,框架经 metrics.register 自动注册）：`getSlotReadBytesPerSecond()` / `getSlotReadMessagesPerSecond()` / `getAssembledTxsPerSecond()` / `getOutputRecordsPerSecond()` / `getOutputBytesPerSecond()` / `getLagBytes()` / `getPendingPreparedCount()` / `getPipeDiskUsageBytes()`

**Bridge 设计（线程安全核心）**：计数与速率的写方是 consumer/reader 线程,读方是 JMX 任意线程——速率在 consumer 的 10s tick 内预计算（窗口差分:`Totals` 快照 + 上一快照 + 窗口秒数 → volatile 字段写一次）,JMX 侧只读 volatile,零锁零计算。`lagBytes = session.lastReceiveLsn() - frontier.get()`、`pipeDiskUsageBytes` 同在 tick 采样（目录遍历不进 JMX 读路径）。

- [ ] **Step 1: 写失败单测**——metrics 类经注入 bridge/假 suppliers 断言各 getter 值;未装配槽返回 0/-1;bridge setSuppliers 后 getters 生效
- [ ] **Step 2: 跑测确认失败**
- [ ] **Step 3: 实现三个新类 + Task 换装 + 三个访问器**（tick 内预计算挂 TransactionConsumer 已有 10s 周期处——metrics 报告与 bridge 快照同 tick）
- [ ] **Step 4: 跑测全绿 + `PostgresStreamConnectorTaskTest` 存量全绿**
- [ ] **Step 5: Commit & push**——`feat(ms5): 指标 MBean 面——三段速率/滞后/挂起 prepared/管道磁盘占用经 Debezium metrics 体系暴露`

---

### Task 5: IT——缺省配置 + snapshot.mode 拒绝 + metrics 观测

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/DefaultsAndMetricsIT.java`

**Interfaces:**
- Consumes: `StreamITBase`（baseConfig 会显式设 snapshot.mode=no_data——本 IT 场景①**不用 baseConfig 的该键**：以 `.with("snapshot.mode", null)` 不行,Configuration 无删键——改为自组最小配置复制 baseConfig 语义但**不含** snapshot.mode 与 provide.transaction.metadata 两键,验证注入路径）；`SlotTwoPhaseMismatchIT` 的引擎失败信号模式（CompletionCallback + await,场景②复用）；Task 4 的 metrics bridge（经反射或包内可见性持有断言——优选:IT 同包直接持有 factory 建的 metrics 实例不可行,则断言日志行存在（logback ListAppender 收 INFO 三行）+ throughputMetrics() getter 非零）

- [ ] **Step 1: 写三场景**：
  1. `defaultsYieldNoDataSnapshotAndTransactionMetadata`：自组最小配置（无 snapshot.mode/metadata 两键）start → 写入小事务 → 消费到数据记录 + **事务元数据 topic 记录**（注入 provide.transaction.metadata=true 的验收面）且无快照记录（op="r" 零条）
  2. `snapshotModeInitialFailsStartup`：baseConfig + `.with("snapshot.mode", "initial")` → 引擎启动失败（CompletionCallback 模式,复用 SlotTwoPhaseMismatchIT 的 await 形态）,异常链含 "no_data only"
  3. `metricsObservableAfterTraffic`：baseConfig start → 写入若干事务消费到 → 轮询断言 `throughputMetrics()` 可达且 `totals().outputRecords() > 0`、日志出现 INFO 吞吐行（ListAppender 或降级仅断言 totals——若流式源实例从 IT 不可达,经 metrics MBean getter 亦不可达,则以日志行断言为准并在报告注明观测路径）
- [ ] **Step 2: 跑 IT 三场景全绿**（需 Docker）
- [ ] **Step 3: 全模块回归 `mvn test -pl vb-stream-connector-postgres-stream` 记录总数**
- [ ] **Step 4: Commit & push**——`test(ms5): 缺省配置 IT(注入 no_data+事务元数据) + snapshot.mode=initial 启动拒绝 + 指标观测`

---

### Task 6: R2 审计文档 + 记档收官

**Files:**
- Create: `docs/superpowers/specs/2026-09-04-ms5-r2-incremental-snapshot-audit.md`
- Modify: `CLAUDE.md`（根,connector 源码结构段 MS5 句 + IT 清单 + 用例计数）；`vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/CLAUDE.md`（模块记档）

**审计文档内容骨架**（写前先读 `2026-09-02-ms2-r1-r3-audit.md` 的行文形态）：
1. 审计范围与方法（静态代码审读:R1/R3 审计结论 + vanilla `IncrementalSnapshot`/`SignalProcessor` 3.6.1 源码——sources jar 在 `/Users/saxisuer/Documents/Repository/io/debezium/` 下可 unzip -p 读）
2. 交错面①——**第二 dispatch 线程**：vanilla 增量快照在 coordinator/独立线程发 chunk 记录走同一 `PostgresEventDispatcher`；R1 结论"dispatch 全部状态写仅 consumer 线程"被打破点逐项列（dispatcher 内共享可变态、`StreamEventMetadataProvider`、offset 写）
3. 交错面②——main 连接：信号表轮询/chunk SELECT 与 'R' enrich 的 R3 独占时序是否仍成立（增量快照读发生在 streaming 期间,时序独占前提消失）
4. 交错面③——offset/前沿：chunk 事务的 offset 锚定与 End 锚定输出前沿的交互（chunk 期间 frontier 是否被钉住→slot 滞后;lsn_commit 语义冲突）
5. 结论：接入前置条件清单 + 是否建议 MS6 接入（给出建议与理由,不留 TBD）

- [ ] **Step 1: 写审计文档**（读码取证,每个论断附 文件:行 或 jar 源码引证）
- [ ] **Step 2: CLAUDE.md 两处记档**（MS5 句形态参照 MS4 记档;计数按 Task 5 实际数字）
- [ ] **Step 3: Commit & push**——`docs(ms5): R2 增量快照交错审计(只审不接,接入决策留 MS6) + MS5 收官记档`

---

## 自审记录

- **Spec 覆盖**：§2 → Task 1；§3 → Task 1（注入）；§4 → Task 6；§5.1 → Task 2/3；§5.2 → Task 4；§6 → Task 1/4/5 单测与 IT + Task 6 记档
- **占位符**：Task 3 插桩的构造参数传导形态与 Task 5 场景③的 metrics 观测路径标注了"先读现有形态再定/降级路径"——两处均为实现期择形（约束已钉：接线映射表逐点给定、断言核心给定），非 TBD
- **类型一致性**：`StreamThroughputMetrics` API 在 Task 2 定义、Task 3/4 引用一致；`StreamMetricsBridge.setSuppliers` 与三个访问器（`lastReceiveLsn`/`pendingPreparedCount`/`diskUsageBytes`）在 Task 4 内定义引用一致
