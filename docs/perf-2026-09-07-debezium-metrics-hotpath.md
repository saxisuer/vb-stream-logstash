# Debezium 内建 metrics 事件热路径剖析（2026-09-07，Arthas 火焰图归因全记录）

**问题**：connector 化（vb-stream-reader + embedded engine）输出链实测 ~4.5 万 rec/s，比引擎
直渲染形态（50 万 rec/s）低一个量级；Connect 面参数（queue/batch/flush/UNORDERED）与宿主
Consumer 渲染短路四组对照全部无效。
**结论**：Debezium 内建 metrics 在每条数据事件的 dispatch 热路径上无条件做两件重活
（摘要字符串化 + 计量队列唤醒），合计占 consumer 线程 **68.5%** CPU。覆写
`onEvent` 为空短路后，8B×200 万大事务回放 **45.6s → 6.5s（快 7 倍）**，全链上限回归
walsender 供给 × 行宽（与引擎形态同边界）。基线数字见
`docs/benchmarks-baseline.md` 的 connector 化段与 Arthas 段，本文是剖析全记录。

## 一、接线：metrics 怎么进入数据面热路径

三步织入，全部在 Debezium 框架代码：

1. **coordinator 注入**：`ChangeEventSourceCoordinator.java:360`
   `eventDispatcher.setEventListener(streamingMetrics)`——把 metrics factory 产出的实例
   （我们的 `StreamStreamingChangeEventSourceMetrics`）设为 dispatcher 的 eventListener。
2. **每事件回调**：`EventDispatcher.java:251`（匿名类 `EventDispatcher$2.changeRecord`）：
   `eventListener.onEvent(...)` 在 `receiver.changeRecord(...)`（真正交付进
   ChangeEventQueue）**之前**对每条数据事件调用。
3. **两支重活**：`DefaultStreamingChangeEventSourceMetrics.onEvent`（:134-139）无条件调
   `super.onEvent`（→ CommonEventMeter）与 `streamingMeter.onEvent`；唯一有开关的
   `activityMonitoringMeter`（`isAdvancedMetricsEnabled`，默认关）不是问题支路。

完整栈（consumer 线程，火焰图实测形态）：

```
TransactionConsumer.processBucket → BucketReplayer.replay → MessagePipe.readRange
→ DispatcherTransactionListener.onRowChange → PostgresEventDispatcher.dispatchDataChangeEvent
→ RelationalChangeRecordEmitter.emitCreateRecord → EventDispatcher$2.changeRecord
   ├→ eventListener.onEvent(...)     ← 指标计量（68.5% consumer CPU，本问题）
   └→ receiver.changeRecord(...)     ← 真正交付（queue enqueue 全链仅 0.4%，无辜）
```

## 二、度量原理与自洽性验证

async-profiler（Arthas `profiler`，event=cpu，10ms 间隔）对消耗 CPU 的线程拍全栈：
7758 样本 ≈ 77.6s 全进程 CPU。线程归属：consumer（transaction-consumer）4303 样本
（~43s CPU）≈ 回放 wall 45.6s 的 94%——单线程满载，统计行的回放耗时与火焰图线程占比
两个独立口径互相咬合，测量可信。换算：43.03s ÷ 200 万条 = **21.5µs/条**。

## 三、每条事件 21.5µs 的实测预算（按"栈中含帧"聚合）

| 链路段（可靠边界帧） | 样本 | 占 consumer | 每条事件 | 性质 |
|---|---|---|---|---|
| **指标段**（`DefaultStreamingChangeEventSourceMetrics.onEvent`） | 2946 | **68.5%** | **14.7µs** | 修复目标 |
| ├─ 计量唤醒（`MeasurementCollector`） | 2921 | 67.9% | ~14.6µs | **主犯** |
| └─ 摘要字符串化（`toSummaryString`） | 795 | 18.5% | ~4.0µs | 从犯 |
| &nbsp;&nbsp;&nbsp;&nbsp;└─ 其中 `ObjectMapper.<init>` | 263 | 6.1% | ~1.3µs | 见第五节 |
| emit 真正工作（emitCreateRecord − 指标段） | ~1045 | 24.3% | ~5.2µs | 必要（schema/Struct） |
| resolveAndInstall（asOf 表解析） | 155 | 3.6% | ~0.8µs | 必要 |
| decodeSingle（协议解码） | 58 | 1.3% | ~0.3µs | 必要（自研管线极薄） |
| readRange 本体（CQ 回读） | ~99 | ~2.3% | ~0.5µs | 必要 |
| engine 消费（handleBatch，另线程） | 58（全进程 0.7%） | — | — | 前两轮错怪的对象 |

## 四、两支热点的代码链

**热点一·计量唤醒（14.6µs/条）**：`DefaultStreamingChangeEventSourceMetrics.onEvent:136`
→ `StreamingMeter.onEvent`（`StreamingMeter.java:138`）——每条事件 `new
LagBehindSourceEvent(...)` 后 `measurementCollector.accept(...)`；`MeasurementCollector.accept`
（`MeasurementCollector.java:44-52`）→ `LinkedBlockingQueue.offer` → `signalNotEmpty` →
`ReentrantLock.unlock` → `LockSupport.unpark` → **`pthread_cond_signal`**（唤醒专职 drain 的
publisherThread）。每条事件一次 futex 唤醒 + 对端调度。该设计为低吞吐观测面（类注释自认
"prevents any contention during record processing"——用队列隔离阻塞，但**唤醒本身留在了
热路径**），无配置开关。

**热点二·摘要字符串化（4.0µs/条）**：`PipelineMetrics.onEvent:59` → `CommonEventMeter.onEvent`
（`CommonEventMeter.java:43-46`）——无条件 `lastEvent = metadataProvider.toSummaryString(...)`。
`toSummaryString` 是 `EventMetadataProvider` 的 **default 方法**（:45，我们未覆写）：
`new EventFormatter().sourcePosition(...).key(key).toString()` → `EventFormatter.printStruct` →
`SchemaUtil.asDetailedString(Struct)`（`SchemaUtil.java:110`）`new RecordWriter().detailed(true)
.append(struct).toString()`。拼好的字符串只存进等待被下次覆盖的 `lastEvent` 字段（JMX
观测面）。

## 五、`RecordWriter` 的死字段 `om`（"零使用仍有成本"的典型）

`SchemaUtil.RecordWriter`（:134-135）`private final ObjectMapper om = new ObjectMapper();`
——而 `append()` 的全部分支（Schema/Struct/ByteBuffer/Map/数组）**一律走 StringBuilder**，
`om` 零引用（上游遗留死字段）。字段初始化器编译进构造器**无条件执行**：每条事件
`new RecordWriter()` 即付一次 Jackson ObjectMapper 构造（含 `PrivateMaxEntriesCache` 等
内部结构），~1.3µs/条。

**权重修正的教训（叶子 vs 含帧）**：叶子视角（栈顶恰停在该帧）只数到 118 样本（1.5%），
含帧视角（栈中含该帧）263 样本（**3.4%**）——同一帧差 2.2 倍。构造函数返回极快、栈顶暴露
短，叶子视角系统性低估。正确口径是含帧聚合（火焰图里帧的总宽度）。即便按 3.4%，om 单独
仍非主犯（唤醒链是它的 11 倍）——它是"每事件为交付之外的目的做重量级准备"的最具辨识度
标志物，不是量刑对象。

## 六、测量坑：JIT 内联抹帧

`StreamingMeter.onEvent` 帧出现 2127 次，其内部调用的 `MeasurementCollector` 出现 2921 次
——**子帧多于父帧**：JIT 把 `onEvent` 内联进调用者后该帧从栈上消失，未内联的深边界
（`MeasurementCollector`）保留。用内联候选帧统计会低估；聚合须选稳定未内联的边界帧
（本文表即按此口径）。

## 七、修复与超线性收益

修复（commit a2fdcc3）：`StreamStreamingChangeEventSourceMetrics.onEvent` 覆写为空、不调
super——dispatcher 的 eventListener 持本实例，虚拟分派直达空实现，两支热点整链短路。
取舍：Debezium 内建 MBean 的 `totalNumberOfEventsSeen`/`lastEvent`/`milliSecondsBehindSource`
停更（事件观测走自研 `StreamThroughputMetrics` 的输出 rec/s，延迟走自研 lagBytes）。

效果超线性：短路 14.7µs/条 的线性预测是 ~13.6s，实测 **6.5s（3.25µs/条）**——指标段
每条还分配 LagBehindSourceEvent + ObjectMapper 及缓存 + 1KB 级摘要字符串，200 万条的
垃圾分配撑起了显著 GC 份额（全进程样本中的 G1 帧）；垃圾消失 → GC 连锁收益。

## 八、方法论总结

1. **排除法归因会错**：四组参数对照（A queue/batch/flush、B UNORDERED、C 渲染短路）全部
   无效后，"反压链墙钟"被错误记在 engine 消费端（实际 0.7%）——没有 CPU 采样证据的
   推断只配生成假设。
2. **火焰图读调用链宽度，不读叶子高度**：显眼的具名帧（`ObjectMapper.<init>`）易被高估，
   匿名 native 帧（`pthread_cond_signal@@GLIBC_2.3.2`，25%）藏着主犯——必须按帧归属向上
   聚合（signal → MeasurementCollector → StreamingMeter → onEvent）定位。
3. **聚合用未内联边界帧**，叶子/内联候选帧只作构成参考。
4. **两口径交叉验证**（统计行回放耗时 vs 火焰图线程 CPU）通过后再下结论。

## 复测材料

WSL：`~/perf/run/`（reader 启动脚本/外部配置/cdc-OFF logback/classpath）；Arthas 在
`~/arthas/`（`java -jar arthas-boot.jar -c "profiler start --format collapsed" <pid>`，
负载触发后 `profiler stop --file /tmp/flame.collapsed`）；聚合脚本形态见本文第三节
（python3 按"栈中含帧"求和）。本次采样产物 `/tmp/flame.collapsed`（7758 样本，已用于
本文全部数字）。
