# 吞吐与分布指标（ThroughputMetrics）设计

日期：2026-08-31 · 状态：已批准（用户确认方案 A′ 后直接执行）

## 1. 背景与目标

冒烟运行期间无法观察各环节吞吐——读取是否跟上、consumer 是否积压、事务长尾多大，只能靠事后推算。本设计为 2.0 双线程管线补一套**纯本地周期日志行**指标：

- SLOT 读取速率：`(KB/MB/GB)/s` + `records/s`
- 事务组装速率：`tx/s`
- 流式输出速率：`records/s` + `tx/s` + `(KB/MB/GB)/s`
- 事务回放耗时分布：P90 / P95 / max
- 事务大小分布：P90 / P95 / max

### 选型结论（方案 A′）

消费通道确认为**仅本地周期日志行**（无 Prometheus/JMX/Logstash 指标对接计划），故：

- **计数与速率手写**：`LongAdder` + 报告周期差分——两个成熟框架（Micrometer/Dropwizard Metrics）在这一形态下真正提供的东西恰是无用的（Micrometer 不在应用内算速率、Dropwizard `Meter` 是 EWMA 平滑而非窗口平均），而需要自定义的（字节单位换算、与现有中文统计行风格一致的输出）框架都不提供。
- **分位数交给 HdrHistogram**（`org.hdrhistogram:HdrHistogram:2.2.2`，零传递依赖 ~130KB）：窗口分位数需要"有界内存下按精度记录任意动态范围"的数据结构，手写只能得到粗精度对数桶；HDR 是该问题的标准积木（Micrometer `DistributionSummary` 百分位内部同源）。
- 保险：全部计量收在 `ThroughputMetrics` 一个类后面，未来若需接监控系统，埋点调用点不动、只换该类实现。

## 2. 指标清单与口径（8 个量）

| # | 指标 | 口径 | 记录点（线程） |
|---|---|---|---|
| 1 | slot 读取 bytes | `raw.length` 累计（pgoutput 协议载荷，含类型字节与流式 xid 前缀） | `TransactionAssembler.onRaw` 入口（reader） |
| 2 | slot 读取 records | 全部 pgoutput 消息计数，**含控制消息与 Relation**——"从槽读到什么"的诚实口径；与输出侧 records **不可直接对照**（口径差：控制消息 vs 仅 TxChange），bytes 才是两端口径一致的对照对 | 同上 |
| 3 | 组装 tx | `handoff` 计数（提交事务：Commit / StreamCommit / CommitPrepared；回滚与 StreamAbort 整桶丢弃不计） | `handoff`（reader） |
| 4 | 输出 bytes | 回放器逐单元 `payload.length` 累计（= 从管道重读的原始字节，用户确认的口径） | `BucketReplayer` 回放循环（consumer） |
| 5 | 输出 records | TxChange 交付计数（= emitted，aborted 子事务过滤后实付数；Begin/End 事件不计） | `TransactionConsumer.processBucket`（consumer） |
| 6 | 输出 tx | End 事件发出 = 一个事务完整交付（`processBucket` 尾部） | 同上 |
| 7 | 回放耗时分布 | `processBucket` 起止 nanoTime，**含下游回调耗时**（渲染/适配器攒块）——"输出一个事务要多久"的完整语义；仅完成事务入分布（fail-fast 截断的不入） | 同上 |
| 8 | 事务大小分布 | 桶记账 `unitCount`（aborted 过滤**前**）——代表事务真实大小；与输出 records 的差 = 被剔除的子事务量 | 同上 |

## 3. 组件与接线——Main 零改动

新类 **`ThroughputMetrics`**（replication 包，包私有）：

- 6 个 `LongAdder`（#1–#6）+ 2 个 `SingleWriterRecorder`（HDR，2 位有效数字 ≈1% 精度）
- 热路径单行方法：`onSlotMessage(raw)` / `onTxHandedOff()` / `onReplayedUnit(len)` / `onTxOutput(durationNanos, unitCount, emittedRecords)`
- `reportLines(nowNanos)`：与上次快照差分算 6 速率、取两个区间直方图的 P90/P95/max，格式化两行字符串并推进快照基线
- 单位格式化纯静态函数（可单测）

接线全部在 replication 包内部：`TransactionAssembler` 构造时创建实例 → 自用（#1–#3）→ 传 `TransactionConsumer` 构造（#5–#8）→ 传 `BucketReplayer` 构造（#4）。**`Main`、公共 API、配置面零改动**——指标常开（LongAdder ~ns 级、HDR recordValue ~20ns 级），与现有"10s 统计周期常量不做配置面"哲学一致。

报告挂 `TransactionConsumer.maybeStats` 现有 10s tick（consumer 线程，poll(1s) 采样驱动），无新线程无调度器；同步测试形态 `run()` 不被调用 → 只计数不打印（供断言）。

## 4. 报告语义与输出格式

- **速率** = 报告周期内计数 delta ÷ 实际流逝秒数（nanoTime 差，非固定 10÷）
- **分位数** = 最近一个报告窗口（`SingleWriterRecorder.getIntervalHistogram()` 区间隔离，窗口外样本不稀释当前值；返回的直方图被 Recorder 回收复用，读后即弃不持有）
- 字节单位 **SI 十进制**（1000 进位，PG 工具链惯例）：值 ≥1000 进一位、保留 1 位小数；耗时单位 ns → µs → ms → s 同规则（值 ≥100 用整数）；records/tx 整数千分位（`Locale.ROOT`）
- 每次 tick 在现有桶状态行后追加两行 INFO：

```
consumer 统计: LIVE=0 HANDED_OFF=0 OUTPUTTING=1 frontierLsn=0x16B5A48   ← 现有行不动
吞吐: slot=12.4 MB/s (85,231 msg/s) | 组装=42 tx/s | 输出=11.8 MB/s (81,004 rec/s, 41 tx/s)
分布: 回放耗时 p90=3.2ms p95=6.8ms max=125ms | 事务大小 p90=2,150 rec p95=5,100 rec max=48,200 rec
```

- 空窗口：速率打 0.0 照常；分布零样本打 `n/a`（空区间无分位可言）

## 5. 线程模型与边界

- 计数器：`LongAdder`（slot 侧 reader 单写、输出侧 consumer 单写、tick 在 consumer 读——读弱一致即可，统计非精确记账）
- 直方图：`SingleWriterRecorder`（record 与 tick 同为 consumer 线程，单写者假设成立；#1–#3 的 reader 侧只有 LongAdder，无跨线程直方图）
- **上界钳制**：duration 钳制 ≤1h（ns）、txSize 钳制 ≤1e9——防 `recordValue` 越界抛异常进热路径（正常负载远达不到，钳到上界本身已是"病态慢/大"的信号）
- 指标永不向热路径抛异常、无可关闭资源、停机无需收尾（无终局报告——YAGNI）
- 失败事务（End 前 fail-fast）不入分布：duration/size 只统计完整交付的事务，与"输出 tx"计数口径一致

## 6. 内存与生命周期

随进程常驻且**有界**：6 个 LongAdder（各 ~24B）+ 2 个 Recorder（2 位精度、钳制范围下桶数对数级，各数 KB）+ 快照基线若干 long。无跨窗口累积。

## 7. 依赖变更

pom 新增 `org.hdrhistogram:HdrHistogram:2.2.2`（compile，零传递依赖）。

## 8. 测试策略

- `ThroughputMetricsTest`（单测）：
  - 格式化边界：bytes（999→KB 进位）、nanos（µs/ms/s、≥100 取整）、千分位
  - 速率差分：注入基准戳与 now，断言 delta÷elapsed 与字符串
  - 分位数窗口隔离：首窗样本不影响次窗（区间隔离）；零样本 `n/a`；越界值钳制不抛
- 组装器接线测试：同步形态喂 MsgBuilder 造的完整事务字节流，经包私有访问器断言 6 计数与分布样本数全对上（防 4 处单行插桩漏挂）
- 既有 158 用例回归全绿（`mvn clean test`）

## 9. 决策记录

| 决策 | 备选 | 取舍理由 |
|---|---|---|
| 手写计数+差分速率 | Micrometer/Dropwizard | 消费通道仅日志行；框架的核心价值（registry 门面/EWMA）用不上，格式化与单位换算反正自己写 |
| HdrHistogram 只作数据结构 | 手写对数桶（零依赖退路） | 精度（1% vs ±25%）与维护性差距过大；零传递依赖不违项目从简文化 |
| 指标常开无配置面 | `vb.metrics.*` 开关 | 写入成本 ns 级；沿用既有统计常量哲学 |
| 事务大小取 unitCount（过滤前） | emitted（过滤后） | 分布的目的是容量规划——事务"本相"多大；与输出的差值本身携带信息 |
| Main 零改动、metrics 包内私有 | 公共 API/构造注入 | 全部插桩点在 replication 包内，外部无消费方 |
