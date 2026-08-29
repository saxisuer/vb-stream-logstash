# JMH 基准与性能基线（assembly-spill Task 13 建立，1.7 Task 9 换管道口径，1.7.1 Task 2 增归因段，2.0 Task 5 增输出契约换血对照段）

五个 JMH 基准（`src/jmh/java/org/vastdata/vbstream/bench/`，`-Pjmh` 档才参与编译）以**真实录制语料**
离线回放，度量 pgoutput 解码、路由窥探、组装总成本与管道路径（`MessagePipe` append / 冻结桶回放）
的成本面，为"原始字节驱动 + 延迟解码 + reader/consumer 解耦"路线提供量化对照。语料与基准均不
依赖运行期 Docker——录制一次，反复回放。

## 运行方法

```bash
# 1) 编译（jmh profile 引入 org.openjdk.jmh:jmh-core/jmh-generator-annprocess:1.37（test scope），
#    把 src/jmh/java 挂为 test 源码目录，并经 annotationProcessorPaths 生成基准桩）
mvn -Pjmh clean test-compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt

# 2) 运行（冒烟档：1 fork / 预热 1s / 测量 2s）
java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" \
  org.openjdk.jmh.Main "org.vastdata.vbstream.bench" -f 1 -w 1s -r 2s \
  -jvmArgsAppend="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -jvmArgsAppend="--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED" \
  -jvmArgsAppend="--add-opens=java.base/java.lang.reflect=ALL-UNNAMED" \
  -jvmArgsAppend="--add-opens=java.base/sun.nio.fs=ALL-UNNAMED"
```

注意：

- **两条命令都须在模块根目录（pom.xml 所在目录）运行**——classpath 与 `target/cp.txt` 均为相对
  路径，换目录执行会静默拼错 classpath（找不到主类或语料）。
- **`-jvmArgsAppend` 必带**（等号单 token 形式）。JMH 每个 fork 是全新 JVM，不继承启动器的
  `--add-opens`；Chronicle Queue 的 mmap 走反射调 `sun.nio.ch`，缺开包会在 fork 内直接失败
  （1.7 起 CQ 是主缓冲管道，**全部**基准类都经 `MessagePipe` 建 CQ，无一豁免）。
  该清单与 pom 的 surefire argLine 同源（pom 中 `jdk.unsupported/sun.misc` 项在 JDK 17
  默认已开放、为 no-op，此处略去）。
- 正式基线建议去掉冒烟档参数跑默认档（5×2s 预热 + 5×2s 测量，1 fork），命令行控制，类上不硬编码。
- `mvn clean test`（无 profile）不编译基准不依赖 JMH；`-Pjmh test` 下基准桩类名虽匹配
  `*Test` 模式，但无 JUnit 注解，Surefire/JUnit Platform 发现零测试，不会误跑。

## 语料

`src/test/resources/bench-corpus/corpus.bin`（提交进库，`.gitattributes` 标 binary）：由
`it.BenchCorpusRecordTest`（生成器测试）对 Testcontainers PG 执行 `src/main/resources/sql/`
全部 6 个场景脚本（psql 容器内执行）经 `SessionHarness.rawMessages()` 录制的真实 pgoutput
字节流。**重录触发**：语料缺失，或脚本/建表 DDL 内容变化（SHA-256 指纹边车
`scripts.sha256` 比对）；指纹一致时该测试只做健康断言（不启容器，秒过）。

| 项 | 值 |
|---|---|
| 消息条数 | 84 |
| 总字节 | 348,581（avg 4,149 B/条；min 1 B=StreamStop；max 16,524 B=块外 Update，流式块内最大 16,422 B=Insert） |
| 数据消息单元（I/U/D/M） | 44 条 / 347,319 B（avg 7,893 B；min 21 B=主键 DELETE）；其中**流式块内 20 条 / 328,440 B**（16.4KB 级大载荷）、块外 24 条 / 18,879 B |
| 类型分布 | B=11, C=11, I=30, U=9, D=5, R=4, S=6, E=6, c=1, A=1（10 种） |
| 场景覆盖 | 类型边界值 INSERT/UPDATE/DELETE、TOAST 'u' 标志、REPLICA IDENTITY FULL 全列 old tuple 与 Relation 重发、流式大事务提交（StreamStart/Stop/StreamCommit 分段）与流式回滚（StreamAbort） |

## 基准口径（2.0 形态；Decode/RoutePeek/两 append/归因口径自 1.7 沿用未变）

| 基准 | 计时体 | 模式/单位 |
|---|---|---|
| `DecodeBenchmark.decodeOne` | 顺序态 `PgOutputDecoder`（PARALLEL）按序推整语料，一条消息完整解码 | avgt，µs/条 |
| `RoutePeekBenchmark.peekRoute` | 同语料同游标，仅类型字节 + 流式块内 Int32 xid 前缀窥探（1.6 沿用口径，与 `TransactionAssembler.onRaw` 路由窥探同构） | avgt，ns/条 |
| `RoutePeekBenchmark.peekRouteWithOid` | 同上另窥数据消息 relation oid（I/U/D 单 Int32、T 数组、M 无——与 1.7 追加期 `collectOids` 同构，oidSet 供交接快照圈定） | avgt，ns/条 |
| `AssembleMemoryBenchmark.assembleWholeCorpus` | **同步形态** `TransactionAssembler`（不开 consumer 线程）逐条吃整份语料一轮（append + 路由/oid 窥探 + 桶段记账 + 交接快照 + 回放解码渲染；listener 为 2.0 流式契约 `onEvent` 的 no-op 形态、observer 亦 no-op；类名沿用 1.6 保持序列对照） | avgt，ms/轮 |
| `PipePathBenchmark.replayBucket` | 预构造 2000 单元冻结桶（语料回卷 100 轮、捕获流式块内单元，≈32.8MB；经 `BenchPipeBridge` 走组装器 reader 侧同构的 append 记账落盘），逐段 readRange + decodeSingle + 快照 asOf 渲染交**零分配 sink 计数**（`TransactionConsumer.processBucket` 的回放半程，2.0 流式交付形态——不攒整桶 List，返回交付条数） | avgt，ms/桶 |
| `PipePathBenchmark.appendOneMessage` | 向管道追加一条裸消息字节（21B 最小真实数据消息，无帧化——一条 CQ 记录即一条完整消息；计时体只剩 writeBytes + index 取回） | thrpt，ops/s |

## 结果表

### 1.7 本机基线（2026-08-29，冒烟档 `-f 1 -w 1s -r 2s`）

环境：macOS 15（Darwin 24.6.0）· MacBook Pro（Intel i9-8950HK 2.9GHz，12 线程，32GB）·
Azul Zulu JDK 17.0.11 · Maven 3.9.4 · chronicle-queue 2026.6 · JMH 1.37（Docker 本轮未用——语料回放）。

| 基准 | 模式 | 得分 ±99.9% CI | 换算 |
|---|---|---|---|
| DecodeBenchmark.decodeOne | avgt | **1.040 ± 0.048 µs/条** | ≈4.0 GB/s（avg 4,149 B/条） |
| RoutePeekBenchmark.peekRoute | avgt | **8.027 ± 0.279 ns/条** | peek/decode ≈ 0.77% |
| RoutePeekBenchmark.peekRouteWithOid | avgt | **8.712 ± 0.292 ns/条** | 较 peekRoute +0.69 ns（+8.5%）；/decode ≈ 0.84% |
| AssembleMemoryBenchmark.assembleWholeCorpus | avgt | **1.395 ± 0.153 ms/轮** | 84 条/轮 ≈ 16.6 µs/条 |
| PipePathBenchmark.replayBucket | avgt | **13.258 ± 2.904 ms/桶** | 2000 单元 ≈ 6.63 µs/单元 ≈ 2.5 GB/s（≈32.8MB） |
| PipePathBenchmark.appendOneMessage | thrpt | **4,307,529 ± 251,307 ops/s** | 21B/条 ≈ 90 MB/s |

**基线要点（对照结论）**：

- **peek/decode ≈ 0.77%**（8.03 ns vs 1,040 ns，约 **130 倍**差距）——组装器路由期只窥类型字节
  与 xid 前缀、把完整解码推迟到 consumer 回放，路由开销相对解码可忽略；1.7 在此之上新增的
  **oid 窥探仅 +0.69 ns**（快照圈定的记账成本），reader 记账路径仍然极轻。
- **同步组装一轮语料 1.395ms**（84 条含 44 数据消息、其中 36 条提交回放解码——44 − 被回滚
  流式事务的 8 个单元；组装器实跑复核 transactions=12 / replayed_units=36）≈ 16.6 µs/条。
  与 1.6 的 0.081ms/轮差约 17 倍，主因是 1.7 组装路径**每条消息都 append 进 CQ 管道**
  （一轮 ≈348KB 落盘，≈250 MB/s 混合帧吞吐）且提交点多了快照拷贝——这是"组装期堆内零字节
  引用"换来的磁盘写成本，由 consumer 线程的回放读回收；回放解码本身（对照 Decode 的 1.0 µs/条）
  在总成本中仍是零头量级。
- **冻结桶回放 13.26ms/2000 单元**（≈6.63 µs/单元，≈2.5 GB/s）——readRange 的 mmap 回读副本 +
  16KB 级大元组解码 + 快照 asOf 渲染的合计，即 transaction-consumer 线程的真实工作负载。
  与 1.6 SPILLED 回放对照：每单元成本高于 1.6（6.63 vs 4.59 µs——本桶为纯 16.4KB 流式单元，
  1.6 桶混有 21B 小单元摊薄），字节吞吐反而更高（2.5 vs 1.7 GB/s，大单元摊薄了逐条回读开销）；
  载荷构成不同，只宜作量级对照。
- **pipe.append 裸消息 4.31M ops/s**（21B）≈ 0.23 µs/条——reader 线程每条消息的落盘成本。
  对照 1.6 SPILLED append 的 3.81M ops/s（30B 帧化）：**reader 路径成本类同**（ops/s 高约
  13%、单条少 9B 帧头——1.7 起帧化退役，一条 CQ 记录即一条完整消息），机制本身（writeBytes +
  index 取回）未变；对照回放的 6.63 µs/单元，append 不构成解耦管道的瓶颈。

### 1.6 历史基线（2026-08-28，冒烟档；MEMORY/SPILLED 对照口径已随 1.7 退役，作跨版本参照保留）

环境同上（Docker Desktop Server 24.0.2，仅录制期用）。1.7 起仍可直接对照的行：decodeOne /
peekRoute / assembleWholeCorpus（口径含义见下表注）；replayBucket 的 MEMORY/SPILLED 双形态与
appendOneFrame 的帧化口径随 MEMORY 桶、`SpoolFrame` 退役——1.7 等价口径见上表（回放 →
`PipePathBenchmark.replayBucket`，append → `appendOneMessage`）。

| 基准 | (path) | 模式 | 得分 ±99.9% CI | 换算 |
|---|---|---|---|---|
| DecodeBenchmark.decodeOne | — | avgt | **1.004 ± 0.113 µs/条** | ≈4.0 GB/s（avg 4,149 B/条） |
| RoutePeekBenchmark.peekRoute | — | avgt | **7.771 ± 0.342 ns/条** | peek/decode ≈ 0.77% |
| AssembleMemoryBenchmark.assembleWholeCorpus | — | avgt | **0.081 ± 0.030 ms/轮** | 84 条/轮 ≈ 0.96 µs/条（1.6 纯内存记账，无 CQ append/快照——与 1.7 口径不可直接比，见上表要点） |
| SpillPathBenchmark.replayBucket | MEMORY | avgt | **3.683 ± 0.160 ms/桶** | 2000 单元 ≈ 1.84 µs/单元 ≈ 4.3 GB/s |
| SpillPathBenchmark.replayBucket | SPILLED | avgt | **9.185 ± 0.233 ms/桶** | 2000 单元 ≈ 4.59 µs/单元 ≈ 1.7 GB/s |
| SpillPathBenchmark.appendOneFrame | — | thrpt | **3,805,921 ± 320,394 ops/s** | 30B/帧 ≈ 114 MB/s |

1.6 要点摘录（结论仍有效的部分）：peek/decode ≈ 0.77%（路由窥探相对解码可忽略）；SPILLED
回放 ≈ MEMORY 的 2.49 倍（溢写回放纯代价 = readRange 回读 + unframe——1.7 回放口径不再有
MEMORY 对照，unframe 亦随帧退役）。

### 结果表模板（复测时照抄填写）

| 基准 | 模式 | 得分 ±99.9% CI | 换算 |
|---|---|---|---|
| DecodeBenchmark.decodeOne | avgt | µs/条 | GB/s |
| RoutePeekBenchmark.peekRoute | avgt | ns/条 | peek/decode = ___ % |
| RoutePeekBenchmark.peekRouteWithOid | avgt | ns/条 | 较 peekRoute + ___ ns |
| AssembleMemoryBenchmark.assembleWholeCorpus | avgt | ms/轮 | µs/条 |
| PipePathBenchmark.replayBucket | avgt | ms/桶 | µs/单元；GB/s |
| PipePathBenchmark.replayBucket:gc.alloc.rate.norm（`-prof gc` 档） | avgt | B/op | 2.0 前后对照入档（见 2.0 段） |
| PipePathBenchmark.appendOneMessage | thrpt | ops/s | MB/s（×21B/条） |

### 1.7.1 组装成本归因（2026-08-29，三腿互证）

背景：1.7 的 `assembleWholeCorpus` 1.395 ms/轮（≈16.6 µs/条）对 1.6 纯内存基线 0.081 ms/轮差
≈17×（绝对差距 1,314 µs/轮），而孤立热页口径 `appendOneMessage` 仅 ≈0.23 µs/条——append 机制
只能解释差距的 ~1.5%。本节把其余部分逐段归因（设计：`docs/superpowers/specs/2026-08-29-assembly-cost-attribution-design.md`）。

**三腿与档位**：

- **腿 1 栈采样**：`-prof stack:lines=8` + `-prof gc`，`-f 1 -w 3s -r 5s -i 10`（采样档；栈深渲染
  8 行，迭代数加倍以稀释偶发停顿）。本轮 1.460 ± 0.564 ms/轮，干净迭代 1.11–1.45 ms 与基线一致；
  迭代 1/7 分别被 1.80 s / 1.07 s 的 CQ `ChunkedMappedFile` grow-file 停顿撞中——分钟级滚动文件
  扩容瞬间的秒级尾延迟，属存储路径外沿的真实现象，不混入均值归因。
- **腿 2 组件口径**：六基准一次 JMH 调用同轮跑（冒烟档 `-f 1 -w 1s -r 2s`，与既有基线同档）。
- **腿 3 差分**：纯计算，见下文算式。

**腿 1 关键段**（RUNNABLE 线程态内占比；另有 46.1% 为 "everything is filtered" 内联盲区，见对账段）：
`AbstractBytes.writeInt`（writeFully 写入链）=17.5%、`MessagePipe.deletableFiles` 的
opendir/stat/readdir 合计=15.7%、msync（chronicle 后台释放线程）=6.0%、`RandomAccessFile.setLength`
（文件增长）=2.6%、`String.<init>`（回放解码）=1.6%。GC 侧写：`-prof gc` 实测 alloc 767 KB/轮
（≈9.1 KB/条）、gc.time 86 ms/约 50 s 测量窗 ≈0.2% 墙钟——**GC 非嫌疑**。

**腿 2 同轮数字**（2026-08-29，冒烟档 `-f 1 -w 1s -r 2s`，环境同 1.7 基线）：

| 基准 | 得分 ±99.9% CI | 换算 |
|---|---|---|
| PipePathBenchmark.appendOneLargeMessage | 26,684 ± 6,540 ops/s | 37.48 µs/条（16,524 B，每条跨 ≈4.03 个新 mmap 页，≈441 MB/s） |
| PipePathBenchmark.appendOneMessage | 3,794,167 ± 941,372 ops/s | 0.264 µs/条（21B 热页） |
| AssembleMemoryBenchmark.assembleWholeCorpus（同轮对照） | 1.515 ± 0.588 ms/轮 | 18.0 µs/条（与基线 16.6 CI 重叠，归因分母仍用基线 1,395 µs/轮） |
| AssemblyAttributionBenchmark.snapshotCopyPerHandoff | 110.4 ± 46.5 ns/次 | 排除法：百 ns 级，非嫌疑 |
| AssemblyAttributionBenchmark.handoffQueueOfferPoll | 79.5 ± 26.8 ns/对 | 排除法：百 ns 级，非嫌疑 |
| DecodeBenchmark.decodeOne | 1.223 ± 0.213 µs/条 | ≈3.4 GB/s（基线 1.040 同量级） |
| RoutePeekBenchmark.peekRoute / peekRouteWithOid | 9.06 ± 2.45 / 9.75 ± 0.84 ns/条 | 窥探+oid 记账④ 的证据 |

**腿 3 差分算式**（每缺页成本，brief/spec §3 公式）：

```
每缺页 = (1/appendOneLargeMessage − 16,524B 的 memcpy 单价 − 机制) ÷ (16,524 ÷ 4,096)
       = (37.48 µs − 6.61 µs − 0.264 µs) ÷ 4.034 = 7.59 µs/缺页      （CI 传导：5.76 ~ 10.60）
  memcpy 单价 = 2.5 GB/s（replayBucket 口径反推，含回读+解码+渲染的合并吞吐）
  机制成本   = appendOneMessage 热页口径 0.264 µs（21B 的 memcpy 可忽略）
一轮新触页 = 348,581 B ÷ 4,096 B = 85.1 → ≈86 页（顺序追加流，页连续触达）
缺页贡献   = 86 页 × 7.59 µs ≈ 652 µs/轮 ≈ 7.77 µs/条
```

**归因表**（分母 = 基线 1,395 µs/轮 = 16.6 µs/条；"修复前后对照"列由 Task 3 按启用准则补写）：

| 段 | µs/条 | 占 16.6 µs | 证据口径 | 误差域 |
|---|---|---|---|---|
| ① append 机制（writeBytes+index 取回，热页） | 0.264 | 1.6% | `appendOneMessage` 同轮 3.79M ops/s × 84 条 | 21B 热页口径；机制对载荷弱敏感，memcpy 单列② |
| ② append 载荷 memcpy | 1.66 | 10.0% | 348,581 B/轮 ÷ 2.5 GB/s | 2.5 GB/s 是回读+解码+渲染合并吞吐 → 单价偏保守（慢）→ ②偏高、③偏低，②+③合计不受影响 |
| ③ append 缺页（新 mmap 页触碰摊销） | 7.77 | 46.8% | 大/小载荷差分（腿 3 算式）× 86 页/轮 | **最宽行**：±24.5% CI 传导 → 全区间 35~65%；且缺页成本速率依赖（隔离口径 441 MB/s 写压 vs 组装流 250 MB/s）；腿 1 因 safepoint 偏置无法独立确证（见对账段）。按上界倾向读 |
| ④ 路由窥探（类型+xid+oid 记账） | 0.010 | 0.06% | `peekRouteWithOid` 同轮 9.75 ns × 84 | ns 级，可忽略 |
| ⑤ live 解码（40 条控制消息+'R'） | 0.004 | <0.1% | 控制消息共 1,262 B，按 decode 吞吐（3.4~4.0 GB/s）字节折算 | 字节折算**低估**小消息每条固定成本（派发/建对象不随字节缩放），本行是下界；即便按"40 条全按 4KB 均值消息解码"的荒谬上界也仅 ≈49 µs/轮（3.5%），实际量级 <1% 结论不变 |
| ⑥ 交接级（快照拷贝+队列一对） | 0.027 | 0.2% | `AssemblyAttributionBenchmark` 同轮 × 12 交接/轮（11 Commit + 1 StreamCommit；StreamAbort 整桶丢弃不走 handoff） | 排除法入档：百 ns 级 |
| ⑦ 回放半程（readRange+解码+asOf 渲染） | 2.84 | 17.1% | `replayBucket` 既有 6.63 µs/单元 × 36 提交单元（44 数据单元 − 被回滚流式事务 8 单元；组装器实跑复核 transactions=12 / replayed_units=36） | 上界口径（6.63 按纯 16.4KB 大单元标定，语料混 24 条小单元实际更低）；线上异步形态在 consumer 线程，不占 reader 关键路径 |
| ⑧ `deletableFiles` 目录扫描 | ≈1.79 | 6~16%（中值 ≈10.8%） | 腿 1 采样 opendir/stat/readdir = 15.7% RUNNABLE + 代码路径计数（13 完结点/轮 = 12 交接 + 1 整桶丢弃，均经 `maintainWatermarks`） | **新发现（原嫌疑清单外）**；未建隔离微基准——下界推导：13 次 ×（opendir + ≈3 readdir + ≈3 stat，各 ≈1 µs 级）≈ 80 µs/轮，上端取采样占比 ≈220 µs/轮；每次调用还新建 `DateTimeFormatter`（MessagePipe.java:233）未计入，真实成本偏区间上端（Task 3 隔离口径实测收窄为 ≈35.7%，见下节修复对照） |
| ⑨ GC | ≈0.03 | ≈0.2% | 腿 1 `-prof gc`：86 ms/50 s | alloc 767 KB/轮是背景压力，非时段成本 |
| **存储路径小计 ①+②+③** | **9.7** | **≈58%（区间 47~77%）** | 三腿合成 | Task 3 决策门与 1.7.2 换算的主输入 |
| 已归因合计 | ≈14.4 | ≈86.6% | 上行①–⑨加总 ≈1,208.4 µs/轮 | — |
| **未归因残差** | **≈2.2** | **≈13.4%** | 1,395 − 已归因 ≈ 186.6 µs/轮 | 内联热路径（路由状态机/桶段记账/TxChange 与 Transaction 分配/CQ appender 内部记账）+ 采样与模型误差；与腿 1 的 46.1% "filtered" 盲区对应 |

> **互证脚注**：把 ⑧ 修正为 35.7%（497.9 µs/轮 ≈ 5.93 µs/条）同时把 ③ 读到 CI 下端（≈5.9 µs/条）时，
> 已归因合计 ≈16.66 µs/条，与 16.6 µs/条 基线严丝合缝（残差归零）——恰好实证对账段
> "腿 2/3 高估 ③"的偏置方向：被 ⑧ 收窄挪走的质量正是 ③ 原本虚高的部分。

**三腿对账**：

- **腿 1 未能独立确证③的量值**：write 系可见仅 ≈20% RUNNABLE（writeInt 17.5% + setLength 2.6%）
  远低于模型 58%。两个机制性偏置方向相反：(a) **safepoint 偏置**——采样经
  `Thread.getStackTrace` 需目标线程回到安全点，长时间 `Unsafe.copyMemory`/页错误内核停留使
  write 帧欠采样（方向：腿 1 低估 write）；(b) 隔离大消息口径写流 441 MB/s 的脏页回写竞争高于
  组装流 250 MB/s，每缺页成本在组装上下文可能更低（方向：腿 2/3 高估③）。③按上界倾向读；
  取 CI 下端 5.76 µs/缺页（存储 ≈47%）仍是最大段，下述 1.7.2 结论不变。
- **组件模型内部互证**：按条数直算（20 大消息 × 37.48 + 64 小 × 0.264 ≈ 766 µs/轮）与
  ①+②+③ 合成（814 µs/轮）同量级——差 ≈6% 来自 24 条块外中等载荷（≈787 B/条）的字节与页
  在两模型中的归类差异。
- **⑧ 的机理**：`maintainWatermarks()` 在每个桶完结点调 `releaseBelow` → `deletableFiles` 做
  全目录 `Files.list` + 逐文件 `Files.isRegularFile`（stat）——语料每轮 13 个完结点
  （11 Commit + 1 StreamCommit + 1 StreamAbort），单 cycle 内删集恒空也照付扫描费（腿 1 采样中
  无任何 releaseBelow 触发的 unlink 帧，佐证只扫不删）。

**1.7.2 建议定稿**（Task 4；按 spec §1 换算逻辑——存储路径合计占比 X% → 混合存储收益上限即 X%。
依据即本节归因表 + 下节 Task 3 判定与修复对照，无新增论证）：

存储路径（append 机制 + memcpy + 缺页）合计 **≈58%（区间 47~77%）→ 混合存储收益上限即 ≈58%**
（16.6 µs/条的理论下限 ≈7 µs/条）。**结论：不值得开 1.7.2 混合存储**，理由：

1. 58% 中约 47 个百分点是缺页——"顺序写新页"的固有代价（Task 3 出路 C 判定：架构固有、不修）；
   混合存储只是把这部分换成堆内存增长（1.6 无界堆正是 1.7 换血动机）并把 O(事务大小) 的转储
   突发停顿带回 reader 提交点；
2. 真正的"纯 CQ 机制税"只有 ①+② ≈1.9 µs/条（≈12%），而"小事务跳过 CQ"形态只省小消息那部分
   append——实际可省远低于 58% 上限，代价是复活 nextSeq/信封帧/双形态回放整套装置；
3. 绝对量充足且修复后进一步收窄：⑧ 节流修复后组装 ≈9.1 µs/条（assembleWholeCorpus 0.761 ms/轮），
   对 1.6 纯内存基线 0.96 µs/条的差距从 ≈17× 收窄到 ≈9.5×，其中缺页 ≈46.8%（归因表口径）属固有
   ——混合存储若开，收益上限仍按存储占比口径表述（≈58%）；且 reader 记账路径（④+⑤+⑥ 合计
   ≈0.04 µs/条）未被侵蚀——解耦架构的核心红利与存储成本无关。
4. 针对性修复已按"单项 ≥15% 且不动架构"准则裁定完毕（Task 3，实施与对照见下节）：⑧
   `deletableFiles` 扫描档位节流 + 解析器记忆化已实施（assembleWholeCorpus −38.3%，CI 分离）；
   ③ 判定架构固有、不修；⑦ 在 consumer 线程、不占 reader 关键路径且不在修复菜单内；①+②+⑨
   低于阈值，④⑤⑥ 为零头，不动——修复菜单清空，1.7.2 没有未决的前置修复项。

### 1.7.1 Task 3 修复决策门与实施（2026-08-29，⑧ 隔离收窄 + 出路 B 修复 + A/B 对照）

Task 2 审查约束"⑧ 的 6~16% 区间不得直接翻修复决策，须先补隔离口径收窄"——本节即该口径与
后续门判定。**已入档归因表各行数字保持原样**，本节是收窄后的判定读数与修复对照。

**⑧ 隔离口径**：新增 `AssemblyAttributionBenchmark.deletableFilesScan`（`BenchPipeBridge.deletableFiles`
同包桥透出 `MessagePipe` 包私有纯函数）——Setup 把整份语料经真实管道落盘一轮（目录内容与组装器
实跑同形：`metadata.cq4t` + 当前 cycle 的 `.cq4`），neededCycle 取当前追加前沿所在档位 → 删集
恒空、只扫不删，与 `assembleWholeCorpus` 每轮 13 个桶完结点上 `releaseBelow` 的真实形态一致。

**收窄结果**（3 fork 档 `-f 3 -wi 5 -i 5 -w 2s -r 2s`；冒烟档 40.909 ± 17.255 µs/次同量级）：

| 口径 | 得分 ±99.9% CI |
|---|---|
| `deletableFilesScan` 单次扫描（修复前） | **38.303 ± 2.303 µs/次** |
| 栈剖析（`-prof stack`，RUNNABLE 内占比） | opendir0 = 22.6%、stat（isRegularFile+isDirectory 两处）= 14.6%、readdir = 8.2%、closedir = 1.2%——syscall 合计 ≈18 µs 是**下限**；`DateTimeFormatter.ofPattern` 新建 ≈1% |

原区间下界建立在"opendir/readdir/stat 各 ≈1 µs"的假设上，实测 macOS 单次 opendir 即 4~9 µs 级
——假设不成立，区间整体上移。

**门判定（准则：单项 ≥15% 且不动架构）**：

- **⑧ → 出路 B（达标且可修）**：13 完结点/轮 × 38.303 µs = 497.9 µs/轮，占基线 1,395 µs/轮
  **≈35.7%**（远超 15%；即便按 syscall 下限 18 µs/次折算 234 µs/轮 ≈16.8% 也过线）。
- **③ 缺页 46.8% → 出路 C（架构固有）**：顺序写新页的固有代价，修复菜单内的预触碰只是把
  缺页挪到 append 之前、不减总量，其余选项必动 CQ 或架构——判定"架构固有，不修"，作为 1.7.2
  初稿"不值得开混合存储"的加权论据（初稿结论维持）。
- ⑦ 17.1% 在 consumer 线程、不占 reader 关键路径（不在修复菜单）；①②⑨④⑤⑥ <15% 或零头，不动。

**修复内容（`MessagePipe`，零架构改动）**：

1. **`releaseBelow` 按档位节流**：needed cycle 与上次实际扫描相同即跳过目录扫描（同档位内可删集
   不可能变化——滚动文件只随 append 前沿出现在当前/未来档位，不回填旧档名）。删档检查延后到
   档位推进，删除惰性化语义不变（水位计算不变；删除失败的重试同样顺延到下次档位推进，残留只占
   磁盘）。节流字段只在 reader 线程读写（单写者），无并发问题。
2. **`DateTimeFormatter` 按周期格式进程级记忆化**（`FORMATTER_BY_PATTERN`）：原每次扫描
   `ofPattern` 新建解析器；解析器不可变线程安全，按模式串缓存后语义不变。

**修复前后对照**（同会话同档 3 fork `-f 3 -wi 5 -i 5 -w 2s -r 2s`；修复前经 `git stash` 单独
还原 `MessagePipe` 后重建复跑，其余源码相同）：

| 基准 | 修复前 | 修复后 | Δ |
|---|---|---|---|
| `AssembleMemoryBenchmark.assembleWholeCorpus` | 1.234 ± 0.151 ms/轮 | **0.761 ± 0.107 ms/轮** | **−0.473 ms（−38.3%，CI 分离）** |
| `AssemblyAttributionBenchmark.deletableFilesScan`（纯函数口径，节流不作用于它） | 38.303 ± 2.303 µs/次 | 35.950 ± 0.642 µs/次 | −2.35 µs（≈−6%，CI 轻微重叠——memo 只省 ofPattern） |

- A/B 差分与隔离口径**互证**：Δ473 µs/轮 ÷ 13 次/轮 ≈ 36 µs/次，与隔离口径 38.3 µs/次吻合
  ——腿 1 采样的"opendir/stat/readdir = 15.7% RUNNABLE"系**低估**（native 停留欠采样，与对账段
  方向 (a) 同类）；⑧ 修复前的真实占比即 ≈35.7%（对 Task 2 基线 1,395 µs；对本会话修复前
  1.234 ms 为 38~40%）。
- 修复后 `assembleWholeCorpus` ≈0.761 ms/轮 ≈ **9.1 µs/条**（84 条/轮）；⑧ 的稳态残余 ≈ 每档位
  推进一次扫描（MINUTELY 下 ≥60 s 一次，摊到每轮 ≈0），单次成本仍是 ≈36 µs（扫描本身未变）。
- 回归护栏：`MessagePipeTest` 既有 4 用例零改动全绿 + 新增节流用例
  `releaseBelowSkipsScanUntilNeededCycleAdvances`（同档跳过/档位推进补删/真实数据文件保留）；
  `mvn clean test` 144 用例全绿（143 + 新增 1）。

### 2.0 输出契约换血对照（2026-08-29，Task 5；存证 `gc-before.txt` / `gc-after.txt` / `assemble-after.txt`，SDD 目录）

**契约变更**（设计 `docs/superpowers/specs/2026-08-29-transaction-streaming-output-design.md`）：输出契约从
`onTransaction(Transaction)` 整块交付换为 `onEvent(TransactionEvent)` 流式交付
（`Begin → TxChange* → End`，单回调单背压点，End 返回 = 完整消费确认、前沿随之推进）；
`BucketReplayer.replay` 改 sink 签名（逐条交付不再攒 List，返回交付条数）；block 形态
（`vb.output.mode=block`）经 `BlockOutputAdapter` 在输出边界攒回整块（恢复 1.7 原子交付与
O(事务) 堆语义的逃生门）。基准面随之迁移：`BenchPipeBridge.replay()`（内部攒 List 的编译过渡
形态）→ **`replayCounting()`**（零分配 sink `c -> {}`，返回交付条数防死码）；
`AssembleMemoryBenchmark` 的 no-op listener 迁到 2.0 契约零操作形态（`event -> {}`）。
Decode / RoutePeek / 两 append / AssemblyAttribution 不触碰输出契约（路径零改动），未复测。

**replayBucket 前后对照**（gc 档 `-f 1 -w 3s -r 5s -prof gc`，5 个 `--add-opens` 等号单 token
形式自带；前 = 换血前 1.7 整块回放形态，后 = 2.0 sink 计数形态；同机同 JDK 同日）：

| 指标 | 换血前（1.7 整块回放） | 换血后（2.0 sink 计数） | Δ |
|---|---|---|---|
| avgt | 11.858 ± 0.559 ms/op | 12.690 ± 0.506 ms/op | 99.9% CI 重叠（[11.298, 12.417] vs [12.184, 13.196]），无统计显著变化 |
| **gc.alloc.rate.norm** | **99,889,660.223 ± 440.242 B/op**（≈95.27 MiB/op） | **99,856,145.687 ± 505.797 B/op**（≈95.23 MiB/op） | **−33,514.536 B/op（≈32.7 KiB/op，≈0.034%）** |
| gc.alloc.rate | 8033.671 ± 382.906 MB/sec | 7503.527 ± 292.533 MB/sec | — |
| gc.count / gc.time | 204 counts / 437 ms | 345 counts / 251 ms | 同量分配总量下 young GC 切分差异（分配微降使触发点分布变化），非语义信号 |

**口径注记（如实入档，勿过度解读）**：

- Δ≈32.7 KiB/op 恰为旧口径回放器内部 **2000 元素 ArrayList 攒集**的量级（backing array 增长
  拷贝与废弃数组合计）——这是**本基准面上消失的全部份额**，与"逐条 TxChange + 解码元组"的
  大头（≈49.9 KB/单元）相比可忽略，故降幅百分比极小属预期而非异常。
- `List.copyOf` 与 `Transaction` 封箱分配**从未在本基准面**（1.7 计时体只测回放半程，封箱在
  consumer 的封箱步）——线上 streaming 形态相对 1.7 实际免除的分配 = 回放期 List 攒集 +
  封箱 `List.copyOf` + `Transaction` 包装，其中基准可测的只有第一项；block 形态经
  `BlockOutputAdapter` 仍全额支付（攒集 + `List.copyOf` + 封箱，且攒集 List 有 ArrayList
  高水位保留——clear 不缩容）。
- **gc.alloc.rate.norm 度量分配速率，不是存活堆峰**：readRange 载荷副本（≈16.4 KB/单元）与
  解码元组/record/字符串在两形态都在——这是"逐条交付"本身的成本（TxChange 仍逐条构造，
  streaming 下 sink 收到即弃、成为即死对象，young GC 回收代价极低）。**"回放期堆峰
  O(单条)"是结构性结论，不依赖也不由该数字证明**：2.0 消费路径无任何跨单元累积容器
  （`processBucket` 逐条经 sink 交付、桶元数据只有 index 段与两个事件对象，TxChange 与
  载荷副本在下一单元回放时即不可达）；1.7 形态的回放期堆峰则为 O(事务)（`List<TxChange>`
  攒齐 + 封箱 `Transaction` 同刻存活，解码形态较原始字节膨胀 2~4×）。

**assembleWholeCorpus 复测**（冒烟档 `-f 1 -w 1s -r 2s`，与 1.7 基线行同档）：
**0.925 ± 0.331 ms/轮**（≈11.0 µs/条）——与 1.7 基线 1.395 ± 0.153、1.7.1 修复后
0.761 ± 0.107 均 CI 重叠、同量级：2.0 契约换血对组装主路径成本无统计可见影响（每桶新增的
Begin/End 事件对象为每轮 12 组 record，量级可忽略；no-op listener 交付零成本）。

## 已知口径限制

- 冒烟档（1 fork、5×2s 迭代）CI 较宽（`replayBucket` 本轮 ±22% 最宽），趋势结论（数量级/
  倍率）稳健，绝对值复测请用默认档。
- `replayBucket` 的桶为**纯流式块内单元**（语料每轮 20 条 16.4KB 级单元 ×100 轮 = 2000 单元
  ≈32.8MB；语料回卷 8400 条 append，控制消息/块外单元/块内 'R' 均照常落盘为段间垃圾字节，
  段结构自然分裂非人造单段）——载荷构成与 1.6 混合桶不同，跨版本只作量级对照；语料重录后
  单元配比漂移，实际单元数以基准 Setup 产物为准（`BenchPipeBridge.PipedBucket#unitCount`
  可自检）。aborted 过滤对基准桶恒空集（该基准不测子事务剔除，属正确性测试的范畴，见
  `DecoupledPipelineTest`）。
- `appendOneMessage` 用 21B 最小真实消息（压磁盘增速），测的是 append 机制吞吐；16KB 大消息的
  吞吐可用 `-p` 扩参或改用桶转储场景（`replayBucket` 的 Setup 即 8400 条混合消息顺写）另行测量。
- `AssembleMemoryBenchmark` 为同步形态（reader 记账 + 回放合一单线程计量）——线上异步形态把
  两半程拆到双线程，端到端总量不变，线程拆分本身的开销（交接队列/原子前沿）不在本基准面。
- 1.7.1 归因（上节）的固有局限：JMH stack profiler 对内联后纯 Java 热路径显示为 "everything
  is filtered" 盲区（46.1% RUNNABLE）、对长时间 native 停留（copyMemory/页错误）有 safepoint
  欠采样偏置——腿 1 只用于段发现与量级互证，量值以组件口径（腿 2）与差分（腿 3）为准；
  CQ 分钟级滚动文件 grow-file 的秒级停顿是真实尾延迟来源，但不在均值归因内。
