# JMH 基准与性能基线（assembly-spill Task 13 建立，1.7 Task 9 换管道口径，1.7.1 Task 2 增归因段）

四个 JMH 基准（`src/jmh/java/org/vastdata/vbstream/bench/`，`-Pjmh` 档才参与编译）以**真实录制语料**
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

## 基准口径（1.7 形态）

| 基准 | 计时体 | 模式/单位 |
|---|---|---|
| `DecodeBenchmark.decodeOne` | 顺序态 `PgOutputDecoder`（PARALLEL）按序推整语料，一条消息完整解码 | avgt，µs/条 |
| `RoutePeekBenchmark.peekRoute` | 同语料同游标，仅类型字节 + 流式块内 Int32 xid 前缀窥探（1.6 沿用口径，与 `TransactionAssembler.onRaw` 路由窥探同构） | avgt，ns/条 |
| `RoutePeekBenchmark.peekRouteWithOid` | 同上另窥数据消息 relation oid（I/U/D 单 Int32、T 数组、M 无——与 1.7 追加期 `collectOids` 同构，oidSet 供交接快照圈定） | avgt，ns/条 |
| `AssembleMemoryBenchmark.assembleWholeCorpus` | **同步形态** `TransactionAssembler`（不开 consumer 线程）逐条吃整份语料一轮（append + 路由/oid 窥探 + 桶段记账 + 交接快照 + 回放解码渲染；listener/observer no-op；类名沿用 1.6 保持序列对照） | avgt，ms/轮 |
| `PipePathBenchmark.replayBucket` | 预构造 2000 单元冻结桶（语料回卷 100 轮、捕获流式块内单元，≈32.8MB；经 `BenchPipeBridge` 走组装器 reader 侧同构的 append 记账落盘），逐段 readRange + decodeSingle + 快照 asOf 渲染（`TransactionConsumer.processBucket` 的回放半程） | avgt，ms/桶 |
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
| ⑧ `deletableFiles` 目录扫描 | ≈1.79 | 6~16%（中值 ≈10.8%） | 腿 1 采样 opendir/stat/readdir = 15.7% RUNNABLE + 代码路径计数（13 完结点/轮 = 12 交接 + 1 整桶丢弃，均经 `maintainWatermarks`） | **新发现（原嫌疑清单外）**；未建隔离微基准——下界推导：13 次 ×（opendir + ≈3 readdir + ≈3 stat，各 ≈1 µs 级）≈ 80 µs/轮，上端取采样占比 ≈220 µs/轮；每次调用还新建 `DateTimeFormatter`（MessagePipe.java:233）未计入，真实成本偏区间上端 |
| ⑨ GC | ≈0.03 | ≈0.2% | 腿 1 `-prof gc`：86 ms/50 s | alloc 767 KB/轮是背景压力，非时段成本 |
| **存储路径小计 ①+②+③** | **9.7** | **≈58%（区间 47~77%）** | 三腿合成 | Task 3 决策门与 1.7.2 换算的主输入 |
| 已归因合计 | ≈14.4 | ≈86.6% | 上行①–⑨加总 ≈1,208.4 µs/轮 | — |
| **未归因残差** | **≈2.2** | **≈13.4%** | 1,395 − 已归因 ≈ 186.6 µs/轮 | 内联热路径（路由状态机/桶段记账/TxChange 与 Transaction 分配/CQ appender 内部记账）+ 采样与模型误差；与腿 1 的 46.1% "filtered" 盲区对应 |

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

**1.7.2 建议初稿**（按 spec §1 换算逻辑——存储路径合计占比 X% → 混合存储收益上限即 X%）：

存储路径（append 机制 + memcpy + 缺页）合计 **≈58%（区间 47~77%）→ 混合存储收益上限即 ≈58%**
（16.6 µs/条的理论下限 ≈7 µs/条）。**判断：不值得开 1.7.2 混合存储**，理由：

1. 58% 中约 47 个百分点是缺页——"顺序写新页"的固有代价；混合存储只是把这部分换成堆内存增长
   （1.6 无界堆正是 1.7 换血动机）并把 O(事务大小) 的转储突发停顿带回 reader 提交点；
2. 真正的"纯 CQ 机制税"只有 ①+② ≈1.9 µs/条（≈12%），而"小事务跳过 CQ"形态只省小消息那部分
   append——实际可省远低于 58% 上限，代价是复活 nextSeq/信封帧/双形态回放整套装置；
3. 绝对量充足：16.6 µs/条 ≈ 250 MB/s 混合吞吐，且 reader 记账路径（④+⑤+⑥ 合计 ≈0.04 µs/条）
   未被侵蚀——解耦架构的核心红利与存储成本无关。
4. 便宜得多的针对性修复（Task 3 决策门输入，按"单项 ≥15% 且不动架构"准则裁定）：⑧
   `deletableFiles` 扫描降频（6~16%，零架构改动——例如仅 neededCycle 前移才扫描、或缓存上一次
   列举结果与 mtime）；③ 缺页段占比虽 >15%，但修复菜单中的 mmap 页预触碰依赖 CQ 侧便宜开关，
   可能落空（spec §4 已预告）；⑦ 回放半程报 17.1%（上界口径）但线上跑在 consumer 线程、不占
   reader 关键路径，且不在修复菜单内；①+②+⑨ 低于阈值，④⑤⑥ 为零头，不动。

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
