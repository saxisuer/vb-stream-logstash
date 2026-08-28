# JMH 基准与性能基线（assembly-spill Task 13 建立，1.7 Task 9 换管道口径）

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
| 总字节 | 348,581（avg 4,149 B/条；min 1 B=StreamStop；max 16,524 B=流式 Insert） |
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
- **同步组装一轮语料 1.395ms**（84 条含 44 数据消息、其中约 32 条提交回放解码）≈ 16.6 µs/条。
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
