# JMH 基准与性能基线（assembly-spill Task 13）

四个 JMH 基准（`src/jmh/java/org/vastdata/vbstream/bench/`，`-Pjmh` 档才参与编译）以**真实录制语料**
离线回放，度量 pgoutput 解码、路由窥探、纯内存组装与溢写（spill）双形态回放的成本面，为
assembly-spill 设计的"原始字节驱动 + 延迟解码"路线提供量化对照。语料与基准均不依赖运行期
Docker——录制一次，反复回放。

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

- **`-jvmArgsAppend` 必带**（等号单 token 形式）。JMH 每个 fork 是全新 JVM，不继承启动器的
  `--add-opens`；Chronicle Queue 的 mmap 走反射调 `sun.nio.ch`，缺开包会在 fork 内直接失败。
  该清单与 pom 的 surefire argLine 同源（`jdk.internal.misc` 项在 JDK 17 为 no-op，略去）。
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
| 数据消息单元（I/U/D/M） | 44 条 / 347,319 B（avg 7,893 B；min 21 B=主键 DELETE） |
| 类型分布 | B=11, C=11, I=30, U=9, D=5, R=4, S=6, E=6, c=1, A=1（10 种） |
| 场景覆盖 | 类型边界值 INSERT/UPDATE/DELETE、TOAST 'u' 标志、REPLICA IDENTITY FULL 全列 old tuple 与 Relation 重发、流式大事务提交（StreamStart/Stop/StreamCommit 分段）与流式回滚（StreamAbort） |

## 基准口径

| 基准 | 计时体 | 模式/单位 |
|---|---|---|
| `DecodeBenchmark.decodeOne` | 顺序态 `PgOutputDecoder`（PARALLEL）按序推整语料，一条消息完整解码 | avgt，µs/条 |
| `RoutePeekBenchmark.peekRoute` | 同语料同游标，仅类型字节 + 流式块内 Int32 xid 前缀窥探（与 `TransactionAssembler.onRaw` 路由窥探同构） | avgt，ns/条 |
| `AssembleMemoryBenchmark.assembleWholeCorpus` | threshold=∞（spill 启用永不可越）的 `TransactionAssembler` 逐条吃整份语料一轮（listener/observer no-op） | avgt，ms/轮 |
| `SpillPathBenchmark.replayBucket` | 预构造 2000 单元桶（语料数据单元循环填充，≈15.8MB），MEMORY（堆内引用）vs SPILLED（溢写池 readRange 回读+unframe）两 `@Param` 回放，输入同批单元 | avgt，ms/桶 |
| `SpillPathBenchmark.appendOneFrame` | 向溢写池追加一帧预帧化字节（21B 最小真实数据消息 + 9B 帧头 = 30B；帧化在 Setup 完成，计时体只剩 writeBytes + index 取回） | thrpt，ops/s |

## 结果表

### 本机基线（2026-08-28，冒烟档 `-f 1 -w 1s -r 2s`）

环境：macOS 15（Darwin 24.6.0）· MacBook Pro（Intel i9-8950HK 2.9GHz，12 线程，32GB）·
Azul Zulu JDK 17.0.11 · Maven 3.9.4 · chronicle-queue 2026.6 · JMH 1.37 · Docker Desktop（Server 24.0.2，
仅录制期用）。

| 基准 | (path) | 模式 | 得分 ±99.9% CI | 换算 |
|---|---|---|---|---|
| DecodeBenchmark.decodeOne | — | avgt | **1.004 ± 0.113 µs/条** | ≈4.0 GB/s（avg 4,149 B/条） |
| RoutePeekBenchmark.peekRoute | — | avgt | **7.771 ± 0.342 ns/条** | ≈128 GB/s |
| AssembleMemoryBenchmark.assembleWholeCorpus | — | avgt | **0.081 ± 0.030 ms/轮** | 84 条/轮 ≈ 0.96 µs/条 |
| SpillPathBenchmark.replayBucket | MEMORY | avgt | **3.683 ± 0.160 ms/桶** | 2000 单元 ≈ 1.84 µs/单元 ≈ 4.3 GB/s |
| SpillPathBenchmark.replayBucket | SPILLED | avgt | **9.185 ± 0.233 ms/桶** | 2000 单元 ≈ 4.59 µs/单元 ≈ 1.7 GB/s |
| SpillPathBenchmark.appendOneFrame | — | thrpt | **3,805,921 ± 320,394 ops/s** | 30B/帧 ≈ 114 MB/s |

**基线要点（对照结论）**：

- **peek/decode ≈ 0.77%**（7.77 ns vs 1,004 ns，约 **129 倍**差距）——组装器路由期只窥类型字节
  与 xid 前缀、把完整解码推迟到提交期回放，路由开销相对解码可忽略；大事务流式入桶的
  "原始字节驱动"路线在成本上成立。
- **SPILLED 回放 ≈ MEMORY 的 2.49 倍**（9.19ms vs 3.68ms，同批 2000 单元/≈15.8MB）——
  差价全部来自 readRange 的 mmap 回读副本 + unframe 帧复原（1.7 vs 4.3 GB/s）；即溢写
  换取的堆内存节省，在提交期回放约多付 1.5 倍时间，且仍在 GB/s 量级。
- **纯内存组装一轮语料 0.081ms**（84 条含 44 数据消息、其中约 32 条提交回放解码）≈
  0.96 µs/条，与 decodeOne 的 1.0 µs/条同量级——组装总成本由回放解码主导，桶记账与
  路由窥探是零头的零头（与 peek/decode 比值互相印证）。
- **spool.append 单帧 3.8M ops/s**（30B 帧）——溢写转储不是流式入桶路径的瓶颈
  （对照 SPILLED 回放 4.59 µs/单元，append 仅 ≈0.26 µs/帧，且实测帧越大摊销越薄）。

### 结果表模板（复测时照抄填写）

| 基准 | (path) | 模式 | 得分 ±99.9% CI | 换算 |
|---|---|---|---|---|
| DecodeBenchmark.decodeOne | — | avgt | µs/条 | GB/s |
| RoutePeekBenchmark.peekRoute | — | avgt | ns/条 | peek/decode = ___ % |
| AssembleMemoryBenchmark.assembleWholeCorpus | — | avgt | ms/轮 | µs/条 |
| SpillPathBenchmark.replayBucket | MEMORY | avgt | ms/桶 | µs/单元 |
| SpillPathBenchmark.replayBucket | SPILLED | avgt | ms/桶 | SPILLED/MEMORY = ___ × |
| SpillPathBenchmark.appendOneFrame | — | thrpt | ops/s | MB/s（×30B/帧） |

## 已知口径限制

- 冒烟档（1 fork、5×2s 迭代）CI 较宽，趋势结论（数量级/倍率）稳健，绝对值复测请用默认档。
- `replayBucket` 的 MEMORY 形态单元字节数组跨调用复用（已在页缓存），SPILLED 回读为
  mmap 副本——两者差值即溢写回放代价，不含磁盘冷读（首次热身后均走页缓存）。
- `appendOneFrame` 用 21B 最小真实消息帧（压磁盘增速），测的是 append 机制吞吐；
  16KB 大帧的吞吐可用 `-p` 扩参或改用桶转储场景（`replayBucket` 的 Setup 即 2000 帧
  顺写）另行测量。
- 语料含 2 个 aborted 流式事务的消息（StreamAbort 路径），回放侧 aborted 过滤对
  `replayBucket` 恒空集（该基准不测子事务剔除，属正确性测试的范畴，见 AssemblySpillTest）。
