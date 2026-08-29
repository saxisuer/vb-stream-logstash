# jmh/ 基准源码根——五基准离线回放

独立源码根的动机：JMH 依赖（jmh-core + annprocess）与基准编译只在 `-Pjmh` profile 参与，默认构建（含 `mvn test`）零 JMH 依赖；源码根经 build-helper 挂为 test 源码目录（能复用 src/test 基建），挂载与注解处理细节见 pom.xml 的 `jmh` profile 注释。

## 语料依赖链

```
it.BenchCorpusRecordTest（真库录制，src/test）
  → src/test/resources/bench-corpus/corpus.bin（提交进库；6 场景脚本叠加，84 条真实消息）
  → bench.BenchCorpus.load()（统一取用点，缺失抛带修复指引的 ISE）
  → bench.CorpusLoader（[I32 len][bytes] 长度前缀流读写器，big-endian）
```

录制内容覆盖：类型边界值 INSERT/UPDATE/DELETE、TOAST 未变标志、REPLICA IDENTITY FULL 的全列 old tuple 与 Relation 重发、流式大事务分段提交、StreamAbort。语料收尾于块外 Commit、桶全闭合，回卷重放才合法（Decode/AssembleMemory 的循环游标依赖此前提）——重录后新语料须保持该形态。改动 `src/main/resources/sql/` 场景脚本或建表 DDL 会使 SHA-256 指纹失配、`BenchCorpusRecordTest` 自动重录（需 Docker，产物提交回库）。

## 五基准各测哪条路径（1.7 形态）

| 基准 | 测什么 | 口径 |
|---|---|---|
| `DecodeBenchmark` | 一条消息的**完整解码**（tuple 逐列 + 剩余字节校验），顺序态 decoder（PARALLEL，inStream 随真实流序演进） | µs/条（可换算 GB/s） |
| `RoutePeekBenchmark` | 组装器**路由窥探**两口径：`peekRoute` 只读类型字节 + 流式块内 Int32 xid 前缀（1.6 沿用，与 onRaw 路由同构）；`peekRouteWithOid` 另窥数据消息 relation oid（与 1.7 追加期 `collectOids` 同构——oidSet 供交接快照圈定） | ns/条——与 Decode 相除得窥探占解码的比值 |
| `AssembleMemoryBenchmark` | **整语料一轮完整组装**（**同步形态**组装器：pipe.append + 路由/oid 窥探 + 桶段记账 + 交接快照 + 回放解码渲染，listener/observer 均 no-op；类名沿用 1.6 保持基线序列） | ms/轮 |
| `PipePathBenchmark` | **管道三口径**：`replayBucket` 预构造 2000 单元冻结桶（语料回卷 100 轮、捕获流式块内单元 ≈32.8MB，经 `BenchPipeBridge` 走组装器 reader 侧同构 append 记账落盘）逐段 readRange + decodeSingle + 快照 asOf 渲染——即 `TransactionConsumer.processBucket` 的回放半程；`appendOneMessage` 裸消息追加吞吐（无帧化，一条 CQ 记录即一条完整消息，21B 热页）；`appendOneLargeMessage` 追加 ≈16KB 大消息（1.7.1 新增冷页口径——每条跨入约 4 个新 mmap 页，与 21B 口径差分换算每缺页成本） | ms/桶；两 append 均为 thrpt ops/s |
| `AssemblyAttributionBenchmark` | **每次交接级开销**两口径：`snapshotCopyPerHandoff` 交接快照拷贝（registry.snapshot）、`handoffQueueOfferPoll` 交接队列 add+poll 一对——归因排除法数据源 | ns/次、ns/对 |

口径间关系：Decode 是 RoutePeek 两口径的对照上限；AssembleMemory 与 PipePathBenchmark.replayBucket 对照可见回放半程在组装总成本中的占比；AssembleMemory 与 Decode 对照可见组装开销中解码的占比。1.7.1 归因把 assembleWholeCorpus 总成本沿"窥探 / append（21B 热页与 16KB 冷页两口径）/ 每次交接级开销"逐项分解——RoutePeek + PipePath 两 append 口径 + AssemblyAttribution 合成该分解视图。

## BenchPipeBridge（src/test，跨包桥）

`replication` 包的管道机制（`MessagePipe`/`TxBuffer`/`BucketReplayer`）是包私有，`PipePathBenchmark` 经 `src/test/java/.../replication/BenchPipeBridge`（同包桥接类，角色同被 1.7 Task 5 删除的 1.6 `BenchSpillBridge`）取用：`dump(rawMsgs, registry, captureInStream, dir, rollCycle)` 以组装器 reader 侧同构的记账循环（append 取 index 作 seq、'R' 记版本日志、数据单元窥 oid 记段）把语料转储成单个冻结桶，返回句柄的 `replay()` 与 `TransactionConsumer.processBucket` 的回放半程同构、`append(byte[])` 供吞吐口径、`unitCount()/indexSpan()` 供口径自检。仅测试代码可用，不属于主代码契约。

## 运行与基线

运行命令（`--add-opens` 须经 `-jvmArgsAppend` 自带——1.7 起全部基准经 `MessagePipe` 建 CQ，无一豁免）、冒烟档参数与**基线数字**（1.7 段 + 1.6 历史参照）见 `docs/benchmarks-baseline.md`——改基准或改组装器/解码器/管道热点路径后须重跑对照并把数字更新入档。
