# jmh/ 基准源码根——四基准离线回放

独立源码根的动机：JMH 依赖（jmh-core + annprocess）与基准编译只在 `-Pjmh` profile 参与，默认构建（含 `mvn test`）零 JMH 依赖；源码根经 build-helper 挂为 test 源码目录（能复用 src/test 基建），挂载与注解处理细节见 pom.xml 的 `jmh` profile 注释。

## 语料依赖链

```
it.BenchCorpusRecordTest（真库录制，src/test）
  → src/test/resources/bench-corpus/corpus.bin（提交进库；6 场景脚本叠加，84 条真实消息）
  → bench.BenchCorpus.load()（统一取用点，缺失抛带修复指引的 ISE）
  → bench.CorpusLoader（[I32 len][bytes] 长度前缀流读写器，big-endian）
```

录制内容覆盖：类型边界值 INSERT/UPDATE/DELETE、TOAST 未变标志、REPLICA IDENTITY FULL 的全列 old tuple 与 Relation 重发、流式大事务分段提交、StreamAbort。语料收尾于块外 Commit、桶全闭合，回卷重放才合法（Decode/AssembleMemory 的循环游标依赖此前提）——重录后新语料须保持该形态。改动 `src/main/resources/sql/` 场景脚本或建表 DDL 会使 SHA-256 指纹失配、`BenchCorpusRecordTest` 自动重录（需 Docker，产物提交回库）。

## 四基准各测哪条路径

| 基准 | 测什么 | 口径 |
|---|---|---|
| `DecodeBenchmark` | 一条消息的**完整解码**（tuple 逐列 + 剩余字节校验），顺序态 decoder（PARALLEL，inStream 随真实流序演进） | µs/条（可换算 MB/s） |
| `RoutePeekBenchmark` | 组装器**路由窥探**（只读类型字节 + 流式块内 Int32 xid 前缀，不解码——与 onRaw 路由同构） | ns/条——与 Decode 相除得窥探占解码的比值（余下 ~99% 即推迟解码省下的部分） |
| `AssembleMemoryBenchmark` | **整语料一轮完整组装**（threshold=∞ 含水位记账与越限检查；路由窥探 + 桶记账 + 提交回放解码 + asOf 渲染，listener/observer 均 no-op） | ms/轮 |
| `SpillPathBenchmark` | `@Param` MEMORY/SPILLED **同批 2000 单元回放对照**（差值 = 溢写回放纯代价，经 BenchSpillBridge 走组装器同构双分支）；另附 `spool.append` 单帧吞吐 | ms/次（回放对照）；append 为 thrpt ops/s |

口径间关系：Decode 是 RoutePeek 的对照上限；AssembleMemory 与 SpillPathBenchmark 的 SPILLED 回放对照即溢写代价；AssembleMemory 与 Decode 对照可见组装开销中解码的占比。

## 运行与基线

运行命令（`--add-opens` 须经 `-jvmArgsAppend` 自带）、冒烟档参数与**基线数字**见 `docs/benchmarks-baseline.md`——改基准或改组装器/解码器热点路径后须重跑对照并把数字更新入档。
