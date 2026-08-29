# 里程碑 1.7.1 组装成本归因与微优化——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 1.7 组装路径 16.6 µs/条 对 1.6 0.96 µs/条 的 17× 差距逐段归因入档（append/窥探/段记账/快照/队列/回放各占多少），按"单项 ≥15% 且不动架构"准则实施条件修复，产出 1.7.2（是否混合存储）的数据决策依据。

**Architecture:** 三腿归因——JMH `-prof stack` 采样（粗占比）、组件微基准补口（冷页 append 头号嫌疑/快照/队列）、大/小载荷 append 差分换算每缺页成本并与采样对账。依据 spec：`docs/superpowers/specs/2026-08-29-assembly-cost-attribution-design.md`（下称"spec"）。

**Tech Stack:** JMH 1.37（`-Pjmh` 档）、chronicle-queue 2026.6、既有语料基建（`BenchCorpus.load()` / `BenchPipeBridge.dump(rawMsgs, registry, captureInStream, dir, rollCycle)` / `CorpusLoader.deleteRecursively`）。

## Global Constraints

- Java 17；不新增任何依赖；**不动架构**（seq ≡ CQ index、纯段记账、控制消息 append 全保留；控制消息免 append 明确排除）
- 修复准则逐字：**单项占比 ≥15% 且修复不动架构才修**；修复实施后必须复测全口径入档（修复前后对照）
- JMH 源只在 `-Pjmh` 档编译；基准代码每函数中文 javadoc；`mvn clean test`（默认档）143 用例全程零回归——本计划预期生产代码最多动 `MessagePipe.append` 一处（且仅在 Task 3 的门判定通过时）
- 不预设收窄幅度；归因可能证明大头为架构固有（如 mmap 缺页）→ 交付的是决策依据，属设计内结局
- 跑基准的命令与 `--add-opens` 清单照 `docs/benchmarks-baseline.md`（经 `-jvmArgsAppend` 自带）；冒烟档 `-f 1 -w 1s -r 2s`
- **每个任务完成即 `git commit + git push`**；提交信息中文 conventional 风格
- 数字不许编造：跑不动的口径标"待跑"并给命令；归因表每个数字须能指向一次实际运行

---

### Task 1: 归因基准组（新基准 + append 冷页口径）

**Files:**
- Create: `src/jmh/java/org/vastdata/vbstream/bench/AssemblyAttributionBenchmark.java`
- Modify: `src/jmh/java/org/vastdata/vbstream/bench/PipePathBenchmark.java`（AppendState 增最大载荷 + 新基准方法）
- Modify: `src/jmh/CLAUDE.md`（基准表补两口径一行）

**Interfaces:**
- Consumes: `BenchCorpus.load()`（`List<byte[]>`）、`VersionedRelationRegistry.accept(long, Relation)`/`snapshot(Set<Integer>, long)`、`PgOutputMessage.Relation` record（组件以 `src/main/java/org/vastdata/vbstream/protocol/PgOutputMessage.java` 实际定义为准）
- Produces（Task 2 消费）: 基准方法名与口径——`AssemblyAttributionBenchmark.snapshotCopyPerHandoff`（ns/次）、`AssemblyAttributionBenchmark.handoffQueueOfferPoll`（ns/对）、`PipePathBenchmark.appendOneMessage`（既有，21B 热页）、`PipePathBenchmark.appendOneLargeMessage`（新增，≈16KB 冷页跨步）

- [ ] **Step 1: 写 AssemblyAttributionBenchmark**

```java
package org.vastdata.vbstream.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 组装开销归因基准（1.7.1 设计 §3 腿 2）：把组装路径上"窥探与 append 之外"的**每次交接级**
 * 开销逐项隔离计量——每交接 RelationSnapshot 拷贝、交接队列 add+poll 一对。
 * 与 RoutePeekBenchmark（窥探 ns 级已入档，≈9 ns/条）、PipePathBenchmark（append 两口径）
 * 拼出 assembleWholeCorpus 总成本的分解视图，是 1.7.1 归因表的数据源。
 * 口径注意：两项预期都在百 ns 级——若实测如此，即从嫌疑清单排除（归因表的"排除法"同样入档）。
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AssemblyAttributionBenchmark {

    /**
     * 快照 state：预灌 2 oid × 各 2 版本的版本日志（对齐语料每轮约 4 条 'R' 的量级，含一次
     * 同 oid 重发模拟 DDL）——snapshot 的成本形状由（oid 数 × 版本数）决定，与语料真实
     * 'R' 内容无关，手工构造即可复现成本形态。
     */
    @State(Scope.Thread)
    public static class SnapshotState {

        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        Set<Integer> oidSet = Set.of(16384, 16390);
        long maxSeq = 40L;

        /** 责任：灌 4 个版本（2 oid × 2 版本，seq 递增）。边界：Relation record 组件形态以 protocol 包实际定义为准。 */
        @Setup(Level.Trial)
        public void setup() {
            // 组件对齐 PgOutputMessage.Relation 实际 record 定义（Task 3 单测先例：
            // (OptionalLong streamXid, int relationOid, String schema, String table, char replicaIdentity, List columns)）
            registry.accept(10L, relation(16384, "t1_v1"));
            registry.accept(20L, relation(16390, "t2_v1"));
            registry.accept(30L, relation(16384, "t1_v2"));
            registry.accept(40L, relation(16390, "t2_v2"));
        }

        private static PgOutputMessage.Relation relation(int oid, String table) {
            return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table, 'd', java.util.List.of());
        }
    }

    /**
     * 计时体：一次交接快照拷贝（reader 提交路径的 snapshot(oidSet, lastIndex)）。
     * 返回快照对象防死码消除。
     */
    @Benchmark
    public Object snapshotCopyPerHandoff(SnapshotState s) {
        return s.registry.snapshot(s.oidSet, s.maxSeq);
    }

    /**
     * 队列 state：与组装器交接队列同型（LinkedBlockingQueue）+ 一个冻结负载引用。
     * 负载用 Object 而非 TxBuffer（包私有不可及）：队列机制成本与负载类型无关。
     */
    @State(Scope.Thread)
    public static class QueueState {
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        Object payload = new Object();
    }

    /**
     * 计时体：交接一对（reader 侧 add + consumer 侧 poll）。
     * 返回 polled 对象防死码消除。
     */
    @Benchmark
    public Object handoffQueueOfferPoll(QueueState s) {
        s.queue.add(s.payload);
        return s.queue.poll();
    }
}
```

（`Relation` 构造组件若与上面不符，以 `PgOutputMessage.java` 实际定义对齐——保持 2 oid × 2 版本的成本形状即可。）

- [ ] **Step 2: PipePathBenchmark 补大载荷 append 口径**

AppendState（`src/jmh/java/org/vastdata/vbstream/bench/PipePathBenchmark.java:102-135`）加一个字段与 Setup 赋值：

```java
/** 语料中最大的流式块内数据消息字节（≈16KB——每次 append 跨入约 4 个新 mmap 页，冷缺页口径的载荷源）。 */
private byte[] largest;
```

Setup 末尾（`smallest = smallestDataMessage(...)` 之后）：

```java
largest = largestDataMessage(BenchCorpus.load());
```

类尾部加静态辅助与基准方法（`smallestDataMessage` 旁边）：

```java
/**
 * 责任：取语料中最大的数据消息字节（冷页口径载荷源——大消息 append 每条跨入多个新 mmap 页，
 * 稳态下每条≈每 4KB 一次软缺页，与热页小消息口径差分即得每缺页成本；不判流式性——口径
 * 只需要大载荷，流式与否不影响缺页成本形状）。
 * 边界：无数据消息抛 IllegalStateException（录制侧健康断言保证含 I/U/D）。
 */
private static byte[] largestDataMessage(List<byte[]> corpus) {
    byte[] largest = null;
    for (byte[] raw : corpus) {
        char type = (char) raw[0];
        if ((type == 'I' || type == 'U' || type == 'D' || type == 'T' || type == 'M')
                && (largest == null || raw.length > largest.length)) {
            largest = raw;
        }
    }
    if (largest == null) {
        throw new IllegalStateException("语料中无数据消息（I/U/D/T/M），冷页口径不可构造");
    }
    return largest;
}
```

```java
/**
 * 计时体（Throughput）：向管道追加一条 ≈16KB 大消息——每次调用把 append 前沿推进约 4 个
 * 新 mmap 页，稳态下即"每 4KB 一次软缺页 + memcpy"的真实成本（1.7.1 归因的头号嫌疑口径）。
 * 与 appendOneMessage（21B 热页）差分换算每缺页成本，乘 assembleWholeCorpus 每轮新触页数
 * （348KB/轮 ≈ 87 页）即得缺页对 17× 差距的预期贡献，与 -prof stack 采样对账。
 * 返回 index 防死码消除。
 */
@Benchmark
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public long appendOneLargeMessage(AppendState state) {
    return state.pipe.append(state.largest);
}
```

（`largestDataMessage` 的 streamed 粗判若与 corpus 实际形态不符——例如最大数据消息在块外——直接去掉 streamed 条件取"最大数据消息"即可，口径注记改为"大载荷（≈N KB，实测为准）"；关键是大载荷，不必强求流式。）

- [ ] **Step 3: 编译与冒烟自检**

```bash
mvn -Pjmh clean test-compile
mvn clean test    # 默认档零回归（jmh 不参与）
# 冒烟（命令模板照 baseline 文档；cd 模块根）
mvn -Pjmh clean test-compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" org.openjdk.jmh.Main \
  "org.vastdata.vbstream.bench.AssemblyAttributionBenchmark" "org.vastdata.vbstream.bench.PipePathBenchmark.appendOneLargeMessage" \
  -f 1 -w 1s -r 2s \
  -jvmArgsAppend "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens java.base/sun.nio.ch=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens jdk.unsupported/sun.misc=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens java.base/sun.nio.fs=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens java.base/java.lang.reflect=ALL-UNNAMED"
```
Expected: 四个口径（snapshot/queue/large/既有 append）全部出数且量级 sane（snapshot/queue 预期百 ns 级、large append 预期显著低于 21B 口径的 ops/s）。

- [ ] **Step 4: jmh/CLAUDE.md 基准表补行**

`src/jmh/CLAUDE.md` 四基准表改为五基准：`AssemblyAttributionBenchmark` 一行（"每次交接级开销两口径：snapshot 拷贝 / 交接队列 add+poll——归因排除法数据源"）+ PipePathBenchmark 行补 `appendOneLargeMessage` 口径描述（冷页跨步）。

- [ ] **Step 5: 提交推送**

```bash
git add src/jmh docs
git commit -m "bench(jmh): 1.7.1 归因基准组——交接级开销隔离口径 + append 冷页大载荷口径"
git push
```

---

### Task 2: 归因跑批与入档

**Files:**
- Modify: `docs/benchmarks-baseline.md`（1.7.1 归因表段 + 1.7.2 建议初稿）

**Interfaces:**
- Consumes: Task 1 的四口径 + 既有口径（`assembleWholeCorpus`/`decodeOne`/`peekRoute`/`peekRouteWithOid`/`appendOneMessage`/`replayBucket`）
- Produces: 归因表（Task 3 决策门的输入）——每行：段名 / µs 每条或每次 / 占 16.6 µs 的百分比 / 证据口径 / 误差域说明

- [ ] **Step 1: 跑 `-prof stack` 采样**（腿 1）

```bash
java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" org.openjdk.jmh.Main \
  "org.vastdata.vbstream.bench.AssembleMemoryBenchmark.assembleWholeCorpus" \
  -f 1 -w 3s -r 5s -prof stack \
  -jvmArgsAppend ...（同 Task 1 Step 3 的五项 --add-opens）
```
记录：栈采样占比表（live 解码 / append(writeBytes) / 窥探记账 / 快照 / 队列 / readRange+回放解码 / GC / 其他）。

- [ ] **Step 2: 跑组件口径**（腿 2，Task 1 已冒烟，此处按同档正式记录）——`AssemblyAttributionBenchmark` 两口径 + `PipePathBenchmark.appendOneLargeMessage` + 既有 `appendOneMessage`（同轮复测，避免跨 run 漂移）。

- [ ] **Step 3: 差分对账**（腿 3）——纯计算，写入报告草稿：
  - 每缺页成本 ≈ (1/appendOneLargeMessage.ops − 16KB 的 memcpy 估计 − wrap 分配) / (16KB/4KB)；memcpy 估计用 348KB/轮 replay 口径的 2.5 GB/s 反推
  - 缺页对每轮贡献 ≈ 每缺页成本 × 87 页/轮 → 折 µs/条（÷84）
  - 与 stack 采样中 writeBytes/native 占比对账；与 `appendOneMessage`（热页 0.23 µs）对照标注"组装在 Context 中的真实 append 成本 = 热页 + 缺页摊销"

- [ ] **Step 4: 写入 baseline 1.7.1 段**：归因表（段/量/占比/证据/误差域）+ 未归因残差行（三腿对不拢的部分如实留残差，不硬凑 100%）+ **1.7.2 建议初稿**（"存储路径（append+缺页）合计占比 X% → 混合存储收益上限即 X%，值得/不值得"——按 spec §1 的换算逻辑写，数字以实测为准）。

- [ ] **Step 5: 提交推送**

```bash
git add docs/benchmarks-baseline.md
git commit -m "docs(bench): 1.7.1 归因表入档——17× 差距分解与 1.7.2 建议初稿（三腿互证）"
git push
```

---

### Task 3: 修复决策门与条件实施

**Files:**
- Modify（仅当门判定通过且修复落点在此）: `src/main/java/org/vastdata/vbstream/replication/MessagePipe.java`
- Modify: `docs/benchmarks-baseline.md`（修复前后对照或"无修复判定"记录）
- Test（回归护栏）: `src/test/java/org/vastdata/vbstream/replication/MessagePipeTest.java`（既有 4 用例零改动须全绿）

**Interfaces:**
- Consumes: Task 2 归因表；修复准则（单项 ≥15% 且不动架构）
- Produces: 判定记录 +（条件）修复实现 +（条件）修复后全口径复测数字

- [ ] **Step 1: 决策门**——读 Task 2 归因表，按准则走三出路之一（把判定与依据写进 baseline 1.7.1 段）：
  - **出路 A（无达标项）**：最大可修项 <15% 或无可修项 → 记录"无修复判定"，跳到 Step 4
  - **出路 B（达标且可修）**：存在单项 ≥15% 且不动架构 → Step 2/3 实施修复
  - **出路 C（达标但架构固有）**：单项 ≥15% 但修复必然动架构（如缺页为 CQ-per-message 固有，预触碰只是挪时间点不减总量）→ 记录"架构固有判定"并把它作为 1.7.2 建议的加权论据，跳到 Step 4
- [ ] **Step 2（仅出路 B）: 实施修复**——TDD：先确认/补 `MessagePipeTest` 对修复面的既有断言（append 返回 index 单调、readRange 内容保真——已覆盖），再实施；修复方案在实施时从归因数据反推确定（例：若 `BytesStore.wrap` 分配经 stack 采样证明可见，改 `MessagePipe.append` 持可复用写入视图——注意 wrap 分配若为年轻代 TLAB 分配则成本 ~ns 级，大概率属出路 A/C，勿预设）
- [ ] **Step 3（仅出路 B）: 复测**——`assembleWholeCorpus` + 受影响组件口径按同档复跑，修复前后对照入档；`mvn clean test` 143 全绿
- [ ] **Step 4: 提交推送**

```bash
git add -A
git commit -m "perf(pipe): 1.7.1 条件修复——<判定结果：修复内容或无修复/架构固有记录>"
git push
```

---

### Task 4: 收尾——1.7.2 建议定稿与文档同步

**Files:**
- Modify: `docs/benchmarks-baseline.md`（1.7.1 段终稿化）
- Modify: `CLAUDE.md`（根：里程碑状态 1.7 → 1.7.1 一句话——归因结论 + 1.7.2 建议）
- Modify: `src/main/java/org/vastdata/vbstream/replication/CLAUDE.md`（仅当 Task 3 动了 MessagePipe：线程约束段落的成本注记）

**Interfaces:**
- Consumes: Task 3 的终态（修复或判定记录）

- [ ] **Step 1: 1.7.2 建议定稿**——依据归因表 + Task 3 判定，把初稿升级为定稿建议（值得做 → 收益上限 X%、堆语义决策点引用 1.7 spec §1.1 的分析；不值得 → 论据），一段话，写进 baseline 1.7.1 段末
- [ ] **Step 2: 根 CLAUDE.md 里程碑段补 1.7.1 一行**（状态：已完成归因/结论一句话/测试数不变 143）
- [ ] **Step 3: 全量回归 + 提交推送**

```bash
mvn clean test
git add -A && git commit -m "docs: 1.7.1 收尾——归因终稿/1.7.2 建议/里程碑状态同步" && git push
```

---

## 任务依赖

Task 1 → Task 2 → Task 3 → Task 4（严格串行：归因数据是决策门输入）。

## 与 spec 的两处计划期细化（已在 spec 同步修订）

1. 腿 3 从"@Setup(Level.INVOCATION) 冷管道对照"改为"大/小载荷 append 差分 + 算术对账"——原方案每调用建池（~ms 级）会污染 1.4ms 的被测体，差分法同一意图且不受污染
2. `AssemblyAttributionBenchmark` 的 snapshot state 用手工灌版本（2 oid × 2 版本）而非语料回放——快照成本形状由 oid×版本数决定，语料 'R' 的真实内容不影响成本量级
