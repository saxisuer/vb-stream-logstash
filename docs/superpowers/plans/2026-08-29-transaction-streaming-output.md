# 里程碑 2.0 事务流式输出——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把输出契约从 `onTransaction(Transaction)` 整块改为 `onEvent(TransactionEvent)` 流式三段交付（头/逐变更/尾），回放期堆峰值从 O(事务) 降到 O(单条)，输出格式与 1.7 逐字节一致。

**Architecture:** sealed 事件族纯增量先行（Task 1）→ listener/consumer/replayer/ConsoleListener/测试夹具原子换血（Task 2，断言零改动）→ 流式时序与事件流全等验收（Task 3）→ IT 迁移（Task 4）→ 基准与 `-prof gc` 堆峰对照 + 文档（Task 5）。依据 spec：`docs/superpowers/specs/2026-08-29-transaction-streaming-output-design.md`（下称"spec"）。

**Tech Stack:** Java 17（sealed interface 两层嵌套合法）、JUnit 6、JMH（`-Pjmh` 档）、Testcontainers。

## Global Constraints

- Java 17（无 record pattern switch，instanceof 链）；不新增依赖
- **输出格式与 1.7 逐字节一致**：`TXN-BEGIN xid=%d kind=%s gid=%s commitLsn=0x%s commitTs=%s changes=%d` / `  [%d] %s` / `TXN-END   xid=%d` 三行形态不变（changes=N 的 N 来自 `Begin.expectedChanges`）
- **既有断言零改动**：全部 `List<Transaction>` 断言经 `TransactionCollector` 重组存活——这是 1.6→2.0 四轮验收的等价币
- 每函数中文 javadoc（职责/关键步骤/边界/线程约束）；日志 slf4j `{}` 占位符
- 验证编译必须 `mvn clean test-compile`；`mvn clean test` 全程绿（Docker 在位含 IT）
- reader 侧唯一允许的改动 = `TxBuffer.unitCount`（一个 long 自增）；MessagePipe/状态机/低水位/节流/交接协议零触碰
- 每任务完成即 `git commit + git push`，中文 conventional 提交信息
- 数字不许编造（基准/堆峰对照指向实际运行，存证入报告）

---

### Task 1: TransactionEvent 事件族（纯增量）+ 换血前 gc 基线存档

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/TransactionEvent.java`
- Modify: `src/main/java/org/vastdata/vbstream/replication/TxChange.java`（`extends TransactionEvent`）
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionEventTest.java`（新建）

**Interfaces:**
- Produces（Task 2 消费）:
  - `public sealed interface TransactionEvent permits TransactionEvent.Begin, TransactionEvent.End, TxChange`
  - `record Begin(long xid, TransactionKind kind, String gid, long commitLsn, long endLsn, Instant commitTimestamp, long expectedChanges) implements TransactionEvent`
  - `record End(long xid, long emittedChanges) implements TransactionEvent`
  - `TxChange` 声明改为 `public sealed interface TxChange extends TransactionEvent permits RowChange, TruncateChange, MsgChange`（其余零改动——加 extends 不破坏任何现有 instanceof/sealed 使用）

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TransactionEvent 事件族形状单测（2.0 spec §2）：Begin/End record 值语义、TxChange 的
 * IS-A 关系（sealed permits 成员可用多态接收）、gid 非 2PC 为 null 的约定与 Transaction 一致。
 */
class TransactionEventTest {

    @Test
    void beginEndAreValueRecords() {
        TransactionEvent.Begin b = new TransactionEvent.Begin(101L, TransactionKind.NORMAL,
                null, 1L, 2L, Instant.EPOCH, 3L);
        assertEquals(101L, b.xid());
        assertEquals(3L, b.expectedChanges());
        assertEquals(new TransactionEvent.End(101L, 3L), new TransactionEvent.End(101L, 3L));
    }

    /** TxChange 是事件族成员：三种变更实现都能以 TransactionEvent 多态接收（permits 编译期保证，此处运行期再钉一次）。 */
    @Test
    void txChangesAreTransactionEvents() {
        TransactionEvent e1 = new RowChange(DmlKind.INSERT,
                new PgOutputMessage.Relation(OptionalLong.empty(), 1, "public", "t", 'd', List.of()),
                Optional.empty(), Optional.empty(), OptionalLong.empty());
        TransactionEvent e2 = new TruncateChange(List.of(), java.util.Set.of(), OptionalLong.empty());
        TransactionEvent e3 = new MsgChange(true, "p", new byte[0], OptionalLong.empty());
        assertTrue(e1 instanceof TxChange && e2 instanceof TxChange && e3 instanceof TxChange);
    }
}
```

（`RowChange`/`TruncateChange`/`MsgChange`/`PgOutputMessage.Relation` 的构造组件以各 record 实际定义为准对齐——断言语义不变。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -Dtest=TransactionEventTest`
Expected: 编译失败（TransactionEvent 不存在）。

- [ ] **Step 3: 实现**

`TransactionEvent.java`（javadoc 按 spec §2 全文写，含"单回调=单一背压点/End 返回=下游确认完整消费"契约注记）；`TxChange.java` 声明行加 `extends TransactionEvent`（javadoc 补一句事件族成员身份）。

- [ ] **Step 4: 跑测试与全量确认通过**

Run: `mvn test -Dtest=TransactionEventTest && mvn clean test`
Expected: PASS；全量 144 零回归（纯增量）。

- [ ] **Step 5: 换血前 gc 基线存档（Task 5 对照用）**

```bash
mvn -Pjmh clean test-compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" org.openjdk.jmh.Main \
  "org.vastdata.vbstream.bench.PipePathBenchmark.replayBucket" -f 1 -w 3s -r 5s -prof gc \
  -jvmArgsAppend "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens java.base/sun.nio.ch=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens jdk.unsupported/sun.misc=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens java.base/sun.nio.fs=ALL-UNNAMED" \
  -jvmArgsAppend "--add-opens java.base/java.lang.reflect=ALL-UNNAMED" \
  | tee /tmp/m20-gc-before.txt
```
记录 `gc.alloc.rate.norm`（≈2000 单元 × 解码对象的分配量）——Task 5 换血后同口径对照，预期显著下降（TxChange 累积列表消失；注意 replayBucket 口径在换血前本就把 List 当返回值，下降幅度主要来自列表与 List.copyOf——如实入档，不夸大为"堆峰"本身）。

- [ ] **Step 6: 提交推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication/TransactionEvent.java src/main/java/org/vastdata/vbstream/replication/TxChange.java src/test/java/org/vastdata/vbstream/replication/TransactionEventTest.java
git commit -m "feat(replication): TransactionEvent 事件族——sealed 头/尾/变更三形态（2.0 纯增量）"
git push
```

---

### Task 2: 契约换血（listener/consumer/replayer/ConsoleListener/收集器/测试夹具，断言零改动）

> 原子换血任务：中途不可编译属预期，以全量绿收口。Main 按 `vb.output.mode` 接线（见 Step 3 末尾）。

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionListener.java`（onEvent 重定义，流式主契约）
- Create: `src/main/java/org/vastdata/vbstream/replication/BlockTransactionListener.java`（1.7 契约保留改名：`onTransaction(Transaction)`）
- Create: `src/main/java/org/vastdata/vbstream/replication/BlockOutputAdapter.java`（流式→整块边界适配器）
- Create: `src/main/java/org/vastdata/vbstream/replication/OutputMode.java`（枚举 + `vb.output.mode` 解析）
- Create: `src/main/java/org/vastdata/vbstream/replication/TransactionCollector.java`
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionConsumer.java`（processBucket 流式化）
- Modify: `src/main/java/org/vastdata/vbstream/replication/BucketReplayer.java`（replay 改 sink 签名）
- Modify: `src/main/java/org/vastdata/vbstream/replication/TxBuffer.java`（+unitCount）与 `TransactionAssembler.java`（appendUnit 自增）
- Modify: `src/main/java/org/vastdata/vbstream/replication/Transaction.java`（javadoc 换角色"block 交付单元 + 重组值对象"）
- Modify: `src/main/java/org/vastdata/vbstream/ConsoleListener.java`（**双实现**：onEvent 流式渲染 + onTransaction 1.7 渲染保留，共享 renderChange）
- Modify: `src/main/java/org/vastdata/vbstream/Main.java`（按 `vb.output.mode` 接线：STREAMING→console 直传；BLOCK→`new BlockOutputAdapter(console)`）
- Modify: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`（run 夹具换收集器）、`BucketReplayerTest.java`（sink 驱动）、`TransactionModelTest.java`（如有 listener 引用）、`ConsoleListenerTest.java`（onEvent 断言 + onTransaction 保留断言）、`BenchPipeBridge.java`（replay 内部收集适配）
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionCollectorTest.java`、`BlockOutputAdapterTest.java`、`OutputModeTest.java`（新建）

**Interfaces:**
- Consumes: Task 1 事件族
- Produces（Task 3/4/5 消费）:
  - `TransactionListener { void onEvent(TransactionEvent event); }`（流式主契约）
  - `BlockTransactionListener { void onTransaction(Transaction transaction); }`（1.7 契约保留改名，block 模式消费 API）
  - `BlockOutputAdapter implements TransactionListener`（构造收 `BlockTransactionListener`；Begin 攒、End 封箱转发后丢弃）
  - `OutputMode { STREAMING, BLOCK }` + `static OutputMode fromSystemProperties()`（默认 STREAMING；未知值 IAE 附可用值）
  - `TransactionCollector implements TransactionListener`：`void onEvent(...)` + `List<Transaction> transactions()`
  - `BucketReplayer.replay(TxBuffer, MessagePipe, java.util.function.Consumer<TxChange> sink) → long`（交付数）
  - `TxBuffer.unitCount`（long，reader 追加期自增，交接后只读）

- [ ] **Step 1: 写 TransactionCollectorTest（TDD）**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TransactionCollector 单测（2.0 spec §2）：正常重组、流合法性 fail-fast、aborted 过滤下
 * emitted < expected 的合法性（spec 自审修定的关键边界）。
 */
class TransactionCollectorTest {

    private static TransactionEvent.Begin begin(long xid, long expected) {
        return new TransactionEvent.Begin(xid, TransactionKind.NORMAL, null, 1L, 2L, Instant.EPOCH, expected);
    }

    /** 正常流：Begin(3) + 2 变更 + End(2)（aborted 过滤形态，emitted<expected 合法）→ 重组出 1 个 2 变更事务。 */
    @Test
    void reassemblesTransactionAndAllowsEmittedBelowExpected() {
        TransactionCollector c = new TransactionCollector();
        RowChange r1 = new RowChange(DmlKind.INSERT, /* Relation 与 TupleData 夹具按 record 实际组件构造 */,
                java.util.Optional.empty(), java.util.Optional.empty(), java.util.OptionalLong.empty());
        RowChange r2 = new RowChange(DmlKind.DELETE, /* ... */, java.util.Optional.empty(),
                java.util.Optional.empty(), java.util.OptionalLong.empty());
        c.onEvent(begin(1L, 3L));
        c.onEvent(r1);
        c.onEvent(r2);
        c.onEvent(new TransactionEvent.End(1L, 2L));
        assertEquals(1, c.transactions().size());
        assertEquals(2, c.transactions().get(0).changes().size());
    }

    @Test
    void rejectsEndWithoutBegin() {
        TransactionCollector c = new TransactionCollector();
        assertThrows(IllegalStateException.class, () -> c.onEvent(new TransactionEvent.End(1L, 0L)));
    }

    @Test
    void rejectsNestedBegin() {
        TransactionCollector c = new TransactionCollector();
        c.onEvent(begin(1L, 0L));
        assertThrows(IllegalStateException.class, () -> c.onEvent(begin(2L, 0L)));
    }

    @Test
    void rejectsEmittedAboveExpectedAndCountMismatch() {
        TransactionCollector c = new TransactionCollector();
        c.onEvent(begin(1L, 1L));
        assertThrows(IllegalStateException.class, () -> c.onEvent(new TransactionEvent.End(1L, 2L)));
        TransactionCollector c2 = new TransactionCollector();      // End 的 emitted 与实收条数对账
        c2.onEvent(begin(1L, 2L));
        c2.onEvent(new TransactionEvent.End(1L, 1L));              // 声称 1 实收 0
        // 上一行本身合法（emitted≤expected），封箱时应因 1 != 0 抛 ISE——断言放 transactions() 访问或 onEvent 内（实现选一，测试随之）
        assertThrows(IllegalStateException.class, () -> c2.transactions());
    }
}
```

（RowChange 构造夹具以 record 实际组件对齐；最后一个用例的抛出时机按实现定——设计允许在 End 处理时对账抛出，测试写法随之固定为对应形态。）

**Step 1b: 写 BlockOutputAdapter/OutputMode 失败测试**

`OutputModeTest.java`：

```java
/** OutputMode 解析单测：默认 STREAMING、合法值大小写宽容、未知值 fail-fast 附可用值（风格同 rollCycle 解析）。属性用例 finally 恢复原值。 */
class OutputModeTest {
    @Test
    void defaultsToStreaming() {
        System.clearProperty("vb.output.mode");
        assertEquals(OutputMode.STREAMING, OutputMode.fromSystemProperties());
    }

    @Test
    void parsesBlockCaseInsensitively() {
        System.setProperty("vb.output.mode", "block");
        try {
            assertEquals(OutputMode.BLOCK, OutputMode.fromSystemProperties());
        } finally {
            System.clearProperty("vb.output.mode");
        }
    }

    @Test
    void unknownValueFailsFast() {
        System.setProperty("vb.output.mode", "NOPE");
        try {
            assertThrows(IllegalArgumentException.class, OutputMode.fromSystemProperties());
        } finally {
            System.clearProperty("vb.output.mode");
        }
    }
}
```

`BlockOutputAdapterTest.java`：

```java
/** BlockOutputAdapter 单测：同一事件流经适配器与 TransactionCollector 的整块产物全等（block 模式等价验收）；流不合法时（End 无 Begin）原样 ISE 不转发。夹具 Begin/TxChange 构造照 TransactionCollectorTest。 */
class BlockOutputAdapterTest {

    @Test
    void adapterForwardsSameTransactionsAsCollectorReassembles() {
        List<Transaction> viaAdapter = new ArrayList<>();
        BlockOutputAdapter adapter = new BlockOutputAdapter(viaAdapter::add);
        TransactionCollector collector = new TransactionCollector();
        // 同一事件序列分别喂 adapter 与 collector：Begin(3)+2×TxChange（emitted<expected 合法）+End(2)
        // 断言 viaAdapter 与 collector.transactions() 全等（List.equals）
    }

    @Test
    void illegalStreamPropagatesWithoutForwarding() {
        List<Transaction> out = new ArrayList<>();
        BlockOutputAdapter adapter = new BlockOutputAdapter(out::add);
        assertThrows(IllegalStateException.class, () -> adapter.onEvent(new TransactionEvent.End(1L, 0L)));
        assertEquals(List.of(), out);       // 零转发——block 模式原子性
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -Dtest='TransactionCollectorTest,BlockOutputAdapterTest,OutputModeTest'`
Expected: 编译失败（三个新类不存在）。

- [ ] **Step 3: 实现换血（全部主代码）**

`TransactionListener.java` 新全文（javadoc 按 spec §2：consumer 线程调用、End 返回=下游确认完整消费、回调拖长推迟前沿推进）：

```java
@FunctionalInterface
public interface TransactionListener {
    /** 收到一个事务输出事件（头 Begin / 逐变更 TxChange / 尾 End），按序流式回调。 */
    void onEvent(TransactionEvent event);
}
```

`BlockTransactionListener.java`（1.7 契约保留改名，javadoc：2.0 起为非默认形态——block 模式经 `BlockOutputAdapter` 重组整块后回调；原子性在 block 模式下保留——适配器攒齐才转发，中途失败下游零输出）：

```java
/** 整块事务消费契约（1.7 契约保留，2.0 起非默认——vb.output.mode=block 时经 {@link BlockOutputAdapter} 启用）。 */
@FunctionalInterface
public interface BlockTransactionListener {
    /** 收到一个已确认提交的完整事务（BLOCK 模式：适配器 End 重组后转发；ROLLBACK 路径不回调）。 */
    void onTransaction(Transaction transaction);
}
```

`BlockOutputAdapter.java`（javadoc：非线程安全——consumer 线程；事务级转发后丢弃，不累积历史）：

```java
/** 流式→整块输出边界适配器（2.0 spec §2）：Begin 开桶攒 TxChange，End 封箱转发目标后丢弃；
 * 中途异常攒的内容随失败丢弃，目标零输出（block 模式恢复 1.7 原子交付语义）。 */
public final class BlockOutputAdapter implements TransactionListener {

    private final BlockTransactionListener target;
    private TransactionEvent.Begin open;
    private final List<TxChange> changes = new ArrayList<>();

    public BlockOutputAdapter(BlockTransactionListener target) {
        this.target = Objects.requireNonNull(target, "target");
    }

    @Override
    public void onEvent(TransactionEvent event) {
        if (event instanceof TransactionEvent.Begin b) {
            if (open != null) {
                throw new IllegalStateException("Begin 内嵌 Begin: xid=" + b.xid());
            }
            open = b;
            changes.clear();
        } else if (event instanceof TransactionEvent.End e) {
            if (open == null || e.xid() != open.xid()) {
                throw new IllegalStateException("End 无匹配 Begin: xid=" + e.xid());
            }
            target.onTransaction(new Transaction(open.xid(), open.kind(), open.gid(),
                    open.commitLsn(), open.endLsn(), open.commitTimestamp(), List.copyOf(changes)));
            open = null;
            changes.clear();
        } else if (event instanceof TxChange c) {
            if (open == null) {
                throw new IllegalStateException("变更先于 Begin 到达");
            }
            changes.add(c);
        }
    }
}
```

`OutputMode.java`（枚举 + 解析；javadoc 注明两模式语义差异：streaming=O(单条) 堆/半截可能输出；block=O(事务) 堆/原子交付——vb.output.mode 逃生门回到 1.7 语义）：

```java
/** 输出形态（2.0 spec §1.1）：STREAMING=流式事件交付（默认，回放期堆 O(单条)）；
 * BLOCK=边界适配器重组整块（1.7 语义逃生门，堆 O(事务)、原子交付）。 */
public enum OutputMode {
    STREAMING, BLOCK;

    /** 读 vb.output.mode（默认 STREAMING，大小写宽容；未知值 IAE 附可用值——风格同 PipeConfig.parseRollCycle）。 */
    public static OutputMode fromSystemProperties() {
        String v = System.getProperty("vb.output.mode", "streaming").trim();
        return Arrays.stream(values()).filter(m -> m.name().equalsIgnoreCase(v)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown vb.output.mode '%s', usable values: %s".formatted(v, Arrays.toString(values()))));
    }
}
```

`TransactionCollector.java`（公开类；javadoc：非线程安全——单 consumer 线程；校验语义含 emitted<expected 合法）：

```java
/** 事件流重组器（2.0 spec §2）：把流式事件攒回整块 Transaction——测试等价币/未来需要整块的下游用。 */
public final class TransactionCollector implements TransactionListener {

    private final List<Transaction> transactions = new ArrayList<>();
    private TransactionEvent.Begin open;
    private final List<TxChange> changes = new ArrayList<>();

    @Override
    public void onEvent(TransactionEvent event) {
        if (event instanceof TransactionEvent.Begin b) {
            if (open != null) {
                throw new IllegalStateException("Begin 内嵌 Begin: xid=" + b.xid());
            }
            open = b;
            changes.clear();
        } else if (event instanceof TransactionEvent.End e) {
            if (open == null || e.xid() != open.xid()) {
                throw new IllegalStateException("End 无匹配 Begin: xid=" + e.xid());
            }
            if (e.emittedChanges() > open.expectedChanges() || e.emittedChanges() != changes.size()) {
                throw new IllegalStateException("End 对账失败: xid=" + e.xid() + " emitted=" + e.emittedChanges()
                        + " expected=" + open.expectedChanges() + " received=" + changes.size());
            }
            transactions.add(new Transaction(open.xid(), open.kind(), open.gid(), open.commitLsn(),
                    open.endLsn(), open.commitTimestamp(), List.copyOf(changes)));
            open = null;
        } else if (event instanceof TxChange c) {
            if (open == null) {
                throw new IllegalStateException("变更先于 Begin 到达");
            }
            changes.add(c);
        } else {
            throw new IllegalStateException("未知事件类型: " + event.getClass());
        }
    }

    /** 已重组完成的事务列表（按完成序；End 封箱后可见）。 */
    public List<Transaction> transactions() {
        return transactions;
    }
}
```

`TransactionConsumer.processBucket`（:115 起替换，frontier/DONE 语义不变）：

```java
void processBucket(TxBuffer bucket) {
    bucket.state = BucketState.OUTPUTTING;
    listener.onEvent(new TransactionEvent.Begin(bucket.xid, bucket.kind, bucket.gid,
            bucket.commitLsn, bucket.endLsn, bucket.commitTimestamp, bucket.unitCount));
    long[] emitted = {0L};                                  // 数组：异常路径计数存活（fail-fast 截断日志用）
    try {
        replayer.replay(bucket, pipe, change -> {
            emitted[0]++;
            listener.onEvent(change);
        });
    } catch (Throwable t) {
        LOG.error("事务流式输出中断（已输出 {}/{} 条）: xid={} firstIndex={}",
                emitted[0], bucket.unitCount, bucket.xid, bucket.firstIndex, t);
        throw t;
    }
    listener.onEvent(new TransactionEvent.End(bucket.xid, emitted[0]));
    outputFrontier.accumulateAndGet(bucket.endLsn, Math::max);
    bucket.state = BucketState.DONE;
}
```

（consumer 循环 catch 的 ERROR 行去掉与上文重复的字段，保留 xid 与 onFailure/不排干退出语义。）

`BucketReplayer.replay`（:83 起签名与返回值改造；逐单元三步原样，aborted 过滤后 `sink.accept` 并计数）：

```java
/** 按段回读逐条交付（2.0：不再构造列表——堆内 O(单条)）。返回交付数（aborted 过滤后）。 */
long replay(TxBuffer bucket, MessagePipe pipe, Consumer<TxChange> sink) {
    long[] emitted = {0L};
    for (long[] segment : bucket.segments) {
        pipe.readRange(segment[0], segment[1], (index, payload) -> {
            OptionalLong streamXid = bucket.hasPrefix
                    ? OptionalLong.of(RawPeeks.unsignedInt(payload, 1))
                    : OptionalLong.empty();
            if (streamXid.isPresent() && bucket.abortedSubxids.contains(streamXid.getAsLong())) {
                return;
            }
            sink.accept(replayUnit(payload, index, streamXid));
            emitted[0]++;
        });
    }
    return emitted[0];
}
```

（`replayUnit` 零改动；类 javadoc 的"MEMORY 形态"残留叙述若还有则顺手清理。）

`TxBuffer` 增字段（javadoc：reader 追加期自增、交接后只读、 Begin.expectedChanges 来源）：`long unitCount;`
`TransactionAssembler.appendUnit`（或 collectOids/appendIndex 调用处）加 `bucket.unitCount++;`
`Transaction.java` javadoc 换角色（"2.0 起为 block 模式交付单元 + 流式重组值对象"），record 定义零改动。
`ConsoleListener`：实现 **`TransactionListener` 与 `BlockTransactionListener` 双契约**——既有 `onTransaction(Transaction)` 渲染**原样保留**（签名加 `implements BlockTransactionListener`；`changes=N` 取 `transaction.changes().size()`，1.7 行为不变）；新增实例字段 `private int rowSeq;`（javadoc 注明：事务内行号，Begin 清零，线程限定 consumer——流式渲染路径从无状态变轻状态）与流式 `onEvent`：

```java
@Override
public void onEvent(TransactionEvent event) {
    if (event instanceof TransactionEvent.Begin b) {
        CDC.info("TXN-BEGIN xid={} kind={} gid={} commitLsn=0x{} commitTs={} changes={}",
                b.xid(), b.kind(), b.gid(), Long.toHexString(b.commitLsn()), b.commitTimestamp(),
                b.expectedChanges());
        rowSeq = 1;
    } else if (event instanceof TxChange change) {
        CDC.info("  [{}] {}", rowSeq++, renderChange(change));
    } else if (event instanceof TransactionEvent.End e) {
        CDC.info("TXN-END   xid={}", e.xid());
    }
}
```

（`renderChange` 两路径共享零改动；类 javadoc 改述：双契约实现者——流式直渲染 / block 经适配器走 onTransaction；流式头行 `changes=N` 为 expected（aborted 过滤前）、block 为实际条数，仅含子事务回滚的事务两模式头行有差异，javadoc 注明。）

`Main.java` 接线（替换原 listener 直传处）：

```java
OutputMode mode = OutputMode.fromSystemProperties();
LOG.info("输出形态: mode={}", mode);
TransactionListener output = mode == OutputMode.BLOCK
        ? new BlockOutputAdapter(console)   // 1.7 语义逃生门：原子交付、O(事务) 堆
        : console;                          // 流式直渲染：O(单条) 堆
// 组装器构造的第一参传 output
```

（javadoc 与启动日志同步两模式语义；配置缺失/非法启动期 fail-fast 沿 OutputMode 解析。）

- [ ] **Step 4: 测试侧迁移（断言零改动）**

- `TransactionAssemblerTest` 两个 `run(...)` 夹具：`out::add` 换 `TransactionCollector`（返回 `collector.transactions()`）；`handedOffBucketConstrainsPipeWatermark` 的阻塞 listener `t -> {...}` 换 `e -> { inCallback.countDown(); await... }`（阻塞点任意事件即可）
- `BucketReplayerTest`：驱动处 `List<TxChange> changes = new ArrayList<>(); replayer.replay(bucket, pipe, changes::add)`——既有断言对着该列表，零改动
- `ConsoleListenerTest`：既有 onTransaction 断言**原样保留**（block 渲染路径回归）；新增 onEvent 序列断言（构造 Begin/变更/End 事件喂入，捕获 CDC 日志断言与现有格式字符串一致——流式头行 `changes=N` 取 Begin.expectedChanges，无 aborted 过滤时与 block 输出逐字节一致）
- `DecoupledEquivalenceTest`：两侧 listener 换 `TransactionCollector`，`assertEquals(syncOut, asyncOut)` 断言零改动（Task 3 再升级为事件流全等）
- `BenchPipeBridge.replay()`：内部 `List<TxChange> out = new ArrayList<>(); replayer.replay(bucket, pipe, out::add); return out;`（编译适配；Task 5 改计数口径）
- 其余引用 `onTransaction` 的测试按编译器指引逐一迁移（原则：**断言值零改动**，只换驱动形态）

- [ ] **Step 5: 全量验证 + 提交**

Run: `mvn clean test`
Expected: 全绿（用例数 = 144 + TransactionEventTest 2 + TransactionCollectorTest 4 ± 迁移中删除的等价冗余，报告如实计数）。

```bash
git add -A
git commit -m "feat(replication)!: 输出契约流式化——onEvent 事件交付 + TransactionCollector 重组等价币（断言零改动）"
git push
```

---

### Task 3: 流式时序证明 + 事件流全等升级

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/replication/StreamingDeliveryTest.java`
- Modify: `src/test/java/org/vastdata/vbstream/replication/DecoupledEquivalenceTest.java`

**Interfaces:**
- Consumes: Task 2 的 onEvent 契约与异步构造器

- [ ] **Step 1: 写流式时序证明用例（spec §5.2 的核心验收）**

```java
package org.vastdata.vbstream.replication;

// imports：junit、PgWire、StreamingMode、j.u.c、LegacyRollCycles、Path

/**
 * 流式交付时序证明（2.0 spec §5.2）：变更事件在回放进行中即已到达 listener，先于事务完成——
 * 直接证明"边回放边输出"。构造：单事务 3 条 Insert；listener 在第 1 条 TxChange 后闭锁阻塞
 * （End 永不可达）；断言：已收到恰 1 条变更、无 End、无重组完成的事务。
 */
class StreamingDeliveryTest {

    @Test
    void changeEventsArriveBeforeTransactionCompletes() throws Exception {
        CountDownLatch firstChange = new CountDownLatch(1);
        AtomicInteger changesSeen = new AtomicInteger();
        AtomicInteger endSeen = new AtomicInteger();
        List<Transaction> reassembled = new CopyOnWriteArrayList<>();   // 恒空——End 未达
        TransactionListener blocking = event -> {
            if (event instanceof TxChange) {
                if (changesSeen.incrementAndGet() == 1) {
                    firstChange.countDown();
                }
                awaitForever();               // 阻塞在第一条变更里——End/后续变更永不可达
            } else if (event instanceof TransactionEvent.End) {
                endSeen.incrementAndGet();
            }
        };
        // 异步构造器（阻塞 listener）→ 喂 Relation/Begin/3×Insert/Commit → await firstChange(5s)
        // 断言：changesSeen==1、endSeen==0、reassembled 恒空（阻塞 listener 不重组——用独立收集器对照可省）
        // finally：close 组装器（consumer 卡在回调，join 60s 超时 WARN 后放行——测试收尾允许）
    }
}
```

（`awaitForever` = latch await 带超时的私有辅助；骨架按注释展开为完整实现，断言三件套：变更已出、End 未出、事务未完成。）

- [ ] **Step 2: DecoupledEquivalenceTest 升级事件流全等 + block 模式等价**

两侧 listener 改为 `List<TransactionEvent> events = new ArrayList<>(); event -> events.add(event)`（收集完整事件流含头尾），断言 `assertEquals(syncEvents, asyncEvents)`——比 List\<Transaction\> 更严（头尾元数据进断言）。原 Transaction 收集器断言保留为第二断言（双保险）。
**block 模式等价（spec §5.1）**：同一字节流再跑一异步组装器，listener = `new BlockOutputAdapter(blockOut::add)`，断言 `blockOut` 与收集器重组结果全等——两模式输出语义一致性入验收。

- [ ] **Step 3: 跑测 + 全量 + 提交**

Run: `mvn test -Dtest='StreamingDeliveryTest,DecoupledEquivalenceTest' && mvn clean test`
Expected: PASS 全绿。

```bash
git add -A && git commit -m "test(replication): 流式交付时序证明 + 同步/异步完整事件流全等验收" && git push
```

---

### Task 4: IT 迁移

**Files:**
- Modify: `src/test/java/org/vastdata/vbstream/it/DecoupledPipelineTest.java`、`ReaderUnblockedTest.java`、`FrontierCapTest.java`

**Interfaces:**
- Consumes: Task 2 契约（阻塞点选首个事件即可——Begin 阻塞则 End 不可达，frontier 不推进，两 IT 语义等价保持）。

- [ ] **Step 1: DecoupledPipelineTest**：`replayAsync` 的 `List<Transaction> out; listener = out::add` 换 `TransactionCollector`，断言对 `collector.transactions()` 零改动；javadoc 补一句事件流交付形态。
- [ ] **Step 2: ReaderUnblockedTest**：阻塞 listener `t -> { out.add(t); latch... }` 换 `event -> { firstOutput.countDown(); await(20s); }`（阻塞在 Begin——consumer 卡住、reader 继续接收的语义不变）；`assertEquals(10, out.size())` 改经收集器（阻塞放行后 End 到达、收集器封箱）。
- [ ] **Step 3: FrontierCapTest**：同样阻塞在首事件（Begin）——End 不可达 → frontier 不推进 → confirmed_flush 钉住的断言语义不变；放行后 End 到达 → frontier ← endLsn。
- [ ] **Step 4: 跑 IT + 全量 + 提交**

Run: `mvn clean test -Dtest='DecoupledPipelineTest,ReaderUnblockedTest,FrontierCapTest' && mvn clean test`
Expected: 全绿。

```bash
git add -A && git commit -m "test(it): 三组 IT 迁移事件契约（阻塞点=Begin，语义等价保持）" && git push
```

---

### Task 5: 基准迁移 + gc 对照 + 文档收尾

**Files:**
- Modify: `src/test/java/org/vastdata/vbstream/replication/BenchPipeBridge.java`（replay 改计数口径）、`src/jmh/java/org/vastdata/vbstream/bench/AssembleMemoryBenchmark.java`（noop listener 形态）、`PipePathBenchmark.java`（replayBucket 计数 sink）
- Modify: `docs/benchmarks-baseline.md`（2.0 段）、`CLAUDE.md`（根）、`src/main/java/org/vastdata/vbstream/replication/CLAUDE.md`、`src/jmh/CLAUDE.md`、`README.md`

**Interfaces:**
- Consumes: Task 1 存证的 `/tmp/m20-gc-before.txt`、Task 2 契约。

- [ ] **Step 1: 基准口径迁移**——`BenchPipeBridge` 增 `long replayCounting()`（`replayer.replay(bucket, pipe, c -> {})` 返回条数；原 `replay()` 保留或删除以实际引用定）；`PipePathBenchmark.replayBucket` 计时体改 `return state.piped.replayCounting();`（返回 long 防死码）；`AssembleMemoryBenchmark` 的 `tx -> { }` listener 形态随编译器指引迁移。
- [ ] **Step 2: 换血后 gc 对照**——Task 1 Step 5 同命令重跑 `replayBucket -prof gc`，`/tmp/m20-gc-after.txt`；两侧 `gc.alloc.rate.norm` 差值入档（如实标注口径：列表累积与 List.copyOf 的分配消失；"堆峰 O(单条)"的结构论证单独一段，不与该数字混同）。
- [ ] **Step 3: baseline 2.0 段**——契约变更说明 + replayBucket 前后（含 gc.norm）+ AssembleMemory 复测 + 口径注记（jmh/CLAUDE.md 四/五基准表同步）。
- [ ] **Step 4: 文档同步**——根 CLAUDE.md（里程碑状态 2.0：流式输出契约/双形态 vb.output.mode 配置/frontier 锚 End/TxBuffer.unitCount；运行 Main 段补 `-Dvb.output.mode=streaming|block`；`mvn test` 用例数更新）；replication/CLAUDE.md（TransactionListener/BlockTransactionListener/BlockOutputAdapter/OutputMode/TransactionCollector/TransactionEvent 节 + TransactionConsumer/BucketReplayer 2.0 形态 + ConsoleListener 双契约与轻状态注记）；README（若引用输出格式/契约则同步）。
- [ ] **Step 5: 全量 + 提交**

Run: `mvn clean test`（Docker 在位含 IT）
```bash
git add -A && git commit -m "bench+docs: 2.0 收尾——事件契约基准口径/gc 对照入档/CLAUDE.md×3+README 同步" && git push
```

---

## 任务依赖

Task 1 → Task 2 → Task 3 → Task 4 → Task 5（严格串行：Task 2 原子换血，其余依次叠加验收）。
