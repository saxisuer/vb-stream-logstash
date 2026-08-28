# 里程碑 1.7 读取与组装输出解耦——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** reader 线程只做"记账 + 写 Chronicle Queue + 反馈 LSN"，事务回放与输出移到 `TransactionAssembler` 内部的 consumer 线程；CQ 从溢写池升级为主缓冲管道。

**Architecture:** 1.6 的组装状态机原地保留在 reader 线程，存储层从 MEMORY/SPILLED 混合改为"纯 CQ index 段记账"（seq ≡ CQ index）；提交消息把冻结的桶（含 Relation 版本快照）交接给 `TransactionConsumer` 线程回放输出；LSN 反馈按输出前沿封顶。依据 spec：`docs/superpowers/specs/2026-08-29-reader-consumer-decoupling-design.md`（下称"spec"）。

**Tech Stack:** Java 17、Maven、JUnit 6（Jupiter）、chronicle-queue 2026.6、Testcontainers（IT）、JMH（`-Pjmh` 档）。

## Global Constraints

- Java 17 语法（无 record pattern switch）；不新增任何依赖。
- 日志一律 slf4j，禁止 `System.out`/`System.err`；`{}` 占位符；CDC 数据走 logger `org.vastdata.vbstream.cdc`。
- **每个函数（含私有/测试辅助）必须有 javadoc 逻辑描述**（职责/关键步骤/边界与异常语义/线程约束），中文，参照现有代码密度。协议相关代码指向 spec 章节。
- 测试用例与生产代码同规约；测试类 javadoc 说明夹具约定。
- 验证编译必须 `mvn clean test-compile`（增量编译可能假绿）；跑测试 `mvn test -Dtest=Xxx`。
- **每个任务完成即 `git commit + git push`**（跨机开发约定）；提交信息中文 conventional 风格（如 `feat(replication): ...`）。
- JMH 源码（`src/jmh`）默认构建不编译——Task 5 会让 `SpillPathBenchmark`/`AssembleMemoryBenchmark` 暂时失编译，属预期，Task 9 收敛（`mvn test` 全程保持绿）。
- 线程约束红线：reader 路径只触碰 pipe.append / 桶记账 / registry / 队列入队；consumer 路径只触碰 pipe.readRange / 冻结桶 / 输出回调 / 前沿累加。共享可变量仅限：交接队列、桶的 `volatile state`、`AtomicLong` 前沿、`AtomicInteger` 存活桶计数。

---

### Task 1: PipeConfig（`vb.pipe.*` 配置面）

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/PipeConfig.java`
- Create: `src/test/java/org/vastdata/vbstream/replication/PipeConfigTest.java`

**Interfaces:**
- Produces: `public record PipeConfig(Path dir, RollCycle rollCycle)`；`static PipeConfig fromSystemProperties()`。Task 5 起被 `TransactionAssembler`/`Main` 消费。

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** PipeConfig 配置面单测：默认值、系统属性覆盖、非法 rollCycle 启动期 fail-fast。逐用例设置系统属性并在 finally 清理，防用例间串扰。 */
class PipeConfigTest {

    /** 设置系统属性并在测试结束后恢复原值（缺失则移除），保证用例隔离。 */
    private static void withProp(String key, String value, Runnable body) {
        String old = System.getProperty(key);
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
        try {
            body.run();
        } finally {
            if (old == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, old);
            }
        }
    }

    @Test
    void defaultsMatchSpec() {
        PipeConfig cfg = PipeConfig.fromSystemProperties();
        assertEquals(Path.of("pipe-queue"), cfg.dir());
        assertEquals(LegacyRollCycles.MINUTELY, cfg.rollCycle());
    }

    @Test
    void overridesBothProperties() {
        withProp("vb.pipe.dir", "my-pipe", () ->
                withProp("vb.pipe.rollCycle", "hourly", () -> {
                    PipeConfig cfg = PipeConfig.fromSystemProperties();
                    assertEquals(Path.of("my-pipe"), cfg.dir());
                    assertEquals(LegacyRollCycles.HOURLY, cfg.rollCycle());
                }));
    }

    @Test
    void unknownRollCycleFailsFastWithUsableValues() {
        withProp("vb.pipe.rollCycle", "NOPE", () ->
                assertThrows(IllegalArgumentException.class, PipeConfig::fromSystemProperties));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -Dtest=PipeConfigTest`
Expected: 编译失败（PipeConfig 不存在）。

- [ ] **Step 3: 实现 PipeConfig**

照 `SpillConfig.java` 的结构与 javadoc 密度写（`src/main/java/org/vastdata/vbstream/replication/SpillConfig.java` 是模板）：

```java
package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;

import java.nio.file.Path;
import java.util.Arrays;

/**
 * 解耦管道（reader → Chronicle Queue → consumer 的主缓冲）配置。不可变；默认值见 1.7 设计 §8：
 * 目录 {@code pipe-queue}、滚动周期 MINUTELY，可经 {@code -Dvb.pipe.*} 覆盖。
 * 管道是解耦架构的地基（没有"禁用"逃生门——绕过管道等于回到 1.6 同步阻塞形态）。
 *
 * @param dir       管道目录（瞬态工作区，打开时整体清空重建，不跨重启续用；真源是复制槽）
 * @param rollCycle 管道队列滚动周期（默认 {@link LegacyRollCycles#MINUTELY}），决定滚动文件粒度与删除水位档位
 */
public record PipeConfig(Path dir, RollCycle rollCycle) {

    /**
     * 从系统属性构造配置：{@code vb.pipe.dir}（默认 {@code pipe-queue}）、{@code vb.pipe.rollCycle}
     * （默认 {@code MINUTELY}，枚举名大小写宽容）。属性缺失或空白取默认；rollCycle 无法识别抛
     * {@link IllegalArgumentException}（消息附可用值列表），启动期 fail-fast。
     *
     * @return 按当前系统属性解析出的配置实例
     */
    public static PipeConfig fromSystemProperties() {
        return new PipeConfig(
                Path.of(prop("vb.pipe.dir", "pipe-queue")),
                parseRollCycle(prop("vb.pipe.rollCycle", "MINUTELY")));
    }

    // parseRollCycle / prop：与 SpillConfig 同名方法同实现，仅属性前缀与错误消息中的属性名不同
    // （"unknown vb.pipe.rollCycle '%s', usable values: %s"）。
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -Dtest=PipeConfigTest`
Expected: PASS（3 用例）。

- [ ] **Step 5: 提交推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication/PipeConfig.java src/test/java/org/vastdata/vbstream/replication/PipeConfigTest.java
git commit -m "feat(replication): PipeConfig——vb.pipe.* 解耦管道配置面"
git push
```

---

### Task 2: MessagePipe（主缓冲管道，去信封帧 + readRange 携带 index）

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/MessagePipe.java`
- Create: `src/test/java/org/vastdata/vbstream/replication/MessagePipeTest.java`

**Interfaces:**
- Produces（Task 5/6 消费）:
  - `long append(byte[] payload)` — **仅 reader 线程**；返回 CQ index（即消息 seq）
  - `void readRange(long firstIndex, long lastIndex, BiConsumer<Long, byte[]> payloadConsumer)` — **仅 consumer 线程**；每条回调 `(该条真实 CQ index, payload 副本)`
  - `long releaseBelow(long lowestNeededIndex)` / `long lastAppendedIndex()` / `void close()` — 仅 reader 线程
- 语义与 `MessageSpool` 相同（wipe-on-open、保守删档、close 顺序 tailer→appender→queue）；本任务**新增** MessagePipe 与 MessageSpool 暂时共存，Task 5 切换后删除旧类。

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** MessagePipe 单测：append/readRange 往返、index 暴露、wipe-on-open、错位 fail-fast。夹具：@TempDir 独立目录，每用例新建管道。 */
class MessagePipeTest {

    @TempDir
    Path dir;

    @Test
    void readRangeExposesRealIndexPerMessage() throws IOException {
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            long i0 = pipe.append(new byte[]{'I', 1});
            long i1 = pipe.append(new byte[]{'I', 2});
            long i2 = pipe.append(new byte[]{'C'});
            List<Long> indexes = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();
            pipe.readRange(i0, i2, (idx, payload) -> {
                indexes.add(idx);
                payloads.add(payload);
            });
            assertEquals(List.of(i0, i1, i2), indexes);       // index 单调且即真实 CQ index
            assertEquals((byte) 1, payloads.get(0)[1]);       // payload 为副本、内容保真
            assertEquals((byte) 2, payloads.get(1)[1]);
            assertEquals((byte) 'C', payloads.get(2)[0]);
        }
    }

    @Test
    void singleMessageRangeYieldsExactlyOne() throws IOException {
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            long first = pipe.append(new byte[]{'B'});
            pipe.append(new byte[]{'I', 9});
            long second = pipe.append(new byte[]{'C'});
            List<Long> seen = new ArrayList<>();
            pipe.readRange(second, second, (idx, p) -> seen.add(idx));
            assertEquals(List.of(second), seen);
        }
    }

    @Test
    void wipeOnOpenClearsStaleFiles() throws IOException {
        try (MessagePipe first = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            first.append(new byte[]{'B'});
        }
        try (MessagePipe second = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            List<byte[]> seen = new ArrayList<>();
            second.readRange(0, 100, (idx, p) -> seen.add(p));
            assertEquals(List.of(), seen);        // 旧数据整体抹掉，空手而归不抛
        }
    }

    @Test
    void mismatchedStartIndexFailsFast() throws IOException {
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            pipe.append(new byte[]{'B'});
            pipe.append(new byte[]{'C'});
            assertThrows(IllegalStateException.class,
                    () -> pipe.readRange(9999, 10000, (idx, p) -> { }));
        }
    }
}
```

（import 里补 `net.openhft.chronicle.queue.rollcycles.LegacyRollCycles`。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -Dtest=MessagePipeTest`
Expected: 编译失败（MessagePipe 不存在）。

- [ ] **Step 3: 实现 MessagePipe**

以 `MessageSpool.java` 为底本（git 不动它），`git cp` 语义新建 MessagePipe：

```bash
cp src/main/java/org/vastdata/vbstream/replication/MessageSpool.java src/main/java/org/vastdata/vbstream/replication/MessagePipe.java
```

改动点（逐条）：
1. 类名/构造器名/LOG 换 `MessagePipe`；javadoc 重写——"主缓冲管道（1.7 设计 §4.2）：一条 CQ 记录 = 一条完整 pgoutput 消息（含控制消息，为建立 seq 时间线），无信封帧"。
2. **线程约束 javadoc 重写**（替换"非线程安全，单写者"段）：`append`/`lastAppendedIndex`/`releaseBelow`/`close` 由 reader 线程调用，`readRange` 由 consumer 线程调用——appender 与 tailer 各自单线程使用，跨线程由 Chronicle Queue 的单 appender/多 tailer 内存模型保证（官方支持）；两类方法不得交叉线程调用。
3. `append(byte[] framed)` → `append(byte[] payload)`，参数名与 javadoc 相应改（"调用方保证字节构造后不变；本方法只搬字节"）。
4. `readRange` 签名：`ObjIntConsumer<byte[]> framedConsumer` → `BiConsumer<Long, byte[]> payloadConsumer`；循环体内把 `framedConsumer.accept(readFrameBytes(dc), ordinal++)` 改为 `payloadConsumer.accept(idx, readFrameBytes(dc))`（`idx` 来自现有的 `long idx = dc.index()` 局部变量）；javadoc 中"帧字节 + 区间内序号"改为"payload 副本 + 该条真实 CQ index（调用方作 seq 用）"。`readFrameBytes` 私有方法与其余守卫（首条 index 必须 == firstIndex 等）原样保留。
5. import：`java.util.function.ObjIntConsumer` → `java.util.function.BiConsumer`。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -Dtest=MessagePipeTest`
Expected: PASS（4 用例）。

- [ ] **Step 5: 提交推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication/MessagePipe.java src/test/java/org/vastdata/vbstream/replication/MessagePipeTest.java
git commit -m "feat(replication): MessagePipe——主缓冲管道（去帧/readRange 携带 index/跨线程分工）"
git push
```

---

### Task 3: RelationLookup + RelationSnapshot + registry.snapshot()

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/RelationLookup.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/RelationSnapshot.java`
- Modify: `src/main/java/org/vastdata/vbstream/replication/RelationRegistry.java`（implements RelationLookup）
- Modify: `src/main/java/org/vastdata/vbstream/replication/VersionedRelationRegistry.java`（新增 `snapshot`）
- Modify: `src/main/java/org/vastdata/vbstream/replication/PgOutputListener.java`（`onMessage` 第二参换 `RelationLookup`）
- Modify: `src/main/java/org/vastdata/vbstream/ConsoleListener.java`（`onMessage`/`render`/`tableOf`/`tupleOf` 的 registry 参型换 `RelationLookup`）
- Test: `src/test/java/org/vastdata/vbstream/replication/RelationSnapshotTest.java`（新建）

**Interfaces:**
- Produces:
  - `public interface RelationLookup { Optional<PgOutputMessage.Relation> find(int relationOid); }`——宽松视图（miss 返回 empty，供渲染降级）。`RelationRegistry` 天然实现。
  - `final class RelationSnapshot implements RelationLookup`：`PgOutputMessage.Relation require(int relationOid, long asOfSeq)`（二分取 ≤ asOfSeq 最新版，未命中抛 ISE"未先行到达"——消息风格与 `VersionedRelationRegistry.require` 一致）+ `find(int)`（快照内最新版，miss empty）。
  - `VersionedRelationRegistry`：`public RelationSnapshot snapshot(Set<Integer> oids, long maxSeq)`——各 oid 拷出 seq ≤ maxSeq 的版本前缀；oid 无版本或全部晚于 maxSeq 时**省略**（留给 require 时 fail-fast，与 1.6 回放期报错时机一致）。
- 动机（spec §4.3 + 计划期补强）：回放渲染发生在 consumer 线程，不能再读 reader 的 HashMap registry——不可变快照随桶交接；`ConsoleListener.onMessage` 的参型从 `RelationRegistry` 放宽为 `RelationLookup` 后，live 解码点传 registry（reader 线程）、回放解码点传桶快照（consumer 线程），竞争由构造消除。

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RelationSnapshot 单测：快照截止、asOf 二分、省略 oid 的 require fail-fast、find 最新视图。Relation 消息经真实 PgWire 无需构造——直接 new Relation record（其组件见 protocol 包）。 */
class RelationSnapshotTest {

    /** 构造两列 Relation 消息 record（protocol 包的 record 组件：oid/schema/table/columns）。 */
    private static PgOutputMessage.Relation rel(int oid, String table) {
        return new PgOutputMessage.Relation(oid, "public", table, List.of());
    }

    @Test
    void snapshotCutsVersionsAboveMaxSeq() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, rel(1, "v1"));
        registry.accept(20L, rel(1, "v2"));
        registry.accept(30L, rel(1, "v3"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 20L);
        assertEquals("v2", snap.require(1, 20L).table());   // ≤20 的最新版
        assertThrows(IllegalStateException.class, () -> snap.require(1, 15L));  // v1 已在截止之外？——不，v1 在快照里
        assertEquals("v1", snap.require(1, 15L).table());
    }

    @Test
    void oidWithoutVersionsIsOmittedAndRequiresFails() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        RelationSnapshot snap = registry.snapshot(Set.of(99), 100L);
        assertThrows(IllegalStateException.class, () -> snap.require(99, 50L));  // "未先行到达"
        assertTrue(snap.find(99).isEmpty());
    }

    @Test
    void findReturnsLatestWithinSnapshot() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(5L, rel(1, "old"));
        registry.accept(6L, rel(1, "new"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 6L);
        assertEquals(Optional.of("new"), snap.find(1).map(PgOutputMessage.Relation::table));
    }
}
```

注意：`snapshotCutsVersionsAboveMaxSeq` 中第 2 个断言行是注释性说明（v1 在快照内），实际断言以第 3 行 `require(1,15L)=="v1"` 为准——写测试时删掉那行 `assertThrows`（保留 `assertEquals("v1", ...)`）。若 `PgOutputMessage.Relation` 的 record 组件与上面不符，以 `src/main/java/org/vastdata/vbstream/protocol/PgOutputMessage.java` 实际定义为准调整构造。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -Dtest=RelationSnapshotTest`
Expected: 编译失败（RelationSnapshot 不存在）。

- [ ] **Step 3: 实现 RelationLookup / RelationSnapshot / snapshot**

```java
// RelationLookup.java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import java.util.Optional;

/**
 * Relation 宽松查询视图（find 语义：miss 返回 empty，供渲染降级为 "oid:N"）。
 * 存在动机（1.7 设计 §4.3）：逐消息渲染的 Relation 来源随线程不同——live 解码点在 reader 线程
 * 传版本日志最新视图，回放解码点在 consumer 线程传桶内不可变快照；本接口是两者的公共形态，
 * 使 ConsoleListener 不依赖具体 registry 实现。
 * 线程约束：实现方自行保证线程语义（VersionedRelationRegistry 版仅限 reader 单写者线程；
 * RelationSnapshot 版不可变、线程任意）。
 */
public interface RelationLookup {
    /** 按 oid 查最新已知 Relation；未知返回 empty（调用方降级渲染，不 fail-fast）。 */
    Optional<PgOutputMessage.Relation> find(int relationOid);
}
```

```java
// RelationSnapshot.java（骨架，javadoc 按项目密度补全）
package org.vastdata.vbstream.replication;

// imports 同 RelationLookup + java.util.{HashMap, List, Map, Optional}

/**
 * 不可变 Relation 版本快照（1.7 设计 §4.3）：reader 在桶交接瞬间从 {@link VersionedRelationRegistry}
 * 拷出（各 oid 取 seq ≤ maxSeq 的版本前缀），随冻结桶交给 consumer 线程回放渲染——consumer 不共享
 * reader 的 registry，跨线程零并发改造。线程约束：不可变，任意线程。
 */
final class RelationSnapshot implements RelationLookup {

    /** 单版本条目（seq = 该 Relation 消息的 CQ index）。 */
    record Entry(long seq, PgOutputMessage.Relation rel) {}

    private final Map<Integer, List<Entry>> versions;

    RelationSnapshot(Map<Integer, List<Entry>> versions) {
        this.versions = versions;   // 仅由 registry.snapshot 构造，入参即私有（构造后不再改）
    }

    /** asOf 二分取 ≤ asOfSeq 的最新版；oid 无版本或全部晚于 asOfSeq 抛 ISE（"Relation 未先行到达"，与 VersionedRelationRegistry.require 同风格）。 */
    PgOutputMessage.Relation require(int relationOid, long asOfSeq) {
        List<Entry> list = versions.get(relationOid);
        // 手写 floorIndex 二分（照 VersionedRelationRegistry.floorIndex 的写法）
        // idx < 0 → throw new IllegalStateException("Relation oid=" + oid + " 未先行到达（asOf seq=" + asOfSeq + "），协议流异常")
    }

    /** 快照内该 oid 的最新版本（宽松视图，miss empty）。 */
    @Override
    public Optional<PgOutputMessage.Relation> find(int relationOid) {
        List<Entry> list = versions.get(relationOid);
        return (list == null || list.isEmpty()) ? Optional.empty() : Optional.of(list.get(list.size() - 1).rel());
    }
}
```

`VersionedRelationRegistry` 新增方法（放在 `pruneBelow` 之后）：

```java
/**
 * 责任：把指定 oid 集合在 maxSeq 时刻已生效的版本前缀拷成不可变快照（1.7 设计 §4.3）。
 * 关键步骤：逐 oid 二分找 ≤ maxSeq 的最新版本下标，把 [0..idx] 的 (seq, Relation) 拷入新列表；
 * 拷贝是浅拷（Relation record 不可变，引用可安全共享）。oid 无版本或全部版本晚于 maxSeq 时省略——
 * 回放期 RelationSnapshot.require 会以"未先行到达"fail-fast，报错时机与 1.6 直查 registry 一致。
 * 边界：oids 为 null 抛 NPE；maxSeq ≤ 0 时所有 oid 都省略（空桶无渲染需求）。单写者（reader）调用。
 */
public RelationSnapshot snapshot(Set<Integer> oids, long maxSeq) {
    Map<Integer, List<RelationSnapshot.Entry>> out = new HashMap<>();
    for (Integer oid : oids) {
        List<Version> list = versions.get(oid);
        if (list == null) {
            continue;
        }
        int idx = floorIndex(list, maxSeq);
        if (idx < 0) {
            continue;
        }
        List<RelationSnapshot.Entry> copied = new ArrayList<>(idx + 1);
        for (int i = 0; i <= idx; i++) {
            copied.add(new RelationSnapshot.Entry(list.get(i).seq(), list.get(i).rel()));
        }
        out.put(oid, List.copyOf(copied));
    }
    return new RelationSnapshot(out);
}
```

签名面切换（编译器指引逐点改）：
- `RelationRegistry` 声明加 `implements RelationLookup`（find 已存在，零实现改动）。
- `PgOutputListener.onMessage(PgOutputMessage, RelationRegistry)` 第二参型 → `RelationLookup`（javadoc 注明放宽动机）。
- `ConsoleListener.onMessage`/`render(PgOutputMessage, RelationRegistry)`/`tableOf(int, RelationRegistry)`/`tupleOf(int, TupleData, RelationRegistry)` 的 registry 参型全部 → `RelationLookup`（方法体零改动——只用 find）。`ConsoleListener` 里 `RelationRegistry` 的 import 换成 `RelationLookup`。
- `DecodedMessageBridge` 调 `target.onMessage(msg, registry())`：`registry()` 返回 `RelationRegistry`，是 `RelationLookup` 子型，无需改动（跑 `mvn clean test-compile` 确认）。

- [ ] **Step 4: 跑全量测试确认通过**

Run: `mvn clean test -Dtest='RelationSnapshotTest,ConsoleListenerTest,RelationRegistryTest,VersionedRelationRegistryTest,RawSessionContractTest'`
Expected: PASS（含签名切换无回归）。

- [ ] **Step 5: 提交推送**

```bash
git add -A src/main/java/org/vastdata/vbstream/replication/ src/main/java/org/vastdata/vbstream/ConsoleListener.java src/test/java/org/vastdata/vbstream/replication/RelationSnapshotTest.java
git commit -m "feat(replication): RelationSnapshot 版本快照 + RelationLookup 渲染视图（consumer 线程不共享 registry）"
git push
```

---

### Task 4: PgReplicationSession 反馈前沿封顶

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/PgReplicationSession.java:115-143`
- Test: `src/test/java/org/vastdata/vbstream/replication/PgReplicationSessionTest.java`（新建，纯函数级）

**Interfaces:**
- Produces: `public void run(RawMessageListener listener, LongSupplier outputFrontier) throws SQLException, IOException`（既有 `run(listener)` 委托 `run(listener, () -> 0L)`，签名不变向后兼容）；包私有静态纯函数 `static long capFeedback(long received, long outputFrontier)`。Task 7 的 Main 用重载。

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** capFeedback 纯函数单测：前沿 ≤0 视为无 cap（首个事务输出前与 1.6 行为一致）、否则取 min。 */
class PgReplicationSessionTest {

    @Test
    void zeroOrNegativeFrontierMeansNoCap() {
        assertEquals(500L, PgReplicationSession.capFeedback(500L, 0L));
        assertEquals(500L, PgReplicationSession.capFeedback(500L, -1L));
    }

    @Test
    void positiveFrontierCapsToMinimum() {
        assertEquals(300L, PgReplicationSession.capFeedback(500L, 300L));
        assertEquals(500L, PgReplicationSession.capFeedback(300L, 500L));  // 前沿不会超过已收到，防御性取 min
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -Dtest=PgReplicationSessionTest`
Expected: 编译失败（capFeedback 不存在）。

- [ ] **Step 3: 实现**

在 `PgReplicationSession` 中：

```java
/**
 * 责任：反馈位点封顶纯函数（1.7 设计 §5）——LSN 确认锚定输出前沿，crash 时未输出事务必然被重发。
 * 关键步骤：前沿 ≤0（尚未有任何事务输出）视为无 cap，反馈已收到值；否则取 min（前沿不会超过
 * 已收到，防御性钳制）。纯函数无副作用，供 run 循环每轮调用与单测直接驱动。
 */
static long capFeedback(long received, long outputFrontier) {
    return outputFrontier <= 0L ? received : Math.min(received, outputFrontier);
}
```

`run` 改造：现有 `run(RawMessageListener)` 方法体整体移入新重载 `run(RawMessageListener listener, LongSupplier outputFrontier)`，反馈三行替换：

```java
// 原：
// LogSequenceNumber last = stream.getLastReceiveLSN();
// stream.setAppliedLSN(last);
// stream.setFlushedLSN(last);
// 新：
long confirmed = capFeedback(stream.getLastReceiveLSN().asLong(), outputFrontier.getAsLong());
LogSequenceNumber last = LogSequenceNumber.valueOf(confirmed);
stream.setAppliedLSN(last);
stream.setFlushedLSN(last);
```

（`LOG.debug("LSN 反馈...")` 行同步打印 confirmed。）旧签名保留一行委托：

```java
/** 兼容重载：无输出前沿（不封顶，等价 1.6 行为）。javadoc 注明见带 LongSupplier 版。 */
public void run(RawMessageListener listener) throws SQLException, IOException {
    run(listener, () -> 0L);
}
```

run 主 javadoc 补一段：`outputFrontier` 语义（consumer 已输出事务的最大 endLsn，0=无 cap；status 包照常按反馈周期发送，前沿不前进只影响确认值不影响心跳——不会触发 wal_sender_timeout 断连）。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -Dtest=PgReplicationSessionTest`
Expected: PASS（2 用例）。

- [ ] **Step 5: 提交推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication/PgReplicationSession.java src/test/java/org/vastdata/vbstream/replication/PgReplicationSessionTest.java
git commit -m "feat(replication): 反馈位点按输出前沿封顶——run(listener, LongSupplier) 重载 + capFeedback 纯函数"
git push
```

---

### Task 5: 组装器存储层换血（纯 CQ 段记账，单线程形态）

> 本任务是原子换血：TxBuffer 字段、onRaw 追加序、回放读回、低水位、五个退役类删除必须一次落地（中途不可编译）。完成后形态 = "1.7a：纯 CQ 存储 + 同步内联回放"，全部测试绿；Task 6 再把回放移交接。

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/TxBuffer.java`（从 TransactionAssembler 私有内部类提升为包私有顶层类）
- Create: `src/main/java/org/vastdata/vbstream/replication/RawPeeks.java`（窥探辅助）
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`（大改）
- Modify: `src/main/java/org/vastdata/vbstream/replication/BucketReplayer.java`（大改）
- Delete: `MessageSpool.java`、`SpoolFrame.java`、`PayloadUnit.java`、`SpillConfig.java`（主代码）+ `MessageSpoolTest.java`、`SpoolFrameTest.java`、`SpillConfigTest.java`、`BenchSpillBridge.java`（测试）+ `SpillWatermarkProbe.java`（测试）
- Create: `src/test/java/org/vastdata/vbstream/replication/PipeWatermarkProbe.java`（顶替 SpillWatermarkProbe）
- Modify: `TransactionAssemblerTest.java`（夹具改造）、`BucketReplayerTest.java`（重写驱动方式）、IT `AssemblySpillTest.java`/`TransactionAssemblyTest.java`（编译适配）

**Interfaces:**
- Consumes: Task 1 `PipeConfig`、Task 2 `MessagePipe`、Task 3 `RelationSnapshot.require`（Task 6 才用快照，本任务回放仍直查 registry——单线程无竞争）。
- Produces（Task 6 消费）:
  - `TxBuffer`（包私有）：字段见 Step 3a
  - `TransactionAssembler` 构造器：`(TransactionListener, StreamingMode, VersionedRelationRegistry, PipeConfig)` + `(…, Consumer<PgOutputMessage> decodedObserver)`（后者本任务暂为单 Consumer；Task 6 改 BiConsumer 并加异步构造器）
  - `BucketReplayer`：构造 `(StreamingMode, Consumer<PgOutputMessage> decodedObserver)`；`List<TxChange> replay(TxBuffer bucket, MessagePipe pipe, RelationResolver resolver)`；嵌套 `@FunctionalInterface interface RelationResolver { PgOutputMessage.Relation require(int relationOid, long asOfSeq); }`
  - 包私有观测点：`long pipeWatermark()`（顶替 `spillWatermark()`，`PipeWatermarkProbe` 透出）
- 删除面：`vb.spill.*`、hybrid、`PayloadUnit`/`SpoolFrame`/`MessageSpool`/`SpillConfig` 全部引用点。

- [ ] **Step 1: 写 TxBuffer 与 RawPeeks（无行为变化，先落类型）**

```java
// TxBuffer.java
package org.vastdata.vbstream.replication;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * 组装桶（1.7 设计 §4.1）：纯 CQ index 段记账——桶内不持有任何 payload 字节，堆占用只有元数据
 * （段数 × long[2] + oid/aborted 集合）。数据消息字节只在 reader 追加时写一次 CQ、consumer 回放时读一次。
 *
 * <p>字段语义：firstIndex/lastIndex 是桶内数据单元的 CQ index 全局端点（firstIndex < 0 = 空桶）；
 * firstIndex 兼任 1.6 的 minSeq（seq ≡ CQ index，见组装器 javadoc），参与 registry 剪枝低水位。
 * segments 是 [first,last] 闭区间连续段列表（追加顺序）；连续段规则=上一次全局 append（含控制消息）
 * 是本桶数据消息才顺延，否则新开段——一段内全部是同桶数据单元（构造保证）。
 *
 * <p>hasPrefix 是桶级不变量：流式桶的单元恒在流块内收到（带 4 字节 xid 前缀）、普通/两阶段桶恒在
 * 块外（无前缀）；追加期校验，混现即 ISE fail-fast（协议不允许，防御）。回放据此决定 decodeSingle
 * 的 inStream 实参并重窥前缀值作 streamXid（子事务过滤用）。
 *
 * <p>线程约束：LIVE 期间仅 reader 线程触碰（单写者）。
 */
final class TxBuffer {

    final long xid;
    String gid;
    long firstIndex = -1L;
    long lastIndex = -1L;
    final ArrayDeque<long[]> segments = new ArrayDeque<>();
    /** 追加期窥出的 relation oid 集合（I/U/D 单 oid、T 多 oid、M 无）——交接快照圈定范围用。 */
    final Set<Integer> oidSet = new HashSet<>();
    boolean hasPrefix;
    boolean prefixKnown = false;
    final Set<Long> abortedSubxids = new HashSet<>();

    TxBuffer(long xid) {
        this.xid = xid;
    }
}
```

```java
// RawPeeks.java
package org.vastdata.vbstream.replication;

import java.nio.charset.StandardCharsets;

/**
 * raw 字节窥探辅助（从 1.6 TransactionAssembler 的私有静态方法整编为包私有工具，组装器路由与
 * 回放器 streamXid 重窥共用）。全部纯函数。符号位处理约定见 {@link #intAt} 的 javadoc
 * （Task 12 实测踩坑：无符号字节必须先 &amp;0xFF 再拼）。
 */
final class RawPeeks {

    private RawPeeks() {
    }

    /** big-endian 读 4 字节有符号整数（oid 等）。每字节先 &amp;0xFF 再移位拼接——byte 有符号，直接 | 会把符号位扩散到高位。 */
    static int intAt(byte[] raw, int offset) {
        return ((raw[offset] & 0xFF) << 24) | ((raw[offset + 1] & 0xFF) << 16)
                | ((raw[offset + 2] & 0xFF) << 8) | (raw[offset + 3] & 0xFF);
    }

    /** big-endian 读 4 字节无符号整数入 long（流式前缀 xid）。 */
    static long unsignedInt(byte[] raw, int offset) {
        return intAt(raw, offset) & 0xFFFFFFFFL;
    }

    /** big-endian 读 8 字节 long（LogicalMsg 的 lsn 窥探）。 */
    static long longAt(byte[] raw, int offset) {
        return (unsignedInt(raw, offset) << 32) | unsignedInt(raw, offset + 4);
    }

    /** 读 offset 起的 null 结尾 UTF-8 字符串（LogicalMsg prefix，仅异常/告警路径）。 */
    static String cstringAt(byte[] raw, int offset) {
        int end = offset;
        while (raw[end] != 0) {
            end++;
        }
        return new String(raw, offset, end - offset, StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: BucketReplayer 重写（新驱动契约）**

新全文骨架（保留原 javadoc 密度，按新契约改写）：

```java
final class BucketReplayer {

    /** Relation asOf 解析器：1.7 起回放与 registry 解耦——单线程形态传 registry::require，解耦形态传桶快照。 */
    @FunctionalInterface
    interface RelationResolver {
        PgOutputMessage.Relation require(int relationOid, long asOfSeq);
    }

    private final PgOutputDecoder decoder;
    private final Consumer<PgOutputMessage> decodedObserver;

    BucketReplayer(StreamingMode mode, Consumer<PgOutputMessage> decodedObserver) { ... }

    /**
     * 回放一个桶：逐段 readRange（readRange 保证段内全部是本桶单元），逐单元三步——
     * aborted 过滤（hasPrefix 时重窥 raw[1..4] 得 streamXid，命中 abortedSubxids 跳过）→
     * decodeSingle(payload, bucket.hasPrefix) → 构造 TxChange（Relation 经 resolver.require(oid, index)，
     * index 即单元 seq）。空桶产出空列表。
     */
    List<TxChange> replay(TxBuffer bucket, MessagePipe pipe, RelationResolver resolver) {
        List<TxChange> changes = new ArrayList<>();
        for (long[] segment : bucket.segments) {
            pipe.readRange(segment[0], segment[1], (index, payload) -> {
                OptionalLong streamXid = bucket.hasPrefix
                        ? OptionalLong.of(RawPeeks.unsignedInt(payload, 1))
                        : OptionalLong.empty();
                if (streamXid.isPresent() && bucket.abortedSubxids.contains(streamXid.getAsLong())) {
                    return;
                }
                changes.add(replayUnit(payload, index, streamXid, resolver));
            });
        }
        return changes;
    }

    private TxChange replayUnit(byte[] payload, long seq, OptionalLong streamXid, RelationResolver resolver) {
        // 原 replayUnit 的类型守卫 + decodeSingle(ByteBuffer.wrap(payload), streamXid.isPresent())
        // + decodedObserver.accept(msg) + instanceof 链原样迁移，仅 registry.require(...) 换 resolver.require(...)
    }
}
```

`BucketReplayerTest.java` 重写：原用例以 `PayloadUnit` 列表驱动——改为"append 进 MessagePipe + 构造 TxBuffer 段"驱动（`@TempDir` 建管道，`pipe.append` 记 index，`bucket.segments.addLast(new long[]{i0, i1})`，`replayer.replay(bucket, pipe, registry::require)`）。断言值**原样保留**（这些就是 1.6 回放语义基线）；删除 SpoolFrame 相关用例。测试类 javadoc 说明新夹具约定。

- [ ] **Step 3: TransactionAssembler 改造**

逐方法改动清单（原行号见当前文件）：

1. **字段**（:72-104）：删 `spill`/`spool`/`memoryBytes`/`lastSpillAppender`；新增 `private final MessagePipe pipe;`、`private long maxAppendedIndex = -1L;`（reader 记账，替代 `spool.lastAppendedIndex()`——空队列时 appender 未写过会抛）、`private TxBuffer lastAppendOwner;`（连续段判定，控制消息 append 后置 null）。构造器：`SpillConfig spill` 参数 → `PipeConfig pipeConfig`；构造体内 `this.pipe = new MessagePipe(pipeConfig.dir(), pipeConfig.rollCycle());`（急切建立——管道是地基，构造即 wipe 目录）；`this.replayer = new BucketReplayer(mode, decodedObserver);`。4 参便捷构造器同步换型。类 javadoc 的混合缓冲段整体替换为"纯段记账 + seq ≡ CQ index"表述（spec §2/§4.1），并加一句：**每条消息（含控制消息与 'R'）先 append 取 index 作 seq，再做记账路由**——数据单元与 'R' 版本天然同序。
2. **onRaw**（:211-235）：首行 `long seq = pipe.append(raw);`（替代 `nextSeq++`；`nextSeq` 字段删除）；`maxAppendedIndex = seq;`；控制消息分支末尾统一 `lastAppendOwner = null;`（switch 的控制/'R'/Y/O/default 各分支——用一个小 wrapper 或在 decode 后统一置空；'R' 分支 `registry.accept(seq, rel)` 保持）。
3. **routeData/routeLogicalMsg/appendUnit**（:268-303）：`appendUnit` 重写：

```java
/**
 * 数据消息入桶（1.7 设计 §4.1）：窥 streamXid（现状）+ 校验桶级 hasPrefix 不变量 + 窥 oid 入
 * oidSet + appendIndex 记段。字节本身已在 onRaw 首行 append 进管道（index 即 seq），此处不再写 CQ。
 */
private void appendUnit(TxBuffer bucket, byte[] raw, long seq) {
    boolean inStream = currentStream != null;
    if (!bucket.prefixKnown) {
        bucket.hasPrefix = inStream;
        bucket.prefixKnown = true;
    } else if (bucket.hasPrefix != inStream) {
        throw new IllegalStateException("桶内单元流式前缀混现: xid=" + bucket.xid
                + " hasPrefix=" + bucket.hasPrefix + " 当前 inStream=" + inStream);
    }
    collectOids(bucket, raw);
    appendIndex(bucket, seq);
}
```

```java
/**
 * 窥数据消息的 relation oid 记入桶的 oidSet（快照圈定用）：I/U/D 在类型字节（及可选 4 字节前缀）
 * 后取 Int32 relationOid；T 读 I32 表数 + 选项字节后的 oid 数组；M 无 oid 跳过。偏移与
 * describeData 同源（协议线格式见 spec 附录）。
 */
private void collectOids(TxBuffer bucket, byte[] raw) {
    int base = currentStream != null ? 5 : 1;
    switch (raw[0]) {
        case 'I', 'U', 'D' -> bucket.oidSet.add(RawPeeks.intAt(raw, base));
        case 'T' -> {
            int n = RawPeeks.intAt(raw, base);
            for (int i = 0; i < n; i++) {
                bucket.oidSet.add(RawPeeks.intAt(raw, base + 5 + 4 * i));
            }
        }
        default -> { /* 'M' 无 oid */ }
    }
}
```

```java
/**
 * 把数据单元的 CQ index 记入桶的连续段（原 appendSpillIndex 规则扩展）：上一次全局 append 的
 * owner 是本桶才顺延当前段，否则新开段 [index,index]——控制消息的 append 会把 owner 置 null，
 * 天然断段。firstIndex/lastIndex 维护全局端点。只在 reader 线程调用。
 */
private void appendIndex(TxBuffer bucket, long index) {
    if (lastAppendOwner == bucket && !bucket.segments.isEmpty()) {
        bucket.segments.peekLast()[1] = index;
    } else {
        bucket.segments.addLast(new long[]{index, index});
    }
    if (bucket.firstIndex < 0) {
        bucket.firstIndex = index;
    }
    bucket.lastIndex = index;
    lastAppendOwner = bucket;
}
```

4. **storeUnit/newBucket/spillAll/spool()**：整体删除（连同调用点）。
5. **提交三分支**（commit :432 / streamCommit :489 / commitPrepared :558）：`List<TxChange> changes = replay(bucket);` 的私有 `replay(TxBuffer)` 改为：

```java
/** 提交路径回放（本任务仍单线程内联；Task 6 移交 consumer）：逐段读回 + 按 registry asOf 渲染。 */
private List<TxChange> replay(TxBuffer bucket) {
    return replayer.replay(bucket, pipe, registry::require);
}
```

6. **retireBucket**（:687）：删 MEMORY 记账与 lastSpillAppender 清理，改为：

```java
/** 桶完结（提交/整桶丢弃）后统一收尾：CQ 删除低水位检查 + registry 剪枝（两个低水位作用域见 1.7 设计 §3.2）。 */
private void retireBucket(TxBuffer bucket) {
    if (lastAppendOwner == bucket) {
        lastAppendOwner = null;
    }
    releasePiped();
    pruneRegistryVersions();
}
```

7. **releaseSpooled → releasePiped**：`spool.releaseBelow(spillWatermark())` → `pipe.releaseBelow(pipeWatermark())`（无 null 守卫——pipe 恒存在）。
8. **spillWatermark → pipeWatermark**（:750）：去掉 spool null 哨兵与 Mode 判定：

```java
/**
 * CQ 删除低水位 = min(存活桶 firstIndex, maxAppendedIndex+1)。低于该 index 的条目不会再被回读。
 * 空桶（firstIndex<0）不参与取最小值；一个存活桶都没有时取 maxAppendedIndex+1（已落盘内容全是垃圾）。
 * 本任务（单线程）只有存活桶维度；Task 6 加入非 DONE 交接桶维度。包私有，仅供同包单测/探针。
 */
long pipeWatermark() {
    long lowest = maxAppendedIndex + 1;
    lowest = Math.min(lowest, floor(currentNormalTx));
    lowest = Math.min(lowest, floor(currentPrepareTx));
    for (TxBuffer bucket : streamedByXid.values()) {
        lowest = Math.min(lowest, floor(bucket));
    }
    for (TxBuffer bucket : preparedByGid.values()) {
        lowest = Math.min(lowest, floor(bucket));
    }
    return lowest;
}

/** 存活桶的 firstIndex 低水位候选；桶 null 或空桶返回 Long.MAX_VALUE。 */
private static long floor(TxBuffer bucket) {
    return (bucket == null || bucket.firstIndex < 0) ? Long.MAX_VALUE : bucket.firstIndex;
}
```

（`pruneRegistryVersions`（:722）与 `bucketFloor`：`bucket.minSeq` → `bucket.firstIndex`，其余不动。`storageOf`（:793）改打印段形态：`"PIPE[" + segments.size() + " segs " + firstIndex + ".." + lastIndex + "]"`。）
9. **close()**：`spool` 守卫删，`try { pipe.close(); } catch (RuntimeException e) { LOG.warn(...); }`。
10. **intAt/unsignedInt/longAt/cstringAt**（:805-827）：删除（改用 `RawPeeks.`，`describeData` 等调用点前缀化）。
11. **liveBuckets**：保留（Task 6 统计还用不到，先留给 watermarks…… 实际本轮只有 prune/watermark 各自内联遍历——liveBuckets 若无调用点则删除）。

- [ ] **Step 4: 删除退役类与旧探针，落新探针**

```bash
git rm src/main/java/org/vastdata/vbstream/replication/MessageSpool.java \
       src/main/java/org/vastdata/vbstream/replication/SpoolFrame.java \
       src/main/java/org/vastdata/vbstream/replication/PayloadUnit.java \
       src/main/java/org/vastdata/vbstream/replication/SpillConfig.java \
       src/test/java/org/vastdata/vbstream/replication/MessageSpoolTest.java \
       src/test/java/org/vastdata/vbstream/replication/SpoolFrameTest.java \
       src/test/java/org/vastdata/vbstream/replication/SpillConfigTest.java \
       src/test/java/org/vastdata/vbstream/replication/BenchSpillBridge.java \
       src/test/java/org/vastdata/vbstream/replication/SpillWatermarkProbe.java
```

`PipeWatermarkProbe.java`（照 SpillWatermarkProbe 原样改点名与 javadoc）：

```java
/**
 * 跨包测试桥：把 {@link TransactionAssembler} 的包私有 {@code pipeWatermark()}（CQ 删除低水位）
 * 以公开静态方法透出给 it 包集成测试。仅测试代码可用，不属于主代码契约。
 */
public final class PipeWatermarkProbe {
    private PipeWatermarkProbe() { }

    /** 责任：读取组装器当前 CQ 删除低水位（语义同 TransactionAssembler.pipeWatermark()）。纯读无副作用。 */
    public static long of(TransactionAssembler assembler) {
        return assembler.pipeWatermark();
    }
}
```

- [ ] **Step 5: TransactionAssemblerTest 夹具改造**

1. 常量与夹具：删 `NO_SPILL`；加 `@TempDir static Path PIPE_DIR;`（JUnit 支持静态 @TempDir，类级共享、用例间由 wipe-on-open 顺序清空）与 `private static PipeConfig pipeCfg() { return new PipeConfig(PIPE_DIR, LegacyRollCycles.MINUTELY); }`。
2. 两个 `run(...)` 夹具：构造器第 4 参 `NO_SPILL` → `pipeCfg()`；try-with-resources 语义不变（close 关管道）。47 处 `run(...)` 调用点**零改动**。
3. 文末"混合模式组"（双阈值等价用例，类 javadoc所说 Task 10 组）整体删除——单形态后无对照面（等价性验收由 Task 6 的同步/异步对照接管）；类 javadoc 的相应段落改写为"1.7 纯段记账：数据经 CQ 往返（append→readRange），等价基线=既有期望值不变"。
4. 涉及 `spillWatermark()`/`SpillWatermarkProbe` 的用例（如有）：断言改经 `assembler.pipeWatermark()`，语义按新低水位（无 -1 哨兵；未 append 过时 = 0）。
5. 期望值**全部不变**——这是 1.6 → 1.7a 的无损性验收本体。

- [ ] **Step 6: IT 最小编译适配**

- `it/TransactionAssemblyTest.java`：`SpillConfig` 引用 → `PipeConfig`（`new PipeConfig(tmpDir, MINUTELY)`，@TempDir 已有则复用，没有则加 `@TempDir Path pipeDir` 字段）；断言不变。
- `it/AssemblySpillTest.java`：同上换配置型；`SpillWatermarkProbe.of` → `PipeWatermarkProbe.of`；场景 ①（双阈值等价）删除（类 javadoc 标注"1.7 起单形态，等价性移至同步/异步对照"），场景 ②③④（交错/DDL asOf/回滚删档）断言原样、驱动方式不变（录制 raw 回放喂组装器）。类名暂不改（Task 8 一并处理）。

- [ ] **Step 7: 全量验证**

```bash
mvn clean test
```
Expected: 全绿（151 ± 删除/新增用例数；`mvn test` 单命令含 Docker IT——Docker 不可用时先 `mvn clean test -Dtest='org.vastdata.vbstream.replication.*'` 跑单测层，再补跑 IT）。同时确认 `mvn clean test-compile` 无残留引用（BenchSpillBridge 删除后 `src/jmh` 的 SpillPathBenchmark/AssembleMemoryBenchmark 会失编译——默认构建不含它们，属预期，Task 9 收敛）。

- [ ] **Step 8: 提交推送**

```bash
git add -A
git commit -m "feat(replication)!: 存储层换血——纯 CQ index 段记账（seq≡index），删除 hybrid/PayloadUnit/SpoolFrame/MessageSpool/SpillConfig"
git push
```

---

### Task 6: 桶状态机 + TransactionConsumer + 交接（解耦本体）

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/TxBuffer.java`（状态机字段 + 冻结字段）
- Create: `src/main/java/org/vastdata/vbstream/replication/BucketState.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/TransactionConsumer.java`
- Modify: `TransactionAssembler.java`（handoff/构造器/close/水位/统计）、`BucketReplayer.java`（observer 换 BiConsumer）
- Test: `TransactionAssemblerTest.java`（异步组补充）、新建 `DecoupledEquivalenceTest.java`

**Interfaces:**
- Consumes: Task 3 `RelationSnapshot`、Task 5 `TxBuffer`/`MessagePipe`/`BucketReplayer.RelationResolver`。
- Produces:
  - `enum BucketState { LIVE, HANDED_OFF, OUTPUTTING, DONE }`（包私有）
  - `TransactionConsumer implements Runnable`：构造 `(TransactionListener, StreamingMode, MessagePipe, BlockingQueue<TxBuffer>, AtomicLong outputFrontier, AtomicInteger liveBucketCount, Runnable onFailure, BiConsumer<PgOutputMessage, RelationLookup> replayObserver)`；`void run()`（毒丸循环 + 周期统计）；`void processBucket(TxBuffer)`（单桶同步处理——同步测试与异步循环共用）
  - `TransactionAssembler` 异步构造器：`(TransactionListener, StreamingMode, VersionedRelationRegistry, PipeConfig, BiConsumer<PgOutputMessage, RelationLookup> decodedObserver, AtomicLong outputFrontier, Runnable onFailure)`（public，Main 用）；既有包私有构造器保持同步模式（observer 参型同步换 BiConsumer）
  - `static final TxBuffer POISON`（在 TxBuffer 上）
  - 包私有测试面：`java.util.List<TxBuffer> handedOffForTest()`（`List.copyOf(handedOff)` 快照——状态机保护用例断言 firstIndex 用，先例同 `pipeWatermark()`）

- [ ] **Step 1: TxBuffer 状态机化**

新增字段（冻结语义 javadoc 按 spec §3.1 写全）：

```java
/** 桶生命周期状态（1.7 设计 §3.1）。写侧归属：reader 写到 HANDED_OFF（交接即冻结），consumer 写后两态。唯一跨线程可变字段。 */
volatile BucketState state = BucketState.LIVE;
/** 交接时捕获的封箱元数据（来自提交控制消息 live 解码）。冻结字段。 */
TransactionKind kind;
long commitLsn;
long endLsn;
java.time.Instant commitTimestamp;
/** 交接时从 registry 拷出的版本快照（oidSet 圈定，截止 lastIndex）。冻结字段；空桶为空快照。 */
RelationSnapshot relationSnapshot;
/** 交接时刻（nanoTime）——consumer 统计最老滞留用。冻结字段。 */
long handoffNanos;

/** 毒丸哨兵：consumer 循环见到即退出（close 排干协议）。 */
static final TxBuffer POISON = new TxBuffer(-1L);
```

`BucketState` 枚举（四种，各自 javadoc 一行：LIVE=reader 记账中（含 2PC 挂起）；HANDED_OFF=已交接待回放；OUTPUTTING=consumer 回放中；DONE=输出完成，可清理）。

- [ ] **Step 2: 写失败测试（解耦等价 + 状态机保护）**

新建 `DecoupledEquivalenceTest.java`：

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.protocol.StreamingMode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 解耦等价性验收（1.7 设计 §9.1）：同一字节流分别过同步消费（单线程直调 processBucket——既有
 * 33+ 用例的驱动形态，锚定 1.6 期望）与真实双线程管道（异步构造器 + close 排干），断言
 * Transaction 序列完全相等。
 */
class DecoupledEquivalenceTest {

    @TempDir
    Path dir;

    /** 生成一段多形态字节流：普通事务 + 流式交错 + 2PC + 子事务回滚（PgWire 构造，与 TransactionAssemblerTest 同风格）。 */
    private static byte[][] mixedStream() {
        return new byte[][] {
                PgWire.relation(16384, "public", "t", "id", "v"),
                PgWire.begin(101),
                PgWire.insert(16384, PgWire.tuple("1", "a")),
                PgWire.commit(1, 2),
                PgWire.streamStart(7001, true),
                PgWire.streamed(7003, PgWire.insert(16384, PgWire.tuple("2", "b"))),
                PgWire.streamStop(),
                PgWire.streamAbort(7001, 7003),      // 子事务回滚：单元应在回放期被剔除
                PgWire.streamCommit(7001, 3, 4),
        };
    }

    @Test
    void asyncPipelineEqualsSynchronous() throws Exception {
        List<Transaction> syncOut = new ArrayList<>();
        try (TransactionAssembler sync = new TransactionAssembler(
                syncOut::add, StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg())) {
            for (byte[] m : mixedStream()) {
                sync.onRaw(m);
            }
        }
        List<Transaction> asyncOut = new ArrayList<>();
        AtomicLong frontier = new AtomicLong();
        try (TransactionAssembler async = new TransactionAssembler(asyncOut::add, StreamingMode.ON,
                new VersionedRelationRegistry(), pipeCfg(),
                (msg, view) -> { }, frontier, () -> { })) {
            for (byte[] m : mixedStream()) {
                async.onRaw(m);
            }
        }   // close：毒丸 → consumer 排干余桶 → join → pipe 关闭——排干后输出确定
        assertEquals(syncOut, asyncOut);
        assertEquals(asyncOut.get(asyncOut.size() - 1).endLsn(), frontier.get());   // 前沿 = 末个输出事务 endLsn
    }

    private PipeConfig pipeCfg() {
        return new PipeConfig(dir, net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY);
    }
}
```

（`PgWire.begin/commit/streamStart/streamCommit/streamAbort` 的参数形态以 `src/test/java/org/vastdata/vbstream/replication/PgWire.java` 实际签名为准对齐；streamAbort 需 StreamingMode.ON 的非 parallel 形态。）

TransactionAssemblerTest 补一个状态机保护用例（放类尾；spec §9.2"HANDED_OFF 桶对 releaseBelow 的保护"的落点）：

```java
/** 1.7：在途交接桶约束 CQ 删除低水位。构造：异步组装器 + 阻塞 listener 定格第一个桶在 OUTPUTTING；随后交接第二个事务，断言 pipeWatermark() 不越过被阻塞桶的 firstIndex（两个桶都非 DONE，低水位被钉住）；放行后排干、close 退出，两事务均已输出。 */
@Test
void handedOffBucketConstrainsPipeWatermark() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch inCallback = new CountDownLatch(1);
    AtomicLong frontier = new AtomicLong();
    TransactionAssembler assembler = new TransactionAssembler(t -> {
        inCallback.countDown();
        try {
            release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }, StreamingMode.ON, new VersionedRelationRegistry(), pipeCfg(),
            (msg, view) -> { }, frontier, () -> { });
    try {
        assembler.onRaw(relation());
        assembler.onRaw(PgWire.begin(101));
        assembler.onRaw(insert("1", "a"));
        assembler.onRaw(commitTx(1, 2));        // 第一个桶交接，consumer 进入回调并阻塞
        assertTrue(inCallback.await(5, TimeUnit.SECONDS));
        long blockedFirst = assembler.handedOffForTest().get(0).firstIndex;
        assembler.onRaw(PgWire.begin(102));
        assembler.onRaw(insert("2", "b"));
        assembler.onRaw(commitTx(3, 4));        // 第二个桶交接（排队，同样非 DONE）
        assertTrue(assembler.pipeWatermark() <= blockedFirst,
                "在途桶应钉住删除低水位: wm=" + assembler.pipeWatermark() + " blockedFirst=" + blockedFirst);
    } finally {
        release.countDown();
        assembler.close();                      // 排干并退出（try/finally 而非 try-with-resources：latch 要先放行）
    }
}
```

（`commitTx` 为测试类内既有提交字节辅助或新增（`PgWire.commit` 包装，参数形态以 PgWire 实际签名为准）；`handedOffForTest()` 是 Task 6 Step 5 新增的包私有测试面。）

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn test -Dtest='DecoupledEquivalenceTest,TransactionAssemblerTest'`
Expected: 编译失败（异步构造器/状态机不存在）。

- [ ] **Step 4: 实现 TransactionConsumer**

```java
package org.vastdata.vbstream.replication;

// imports: protocol 消息/StreamingMode、slf4j、java.util.concurrent.*、java.util.function.*

/**
 * 事务消费器（1.7 设计 §4.4）：从交接队列取冻结桶，回放渲染成 {@link Transaction} 回调输出，
 * 上报 LSN 前沿。它从组装器抽出单独成类，是为了既有单测能以"同线程消费"驱动（直接调
 * {@link #processBucket}，锚定 1.6 期望）与真实线程形态共用同一段处理逻辑。
 *
 * <p>循环协议：{@link #queue}.poll(1s)——null（暂时无交接）做周期统计后继续；取到
 * {@link TxBuffer#POISON} 退出；否则 processBucket。失败语义：处理中抛出的任何 Throwable 记
 * ERROR、触发 onFailure、退出循环**不排干**（fail-fast，与 1.6"异常上抛终止会话"等价）；
 * 捕捉 Throwable 防 consumer 静默死亡导致 reader 无限追加。
 *
 * <p>线程约束：run() 由 consumer 线程执行；processBucket 的触碰面 = 冻结桶 + pipe.readRange +
 * listener 回调 + 前沿累加 + 桶状态字段——全部在 consumer 线程或并发安全结构上。
 */
final class TransactionConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionConsumer.class);
    /** 统计/告警周期与最老滞留告警阈值（1.7 设计 §7；常量不做配置面）。 */
    private static final long STATS_INTERVAL_NANOS = 10_000_000_000L;
    private static final long STALE_WARN_NANOS = 60_000_000_000L;

    private final TransactionListener listener;
    private final BucketReplayer replayer;
    private final MessagePipe pipe;
    private final BlockingQueue<TxBuffer> queue;
    private final AtomicLong outputFrontier;
    private final AtomicInteger liveBucketCount;
    private final Runnable onFailure;

    TransactionConsumer(TransactionListener listener, StreamingMode mode, MessagePipe pipe,
            BlockingQueue<TxBuffer> queue, AtomicLong outputFrontier,
            AtomicInteger liveBucketCount, Runnable onFailure,
            BiConsumer<PgOutputMessage, RelationLookup> replayObserver) { ... }

    @Override
    public void run() {
        long lastStats = System.nanoTime();
        boolean outputting = false;
        while (true) {
            TxBuffer bucket;
            try {
                bucket = queue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("consumer 被中断，退出（排干未完成——由 close 协议负责，此路径仅防御）");
                return;
            }
            lastStats = maybeStats(lastStats, outputting);
            if (bucket == null) {
                continue;
            }
            if (bucket == TxBuffer.POISON) {
                return;
            }
            outputting = true;
            try {
                processBucket(bucket);
            } catch (Throwable t) {
                LOG.error("事务回放失败，consumer 终止（fail-fast）: xid={} firstIndex={}",
                        bucket.xid, bucket.firstIndex, t);
                onFailure.run();
                return;
            }
            outputting = false;
        }
    }

    /**
     * 责任：处理一个冻结桶（同步/异步共用）。关键步骤：state=OUTPUTTING → 回放（快照 resolver +
     * 快照渲染视图）→ 封箱 Transaction 回调 listener → 前沿以 endLsn 单调累加 → state=DONE。
     * 边界：空桶产出空 changes；回放异常原样上抛（异步由 run 捕获，同步直传调用方——既有用例的
     * fail-fast 断言路径）。线程：consumer 线程或同步测试线程。
     */
    void processBucket(TxBuffer bucket) {
        bucket.state = BucketState.OUTPUTTING;
        List<TxChange> changes = replayer.replay(bucket, pipe, bucket.relationSnapshot::require);
        listener.onTransaction(new Transaction(bucket.xid, bucket.kind, bucket.gid,
                bucket.commitLsn, bucket.endLsn, bucket.commitTimestamp, changes));
        outputFrontier.accumulateAndGet(bucket.endLsn, Math::max);
        bucket.state = BucketState.DONE;
    }

    /** 周期统计（10s）：LIVE/HANDED_OFF/OUTPUTTING 计数 + 最老交接桶滞留时长；滞留 >60s 升 WARN（队列 peek 取 handoffNanos）。 */
    private long maybeStats(long lastStats, boolean outputting) { ... }
}
```

- [ ] **Step 5: TransactionAssembler 接线**

1. 新字段：`private final BlockingQueue<TxBuffer> handoffQueue = new LinkedBlockingQueue<>();`、`private final ArrayDeque<TxBuffer> handedOff = new ArrayDeque<>();`（reader 私有，DONE 惰性清理）、`private final TransactionConsumer consumer;`、`private final Thread consumerThread;`（异步形态非 null）、`private final AtomicLong outputFrontier;`、`private final AtomicInteger liveCount = new AtomicInteger();`。
2. **异步构造器**（public，7 参，observer 为 `BiConsumer<PgOutputMessage, RelationLookup>`）：建 consumer + 线程 `transaction-consumer`（非守护）并 start；`outputFrontier` 直存。**同步构造器**（包私有，原参型 observer 换 BiConsumer）：consumer 建但不开线程，handoff 直调 `consumer.processBucket(bucket)`（frontier 用内部 AtomicLong）。
3. **observer 分流**：`decode()`（live，reader 线程）回调 `decodedObserver.accept(msg, registry)`；`BucketReplayer` 构造改收 BiConsumer，`replayUnit` 内回调 `decodedObserver.accept(msg, bucket.relationSnapshot)`——但 Task 5 的 replay 签名只有 resolver……统一改：`replayer.replay(bucket, pipe)` 内部用 `bucket.relationSnapshot::require` 作 resolver、`bucket.relationSnapshot` 作渲染视图（快照进桶后单双形态同路）；组装器内联 `replay(TxBuffer)` 私有方法删除，提交分支直接走 handoff。
4. **提交三分支改 handoff**（commit/streamCommit/commitPrepared）：

```java
/**
 * 交接（1.7 设计 §4.4）：拷快照（oidSet 圈定，截止 lastIndex）→ 捕获封箱元数据 → state=HANDED_OFF
 * → 入 handedOff 记账 → 入队（同步模式直调 processBucket）→ 维护低水位。立即返回——reader 路径
 * 从此不含回放。只在 reader 线程调用。
 */
private void handoff(TxBuffer bucket, TransactionKind kind, long commitLsn, long endLsn,
        Instant commitTimestamp) {
    bucket.kind = kind;
    bucket.commitLsn = commitLsn;
    bucket.endLsn = endLsn;
    bucket.commitTimestamp = commitTimestamp;
    bucket.relationSnapshot = registry.snapshot(bucket.oidSet, bucket.lastIndex);
    bucket.state = BucketState.HANDED_OFF;
    bucket.handoffNanos = System.nanoTime();
    handedOff.add(bucket);
    liveCount.decrementAndGet();
    if (consumerThread == null) {
        consumer.processBucket(bucket);        // 同步消费（测试锚定路径）
    } else {
        handoffQueue.add(bucket);
    }
    maintainWatermarks();
}
```

（三个提交方法的调用点相应改为 `handoff(bucket, TransactionKind.NORMAL, m.commitLsn(), m.endLsn(), m.commitTimestamp())` 等；`newBucket` 里 `liveCount.incrementAndGet()`；另加包私有 `List<TxBuffer> handedOffForTest() { return List.copyOf(handedOff); }`——javadoc 注明测试面。）
5. **maintainWatermarks**（替代 retireBucket 的收尾位）：`handedOff.removeIf(b -> b.state == BucketState.DONE);` → `pipe.releaseBelow(pipeWatermark())` → `pruneRegistryVersions()`。调用点：handoff、streamAbort 整桶丢弃、rollbackPrepared 丢弃。
6. **pipeWatermark 扩非 DONE 维度**：

```java
long pipeWatermark() {
    long lowest = maxAppendedIndex + 1;
    // …原存活桶四路取 min…
    for (TxBuffer bucket : handedOff) {
        if (bucket.state != BucketState.DONE && bucket.firstIndex >= 0) {
            lowest = Math.min(lowest, bucket.firstIndex);
        }
    }
    return lowest;
}
```

（`pruneRegistryVersions` **不含** handedOff——快照自足，spec §3.2。）
7. **close()**：`handoffQueue.add(TxBuffer.POISON);` → `consumerThread.join();`（带超时 60s，超时 WARN——防 consumer 卡死拖住停机；同步形态跳过）→ `pipe.close()` 兜异常 WARN。javadoc 写排干语义（已提交未输出的事务不丢）。

- [ ] **Step 6: 跑测试确认通过**

Run: `mvn test -Dtest='DecoupledEquivalenceTest,TransactionAssemblerTest,BucketReplayerTest'`
Expected: PASS——33+ 既有用例经同步路径期望值不变；等价用例双形态输出全等。

- [ ] **Step 7: 全量 + 提交推送**

```bash
mvn clean test
git add -A
git commit -m "feat(replication): 桶状态机 + TransactionConsumer——提交期交接冻结桶（快照随行），回放输出移出 reader 路径"
git push
```

---

### Task 7: Main 装配切换

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/Main.java`
- Modify: `src/main/java/org/vastdata/vbstream/ConsoleListener.java`（javadoc 双线程注记，零代码改动）

**Interfaces:**
- Consumes: Task 6 异步构造器、Task 4 `run(listener, LongSupplier)`、Task 1 `PipeConfig`。

- [ ] **Step 1: 改 Main**

reader 线程 lambda 内（原 :54-63）：

```java
AtomicLong outputFrontier = new AtomicLong();   // 移到 try(session) 之前
...
Thread worker = new Thread(() -> {
    try (TransactionAssembler assembler = new TransactionAssembler(console, config.streamingMode(),
            registry, pipe, (msg, view) -> console.onMessage(msg, view),
            outputFrontier, stop::countDown)) {
        session.run(assembler, outputFrontier::get);
    } catch (Exception e) {
        LOG.error("复制流中断: {}（槽 {} 已保留，重启续传）", e.toString(), config.slotName(), e);
        stop.countDown();
    }
}, "pgoutput-reader");
```

（`SpillConfig spill = SpillConfig.fromSystemProperties()` → `PipeConfig pipe = PipeConfig.fromSystemProperties()`，日志行改 `pipe 配置: dir={} rollCycle={}`；Main 类/方法 javadoc 按 1.7 形态改写——"reader 记账 + CQ 管道 + consumer 回放输出、反馈按输出前沿封顶、consumer 失败 countDown 停机"。）

- [ ] **Step 2: ConsoleListener javadoc 注记**

`onMessage` javadoc 补：调用线程自 1.7 起为**两处**——reader 线程（控制消息/'R' live 解码，RelationLookup=版本日志）与 consumer 线程（回放解码，RelationLookup=桶快照）；本类无状态且 slf4j 线程安全。`onTransaction` javadoc：调用线程改为 consumer 线程。

- [ ] **Step 3: 编译 + 冒烟（Docker 在位时）+ 提交**

```bash
mvn clean test-compile
# 可选冒烟：src/docker 起 PG 后按根 CLAUDE.md "运行 Main" 段（-Dvb.pipe.* 替代 -Dvb.spill.*）观察 TXN 块输出
git add -A && git commit -m "feat(main): 装配切换——异步组装器 + 输出前沿反馈 + pipe 配置面" && git push
```

---

### Task 8: 集成测试升级（解耦验证 + frontier cap + 管道场景）

**Files:**
- Rename+Modify: `src/test/java/org/vastdata/vbstream/it/AssemblySpillTest.java` → `DecoupledPipelineTest.java`
- Create: `src/test/java/org/vastdata/vbstream/it/ReaderUnblockedTest.java`
- Create: `src/test/java/org/vastdata/vbstream/it/FrontierCapTest.java`

**Interfaces:**
- Consumes: Task 6 异步构造器（close 排干 = 确定性断言入口：喂完/录完后 close，输出即终态）、Task 4 反馈重载。

- [ ] **Step 1: ReaderUnblockedTest（头名测试：读不被输出阻塞）**

夹具习语照 AssemblySpillTest（独立槽名 + `@BeforeEach` 清残留 + `@AfterEach` drop；建表进 publication 的幂等写法照 TruncateTest）：

```java
package org.vastdata.vbstream.it;

// imports：replication 主类、junit、j.u.c、java.nio.file.Path、LegacyRollCycles

/**
 * 1.7 头名验收（设计 §9.3）：consumer 阻塞在输出回调期间，reader 持续从复制流接收消息。
 * 构造：异步组装器 + onTransaction 内 await latch 的阻塞 listener；reader 线程的 raw 回调先计数再喂组装器；
 * 阻塞窗口内继续写入并断言接收计数增长；放行后 close 排干，输出事务数 == 提交数。
 * 需要本机 Docker（PgTestEnv 单例容器）。
 */
class ReaderUnblockedTest {

    private static final String SLOT = "reader_unblocked";

    @BeforeEach
    void cleanResidualSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    @AfterEach
    void dropSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    @Test
    void readerContinuesWhileConsumerBlocked() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstOutput = new CountDownLatch(1);
        AtomicLong frontier = new AtomicLong();
        AtomicLong received = new AtomicLong();
        List<Transaction> out = new CopyOnWriteArrayList<>();
        ReplicationConfig config = PgTestEnv.newConfig(SLOT, "vb_pub");
        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            PgTestEnv.execSql("CREATE TABLE IF NOT EXISTS t_unblock(id int, v text)");
            PgTestEnv.execSql("ALTER PUBLICATION vb_pub ADD TABLE t_unblock");   // 已存在抛错时照 TruncateTest 习语捕获忽略
            try (TransactionAssembler assembler = new TransactionAssembler(t -> {
                out.add(t);
                firstOutput.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, config.streamingMode(), new VersionedRelationRegistry(),
                    new PipeConfig(Path.of("target/reader-unblocked-pipe"), LegacyRollCycles.MINUTELY),
                    (msg, view) -> { }, frontier, () -> { })) {
                Thread reader = new Thread(() -> {
                    try {
                        session.run(raw -> {
                            received.incrementAndGet();
                            assembler.onRaw(raw);
                        }, frontier::get);
                    } catch (Exception e) {
                        // close 触发的断连走这里，属预期
                    }
                }, "pgoutput-reader");
                reader.setDaemon(true);
                reader.start();
                for (int i = 0; i < 5; i++) {
                    PgTestEnv.execSql("BEGIN; INSERT INTO t_unblock VALUES (" + i + ", 'x'); COMMIT;");
                }
                assertTrue(firstOutput.await(10, TimeUnit.SECONDS));   // consumer 已阻塞在第一个事务回调里
                long atBlock = received.get();
                for (int i = 5; i < 10; i++) {                          // 阻塞期间继续写 5 个事务
                    PgTestEnv.execSql("BEGIN; INSERT INTO t_unblock VALUES (" + i + ", 'y'); COMMIT;");
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (received.get() <= atBlock && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                assertTrue(received.get() > atBlock,
                        "consumer 阻塞期间 reader 未继续接收: received=" + received.get());
                release.countDown();
            }   // close：毒丸排干
            assertEquals(10, out.size());       // 排干后全部事务输出，一个不少
        }
    }
}
```

- [ ] **Step 2: FrontierCapTest（confirmed_flush 封顶两段式）**

LSN 锚点策略（避免硬编码 T.endLsn）：写 T 前后各取 `pg_current_wal_insert_lsn()` 为 `before`/`after`——T 是唯一活动，`before < T.endLsn ≤ after` 恒成立，故"阻塞期 confirmed ≤ before"严格强于"confirmed < T.endLsn"，"放行后 confirmed > before"即证封顶解除。SQL 断言助手照 `NormalTransactionTest.feedbackIsAdoptedByServerAndConfirmedFlushAdvances` 的写法（`newSqlConnection` 查 `pg_replication_slots.confirmed_flush_lsn`，LSN 十六进制字符串经 `LogSequenceNumber.valueOf(String)` 解析比较）：

```java
package org.vastdata.vbstream.it;

// imports 同 ReaderUnblockedTest + org.postgresql.replication.LogSequenceNumber

/**
 * 1.7 反馈语义验收（设计 §9.3/§5）：未输出事务不推进槽 confirmed_flush_lsn，输出后推进。
 * 两段式：consumer 阻塞期提交 T（唯一 WAL 活动），等 ≥ 2 个反馈周期（PgTestEnv 反馈 2s → sleep 5s），
 * 断言 confirmed ≤ before（封顶钉在 T 之前）；放行排干后再等，断言 confirmed > before（前沿解封）。
 */
class FrontierCapTest {

    private static final String SLOT = "frontier_cap";

    @BeforeEach
    void cleanResidualSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    @AfterEach
    void dropSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    @Test
    void unflushedTransactionHoldsConfirmedFlush() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstOutput = new CountDownLatch(1);
        AtomicLong frontier = new AtomicLong();
        ReplicationConfig config = PgTestEnv.newConfig(SLOT, "vb_pub");
        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            PgTestEnv.execSql("CREATE TABLE IF NOT EXISTS t_cap(id int)");
            PgTestEnv.execSql("ALTER PUBLICATION vb_pub ADD TABLE t_cap");
            try (TransactionAssembler assembler = new TransactionAssembler(t -> {
                firstOutput.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, config.streamingMode(), new VersionedRelationRegistry(),
                    new PipeConfig(Path.of("target/frontier-cap-pipe"), LegacyRollCycles.MINUTELY),
                    (msg, view) -> { }, frontier, () -> { })) {
                Thread reader = new Thread(() -> {
                    try {
                        session.run(assembler, frontier::get);
                    } catch (Exception e) {
                        // close 触发的断连走这里，属预期
                    }
                }, "pgoutput-reader");
                reader.setDaemon(true);
                reader.start();
                long before = lsnOf("SELECT pg_current_wal_insert_lsn()");
                PgTestEnv.execSql("BEGIN; INSERT INTO t_cap VALUES (1); COMMIT;");
                long after = lsnOf("SELECT pg_current_wal_insert_lsn()");
                assertTrue(firstOutput.await(10, TimeUnit.SECONDS));   // consumer 阻塞在 T 的回调里
                Thread.sleep(5_000);                                   // ≥ 2 个反馈周期
                long confirmedBlocked = confirmedFlushLsn();
                assertTrue(confirmedBlocked <= before,
                        "未输出事务应钉住 confirmed_flush: confirmed=" + Long.toHexString(confirmedBlocked)
                                + " before=" + Long.toHexString(before));
                release.countDown();
                Thread.sleep(5_000);
                long confirmedReleased = confirmedFlushLsn();
                assertTrue(confirmedReleased > before,
                        "输出后 confirmed_flush 应越过封顶: confirmed=" + Long.toHexString(confirmedReleased));
                assertTrue(after >= before);                           // 自洽护栏
            }
        }
    }

    /** 查询单值 LSN 函数并解析为 long（十六进制 "X/Y" 形态经 LogSequenceNumber.valueOf）。 */
    private static long lsnOf(String sql) throws Exception { /* PgTestEnv.newSqlConnection + Statement 查询 + 解析 */ }

    /** 当前槽的 confirmed_flush_lsn（0x0 解析为 0）。 */
    private static long confirmedFlushLsn() throws Exception {
        return lsnOf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='" + SLOT + "'");
    }
}
```

（`lsnOf`/`confirmedFlushLsn` 的 JDBC 细节照 NormalTransactionTest 同名逻辑实现——它已有 LSN 字符串解析先例；SQL 函数与列查询统一走该助手。）

- [ ] **Step 3: DecoupledPipelineTest（原 AssemblySpillTest 场景 ②③④ 改异步管道）**

`git mv` 后：类名/文件名改 `DecoupledPipelineTest`；三场景驱动从"同步组装器回放录制流"改为"异步组装器（真实双线程）+ close 排干后断言"——录制仍是 SessionHarness raw 双轨；断言原样（交错多桶输出序 / 事务内 DDL asOf 前后段渲染 / StreamAbort 剔除；场景 ④ 删档断言改经 `PipeWatermarkProbe`+目录文件数，时点在 close 之后）。类 javadoc 重写。

- [ ] **Step 4: 跑 IT + 提交**

```bash
mvn clean test -Dtest='ReaderUnblockedTest,FrontierCapTest,DecoupledPipelineTest'
mvn clean test   # 全量
git add -A && git commit -m "test(it): 解耦头名验收/前沿封顶两段式/管道三场景（原 AssemblySpillTest 升级）" && git push
```

---

### Task 9: JMH 桥与基准 + 文档同步

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/replication/BenchPipeBridge.java`
- Modify: `src/jmh/java/org/vastdata/vbstream/bench/SpillPathBenchmark.java`（改名 `PipePathBenchmark.java`）、`AssembleMemoryBenchmark.java`、`RoutePeekBenchmark.java`
- Modify: `docs/benchmarks-baseline.md`、`CLAUDE.md`（根）、`src/main/java/org/vastdata/vbstream/replication/CLAUDE.md`、`src/test/java/org/vastdata/vbstream/it/CLAUDE.md`、`README.md`

**Interfaces:**
- Consumes: Task 2/5/6 的 MessagePipe/BucketReplayer/TxBuffer/RelationSnapshot。

- [ ] **Step 1: BenchPipeBridge**（照被删 BenchSpillBridge 的角色重写）：`dump(List<byte[]> rawMsgs, VersionedRelationRegistry registry)` → 返回句柄（持 pipe + 桶区间 + replayer），`replay()` 逐段 readRange + 快照渲染（`RelationSnapshot` 经 `registry.snapshot(oidSet, lastIndex)` 预构），`append(byte[])` 供吞吐口径。javadoc 注明与 consumer.processBucket 同构。
- [ ] **Step 2: 基准适配**：`SpillPathBenchmark` → `PipePathBenchmark`（两口径：pipe.append 吞吐、readRange+replay 回放）；`AssembleMemoryBenchmark` 的 SpillConfig 引用换 PipeConfig、溢写相关状态删除；`RoutePeekBenchmark` 补 oid 窥探口径（collectOids 同构 peek）。跑通验证：

```bash
mvn -Pjmh clean test-compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" org.openjdk.jmh.Main "org.vastdata.vbstream.bench" -jvmArgsAppend "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED" ... # 完整 --add-opens 清单见 baseline 文档
```

- [ ] **Step 3: baseline 文档补 1.7 段**：新口径数字入档（与 1.6 append 口径对照，注明"reader 路径成本类同 1.6 SPILLED append；回放口径为新增"）。
- [ ] **Step 4: CLAUDE.md ×3 + README**：根 CLAUDE.md 的架构图/里程碑段/`vb.spill.*` 表 → `vb.pipe.*` 与双线程形态；replication/CLAUDE.md 重写组装器/管道/消费器节（状态机、两个低水位、seq≡index、hasPrefix 不变量、线程约束红线）；it/CLAUDE.md 测试清单更新（新两类 + DecoupledPipelineTest）；README 示例与参数表同步。
- [ ] **Step 5: spec 核对**：对照 `2026-08-29-reader-consumer-decoupling-design.md` 逐节确认实现面覆盖（本计划已含两处计划期补强：桶级 hasPrefix 不变量、RelationLookup 渲染视图——spec 已同步修订则核对无出入）。
- [ ] **Step 6: 全量 + 提交推送**

```bash
mvn clean test
git add -A && git commit -m "docs+bench: 1.7 收尾——JMH 管道口径/基线文档/CLAUDE.md×3/README 同步" && git push
```

---

## 任务依赖

- Task 1/2/3/4 相互独立（可并行）。
- Task 5 依赖 1+2+3；Task 6 依赖 5；Task 7 依赖 6+4；Task 8 依赖 7；Task 9 收尾依赖 8。
