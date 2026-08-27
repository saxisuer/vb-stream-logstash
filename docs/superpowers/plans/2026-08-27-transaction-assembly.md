# 事务组装与原子输出 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在逐消息解码之上叠加 `TransactionAssembler` 状态机：把同事务变更缓冲组装为不可变 `Transaction`，收到提交信号（Commit/StreamCommit/CommitPrepared）后整体输出。

**Architecture:** 纯内存状态机（无 IO、无线程，run 循环线程内被调用）。三类桶：普通事务单指针（Commit 消息无 xid，协议保证 Begin..Commit 串行）、流式多桶 `Map<topXid, TxBuffer>` + `currentStream` 流块指针（spec §4.2 已验证多活动流式事务交错）、2PC 挂起池 `Map<gid, TxBuffer>`。spec：`docs/superpowers/specs/2026-08-27-transaction-assembly-design.md`（含附录 B 源码依据）。

**Tech Stack:** Java 17（**禁用** record pattern switch——Java 21 特性，用 instanceof 链）、JUnit 6、Testcontainers（postgres:18，`logical_decoding_work_mem=64kB`）、slf4j/logback。

**项目规约（CLAUDE.md，必须遵守）：**
- 日志一律 slf4j（`private static final Logger LOG = LoggerFactory.getLogger(Xxx.class)`），禁止 System.out/err；消息用 `{}` 占位符；CDC 数据走专用 logger `org.vastdata.vbstream.cdc`
- **每个函数（含私有方法、测试辅助）必须有 javadoc 逻辑描述**：职责、关键步骤、边界与异常语义、线程约束
- 每任务完成 commit + push（跨机开发）

**对 spec 的两处实现细化（执行时同步回写 spec）：**
1. `Commit` 消息**无 xid 字段**（见 `PgOutputMessage.java:15`）——普通/2PC 活动事务各用单指针 `currentNormalTx` / `currentPrepareTx`（Begin 置位、Commit/Prepare 处置），非 Map 按 key 取
2. `RowChange.before/after` 均 `Optional<TupleData>`（INSERT before=empty、DELETE after=empty、UPDATE before 视 replica identity）——避免 null 组件

---

### Task 1: 数据模型与事务契约（9 个新文件）

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/TransactionKind.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/DmlKind.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/TxChange.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/RowChange.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/TruncateChange.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/MsgChange.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/Transaction.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/TransactionListener.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionModelTest.java`

- [ ] **Step 1: 写失败的模型测试**

创建 `src/test/java/org/vastdata/vbstream/replication/TransactionModelTest.java`：

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 数据模型契约测试：构造可访问、防御性拷贝、数组值相等。 */
class TransactionModelTest {

    private static final Instant TS = Instant.parse("2026-08-27T00:00:00Z");

    private static PgOutputMessage.Relation relation(int oid) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", "t", 'd',
                List.of(new Column("id", 23, -1, true), new Column("v", 25, -1, false)));
    }

    private static TupleData row(String id, String v) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(v)));
    }

    @Test
    void transactionDefensivelyCopiesChanges() {
        // 验证紧凑构造器的 List.copyOf：外部改动源 List 不得影响 Transaction（不可变值对象语义）
        var changes = new java.util.ArrayList<TxChange>();
        changes.add(new RowChange(DmlKind.INSERT, relation(1),
                Optional.empty(), Optional.of(row("1", "a")), OptionalLong.empty()));
        Transaction txn = new Transaction(505L, TransactionKind.NORMAL, null,
                0x100L, 0x180L, TS, changes);
        changes.clear();
        assertEquals(1, txn.changes().size());
        assertThrows(UnsupportedOperationException.class, () -> txn.changes().add(
                new MsgChange(true, "p", new byte[0], OptionalLong.empty())));
    }

    @Test
    void msgChangeHasValueEqualsForByteArray() {
        // byte[] 组件必须值相等（record 默认对数组退化为引用相等，同 PgOutputMessage.LogicalMsg 的处理）
        MsgChange a = new MsgChange(true, "p", new byte[]{1, 2}, OptionalLong.empty());
        MsgChange b = new MsgChange(true, "p", new byte[]{1, 2}, OptionalLong.empty());
        assertEquals(a, b);
        assertNotEquals(a, new MsgChange(true, "p", new byte[]{1, 3}, OptionalLong.empty()));
    }

    @Test
    void truncateChangeCollectsRelationSnapshots() {
        TruncateChange tc = new TruncateChange(
                List.of(relation(1), relation(2)), EnumSet.of(TruncateOption.CASCADE), OptionalLong.empty());
        assertEquals(2, tc.relations().size());
        assertEquals(java.util.Set.of(TruncateOption.CASCADE), tc.options());
    }
}
```

- [ ] **Step 2: 运行验证编译失败（红）**

Run: `mvn test -Dtest=TransactionModelTest`
Expected: **COMPILATION ERROR**（`Transaction`/`TxChange`/`RowChange` 等类不存在）

- [ ] **Step 3: 写 9 个模型/契约文件**

`src/main/java/org/vastdata/vbstream/replication/TransactionKind.java`：

```java
package org.vastdata.vbstream.replication;

/** 事务形态：普通（Begin..Commit）、流式大事务（StreamStart..StreamCommit）、两阶段（BeginPrepare/StreamPrepare 后经 CommitPrepared 确认）。 */
public enum TransactionKind {
    /** 普通事务：变更整体缓冲，Commit 后一次输出。 */
    NORMAL,
    /** 流式大事务：越过 logical_decoding_work_mem 被驱逐流式，StreamCommit 后一次输出。 */
    STREAMED,
    /** 两阶段提交：PREPARE 后挂起，COMMIT PREPARED 才输出（ROLLBACK PREPARED 丢弃）。 */
    TWO_PHASE
}
```

`src/main/java/org/vastdata/vbstream/replication/DmlKind.java`：

```java
package org.vastdata.vbstream.replication;

/** 行级 DML 种类，对应 pgoutput 的 Insert/Update/Delete 三种消息。 */
public enum DmlKind { INSERT, UPDATE, DELETE }
```

`src/main/java/org/vastdata/vbstream/replication/TxChange.java`：

```java
package org.vastdata.vbstream.replication;

import java.util.OptionalLong;

/**
 * 事务内一条变更的密封基接口。实现自带 streamXid 组件（与接口方法同名，record 自动实现）。
 */
public sealed interface TxChange permits RowChange, TruncateChange, MsgChange {

    /**
     * 该变更所属（子）事务的 xid。
     *
     * <p>流式块内非空：DML/Truncate 消息的 xid 前缀 = 产生变更的（子）事务 xid，
     * Message 的前缀 = 顶层 xid（spec 附录 B.5）；普通事务恒为 empty。
     * 供 StreamAbort(sub) 时按子事务剔除变更、下游追溯子事务归属。
     *
     * @return （子）事务 xid；非流式变更返回 empty
     */
    OptionalLong streamXid();
}
```

`src/main/java/org/vastdata/vbstream/replication/RowChange.java`：

```java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * 行变更。before/after 语义沿用协议：INSERT 仅 after；UPDATE before 可选（replica identity 决定）、
 * after 必有；DELETE 仅 before。两者统一 Optional 以避免 null 组件（对 spec §3 的实现细化）。
 *
 * @param dml       DML 种类
 * @param relation  变更时刻的表元数据快照（嵌入而非引用 registry——表定义变化时协议会重发 Relation，
 *                  逐变更快照天然对齐；下游自包含，无需 registry）
 * @param before    旧元组：DELETE 必有；UPDATE 取决于 replica identity；INSERT 恒 empty
 * @param after     新元组：INSERT/UPDATE 必有；DELETE 恒 empty
 * @param streamXid 所属（子）事务 xid，见 {@link TxChange#streamXid()}
 */
public record RowChange(DmlKind dml, PgOutputMessage.Relation relation,
                        Optional<TupleData> before, Optional<TupleData> after,
                        OptionalLong streamXid) implements TxChange {

    /** 组件全量校验留给组装器（fail-fast 语义在其一侧）；本 record 仅承载不可变数据。 */
    public RowChange {
        before = before == null ? Optional.empty() : before;
        after = after == null ? Optional.empty() : after;
    }
}
```

`src/main/java/org/vastdata/vbstream/replication/TruncateChange.java`：

```java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * TRUNCATE 变更。一条 TRUNCATE 语句可截断多表：一次变更携带全部受影响表的 Relation 快照。
 *
 * @param options    TRUNCATE 选项（CASCADE / RESTART_IDENTITY）
 * @param relations  全部受影响表的元数据快照（顺序与协议 relationOids 一致）
 * @param streamXid  所属（子）事务 xid，见 {@link TxChange#streamXid()}
 */
public record TruncateChange(Set<TruncateOption> options, List<PgOutputMessage.Relation> relations,
                             OptionalLong streamXid) implements TxChange {

    /** 防御性拷贝：options/relations 收集为不可变集合，保证值对象语义。 */
    public TruncateChange {
        options = Set.copyOf(options);
        relations = List.copyOf(relations);
    }
}
```

`src/main/java/org/vastdata/vbstream/replication/MsgChange.java`：

```java
package org.vastdata.vbstream.replication;

import java.util.Arrays;
import java.util.OptionalLong;

/**
 * pg_logical_emit_message 产生的事务内逻辑消息。
 *
 * @param transactional true=事务性消息（随事务缓冲、提交才输出）；false=即时消息
 * @param prefix        消息前缀
 * @param content       消息字节内容
 * @param streamXid     所属（子）事务 xid，见 {@link TxChange#streamXid()}
 */
public record MsgChange(boolean transactional, String prefix, byte[] content,
                        OptionalLong streamXid) implements TxChange {

    /** content 为 byte[] 组件，需值相等语义（record 默认对数组退化为引用相等），故显式 override。 */
    @Override
    public boolean equals(Object o) {
        return o == this || o instanceof MsgChange other
                && transactional == other.transactional
                && prefix.equals(other.prefix)
                && Arrays.equals(content, other.content)
                && streamXid.equals(other.streamXid);
    }

    @Override
    public int hashCode() {
        int result = Boolean.hashCode(transactional);
        result = 31 * result + prefix.hashCode();
        result = 31 * result + Arrays.hashCode(content);
        result = 31 * result + streamXid.hashCode();
        return result;
    }
}
```

`src/main/java/org/vastdata/vbstream/replication/Transaction.java`：

```java
package org.vastdata.vbstream.replication;

import java.time.Instant;
import java.util.List;

/**
 * 一个已确认提交的完整事务（不可变原子单元，回调给 {@link TransactionListener}）。
 *
 * @param xid             事务 id：NORMAL 来自 Begin、STREAMED 来自 StreamStart、TWO_PHASE 来自 BeginPrepare/StreamPrepare
 * @param kind            事务形态
 * @param gid             两阶段事务的全局 id（非 null 当且仅当 kind=TWO_PHASE），其余 null
 * @param commitLsn       提交记录 LSN（Commit/StreamCommit/CommitPrepared 的对应字段）
 * @param endLsn          提交结束 LSN
 * @param commitTimestamp 提交时间戳
 * @param changes         事务内变更，按协议到达顺序
 */
public record Transaction(long xid, TransactionKind kind, String gid,
                          long commitLsn, long endLsn, Instant commitTimestamp,
                          List<TxChange> changes) {

    /** 防御性拷贝：changes 收集为不可变 List，回调后调用方持有的源缓冲不再影响本对象。 */
    public Transaction {
        changes = List.copyOf(changes);
    }
}
```

`src/main/java/org/vastdata/vbstream/replication/TransactionListener.java`：

```java
package org.vastdata.vbstream.replication;

/**
 * 事务消费契约：组装完成的原子事务到达即回调。
 *
 * <p>线程约束：调用线程 = TransactionAssembler 所在的 run 循环线程（同步执行，
 * 回调耗时直接拖慢消息循环与 LSN 反馈，实现方应快速返回或自行转交）。
 */
@FunctionalInterface
public interface TransactionListener {

    /**
     * 收到一个已确认提交的完整事务。
     *
     * @param transaction 不可变事务单元；ROLLBACK 路径不会回调
     */
    void onTransaction(Transaction transaction);
}
```

- [ ] **Step 4: 运行验证通过（绿）**

Run: `mvn test -Dtest=TransactionModelTest`
Expected: PASS（3 tests）

- [ ] **Step 5: Commit**

```bash
git add src/main/java/org/vastdata/vbstream/replication/ src/test/java/org/vastdata/vbstream/replication/TransactionModelTest.java
git commit -m "feat(assembly): 事务组装数据模型——Transaction/TxChange 密封族/TransactionListener 契约"
git push
```

---

### Task 2: 组装器骨架与普通事务路径

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`
- Modify: `docs/superpowers/specs/2026-08-27-transaction-assembly-design.md`（§4.1 Commit 行按"Commit 无 xid"修正为单指针）

- [ ] **Step 1: 写失败的普通事务/负例测试**

创建 `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`（夹具方法后续任务复用）：

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TransactionAssembler 状态机单测：直接以 protocol record 构造消息序列（组装器输入是已解析消息，
 * 无需字节级构造）。每用例断言输出 Transaction 的形态/顺序/内容与 fail-fast 行为。
 */
class TransactionAssemblerTest {

    private static final Instant TS = Instant.parse("2026-08-27T00:00:00Z");
    private static final int OID = 16384;

    /** 构造两列 (id int, v text) 的 Relation 消息，列序与 {@link #row} 对齐。 */
    private static PgOutputMessage.Relation relation() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), OID, "public", "t", 'd',
                List.of(new Column("id", 23, -1, true), new Column("v", 25, -1, false)));
    }

    /** 构造一行文本元组 (id, v)。 */
    private static TupleData row(String id, String v) {
        return new TupleData(List.of(new TupleValue.Text(id), new TupleValue.Text(v)));
    }

    /** 流式块外的 Insert 消息。 */
    private static PgOutputMessage.Insert insert(String id, String v) {
        return new PgOutputMessage.Insert(OptionalLong.empty(), OID, row(id, v));
    }

    /** 流式块内的 Insert 消息（streamXid=产生该变更的（子）事务 xid）。 */
    private static PgOutputMessage.Insert streamedInsert(long streamXid, String id, String v) {
        return new PgOutputMessage.Insert(OptionalLong.of(streamXid), OID, row(id, v));
    }

    /**
     * 依序把消息喂给新组装器（Relation 先经 registry.accept，其余经 assembler.accept——与 Main 的装配顺序一致），
     * 收集输出的 Transaction。
     */
    private static List<Transaction> run(PgOutputMessage... msgs) {
        RelationRegistry registry = new RelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add);
        for (PgOutputMessage m : msgs) {
            registry.accept(m);
            assembler.accept(m, registry);
        }
        return out;
    }

    @Test
    void assemblesNormalTransactionInOrder() {
        List<Transaction> out = run(
                new PgOutputMessage.Begin(0x100L, TS, 505L),
                relation(),
                insert("1", "a"),
                new PgOutputMessage.Update(OptionalLong.empty(), OID,
                        java.util.Optional.empty(), row("1", "b")),
                new PgOutputMessage.Delete(OptionalLong.empty(), OID, row("1", "b")),
                new PgOutputMessage.Commit(0x100L, 0x180L, TS));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(505L, t.xid());
        assertEquals(TransactionKind.NORMAL, t.kind());
        assertNull(t.gid());
        assertEquals(0x100L, t.commitLsn());
        assertEquals(0x180L, t.endLsn());
        assertEquals(TS, t.commitTimestamp());
        assertEquals(3, t.changes().size());
        RowChange c0 = (RowChange) t.changes().get(0);
        assertEquals(DmlKind.INSERT, c0.dml());
        assertEquals("t", c0.relation().table());          // Relation 快照嵌入
        assertEquals(row("1", "a"), c0.after().orElseThrow());
        assertEquals(DmlKind.UPDATE, ((RowChange) t.changes().get(1)).dml());
        assertEquals(DmlKind.DELETE, ((RowChange) t.changes().get(2)).dml());
    }

    @Test
    void consecutiveTransactionsEmitOneByOne() {
        List<Transaction> out = run(
                new PgOutputMessage.Begin(0x1L, TS, 1L),
                insert("1", "a"),
                new PgOutputMessage.Commit(0x1L, 0x2L, TS),
                new PgOutputMessage.Begin(0x3L, TS, 2L),
                insert("2", "b"),
                new PgOutputMessage.Commit(0x3L, 0x4L, TS));
        assertEquals(List.of(1L, 2L), out.stream().map(Transaction::xid).toList());
    }

    @Test
    void rejectsCommitWithoutBegin() {
        assertThrows(IllegalStateException.class, () ->
                run(new PgOutputMessage.Commit(1L, 2L, TS)));
    }

    @Test
    void rejectsDuplicateBegin() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Begin(1L, TS, 1L),
                new PgOutputMessage.Begin(2L, TS, 1L)));
    }

    @Test
    void rejectsChangeWithoutActiveBucket() {
        assertThrows(IllegalStateException.class, () -> run(
                relation(),
                insert("1", "a")));
    }

    @Test
    void rejectsUnknownRelationOid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Begin(1L, TS, 1L),
                insert("1", "a")));   // 未发 Relation：registry.require miss
    }
}
```

- [ ] **Step 2: 运行验证编译失败（红）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: **COMPILATION ERROR**（`TransactionAssembler` 不存在）

- [ ] **Step 3: 实现 TransactionAssembler（骨架 + 普通路径）**

创建 `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`。本任务实现 Begin/Insert/Update/Delete/Truncate/LogicalMsg/Commit 与 dispatch；流式/2PC 分支留到 Task 3/5（dispatch 中先对未实现的消息类型抛 `IllegalStateException("未支持的消息类型")`——Task 3/5 逐个补齐）：

```java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * pgoutput 消息 → 原子事务的组装状态机（纯内存、无 IO）。
 *
 * <p>职责：按消息驱动规则（spec §4.1）缓冲同事务变更，收到提交信号（Commit/StreamCommit/
 * CommitPrepared）后封箱为不可变 {@link Transaction} 回调；回滚路径（RollbackPrepared/
 * StreamAbort）剔除或丢弃缓冲后不回调。
 *
 * <p>桶模型（spec §4.2/§4.3，含对 spec 的实现细化）：
 * <ul>
 *   <li>普通事务：单指针 {@code currentNormalTx}——Commit 消息无 xid 字段，且 walsender 按
 *       LSN 序串行输出 Begin..Commit，同时至多一个活动普通事务</li>
 *   <li>流式事务：{@code streamedByXid} 多桶（key=StreamStart 的顶层 xid）+ {@code currentStream}
 *       流块上下文指针——多个并发大事务的流段会交错（spec §4.2 已源码验证），流块本身不嵌套</li>
 *   <li>两阶段：活动期单指针 {@code currentPrepareTx}，PREPARE 后转 {@code preparedByGid}
 *       挂起池等待 COMMIT PREPARED / ROLLBACK PREPARED</li>
 * </ul>
 *
 * <p>线程约束：非线程安全。设计为在单一 run 循环线程内被调用（与 PgOutputDecoder 同约束）；
 * 输出的 Transaction 不可变，可跨线程传递。
 */
public final class TransactionAssembler {

    private final TransactionListener listener;

    /** 活动普通事务桶（Begin 置位，Commit 封箱清空；协议保证 Begin..Commit 串行不嵌套）。 */
    private TxBuffer currentNormalTx;
    /** 活动两阶段事务桶（BeginPrepare 置位，Prepare 转挂起池）。 */
    private TxBuffer currentPrepareTx;
    /** 当前流块上下文：stream_start..stream_stop 之间非 null，指向 streamedByXid 中某桶。 */
    private TxBuffer currentStream;
    /** 流式事务桶，key=顶层 xid（多桶并存，段间交错——spec §4.2）。 */
    private final Map<Long, TxBuffer> streamedByXid = new HashMap<>();
    /** 两阶段挂起池，key=gid（PREPARE 至 COMMIT/ROLLBACK PREPARED 之间，可能长期挂起）。 */
    private final Map<String, TxBuffer> preparedByGid = new HashMap<>();

    /** 组装缓冲：xid 与变更序列；gid 仅两阶段桶非 null。非线程安全（仅 run 线程触碰）。 */
    private static final class TxBuffer {
        final long xid;
        String gid;
        final List<TxChange> changes = new ArrayList<>();

        TxBuffer(long xid) {
            this.xid = xid;
        }
    }

    /**
     * 构造组装器。
     *
     * @param listener 完整事务到达时的回调（同步调用，调用线程与本组装器的调用线程一致）
     */
    public TransactionAssembler(TransactionListener listener) {
        this.listener = java.util.Objects.requireNonNull(listener, "listener");
    }

    /**
     * 喂入一条已解析消息（调用方需先让 registry 消化 Relation 元数据，参照 Main 的装配顺序）。
     *
     * <p>关键步骤：按消息类型分发到对应规则（spec §4.1 全路径表）；任何桶缺失/重复/流块状态
     * 异常均抛 {@link IllegalStateException}（fail-fast，协议流不应出现）。
     *
     * @param message  协议消息（19 种之一）
     * @param registry 关系元数据缓存，用于把变更的 relationOid 解析为快照
     */
    public void accept(PgOutputMessage message, RelationRegistry registry) {
        // 注：record pattern switch 是 Java 21 正式特性，本项目约束 Java 17，故用 instanceof 链
        if (message instanceof PgOutputMessage.Begin m) {
            begin(m);
        } else if (message instanceof PgOutputMessage.Commit m) {
            commit(m);
        } else if (message instanceof PgOutputMessage.Insert m) {
            activeBucket().changes.add(new RowChange(DmlKind.INSERT, registry.require(m.relationOid()),
                    Optional.empty(), Optional.of(m.newTuple()), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Update m) {
            activeBucket().changes.add(new RowChange(DmlKind.UPDATE, registry.require(m.relationOid()),
                    m.oldTuple(), Optional.of(m.newTuple()), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Delete m) {
            activeBucket().changes.add(new RowChange(DmlKind.DELETE, registry.require(m.relationOid()),
                    Optional.of(m.oldTuple()), Optional.empty(), m.streamXid()));
        } else if (message instanceof PgOutputMessage.Truncate m) {
            List<PgOutputMessage.Relation> snapshots = Arrays.stream(m.relationOids())
                    .mapToObj(registry::require)
                    .toList();
            activeBucket().changes.add(new TruncateChange(snapshots, m.options(), m.streamXid()));
        } else if (message instanceof PgOutputMessage.LogicalMsg m) {
            logicalMsg(m);
        } else if (message instanceof PgOutputMessage.StreamStart m) {
            throw new IllegalStateException("流式路径尚未实现（Task 3）: " + m);
        } else if (message instanceof PgOutputMessage.StreamStop m) {
            throw new IllegalStateException("流式路径尚未实现（Task 3）: " + m);
        } else if (message instanceof PgOutputMessage.StreamCommit m) {
            throw new IllegalStateException("流式路径尚未实现（Task 3）: " + m);
        } else if (message instanceof PgOutputMessage.StreamAbort m) {
            throw new IllegalStateException("StreamAbort 尚未实现（Task 4）: " + m);
        } else if (message instanceof PgOutputMessage.BeginPrepare m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.Prepare m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.CommitPrepared m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.RollbackPrepared m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.StreamPrepare m) {
            throw new IllegalStateException("两阶段路径尚未实现（Task 5）: " + m);
        } else if (message instanceof PgOutputMessage.Relation || message instanceof PgOutputMessage.Type) {
            // 元数据消息：registry 职责（调用方已转发），组装器不处理
        } else if (message instanceof PgOutputMessage.Origin m) {
            // 级联复制源位点：本里程碑非目标，透传忽略
        } else {
            throw new IllegalStateException("未知消息类型: " + message.getClass());
        }
    }

    /**
     * 取当前应接收变更的活动桶。
     *
     * <p>查找顺序（spec §4.3）：流块上下文（最高优先）→ 活动两阶段桶 → 活动普通桶；
     * 三者皆空说明变更消息游离在任何事务外，协议流异常。
     */
    private TxBuffer activeBucket() {
        if (currentStream != null) {
            return currentStream;
        }
        if (currentPrepareTx != null) {
            return currentPrepareTx;
        }
        if (currentNormalTx != null) {
            return currentNormalTx;
        }
        throw new IllegalStateException("变更消息到达但无任何活动事务桶");
    }

    /** Begin：开新普通事务桶；已有未闭合普通事务即 fail-fast（协议上 Begin..Commit 不嵌套）。 */
    private void begin(PgOutputMessage.Begin m) {
        if (currentNormalTx != null) {
            throw new IllegalStateException("Begin 到达但普通事务未闭合: xid=" + currentNormalTx.xid);
        }
        currentNormalTx = new TxBuffer(m.xid());
    }

    /** Commit（无 xid 字段）：封箱当前普通事务桶为 NORMAL Transaction 回调并清空指针；无桶即 fail-fast。 */
    private void commit(PgOutputMessage.Commit m) {
        if (currentNormalTx == null) {
            throw new IllegalStateException("Commit 到达但无活动普通事务");
        }
        TxBuffer bucket = currentNormalTx;
        currentNormalTx = null;
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.NORMAL, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), bucket.changes));
    }

    /**
     * LogicalMsg：事务性消息必须落在活动桶内（无桶即 fail-fast）；
     * 非事务性消息有活动桶则随桶走（abort 剔除按 streamXid，语义安全），无桶则 WARN 丢弃
     * （协议允许其游离于任何事务之外，非协议流异常）。
     */
    private void logicalMsg(PgOutputMessage.LogicalMsg m) {
        if (m.transactional()) {
            activeBucket().changes.add(new MsgChange(true, m.prefix(), m.content(), m.streamXid()));
            return;
        }
        if (currentStream != null || currentPrepareTx != null || currentNormalTx != null) {
            activeBucket().changes.add(new MsgChange(false, m.prefix(), m.content(), m.streamXid()));
        }
        // 游离的非事务性消息：无桶可归属，丢弃（本里程碑不做独立事件通道）
    }
}
```

注意：`logicalMsg` 中 WARN 日志按项目规约需走 slf4j——在类顶部加：

```java
private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TransactionAssembler.class);
```

并在 `logicalMsg` 丢弃分支加 `LOG.warn("非事务性消息游离于任何事务之外，丢弃: prefix={} lsn=0x{}", m.prefix(), Long.toHexString(m.lsn()));`（import 放文件头，与项目风格一致）。

- [ ] **Step 4: 运行验证通过（绿）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: PASS（6 tests）

- [ ] **Step 5: 同步修正 spec §4.1 的 Commit 行**

在 `docs/superpowers/specs/2026-08-27-transaction-assembly-design.md` §4.1 表中，把：

```
| `Commit(xid, ...)` | 从 `pendingByXid` 取桶（miss → fail-fast），封箱为 `Transaction(NORMAL, gid=null, ...)` 回调，移除桶 |
```

改为：

```
| `Commit(...)` | **消息无 xid 字段**——封箱当前普通事务桶 `currentNormalTx`（无桶 → fail-fast）为 `Transaction(NORMAL, gid=null, ...)` 回调并清空指针；普通/2PC 活动桶实现为单指针（协议保证 Begin..Commit / BeginPrepare..Prepare 串行不嵌套），对应地 §4 内部结构 `pendingByXid` 由两指针替代 |
```

- [ ] **Step 6: Commit + push**

```bash
git add src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java \
        src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java \
        docs/superpowers/specs/2026-08-27-transaction-assembly-design.md
git commit -m "feat(assembly): 组装器骨架与普通事务路径——Begin/DML/LogicalMsg/Commit + fail-fast"
git push
```

---

### Task 3: 流式路径（多桶 + 流块上下文）

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`（追加用例）

- [ ] **Step 1: 追加失败的流式用例（spec §6.1 用例 3/5/6 + 负例）**

在 `TransactionAssemblerTest` 追加（夹具 `streamedInsert` 已在 Task 2 定义）：

```java
    private static final long TOP_A = 7001L;
    private static final long TOP_B = 7002L;
    private static final long SUB = 7003L;

    @Test
    void assemblesSingleStreamedTransaction() {
        List<Transaction> out = run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                relation(),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamCommit(TOP_A, 0x500L, 0x580L, TS));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TOP_A, t.xid());
        assertEquals(TransactionKind.STREAMED, t.kind());
        assertNull(t.gid());
        assertEquals(2, t.changes().size());
        // streamXid 逐变更保留（子事务归属可追溯）
        assertEquals(OptionalLong.of(TOP_A), t.changes().get(0).streamXid());
        assertEquals(OptionalLong.of(SUB), t.changes().get(1).streamXid());
    }

    @Test
    void interleavedStreamingTransactionsEmitIndependently() {
        // spec §4.2 场景：两个并发大事务流段交错，多桶各自独立（B.1 全局内存阈值 + B.2 轮番驱逐）
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_B, true),
                streamedInsert(TOP_B, "9", "i"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_A, false),
                streamedInsert(TOP_A, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_B, false),
                streamedInsert(TOP_B, "8", "h"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamCommit(TOP_A, 0x1L, 0x2L, TS),
                new PgOutputMessage.StreamCommit(TOP_B, 0x3L, 0x4L, TS));
        assertEquals(2, out.size());
        assertEquals(TOP_A, out.get(0).xid());
        assertEquals(TransactionKind.STREAMED, out.get(0).kind());
        assertEquals(TOP_B, out.get(1).xid());
        // A 桶两段共 2 条、B 桶两段共 2 条——段间交错不丢不混
        assertEquals(2, out.get(0).changes().size());
        assertEquals(2, out.get(1).changes().size());
        RowChange first = (RowChange) out.get(0).changes().get(0);
        assertEquals("1", ((TupleValue.Text) first.after().orElseThrow().columns().get(0)).value());
    }

    @Test
    void smallNormalTransactionBetweenStreamSegmentsRoutesCorrectly() {
        // 流段间隙插入的普通小事务先行输出，流事务随后（currentStream 在 stream_stop 后让位）
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.Begin(99L, TS, 99L),
                insert("5", "x"),
                new PgOutputMessage.Commit(1L, 2L, TS),
                new PgOutputMessage.StreamStart(TOP_A, false),
                streamedInsert(TOP_A, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamCommit(TOP_A, 3L, 4L, TS));
        assertEquals(List.of(99L, TOP_A), out.stream().map(Transaction::xid).toList());
        assertEquals(TransactionKind.NORMAL, out.get(0).kind());
        assertEquals(TransactionKind.STREAMED, out.get(1).kind());
        assertEquals(2, out.get(1).changes().size());
    }

    @Test
    void rejectsStreamContinueForUnknownXid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, false)));   // 首段标记 false 但无桶
    }

    @Test
    void rejectsDuplicateFirstSegment() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamStart(TOP_A, true),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamStart(TOP_A, true)));   // 同顶层事务再次 first=true
    }

    @Test
    void rejectsStreamStopWithoutStreamBlock() {
        assertThrows(IllegalStateException.class, () ->
                run(new PgOutputMessage.StreamStop()));
    }
```

- [ ] **Step 2: 运行验证失败（红）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: FAIL（新增 6 用例因 `IllegalStateException("流式路径尚未实现")` 失败；Task 2 的 6 用例仍绿）

- [ ] **Step 3: 实现流式三分支**

`accept` 中替换三个占位分支为：

```java
        } else if (message instanceof PgOutputMessage.StreamStart m) {
            streamStart(m);
        } else if (message instanceof PgOutputMessage.StreamStop m) {
            streamStop();
        } else if (message instanceof PgOutputMessage.StreamCommit m) {
            streamCommit(m);
```

新增私有方法：

```java
    /**
     * StreamStart(xid, firstSegment)：xid 恒为顶层 xid（spec B.3）。
     *
     * <p>firstSegment=true（该顶层事务首段）→ 新建桶入 streamedByXid（已存在同 xid → fail-fast）；
     * false（后续段）→ 桶必须已存在（miss → fail-fast）。两种情况都切换 currentStream 到该桶。
     */
    private void streamStart(PgOutputMessage.StreamStart m) {
        TxBuffer bucket;
        if (m.firstSegment()) {
            bucket = new TxBuffer(m.xid());
            if (streamedByXid.putIfAbsent(m.xid(), bucket) != null) {
                throw new IllegalStateException("流式事务桶已存在: xid=" + m.xid());
            }
        } else {
            bucket = streamedByXid.get(m.xid());
            if (bucket == null) {
                throw new IllegalStateException("StreamStart(first=false) 但顶层事务无桶: xid=" + m.xid());
            }
        }
        currentStream = bucket;
    }

    /**
     * StreamStop：流块边界（消息不携带 xid）。currentStream 必须非 null（否则 fail-fast），置 null。
     * 流桶保留在 streamedByXid 中等待后续段或 StreamCommit/StreamAbort/StreamPrepare。
     */
    private void streamStop() {
        if (currentStream == null) {
            throw new IllegalStateException("StreamStop 到达但无进行中的流块");
        }
        currentStream = null;
    }

    /**
     * StreamCommit(xid)：顶层事务全部流段已收齐，封箱 STREAMED Transaction 回调并移除桶；
     * 桶 miss 或仍有未闭合流块（协议保证 stream_commit 必在流块外，spec B.3）均 fail-fast。
     */
    private void streamCommit(PgOutputMessage.StreamCommit m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamCommit 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamCommit 对应流式事务桶不存在: xid=" + m.xid());
        }
        listener.onTransaction(new Transaction(m.xid(), TransactionKind.STREAMED, null,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), bucket.changes));
    }
```

- [ ] **Step 4: 运行验证通过（绿）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: PASS（12 tests）

- [ ] **Step 5: Commit + push**

```bash
git add src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java \
        src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java
git commit -m "feat(assembly): 流式路径——多桶 streamedByXid + currentStream 流块上下文，双事务段间交错用例"
git push
```

---

### Task 4: StreamAbort（子事务剔除 / 整顶层移除）

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`

- [ ] **Step 1: 追加失败的 abort 用例（spec §6.1 用例 4）**

```java
    @Test
    void streamAbortRemovesSubtransactionChanges() {
        // 子事务回滚：仅剔除 streamXid==sub 的已流式变更，其余保留
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                streamedInsert(SUB, "3", "c"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamAbort(TOP_A, SUB, OptionalLong.empty(), OptionalLong.empty()),
                new PgOutputMessage.StreamCommit(TOP_A, 1L, 2L, TS));
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).changes().size());
        assertEquals(OptionalLong.of(TOP_A), out.get(0).changes().get(0).streamXid());
    }

    @Test
    void streamAbortOfWholeTopTransactionDropsBucket() {
        // 整顶层回滚（decode 层先逐子后顶，最后一条 top==sub，spec B.4）：桶整体移除，StreamCommit 无从回调
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                streamedInsert(SUB, "2", "b"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamAbort(TOP_A, SUB, OptionalLong.empty(), OptionalLong.empty()),
                new PgOutputMessage.StreamAbort(TOP_A, TOP_A, OptionalLong.empty(), OptionalLong.empty()));
        assertEquals(0, out.size());
        // 桶已移除：后续同 xid 的 StreamCommit 应 fail-fast（非静默）
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamCommit(TOP_A, 1L, 2L, TS)));
    }

    @Test
    void rejectsStreamAbortForUnknownTopXid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.StreamAbort(404L, 405L, OptionalLong.empty(), OptionalLong.empty())));
    }
```

- [ ] **Step 2: 运行验证失败（红）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: FAIL（3 新用例命中 `"StreamAbort 尚未实现"` 占位）

- [ ] **Step 3: 实现 streamAbort**

替换占位分支为 `} else if (message instanceof PgOutputMessage.StreamAbort m) { streamAbort(m); }`，新增：

```java
    /**
     * StreamAbort(top, sub)：已流式事务的（子）事务回滚，剔除其已下发的变更（spec B.4）。
     *
     * <p>top==sub（整顶层回滚，decode 层"先子后顶"的最后一条）→ 移除整个桶；
     * 否则从桶中剔除所有 streamXid==sub 的变更（Message 的 streamXid=顶层 xid，不会误删）。
     * 桶 miss 或流块未闭合均 fail-fast（abort 必在流块外）。
     */
    private void streamAbort(PgOutputMessage.StreamAbort m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamAbort 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.get(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamAbort 对应流式事务桶不存在: xid=" + m.xid());
        }
        if (m.xid() == m.subxid()) {
            streamedByXid.remove(m.xid());
        } else {
            bucket.changes.removeIf(c -> c.streamXid().isPresent() && c.streamXid().getAsLong() == m.subxid());
        }
    }
```

- [ ] **Step 4: 运行验证通过（绿）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: PASS（15 tests）

- [ ] **Step 5: Commit + push**

```bash
git add src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java \
        src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java
git commit -m "feat(assembly): StreamAbort——子事务按 streamXid 剔除、整顶层回滚移除桶"
git push
```

---

### Task 5: 两阶段全路径

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`

- [ ] **Step 1: 追加失败的 2PC 用例（spec §6.1 用例 7/8/9 + 负例）**

```java
    private static final String GID = "gid-1";

    @Test
    void twoPhaseCommitEmitsOnCommitPrepared() {
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.BeginPrepare(0x10L, 0x18L, TS, 601L, GID),
                insert("1", "a"),
                new PgOutputMessage.Prepare(0x10L, 0x18L, TS, 601L, GID),
                new PgOutputMessage.CommitPrepared(0x20L, 0x28L, TS, 601L, GID));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.TWO_PHASE, t.kind());
        assertEquals(GID, t.gid());
        assertEquals(601L, t.xid());
        assertEquals(0x20L, t.commitLsn());
        assertEquals(1, t.changes().size());
    }

    @Test
    void rollbackPreparedDiscardsSilently() {
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.BeginPrepare(0x10L, 0x18L, TS, 601L, GID),
                insert("1", "a"),
                new PgOutputMessage.Prepare(0x10L, 0x18L, TS, 601L, GID),
                new PgOutputMessage.RollbackPrepared(0x10L, 0x30L, TS, TS, 601L, GID));
        assertEquals(0, out.size());
    }

    @Test
    void streamedTwoPhaseEmitsOnCommitPrepared() {
        // 流式 2PC：StreamPrepare 前必有最后一个流段并已闭合（spec B.6），桶从 streamedByXid 转挂起池
        List<Transaction> out = run(
                relation(),
                new PgOutputMessage.StreamStart(TOP_A, true),
                streamedInsert(TOP_A, "1", "a"),
                new PgOutputMessage.StreamStop(),
                new PgOutputMessage.StreamPrepare(0x10L, 0x18L, TS, TOP_A, GID),
                new PgOutputMessage.CommitPrepared(0x20L, 0x28L, TS, TOP_A, GID));
        assertEquals(1, out.size());
        Transaction t = out.get(0);
        assertEquals(TransactionKind.TWO_PHASE, t.kind());
        assertEquals(GID, t.gid());
        assertEquals(TOP_A, t.xid());
        assertEquals(1, t.changes().size());
    }

    @Test
    void rejectsCommitPreparedForUnknownGid() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.CommitPrepared(1L, 2L, TS, 1L, "no-such-gid")));
    }

    @Test
    void rejectsDuplicatePrepareGid() {
        assertThrows(IllegalStateException.class, () -> run(
                relation(),
                new PgOutputMessage.BeginPrepare(1L, 2L, TS, 601L, GID),
                new PgOutputMessage.Prepare(1L, 2L, TS, 601L, GID),
                // 同 gid 第二次 Prepare：挂起池已存在 → fail-fast
                new PgOutputMessage.BeginPrepare(3L, 4L, TS, 602L, GID),
                new PgOutputMessage.Prepare(3L, 4L, TS, 602L, GID)));
    }

    @Test
    void rejectsPrepareWithoutBeginPrepare() {
        assertThrows(IllegalStateException.class, () -> run(
                new PgOutputMessage.Prepare(1L, 2L, TS, 601L, GID)));
    }
```

- [ ] **Step 2: 运行验证失败（红）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: FAIL（6 新用例命中两阶段占位）

- [ ] **Step 3: 实现两阶段五分支**

替换五个占位分支：

```java
        } else if (message instanceof PgOutputMessage.BeginPrepare m) {
            beginPrepare(m);
        } else if (message instanceof PgOutputMessage.Prepare m) {
            prepare(m);
        } else if (message instanceof PgOutputMessage.CommitPrepared m) {
            commitPrepared(m);
        } else if (message instanceof PgOutputMessage.RollbackPrepared m) {
            rollbackPrepared(m);
        } else if (message instanceof PgOutputMessage.StreamPrepare m) {
            streamPrepare(m);
```

新增方法：

```java
    /** BeginPrepare：开活动两阶段桶（记 gid/xid）；已有未闭合两阶段桶即 fail-fast（b..P 串行不嵌套）。 */
    private void beginPrepare(PgOutputMessage.BeginPrepare m) {
        if (currentPrepareTx != null) {
            throw new IllegalStateException("BeginPrepare 到达但两阶段事务未闭合: gid=" + currentPrepareTx.gid);
        }
        currentPrepareTx = new TxBuffer(m.xid());
        currentPrepareTx.gid = m.gid();
    }

    /**
     * Prepare：活动两阶段桶转挂起池（gid 已存在 → fail-fast）。
     * 事务自此挂起，等待 CommitPrepared（输出）或 RollbackPrepared（丢弃），可能长期等待甚至跨重启（持久化非目标，spec §7）。
     */
    private void prepare(PgOutputMessage.Prepare m) {
        if (currentPrepareTx == null || currentPrepareTx.xid != m.xid()
                || !currentPrepareTx.gid.equals(m.gid())) {
            throw new IllegalStateException("Prepare 与活动两阶段事务不匹配: gid=" + m.gid() + " xid=" + m.xid());
        }
        TxBuffer bucket = currentPrepareTx;
        currentPrepareTx = null;
        if (preparedByGid.putIfAbsent(bucket.gid, bucket) != null) {
            throw new IllegalStateException("挂起池已存在同 gid 事务: " + bucket.gid);
        }
    }

    /** CommitPrepared：挂起池取桶（miss → fail-fast）封箱 TWO_PHASE Transaction 回调（用户确认的输出时机）。 */
    private void commitPrepared(PgOutputMessage.CommitPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("CommitPrepared 对应 gid 不存在: " + m.gid());
        }
        listener.onTransaction(new Transaction(bucket.xid, TransactionKind.TWO_PHASE, bucket.gid,
                m.commitLsn(), m.endLsn(), m.commitTimestamp(), bucket.changes));
    }

    /** RollbackPrepared：挂起池取桶（miss → fail-fast）静默丢弃，不回调（用户确认的回滚语义）。 */
    private void rollbackPrepared(PgOutputMessage.RollbackPrepared m) {
        TxBuffer bucket = preparedByGid.remove(m.gid());
        if (bucket == null) {
            throw new IllegalStateException("RollbackPrepared 对应 gid 不存在: " + m.gid());
        }
        LOG.warn("两阶段事务回滚，丢弃已缓冲变更: gid={} xid={} changes={}",
                m.gid(), m.xid(), bucket.changes.size());
    }

    /**
     * StreamPrepare(xid, gid)：流式 2PC 的 prepare。流桶从 streamedByXid 移出（miss 或流块未闭合 → fail-fast，
     * stream_prepare 前服务端必已发完最后一个流段并 stream_stop，spec B.6），记 gid 后转入挂起池。
     */
    private void streamPrepare(PgOutputMessage.StreamPrepare m) {
        if (currentStream != null) {
            throw new IllegalStateException("StreamPrepare 到达但流块未闭合: xid=" + currentStream.xid);
        }
        TxBuffer bucket = streamedByXid.remove(m.xid());
        if (bucket == null) {
            throw new IllegalStateException("StreamPrepare 对应流式事务桶不存在: xid=" + m.xid());
        }
        bucket.gid = m.gid();
        if (preparedByGid.putIfAbsent(bucket.gid, bucket) != null) {
            throw new IllegalStateException("挂起池已存在同 gid 事务: " + bucket.gid);
        }
    }
```

- [ ] **Step 4: 运行验证通过（绿，全量单测回归）**

Run: `mvn test -Dtest=TransactionAssemblerTest`
Expected: PASS（21 tests）
Run: `mvn test -Dtest='TransactionModelTest,TransactionAssemblerTest'`
Expected: PASS

- [ ] **Step 5: Commit + push**

```bash
git add src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java \
        src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java
git commit -m "feat(assembly): 两阶段全路径——挂起池 preparedByGid，COMMIT PREPARED 输出/ROLLBACK 丢弃"
git push
```

---

### Task 6: ConsoleListener 事务块输出与 Main 装配

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/ConsoleListener.java`
- Modify: `src/main/java/org/vastdata/vbstream/Main.java`
- Test: `src/test/java/org/vastdata/vbstream/ConsoleListenerTest.java`

- [ ] **Step 1: 写失败的事务渲染测试**

创建 `src/test/java/org/vastdata/vbstream/ConsoleListenerTest.java`（logback ListAppender 捕获 CDC logger 输出）：

```java
package org.vastdata.vbstream;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.DmlKind;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionKind;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ConsoleListener 事务块渲染格式测试（logback ListAppender 捕获 CDC logger）。 */
class ConsoleListenerTest {

    private final Logger cdc = (Logger) org.slf4j.LoggerFactory.getLogger("org.vastdata.vbstream.cdc");
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void attach() {
        appender.start();
        cdc.addAppender(appender);
    }

    @AfterEach
    void detach() {
        cdc.detachAppender(appender);
    }

    private static PgOutputMessage.Relation relation() {
        return new PgOutputMessage.Relation(OptionalLong.empty(), 16384, "public", "t_stream", 'd',
                List.of(new Column("id", 23, -1, true), new Column("payload", 25, -1, false)));
    }

    /** 断言 onTransaction 渲染头/变更/尾三段结构。 */
    @Test
    void rendersTransactionBlockHeaderBodyFooter() {
        RowChange insert = new RowChange(DmlKind.INSERT, relation(),
                Optional.empty(),
                Optional.of(new TupleData(List.of(new TupleValue.Text("1"), new TupleValue.Text("aaa")))),
                OptionalLong.empty());
        Transaction txn = new Transaction(505L, TransactionKind.STREAMED, null,
                0x1BD9E70L, 0x1BD9E80L, Instant.parse("2026-08-27T08:00:00Z"), List.of(insert));

        new ConsoleListener().onTransaction(txn);

        assertEquals(3, appender.list.size());
        String header = appender.list.get(0).getFormattedMessage();
        assertTrue(header.startsWith("TXN-BEGIN xid=505 kind=STREAMED gid=null"), "头行不符: " + header);
        assertTrue(header.contains("changes=1"), "头行变更数不符: " + header);
        String body = appender.list.get(1).getFormattedMessage();
        assertTrue(body.contains("[1] INSERT public.t_stream"), "变更行不符: " + body);
        assertTrue(body.contains("id=1"), "列渲染不符: " + body);
        assertEquals("TXN-END   xid=505", appender.list.get(2).getFormattedMessage());
    }
}
```

- [ ] **Step 2: 运行验证编译失败（红）**

Run: `mvn test -Dtest=ConsoleListenerTest`
Expected: **COMPILATION ERROR**（`ConsoleListener` 无 `onTransaction`）

- [ ] **Step 3: 改造 ConsoleListener**

`ConsoleListener` 声明改为同时实现两契约，新增事务渲染；`onMessage` 降为 DEBUG（spec §5：Main 默认走事务形态，逐消息细节默认关闭）：

```java
public final class ConsoleListener implements PgOutputListener, TransactionListener {
```

`onMessage` 内 `CDC.info(...)` 改为 `CDC.debug(...)`（注释同步：逐消息细节 DEBUG，事务块 INFO）。

新增方法（放在 `onMessage` 之后）：

```java
    /**
     * 事务块输出：头/尾各一行 INFO（CDC logger），变更行逐条复用关系快照渲染。
     * 调用线程 = run 循环线程（与 onMessage 同约束）。
     */
    @Override
    public void onTransaction(Transaction transaction) {
        CDC.info("TXN-BEGIN xid={} kind={} gid={} commitLsn=0x{} commitTs={} changes={}",
                transaction.xid(), transaction.kind(), transaction.gid(),
                Long.toHexString(transaction.commitLsn()), transaction.commitTimestamp(),
                transaction.changes().size());
        int seq = 1;
        for (TxChange change : transaction.changes()) {
            CDC.info("  [{}] {}", seq++, renderChange(change));
        }
        CDC.info("TXN-END   xid={}", transaction.xid());
    }

    /** 单条变更渲染：列名取自嵌入的 Relation 快照（不依赖 registry，下游自包含）。 */
    private static String renderChange(TxChange change) {
        if (change instanceof RowChange rc) {
            return "%s %s BEFORE=%s AFTER=%s%s".formatted(rc.dml(), tableOf(rc.relation()),
                    rc.before().map(t -> tupleOf(t, rc.relation())).orElse("-"),
                    rc.after().map(t -> tupleOf(t, rc.relation())).orElse("-"),
                    suffix(rc.streamXid()));
        }
        if (change instanceof TruncateChange tc) {
            return "TRUNCATE %s options=%s%s".formatted(
                    tc.relations().stream().map(ConsoleListener::tableOf).toList(),
                    tc.options(), suffix(tc.streamXid()));
        }
        if (change instanceof MsgChange mc) {
            return "MESSAGE prefix=%s bytes=%d%s".formatted(mc.prefix(), mc.content().length, suffix(mc.streamXid()));
        }
        throw new IllegalStateException("未知变更类型: " + change.getClass());
    }

    /** 表名渲染（基于嵌入快照）。 */
    private static String tableOf(PgOutputMessage.Relation relation) {
        return relation.schema() + "." + relation.table();
    }

    /** 列名=值 打印（基于嵌入快照，与逐消息版 tupleOf 同规则：text 截断 64 字符，NULL/TOAST 显式标注）。 */
    private static String tupleOf(TupleData tuple, PgOutputMessage.Relation relation) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < tuple.columns().size(); i++) {
            String column = i < relation.columns().size() ? relation.columns().get(i).name() : "#" + i;
            TupleValue value = tuple.columns().get(i);
            String rendered;
            if (value instanceof TupleValue.Null) {
                rendered = "NULL";
            } else if (value instanceof TupleValue.UnchangedToast) {
                rendered = "<toast-unchanged>";
            } else if (value instanceof TupleValue.Text t) {
                String s = t.value();
                rendered = s.length() > 64 ? s.substring(0, 64) + "...(" + s.length() + "B)" : s;
            } else if (value instanceof TupleValue.Binary b) {
                rendered = "0x" + HexFormat.of().formatHex(b.value());
            } else {
                throw new IllegalStateException("未知列值类型: " + value.getClass());
            }
            parts.add(column + "=" + rendered);
        }
        return parts.toString();
    }
```

需补 import：`org.vastdata.vbstream.replication.Transaction`、`TransactionListener`、`TxChange`、`RowChange`、`TruncateChange`、`MsgChange`。

- [ ] **Step 4: 改造 Main**

`Main.java` 的 try 块改为装配 assembler（其余不动）：

```java
        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            ConsoleListener console = new ConsoleListener();
            TransactionAssembler assembler = new TransactionAssembler(console);
            Thread worker = new Thread(() -> {
                try {
                    session.run((msg, registry) -> {
                        registry.accept(msg);          // 元数据先入缓存（Relation 必先于 DML）
                        assembler.accept(msg, registry); // 组装器随后消费同一消息流
                    });
                } catch (Exception e) {
                    LOG.error("复制流中断: {}（槽 {} 已保留，重启续传）", e.toString(), config.slotName(), e);
                    stop.countDown();
                }
            }, "pgoutput-reader");
            worker.start();
            stop.await();
            LOG.info("正在关闭复制流...");
        } catch (Exception e) {
```

补 import：`org.vastdata.vbstream.replication.TransactionAssembler`。

- [ ] **Step 5: 编译 + 全量已有单测回归**

Run: `mvn test -Dtest='ConsoleListenerTest,TransactionModelTest,TransactionAssemblerTest'`
Expected: PASS
Run: `mvn compile`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit + push**

```bash
git add src/main/java/org/vastdata/vbstream/ConsoleListener.java \
        src/main/java/org/vastdata/vbstream/Main.java \
        src/test/java/org/vastdata/vbstream/ConsoleListenerTest.java
git commit -m "feat(assembly): ConsoleListener 事务块输出（TXN-BEGIN/END）与 Main 组装器装配，逐消息降 DEBUG"
git push
```

---

### Task 7: 集成测试骨架 + 场景 1（普通事务）

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/it/TransactionAssemblyTest.java`

集成测试用**录制后离线回放**模式：SessionHarness 照旧录制 `PgOutputMessage`，测试线程在停止后依序回放给新 `RelationRegistry` + `TransactionAssembler`（组装器是确定性纯状态机，离线回放与在线组装结果一致；SessionHarness 零改动）。

- [ ] **Step 1: 写测试类骨架与场景 1**

```java
package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.DmlKind;
import org.vastdata.vbstream.replication.RelationRegistry;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.TransactionKind;
import org.vastdata.vbstream.replication.RowChange;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务组装集成测试：真库构造四场景（spec §6.2），录制 pgoutput 消息后离线回放给组装器，
 * 断言 Transaction 完整性。容器与槽清理复用 PgTestEnv。
 */
class TransactionAssemblyTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_assembly");
    }

    /**
     * 离线回放录制流：Relation 先经 registry（与 Main 装配顺序一致），全部消息喂新组装器，
     * 收集输出的 Transaction。回放中组装器的 fail-fast 同样会抛（等效在线校验）。
     */
    private static List<Transaction> assembleRecording(List<PgOutputMessage> messages) {
        RelationRegistry registry = new RelationRegistry();
        List<Transaction> out = new ArrayList<>();
        TransactionAssembler assembler = new TransactionAssembler(out::add);
        for (PgOutputMessage m : messages) {
            registry.accept(m);
            assembler.accept(m, registry);
        }
        return out;
    }

    @Test
    void assemblesNormalMultiStatementTransaction() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_assembly",
                "CREATE PUBLICATION pub_assembly FOR TABLE t_assembly",
                "TRUNCATE t_assembly");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_assembly", "pub_assembly"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("INSERT INTO t_assembly VALUES (1,'a'),(2,'b')");
                st.execute("UPDATE t_assembly SET v='c' WHERE id=1");
                st.execute("DELETE FROM t_assembly WHERE id=2");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), "应恰一个事务: " + txns);
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.NORMAL, t.kind());
            assertNull(t.gid());
            assertTrue(t.xid() > 0, "xid 应来自 Begin: " + t.xid());
            assertEquals(4, t.changes().size());
            List<DmlKind> dmls = t.changes().stream()
                    .map(ch -> ((RowChange) ch).dml()).toList();
            assertEquals(List.of(DmlKind.INSERT, DmlKind.INSERT, DmlKind.UPDATE, DmlKind.DELETE), dmls);
            RowChange first = (RowChange) t.changes().get(0);
            assertEquals("t_assembly", first.relation().table());
            assertEquals("1", ((TupleValue.Text) first.after().orElseThrow().columns().get(0)).value());
            assertEquals("a", ((TupleValue.Text) first.after().orElseThrow().columns().get(1)).value());
        }
    }
}
```

- [ ] **Step 2: 运行（需本机 Docker）**

Run: `mvn test -Dtest=TransactionAssemblyTest`
Expected: PASS（1 test）

- [ ] **Step 3: Commit + push**

```bash
git add src/test/java/org/vastdata/vbstream/it/TransactionAssemblyTest.java
git commit -m "test(assembly): 集成场景 1——普通多语句事务组装完整性（录制后离线回放模式）"
git push
```

---

### Task 8: 集成场景 2（流式 + 子事务回滚）与场景 4（双连接交错）

**Files:**
- Modify: `src/test/java/org/vastdata/vbstream/it/TransactionAssemblyTest.java`

- [ ] **Step 1: 追加场景 2 与场景 4**

```java
    @Test
    void streamedTransactionWithSubtransactionRollbackAssemblesCleanly() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_stream(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_assembly_stream",
                "CREATE PUBLICATION pub_assembly_stream FOR TABLE t_assembly_stream",
                "TRUNCATE t_assembly_stream");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_assembly", "pub_assembly_stream"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                // 逐行写入构造流式（单语句 INSERT..SELECT 批量不触发流式——根 CLAUDE.md 实测结论；
                // 每行 repeat(md5,512)≈32KB，第 3 行起 rb->size 越过 64kB work_mem 触发驱逐）
                for (int i = 1; i <= 30; i++) {
                    st.execute("INSERT INTO t_assembly_stream VALUES (" + i + ", repeat(md5(" + i + "::text), 512))");
                }
                // 子事务：写入后回滚（这些变更会被流式下发，再由 StreamAbort 剔除）
                st.execute("SAVEPOINT sp1");
                for (int i = 201; i <= 210; i++) {
                    st.execute("INSERT INTO t_assembly_stream VALUES (" + i + ", repeat(md5(" + i + "::text), 512))");
                }
                st.execute("ROLLBACK TO SAVEPOINT sp1");
                st.execute("INSERT INTO t_assembly_stream VALUES (999, 'tail')");
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), "应恰一个流式事务: " + txns);
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.STREAMED, t.kind());
            assertEquals(31, t.changes().size(), "存活 30 行 + 尾行，被回滚的 10 行必须被 StreamAbort 剔除");
            // 被回滚子事务的 id（201..210）不得出现
            for (Object change : t.changes()) {
                RowChange rc = (RowChange) change;
                String id = ((TupleValue.Text) rc.after().orElseThrow().columns().get(0)).value();
                assertTrue(Integer.parseInt(id) < 201 || Integer.parseInt(id) == 999,
                        "被回滚子事务的行混入: id=" + id);
            }
        }
    }

    @Test
    void twoConcurrentLargeTransactionsAssembleIndependently() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_inter(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_assembly_inter",
                "CREATE PUBLICATION pub_assembly_inter FOR TABLE t_assembly_inter",
                "TRUNCATE t_assembly_inter");
        java.util.concurrent.atomic.AtomicInteger streamCommits = new java.util.concurrent.atomic.AtomicInteger();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_assembly", "pub_assembly_inter"),
                msg -> msg instanceof PgOutputMessage.StreamCommit
                        && streamCommits.incrementAndGet() >= 2)) {
            // 双连接各自 BEGIN，交替单行写入（每行 ~32KB；单事务 10 行 320KB 独立越过 64kB work_mem，
            // 两事务必然都被驱逐流式；交替积累时全局 rb->size 超限触发轮番驱逐——spec §4.2 交错场景）
            try (Connection a = PgTestEnv.newSqlConnection(); Connection b = PgTestEnv.newSqlConnection()) {
                a.setAutoCommit(false);
                b.setAutoCommit(false);
                try (Statement sa = a.createStatement(); Statement sb = b.createStatement()) {
                    for (int i = 1; i <= 10; i++) {
                        sa.execute("INSERT INTO t_assembly_inter VALUES (" + i + ", repeat(md5(" + i + "::text), 512))");
                        sb.execute("INSERT INTO t_assembly_inter VALUES (10000" + i + ", repeat(md5(" + i + "::text), 512))");
                    }
                }
                a.commit();
                b.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(2, txns.size(), "两并发大事务各输出一次: " + txns);
            for (Transaction t : txns) {
                assertEquals(TransactionKind.STREAMED, t.kind());
                assertEquals(10, t.changes().size(), "各自 10 行完整: xid=" + t.xid());
            }
            // 两桶互不混流：每个事务的行 id 必须整组落在 1..10（A）或 100001..100010（B）之一
            for (Transaction t : txns) {
                List<Long> ids = t.changes().stream()
                        .map(ch -> Long.parseLong(((TupleValue.Text) ((RowChange) ch)
                                .after().orElseThrow().columns().get(0)).value()))
                        .toList();
                boolean isLowSet = ids.get(0) < 100;
                for (long id : ids) {
                    assertTrue((id < 100) == isLowSet, "事务内混入他桶行: xid=" + t.xid() + " id=" + id);
                }
                assertEquals(10, ids.stream().distinct().count(), "行 id 不重复");
            }
        }
    }
```

- [ ] **Step 2: 运行（需 Docker；场景 4 若偶发超时重跑一次确认非代码问题）**

Run: `mvn test -Dtest=TransactionAssemblyTest`
Expected: PASS（3 tests）

- [ ] **Step 3: Commit + push**

```bash
git add src/test/java/org/vastdata/vbstream/it/TransactionAssemblyTest.java
git commit -m "test(assembly): 集成场景 2/4——流式子事务回滚剔除 + 双连接并发大事务多桶交错"
git push
```

---

### Task 9: 集成场景 3（2PC 双路）+ 全量回归 + push

**Files:**
- Modify: `src/test/java/org/vastdata/vbstream/it/TransactionAssemblyTest.java`

- [ ] **Step 1: 追加场景 3**

```java
    @Test
    void twoPhaseCommitAndRollbackBothHandled() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_assembly_2pc(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_assembly_2pc",
                "CREATE PUBLICATION pub_assembly_2pc FOR TABLE t_assembly_2pc",
                "TRUNCATE t_assembly_2pc");
        // 停止条件：RollbackPrepared（序列中最后到达的终结消息）
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_assembly", "pub_assembly_2pc"),
                msg -> msg instanceof PgOutputMessage.RollbackPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                c.setAutoCommit(false);
                st.execute("INSERT INTO t_assembly_2pc VALUES (1,'x')");
                st.execute("PREPARE TRANSACTION 'gid_commit'");
                st.execute("INSERT INTO t_assembly_2pc VALUES (2,'y')");
                st.execute("PREPARE TRANSACTION 'gid_rollback'");
            }
            PgTestEnv.execSql("COMMIT PREPARED 'gid_commit'");
            PgTestEnv.execSql("ROLLBACK PREPARED 'gid_rollback'");
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Transaction> txns = assembleRecording(harness.messages());
            assertEquals(1, txns.size(), "仅 COMMIT PREPARED 的 gid 输出: " + txns);
            Transaction t = txns.get(0);
            assertEquals(TransactionKind.TWO_PHASE, t.kind());
            assertEquals("gid_commit", t.gid());
            assertEquals(1, t.changes().size());
            assertEquals(DmlKind.INSERT, ((RowChange) t.changes().get(0)).dml());
        }
    }
```

注意：同连接连续两个 `PREPARE TRANSACTION` 每次都隐式结束当前事务块，`PREPARE` 后无需 commit（prepared 事务已脱离会话）；`Connection` 关闭时无未决事务。若驱动对 `PREPARE TRANSACTION` 报“事务已中止”类错误，改为每条 prepare 单独 try-with-resources 连接（语义相同）。

- [ ] **Step 2: 运行本类全部 4 场景**

Run: `mvn test -Dtest=TransactionAssemblyTest`
Expected: PASS（4 tests）

- [ ] **Step 3: 全量回归（单测 + 集成，53 旧 + ~30 新）**

Run: `mvn test`
Expected: 全绿（含既有 53 个测试——本里程碑对 protocol/replication 既有代码零行为改动，唯一改动是 ConsoleListener 逐消息 INFO→DEBUG 与 Main 装配，均无既有测试断言依赖）

- [ ] **Step 4: Commit + push**

```bash
git add src/test/java/org/vastdata/vbstream/it/TransactionAssemblyTest.java
git commit -m "test(assembly): 集成场景 3——2PC 双路（COMMIT PREPARED 输出/ROLLBACK 丢弃），全量回归绿"
git push
```

---

## 验收清单（对照 spec §8 交付物）

- [ ] replication 包 9 个新文件：`Transaction`、`TransactionKind`、`TxChange`、`RowChange`、`DmlKind`、`TruncateChange`、`MsgChange`、`TransactionListener`、`TransactionAssembler`
- [ ] `ConsoleListener`/`Main` 改造（事务块输出 + 装配，逐消息降 DEBUG）
- [ ] 单测 `TransactionAssemblerTest` 覆盖 spec §6.1 十类用例；`TransactionModelTest`、`ConsoleListenerTest`
- [ ] 集成 `TransactionAssemblyTest` 覆盖 spec §6.2 四场景
- [ ] `mvn test` 全绿；spec §4.1 Commit 行修正已回写
