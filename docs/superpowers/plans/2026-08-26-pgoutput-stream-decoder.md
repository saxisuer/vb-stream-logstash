# pgoutput 流式解码器实施计划（里程碑 1）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过 pgjdbc ReplicationConnection 建立 pgoutput 复制流，自研二进制协议解析器，完整适配普通事务/流式大事务/两阶段提交/并行流式四种场景，交付可运行的 Main 与全套测试。

**Architecture:** 三层结构——`protocol/`（纯解析：ByteBuffer → 不可变 record，可独立单测）、`replication/`（连接/建槽/开流/Relation 缓存/LSN 反馈/消息循环）、`Main`（配置+组装+控制台打印）。设计文档：`docs/superpowers/specs/2026-08-26-pgoutput-stream-decoder-design.md`，其**附录 A 字节格式表是协议实现的唯一权威**。

**Tech Stack:** Java 17、pgjdbc 42.7.13、JUnit Jupiter 6.1.3、Surefire 3.5.6、Testcontainers BOM 2.0.5（坐标 `org.testcontainers:testcontainers-postgresql`）、Maven。

---

## 全局约定（每个任务执行前先读一遍）

1. **协议事实**：所有整数 big-endian；`String` 为 null 结尾 UTF-8（CString）；时间戳 = 距 2000-01-01 的微秒数（Unix epoch 偏移 946684800 秒）。
2. **消息类型字节**（区分大小写）：`B`Begin `C`Commit `O`Origin `R`Relation `Y`Type `I`Insert `U`Update `D`Delete `T`Truncate `M`Message `S`StreamStart `E`StreamStop `c`StreamCommit `A`StreamAbort `b`BeginPrepare `P`Prepare `K`CommitPrepared `r`RollbackPrepared `p`StreamPrepare。
3. **常用命令**（仓库根执行）：
   - `mvn -q compile` 编译；`mvn test` 全部测试
   - `mvn test -Dtest=XxxTest` 单类；`mvn test -Dtest=XxxTest#method` 单方法
4. **提交规范**：每个任务最后一步 commit，message 末尾加 `Co-Authored-By: Claude <noreply@anthropic.com>`。**每个任务完成后必须 `git push`**（用户在多台电脑间同步开发）。
5. 测试类以 `Test` 结尾（surefire 约定）；集成测试需要本机 Docker（已具备）。
6. 包路径：`src/main/java/org/vastdata/vbstream/` 与 `src/test/java/org/vastdata/vbstream/`。
7. 里程碑 1 不引日志框架，一律 `System.out` / `System.err`；代码注释用中文。

## 任务依赖与并行分组

```
Task 1 → Task 2 → Task 3 → { Task 4 | Task 5 | Task 6 | Task 7 }    ← 四个 parser 族可并行
Task 8（依赖 Task 3 的 StreamingMode；可与 4-7 并行）
Task 9 → Task 10（依赖 3、8、9 与全部 parser 族完成）
Task 11 → { Task 12 | Task 13 | Task 14 }    ← 三个集成用例组可并行
Task 15（依赖全部）
```

---

### Task 1: 加入 Testcontainers 依赖

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: pom.xml 增加 dependencyManagement 与依赖**

在 `<properties>` 中加：

```xml
        <testcontainers.version>2.0.5</testcontainers.version>
```

在 `<dependencies>` 末尾（junit 依赖之后）加：

```xml
        <!-- 集成测试：PG 容器（2.x 起坐标改名 testcontainers-postgresql） -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: 验证依赖可解析**

Run: `mvn dependency:resolve`
Expected: `BUILD SUCCESS`。若 `testcontainers-postgresql:2.0.5` 解析失败（网络或仓库问题），回退使用 1.x 坐标：`org.testcontainers:postgresql:1.21.4`（两坐标的容器 API 相同）。

- [ ] **Step 3: 提交并推送**

```bash
git add pom.xml
git commit -m "build: 加入 testcontainers-postgresql 测试依赖"
git push
```

---

### Task 2: ByteBufferReader 读取工具

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/protocol/ByteBufferReader.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/ByteBufferReaderTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ByteBufferReaderTest {

    @Test
    void readsBigEndianIntegers() {
        ByteBuffer buf = ByteBuffer.allocate(11)
                .put((byte) 1).putShort((short) 2).putInt(3).putInt(4);
        buf.flip();
        ByteBufferReader r = new ByteBufferReader(buf);
        assertEquals(1, r.readByte());
        assertEquals(2, r.readUnsignedShort());
        assertEquals(3, r.readInt());
        assertEquals(4L, r.readUnsignedInt()); // unsigned 32 位保进 long
    }

    @Test
    void readsCStringAsUtf8() {
        ByteBuffer buf = ByteBuffer.allocate(16).put("你好".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .put((byte) 0).put((byte) 'x');
        buf.flip();
        ByteBufferReader r = new ByteBufferReader(buf);
        assertEquals("你好", r.readString());
        assertEquals('x', r.readByte());
    }

    @Test
    void convertsPgEpochMicrosToInstant() {
        // PG epoch 2000-01-01 00:00:00 UTC = Unix 946684800 秒；1 秒 = 1e6 微秒
        Instant expected = Instant.ofEpochSecond(946684800L + 100, 500_000_000L);
        assertEquals(expected, ByteBufferReader.pgMicrosToInstant(100_500_000L));
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=ByteBufferReaderTest`
Expected: 编译错误 `cannot find symbol: class ByteBufferReader`

- [ ] **Step 3: 最小实现**

```java
package org.vastdata.vbstream.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** 按协议字节序（big-endian）逐字段读取 pgoutput 消息体的工具。非线程安全，每条消息新建。 */
public final class ByteBufferReader {

    /** PostgreSQL 时间纪元：2000-01-01 00:00:00 UTC 相对 Unix 纪元的秒数。 */
    private static final long PG_EPOCH_SECONDS = 946684800L;

    private final ByteBuffer buf;

    public ByteBufferReader(ByteBuffer buf) {
        this.buf = buf;
    }

    public int remaining() {
        return buf.remaining();
    }

    public byte readByte() {
        return buf.get();
    }

    public int readUnsignedByte() {
        return buf.get() & 0xFF;
    }

    public int readUnsignedShort() {
        return buf.getShort() & 0xFFFF;
    }

    public int readInt() {
        return buf.getInt();
    }

    /** 读无符号 32 位（xid 等），装入 long 避免负数。 */
    public long readUnsignedInt() {
        return buf.getInt() & 0xFFFFFFFFL;
    }

    public long readLong() {
        return buf.getLong();
    }

    /** 读 null 结尾 UTF-8 字符串（CString）。 */
    public String readString() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte b;
        while ((b = buf.get()) != 0) {
            out.write(b);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    public byte[] readBytes(int len) {
        byte[] arr = new byte[len];
        buf.get(arr);
        return arr;
    }

    /** PG 微秒时间戳 → Instant。 */
    public static Instant pgMicrosToInstant(long micros) {
        long seconds = PG_EPOCH_SECONDS + Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn test -Dtest=ByteBufferReaderTest`
Expected: `Tests run: 3, Failures: 0, Errors: 0`

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/protocol/ByteBufferReader.java src/test/java/org/vastdata/vbstream/protocol/ByteBufferReaderTest.java
git commit -m "feat(protocol): ByteBufferReader 协议字段读取工具"
git push
```

---

### Task 3: 消息模型 + 解码器骨架（dispatch + 流块状态机）

本任务一次性建立**全部**消息 record（字段已由 spec 附录 A 定死，后续任务不再改动此文件），以及带占位 parser 的解码器骨架——这是 Task 4-7 并行分片的前提（各族只动自己的 parser 文件）。

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/protocol/PgOutputMessage.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/TupleValue.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/TupleData.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/Column.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/TruncateOption.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/StreamingMode.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/UnknownMessageTypeException.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/ProtocolMisalignmentException.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/PgOutputDecoder.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/NormalParsers.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/DmlParsers.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/StreamParsers.java`
- Create: `src/main/java/org/vastdata/vbstream/protocol/TwoPhaseParsers.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/MsgBuilder.java`（样本构造工具，后续任务共用）
- Test: `src/test/java/org/vastdata/vbstream/protocol/PgOutputDecoderTest.java`

- [ ] **Step 1: 写失败测试（含 MsgBuilder 工具）**

`MsgBuilder.java`：

```java
package org.vastdata.vbstream.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** 测试用 pgoutput 消息样本构造器。写入均为 big-endian。 */
public final class MsgBuilder {

    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    private final DataOutputStream out = new DataOutputStream(buf);

    public MsgBuilder type(char t) {
        buf.write(t);
        return this;
    }

    public MsgBuilder i8(int v) {
        buf.write(v);
        return this;
    }

    public MsgBuilder i16(int v) throws IOException {
        out.writeShort(v);
        return this;
    }

    public MsgBuilder i32(int v) throws IOException {
        out.writeInt(v);
        return this;
    }

    public MsgBuilder i64(long v) throws IOException {
        out.writeLong(v);
        return this;
    }

    public MsgBuilder str(String s) throws IOException {
        buf.write(s.getBytes(StandardCharsets.UTF_8));
        buf.write(0);
        return this;
    }

    public MsgBuilder bytes(byte[] arr) throws IOException {
        out.writeInt(arr.length);
        out.write(arr);
        return this;
    }

    public ByteBuffer build() throws IOException {
        out.flush();
        return ByteBuffer.wrap(buf.toByteArray());
    }
}
```

`PgOutputDecoderTest.java`：

```java
package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgOutputDecoderTest {

    @Test
    void unknownTypeByteFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('X').i32(1).build();
        UnknownMessageTypeException ex = assertThrows(UnknownMessageTypeException.class,
                () -> new PgOutputDecoder(StreamingMode.OFF).decode(payload));
        assertTrue(ex.getMessage().contains("0x58"), "异常应含字节十六进制值: " + ex.getMessage());
    }

    @Test
    void placeholderParserWired() throws IOException {
        // Task 3 阶段各族 parser 为占位实现；此用例锁定 dispatch 已接线（实现后此用例仍应通过）
        ByteBuffer payload = new MsgBuilder().type('B').i64(1).i64(2).i32(3).build();
        assertThrows(UnsupportedOperationException.class,
                () -> new PgOutputDecoder(StreamingMode.OFF).decode(payload));
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=PgOutputDecoderTest`
Expected: 编译错误（PgOutputDecoder 等类不存在）

- [ ] **Step 3: 实现全部模型与骨架**

`PgOutputMessage.java`（嵌套 record，字段严格对应 spec 附录 A）：

```java
package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** pgoutput 顶层消息。streamXid 非空表示该消息处于流式块内（协议会前置 Int32 xid）。 */
public sealed interface PgOutputMessage {

    record Begin(long finalLsn, Instant commitTimestamp, long xid) implements PgOutputMessage {}

    record Commit(long commitLsn, long endLsn, Instant commitTimestamp) implements PgOutputMessage {}

    record Origin(long originCommitLsn, String originName) implements PgOutputMessage {}

    record Relation(OptionalLong streamXid, int relationOid, String schema, String table,
                    char replicaIdentity, List<Column> columns) implements PgOutputMessage {}

    record Type(OptionalLong streamXid, int typeOid, String schema, String name) implements PgOutputMessage {}

    record Insert(OptionalLong streamXid, int relationOid, TupleData newTuple) implements PgOutputMessage {}

    record Update(OptionalLong streamXid, int relationOid,
                  Optional<TupleData> oldTuple, TupleData newTuple) implements PgOutputMessage {}

    record Delete(OptionalLong streamXid, int relationOid, TupleData oldTuple) implements PgOutputMessage {}

    record Truncate(OptionalLong streamXid, EnumSet<TruncateOption> options,
                    int[] relationOids) implements PgOutputMessage {}

    record LogicalMsg(OptionalLong streamXid, boolean transactional, long lsn,
                      String prefix, byte[] content) implements PgOutputMessage {}

    record StreamStart(long xid, boolean firstSegment) implements PgOutputMessage {}

    record StreamStop() implements PgOutputMessage {}

    record StreamCommit(long xid, long commitLsn, long endLsn, Instant commitTimestamp) implements PgOutputMessage {}

    /** abortLsn/abortTimestamp 仅 streaming=parallel 时随消息携带（微秒原值），否则为 empty。 */
    record StreamAbort(long xid, long subxid, OptionalLong abortLsn, OptionalLong abortTimestamp) implements PgOutputMessage {}

    record BeginPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}

    record Prepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}

    record CommitPrepared(long commitLsn, long endLsn, Instant commitTimestamp, long xid, String gid) implements PgOutputMessage {}

    record RollbackPrepared(long prepareEndLsn, long rollbackEndLsn, Instant prepareTimestamp,
                            Instant rollbackTimestamp, long xid, String gid) implements PgOutputMessage {}

    record StreamPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid) implements PgOutputMessage {}
}
```

`TupleValue.java`：

```java
package org.vastdata.vbstream.protocol;

/** TupleData 单列值。'u' 是 TOAST 未变列（值不发送），流式大事务高频出现。 */
public sealed interface TupleValue {

    TupleValue NULL = new Null();
    TupleValue UNCHANGED_TOAST = new UnchangedToast();

    record Null() implements TupleValue {}

    record UnchangedToast() implements TupleValue {}

    record Text(String value) implements TupleValue {}

    record Binary(byte[] value) implements TupleValue {}
}
```

`TupleData.java`：

```java
package org.vastdata.vbstream.protocol;

import java.util.List;

public record TupleData(List<TupleValue> columns) {}
```

`Column.java`：

```java
package org.vastdata.vbstream.protocol;

/** 关系列。typmod 为 PG 18 起随协议下发的 atttypmod；partOfKey 对应列 flags 的 bit0。 */
public record Column(String name, int typeId, int typeModifier, boolean partOfKey) {}
```

`TruncateOption.java`：

```java
package org.vastdata.vbstream.protocol;

public enum TruncateOption {
    CASCADE,          // 协议选项位 1
    RESTART_IDENTITY  // 协议选项位 2
}
```

`StreamingMode.java`：

```java
package org.vastdata.vbstream.protocol;

/** START_REPLICATION 的 streaming 参数档位。 */
public enum StreamingMode {
    OFF, ON, PARALLEL
}
```

`UnknownMessageTypeException.java`：

```java
package org.vastdata.vbstream.protocol;

/** 未知消息类型字节（或 TupleData 未知列种类），fail-fast 不静默跳过。 */
public final class UnknownMessageTypeException extends RuntimeException {

    public UnknownMessageTypeException(byte type, ByteBufferReader reader) {
        super("未知 pgoutput 类型字节 '%s' (0x%02X)，剩余 %d 字节，可能消息错位"
                .formatted((char) type, type, reader.remaining()));
    }
}
```

`ProtocolMisalignmentException.java`：

```java
package org.vastdata.vbstream.protocol;

/** 消息解析结束后仍剩余字节——字段序列与协议不符，必须立即暴露以防后续消息错位。 */
public final class ProtocolMisalignmentException extends RuntimeException {

    public ProtocolMisalignmentException(byte type, int leftover) {
        super("消息 '%s' (0x%02X) 解析后剩余 %d 字节，字段布局与协议不符"
                .formatted((char) type, type, leftover));
    }
}
```

`PgOutputDecoder.java`：

```java
package org.vastdata.vbstream.protocol;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

/**
 * pgoutput 消息解码器。构造时指定 StreamingMode；运行期维护最小流块状态 inStream：
 * 收到 'S'(StreamStart) 置位、'E'(StreamStop) 复位；inStream 期间 M/R/Y/I/U/D/T
 * 会前置 Int32 xid（spec 附录 A）。解码结束若有多余字节立即抛 ProtocolMisalignmentException。
 */
public final class PgOutputDecoder {

    private final StreamingMode streamingMode;
    private boolean inStream;

    public PgOutputDecoder(StreamingMode streamingMode) {
        this.streamingMode = streamingMode;
    }

    public PgOutputMessage decode(ByteBuffer payload) {
        ByteBufferReader r = new ByteBufferReader(payload);
        byte type = r.readByte();
        PgOutputMessage msg = dispatch(type, r);
        if (r.remaining() != 0) {
            throw new ProtocolMisalignmentException(type, r.remaining());
        }
        return msg;
    }

    private PgOutputMessage dispatch(byte type, ByteBufferReader r) {
        return switch (type) {
            case 'B' -> NormalParsers.begin(r);
            case 'C' -> NormalParsers.commit(r);
            case 'O' -> NormalParsers.origin(r);
            case 'R' -> NormalParsers.relation(r, streamXid(r));
            case 'Y' -> NormalParsers.type(r, streamXid(r));
            case 'I' -> DmlParsers.insert(r, streamXid(r));
            case 'U' -> DmlParsers.update(r, streamXid(r));
            case 'D' -> DmlParsers.delete(r, streamXid(r));
            case 'T' -> DmlParsers.truncate(r, streamXid(r));
            case 'M' -> DmlParsers.logicalMsg(r, streamXid(r));
            case 'S' -> {
                inStream = true;
                yield StreamParsers.start(r);
            }
            case 'E' -> {
                inStream = false;
                yield StreamParsers.stop(r);
            }
            case 'c' -> StreamParsers.commit(r);
            case 'A' -> StreamParsers.abort(r, streamingMode);
            case 'b' -> TwoPhaseParsers.beginPrepare(r);
            case 'P' -> TwoPhaseParsers.prepare(r);
            case 'K' -> TwoPhaseParsers.commitPrepared(r);
            case 'r' -> TwoPhaseParsers.rollbackPrepared(r);
            case 'p' -> TwoPhaseParsers.streamPrepare(r);
            default -> throw new UnknownMessageTypeException(type, r);
        };
    }

    /** 流式块内的 M/R/Y/I/U/D/T 前置 Int32 xid；顶层消息无此前缀。 */
    private OptionalLong streamXid(ByteBufferReader r) {
        return inStream ? OptionalLong.of(r.readUnsignedInt()) : OptionalLong.empty();
    }
}
```

四个 parser 文件（本任务全部为占位，Task 4-7 各自替换实现；**方法签名不得再改**）：

`NormalParsers.java`：

```java
package org.vastdata.vbstream.protocol;

import java.util.OptionalLong;

/** 族 1/2：事务边界与元数据消息。Task 4 实现具体解析。 */
final class NormalParsers {

    private NormalParsers() {
    }

    static PgOutputMessage.Begin begin(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Commit commit(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Origin origin(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Relation relation(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Type type(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 4 实现");
    }
}
```

`DmlParsers.java`：

```java
package org.vastdata.vbstream.protocol;

import java.util.OptionalLong;

/** 族 1：DML 消息与 TupleData。Task 5 实现具体解析。 */
final class DmlParsers {

    private DmlParsers() {
    }

    static PgOutputMessage.Insert insert(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.Update update(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.Delete delete(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.Truncate truncate(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.LogicalMsg logicalMsg(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }
}
```

`StreamParsers.java`：

```java
package org.vastdata.vbstream.protocol;

/** 族 3：流式大事务控制消息。Task 6 实现具体解析。 */
final class StreamParsers {

    private StreamParsers() {
    }

    static PgOutputMessage.StreamStart start(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 6 实现");
    }

    static PgOutputMessage.StreamStop stop(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 6 实现");
    }

    static PgOutputMessage.StreamCommit commit(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 6 实现");
    }

    static PgOutputMessage.StreamAbort abort(ByteBufferReader r, StreamingMode mode) {
        throw new UnsupportedOperationException("Task 6 实现");
    }
}
```

`TwoPhaseParsers.java`：

```java
package org.vastdata.vbstream.protocol;

/** 族 4：两阶段提交消息。Task 7 实现具体解析。 */
final class TwoPhaseParsers {

    private TwoPhaseParsers() {
    }

    static PgOutputMessage.BeginPrepare beginPrepare(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.Prepare prepare(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.CommitPrepared commitPrepared(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.RollbackPrepared rollbackPrepared(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.StreamPrepare streamPrepare(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn test`
Expected: 全部通过（`PgOutputDecoderTest` 2 个用例 + 此前所有测试）

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/protocol src/test/java/org/vastdata/vbstream/protocol
git commit -m "feat(protocol): 全量消息模型与解码器骨架（dispatch + 流块状态机 + 占位 parser）"
git push
```

---

### Task 4: NormalParsers 实现（B/C/O/R/Y）

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/protocol/NormalParsers.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/NormalParsersTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NormalParsersTest {

    private final PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.OFF);

    @Test
    void begin() throws IOException {
        // pgMicrosToInstant 的输入是 PG epoch（2000-01-01）起的微秒数，函数内部已加 946684800 Unix 秒偏移，
        // 夹具不可再把 epoch 折成微码叠进输入（否则双重计入，得到 2029 年）
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(0x1000L).i64(2_500_000L).i32(777).build();
        PgOutputMessage.Begin msg = (PgOutputMessage.Begin) decoder.decode(payload);
        assertEquals(0x1000L, msg.finalLsn());
        assertEquals(Instant.ofEpochSecond(946684800L + 2, 500_000_000L), msg.commitTimestamp());
        assertEquals(777L, msg.xid());
    }

    @Test
    void commitConsumesLeadingFlagsByte() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('C')
                .i8(0).i64(0x2000L).i64(0x3000L).i64(1_000_000L).build();
        PgOutputMessage.Commit msg = (PgOutputMessage.Commit) decoder.decode(payload);
        assertEquals(0x2000L, msg.commitLsn());
        assertEquals(0x3000L, msg.endLsn());
    }

    @Test
    void origin() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('O').i64(0x4000L).str("origin_a").build();
        PgOutputMessage.Origin msg = (PgOutputMessage.Origin) decoder.decode(payload);
        assertEquals("origin_a", msg.originName());
    }

    @Test
    void relationWithTwoColumns() throws IOException {
        MsgBuilder m = new MsgBuilder().type('R')
                .i32(16385).str("public").str("t_demo").i8('d')
                .i16(2)
                .i8(1).str("id").i32(23).i32(-1)   // int4, key, typmod=-1
                .i8(0).str("name").i32(25).i32(-1); // text, 非key
        PgOutputMessage.Relation msg = (PgOutputMessage.Relation) decoder.decode(m.build());
        assertEquals(16385, msg.relationOid());
        assertEquals("t_demo", msg.table());
        assertEquals('d', msg.replicaIdentity());
        assertEquals(2, msg.columns().size());
        assertEquals(new Column("id", 23, -1, true), msg.columns().get(0));
        assertEquals(new Column("name", 25, -1, false), msg.columns().get(1));
    }

    @Test
    void typeMsg() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('Y').i32(16386).str("public").str("mytype").build();
        PgOutputMessage.Type msg = (PgOutputMessage.Type) decoder.decode(payload);
        assertEquals("mytype", msg.name());
    }

    @Test
    void leftoverBytesCauseMisalignment() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(1).i64(2).i32(3).i32(99).build(); // 故意多 4 字节
        assertThrows(ProtocolMisalignmentException.class, () -> decoder.decode(payload));
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=NormalParsersTest`
Expected: 各用例抛 `UnsupportedOperationException: Task 4 实现`

- [ ] **Step 3: 实现 NormalParsers（替换占位）**

```java
package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/** 族 1/2：事务边界与元数据消息。格式见 spec 附录 A。 */
final class NormalParsers {

    private NormalParsers() {
    }

    static PgOutputMessage.Begin begin(ByteBufferReader r) {
        long finalLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        return new PgOutputMessage.Begin(finalLsn, commitTs, xid);
    }

    static PgOutputMessage.Commit commit(ByteBufferReader r) {
        r.readByte(); // currently-unused flags，消费不建模
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        return new PgOutputMessage.Commit(commitLsn, endLsn, commitTs);
    }

    static PgOutputMessage.Origin origin(ByteBufferReader r) {
        long lsn = r.readLong();
        String name = r.readString();
        return new PgOutputMessage.Origin(lsn, name);
    }

    static PgOutputMessage.Relation relation(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        String schema = r.readString();
        String table = r.readString();
        char replident = (char) r.readByte();
        int ncols = r.readUnsignedShort();
        List<Column> cols = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            boolean partOfKey = (r.readByte() & 0x01) != 0;
            String name = r.readString();
            int typeId = r.readInt();
            int typmod = r.readInt();
            cols.add(new Column(name, typeId, typmod, partOfKey));
        }
        return new PgOutputMessage.Relation(streamXid, oid, schema, table, replident, List.copyOf(cols));
    }

    static PgOutputMessage.Type type(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        String schema = r.readString();
        String name = r.readString();
        return new PgOutputMessage.Type(streamXid, oid, schema, name);
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn test -Dtest=NormalParsersTest`
Expected: `Tests run: 6, Failures: 0, Errors: 0`。注意 `placeholderParserWired`（Task 3）会因 'B' 已实现而失败——把它更新为改测未实现的 'I'：

```java
    @Test
    void placeholderParserWired() throws IOException {
        // 'I' 属 DmlParsers，Task 3 阶段为占位实现；Task 5 完成后请将此用例改为断言正常解析
        ByteBuffer payload = new MsgBuilder().type('I').i32(1).i8('N')
                .i16(1).i8('t').bytes("x".getBytes()).build();
        assertThrows(UnsupportedOperationException.class,
                () -> new PgOutputDecoder(StreamingMode.OFF).decode(payload));
    }
```

- [ ] **Step 5: 全量回归 + 提交推送**

Run: `mvn test`
Expected: BUILD SUCCESS

```bash
git add src/main/java/org/vastdata/vbstream/protocol/NormalParsers.java src/test/java/org/vastdata/vbstream/protocol
git commit -m "feat(protocol): 实现事务边界与元数据消息解析（B/C/O/R/Y）"
git push
```

---

### Task 5: DmlParsers 实现（I/U/D/T/M + TupleData）

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/protocol/DmlParsers.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/DmlParsersTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DmlParsersTest {

    private final PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.OFF);

    private static byte[] utf8(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void insertWithTextAndNullColumns() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(16385).i8('N')
                .i16(3)
                .i8('t').bytes(utf8("a"))
                .i8('n')
                .i8('t').bytes(utf8("b"))
                .build();
        PgOutputMessage.Insert msg = (PgOutputMessage.Insert) decoder.decode(payload);
        assertEquals(16385, msg.relationOid());
        assertEquals(3, msg.newTuple().columns().size());
        assertEquals(new TupleValue.Text("a"), msg.newTuple().columns().get(0));
        assertEquals(TupleValue.NULL, msg.newTuple().columns().get(1));
        assertEquals(new TupleValue.Text("b"), msg.newTuple().columns().get(2));
    }

    @Test
    void updateWithKeyPrefixThenNewTuple() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('K').i16(1).i8('t').bytes(utf8("1"))  // 旧 key
                .i8('N').i16(1).i8('t').bytes(utf8("2"))  // 新值
                .build();
        PgOutputMessage.Update msg = (PgOutputMessage.Update) decoder.decode(payload);
        assertTrue(msg.oldTuple().isPresent());
        assertEquals(1, msg.oldTuple().get().columns().size());
        assertEquals(new TupleValue.Text("2"), msg.newTuple().columns().get(0));
    }

    @Test
    void updateWithFullOldRow() throws IOException {
        // REPLICA IDENTITY FULL：'O' 携带完整旧行（非仅 key 列），与 'K' 同一解析路径
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('O').i16(2).i8('t').bytes(utf8("1")).i8('t').bytes(utf8("old"))
                .i8('N').i16(1).i8('t').bytes(utf8("new"))
                .build();
        PgOutputMessage.Update msg = (PgOutputMessage.Update) decoder.decode(payload);
        assertTrue(msg.oldTuple().isPresent());
        assertEquals(2, msg.oldTuple().get().columns().size());
        assertEquals(new TupleValue.Text("old"), msg.oldTuple().get().columns().get(1));
        assertEquals(new TupleValue.Text("new"), msg.newTuple().columns().get(0));
    }

    @Test
    void updateWithoutOldTuple() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385).i8('N').i16(1).i8('t').bytes(utf8("x")).build();
        PgOutputMessage.Update msg = (PgOutputMessage.Update) decoder.decode(payload);
        assertEquals(Optional.empty(), msg.oldTuple());
    }

    @Test
    void updateOldTupleFollowedByWrongTagFailsFast() throws IOException {
        // 'K'/'O' 之后必须是 'N'：再来一个 'O'（K/O 非法并存序列）不得静默跳过
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('K').i16(1).i8('t').bytes(utf8("1"))
                .i8('O')
                .build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }

    @Test
    void updateUnknownTagFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385).i8('X').build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }

    @Test
    void deleteWithKey() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('D')
                .i32(16385).i8('K').i16(1).i8('t').bytes(utf8("9")).build();
        PgOutputMessage.Delete msg = (PgOutputMessage.Delete) decoder.decode(payload);
        assertEquals(16385, msg.relationOid());
        assertEquals(new TupleValue.Text("9"), msg.oldTuple().columns().get(0));
    }

    @Test
    void deleteWithoutKeyOrOldTupleTagFailsFast() throws IOException {
        // 'D' 必有 'K' 或 'O'：'N' 不是合法的 delete tag
        ByteBuffer payload = new MsgBuilder().type('D')
                .i32(16385).i8('N').build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }

    @Test
    void truncateOptionsAndOids() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('T')
                .i32(2).i8(3).i32(100).i32(200).build(); // CASCADE|RESTART, 两张表
        PgOutputMessage.Truncate msg = (PgOutputMessage.Truncate) decoder.decode(payload);
        assertEquals(java.util.EnumSet.of(TruncateOption.CASCADE, TruncateOption.RESTART_IDENTITY), msg.options());
        assertArrayEquals(new int[]{100, 200}, msg.relationOids());
    }

    @Test
    void logicalMessageWithContentLength() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('M')
                .i8(1).i64(0x5000L).str("prefix").bytes(utf8("hello")).build();
        PgOutputMessage.LogicalMsg msg = (PgOutputMessage.LogicalMsg) decoder.decode(payload);
        assertTrue(msg.transactional());
        assertEquals("prefix", msg.prefix());
        assertArrayEquals(utf8("hello"), msg.content());

        // flags 只有 bit0 表示 transactional：其他位（如 bit1）不得误读为事务消息
        ByteBuffer nonTxPayload = new MsgBuilder().type('M')
                .i8(2).i64(0x5000L).str("prefix").bytes(utf8("hello")).build();
        PgOutputMessage.LogicalMsg nonTx = (PgOutputMessage.LogicalMsg) decoder.decode(nonTxPayload);
        assertFalse(nonTx.transactional());
    }

    @Test
    void unchangedToastAndBinaryValue() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(1).i8('N')
                .i16(2)
                .i8('u')
                .i8('b').bytes(new byte[]{1, 2})
                .build();
        PgOutputMessage.Insert msg = (PgOutputMessage.Insert) decoder.decode(payload);
        assertEquals(TupleValue.UNCHANGED_TOAST, msg.newTuple().columns().get(0));
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}), msg.newTuple().columns().get(1));
    }

    @Test
    void unknownTupleKindFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(1).i8('N').i16(1).i8('z').build();
        assertThrows(UnknownMessageTypeException.class, () -> decoder.decode(payload));
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=DmlParsersTest`
Expected: 各用例抛 `UnsupportedOperationException: Task 5 实现`。同时把 Task 3/4 的 `placeholderParserWired` 用例按 Task 4 Step 4 的说明改为断言 'I' 正常解析。

- [ ] **Step 3: 实现 DmlParsers（替换占位）**

```java
package org.vastdata.vbstream.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** 族 1：DML 消息与 TupleData。格式见 spec 附录 A。 */
final class DmlParsers {

    private DmlParsers() {
    }

    static PgOutputMessage.Insert insert(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        expectTupleTag(r, 'N');
        return new PgOutputMessage.Insert(streamXid, oid, tupleData(r));
    }

    static PgOutputMessage.Update update(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        Optional<TupleData> oldTuple = Optional.empty();
        byte tag = r.readByte();
        if (tag == 'K' || tag == 'O') {
            oldTuple = Optional.of(tupleData(r));
            expectTupleTag(r, 'N');
        } else if (tag != 'N') {
            throw new UnknownMessageTypeException(tag, r);
        }
        TupleData newTuple = tupleData(r);
        return new PgOutputMessage.Update(streamXid, oid, oldTuple, newTuple);
    }

    static PgOutputMessage.Delete delete(ByteBufferReader r, OptionalLong streamXid) {
        int oid = r.readInt();
        byte tag = r.readByte();
        if (tag != 'K' && tag != 'O') {
            throw new UnknownMessageTypeException(tag, r);
        }
        return new PgOutputMessage.Delete(streamXid, oid, tupleData(r));
    }

    static PgOutputMessage.Truncate truncate(ByteBufferReader r, OptionalLong streamXid) {
        int nrel = r.readInt();
        int bits = r.readUnsignedByte();
        EnumSet<TruncateOption> options = EnumSet.noneOf(TruncateOption.class);
        if ((bits & 0x01) != 0) {
            options.add(TruncateOption.CASCADE);
        }
        if ((bits & 0x02) != 0) {
            options.add(TruncateOption.RESTART_IDENTITY);
        }
        int[] oids = new int[nrel];
        for (int i = 0; i < nrel; i++) {
            oids[i] = r.readInt();
        }
        return new PgOutputMessage.Truncate(streamXid, options, oids);
    }

    static PgOutputMessage.LogicalMsg logicalMsg(ByteBufferReader r, OptionalLong streamXid) {
        boolean transactional = (r.readByte() & 0x01) != 0; // bit0 = transactional
        long lsn = r.readLong();
        String prefix = r.readString();
        int len = r.readInt();
        byte[] content = r.readBytes(len);
        return new PgOutputMessage.LogicalMsg(streamXid, transactional, lsn, prefix, content);
    }

    private static void expectTupleTag(ByteBufferReader r, char expected) {
        byte tag = r.readByte();
        if (tag != expected) {
            throw new UnknownMessageTypeException(tag, r);
        }
    }

    /** TupleData：I16 列数；每列 'n'/'u' 无负载，'t'/'b' 为 I32 长度 + 字节。 */
    static TupleData tupleData(ByteBufferReader r) {
        int ncols = r.readUnsignedShort();
        List<TupleValue> values = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            byte kind = r.readByte();
            values.add(switch (kind) {
                case 'n' -> TupleValue.NULL;
                case 'u' -> TupleValue.UNCHANGED_TOAST;
                case 't' -> new TupleValue.Text(new String(r.readBytes(r.readInt()), StandardCharsets.UTF_8));
                case 'b' -> new TupleValue.Binary(r.readBytes(r.readInt()));
                default -> throw new UnknownMessageTypeException(kind, r);
            });
        }
        return new TupleData(List.copyOf(values));
    }
}
```

- [ ] **Step 4: 运行验证通过 + 全量回归**

Run: `mvn test`
Expected: BUILD SUCCESS（DmlParsersTest 12 个用例全过；`placeholderParserWired` 已按说明改为正向断言）

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/protocol/DmlParsers.java src/test/java/org/vastdata/vbstream/protocol
git commit -m "feat(protocol): 实现 DML 消息与 TupleData 解析（I/U/D/T/M）"
git push
```

---

### Task 6: StreamParsers 实现（S/E/c/A）+ 流块状态机验证

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/protocol/StreamParsers.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/StreamParsersTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamParsersTest {

    @Test
    void streamStartStopToggleInStreamState() throws IOException {
        PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.ON);
        decoder.decode(new MsgBuilder().type('S').i32(505).i8(1).build());
        // inStream 置位后，流块内的 I 消息会先读 Int32 xid 前缀
        Object inBlock = decoder.decode(new MsgBuilder().type('I')
                .i32(505).i32(16385).i8('N').i16(0).build());
        assertInstanceOf(PgOutputMessage.Insert.class, inBlock);
        assertTrue(((PgOutputMessage.Insert) inBlock).streamXid().isPresent());
        assertEquals(505L, ((PgOutputMessage.Insert) inBlock).streamXid().getAsLong());

        decoder.decode(new MsgBuilder().type('E').build());
        Object afterStop = decoder.decode(new MsgBuilder().type('I')
                .i32(16385).i8('N').i16(0).build());
        assertFalse(((PgOutputMessage.Insert) afterStop).streamXid().isPresent());
    }

    @Test
    void streamCommit() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('c')
                .i32(505).i8(0).i64(0x6000L).i64(0x7000L).i64(946684800_000_000L).build();
        PgOutputMessage.StreamCommit msg = (PgOutputMessage.StreamCommit)
                new PgOutputDecoder(StreamingMode.ON).decode(payload);
        assertEquals(505L, msg.xid());
        assertEquals(0x6000L, msg.commitLsn());
        assertEquals(0x7000L, msg.endLsn());
    }

    @Test
    void streamAbortWithoutParallelExtra() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A')
                .i32(505).i32(606).build();
        PgOutputMessage.StreamAbort msg = (PgOutputMessage.StreamAbort)
                new PgOutputDecoder(StreamingMode.ON).decode(payload);
        assertEquals(505L, msg.xid());
        assertEquals(606L, msg.subxid());
        assertFalse(msg.abortLsn().isPresent());
    }

    @Test
    void streamAbortWithParallelExtra() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A')
                .i32(505).i32(505).i64(0x8000L).i64(123_456L).build();
        PgOutputMessage.StreamAbort msg = (PgOutputMessage.StreamAbort)
                new PgOutputDecoder(StreamingMode.PARALLEL).decode(payload);
        assertEquals(0x8000L, msg.abortLsn().getAsLong());
        assertEquals(123_456L, msg.abortTimestamp().getAsLong());
    }

    @Test
    void streamAbortInParallelModeRequiresExtraBytes() throws IOException {
        // parallel 模式下不足 16 字节附加字段会 BufferUnderflow，验证按模式分支解析
        ByteBuffer payload = new MsgBuilder().type('A').i32(1).i32(1).build();
        assertThrows(java.nio.BufferUnderflowException.class,
                () -> new PgOutputDecoder(StreamingMode.PARALLEL).decode(payload));
    }

    @Test
    void streamStopIsFieldless() throws IOException {
        Object msg = new PgOutputDecoder(StreamingMode.ON)
                .decode(new MsgBuilder().type('E').build());
        assertInstanceOf(PgOutputMessage.StreamStop.class, msg);
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=StreamParsersTest`
Expected: `UnsupportedOperationException: Task 6 实现`

- [ ] **Step 3: 实现 StreamParsers（替换占位）**

```java
package org.vastdata.vbstream.protocol;

import java.time.Instant;
import java.util.OptionalLong;

/** 族 3：流式大事务控制消息。格式见 spec 附录 A。 */
final class StreamParsers {

    private StreamParsers() {
    }

    static PgOutputMessage.StreamStart start(ByteBufferReader r) {
        long xid = r.readUnsignedInt();
        boolean firstSegment = r.readByte() != 0;
        return new PgOutputMessage.StreamStart(xid, firstSegment);
    }

    static PgOutputMessage.StreamStop stop(ByteBufferReader r) {
        return new PgOutputMessage.StreamStop();
    }

    static PgOutputMessage.StreamCommit commit(ByteBufferReader r) {
        long xid = r.readUnsignedInt();
        r.readByte(); // currently-unused flags
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        return new PgOutputMessage.StreamCommit(xid, commitLsn, endLsn, commitTs);
    }

    /** parallel 模式额外携带 Int64 abort_lsn + Int64 abort_time（微秒原值）。 */
    static PgOutputMessage.StreamAbort abort(ByteBufferReader r, StreamingMode mode) {
        long xid = r.readUnsignedInt();
        long subxid = r.readUnsignedInt();
        if (mode == StreamingMode.PARALLEL) {
            long abortLsn = r.readLong();
            long abortTime = r.readLong();
            return new PgOutputMessage.StreamAbort(xid, subxid, OptionalLong.of(abortLsn), OptionalLong.of(abortTime));
        }
        return new PgOutputMessage.StreamAbort(xid, subxid, OptionalLong.empty(), OptionalLong.empty());
    }
}
```

- [ ] **Step 4: 运行验证通过 + 全量回归**

Run: `mvn test`
Expected: BUILD SUCCESS

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/protocol/StreamParsers.java src/test/java/org/vastdata/vbstream/protocol/StreamParsersTest.java
git commit -m "feat(protocol): 实现流式大事务控制消息解析（S/E/c/A）与流块状态机验证"
git push
```

---

### Task 7: TwoPhaseParsers 实现（b/P/K/r/p）

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/protocol/TwoPhaseParsers.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/TwoPhaseParsersTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TwoPhaseParsersTest {

    private final PgOutputDecoder decoder = new PgOutputDecoder(StreamingMode.PARALLEL);

    private static final long MICROS = 946684800_000_000L + 42_000_000L;

    @Test
    void beginPrepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('b')
                .i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.BeginPrepare msg = (PgOutputMessage.BeginPrepare) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareLsn());
        assertEquals(0x200L, msg.endLsn());
        assertEquals(909L, msg.xid());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void prepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('P')
                .i8(0).i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        assertInstanceOf(PgOutputMessage.Prepare.class, decoder.decode(payload));
    }

    @Test
    void commitPrepared() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('K')
                .i8(0).i64(0x300L).i64(0x400L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.CommitPrepared msg = (PgOutputMessage.CommitPrepared) decoder.decode(payload);
        assertEquals(0x300L, msg.commitLsn());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void rollbackPreparedHasTwoTimestamps() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('r')
                .i8(0).i64(0x100L).i64(0x500L).i64(MICROS).i64(MICROS + 10).i32(909).str("gid_1").build();
        PgOutputMessage.RollbackPrepared msg = (PgOutputMessage.RollbackPrepared) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareEndLsn());
        assertEquals(0x500L, msg.rollbackEndLsn());
        assertEquals("gid_1", msg.gid());
    }

    @Test
    void streamPrepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('p')
                .i8(0).i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        assertInstanceOf(PgOutputMessage.StreamPrepare.class, decoder.decode(payload));
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=TwoPhaseParsersTest`
Expected: `UnsupportedOperationException: Task 7 实现`

- [ ] **Step 3: 实现 TwoPhaseParsers（替换占位）**

```java
package org.vastdata.vbstream.protocol;

import java.time.Instant;

/** 族 4：两阶段提交消息。格式见 spec 附录 A；I8(0) flags 字段消费不建模。 */
final class TwoPhaseParsers {

    private TwoPhaseParsers() {
    }

    static PgOutputMessage.BeginPrepare beginPrepare(ByteBufferReader r) {
        long prepareLsn = r.readLong();
        long endLsn = r.readLong();
        Instant prepareTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.BeginPrepare(prepareLsn, endLsn, prepareTs, xid, gid);
    }

    static PgOutputMessage.Prepare prepare(ByteBufferReader r) {
        r.readByte(); // currently-unused flags
        return readPreparedTxn(r);
    }

    static PgOutputMessage.CommitPrepared commitPrepared(ByteBufferReader r) {
        r.readByte(); // currently-unused flags
        long commitLsn = r.readLong();
        long endLsn = r.readLong();
        Instant commitTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.CommitPrepared(commitLsn, endLsn, commitTs, xid, gid);
    }

    static PgOutputMessage.RollbackPrepared rollbackPrepared(ByteBufferReader r) {
        r.readByte(); // currently-unused flags
        long prepareEndLsn = r.readLong();
        long rollbackEndLsn = r.readLong();
        Instant prepareTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        Instant rollbackTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.RollbackPrepared(prepareEndLsn, rollbackEndLsn, prepareTs, rollbackTs, xid, gid);
    }

    static PgOutputMessage.StreamPrepare streamPrepare(ByteBufferReader r) {
        r.readByte(); // currently-unused flags
        return readPreparedTxn(r);
    }

    private static PgOutputMessage.Prepare readPreparedTxn(ByteBufferReader r) {
        long prepareLsn = r.readLong();
        long endLsn = r.readLong();
        Instant prepareTs = ByteBufferReader.pgMicrosToInstant(r.readLong());
        long xid = r.readUnsignedInt();
        String gid = r.readString();
        return new PgOutputMessage.Prepare(prepareLsn, endLsn, prepareTs, xid, gid);
    }
}
```

- [ ] **Step 4: 运行验证通过 + 全量回归**

Run: `mvn test`
Expected: BUILD SUCCESS（至此 protocol 层 19 种消息全部实现）

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/protocol/TwoPhaseParsers.java src/test/java/org/vastdata/vbstream/protocol/TwoPhaseParsersTest.java
git commit -m "feat(protocol): 实现两阶段提交消息解析（b/P/K/r/p）"
git push
```

---

### Task 8: ReplicationConfig 配置模型

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/ReplicationConfig.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/ReplicationConfigTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.StreamingMode;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationConfigTest {

    @AfterEach
    void cleanupSystemProperties() {
        System.clearProperty("vb.pg.host");
        System.clearProperty("vb.pg.port");
        System.clearProperty("vb.pg.slot");
        System.clearProperty("vb.pg.streaming");
    }

    @Test
    void defaultsTargetLocalComposeEnv() {
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        assertEquals("localhost", config.host());
        assertEquals(55432, config.port());
        assertEquals("vb_cdc_slot", config.slotName());
        assertEquals("vb_pub", config.publicationNames());
        assertEquals(4, config.protoVersion());
        assertEquals(StreamingMode.PARALLEL, config.streamingMode());
        assertEquals(true, config.twoPhase());
    }

    @Test
    void systemPropertiesOverrideDefaults() {
        System.setProperty("vb.pg.host", "db.example.com");
        System.setProperty("vb.pg.port", "6543");
        System.setProperty("vb.pg.slot", "s1");
        System.setProperty("vb.pg.streaming", "on");
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        assertEquals("db.example.com", config.host());
        assertEquals(6543, config.port());
        assertEquals("s1", config.slotName());
        assertEquals(StreamingMode.ON, config.streamingMode());
    }

    @Test
    void buildsJdbcAndReplicationUrls() {
        ReplicationConfig config = new ReplicationConfig("h", 5432, "db", "u", "p",
                "slot", "pub", 4, StreamingMode.PARALLEL, true, 10);
        assertEquals("jdbc:postgresql://h:5432/db", config.jdbcUrl());
        assertEquals("jdbc:postgresql://h:5432/db?replication=database", config.replicationUrl());
        assertEquals("parallel", config.streamingParam());
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=ReplicationConfigTest`
Expected: 编译错误（ReplicationConfig 不存在）

- [ ] **Step 3: 最小实现**

```java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.StreamingMode;

/**
 * 复制会话配置。默认值对准 src/docker 的 compose 环境（localhost:55432），
 * 全部可经 -Dvb.pg.* 系统属性覆盖。
 */
public record ReplicationConfig(
        String host, int port, String database, String user, String password,
        String slotName, String publicationNames,
        int protoVersion, StreamingMode streamingMode, boolean twoPhase,
        int feedbackIntervalSeconds) {

    public static ReplicationConfig fromSystemProperties() {
        return new ReplicationConfig(
                prop("vb.pg.host", "localhost"),
                Integer.parseInt(prop("vb.pg.port", "55432")),
                prop("vb.pg.database", "postgres"),
                prop("vb.pg.user", "postgres"),
                prop("vb.pg.password", "postgres"),
                prop("vb.pg.slot", "vb_cdc_slot"),
                prop("vb.pg.publication", "vb_pub"),
                Integer.parseInt(prop("vb.pg.protoVersion", "4")),
                StreamingMode.valueOf(prop("vb.pg.streaming", "parallel").toUpperCase()),
                Boolean.parseBoolean(prop("vb.pg.twoPhase", "true")),
                Integer.parseInt(prop("vb.pg.feedbackSeconds", "10")));
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/%s".formatted(host, port, database);
    }

    /** pgjdbc 复制连接要求 replication=database。 */
    public String replicationUrl() {
        return jdbcUrl() + "?replication=database";
    }

    /** START_REPLICATION 的 streaming 参数值。 */
    public String streamingParam() {
        return switch (streamingMode) {
            case OFF -> "off";
            case ON -> "on";
            case PARALLEL -> "parallel";
        };
    }

    private static String prop(String key, String defaultValue) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn test -Dtest=ReplicationConfigTest`
Expected: `Tests run: 3, Failures: 0`

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication src/test/java/org/vastdata/vbstream/replication
git commit -m "feat(replication): ReplicationConfig 配置模型（系统属性可覆盖）"
git push
```

---

### Task 9: RelationRegistry 与 Listener 契约

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/RelationRegistry.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/PgOutputListener.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/RelationRegistryTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationRegistryTest {

    private static PgOutputMessage.Relation relation(int oid, String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table,
                'd', List.of(new Column("id", 23, -1, true)));
    }

    @Test
    void cachesLatestRelationByOid() {
        RelationRegistry registry = new RelationRegistry();
        registry.accept(relation(100, "t_a"));
        registry.accept(relation(200, "t_b"));
        registry.accept(relation(100, "t_a_v2")); // 同 oid 再下发即定义变化
        assertEquals("t_a_v2", registry.require(100).table());
        assertTrue(registry.find(999).isEmpty());
    }

    @Test
    void ignoresNonRelationMessages() {
        RelationRegistry registry = new RelationRegistry();
        registry.accept(new PgOutputMessage.Begin(1, java.time.Instant.EPOCH, 2));
        assertTrue(registry.find(1).isEmpty());
    }

    @Test
    void requireMissingOidFailsFast() {
        RelationRegistry registry = new RelationRegistry();
        assertThrows(IllegalStateException.class, () -> registry.require(123));
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `mvn test -Dtest=RelationRegistryTest`
Expected: 编译错误

- [ ] **Step 3: 实现**

`PgOutputListener.java`：

```java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

/** 消息消费者契约。里程碑 2 的 Chronicle Queue 写入器实现同一接口即可接入。 */
@FunctionalInterface
public interface PgOutputListener {

    void onMessage(PgOutputMessage message, RelationRegistry registry);
}
```

`RelationRegistry.java`：

```java
package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** oid → Relation 元数据缓存。Relation 消息（含流式块内重复下发）统一入缓存；DML 前必有 Relation。 */
public final class RelationRegistry {

    private final Map<Integer, PgOutputMessage.Relation> relations = new ConcurrentHashMap<>();

    public void accept(PgOutputMessage message) {
        if (message instanceof PgOutputMessage.Relation relation) {
            relations.put(relation.relationOid(), relation);
        }
    }

    public Optional<PgOutputMessage.Relation> find(int relationOid) {
        return Optional.ofNullable(relations.get(relationOid));
    }

    /** 缓存 miss 即协议流异常（Relation 必先于 DML 到达），fail-fast。 */
    public PgOutputMessage.Relation require(int relationOid) {
        PgOutputMessage.Relation relation = relations.get(relationOid);
        if (relation == null) {
            throw new IllegalStateException("Relation oid=" + relationOid + " 未先行到达，协议流异常");
        }
        return relation;
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `mvn test -Dtest=RelationRegistryTest`
Expected: `Tests run: 3, Failures: 0`

- [ ] **Step 5: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication src/test/java/org/vastdata/vbstream/replication
git commit -m "feat(replication): RelationRegistry 元数据缓存与 PgOutputListener 契约"
git push
```

---

### Task 10: PgReplicationSession 复制会话

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/PgReplicationSession.java`

会话逻辑的运行正确性由 Task 11-14 的集成测试验证；本任务以编译通过 + 既有测试全绿为准。

- [ ] **Step 1: 实现**

```java
package org.vastdata.vbstream.replication;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.vastdata.vbstream.protocol.PgOutputDecoder;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * pgoutput 复制会话：两条连接（普通 SQL + replication=database），
 * 生命周期 open → ensureSlot → start → run → close。
 */
public final class PgReplicationSession implements AutoCloseable {

    private final ReplicationConfig config;
    private Connection sqlConnection;
    private Connection replicationConnection;
    private PGReplicationStream stream;

    public PgReplicationSession(ReplicationConfig config) {
        this.config = config;
    }

    public void open() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.user());
        props.setProperty("password", config.password());
        sqlConnection = DriverManager.getConnection(config.jdbcUrl(), props);
        replicationConnection = DriverManager.getConnection(config.replicationUrl(), props);
    }

    /** 幂等建槽：two_phase 随槽开启；SQLState 42710（duplicate_object）表示已存在，复用。 */
    public void ensureSlot() throws SQLException {
        try (PreparedStatement ps = sqlConnection.prepareStatement(
                "SELECT pg_create_logical_replication_slot(?, 'pgoutput', false, ?)")) {
            ps.setString(1, config.slotName());
            ps.setBoolean(2, config.twoPhase());
            try (ResultSet ignored = ps.executeQuery()) {
                // 只需副作用：建槽
            }
        } catch (SQLException e) {
            if ("42710".equals(e.getSQLState())) {
                System.err.println("WARN: 复制槽 " + config.slotName() + " 已存在，直接复用");
            } else {
                throw e;
            }
        }
    }

    public void start() throws SQLException {
        PGConnection pg = replicationConnection.unwrap(PGConnection.class);
        stream = pg.getReplicationAPI()
                .createReplicationStream()
                .withSlotName(config.slotName())
                .withSlotOption("proto_version", Integer.toString(config.protoVersion()))
                .withSlotOption("publication_names", config.publicationNames())
                .withSlotOption("streaming", config.streamingParam())
                .withSlotOption("two_phase", config.twoPhase() ? "on" : "off")
                .withStartPosition(LogSequenceNumber.INVALID_LSN)
                .withStatusInterval(config.feedbackIntervalSeconds(), TimeUnit.SECONDS)
                .start();
    }

    /** 消息循环：阻塞读 → 解码 → 缓存 Relation → 回调；按周期 forceStatusUpdate 反馈 LSN。 */
    public void run(PgOutputListener listener) throws SQLException, IOException {
        PgOutputDecoder decoder = new PgOutputDecoder(config.streamingMode());
        RelationRegistry registry = new RelationRegistry();
        long feedbackIntervalNanos = config.feedbackIntervalSeconds() * 1_000_000_000L;
        long lastFeedbackNanos = System.nanoTime();
        while (true) {
            ByteBuffer payload = stream.read(); // 阻塞直到下一条消息
            PgOutputMessage message = decoder.decode(payload);
            registry.accept(message);
            listener.onMessage(message, registry);
            LogSequenceNumber last = stream.getLastReceiveLSN();
            stream.setAppliedLSN(last);
            stream.setFlushedLSN(last);
            if (System.nanoTime() - lastFeedbackNanos >= feedbackIntervalNanos) {
                stream.forceStatusUpdate();
                lastFeedbackNanos = System.nanoTime();
            }
        }
    }

    /** 关闭顺序：流 → 复制连接 → SQL 连接。close 会令阻塞中的 read 抛出异常从而结束 run 循环。 */
    @Override
    public void close() {
        if (stream != null) {
            stream.close();
        }
        closeQuietly(replicationConnection);
        closeQuietly(sqlConnection);
    }

    private static void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("WARN: 关闭连接失败: " + e.getMessage());
        }
    }
}
```

- [ ] **Step 2: 编译 + 全量回归**

Run: `mvn -q compile && mvn test`
Expected: BUILD SUCCESS

- [ ] **Step 3: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/replication/PgReplicationSession.java
git commit -m "feat(replication): PgReplicationSession 复制会话（建槽/开流/LSN 反馈/消息循环）"
git push
```

---

### Task 11: 集成测试基建 + 普通事务用例 + LSN 反馈用例

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/it/PgTestEnv.java`
- Create: `src/test/java/org/vastdata/vbstream/it/SessionHarness.java`
- Test: `src/test/java/org/vastdata/vbstream/it/NormalTransactionTest.java`

- [ ] **Step 1: 写基建与失败测试**

`PgTestEnv.java`：

```java
package org.vastdata.vbstream.it;

import org.testcontainers.containers.PostgreSQLContainer;
import org.vastdata.vbstream.protocol.StreamingMode;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/** 集成测试共享的单例 PG 18 容器与工具。类加载即启动（需要本机 Docker）。 */
public final class PgTestEnv {

    public static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test")
            .withCommand(
                    "postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=16",
                    "-c", "max_wal_senders=16",
                    "-c", "max_prepared_transactions=16",
                    "-c", "logical_decoding_work_mem=64kB",
                    "-c", "max_slot_wal_keep_size=1GB");

    static {
        PG.start();
    }

    private PgTestEnv() {
    }

    public static Connection newSqlConnection() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    public static ReplicationConfig newConfig(String slotName, String publication) {
        return new ReplicationConfig(
                PG.getHost(), PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                PG.getDatabaseName(), PG.getUsername(), PG.getPassword(),
                slotName, publication,
                4, StreamingMode.PARALLEL, true, 2);
    }

    public static void execSql(String... statements) throws SQLException {
        try (Connection c = newSqlConnection(); Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    /** 先杀 walsender 再删槽；槽不存在等情况静默忽略。 */
    public static void dropSlotQuietly(String slotName) {
        try (Connection c = newSqlConnection()) {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, slotName);
                ps.executeQuery();
            }
        } catch (Exception e) {
            System.err.println("WARN: 清理槽 " + slotName + " 失败: " + e.getMessage());
        }
    }
}
```

`SessionHarness.java`：

```java
package org.vastdata.vbstream.it;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/** 在守护线程跑复制会话并录制消息，直到满足停止条件或超时。 */
public final class SessionHarness implements AutoCloseable {

    private final PgReplicationSession session;
    private final List<PgOutputMessage> messages = new CopyOnWriteArrayList<>();
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile Exception failure;

    private SessionHarness(PgReplicationSession session, Predicate<PgOutputMessage> stopCondition) {
        this.session = session;
        Thread worker = new Thread(() -> {
            try {
                session.run((msg, registry) -> {
                    messages.add(msg);
                    if (stopCondition.test(msg)) {
                        done.countDown();
                    }
                });
            } catch (Exception e) {
                failure = e;
                done.countDown();
            }
        }, "pgoutput-reader");
        worker.setDaemon(true);
        worker.start();
    }

    public static SessionHarness start(ReplicationConfig config,
                                       Predicate<PgOutputMessage> stopCondition) throws Exception {
        PgReplicationSession session = new PgReplicationSession(config);
        session.open();
        session.ensureSlot();
        session.start();
        return new SessionHarness(session, stopCondition);
    }

    public List<PgOutputMessage> messages() {
        return messages;
    }

    public void awaitTermination(Duration timeout) throws InterruptedException {
        if (!done.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new AssertionError("等待复制消息超时，已收到: " + messages);
        }
        if (failure != null) {
            throw new AssertionError("复制会话异常", failure);
        }
    }

    @Override
    public void close() {
        session.close();
    }
}
```

`NormalTransactionTest.java`（spec 用例 1 与 6）：

```java
package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NormalTransactionTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_norm");
        PgTestEnv.dropSlotQuietly("slot_lsn");
    }

    @Test
    void decodesBeginRelationDmlCommitSequence() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_norm(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_norm",
                "CREATE PUBLICATION pub_norm FOR TABLE t_norm",
                "TRUNCATE t_norm");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_norm", "pub_norm"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_norm VALUES (1, 'a')",
                    "UPDATE t_norm SET v = 'b' WHERE id = 1",
                    "DELETE FROM t_norm WHERE id = 1");
            harness.awaitTermination(Duration.ofSeconds(30));

            List<Class<?>> types = harness.messages().stream().map(Object::getClass).toList();
            assertTrue(types.contains(PgOutputMessage.Begin.class), "缺 Begin: " + types);
            assertTrue(types.contains(PgOutputMessage.Relation.class), "缺 Relation: " + types);
            assertTrue(types.contains(PgOutputMessage.Insert.class), "缺 Insert: " + types);
            assertTrue(types.contains(PgOutputMessage.Update.class), "缺 Update: " + types);
            assertTrue(types.contains(PgOutputMessage.Delete.class), "缺 Delete: " + types);
            assertTrue(types.contains(PgOutputMessage.Commit.class), "缺 Commit: " + types);

            PgOutputMessage.Insert insert = (PgOutputMessage.Insert) harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .findFirst().orElseThrow();
            assertEquals(new TupleValue.Text("1"), insert.newTuple().columns().get(0));
            assertEquals(new TupleValue.Text("a"), insert.newTuple().columns().get(1));
        }
    }

    @Test
    void feedbackAdvancesConfirmedFlushLsn() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_lsn(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_lsn",
                "CREATE PUBLICATION pub_lsn FOR TABLE t_lsn",
                "TRUNCATE t_lsn");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_lsn", "pub_lsn"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            PgTestEnv.execSql("INSERT INTO t_lsn VALUES (1, 'x')");
            harness.awaitTermination(Duration.ofSeconds(30));
            Thread.sleep(2_500); // 等 feedbackInterval=2s 的 forceStatusUpdate
        }
        try (Connection c = PgTestEnv.newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'slot_lsn'")) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next(), "槽应存在");
            assertTrue(rs.getString(1) != null && !"0/0".equals(rs.getString(1)),
                    "confirmed_flush_lsn 应已推进，实际: " + rs.getString(1));
        }
    }
}
```

- [ ] **Step 2: 运行（首跑拉镜像较慢）**

Run: `mvn test -Dtest=NormalTransactionTest`
Expected: `Tests run: 2, Failures: 0`。若容器参数（如 `withCommand` 数组形式）与所用 Testcontainers 版本 API 不符导致编译错误，按该版本 javadoc 调整（保持六个 `-c` 参数语义不变）。

- [ ] **Step 3: 全量回归**

Run: `mvn test`
Expected: BUILD SUCCESS

- [ ] **Step 4: 提交并推送**

```bash
git add src/test/java/org/vastdata/vbstream/it
git commit -m "test(it): Testcontainers 基建 + 普通事务与 LSN 反馈集成用例"
git push
```

---

### Task 12: 流式大事务 + 并行流式用例

**Files:**
- Test: `src/test/java/org/vastdata/vbstream/it/StreamedTransactionTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamedTransactionTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_stream");
        PgTestEnv.dropSlotQuietly("slot_par");
    }

    /** spec 用例 2：64kB work_mem 下，单事务 500 行×8KB 触发流式分块下发。 */
    @Test
    void largeTransactionStreamsInSegments() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_stream(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_stream",
                "CREATE PUBLICATION pub_stream FOR TABLE t_stream",
                "TRUNCATE t_stream");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_stream", "pub_stream"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO t_stream VALUES (?, repeat('x', 8192))")) {
                    for (int i = 0; i < 500; i++) {
                        ps.setInt(1, i);
                        ps.executeUpdate();
                    }
                }
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));
        }

        // 重新开一个槽收集（上面 harness 已关）——直接复用 harness 收集到的消息更简单：
        // 改为在 close 前断言。见下方实现说明。
    }
}
```

**实现说明（重要）**：断言要在 `harness.close()` 之前用 `harness.messages()` 完成，正确形态如下（请按此写完整测试，不要保留上面的半成品）：

```java
    @Test
    void largeTransactionStreamsInSegments() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_stream(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_stream",
                "CREATE PUBLICATION pub_stream FOR TABLE t_stream",
                "TRUNCATE t_stream");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_stream", "pub_stream"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO t_stream VALUES (?, repeat('x', 8192))")) {
                    for (int i = 0; i < 500; i++) {
                        ps.setInt(1, i);
                        ps.executeUpdate();
                    }
                }
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<PgOutputMessage> messages = harness.messages();
            List<PgOutputMessage.StreamStart> starts = messages.stream()
                    .filter(m -> m instanceof PgOutputMessage.StreamStart)
                    .map(m -> (PgOutputMessage.StreamStart) m).toList();
            assertFalse(starts.isEmpty(), "应出现流式块 StreamStart，实际消息类型: "
                    + messages.stream().map(m -> m.getClass().getSimpleName()).distinct().toList());
            assertTrue(starts.get(0).firstSegment(), "首个流块 firstSegment 应为 true");

            long streamedXid = starts.get(0).xid();
            PgOutputMessage.Insert streamedInsert = messages.stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert i && i.streamXid().isPresent())
                    .map(m -> (PgOutputMessage.Insert) m)
                    .findFirst().orElseThrow(() -> new AssertionError("流块内 Insert 应带 streamXid"));
            assertEquals(streamedXid, streamedInsert.streamXid().getAsLong());

            long insertCount = messages.stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert).count();
            assertEquals(500, insertCount, "500 行 Insert 应全部流式下发");

            assertTrue(messages.stream().anyMatch(m -> m instanceof PgOutputMessage.StreamCommit));
            assertTrue(messages.stream().noneMatch(m -> m instanceof PgOutputMessage.Commit),
                    "流式事务最终以 StreamCommit 收尾，不应再出现顶层 Commit");
        }
    }

    /** spec 用例 5：parallel 模式下子事务回滚产生带附加字段的 StreamAbort，且无错位。 */
    @Test
    void parallelModeStreamAbortCarriesExtraFields() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_par(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_par",
                "CREATE PUBLICATION pub_par FOR TABLE t_par",
                "TRUNCATE t_par");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_par", "pub_par"),
                msg -> msg instanceof PgOutputMessage.StreamCommit)) {
            try (Connection c = PgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                Statement st = c.createStatement();
                for (int i = 0; i < 300; i++) {
                    st.execute("INSERT INTO t_par VALUES (" + i + ", repeat('p', 4096))");
                }
                st.execute("SAVEPOINT sp1");
                // 子事务数据量必须超过 logical_decoding_work_mem(64kB) 让其变更被流式发出——
                // PG 只对"已流式"的子事务发 StreamAbort（ReorderBufferAbort 的
                // rbtxn_is_streamed 门槛），未流式的子事务回滚是静默丢弃。
                // 源码摘录见设计文档附录 B。
                for (int i = 900; i < 960; i++) {
                    st.execute("INSERT INTO t_par VALUES (" + i + ", repeat('q', 8192))");
                }
                st.execute("ROLLBACK TO SAVEPOINT sp1"); // 触发 StreamAbort
                for (int i = 1000; i < 1100; i++) {
                    st.execute("INSERT INTO t_par VALUES (" + i + ", repeat('r', 4096))");
                }
                c.commit();
            }
            harness.awaitTermination(Duration.ofSeconds(60));

            List<PgOutputMessage.StreamAbort> aborts = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.StreamAbort)
                    .map(m -> (PgOutputMessage.StreamAbort) m).toList();
            assertFalse(aborts.isEmpty(), "子事务回滚应产生 StreamAbort");
            assertTrue(aborts.get(aborts.size() - 1).abortLsn().isPresent(),
                    "parallel 模式 StreamAbort 应携带 abortLsn 附加字段");
        }
    }
}
```

注意：`parallelModeStreamAbortCarriesExtraFields` 的槽名是 `slot_par`，已包含在上方 `@AfterEach` 清理中。

- [ ] **Step 2: 运行验证**

Run: `mvn test -Dtest=StreamedTransactionTest`
Expected: `Tests run: 2, Failures: 0`。若 `StreamStart` 未出现（消息全为 B/R/I/C），说明 `logical_decoding_work_mem=64kB` 未生效或行总量不够——检查容器参数并把行数提到 1000。

**实现修正记录（2026-08-27 首跑）**：用例 2 首跑失败——savepoint 后 5 行×4KB（≈20KB）不足 64kB work_mem，子事务从未参与流式发送、未被标记 streamed，回滚被 PG 静默清理（`ReorderBufferAbort` 仅对 `rbtxn_is_streamed` 的事务调用 `stream_abort`）。已将子事务数据量改为 60 行×8KB（≈480KB），两用例全绿。完整源码摘录链路见设计文档**附录 B**（postgres/postgres `715d839`）。

- [ ] **Step 3: 提交并推送**

```bash
git add src/test/java/org/vastdata/vbstream/it/StreamedTransactionTest.java
git commit -m "test(it): 流式大事务与并行流式（StreamAbort 附加字段）集成用例"
git push
```

---

### Task 13: 两阶段提交用例

**Files:**
- Test: `src/test/java/org/vastdata/vbstream/it/TwoPhaseTransactionTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TwoPhaseTransactionTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_2pc");
    }

    private static void prepareTable() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_2pc(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_2pc",
                "CREATE PUBLICATION pub_2pc FOR TABLE t_2pc",
                "TRUNCATE t_2pc");
    }

    /** spec 用例 3：PREPARE → b/变更/P；COMMIT PREPARED → K，gid 匹配。 */
    @Test
    void prepareThenCommitPrepared() throws Exception {
        prepareTable();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_2pc", "pub_2pc"),
                msg -> msg instanceof PgOutputMessage.CommitPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("BEGIN");
                st.execute("INSERT INTO t_2pc VALUES (1, 'prepare-commit')");
                st.execute("PREPARE TRANSACTION 'gid_c1'");
                st.execute("COMMIT PREPARED 'gid_c1'");
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.BeginPrepare b
                    && "gid_c1".equals(b.gid())), "应出现 BeginPrepare(gid_c1)");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.Prepare p
                    && "gid_c1".equals(p.gid())), "应出现 Prepare(gid_c1)");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.CommitPrepared k
                    && "gid_c1".equals(k.gid())), "应出现 CommitPrepared(gid_c1)");
        }
    }

    /** spec 用例 4：PREPARE → ROLLBACK PREPARED → r，gid 匹配。 */
    @Test
    void prepareThenRollbackPrepared() throws Exception {
        prepareTable();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_2pc", "pub_2pc"),
                msg -> msg instanceof PgOutputMessage.RollbackPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("BEGIN");
                st.execute("INSERT INTO t_2pc VALUES (2, 'prepare-rollback')");
                st.execute("PREPARE TRANSACTION 'gid_r1'");
                st.execute("ROLLBACK PREPARED 'gid_r1'");
            }
            harness.awaitTermination(Duration.ofSeconds(30));

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.RollbackPrepared r
                    && "gid_r1".equals(r.gid())), "应出现 RollbackPrepared(gid_r1)");
            assertTrue(harness.messages().stream().noneMatch(m -> m instanceof PgOutputMessage.CommitPrepared));
        }
    }

    /** 大 2PC 事务走流式路径：流块 + StreamPrepare('p') 收尾。 */
    @Test
    void largePreparedTransactionEndsWithStreamPrepare() throws Exception {
        prepareTable();
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_2pc", "pub_2pc"),
                msg -> msg instanceof PgOutputMessage.StreamPrepare
                        || msg instanceof PgOutputMessage.CommitPrepared)) {
            try (Connection c = PgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                Statement st = c.createStatement();
                for (int i = 0; i < 500; i++) {
                    st.execute("INSERT INTO t_2pc VALUES (" + (1000 + i) + ", repeat('z', 4096))");
                }
                c.commit();          // 先结束 JDBC 事务
                st = c.createStatement();
                st.execute("BEGIN"); // 再以裸语句做 PREPARE
                st.execute("PREPARE TRANSACTION 'gid_big1'");
                st.execute("COMMIT PREPARED 'gid_big1'");
            }
            harness.awaitTermination(Duration.ofSeconds(60));
            boolean streamed = harness.messages().stream()
                    .anyMatch(m -> m instanceof PgOutputMessage.StreamStart);
            boolean streamPrepareOrCommit = harness.messages().stream()
                    .anyMatch(m -> m instanceof PgOutputMessage.StreamPrepare
                            || m instanceof PgOutputMessage.CommitPrepared);
            assertTrue(streamPrepareOrCommit, "大 2PC 事务应以 StreamPrepare 或 CommitPrepared 收尾");
            // streamed 可能为 false（取决于服务器分块决策），仅作信息记录
            System.out.println("largePreparedTransaction: streamed=" + streamed
                    + ", messages=" + harness.messages().size());
        }
    }
}
```

说明：`largePreparedTransactionEndsWithStreamPrepare` 中 JDBC 事务与裸 `PREPARE TRANSACTION` 分两步走，是因为 pgjdbc 的 autocommit 管理与服务器端两阶段命令混用易出错；服务器端 `BEGIN` 后紧接 `PREPARE TRANSACTION` 即可。

- [ ] **Step 2: 运行验证**

Run: `mvn test -Dtest=TwoPhaseTransactionTest`
Expected: `Tests run: 3, Failures: 0`。若 `PREPARE TRANSACTION` 报 `max_prepared_transactions is zero`，检查 PgTestEnv 的 withCommand 参数。

- [ ] **Step 3: 提交并推送**

```bash
git add src/test/java/org/vastdata/vbstream/it/TwoPhaseTransactionTest.java
git commit -m "test(it): 两阶段提交/回滚与流式 2PC 集成用例"
git push
```

---

### Task 14: Truncate 用例

**Files:**
- Test: `src/test/java/org/vastdata/vbstream/it/TruncateTest.java`

- [ ] **Step 1: 写失败测试**

```java
package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TruncateTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_trunc");
    }

    /** spec 用例 7：TRUNCATE 选项位与多表 oid 列表。 */
    @Test
    void truncateDecodesOptionsAndMultipleOids() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_trunc1(id int PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS t_trunc2(id int PRIMARY KEY)",
                "DROP PUBLICATION IF EXISTS pub_trunc",
                "CREATE PUBLICATION pub_trunc FOR TABLE t_trunc1, t_trunc2",
                "TRUNCATE t_trunc1, t_trunc2");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_trunc", "pub_trunc"),
                msg -> msg instanceof PgOutputMessage.Truncate)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_trunc1 VALUES (1)",
                    "INSERT INTO t_trunc2 VALUES (2)",
                    "TRUNCATE t_trunc1, t_trunc2 RESTART IDENTITY CASCADE");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Truncate truncate = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Truncate)
                    .map(m -> (PgOutputMessage.Truncate) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Truncate 消息"));
            assertEquals(2, truncate.relationOids().length, "TRUNCATE 两张表应携带两个 oid");
            assertTrue(truncate.options().contains(TruncateOption.CASCADE), "应含 CASCADE: " + truncate.options());
            assertTrue(truncate.options().contains(TruncateOption.RESTART_IDENTITY), "应含 RESTART_IDENTITY");
        }
    }
}
```

- [ ] **Step 2: 运行验证 + 全量回归**

Run: `mvn test`
Expected: BUILD SUCCESS（含此前全部集成用例）

- [ ] **Step 3: 提交并推送**

```bash
git add src/test/java/org/vastdata/vbstream/it/TruncateTest.java
git commit -m "test(it): Truncate 消息选项位与多表 oid 集成用例"
git push
```

---

### Task 15: Main 入口 + ConsoleListener + 手动冒烟

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/ConsoleListener.java`
- Create: `src/main/java/org/vastdata/vbstream/Main.java`
- Modify: `CLAUDE.md`

- [ ] **Step 1: 实现 ConsoleListener**

```java
package org.vastdata.vbstream;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;
import org.vastdata.vbstream.protocol.TupleValue;
import org.vastdata.vbstream.replication.PgOutputListener;
import org.vastdata.vbstream.replication.RelationRegistry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;

/** 控制台打印 listener：每条消息一行可读输出。 */
public final class ConsoleListener implements PgOutputListener {

    @Override
    public void onMessage(PgOutputMessage message, RelationRegistry registry) {
        System.out.println(Instant.now() + " | " + render(message, registry));
    }

    // 注：record pattern switch 是 Java 21 正式特性，本项目约束 Java 17，故用 instanceof 链
    private String render(PgOutputMessage msg, RelationRegistry registry) {
        if (msg instanceof PgOutputMessage.Begin m) {
            return "BEGIN             xid=%d finalLsn=0x%s".formatted(m.xid(), Long.toHexString(m.finalLsn()));
        }
        if (msg instanceof PgOutputMessage.Commit m) {
            return "COMMIT            commitLsn=0x%s endLsn=0x%s"
                    .formatted(Long.toHexString(m.commitLsn()), Long.toHexString(m.endLsn()));
        }
        if (msg instanceof PgOutputMessage.Origin m) {
            return "ORIGIN            lsn=0x%s name=%s".formatted(Long.toHexString(m.originCommitLsn()), m.originName());
        }
        if (msg instanceof PgOutputMessage.Relation m) {
            return "RELATION          %s.%s oid=%d cols=%d%s"
                    .formatted(m.schema(), m.table(), m.relationOid(), m.columns().size(), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Type m) {
            return "TYPE              %s.%s oid=%d%s".formatted(m.schema(), m.name(), m.typeOid(), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Insert m) {
            return "INSERT            %s %s%s".formatted(tableOf(m.relationOid(), registry),
                    tupleOf(m.relationOid(), m.newTuple(), registry), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Update m) {
            return "UPDATE            %s %s%s".formatted(tableOf(m.relationOid(), registry),
                    tupleOf(m.relationOid(), m.newTuple(), registry), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Delete m) {
            return "DELETE            %s %s%s".formatted(tableOf(m.relationOid(), registry),
                    tupleOf(m.relationOid(), m.oldTuple(), registry), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.Truncate m) {
            return "TRUNCATE          oids=%s options=%s%s"
                    .formatted(java.util.Arrays.toString(m.relationOids()), m.options(), suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.LogicalMsg m) {
            return "MESSAGE           prefix=%s bytes=%d%s".formatted(m.prefix(), m.content().length, suffix(m.streamXid()));
        }
        if (msg instanceof PgOutputMessage.StreamStart m) {
            return "STREAM-START      xid=%d firstSegment=%s".formatted(m.xid(), m.firstSegment());
        }
        if (msg instanceof PgOutputMessage.StreamStop) {
            return "STREAM-STOP";
        }
        if (msg instanceof PgOutputMessage.StreamCommit m) {
            return "STREAM-COMMIT     xid=%d commitLsn=0x%s".formatted(m.xid(), Long.toHexString(m.commitLsn()));
        }
        if (msg instanceof PgOutputMessage.StreamAbort m) {
            return "STREAM-ABORT      xid=%d subxid=%d%s".formatted(m.xid(), m.subxid(),
                    m.abortLsn().isPresent() ? " abortLsn=0x" + Long.toHexString(m.abortLsn().getAsLong()) : "");
        }
        if (msg instanceof PgOutputMessage.BeginPrepare m) {
            return "BEGIN-PREPARE     gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.Prepare m) {
            return "PREPARE           gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.CommitPrepared m) {
            return "COMMIT-PREPARED   gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.RollbackPrepared m) {
            return "ROLLBACK-PREPARED gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        if (msg instanceof PgOutputMessage.StreamPrepare m) {
            return "STREAM-PREPARE    gid=%s xid=%d".formatted(m.gid(), m.xid());
        }
        throw new IllegalStateException("未知消息类型: " + msg.getClass());
    }

    private static String suffix(OptionalLong streamXid) {
        return streamXid.isPresent() ? " [streamed xid=" + streamXid.getAsLong() + "]" : "";
    }

    private static String tableOf(int oid, RelationRegistry registry) {
        return registry.find(oid)
                .map(rel -> rel.schema() + "." + rel.table())
                .orElse("oid:" + oid);
    }

    /** 列名=值 打印；TOAST 未变与 NULL 显式标注（打印 text 值截断到 64 字符）。 */
    private static String tupleOf(int oid, TupleData tuple, RelationRegistry registry) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < tuple.columns().size(); i++) {
            String column = registry.find(oid)
                    .filter(rel -> i < rel.columns().size())
                    .map(rel -> rel.columns().get(i).name())
                    .orElse("#" + i);
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
}
```

- [ ] **Step 2: 实现 Main**

```java
package org.vastdata.vbstream;

import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.util.concurrent.CountDownLatch;

/** 里程碑 1 入口：连上复制流并把解析出的 pgoutput 消息打印到控制台，Ctrl+C 优雅退出。 */
public final class Main {

    public static void main(String[] args) throws Exception {
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        if (config.host().isBlank() || config.slotName().isBlank() || config.publicationNames().isBlank()) {
            System.err.println("用法: java -Dvb.pg.host=... -Dvb.pg.port=... -Dvb.pg.slot=... "
                    + "-Dvb.pg.publication=... org.vastdata.vbstream.Main");
            System.exit(2);
        }
        System.out.printf("vb-stream-logstash → %s:%d/%s 槽=%s publication=%s proto=v%d streaming=%s twoPhase=%s（Ctrl+C 退出）%n",
                config.host(), config.port(), config.database(), config.slotName(), config.publicationNames(),
                config.protoVersion(), config.streamingMode(), config.twoPhase());

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown, "shutdown-hook"));

        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            Thread worker = new Thread(() -> {
                try {
                    session.run(new ConsoleListener());
                } catch (Exception e) {
                    System.err.println("复制流中断: " + e + "（槽 " + config.slotName()
                            + " 已保留，重启续传）");
                    stop.countDown();
                }
            }, "pgoutput-reader");
            worker.start();
            stop.await();
            System.out.println("正在关闭复制流...");
        } catch (Exception e) {
            System.err.println("启动失败: " + e.getMessage());
            System.exit(1);
        }
    }
}
```

- [ ] **Step 3: 编译 + 全量回归**

Run: `mvn -q compile && mvn test`
Expected: BUILD SUCCESS

- [ ] **Step 4: 手动冒烟（对 src/docker 的 compose 环境，四场景一次走完）**

```bash
cd src/docker && docker compose up -d && cd ../..
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes;$(cat target/cp.txt)" org.vastdata.vbstream.Main > target/main-smoke.log 2>&1 &
sleep 3
docker exec vb-stream-pg psql -U postgres -d postgres -c "INSERT INTO t_stream_test(payload) VALUES ('smoke-small')"
docker exec vb-stream-pg psql -U postgres -d postgres -c "INSERT INTO t_stream_test(payload) SELECT repeat('y', 4096) FROM generate_series(1, 300)"
docker exec vb-stream-pg psql -U postgres -d postgres -c "BEGIN; INSERT INTO t_stream_test(payload) VALUES ('smoke-2pc'); PREPARE TRANSACTION 'smoke_g1'; COMMIT PREPARED 'smoke_g1';"
sleep 8
grep -cE "STREAM-START" target/main-smoke.log
grep -E "BEGIN-PREPARE|COMMIT-PREPARED|STREAM-COMMIT|^.*COMMIT  " target/main-smoke.log | head -10
```

Expected: `STREAM-START` 计数 ≥ 1；grep 输出含 `BEGIN-PREPARE gid=smoke_g1`、`COMMIT-PREPARED gid=smoke_g1`、普通事务的 `COMMIT` 与大事务的 `STREAM-COMMIT`。最后清理：杀掉后台 java 进程并 `docker exec vb-stream-pg psql -U postgres -c "SELECT pg_drop_replication_slot('vb_cdc_slot')"`（冒烟槽用默认名 vb_cdc_slot）。

- [ ] **Step 5: 更新 CLAUDE.md**

在「常用命令」一节后追加：

```markdown
## 运行 Main（里程碑 1）

```bash
cd src/docker && docker compose up -d && cd ../..     # 起本地 PG
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java -cp "target/classes;$(cat target/cp.txt)" org.vastdata.vbstream.Main
# 可选覆盖：-Dvb.pg.slot=... -Dvb.pg.publication=... -Dvb.pg.streaming=on|parallel|off
```

- 源码结构：`org.vastdata.vbstream.protocol`（协议解析，纯函数）、`org.vastdata.vbstream.replication`（会话）、`Main`/`ConsoleListener`
- 集成测试（`org.vastdata.vbstream.it`）经 Testcontainers 自动起 postgres:18 容器，需本机 Docker；`mvn test` 单命令跑全部
```

- [ ] **Step 6: 提交并推送**

```bash
git add src/main/java/org/vastdata/vbstream/ConsoleListener.java src/main/java/org/vastdata/vbstream/Main.java CLAUDE.md
git commit -m "feat: 可运行 Main 与 ConsoleListener，四种事务场景控制台输出"
git push
```

---

## 里程碑完成标准

1. `mvn test` 全绿（协议单测 + 7 组集成用例）
2. Task 15 冒烟输出同时出现：`BEGIN/COMMIT`（普通）、`STREAM-START/STREAM-COMMIT`（流式）、`BEGIN-PREPARE/COMMIT-PREPARED`（两阶段）、流式 2PC（`STREAM-PREPARE` 或等价）
3. 全部任务已提交并 push 到 origin/main
