# MS1 connector 协议层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `vb-stream-connector-postgres-stream` 模块落成协议层——Debezium 风格重写 pgoutput 流式解码器(含流式块与两阶段消息)+ Connector/Task/Config 最小子系,全部单测(不起 PG、不连库)。

**Architecture:** pom 引入 `io.debezium:debezium-connector-postgres:3.6.1.Final`;`protocol` 子包按引擎 `vb-stream-engine` 协议包 1:1 重写(字节格式权威 = 引擎源码,见各任务参照表);连接器三件套(`PostgresStreamConnector`/`PostgresStreamConnectorConfig`/`PostgresStreamConnectorTask`)按 Debezium 3.6 API 面实现最小骨架(任务不连库,poll 返回空,配置校验经基类 `validateAndRecord` 自动 fail-fast)。

**Tech Stack:** Java 17、Maven 多模块、io.debezium 3.6.1.Final、JUnit 6(仓库既定 6.1.3)、slf4j。

**Spec:** `docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md` §5.1(配置面)、§10 MS1;字节格式第一手总表 `docs/superpowers/specs/2026-08-26-pgoutput-stream-decoder-design.md` 附录 A。

## Global Constraints

- **禁止 import `org.vastdata.vbstream` 任何类**(spec D2:重写不移植;connector pom 不依赖 vb-stream-engine,结构性保证)。引擎源码只作只读参照,各任务给出精确参照路径与行号
- Debezium 版本锚定 **`3.6.1.Final`**(Central 最新 Final,2026-09-01 查证;3.7 线仅 Beta 不取)
- Java 17 / UTF-8;**每个函数(含私有与测试辅助)必须有 javadoc 逻辑描述**(项目规约,record 组件与常量同样注明);**日志一律 slf4j,禁止 System.out/err**
- 协议层保持引擎侧包边界:除 `java.*` 与 `org.slf4j` 外零依赖(protocol 包不 import Debezium 类)
- 引擎侧既有语义逐条保留(重写验收口径):fail-fast 异常语义、剩余字节检查、`OptionalLong streamXid` 约定(非空=流式块内)、`UnchangedToast`≠`Null`、数组组件值相等、"消费不建模"的 flags 字节必须读掉
- 测试断言用 JUnit Jupiter `Assertions` 静态导入(与引擎一致,不引 AssertJ)
- 每任务 commit + push;commit message 末尾 `Co-Authored-By: Claude <noreply@anthropic.com>`
- 验证一律 `clean` 目标;单模块命令带 `-pl vb-stream-connector-postgres-stream`

**引擎参照路径速查**(下文 **P** = `vb-stream-engine/src/main/java/org/vastdata/vbstream/protocol`,**T** = `vb-stream-engine/src/test/java/org/vastdata/vbstream/protocol`;**新包** = `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream`,**新测试包** = 对应 `src/test/java/...`):

| 新类 | 引擎参照 | 备注 |
|---|---|---|
| `protocol.WireReader` | P/ByteBufferReader.java(71 行) | 更名避免与直觉冲突;API 逐方法一致 |
| `protocol.PgOutputMessage` | P/PgOutputMessage.java(96 行) | sealed + 19 record,组件名一致 |
| `protocol.TupleData`/`TupleValue`/`RelationColumn`/`TruncateOption`/`StreamingMode` | P/ 同名(Column→RelationColumn 避与 io.debezium.relational.Column 撞名) | |
| `protocol.NormalParsers`/`DmlParsers`/`StreamParsers`/`TwoPhaseParsers` | P/ 同名 | 包私有 static |
| `protocol.PgOutputStreamDecoder` | P/PgOutputDecoder.java(123 行) | 双入口 + inStream 状态机 |
| `protocol.ProtocolMisalignmentException`/`UnknownMessageTypeException` | P/ 同名 | |
| 测试 `protocol.MsgBuilder` | T/MsgBuilder.java(56 行) | |
| `PostgresStreamConnector`/`PostgresStreamConnectorConfig`/`PostgresStreamConnectorTask`/`Module` | Debezium API(见 Task 7) | 新包根 |

---

### Task 1: pom 依赖接入与 Module

**Files:**
- Modify: `pom.xml`(根:properties 加 `<debezium.version>3.6.1.Final</debezium.version>`)
- Modify: `vb-stream-connector-postgres-stream/pom.xml`
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/Module.java`

**Interfaces:**
- Consumes: 无
- Produces: 编译期依赖 `io.debezium:debezium-connector-postgres` + `org.apache.kafka:connect-api`(provided);`Module.version()/name()/contextName()` 供 Task 7 的 Connector/Task 引用(签名照 Debezium `io.debezium.connector.postgresql.Module`)

- [ ] **Step 1: 根 pom 加版本属性**

`<properties>` 内追加一行:`<debezium.version>3.6.1.Final</debezium.version>`

- [ ] **Step 2: connector pom 加依赖**

`vb-stream-connector-postgres-stream/pom.xml` 的 `<dependencies>`(现有 junit 之后)追加:

```xml
        <!-- Debezium PG 连接器:复用其 Config/Schema/Emitter/快照/offset 体系(spec D3);
             版本锚定 Central 最新 Final(spec §11/D6,2026-09-01 查证),3.7 线仅 Beta 不取 -->
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-connector-postgres</artifactId>
            <version>${debezium.version}</version>
        </dependency>

        <!-- Kafka Connect API:Connector/Task/ConfigDef 编译面;运行期由 Connect runtime 提供 -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>connect-api</artifactId>
            <version>${kafka.connect-api.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- slf4j:日志门面(Debezium 同款 provided 姿态;实现由运行环境/测试域 logback 提供) -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j-api.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- 测试域日志实现 -->
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
            <scope>test</scope>
        </dependency>
```

注意:`${kafka.connect-api.version}` 与 slf4j-api 的版本都**无现成属性/管理**——流程:先只加 debezium 依赖,跑 `mvn -pl vb-stream-connector-postgres-stream dependency:tree` 从输出读出 `org.apache.kafka:connect-api` 与 `org.slf4j:slf4j-api` 的传递解析版本(与 debezium 3.6.1.Final 对齐),写入根 pom 新属性 `<kafka.connect-api.version>` 与 `<slf4j-api.version>`,再落上面两条带版本声明(本仓库无 dependencyManagement,无版本声明会直接构建失败)。两版本号记入提交说明留档。

- [ ] **Step 3: Module 类**

```java
package org.vastdata.debezium.connector.postgresql.stream;

/**
 * 连接器模块元数据:版本号与标识名,供 Connector.version() 与日志/指标上下文引用。
 * 形态对齐 io.debezium.connector.postgresql.Module(常量直读,不经 build.version 资源加载——
 * 本模块暂无打包资源注入环节,MS6 打包时若引入再切换)。
 */
public final class Module {

    /** 模块版本,随仓库 1.0-SNAPSHOT。 */
    public static final String VERSION = "1.0-SNAPSHOT";

    /** 连接器逻辑名(日志与配置校验上下文用),与 Debezium PG 的 "postgresql" 区分。 */
    public static final String NAME = "postgresql-stream";

    /** 指标/日志上下文名(Debezium CdcSourceTaskContext 语境)。 */
    public static final String CONTEXT_NAME = "PostgresStream";

    private Module() {
        // 常量类不可实例化
    }

    /**
     * 返回模块版本号。
     *
     * @return 常量 VERSION,永不抛错
     */
    public static String version() {
        return VERSION;
    }
}
```

- [ ] **Step 4: 验证**

Run: `mvn clean test`
Expected: 三段 reactor SUCCESS(引擎 177 全绿、connector 无测试);`dependency:tree` 输出中 `io.debezium:debezium-connector-postgres:3.6.1.Final`、`org.apache.kafka:connect-api:<锚定号>`、`org.slf4j:slf4j-api:<锚定号>` 可见;`vb-stream-connector-postgres-stream` **不依赖** `vb-stream-engine`(D2 结构保证,tree 里不得出现)。

- [ ] **Step 5: Commit + push**

```bash
git add pom.xml vb-stream-connector-postgres-stream/pom.xml vb-stream-connector-postgres-stream/src
git commit -m "build: connector 接入 Debezium 3.6.1.Final 依赖与 Module 元数据(connect-api provided,版本经 dependency:tree 解析锚定)

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

---

### Task 2: WireReader 读原语 + 双异常 + 测试侧 MsgBuilder

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/protocol/WireReader.java`
- Create: `.../protocol/ProtocolMisalignmentException.java`、`.../protocol/UnknownMessageTypeException.java`
- Test: `.../src/test/java/org/vastdata/debezium/connector/postgresql/stream/protocol/WireReaderTest.java`、`.../protocol/MsgBuilder.java`

**Interfaces:**
- Consumes: 无(纯 JDK)
- Produces(Task 4-6 全靠它):`WireReader`——`WireReader(ByteBuffer)`;`int remaining()`;`byte readByte()`;`int readUnsignedByte()`;`int readUnsignedShort()`;`int readInt()`;`long readUnsignedInt()`(无符号→long);`long readLong()`;`String readString()`(CString 读到 \0);`byte[] readBytes(int)`;`static Instant pgMicrosToInstant(long)`。`MsgBuilder`——链式 `type(char)/i8(int)/i16(int)/i32(int)/i64(long)/str(String)/bytes(byte[])(I32 长度前缀+字节)/build()→ByteBuffer`,均 `throws IOException`

**参照**:P/ByteBufferReader.java 全文(71 行,逐方法 1:1);两异常 P/ 同名(构造器签名一致);MsgBuilder T/MsgBuilder.java 全文(56 行)。关键不可简化点:PG 纪元 946684800s 偏移;`pgMicrosToInstant` 用 floorDiv/floorMod(负微秒正确);xid 一律走 `readUnsignedInt`;`bytes()` 写 I32 长度前缀(专用于 't'/'b' 列值与 LogicalMsg content,列种类字节须另行 `i8('t')`)。

- [ ] **Step 1: 写失败测试**(对照 T/ByteBufferReaderTest.java 三个用例 1:1 翻写:无符号边界 `0xFFFFFFFF→4294967295L`、CString 多字节 UTF-8、`pgMicrosToInstant` 正/负微秒;每个用例 javadoc 说明验证意图;MsgBuilder 的 build 字节序在本测试类内以手拼 `ByteBuffer.wrap(new byte[]{...})` 抽查一处)

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn clean test -pl vb-stream-connector-postgres-stream`
Expected: 编译失败(WireReader/MsgBuilder 不存在)

- [ ] **Step 3: 最小实现**(WireReader/两异常/MsgBuilder,含全部 javadoc;异常消息保留引擎侧"剩余 N 字节"诊断格式)

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn clean test -pl vb-stream-connector-postgres-stream`
Expected: 新测试全绿;connector 模块 BUILD SUCCESS

- [ ] **Step 5: Commit + push**(message:`feat: connector 协议层读原语 WireReader + 双异常 + MsgBuilder 测试基建——引擎 protocol 包 1:1 重写第一步`)

---

### Task 3: 消息模型(19 record + 值类型)

**Files:**
- Create: `.../protocol/PgOutputMessage.java`(sealed interface + 嵌套 record)
- Create: `.../protocol/TupleData.java`、`TupleValue.java`、`RelationColumn.java`、`TruncateOption.java`、`StreamingMode.java`
- Test: `.../protocol/PgOutputMessageTest.java`、`TupleValueTest.java`

**Interfaces:**
- Consumes: 无
- Produces(Task 4-6 依赖):**19 个 record,组件名与类型逐一对照**(全部带 javadoc 组件语义):

```
Begin(long finalLsn, Instant commitTimestamp, long xid)
Commit(long commitLsn, long endLsn, Instant commitTimestamp)
Origin(long originCommitLsn, String originName)
Relation(OptionalLong streamXid, int relationOid, String schema, String table, char replicaIdentity, List<RelationColumn> columns)
Type(OptionalLong streamXid, int typeOid, String schema, String name)
Insert(OptionalLong streamXid, int relationOid, TupleData newTuple)
Update(OptionalLong streamXid, int relationOid, Optional<TupleData> oldTuple, TupleData newTuple)
Delete(OptionalLong streamXid, int relationOid, TupleData oldTuple)
Truncate(OptionalLong streamXid, EnumSet<TruncateOption> options, int[] relationOids)
LogicalMsg(OptionalLong streamXid, boolean transactional, long lsn, String prefix, byte[] content)
StreamStart(long xid, boolean firstSegment)
StreamStop()
StreamCommit(long xid, long commitLsn, long endLsn, Instant commitTimestamp)
StreamAbort(long xid, long subxid, OptionalLong abortLsn, OptionalLong abortTimestamp)
BeginPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid)
Prepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid)
CommitPrepared(long commitLsn, long endLsn, Instant commitTimestamp, long xid, String gid)
RollbackPrepared(long prepareEndLsn, long rollbackEndLsn, Instant prepareTimestamp, Instant rollbackTimestamp, long xid, String gid)
StreamPrepare(long prepareLsn, long endLsn, Instant prepareTimestamp, long xid, String gid)
```

`TupleValue`(sealed):常量 `NULL`/`UNCHANGED_TOAST` + `Null`/`UnchangedToast`/`Text(String)`/`Binary(byte[])`;`RelationColumn(String name, int typeId, int typeModifier, boolean partOfKey)`;`TruncateOption`(CASCADE=bit0/RESTART_IDENTITY=bit1);`StreamingMode`(OFF/ON/PARALLEL)。

**参照**:P/PgOutputMessage.java(96 行)、P/TupleValue.java(30 行)等。**必须保留**:数组组件(`Truncate.relationOids`、`LogicalMsg.content`、`Binary.value`)显式 override equals/hashCode 值相等(P/PgOutputMessage.java:32-50、:53-75、P/TupleValue.java:20-28);`StreamAbort.abortTimestamp` 是微秒**原值**不转 Instant(P:84);不可变 List 构造侧 `List.copyOf`。

- [ ] **Step 1: 写失败测试**(对照 T/PgOutputMessageTest.java 2 用例——LogicalMsg/Truncate 数组值相等、同组件不同数组实例 equal;T/TupleValueTest.java 2 用例——Binary 值相等+hashCode 一致;另加 `UNCHANGED_TOAST != NULL` 与 `UNCHANGED_TOAST` 语义注释断言)
- [ ] **Step 2: 跑测试确认失败**(编译错)
- [ ] **Step 3: 最小实现**(全部 javadoc:record 组件语义、sealed 层级总述)
- [ ] **Step 4: 跑测试确认通过**
- [ ] **Step 5: Commit + push**(`feat: connector 协议层消息模型——PgOutputMessage 19 record + TupleData/TupleValue/RelationColumn/TruncateOption/StreamingMode`)

---

### Task 4: NormalParsers 族 + 剩余字节检查

**Files:**
- Create: `.../protocol/NormalParsers.java`(包私有 static 工具类)
- Test: `.../protocol/NormalParsersTest.java`

**Interfaces:**
- Consumes: Task 2 WireReader、Task 3 消息模型
- Produces(Task 6 decoder 分发调用):`begin(WireReader)`、`commit(WireReader)`、`origin(WireReader)`、`relation(WireReader, OptionalLong)`、`type(WireReader, OptionalLong)`——返回对应 record;本任务同时产出**出口剩余字节检查语义的验证**(实现在 Task 6 的 decoder `finish`,此处测试以"读完 relation 后 remaining==0、多留一字节即抛 ProtocolMisalignmentException"的用例在 parser 层直接断言 reader 状态)

**字节格式表**(权威:P/NormalParsers.java 各方法):

| 消息 | 字节序列(类型字节已由 decoder 剥离) |
|---|---|
| 'B' begin | I64 finalLsn + I64 commitTs(微秒) + I32 xid(无符号) |
| 'C' commit | **I8 flags(读掉不建模)** + I64 commitLsn + I64 endLsn + I64 commitTs |
| 'O' origin | I64 originCommitLsn + CString name |
| 'R' relation | I32 oid + CString schema + CString table + I8 replicaIdentity(char)+ I16 列数 + 每列 [I8 flags(bit0=key) + CString name + I32 typeId + I32 typmod] |
| 'Y' type | I32 typeOid + CString schema + CString name |

**参照**:P/NormalParsers.java(58 行);测试对照 T/NormalParsersTest.java 全部 6 用例 1:1:'B' 高位 xid 无符号 + PG epoch 微秒、'C' flags 字节消费、'O'、'R' 双列 + RelationColumn 相等、'Y'、剩余字节→Misalignment。

- [ ] **Step 1: 写失败测试**(MsgBuilder 造字节,6 用例 + javadoc)
- [ ] **Step 2: 确认失败**(`mvn clean test -pl vb-stream-connector-postgres-stream`,编译错)
- [ ] **Step 3: 最小实现**
- [ ] **Step 4: 确认通过**
- [ ] **Step 5: Commit + push**(`feat: connector 协议层 NormalParsers——B/C/O/R/Y 五类消息解析`)

---

### Task 5: DmlParsers 族

**Files:**
- Create: `.../protocol/DmlParsers.java`
- Test: `.../protocol/DmlParsersTest.java`

**Interfaces:**
- Consumes: Task 2/3
- Produces:`insert(WireReader, OptionalLong)`、`update(WireReader, OptionalLong)`、`delete(WireReader, OptionalLong)`、`truncate(WireReader, OptionalLong)`、`logicalMsg(WireReader, OptionalLong)`、包内 `tupleData(WireReader)`(Task 6 无直接调用,但 decoder 分发后同走此路径)

**字节格式表**(权威:P/DmlParsers.java):

| 消息 | 字节序列 |
|---|---|
| 'I' insert | I32 relationOid + 'N' 标记 + TupleData |
| 'U' update 三形态 | ①'K' 标记 + TupleData(key)→'N' + TupleData ②'O' 标记 + TupleData(old)→'N' + TupleData ③直接 'N' + TupleData |
| 'D' delete | I32 relationOid + 'K' 标记 + TupleData |
| 'T' truncate | I32 关系数 + I8 选项位(bit0=CASCADE,bit1=RESTART_IDENTITY)+ N × I32 oid |
| 'M' logicalMsg | I8 flags(bit0=transactional) + I64 lsn + CString prefix + I32 长度 + content 字节 |
| TupleData | I16 列数 + 每列 [I8 种类('t'→I32 长度+字节 / 'b'→I32 长度+字节 / 'n'→null / 'u'→TOAST 未变)] |

**语义红线**:元组标记校验 fail-fast 抛 `UnknownMessageTypeException`('U' 的 K 后必须是 tuple,'N' 出现在 delete 处拒绝——引擎 P/DmlParsers.java:71 `expectTupleTag` 同款);flags 位隔离('M' 的 bit1 不得误读为 transactional——T/DmlParsersTest.java:120-134 的 bit 隔离用例必须搬)。

**参照**:P/DmlParsers.java(94 行);测试对照 T/DmlParsersTest.java 全部 **12 用例** 1:1(三形态 update、K 后错标记 fail-fast、未知标记、'N' 拒绝、truncate 选项位、'M' content 长度 + flags 位隔离、'u' TOAST + 'b' binary、未知列种类)。

- [ ] **Step 1-5**:同 Task 4 循环(失败测试→跑红→实现→跑绿→commit `feat: connector 协议层 DmlParsers——I/U/D/T/M 与 TupleData 解析,update 三形态与 TOAST 语义`)

---

### Task 6: StreamParsers + TwoPhaseParsers + PgOutputStreamDecoder

**Files:**
- Create: `.../protocol/StreamParsers.java`、`.../protocol/TwoPhaseParsers.java`、`.../protocol/PgOutputStreamDecoder.java`
- Test: `.../protocol/StreamParsersTest.java`、`.../protocol/TwoPhaseParsersTest.java`、`.../protocol/PgOutputStreamDecoderTest.java`

**Interfaces:**
- Consumes: Task 2-5 全部
- Produces(MS2 组装器的解码入口):`PgOutputStreamDecoder(StreamingMode)`;`PgOutputMessage decode(ByteBuffer)`(实例状态机:'S' 置 inStream、'E' 复位);`PgOutputMessage decodeSingle(ByteBuffer, boolean inStream)`(不碰实例字段;白名单仅 **M/R/Y/I/U/D/T** 七类,其余抛 IllegalArgumentException——含 'S'/'E'/'c'/'A' 与全部两阶段类,它们没有可复用的前缀语义);私有 `dispatch(byte, WireReader, boolean)` 统一分发 + 出口剩余字节≠0 抛 `ProtocolMisalignmentException` + 单条 DEBUG 日志。流式块内 xid 前缀:仅上述七类,**位置在类型字节之后、消息体之前**,`inStream ? readUnsignedInt() : empty`(P/PgOutputDecoder.java:120-122)。包私有 parser:`StreamParsers.start/stop/commit/abort(WireReader, StreamingMode)`、`TwoPhaseParsers.beginPrepare/prepare/commitPrepared/rollbackPrepared/streamPrepare`。

**字节格式表**(权威:P/StreamParsers.java、P/TwoPhaseParsers.java):

| 消息 | 字节序列 | 关键点 |
|---|---|---|
| 'S' StreamStart | I32 xid + I8 firstSegment(≠0 即 true) | 自身**无** xid 前缀(无条件字段) |
| 'E' StreamStop | 无字段 | |
| 'c' StreamCommit | I32 xid + I64 commitLsn + I64 endLsn + I64 commitTs | |
| 'a' StreamAbort | I32 xid + I32 subxid + **仅 mode==PARALLEL 追加** I64 abortLsn + I64 abortTime(否则 OptionalLong.empty) | 错读/漏读 16 字节全错位;parallel 模式缺 16 字节→BufferUnderflow 用例必搬 |
| 'b' BeginPrepare | I64 prepareLsn + I64 endLsn + I64 prepareTs + I32 xid + CString gid(**无 flags 字节**) | |
| 'P' Prepare | **I8(0) flags 读掉不建模** + I64 prepareLsn + I64 endLsn + I64 prepareTs + I32 xid + CString gid | 与 'p' 同构,共用私有读取方法仅 record 类型不同 |
| 'K' CommitPrepared | I8(0) + I64 commitLsn + I64 endLsn + I64 commitTs + I32 xid + CString gid | |
| 'r' RollbackPrepared | I8(0) + I64 prepareEndLsn + I64 rollbackEndLsn + **两个** I64 微秒时间戳(prepare、rollback 顺序不可换) + I32 xid + CString gid | |
| 'p' StreamPrepare | I8(0) + 与 'P' 完全同构五字段 | |

**参照**:P/StreamParsers.java(42 行)、P/TwoPhaseParsers.java(66 行,`readPreparedTxn` 复用手法照搬)、P/PgOutputDecoder.java(123 行)。测试对照:T/StreamParsersTest.java **6 用例**(S/E 状态机切换 + 块内 I 前缀有无、'c' epoch 跨度微秒、'A' 非/parallel 附加字段、parallel 缺字节 BufferUnderflow、'E' 无字段)、T/TwoPhaseParsersTest.java **5 用例**('b'/'P'/'K'/'r' 双时间戳顺序/'p')、T/PgOutputStreamDecoderTest 对照 T/PgOutputDecoderTest.java **5 用例**(未知类型字节 0x58 fail-fast、'I' 分发、decodeSingle 前缀契约、前缀假设错配 fail-fast、decodeSingle 不污染 decode 状态机——最后这个用例的本地 byte[] 助手一并翻写)。

- [ ] **Step 1-5**:同循环(三个测试类先红后绿;commit `feat: connector 协议层解码器——流式 S/E/c/a + 两阶段 b/P/K/r/p + 双入口状态机,引擎 protocol 包重写收官`)

---

### Task 7: Config / Connector / Task 最小子系

**Files:**
- Create: 新包根 `PostgresStreamConnectorConfig.java`、`PostgresStreamConnector.java`、`PostgresStreamConnectorTask.java`
- Test: `.../stream/PostgresStreamConnectorConfigTest.java`、`.../stream/PostgresStreamConnectorTest.java`、`.../stream/PostgresStreamConnectorTaskTest.java`

**Interfaces:**
- Consumes: Task 1 Module;Debezium 3.6.1.Final API(下述签名经 3.6.0/3.6.1/3.7.0.Beta1 三 tag 核对一致)
- Produces(MS2 消费):`PostgresStreamConnectorConfig.ALL_FIELDS`(含 4 新 Field);实例方法 `streamingMode()`→StreamingMode、`twoPhase()`→boolean、`pipeDir()`/`pipeRollCycle()`→String;`PostgresStreamConnectorTask` 骨架(七方法)

**Config 骨架**(权威姿势:Debezium `PostgresConnectorConfig.java:632-641` Field 样例 + `:813-820` STREAM_PARAMS):

```java
public class PostgresStreamConnectorConfig extends PostgresConnectorConfig {
    public static final Field SLOT_STREAMING = Field.create("slot.streaming")
            .withDisplayName("Slot streaming").withType(Type.STRING).withDefault("on")
            .withValidation(PostgresStreamConnectorConfig::validateSlotStreaming);   // 枚举校验 OFF/ON/PARALLEL(大小写宽容)
    public static final Field SLOT_TWO_PHASE = Field.create("slot.two.phase")
            .withDisplayName("Slot two-phase").withType(Type.BOOLEAN).withDefault(true)
            .withValidation(Field::isBoolean);
    public static final Field PIPE_DIR = Field.create("pipe.dir")
            .withDisplayName("Pipe directory").withType(Type.STRING).withDefault("pg-stream-pipe-queue");
    public static final Field PIPE_ROLL_CYCLE = Field.create("pipe.roll.cycle")
            .withDisplayName("Pipe roll cycle").withType(Type.STRING).withDefault("MINUTELY");
    public static final Field.Set ALL_FIELDS = PostgresConnectorConfig.ALL_FIELDS.with(SLOT_STREAMING, SLOT_TWO_PHASE, PIPE_DIR, PIPE_ROLL_CYCLE);

    public PostgresStreamConnectorConfig(Configuration config) { super(config); }   // 单行 super,父构造器 public
    @Override public String getConnectorName() { return Module.NAME; }
    @Override public String getContextName() { return Module.CONTEXT_NAME; }
    // getSnapshotMode()/getSnapshotLockingMode()/getSourceInfoStructMaker(Version):父类 PG 实现已具象,无需覆盖

    /** slot.streaming 枚举 + parallel 必须搭配 slot.two.phase=true(spec §5.1 启动期 fail-fast)。 */
    static int validateSlotStreaming(Configuration config, Field field, Field.ValidationOutput problems) {
        // 先验枚举值;再在值为 parallel 且 two.phase 显式 false 时报 problem(返回 1)
    }
    public StreamingMode streamingMode() { /* 大小写宽容解析 OFF/ON/PARALLEL,非法值已被校验挡下,此处 toUpperCase + valueOf */ }
    public boolean twoPhase() { return getConfig().getBoolean(SLOT_TWO_PHASE); }
    public String pipeDir() { return getConfig().getString(PIPE_DIR); }
    public String pipeRollCycle() { return getConfig().getString(PIPE_ROLL_CYCLE); }
}
```

(`validateSlotStreaming` 的完整实现照 Field.ValidationOutput 三元契约写:合法返回 0;枚举非法 `problems.accept(field, value, "...")` 后返回 1;parallel 且 `config.getBoolean(SLOT_TWO_PHASE)==false` 时 `problems.accept(SLOT_TWO_PHASE, false, "slot.streaming=parallel requires slot.two.phase=true")` 返回 1。)

**Connector 骨架**(继承 `PostgresConnector`,最小覆盖):

```java
public class PostgresStreamConnector extends PostgresConnector {
    @Override public String version() { return Module.version(); }
    @Override public Class<? extends Task> taskClass() { return PostgresStreamConnectorTask.class; }
    @Override public ConfigDef config() {
        ConfigDef def = PostgresConnectorConfig.configDef();   // public static,返回可变副本
        // 四个新 Field 经 ConfigDef.define 补进(类型/默认/重要级/描述对照 Field 定义),供 Connect REST 暴露
        def.define(SLOT_STREAMING.getName(), ConfigDef.Type.STRING, "on", ConfigDef.Importance.LOW, "...");
        // ... 同式三条
        return def;
    }
    @Override public Field.Set getConfigFields() { return PostgresStreamConnectorConfig.ALL_FIELDS; }
}
```

**Task 骨架**(`extends BaseSourceTask<PostgresPartition, PostgresOffsetContext>`,七个抽象方法,MS1 不连库):

```java
public class PostgresStreamConnectorTask extends BaseSourceTask<PostgresPartition, PostgresOffsetContext> {
    @Override protected CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
        return new CdcSourceTaskContext<>(config, new PostgresStreamConnectorConfig(config), Map.of());  // 基类 start(Map) :267 立即解引用,必须非 null
    }
    @Override protected ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> start(Configuration config) {
        return null;   // MS1 骨架不连库;MS2 换成流式 source 协调器
    }
    @Override protected List<SourceRecord> doPoll() throws InterruptedException {
        return Collections.emptyList();   // MS1 无记录;MS2 接 ChangeEventQueue
    }
    @Override protected Optional<ErrorHandler> getErrorHandler() { return Optional.empty(); }
    @Override protected void doStop() { }
    @Override public String version() { return Module.version(); }
    @Override public String connectorName() { return Module.NAME; }
    @Override public Field.Set getAllConfigurationFields() { return PostgresStreamConnectorConfig.ALL_FIELDS; }
}
```

**测试**(三个类,不起 Connect runtime——task 不调 `start(Map)`,直接方法级断言;`start(Map)` 全链路留 MS2 embedded engine):
- ConfigTest:①默认值解析(streamingMode()==ON、twoPhase()==true、pipeDir 默认、rollCycle 默认);②`slot.streaming=parallel` + `slot.two.phase=false` → `config.validate(ALL_FIELDS)` 中该 Field 有 1 条错误消息;③`slot.streaming=bad` → 1 条错误;④`ALL_FIELDS` 含 4 个新名字;⑤大小写宽容(`ON`/`on`/`On` 同值)。构造最小可用 Configuration:至少含 hostname/port/user/database 四项必填(照 Debezium 必填面,缺省补 `"localhost"/"5432"/"postgres"/"postgres"`)
- ConnectorTest:`taskClass()`/`version()` 正确;`config()` 返回的 ConfigDef 含 4 个新 key 且 default 与 Field 一致
- TaskTest:直接调 `getAllConfigurationFields()`/`connectorName()`/`version()` 断言;`preStart(最小配置)` 返回非 null 且其 config 类正确(不抛即过)

- [ ] **Step 1-5**:同 TDD 循环(commit `feat: connector 三件套——Config 四新配置项(parallel×two_phase fail-fast)+ Connector/Task 最小骨架(不连库,poll 空)`)

---

### Task 8: 模块收尾——包级文档与全量验收

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/protocol/CLAUDE.md`(对照 P/CLAUDE.md 69 行的结构:包职责、消息类型总表、线程约束、字节格式速查并指向引擎 spec 附录 A 与引擎包作互证)
- Modify: 根 `CLAUDE.md` 源码结构节——connector 模块行从"MS1 起开发"改为实际内容清单(协议层 + 三件套骨架)

**Interfaces:**
- Consumes: Task 1-7
- Produces: MS2 的入口文档

- [ ] **Step 1: 写 protocol/CLAUDE.md**(职责/19 消息表/双入口契约/inStream 状态机/线程约束:decoder 非线程安全单读取线程/与引擎包的 1:1 映射表)
- [ ] **Step 2: 更新根 CLAUDE.md connector 模块行**
- [ ] **Step 3: 全量验收**

Run: `mvn clean test`
Expected: 三段 SUCCESS;connector 模块新测试计数 = WireReader 3 + 消息模型 4+ + Normal 6 + Dml 12 + Stream 6 + TwoPhase 5 + Decoder 5 + Config 5 + Connector 3 + Task 3(≈52);引擎 177 不回归;`grep -rn "org.vastdata.vbstream" vb-stream-connector-postgres-stream/src/` **零命中**(D2 验收)。

- [ ] **Step 4: Commit + push**(`docs: connector 协议层包级文档 + CLAUDE.md 模块行更新——MS1 收官`)

---

## 验收汇总(对照 spec §10 MS1)

- [ ] `mvn clean test` 全绿(引擎 177 + connector 新增单测,全程不起 PG、不连库)
- [ ] connector 模块零引擎 import(grep 验收)
- [ ] 协议层行为对齐引擎:19 消息、xid 前缀七类白名单、parallel abort 附加字段、两阶段五消息、"消费不建模" flags 字节、fail-fast 异常族
- [ ] Config 四新配置项 + parallel×two_phase 启动期校验;Connector/Task 骨架经单测
- [ ] Debezium 依赖 3.6.1.Final,connect-api 版本经 dependency:tree 锚定留档
