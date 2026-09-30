# debezium19 模块自含化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 移除 `vb-stream-connector-postgres-stream-debezium19` 对 `io.debezium:debezium-connector-postgres` 的依赖，所用 vanilla 类裁剪复刻进自有命名空间，模块打包/运行全自含。

**Architecture:** 三阶段推进——先在 vanilla 依赖仍在的前提下按依赖序把 21+2 文件复刻进 `org.vastdata.debezium.connector.postgresql.*`（新包，与 `.stream` 平级）；再全量切换接缝 import（主源码+测试）；最后删依赖、加字节码边界测试钉死。行为锚定 = 现有 275 用例断言面零改动。

**Tech Stack:** Java 17 + Maven；vanilla 源基准 = 本地仓 sources jar（Apache 2.0）；JUnit 6 + Testcontainers。

**Spec:** `docs/superpowers/specs/2026-09-30-debezium19-selfcontained-design.md`

## Global Constraints

- 桶 A（`src/main/java/org/vastdata/debezium/connector/postgresql/stream/` 的 protocol 子包与内核文件）**零改动**——本次只动桶 B 接缝文件的 import 行与新增 vendored 包
- 复刻文件保留 vanilla Apache 2.0 版权头，javadoc 首段加一行：`复刻自 io.debezium.connector.postgresql.Xxx（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻）` + 裁剪说明
- 复刻文件逻辑零改动（仅：包声明/import 迁移、§列明的裁剪、javadoc 来源标注）；vanilla 的其它 `io.debezium.*`（core）import 原样保留
- vanilla 源码唯一取源处（勿用 /tmp 残留）：`/Users/saxisuer/Documents/Repository/io/debezium/debezium-connector-postgres/1.9.7.Final/debezium-connector-postgres-1.9.7.Final-sources.jar`
- 新包命名：`io.debezium.connector.postgresql` → `org.vastdata.debezium.connector.postgresql`（子包结构镜像 vanilla：根 / `connection` / `connection.pgoutput` / `data`）
- 开发规约（方法名英文 camelCase、import 简名、slf4j、全函数 javadoc）适用于**新写**代码；复刻文件沿用 vanilla javadoc 形态
- 每任务完成即 commit（跨电脑开发，任务粒度 commit + push）
- 验证编译必须 `mvn clean test-compile`（增量编译假绿陷阱）；模块测试命令统一带 `-pl vb-stream-connector-postgres-stream-debezium19`
- 离线回归（无 Docker）：`mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest='!*IT'`；全量（需 Docker）：`mvn -pl vb-stream-connector-postgres-stream-debezium19 test`

## 机械变换模板（所有复刻任务共用）

每个复刻文件 = 以下四步，任务内逐文件执行：

1. `cp` vanilla 源文件到 `vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/<同名路径>`
2. 包声明与自有 import 切换：文件内 `io.debezium.connector.postgresql` 全部替换为 `org.vastdata.debezium.connector.postgresql`（含 package 行与 import 行；`io.debezium` core 的 import 不动）
3. 应用该任务的裁剪表（删除引用排除集类型的成员/分支，删后必须无悬挂引用）
4. javadoc 类首段插来源标注行；vanilla 版权头保留

## Task 依赖序

1(叶子) → 2(连接/类型) → 3(TOAST 链) → 4(转换/Schema) → 5(offset/dispatcher) → 6(Config/Connector 自有化) → 7(接缝全量切换+离线回归) → 8(删依赖+边界测试) → 9(全量回归+文档)

---

### Task 1: 复刻基座——sources 提取 + 四叶子文件 + PgConnectionSupplier 新接口

**Files:**
- Create: `vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/PgOid.java`
- Create: `.../postgresql/PostgresType.java`
- Create: `.../postgresql/connection/Lsn.java`
- Create: `.../postgresql/data/Ltree.java`
- Create: `.../postgresql/connection/PgConnectionSupplier.java`（新写，非复刻）

**Interfaces:**
- Produces: 上述五类型，包名 `org.vastdata.debezium.connector.postgresql(.connection/.data)`，类名/签名与 vanilla 逐字一致（PgConnectionSupplier 除外——见步骤）
- PgConnectionSupplier 形态（收编 vanilla `PostgresStreamingChangeEventSource` 的嵌套接口，供 Task 3 的 `ReplicationMessage`/`PgOutputReplicationMessage` 引用）：

```java
package org.vastdata.debezium.connector.postgresql.connection;

import org.vastdata.debezium.connector.postgresql.PostgresConnection;

/**
 * 责任:复制消息解码点按需取 JDBC 连接的供给口。
 * 来源:收编自 io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier
 * (debezium-connector-postgres 1.9.7.Final)——原为嵌套接口,自含化时提为顶层(其宿主类属排除集)。
 */
public interface PgConnectionSupplier {
    PostgresConnection get();
}
```

- [ ] **Step 1: 提取 vanilla sources 到工作目录**

```bash
rm -rf /tmp/vanilla19-src && mkdir -p /tmp/vanilla19-src && \
cd /tmp/vanilla19-src && unzip -q \
  /Users/saxisuer/Documents/Repository/io/debezium/debezium-connector-postgres/1.9.7.Final/debezium-connector-postgres-1.9.7.Final-sources.jar
```

预期：`io/debezium/connector/postgresql/` 树就位（79 文件）。后续任务的 vanilla 源均指此目录。

- [ ] **Step 2: 按机械变换模板复刻四叶子文件（无裁剪）**

`PgOid.java`、`PostgresType.java`（根包）、`connection/Lsn.java`、`data/Ltree.java`——对 vanilla 同路径同名文件执行模板四步（模板第 3 步本任务为空）。

- [ ] **Step 3: 新写 PgConnectionSupplier**（内容见 Interfaces 块）

- [ ] **Step 4: 编译验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test-compile`
Expected: BUILD SUCCESS（新包与 vanilla 依赖并存，无冲突）

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/
git commit -m "feat(debezium19): 自含化基座——PgOid/PostgresType/Lsn/Ltree 复刻 + PgConnectionSupplier 收编为顶层接口"
```

---

### Task 2: 连接与类型注册表——PostgresConnection 裁剪 + ServerInfo 裁剪 + TypeRegistry

**Files:**
- Create: `.../postgresql/connection/PostgresConnection.java`（**裁剪**）
- Create: `.../postgresql/connection/ServerInfo.java`（**深度裁剪**）
- Create: `.../postgresql/TypeRegistry.java`（无裁剪）

**Interfaces:**
- Consumes: Task 1 全部
- Produces: `PostgresConnection` 的**保留面**（接缝代码实际调用，2026-09-30 grep 实证）：
  - `public static final String CONNECTION_GENERAL`
  - 构造器：`(JdbcConfiguration, String)` 与 `(JdbcConfiguration, PostgresValueConverterBuilder, String)`
  - `PostgresConnection connection()`（返回内部 JDBC 连接，供 `getMetaData()`）
  - `void setAutoCommit(boolean)` / `void commit()` / `void close()`（含 throws 形态与 vanilla 一致）
  - `Charset getDatabaseCharset()`
  - `TypeRegistry getTypeRegistry()` / `PostgresDefaultValueConverter getDefaultValueConverter()`（注意：返回类型经 import 切换指向本包 Task 3 的类——本任务结束时若 Task 3 未建会编译失败，故本任务与 Task 3 在同一编译 gate 下先建骨架再统一验证，见 Step 4）
  - `Optional<Column> readColumnForDecoder(ResultSet, TableId, Tables.ColumnNameFilter)`
  - `List<String> readPrimaryKeyNames(TableId)` / `Set<TableId> readTableUniqueIndices(TableId)`
  - `ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId)`
  - 嵌套接口 `PostgresValueConverterBuilder`（`PostgresValueConverter build(TypeRegistry)`；返回类型指向 Task 4 的本包 `PostgresValueConverter`）

- [ ] **Step 1: 复刻 TypeRegistry**（模板四步，无裁剪；其 `TypeRegistry(PostgresConnection)` 构造经 import 切换指向本包连接）

- [ ] **Step 2: 深度裁剪复刻 ServerInfo**

vanilla `connection/ServerInfo.java`（203 行）只保留：类壳 + 嵌套 `enum ReplicaIdentity`（含其 getValue/toBoolean 全部成员）。删除：其余全部嵌套类型与字段（`ServerInfo.Tooltip`、pg configuration 探测面等）。类 javadoc 裁剪说明注明"仅保留 ReplicaIdentity（PostgresSchema.assure... 的唯一使用面）"。

- [ ] **Step 3: 裁剪复刻 PostgresConnection**

vanilla `connection/PostgresConnection.java`（678 行）应用模板 + 裁剪表：
- **删除**（签名/实现引用排除集类型，编译不可达）：`readServerInfo()`、`printReplicationSlotInfo()`、`getSlotState`/槽状态查询族、`prepareReplicationStream`/`createReplicationStream`/`openConnection` 复制流族、所有 `ReplicationConnection`/`MessageDecoder`/decoder 常量与成员、`executeWithAutoCommit` 中仅为复制流服务的重载（若仅被删除成员调用）
- **保留**：Interfaces 块列出的全部成员 + `execute(String)`/`query(...)` 等通用查询辅助（enrich 循环与 `readReplicaIdentityInfo` 依赖）+ 静态 `defaultPort`/`resolveDatabaseContext` 等被保留成员依赖的私有辅助
- 删完后全文无 `io.debezium.connector.postgresql.connection.ReplicationConnection|MessageDecoder|wal2json|pgproto` 残留引用（grep 验证）

- [ ] **Step 4: 编译验证（Task 2+3 联合 gate）**

`PostgresConnection` 的返回类型引用 Task 3 的 `PostgresDefaultValueConverter`、Task 4 的 `PostgresValueConverter`——先创建 Task 3 的两文件（`PostgresDefaultValueConverter`、`ReplicationMessageColumnValueResolver`）完成复刻，再一起编译。若严格串行执行：本 Step 允许先只编译到"未解析符号"清单为空以外的 Task 3 文件就位为止。

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test-compile`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/
git commit -m "feat(debezium19): PostgresConnection 裁剪复刻成 JDBC 查询包装 + ServerInfo 仅存 ReplicaIdentity + TypeRegistry 复刻"
```

---

### Task 3: TOAST 哨兵链与列值转换载体

**Files:**
- Create: `.../postgresql/connection/PostgresDefaultValueConverter.java`（无裁剪）
- Create: `.../postgresql/connection/ReplicationMessageColumnValueResolver.java`（无裁剪）
- Create: `.../postgresql/connection/ReplicationMessage.java`（无裁剪；嵌套 Column/ColumnValue/Operation/NoopMessage 随文件）
- Create: `.../postgresql/connection/AbstractReplicationMessageColumn.java`（无裁剪）
- Create: `.../postgresql/UnchangedToastedReplicationMessageColumn.java`（无裁剪）
- Create: `.../postgresql/connection/pgoutput/PgOutputReplicationMessage.java`（无裁剪；其 `PgConnectionSupplier` 引用经 import 切换指向 Task 1 本包接口，static `getValue` 是 `TypeRegistryColumnValueMapper` 的运行期主力）

**Interfaces:**
- Consumes: Task 1（Lsn/PgConnectionSupplier）、Task 2（TypeRegistry/PostgresConnection）
- Produces: `PgOutputReplicationMessage.getValue(String columnName, PostgresType type, String fullType, String rawValue, PgConnectionSupplier connection, boolean includeUnknownDatatypes)` 与 `UnchangedToastedReplicationMessageColumn` 构造器——Task 7 切换后 `.stream/TypeRegistryColumnValueMapper` 的调用面

- [ ] **Step 1: 六文件按模板复刻**（裁剪表为空；若 Task 2 已提前建过前两文件则跳过）

- [ ] **Step 2: 编译验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test-compile`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/
git commit -m "feat(debezium19): TOAST 哨兵链与 PgOutputReplicationMessage 复刻——typed 模式列值转换载体自含"
```

---

### Task 4: 值转换器与 Schema 链

**Files:**
- Create: `.../postgresql/PostgresValueConverter.java`（**裁剪一处**）
- Create: `.../postgresql/PostgresSchema.java`（无裁剪；`ServerInfo` import 切到本包 Task 2 裁剪版）
- Create: `.../postgresql/PostgresTopicSelector.java`（无裁剪）
- Create: `.../postgresql/SourceInfo.java`（无裁剪）

**Interfaces:**
- Consumes: Task 1-3
- Produces:
  - `PostgresValueConverter.of(Configuration/PostgresConnectorConfig?, Charset, TypeRegistry)` 静态工厂与 11 `byte[]` 构造参形态（对 Task 6 的本包 Config 有 import 依赖——与 Task 6 同 gate，见 Step 3）
  - `PostgresSchema`（5 参构造）+ `assureNonNestedTable`/`tableFor` 等 `.stream/StreamPostgresSchema` 包装面所需
  - `PostgresTopicSelector.create(config)`、`SourceInfo` 常量面

- [ ] **Step 1: 裁剪复刻 PostgresValueConverter**

模板 + 裁剪表（唯一一处，2026-09-30 实证 vanilla 行 973-975）：删除 `data instanceof PgProto.Point` 分支（point 列经 unknown 面处理）。该分支只为 pgproto 解码产物存在，本连接器协议自建永不产出该类型。javadoc 裁剪说明记档。注意其 `HStoreHandlingMode`/`IntervalHandlingMode` import 切换后指向 **Task 6 的本包 Config 嵌套枚举**。

- [ ] **Step 2: 复刻 PostgresSchema / PostgresTopicSelector / SourceInfo**（模板，无裁剪）

- [ ] **Step 3: 编译验证（与 Task 6 联合 gate）**

`PostgresValueConverter` 引用本包 Config 嵌套枚举——若严格串行，本 Step 预期 FAIL（符号未找到：`org.vastdata.debezium.connector.postgresql.PostgresConnectorConfig`），记下后直接进 Task 6 建好 Config 再回来验证；或与 Task 6 合并为一个执行批。

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test-compile`
Expected: 与 Task 6 联合后 BUILD SUCCESS

- [ ] **Step 4: Commit（与 Task 6 的产出分开提交，本任务四文件单独一笔）**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/PostgresValueConverter.java \
        vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/PostgresSchema.java \
        vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/PostgresTopicSelector.java \
        vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/SourceInfo.java
git commit -m "feat(debezium19): PostgresValueConverter(裁 PgProto.Point 死分支)/PostgresSchema/TopicSelector/SourceInfo 复刻"
```

---

### Task 5: offset 与 dispatcher 链

**Files:**
- Create: `.../postgresql/PostgresOffsetContext.java`（**裁剪一处**）
- Create: `.../postgresql/PostgresPartition.java`（无裁剪）
- Create: `.../postgresql/PostgresEventDispatcher.java`（**裁剪**）
- Create: `.../postgresql/PostgresEventMetadataProvider.java`（无裁剪）
- Create: `.../postgresql/PostgresErrorHandler.java`（无裁剪）

**Interfaces:**
- Consumes: Task 2/4（PostgresConnection、PostgresSchema/Config 面）
- Produces:
  - `PostgresOffsetContext` + 嵌套 `Loader`（`.stream/PostgresStreamConnectorTask` 使用）
  - `PostgresEventDispatcher` 11 参构造（`.stream/DispatcherTransactionListener` 使用；裁剪后不含 logical message 面）
  - `PostgresErrorHandler(config, queue)` 构造

- [ ] **Step 1: 裁剪复刻 PostgresOffsetContext**

模板 + 裁剪表：删除 `asOffsetState()` 方法与 `spi.OffsetState` import（2026-09-30 实证 vanilla 行 255 起；调用方全在排除集的槽状态打印面）。javadoc 裁剪说明记档。

- [ ] **Step 2: 裁剪复刻 PostgresEventDispatcher**

模板 + 裁剪表：删除 `LogicalDecodingMessage` 面——`dispatchLogicalDecodingMessage(...)`、`enqueueLogicalDecodingMessage(...)`、字段 `logicalDecodingMessageMonitor`/`messageFilter` 及构造体内对应初始化、相关 import（含 `LogicalDecodingMessageMonitor`/`LogicalDecodingMessageFilter` 两个根包类型的 import）。依据：MS3.5 裁定逻辑消息解析不发射，dispatcher 该路径运行期不可达（vanilla 81 行小文件，裁后约半）。javadoc 裁剪说明记档。

- [ ] **Step 3: 复刻 PostgresPartition / PostgresEventMetadataProvider / PostgresErrorHandler**（模板，无裁剪）

- [ ] **Step 4: 编译验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test-compile`
Expected: 与 Task 6 联合后 BUILD SUCCESS（dispatcher 引用本包 Config/Schema 面，Task 4/6 就位后过）

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/
git commit -m "feat(debezium19): offset/dispatcher 链复刻——PostgresOffsetContext 裁 asOffsetState、PostgresEventDispatcher 裁逻辑消息发射面"
```

---

### Task 6: Config 与 Connector 自有化（接缝首批切换）

**Files:**
- Create: `.../postgresql/PostgresConnectorConfig.java`（**裁剪复刻**，vanilla 1437 行）
- Modify: `.../stream/PostgresStreamConnector.java`（去 `extends PostgresConnector`，改 `extends RelationalBaseSourceConnector`）
- Modify: `.../stream/PostgresStreamConnectorConfig.java`（父类切到本包 Config）
- Test: 既有 `PostgresStreamConnectorConfigTest` 为行为锚（本任务后须绿）

**Interfaces:**
- Consumes: Task 1-5 全部；debezium-core `RelationalDatabaseConnectorConfig`/`RelationalBaseSourceConnector`
- Produces:
  - 本包 `PostgresConnectorConfig`：保留 vanilla 全部字段/getter 面（TOPIC_PREFIX/heartbeat/decimal/hstore/interval/timespan/unknown/datatype/ssl/tombstone/heartbeat.action.query/...）与嵌套枚举（含 `HStoreHandlingMode`/`IntervalHandlingMode`——Task 4 已依赖）；**剔除** snapshotter 族与 plugin 多插件面（见裁剪表）
  - `.stream` 两接缝类对外行为不变（validateAllFields 离线门槛五字段、taskConfigs 注入、version()）

- [ ] **Step 1: 裁剪复刻 PostgresConnectorConfig**

模板 + 裁剪表（2026-09-30 实证 vanilla 行 36-43 import 与行 184-230 枚举体）：
- `SnapshotMode` 枚举改为**纯符号枚举**：保留 `ALWAYS/INITIAL/NEVER/INITIAL_ONLY/EXPORTED/CONFIGURATION` 等别名常量（值面照旧），删除 `SnapshotterBuilder builderFunc` 字段、`getSnapshotter()`/`buildSnapshotter()`、`CUSTOM("custom", ...)` 的实例化 lambda 与 `snapshot.custom.class` 读取（`SNAPSHOT_MODE_CLASS` Field 删除）、`io.debezium.connector.postgresql.spi.Snapshotter` 与四个 snapshotter import
- `PLUGIN_NAME` 处理：删除 `PgProtoMessageDecoder`/`NonStreamingWal2JsonMessageDecoder`/`StreamingWal2JsonMessageDecoder` import 及 decoder 映射成员；`plugin.name` 若作为 Field 存在则保留但取值面裁为 pgoutput 语义（无 decoder 类引用）
- 其余（含 `getJdbcConfig()`/`getLogicalName()`/`getContextName()`/`getTableFilters()`/queue 四参 getters/`schemaNameAdjustmentMode()`）逐字保留
- 类 javadoc 裁剪说明记档

- [ ] **Step 2: 切换 PostgresStreamConnectorConfig 父类**

`extends io.debezium.connector.postgresql.PostgresConnectorConfig` → `extends org.vastdata.debezium.connector.postgresql.PostgresConnectorConfig`（import 行替换）。其内部"SNAPSHOT_MODE 同名替换"逻辑语义不变——替换的是本包父的 Field。

- [ ] **Step 3: 改写 PostgresStreamConnector 装配**

- 删 `import io.debezium.connector.postgresql.PostgresConnector;`，`extends PostgresConnector` → `extends RelationalBaseSourceConnector`（import `io.debezium.relational.RelationalBaseSourceConnector`）
- 补齐抽象面（2026-09-30 实证 vanilla `PostgresConnector.java`（149 行，`extends RelationalBaseSourceConnector`）的成员清单：`version()` / `taskClass()` / `start(Map)` / `taskConfigs(int)` / `stop()` / `config()` / `validateConnection(Map, Configuration)` / `validateAllFields(Configuration)`）：
  - `taskClass()` 返回 `PostgresStreamConnectorTask.class`；`version()` 返回 `Module.version()`（本包 `.stream/Module` 已有）
  - `start`/`stop`/`config`/`validateConnection` 照 vanilla 形态实现（`validateConnection` 的槽/权限探测若引用已裁剪的连接方法，裁至"离线五字段门槛"并与现有 `PostgresStreamConnector.validate` 行为一致——以 `PostgresStreamConnectorConfigTest`/`DefaultsAndMetricsIT` 场景 2 为锚）
  - `taskConfigs`/`validateAllFields` 现有覆写**逐字不动**（仅 import 面）

- [ ] **Step 4: 编译 + 配置面测试回归**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test -Dtest='PostgresStreamConnectorConfigTest,AbstractStreamITContractTest'`
Expected: PASS（Config 行为锚；IT 类被 `-Dtest` 显式点名外的全排除）

注：此步其余 `.stream` 接缝仍 import vanilla 类型，vanilla 依赖仍在——混合态编译通过即预期。

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/
git commit -m "feat(debezium19): Config 裁剪复刻(剔 snapshotter/多插件面) + Connector 改 extends RelationalBaseSourceConnector 自有装配"
```

---

### Task 7: 接缝全量 import 切换（主源码 + 测试）

**Files:**
- Modify: `.stream` 包全部引用 vanilla 类的主源码（2026-09-30 实测 14 文件）：`TypeRegistryColumnValueMapper`/`TruncateEmitter`/`StringValueConverter`/`DispatcherTransactionListener`/`RowChangeEmitter`/`PostgresStreamConnectorConfig`/`PostgresStreamConnector`/`StreamPostgresSchema`/`PostgresStreamConnectorTask`/`StreamEventMetadataProvider`/`RelationMetadataSource`/`StreamChangeEventSourceFactory`/`PostgresStreamStreamingChangeEventSource`/`TypeRegistryColumnValueMapper` 所在 `Module.java` 仅 javadoc 提及不用动
- Modify: 测试侧引用 8 类型 + `NeverSnapshotter` 字符串一处（`PostgresStreamConnectorConfigTest:330` 附近）

**Interfaces:**
- Consumes: Task 1-6 全部产出
- Produces: 模块 main 源码 `grep -r 'io\.debezium\.connector\.postgresql\.' src/main/java` 结果为**仅剩 javadoc 文字性提及**（可后续清理）——代码引用清零

- [ ] **Step 1: 主源码 import 切换**

14 文件执行：`io.debezium.connector.postgresql` → `org.vastdata.debezium.connector.postgresql`（import 行与代码内 FQN；`PostgresStreamingChangeEventSource.PgConnectionSupplier` 引用改为本包 `connection.PgConnectionSupplier`——`.stream/PostgresStreamStreamingChangeEventSource` 与 `RelationMetadataSource` 各一处）。javadoc 里的 vanilla 提及保留原文（历史对照），但**类型链接**若指向已消失的 vanilla 类需改本包简名。

- [ ] **Step 2: 测试侧切换 + NeverSnapshotter 字符串修正**

- 8 类型 import 同规则切换
- `PostgresStreamConnectorConfigTest` 中 `"snapshot.custom.class", "io.debezium.connector.postgresql.snapshot.NeverSnapshotter"` 用例：该维随 `SNAPSHOT_MODE_CLASS` 删除而失效——改为纯非法 `snapshot.mode` 值断言（语义不变：非法值仍 validate/启动期拒绝），删除 custom class 维

- [ ] **Step 3: 离线全量回归**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test -Dtest='!*IT'`
Expected: 全 PASS（离线单测 + 基座契约；vanilla 依赖此时仍在 pom，但 main/test 代码已零引用）

- [ ] **Step 4: Commit + push**

```bash
git add vb-stream-connector-postgres-stream-debezium19/src
git commit -m "refactor(debezium19): 接缝全量切换自有命名空间——主源码 14 文件与测试 import 迁移，vanilla 代码引用清零"
git push
```

---

### Task 8: 移除依赖 + 字节码边界测试 + 打包验证

**Files:**
- Modify: `vb-stream-connector-postgres-stream-debezium19/pom.xml`
- Create: `vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/VanillaBoundaryTest.java`

**Interfaces:**
- Consumes: `target/classes` 产物
- Produces: pom 无 `debezium-connector-postgres`；显式 `io.debezium:debezium-core:${debezium19.version}`（compile）；`VanillaBoundaryTest` 常驻防回归

- [ ] **Step 1: 写边界测试（先写后改 pom，红-绿闭环）**

```java
package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * 责任:模块自含性边界钉死——扫描 target/classes 全部 .class 字节码,断言无任何
 * io/debezium/connector/postgresql 常量池引用(vanilla 连接器类不得回流)。
 * 边界:仅静态扫描已编译产物,不运行期加载;缺 target/classes(未编译直接跑)时
 * 先由 surefire 的 test-compile 阶段保证存在,找不到即 fail。
 * 线程约束:无共享状态。
 */
class VanillaBoundaryTest {

    @Test
    void noClassReferencesVanillaPostgresConnectorPackage() throws IOException {
        Path classes = Path.of("target", "classes");
        assertTrue(Files.isDirectory(classes), "target/classes 缺失——先 mvn compile");
        try (Stream<Path> files = Files.walk(classes)) {
            // 字节码常量池里的包引用形如 io/debezium/connector/postgresql(斜杠形态);
            // 自有包前缀是 org/vastdata/debezium,不会误命中
            long violations = files.filter(p -> p.toString().endsWith(".class"))
                    .filter(p -> containsVanillaReference(p))
                    .count();
            assertTrue(violations == 0, "发现引用 vanilla PG 连接器包的类: " + violations + " 个");
        }
    }

    private boolean containsVanillaReference(Path classFile) {
        try {
            byte[] bytes = Files.readAllBytes(classFile);
            byte[] needle = "io/debezium/connector/postgresql".getBytes();
            outer:
            for (int i = 0; i <= bytes.length - needle.length; i++) {
                for (int j = 0; j < needle.length; j++) {
                    if (bytes[i + j] != needle[j]) {
                        continue outer;
                    }
                }
                return true;
            }
            return false;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
```

先运行（pom 尚未改，vanilla 仍在但代码零引用）：Expected PASS（代码已不引用）；若 FAIL 说明 Task 7 有漏网，回去补。

- [ ] **Step 2: 改 pom**

- 删除 `io.debezium:debezium-connector-postgres` 依赖块（含其注释，注释改为记录自含化裁定并指向 spec）
- 原位置新增：

```xml
        <!-- Debezium core 1.9.7(框架层:EventDispatcher 基类/ChangeEventQueue/relational 体系);
             原 vanilla 连接器依赖已自含化(spec 2026-09-30):所用类裁剪复刻进
             org.vastdata.debezium.connector.postgresql;宿主同版本类重复无害(first-win),
             ConnectPluginIT 独立 runtime 亦需本件落 lib/ -->
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-core</artifactId>
            <version>${debezium19.version}</version>
        </dependency>
```

- assembly `plugin.xml`：排除清单**不动**（防御性保留）

- [ ] **Step 3: 移除后全验证（关键 gate：类路径无 vanilla 也能编译运行）**

```bash
mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test -Dtest='!*IT'
```
Expected: 全 PASS（含 VanillaBoundaryTest）——这一步同时证明编译期零 vanilla 引用

- [ ] **Step 4: 打包产物断言**

```bash
mvn -pl vb-stream-connector-postgres-stream-debezium19 package -DskipTests && \
ZIP=vb-stream-connector-postgres-stream-debezium19/target/vb-stream-connector-postgres-stream-debezium19-plugin/vb-stream-connector-postgres-stream-debezium19-plugin.zip && \
echo "vanilla 计数(应为 0):" && unzip -l $ZIP | grep -c 'debezium-connector-postgres' ; \
echo "core(应有一行):" && unzip -l $ZIP | grep 'debezium-core' | head -1
```
Expected: vanilla 计数 0（不在产物）；`lib/debezium-core-1.9.7.Final.jar` 在列

- [ ] **Step 5: Commit + push**

```bash
git add vb-stream-connector-postgres-stream-debezium19/pom.xml vb-stream-connector-postgres-stream-debezium19/src/test
git commit -m "feat(debezium19): 移除 debezium-connector-postgres 依赖——显式 debezium-core + VanillaBoundaryTest 字节码边界钉死"
git push
```

---

### Task 9: 全量回归（Docker IT）+ 文档同步 + 收官

**Files:**
- Modify: `vb-stream-connector-postgres-stream-debezium19/CLAUDE.md`
- Modify: `vb-stream-connector-postgres-stream-debezium19/README.md`
- Modify: 根 `CLAUDE.md`（模块行一句话）
- Modify: `vb-stream-connector-postgres-stream-debezium19/src/main/assembly/plugin.xml`（仅注释措辞若过时，行为不动——可选）

**Interfaces:**
- Consumes: Task 8 的完整产物
- Produces: 全绿回归 + 文档与实现一致

- [ ] **Step 1: 全量测试（需本机 Docker；含 27 IT 与 ConnectPluginIT——后者是自含性硬需求的端到端验收锚）**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test`
Expected: 276 用例全 PASS（275 + 新增 VanillaBoundaryTest；ConnectPluginIT 自动含 package 产物前置断言）

若 ConnectPluginIT 因产物缺件 fail-fast：先 `mvn -pl vb-stream-connector-postgres-stream-debezium19 package -DskipTests` 再跑。

- [ ] **Step 2: 文档同步**

- 模块 `CLAUDE.md`：定位节依赖面（"整模块复制+接缝改写"补"vanilla 类自含裁剪复刻，pom 零 connector-postgres 依赖"）；源码三桶表增 vendored 子系统行（21+2 文件清单指针到 spec §4.2）；"1.9.7 API 差异清单"节加前言"已自含，差异清单转为复刻裁剪的历史依据"；测试矩阵数字更新（+1 边界用例）
- 模块 `README.md`：定位/打包安装节（lib/ 清单变化：无 vanilla jar、debezium-core 显式）+ 与 3.6.1 模块差异表新增"vanilla 依赖自含化"行——**两处文档的差异表条目同步维护**
- 根 `CLAUDE.md`：debezium19 模块行补一句自含化
- 前置 spec `2026-09-30-debezium19-connector-design.md` 的 D3 行加勘误指针（指向自含化 spec）

- [ ] **Step 3: 全仓回归（防跨模块误伤）**

Run: `mvn clean test`
Expected: 全仓绿（引擎 224 + 其余模块 + 本模块 276）

- [ ] **Step 4: Commit + push**

```bash
git add -A
git commit -m "docs(debezium19): 自含化收官——模块 CLAUDE/README/根索引同步,全仓回归绿"
git push
```

---

## 风险提示（执行者必读）

1. **Task 2/4/5/6 存在编译相互依赖**（连接→转换器→Config 链）：串行执行时遇到"符号未找到"属预期，按各任务 Step 说明联动验证；子代理逐任务派发时建议 Task 2+3、Task 4+5+6 各为一个批次
2. **裁剪以编译器为最终裁判**：裁剪表之外的成员若引用排除集类型，一并删除并在 javadoc 裁剪说明补记；反之被保留成员依赖的辅助**必须保留**
3. **禁止顺手重构**：复刻文件除模板四步与裁剪表外零改动——diff 噪声会让"与 vanilla 对照"失效
4. **`mvn clean` 不可省**（假绿陷阱，见 Global Constraints）
5. ConnectPluginIT 首跑前确认 Docker daemon 在跑
