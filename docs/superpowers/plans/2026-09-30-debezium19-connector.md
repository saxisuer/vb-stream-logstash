# vb-stream-connector-postgres-stream-debezium19 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 复刻 `vb-stream-connector-postgres-stream`（Debezium 3.6.1.Final）为同包名新模块 `vb-stream-connector-postgres-stream-debezium19`，功能全量对齐 MS1-MS6，运行于内置 Debezium 1.9.7.Final 的宿主产品。

**Architecture:** 整模块复制 + 接缝层改写（spec 方案 A）：桶 A 42 文件（零/近零 Debezium 依赖的协议层/组装/管道/会话/指标内核）原样复制；桶 B 18 文件按 1.9.7 官方 `PostgresConnectorTask`/`PostgresStreamingChangeEventSource` 的装配形态改写；测试侧自建 Jupiter 基座 `AbstractStreamIT` 替代 1.9.7 的 JUnit 4 官方基座。

**Tech Stack:** Java 17 / Maven / Debezium 1.9.7.Final / Kafka connect-api 3.1.0 / pgjdbc 42.7.13 / Chronicle Queue / HdrHistogram / JUnit 6 / Testcontainers 2.x（postgres:18 + cp-kafka/connect 7.1.0）。

**Spec:** `docs/superpowers/specs/2026-09-30-debezium19-connector-design.md`（含已验证的 1.9.7 API 事实清单——本计划所有 1.9.7 签名的依据来源；执行任务前先读 spec §2/§4/§5）

## Global Constraints

- 现有模块 `vb-stream-connector-postgres-stream`（3.6.1 版）**一行不改**；根 pom 仅追加一行 module。
- 包名保持 `org.vastdata.debezium.connector.postgresql.stream` 不变（spec D4）。
- 版本：debezium `1.9.7.Final`（模块内属性 `debezium19.version`）、connect-api/runtime/json `3.1.0`（模块内属性 `kafka19.version`）；pgjdbc/chronicle-queue/HdrHistogram/JUnit/logback/testcontainers 复用根属性。
- 方法名英文 camelCase；类型引用一律 import 简名（禁 FQN 内联）；日志走 slf4j 禁 System.out；每个函数（含测试辅助）有 javadoc。
- 编译目标 `release 17`（根属性继承，不覆盖）。
- Windows Git Bash 环境；工作目录 = 仓库根 `C:\Users\浦晟\Documents\workspace\vb-stream-logstash`。
- 每个任务完成即 `git commit` 并 `git push`（跨机开发规约）；commit 信息末尾带 `Co-Authored-By: Claude <noreply@anthropic.com>`。
- 测试运行统一命令 `mvn -pl vb-stream-connector-postgres-stream-debezium19 test`（IT 需本机 Docker）。
- **双线同步免责**：本计划所有复制自 3.6.1 模块的代码，若发现与 1.9.7 API 冲突，以 1.9.7 编译/测试通过为准改写，不回改 3.6.1 模块。

---

### Task 1: 模块骨架（pom + assembly + 资源）

**Files:**
- Modify: `pom.xml`（根，仅加一行 module）
- Create: `vb-stream-connector-postgres-stream-debezium19/pom.xml`
- Create: `vb-stream-connector-postgres-stream-debezium19/src/main/assembly/plugin.xml`
- Create: `vb-stream-connector-postgres-stream-debezium19/src/test/resources/logback-test.xml`
- Create: `vb-stream-connector-postgres-stream-debezium19/src/main/resources/META-INF/services/org.apache.kafka.connect.source.SourceConnector`

**Interfaces:**
- Consumes: 根 pom 的属性（`postgresql.version`/`chronicle-queue.version`/`hdrhistogram.version`/`junit-jupiter.version`/`testcontainers.version`/`logback.version`/`maven-assembly-plugin.version`）
- Produces: 可编译空模块；assembly finalName `vb-stream-connector-postgres-stream-debezium19-plugin`

- [ ] **Step 1: 根 pom 追加 module**

`pom.xml` 的 `<modules>` 末尾（`vb-stream-reader` 之后）加：

```xml
        <module>vb-stream-connector-postgres-stream-debezium19</module>
```

- [ ] **Step 2: 写模块 pom.xml**

`vb-stream-connector-postgres-stream-debezium19/pom.xml` 完整内容（与 3.6.1 模块 pom 的差异：debezium/kafka 版本、无 tests classifier 三件、无 assertj 依赖说明中的 3.x 措辞）：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.vastdata</groupId>
        <artifactId>vb-stream-logstash</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>vb-stream-connector-postgres-stream-debezium19</artifactId>

    <!-- 模块边界（spec D4/§3）：不依赖 vb-stream-engine；1.9.7 专属版本属性全部留模块内，
         根 pom 不出现双 debezium 版本属性；复用根属性的依赖与本仓其余模块同版本。 -->
    <properties>
        <debezium19.version>1.9.7.Final</debezium19.version>
        <kafka19.version>3.1.0</kafka19.version>
        <assertj.version>3.27.7</assertj.version>
        <awaitility.version>4.3.0</awaitility.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- assertj：自建 IT 基座与 Debezium 1.9.7 传递测试类的断言面（3.x 线兼容） -->
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- pgjdbc：显式 pin 根属性 42.7.13——压掉 Debezium 1.9.7 传递的 42.3.x，
             复制会话 API 与本仓其余模块同版本 -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>${postgresql.version}</version>
        </dependency>

        <!-- Chronicle Queue：CQ 主缓冲管道（MessagePipe）；照 3.6.1 模块排除 chronicle-analytics -->
        <dependency>
            <groupId>net.openhft</groupId>
            <artifactId>chronicle-queue</artifactId>
            <version>${chronicle-queue.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>net.openhft</groupId>
                    <artifactId>chronicle-analytics</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- HdrHistogram：StreamThroughputMetrics 分位数结构 -->
        <dependency>
            <groupId>org.hdrhistogram</groupId>
            <artifactId>HdrHistogram</artifactId>
            <version>${hdrhistogram.version}</version>
        </dependency>

        <!-- Debezium PG 连接器 1.9.7：Config/Schema/Emitter/offset 体系（spec D3）；
             宿主产品亦为同版本，嵌入场景类重复无害（first-win） -->
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-connector-postgres</artifactId>
            <version>${debezium19.version}</version>
        </dependency>

        <!-- Kafka Connect API 3.1.0（1.9.7 官方 Kafka 矩阵，spec §3）；
             运行期由 Connect runtime / 宿主产品提供 -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>connect-api</artifactId>
            <version>${kafka19.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- slf4j：2.x 与 Debezium 1.9.7（1.7 编译）二进制兼容，ServiceLoader 绑定恢复
             logback 1.5——同 3.6.1 模块 2026-09-01 用户裁决 -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j-api.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- 自建 IT 基座（Task 8）的 embedded 引擎与 engine API（1.9.7 同步引擎实现）；
             注意：与 3.6.1 模块不同，本模块不需要任何 tests classifier 依赖 -->
        <dependency>
            <groupId>io.debezium</groupId>
            <artifactId>debezium-embedded</artifactId>
            <version>${debezium19.version}</version>
            <scope>test</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>connect-runtime</artifactId>
            <version>${kafka19.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>connect-json</artifactId>
            <version>${kafka19.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>${awaitility.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <includes>
                        <include>**/Test*.java</include>
                        <include>**/*Test.java</include>
                        <include>**/*Tests.java</include>
                        <include>**/*TestCase.java</include>
                        <include>**/*IT.java</include>
                    </includes>
                </configuration>
            </plugin>
            <!-- maven-assembly：plugin 目录清单打包（spec §7）；R4 清单边界同 3.6.1 模块：
                 connect-api/slf4j-api(kafka-clients 及其独占子件) provided/显式排除 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>${maven-assembly-plugin.version}</version>
                <configuration>
                    <descriptors>
                        <descriptor>src/main/assembly/plugin.xml</descriptor>
                    </descriptors>
                    <appendAssemblyId>false</appendAssemblyId>
                    <finalName>vb-stream-connector-postgres-stream-debezium19-plugin</finalName>
                </configuration>
                <executions>
                    <execution>
                        <id>make-plugin</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: 写 assembly/plugin.xml**

从 `vb-stream-connector-postgres-stream/src/main/assembly/plugin.xml` 逐字节复制，唯一改动：第一个 dependencySet 的 `<include>` 换为：

```xml
                <include>org.vastdata:vb-stream-connector-postgres-stream-debezium19</include>
```

注释中 "R4 两连接器并存" 措辞保留（同样适用）。

- [ ] **Step 4: 复制资源文件**

```bash
mkdir -p vb-stream-connector-postgres-stream-debezium19/src/main/resources/META-INF/services
cp vb-stream-connector-postgres-stream/src/main/resources/META-INF/services/org.apache.kafka.connect.source.SourceConnector \
   vb-stream-connector-postgres-stream-debezium19/src/main/resources/META-INF/services/
mkdir -p vb-stream-connector-postgres-stream-debezium19/src/test/resources
cp vb-stream-connector-postgres-stream/src/test/resources/logback-test.xml \
   vb-stream-connector-postgres-stream-debezium19/src/test/resources/
```

SPI 文件内容应为单行 `org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector`（同包名，无需改动，复制后 `cat` 核对）。

- [ ] **Step 5: 空模块编译验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test`
Expected: BUILD SUCCESS（0 源文件 0 测试）

- [ ] **Step 6: Commit**

```bash
git add pom.xml vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): 模块骨架——pom/assembly/SPI/logback 资源（1.9.7+kafka3.1 版本锚）"
git push
```

---

### Task 2: 桶 A 全量复制 + 桶 A 测试闭包

**Files:**
- Create: 42 个主源码文件（见 Step 1 清单）
- Create: ~24 个测试文件（见 Step 3 清单）

**Interfaces:**
- Consumes: 3.6.1 模块源码（只读复制源）
- Produces: 桶 A 全部类型，签名与 3.6.1 模块逐一相同（零改动复制）；后续桶 B 任务在同包内直接引用

- [ ] **Step 1: 复制桶 A 主源码 42 文件**

```bash
SRC=vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream
DST=vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/stream
mkdir -p $DST/protocol
cp $SRC/protocol/*.java $DST/protocol/
# 核心层 26 文件（零 Debezium import）+ ResolvedRelation（1 import，TableId 级，1.9.7 存在）
cd $SRC && cp VersionedRelationRegistry.java TxChange.java TxBuffer.java TruncateChange.java \
  TransactionKind.java TransactionEvent.java TransactionConsumer.java StreamingTransactionListener.java \
  StreamedTransactionAssembler.java StreamThroughputMetrics.java StreamMetricsBridge.java RowChange.java \
  ReplicationSession.java RelationSnapshot.java RelationResolver.java RelationLookup.java RawPeeks.java \
  RawMessageListener.java MsgChange.java Module.java MessagePreview.java MessagePipe.java DmlKind.java \
  ColumnValueMapper.java BucketTableResolver.java BucketState.java BucketReplayer.java ResolvedRelation.java \
  $DST/ && cd ../../../..
```

- [ ] **Step 2: 桶 A 编译验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 compile`
Expected: BUILD SUCCESS。若个别文件因引用桶 B 类失败：确认被引类后，把该文件从 DST 移除并记入对应桶 B 任务的文件清单（判定规则：`grep -l "被引类名" $DST` 找出引用者）。`StreamMetricsBridge` 若引用 `StreamStreamingChangeEventSourceMetrics` 则挪到 Task 6。

- [ ] **Step 3: 复制桶 A 测试闭包**

```bash
TSRC=vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream
TDST=vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream
mkdir -p $TDST/protocol
cp $TSRC/protocol/*.java $TDST/protocol/
cd $TSRC && cp BucketReplayerTest.java DecoupledEquivalenceTest.java LoggingBindingTest.java \
  MessagePipeTest.java MessagePreviewTest.java PgWire.java PgWireTest.java PipeDirCleanup.java \
  RelationSnapshotTest.java ReplicationSessionTest.java ResolvedRelationTest.java \
  StreamThroughputMetricsTest.java StreamedTransactionAssemblerTest.java StreamingDeliveryTest.java \
  SyncDeliveryTest.java TestRelations.java Transaction.java TransactionConsumerLoopTest.java \
  TransactionEventTest.java TransactionRecorder.java TransactionRecorderTest.java \
  VersionedRelationRegistryTest.java $TDST/ && cd ../../../..
```

（`StreamThroughputMetricsWiringTest.java` **不复制**——其接线断言碰桶 B 的 Task，归 Task 7。）

- [ ] **Step 4: 桶 A 测试全绿验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test`
Expected: BUILD SUCCESS，测试数与 3.6.1 模块对应测试集合一致（protocol 7 组 + 上述 17 组）。若某测试因 import 桶 B 类编译失败：该测试移回、文件名记入对应桶 B 任务（同 Step 2 判定规则）。Windows 管道测试若因目录占用失败，参照 `PipeDirCleanup` 既有约定重跑一次确认。

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): 桶 A 复制——protocol 15 + 核心层 27 主源码与 24 个零接缝测试原样落地"
git push
```

---

### Task 3: Config 与 Connector 改写（snapshot 档 no_data → never）

**Files:**
- Create: `.../PostgresStreamConnectorConfig.java`（从 3.6.1 模块复制后改写）
- Create: `.../PostgresStreamConnector.java`（同上）
- Test: `.../PostgresStreamConnectorConfigTest.java`、`.../PostgresStreamConnectorTest.java`（复制后翻译）

**Interfaces:**
- Consumes: `io.debezium.connector.postgresql.PostgresConnectorConfig`（1.9.7：`SnapshotMode` 枚举 `ALWAYS/INITIAL/NEVER/INITIAL_ONLY/EXPORTED/CUSTOM`，无 `NO_DATA`）
- Produces: `PostgresStreamConnectorConfig`（构造 `Configuration` → config，含 `slot.*`/`pipe.*`/`values.as.string` 自建面与"仅 never"校验）；`PostgresStreamConnector`（`version()` 读 `Module`；`taskClass()` 返回 `PostgresStreamConnectorTask.class`——该类 Task 7 才落地，本任务先留 import，编译验证推迟到 Task 7 时用一个临时桩或调整任务内顺序：**先写 Config 相关，Connector 类放本任务最后一步且此任务编译验证只跑 Config 测试** `mvn test -Dtest=PostgresStreamConnectorConfigTest`）

改写规则（TDD：先翻译测试再改主码）：

- [ ] **Step 1: 复制四个文件到新模块并读 3.6.1 版源码**

```bash
cp vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/{PostgresStreamConnectorConfig,PostgresStreamConnector}.java \
   vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/stream/
cp vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/{PostgresStreamConnectorConfigTest,PostgresStreamConnectorTest}.java \
   vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/
```

然后通读四文件（3.6.1 版），标出所有 `SnapshotMode.NO_DATA` / `no_data` 字面量出现点。

- [ ] **Step 2: 翻译 ConfigTest（失败测试先行）**

对 `PostgresStreamConnectorConfigTest.java`：
- 所有 `no_data` 字面量断言改 `never`；所有"非法快照档拒绝"用例的入参集合保持（always/initial/initial_only/exported/custom 均应拒绝 + 新增 never 为唯一合法值断言）；
- 若用例引用 `AbstractAsyncEngineConnectorTest` 或 Debezium 3.x 测试类：移除该 import，该类基座改继承 `java.lang.Object`（纯 config 单测不需要基座；若确需引擎行为，把该用例移入 it 包待 Task 9）；
- 断言 API 用 assertj（已依赖）。

- [ ] **Step 3: 跑测试验证失败**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest=PostgresStreamConnectorConfigTest`
Expected: FAIL（主码仍钉 no_data）

- [ ] **Step 4: 改写 PostgresStreamConnectorConfig**

基于 3.6.1 版逐段改：
- `NO_DATA`/`no_data` 全部换 `NEVER`/`never`（1.9.7 枚举名 `SnapshotMode.NEVER`）；同名替换父 Field 的钉死逻辑保持（`snapshot.mode` 的 Field 定义覆盖默认值 `never`）；
- import 里若有 3.x 独有类（如 `SnapshotterService` 相关 javadoc `{@link}`）：删除或改 `Snapshotter`（`io.debezium.connector.postgresql.spi.Snapshotter`）；
- 自建配置面（`slot.streaming`/`slot.two.phase`/`pipe.dir`/`pipe.roll.cycle`/`slot.feedback.interval.ms`/`slot.messages`/`values.as.string`）零改动；
- javadoc 同步：MS5 一节描述从 no_data 改为 never + 差异一句话（"1.9.7 无 no_data 档；本连接器表结构自举走流内 'R' enrich，never 与 no_data 行为等价——spec §5.1"）。

- [ ] **Step 5: 跑测试验证通过**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest=PostgresStreamConnectorConfigTest`
Expected: PASS 全绿

- [ ] **Step 6: 改写 PostgresStreamConnector + ConnectorTest**

`PostgresStreamConnector.java`：3.6.1 版 `validate()` 若引用 `DebeziumHeaderProducer`/bean 体系——删除该检查段（1.9.7 无此类）；`version()` 读 `Module.getVersion()`（桶 A 的 `Module` 已在）；`taskClass()` 指向 `PostgresStreamConnectorTask`（Task 7 落地前先创建最小桩文件：仅类声明 + `taskClass` 引用可编译，Task 7 替换全文）。`PostgresStreamConnectorTest` 翻译同 Step 2 规则。

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest='PostgresStreamConnector*'`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): Config/Connector 接缝——snapshot 档 no_data→never 替换与 validate 按 1.9.7 面收敛"
git push
```

---

### Task 4: Schema 与 Relation enrich 链改写（TopicSelector 体系）

**Files:**
- Create: `.../StreamPostgresSchema.java`、`.../RelationMetadataSource.java`、`.../RelationTableFactory.java`、`.../TypeRegistryColumnValueMapper.java`、`.../StringColumnValueMapper.java`、`.../StringValueConverter.java`
- Test: 对应 5 个测试（`RelationTableFactoryTest`/`TypeRegistryColumnValueMapperTest`/`StringColumnValueMapperTest`/`StringValueConverterTest`）

**Interfaces:**
- Consumes: 1.9.7 `PostgresSchema` 5 参构造 `(connectorConfig, typeRegistry, defaultValueConverter, topicSelector, valueConverter)`；`TopicSelector<TableId>`（`PostgresTopicSelector.create(connectorConfig)`，2.0 前体系）；`PostgresConnection` 构造 `new PostgresConnection(config.getJdbcConfig(), valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL)`；`PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry)`
- Produces: `StreamPostgresSchema`（1.9.7 构造面）；enrich 链 5 类（对外方法签名与 3.6.1 版一致）

- [ ] **Step 1: 复制六主码五测试，通读标差异**

```bash
cd vb-stream-connector-postgres-stream/src && for f in StreamPostgresSchema RelationMetadataSource RelationTableFactory TypeRegistryColumnValueMapper StringColumnValueMapper StringValueConverter; do cp main/java/org/vastdata/debezium/connector/postgresql/stream/$f.java ../vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/stream/; done && for t in RelationTableFactoryTest TypeRegistryColumnValueMapperTest StringColumnValueMapperTest StringValueConverterTest; do cp test/java/org/vastdata/debezium/connector/postgresql/stream/$t.java ../vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/; done && cd ../..
```

通读各文件 3.6.1 版，列出每个 io.debezium import 的用途。

- [ ] **Step 2: 1.9.7 侧 API 核对（本任务的关键输入）**

`StreamPostgresSchema` 的 3.6.1 版若内部构造 `PostgresSchema.builder()...`（3.x builder 形态）——1.9.7 无 builder，改直接构造。目标形态（1.9.7 官方 `PostgresConnectorTask:96` 同款）：

```java
// 1.9.7 官方装配（spec §2 已核对）：
// valueConverterBuilder 与 schema 的构造面
final PostgresValueConverterBuilder valueConverterBuilder =
        (typeRegistry) -> PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);
final PostgresValueConverter valueConverter = valueConverterBuilder.build(typeRegistry);
schema = new PostgresSchema(connectorConfig, typeRegistry, defaultValueConverter, topicSelector, valueConverter);
// topicSelector 来源（2.0 前是 TopicSelector 体系，非 TopicNamingStrategy）：
final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
```

`RelationMetadataSource`/`RelationTableFactory` 里的 `PostgresConnection` 构造若用 3.x 的 `MainConnectionProvidingConnectionFactory`/builder——改上表 1.9.7 三参构造（`getJdbcConfig()` + converter builder + `CONNECTION_GENERAL`）。`databaseCharset` 经 `try (PostgresConnection c = new PostgresConnection(config.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL)) { c.getDatabaseCharset(); }` 探测（官方 Task 71-73 行同款）。

`TypeRegistryColumnValueMapper`：`PostgresType` 1.9.7 API 差异预期极小（`getName()`/`getOid()`/`isText()` 等核心面稳定），逐方法核对即可，预期零改动或一两处签名。`StringValueConverter`（12 import 全是 kafka connect data）与 `StringColumnValueMapper` 预期零改动——核对后放行。

- [ ] **Step 3: TDD 循环（逐文件：先修测试 → 跑失败 → 改主码 → 跑绿）**

对 5 个测试文件逐个：翻译（3.x 测试类 import 若有 `AbstractAsyncEngineConnectorTest` 移除改纯 JUnit；构造器调用点按 Step 2 目标形态改），`mvn test -Dtest=<类>` 验证 FAIL→改主码→PASS。`RelationTableFactoryTest` 需要 PG 的部分若走 Testcontainers——保持（postgres:18 容器，参照 3.6.1 版该测试的环境接入方式照搬）。

- [ ] **Step 4: 全模块测试回归**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test`
Expected: PASS（Task 2+3+4 累计全绿）

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): Schema 与 Relation enrich 链——TopicSelector 体系与 PostgresConnection 1.9.7 构造面"
git push
```

---

### Task 5: metrics 三件改写

**Files:**
- Create: `.../StreamStreamingChangeEventSourceMetricsMXBean.java`、`.../StreamStreamingChangeEventSourceMetrics.java`、`.../StreamChangeEventSourceMetricsFactory.java`、`.../StreamMetricsBridge.java`（若 Task 2 时被挪出）
- Test: `.../StreamStreamingChangeEventSourceMetricsTest.java`

**Interfaces:**
- Consumes: 1.9.7 `io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean`（父接口，方法集与 3.x 有差——执行时以 1.9.7 jar 为准）；`DefaultStreamingChangeEventSourceMetrics`（1.9.7 内部 `ConnectionMeter`/`StreamingMeter` 聚合结构，构造器签名不同）
- Produces: 自有八方法 `getSlotReadBytesPerSecond()/getSlotReadMessagesPerSecond()/getAssembledTxsPerSecond()/getOutputRecordsPerSecond()/getOutputBytesPerSecond()/getLagBytes()/getPendingPreparedCount()/getPipeDiskUsageBytes()` 零改动；`StreamMetricsBridge` 挂 tick 预计算 + JMX 读零锁模式不变

- [ ] **Step 1: 摸清 1.9.7 metrics 真实 API（先于复制）**

```bash
cd vb-stream-connector-postgres-stream-debezium19 && mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt && cd ..
JAR=$(tr ';' '\n' < vb-stream-connector-postgres-stream-debezium19/target/cp.txt | grep debezium-core)
unzip -p "$JAR" io/debezium/pipeline/metrics/StreamingChangeEventSourceMXBean.class > /tmp/m1.class && javap -classpath "$JAR" io.debezium.pipeline.metrics.StreamingChangeEventSourceMXBean io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory
```

记录：MXBean 接口全部方法签名、`DefaultStreamingChangeEventSourceMetrics` 构造器参数表、`DefaultChangeEventSourceMetricsFactory.getStreamingMetrics(...)` 签名。**此输出是本任务改写的唯一事实源**（1.9.7 与 3.x 在此域结构差异大，勿凭记忆写）。

- [ ] **Step 2: 复制四主码一测试**

```bash
cd vb-stream-connector-postgres-stream/src && for f in StreamStreamingChangeEventSourceMetricsMXBean StreamStreamingChangeEventSourceMetrics StreamChangeEventSourceMetricsFactory StreamMetricsBridge; do cp main/java/org/vastdata/debezium/connector/postgresql/stream/$f.java ../vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/stream/ 2>/dev/null || true; done && cp test/java/org/vastdata/debezium/connector/postgresql/stream/StreamStreamingChangeEventSourceMetricsTest.java ../vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/ && cd ../..
```

（`StreamMetricsBridge` 若 Task 2 已复制则跳过——`|| true` 容错。）

- [ ] **Step 3: 改写三件（自上而下：MXBean → 实现 → 工厂）**

- MXBean：自有八方法零改动；父接口 `StreamingChangeEventSourceMetricsMXBean` 1.9.7 存在，extends 保留；
- 实现：3.6.1 版 `extends DefaultStreamingChangeEventSourceMetrics` + 构造器 super(...) 调用按 Step 1 的 1.9.7 构造器参数表重排；被 override 的父方法（如 `onEvent`/`onNextFilteredRecord` 计数钩子）按 1.9.7 父类实际存在的钩子方法集增删——**只 override 1.9.7 父类确有的方法**（以 `javap` 输出为准），自有指标记录点（五点插桩的 `record*` 入口）不变；
- 工厂：`getStreamingMetrics`/`getSnapshotMetrics` 的覆写按 1.9.7 `DefaultChangeEventSourceMetricsFactory` 的可覆写方法面调整，返回我们的实现类；
- 测试翻译同前规（3.x 基座类 import 移除）。

- [ ] **Step 4: TDD 验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest=StreamStreamingChangeEventSourceMetricsTest`
Expected: FAIL→改→PASS；再跑全模块 `mvn test` 全绿。

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): metrics 三件——1.9.7 MXBean 方法集与 DefaultStreaming 构造面，自有八指标零改动"
git push
```

---

### Task 6: dispatcher 与回放发射改写

**Files:**
- Create: `.../DispatcherTransactionListener.java`、`.../RowChangeEmitter.java`、`.../TruncateEmitter.java`、`.../StreamEventMetadataProvider.java`
- Test: `.../DispatcherTransactionListenerTest.java`、`.../RowChangeEmitterTest.java`

**Interfaces:**
- Consumes: 1.9.7 `PostgresEventDispatcher<TableId>` 11 参构造：`(connectorConfig, topicSelector, schema, queue, connectorConfig.getTableFilters().dataCollectionFilter(), DataChangeEvent::new, PostgresChangeRecordEmitter::updateSchema, metadataProvider, heartbeatFactory, schemaNameAdjuster, jdbcConnection)`；`PgOutputReplicationMessage` 1.9.7 构造 `(Operation op, String table, Instant commitTimestamp, Long transactionId, List<Column> oldColumns, List<Column> newColumns)`
- Produces: `DispatcherTransactionListener`（`StreamingTransactionListener` → Debezium dispatcher 桥，事务边界 offset 语义不变）；`RowChangeEmitter`/`TruncateEmitter`/`StreamEventMetadataProvider`（对外方法签名与 3.6.1 版一致）

- [ ] **Step 1: 复制四主码两测试并通读**

```bash
cd vb-stream-connector-postgres-stream/src && for f in DispatcherTransactionListener RowChangeEmitter TruncateEmitter StreamEventMetadataProvider; do cp main/java/org/vastdata/debezium/connector/postgresql/stream/$f.java ../vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/stream/; done && for t in DispatcherTransactionListenerTest RowChangeEmitterTest; do cp test/java/org/vastdata/debezium/connector/postgresql/stream/$t.java ../vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/; done && cd ../..
```

- [ ] **Step 2: 改写 DispatcherTransactionListener**

3.6.1 版对 dispatcher 的调用点逐个核对 1.9.7 `PostgresEventDispatcher` 实际方法面（`javap` 同 Task 5 方式）：
- `dispatchChangeEvent`/`dispatchTransactionStartedEvent`/`dispatchTransactionCommittedEvent` 系列 1.9.7 已有（事务元数据 1.1 起），签名差异（泛型/partition 参）按 1.9.7 调整；
- 若 3.6.1 版用到 1.9.7 无有的 dispatcher 方法（如 3.x 的 schema 变更通知重载）：改走最近等价方法；
- `UnchangedToastedPlaceholder`/`UnchangedToastedReplicationMessageColumn` 1.9.7 已有（import 不动）；
- 事务 id 纯数字与元数据提供者同源的既有语义零改动。

- [ ] **Step 3: 改写 RowChangeEmitter + StreamEventMetadataProvider + TruncateEmitter**

- `RowChangeEmitter`：`PgOutputReplicationMessage` 构造按 Interfaces 给的 1.9.7 六参签名（`List<Column>` 双列）；对该消息对象的方法调用（`getNewColumns()`/`getOldColumns()`/`getTable()`/`getOperation()` 面）逐个 javap 核对；
- `StreamEventMetadataProvider`：`OffsetContext` getter 面（`getOffset()`/`getSourceInfo()` 等）1.9.7 差异核对——1.9.7 `PostgresOffsetContext` 有 `getSourceInfo()`/`getLsn()` 面，微调即可；
- `TruncateEmitter`：`Envelope.Operation.TRUNCATE` 与 dispatcher 发射面 1.9.7 均有，预期小改；`skipped.operations` 门控语义零改动。

- [ ] **Step 4: TDD 验证**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest='DispatcherTransactionListenerTest,RowChangeEmitterTest'`
Expected: FAIL→改→PASS；全模块 `mvn test` 全绿。

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): dispatcher 桥与回放发射——PostgresEventDispatcher 1.9.7 方法面，事务边界 offset 语义不变"
git push
```

---

### Task 7: streaming 源 + 工厂 + Task 装配（全模块编译里程碑）

**Files:**
- Create: `.../PostgresStreamStreamingChangeEventSource.java`、`.../StreamChangeEventSourceFactory.java`、`.../PostgresStreamConnectorTask.java`（替换 Task 3 的桩）
- Test: `.../StreamThroughputMetricsWiringTest.java`、`.../PostgresStreamConnectorTaskTest.java`、`.../PostgresStreamConnectorTest` 补全

**Interfaces:**
- Consumes: 前 6 个任务全部产物；1.9.7 接口面：`ChangeEventSourceFactory<P,O>` 的 `getStreamingChangeEventSource()` **零参**、`getSnapshotChangeEventSource(SnapshotProgressListener<P>)` 单参、`getIncrementalSnapshotChangeEventSource(offsetContext, listener, dataChangeEventListener)`；`StreamingChangeEventSource.execute(context, partition, offsetContext)` 三参；PG 专属 `PostgresChangeEventSourceCoordinator` 12 参构造
- Produces: 全模块 60 主源码 1.9.7 编译全绿；`PostgresStreamConnectorTask.start(Configuration)` 返回 coordinator

- [ ] **Step 1: 复制三主码两测试**

```bash
cd vb-stream-connector-postgres-stream/src && for f in PostgresStreamStreamingChangeEventSource StreamChangeEventSourceFactory PostgresStreamConnectorTask; do cp main/java/org/vastdata/debezium/connector/postgresql/stream/$f.java ../vb-stream-connector-postgres-stream-debezium19/src/main/java/org/vastdata/debezium/connector/postgresql/stream/; done && for t in StreamThroughputMetricsWiringTest PostgresStreamConnectorTaskTest; do cp test/java/org/vastdata/debezium/connector/postgresql/stream/$t.java ../vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/; done && cd ../..
```

- [ ] **Step 2: 改写 StreamChangeEventSourceFactory**

接口方法签名按 Interfaces 的 1.9.7 面改（3.6.1 版带 partition/offset 的两方法签名要收敛）。工厂仍返回我们自建的 `PostgresStreamStreamingChangeEventSource`（streaming）与我们自建的 snapshot 桩（若 3.6.1 版工厂有 snapshot 分支——按 3.6.1 版现状保留结构，snapshot source 类若在 3.6.1 版已存在则同规则复制改写；incremental 分支返回 `Optional.empty()`——R2 裁定 v1 不接）。

- [ ] **Step 3: 改写 PostgresStreamStreamingChangeEventSource**

监督壳行为零改动（心跳/停机 D7 shutdownFast 不排干/`PgConnectionSupplier`——1.9.7 有此 static interface，spec §2 已核实）。改动点：`execute` 与上下文接口的 1.9.7 形态、内部若引用 3.x 独有类（`SnapshottingTask` 等）删除对应分支。

- [ ] **Step 4: 改写 PostgresStreamConnectorTask（对照 1.9.7 官方装配骨架）**

3.6.1 版 Task 的 3.x 装配段（bean/notification/schema-factory/heartbeat/snapshotter-service/signal 面）替换为 1.9.7 官方形态。官方骨架（v1.9.7.Final `PostgresConnectorTask.start`，本计划已核对，关键段抄录）：

```java
// ── 1.9.7 官方装配骨架（改造时保留我们的流式源替换官方 streaming source）──
final PostgresStreamConnectorConfig connectorConfig = new PostgresStreamConnectorConfig(config);
final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
final Snapshotter snapshotter = connectorConfig.getSnapshotter();   // 我们钉 never → NeverSnapshotter
final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();

final Charset databaseCharset;
try (PostgresConnection tempConnection = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL)) {
    databaseCharset = tempConnection.getDatabaseCharset();
}
final PostgresValueConverterBuilder valueConverterBuilder =
        (typeRegistry) -> PostgresValueConverter.of(connectorConfig, databaseCharset, typeRegistry);
jdbcConnection = new PostgresConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL);
final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
final PostgresDefaultValueConverter defaultValueConverter = jdbcConnection.getDefaultValueConverter();

schema = /* 我们的 StreamPostgresSchema（Task 4 已按此构造面改写）*/;
this.taskContext = /* CdcSourceTaskContext 1.9.7 形态：官方用 PostgresTaskContext(connectorConfig, schema, topicSelector) */;
final Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets = getPreviousOffsets(
        new PostgresPartition.Provider(connectorConfig), new PostgresOffsetContext.Loader(connectorConfig));
final Clock clock = Clock.system();

queue = new ChangeEventQueue.Builder<DataChangeEvent>()
        .pollInterval(connectorConfig.getPollInterval())
        .maxBatchSize(connectorConfig.getMaxBatchSize())
        .maxQueueSize(connectorConfig.getMaxQueueSize())
        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
        .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
        .build();
ErrorHandler errorHandler = new PostgresErrorHandler(connectorConfig, queue);
final PostgresEventMetadataProvider metadataProvider = /* 我们的 StreamEventMetadataProvider（Task 6）*/;

final PostgresEventDispatcher<TableId> dispatcher = new PostgresEventDispatcher<>(
        connectorConfig, topicSelector, schema, queue,
        connectorConfig.getTableFilters().dataCollectionFilter(),
        DataChangeEvent::new,
        PostgresChangeRecordEmitter::updateSchema,
        metadataProvider,
        new HeartbeatFactory<>(connectorConfig, topicSelector, schemaNameAdjuster,
                () -> new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL),
                exception -> { /* 57P01→DebeziumException / 57P03→RetriableException，官方同款 */ }),
        schemaNameAdjuster,
        jdbcConnection);

// coordinator：PG 专属子类（非裸 ChangeEventSourceCoordinator）
ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator =
        new PostgresChangeEventSourceCoordinator(
                previousOffsets, errorHandler, PostgresStreamConnector.class, connectorConfig,
                /* 我们的 StreamChangeEventSourceFactory（Task 7 Step 2）*/,
                /* 我们的 StreamChangeEventSourceMetricsFactory（Task 5）*/,
                dispatcher, schema, snapshotter, slotInfo);
coordinator.start(taskContext, this.queue, metadataProvider);
```

3.6.1 版 Task 中**必须保留的自建装配**（这些不在官方骨架里，是本连接器的本体）：`initialContext` 读后 commit（防 CREATE SLOT 自死锁）、vanilla 命名豆注册面若有的 1.9.7 等价（无 bean 体系则删除该段，注册面本来是给 Debezium bean 容器的）、`values.as.string` 双轨切换（Task start 的 schema 转换器换 `StringValueConverter` + 监督壳值映射器 `StringColumnValueMapper`——两处同开关）。槽创建不走官方 `createReplicationSlot()`（我们的 `ReplicationSession` 自带幂等建槽带 two_phase），官方骨架中 replicationConnection/slot 创建段替换为我们的会话装配。

- [ ] **Step 5: 全模块编译 + 全部离线测试绿**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 clean test`
Expected: BUILD SUCCESS，60 主源码全编译；离线测试全绿（IT 尚未翻译，it 包暂无文件）

- [ ] **Step 6: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): streaming 源/工厂/Task 装配——1.9.7 官方骨架+自建会话替换，全模块编译绿"
git push
```

---

### Task 8: IT 基座 AbstractStreamIT + 环境/基类翻译

**Files:**
- Create: `.../it/AbstractStreamIT.java`（自建，~200 行）
- Create: `.../it/StreamPgTestEnv.java`、`.../it/StreamITBase.java`（从 3.6.1 复制后按新基座翻译）
- Test: 冒烟——临时在 StreamITBase 加一个最小端到端用例（Task 9 会替换为完整 IT 集合）

**Interfaces:**
- Consumes: 1.9.7 `DebeziumEngine.create(Connect.class)`（spec §2 事实 3）；`io.debezium.engine.ChangeConsumer`（1.9.7 有，`handleBatch(List<SourceRecord>, RecordCommitter)`）；`DebeziumEngine.CompletionCallback`
- Produces: 基座 API（后续所有 IT 依赖，**方法名与 3.6.1 基座对齐**）：`start(Class<? extends SourceConnector>, Configuration)`、`stopConnector()`、`int consumeRecords(int minRecords)`、`int consumeRecords(int minRecords, Consumer<SourceRecord>)`、`SourceRecords consumeRecordsByTopic(int numRecords)`、`CompletionCallback` 捕获通道（失败信号经 `awaitEngine`/`assertNoEngineFailure` 暴露）

- [ ] **Step 1: 复制 StreamPgTestEnv/StreamITBase 并读原基座用法**

```bash
mkdir -p vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it
cp vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/{StreamPgTestEnv,StreamITBase}.java \
   vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/
```

通读 3.6.1 版 `StreamITBase`：它 extends `AbstractAsyncEngineConnectorTest` 并封装了哪些方法（后续 IT 只碰 StreamITBase 还是直接碰基座——按实际情况定 `AbstractStreamIT` 的暴露面）。

- [ ] **Step 2: 写 AbstractStreamIT（完整实现）**

```java
package org.vastdata.debezium.connector.postgresql.stream.it;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.Connect;
import io.debezium.util.Stopwatch;

/**
 * 1.9.7 线自建 IT 基座（替代 3.6.1 的 AbstractAsyncEngineConnectorTest——1.9.7 官方基座为
 * JUnit 4 编译，JUnit 6 无法继承，spec §6.1）。
 * <p>职责：内装同步 embedded 引擎（{@code DebeziumEngine.create(Connect.class)} 的 1.9.7 工厂
 * 形态），专用线程 run（run() 阻塞至 close）；ChangeConsumer 把批记录逐条入队并 markProcessed，
 * 手动 offset 记账；对外暴露与 3.6.1 基座同名的方法面（start/stopConnector/consumeRecords/
 * consumeRecordsByTopic + CompletionCallback 捕获），使 IT 翻译保持 import 级。</p>
 * <p>线程约束：start/stop/consume 均由测试线程调用；引擎线程仅做 run 与记录入队。</p>
 */
public abstract class AbstractStreamIT {

    /** 引擎输出的记录队列（ChangeConsumer 入队，测试线程 poll）。 */
    private final LinkedBlockingQueue<SourceRecord> records = new LinkedBlockingQueue<>();

    /** 引擎线程。 */
    private Thread engineThread;

    /** 引擎实例（run 期间非 null）。 */
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;

    /** 完成闩：引擎 run() 返回（正常或异常）后 countDown。 */
    private final CountDownLatch stopped = new CountDownLatch(1);

    /** 引擎失败信号（成功/未启动为 null；经 CompletionCallback 捕获）。 */
    private volatile Throwable engineFailure;

    /**
     * 启动连接器：按配置构造 embedded 引擎并在专用线程 run。
     * <p>offset 存储、记录队列等 engine builder 属性由调用方 Configuration 提供；本方法只补
     * 引擎面（class loader / completion callback / change consumer）。</p>
     *
     * @param connectorClass 连接器实现类
     * @param config 连接器配置（含 offset.storage 等引擎属性）
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration config) {
        Properties props = config.asProperties();
        props.setProperty("name", "stream-it-engine");
        props.setProperty("connector.class", connectorClass.getName());
        props.setProperty("offset.flush.interval.ms", "0"); // 手动记账，禁自动 flush 干扰
        engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .using((success, message, error) -> {
                    if (!success) {
                        engineFailure = error;
                    }
                    stopped.countDown();
                })
                .notifying((List<RecordChangeEvent<SourceRecord>> batch,
                        DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) -> {
                    for (RecordChangeEvent<SourceRecord> event : batch) {
                        records.add(event.record());
                        committer.markProcessed(event);
                    }
                    committer.markBatchFinished();
                })
                .using(getClass().getClassLoader())
                .build();
        engineThread = new Thread(engine::run, "stream-it-engine");
        engineThread.setDaemon(false);
        engineThread.start();
    }

    /**
     * 请求停机并等待引擎线程退出（close 触发优雅停止，run 返回后 join）。
     */
    protected void stopConnector() {
        if (engine != null) {
            try {
                engine.close();
            }
            catch (Exception e) {
                throw new IllegalStateException("engine close failed", e);
            }
        }
        awaitEngine();
    }

    /**
     * 等待引擎停机；若 CompletionCallback 捕获过失败则抛出（失败信号优先于超时暴露）。
     */
    protected void awaitEngine() {
        try {
            stopped.await(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        if (engineFailure != null) {
            throw new AssertionError("engine completed with failure", engineFailure);
        }
    }

    /**
     * 断言引擎未因失败停机（长跑 IT 中途探测用）。
     */
    protected void assertNoEngineFailure() {
        if (engineFailure != null) {
            throw new AssertionError("engine failed", engineFailure);
        }
    }

    /**
     * 消费至少 {@code minRecords} 条记录（poll 队列，60s 截止；凑够即返回）。
     *
     * @return 实际取到的记录（可能多于请求量时不回收——保持基座语义简单）
     */
    protected List<SourceRecord> consumeRecords(int minRecords) throws InterruptedException {
        return consumeRecords(minRecords, r -> {
        });
    }

    /**
     * 消费至少 {@code minRecords} 条并对每条执行副作用（断言用）。
     */
    protected List<SourceRecord> consumeRecords(int minRecords, Consumer<SourceRecord> consumer) throws InterruptedException {
        List<SourceRecord> result = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (result.size() < minRecords) {
            SourceRecord r = records.poll(100, TimeUnit.MILLISECONDS);
            if (r != null) {
                result.add(r);
                consumer.accept(r);
            }
            else if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + minRecords + " records, got " + result.size());
            }
        }
        return result;
    }

    /** 按主题聚合的记录集（对齐 3.6.1 基座 SourceRecords 语义）。 */
    protected static final class SourceRecords {
        private final Map<String, List<SourceRecord>> byTopic = new LinkedHashMap<>();

        /** 记录一条到其主题桶。 */
        void add(SourceRecord record) {
            byTopic.computeIfAbsent(record.topic(), t -> new ArrayList<>()).add(record);
        }

        /** 取指定主题的全部记录；无记录返回空列表。 */
        public List<SourceRecord> recordsForTopic(String topic) {
            return byTopic.getOrDefault(topic, List.of());
        }

        /** 全部主题的全部记录展平。 */
        public List<SourceRecord> allRecords() {
            return byTopic.values().stream().flatMap(List::stream).toList();
        }
    }

    /**
     * 消费 {@code numRecords} 条并按主题聚合。
     */
    protected SourceRecords consumeRecordsByTopic(int numRecords) throws InterruptedException {
        SourceRecords grouped = new SourceRecords();
        consumeRecords(numRecords).forEach(grouped::add);
        return grouped;
    }

    /** 每用例后兜底停机，防引擎线程跨用例泄漏。 */
    @AfterEach
    void afterEachStop() {
        stopConnector();
    }
}
```

注意：1.9.7 `DebeziumEngine.Builder` 的 `.notifying(ChangeConsumer)` 重载存在与否以 javap 核对为准（`io.debezium.engine.DebeziumEngine$Builder#notifying(io.debezium.engine.ChangeConsumer)`）；若签名泛型形态略异，按编译器指引调整 lambda 类型标注。`Stopwatch` import 若未用删除。

- [ ] **Step 3: 翻译 StreamITBase 指向新基座**

`extends AbstractAsyncEngineConnectorTest` 改 `extends AbstractStreamIT`；基座方法若 3.6.1 版用了我们没有的（如 `waitForConnectorToStart`）——在 `AbstractStreamIT` 补最小实现（轮询 `records.isEmpty()` + `assertNoEngineFailure`）或从 `StreamITBase` 中删除该调用并改为等待记录到达。

- [ ] **Step 4: 冒烟用例验证基座可用**

临时用例（可并入 StreamITBase 同文件或独立 `SmokeIT.java`，Task 9 起替换）：

```java
/** 最小端到端：3 行 INSERT → 3 条 c 记录 + 事务元数据 BEGIN/END。 */
@Test
void smokeThreeInsertsReachConsumer() throws Exception {
    // StreamPgTestEnv.start()（postgres:18 单例）+ 建表 t_smoke + publication + 槽
    // start(PostgresStreamConnector.class, StreamPgTestEnv.connectorConfig(slot, publication))
    // jdbc 执行 INSERT 3 行
    // consumeRecords(5) 断言：3 条 op=c + 2 条事务元数据（BEGIN/END）
}
```

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest=SmokeIT`
Expected: PASS（需 Docker）。**这是 1.9.7 接缝层真正跑通的首个证据点**——pgjdbc 42.7.13 × 1.9.7 兼容性风险在此暴露（spec §6.3），若 `PostgresConnection` 层报 NoSuchMethod 之类：按 spec 回退行处理（pin 宿主实际 pgjdbc 版本）并回报。

- [ ] **Step 5: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "feat(debezium19): 自建 Jupiter IT 基座 AbstractStreamIT——1.9.7 同步引擎内装，冒烟端到端绿"
git push
```

---

### Task 9: 核心 IT 批一翻译（流式/回滚/DDL/重启/解耦三件）

**Files:**
- Create: `.../it/EndToEndStreamedTxIT.java`、`.../it/StreamAbortFilterIT.java`、`.../it/InTxnDdlAsOfIT.java`、`.../it/RestartSemanticsIT.java`、`.../it/ReaderUnblockedIT.java`、`.../it/FrontierCapIT.java`、`.../it/ReaderThroughputIT.java`

**Interfaces:**
- Consumes: Task 8 基座 API（同名方法面）；`StreamPgTestEnv`/`StreamITBase`
- Produces: 7 组核心语义 IT 在 1.9.7 线全绿

- [ ] **Step 1: 批量复制七组 IT**

```bash
cd vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it && \
  cp EndToEndStreamedTxIT.java StreamAbortFilterIT.java InTxnDdlAsOfIT.java RestartSemanticsIT.java \
     ReaderUnblockedIT.java FrontierCapIT.java ReaderThroughputIT.java \
     ../../../../../../../../vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/ && cd ../../../../../../../..
```

- [ ] **Step 2: 逐组翻译（机械规则 + 语义锚点）**

机械规则（每组同）：
- `import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest` 删；若类直接 extends 它改 extends `StreamITBase`（多数应已 extends StreamITBase，只改 StreamITBase 一处——Task 8 已做）；
- 基座独有方法调用（`waitForStreamingToStart` 等若出现）改基座等价（轮询记录 + assertNoEngineFailure）；
- offset 存储相关配置若 3.6.1 基座默认提供而新基座没有：在用例 Configuration 里显式补 `offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore` + 临时文件路径（3.6.1 官方基座同款默认值）。

语义锚点（每组验收口径不变，摘自模块 CLAUDE.md）：
- `EndToEndStreamedTxIT`：流式大事务进 Kafka 记录/重启续传/亚秒反馈三场景；
- `StreamAbortFilterIT`：被回滚行不进 Kafka/存活行完整/END 计数=实付；
- `InTxnDdlAsOfIT`：同事务 DDL 前后段各按变更时刻表结构渲染（列数分界正确）；
- `RestartSemanticsIT`：半事务停机 D7 重发补齐/offset 落后重复取并集/无缝续传三情况；
- `ReaderUnblockedIT`/`FrontierCapIT`/`ReaderThroughputIT`：consumer 阻塞期间 reader 持续接收放行后排干/未输出事务钉住 confirmed_flush/500 行事务 35s 内录完。

- [ ] **Step 3: 逐组跑绿**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest='EndToEndStreamedTxIT,StreamAbortFilterIT,InTxnDdlAsOfIT,RestartSemanticsIT,ReaderUnblockedIT,FrontierCapIT,ReaderThroughputIT'`
Expected: PASS 全绿（需 Docker；失败按 systematic-debugging 排障——语义口径与 3.6.1 版一致，差异只应在基座接缝）

- [ ] **Step 4: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "test(debezium19): 核心 IT 七组翻译——流式/回滚过滤/DDL asOf/重启/解耦三件全绿"
git push
```

---

### Task 10: 专项 IT 批二翻译（Truncate/全串/逻辑消息/two_phase/槽预检/缺省指标）

**Files:**
- Create: `.../it/TruncateIT.java`、`.../it/StringValuesIT.java`、`.../it/LogicalMsgIT.java`、`.../it/TwoPhaseIT.java`、`.../it/SlotTwoPhaseMismatchIT.java`、`.../it/DefaultsAndMetricsIT.java`

**Interfaces:**
- Consumes: Task 8 基座；Task 5 metrics 三件（DefaultsAndMetricsIT 的统计 tick 断言）
- Produces: 6 组专项 IT 全绿；two_phase 四场景在 1.9.7 线的完整证据

- [ ] **Step 1: 批量复制六组 IT**（同 Task 9 Step 1 命令式样，文件名换为本任务清单）

- [ ] **Step 2: 逐组翻译（机械规则同 Task 9；语义锚点）**

- `TruncateIT`：双表 TRUNCATE 逐表发射/默认门控零记录；
- `StringValuesIT`：values.as.string=true 六类型列值+schema 全 STRING 含数组原文流动/缺省时类型化基准不受扰；
- `LogicalMsgIT`：纯消息流不钉死 confirmed_flush/crash 注入护栏钉住未输出事务重启尾部不丢/中段停机整事务重发取并集；
- `TwoPhaseIT`：PREPARE 挂起零发射/ROLLBACK PREPARED 弃桶/parallel 档 StreamPrepare 大事务全量落 Kafka/prepared 挂起期停机重启续传；
- `SlotTwoPhaseMismatchIT`：存量槽 two_phase 不匹配启动期拒绝（失败信号经 CompletionCallback 捕获——**此处必须用我们的 engineFailure 通道**，异常链含 DROP SLOT 指引、槽不删）；
- `DefaultsAndMetricsIT`：缺省配置注入 never+事务元数据/snapshot.mode=initial 启动期拒绝/10s 统计 tick INFO 行可观测。

- [ ] **Step 3: 逐组跑绿**

Run: `mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest='TruncateIT,StringValuesIT,LogicalMsgIT,TwoPhaseIT,SlotTwoPhaseMismatchIT,DefaultsAndMetricsIT'`
Expected: PASS（需 Docker）

- [ ] **Step 4: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "test(debezium19): 专项 IT 六组翻译——Truncate/全串/逻辑消息/two_phase/槽预检/缺省指标全绿"
git push
```

---

### Task 11: ConnectPluginIT（真 Kafka Connect 验收，cp 7.1）

**Files:**
- Create: `.../it/ConnectPluginIT.java`

**Interfaces:**
- Consumes: assembly 产物 `target/vb-stream-connector-postgres-stream-debezium19-plugin/`；Confluent 容器 cp-kafka/cp-kafka-connect **7.1.0**（= Kafka 3.1.0，1.9.7 官方矩阵，spec §6.2）
- Produces: 真 Connect runtime 验收证据（REST 建连接器 → INSERT → topic 收数断言）

- [ ] **Step 1: 复制并翻译**

```bash
cp vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/ConnectPluginIT.java \
   vb-stream-connector-postgres-stream-debezium19/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/
```

翻译点：
- 镜像 tag：`cp-kafka-connect:8.3.0`→`cp-kafka-connect:7.1.0`、`cp-kafka:8.3.0`→`cp-kafka:7.1.0`（Confluent 镜像名以 3.6.1 版 IT 实际用名为准，只改版本段）；
- 产物路径断言：`vb-stream-connector-postgres-stream-plugin`→`vb-stream-connector-postgres-stream-debezium19-plugin`（@BeforeAll 前置 fail-fast 提示文案同步）；
- REST 流程/断言（op 恰 {c}/零 op=r + 事务元数据 BEGIN/END）零改动；
- KRaft 配置：cp 7.1 的 KRaft 支持与 8.3 参数面可能有差（7.1 时代 Kafka 3.1 的 KRaft 仍可用但 env 形态旧）——若容器起不来，改用 cp-kafka 7.1 默认 ZK 模式 + connect 容器同网络（ConfluentKafkaContainer 7.1 默认形态即 ZK，参数减到最少）。

- [ ] **Step 2: 打包 + 跑绿**

```bash
mvn -pl vb-stream-connector-postgres-stream-debezium19 package -DskipTests
mvn -pl vb-stream-connector-postgres-stream-debezium19 test -Dtest=ConnectPluginIT
```
Expected: PASS（需 Docker；下载 cp 7.1 镜像首次较慢）

- [ ] **Step 3: Commit**

```bash
git add vb-stream-connector-postgres-stream-debezium19/
git commit -m "test(debezium19): ConnectPluginIT 真 Connect 验收——cp 7.1(Kafka 3.1) 容器组全绿"
git push
```

---

### Task 12: 打包产物验证 + 文档收尾

**Files:**
- Verify: `vb-stream-connector-postgres-stream-debezium19/target/vb-stream-connector-postgres-stream-debezium19-plugin/`
- Create: `vb-stream-connector-postgres-stream-debezium19/README.md`、`vb-stream-connector-postgres-stream-debezium19/CLAUDE.md`
- Modify: 根 `CLAUDE.md`（模块索引行 + 源码结构两处）

**Interfaces:**
- Consumes: 全部前序产物
- Produces: 交付态模块（产物 + 文档 + 全仓 `mvn test` 双绿）

- [ ] **Step 1: 产物结构断言**

```bash
ls vb-stream-connector-postgres-stream-debezium19/target/vb-stream-connector-postgres-stream-debezium19-plugin/
# 预期：插件根有连接器自身 jar；lib/ 含 debezium-connector-postgres-1.9.7.Final.jar、
# postgresql-42.7.13.jar、chronicle-*、HdrHistogram-*；
# 必须不含：connect-api、kafka-clients、slf4j-api、zstd/lz4/snappy、jakarta.ws.rs-api（R4 边界）
```

- [ ] **Step 2: 写模块 README.md**

五节对齐 3.6.1 模块 README（定位/配置面/打包安装/at-least-once 语义/已知限制），差异处替换：
- 定位段：宿主 = 内置 Debezium 1.9.7.Final 的产品；Kafka 3.1 线；
- 配置面：`snapshot.mode` 仅 `never`（差异说明一句 + spec §5.1 引用）；
- 已知限制：无 notification 通道（1.9.7 无此体系）；
- 新增"与 3.6.1 模块的差异与同步约定"节：差异表（no_data→never/notification 裁剪/IT 基座自建 AbstractStreamIT/Kafka 3.1/容器 cp 7.1）+ 桶 A 内核双线同步约定（spec §7）。

- [ ] **Step 3: 写模块 CLAUDE.md**

结构对齐 3.6.1 模块 CLAUDE.md，内容按本计划事实重写：三桶结构、1.9.7 API 差异清单（抄 spec §2）、自建基座说明、测试矩阵与容器版本。

- [ ] **Step 4: 根 CLAUDE.md 补索引**

项目概述的模块列表加：`vb-stream-connector-postgres-stream-debezium19`（1.9.7 宿主嵌入版，与 3.6.1 模块同包名复制+接缝改写，差异与双线同步约定见其模块 CLAUDE.md）；"源码结构"节补对应两行（main/test）。

- [ ] **Step 5: 全仓双模块回归**

Run: `mvn clean test`
Expected: 全部六+1 模块 BUILD SUCCESS（1.9.7 模块测试数对齐 3.6.1 模块的离线+IT 规模，27 IT 全绿）

- [ ] **Step 6: Commit**

```bash
git add CLAUDE.md vb-stream-connector-postgres-stream-debezium19/
git commit -m "docs(debezium19): 模块 README/CLAUDE.md 与根索引——差异表+双线同步约定，全仓回归绿"
git push
```

---

## 自审记录（写完计划后过一遍）

1. **Spec 覆盖**：spec §3 依赖边界→Task 1；§4 桶 A→Task 2、桶 B 18 文件→Task 3-7（Config/Connector 2 + Schema/enrich 6 + metrics 4 + dispatcher/回放 4 + 源/工厂/Task 3，含 StringValueConverter 等"核对后放行"项）；§5.1 never→Task 3；§5.2 two_phase→Task 10；§5.3 notification 裁剪→Task 7（装配段不存在即裁）；§5.4 metrics→Task 5；§5.5 其余功能面→Task 4/6/10；§6.1 基座→Task 8；§6.2 矩阵→Task 9/10/11；§6.3 风险回退→Task 8 Step 4（pgjdbc 风险暴露点）；§7 打包文档→Task 1/11/12。无缺口。
2. **占位符扫描**：Task 9 Step 1 的 cp 命令含长相对路径（`../../../../../..`）——执行时以仓库根为 cwd 用绝对相对路径重写；无 TBD/TODO 型步骤。
3. **类型一致性**：`AbstractStreamIT` 方法签名在 Task 8 Interfaces 块与代码块一致（start/stopConnector/consumeRecords×2/consumeRecordsByTopic/awaitEngine/assertNoEngineFailure）；`StreamStreamingChangeEventSourceMetricsMXBean` 八方法名在 Task 5 与 spec §5.4 一致。
