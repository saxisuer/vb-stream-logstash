# MS6 实现计划：插件打包（R4）+ Connect 容器验收 IT + 模块 README + R2 延期记档

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落 MS6 收官四件——maven-assembly 打 plugin 目录清单（R4 两连接器并存）、真 Kafka Connect 容器验收 IT、模块 README（配置/安装/语义/限制一档全）、R2 增量快照延期记档 + CLAUDE.md 收官记档。

**Architecture:** assembly descriptor 取 **runtime scope 依赖集**（`connect-api`/`slf4j-api` 已 provided、测试依赖已 test——scope 卫生天然画出清单边界，无需手工排除表）；连接器 jar 落插件根、依赖 jar 落 `lib/`（Debezium 官方插件包同构布局）。验收 IT 独立于 StreamITBase：cp-kafka-connect 容器 + plugin.path 装载 + REST 建连接器 + Kafka topic 收数断言。文档落模块 README。

**Tech Stack:** Java 17 + Maven（maven-assembly-plugin）、Testcontainers（confluentinc cp-kafka-connect/cp-kafka 镜像）、复用 `StreamPgTestEnv.PG` 单例。

**Spec:** `docs/superpowers/specs/2026-09-05-ms6-packaging-docs-design.md`

## Global Constraints

- 日志一律 slf4j、禁止 System.out/System.err；消息用 `{}` 占位符
- **每个函数（含私有方法与测试辅助方法）必须有 javadoc 逻辑描述**（职责/关键步骤/边界/线程约束）
- 模块边界 D2：连接器主代码零 `org.vastdata.vbstream` import
- 每任务完成即 `git commit` 并 `git push origin worktree-ms6-packaging-docs`；commit message 尾加 `Co-Authored-By: Claude <noreply@anthropic.com>`
- 单测命令：`mvn test -pl vb-stream-connector-postgres-stream -Dtest=类名`；IT 需本机 Docker
- 工作目录：当前 worktree 根（`/Users/saxisuer/Documents/workspace-intellij/vb-stream-logstash/.claude/worktrees/ms6-packaging-docs`，路径相对它）
- 既有 `-Pjmh` 档与默认 `mvn test` 行为不得受打包改动影响

---

### Task 1: maven-assembly plugin 目录清单打包

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/main/assembly/plugin.xml`
- Modify: `vb-stream-connector-postgres-stream/pom.xml`（build/plugins 加 maven-assembly-plugin）

**Interfaces:**
- Consumes: 模块既有 scope 卫生（`connect-api`/`slf4j-api` provided、测试依赖 test）
- Produces: `mvn -pl vb-stream-connector-postgres-stream package` 产出 `target/vb-stream-connector-postgres-stream-plugin/`（根=连接器 jar，`lib/`=runtime 依赖 jar）+ 同名 zip——Task 2 IT 的输入物；产物目录名以 `<finalName>`+`<appendAssemblyId>false` 钉死为该路径

- [ ] **Step 1: 写 assembly descriptor**

```xml
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.2.0 http://maven.apache.org/xsd/assembly-2.2.0.xsd">
    <id>plugin</id>
    <formats>
        <format>dir</format>
        <format>zip</format>
    </formats>
    <includeBaseDirectory>false</includeBaseDirectory>

    <!-- 责任:依赖集只取 runtime scope——connect-api/slf4j-api(provided,Connect runtime 已提供)
         与测试依赖(test)被 scope 过滤天然排除,R4 两连接器并存的清单边界由此钉死;
         全部落 lib/(连接器自身 jar 由 useProjectArtifact 输出到插件根,官方插件包同构布局)。 -->
    <dependencySets>
        <dependencySet>
            <useProjectArtifact>true</useProjectArtifact>
            <outputDirectory>/</outputDirectory>
            <includes>
                <include>org.vastdata:vb-stream-connector-postgres-stream</include>
            </includes>
        </dependencySet>
        <dependencySet>
            <useProjectArtifact>false</useProjectArtifact>
            <outputDirectory>/lib</outputDirectory>
            <scope>runtime</scope>
        </dependencySet>
    </dependencySets>
</assembly>
```

（XML 注释按上式落为标准 `<!-- -->`；两个 dependencySet 的语义注释保留。）

- [ ] **Step 2: pom 接线**

`vb-stream-connector-postgres-stream/pom.xml` build/plugins 追加（版本属性进根 pom `<properties>`，与既有 surefire 版本管理同款形态；assembly 插件 3.x 系最新稳定版）：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-assembly-plugin</artifactId>
    <version>${maven-assembly-plugin.version}</version>
    <configuration>
        <descriptors>
            <descriptor>src/main/assembly/plugin.xml</descriptor>
        </descriptors>
        <appendAssemblyId>false</appendAssemblyId>
        <!-- finalName 形态：vb-stream-connector-postgres-stream-plugin——dir/zip 产物路径的确定性锚点 -->
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
```

`<finalName>` 配置为 `vb-stream-connector-postgres-stream-plugin`（与模块 artifactId 拼接语义一致即可，产物目录名 `target/vb-stream-connector-postgres-stream-plugin/` 钉死）。

- [ ] **Step 3: 构建并核对产物结构**

Run: `mvn -pl vb-stream-connector-postgres-stream clean package -DskipTests`
Expected: BUILD SUCCESS，`target/vb-stream-connector-postgres-stream-plugin/` 存在，核对手工清单（记入报告）：
- 根：`vb-stream-connector-postgres-stream-1.0-SNAPSHOT.jar` 恰一个
- `lib/`：`postgresql`、`chronicle-queue`（含 chronicle-core/bytes/wire/threads 传递）、`HdrHistogram`、`debezium-connector-postgres`、`debezium-connector-common`、`debezium-api`/`debezium-config`/`debezium-util` 等 runtime 传递依赖齐全
- **不在清单**：`connect-api`、`kafka-clients`、`slf4j-api`、任何 test 依赖（JUnit/Testcontainers/logback-classic/debezium-embedded）——逐一 grep `lib/` 确认零命中并记入报告
- zip 分发物 `target/vb-stream-connector-postgres-stream-plugin.zip` 存在

- [ ] **Step 4: 确认默认构建不破**

Run: `mvn clean test -pl vb-stream-engine,vb-stream-connector-postgres-stream`
Expected: 全绿（打包改动零影响测试路径）

- [ ] **Step 5: Commit & push**——`build(ms6): maven-assembly plugin 目录清单——runtime scope 集画边界,连接器 jar 根+lib/ 依赖(R4 两连接器并存)`

---

### Task 2: ConnectPluginIT——真 Kafka Connect 容器验收

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/ConnectPluginIT.java`
- Modify（如需）: `vb-stream-connector-postgres-stream/pom.xml`（testcontainers-kafka 坐标——`org.testcontainers:kafka` 与既有 postgresql 同 BOM 版本管理）

**Interfaces:**
- Consumes: Task 1 产物 `target/vb-stream-connector-postgres-stream-plugin/`（**IT 前置**：@BeforeAll 断言产物存在且结构合规——根恰一连接器 jar、`lib/` 非空、connect-api/kafka-clients/slf4j-api 零命中；产物缺失即 fail-fast 提示先跑 `mvn -pl ... package -DskipTests`）；`StreamPgTestEnv.PG` 单例容器与 `execSql/newSqlConnection`；surefire 已含 `**/*IT.java`
- Produces: 无（验收终点类）

- [ ] **Step 1: 写 ConnectPluginIT**（容器编排是判断密集面，按此骨架落地）：

```java
/**
 * MS6/R4 真连接器验收:插件装进真 Kafka Connect 运行(非 embedded engine)——类加载面
 * (plugin.path 隔离类加载器加载本插件与 lib/ 依赖)、REST 配置暴露面(configDef/validate
 * 三层防线)、序列化面、连接器生命周期全走真路径。
 * 关键步骤:@BeforeAll 核对 assembly 产物结构(根恰一连接器 jar/lib 依赖齐/excluded 零命中,
 * 缺产物即 fail-fast 提示先 package)→ 起容器组(cp-kafka KRaft + cp-kafka-connect,
 * plugin.path 挂载插件目录副本)→ await Connect REST 就绪 → PUT /connectors 建连接器
 * (database.* 指向 StreamPgTestEnv.PG,topic.prefix ms6connect,快照 no_data 走默认注入)
→ 夹具表 publication 预建 → INSERT → 轮询 Kafka consumer(topic ms6connect.public.t_plug)
→ 断言记录 op=c/值等 → @AfterEach 停删连接器与容器。
 * 边界:cp 镜像大、首次拉取慢——独立类可 -Dtest 单跑;断言超时口径 60s 级 await。
 */
```

实现要点（先调研再落码，调研结论记入报告）：
- 镜像与编排：`confluentinc/cp-kafka-connect:<与 cp-kafka 同 7.x 稳定版>`（Kafka Connect）+ Kafka 单容器（cp-kafka KRaft 或同系镜像）——Testcontainers `KafkaContainer`（org.testcontainers:kafka 的 `ConfluentKafkaContainer`）+  `GenericContainer` 起 connect，env 面：`CONNECT_BOOTSTRAP_SERVERS`/`CONNECT_REST_PORT`/`CONNECT_GROUP_ID`/`CONNECT_CONFIG_STORAGE_TOPIC` 等最小集 + `CONNECT_PLUGIN_PATH=/plugins`；`withCopyFileToContainer`/挂载把插件目录复制进去（`Archive.tar` 或逐 jar copy——择简）
- REST 客户端：JDK `HttpClient` 裸调（不引新依赖）——PUT connector config（JSON 手拼或 `Configuration` 序列化）、GET status 轮询 RUNNING
- Kafka 消费：用 `org.apache.kafka:kafka-clients`（test scope 已有 connect-runtime→kafka-clients 传递；如 classpath 不可达则加 test 依赖 `kafka-clients`）——`KafkaConsumer` assign+seekToBeginning 轮询收数
- 连接器配置 JSON：database 四件套（PG 容器 mapped host/port）+ `slot.name=ms6_plug` + `publication.name=pub_ms6_plug` + `topic.prefix=ms6connect`——**不设 snapshot.mode/provide.transaction.metadata**（顺带验收默认注入在真 Connect 生效）
- 夹具：表 `t_plug(id int PK, v text)` + publication 预建、槽 @AfterEach 删（PG 单例跨 IT 类共享,清删幂等）

- [ ] **Step 2: 先跑产物前置断言（红）**——`mvn -pl vb-stream-connector-postgres-stream package -DskipTests` 后 `mvn test -pl ... -Dtest=ConnectPluginIT`：容器编排失败/镜像拉取失败属 BLOCKED 上报；断言失败可能是真实缺陷不得削弱
- [ ] **Step 3: 全绿 + 独立可重跑验证**（重复跑一次确认夹具自愈）
- [ ] **Step 4: Commit & push**——`test(ms6): ConnectPluginIT——插件装进真 Kafka Connect 运行,REST 建连接器→PG 写入→topic 收数(R4 类加载面验收)`

---

### Task 3: 模块 README + R2 延期记档 + CLAUDE.md 收官

**Files:**
- Create: `vb-stream-connector-postgres-stream/README.md`
- Modify: `docs/superpowers/specs/2026-09-05-ms5-r2-incremental-snapshot-audit.md`（结论节补 MS6 裁定一行）
- Modify: `CLAUDE.md`（根：MS6 句 + 用例计数）；`vb-stream-connector-postgres-stream/src/main/java/.../stream/CLAUDE.md`（模块记档）；根 `README.md`（补连接器入口指针一行，若已有则跳过）

**Interfaces:**
- Consumes: Task 1 产物路径与命令、Task 2 IT 名、既有配置面真源（`PostgresStreamConnectorConfig` javadoc——README 配置表与其逐一对照，防两处漂移）

- [ ] **Step 1: 写模块 README**——五节骨架（spec §4）：定位与架构（一段话）/配置面全档（六自定项+两默认覆盖,逐项语义/默认值/约束,表格形态）/打包与安装（mvn package → 产物结构 → plugin.path → REST 建连接器示例 JSON——可直接引 ConnectPluginIT 的真实配置）/at-least-once 与停机语义（End 锚定前沿、确认封顶、crash 整事务重发不去重、D7 不排干）/Known limitations（数组列 fail-fast、未知类型静默 null、LogicalMsg 不发射、snapshot.mode 仅 no_data、增量快照不接+R2 审计路线图指针）。中文,行文密度与项目文档一致
- [ ] **Step 2: R2 审计补裁定行**——结论节末尾追加：`> MS6 裁定（2026-09-05）：v1 不接 signal-based 增量快照——需要时按本档前置条件五项路线图实施。`
- [ ] **Step 3: CLAUDE.md 两处 + 根 README 记档**——根 CLAUDE.md connector 段追加 MS6 句（形态参照 MS5 句：打包形态/ConnectPluginIT/README 指针）+ 用例计数按 Task 2 后全量实测；模块 stream/CLAUDE.md 同步打包段；根 README 检查补连接器指针
- [ ] **Step 4: 全量回归**——`mvn clean test` 全绿（全仓双模块）+ `mvn -pl vb-stream-connector-postgres-stream package -DskipTests` 产物复核，计数记入报告
- [ ] **Step 5: Commit & push**——`docs(ms6): 模块 README(配置/安装/语义/限制) + R2 延期裁定记档 + MS6 收官记档`

---

## 自审记录

- **Spec 覆盖**：§2 → Task 1；§3 → Task 2（产物结构断言前置并入其 @BeforeAll）；§4 → Task 3 Step 1；§5 → Task 3 Step 2/3；§6 → Task 1 Step 3/4 + Task 2/3
- **占位符**：Task 2 的镜像版本/容器 env 细节/Kafka 消费挂载方式标注"先调研再落码"——约束已钉（REST 建连接器/默认注入验收/topic 收数断言/60s 超时口径），属实现期择形非 TBD；assembly 版本"3.x 最新稳定版"由实现者查 Central 定版记入报告
- **类型一致性**：产物路径 `target/vb-stream-connector-postgres-stream-plugin/` 在 Task 1 钉死、Task 2 消费一致
