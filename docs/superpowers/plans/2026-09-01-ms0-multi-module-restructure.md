# MS0 仓库多模块化改造 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** vb-stream-logstash 单模块仓库改造为 parent + `vb-stream-engine`(现有代码)+ `vb-stream-connector-postgres-stream`(空骨架)两模块结构,现有 177 测试保持全绿、Main 冒烟可跑、`-Pjmh` 档不破。

**Architecture:** 根 pom 转 `packaging=pom` 聚合(版本属性 + surefire argLine 上移继承),现有源码树 `git mv` 入 `vb-stream-engine/`(保历史),jmh profile 随引擎走(相对路径 `src/jmh/java` 按模块解析);connector 模块只建 pom + package-info,不引任何 Debezium 依赖(MS1 再引)。

**Tech Stack:** Maven 多模块、Java 17、JUnit 6(Surefire 3.5.6)、git mv 保历史。

**Spec:** `docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md` §3(模块结构)、§10(MS0 验收)。

## Global Constraints

- Java 17(`maven.compiler.release=17`),UTF-8
- 现有 177 个测试必须保持全绿(验收口径 `mvn clean test`,含 Testcontainers 集成测试——**执行前 Docker Desktop 必须在运行**)
- 验证编译一律用 `clean` 目标(增量编译可能假绿)
- surefire argLine 的 `--add-opens` 清单逐 token 不变:`--add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED --add-opens java.base/sun.nio.fs=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED`(Chronicle Queue mmap 反射必需)
- 源码移动只用 `git mv`(保历史);`src/docker/`、`docs/`、`CLAUDE.md` 留仓库根
- connector 模块本里程碑**不 import 引擎任何类、不引 Debezium 依赖**(spec D2/D6:依赖 MS1 定版本后引入)
- 每个任务结束 commit + push(跨多台电脑开发约定);commit message 末尾加 `Co-Authored-By: Claude <noreply@anthropic.com>`
- pom 中的中文注释(依赖用途、jmh 档说明等)随依赖/配置原样迁移,不删不改写

---

### Task 1: 引擎模块抽出与根 pom 转 parent

**Files:**
- Modify: `pom.xml`(根:packaging=pom、加 `<modules>`、删 dependencies 与 jmh profile)
- Create: `vb-stream-engine/pom.xml`
- Move: `src/main` → `vb-stream-engine/src/main`;`src/test` → `vb-stream-engine/src/test`;`src/jmh` → `vb-stream-engine/src/jmh`(git mv;`src/docker` 留根)

**Interfaces:**
- Consumes: 无(首个任务)
- Produces: 聚合根 pom(coordinates `org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`,packaging=pom,module `vb-stream-engine`);引擎模块 `org.vastdata:vb-stream-engine:1.0-SNAPSHOT`(全部现有依赖与 jmh profile)。Task 2 依赖根 pom 的 `<modules>` 存在;两个模块 pom 都以根为 parent。

- [ ] **Step 1: 创建引擎 pom(依赖与 jmh profile 从根 pom 原样搬入)**

创建 `vb-stream-engine/pom.xml`,内容:

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

    <artifactId>vb-stream-engine</artifactId>

    <dependencies>
        <!-- PostgreSQL JDBC 驱动：含逻辑复制 API（ReplicationConnection / PGReplicationStream） -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>${postgresql.version}</version>
        </dependency>

        <!-- Chronicle Queue：持久化低延迟消息队列，用于缓存/回放 CDC 事件。
             排除 chronicle-analytics（chronicle-core 传递引入的用量遥测，HTTP 打点上报）：
             core 的 AnalyticsFacade 对其反射加载、缺类即回落 MuteAnalytics 空实现，排除是官方
             DISCLAIMER 认可的关闭方式；对 classpath 全局生效（Main/测试/JMH），启动横幅
             "Chronicle Queue reports usage statistics" 随之消失 -->
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

        <!-- HdrHistogram：高动态范围直方图（零传递依赖）——ThroughputMetrics 的分位数数据结构
             （P90/P95/max，SingleWriterRecorder 区间窗口），设计见
             docs/superpowers/specs/2026-08-31-throughput-metrics-design.md -->
        <dependency>
            <groupId>org.hdrhistogram</groupId>
            <artifactId>HdrHistogram</artifactId>
            <version>${hdrhistogram.version}</version>
        </dependency>

        <!-- logback：slf4j 绑定（chronicle 传递引入 slf4j-api，缺实现时 NOP 静默） -->
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>${logback.version}</version>
        </dependency>

        <!-- JUnit 6（要求 Java 17+） -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- 集成测试：PG 容器（2.x 起坐标改名 testcontainers-postgresql） -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-postgresql</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <profiles>
        <!-- JMH 基准档（Task 13）：-Pjmh 激活。基准源码放独立根 src/jmh/java（经 build-helper 挂为
             test 源码目录），默认构建完全不引入 JMH 依赖与基准编译——`mvn clean test` 不受影响；
             `-Pjmh test-compile` 才把四类 *Benchmark 与 JMH 生成的基准桩编进 target/test-classes。
             annprocess 以 compiler 的 annotationProcessorPaths 显式接入（不依赖 classpath 隐式发现，
             新版 JDK 已默认禁用隐式注解处理）；桩类由 Surefire 默认命名规则排除（*Benchmark 非 *Test），
             `mvn -Pjmh test` 也不会误跑基准。运行方法见 docs/benchmarks-baseline.md。 -->
        <profile>
            <id>jmh</id>
            <dependencies>
                <!-- JMH 核心：基准注解 + 运行器（org.openjdk.jmh.Main 入口在测试 classpath 上） -->
                <dependency>
                    <groupId>org.openjdk.jmh</groupId>
                    <artifactId>jmh-core</artifactId>
                    <version>${jmh.version}</version>
                    <scope>test</scope>
                </dependency>
                <!-- JMH 注解处理器：把 @Benchmark 方法生成为可被 Main 发现的基准桩类 -->
                <dependency>
                    <groupId>org.openjdk.jmh</groupId>
                    <artifactId>jmh-generator-annprocess</artifactId>
                    <version>${jmh.version}</version>
                    <scope>test</scope>
                </dependency>
            </dependencies>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.codehaus.mojo</groupId>
                        <artifactId>build-helper-maven-plugin</artifactId>
                        <version>${maven-build-helper-plugin.version}</version>
                        <executions>
                            <execution>
                                <id>add-jmh-test-source</id>
                                <phase>generate-test-sources</phase>
                                <goals>
                                    <goal>add-test-source</goal>
                                </goals>
                                <configuration>
                                    <sources>
                                        <source>src/jmh/java</source>
                                    </sources>
                                </configuration>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-compiler-plugin</artifactId>
                        <configuration>
                            <annotationProcessorPaths>
                                <path>
                                    <groupId>org.openjdk.jmh</groupId>
                                    <artifactId>jmh-generator-annprocess</artifactId>
                                    <version>${jmh.version}</version>
                                </path>
                            </annotationProcessorPaths>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>

</project>
```

- [ ] **Step 2: 改根 pom——packaging=pom、加 modules、删 dependencies 与 jmh profile**

对根 `pom.xml` 做三处修改(其余 properties / surefire 插件配置原样保留):

(a) `<version>1.0-SNAPSHOT</version>` 之后新增一行:

```xml
    <packaging>pom</packaging>
```

(b) `<packaging>pom</packaging>` 之后、`<properties>` 之前插入:

```xml
    <modules>
        <module>vb-stream-engine</module>
    </modules>
```

(c) 整段删除根 pom 的 `<dependencies>...</dependencies>`(六项依赖已搬引擎 pom)与 `<profiles>...</profiles>`(jmh profile 已搬引擎 pom)。保留 `<build>` 里 surefire 插件(版本 + `--add-opens` argLine 由两模块继承;packaging=pom 的 parent 自身不执行测试)。

- [ ] **Step 3: git mv 源码树入引擎模块(保历史,src/docker 留根)**

```bash
mkdir -p vb-stream-engine/src
git mv src/main vb-stream-engine/src/main
git mv src/test vb-stream-engine/src/test
git mv src/jmh vb-stream-engine/src/jmh
```

执行后 `ls src/` 应只剩 `docker`。

- [ ] **Step 4: 全量测试验证(Docker 必须在运行)**

Run: `mvn clean test`
Expected: reactor 依次构建 `vb-stream-logstash (parent)` 与 `vb-stream-engine`;引擎 **177 个测试全绿**;无编译错误。若出现 `org/vastdata/vbstream` 包找不到,说明 Step 3 移动不完整。

- [ ] **Step 5: jmh 档验证**

Run: `mvn -Pjmh clean test-compile`
Expected: BUILD SUCCESS,`vb-stream-engine/target/test-classes` 下生成 `*Benchmark` 桩类(可 `ls vb-stream-engine/target/test-classes/org/vastdata/vbstream/bench/ | head` 确认)。

- [ ] **Step 6: Commit + push**

```bash
git add pom.xml vb-stream-engine/pom.xml
git commit -m "build: 仓库多模块化——根 pom 转 parent 聚合,现有代码 git mv 入 vb-stream-engine(177 测试保持全绿,-Pjmh 档不破)

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

(git mv 已把源码移动连同删除暂存;`git add` 只补两个 pom。提交前 `git status` 确认无未跟踪残留。)

---

### Task 2: connector 空模块骨架

**Files:**
- Modify: `pom.xml`(根:`<modules>` 追加 connector)
- Create: `vb-stream-connector-postgres-stream/pom.xml`
- Create: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/package-info.java`
- Modify: `.gitignore`(追加 `pg-stream-pipe-queue/`)

**Interfaces:**
- Consumes: Task 1 的根 parent pom(版本属性 `${junit-jupiter.version}` 继承)
- Produces: 模块 `org.vastdata:vb-stream-connector-postgres-stream:1.0-SNAPSHOT`,包根 `org.vastdata.debezium.connector.postgresql.stream`——MS1 起的全部 connector 代码落这个包;默认 CQ 目录 `pg-stream-pipe-queue/`(spec §5.1)

- [ ] **Step 1: 创建 connector pom(不引任何 Debezium 依赖——版本 MS1 定,spec §11)**

创建 `vb-stream-connector-postgres-stream/pom.xml`:

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

    <artifactId>vb-stream-connector-postgres-stream</artifactId>

    <!-- MS0 骨架：不引任何 Debezium / 引擎依赖（spec D2/D6/§11——Debezium 稳定版版本号 MS1 定，
         引擎类不 import）。此处仅挂 JUnit 以保证模块测试骨架就绪。 -->
    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

</project>
```

- [ ] **Step 2: 创建包根 package-info**

创建 `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/package-info.java`:

```java
/**
 * PG 逻辑解码 stream 模式的 Debezium 连接器（debezium-connector-postgres-stream）。
 *
 * <p>双自建线程（vb-pgoutput-reader / vb-transaction-consumer）+ Chronicle Queue 管道 +
 * End 锚定 LSN 的流式 CDC 管道：reader 从复制槽 drain 轮询 raw 字节落管道、桶记账，
 * consumer 回放桶逐条经 Debezium dispatcher 发射 Kafka Connect 记录；快照/offset/
 * 事务元数据体系复用 io.debezium 的 debezium-connector-postgres。
 *
 * <p>包名刻意与 {@code io.debezium.*} 隔离：两连接器插件在同一 Kafka Connect 集群
 * 并存时零类冲突。设计见
 * {@code docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md}。
 */
package org.vastdata.debezium.connector.postgresql.stream;
```

- [ ] **Step 3: 根 pom modules 追加 connector**

根 `pom.xml` 的 `<modules>` 改为:

```xml
    <modules>
        <module>vb-stream-engine</module>
        <module>vb-stream-connector-postgres-stream</module>
    </modules>
```

- [ ] **Step 4: .gitignore 追加 connector 默认管道目录**

在 `### 管道队列目录（MessagePipe wipe-on-open 瞬态工作区，默认 vb.pipe.dir）###` 段的 `pipe-queue/` 行后追加:

```
pg-stream-pipe-queue/
```

- [ ] **Step 5: 双模块构建验证**

Run: `mvn clean test`
Expected: reactor 依次构建 parent、`vb-stream-engine`(177 全绿)、`vb-stream-connector-postgres-stream`(无测试,Surefire 跳过,BUILD SUCCESS)。

- [ ] **Step 6: Commit + push**

```bash
git add pom.xml .gitignore vb-stream-connector-postgres-stream
git commit -m "build: connector 空模块骨架——vb-stream-connector-postgres-stream(pom + 包根 package-info,Debezium 依赖 MS1 引入)+ 默认管道目录 gitignore

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

---

### Task 3: 文档更新与 Main 冒烟验收

**Files:**
- Modify: `CLAUDE.md`(坐标/常用命令/运行 Main/源码结构/JMH 五处路径)
- Modify: `docs/benchmarks-baseline.md`(运行路径改模块相对)

**Interfaces:**
- Consumes: Task 1/2 的最终目录结构
- Produces: 与新结构一致的仓库文档(MS1 起执行者的入口说明)

- [ ] **Step 1: 更新 CLAUDE.md 五处**

(a) 坐标行——原文:

```
- 坐标：`org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（Vastbase 生态；artifactId 暗示最终会以某种形式与 Logstash 集成，集成方式尚未确定）
```

改为:

```
- 坐标：聚合 parent `org.vastdata:vb-stream-logstash:1.0-SNAPSHOT`（packaging=pom）+ 两模块：`vb-stream-engine`（现有引擎：protocol / replication / Main / ConsoleRenderer）与 `vb-stream-connector-postgres-stream`（Debezium 流式连接器，设计见 docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md）
```

(b) 常用命令代码块——把两条 `-Dtest` 示例行改为带 `-pl vb-stream-engine` 前缀(多模块后不带 `-pl` 会在无匹配测试的模块报 "No tests matching pattern"),原:

```
mvn test -Dtest=ClassName            # 运行单个测试类
mvn test -Dtest=ClassName#method     # 运行单个测试方法
```

改为:

```
mvn test -pl vb-stream-engine -Dtest=ClassName            # 运行引擎单个测试类(多模块后 -Dtest 须带 -pl)
mvn test -pl vb-stream-engine -Dtest=ClassName#method     # 运行引擎单个测试方法
```

(其余 `mvn clean package` / `mvn compile` / `mvn test` / `mvn dependency:tree` 四行不动,仓库根直接跑即全 reactor。)

(c) 运行 Main 的两条命令——原:

```
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
```

改:

```
mvn -q -pl vb-stream-engine compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
```

原 `-cp "target/classes:$(cat target/cp.txt)"` 改 `-cp "vb-stream-engine/target/classes:$(cat vb-stream-engine/target/cp.txt)"`。

(d) JMH 运行段(“JMH 基准运行方式见……须在模块根目录运行”)——命令前缀改为 `-pl vb-stream-engine`,`-cp` 改 `"vb-stream-engine/target/classes:vb-stream-engine/target/test-classes:$(cat vb-stream-engine/target/cp.txt)"`,并把"须在模块根目录运行"改为"须在仓库根目录带 `-pl vb-stream-engine` 运行(或进入 vb-stream-engine/ 目录)"。

(e) 源码结构 bullets——`src/main/java`、`src/test/java`、`src/jmh/java` 三行路径前缀改 `vb-stream-engine/src/...`,并在该列表后追加一行:

```
    - `vb-stream-connector-postgres-stream/src/main/java`：`org.vastdata.debezium.connector.postgresql.stream`（Debezium 流式连接器,MS1 起开发）
```

- [ ] **Step 2: 更新 docs/benchmarks-baseline.md 运行路径**

Run: `grep -n 'target/cp.txt\|target/classes\|target/test-classes\|模块根目录' docs/benchmarks-baseline.md`

把每处命中的路径加 `vb-stream-engine/` 前缀(例:`target/classes` → `vb-stream-engine/target/classes`),"模块根目录"表述改为 `vb-stream-engine/`。改完再跑一次上述 grep 确认无裸 `target/` 路径残留(基线数字段落不动,只改运行方式段)。

- [ ] **Step 3: Main 冒烟验收(Docker 必须在运行)**

```bash
cd src/docker && docker compose up -d && cd ../..
mvn -q -pl vb-stream-engine compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
java --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens jdk.unsupported/sun.misc=ALL-UNNAMED \
     --add-opens java.base/sun.nio.fs=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "vb-stream-engine/target/classes:$(cat vb-stream-engine/target/cp.txt)" org.vastdata.vbstream.Main
```

Expected: 正常启动(建/复用槽、起 reader/consumer 线程),运行期每 10s 打四行统计 INFO(消费者统计行 + `吞吐:` + `分布:` + `峰值:`);`Ctrl+C` 后干净退出(shutdown hook join reader 后退出,无异常栈)。向测试表插几行可见 TXN-BEGIN/行/TXN-END 输出。

- [ ] **Step 4: Commit + push**

```bash
git add CLAUDE.md docs/benchmarks-baseline.md
git commit -m "docs: 多模块化后的路径与结构说明——CLAUDE.md 坐标/命令/运行路径/源码结构,benchmark 基线文档运行路径改模块相对

Co-Authored-By: Claude <noreply@anthropic.com>"
git push
```

---

## 验收汇总(对照 spec §10 MS0)

- [ ] `mvn clean test`(仓库根):parent + 两模块 BUILD SUCCESS,引擎 177 测试全绿
- [ ] `mvn -Pjmh clean test-compile`:jmh 档正常编译
- [ ] Main 冒烟可跑、10s 统计行正常、Ctrl+C 干净退出
- [ ] `git log --follow vb-stream-engine/src/main/java/org/vastdata/vbstream/Main.java` 能追到迁移前的历史(git mv 保历史生效)
- [ ] CLAUDE.md / benchmarks-baseline.md 路径与新结构一致
