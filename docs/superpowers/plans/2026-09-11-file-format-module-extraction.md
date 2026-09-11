# vb-stream-file-format 模块抽取实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 vb-stream-reader 内的 VBFG 契约层（format 包）抽成独立 Maven 模块 `vb-stream-file-format`，供未来读取/解析侧复用。

**Architecture:** 纯抽取、零行为变化——16 个契约层文件（15 类 + package-info）与 12 个测试用例 git mv 进新模块并改包名（`org.vastdata.vbstream.reader.format` → `org.vastdata.vbstream.format`）；落地链路（file 包，依赖 Debezium API）留在 reader，仅改 import 来源；文档四处同步。

**Tech Stack:** Java 17 + Maven 多模块；新模块运行时**零第三方依赖**（纯 JDK），test 仅 JUnit 6。

**Spec:** `docs/superpowers/specs/2026-09-11-file-format-module-extraction-design.md`

## Global Constraints

- 新模块坐标：`org.vastdata:vb-stream-file-format:1.0-SNAPSHOT`，parent 为根 pom，packaging 默认 jar（spec §3）
- 新模块运行时零依赖（纯 JDK，无 slf4j/kafka/debezium）；test 仅 `junit-jupiter` `${junit-jupiter.version}`（spec §3）
- 包名：`org.vastdata.vbstream.format`（spec §3）
- 迁移零行为变化：类代码不动（仅 package/import 行），字节输出不变（spec §2/§4）
- 测试零增减：全仓合计用例数不变（reader 28 → 16，新模块 12）（spec §6）
- 跨仓同步契约随迁：布局不兼容变更须两仓递增 `ChangeFileWriter.VERSION`；`TypeCode`/`Op` 枚举只追加不重排不改名（spec §5）
- TDD 节奏适配说明：本计划是**迁移任务**——验收测试已存在（VarintTest 6 用例 + ChangeFileIOTest 6 用例随迁），每任务的测试环节是"迁移后跑既有测试确认全绿"，不新写失败测试
- 项目规约：方法名英文 camelCase；类型引用 import 简名禁止 FQN 内联；日志 slf4j（本模块无日志）；每函数 javadoc；每任务完成 commit

---

### Task 1: 新模块脚手架与契约层迁移

**Files:**
- Create: `vb-stream-file-format/pom.xml`
- Create: `vb-stream-file-format/src/main/java/org/vastdata/vbstream/format/`（16 文件，自 `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/format/` git mv）
- Create: `vb-stream-file-format/src/test/java/org/vastdata/vbstream/format/`（`VarintTest.java`、`ChangeFileIOTest.java`，自 reader 同路径 git mv）
- Modify: 根 `pom.xml` 的 `<modules>`（engine 之后插入）
- Delete（经 git mv 自动）: reader 侧原 format 目录与两个测试文件

**Interfaces:**
- Consumes: 无（首个任务）
- Produces: `org.vastdata.vbstream.format` 包的全部公开类型——`Varint`（`writeUnsigned(DataOutput,long)`/`readUnsigned(DataInput)`/`writeZigzag`/`readZigzag`）、`Values`（`write(DataOutput,TypeCode,Object)`/`read(DataInput,TypeCode)`）、`TypeCode`/`Op` 枚举、`ColumnDef(name,type)`、`TableDef(id,db,schema,table,keyColumns,columns)`、`Record` sealed 族、`FileNaming.fileName(task,seq,time,ext)`/`nextSeq(dataDir,task)`、`ChangeFileWriter(file,seq,sourceDb)`（`writeBegin/writeEvent/writeTruncate/writeCommit/finish/recordCount`）、`ChangeFileReader.open(file)`（`Iterable<Record>` + `seq()/sourceDb()/complete()`）。Task 2 的 import 批改依赖这些类名不变。

- [ ] **Step 1: 建模块目录与 pom**

`vb-stream-file-format/pom.xml` 全文（照 reader pom 的形态裁剪——无任何运行时依赖，无 surefire 定制）：

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

    <artifactId>vb-stream-file-format</artifactId>

    <!-- 模块定位：VBFG 落地文件格式契约层（模型 + 二进制流式读写 + LEB128 + 文件命名），
         移植自 vb-cdc-file-transform 的 cdc-file-format（2026-09-11 快照）。零第三方依赖是
         刻意设计——未来读取/解析侧（file-sink、转换/校验工具）以一个纯 JDK 依赖获得完整
         契约；跨仓同步契约见模块 CLAUDE.md。 -->
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

根 `pom.xml` 的 `<modules>` 改为（新模块插在 engine 之后——按依赖层次序）：

```xml
    <modules>
        <module>vb-stream-engine</module>
        <module>vb-stream-file-format</module>
        <module>vb-stream-connector-postgres-stream</module>
        <module>vb-stream-reader</module>
    </modules>
```

- [ ] **Step 2: git mv 主代码与测试（保文件历史）**

```bash
mkdir -p vb-stream-file-format/src/main/java/org/vastdata/vbstream
mkdir -p vb-stream-file-format/src/test/java/org/vastdata/vbstream
git mv vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/format \
       vb-stream-file-format/src/main/java/org/vastdata/vbstream/format
git mv vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/format \
       vb-stream-file-format/src/test/java/org/vastdata/vbstream/format
```

- [ ] **Step 3: 批改包名（package 声明与包内互引 import）**

新模块内 18 个文件（16 主 + 2 测试）统一替换：

```bash
grep -rl 'org\.vastdata\.vbstream\.reader\.format' vb-stream-file-format/src | \
  xargs sed -i 's/org\.vastdata\.vbstream\.reader\.format/org.vastdata.vbstream.format/g'
```

- [ ] **Step 4: 新模块独立测试（此刻 reader 编译会红——预期，reader 修复在 Task 2）**

```bash
mvn test -pl vb-stream-file-format
```
Expected: `Tests run: 12, Failures: 0, Errors: 0`（VarintTest 6 + ChangeFileIOTest 6），零 Docker、秒级。

- [ ] **Step 5: Commit**

```bash
git add pom.xml vb-stream-file-format vb-stream-reader
git commit -m "refactor(file-format): VBFG 契约层抽独立模块——16 文件 git mv 保历史，包名 org.vastdata.vbstream.format"
```

---

### Task 2: reader 切换依赖与 import 批改

**Files:**
- Modify: `vb-stream-reader/pom.xml`（加 compile 依赖）
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/EnvelopeParser.java`、`ParsedEvent.java`、`VbfgEventWriter.java`、`FileRollingWriter.java`（format import 行）
- Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/file/FileChangeConsumerTest.java`、`it/ReaderFileSinkIT.java`（format import 行）

**Interfaces:**
- Consumes: Task 1 的 `org.vastdata.vbstream.format` 全部公开类型（类名零变化，仅包名变）
- Produces: reader 恢复编译、全部 16 用例全绿（`FileChangeConsumerTest` 3 + `FileRollingWriterTest` 3 + `ReaderPropertiesTest` 7 + `ReaderEndToEndIT` 2 + `ReaderFileSinkIT` 1）——Task 3 的全仓回归依赖此状态

- [ ] **Step 1: reader pom 加依赖（插在 connector 依赖之后）**

```xml
        <!-- VBFG 契约层:file 输出形态的格式读写(file 包与 ReaderFileSinkIT 经此传递依赖) -->
        <dependency>
            <groupId>org.vastdata</groupId>
            <artifactId>vb-stream-file-format</artifactId>
            <version>${project.version}</version>
        </dependency>
```

- [ ] **Step 2: import 批改（reader 内全部残留引用）**

```bash
grep -rl 'org\.vastdata\.vbstream\.reader\.format' vb-stream-reader/src | \
  xargs sed -i 's/org\.vastdata\.vbstream\.reader\.format/org.vastdata.vbstream.format/g'
grep -rn 'org\.vastdata\.vbstream\.reader\.format' vb-stream-reader/src || echo "零残留"
```
Expected: 第二条命令输出 `零残留`。

- [ ] **Step 3: reader 全量测试（含 Docker IT）**

```bash
mvn test -pl vb-stream-reader
```
Expected: `Tests run: 16, Failures: 0, Errors: 0`（12 用例已随迁走，`ReaderFileSinkIT` 经新模块坐标读回落地文件——跨模块复用验收）。

- [ ] **Step 4: Commit**

```bash
git add vb-stream-reader
git commit -m "refactor(reader): format import 切换 vb-stream-file-format 传递依赖——28 用例降 16（12 随迁），全绿"
```

---

### Task 3: 文档同步与全仓回归

**Files:**
- Create: `vb-stream-file-format/CLAUDE.md`
- Modify: `vb-stream-reader/CLAUDE.md`（file 形态节的格式部分改为指向新模块）
- Modify: `vb-stream-reader/README.md`（file 形态节的格式句指向新模块）
- Modify: 根 `CLAUDE.md`（坐标行、源码结构段）
- Modify: 根 `README.md`（坐标行三模块 → 四模块）

**Interfaces:**
- Consumes: Task 1/2 的最终模块拓扑
- Produces: 文档与代码一致；全仓 `mvn test` 524 用例全绿

- [ ] **Step 1: 新模块 CLAUDE.md**

全文：

```markdown
# vb-stream-file-format — VBFG 落地文件格式契约层

跨模块共享的落地文件契约：精简事件模型 + 二进制流式读写 + LEB128 编解码 + 文件命名。
移植自 vb-cdc-file-transform 仓的 cdc-file-format 模块（2026-09-11 快照，写侧 + 读侧完整
移植），目标是本仓产出的落地文件与该项目 cdc-sink 的消费格式**逐字节互通**。

## 常用命令

零 Docker、秒级：`mvn test -pl vb-stream-file-format`（12 用例）。

## 二进制布局（修改 Writer/Reader 前必读）

    [Magic "VBFG"][version][seq][sourceDb]        —— 文件头，不进 CRC
    记录流，每条 = [varint 长度][类型字节 + 载荷]，全部计入 CRC32：
      TABLE_DEF(1): defId, db, schema, table, key列下标[], 列[]{name, typeCode}
      BEGIN(2):     txid, lsn
      EVENT(3):     defId, op, null位图, 非null值序列（按列序）
      COMMIT(4):    txid, lsn, ts
      TRUNCATE(5):  defId
      FOOTER(6):    recordCount, crc32             —— 最后一条，自身不进 CRC

变长整数（`Varint`）：LEB128 小端序。无符号用于长度/个数/id；zigzag 用于有符号数值
（txid/lsn/时间戳）。

## 关键不变量

- **跨仓同步契约**：字节布局任何不兼容变更须在两仓（本模块与 vb-cdc-file-transform）
  同步递增 `ChangeFileWriter.VERSION`；`TypeCode.id()`（ordinal+1）与 `Op.id()`（ordinal）
  已持久化进文件——枚举只能追加，不能重排、改名、删除。
- **TypeCode 值域**：原生六种 + 时间五种；时间类型值一律字符串载荷，布局同 STRING。
- **defId 文件内作用域**：每份文件从 1 重新分配；`TableDef` 的 equals/hashCode 刻意忽略
  id——Writer 靠此做文件内 TABLE_DEF 去重，列结构变化（DDL）分配新 id。
- **FOOTER 不进 CRC、不计 recordCount**；其余每条记录的"长度前缀 + 载荷"都计入 CRC32。
- **`Values` 不处理 null**：null 由 EVENT 的位图表达，只序列化非 null 值，列序严格对齐。
- **`FileNaming`**：`<task>-<seq 16位零填充>-<yyyyMMddHHmmss>.bin`，**文件名字典序 = 消费
  顺序**；`nextSeq()` 扫数据目录恢复序号——seq 宽度与零填充是排序正确性的基础。
- Reader 流式 `Iterator<Record>` 惰性读、常量内存；迭代到 FOOTER 才校验记录数与 CRC；
  截断（记录中间 EOF）与 CRC 不一致抛 IOException（迭代侧 UncheckedIOException 包装）。

## 依赖与使用方

**零第三方依赖是刻意设计**（编译期仅父 pom，测试期 JUnit）——未来读取/解析侧
（file-sink、转换/校验工具）以纯 JDK 依赖获得完整契约，不要引入任何运行时依赖。
当前使用方：vb-stream-reader 的 file 输出形态（落地链路在其 `file` 包——依赖 Debezium
API 属链路不属契约，故留 reader）。落地链路语义（tmp→fsync→原子 rename、COMMIT 边界
切分、offset 联动）见 vb-stream-reader 模块文档。

## 测试

`VarintTest`（roundtrip/边界值/已知字节形态/负值拒绝/超长流拒绝）+
`ChangeFileIOTest`（全记录类型 RoundTrip、null 位图、多表 defId 去重、CRC 破坏与截断
检测、非 VBFG 拒绝、列数不匹配 fail-fast）——契约层自包含的验收面，也是移植正确性锚定。
```

- [ ] **Step 2: reader CLAUDE.md 批改**

「file 输出形态」节的 `format` 包要点改为（替换原"**`format` 包（VBFG 契约层移植）**"整条）：

```markdown
- **`vb-stream-file-format` 模块（VBFG 契约层，compile 依赖）**：cdc-file-format 的源码
  移植（写侧+读侧），包 `org.vastdata.vbstream.format`——文件布局、跨仓同步契约、关键
  不变量见该模块 CLAUDE.md。reader 的 file 包是其首个使用方（EnvelopeParser 解析结果经
  VbfgEventWriter 写入；ReaderFileSinkIT 经传递依赖读回验收跨模块复用）。
```

组件表的 `SinkConfig` 行不动；file 形态节其余条目（file 包/契约/偏离）不动——偏离在
EnvelopeParser 属 reader。

- [ ] **Step 3: 其余三处文档批改**

`vb-stream-reader/README.md` file 形态节"格式"行末尾追加一句：

```markdown
（契约层在独立模块 `vb-stream-file-format`，布局细目与跨仓同步契约见该模块 CLAUDE.md）
```

根 `README.md` 坐标行"三模块"改"四模块"，模块清单插入
`vb-stream-file-format`（VBFG 落地文件契约层——纯 JDK 零依赖）。

根 `CLAUDE.md` 坐标行同步四模块；源码结构段 `vb-stream-reader/src/main/java` 条目中
`format` 包描述移除（改为一句"VBFG 契约层已抽 `vb-stream-file-format` 模块"），并新增
`vb-stream-file-format/src/main/java` 一行（包 `org.vastdata.vbstream.format`，指向模块
CLAUDE.md）。

- [ ] **Step 4: 全仓回归（验收 spec §6）**

```bash
mvn test
```
Expected: 四模块全绿，全仓合计 524 用例（引擎 224 + 连接器 272 + file-format 12 + reader 16）。

```bash
grep -rn "org\.vastdata\.vbstream\.reader\.format" --include="*.java" . | grep -v target || echo "零残留"
```
Expected: `零残留`（历史 spec/plans 文档中的旧包名按惯例不追改，仅查 .java）。

- [ ] **Step 5: Commit + push**

```bash
git add CLAUDE.md README.md vb-stream-file-format/CLAUDE.md vb-stream-reader/CLAUDE.md vb-stream-reader/README.md
git commit -m "docs(file-format): 模块抽取文档同步——新模块 CLAUDE.md + 四处模块拓扑更新"
git push
```
