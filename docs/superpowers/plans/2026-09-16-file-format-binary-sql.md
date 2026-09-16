# file-format 三模块拆分与 binary/sql 双格式 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 vb-stream-file-format 拆为基座 + binary + sql 三模块(同构对方仓 vb-cdc-file-transform),reader 的 file 输出形态新增 SQL 文本落地格式,配置命名空间 `sink.*` 整体迁移 `reader.*`。

**Architecture:** 格式层零依赖三模块(基座 IR/命名 ← binary 读写 ← sql 渲染各自依赖基座);reader 的 `file` 包保持 `FileChangeConsumer → FileRollingWriter(切分/publish/offset 联动外壳) → EventFileWriter(格式实现)` 数据流,新增 `OutputFormat` 枚举工厂按配置选 `VbfgEventWriter`/`SqlEventWriter`。DDL 与 json 不做(spec §8 裁定)。

**Tech Stack:** Java 17 + Maven 多模块;JUnit 6(Jupiter);Testcontainers(postgres:18,reader IT)。

**Spec:** `docs/superpowers/specs/2026-09-16-file-format-binary-sql-design.md`(本计划从 spec 立论,执行者两份都读)

## Global Constraints

- **测试与主代码方法名一律英文 camelCase**,行为描述式命名;**禁止中文方法名**。
- **类型引用一律 import 后用简名,禁止全限定类名内联**(嵌套类型用外层简名,如 `OutputConfig.Mode`)。
- **日志一律 slf4j**(`LoggerFactory.getLogger`),禁止 `System.out/err`;消息用 `{}` 占位符。
- **每个函数(含私有/测试辅助)必须有 javadoc**:职责、关键步骤、边界与异常语义。
- **三格式模块零第三方依赖**:基座编译期仅父 pom;binary/sql 编译期仅父 pom + 基座;测试期 JUnit。**不引入任何运行时依赖**。
- **binary 模块字节布局零变化**:迁移是纯移包,不改任何编码逻辑、不动 `ChangeFileWriter.VERSION`(=1);**不进 DDL(7)**。
- SqlRenderer 与对方仓分叉点唯一:**无 `renderDdl`/`ParsedDdl`**(DDL 裁定)。
- 配置命名空间 **`reader.*`**(文件裸键)/ **`vb.reader.*`**(系统属性);全链路无 `sink` 字样(类名/键/IT 类名/日志文案/注释)。
- 避开 engine 已占用的 `vb.output.*`——reader 一律用 `vb.reader.*`,不得引入 `output.` 前缀键。
- 每任务完成即 commit(信息用 `feat:`/`refactor:`/`test:`/`docs:` 前缀 + 中文主题);全部完成后 push。
- Windows 开发机:`mvn test -pl <模块>` 带模块名运行;shell 为 Git Bash(POSIX 路径)。
- 跨模块验证命令(每任务收尾):`mvn test -pl vb-stream-file-format,vb-stream-binary-format,vb-stream-sql-format,vb-stream-reader`(-Dtest 过滤单类时须带 -pl)。

---

### Task 1: 基座 IR 下沉(ParsedEvent)与 FileNaming 三后缀

**Files:**
- Create: `vb-stream-file-format/src/main/java/org/vastdata/vbstream/format/ParsedEvent.java`
- Create: `vb-stream-file-format/src/test/java/org/vastdata/vbstream/format/FileNamingTest.java`
- Delete: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/ParsedEvent.java`
- Modify: `vb-stream-file-format/src/main/java/org/vastdata/vbstream/format/FileNaming.java`
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/EnvelopeParser.java`(import)
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/VbfgEventWriter.java`(import)

**Interfaces:**
- Consumes: 基座现有 `TableDef.of(String db, String schema, String table, List<String> keyColumns, List<ColumnDef> columns)`、`Op`、`TypeCode`;`FileNaming` 现有四参 `fileName(String task, long seq, LocalDateTime time, String extension)` 与 `parseSeqOpt`/`nextSeq`(本任务不动方法面)。
- Produces: `public record ParsedEvent(TableDef tableDef, Op op, Object[] values, boolean truncate, long lsn, long txid, long timestampMs)`(values 防御性 clone);`FileNaming` 后缀正则认 `bin|json|sql`(方法签名零变化)。Task 2 的 SqlRenderer 与 Task 5 的接线依赖这些签名。

- [ ] **Step 1: 写 FileNamingTest(失败)**

新建 `vb-stream-file-format/src/test/java/org/vastdata/vbstream/format/FileNamingTest.java`:

```java
package org.vastdata.vbstream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link FileNaming} 文件命名契约单测:三后缀(bin/json/sql)组装与解析、四参重载、
 * seq 从数据目录恢复(跨后缀——重启换格式时目录里可能混有旧后缀文件,seq 恢复与后缀无关)。
 */
class FileNamingTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /**
     * 场景:四参 fileName 按传入后缀组装(bin/sql);文件名形态
     * {@code <task>-<seq 16位零填充>-<yyyyMMddHHmmss>.<ext>}。
     */
    @Test
    void fileNameAssemblesWithRequestedExtension() {
        LocalDateTime t = LocalDateTime.of(2026, 9, 16, 8, 30, 0);
        assertEquals("tk-0000000000000007-20260916083000.sql",
                FileNaming.fileName("tk", 7, t, "sql"));
        assertEquals("tk-0000000000000007-20260916083000.bin",
                FileNaming.fileName("tk", 7, t, "bin"));
    }

    /**
     * 场景:parseSeqOpt 对三种后缀都解析出 seq;非法命名(缺段时间戳/宽度不对)返回 empty。
     */
    @Test
    void parseSeqAcceptsAllThreeSuffixes() {
        assertEquals(OptionalLong.of(42), FileNaming.parseSeqOpt("tk-0000000000000042-20260101000000.bin"));
        assertEquals(OptionalLong.of(42), FileNaming.parseSeqOpt("tk-0000000000000042-20260101000000.json"));
        assertEquals(OptionalLong.of(42), FileNaming.parseSeqOpt("tk-0000000000000042-20260101000000.sql"));
        assertTrue(FileNaming.parseSeqOpt("tk-42-20260101000000.bin").isEmpty(), "seq 宽度不足不解析");
        assertTrue(FileNaming.parseSeqOpt("tk-0000000000000042-20260101.bin").isEmpty(), "时间戳段残缺不解析");
    }

    /**
     * 场景:nextSeq 跨后缀恢复——目录里预置 .sql/.json 的高 seq 文件(上次会话用了别的格式),
     * 新构造侧扫目录取 max+1,与后缀无关。
     *
     * @throws IOException 目录扫描失败上抛
     */
    @Test
    void nextSeqRestoresAcrossSuffixes() throws IOException {
        Files.writeString(dir.resolve("tk-0000000000000009-20260101000000.bin"), "old");
        Files.writeString(dir.resolve("tk-0000000000000099-20260101000000.sql"), "old");
        Files.writeString(dir.resolve("tk-0000000000000050-20260101000000.json"), "old");
        assertEquals(100, FileNaming.nextSeq(dir.toAbsolutePath(), "tk"),
                "取三种后缀中的最大 seq 99 + 1");
        assertEquals(1, FileNaming.nextSeq(dir.toAbsolutePath(), "other"),
                "目录里没有本 task 的文件时从 1 起");
    }
}
```

注:`parseSeqOpt` 与四参 `fileName` 均为 `FileNaming` 现有方法(已核对 `FileNaming.java:34,42`)——本测试直接使用,不新增方法面。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-file-format -Dtest=FileNamingTest`
Expected: FAIL(`.sql` 后缀的组装/解析断言失败——正则与调用尚不认 sql;既有实现无编译错)

- [ ] **Step 3: 改 FileNaming 正则 + 下沉 ParsedEvent**

`FileNaming.java` 唯一修改:NAME 正则 `\\.(bin|json)` → `\\.(bin|json|sql)`(第 28 行;类 javadoc 的后缀说明同步)。方法面零变化——四参 `fileName` 与 `parseSeqOpt`/`nextSeq` 现状即所需形态。

新建 `vb-stream-file-format/src/main/java/org/vastdata/vbstream/format/ParsedEvent.java`(内容 = reader file 包同名 record 移包并公开,javadoc 保留):

```java
package org.vastdata.vbstream.format;

/**
 * envelope 解析结果:写落地文件所需的表定义 + 操作 + 对齐列值(binary 与 sql 两种格式的
 * 共享 IR——Values 编码与 SqlRenderer 渲染都以本 record 为输入)。
 * values 数组组件显式值相等语义(record 默认对数组退化引用相等)。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 ParsedEvent(2026-09-16 快照,原在
 * reader file 包,拆分三模块时下沉基座)。
 */
public record ParsedEvent(TableDef tableDef, Op op, Object[] values, boolean truncate,
                          long lsn, long txid, long timestampMs) {

    public ParsedEvent {
        values = values.clone();
    }

    @Override
    public Object[] values() {
        return values.clone();
    }
}
```

删除 `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/ParsedEvent.java`;`EnvelopeParser.java` 与 `VbfgEventWriter.java` 加 `import org.vastdata.vbstream.format.ParsedEvent;`(其余零改动——两文件现有 import 已含 format 包其他类型)。全仓 grep `reader.file.ParsedEvent` 确认无残留引用(reader 测试 `FileChangeConsumerTest`/`ConnectTestRecords` 若引用了包私有 record,同步改 import)。

- [ ] **Step 4: 跑全量测试确认绿**

Run: `mvn test -pl vb-stream-file-format,vb-stream-reader`
Expected: PASS(file-format 新增 FileNamingTest;reader 既有用例全绿)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(file-format): ParsedEvent 下沉基座 + FileNaming 后缀正则扩 sql"
```

---

### Task 2: vb-stream-sql-format 模块(SqlRenderer 移植)

**Files:**
- Create: `vb-stream-sql-format/pom.xml`
- Create: `vb-stream-sql-format/src/main/java/org/vastdata/vbstream/format/sql/SqlRenderer.java`
- Create: `vb-stream-sql-format/src/test/java/org/vastdata/vbstream/format/sql/SqlRendererTest.java`
- Modify: `pom.xml`(根聚合 `<modules>`)

**Interfaces:**
- Consumes: 基座 `ParsedEvent`、`TableDef`、`ColumnDef`、`Op`、`TypeCode`(Task 1)。
- Produces: `public static String SqlRenderer.render(ParsedEvent event)`——返回以 `;\n` 结尾的单条语句;失败(无主键 u/d、INTERVAL 非微秒形态、字符串含 NUL、类型载荷错位)抛 `IllegalArgumentException`。Task 5 的 `SqlEventWriter` 调用它。

- [ ] **Step 1: 建模块骨架 + 写 SqlRendererTest(失败)**

根 `pom.xml` `<modules>` 在 `vb-stream-file-format` 之后插两行(顺序:engine, file-format, **sql-format, binary-format**(binary Task 3 建), connector, reader——本任务先插 sql 一行,binary 行 Task 3 再插;若愿一次插全两行,Task 3 建模块前须容忍 reactor 报缺模块,**故本任务只插 sql 行**)。

新建 `vb-stream-sql-format/pom.xml`:

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

    <artifactId>vb-stream-sql-format</artifactId>

    <!-- 模块定位：SQL 文本落地格式——IR → 可执行 SQL 语句渲染（PG/VastBase 方言），移植自
         vb-cdc-file-transform 的 cdc-sql-file-format（2026-09-16 快照，裁掉 renderDdl——本项目
         不做 DDL，见 spec §8）。零第三方依赖刻意设计。 -->
    <dependencies>
        <dependency>
            <groupId>org.vastdata</groupId>
            <artifactId>vb-stream-file-format</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

新建 `vb-stream-sql-format/src/test/java/org/vastdata/vbstream/format/sql/SqlRendererTest.java`——对方仓 12 用例去掉 DDL 用例后的 11 个,方法名改英文 camelCase,断言逐字符保留:

```java
package org.vastdata.vbstream.format.sql;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.format.ColumnDef;
import org.vastdata.vbstream.format.Op;
import org.vastdata.vbstream.format.ParsedEvent;
import org.vastdata.vbstream.format.TableDef;
import org.vastdata.vbstream.format.TypeCode;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * IR → SQL 文本渲染规则钉行为(移植自 vb-cdc-file-transform 的 SqlRendererTest,断言逐字符
 * 保留——两仓渲染行为可对照)。语句语义与对方仓 cdc-sink 的 RecordSqlGenerator 对齐:
 * INSERT 全列 / UPDATE SET after 全列 WHERE 主键 / DELETE WHERE 主键。
 */
class SqlRendererTest {

    /**
     * 责任:构造带主键与列集的表定义(全部用例共用的组装便捷形态)。
     *
     * @param keys 主键列名集
     * @param cols 列定义
     * @return 表定义(db 固定 "db",schema "public",表 "orders")
     */
    private static TableDef table(List<String> keys, ColumnDef... cols) {
        return TableDef.of("db", "public", "orders", keys, List.of(cols));
    }

    /**
     * 责任:构造非 truncate 的数据事件(lsn/txid/ts 用占位常量——渲染不消费)。
     */
    private static ParsedEvent event(TableDef def, Op op, Object[] values) {
        return new ParsedEvent(def, op, values, false, 1L, 1L, 0L);
    }

    /**
     * 场景:INSERT 全列 + 各 TypeCode 字面量——INT 十进制、BOOL、FLOAT64、BYTES decode()
     * 小写 hex、时间五种单引号原样、INTERVAL 微秒分解、STRING 单引号翻倍、null→NULL。
     */
    @Test
    void insertRendersAllColumnsAndTypeLiterals() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("name", TypeCode.STRING),
                new ColumnDef("amount", TypeCode.STRING),
                new ColumnDef("flag", TypeCode.BOOL),
                new ColumnDef("price", TypeCode.FLOAT64),
                new ColumnDef("raw", TypeCode.BYTES),
                new ColumnDef("ts", TypeCode.TIMESTAMP),
                new ColumnDef("tstz", TypeCode.TIMESTAMPTZ),
                new ColumnDef("d", TypeCode.DATE),
                new ColumnDef("t", TypeCode.TIME),
                new ColumnDef("iv", TypeCode.INTERVAL),
                new ColumnDef("memo", TypeCode.STRING));
        String sql = SqlRenderer.render(event(def, Op.CREATE, new Object[]{
                1L, "alice's", "123.45", Boolean.TRUE, 9.5, new byte[]{(byte) 0xA1, 0x03},
                "2026-09-15 10:00:00.000000", "2026-09-10T07:30:00.000000Z", "2026-09-10",
                "12:34:56.789456", "45296789000 microseconds", null}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"name\", \"amount\", \"flag\", "
                + "\"price\", \"raw\", \"ts\", \"tstz\", \"d\", \"t\", \"iv\", \"memo\") VALUES "
                + "(1, 'alice''s', '123.45', TRUE, 9.5, decode('a103','hex'), "
                + "'2026-09-15 10:00:00.000000', '2026-09-10T07:30:00.000000Z', '2026-09-10', "
                + "'12:34:56.789456', '0 days 12:34:56.789000', NULL);\n", sql);
    }

    /**
     * 场景:列名含双引号翻倍转义;Float NaN 渲染带引号形态(非合法裸字面量)。
     */
    @Test
    void insertEscapesQuotedIdentifiersAndRendersFloatSpecialValues() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("\"weird\"col", TypeCode.STRING),
                new ColumnDef("f", TypeCode.FLOAT32));
        String sql = SqlRenderer.render(event(def, Op.READ, new Object[]{1L, "含\"引号", Float.NaN}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"\"\"weird\"\"col\", \"f\") "
                + "VALUES (1, '含\"引号', 'NaN');\n", sql);
    }

    /**
     * 场景:UPDATE SET after 全列、WHERE 主键列。
     */
    @Test
    void updateSetsAllColumnsAndFiltersByPrimaryKey() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("name", TypeCode.STRING));
        String sql = SqlRenderer.render(event(def, Op.UPDATE, new Object[]{1L, "alice2"}));
        assertEquals("UPDATE \"public\".\"orders\" SET \"id\" = 1, \"name\" = 'alice2' "
                + "WHERE \"id\" = 1;\n", sql);
    }

    /**
     * 场景:DELETE 只渲染 WHERE 主键。
     */
    @Test
    void deleteFiltersByPrimaryKey() {
        TableDef def = table(List.of("id"), new ColumnDef("id", TypeCode.INT));
        assertEquals("DELETE FROM \"public\".\"orders\" WHERE \"id\" = 2;\n",
                SqlRenderer.render(event(def, Op.DELETE, new Object[]{2L})));
    }

    /**
     * 场景:TRUNCATE 渲染为逐表 TRUNCATE TABLE 语句(op 与 values 均不消费)。
     */
    @Test
    void truncateRendersPerTableStatement() {
        ParsedEvent ev = new ParsedEvent(table(List.of(), new ColumnDef[0]), null,
                new Object[0], true, 1L, 1L, 0L);
        assertEquals("TRUNCATE TABLE \"public\".\"orders\";\n", SqlRenderer.render(ev));
    }

    /**
     * 场景:无主键表的 UPDATE/DELETE 抛 IAE(捕获端失败优于静默丢;与 binary"不支持类型抛异常"
     * 同构——引擎停止、offset 不动、重启重放)。
     */
    @Test
    void updateAndDeleteWithoutPrimaryKeyThrow() {
        TableDef def = table(List.of(), new ColumnDef("id", TypeCode.INT));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.UPDATE, new Object[]{1L})));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.DELETE, new Object[]{1L})));
    }

    /**
     * 场景:字符串载荷含 NUL 字符抛 IAE(SQL 文本无法表达,不产出损坏文件)。
     */
    @Test
    void stringWithNulThrows() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT), new ColumnDef("v", TypeCode.STRING));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "a\0b"})));
    }

    /**
     * 场景:多行字符串值原样进单引号字面量(换行不转义)。
     */
    @Test
    void multiLineStringValueRendersVerbatim() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT), new ColumnDef("v", TypeCode.STRING));
        String sql = SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "第一行\n第二行"}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"v\") VALUES (1, '第一行\n第二行');\n", sql);
    }

    /**
     * 场景:负值 INTERVAL 天与时分秒两段都带符号;非微秒形态载荷抛 IAE(VastBase 拒收
     * microseconds 直写,渲染侧分解——对方仓实测裁定)。
     */
    @Test
    void intervalNegativeDecomposesAndNonMicrosecondFormThrows() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT), new ColumnDef("iv", TypeCode.INTERVAL));
        String sql = SqlRenderer.render(event(def, Op.CREATE,
                new Object[]{1L, "-45296789000 microseconds"}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"iv\") "
                + "VALUES (1, '-0 days -12:34:56.789000');\n", sql);
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "abc"})));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRenderer.render(event(def, Op.CREATE, new Object[]{1L, "12x microseconds"})));
    }

    /**
     * 场景:空 byte[] 渲染 decode('','hex');±Infinity 走带引号路径。
     */
    @Test
    void emptyByteaAndSignedInfinityLiterals() {
        TableDef def = table(List.of("id"),
                new ColumnDef("id", TypeCode.INT),
                new ColumnDef("raw", TypeCode.BYTES),
                new ColumnDef("f1", TypeCode.FLOAT32),
                new ColumnDef("f2", TypeCode.FLOAT64));
        String sql = SqlRenderer.render(event(def, Op.CREATE,
                new Object[]{1L, new byte[0], Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}));
        assertEquals("INSERT INTO \"public\".\"orders\" (\"id\", \"raw\", \"f1\", \"f2\") "
                + "VALUES (1, decode('','hex'), 'Infinity', '-Infinity');\n", sql);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-sql-format`
Expected: FAIL(编译错——`SqlRenderer` 不存在)

- [ ] **Step 3: 移植 SqlRenderer**

新建 `vb-stream-sql-format/src/main/java/org/vastdata/vbstream/format/sql/SqlRenderer.java`——对方仓 1:1 移植(去掉 `renderDdl` 与 `ParsedDdl` import),包/重导入按本仓。完整代码:

```java
package org.vastdata.vbstream.format.sql;

import org.vastdata.vbstream.format.ColumnDef;
import org.vastdata.vbstream.format.Op;
import org.vastdata.vbstream.format.ParsedEvent;
import org.vastdata.vbstream.format.TableDef;
import org.vastdata.vbstream.format.TypeCode;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.StringJoiner;

/**
 * IR → 可直接执行的 SQL 文本(PG/VastBase 方言)。纯函数、零状态、零第三方依赖。
 *
 * <p>语句语义与 vb-cdc-file-transform 仓 cdc-sink 的 RecordSqlGenerator 对齐:INSERT 全列、
 * UPDATE SET after 全列 WHERE 主键、DELETE WHERE 主键;无主键表的 u/d 抛异常(失败优于静默丢,
 * 引擎停止、offset 不推进、重启重放)。值以字面量渲染:时间五种与 decimal 沿"字符串落地"策略,
 * 由目标库按目标列类型隐式转换。裸 SQL 语义:如实反映源端操作序列,重复执行的主键冲突由执行方
 * 处理。移植自对方仓 cdc-sql-file-format(2026-09-16 快照,裁掉 renderDdl——本项目不做 DDL)。
 */
public final class SqlRenderer {

    private SqlRenderer() {
    }

    /**
     * 责任:渲染一条数据事件(INSERT/UPDATE/DELETE/TRUNCATE)为以 {@code ;\n} 结尾的语句。
     * 边界:无主键 u/d、INTERVAL 载荷非微秒形态、字符串含 NUL 抛 IAE(见类 javadoc)。
     */
    public static String render(ParsedEvent event) {
        if (event.truncate()) {
            return truncate(event.tableDef());
        }
        return switch (event.op()) {
            case CREATE, READ -> insert(event.tableDef(), event.values());
            case UPDATE -> update(event.tableDef(), event.values());
            case DELETE -> delete(event.tableDef(), event.values());
        };
    }

    /** 责任:TRUNCATE 语句(源端一条多表 TRUNCATE 按表到达多条事件,逐表一条语句)。 */
    private static String truncate(TableDef def) {
        return "TRUNCATE TABLE " + qualified(def) + ";\n";
    }

    /** 责任:INSERT 全列语句。边界:values 与 def.columns 长度对齐由上游 EnvelopeParser 保证。 */
    private static String insert(TableDef def, Object[] values) {
        StringJoiner cols = new StringJoiner(", ");
        StringJoiner vals = new StringJoiner(", ");
        for (int i = 0; i < def.columns().size(); i++) {
            cols.add(identifier(def.columns().get(i).name()));
            vals.add(literal(def.columns().get(i).type(), values[i]));
        }
        return "INSERT INTO " + qualified(def) + " (" + cols + ") VALUES (" + vals + ");\n";
    }

    /** 责任:UPDATE 语句(SET after 镜像全列,WHERE 主键)。边界:无主键抛 IAE。 */
    private static String update(TableDef def, Object[] values) {
        if (def.keyColumns().isEmpty()) {
            throw new IllegalArgumentException("UPDATE 渲染需要主键,表 %s 无主键".formatted(def.table()));
        }
        StringJoiner set = new StringJoiner(", ");
        for (int i = 0; i < def.columns().size(); i++) {
            ColumnDef col = def.columns().get(i);
            set.add(identifier(col.name()) + " = " + literal(col.type(), values[i]));
        }
        return "UPDATE " + qualified(def) + " SET " + set + whereKey(def, values) + ";\n";
    }

    /** 责任:DELETE 语句(WHERE 主键)。边界:无主键抛 IAE。 */
    private static String delete(TableDef def, Object[] values) {
        if (def.keyColumns().isEmpty()) {
            throw new IllegalArgumentException("DELETE 渲染需要主键,表 %s 无主键".formatted(def.table()));
        }
        return "DELETE FROM " + qualified(def) + whereKey(def, values) + ";\n";
    }

    /**
     * 责任:WHERE 主键列 = 镜像值(UPDATE 用 after 主键定位——主键值变更场景是已知限制,
     * 旧主键行残留,与对方仓 sink 同条)。边界:主键列不在镜像列中抛 IAE(TableDef 自相矛盾)。
     */
    private static String whereKey(TableDef def, Object[] values) {
        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < def.columns().size(); i++) {
            String name = def.columns().get(i).name();
            if (def.keyColumns().contains(name)) {
                conditions.add(identifier(name) + " = " + literal(def.columns().get(i).type(), values[i]));
            }
        }
        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("主键列不在镜像列中,无法构造 WHERE(表 %s)".formatted(def.table()));
        }
        return " WHERE " + String.join(" AND ", conditions);
    }

    /** 责任:带引号的限定表名 "schema"."table"(保留大小写原样,内部引号翻倍)。 */
    private static String qualified(TableDef def) {
        return identifier(def.schema()) + "." + identifier(def.table());
    }

    /** 责任:标识符加引号——内部 {@code "} 翻倍。 */
    private static String identifier(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    /**
     * 责任:TypeCode 驱动的字面量渲染。关键步骤:null → NULL;INTERVAL 先于 String 通用分派
     * 拦截(载荷虽是字符串但需分解渲染);String 加单引号内部翻倍;其余按类型出裸字面量。
     * 边界:字符串含 NUL、非字符串载荷的类型错位均抛 IAE。
     */
    private static String literal(TypeCode type, Object value) {
        if (value == null) {
            return "NULL";
        }
        // INTERVAL 载荷虽是字符串,但需分解渲染("<n> microseconds" 直写服务端拒收),须在 String 通用分派之前拦截
        if (type == TypeCode.INTERVAL) {
            if (!(value instanceof String s)) {
                throw new IllegalArgumentException("该类型应以字符串载荷到达: " + type);
            }
            return intervalLiteral(s);
        }
        if (value instanceof String s) {
            if (s.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("字符串含 NUL 字符,无法渲染为 SQL 文本");
            }
            return "'" + s.replace("'", "''") + "'";
        }
        return switch (type) {
            case BOOL -> (Boolean) value ? "TRUE" : "FALSE";
            case INT -> value.toString(); // EnvelopeParser 已把整型归一为 Long
            case FLOAT32, FLOAT64 -> {
                double d = ((Number) value).doubleValue();
                // NaN/Infinity 非合法裸字面量,PG 系接受带引号形态再按目标列转换
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    yield "'" + value + "'";
                }
                yield value.toString();
            }
            // VastBase 的 bytea 解析器不认 '\x' 十六进制前缀格式(按字面文本存储,数据损坏),
            // decode() 双端可用且返回 bytea 无需 cast
            case BYTES -> "decode('" + HexFormat.of().formatHex((byte[]) value) + "','hex')";
            // STRING 与时间五种必须以 String 载荷到达(上方 instanceof 已分派),否则属上游错位
            default -> throw new IllegalArgumentException("该类型应以字符串载荷到达: " + type);
        };
    }

    /**
     * 责任:INTERVAL 落地串 {@code "<n> microseconds"} → 总时长无损的分解形态(如
     * {@code '429 days 07:05:06.000000'};负值天与时分秒两段都带符号)。
     * 边界:非微秒形态(后缀不符/数字段不合法)抛 IAE。年月结构不可还原(IR 只保留总微秒)。
     */
    private static String intervalLiteral(String s) {
        String suffix = " microseconds";
        if (!s.endsWith(suffix)) {
            throw new IllegalArgumentException("INTERVAL 载荷非微秒形态,无法渲染: " + s);
        }
        long micros;
        try {
            micros = Long.parseLong(s.substring(0, s.length() - suffix.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("INTERVAL 载荷非微秒形态,无法渲染: " + s, e);
        }
        boolean neg = micros < 0;
        long abs = Math.absExact(micros);
        long days = abs / 86_400_000_000L;
        long rem = abs % 86_400_000_000L;
        String sign = neg ? "-" : "";
        return "'" + sign + days + " days " + sign
                + "%02d:%02d:%02d.%06d".formatted(rem / 3_600_000_000L, (rem / 60_000_000L) % 60,
                        (rem / 1_000_000L) % 60, rem % 1_000_000L) + "'";
    }
}
```

- [ ] **Step 4: 跑测试确认绿**

Run: `mvn test -pl vb-stream-sql-format -am`
Expected: PASS(11 用例)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(sql-format): SQL 文本落地格式模块——SqlRenderer 移植(裁 renderDdl)11 用例"
```

---

### Task 3: vb-stream-binary-format 模块(纯移包迁移)

**Files:**
- Create: `vb-stream-binary-format/pom.xml`
- Move(git mv): 基座 8 个记录/读写类 → `vb-stream-binary-format/src/main/java/org/vastdata/vbstream/format/binary/`:`Record`、`BeginRecord`、`CommitRecord`、`EventRecord`、`TableDefRecord`、`TruncateRecord`、`ChangeFileWriter`、`ChangeFileReader`
- Move(git mv): `Varint`、`Values` → `vb-stream-binary-format/src/main/java/org/vastdata/vbstream/format/binary/io/`
- Create: `vb-stream-binary-format/src/main/java/org/vastdata/vbstream/format/binary/package-info.java`
- Move(git mv): `VarintTest`、`ChangeFileIOTest` → `vb-stream-binary-format/src/test/java/org/vastdata/vbstream/format/binary/io/`(VarintTest)、`.../binary/`(ChangeFileIOTest)
- Modify: `pom.xml`(根聚合加模块行)、`vb-stream-reader/pom.xml`(依赖替换)、reader 侧 import(`VbfgEventWriter`、`ReaderFileSinkIT`、`ConnectTestRecords` 等使用点)、基座 `package-info.java`(职责缩小)
- Create: `vb-stream-binary-format/CLAUDE.md`(内容见 Task 7,本任务先建最小骨架占位可省——文档统一 Task 7 写,此处不建)

**Interfaces:**
- Consumes: 基座 `TableDef`/`Op`/`TypeCode`(记录类与读写的签名引用)。
- Produces: `org.vastdata.vbstream.format.binary.ChangeFileWriter`(构造 `(Path file, int seq, String task)`、`writeBegin/writeEvent/writeTruncate/writeCommit/finish/close/recordCount`)、`org.vastdata.vbstream.format.binary.ChangeFileReader`(`open(Path)`、`Iterator<Record>`、`complete()/seq()/sourceDb()`)、记录类(包内可见性不变,`Record` 等是 public)。Task 5 的 `VbfgEventWriter` 与 Task 6 的 IT 经新包名引用。**所有类内容零改动,仅 package 声明与 import 变化。**

- [ ] **Step 1: 建模块 pom + git mv**

根 `pom.xml` `<modules>` 在 `vb-stream-file-format` 后插 `<module>vb-stream-binary-format</module>`。新建 `vb-stream-binary-format/pom.xml`(依赖:基座 + junit,定位注释同 sql 模块风格——"VBFG 二进制落地格式:记录模型 + 二进制流式读写,移植自 cdc-binary-file-format 2026-09-16 快照;字节布局与跨仓同步契约见模块 CLAUDE.md;零第三方依赖"):

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

    <artifactId>vb-stream-binary-format</artifactId>

    <!-- 模块定位：VBFG 二进制落地格式——记录模型 + 二进制流式读写 + LEB128 编解码，移植自
         vb-cdc-file-transform 的 cdc-binary-file-format（2026-09-16 快照；本项目 2026-09-11
         移植的是其前身 cdc-file-format 全量，本次拆分对齐对方仓模块边界）。字节布局跨仓契约：
         与 vb-cdc-file-transform 逐字节互通，不兼容变更须两仓同步递增 ChangeFileWriter.VERSION。 -->
    <dependencies>
        <dependency>
            <groupId>org.vastdata</groupId>
            <artifactId>vb-stream-file-format</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit-jupiter.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

git mv 全部 10 个主类 + 2 个测试类到新模块路径,然后逐文件改 package 声明:
- 记录/读写 8 类:`package org.vastdata.vbstream.format.binary;` + 补基座 import(`org.vastdata.vbstream.format.TableDef/Op/TypeCode`,record 家族中实际引用者)+ `ChangeFileWriter`/`ChangeFileReader` 补 `import org.vastdata.vbstream.format.binary.io.Varint;`(Reader 另有 `io.Values`,按实际引用补)
- `Varint`/`Values`:`package org.vastdata.vbstream.format.binary.io;` + `Values` 补基座 `TypeCode` import
- 测试 2 类:package 对应调整 + import 按新包名(**断言与方法体零改动**)
- 基座 `package-info.java`:包描述改为"格式层公共基座:文件命名契约 + 事件 IR(表结构/操作/类型/ParsedEvent),供 binary/sql 格式模块与 reader 落地链路复用"
- 新建 `vb-stream-binary-format/src/main/java/org/vastdata/vbstream/format/binary/package-info.java`(一句话:VBFG 二进制记录模型与流式读写,布局见 `ChangeFileWriter`)

- [ ] **Step 2: 改 reader 依赖与 import**

`vb-stream-reader/pom.xml`:依赖 `vb-stream-file-format` 替换为:

```xml
        <!-- VBFG 二进制格式:file 输出形态 binary 格式的读写(VbfgEventWriter 与
             ReaderFileOutputIT 经此传递依赖;基座 IR 经其传递) -->
        <dependency>
            <groupId>org.vastdata</groupId>
            <artifactId>vb-stream-binary-format</artifactId>
            <version>${project.version}</version>
        </dependency>
```

reader 全模块 grep `org.vastdata.vbstream.format.(Record|BeginRecord|CommitRecord|EventRecord|TableDefRecord|TruncateRecord|ChangeFileWriter|ChangeFileReader|Varint|Values)` 改新包名——已知点:`VbfgEventWriter`(ChangeFileWriter)、`ReaderFileSinkIT`(11 个 import)。`EnvelopeParser`/`VbfgEventWriter` 引用的 `TableDef/Op/TypeCode/ParsedEvent` 留在基座,**import 不动**。

- [ ] **Step 3: 全量编译 + 测试**

Run: `mvn test -pl vb-stream-file-format,vb-stream-binary-format,vb-stream-sql-format,vb-stream-reader -am`
Expected: PASS(file-format 剩 FileNamingTest;binary 12 用例断言零改动全绿;sql 11;reader 全绿——含 FileRollingWriterTest/FileChangeConsumerTest/IT 需 Docker,本机有)

- [ ] **Step 4: 验证既有 .bin 字节形态未变(契约锚定)**

`ChangeFileIOTest` 已含全记录类型 RoundTrip 与已知字节形态断言(VarintTest 有已知字节形态用例)——Step 3 全绿即为字节形态不变的证明,无需额外产物。在 commit 信息里点名"12 用例断言零改动"。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(binary-format): VBFG 读写独立成模块——10 类纯移包,12 用例断言零改动"
```

---

### Task 4: reader 配置迁移(SinkConfig→OutputConfig,sink.*→reader.*)

**Files:**
- Create: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/OutputConfig.java`(内容 = SinkConfig 改名与键替换)
- Delete: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/SinkConfig.java`
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/ReaderProperties.java`(前缀常量 + resolveSink→resolveOutput)
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/Main.java`(装配点与 javadoc)
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/FileChangeConsumer.java`(构造参数类型与 javadoc)
- Modify: `vb-stream-reader/src/main/resources/dbconfig.properties`(补可选注释区)
- Move(git mv)+ Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/it/ReaderFileSinkIT.java` → `ReaderFileOutputIT.java`(类名 + SinkConfig 引用)
- Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/ReaderPropertiesTest.java`(键名迁移)

**Interfaces:**
- Consumes: 无新依赖。
- Produces: `public record OutputConfig(Mode mode, Path dataDir, Path tmpDir, String task, int rollMaxRecords, long rollIntervalMs)`、`public enum OutputConfig.Mode {LOG, FILE}`、`static OutputConfig OutputConfig.from(Map<String, String> readerProps, Properties debeziumProps)`、`ReaderProperties.resolveOutput(Properties)`、常量 `ReaderProperties.READER_FILE_PREFIX = "reader."` / `READER_SYS_PREFIX = "vb.reader."`。Task 5 在 `OutputConfig` 上加 `format` 组件(构造调用点届时同步)。

- [ ] **Step 1: 改造 ReaderPropertiesTest(先行,测试即迁移规格)**

`ReaderPropertiesTest.java` 逐点修改:
- `@AfterEach clearSystemProps`:`vb.sink.mode`/`vb.sink.data-dir` → `vb.reader.mode`/`vb.reader.data-dir`;
- `sinkDefaultsToLogWithTaskFromTopicPrefix` → 更名 `readerDefaultsToLogWithTaskFromTopicPrefix`,内部:`System.setProperty("vb.reader.mode", "file")`、`vb.reader.data-dir`;断言 `props` 无 `reader.` 前缀键;`OutputConfig out = ReaderProperties.resolveOutput(props)`;`assertEquals(OutputConfig.Mode.FILE, out.mode(), ...)` 等(注释文案 sink→输出形态);
- `externalFileSinkKeysMergedWithSystemOverride` → 更名 `externalFileReaderKeysMergedWithSystemOverride`:外部文件内容 `reader.mode=file`/`reader.data-dir=file-dir`/`reader.task=file-task`;`-Dvb.reader.data-dir=sys-dir` 覆盖断言;非法值用例 `vb.reader.mode=bogus`,断言异常文案含 `vb.reader.mode`。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-reader -Dtest=ReaderPropertiesTest`
Expected: FAIL(编译错——`OutputConfig`/`resolveOutput` 不存在)

- [ ] **Step 3: 实施改名迁移**

1. `SinkConfig.java` → `OutputConfig.java`(git mv 后改):类名、javadoc(键例 `vb.sink.mode` → `vb.reader.mode`;说明命名空间避开 engine 的 `vb.output.*`)、`from` 内键前缀全部 `sink.` → `reader.`(`reader.mode`/`reader.task`/`reader.data-dir`/`reader.tmp-dir`/`reader.roll.max-records`/`reader.roll.interval-ms`),非法 mode 报错文案 `"未知的 vb.reader.mode: ...(可选 log|file)"`;
2. `ReaderProperties.java`:`SINK_FILE_PREFIX`/`SINK_SYS_PREFIX` 常量与 javadoc → `READER_FILE_PREFIX = "reader."` / `READER_SYS_PREFIX = "vb.reader."`(javadoc 注明"reader 自用输出形态命名空间,不透传 Debezium");`resolve()` 内两个前缀判断换新常量;`resolveSink` → `resolveOutput`(局部变量 sink → output,javadoc 同步);
3. `Main.java`:`resolveSink` 调用 → `resolveOutput`;`SinkConfig` 类型 → `OutputConfig`;file 形态 INFO 行与 javadoc 文案同步;
4. `FileChangeConsumer.java`:构造参数 `SinkConfig config` → `OutputConfig config`,import 与 javadoc 同步;
5. `ReaderFileSinkIT.java` → `ReaderFileOutputIT.java`(git mv + 类名 + `SinkConfig` import/引用 → `OutputConfig`);
6. 全模块 grep `-i sink`(java + properties)确认零残留(注释/CDC logger 名 `org.vastdata.vbstream.reader.cdc` 不含 sink,不动);
7. `dbconfig.properties` 末尾可选区后追加:

```properties
# ---- 输出形态(reader 自用命名空间,不透传 Debezium;默认 log)----
# reader.mode=log|file          # file=CDC 记录落地为文件(tmp→fsync→原子 rename,COMMIT 边界切分)
# reader.data-dir=data/cdc-files
# reader.tmp-dir=data/cdc-tmp
# reader.roll.max-records=1000
# reader.roll.interval-ms=10000
```

(`reader.format` 行 Task 5 补)

- [ ] **Step 4: 全量测试**

Run: `mvn test -pl vb-stream-reader`
Expected: PASS(ReaderPropertiesTest 迁移后用例 + 全部既有;IT 更名后照常)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(reader): 输出配置 sink.* 迁移 reader.*——SinkConfig 更名 OutputConfig,模板补配置可见性"
```

---

### Task 5: reader 格式接线(OutputFormat 工厂 + SqlEventWriter)

**Files:**
- Create: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/OutputFormat.java`
- Create: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/SqlEventWriter.java`
- Create: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/file/SqlEventWriterTest.java`
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/FileRollingWriter.java`(格式注入)
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/FileChangeConsumer.java`(透传 format)
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/OutputConfig.java`(加 format 组件)
- Modify: `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/Main.java`(INFO 行加 format)
- Modify: `vb-stream-reader/pom.xml`(加 vb-stream-sql-format 依赖)
- Modify: `vb-stream-reader/src/main/resources/dbconfig.properties`(补 reader.format 注释行)
- Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/file/FileRollingWriterTest.java`(格式分派用例)
- Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/ReaderPropertiesTest.java`(format 解析断言)
- Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/it/ReaderFileOutputIT.java`(构造调用加 format 实参)

**Interfaces:**
- Consumes: `SqlRenderer.render(ParsedEvent)`(Task 2)、`EventFileWriter` 接口(现存)、`EnvelopeParser.parse/isTransactionMetadata`(现存)、`OutputConfig`(Task 4)。
- Produces: `public enum OutputFormat {BINARY, SQL}`——`public String extension()`(`bin`/`sql`)、`public static OutputFormat parse(String value)`(非法抛 IAE,文案含 `reader.format` 与"可选 binary, sql")、包私有 `EventFileWriter newWriter(Path file, int seq, String task)`;`final class SqlEventWriter implements EventFileWriter`(构造 `(Path file, int seq, String task)`);`OutputConfig` 新组件 `OutputFormat format`(构造器参数序:`mode, format, dataDir, tmpDir, task, rollMaxRecords, rollIntervalMs`);`FileRollingWriter` 构造新参序 `(String task, Path dataDir, Path tmpDir, int rollMaxRecords, long rollIntervalMs, OutputFormat format, Clock clock)`。

- [ ] **Step 1: 写 SqlEventWriterTest 与 FileRollingWriterTest 扩展(失败)**

新建 `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/file/SqlEventWriterTest.java`:

```java
package org.vastdata.vbstream.reader.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.dataRecord;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.marker;

/**
 * {@link SqlEventWriter} 的文本形态单测(零 PG):头注释/事务边界裸 BEGIN;COMMIT;/数据事件
 * 渲染语句,逐行拼出与手写期望一致的文件内容。记录手造见 {@link ConnectTestRecords};
 * 事务边界记录的 raw 载荷 SQL 写入器不消费(只取 marker 字段),传 null 即可。
 */
class SqlEventWriterTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /**
     * 场景:完整事务写出的文件 = 头注释 + BEGIN; + INSERT 语句 + COMMIT;——SqlRenderer
     * 渲染 ParsedEvent 的端到端文本面(值字面量规则由 SqlRendererTest 锚定,此处只验管线)。
     *
     * @throws IOException 文件读写失败上抛
     */
    @Test
    void writesHeaderBeginStatementAndCommit() throws IOException {
        Path file = dir.resolve("out.sql");
        try (SqlEventWriter w = new SqlEventWriter(file, 7, "tk")) {
            w.writeBegin(marker(true, "5"), null);
            w.writeEvent(dataRecord(1, "hello"));
            w.writeCommit(marker(false, "5"), null);
            w.finish();
            assertEquals(1, w.recordCount(), "recordCount 记数据事件数(不含 BEGIN/COMMIT)");
        }
        String content = Files.readString(file);
        assertEquals("-- vb-stream task=tk seq=0000000000000007\n"
                + "BEGIN;\n"
                + "INSERT INTO \"public\".\"t\" (\"id\", \"payload\") VALUES (1, 'hello');\n"
                + "COMMIT;\n", content);
    }
}
```

注:期望行按 `ConnectTestRecords.dataRecord` 的既有 schema(表 `public.t`,列 `id` INT32→TypeCode.INT / `payload` STRING,id 值经 EnvelopeParser 归一 Long→`1`)——若执行时该辅助类 schema 与此不符,以现状修正期望行,但必须保持完整语句行文本断言。

`FileRollingWriterTest.java` 追加两用例(其余既有用例的 `new FileRollingWriter(...)` 调用处按新参序补 `OutputFormat.BINARY` 实参——插在 rollIntervalMs 与 clock 之间):

```java
    /**
     * 场景:格式分派——SQL 格式的 publish 产物后缀 .sql、文件名前段命名规则与 binary 相同
     * (字典序 = 消费顺序与格式无关)。
     */
    @Test
    void sqlFormatPublishesSqlSuffixFile() throws IOException {
        try (FileRollingWriter w = new FileRollingWriter("tk", dir.resolve("data"), dir.resolve("tmp"),
                1, 60_000L, OutputFormat.SQL, Clock.systemUTC())) {
            w.onBegin(marker(true, "1"), null);
            w.onEvent(dataRecord(1, "v"));
            Optional<Path> published = w.onCommit(marker(false, "1"), null);
            assertTrue(published.isPresent(), "roll.max-records=1 首 COMMIT 即切分");
            assertTrue(published.get().getFileName().toString().matches("tk-\\d{16}-\\d{14}\\.sql"),
                    "SQL 格式文件后缀 .sql: " + published.get().getFileName());
            assertTrue(Files.exists(published.get()), "已 publish 文件落在数据目录");
        }
    }

    /**
     * 场景:SQL 格式文件内容形态——头注释 + 裸 BEGIN;/COMMIT; 包住语句(publish 后读回)。
     */
    @Test
    void sqlFormatFileContainsBeginStatementCommit() throws IOException {
        try (FileRollingWriter w = new FileRollingWriter("tk", dir.resolve("data"), dir.resolve("tmp"),
                1, 60_000L, OutputFormat.SQL, Clock.systemUTC())) {
            w.onBegin(marker(true, "1"), null);
            w.onEvent(dataRecord(1, "v"));
            w.onCommit(marker(false, "1"), null);
        }
        try (DirectoryStream<Path> ls = Files.newDirectoryStream(dir.resolve("data"), "*.sql")) {
            Path published = ls.iterator().next();
            String content = Files.readString(published);
            assertTrue(content.startsWith("-- vb-stream task=tk seq="), "头注释带 task 与 seq: " + content);
            assertTrue(content.contains("BEGIN;\n"), "事务边界裸 BEGIN;");
            assertTrue(content.contains("COMMIT;\n"), "事务边界裸 COMMIT;");
        }
    }
```

`ReaderPropertiesTest` 的 `readerDefaultsToLogWithTaskFromTopicPrefix` 追加断言(import `org.vastdata.vbstream.reader.file.OutputFormat`):

```java
        assertEquals(OutputFormat.BINARY, out.format(), "reader.format 缺省 binary");
```

新增独立用例 `invalidReaderFormatFailsFast`(放在 `externalFileReaderKeysMergedWithSystemOverride` 之后;`-Dvb.reader.format` 值在用例尾自清,不进 `@AfterEach` 清单):

```java
    /**
     * 场景:非法 reader.format 启动期 fail-fast 抛 IAE,报错文案指向配置键(可选 binary/sql)。
     */
    @Test
    void invalidReaderFormatFailsFast() {
        System.setProperty("vb.reader.format", "bogus");
        try {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ReaderProperties.resolveOutput(ReaderProperties.resolve()));
            assertTrue(e.getMessage().contains("reader.format"),
                    "非法 format 报错指向配置键: " + e.getMessage());
        }
        finally {
            System.clearProperty("vb.reader.format");
        }
    }
```

`ReaderFileOutputIT` 的 `new OutputConfig(OutputConfig.Mode.FILE, dataDir, tmpDir, "readerit", 1, 60_000L)` → 插参 `OutputFormat.BINARY`:`new OutputConfig(OutputConfig.Mode.FILE, OutputFormat.BINARY, dataDir, tmpDir, "readerit", 1, 60_000L)`(import `org.vastdata.vbstream.reader.file.OutputFormat`)。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-reader -Dtest='SqlEventWriterTest,FileRollingWriterTest,ReaderPropertiesTest'`
Expected: FAIL(编译错——`OutputFormat`/`SqlEventWriter` 不存在、`OutputConfig` 构造参序不符)

- [ ] **Step 3: 实现 OutputFormat + SqlEventWriter + 接线**

新建 `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/OutputFormat.java`:

```java
package org.vastdata.vbstream.reader.file;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * file 输出形态的落地格式:格式枚举即工厂——每值携带文件扩展名与写入器构造,滚动/发布/offset
 * 联动收在格式无关的 {@link FileRollingWriter} 外壳。移植自 vb-cdc-file-transform 仓
 * cdc-capture 的 OutputFormat(2026-09-16 快照;json 不实现,见 spec §8——接口保留扩展点)。
 *
 * <p>扩展名即消费分派依据(文件名字典序 = 消费顺序与格式无关);binary 与 vb-cdc-file-transform
 * 的 cdc-sink 逐字节互通,sql 为可直接执行的语句文本。
 */
public enum OutputFormat {

    /** VBFG 二进制格式(默认)——与对方仓 cdc-sink 消费端互通。 */
    BINARY("bin"),

    /** SQL 文本格式——IR 渲染的可执行语句(裸重放语义,主键冲突由执行方处理)。 */
    SQL("sql");

    private final String extension;

    OutputFormat(String extension) {
        this.extension = extension;
    }

    /** 落地文件的扩展名(文件名契约见 {@link org.vastdata.vbstream.format.FileNaming})。 */
    public String extension() {
        return extension;
    }

    /**
     * 责任:按格式建事件写入器(tmp .part 文件的实现细节由各格式自理)。
     * 边界:构造 IO 失败原样上抛 IOException(调用方 FileRollingWriter 停引擎,offset 不动)。
     */
    EventFileWriter newWriter(Path file, int seq, String task) throws IOException {
        return switch (this) {
            case BINARY -> new VbfgEventWriter(file, seq, task);
            case SQL -> new SqlEventWriter(file, seq, task);
        };
    }

    /**
     * 责任:配置值 → 格式(大小写宽容)。边界:非法值抛 IAE(启动期 fail-fast,报错文案
     * 指向 {@code reader.format} 键与可选值)。
     */
    public static OutputFormat parse(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "binary" -> BINARY;
            case "sql" -> SQL;
            default -> throw new IllegalArgumentException(
                    "未知的 reader.format: " + value + "(可选 binary, sql)");
        };
    }
}
```

新建 `vb-stream-reader/src/main/java/org/vastdata/vbstream/reader/file/SqlEventWriter.java`:

```java
package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.vastdata.vbstream.format.ParsedEvent;
import org.vastdata.vbstream.format.sql.SqlRenderer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * SQL 文本格式的事件写入器:envelope → IR → {@link SqlRenderer} 渲染语句逐行写文件。
 * 事务边界渲染为裸 {@code BEGIN;}/{@code COMMIT;};文件头一行 {@code --} 注释携带 task/seq
 * (lsn/txid 不落文件)。无 FOOTER/CRC——完整性靠 tmp .part 暂存 + 原子 rename 的 publish
 * 机制(与 binary 同一外壳,约定"数据目录里无 .part 后缀即完整文件")。
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 SqlEventWriter(2026-09-16 快照)。
 */
final class SqlEventWriter implements EventFileWriter {

    private final FileOutputStream fos;
    private final OutputStream out;
    private long events;

    /**
     * 责任:打开目标文件并写头注释行(task/seq 进注释——执行侧忽略,消费侧可溯源)。
     * 边界:文件已存在则从头部覆写(调用方约定写 tmp 目录的全新文件)。
     */
    SqlEventWriter(Path file, int seq, String task) throws IOException {
        this.fos = new FileOutputStream(file.toFile());
        this.out = new BufferedOutputStream(fos);
        out.write(("-- vb-stream task=%s seq=%016d\n".formatted(task, seq)).getBytes());
    }

    /** 责任:写裸 BEGIN;(事务外壳语句——多事务共文件时逐事务包裹)。 */
    @Override
    public void writeBegin(TransactionMarker marker, SourceRecord raw) throws IOException {
        out.write("BEGIN;\n".getBytes());
    }

    /**
     * 责任:渲染一条数据事件为语句行。边界:渲染失败(无主键 u/d、载荷形态非法等)抛
     * IllegalArgumentException → 调用方停引擎、offset 不推进、重启重放(不丢数据契约)。
     */
    @Override
    public void writeEvent(SourceRecord record) throws IOException {
        ParsedEvent event = EnvelopeParser.parse(record);
        out.write(SqlRenderer.render(event).getBytes());
        events++;
    }

    /** 责任:写裸 COMMIT;。 */
    @Override
    public void writeCommit(TransactionMarker marker, SourceRecord raw) throws IOException {
        out.write("COMMIT;\n".getBytes());
    }

    /** 责任:flush + fsync(publish 前的完整性保证,之后由调用方原子 rename)。幂等:重复调用直接返回。 */
    @Override
    public void finish() throws IOException {
        out.flush();
        fos.getFD().sync();
    }

    /** 已写数据事件数(不含 BEGIN/COMMIT 边界语句——与 binary 的 recordCount 口径差异记档)。 */
    @Override
    public long recordCount() {
        return events;
    }

    /** 责任:关闭底层流(未 finish 的半成品由调用方删除 tmp 文件)。 */
    @Override
    public void close() throws IOException {
        out.close();
    }
}
```

接线修改:
1. `FileRollingWriter`:字段加 `private final OutputFormat format;`,构造参数在 `rollIntervalMs` 后插 `OutputFormat format`(赋值与 javadoc 同步);`writer()` 内 `new VbfgEventWriter(currentTmpPath, (int) currentSeq, task)` → `format.newWriter(currentTmpPath, (int) currentSeq, task)`;`tmpPath`/`publish` 的 `FileNaming.fileName(task, currentSeq, LocalDateTime.now(clock), "bin")` → 末参 `format.extension()`(publish 与 tmpPath 两处);类 javadoc 的"格式工厂收敛为 VBFG 单形态"句删除、改述"格式经 OutputFormat 注入";
2. `FileChangeConsumer`:构造 `new FileRollingWriter(config.task(), config.dataDir(), config.tmpDir(), config.rollMaxRecords(), config.rollIntervalMs(), config.format(), Clock.systemUTC())`;
3. `OutputConfig`:组件加 `OutputFormat format`(参数序 `mode, format, dataDir, tmpDir, task, rollMaxRecords, rollIntervalMs`);`from` 内 `OutputFormat.parse(sinkProps.getOrDefault("reader.format", "binary"))`(变量名 outputProps 按 Task 4 现状);import `org.vastdata.vbstream.reader.file.OutputFormat`;javadoc 补 format 语义;
4. `Main`:file 形态 INFO 行补 `format={}` 与 `sink.format()`→`out.format()` 实参;javadoc 的形态列表补 format 一句;
5. `vb-stream-reader/pom.xml` 加依赖(binary 依赖块后):

```xml
        <!-- SQL 文本格式:file 输出形态 sql 格式的语句渲染(SqlEventWriter 经此依赖) -->
        <dependency>
            <groupId>org.vastdata</groupId>
            <artifactId>vb-stream-sql-format</artifactId>
            <version>${project.version}</version>
        </dependency>
```

6. `dbconfig.properties` 的输出形态注释区插一行:`# reader.format=binary|sql      # file 形态的落地格式(binary=VBFG 二进制互通格式,默认;sql=可执行 SQL 文本)`。

- [ ] **Step 4: 全量测试**

Run: `mvn test -pl vb-stream-reader`
Expected: PASS(SqlEventWriterTest 1 + FileRollingWriterTest 5 + ReaderPropertiesTest 8 + 既有全绿,IT 含)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(reader): file 输出形态新增 sql 落地格式——OutputFormat 枚举工厂 + SqlEventWriter"
```

---

### Task 6: IT sql 场景(JDBC 执行回查)

**Files:**
- Modify: `vb-stream-reader/src/test/java/org/vastdata/vbstream/reader/it/ReaderFileOutputIT.java`(加场景方法 + JDBC 辅助)

**Interfaces:**
- Consumes: `OutputFormat.SQL`、`FileChangeConsumer`、`EngineLifecycle`、`ReaderPgEnv`(Task 4/5)。
- Produces: 无(测试收口)。

- [ ] **Step 1: 写失败场景(场景本身就是验收——"产物可执行")**

`ReaderFileOutputIT` 追加(类 javadoc 场景清单同步补一句):

```java
    /**
     * 场景 sql 格式执行回查:六类型列建表 → 起引擎(SQL 格式,roll.max-records=1)→ INSERT 一行
     * → .sql 文件落地 → 停机 → <b>同一容器另建镜像表并逐文件执行落地 SQL</b> → SELECT 断言
     * 值往返。验收 sql 格式的存在意义:产物无需专用消费端即可在 PG 上重放。
     *
     * @throws Exception 环境异常(容器/SQL)或断言超时原样上抛
     */
    @Test
    void sqlFormatFileExecutesBackWithValuesRoundTrip() throws Exception {
        String table = "t_reader_file_sql";
        String mirror = "t_reader_file_sql_exec";
        slotName = "reader_file_sql_slot";
        ReaderPgEnv.execSql(
                "DROP TABLE IF EXISTS " + table,
                "CREATE TABLE " + table + "(id INT PRIMARY KEY, payload TEXT, flag BOOLEAN,"
                        + " score DOUBLE PRECISION, created DATE, ts TIMESTAMP)",
                "DROP PUBLICATION IF EXISTS pub_reader_file_sql",
                "CREATE PUBLICATION pub_reader_file_sql FOR TABLE " + table);

        Path dataDir = Files.createTempDirectory(tempDir, "cdc-sql");
        Path tmpDir = Files.createTempDirectory(tempDir, "cdc-sql-tmp");
        Path offsetFile = Files.createTempFile(tempDir, "offsets-sql", ".dat");
        try (FileChangeConsumer consumer = new FileChangeConsumer(new OutputConfig(OutputConfig.Mode.FILE,
                OutputFormat.SQL, dataDir, tmpDir, "readeritsql", 1, 60_000L))) {
            EngineLifecycle lifecycle = EngineLifecycle.start(
                    baseProps(slotName, "pub_reader_file_sql",
                            Files.createTempDirectory(tempDir, "pipe"), offsetFile),
                    consumer);
            try {
                ReaderPgEnv.awaitWalsender(slotName, 30_000);
                ReaderPgEnv.execSql("INSERT INTO " + table + " VALUES (1, 'sqlhello', true, 4.5,"
                        + " '2026-09-16', '2026-09-16 11:22:33.654321')");
                awaitPublishedSqlFile(dataDir, 30_000);
            }
            finally {
                assertFalse(lifecycle.shutdown(60_000), "停机非失败");
            }
        }

        // 同容器建镜像表(结构同源表),整文件内容一次 execute——头注释与 BEGIN;/COMMIT; 由服务端处理
        ReaderPgEnv.execSql("DROP TABLE IF EXISTS " + mirror,
                "CREATE TABLE " + mirror + " (LIKE " + table + " INCLUDING ALL)");
        List<Path> files = sqlFiles(dataDir);
        assertEquals(1, files.size(), "首事务即切分恰发布一个 .sql 文件");
        String script = Files.readString(files.get(0));
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl(), "postgres", "postgres");
             java.sql.Statement st = conn.createStatement()) {
            st.execute(script);
        }
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl(), "postgres", "postgres");
             java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery(
                     "SELECT id, payload, flag, score, created, ts FROM " + mirror)) {
            assertTrue(rs.next(), "执行回查恰一行");
            assertEquals(1, rs.getInt("id"));
            assertEquals("sqlhello", rs.getString("payload"));
            assertTrue(rs.getBoolean("flag"));
            assertEquals(4.5, rs.getDouble("score"), 0.0);
            assertEquals(java.time.LocalDate.of(2026, 9, 16), rs.getDate("created").toLocalDate());
            assertEquals(java.time.LocalDateTime.of(2026, 9, 16, 11, 22, 33, 654_321_000),
                    rs.getTimestamp("ts").toLocalDateTime());
            assertFalse(rs.next(), "无多余行");
        }
    }
```

辅助方法(追在 `binFiles` 后;`awaitPublishedFile`/`binFiles` 的既有形态照抄改后缀):

```java
    /** 轮询等待数据目录出现已发布 .sql 文件(与 awaitPublishedFile 同款超时语义)。 */
    private static void awaitPublishedSqlFile(Path dataDir, long timeoutMillis)
            throws InterruptedException, java.io.IOException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (sqlFiles(dataDir).isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(timeoutMillis + "ms 内无落地文件发布(链路异常)");
            }
            Thread.sleep(100);
        }
    }

    /** 数据目录下已发布 .sql 文件清单。 */
    private static List<Path> sqlFiles(Path dataDir) throws java.io.IOException {
        try (DirectoryStream<Path> ls = Files.newDirectoryStream(dataDir, "*.sql")) {
            List<Path> files = new ArrayList<>();
            for (Path p : ls) {
                files.add(p);
            }
            return files;
        }
    }

    /**
     * 责任:组装直连容器的 JDBC URL(执行回查用——ReaderPgEnv 的 execSql 面向 DDL/DML,
     * 查询结果断言需自持连接取 ResultSet)。
     */
    private static String jdbcUrl() {
        return "jdbc:postgresql://" + ReaderPgEnv.PG.getHost() + ":" + ReaderPgEnv.PG.getMappedPort(
                org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT)
                + "/" + ReaderPgEnv.PG.getDatabaseName();
    }
```

注意三点(执行时先读 `ReaderPgEnv` 校准):
1. **用户名/口令**:示例按 `postgres/postgres` 写——以 `ReaderPgEnv.PG.getUsername()/getPassword()` 实际值为准(`baseProps` 里就是这么取的,照抄);
2. **`import java.sql.*` 简名规约**:上面的 `java.sql.Connection` 全限定是示意——实际代码须 `import java.sql.Connection;` 等后用简名(仓规约禁 FQN 内联;此处唯一命名冲突风险无,直接 import);
3. `PG` 的可见性:`ReaderPgEnv.PG` 在 `ReaderEndToEndIT` 同包可用,确认是包可见/public static 字段,照现状引用。

- [ ] **Step 2: 跑 IT 确认通过**

Run: `mvn test -pl vb-stream-reader -Dtest=ReaderFileOutputIT`
Expected: PASS(两场景:既有 binary 场景 + 新 sql 场景;需 Docker)

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(reader): sql 格式 IT——落地 .sql 在镜像表执行回查,六类型列值往返"
```

---

### Task 7: 文档同步(四份 CLAUDE.md + 根 CLAUDE.md)

**Files:**
- Create: `vb-stream-binary-format/CLAUDE.md`、`vb-stream-sql-format/CLAUDE.md`
- Modify: `vb-stream-file-format/CLAUDE.md`(基座改写)、`vb-stream-reader/CLAUDE.md`、根 `CLAUDE.md`

**Interfaces:**
- Consumes: Task 1-6 的最终形态。
- Produces: 文档(无代码接口)。

- [ ] **Step 1: 写两新模块 CLAUDE.md**

`vb-stream-binary-format/CLAUDE.md` 要点:模块定位(VBFG 二进制格式:记录模型 + 二进制流式读写,写侧 reader file 包、布局与不变量自基座 2026-09-11 移植沿革);常用命令(`mvn test -pl vb-stream-binary-format`,12 用例零 Docker);二进制布局(照抄基座 CLAUDE.md 现有布局段——Magic/六记录类型/FOOTER);关键不变量(FOOTER 不进 CRC、TypeCode/Op 枚举持久化只追加、defId 文件内作用域与 TableDef 去重、Values null 位图、**跨仓同步契约:不兼容变更两仓同步递增 VERSION;已知限制——不识别对方仓 DDL(7) 记录(本项目不做 DDL,读到报未知记录类型;写侧主链路不受影响)**;Reader 流式语义);依赖(基座 + 零第三方)。

`vb-stream-sql-format/CLAUDE.md` 要点:模块定位(SQL 文本落地格式:IR → 可执行语句,PG/VastBase 方言;移植自 cdc-sql-file-format 2026-09-16 快照,**与对方仓分叉点唯一:无 renderDdl**);命令(`mvn test -pl vb-stream-sql-format`,11 用例);渲染规则表(INSERT/UPDATE/DELETE/TRUNCATE + 值字面量表——从 spec §3 抄);两处 VastBase 实测裁定原文(BYTES 用 decode() 不用 '\x'::bytea;INTERVAL 分解渲染不直写 microseconds——服务端拒收);已知限制(无主键 u/d 抛异常、不支持主键值变更、裸重放无幂等、无表名列名映射、lsn/txid 不落文件无 FOOTER/CRC)。

- [ ] **Step 2: 改写基座 CLAUDE.md**

`vb-stream-file-format/CLAUDE.md` 全文改写:标题"格式层公共基座——文件命名契约 + 事件 IR";定位段(FileNaming 三后缀 + TableDef/ColumnDef/Op/TypeCode/ParsedEvent,ParsedEvent 2026-09-16 自 reader 下沉;binary/sql 两格式模块依赖本基座);二进制布局段**移除**(指向 vb-stream-binary-format);枚举持久化警告保留(TypeCode/Op 的 id 持久化语义跨模块——动枚举前读 binary 模块文档);FileNaming 不变量段保留 + 三后缀;零依赖声明保留;测试段(FileNamingTest)。历史沿革一句:2026-09-11 全量移植 → 2026-09-16 拆分三模块对齐对方仓边界。

- [ ] **Step 3: 更新 reader 与根 CLAUDE.md**

`vb-stream-reader/CLAUDE.md`:
- `SinkConfig` 段 → `OutputConfig`(键全部 `reader.*`,加 `reader.format=binary|sql` 项说明与默认值;命名空间说明补"避开 engine 的 vb.output.*");
- file 输出形态段:标题改"file 输出形态(落地文件,binary/sql 双格式)";`file` 包组件清单补 `OutputFormat`(枚举工厂)与 `SqlEventWriter`;`vb-stream-file-format 模块` 依赖描述改为 `vb-stream-binary-format + vb-stream-sql-format`(基座传递);"运行 file 形态"补 `-Dvb.reader.format=sql` 示例;
- `ReaderFileSinkIT` 引用全部改 `ReaderFileOutputIT` 并补 sql 场景一句;
- 测试形态段补 SqlEventWriterTest/FileRollingWriterTest 扩展一句。

根 `CLAUDE.md`:
- 项目概述坐标段:"四模块" → "六模块",清单补两新模块一句话定位;
- "源码结构"段:`vb-stream-file-format/src/main/java` 行改述基座(IR+命名);插 `vb-stream-binary-format` 与 `vb-stream-sql-format` 两行(结构同款:路径 + 包 + 一句定位);`vb-stream-reader` 行的 file 形态描述补 binary/sql 双格式与 `vb.reader.*` 键;
- 全文 grep `vb.sink`/`sink.mode`/`SinkConfig`/`ReaderFileSinkIT` 清零换新(含"运行 Main"reader 段);
- "架构总览"表格若有 file-format 行则拆行同步。

- [ ] **Step 4: 终验 + push**

Run: `mvn test`(全模块,需 Docker)
Expected: 全绿(engine 224 + connector 272 + reader 全部 + 三格式模块 12+11+FileNaming)

```bash
git add -A
git commit -m "docs: 三模块拆分与 binary/sql 双格式文档同步——两新模块 CLAUDE.md + 基座改写 + reader/根更新"
git push
```

---

## Self-Review 记录

- **Spec 覆盖**:spec §2(三模块/依赖/FileNaming 边界)→ Task 1/2/3;§3(SqlRenderer)→ Task 2;§4(接线)→ Task 5;§5(配置迁移+模板)→ Task 4/5;§6(测试六层)→ Task 1(FileNamingTest)/2(SqlRendererTest)/3(随迁)/5(FileRollingWriterTest 扩展+ReaderPropertiesTest)/6(IT sql 场景);§7(文档)→ Task 7;§8(裁定记档)→ Task 3/7 的 CLAUDE.md 已知限制段。无缺口。
- **现状勘误(对 spec)**:spec §2 的"四参 fileName 重载"按对方仓写——本仓现状 `FileNaming.fileName` 已是四参(`FileNaming.java:34`,reader 调用点均四参),故 Task 1 的 FileNaming 改动收敛为仅后缀正则扩 `.sql`,方法面零变化(已核对源码)。
- **占位符扫描**:Task 6 的"以 ReaderPgEnv 现状为准校准凭据/PG 可见性"是**有明确校准指令的现状依赖**(非 TBD)——执行者读指定文件后按给定模板落地,断言结构完整;Task 5 期望行已按 `ConnectTestRecords` 真实 schema(`public.t`/`id`+`payload`)写为精确文本。
- **类型一致性**:`OutputFormat.parse` 返回 `OutputFormat`(Task 5 定义,`OutputConfig.from` 与 `ReaderPropertiesTest` 用);`OutputConfig` 构造参序在 Task 4 定为 6 参、Task 5 扩 7 参并同步全部调用点(Main 不直接构造 record——只有 IT 与测试构造,Task 5 Step 1 已列);`SqlEventWriter` 构造 `(Path, int, String)` 与 `OutputFormat.newWriter` 调用一致;`FileRollingWriter` 7 参构造在 Task 5 定义并同步 FileChangeConsumer 与既有测试。
