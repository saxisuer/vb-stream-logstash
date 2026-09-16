# file-format 三模块拆分与 binary/sql 双格式落地设计

- 日期:2026-09-16
- 状态:已与用户逐节确认,待实施
- 上游:vb-cdc-file-transform 仓 2026-09-16 快照(`34160e9` 格式层拆分 + `cddd2bb` SQL 格式
  + `21ce7d5` sink 三格式消费)

## 1. 背景与目标

vb-cdc-file-transform 仓的格式层发生结构性变化:原 `cdc-file-format` 拆为三模块——基座
`cdc-file-format`(文件命名 + 事件 IR)、`cdc-binary-file-format`(VBFG 二进制读写)、
`cdc-sql-file-format`(SQL 文本渲染);capture 侧经 `capture.format` 三格式(binary/json/sql)
落地。本仓吸收该变化:

1. **vb-stream-file-format 拆为同构三模块**(基座 + binary + sql),保持与对方仓结构对应;
2. **新增 SQL 文本落地格式**——reader 的 file 输出形态可选 `.sql` 产物(可直接执行,目标端
   无需 cdc-sink 也能消费);
3. **不做 DDL、不做 json**(用户裁定,见 §8)。

## 2. 模块结构与依赖

```
vb-stream-file-format(基座,坐标不变)          零第三方依赖
  org.vastdata.vbstream.format
    FileNaming        ← 同步对方仓:后缀正则 (bin|json|sql) + 四参 fileName(task,seq,time,ext) 重载
    TableDef / ColumnDef / Op / TypeCode        (原有 IR,不动)
    ParsedEvent       ← 从 reader file 包下沉(包私有→public,内容不变)

vb-stream-binary-format(新)                   依赖基座,零第三方依赖
  org.vastdata.vbstream.format.binary
    Record / BeginRecord / CommitRecord / EventRecord / TableDefRecord / TruncateRecord
    ChangeFileWriter / ChangeFileReader
  org.vastdata.vbstream.format.binary.io
    Varint / Values                              (自基座 format 根迁入 io 子包)

vb-stream-sql-format(新)                      依赖基座,零第三方依赖
  org.vastdata.vbstream.format.sql
    SqlRenderer                                  (对方仓 1:1 移植,裁掉 renderDdl)
```

- 根 pom `<modules>` 加 `vb-stream-binary-format`、`vb-stream-sql-format`;版本与基座同
  (`1.0-SNAPSHOT`,随 parent)。
- `vb-stream-reader` 依赖从 `vb-stream-file-format` 换为 `vb-stream-binary-format` +
  `vb-stream-sql-format`(基座经传递)。

### FileNaming 同步边界

只同步两处:后缀正则扩 `.sql`(json 一并认,与对方仓一致)、四参 `fileName` 重载(ext 参数,
旧三参形态默认 `"bin"` 保留)。**不引入**对方仓新加的 `parseSeqOpt`/严格版 `parseSeq`
拆层(其使用方在 cdc-sink,本仓无 sink 组件,`nextSeq` 的宽容解析保持现状)。

### binary-format 迁移原则

- **字节布局零变化**:不进 DDL(7),VERSION 不动,现有 `.bin` 产物与读回完全兼容,跨仓
  互通契约不破坏;
- 12 个既有测试(`VarintTest` + `ChangeFileIOTest`)随模块迁移,断言零改动——移植正确性
  的回归锚定。

## 3. sql-format:SqlRenderer 移植

纯函数 1:1 移植(对方仓 `SqlRenderer`,IR → PG 方言可执行 SQL 文本):

| 事件 | 渲染 |
|---|---|
| INSERT(op=c/r) | `INSERT INTO "s"."t" ("c1",…,"cn") VALUES (v1,…,vn);` 全列 |
| UPDATE | `UPDATE "s"."t" SET "c1"=v1,… WHERE "pk"=vpk AND …;`(SET after 全列,WHERE 主键) |
| DELETE | `DELETE FROM "s"."t" WHERE "pk"=vpk AND …;` |
| TRUNCATE | `TRUNCATE TABLE "s"."t";` 逐表一条 |

值字面量(TypeCode 驱动)全套保留:

- BOOL → `TRUE`/`FALSE`;INT → 十进制;FLOAT32/64 → shortest-round-trip,`NaN`/`±Infinity`
  加单引号;
- STRING 与时间五种(TIMESTAMP/TIMESTAMPTZ/DATE/TIME)→ 单引号,内部 `'` 翻倍
  (假定 standard_conforming_strings=on,不用 `E''`);
- INTERVAL → 载荷 `"<n> microseconds"` 解析微秒,分解为 `[-]<days> days [-]HH:MM:SS.ffffff`
  (负值两段都带符号;直写微秒形态 VastBase 服务端拒收——对方仓实测裁定);
- BYTES → `decode('<小写hex>','hex')`(不用 `'\x'::bytea`——VastBase bytea 解析器不认
  `\x` 前缀,对方仓实测裁定);
- null → `NULL`;标识符 `"schema"."table"` 内部 `"` 翻倍、大小写保留。

失败语义:无主键表的 UPDATE/DELETE、INTERVAL 载荷非微秒形态、字符串含 NUL → 抛
`IllegalArgumentException` → 引擎停止、offset 不推进、重启重放(与 binary"不支持的类型
抛异常"同构,不丢数据契约)。

**裁剪**:`renderDdl`/`ParsedDdl` 不移植(DDL 裁定,§8)——与对方仓 SqlRenderer 的唯一
分叉点,模块 CLAUDE.md 记档。

## 4. reader `file` 包接线

数据流不变:`FileChangeConsumer → FileRollingWriter(COMMIT 边界切分 + publish)→
EventFileWriter(格式实现)`——切分/发布/offset 联动逻辑零改动,格式只落在
`EventFileWriter` 的实现与工厂选择上。

- **`OutputFormat` 枚举 = 格式工厂**(照对方仓形态):`BINARY("bin")` / `SQL("sql")` 两值,
  提供 `extension()` 与 `newWriter(Path file, int seq, String task)`;`parse` 大小写宽容,
  非法值 IAE fail-fast(提示"可选 binary, sql")。json 不实现、不提示。
- **`SqlEventWriter`**(新,照对方仓):裸 `FileOutputStream + BufferedOutputStream`;构造写
  头注释 `-- vb-stream task=<task> seq=<%016d>`;`writeBegin`/`writeCommit` 写常量
  `BEGIN;\n` / `COMMIT;\n`;`writeEvent` = `SqlRenderer.render(EnvelopeParser.parse(record))`
  逐语句写行;`finish()` = flush + `getFD().sync()`;`recordCount()` 返回已写事件数。
  **无 FOOTER/CRC,lsn/txid 不落文件**——完整性靠 `.part` 同目录暂存 + 原子 rename 的
  publish 机制(与 binary 同一外壳,约定"数据目录里无 `.part` 后缀即完整文件")。
- **`VbfgEventWriter`**:import 换 `format.binary` 模块,逻辑零改动(不含 DDL 分派)。
- **`EnvelopeParser`**:产出 `ParsedEvent` 改 import 基座(原包私有 record 删除);DDL
  识别/解析(`isDdlRecord`/`parseDdl`)不进。
- **`FileRollingWriter`**:构造注入 `OutputFormat`;`writer()` 惰性建文件改走
  `format.newWriter(...)`;`tmpPath`/`publish` 的文件后缀改 `format.extension()`。

## 5. 配置面

命名空间 **`vb.reader.*`**(无 sink 字样;避开 engine 已占用的 `vb.output.*`——engine 的
`vb.output.mode=streaming|block` 与 reader 的键彻底分开):

```properties
reader.mode=log|file              # 输出目的地,默认 log
reader.format=binary|sql          # file 形态的落地格式,默认 binary
reader.data-dir=data/cdc-files
reader.tmp-dir=data/cdc-tmp
reader.roll.max-records=1000
reader.roll.interval-ms=10000
```

- **现存命名整体迁移**(2026-09-11 引入的 `sink.*` → `reader.*`):类 `SinkConfig` →
  `OutputConfig`(`Mode {LOG, FILE}`),`ReaderProperties.resolveSink` → `resolveOutput`,
  保留键拦截 `sink.*` → `reader.*`;波及 `Main` 装配点、`FileChangeConsumer` 构造参数
  类型、`ReaderPropertiesTest` 相应用例;现存 IT 类 `ReaderFileSinkIT` 一并更名
  `ReaderFileOutputIT`(清掉 sink 字样)。
- **模板补配置可见性**(现存缺口修复):dbconfig.properties 可选注释区补 `reader.*` 六键
  (此前 file 形态的 sink.* 键从未进模板,配置面不可见)。

## 6. 测试策略

| 层 | 测试 | 内容 |
|---|---|---|
| 基座 | `FileNamingTest`(新) | 三后缀命名组装/解析/nextSeq(基座此前无独立测试,拆分补上) |
| binary | `VarintTest` + `ChangeFileIOTest` | 随模块迁移,断言零改动 |
| sql | `SqlRendererTest`(新) | 移植对方仓 12 用例 → 11(去 DDL 用例),方法名改英文 camelCase(仓规约);覆盖:INSERT 全列与各类型字面量/标识符转义与浮点特殊值/UPDATE/DELETE/TRUNCATE/无主键 u+d 抛异常/NUL 抛异常/多行字符串/INTERVAL 负值分解与非微秒形态抛异常/空 bytea 与 ±Infinity |
| reader 单测 | `FileRollingWriterTest` 扩展 | 格式分派:binary/sql 各自的扩展名、文件内容形态(头注释/语句行)、同外壳切分行为 |
| reader 单测 | `ReaderPropertiesTest` 改造 | sink.* 用例迁 reader.*;新增 reader.format 解析(默认/合法/非法 fail-fast) |
| reader IT | `ReaderFileOutputIT`(原 `ReaderFileSinkIT` 更名)加 sql 场景 | 六类型列 INSERT 落地 `.sql` → **JDBC 执行回查**(同容器独立 schema 执行产物再 SELECT 断言值往返——证明产物真可执行;此为测试验收手段,产品代码无执行路径) |

## 7. 文档

- 两新模块各配 CLAUDE.md(binary:布局/不变量/跨仓契约;sql:渲染规则/两处 VastBase 实测
  裁定/已知限制);
- 基座 CLAUDE.md 改写(职责缩小为 IR + 文件命名,指向两格式模块);
- reader CLAUDE.md:file 形态节补 `reader.format` 与 `OutputConfig` 改名、配置键迁移说明;
- 根 CLAUDE.md:模块清单(四模块 → 六模块)、`vb.sink.*` 引用全部换 `vb.reader.*`。

## 8. 范围裁定与已知限制(记档)

1. **DDL 全链路不做**(用户裁定"本项目目前不考虑 DDL 事件"):`DdlRecord`、
   `ChangeFileWriter.writeDdl`/`ChangeFileReader` 的 T_DDL 分支、`ParsedDdl`、
   `SqlRenderer.renderDdl`、`EnvelopeParser.isDdlRecord/parseDdl` 均不进。已知限制:
   binary-format 的 Reader 读到对方仓新产文件中的 DDL(7) 记录会报未知记录类型——本仓
   角色是写侧生产者,主链路(本仓写 → 对方仓 cdc-sink 读)不受影响;未来接入 DDL 时按
   对方仓布局(追加类型、VERSION 不递增)补齐即可。
2. **json 格式不做**(用户裁定):`OutputFormat` 不含 JSON 值;`EventFileWriter` 接口与
   `FileRollingWriter` 外壳保留扩展点,未来需要时按对方仓 `JsonEventWriter`
   (Kafka Connect `JsonConverter` 序列化 envelope 原文)补实现。
3. **sql 产物语义**:裸 SQL 重放——如实反映源端操作序列,重复执行的主键冲突由执行方处理,
   不做 UPSERT/幂等;无主键表的 u/d 落地失败(引擎停止优于静默丢);不支持源端主键值变更
   (UPDATE 用 after 主键定位,旧主键行残留);无表名/列名映射(SQL 在捕获端固化源端原名)。
   与对方仓 sql 格式的已知限制逐条一致。
4. **命名迁移破坏性**:`vb.sink.*` 键与 `SinkConfig` 更名无兼容层(冒烟应用,无存量部署
   负担);IT 与模板同批更新。
