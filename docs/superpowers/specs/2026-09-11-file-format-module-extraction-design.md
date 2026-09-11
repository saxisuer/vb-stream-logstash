# vb-stream-file-format 模块抽取设计

日期：2026-09-11
状态：已批准（brainstorming 对话定稿）

## 1. 背景与动机

2026-09-11 落地的 file 输出形态（commit `6837947`）把 VBFG 落地文件契约层放在了
`vb-stream-reader` 的 `org.vastdata.vbstream.reader.format` 包。该包是**零第三方依赖的纯
契约层**（模型 + 流式读写器 + LEB128 编解码 + 文件命名——与 vb-cdc-file-transform 仓
cdc-file-format 的定位同构），而 reader 是"宿主应用"——契约层寄生在宿主里，未来本仓的
读取/解析侧（file-sink、转换/校验工具）想复用就只能依赖宿主模块（连带 Debezium 全家桶）。

抽取为独立 Maven 模块后，任何需要读/写 VBFG 文件的模块以一个轻依赖（纯 JDK）获得完整
契约，reader 退化为契约层的第一个使用方。

## 2. 目标与非目标

**目标**：
- `format` 契约层成为独立模块 `vb-stream-file-format`（纯 JDK，运行时零依赖）
- reader 的落地链路（`file` 包）不改行为，仅改 import 来源
- 全仓测试全绿（模块数 3 → 4），新模块可独立 `mvn test`（零 Docker）

**非目标**：
- 不抽 `file` 落地链路（EnvelopeParser/FileRollingWriter/FileChangeConsumer——依赖
  Debezium/kafka-connect API，是链路不是契约；当前唯一使用方是 reader，YAGNI）
- 不新增/修改任何公开 API 与字节格式（迁移是零行为变化）
- 不考虑对外部仓发布（定位：本仓内部共享层；外部需求出现时再议）

## 3. 模块结构与依赖方向

```
parent pom <modules> 追加（按依赖层次序插在 engine 之后）：
  vb-stream-engine
  vb-stream-file-format     ← 新增
  vb-stream-connector-postgres-stream
  vb-stream-reader
```

- 坐标：`org.vastdata:vb-stream-file-format:1.0-SNAPSHOT`（packaging=jar，parent 引根 pom）
- 依赖：**运行时零依赖**（纯 JDK）；test 仅 JUnit（照根 pom 的 test 依赖声明形态）
- 依赖方向：`reader → file-format`（compile）；`file-format` 不依赖任何兄弟模块；engine/
  connector 不受影响
- 包名：`org.vastdata.vbstream.reader.format` → `org.vastdata.vbstream.format`（剥掉
  `.reader` 层级——模块名即命名空间）

## 4. 迁移清单

**主代码（16 个文件 = 15 类 + package-info，git mv + 包名/import 批改，代码零行为变化）**：
`Varint`、`Values`、`TypeCode`、`Op`、`ColumnDef`、`TableDef`、`Record`（sealed 接口）、
`TableDefRecord`、`BeginRecord`、`EventRecord`、`CommitRecord`、`TruncateRecord`、
`FileNaming`、`ChangeFileWriter`、`ChangeFileReader`、`package-info`。

**测试随迁**：`VarintTest`、`ChangeFileIOTest`（RoundTrip/破坏检测——契约层的自包含验收面）。

**留在 reader**：`file` 包全部（`EnvelopeParser`/`ParsedEvent`/`TransactionMarker`/
`EventFileWriter`/`VbfgEventWriter`/`FileRollingWriter`/`FileChangeConsumer`）及其测试
（`FileChangeConsumerTest`/`FileRollingWriterTest`/`ConnectTestRecords`）、`ReaderFileSinkIT`
（端到端验收——其 `ChangeFileReader` import 改从新模块经 compile 传递依赖引入，顺带验收
"跨模块复用 Reader"的形态）。

**pom**：reader 加 `vb-stream-file-format` compile 依赖。

## 5. 文档同步

- 新模块 `CLAUDE.md`：VBFG 布局细目、**跨仓同步契约**（字节布局不兼容变更须两仓递增
  `ChangeFileWriter.VERSION`；`TypeCode`/`Op` 枚举只追加不重排不改名）、移植来源记档
  （vb-cdc-file-transform 2026-09-11 快照）、移植偏离（无——偏离在 EnvelopeParser，属
  reader 的 file 包，随链路留档）
- reader `CLAUDE.md`/`README.md`：file 形态节的"格式"部分改为指向新模块；组件/依赖描述更新
- 根 `CLAUDE.md`：模块列表加一行；`vb-stream-reader/src/main/java` 条目与源码结构段同步
- 根 `README.md`：坐标行三模块 → 四模块

## 6. 验收标准

1. `mvn test`（根，全模块）全绿；用例总数不变（迁移零增减）——reader 由 28 降为 16
   （`VarintTest` 6 + `ChangeFileIOTest` 6 共 12 用例随迁），新模块 12；全仓合计与迁移前一致
2. `mvn test -pl vb-stream-file-format` 独立可跑，零 Docker、秒级
3. `mvn test -pl vb-stream-reader`（含 IT）全绿——`ReaderFileSinkIT` 证明经新模块坐标读回
   落地文件的跨模块复用成立
4. 全仓 grep `org.vastdata.vbstream.reader.format` 零残留（CLAUDE.md 的历史段落按惯例不
   追改除外——本次不涉及）

## 7. 备选与弃选

- **连 file 落地链路一起抽**：新模块须引 debezium-api/connect-api，"零依赖契约层"被污染；
  当前唯一使用方是 reader，YAGNI。将来真有第二个写入方再抽。
- **不抽模块、仅提升包可见性**：外部复用仍需依赖 reader 宿主，问题未解。
