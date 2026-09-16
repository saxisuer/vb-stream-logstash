# vb-stream-file-format — 格式层公共基座

格式层公共基座：**文件命名契约 + 事件 IR**。包 `org.vastdata.vbstream.format`——
`FileNaming`（落地文件命名与序号管理，三后缀 bin/json/sql 共用）与事件 IR
（`TableDef`/`ColumnDef`/`Op`/`TypeCode`/`ParsedEvent`——binary 的 Values 编码与 sql 的
SqlRenderer 渲染都以同一 IR 为输入）。`vb-stream-binary-format` 与 `vb-stream-sql-format`
两格式模块依赖本基座（基座不反向依赖任何格式模块）。

沿革：2026-09-11 自 vb-cdc-file-transform 仓 cdc-file-format 全量移植（彼时含二进制读写），
2026-09-16 对方仓拆分格式层为三模块，本仓同构跟进——基座收缩为 IR + 文件命名
（`ParsedEvent` 同日自 reader file 包下沉），二进制布局随拆分移入 vb-stream-binary-format。

## 常用命令

零 Docker、秒级：`mvn test -pl vb-stream-file-format`（`FileNamingTest` 3 用例）。

## 枚举持久化警告（跨模块生效）

`TypeCode.id()`（ordinal+1）与 `Op.id()`（ordinal）**已持久化进 VBFG 落地文件**的
TABLE_DEF/EVENT 记录——两枚举只能追加，不能重排、改名、删除，否则旧文件无法解码。
持久化消费方在 vb-stream-binary-format（`ChangeFileWriter`/`ChangeFileReader` 与
`io.Values`），动枚举前先读该模块 CLAUDE.md 的不变量段。

## `FileNaming` 不变量

- 命名形态 `<task>-<seq 16位零填充>-<yyyyMMddHHmmss>.{bin|json|sql}`，**文件名字典序 =
  消费顺序**（滚动写入、按序消费共用此约定；扩展名即消费分派依据）；
- `nextSeq()` 扫数据目录恢复序号（重启续号，不依赖额外状态文件）——seq 宽度与零填充是
  排序正确性的基础；`parseSeqOpt` 宽容解析（目录里可能混有其他文件，非法命名返回 empty）。

## 依赖

**零第三方依赖是刻意设计**（编译期仅父 pom，测试期 JUnit）——格式层三模块整体以纯 JDK
依赖提供完整契约，不要引入任何运行时依赖。使用方：vb-stream-binary-format /
vb-stream-sql-format（compile 依赖）、vb-stream-reader（经两格式模块传递）。

## 测试

`FileNamingTest`：三后缀命名组装 / 三后缀 seq 解析 / nextSeq 跨后缀恢复——拆分时补上的
基座独立验收面（此前 FileNaming 无专属测试，仅被 reader 侧文件名形态断言间接覆盖）。
