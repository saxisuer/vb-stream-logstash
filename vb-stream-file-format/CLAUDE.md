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
