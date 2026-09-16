# vb-stream-binary-format — VBFG 二进制落地格式

VBFG 落地文件的二进制格式层：记录模型（`Record` 家族六类）+ 二进制流式读写
（`ChangeFileWriter`/`ChangeFileReader`）+ LEB128 编解码（`io.Varint`/`io.Values`）。
沿革：本仓 2026-09-11 自 vb-cdc-file-transform 仓 cdc-file-format 整体移植（写侧 + 读侧），
2026-09-16 对方仓拆分格式层为三模块，本仓同构跟进——二进制部分自基座独立成本模块
（纯移包 + import 换基座，12 用例断言零改动）；写侧使用方是 vb-stream-reader 的
file 输出形态（`VbfgEventWriter`），目标是本仓产出的 `.bin` 与对方仓 cdc-sink 消费端
**逐字节互通**。

## 常用命令

零 Docker、秒级：`mvn test -pl vb-stream-binary-format`（12 用例——`VarintTest` 6 +
`ChangeFileIOTest` 6）。

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

- **跨仓同步契约**：字节布局任何不兼容变更须在两仓（本模块与 vb-cdc-file-transform 的
  cdc-binary-file-format）同步递增 `ChangeFileWriter.VERSION`；`TypeCode.id()`（ordinal+1）
  与 `Op.id()`（ordinal）已持久化进文件——枚举只能追加，不能重排、改名、删除（两枚举
  定义在基座 vb-stream-file-format，持久化语义跨模块生效，动枚举前先读本节）。
- **TypeCode 值域**：原生六种 + 时间五种；时间类型值一律字符串载荷，布局同 STRING。
- **defId 文件内作用域**：每份文件从 1 重新分配；`TableDef` 的 equals/hashCode 刻意忽略
  id——Writer 靠此做文件内 TABLE_DEF 去重，列结构变化（DDL）分配新 id。
- **FOOTER 不进 CRC、不计 recordCount**；其余每条记录的“长度前缀 + 载荷”都计入 CRC32。
- **`Values` 不处理 null**：null 由 EVENT 的位图表达，只序列化非 null 值，列序严格对齐。
- **Reader 流式语义**：`Iterator<Record>` 惰性读、常量内存；迭代到 FOOTER 才校验记录数与
  CRC；截断（记录中间 EOF）与 CRC 不一致抛 IOException（迭代侧 UncheckedIOException 包装）；
  `complete()` 表示已读到 FOOTER（文件完整）。

## 已知限制（范围裁定记档）

- **不识别对方仓 DDL(7) 记录**：本项目不做 DDL（用户裁定，spec §8）——`ChangeFileReader`
  读到对方仓新产文件中的 DDL 记录会报“未知记录类型” IOException。本仓角色是写侧生产者，
  主链路（本仓写 → 对方仓 cdc-sink 读）不受影响；未来接入 DDL 时按对方仓布局（追加类型、
  VERSION 不递增）补齐即可。
- 文件命名（`<task>-<seq 16位零填充>-<yyyyMMddHHmmss>.bin`）归基座 `FileNaming`
  （三后缀 bin/json/sql 共用，文件名字典序 = 消费顺序），见 vb-stream-file-format 模块文档。

## 依赖

**零第三方依赖是刻意设计**（编译期仅基座 vb-stream-file-format + 父 pom，测试期 JUnit）
——未来读取/解析侧（消费端组件、转换/校验工具）以纯 JDK 依赖获得完整契约，不要引入任何
运行时依赖。

## 测试

`VarintTest`（roundtrip/边界值/已知字节形态/负值拒绝/超长流拒绝）+
`ChangeFileIOTest`（全记录类型 RoundTrip、null 位图、多表 defId 去重、CRC 破坏与截断
检测、非 VBFG 拒绝、列数不匹配 fail-fast）——契约层自包含的验收面，也是 2026-09-11
移植正确性与 2026-09-16 模块拆分（断言零改动）的双重锚定。
