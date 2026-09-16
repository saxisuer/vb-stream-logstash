# vb-stream-sql-format — SQL 文本落地格式

SQL 文本落地格式层：IR（基座 `ParsedEvent`）→ 可直接执行的 SQL 语句文本（PG/VastBase 方言）。
单类 `SqlRenderer`——纯函数、零状态、零第三方依赖。移植自 vb-cdc-file-transform 仓
cdc-sql-file-format 的 SqlRenderer（2026-09-16 快照），**与对方仓的分叉点唯一：裁掉
`renderDdl`**（本项目不做 DDL，用户裁定，spec §8）。使用方是 vb-stream-reader 的 file
输出形态（`reader.format=sql`，`SqlEventWriter` 逐语句写文件）——产物无需专用消费端即可
在目标库直接执行重放。

## 常用命令

零 Docker、秒级：`mvn test -pl vb-stream-sql-format`（`SqlRendererTest` 10 用例）。

## 渲染规则

| 事件 | 渲染 |
|---|---|
| INSERT（op=c/r） | `INSERT INTO "s"."t" ("c1",…,"cn") VALUES (v1,…,vn);` 全列 |
| UPDATE | `UPDATE "s"."t" SET "c1"=v1,… WHERE "pk"=vpk AND …;`（SET after 全列，WHERE 主键） |
| DELETE | `DELETE FROM "s"."t" WHERE "pk"=vpk AND …;` |
| TRUNCATE | `TRUNCATE TABLE "s"."t";` 逐表一条 |

值字面量（TypeCode 驱动）：

- BOOL → `TRUE`/`FALSE`；INT → 十进制；FLOAT32/64 → shortest-round-trip，`NaN`/`±Infinity`
  加单引号（裸字面量非法，PG 系接受带引号形态再按目标列转换）；
- STRING 与时间五种（TIMESTAMP/TIMESTAMPTZ/DATE/TIME）→ 单引号，内部 `'` 翻倍
  （假定 standard_conforming_strings=on，不用 `E''`）；
- INTERVAL → 载荷 `"<n> microseconds"` 解析微秒，分解为 `[-]<days> days [-]HH:MM:SS.ffffff`
  （负值天与时分秒两段都带符号；总时长无损，年月结构不可还原——IR 只保留总微秒）；
- BYTES → `decode('<小写hex>','hex')`；null → `NULL`；
- 标识符 `"schema"."table"`，内部 `"` 翻倍、大小写保留。

### 两处 VastBase 实测裁定（对方仓实测，勿“修正”）

1. **BYTES 用 `decode()` 不用 `'\x'::bytea`**：VastBase 的 bytea 解析器不认 `\x` 十六进制
   前缀格式（按字面文本存储，数据损坏）；decode() 双端可用且返回 bytea 无需 cast。
2. **INTERVAL 分解渲染、不直写 microseconds**：`'-45296789000 microseconds'` 这类
   微秒直写形态 VastBase 服务端拒收，须分解为 days + 时分秒形态（如
   `'-0 days -12:34:56.789000'`——负值天与时分秒两段都带符号）。

## 失败语义（不丢数据契约）

无主键表的 UPDATE/DELETE、INTERVAL 载荷非微秒形态、字符串含 NUL → 抛
IllegalArgumentException → 调用方停引擎、offset 不推进、重启重放（与 binary
“不支持的类型抛异常”同构——失败优于静默丢）。

## 已知限制（与对方仓 sql 格式逐条一致，spec §8 记档）

- 无主键表的 u/d 落地失败（上节失败语义）；
- 不支持源端主键值变更：UPDATE 用 after 主键定位，旧主键行残留；
- 裸 SQL 重放无幂等：如实反映源端操作序列，重复执行的主键冲突由执行方处理（不做 UPSERT）；
- 无表名/列名映射：SQL 在捕获端固化源端原名，目标表名映射归执行侧；
- lsn/txid 不落文件、无 FOOTER/CRC：SQL 文件形态的完整性靠 tmp `.part` 同目录暂存 +
  原子 rename 的 publish 机制（与 binary 同一外壳——`FileRollingWriter`，约定“数据目录里
  无 `.part` 后缀即完整文件”；落地链路见 vb-stream-reader 模块文档）。

## 依赖

零第三方依赖（编译期仅基座 vb-stream-file-format + 父 pom，测试期 JUnit）。

## 测试

`SqlRendererTest` 10 用例：INSERT 全列与各类型字面量 / 标识符转义与浮点特殊值 / UPDATE /
DELETE / TRUNCATE / 无主键 u+d 抛异常 / NUL 抛异常 / 多行字符串原样 / INTERVAL 负值分解
与非微秒形态抛异常 / 空 bytea 与 ±Infinity——移植对方仓用例面（去 DDL 用例），方法名
英文 camelCase（仓规约），移植正确性锚定。
