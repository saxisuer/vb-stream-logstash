# VastBase G100 B 档（DBCOMPATIBILITY 'B'）差异面 spike 报告

**日期**：2026-10-18　**性质**：throwaway 探察（M4 = B 档先导调研，不写产品代码）
**harness**：`spike/vastbase-b-mode/BModeWalSpike.java` + `BModeFacadeSpike.java`（一次性，未进构建）
**环境**：`172.16.107.133:55443/test_b`（user=test/Tester123，rolreplication）——与 test_a（55433，A 档）同主机的第二实例

## 结论速览

**B 档比预想近得多**：WAL 记录面与 A 档**零分叉**（VastBase 布局 walker/parser 全窗零异常、heap UPDATE/DELETE 前缀 21/19 同构、DirectWalSource v3 直连稳定、词典几何 42/29 同、判别第二面 relname=128 同）——catalog 面现网即已全绿（catalog-only 装配成功 + DDL 风暴下 47102 records 消费推进、护栏 ISE 如期）。**差异集中在 DML 类型面**：三个新类型 oid（uint4=15719 / uint8=15726 / tinyblob=17757）不在 `DecodeKinds` 词典（矩阵外列经 `XactGrouper.kindsOf` 是 **ISE fail-fast**，非降级）+ typemap 语义差（date 真值 1082 / timestamp→timestamptz / float→float4 / datetime 微秒截断）。

## 发现清单

### 1. 连接面——test_b 实例握手抖动（A 档无此现象）

- vanilla pgjdbc：约 2/4 成功率（间歇 `EOFException` / `SocketTimeoutException`，同参数时成时败；`sslmode=disable` 参数反而稳定失败——勿用）
- opengauss-jdbc：1/5 成功（EOF/读超时为主）
- **DirectWalSource 裸 v3 私有协议直连：全程稳定**（两轮 spike 零重连失败；'r' 反馈/心跳正常）
- 含义：M4 的 IT/对拍基准路（opengauss-jdbc 逻辑流）需连接重试环吸收，或先排查服务端（pg_hba/连接数/防火墙 idle 断连）；wal 路本体不受影响

### 2. 判别与词典几何——与 A 档零分叉

`datcompatibility=B`；`version()` = `PostgreSQL 9.2.4 ...`（无 branding）；`server_version_num=90204`；`pg_class.relname` attlen=**128**（判别第二面命中 VastBase 档）；pg_class **42** 用户列 / pg_attribute **29** 用户列（同 A 档）；负 attnum 集 `[-9,-1]`（-9=xc_node_id、-8=rowid）——**test_a 实查同为 -9**（VastBase 6.0.0 内核共性，非 B 档差异；OG 5.0.0 布局的 -8 下界对两档都差一）。

### 3. typemap 差异表（逐类型 DDL 实查，B vs A 同轮对拍）

| DDL 类型 | B 档 | A 档 | 备注 |
|---|---|---|---|
| `date` | **date(1082, 4B)** | oradate(9040→1114 重映射) | B 档真日期，epoch 天数渲染已支持、实测正确 |
| `timestamp` | **timestamptz(1184)** | timestamp(1114) | MySQL 语义（带时区）；kind/渲染已在矩阵 |
| `datetime` | timestamp(1114) **typmod=0** | timestamp(1114) typmod=-1 | typmod=0 = MySQL DATETIME 精度 0——**微秒输入服务端截断**（实测存整秒，wal 渲染忠实非 bug） |
| `time` | time(1083) typmod=0 | time(1083) typmod=-1 | 值面无差 |
| `float` | **float4(700)** | float8(701) | MySQL float=4 字节 |
| `int unsigned` | **uint4(15719, len=4)** | 语法拒绝 | 新 oid |
| `bigint unsigned` | **uint8(15726, len=8)** | 语法拒绝 | 新 oid（u64） |
| `tinyint(1)` | int1(5545) typmod=1 | 语法拒绝 | tinyint 本身两档都在（5545） |
| `tinytext`/`mediumtext` | text(25) | 类型不存在 | |
| `tinyblob` | **tinyblob(17757)** | 类型不存在 | 新 oid |
| 其余（tinyint/bool/smallint/int/bigint/real/double/decimal/bit/varchar/char/text/blob/raw/uuid/nchar/nvarchar/binary_*/clob） | 与 A 档同 oid | — | blob(88)/raw(86)/clob(90)/nvarchar2(3969)/int1(5545) **两档均属 DecodeKinds 矩阵外** |

### 4. WAL 记录面——VastBase 布局零改动可用（核心结论）

两轮在线收段（DirectWalSource 无槽裸流 → 16MB 标准段名落盘）+ `FileWalSource` 静态目录档 + `WalStreamWalker`（`WalLayoutVVastbase`）离线全窗走读：

- **全窗零解析异常**（census 33 键记录族全消化：rmid 0/1/2/8/9/10/11/18/30 全谱）
- heap UPDATE `flags=0x18`（CONTAINS_OLD_KEY）old-key 段经 **前缀 21** 解码成功（`[id, null×5]` 键列形态，与 OG DEFAULT 身份 'K' 段同构）；DELETE flags=0x8 无旧元组段——**heap UPDATE/DELETE 前缀 21/19 与 A 档同构实证**
- 探针表元组解码：date/datetime/timestamptz/float4/varchar/int4 全部命中现有 kind 且渲染正确（`2026-10-18` / `2026-10-18 12:30:45` / `...+08`[0001 年渲 LMT `+08:05`] / `3.14`·`1.5e-08`[float4 PG 风格]）
- **'' 空串在 B 档保留为空串**（A 档入库即 NULL）——对拍场景设计需知悉的语义差

### 5. 唯一解码缺口（M4 主工作项）

`uint4(15719)` / `uint8(15726)` / `tinyblob(17757)` 不在 `DecodeKinds.forTypeOid` → `XactGrouper.kindsOf` **ISE fail-fast**（"超出解码词典面"——含这些列的表会砖 DML 流，不是 0x 降级）。磁盘编码待锚：uint4 len=4 / uint8 len=8（推测小端 u32/u64，需 M4 手造字节锚）；tinyblob 推测 varlena（bytea 同形，待锚）。

### 6. 护栏与 catalog 面——现网行为实证

- **DML listener 在场：启动期 ISE 如期**（"B 档=MySQL 风格支持属 M4"文案命中，先于槽副作用）
- **catalog-only（无 listener）：完整装配成功**——判别链（三参 relname 探测）→ bootstrap（B 档词典几何 42/29 解码无误）→ direct 通道消费（DDL 风暴下 records=47102、前沿推进、close 干净）——"catalog 面与兼容模式无关"的 M1 先例在 B 档复验成立

### 7. 附带产品发现——VastBase 档 attnum 下界假阳性 WARN（A/B 同病）

流内 CREATE TABLE 生成的 `pg_attribute` 行 `attnum=-9 (xc_node_id)` 触发 `CatalogReplay` "疑似词典错位"WARN——根因：`WalLayoutVVastbase` 未覆写 `attrMinSystemAttnum()`（继承 OG 5.0.0 的 -8），而 VastBase 6.0.0 内核系统列多一列 `rowid`(-8) 把 xc_node_id 挤到 **-9**（test_a 实查同 -9——A 档同样在犯，只是既有 IT 无人盯 WARN 面）。行为影响 = 每次建表一条假 WARN 噪声（行本身是系统列、跳过无害）。修法：Vastbase 覆写返回 -9 + 离线单测锚。

## M4 范围建议（供 spec 决策）

1. **主项**：`DecodeKinds` + `DiskValueRenderer` 增 uint4/uint8/tinyblob 三 oid（磁盘编码手造字节锚 + 渲染 PG text 同形）；顺带裁定矩阵外 VastBase 系类型（int1/blob/raw/clob/nvarchar2）是同批纳入还是维持 ISE fail-fast 记档
2. **护栏白名单** `{'PG','A'}` → `{'PG','A','B'}`；'C' 维持 ISE。开放面裁定点：仅 VastBase 档（真库已验）还是连 OG 5.0.0 B 档（容器）同开——建议 M4 首发仅 VastBase 档（OG 5.0.0 的 B 档 typemap 是否同形未验）
3. **attnum -9 假阳性修复**（独立小修，A 档同受益，可先行）
4. **对拍 IT**：B 档基准路 = opengauss-jdbc 逻辑流在 test_b 的可用性 + 连接重试环（§1 抖动）；场景面新增 B 专形（date 真值/timestamptz/float4/uint 族/tinyblob/'' 空串保留）
5. **非项**：WAL 布局/词典几何/XACT 面**零改动**（本 spike 实证同构）——M4 无布局层工作

## 待用户裁定

- a) OG 5.0.0 容器的 B 档是否纳入 M4 验证面（建议：否，记未验证）
- b) 矩阵外 VastBase 系类型（int1/blob/raw/clob/nvarchar2）同批支持 or 维持 ISE（建议：uint/tinyblob 三 oid 必做——DDL 语法档位差使它们是 B 档独有高频面；其余记矩阵外降级面）
- c) test_b 连接抖动：M4 前是否先请服务端排查（建议：IT 侧重试环 + 服务端排查并行）
