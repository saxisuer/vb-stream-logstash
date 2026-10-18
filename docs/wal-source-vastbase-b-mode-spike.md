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

**裁定（2026-10-18）**：a) 不支持 OG 5.0.0 B 档（仅 VastBase 档）；b) 矩阵外 VastBase 系类型全部支持；c) test_b 改善后仍有 ~10-20% 握手读超时（vanilla 8/10、opengauss 9/10，connectTimeout 加长无效——服务端认证应答间歇挂起；另注意**快速连续建连（每语句一连接）会触发连片失败**，单持久连接 5/6 稳定——IT 按单连接 + 重试环设计）。

## 编码锚（第二轮 spike，`BModeEncodingSpike`——M4 实施的黄金字节来源）

**扩展类型面（B 档 DDL 全谱）**：unsigned 全家桶 uint1(15705,1B)/uint2(15712,2B)/uint4(15719,4B)/uint8(15726,8B)；year(17851,2B)；binary(9779,varlena)/varbinary(95,varlena)；`float(N)`→float4、`double(M,D)`→numeric、`serial`→int8、`mediumint`→int4；tinyint(1)/int3/numeric unsigned 语法面（后者拒绝）；enum/set 匿名 oid（动态格式，与 vanilla enum 同性质维持矩阵外）。

**磁盘编码（逐类型单列表 + 已知值 + WAL 块 data hex 三方锚）**：

| 类型 | 磁盘编码（实证） | 服务端 text 输出（对拍锚） |
|---|---|---|
| int1(5545) | 1B 有符号补码（127→7F、-128→80） | `127` / `-128` |
| uint1(15705) | 1B 无符号（255→FF） | `255` / `0` |
| uint2(15712) | 2B 小端无符号 | `65535` / `1` |
| uint4(15719) | 4B 小端无符号 | `4294967295` / `1` |
| uint8(15726) | 8B 小端无符号（8B 对齐规则同 int8） | `18446744073709551615` / `1` |
| year(17851) | **u16 = 年-1900**（2026→7E00h=126、1901→1） | `2026` / `1901` |
| clob(90)/nvarchar2(3969) | varlena（与 text 同构） | 原文（`中文CLOB值`；'' 保留） |
| blob(88)/tinyblob(17757)/binary(9779)/varbinary(95) | varlena 字节流（与 bytea 同构） | **裸字节**（非 \x hex——`DEADBEEF` 以 Latin 字符直出） |
| raw(86) | varlena 字节流 | **大写 hex 无前缀**（`010203...0F10`、`FF`——Oracle RAW 风格） |

黄金 hex（两列 `(id int, c <type>)`，t_hoff=0x18、datum 基 off+6、id@6-9 LE）：`int1/127`=020000081800010000007F；`uint4/max`=02000008180001000000FFFFFFFF；`uint8/max`=02000008180001000000 00000000 FFFFFFFFFFFFFFFF（8B 对齐垫 4 零——int4 后相对 datum 基垫 [4,8) 共 4B 到 8B 边界；原稿"垫 3 零"与缺一零字节的 hex 转录同源误记，测试黄金字节以 4 零为准）；`year/2026`=020000081800010000007E00；varlena 族见下节 6B 头。

**B 档 varlena 元组的 6B xl 头（记录层接缝，A/B 对照实证）**：同值同几何（两列 id+c）下——A 档 text 行 = `[02 00][02 08][18]`（5B 头：infomask2=2/infomask=0x0802/hoff=0x18，datum@off+6）；**B 档 clob 行 = `[02 00][00 02 08][18]`（多 1 字节 0x00 插在 infomask2 后，datum@off+7）**——固定宽元组两档同 5B 头。实证：剥掉该字节后现有 `TupleDecoder` 对 B 档 clob/nvarchar2/blob 行（kinds=text/bytea）**零改动解码正确**（'hello clob'/'中文CLOB值'/byte[] 全中）。触发面初判 = 元组含 varlena 列（clob/nvarchar2/blob/tinyblob/raw/binary/varbinary 及 text/varchar 待复验——**text 列在 B 档是否也 6B 头未单独对照**，M4 实施期补锚钉死；text 本身 A 档 5B 头在 B 档库上的形态是首个待验点）。该字节语义（新标记位/压缩提示）未考——按"多一字节"几何接缝处理即可。**⚠ 本段括注已被第三轮补锚推翻（A/B 同形、无多字节）——见文末「第三轮补锚（M4 Task 1）」勘误项。**

**A 档对照副产品**：A 档 `''` 入库即 NULL（varchar row2 渲 `null`）vs B 档保留 `''`——对拍场景设计须知；A 档 int1 值域与 B 档不同（127 插入报 tinyint out of range——A 档 int1 为 0..? 待考，非 M4 范围）。

### 第三轮补锚（M4 Task 1）

背景：text/clob/nvarchar2(32)/blob 四类型第二行值改 NULL（HASNULL 位图锚）+ `vb_wal_enc_copy` 5 行 `INSERT...SELECT` COPY 探针（phase2Dump 增 rmid∈{9,10} 且 opcode=0x50 的 copy-multi dump 分支——本窗口零命中）。锚跑目录 `/tmp/vb-spike-b-anch`，运行全窗零异常（census 键数 34）、探针 finally 自清（真库 `vb_wal_enc_%` 残留核查 = 0）。**本轮实测推翻第二轮「B 档 6B xl 头」括注——勘误见本节末项，Task 2 依 brief 预置回到 spec §2.2 修订接缝设计后再继续。**

**① text 列锚（与 clob 族完全同形，无独立头形态）**：B 档 text 行1 与 clob 行1（同值 'hello clob'）逐字节相同：

```
HEX [text]          lsn=0/1DC64028 data=020002081800010000001768656C6C6F20636C6F62...
HEX [clob]          lsn=0/1DC72BC0 data=020002081800010000001768656C6C6F20636C6F62...
HEX [nvarchar2(32)] lsn=0/1DC79ED8 data=020002081800010000001FE4BDA0E5A5BD6E76617263686172...
HEX [blob]          lsn=0/1DC6B398 data=020002081800010000000BDEADBEEF...
```

判读：`[02 00]`=infomask2（natts=2）、`[02 08]`=infomask（HASVARWIDTH|XMAX_INVALID）、`[18]`=t_hoff=24、**byte@5=垫零**（见勘误）、id@6-9 LE（=1）、varlena@10（1B 头 0x17/0x1F/0x0B 经 (b>>1)&0x7F 反推总长 11/15/5B——数据 10/14/4B：'hello clob' 10B/'你好nvarchar' 14B/'DEADBEEF' 4B，各 +1B 头自洽。原稿"总长 12/16/6B"系长度口径误记——1B varlena 头的总长 = 头字节>>1，含 1B 头自身，勿再另加头或对齐）。

**② NULL 行（HASNULL）锚**：四类型行2 同构：

```
HEX [text] row2 lsn=0/1DC64138 data=0200010818010200000002...
```

判读：infomask 0x0802→**0x0801**（HEAP_HASNULL 顶入、HASVARWIDTH 消失——唯一 varlena 列 NULL，自洽）；**byte@5=0x01 即 t_bits null 位图**（两列表 bit1=第二列 NULL）；datums@6 仅 id（=2 LE），NULL 列无 datum。几何 = vanilla「HASNULL 时位图自 tuple offset 23 起」——既有 `TupleDecoder.decodePayload` 的 HASNULL 路径零改动覆盖。

**③ COPY/MULTI_INSERT 锚**：`vb_wal_enc_copy` 5 行 `INSERT...SELECT` 产出 **5 条独立单 INSERT 记录**（rmid=10/opcode 0x00，rel=24251），窗口内 MULTI_INSERT（rmid 9/0x50）零命中：

```
INSERT lsn=0/1DC81248 head=020002081800010000000B726F7731   ('row1')
INSERT lsn=0/1DC812B8 head=020002081800020000000B726F7732   ('row2')
INSERT lsn=0/1DC81320 head=020002081800030000000B726F7733
INSERT lsn=0/1DC81388 head=020002081800040000000B726F7734
INSERT lsn=0/1DC813F0 head=020002081800050000000B726F7735   ('row5')
```

即该规模批量插入未走 MULTI_INSERT——clob 批量行走的是已覆盖的单 INSERT 路径（几何同 ①）。**MULTI_INSERT entry 头形态仍未采样**（真 COPY FROM / 更大批量待验）：按裁定记「clob MULTI_INSERT 需 Task 3 的 decodeEntry 路径锚、Task 8 IT 端到端兜底」。

**勘误（重要——推翻第二轮「6B xl 头」读法）**：第二轮结论「B 档 varlena 元组比 A 档多一个 0x00 字节插在 infomask2 后、datum@off+7」系字节序转录误读。本轮实测 + 复 dump 第二轮留存锚目录（`/tmp/vb-spike-a-enc` 与 `/tmp/vb-spike-b-enc`——两库不同 WAL 时间线，同值行）证实：**A/B 两档同值元组头区逐字节相同**（`0200020818000100000017 68656C6C6F...` 两边一致），"多出的字节"不存在——byte@5 是 vanilla `[自 tuple offset 23 起字节]` 区的标准成员（HASNULL 行 = t_bits 位图、无 NULL 行 = MAXALIGN(23)=24 的对齐垫零），datum 基 = off+(t_hoff−18)，t_hoff=24 时恒 off+6。这恰是既有 `TupleDecoder.decodePayload` 的模型（tupleStart=off+5−23、游标=tupleStart+t_hoff）——第二轮「零改动解码正确」的实证本就在此模型下成立（"剥字节"叙述与所引字节序不符；第二轮黄金 hex 自身"datum 基 off+6"亦与此互证）。**影响：Task 2 的「B 档 6B 头接缝」前提失效——按 brief 预置，回 spec §2.2 修订接缝设计后再继续；B 档 INSERT 块 data 解码在现有 TupleDecoder 下已零改动覆盖（①②③ 单 INSERT 面全部实测钉死）。**
