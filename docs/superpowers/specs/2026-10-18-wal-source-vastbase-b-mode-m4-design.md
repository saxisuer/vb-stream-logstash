# vb-stream-wal-source VastBase G100 B 档 M4 设计（MySQL 风格兼容库 DML 开门）

日期：2026-10-18　状态：待用户审阅
前置：spike 报告 `docs/wal-source-vastbase-b-mode-spike.md`（两轮——差异面 + 编码锚，本 spec 的全部实证依据来源）
里程碑序列：M2=PG 档 → M3=A 档+分区 → **M4（本 spec）= B 档**

## 0. 范围裁定（用户确认，2026-10-18）

- **仅 VastBase 档（`WalLayoutVVastbase`）开 B 档**——OG 5.0.0 的 B 档不纳入（typemap 是否同形未验，记未验证维持 ISE）
- **矩阵外 VastBase 系类型全部支持**：uint1/uint2/uint4/uint8/year/int1/blob/tinyblob/raw/clob/nvarchar2/binary/varbinary（13 oid 全集，见 §2.1）——不是只做 uint 三件
- test_b 连接现状：改善后仍有 ~10-20% 握手读超时 + 快速连续建连触发连片失败——**IT/装配按单持久连接 + 重试环设计**（spike 实证 5/6 稳定），服务端排查由用户侧并行
- enum/set/jsonb/money/varbit 等动态格式或 vanilla 同源矩阵外类型**不在本批**（与 vanilla 档 enum/域/jsonb 同性质，维持既有 `0x` 降级/矩阵外语义）

## 1. 架构总览

B 档在 M3 交付的 VastBase 档上只动**三个面**，WAL 布局/词典几何/XACT/分区/TOAST 面零改动（spike 实证同构）：

```
WalSource.startOpenGauss（护栏）
  └ 兼容模式白名单按 layout 分档：WalLayoutVVastbase → {'PG','A','B'}；WalLayoutVOpenGauss → {'PG','A'} 不变
TupleDecoder（layout 接缝 ×1）
  └ B 档含 varlena 列元组的 6B xl_heap_header（多 1 字节 @infomask2 后）——档位感知的 datum 起点推算
DecodeKinds + DiskValueRenderer（类型词典 ×13 oid）
  └ 定宽 6 种（int1/uint1/uint2/uint4/uint8/year）+ varlena 7 种（text 族 2 + bytea 族 5）
WalLayoutVVastbase（附带修复）
  └ attrMinSystemAttnum() 覆写 -9（xc_node_id——A/B 档同病的假阳性 WARN）
```

**判别零新增**：B 档库的三参判别（relname attlen=128）与 A 档同路命中 VastBase 档（spike 实证）；兼容模式差异由护栏白名单分档消化，不进判别面。

## 2. 组件明细

### 2.1 类型词典扩展（`DecodeKinds` + `TupleDecoder` + `DiskValueRenderer`）

kind 映射新增（oid 键控、档位无关——A 档 DDL 可达的 int1/blob/raw/clob/nvarchar2 同批受益）：

| oid | 类型 | kind | 宽度/编码（spike 黄金锚） |
|---|---|---|---|
| 5545 | int1 | 新 `int1` | 1B 有符号补码 |
| 15705 | uint1 | 新 `uint1` | 1B 无符号 |
| 15712 | uint2 | 新 `uint2` | 2B 小端无符号 |
| 15719 | uint4 | 新 `uint4` | 4B 小端无符号 |
| 15726 | uint8 | 新 `uint8` | 8B 小端无符号（'d' 对齐同 int8） |
| 17851 | year | 新 `year` | u16 = 年-1900（值域 1901-2155） |
| 90 | clob | 复用 `text` | varlena |
| 3969 | nvarchar2 | 复用 `text` | varlena |
| 88 | blob | 复用 `bytea` | varlena |
| 17757 | tinyblob | 复用 `bytea` | varlena |
| 9779 | binary | 复用 `bytea` | varlena |
| 95 | varbinary | 复用 `bytea` | varlena |
| 86 | raw | 新 `raw` | varlena 字节流（渲染 hex 无 varlena 解包差异——解码同 bytea，渲染分叉见下） |

`TupleDecoder` 逐 kind 解码分支：uint 族按宽度读小端无符号（`& 0xFF`/`u16`/`u32`/`Long.toUnsignedString` 面）；year 读 u16 存 `Integer`（年 = 值+1900，解码期即归一为年值——渲染零算术）；int1 读字节符号扩展。对齐：uint1/uint2/int1/uint8 按 vanilla typalign（'c'/'s'/'d'）——spike 实证 uint8 走 8B 对齐垫零、1B/2B 紧随。

`DiskValueRenderer` 渲染分支（服务器 text 输出同形锚，spike 采集）：

| kind | 渲染 | 锚 |
|---|---|---|
| int1/uint 族 | 十进制（uint 族无符号语义——`Long.toUnsignedString`） | `127`/`-128`/`255`/`65535`/`4294967295`/`18446744073709551615` |
| year | 十进制年 | `2026`/`1901` |
| clob/nvarchar2（text kind） | 既有 text 渲染零改动 | 原文 |
| blob/tinyblob/binary/varbinary（bytea kind） | **与 bytea 现行同形**（`\x` hex） | **有意分叉记档**：服务端 text 输出是裸字节（乱码形），本渲染取 `\x` hex——与 uuid/numeric 先例同性质的方言分叉，对拍矩阵按渲染面归一化（基准路裸字节 → 归一化器 hex 化）或场景避开裸字节断言 |
| raw | 大写 hex 无前缀 | `0102030405060708090A0B0C0D0E0F10`（服务端同形，零分叉） |

blob 族渲染裁定理由：裸字节直出 String 会产出控制字符/替换符污染（CDC 下游不可用），`\x` hex 是既降级面（矩阵外 `0x`）同族的保守选择；`RenderOptions` 三参版合同不动。

### 2.2 B 档 varlena 元组 6B xl 头接缝（`TupleDecoder` + `WalLayout`）

spike 实证：B 档库上**含 varlena 列的元组**在 WAL 块 data 的 xl_heap_header 是 6 字节（`[infomask2 u16][0x00 u8][infomask u16][t_hoff u8]`——比 vanilla/A 档 5B 头多一个 0x00 字节），datum 区整体 +1；**固定宽元组两档同 5B 头**。剥字节后现有解码零改动正确（'hello clob'/'中文CLOB值'/byte[] 全中）。

设计：

- `WalLayout` 增接缝方法 `int tupleHeaderExtraShift(int infomask2)`（缺省 0；语义 = xl 头在 infomask2 后的额外前导字节数）——**为什么按 infomask2 参数化**：实施期首任务是用 spike harness 补锚钉"触发面是 varlena 在场还是 B 档恒 6B"（text 列在 B 档的形态未单独对照）。若恒 6B，改签名去掉参数（layout 静态返回 1）；若按元组形态分叉，由 infomask2/infomask 位判别
- `TupleDecoder.decodePayload`/`decodeEntry`/`decodePageTuple` 三入口统一经接缝平移 datum 起点（`decodeTupleData` 的 tupleStart 公式 `off+5-23+t_hoff` → `off+5+shift-23+t_hoff`）；页内完整 tuple 面（decodePageTuple）是否同构待锚（FPW/INPLACE 面在 B 档未采样——实施期补，初判同源）
- **触发范围 = WalLayoutVVastbase 布局 + B 档库**：接缝值需要"当前库兼容模式"参与——layout 是静态单例，B 档信息不在其上。落点：`WalSource.startOpenGauss` 探测的 compat 值经既有装配线（`CatalogSynchronizer` → `ChangeStream` → `XactGrouper` → `TupleDecoder` 构造）以**档位参数**下传（`TupleDecoder` 增 dialect 构造参数或 `WalLayout` 增 mutable 档位字段的取舍在实施期定——倾向前者，decoder 实例按装配期创建，无并发面）
- **A 档零回归锚**：A 档库（VastBase 档 layout + compat='A'）接缝恒 0——既有 VastDmlParityIT 15 用例全绿是回归门

### 2.3 兼容模式护栏分档（`WalSource`）

`OG_DML_COMPAT_MODES` 白名单从静态集合改为按 layout 分档：

- `WalLayoutVOpenGauss`（OG 5.0.0）：{'PG','A'} 不变——B 档未验证维持 ISE（文案更新：附"OG 5.0.0 的 B 档未验证，请用 VastBase 档"）
- `WalLayoutVVastbase`（含子类）：{'PG','A','B'}
- ISE 文案随分档更新：B 档命中 OG 5.0.0 时指引不同；'C' 档两 layout 均维持 ISE 附"PG 兼容历史别名未验证"
- 护栏次序不变（先于槽副作用）；`OgCompatModeGuardTest` 增分档断言

### 2.4 attnum 下界修复（`WalLayoutVVastbase`）

覆写 `attrMinSystemAttnum()` 返回 **-9**（VastBase 6.0.0 内核系统列 `xc_node_id=-9`、`rowid=-8`——test_a/test_b 双实查同源；OG 5.0.0 布局维持 -8）。修掉流内 CREATE TABLE 的"疑似词典错位"假阳性 WARN（A/B 档同受益）。离线单测锚：-9/-8/-7 系统列行不告警，-10 起告警。

### 2.5 对拍与验收（手动档真库 IT）

- **`VastDmlParityIT` 增 B 档参数化**：沿 M3 "场景体单方法双档委派零复制"先例扩展为三档（PG 库参数化跑法在 OG 容器，A/B 在真库——A/B 双档共用 `VastTestEnv`，B 档经 `-Dvb.it.vastbase.port=55443 -Dvb.it.vastbase.db=test_b` 或测试内第二 env 实例化，取舍实施期定）。**连接纪律（spike 教训）**：单持久连接 + acquire 重试环（≤15 次）进 `VastTestEnv`
- **B 专形场景**（新增，值面按 §2.1 锚）：
  - ①13 类型全矩阵（uint 边界值 0/max、int1 ±128、year 1901/2026、clob 中文、blob/tinyblob/raw/binary/varbinary 十六进制载荷、nvarchar2）
  - ②B 档语义面：'' 保留（vs A 档 NULL——A 档不跑同断言）、datetime 微秒截断（typmod=0 存整秒）
  - ③varlena 6B 头混合场景：多列 varlena/NULL 混合/TOAST external 宽值（clob 大值走 toast 重组）
  - ④UPDATE/DELETE 前像（uint/year 列的前像渲染）
- **基准路**：opengauss-jdbc 逻辑流（`OgParityBaseline` 复用）——B 档 pgoutput 帧面预期与 A 档同形（M3 先例：A 档逻辑流无分叉）；blob 族裸字节输出经归一化器 hex 化（§2.1 裁定的配套）；raw 直通
- **回归门**：M1-M3 全部 OG IT + VastBase 既有 24 用例 + PG 17/18 全量（护栏分档不动 vanilla 面）
- **离线单测**：DecodeKinds 13 oid 映射/未注册负例；TupleDecoder 新 kind 黄金字节（spike hex 直接进测试）；6B 头接缝（B 档 clob 行原样字节解出 'hello clob'——spike 剥字节实验翻正）；DiskValueRenderer 新类型渲染锚（§2.1 表全部字面）；attrMinSystemAttnum -9 锚

## 3. 数据流

零变化（v2 单 pass 分发 → catalog 先施加 → DML 后解码）；唯一新量 = `TupleDecoder` 的档位参数经装配线下传（`startOpenGauss` 探测 compat → `ChangeStream` 构造 → `XactGrouper` 的 decoder 实例）。

## 4. 错误处理

- 护栏：B 档 + OG 5.0.0 layout → ISE 附"未验证，请用 VastBase 档"（先于槽副作用）；'C' 档维持
- 6B 头接缝判别失败面（锚不足导致的错位）→ 现有数组越界/ISE 即 fail-fast（走读自洽前提），无静默错值
- uint8 > Long.MAX_VALUE 值：解码存 `Long.toUnsignedString` 所需的载体（`BigInteger` 或 u64-long + 渲染期无符号化——实施期按渲染面取简）
- 矩阵外动态类型（enum/set/jsonb）维持现状（`XactGrouper.kindsOf` ISE——注意这是 fail-fast 砖流语义，与 blob 族已支持后剩余面收窄的事实同步进 README 已知限制）

## 5. 测试与验收

见 §2.5。规模预估：离线 +25 上下（DecodeKinds/渲染/6B 头/-9 锚）；手动档真库 IT +6 上下（B 专形四场景 + 护栏分档 + 对拍参数化）；总用例 569 → ~600。

## 6. 任务分解（writing-plans 展开骨架）

1. 补锚：B 档 text/varchar 列 6B 头形态对照 + NULL 行/多列/FPW 页内 tuple 采样（spike harness 复用）——钉死接缝参数化形态
2. `WalLayout.tupleHeaderExtraShift` 接缝 + `TupleDecoder` 三入口平移 + 档位参数下传（含 A 档恒 0 回归锚）
3. `DecodeKinds` + `TupleDecoder` 定宽 6 kind + varlena 7 oid 映射
4. `DiskValueRenderer` 渲染分支（blob 族 `\x` 分叉记档 + raw hex）
5. `WalLayoutVVastbase.attrMinSystemAttnum = -9` + 离线锚
6. 护栏白名单分档 + 文案 + `OgCompatModeGuardTest`
7. `VastTestEnv` 连接重试环 + B 档 env 参数化
8. `VastDmlParityIT` 三档参数化 + B 专形四场景 + 基准路归一化器 blob hex 面
9. 文档收口（模块 CLAUDE.md/README/根 CLAUDE.md + spike 报告勘误回填）

## 7. 配置与依赖变更

零新增键、零依赖变化。`vb.wal.tables`/`vb.wal.dml` 在 B 档库直接生效（M3 同构）。

## 8. 已知限制（本 spec 交付后仍存）

- OG 5.0.0 的 B 档：未验证，护栏 ISE（用户裁定 a）
- enum/set/jsonb/money/varbit：矩阵外 fail-fast/降级语义不变
- blob 族渲染与服务器 text 输出有意分叉（`\x` vs 裸字节）——对拍归一化承载，方言渲染分支记后续
- 6B 头的语义成因（服务端标记位）未考——按几何接缝处理，升级内核版本时需复验
- test_b 连接抖动的服务端根因未消——IT 重试环承载，非本 spec 范围
