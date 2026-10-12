# vb-stream-wal-source openGauss 5.0.0 适配 M2 设计（DML/changes 面 · PG 兼容模式）

日期：2026-10-12 · 状态：已与用户逐节确认（brainstorming 五节全过）
前置：M1（catalog 面，spec `2026-10-11-wal-source-opengauss-m1-design.md`，已合并 main）；
调研报告 `docs/wal-source-opengauss-adaptation-research.md`（含实施勘误表）。
里程碑序列（2026-10-12 用户裁定）：**M2（本 spec）= PG 兼容模式 DML 面** → M3 = A 兼容模式
（Oracle 风格原生分区）→ M4 = B 兼容模式。

## 0. 范围裁定（用户确认）

- **兼容模式分期**：M2 只做 **PG 兼容模式库**（`sql_compatibility`/DBCOMPATIBILITY='PG'）；
  A/B 档库启动期 ISE（消息附档位与 M3/M4 指引）。三档分区表实现不同，各自独立转录。
- **DML 范围全面对齐 v2 矩阵 + 两个 OG 特有面**（用户选 A 全量）：v2 六场景（基础 DML 六
  形态/17 类型边界值/TOAST 三形态+重启 unchanged/DDL-in-txn as-of/2PC 四形态/中途停续）
  + attcmprmode 列压缩 + parttype 分区（PG 档语义）。
- **验收基准 = C 混合路线**：前置 spike 探 openGauss 逻辑解码路可用性（pg_recvlogical /
  opengauss-jdbc logical API / vanilla pgjdbc logical 三路实测——逻辑路是 AM_WAL_DB_SENDER
  的**正确**路由形态，与物理路雷区不同）——可用则双路对拍（v2 `DualPathParityIT` 形态），
  不可用降 JDBC 实查断言式（记录在案，不阻塞交付）。
- **结构形态 = 方案①**：复用 v2 changes 包主干（XactGrouper/ToastAssembler/
  DiskValueRenderer/OutputRenderer 零结构改动），OG 差异经三个接缝注入（XACT 解析/
  MULTI_INSERT/分区过滤），沿 M1 的 layout 档位门控纪律。

## 1. 架构总览

```
WalSource.start()（OG 档，M2 形态）
  M1 管线不变：判别 → ensureSlot → state 决策 → bootstrap → WalByteSource（file/direct）
  变化①：撤销 M1 的"OG 档 DML listener WARN 降级 catalog-only"——vb.wal.dml=true
    （缺省）时 ChangeStream 正常装配（ogCatalogOnly 位退役）
  变化②：sink 第二消费者激活：applyRecord（catalog 施加）→ postApply →
    ChangeStream.onRecord（as-of 次序——M1 已预留接缝）

changes 包 OG 面（三接缝）：
  a) XACT 解析：OG 格式转录（四记录族+五 opcode），XactGrouper 状态机零改动消费
  b) MULTI_INSERT：M1 ISE 解除（语料锚 rmid=9/info 0x30 + COPY 场景补锚）
  c) TableFilter：PG 档分区语义（relkind 'r' + parttype）
  值面：DiskValueRenderer 17 类型复用 + OgColumnCompression（attcmprmode 解压，
    varlena 接缝，失败 ISE 防静默错值）
  输出：OutputRenderer TXN 块格式零改动（v2 契约）

启动期护栏：库级 sql_compatibility 非 PG 档 → ISE（探测 SQL 实测钉死；
  候选：SELECT datcompatibility FROM pg_database WHERE datname=current_database()）

验收（spike 前置裁定形态）：
  路A → OgDualPathParityIT：OG 逻辑解码路 vs wal 直解路，xid 交集逐行 diff 空
  路B → 场景断言式 IT：已知 DML → 输出与 JDBC 实查预期逐项相等
```

## 2. 组件明细

| 组件 | 包 | 动作 | 职责要点 |
|---|---|---|---|
| Spike：OG 逻辑解码路可用性 | （throwaway+结论文档） | 新增 | 三路实测（pg_recvlogical sidecar / opengauss-jdbc logical / vanilla pgjdbc logical）；产出对拍路选型 + 输出格式样例（归一化器依据）；结论落 docs/ |
| `OgXactParser`（或 layout 分派，实施定） | layout | 新增 | OG xl_xact_commit/abort/prepare/commit_prepared/abort_prepared 转录（htup.h/xact.h + 语料锚）；产物对齐现有 XACT 视图 |
| `HeapViews` MULTI_INSERT OG 分支 | layout | 改动 | M1 ISE 解除；布局转录（offsets 数组 + INIT_PAGE 语义）+ COPY 批量语料锚 |
| `TableFilter` 分区语义 | changes | 改动 | PG 档：relkind 'r' 且 parttype∈{'n','p'}；分区子表（行物理所在）独立匹配白名单，母表不承载行不匹配 |
| `OgColumnCompression` | changes | 新增 | attcmprmode 列压缩算法转录（源码定位 + 实测锚：COMPRESSION 建列 INSERT 大值采字节）；挂 varlena 接缝 |
| `ChangeStream`/`XactGrouper`/`ToastAssembler`/`OutputRenderer` | changes | **零改动** | 状态机/TOAST/输出全复用（差异被上游接缝吸收） |
| `WalSource`/`startOpenGauss` | api | 改动 | 撤销 WARN 降级；启动期 sql_compatibility 护栏 |
| `OgTestEnv` 扩展 | it | 改动 | PG 兼容库（CREATE DATABASE ... DBCOMPATIBILITY 'PG'）+ A/B 档库（fail-fast 测试用）；对拍路装配 |

**转录/实测任务清单**：①XACT 四记录族 ②MULTI_INSERT ③OG 列压缩算法 ④2PC 面
（PREPARE TRANSACTION 可用性 + prepared 记录形态）⑤分区表 heap 路由（INSERT 落母表
还是分区 relfilenode——PG 档实测锚定，不预设）。

## 3. 数据流

**分区过滤规则**：白名单按物理表匹配（分区子表独立匹配；母表不在匹配面——它不承载行）。
默认全放行时母子表都放行。依据任务⑤实测——若 PG 档记录落母表 relfilenode 则反转规则
（实测锚定）。

**DML 数据流**：WalRecord(用户表 heap) → ChangeStream.onRecord（M1 接缝激活）→
TableFilter（relkind+parttype+白名单）→ 字典 as-of 解析表身份/列序 → XactGrouper 入桶
（到达期解码+渲染；MULTI_INSERT 逐 entry 展开；UPDATE 旧元组——OG logical+FULL 已实证
带全旧元组，DEFAULT 身份 old-key-tuple 面与 v2 同判）→ TOAST（ToastAssembler 重组 +
OgColumnCompression 在 varlena 接缝）→ DiskValueRenderer 值渲染 → COMMIT/COMMIT_PREPARED
批量发射（Begin→过滤后行→End，v2 语义不变）。

**检查点与 at-least-once**：pendingFloorLsn 全复用（StateStore formatVersion 不动——
dmlFloorLsn 字段 M1 已在）；双通道下 floor 语义一致（前沿来自 walker.consumedLsn）。

**XACT 终态**：OG 五 opcode 映射 XactGrouper 现有发射/弃桶/挂起动作；子事务归并
（TOPLEVEL_XID/ASSIGNMENT）格式对齐后零改动消费。

## 4. 错误处理

**fail-fast**：非 PG 兼容档库 ISE（附档位+M3/M4 指引）；未转录的分区路由形态 ISE（附
LSN/relfilenode）；attcmprmode 解压失败 ISE（列压缩失败=格式错位硬失败——区别于 TOAST
值面"解压损坏 ISE 同 pglz 契约"的既有裁定面）；MULTI_INSERT 布局不自洽 ISE；XACT 字段
越界/opcode 未注册 ISE（附 LSN）。

**降级（有痕）**：DEFAULT 身份 old-key-tuple（v2 同判）；TOAST unchanged 哨兵/probe
回查兜底（v2 语义原样；OG 的 probe SQL 兼容性实测，坏则记档）。

**已知限制（文档化）**：A/B 档库整库不支持（M3/M4 范围）；attcmprmode 未转录档 ISE +
文档列明已支持档；对拍降断言式的形态记录在案；分区专属 DDL 场景深度按任务⑤实测裁剪，
不达 v2 深度记档。

## 5. 测试与验收

- **Spike 交付物**：选型结论 + 输出样例 + 三路探测记录落 docs/（代码 throwaway）。
- **离线单测**：OgXactParserTest（手造四族 + 语料锚 + 2PC/子事务归并）；MULTI_INSERT
  （Task 8 语料 + 手造变体；M1 ISE 测试反转为解析断言）；OgColumnCompressionTest
  （黄金向量 + 失败负例）；TableFilter 分区用例；**v2 changes 包现有用例零改动零回归**。
- **IT 矩阵**（PG 兼容库）：v2 六场景全量对拍/断言（对拍器与归一化器复刻 v2 形态）；
  OG 特有面（attcmprmode 各支持档值往返 / 分区 range+list 捕获 / 白名单母-子两形态）；
  护栏（A/B 档库各一 fail-fast 用例）；回归（M1 全部 OG IT + PG 17/18 全量零回归）。
- **验收标准**：①v2 六场景 PG 档全绿；②attcmprmode 支持档值往返 + 分区捕获正确；
  ③A/B 档 ISE 钉死；④changes 包 v2 用例零改动；⑤M1 面 + 17/18 零回归；⑥MULTI_INSERT
  ISE 解除且有语料锚。

## 6. 任务分解（writing-plans 展开骨架）

- T0 Spike：OG 逻辑解码路三路探测 + 选型结论文档（前置，裁定验收形态）
- T1 启动期兼容模式护栏 + M1 WARN 降级撤销（sql_compatibility 探测 SQL 实测）
- T2 OgXactParser 转录 + 语料锚 + 离线单测（含 2PC 可用性实测归并）
- T3 MULTI_INSERT OG 分支 + 语料补锚（COPY 场景）+ M1 ISE 测试反转
- T4 OgColumnCompression 转录 + 黄金向量（attcmprmode 实测采锚）
- T5 分区路由实测（任务⑤）+ TableFilter PG 档语义 + 白名单规则落地
- T6 对拍路装配（按 T0 选型：OgDualPathParityIT 或断言式基座）+ OgTestEnv 扩展
- T7 v2 六场景 IT 全量 + OG 特有面 IT + 护栏 IT
- T8 文档同步与收口（模块/根 CLAUDE.md、README、调研勘误回填、M1 已知限制清单勾销）

依赖序：T0 最先；T1-T5 依赖 T0 无关可并行（但按序执行）；T6 依赖 T0+T2；T7 收口；
T8 文档。

## 7. 配置与依赖变更

配置键零新增（v2 的 `vb.wal.tables`/`vb.wal.dml` 在 OG 档激活即用）；依赖零新增。
M1 的五键语义不变（`vb.wal.og.*`）。

---

## 勘误注记（2026-10-12 实施期回填，T8 追加——原文保留作历史，以本节为准）

M2 T0-T7 实施期实测对本文六处前提/预判的修正：

1. **§0 "attcmprmode 列压缩 + parttype 分区（PG 档语义）"的 attcmprmode 面前提被推翻**：
   attcmprmode 列压缩在 OG 5.0.0 **行存面 DDL 天然不可达**（三路 DDL 语法全拒 +
   tablecmds.cpp 双护栏 + 全库目录扫三源；普通表 attcmprmode∈{0,127}）。T4 交付改为
   DELTA/DICT/PREFIX/NUMSTR 四算法 + meta 走读**纯函数零接线**（黄金向量手造——服务端
   字节不可采）；验收由 §5 "各支持档值往返"降为轻量护栏（压缩 DDL 服务端拒绝判别串 +
   attcmprmode∈{0,127} 库侧钉 + DML 对拍不受扰）。压缩形态实为元组级 HEAP_COMPRESSED
   0x0010 + 页字典（**非 §1 预设的 varlena 接缝 tag**——接缝预设一并推翻）；A 档列存
   （M3）走 compress_kits CU 族不同源，勿复用本类。
2. **§0/§3 "分区子表独立匹配白名单、母表不承载行"前提被推翻**：OG 分区子表**不在
   pg_class**（无 pg_class/pg_attribute 行，列挂母表 oid；分区行在 pg_partition，母表
   relkind='r' + parttype 值域恰 {'n','p'}）——白名单按子表独立匹配不可达。T5 实测路由
   = heap INSERT 落**分区自身 relfilenode**（pg_partition 'p' 行，range/list 四锚
   32809/32810/32821/32822，母表零命中）。
3. **§1 "MULTI_INSERT 语料锚 rmid=9/info 0x30"系任务链误读**：实测 info=0xD0（=0x50|
   INIT_PAGE）×15 + 0x50×2；真布局 = [INIT_PAGE 前置 pd_xid_base u64]flags u8 +
   isCompressed bool@1 + ntuples u16@2 + offsets（INIT_PAGE 省略）+ LogCSN 殿后，entry
   几何 OG==vanilla（2 对齐 + SHORTALIGN(7+datalen)，datalen 只计 tail）。
4. **§3 "子事务归并（TOPLEVEL_XID/ASSIGNMENT）格式对齐后零改动消费"的 TOPLEVEL_XID
   半边被推翻**：OG 流**无 252 TOPLEVEL_XID 标记**（源码钉死）——子事务归并改**终态
   subxacts 经 mergeSubBuckets 补偿**（输出时序等价实证）；映射缺位子回滚多发一条
   BatchAborted（onAborted 零输出，对拍无感）。
5. **§0/§5 验收形态落定 PARITY=LOGICAL**：T0 spike 三路实测
   （`docs/wal-source-opengauss-m2-spike.md`）——opengauss-jdbc `logical()` in-process
   逻辑流可用（proto v1；两处帧级差异：B 帧 xid Int64 / TupleData Int16 ncols 前缀，
   归一化器独立新写不共享 engine PgOutputDecoder），路 B 断言式基座无消费方不建；2PC
   语义断言走捕获器 begins() 事件面（proto v1 下 PREPARE→COMMIT PREPARED 以普通 B..C
   块出现、解码推迟到 COMMIT PREPARED 时点）。
6. **§5 验收② "分区捕获正确"按 R1 裁定降级（显式呈报与"全量"意图的偏差）**：M2 分区面
   = **不砖 + 有痕跳过**而非捕获——parttype='p' 存储护栏放宽（母表行入字典无害，
   种子/流内双面），分区 DML 行因 pg_partition 不在 v1 三表 watched 字典而 fail-safe
   miss 有痕跳过（发射恰 1 控制行、零泄漏、无终态失败）；完整捕获需第四 watched 字典，
   归 M3（A 档原生分区是天然归宿）。依据 = §4 "分区深度按任务⑤实测裁剪"弹性条款 +
   一张分区表砖全库不可接受。
7. **（M3 追记，2026-10-13）第 6 条的"分区跳过语义"已在 M3 退役**：M3
   （`2026-10-13-wal-source-opengauss-m3-design.md`）pg_partition 第四 watched 字典
   落地后分区 DML 行**正常捕获**——且 M3 T0 实测 **PARTITION_NAME=mother**（OG
   pgoutput 逻辑流对分区 DML 报母表身份，分区名零出现）反转了 M3 spec §0 预设
   [分区名输出]与 §3 [TableMeta 身份=分区名]：TableMeta 身份=母表、白名单母表名匹配
   升基准形态（分区名降兼容增强）、对拍归一化器免映射。本节第 6 条的 seed 节流 WARN
   与逐 relfilenode miss DEBUG 痕迹面随捕获语义分别删除/保留为噪声常态 miss 痕迹；
   有痕跳过仅余链断与 DROP 后迟到行两 fail-safe 形态。
