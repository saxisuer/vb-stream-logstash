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
