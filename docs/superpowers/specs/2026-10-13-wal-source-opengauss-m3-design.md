# vb-stream-wal-source openGauss 5.0.0 适配 M3 设计（A 兼容模式 + pg_partition 第四字典）

日期：2026-10-13 · 状态：已与用户逐节确认（brainstorming 五节全过）
前置：M1（catalog 面，`2026-10-11` spec）+ M2（PG 档 DML 面，`2026-10-12` spec，含四条
勘误注记与 M3 前置记档）；调研报告（含 M1/M2 两段勘误表）。
里程碑序列（2026-10-12 用户裁定）：M2=PG 档 → **M3（本 spec）= A 档 + 分区捕获** → M4=B 档。

## 0. 范围裁定（用户确认）

- **A 档开门 + 分区捕获**：兼容模式护栏白名单放宽为 {'PG','A'}（B/'C' 档维持 ISE 附
  M4 指引）；pg_partition 第四 watched 字典落地（PG 档分区捕获顺带受益——T5 实测
  两档分区为同一套原生机制）。
- **分区行输出 = 分区名**（用户记忆 + vanilla pgoutput 缺省双背书；T0 首任务基准路
  采样终审——实测反转为母表名则归一化器映射 + 记档，设计其余面不动）。
- **白名单双匹配（方案 C）**：`vb.wal.tables` 对分区行按"分区名 OR 母表名"任一命中
  放行——OG 档有意增强（身份链零成本旁路），文档记档与 vanilla 语义分叉；分区滚动
  运维场景下用户写母表名即覆盖全部分区。
- **验收 = A 档全量复验 + 分区专项**（用户选 A）：v2 六场景 + COPY + TOAST 在
  DBCOMPATIBILITY 'A' 库全量重跑（IT 参数化加兼容档维度）+ 分区专项（range/list ×
  白名单 × 流内滚动 × 跨分区迁移）。
- **ASTORE 限定维持**：A 档列存表（ORIENTATION=COLUMN）启动期/流内 ISE 不变——列存
  CU 族 WAL 是独立大项，**非目标**（生产若有列存负载须提前呈报改动本边界）。
- **结构 = 方案①完整第四 watched 表**：与 attr/class/nsp 同权（bootstrap 种子 + WAL
  ctid 重放 + stores 容器 + StateStore v4 + 身份链进 CatalogSnapshot）——沿 v2 加
  pg_namespace 的同构先例；否决轻量 probe 懒解析（正常路径依赖服务端交互违伪备库
  立意）与 classRows 寄生（字典语义税）。

## 1. 架构总览

```
M3 = A 档开门 + 分区捕获（pg_partition 第四字典）：

护栏面：白名单 {'PG','A'}（datcompatibility 探测沿用 T1 形态）；B/'C' ISE 附 M4 指引；
  ASTORE 限定不变（列存/段页式/bucket/UStore 维持 ISE）

第四字典（与三表同权）：
  CatalogBootstrap 第四表种子（同事务：pg_partition 关键列——分区 oid/parentid/
    relname/relfilenode 来源列[T0 实测定]）
  CatalogReplay watch + ctid 重放（分区 DDL 全走流内：CREATE/DROP/EXCHANGE/
    MERGE/SPLIT PARTITION 的字典维护，as-of 语义完整）
  CatalogStores 第四容器 + StateStore formatVersion 4
  CatalogSnapshot 身份链：partitionRelfilenode → parentid → 母表身份
    （列挂母表 oid，pg_attribute watch 已覆盖——T5 实测）

DML 捕获面：
  分区行输出 = 分区名（T0 采样终审；反转则归一化器映射）
  TableFilter 白名单双匹配；TableMeta 身份=分区名、列序=母表 pg_attribute
  TableMeta 失效钩子扩第四字典 heap 记录（四表到达即全清）

M2 遗留勾销：'p' 行从"有痕跳过"升级"正常捕获"（R1 临时语义退役——仅链断与
  DROP 后迟到行保留 fail-safe 有痕跳过）；T4 三 minor 按"列存场景待未来项"
  javadoc 记档关闭
```

**根因背景**（M2 T5 实测，勘误表在案）：OG 分区子表**不在 pg_class**（无行、无
pg_attribute 行；列挂母表 oid；分区行在 pg_partition；母表 relkind='r'+parttype 值域
恰 {'n','p'}）；heap INSERT 落**分区自身 relfilenode**（range/list 四锚 32809/32810/
32821/32822，母表零命中）；PG 档分区 DDL=OG 原生形态（vanilla `PARTITION OF` 拒）。

## 2. 组件明细

| 组件 | 包 | 动作 | 职责要点 |
|---|---|---|---|
| T0 实测锚定（首任务） | it/（探针） | 新增 | ①分区 relfilenode 列级出处（pg_partition 列集实查 + WAL 字节对照 + TOAST 关系挂分区/母表顺带）②基准路分区行命名采样（定归一化器形态）；结论进结论文档 |
| `WalLayoutVOpenGauss`/kinds | layout | 改动 | pg_partition 行解码词典（列集 live 推导 + 偏移）；分区 DDL 特殊记录形态（SPLIT/MERGE 段）盘点转录 |
| `CatalogBootstrap` | replay | 改动 | 第四表种子（同事务 `SELECT ctid, ... FROM pg_partition`）；interest 注册面扩四表 |
| `CatalogReplay`/`CatalogStores` | replay | 改动 | 第四 watched 腿（WatchedLeg 加 PARTITION；heap 事件 + PRUNE/INPLACE/截断重建按 pg_partition 列几何适配）；第四容器 + 序列化钩子 |
| `CatalogSnapshot` | api | 改动 | 身份链查询 `partitionOf(relfilenode)→{分区 relname, parentid}`；`relOidOf`/`schemaOf` 链路挂接（分区 relfilenode→母表 oid→母表身份） |
| `TableFilter` | changes | 改动 | 分区行解析（第四字典→母表身份）；白名单双匹配；TableMeta 构造=分区名身份+母表列序 |
| `XactGrouper` | changes | 改动（小） | TableMeta 失效第四调用点；M2 分区 miss DEBUG 路径退役为正常捕获 |
| `WalSource`/护栏 | api | 改动 | 白名单 {'PG','A'}；B 档 ISE 文案更新 |
| `StateStore` | state | 改动 | formatVersion 4（第四字典序列化；v3 拒载回落全新引导——既有语义） |
| `OgTestEnv` | it | 改动 | m2a 库升格 A 档全量场景库；IT 矩阵参数化（兼容档维度 PG+A） |
| `OgParityBaseline` | it | 改动 | 分区行命名归一化（按 T0：分区名直通 or 母表名映射+记档） |

**不动面**：双通道收流（档位无关）、XactGrouper 状态机主体、OutputRenderer、
DiskValueRenderer（Oracle 系类型同义词落同底层 OID——A 档复验实测出例外按排障修）。

## 3. 数据流

**启动**：判别 → 护栏（PG/A 放行）→ ensureSlot → state 决策（v4 续传/v3 拒载回落）→
bootstrap 四表种子（同事务首句 flush LSN 不变）→ 双通道收流 → walker → sink（不变）。

**分区行 DML**：heap 记录（分区 relfilenode）→ TableFilter.resolve：①三表字典直查
miss（预期）→ ②第四字典反查得 {分区 relname, parentid} → ③parentid→classRows 得母表
schema.表名、columnsOf(parentid) 得列序 → ④白名单双匹配 → XactGrouper 入桶（TableMeta
身份=分区名、列序=母表）→ 值渲染/TOAST/发射全复用（TOAST 关系挂分区/母表以 T0 实测，
挂分区则 ToastAssembler relfilenode 匹配面扩第四字典）。

**分区 DDL 竞速**：CREATE PARTITION→第四字典 INS upsert→该分区后续行即刻可解析
（as-of 同构三表 DDL）；DROP/EXCHANGE→字典行移除/换绑→迟到行按 miss 有痕跳过
（fail-safe）。

**检查点**：pendingFloorLsn 不变；四字典随 stores 全量序列化；单线程天然一致点不变。
**TableMeta 失效**：任一四表 heap 记录到达→全清（EXCHANGE 后旧列序缓存即时作废）。

## 4. 错误处理

**fail-fast**：B/'C' 档 ISE（附 M4 指引）；A 档列存表 ISE（ASTORE 限定）；第四字典
heap 记录列几何错位 ISE（附 LSN）；分区 DDL 未转录结构（SPLIT/MERGE 特殊段）ISE+记档
（T0 盘点记录族，未覆盖形态先 ISE）。

**降级/链断**：分区身份链断裂（第四字典命中但 parentid 在 classRows 无行——引导竞速
理论窄缝）fail-safe 有痕跳过（WARN 一次含 relfilenode/parentid + skipped 计数，沿 nsp
链断先例）；DROP 后迟到行同。**M2"分区 miss 有痕跳过"退役**为正常捕获（仅上述两形态
保留）。T4 三 minor（numstrMeta attlen 校验/DELTA bytes=0 dropped 语义/deltaBytes≤attlen）
按"列存场景待未来项"javadoc 记档关闭，不修代码。

**已知限制（文档化）**：B 档整库（M4）；列存/段页式/bucket/UStore 长期非目标（除非
呈报）；分区 boundary 变更的 UPDATE 跨分区迁移 = DELETE+INSERT 两行形态（vanilla 语义
自然捕获，专项场景钉行为）；pg_partition 截断重建未锚变体按语料实测裁剪记档。

## 5. 测试与验收

- **T0 双锚定**：结论文档（列级出处 + 命名采样 + TOAST 挂靠 + 分区 DDL 记录族盘点）。
- **离线**：OgPartitionDictionaryTest（种子/重放/v4 往返/PRUNE·INPLACE 施加）；身份链
  （解析/链断 fail-safe/白名单双匹配四形态）；TableMeta 失效（EXCHANGE）；护栏（A 放行/
  B·C ISE）；**既有全部用例断言零改动**。
- **IT**：A 档全量复验（v2 六场景+COPY+TOAST 于 m2a，参数化 PG+A 双库同套）；分区专项
  （range/list 捕获/白名单四形态/流内 CREATE·DROP 竞速/EXCHANGE 失效/跨分区迁移两行/
  基准路对拍 diff 空）；护栏（m2b B 档 ISE/A 档列存表 ISE）；回归（M1+M2+PG 17/18 全量）。
- **验收标准**：①A 档六场景+COPY 对拍全绿；②分区专项全绿且命名与基准路对齐；③B/C
  档与列存 ISE 钉死；④StateStore v4 续传；⑤既有用例零改动零回归；⑥M2 分区跳过语义
  退役有测试佐证。

## 6. 任务分解（writing-plans 展开骨架）

- T0 实测锚定（分区 relfilenode 列级出处 + 基准路命名采样 + TOAST 挂靠 + 分区 DDL
  记录族盘点——结论驱动后续任务的常量与归一化器形态）
- T1 第四字典三件套（layout kinds + bootstrap 种子 + replay watched 腿 + stores 容器
  + StateStore v4）
- T2 身份链与捕获面（CatalogSnapshot.partitionOf + TableFilter 双匹配 + TableMeta 构造
  + XactGrouper 失效钩子 + M2 跳过语义退役）
- T3 护栏放宽（A 档白名单 + B/C 文案）+ T4 遗留 minor 记档关闭
- T4 OgParityBaseline 分区归一化 + OgTestEnv A 档升格与矩阵参数化
- T5 A 档全量复验 IT + 分区专项 IT + 护栏 IT
- T6 文档同步与收口（模块/根文档、调研 M3 勘误回填、M2 已知限制勾销）

依赖序：T0 最先（锚定物喂 T1/T2/T4）；T1→T2；T3 独立可并行；T4 依赖 T0+T2；T5 收口；
T6 文档。

## 7. 配置与依赖变更

配置键零新增（白名单双匹配为既有 `vb.wal.tables` 的语义增强）；依赖零新增。
