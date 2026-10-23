# vb-stream-wal-source TRUNCATE DML 支持设计（XLOG_TRUNCATE 解析与事件发射）

日期：2026-10-22　状态：已实施（2026-10-23）
前置：spike 报告 `docs/wal-source-truncate-spike.md`（**本 spec 的 Task 0**——四档记录形态锚定，未先行；spec 设计面与 spike 结果解耦，全部字节级常量以 spike 锚为准回填）

> **勘误（2026-10-22 实施期源码实锚，REL_17/18_STABLE `src/include/access/heapam_xlog.h` 逐字）**：设计期"XLOG rmgr + RelFileLocator 键数组"的推断**整体作废**——`XLOG_HEAP_TRUNCATE` 是 **RM_HEAP_ID 下的 opcode 0x30**（17/18 同值，落在 heap opcode 块 DELETE 0x10 与 HOT_UPDATE 0x40 之间），载荷 `xl_heap_truncate{Oid dbId; uint32 nrelids; uint8 flags; Oid relids[]}`——**键是 oid 数组**、flags u8 **前置于** relids（`XLH_TRUNCATE_CASCADE` 1<<0 / `XLH_TRUNCATE_RESTART_SEQS` 1<<1；RESTART 位时 relids 数组尾随待重启序列 oid——解析面只消费前 nrelids 个）。推论：该记录现状正落在 `XactGrouper.dispatchHeap` 的 `default` 分支被静默忽略，修复落点从"onRecord 新增第四臂"**缩为 dispatchHeap 头部加一个 case**（须置于 blocks-empty 早退之前——truncate 记录零块引用）。"XLOG rmgr 老形态"的记忆源自 pre-PG12（9.2 系），恰是 OG/VastBase 内核待 spike 的分叉面。本文各节已按实锚改写。

> **落地状态与勘误回填（2026-10-23 收口，Task 0-8 完成于分支 feat/wal-source-truncate）**：本 spec 已按
> spike `docs/wal-source-truncate-spike.md`（Task 0）四档实测裁定实施完毕——TRUNCATE 事件面落在
> vanilla PG 17/18 档（`TruncateView`[layout] → `XactGrouper.dispatchHeap` 头部 case[先于 blocks
> 早退] → `TableFilter.resolveByOid` 三闸 + relkind='p' 母表滤除 → sealed `BucketEntry` 桶泛化 →
> `TruncateEvent`/`onTruncate` 契约 → `OutputRenderer` engine 逐字节同形；观测面 `emittedTruncates`
> 独立计数 + Main 冒烟行 `trunc=`）；OG 5.0.0 与 VastBase 档零代码零护栏（行为记档）。**下表勘误**为
> spike 实测对本文若干设计期断言的修正（原段落保留作历史，以表为准）：

| 设计期断言 | spike 实测裁定 | 依据 |
|---|---|---|
| §2.1 relids 起点 @9（"`SizeOfHeapTruncate = 9`，@9/@12 以 spike 黄金字节定"）+ RESTART 位时 relids 尾随序列 oid（本文文首勘误块同句） | **relids@12**——`Oid relids[]` 4 字节对齐使 `SizeOfHeapTruncate = offsetof(relids) = 12`，@9-11 是**未初始化栈垫**（实测非零垃圾，禁止读/禁止校验为零）；**无序列尾随**——序列重启（PG 10+）走 heap 记录**之前**的独立 RM_SEQ 记录，heapam_xlog.h "followed by all sequence relids" 注释是 pre-PG10 残留；解析面以 `mainLen == 12 + 4×nrelids` 精确等式自检（落地 `HeapViews.TruncateView`） | spike §1.1/§1.2 黄金字节（@12 读法 oid 恰合、6 条全一致、17 同构逐字节同） |
| §0 "**全档支持**：V17/V18 + OG 5.0.0 + VastBase 一次到位"（用户裁定被服务端事实反转，非选择变更） | **TRUNCATE 捕获收缩为 vanilla 17/18 独有**——OG 5.0.0 与 VastBase（A 档实锚 / B 档按内核同一性外推）**裁决 C 双路不可见**：WAL 纯 catalog 手术（rmid=10 全窗零 0x30，无专用 heap truncate 记录）+ 逻辑流不发 'T' 帧（TRUNCATE 事务解码为空 B..C 块）——**正式行为锚而非缺陷**，零代码零护栏记档；`RESTART IDENTITY` OG 系两档 SQL 层均拒绝（`PGXC does not support RESTART IDENTITY yet` / `only supported in PG compatibility`）；§3.4 分区身份表的 OG/VastBase 行随裁决 C 退役（不适用——两档根本无 TRUNCATE 事件面），§5 的 OG/VastBase IT 前提（放开 'T' 拒绝并归一化）随之作废——Task 6/7 重塑为"零事件不砖流 + 截后 DML 字典正确"语义钉 | spike 裁决表 + §3/§4 census 实锚（OG 16 项 / VastBase 50318 条全窗）+ OG 帧面采样（VastBase 帧面受阻按同内核推定，未复验面记档） |
| 写点未锚（heap 记录族按惯例推 heapam.c 写出） | 写点是 **tablecmds.c `ExecuteTruncateGuts`**（非 heapam.c）；`Assert(XLogLogicalInfoActive())`——**仅 wal_level≥logical 时写该记录**（与 DML 面既有前提一致，无需新增护栏）；flags 按语句声明置位（无 owned 序列的 `RESTART IDENTITY` 也置 bit1）；截断的 pg_class 换段号 UPDATE **先于** heap truncate 记录到达——消费时字典已见新段号而 relid 是逻辑 oid，解析不受段号代际影响 | spike §1.1/§1.3 源码实锚（REL_17/18_STABLE，原文留档 `spike/truncate/heapam_xlog-17/18.h`、`tablecmds-18.c`） |

背景：DML 事件面现状只有 INSERT/UPDATE/DELETE（`ChangeOutputListener.DmlKind` 三值）——`XLOG_TRUNCATE` 记录（XLOG rmgr，非 heap）对 `XactGrouper` 是 no-op，**静默零输出**（无事件、无 WARN、无计数）；engine pgoutput 路对 'T' 消息全链路支持（`PgOutputMessage.Truncate` → `TruncateChange` → `ConsoleRenderer` 渲染），两路输出在 TRUNCATE 语句上分叉且从未被对拍暴露（v2 六场景无 TRUNCATE 场景）。

## 0. 范围裁定（用户确认，2026-10-22）

- **全档支持**：V17/V18 + OG 5.0.0 + VastBase 一次到位（`OgCompatModeGuard` 白名单档位面不涉及——TRUNCATE 是记录族扩展不是新兼容档；B/'C' 档护栏语义不变）
- **零新增配置键**：TRUNCATE 随 `vb.wal.dml` 开关与 `vb.wal.tables` 白名单走——对齐 engine 本体行为（`ConsoleRenderer` 对 truncate 无条件渲染；`skipped.operations` 是 Debezium 连接器生态的键，不是 wal-source 契约源）
- **方案 A 双裁定**：记录识别归 layout 层（`TruncateView` 视图归一四档差异）+ 事件走桶（到达期解析入桶、提交期批量发射、结构化 `onTruncate` 回调）
- **分区身份跟基准路**：不自行裁定——vanilla 档 payload 即分区段号、报各分区自身身份（与 pgoutput `publish_via_partition_root=off` 缺省一致）；OG/VastBase 经 M3 第四字典身份链报母表身份；以 spike 基准路实测为准，分叉则记档裁定（§3.4）

## 1. 架构总览

TRUNCATE 只动**三个面**（布局识别 / 事务组装 / 契约与渲染），catalog 同步面、TOAST、判别、检查点格式（StateStore 不加字段）、离线回放装配零改动：

```
WalRecordParser → WalRecord（既有，零改动）
  └ XactGrouper.dispatchHeap 头部 case：opcode 0x30（XLOG_HEAP_TRUNCATE）——
    现状正落在该分派的 default 分支被静默忽略；case 置于 blocks-empty 早退之前
      └ TruncateView.parse(rec)（layout 包——四档载荷几何归一，OG 系经 layout 分派）
          └ appendTruncateRecord：逐 oid resolveByOid → 过滤后表集 → BucketEntry 入桶
              └ emitBucket：按 recLsn 序发射 → out.onTruncate(TruncateEvent)
                  └ OutputRenderer：TRUNCATE [public.t1, ...] options=[...]（engine 逐字节同形）
```

**为什么 TRUNCATE 不能旁路即时发射**：TRUNCATE 是事务性的（可回滚、可 2PC 挂起、参与 at-least-once 重发），即时发射会把回滚的 truncate 泄给下游且与 engine 桶内次序错位——走桶后回滚丢弃/子回滚扣减/2PC 挂起续传/检查点 floor/离线回放/重发幂等**全部由既有桶机制白拿，零新增状态机**。

## 2. 组件明细

### 2.1 布局层（layout 包）

**`HeapOps` 增常量**（vanilla 侧已源码实锚）：`XLOG_HEAP_TRUNCATE = 0x30`（RM_HEAP_ID 下 opcode 位段）、`XLH_TRUNCATE_CASCADE = 1<<0`、`XLH_TRUNCATE_RESTART_SEQS = 1<<1`；OG 系常量（`OG_*` 前缀，沿 `OG_XLOG_XACT_*` 惯例）由 spike 锚定后入位——若 OG 系与 vanilla 同为 heap 0x30 同构则直认 vanilla 常量，若为 9.2 老 rmgr/几何则 layout 门控分派（沿 `OG_XLOG_HEAP2_CLEAN` 撞号门控先例）。

**`HeapViews` 增 `TruncateView`**（嵌套 record，`parse(rec, layout)` 同既有视图形态——rmid/info 错配 ISE）：

- 归一输出 `(int nrelids, long[] relids, int flags)`：relids 是 **oid 数组**（vanilla 源码实锚）——喂 `TableFilter.resolveByOid`（§2.2 新增的 oid 直查入口）；flags 原样位图
- vanilla 几何（REL_17/18_STABLE 实锚）：main = `[dbId u32@0][nrelids u32@4][flags u8@8][relid u32 × n @9 起]`——relids 起点偏移 9 与 12（XLogRegisterData 逐 chunk MAXALIGN 垫）的取舍以 spike 黄金字节定，常量单点可调；`SizeOfHeapTruncate = offsetof(relids) = 9`
- OG/VastBase 几何：spike 锚定（键类型/头部布局/flags 位序/rmgr 归属全开放），视图内按 layout 分派吸收
- 走读防御：`mainLen < 9` 或 `9 + 4×nrelids > mainLen` 抛 ISE（既有视图契约）；RESTART 位时 relids 尾随的序列 oid 不消费（只读前 nrelids 个）

### 2.2 changes 层（`XactGrouper`）

- **`dispatchHeap` 头部增 case**（`XactGrouper.java:697` 起）：`op == XLOG_HEAP_TRUNCATE` → `appendTruncateRecord(rec)` 后返回——**case 置于 `rec.blocks().isEmpty()` 早退之前**（truncate 记录零块引用，现状该早退与 default 分支双重吞掉它）；OG 系若 spike 判定归 XLOG rmgr，则在 `onRecord` 增 OG 门控臂（代码骨架同型，Task 6 按 spike 落）
- **`TableFilter.resolveByOid(long oid)` 新增**：oid 直查三闸面（relkind/系统模式/白名单）——**不经 `relOidOf` 的 relfilenode 反查**（反查优先在 EXCHANGE 换绑段号/oid 数值碰撞面会产出错误属主，M3 T5 已证该碰撞类真实存在；truncate 载荷键已知是 oid，服务端 decode.c 语义即 oid 直开关系）。`resolveThreeTable` 的 relOidOf 后半段 refactor 为本方法复用（行为零变化）；OG 档 partitionOf 属主链不进本入口（OG truncate 键形态由 spike 定，§3.4）
- **`appendTruncateRecord(top, origin, rec, view)`**：逐 oid `filter.resolveByOid` → 过滤后命中表集（**保持 payload 序**，对齐 engine `relationOids` 序）→ **全 miss（nrelids>0 但零命中）零记账零追加**（对齐 pgoutput 服务端语义：`pgoutput_truncate` 按 publishable 逐关系过滤后 `nrelids==0` 不发 'T' 帧——REL_STABLE 源码面，spike 复核）；任一命中即入桶——桶不存在则新建并记 `firstLsn`（truncate-only 事务的 pendingFloorLsn 覆盖）。truncate 记录低频，**不走 metaCache**（键空间与 filenode 键混入会碰撞，且 CatalogStores 五索引 O(1) 查询本就廉价）
- **桶条目泛化**：`RowEntry` 收敛为 sealed 接口 `BucketEntry`（公共面 `originXid()/recLsn()`），行与 truncate 两实现——`emitBucket` 的 recLsn 稳定排序跨两类保真次序（DML 与 truncate 混排按 WAL 序）、aborted 子事务扣减对 truncate 同样生效（子事务的 truncate 随子回滚剔除，对齐 engine reorderbuffer 语义）、位址三字段发射期重建同构（`XactGrouper.java:1250-1256` 惯例）

### 2.3 契约扩展（`ChangeOutputListener`）

- 嵌套 record `TruncateEvent(long xid, long commitLsn, long lsn, List<TableMeta> tables, Set<TruncateOption> options)` + 两参便捷构造（tables/options，位址三字段占位 0——手造事件单测面，同 `RowChange` 四参惯例）；紧凑构造器 `tables` 经 `List.copyOf`、**`options` 经 `EnumSet.copyOf` 拷贝**——EnumSet 迭代序恒为声明序（渲染 toString 与 engine 逐字节同形的保证；`Set.copyOf` 的迭代序无规格保证，不可用）
- 自有枚举 `TruncateOption { CASCADE, RESTART_IDENTITY }`（**声明序=engine `TruncateOption`**，刻名同源保证 EnumSet.toString 渲染逐字节同形；不 import 引擎——`DmlKind` 同惯例）
- 抽象回调 `void onTruncate(TruncateEvent event)`（Begin/End 之间按桶内序）——实现方仅 `OutputRenderer` + 测试 harness（`WalCapture` 等对拍工具）两处，同步改
- 回调语义：与 `onRow` 同缝（回放线程、Begin 后 End 前、按 WAL 序）

### 2.4 渲染（`OutputRenderer`）

`onTruncate` 渲染与 engine `ConsoleRenderer.java:148-151` **逐字节同形**：

```
TRUNCATE [public.t1, public.t2] options=[]                      ← 普通多表
TRUNCATE [public.t1] options=[CASCADE, RESTART_IDENTITY]        ← 选项位
```

- 表集 = `tables` 流映 `schema.table` 后 List.toString（engine `tableOf` 同构）；`options` 用同声明序枚举集合 toString——`Set.copyOf`/`EnumSet` 形态不敏感（toString 只依赖迭代序=声明序）
- 值截断阈值（`vb.wal.render.truncate`）不适用——truncate 无值面；dropped 列/TOAST 面不涉

### 2.5 观测面

- `XactGrouper.emittedTruncates()` 计数——**不并入 `emittedRows`**（行口径保持纯净，Main 冒烟行 `dml=[buckets=N rows=M partMiss=K trunc=T]` 追加 `trunc=`）；经 `ChangeStream`/`WalSource.dmlTruncates()` 透传（沿 `dmlRows` 惯例）
- `expectedChanges` 记账**含 truncate 条目**（对齐 engine 桶 unitCount——'T' 消息是一个变更单元）；对拍归一化式 `engine.changes==行数 && wal.changes≥行数` 两路同步成立（truncate 行两路都计入行数）

## 3. 语义与边界

### 3.1 过滤语义（per-table）

白名单/relkind/系统模式三闸逐表照走（`TableFilter` 零改动复用）——与 pgoutput 服务端 per-relation publishable 过滤同构：多表 truncate 部分命中 = 事件只携命中表（engine 'T' 帧同形）；全 miss = 无事件（§2.2）。OG 档白名单双匹配（母表名/分区名）沿用 M3。

### 3.2 事务语义（桶机制白拿清单）

回滚整桶丢弃（`onAborted` 零输出）/ SAVEPOINT 子回滚扣减 / 2PC 挂起零发射·弃桶·COMMIT PREPARED 发射·挂起重启续传 / `pendingFloorLsn` 重放下界 / 离线回放档（VastBase）/ at-least-once 重发幂等（重放段 truncate 随桶重发）——**零新增状态，全部由既有桶路径承载**；`resetToastWindow` 不涉（truncate 无行值面）。

### 3.3 fail-safe 与防御

- payload 键字典 miss（DROP 后迟到 truncate/引导窗口噪声）→ 该表静默丢弃（对齐行 miss 噪声常态，不告警不计数——与 partitionChainMisses 的有痕面不同性质：那是身份链**断裂**，这是正常过滤）
- 载荷走读越界/nrelids 与键宽乘积越界 → ISE fail-fast（既有视图契约）
- 未知 flags 位忽略（只认 spike 锚定的 CASCADE/RESTART_IDENTITY 位——服务端未来加位不砖流）
- 主表（relkind 'p'）零物理段——payload 不会含母表键（vanilla）；若 spike 实测出现母表键（OG 系），经 resolve 闸自然消化

### 3.4 分区身份（跟基准路）

| 档 | payload 键 | 输出身份 | 基准路依据 |
|---|---|---|---|
| V17/V18 | **oid 数组**（源码实锚；分区与母表各自身 oid——母表 relkind 'p' 三闸放行） | 逐 oid 解析身份（payload 序全量入列） | pgoutput 'T' 帧报哪些 oid（含不含母表）由 spike ⑤ 采样——两路同源 payload，对拍 IT 钉死一致性 |
| OG/VastBase | 分区键（oid/段号，spike 锚） | **母表身份**（M3 身份链：`resolve` 第四字典属主优先）——预期单一事件携母表×1 还是按分区数重复，**spike 基准路定** | OG pgoutput 'T' 帧行为未采（`OgParityBaseline.Normalizer` 现显式拒绝 'T' 帧） |

分叉处理：若 OG 基准路报分区自身身份 → wal 路跟随（`resolve` 输出面加旁路或归一化器映射，取舍在 spike 后定）；若 OG 基准路不发 'T' 帧（能力缺席）→ OG 档验收退化为 wal 路字面锚 + 记档（沿 uuid/numeric 方言先例），vanilla/VastBase 面不受影响。

## 4. spike 任务（Task 0，`docs/wal-source-truncate-spike.md`）

四档各抓真 TRUNCATE 的 WAL 记录 + 基准路行为（vanilla：既有 Testcontainers postgres:18/17；OG：og-plan 容器；VastBase：真库 172.16.107.133）：

1. **记录头几何**：vanilla 已源码实锚（RM_HEAP_ID / opcode 0x30 / 零块引用）——spike 仅字节复核；OG/VastBase 全待锚（rmid 归属[heap 0x30 同 vanilla？还是 9.2 老 rmgr]/info 值/头几何）
2. **载荷布局**：vanilla 已实锚（`dbId@0/nrelids@4/flags@8/relids@9 起`——relids 起点 @9 vs @12 的 XLogRegisterData MAXALIGN 缝以真实字节定）；OG/VastBase 键类型（oid/段号）、flags 位序待锚
3. **次序锚**：OG 系 pre-PG12 换 relfilenode 语义下 truncate 记录与 pg_class 变更记录的相对 WAL 次序——若 OG 载荷是 oid 则免疫此面；vanilla（PG 12+ 不换段号）无此面
4. **基准路 'T' 帧行为**：OG/VastBase pgoutput 发不发、发什么身份（母表 or 分区）、options 位、多表数组序
5. **形态矩阵**：单表/多表一语句/分区母表/RESTART_IDENTITY/CASCADE/事务内与 DML 混排/回滚后（记录在场桶丢弃）/2PC
6. **engine 路对照**：postgres:18 上 engine 'T' 渲染字面采集（对拍锚的契约源样本）

产出：常量与几何回填 §2.1 表、§3.4 裁定、黄金字节进离线测试。

## 5. 测试与验收

- **离线**：`TruncateViewTest` 新增（四档黄金字节——spike 采集；错配 rmid ISE/越界 ISE 负例）；`XactGrouperTest` 增（入桶与 expectedChanges 记账/回滚整桶弃/子回滚扣减/白名单 per-table 部分命中/全 miss 零记账/truncate-only 事务 floor 参与位/与 DML 混排按 recLsn 序）；`OutputRendererTest` 增（渲染字面锚——`TRUNCATE [public.t1] options=[]` 与选项位形态）；`ChangeStreamTest` 增（onTruncate 契约透传）
- **IT**：`DualPathParityIT` 增 TRUNCATE 场景（postgres:18——基础单表/多表一语句/选项位/回滚零输出/与 DML 混排次序/分区母表多分区——对拍器 parse 面认 truncate 行）；`OgDmlParityIT` 增（PG/A 双档参数化——前提 spike ④ 放开 `OgParityBaseline.Normalizer` 的 'T' 拒绝并归一化）；`VastDmlParityIT` 增（真库手动档）；VastOfflineIT 面由"在线产生 truncate → 离线重放对拍"自然覆盖（离线档 grouper 零分叉）
- **验收口径**：全档"支持 = 双路对拍验收过"（模块纪律）；OG 档若基准路能力缺席则按 §3.4 退化口径记档
- vanilla PG 17 档：TRUNCATE 记录几何与 18 同源则由离线字节锚 + 既有 17 IT 不动覆盖（沿 DML 面 17 档既有口径）；spike 顺带在 17 容器采一份对照

## 6. 已知限制与非目标

- **TRUNCATE ... ONLY？**——PG 无 `TRUNCATE ONLY`（分区母表截断恒 CASCADE 至分区）；OG 系若有私有形态，spike ⑤ 覆盖，不预设
- **物化视图/序列截断**：非 relkind 'r'/'p'，三闸 miss 静默丢弃（防御面自然覆盖）
- **UNLOGGED/TEMP 表**：不入 WAL 逻辑面，天然不涉
- **值面**：truncate 无值渲染，`RenderOptions` 四键全部不涉
- **StateStore/检查点格式零改动**：truncate 桶条目是内存态，floor 语义由既有 `dmlFloorLsn` 承载
- 未来服务端新增 flags 位：忽略语义（§3.3），位值锚定期（大版本升级）随布局档纪律重锚

## 7. 相关文档

- 契约源：engine `ConsoleRenderer.java:148-151`（事务块渲染）/ `243-246`（逐消息版）；`BucketReplayer.java:160-164`（'T' → TruncateChange 逐 oid 快照）；`PgOutputMessage.Truncate`/`TruncateOption`
- 接缝现状：`XactGrouper.java:324-337`（rmid 三路分发）、`1162-1173`（appendRow）、`1238-1262`（emitBucket）；`TableFilter.resolve`（`relOidOf` 双入口归一）；`ChangeOutputListener.java`（契约）
- 里程碑序列：v2 DML 面 → M2/M3/M4（OG/VastBase 兼容档）→ **本 spec = DML 事件种类补全（TRUNCATE）**
