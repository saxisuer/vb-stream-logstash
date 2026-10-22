# wal-source TRUNCATE DML 支持前置 spike——四档 XLOG_HEAP_TRUNCATE 记录形态锚定

**日期**：2026-10-22　**性质**：throwaway 探察（TRUNCATE DML 支持的先导锚定，不写产品代码）
**harness**：`spike/truncate/`（一次性，未进构建——`DumpTruncate.java` 段快照走读 / `LogicalFrameDump.java` 逻辑流帧采样 / `VastbaseTruncateSpike.java` 真库收流 + 工作负载 / `EngineTruncateSample.java` engine 路渲染采样；复用 `FileWalSource.staticDirectory` + `WalStreamWalker`/`WalRecordParser` + `WalSegmentRecorder`/`VastTestEnv` + `EnginePathRunner`/`ParityEnv.capture`，零自研协议栈）
**环境**：PG 18.6（src/docker compose）/ PG 17.11（postgres:17 独立容器）/ openGauss 5.0.0（独立容器，m2pg 'PG' 档库 + m2par_base 基准路角色——配方抄 `OgTestEnv`）/ VastBase G100 真库 `172.16.107.133:55433/test_a`（A 档）

## 结论速览与裁决表

**vanilla 17/18 与任务简报的源码预读有一处关键出入（勘误记档）**：`SizeOfHeapTruncate = offsetof(xl_heap_truncate, relids)` = **12 不是 9**——`Oid relids[]` 要求 4 字节对齐，`uint8 flags@8` 之后有 **3 个对齐垫字节**，实测为**未初始化栈内存（非零垃圾，无语义）**。relids 起点裁定 **@12**。OG 5.0.0 与 VastBase（A 档）**根本不写专用的 heap truncate 记录**——TRUNCATE 在 WAL 里纯以 catalog 手术表达（新段号 CREATE + pg_class/pg_partition UPDATE + FPI + LOCK + commit），基准路逻辑流也不发 'T' 帧（事务解码为空 B..C 块）。

| 档 | 记录面（wal 直解可见性） | 基准路逻辑流 | 裁决 |
|---|---|---|---|
| **vanilla PG 18** | RM_HEAP_ID=**10** opcode **0x30**；`main = [dbId u32][nrelids u32][flags u8][3B 未初始化垫][relid u32×n]`，**relids@12**；flags bit0=CASCADE / bit1=RESTART_SEQS；**仅 wal_level≥logical 时写**（源码 Assert） | engine 收 'T' 帧 = `I32 nrel + I8 bits + n×I32 oid`（DmlParsers 既有解析即覆盖） | **A（heap 0x30 同构可落码）** |
| **vanilla PG 17** | 与 18 **逐字节同构**（同一 mainLen 系 42/46/50；头文件 truncate 区两版逐字同——实锚 REL_17/18_STABLE） | 同上 | **A（17≡18）** |
| **openGauss 5.0.0** | **无任何专用 truncate 记录**（rmid=10 全窗零 0x30；census 实锚）；TRUNCATE = 纯 catalog 手术 | **不发 'T'**：TRUNCATE 事务解码为空 `B..C` 块（INSERT 对照正常 B/R/I/C）；分区母表 TRUNCATE 同样空块 | **C（记录面 + 帧面双 C）** |
| **VastBase G100（test_a，A 档）** | **无专用记录**（同 OG——rmid=10 全窗零 0x30）；VastBase 分叉：heap UPDATE main 前缀 21B、LOCK info=0x01、**rmid=30 DDL-deparse rmgr**（语句文本 JSON，仅 DDL，TRUNCATE 不产生） | 帧面采样**受阻**（真库逻辑槽 sticky-active 事故，见 §5.3——记录面已定，帧面按 OG 同内核推定 C） | **C（记录面实锚）** |
| **VastBase G100（B 档，test_b）** | **未直接采样，按内核同一性外推 C**——test_b 与 test_a 共享同一 OG 6.0.0 内核与 WAL 布局（`WalLayoutVVastbase`，M4 spike 已证 A/B 记录面零分叉），heap truncate 记录的存否是内核代码路径而非兼容档行为，A 档实测零 0x30 即 B 档同 | 同左（推定） | **C（外推）** |

**Task 6 落码的直接含义**：TRUNCATE DML 捕获是 **vanilla 17/18 档独有**能力；OG 系两档 TRUNCATE 对 wal 直解路与逻辑解码路**双双不可见**（不砖流、无痕），文档化即可、无需护栏 ISE。

---

## §1 vanilla PG 18（字节 + 源码双锚）

### 1.1 源码锚（REL_18_STABLE，curl 原文留档 `spike/truncate/heapam_xlog-18.h` + `tablecmds-18.c`）

```c
/* heapam_xlog.h —— 与 REL_17_STABLE 逐字同（truncate 区） */
typedef struct xl_heap_truncate
{
    Oid         dbId;
    uint32      nrelids;
    uint8       flags;
    Oid         relids[FLEXIBLE_ARRAY_MEMBER];
} xl_heap_truncate;
#define SizeOfHeapTruncate  (offsetof(xl_heap_truncate, relids))   /* = 12，非 9 */
```

写点在 **tablecmds.c `ExecuteTruncateGuts`**（非 heapam.c），四个关键事实：

1. **`xl_heap_truncate xlrec;` 是栈变量**——`flags u8@8` 后的 3 个对齐垫字节从未赋值，注册进 WAL 的是**未初始化栈内存**（实测非零：`e4 db 61` / `ee db 61` / `2c da 61` 等，逐记录不同、无语义）。
2. **两次独立注册**：`XLogRegisterData(&xlrec, SizeOfHeapTruncate)` + `XLogRegisterData(logrelids, n * sizeof(Oid))`——XLogInsert 顺序拼接无中间垫，main 连续 = 12 + 4n。
3. `Assert(XLogLogicalInfoActive())`——**wal_level ≥ logical 才写该记录**（与 wal-source DML 面既有前提一致）；`relids_logged` 只收 `RelationIsLogicallyLogged(rel)` 的关系（外键 CASCADE 展开的子表并入；FDW 外表不进）。
4. flags 语义：`behavior == DROP_CASCADE` → bit0；`restart_seqs` → bit1——**按语句声明置位**（无 owned 序列的 `RESTART IDENTITY` 也置 bit1，实测记录 3）。

**序列重启（PG 10+）不进 relids**：`ResetSequence()` 在 heap 记录**之前**写独立 RM_SEQ 记录（waldump 实锚：`rmgr: Sequence ... rel 1663/5/16444` 紧邻 heap TRUNCATE 记录之前）；`TRUNCATE t_ser RESTART IDENTITY` 的 heap 记录 `nrelids=1 relids=[16421]` **无序列尾随**——heapam_xlog.h "followed by all sequence relids" 注释是 pre-PG10 残留。

### 1.2 黄金字节（6 形态 + 分区 2 形态；`spike/truncate/pg18-dump-filter.log` / `pg18-waldump-truncate.log`）

被截表 oid：t_tr1=16398 / t_tr2=16403 / t_trp=16406 / t_trc=16412 / t_ser=16421 / seq=16420 / dboid=5。

| # | 语句 | lsn | tot | info | mainLen | main hex（已按字段分段） |
|---|---|---|---|---|---|---|
| 1 | `TRUNCATE t_tr1` | 0/01833948 | 42 | 0x30 | 16 | `05000000 01000000 00 e4db61 0e400000` |
| 2 | `TRUNCATE t_tr1, t_tr2` | 0/01834538 | 46 | 0x30 | 20 | `05000000 02000000 00 eedb61 0e400000 13400000` |
| 3 | `TRUNCATE t_tr1 RESTART IDENTITY`（无序列） | 0/01834ED8 | 42 | 0x30 | 16 | `05000000 01000000 02 eedb61 0e400000` |
| 4 | `TRUNCATE t_ser RESTART IDENTITY`（有序列） | 0/01835E28 | 42 | 0x30 | 16 | `05000000 01000000 02 2cda61 25400000` |
| 5 | `TRUNCATE t_trp CASCADE`（级联 t_trc） | 0/01836898 | 46 | 0x30 | 20 | `05000000 02000000 01 71da61 16400000 1c400000` |
| 6 | 回滚事务内 `TRUNCATE t_tr2`（xid 779 后 ABORT） | 0/01836C90 | 42 | 0x30 | 16 | `05000000 01000000 00 eedb61 13400000` |
| 7 | `TRUNCATE t_part_m`（分区母表；母 16455/p1 16458/p2 16461） | 0/020277B8 | 50 | 0x30 | 24 | waldump desc：`nrelids: 3, relids: [16455, 16458, 16461]` |
| 8 | `TRUNCATE t_p1`（直接截分区） | 0/02027B88 | 42 | 0x30 | 16 | waldump desc：`nrelids: 1, relids: [16458]` |

**@9/@12 字节裁决**（记录 1）：`@12 读法 relids=[16398]` 恰等于 `'t_tr1'::regclass::oid` ✓；`@9 读法=[241294308]`（把垫垃圾并进 oid 低 3 字节）✗。全部 6 条一致——**relids@12 成立**。几何自检：totLen = 24（记录头）+ 2（DATA_SHORT 终止头）+ 12 + 4n，n=1/2/3 → 42/46/50 全吻合。

flags 位实测矩阵：`[]`=0x00、`[CASCADE]`=0x01（记录 5）、`[RESTART_SEQS]`=0x02（记录 3/4）——与 pg_waldump desc 逐条对上（`pg18-waldump-truncate.log` 交叉验证）。

### 1.3 记录序（Task 6 的 as-of 语义依据；`pg18-window-dump.log` / `pg18-waldump-full.log`）

一个 TRUNCATE 事务（tx 774 = 记录 1）的完整 WAL 序：

```
Standby LOCK rel 16398 ─▶ Storage CREATE base/5/16430 ─▶ Heap2 NEW_CID ×2
─▶ Heap UPDATE pg_class(1259 blk0) flags=0x60（截断 UPDATE：relfilenode 换新行）
─▶ Btree INSERT_LEAF ×3（pg_class 三索引 2662/2663/3455）
─▶ （对 heap→toast(16401)→toast idx(16402) 各重复一轮 LOCK/CREATE/UPDATE pg_class/Btree）
─▶ XLOG FPI（新文件首页镜像）─▶ Transaction INVALIDATION
─▶ ★ Heap TRUNCATE（本记录，nrelids 汇总全部表 oid）
─▶ Transaction COMMIT
```

要点：**pg_class 换段号 UPDATE 先于 heap TRUNCATE 记录到达**——wal 路消费到 truncate 记录时字典已见新段号；TRUNCATE 记录的 relids 是**逻辑 oid**（非新段号，对照记录 1 relid=16398 而 CREATE 的是 16430）；回滚事务的 TRUNCATE 记录**在场**（记录 6）——XactGrouper 的 aborted 过滤天然覆盖。

---

## §2 vanilla PG 17（对照）

同 workload、独立 postgres:17 容器（wal_level=logical + restart）。6 条记录 totLen 与 18 **全等**（42/46 交替），hex 除 oid/垫垃圾外逐字段同构（`pg17-dump-filter.log`）：

```
TRUNCATE-REC lsn=0/01576D10 totLen=42 info=0x30 main=05000000 01000000 00 f8745b 04400000   (t_tr1=16388)
TRUNCATE-REC lsn=0/015778E8 totLen=46 info=0x30 main=05000000 02000000 00 726a5b 04400000 09400000
TRUNCATE-REC lsn=0/015782A0 totLen=42 info=0x30 main=05000000 01000000 02 716a5b 04400000   (RESTART 位)
TRUNCATE-REC lsn=0/015791F0 totLen=42 info=0x30 main=05000000 01000000 02 06755b 1a400000   (t_ser=16410)
TRUNCATE-REC lsn=0/01579BE0 totLen=46 info=0x30 main=05000000 02000000 01 1e755b 0c400000 11400000  (CASCADE)
TRUNCATE-REC lsn=0/01579FD8 totLen=42 info=0x30 main=05000000 01000000 00 716a5b 09400000   (回滚事务)
```

**17 ≡ 18 裁定成立**（头文件 truncate 区 diff 逐字同 + 字节全等）。pg_waldump desc 对拍见 `pg17-waldump-truncate.log`。

---

## §3 openGauss 5.0.0（裁决 C：记录面 + 帧面双缺席）

**采集**：独立容器 + m2pg（'PG' 档）+ 语句族（单表/多表/CASCADE/回滚事务/分区母表/`ALTER TABLE ... TRUNCATE PARTITION p1`）。`RESTART IDENTITY` 被 SQL 层直接拒绝：**`ERROR: PGXC does not support RESTART IDENTITY yet`**——OG 的 RESTART_SEQS 位经 SQL 不可达。段落盘经 docker cp `pg_xlog`（容器无 pg_xlogdump——walk 是唯一走读面）。

### 3.1 记录面（`og-window-dump.log`，census 见运行记录）

- **rmid=10 全窗（16 项 census）零 0x30**：heap 族只有 0x00/0x10/0x20/0x40/0x50/0x70（9.2 语义 0x50=NEWPAGE）。混淆源排除：rmid=9 的 0x30 ×15 条全部 `xid=0` 且载荷是 `[tsid][db][pg_class 段][时戳]` 的 catalog 维护记录（非 truncate）；rmid=0 的 0x30 = 9.2 XLOG NEXTOID。
- TRUNCATE 语句族的 WAL 表达 = **纯 catalog 手术**（首条 `TRUNCATE vb_tr1` 的完整窗口）：

```
Standby LOCK(rmid=8, main 尾携 [db 16392][rel 16393]) ─▶ SMGR CREATE(新段号) ─▶ NEW_CID(rmid=18)×2
─▶ Heap UPDATE pg_class(段 14313, info=0x20, main 13B OG 前缀) ─▶ Btree ×3
─▶ （toast/toast idx 各一轮）─▶ heap NEWPAGE FPI(info=0x50) ─▶ HOT_UPDATE(catalog)
─▶ XACT COMMIT(rmid=1, info=0x04, 数百字节 OG 私有大记录：CSN + inval 消息序列)
```

- 分区母表 `TRUNCATE vb_prm`：pg_partition 段（14318）行 HOT_UPDATE ×每分区 + pg_class UPDATE（母表行 blk1→blk0 迁移）——**同样零专用记录**。
- **裁决 C（记录面）**：wal 直解路在 OG 上无 TRUNCATE 信息源——rmid 0（老 XLOG rmgr）也不存在该记录（9.2 派生内核的 truncate 走 catalog 手术、无逻辑解码专用记录）。

### 3.2 基准路帧面（`og-baseline-frames.log`）

publication 显式 FOR TABLE + m2par_base 角色 + hba sha256 首行（`OgParityBaseline` 全配方）。同槽四事务对照：

```
INSERT 事务：FRAME B → R(public.vb_tr1, relId 0x4009=16393) → I('t' "9" / 't' "z") → C
TRUNCATE vb_tr1：FRAME B → C                      ← 无 'T' 帧，空块
INSERT 入分区：FRAME B → R(public.vb_prm, relId 0x402A=母表 oid —— PARTITION_NAME=mother 再证) → I → C
TRUNCATE vb_prm（分区母表）：FRAME B → C           ← 同样无 'T'
```

**裁决 C（帧面）**：OG pgoutput（proto v1）对 TRUNCATE 静默——事务块开合但零变更帧。与记录面 C 互为因果（无记录可解码）。

---

## §4 VastBase G100（裁决 C：记录面实锚；帧面受阻记档）

**采集**：真库 test_a（A 档）`VastbaseTruncateSpike`（`WalSegmentRecorder` DirectWalSource 裸 v3 收流落段 + vb_wal_ 前缀工作负载 + LSN 记号）；`RESTART IDENTITY` 被拒：**`ERROR: TRUNCATE .. RESTART IDENTITY is only supported in PG compatibility`**（与 OG 的拒绝文案不同——VastBase PG 档库理论可达，但真库无 PG 档库、未采样；记录面 C 使 flags 位问题失焦）。`ALTER TABLE vb_wal_prm TRUNCATE PARTITION p1` 语法可用。

### 4.1 记录面（`vastbase-window-dump.log`，50318 条全窗）

- **rmid=10 全窗零 0x30**（census：0x00/0x10/0x20/0x40/0x50/0x70）——与 OG 同为 catalog 手术表达，VastBase 分叉三处：
  1. heap UPDATE main 前缀 **21B**（xid u64 段后置——既有布局分叉，本次再证）；
  2. Standby LOCK `info=0x01`（OG 5.0.0 实测 0x00）；
  3. **发现 rmid=30 "DDL-deparse" rmgr**：`[dbId u32][tsid u32][len u64][relid u64]["deparse\0" + JSON 语句文本]`——CREATE/ALTER 语句全文落 WAL（DDL 审计面）。**TRUNCATE 语句不产生 rmid=30 记录**（11 条全为 DDL 事务），不能作 TRUNCATE 恢复通道。
- 分区母表 TRUNCATE：pg_partition 段（19833）HOT_UPDATE ×分区 + pg_class UPDATE——零专用记录。

### 4.2 裁决与残余

- **记录面 C 实锚**（与 OG 同构的 catalog 手术 + 零 heap truncate 记录）。
- **帧面（'T' 采样）受阻**：真库逻辑槽子系统在采集过程中进入 sticky-active 态（事故全过程与处置见 §5.3），同内核推定与 OG 同为不发 'T'；标记为**未复验面**，可由控制器后续补。
- **B 档（test_b）外推**：B 档未直接采样，按**内核同一性外推裁决 C**——test_b 与 test_a 共享同一 OG 6.0.0 内核与 WAL 布局（`WalLayoutVVastbase`；M4 spike 已证 A/B 两档记录面零分叉），heap truncate 记录的存否属内核代码路径而非兼容档行为（兼容档只改 SQL 解析/类型面），A 档实测零 0x30 对 B 档同样成立。

---

## §5 engine 路渲染契约样本（Task 6 对拍锚的契约源）

### 5.1 ConsoleRenderer 行字面（PG 18，proto 4 + streaming OFF；`engine-sample.log`）

```
TXN-BEGIN xid=804 kind=NORMAL gid=null commitLsn=0x3051ae8 commitTs=... changes=1
  [1] TRUNCATE [public.t_tr1] options=[]
TXN-END   xid=804
  [1] TRUNCATE [public.t_tr1, public.t_tr2] options=[]
  [1] TRUNCATE [public.t_trp, public.t_trc] options=[CASCADE]
  [1] TRUNCATE [public.t_eng_p, public.t_eng_c] options=[RESTART_IDENTITY, CASCADE]
  [1] TRUNCATE [public.t_p1, public.t_p2] options=[]
```

（TRUNCATE 事务 `changes=1`——一条语句一个变更单元，无论 nrelids。）

### 5.2 两个契约级发现

1. **双位 options 的 toString 序非确定**：`TruncateChange` 紧凑构造器对 options 做 `Set.copyOf`（EnumSet → `ImmutableCollections.SetN`），迭代序 = 身份哈希探查序——本次实测 `[RESTART_IDENTITY, CASCADE]`（恰与枚举声明序相反），**JVM run 间可漂移**。Task 6 的对拍矩阵对双位形态要么归一化排序后比、要么刻意避开双位场景（建议：wal 侧渲染定 `[CASCADE, RESTART_IDENTITY]` 声明序 + parity 断言对该形态做序归一）。
2. **分区母表被服务端从 'T' 帧剔除**：WAL 记录 relids=`[mother(16455), p1, p2]`（§1.2 记录 7），而 engine 渲染 `[public.t_p1, public.t_p2]`——母表无 'R' 关系帧（pgoutput 不发布 relkind='p' 母表行），`BucketReplayer` 的 relationOids→snapshot.require 链路只看到叶子。**wal 路 Task 6 渲染必须同样滤掉 relkind='p' 的 relid**（TableFilter 面已有 relkind 判别可复用），否则 TRUNCATE parity 必 diff。

### 5.3 VastBase 真库 sticky-slot 事故记档（运维面，非 spike 结论面）

首个 VastBase 帧面采样 JVM 被会话级 6m40s 强杀 → 服务端 walsender 僵尸（pg_stat_replication 零行、pg_stat_activity 不可见）→ 此后**新建逻辑槽即 `active=t` 且 attach/drop 一律 "already active"**（含从未附着过的新槽——槽子系统记账被毒化）。已处置：本 spike 的表/publication 全部 DROP、槽 `vb_wal_spike_trunc_slot` 成功删（后续重试删掉）；**残留 4 槽 `vb_wal_spike_tr2/tr3/tr4/tr5_slot`（restart_lsn 2/11553AC8 / 2/11556608 / 2/115584E8 / 2/11559548）暂不可删**——需真库侧清僵尸（wal_sender_timeout 到期/实例重启窗口）。钉住的 WAL 窗口当前很小（约数页），但随业务写入会累积（受 max_slot_wal_keep_size 兜底）。**教训**：真库逻辑流 harness 不可被外层超时硬杀——收流循环必须自带超时退出（本 spike `LogicalFrameDump` 已自带，事故源是被工具强杀的第一次调用）。

---

## §6 Task 6 落码依据清单（自包含，实现者读本节即可动工）

1. **记录识别**（vanilla 17/18）：`rmid==10 && (info & 0x70)==0x30`；main 解析：`dbId=u32@0`（可校验当前库 oid）、`nrelids=u32@4`、`flags=u8@8`、`relids[n]@12 起 u32×n`——**@9-11 是未初始化垫字节，禁止读、禁止校验为零**。
2. **几何不变量**：`mainLen == 12 + 4*nrelids`（可作错位自检）；记录无 block 引用、无 FPW。
3. **事务组装**：TRUNCATE 记录 xid 归桶（作为 1 个变更单元，engine `changes=1` 同口径）；aborted 事务的 TRUNCATE 记录在场——既有 aborted 过滤天然覆盖；记录先于 COMMIT 到达、晚于 pg_class 换段号 UPDATE（消费时字典已是新段号——TableMeta 失效钩子需覆盖 pg_class UPDATE，既有机制应已承载，Task 6 加测试钉死）。
4. **渲染**（对齐 engine 契约源 §5.1）：行字面 `  [N] TRUNCATE [schema.t1, schema.t2] options=[...]`；options 文本 `[]` / `[CASCADE]` / `[RESTART_IDENTITY]`（双位序见 §5.2-1 裁定）；**relids 含 relkind='p' 分区母表时滤除（engine 'T' 帧面母表缺席）**；relid→表身份经字典 as-of 解析（oid 键控，非段号）。
5. **前提**：wal_level≥logical（记录只在 logical 档写——与 DML 面既有前提一致，无需新增护栏）；replica identity 无关（TRUNCATE 无元组）。
6. **OG/VastBase 档**：branch C——零代码、零护栏；README/模块文档记档"TRUNCATE 对 wal 路不可见（服务端无记录、逻辑流不发帧），事务块表现为空块或无块"。
7. **对拍（DualPathParityIT 增场景）**：单表/多表/CASCADE/RESTART（含序列重启事务——RM_SEQ 记录对 wal 路透明）/分区母表（断言只出叶子）/回滚事务零输出。

## 附录：harness 与复现

- `DumpTruncate <filter|window|census> <segDir> <v17|v18|og|vastbase> <fromLsn> <endLsn> [rmid]`——`FileWalSource.staticDirectory` 读尽 + `WalStreamWalker` 走读；filter 模式含 @9/@12 双解读。段目录获取：容器 `docker cp`（pg_wal/pg_xlog）；真库 `VastbaseTruncateSpike`（DirectWalSource 落段）。
- `LogicalFrameDump <jdbcUrl> <user> <pass> <appName> <slot> <publication> <expectedCommits> <timeoutMs>`——`OgParityBaseline` 连接形态（proto v1 + 活性护栏）逐帧 hex；'T' 帧按 vanilla 布局试解读。
- `EngineTruncateSample`——`EnginePathRunner` + `ParityEnv.capture` 收 engine CDC 行（需 `--add-opens` 清单同 engine Main）。
- workload SQL 与全部原始 dump 留档 `spike/truncate/`（pg18-workload.sql / pg18-*waldump*.log / pg18-dump-filter.log / pg17-* / og-* / vastbase-* / engine-sample.log；REL_17/18 源码件 heapam_xlog-17.h / heapam_xlog-18.h / tablecmds-18.c）。
