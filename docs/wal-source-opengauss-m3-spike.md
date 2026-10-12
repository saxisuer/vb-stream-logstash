# openGauss M3 分区捕获实测锚定（spike 结论）

> **PARTITION_NAME=mother**（基准路逻辑流对分区行报**母表名**——预期"分区名"被实测反转，
> vanilla 缺省语义不适用于 OG pgoutput；T4 归一化器分派与 wal 路表名渲染均按母表口径）

- 日期：2026-10-13；容器 `og-plan`（opengauss/opengauss:5.0.0，端口 15700，本 spike 开始时处于停止态、已重新拉起并保持运行）；探针代码 throwaway 在 /tmp/ogprobe（M3WalDump/M3Tuple/M3TupleUpd/M3Toast/M3LogicalName 等，不入库）
- 方法：WAL 面直接读服务端 `pg_xlog/000000010000000000000003` 段文件（og-plan 内
  `pg_receivexlog` sidecar 实测**空闲即断**——rx.log `ReceiveXlogStream failed.`，仅落
  启动后 ~0x80B；M1 file 通道的监督重连在长窗口场景必要，本 spike 改直读段文件绕开），
  经 `WalLayoutVOpenGauss.INSTANCE` + `WalStreamWalker` 全窗走读（13937 条、resyncs=0），
  对 pg_partition（物理 relfilenode **14318**，oid=9016）按已知值锚定；逻辑面沿 M2 spike
  路②（opengauss-jdbc `logical()` proto v1 + 专用 publication/slot）。

测试对象（m2pg 库，OG 原生分区语法）：`t_m3_range(id,name,big)` RANGE 两分区
p1/p2、`t_m3_list` LIST 两分区 pl1/pl2；大字段列 `big text`。

## 1. 锚① pg_partition 列集与分区 relfilenode 来源列

**结论：`pg_partition` 一张表自带分区 relfilenode（第 7 列 `relfilenode` oid 型，数据区
偏移 **+84**），无任何伴生结构——OG 5.0.0 **不存在** `pg_partition_fn`/`pg_partition_enum`
（`\d` 全目录扫仅 pg_partition + 5 个索引），watched 面只需增 pg_partition 一张。**

32 列全集（pg_attribute 实查，attnum/列名/类型）：

| attnum | 列 | 类型 | 数据区偏移 | 备注 |
|---|---|---|---|---|
| 1 | relname | name(64) | 0 | 定长 64B |
| 2 | parttype | char | 64 | **'r'=母表行 / 'p'=分区行**（与 pg_class.parttype 值域不同语义：这里区分行身份） |
| 3 | parentid | oid | 68 | **母表 pg_class oid**（母表行=自 oid） |
| 4 | rangenum | int4 | 72 | |
| 5 | intervalnum | int4 | 76 | |
| 6 | partstrategy | char | 80 | 'r'/'l' |
| **7** | **relfilenode** | **oid** | **84** | **分区物理段号（=分区自身 oid，INSERT 时同值；EXCHANGE 后换源表段号）**；母表行恒 0 |
| 8 | reltablespace | oid | 88 | |
| 9 | relpages | float8 | 96 | 8 对齐 |
| 10 | reltuples | float8 | 104 | |
| 11 | relallvisible | int4 | 112 | |
| **12** | **reltoastrelid** | **oid** | **116** | **分区自有 toast 关系 oid（见锚③）**；母表行恒 0 |
| 13-19 | reltoastidxid/indextblid/indisusable/reldeltarelid/reldeltaidx/relcudescrelid/relcudescidx | | 120-144 | |
| 20-32 | relfrozenxid/intspnum/partkey/intervaltablespace/interval/boundaries/transit/reloptions/relfrozenxid64/relminmxid/partkeyexpr/partitionno/subpartitionno | | | 变长段，M3 路由面不需要 |

WAL 元组几何（六行已知值全验证——t_m3_range/p1/p2/t_m3_list/pl1/pl2）：

- INSERT 块数据 = 现有 `TupleDecoder.decodePayload` 的 `[xl_heap_header 5B][tail]` 形态：
  infomask2=0x0020（natts=32 ✓）、t_hoff=0x20=32；数据区起点 = dataOff+14；
  **9.2 式元组头 oid 在 dataOff+10**（即 [t_hoff-4,t_hoff) 垫区，"oidhdr" 虚拟槽机制原样适用）
- 锚例：p1 行 oid@+10=32900、parentid@+68=32896、relfilenode@+84=32900；
  pl1 行 reltoastrelid@+116=32914（HOT_UPDATE 上同偏移验证 8 行）
- 行身份双面：pg_partition 同时含母表行（parttype='r'，relfilenode=0）与每个分区一行
  （parttype='p'）；**分区子表仍不在 pg_class**（M2 T5 事实复核成立），母表 pg_class 行
  relkind='r'+parttype='p'（t_m3_range/t_m3_list 实查）

**喂 T1**：第四 watched 表 = pg_partition 单表；bootstrap SQL 投影最少列集 =
relname/parttype/parentid/relfilenode/reltoastrelid（+oid 经 oidhdr）；kinds 推导 =
32 列 + 前导 "oidhdr" 虚拟槽，name 定长 64B、oid 对齐 4、float8 对齐 8；字典键 =
ctid 重放（与三表现有机制同构），rowkind 由 parttype@64 区分。

## 2. 锚② 基准路分区行命名

**结论：`PARTITION_NAME=mother`——OG pgoutput 逻辑流对分区 DML 的 R/I 帧报母表身份
（relId=母表 pg_class oid、relname=母表名、ncols=母表列数），非 vanilla 缺省的叶分区名。**

实测帧（opengauss-jdbc 路②，`INSERT INTO t_m3_range VALUES(20,...)` 路由 p1、
`INSERT INTO t_m3_list VALUES(3,...)` 路由 pl1）：

```
[m2pg]     R frame: relId=32896 ns=public relname=t_m3_range ncols=3   ← 32896=母表 oid（p1 oid=32900 不出场）
[m2pg]     I frame: relId=32896 -> public.t_m3_range
[m2pg]     R frame: relId=32908 ns=public relname=t_m3_list ncols=3
[m2probe_a]（A 档）同形：a_m3_range relId=32979 / a_m3_list relId=32991 —— 两档一致
```

- R 帧列定义取母表列集（列挂母表 oid 的 M2 T5 事实在逻辑路同样成立——解码器按母表
  关系身份输出）；**分区名（p1/pl1）在逻辑流全程零出现**
- 反转的机制面解释：OG 内部分区→母表映射在逻辑解码器内完成，输出的 relId 即母表 oid
  ——wal 直解路若渲分区名会对拍必 diff，T4 归一化器与 wal 路表名解析都必须**归一到母表**
- 映射链（wal 路第四字典供 T4/T2）：分区 relfilenode（heap 记录路由锚）→ pg_partition
  'p' 行 → parentid@68 → 母表 oid → pg_class/pg_attribute（列序/类型 as-of）

**喂 T4**：归一化器无需改名分派（基准路已经是母表名）；**wal 侧**输出渲染的表名解析
须经上述映射链归一到母表——`PARTITION_NAME=mother` 是双路一致性裁定行。A 档（M3 开门
目标档）与 PG 档采样同形，无档位分叉。

## 3. 锚③ TOAST 关系挂分区还是母表

**结论：挂分区——每个分区有**自有** toast 关系，命名 `pg_toast_part_<分区relfilenode>`
（vanilla 是 `pg_toast_<oid>`，OG 分区形态带 `part_` 中缀）；挂靠登记在
pg_partition.reltoastrelid@116；母表 pg_class 行与 pg_partition 'r' 行的 reltoastrelid
**均恒 0**（母表无 toast 面）。**

实测链（m2pg，不可压缩 16KB 值 `string_agg(md5(random()::text),'')×512` 插入 p1，
窗口 0/31A3858-0/31A8150）：

| 观测 | 值 |
|---|---|
| toast chunk heap INSERT | 9 条全落 relfilenode **32902 = pg_toast_part_32900**（p1 的 toast main fork；8×2014B + 尾 434B chunk 行） |
| toast 索引记录 | 11 条落 relfilenode 32904（toast 索引，非 heap rmid） |
| 分区 heap INSERT | 恰 1 条落 relfilenode 32900（p1 自身），块数据 37B——元组内 big 列为 18B external 指针 |
| pg_class 实查 | 32902=`pg_toast_part_32900` relkind='t'（32905/32914/32917 同形各挂 p2/pl1/pl2）；t_m3_range pg_class 行 reltoastrelid=0 |
| toast 关系登记 | 仅在 pg_partition 'p' 行 reltoastrelid@116（HOT_UPDATE 在 CREATE 收尾回填，p1=32902/p2=32905/pl1=32914/pl2=32917 逐值验证） |
| chunk 时序 | chunk INSERT 先于分区行 INSERT（toast 先写、行后指）——与 v2 ToastAssembler 窗口语义兼容 |

另注：可压缩大值（`repeat(md5,2000)` 64KB）pglz 后 ~780B **行内压缩**存储、不走 external
（分区 INSERT 块数据 793B 无指针）——与 vanilla TOAST 阈值行为一致，非分区特有分叉。

**喂 T2**：ToastAssembler 匹配面经第四字典扩展——chunk 记录的 relfilenode（如 32902）
反查归属：pg_partition 扫 reltoastrelid 命中 'p' 行 → 分区 relfilenode → parentid → 母表；
**不能**走 v1 三表字典的 pg_class.reltoastrelid 反查（母表行恒 0 必 miss）。external 指针
（18B）与 chunk 重组机制本身零改动。

## 4. 锚④ 分区 DDL 记录族盘点

**结论：五形态 DDL（CREATE/ADD/DROP/MERGE/SPLIT/EXCHANGE——**含 MERGE/SPLIT，OG 5.0.0
语法全支持**）在 pg_partition 上全部是常规 heap 记录族——INSERT / HOT_UPDATE /
非HOT UPDATE（跨页双 block）/ DELETE（无块 data）/ INPLACE（母表 relpages 统计面），
无任何特殊段类型；A 档（m2probe_a）同窗采样形态全等。**

m2pg 逐形态实测（窗口 = 该语句前后 `pg_current_xlog_location()` 标记，rel=14318）：

| DDL | pg_partition 记录形态 |
|---|---|
| CREATE TABLE ... PARTITION BY（含 N 分区） | INSERT 母表行('r') + N×INSERT 分区行('p') + N×HOT_UPDATE（回填 reltoastrelid）+ 1 条 rmid=9 索引维护记录 |
| ALTER TABLE ... ADD PARTITION | INSERT 'p' 行 + HOT_UPDATE（回填 toast relid）+ INPLACE 母表行 |
| ALTER TABLE ... DROP PARTITION | DELETE（单条，块无 data——catalog 无 replica identity 无旧元组） |
| ALTER TABLE ... MERGE PARTITIONS q1,q2 INTO qm | HOT_UPDATE（幸存 q2 行改 relname→qm + 换新 relfilenode）+ DELETE（q1 行）——**无 INSERT** |
| ALTER TABLE ... SPLIT PARTITION s0 AT (25) INTO (s0a,s0b) | 2×INSERT（s0a/s0b）+ HOT_UPDATE×2（新行回填）+ 非 HOT UPDATE（s0 行换 relfilenode，跨页双 block） |
| ALTER TABLE ... EXCHANGE PARTITION (s1) WITH TABLE t_exch | 恰 1×HOT_UPDATE（s1 行 relfilenode 换成 t_exch 段号）——**分区行不换身份，只换路由** |

A 档窗口（≥0/314BCF0）形态普查：INSERT 16（info 0x00/0x80）、UPDATE_hot 13、
UPDATE_nonHOT 3、DELETE 3、INPLACE 2——同族无增量形态。

未转录结构记档：①rmid=9 info=0x10（20B，10 个 u16——索引/关系维护面，非 heap，
不在 watched 消费面）；②非 HOT UPDATE 的双 block 形态（旧页+新页）v1 replay 已通用支持；
③MERGE/SPLIT 附带的数据搬迁走分区段号新 relfilenode，行级 DML 语义由既有 heap 记录
承载——M3 T5 若做 DDL 后字典一致性，按 relfilenode 变更跟踪（现有机制）即可。

**喂 T1/T5**：第四字典的行事件消费完全复用 v1 现有 heap 事件形态（INS/UPD/DEL/INPLACE），
无新增记录形态需要转录；relfilenode 变更（MERGE/EXCHANGE/SPLIT 的 UPDATE 面）天然进
ctid 重放的末态采纳。

## 5. 坑与备注（实施必读）

1. **og-plan 的 pg_receivexlog 空闲即断**：sidecar 起流后仅数秒 `ReceiveXlogStream failed.`
   （rx.log 实证，仅落 ~0x80B）——M1 `ReceivexlogProcess` 的监督重连在真实长窗口是必要面
   非防御位；IT 长窗口场景留意。本 spike 全部 WAL 证据取自服务端 pg_xlog 段文件直读。
2. **pg_partition 自身 relfilenode=14318 ≠ oid 9016**（bootstrap 须按物理段号注册 watched 面，
   与 pg_attribute/pg_class 映射目录段号直认机制（M2 T7 修复 C）同源注意）。
3. 分区行 INSERT 时 relfilenode==自身 oid（32900 等），**EXCHANGE 后换源表段号**（s1→32973）
   ——字典不可假设恒等，以重放末态为准。
4. 逻辑路探针形态沿用 M2 spike 坑 2/3（publication 先于槽、`publication_names` 必传）；
   本 spike 的 m3pub/m3slot 已随探针清理（DROP SLOT 在探针收尾）。
5. 测试残留：m2pg/m2probe_a 中的 t_m3_*/a_m3_* 分区表族与 t_exch/a_exch 保留（后续任务
   可复用重建）；og-plan 容器本 spike 结束时保持运行。
