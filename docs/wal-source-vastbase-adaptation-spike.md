# VastBase G100 逻辑流基准路可用性探测（spike 结论）

**裁定：同形**——VastBase G100 V3.0 真库的 pgoutput 逻辑流帧与 openGauss 5.0.0 档**逐帧同构**（B 帧 xid Int64 / TupleData Int16 ncols 前缀两处帧级差异原样成立、R 与 C 帧等同），T6 双路对拍的基准路**复用 `OgParityBaseline.Normalizer` 的布局假设与形态**（分区行走同一路径——母表身份免映射）；认证面（本服务器 md5 码 5）与槽/publication 面全通，**未撞 M2 码 11 认证死角**。

- 日期：2026-10-13；真库 `172.16.107.133:55433/test_a`（VastBase G100 V3.0，A 兼容档库），用户 `test`；探针代码 throwaway 在仓库根 `target/probe/`（不入库）；驱动 **opengauss-jdbc 5.0.0-og**（与 M2 基准路同驱动、同 classpath 形态）
- 裁定含义：T6（VastDmlParityIT 双路对拍）的基准路 = opengauss-jdbc `logical()` in-process 逻辑流收帧 + 归一化器对成 v2 事务块——与 OG M2/M3 的 PARITY=LOGICAL 路② 同形态，无需新写帧解码

## 0. 基础面

| 项 | 结果 |
|---|---|
| `version()` | **"PostgreSQL 9.2.4 on x86_64-pc-linux-gnu, compiled by g++ (GCC) 7.3.0, 64-bit"**——**无 openGauss/VastBase 标记** |
| `server_version_num` | **90204**（与 vanilla 9.2 及 OG 5.0.0 三方撞号） |
| `getMetaData().getDatabaseProductVersion()` | 9.2.4 |
| `wal_level` | logical |
| 当前库兼容档 | **A**（`pg_database.datcompatibility`——test_a 是 Oracle 风格 A 档；OG 侧 M3 已对 'A' 开门，VastBase 档判别路由不得因 'A' ISE） |
| 槽函数 | `pg_create_logical_replication_slot` / `pg_drop_replication_slot` 在；返回 record 列名 **[slotname, xlog_position]**（OG 风格，与 vanilla 的 slot_name/lsn 不同——与 M2 spike §0 观察一致） |

**判别面关键发现（T4 输入）**：现有 `WalLayouts.forServer(90204, versionText)` 以 version() 文本含
"openGauss" 作唯一区分面——VastBase 的 version() **连 openGauss 标记都没有**（也不含 VastBase
字样），现有判别会把它当 vanilla 9.2 非目标拒绝（ISE）。VastBase 档判别**不能走 version 文本**，
须按 spec T4 的独立探测面（relname 宽度 / name128 词典几何探测）路由。

## 1. 认证（结论①：可用——md5 码 5 直连成功，无死角）

`jdbc:opengauss://…?replication=database&assumeMinServerVersion=9.4` + 用户 test 的复制流连接一次成功。
driver `loggerLevel=TRACE`（JUL handler 放宽到 ALL）复跑采样：

```
 FE=> StartupPacket(user=test, database=test_a, client_encoding=UTF8, DateStyle=ISO,
                    TimeZone=Asia/Shanghai, extra_float_digits=3, application_name=vb_spike_autht, replication=database)
 <=BE AuthenticationReqMD5(salt=b06a3406)      ← 认证码 5（标准 MD5 挑战）
 <=BE AuthenticationOk
```

- **未撞 M2 码 11 死角**（`docs/wal-source-opengauss-m2-spike.md` §4.1：replication 行 md5 + 口令
  sha256 存储 → OG 发码 11 混合挑战、opengauss-jdbc 应答缺 ITERATION_STRING 尾缀且用 2048 旧迭代数
  恒被拒）：本服务器对 test 发的是**纯码 5 md5 挑战**（口令 md5 存储 + replication 行 md5 的服务器侧
  现状），驱动 md5 实现正确、一次通过——**无需专用角色 / hba 改配**
- 探测全程未做任何服务器侧改动（纪律：不动 hba）；若未来口令改 sha256 存储后复测撞码 11，按 M2
  §4.1 方案请运维配专用角色 + `host replication <role> all sha256` 行（码 10 RFC5802 驱动实现正确）

## 2. 槽 / publication（结论②：全通）

逐条结果（全部 vb_spike_ 前缀专用对象；结束后全数 DROP + 三计数验证归零，见 §6 卫生证据）：

| 语句 | 结果 |
|---|---|
| `CREATE TABLE vb_spike_t(id int primary key, v text)` + `ALTER TABLE … REPLICA IDENTITY FULL` | OK |
| `CREATE TABLE vb_spike_p(id int) PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (20))` + 母表 `REPLICA IDENTITY FULL` | OK（OG 原生分区语法同 OG 5.0.0 形态；母表 FULL 传播分区行） |
| `CREATE PUBLICATION vb_spike_pub FOR TABLE vb_spike_t` | OK |
| `ALTER PUBLICATION vb_spike_pub ADD TABLE vb_spike_p`（分区母表显式发布） | OK |
| `SELECT * FROM pg_create_logical_replication_slot('vb_spike_slot','pgoutput')` | OK（返回一行；列名 [slotname, xlog_position]——别按 vanilla 列名取值） |
| `SELECT pg_drop_replication_slot('vb_spike_slot')`（流关闭 + 1s 缓冲后） | OK（首试即成，保留重试面防 walsender 退出竞态） |

## 3. 帧形态（结论③：与 OG 5.0.0 逐帧同构——Normalizer 两处帧级假设全部成立）

四个自动提交事务（xid 59708/59709/59710/59711：INSERT 两行 / UPDATE / DELETE / 分区 INSERT 两行），
共 **16 帧 = B4 + R2 + I4 + U1 + D1 + C4**。原始 hex（**含类型字节**；`|` 与空白仅作字段分隔）：

普通表 vb_spike_t（relId=38190，FULL 身份）：

```
B (25B) 42 | 00 00 00 01 f3 e5 24 a8 | 00 03 00 b1 e8 01 6c 18 | 00 00 00 00 00 00 e9 3c
R (49B) 52 | 00 00 95 2e | "public\0" | "vb_spike_t\0" | 66('f') | 00 02
        [ 01 "id\0" 00000017 ffffffff ][ 01 "v\0" 00000019 ffffffff ]
I (24B) 49 | 00 00 95 2e | 4e('N') | 00 02 | 74 00000001 '1' | 74 00000005 'spike'
I (23B) 49 | 00 00 95 2e | 4e('N') | 00 02 | 74 00000001 '2' | 74 00000004 'row2'
U (41B) 55 | 00 00 95 2e | 4f('O') | 00 02 | 74 00000001 '1' | 74 00000005 'spike'
        4e('N') | 00 02 | 74 00000001 '1' | 74 00000003 'upd'
D (23B) 44 | 00 00 95 2e | 4f('O') | 00 02 | 74 00000001 '2' | 74 00000004 'row2'
C (26B) 43 | 00 | 00 00 00 01 f3 e5 24 a8 | 00 00 00 01 f3 e5 25 08 | 00 03 00 b1 e8 01 6c 18
```

分区表 vb_spike_p（xid 59711，INSERT (5) 落 p1 / (15) 落 p2）：

```
B (25B) 42 | 00 00 00 01 f3 e5 2b 28 | 00 03 00 b1 e8 02 8c c5 | 00 00 00 00 00 00 e9 3f
R (38B) 52 | 00 00 95 36 | "public\0" | "vb_spike_p\0" | 66('f') | 00 01
        [ 01 "id\0" 00000017 ffffffff ]
I (14B) 49 | 00 00 95 36 | 4e('N') | 00 01 | 74 00000001 '5'
I (15B) 49 | 00 00 95 36 | 4e('N') | 00 01 | 74 00000002 '15'
C (26B) 43 | 00 | 00 00 00 01 f3 e5 2b 28 | 00 00 00 01 f3 e5 2b 88 | 00 03 00 b1 e8 02 8c c5
```

与 OG 5.0.0 归一化器（`OgParityBaseline.Normalizer`，M2 spike §2 布局）逐项比对：

| 锚点 | OG 5.0.0 假设 | VastBase 实测 | 判定 |
|---|---|---|---|
| **B 帧 xid 宽度** | Int64（B 帧 25B，高 4B 恒 0） | **25B / xid 字段恰 8B / 高 4B 零**（xid 59708=0xe93c，Int64 与尾 Int32 同值） | **同形** |
| **TupleData Int16 ncols 前缀** | 前缀 + 逐列 kind | **成立**：`4e 00 02 74..`——按前缀走读后 trailing=0（逐字节精确耗尽，四 I/U/D 帧全验）；'t'+Int32 len+文本 / 'n' / 'u' 同形 | **同形** |
| R 帧 | relId Int32 + ns/relname cstring + 身份字节 + Int16 ncols + 每列 {flags Int8 + name cstring + typeId Int32 + typmod Int32} | 逐字段等同（id:23 / v:25 / typmod -1）；身份字节 66='f'（本探测两表均 FULL；M2 观测的 'd'=DEFAULT——同属 vanilla 值域）；**FULL 下列 flags 全 1**（全列入复制身份，vanilla 语义） | **同形** |
| I/U/D 段结构 | I：relId+'N'+TupleData；U：relId+'O'\|'K'+old+'N'+new；D：relId+'O'\|'K'+old | FULL 身份下 U/D 发 'O' 整行旧元组，逐字节同构 | **同形**（'K' DEFAULT 段形态未探测——对拍场景恒 FULL 规避，沿 M2/M3 惯例） |
| C 帧 | flags Int8 + commitLsn Int64 + endLsn Int64 + ts Int64（26B） | 等同；**B 帧 finalLsn == C 帧 commitLsn 同值**（0x1f3e524a8 / 0x1f3e52b28，M2 §4.2 ② 同锚） | **同形** |
| 值形态 | text 长度前缀 UTF-8 | 同（'1'/'spike'/'upd'/'5'/'15' 原文往返） | 同形 |

commitTs 域语义同 OG：epoch-2000 微秒（845189031750680µs ≈ 2026-10-13 14:44 CST，`instantOfPgMicros` 同式换算可用）。

## 4. 分区身份（结论④：R/I 帧报母表——PARTITION_NAME=mother）

分区 INSERT（(5) 落 p1、(15) 落 p2）的帧证据：

- R 帧：relId=**38198**、relname=**vb_spike_p（母表）**、ns=public、ncols=1（母表列集 [id:23]）——
  **分区名 p1/p2 全程零出现**
- I 帧 ×2：relId 均为 **38198（母表）**，值 '5'/'15'
- 母表 REPLICA IDENTITY FULL 传播分区行（R 帧身份字节 'f'）

与 OG 5.0.0 M3 锚② 同语义（母表身份免映射、归一化器对分区行与普通表行同一处理路径、无分派分支）；
配套前提同 OG：分区表须显式 `FOR TABLE <母表>` 发布（`ADD TABLE vb_spike_p` OK 且分区 DML 帧到达）。

## 5. 对拍基座裁定

**同形 → 复用**：T6 基准路 = opengauss-jdbc `logical()` 逻辑流 + `OgParityBaseline.Normalizer` 同布局
归一化——OG 与 vanilla 的两处帧级差异（B xid Int64 / TupleData Int16 前缀）在 VastBase 上**原样成立**，
R/C 帧等同，分区行同路径免映射。增量面（非帧形态差异，归各任务承接）：

1. **判别**（T4）：version 文本判别不可见 VastBase（§0）——须独立探测面（name128 词典几何）
2. **认证**（T5/T6）：本服务器 test 用户 md5 码 5 直连可用，零配置零角色
3. **'K' 段 DEFAULT 形态**：未探测——对拍场景恒 FULL 身份（M2/M3 惯例不变），归一化器 'K' 分支沿用 OG 形态假设
4. **'T' truncate / DDL-in-txn / 2PC 帧**：T0 未探测——归一化器保持显式拒绝面（沿 M2），T6 实施期按需投探补锚

## 6. 坑清单与卫生证据（T5/T6 实施必读）

1. `stream.close()` 恒抛 `Database connection failed when ending copy`（EOF 竞态）——与 M2 坑 5
   同款驱动行为，吞掉即可（本探测实测复现同款异常）
2. `readPending()` 不暴露服务端断流——活性护栏（空轮周期 `forceUpdateStatus()`）沿用；本探测
   20s drain 内活性探测零失败、16 帧全数到达
3. 槽删除须等 walsender 退出（关复制连接 + ~1s 缓冲；本探测首试即成，IT 保留重试面防竞态）
4. `pg_create_logical_replication_slot` 返回列名 [slotname, xlog_position]——`SELECT * FROM` 只消费
   游标，别按 vanilla 列名（slot_name/lsn）取值（沿 M2 惯例）
5. **真库卫生**：本探测只创建/删除 vb_spike_ 前缀对象（publication/两表/槽），未触碰既有业务表
   （orders/snapshot/gs_*）与既有逻辑槽（sync_orders_slot/vb_e2e_*）；结束时验证三计数全零——
   `pg_publication` LIKE 'vb_spike_%' = 0、`pg_class` relname LIKE 'vb_spike_%' = 0、
   `pg_replication_slots` slot_name LIKE 'vb_spike_%' = 0
