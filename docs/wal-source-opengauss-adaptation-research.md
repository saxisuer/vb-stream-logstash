# wal-source 适配 PG 9.2 / openGauss 调研报告（2026-10-11）

> **M1 已落地（2026-10-12）**：openGauss 5.0.0 catalog 面适配按
> `docs/superpowers/specs/2026-10-11-wal-source-opengauss-m1-design.md` 完成十任务
> （判别/布局/双通道收流/槽生命周期/catalog 引导重放/五组 IT——存活红线 5/5、65 轮干扰
> 对拍全等）；调研期 P0 阻塞（"物理流启动致实例退出"）经 Task 5 化解——DirectWalSource
> 按 §2.3 终局机理走 **`replication=true` 纯物理形态**（正确路由 XLogSendPhysical，
> 非 pgjdbc 系 replication=database 的逻辑路由死路），单机形态三认证面直连收流成功、
> 5 轮双 kill 向量实例存活。**下表勘误**为实施期字节实测对本文若干推导值的修正（原段
> 落保留作历史，以表为准）：

| 调研期推导 | 实测裁定 | 依据 |
|---|---|---|
| 块引用定位器 16B（疑带 bucketNode） | **14B**（RelFileNodeOld 12B + extra_flag u16；16B 仅 bucket 变体、0x80 扩展位 fail-fast） | 32KB 窗口 283 条记录全量走读不变量锚——实测裁定见 task-2 报告 |
| pg_class 41 列 | **40 列**（catalog 元组 oid 在 9.2 式元组头非数据列，数据区首列即 relname） | og-plan 元组字节直读 + infomask2 natts=40——实测裁定见 task-1 报告 |
| relfilenode@88 / reltoastrelid@108 / relkind@135（沿 V17 推导） | **84 / 116 / 143**（oid 进元组头少 4B；relpages/reltuples float8 加宽净 8B；五个 oid 列） | 元组字节逐位实测自洽——实测裁定见 task-1 报告 |
| heap 前缀 16/8/2 系（按现代结构推） | **sizeOfHeap 13/11/2**（老结构 5/3/2 + LogCSN u64；INPLACE 无 CSN） | 语料锚 0x2359908/0x2359AD8/0x23587D8——实测裁定见 task-2 报告 |
| 65B 'r' 反馈字段 | 同长，但 **peer_role=3 / peer_state=7** 两 OG 尾缀经真流闭环钉死 | 黄金 wire 对拍——实测裁定见 task-5/8 报告 |
| 记录头 term 恒 `0x80000001` | **单机恒 0x1**（0x80000001 是 HA 段号域形态） | 段页链式走读——实测裁定见 task-1 报告 |
| 'k' keepalive 帧长（未实测） | **恰 33B**（walprotocol.h） | 真流黄金对拍——实测裁定见 task-8 报告 |
| 段文件"完成段恒 16MB 零垫尾" | **pg_receivexlog 预分配 16MB 全零 `.partial`**（文件长度≠接收进度——tail 面须前向扫描最后非零字节） | strace + 对拍实证（旧逻辑静默丢 ~760 条尾记录）——实测裁定见 task-8 报告 |
| `?::pg_lsn` cast（沿 vanilla SQL） | **OG 无 pg_lsn 类型**——cast 每检查点一条 ERROR、槽推进恒失败，须档位化去 cast | pg_type 实查 + 修复后 restart_lsn 真实持久推进——实测裁定见 task-9 报告 |

> **M2 已落地（2026-10-12）**：openGauss 5.0.0 **DML 面**（PG 兼容模式库）按
> `docs/superpowers/specs/2026-10-12-wal-source-opengauss-m2-design.md` 完成 T0-T8——
> 库级兼容模式护栏（PG 档 only，A/B 档启动期 ISE 附 M3/M4 指引）+ XACT/MULTI_INSERT/
> 列压缩/分区四个 OG 差异接缝 + **v2 六场景双路对拍全量**（基准路 = opengauss-jdbc 逻辑流
> + 独立归一化器——T0 spike `docs/wal-source-opengauss-m2-spike.md` 裁定 PARITY=LOGICAL，
> 修正本文 §三"对拍基准缺失/退化场景化断言"的预判）。**下表勘误**为 M2 实施期实测对本文
> 若干推导/预判的修正（原段落保留作历史，以表为准）：

| 调研期推导/预判 | 实测裁定 | 依据 |
|---|---|---|
| attcmprmode 列压缩是 changes 包风险面（§2.1 TOAST 行/§三 changes 行） | **行存面 DDL 天然不可达**——三路 DDL 语法全拒 + tablecmds.cpp 双护栏 + 全库目录扫三源（relcmprs≤1、普通表 attcmprmode∈{0,127}）；压缩形态实为元组级 HEAP_COMPRESSED 0x0010 + 页字典（非 varlena tag）。M2 交付 = 四算法解压纯函数零接线（A 档列存走 compress_kits CU 族不同源，M3 勿复用） | T4 三源实测——task-4 报告 |
| TableFilter 的 'r'/'p' 判别改 parttype 语义（§2.1/§三 replay 行）——分区判别可平移 | parttype 语义成立，但**分区子表不在 pg_class**（无 pg_class/pg_attribute 行，列挂母表 oid；分区行在 pg_partition，母表 relkind='r' + parttype 值域恰 {'n','p'}）——白名单按子表独立匹配不可达；路由 = INSERT 落**分区自身 relfilenode**（pg_partition 'p' 行）。M2 分区面 = 不砖 + 有痕跳过（pg_partition 第四 watched 字典归 M3） | T5 range/list 四锚实测——task-5 报告 |
| DML 面对拍退化为"场景化断言"（§三 测试行：engine 逻辑解码路跑不了 openGauss） | **OG 逻辑解码路可用**——opengauss-jdbc `logical()` proto v1 收 OG pgoutput 帧（两处帧级差异：B 帧 xid Int64 / TupleData Int16 ncols 前缀），双路对拍 PARITY=LOGICAL 成立，v2 六场景全量验收 | M2 spike 三路实测 + T6/T7 对拍 IT |
| md5 认证全通（SQL 与 replication 连接，§2.2——gaussdb 改 md5 后） | 对 **JDBC 逻辑流**存在认证死角：口令 sha256 存储的 gaussdb 走复制流撞**码 11 混合挑战**（opengauss-jdbc 应答缺 iteration 尾缀且 2048 旧迭代数，真 5.0.0 服务端恒拒）+ OG 复制连接在 pg_hba **只匹配 `replication` 关键字行**——解法 = 专用角色 m2par_base + `host replication m2par_base all sha256` 首行（码 10 RFC5802 驱动实现正确；调研期"gaussdb 口令 md5 化"路在部署上不可依赖） | T6 driver trace + 服务端日志实证——task-6 报告 |
| XACT 记录解析重写（§一 1.1/§三 changes 行，按 9.2 格式推） | OG 实况：**无 252 TOPLEVEL_XID 标记**（子事务归并走终态 subxacts 补偿）、COMMIT_COMPACT(0x60) 在用（按源码推导布局解析兜底）、RM_XACT_ID=1 同 vanilla；2PC 完整可用（max_prepared_transactions=200） | T2 源码钉死 + 12+2 语料锚——task-2 报告 |

调研目标：评估 `vb-stream-wal-source`（现支持 PG 17/18）适配 **PG 9.2（openGauss 内核基线）**
的工作量与可行性。方法：①vanilla PG 9.2 与 openGauss 5.0.0 双 Docker 容器实测（协议面/SQL 面/
WAL 字节面）；②REL9_2_STABLE 与 openGauss v5.0.0 源码头文件核对；③pgjdbc 42.7.13 与
opengauss-jdbc 5.0.0-og 双驱动收流实测。所有"实测"标注均有容器复现命令在案（文末附录）。

## 结论速览

| 目标 | 可行性 | 一句话结论 |
|---|---|---|
| **vanilla PG 9.2** | ❌ 不建议 | 四重硬阻塞：无复制槽（9.4 才有）、无 flush LSN 函数、pgjdbc `replication=database` 被拒（9.4+ 语法）、UPDATE 无旧元组（DML 面系统性缺行同"replica 形态"） |
| **openGauss 5.0** | ⚠️ 高潜力但有 P0 阻塞 | WAL 格式面适配可行性高（大量回移植使格式比 vanilla 9.2 更接近现代版，且 UPDATE 带旧元组），但**物理流启动导致整个实例退出**（4 次复现、双驱动双架构）——必须先解锁，否则 receive 面不成立 |

一个总判断先立住：**openGauss ≠ vanilla 9.2**。openGauss 虽报 `server_version_num=90204`，
但其 WAL 子系统回移植了大量 9.3~9.6+ 的基础设施（xlogreader、现代块引用格式、REPLICA
IDENTITY、复制槽），同时加了自有扩展（term/bucket/TDE/段页式/列存）。适配目标是
"openGauss 格式"，不是"9.2 格式"——两者差异比想象大。

## 一、vanilla PG 9.2 差异清单（源码 REL9_2_STABLE + 容器实测）

### 1.1 WAL 布局（layout 包）

| 项 | 9.2 | V17/V18 现状 | 影响 |
|---|---|---|---|
| 页头 magic | `0xD071` | 版本化 | 描述符常量 |
| 页头结构 | magic/info/tli/pageaddr **共 4 字段，无 xlp_rem_len**（短头 16B/长头 32B） | rem_len 在页头@16（短头 24B） | **walker 孤儿续体跳过逻辑须改**：9.2 续体信息在页头后的 `XLogContRecord{u32 xl_rem_len}`（MAXALIGN 8B） |
| 记录头 | 32B：`xl_crc@0`、pad@4、`xl_prev@8`、`xl_xid@16`、`xl_tot_len@20`、**`xl_len@24`（rmgr 数据长，现代版已删）**、`xl_info@28`、`xl_rmid@29` | 24B，tot_len 前置 | 字段偏移全套 + 头长 |
| 块引用 | **`BkpBlock{RelFileNode; ForkNumber; BlockNumber; uint16 hole_offset; uint16 hole_length}`**（24B，块存在性由 `xl_info` 低位 `XLR_BKP_BLOCK(n)=0x08>>n` 表示，最多 4 块） | 现代式 block_id/fork_flags/data_length 流式头 | **WalRecordParser 块链解析整体重写**（两套格式） |
| heap 记录 | `xl_heaptid{RelFileNode+ItemPointerData}` **嵌在主数据里**（关系身份在 rmgr 数据，不在块引用） | 关系身份在块引用 | watched 匹配面（findHeapBlock）从块链改主数据，**提取层大改** |
| UPDATE 截断 | **无 prefix/suffix 截断**（XLH_UPDATE_PREFIX/SUFFIX_FROM_OLD 不存在）——新元组恒完整 | 截断 + 四级重建 | 好消息：整套截断重建/自愈可跳过；坏消息见 1.3 |
| PRUNE | `XLOG_HEAP2_CLEAN{node,block,latestRemovedXid,nredirected,ndead}` + 两段偏移数组（**无 nowunused 段、无 freeze 合并**） | PruneView 三段 + freeze plans | PruneView 重写 |
| opcode | INS 0x00/DEL 0x10/UPD 0x20/HOT 0x40/NEWPAGE 0x50/LOCK 0x60/INPLACE 0x70/INIT_PAGE 0x80；HEAP2: FREEZE 0x00/CLEAN 0x10/CLEANUP_INFO 0x30/VISIBLE 0x40/MULTI_INSERT 0x50 | 数值不同 | 描述符常量 |
| XACT 记录 | `xl_xact_commit{xact_time,xinfo,nrels,nsubxacts,nmsgs,dbId,tsId,xnodes[]}`；opcode COMMIT 0x00/PREPARE 0x10/ABORT 0x20/COMMIT_PREPARED 0x30/ABORT_PREPARED 0x40/ASSIGNMENT 0x50/**COMMIT_COMPACT 0x60** | 现代格式（tlid/xinfo flags/分段对齐数组） | XactGrouper 的 XACT 解析重写 |
| pg_class | 27 列（含 relhasoids/reltoastidxid）；数据区偏移 relfilenode@84/reltoastrelid@104/relkind@115（[4B 位图][数据区] 拼装口径 88/108/119——前两个恰与 V17 同值，需实测锚定） | V18@88/112 | INPLACE 直读/值编码重建偏移重推 |

### 1.2 协议/SQL 面（容器实测，postgres:9.2 @ docker）

- **复制槽不存在**：`pg_proc` 查 `%replication_slot%` = 0，`pg_replication_slots` 视图 = 0（槽
  是 9.4 引入）→ **state 包整个检查点/槽推进生命周期无依托**，保留窗口只剩 `wal_keep_segments`。
- **无 flush 位点函数**：`pg_current_xlog_flush_location` 不存在（9.2 只有 `pg_current_xlog_location`
  write 档与 insert 档）→ 引导一致性的 B 取值面要改（write 可能领先 flush，起流点行为需评估）。
- **pgjdbc 连不上**：`?replication=database` 被服务端拒（`FATAL: invalid value for boolean option
  "replication"`——该值是 9.4+ 语法）→ vanilla pgjdbc 的 `replicationStream().physical()` 在 9.2
  上根本无法建立，需要自定义 startup packet（`replication=true`）+ 自行实现流协议。

### 1.3 DML 面

UPDATE WAL **从不记录旧元组**（CONTAINS_OLD 系列是 wal_level=logical 的 9.4+ 特性，9.2 无逻辑
解码）→ DELETE 前像/UPDATE 旧值系统性缺失，等同现有"replica 形态 UPDATE 缺行"的已知限制，
且**没有截断重建可做**（无截断）。DML 面在 vanilla 9.2 上残缺。

**裁定：vanilla 9.2 为非目标**。除非用户明确接受无槽/自定义协议/DML 缺前像，否则不投入。

## 二、openGauss 5.0.0 差异清单（源码 v5.0.0 tag + 双容器实测）

### 2.1 WAL 布局（字节面实测与源码互证）

| 项 | openGauss 5.0.0 | 证据 |
|---|---|---|
| magic | `0xD074` | 源码 + 段首页字节实测 |
| 页头 | magic@0/info@2/tli@4/**pageaddr@8/rem_len@16**（回移植 9.3+）/**+ openGauss 私有 `xlp_total_len@20`**；短头 24B/长头 40B（与 vanilla 现代版同尺寸、字段不同） | 源码 + 实测（pageaddr 命中 0/2000000、rem_len=4898 与 FIRST_IS_CONTRECORD 置位自洽） |
| 记录头 | **32B（实测）**：`tot_len@0`、**`term@4`（单机恒 0x80000001）**、`xid@8`、**pad@12**、`prev@16`（u64 8 对齐垫 4B）、`info@24`、`rmid@25`、**`bucket_id@26`(u16)**、`crc@28` | 源码（xlog_basic.h，sizeof 按 offsetof=28 但编译器对齐实测 32）+ 实测（prev 链逐记录自洽、MAXALIGN 链式走读通过） |
| 块引用 | **现代式**（`XLogRecordBlockHeader{id,fork_flags,data_length}` + SAME_REL 0x10 + image 头）——回移植 9.5+ 格式；**扩展**：id 高位 `BKID_HAS_BUCKET_OR_SEGPAGE(0x80)`/`BKID_HAS_TDE_PAGE(0x40)`、`XLR_REL_COMPRESS`/`XLR_IS_TOAST` 标志、RelFileNode 疑带 bucketNode（实测解析见 14B+ 形态） | 源码 xlogrecord.h + UPDATE 记录手工解码 |
| heap | rmid=10 同 vanilla；INSERT(0x00)/INSERT+INIT_PAGE(0x80)/HOT_UPDATE(0x40) opcode 实测一致；tuple 数据以 xl_heap_header 5B 前缀在块 data 中 | INSERT/UPDATE 记录值定位（aaaa/100 连 b/cccc/dddd） |
| **UPDATE 旧元组** | **✅ 完整记录**（wal_level=logical + REPLICA IDENTITY FULL 下；实测新元组 dddd 与旧元组 100 连 b 相距 56B 同记录内）——pg_class 有 `relreplident` 列佐证回移植 | REPLICA IDENTITY FULL 对照实验 |
| catalog | **pg_class 41 列**（增 reldeltarelid/reldeltaidx/relcudescrelid/relcudescidx 列存四件、parttype 分区、relcmprs、relhasclusterkey、relrowmovement、relbucket、relbucketkey、relreplident、relfrozenxid64 等）；relkind 第 21 列；**无 relkind 'p'**（openGauss 分区走 parttype 列，分区表 relkind 仍 'r'）——TableFilter 的 'r'/'p' 判别需改 parttype 语义；pg_attribute 24 列（增 attcmprmode/attinitdefval/attkvtype） | information_schema 实查 |
| TOAST | 未实测（结构自 9.2 基线 + openGauss 压缩扩展 attcmprmode 是新增风险面；pglz 预计在，lz4 应无[PG14 特性未回移植待核]） | — |

### 2.2 协议/SQL 面（实测）

- **版本撞号**：`server_version_num = 90204`（与 vanilla 9.2 同值）→ `WalLayouts.forServerVersion`
  无法单独分发，需判别器（`version()` 含 "openGauss" 字样 / 探测 `gs_current_xlog_insert_end_location`
  专有函数）。
- **槽存在且 API 齐**：`pg_create_physical_replication_slot(slotname, dummy_standby)`（⚠️ 第二参
  是 **dummy_standby** 不是 immediately_reserve——语义不同，restart_lsn 保留行为需实测；另有
  `_extern` 变体带 xlog_position 可指定保留位点）、`pg_replication_slot_advance` ✓、
  `pg_drop_replication_slot` ✓、视图含 restart_lsn/confirmed_flush + openGauss 私有 dummy_standby。
- **认证**：默认 `password_encryption_type=2`（sha256 only）→ vanilla pgjdbc **连不上**；改
  `=1`（sha256+md5）+ 重设密码后 **md5 认证全通**（SQL 连接与 replication 连接实测）；镜像
  pg_hba 预埋 `host replication gaussdb 0.0.0.0/0 md5`。或直接换 `org.opengauss:opengauss-jdbc`
  （maven central 在案，pgjdbc fork，sha256 原生支持）。
- **wal_level**：镜像默认已是 `logical`（openGauss 回移植了逻辑解码面）。
- LSN 函数：沿用 xlog 命名（无 wal 命名、无 flush 变体）+ `gs_current_xlog_insert_end_location`。
- `pg_read_binary_file` 直读 `pg_xlog/` 可用（本调研的字节面全靠它）。

### 2.3 P0：物理流启动导致实例整体退出——根因已定位（2026-10-11 二轮排障）

**现象**：`START_REPLICATION PHYSICAL <lsn>` 被接受（日志明确 `received wal replication command`），
随后**整个 gaussdb 实例退出**（容器 Exit 0、日志无 FATAL/PANIC），唯一前兆是
`WARNING: the ha connection is not in the channel list`。复现矩阵：vanilla pgjdbc 42.7.13 /
opengauss-jdbc 5.0.0-og × amd64-qemu / arm64 原生——双驱动双架构 4+ 次复现（排除 qemu 因素）。

**根因链（strace + 源码 + 服务器日志多路钉死）**：

1. **walsender 线程死亡**。openGauss 是线程架构，walsender 是线程。strace 实测：物理流
   START_REPLICATION 后 walsender 线程 `exit(0)`（线程级退出 syscall）——主端口形态下**零日志
   零数据**（连首个 keepalive 都未发出）；HA 端口形态下可见两种死法：`No message received
   from standby for maximum time`（WalSndHandshake 30s 超时路径，walsender.cpp ~L520 的
   `proc_exit(0)`）与静默冻结（线程耗尽、监听全撤、无日志）。
2. **HA 状态机自杀**。openGauss 把物理复制流视为 HA 体系一部分：日志实证 `all build walsenders
   exited` → `Gaussdb exit(0)`——配了 replconninfo 的实例在"物理 walsender 全部退出"时整个实例
   退出；未配 replconninfo 的单机容器同样在物理 walsender 死亡后实例退出（更快更静默）。
   **单机场景下这个设计意味着：任何第三方物理流客户端断开/失礼，数据库实例自杀**。
3. **第三方客户端"失礼"的具体点**：openGauss walreceiver 标准握手是私有序列——
   `IDENTIFY_SYSTEM`（systemid/tli/xlogpos/mode）→ **`IDENTIFY_VERSION`（sversion/pversion/term
   协商）**→ `IDENTIFY_CHANNEL <ha_port>` →（build 场景 `IDENTIFY_CONSISTENCE`）→
   `START_REPLICATION [SLOT ...]`，且流期间要求规律的 standby 反馈。pgjdbc 系驱动（含
   opengauss-jdbc 继承 pgjdbc 的 replication API）直发 START_REPLICATION、跳过全部私有握手，
   运行在服务端未定义状态上。记录头里恒见的 `xl_term=0x80000001` 正是 term 体系的痕迹。
4. **通道与端口体系**（实测）：配 `replconninfo1` 后实例额外监听 **HA 专用端口（localport，
   如 5433）**与心跳端口；HA 端口拒绝 SQL 客户端（`FATAL: the local listen ip and port is not
   for the gsql client`）、主端口 5432 接受复制连接但物理流处于半支持状态（通道校验
   `IsChannelAdapt`：连接双方 IP 须与 replconninfo 的 localhost/remotehost 字符串全等）。
   另两个坑：replconninfo 的 localhost 填错 IP 时 HA socket 创建失败会**阻塞整个实例启动**；
   配置 replconninfo 后若 remotehost 不可达，build walsender 全退 → 启动期实例自杀（研究期用
   宿主机假监听 `nc -lk 5435` 喂住 build 连接可绕过）。

**机理终局（walsender.cpp:471 实锚，修正前两轮"端口/握手"假说）**：真正的变量是**连接形态**
而非端口——`replication=database` 的连接在 openGauss 内核里是 `AM_WAL_DB_SENDER`，WalSndInit
**无条件路由进 `XLogSendLogical`（逻辑发送循环）**，哪怕客户端发的命令是 `START_REPLICATION
PHYSICAL`（该命令在握手层已被接受、CopyBoth 已发出，但从未建逻辑解码上下文）→ 发送循环解
引用空上下文 → walsender 线程即刻静默死亡（零日志零数据）→ HA 状态机把实例带走。这解释了：
①vanilla pgjdbc 与 opengauss-jdbc 同死（pgjdbc 系 replication API 只会发 `replication=database`
形态）；②官方驱动的 65B 扩展反馈对齐（字节码 `ByteBuffer.allocate(65)` 实锚）救不了它——死在
更早的路由上；③`pg_receivexlog` 活——它用 `replication=true`（streamutil.cpp:175-176 实锚，
纯物理 walsender、无 database）→ 正确走 `XLogSendPhysical`，且**从主端口 5432 直连即可**。
openGauss JDBC 从未适配物 理面是因为华为自己不用：其物理复制全是 C 实现（服务端 walreceiver /
pg_receivexlog / gs_basebackup），Java 只做 SQL + 逻辑订阅。另修正：IDENTIFY_VERSION /
IDENTIFY_CHANNEL 是服务端内部 walreceiver 的动作，客户端工具只发 IDENTIFY_SYSTEM——Java
直连的握手面比二轮估计更小。

**Java 直连完整配方（全部有实锚，可直接开工）**：raw socket + v3 startup（`replication=true`、
不带 database；md5）→ `'Q' IDENTIFY_SYSTEM` → `'Q' START_REPLICATION [SLOT] %X/%X`（CopyBoth）
→ 周期 65B 扩展 'r' 反馈（`StandbyReplyMessage`：receive/write/flush/apply/applyRead 各 8B +
peer_role/peer_state 各 4B + sendTime 8B + replyRequested 1B + replyFlags 4B，含 C 对齐垫需
实测钉死）+ 心跳（`standby_message_timeout/2` 发 replyRequested=true）→ 段对齐起点。黄金
对拍基准：strace 抓 pg_receivexlog 真实 wire bytes。pgjdbc 的 replication API 不支持
`replication=true` 形态，故 Java 直连须走自研薄客户端（~300-400 行），与 sidecar 双形态并存。

**对适配的结论**：receive 面需要**自研 openGauss 备库兼容客户端**（不能复用 pgjdbc/opengauss-jdbc
的 replication API）：v3 协议 startup（md5）→ 私有握手序列（IDENTIFY_SYSTEM/IDENTIFY_VERSION
[term 比对]/IDENTIFY_CHANNEL）→ CopyBoth START_REPLICATION → 规律 standby 反馈。wal-source 的
`WalStreamReceiver` 本就是薄层（pgjdbc 只是传输壳），替换为该客户端是可控工作量（预估数百行
原型 + 稳定性打磨）。**风险注记**：即便握手做全，"客户端断开 → 实例退出"的 HA 状态机行为
是否仅限配了 replconninfo 的实例、对生产是否可接受，需在真实部署形态评估；单机（不配
replconninfo）+ 主端口物理流的静默死亡路径，需在握手补全后复验是否消失。

**M0 更优解（2026-10-11 三轮实测）：`pg_receivexlog` sidecar 架构**。openGauss 发行版自带
`pg_receivexlog`（即 pg_receivewal 的 9.2 旧名，`bin/` 下与 `pg_recvlogical` 并列）——它是
带全套私有握手的**合法客户端**，且支持 `--slot`。实测（主端口 5432 直连 + md5）：收流稳定
（WAL 段以 `.partial` 增长落盘、keepalive 正常、写入并行无阻）、**客户端被杀后 walsender 干净
关闭（`walsender thread shut down`）且实例存活**。方案形态：

```
pg_receivexlog -D <wal_dir> [--slot ...]（sidecar 常驻，openGauss 自带零依赖）
  → wal-source 的 receive 面改"文件源"：tail wal_dir 的段文件（.partial 增长→完成后
    rename），字节喂给现有 WalStreamWalker（其 awaitMore 语义天然适配"文件尾部未写完"）
```

代价与待评估项：①多一跳文件 IO（延迟 ms 级）；②`.partial` 文件的尾部边界处理（读多少算
多少，下轮再读——walker 已有该形态）；③反馈/槽推进语义归 pg_receivexlog 自身，与 wal-source
检查点解耦（at-least-once 靠 wal-source 自己的检查点 + 文件重读）；④部署面多一个 sidecar
进程。相比自研协议客户端，此路线**零协议工作量、风险显著更低**——建议 M0 采此形态，
自研客户端降级为备选（低延迟直连需求时再做）。

### 2.4 引导一致性面的两个小坑

- B 函数无 flush 档（9.2/openGauss 同）——起流点用 write 位点时，服务端发流前会等到该位点
  flush（正常写入下自然收敛），行为需实测确认与 `START_REPLICATION` 拒绝面的边界。
- `pg_relation_filenode` 可用（实测三目录 filenode 都能查），catalog 引导 SQL 面基本可平移。

## 三、按模块包的适配工作量

| 包 | 工作量 | 要点 |
|---|---|---|
| `layout` | **大（大头）** | `WalLayoutVOpenGauss` 描述符（magic/页头/记录头 32B/块引用扩展位/catalog 偏移全重推 + 实测锚）；`WalRecordParser` 块引用解析（现代式 + bucket/segpage/TDE 扩展位，遇到扩展位先 fail-fast 再逐个支持）；`HeapViews` opcode 面；**XACT 记录解析新增**（openGauss 格式）；PruneView 核对（openGauss 是否回移植了 14 版新格式待核） |
| `receive` | 小-中，**但前置 P0** | 认证（md5 化或换 opengauss-jdbc 依赖——注意与 pgjdbc 的共存，opengauss-jdbc 是 fork 同包名 `org.postgresql` 部分版本 shaded？实测 5.0.0-og 包名 `org.opengauss`，可共存）；`PhysicalSlotManager` 建槽语义改 dummy_standby/_extern；LSN 函数名 xlog 化；**物理流退出问题解锁** |
| `replay` | 中 | pg_class/pg_attribute 偏移按 openGauss 列序重推（41 列/24 列）+ "两版差恰 N B"式自检锚；INPLACE 直读区/截断重建读区重推（openGauss UPDATE 是否有 prefix/suffix 截断待实测）；自愈 probe SQL 平移（ctid 寻址语法 openGauss 应同）；TableFilter relkind 'p' → parttype 语义 |
| `changes` | 中 | XactGrouper XACT 解析换格式；UPDATE 旧元组 openGauss 已有（✅ 前提成立）；TOAST 面复验（chunk 结构应可平移）；类型矩阵复验（numeric/date 底层同 vanilla；attcmprmode 列压缩是风险面）；PgFloatFormat 的 extra_float_digits 在 openGauss 的缺省行为需核 |
| `state` | 小 | pgVersion 路由 + layout 判别（撞号问题）；StateStore pgVersion 校验面 |
| `api` | 小 | `WalSource` 版本分发加判别器（version() 字符串）；配置面不变 |
| 测试 | 中-大 | **对拍基准缺失**：engine 逻辑解码路跑不了 openGauss（engine 是 pgoutput 协议；openGauss 有自己的逻辑解码体系但插件面不同）——对拍形态退化为 v1 式 JDBC 实查对拍（catalog 面）+ 场景化断言（DML 面）；openGauss 官方 Testcontainers 生态可用（本调研镜像即自带的 multi-arch） |

**总体量级**：layout+replay+changes 三块实质接近"再做一个 V17 首版"的量（约数周级），receive/state/api
是小尾巴；但**P0（物理流退出）不解锁则一切免谈**，且它是纯调研型问题（可能一条配置就通，也可能
要魔改驱动协议）。

## 四、建议里程碑

- **M0（解锁，先于一切）**：物理流退出根因——①读 openGauss walsender 源码
  （`WalSndLoop`/`exec_replication_command` 对未 IDENTIFY_SYSTEM 连接的处理路径）+ replauth/通道
  配置实验；②若需私有握手，评估 opengauss-jdbc 的 replication 实现是否带全（或魔改 pgjdbc 加
  IDENTIFY_SYSTEM）；③输出：能稳定收流 10 分钟不打死实例的 receive 面或"改走文件级采集"的裁定。
- **M1**：`WalLayoutVOpenGauss` + `pg_read_binary_file` 字节源（不依赖流）把 layout/replay 解析
  面做绿——**P0 与 M1 可并行**，字节源足以驱动解析层开发。
- **M2**：receive 面接入（依赖 M0）+ catalog 对拍（JDBC REPEATABLE READ 实查，v1 形态）。
- **M3**：DML 面（XACT 解析/TOAST/类型矩阵/输出）+ 场景化对拍。

## 五、遗留验证项（本次未覆盖）

1. openGauss UPDATE 的 prefix/suffix 截断有无（影响截断重建面是否需要）。
2. openGauss PRUNE 记录格式（是否回移植 PG14 新形态）。
3. XACT 记录字段布局与 rmid 编排全表（本调研只实测了 heap=10；openGauss rmgrlist 需转录）。
4. TOAST chunk/lz4/pglz 面 + attcmprmode 列压缩的值面。
5. openGauss 各大版本（2.x/3.x/4.x/5.x/6.x/7.x）间的 WAL 格式漂移——本调研只钉了 5.0.0；
   目标版本集需用户裁定后逐版核。
6. 建槽 dummy_standby 语义下 restart_lsn 保留窗口与 advance 行为。

## 附录：实测环境

- vanilla：`postgres:9.2`（amd64 模拟）@ localhost:15672，postgres/postgres。
- openGauss：`opengauss/opengauss:5.0.0`（amd64-qemu 与 arm64 原生双跑）@ localhost:15673/15674，
  初始密码 openGauss@123，gaussdb 用户改 md5（type=1 + Gauss_12345）+ REPLICATION 权限。
- 驱动：pgjdbc 42.7.13（本地 m2 仓）/ opengauss-jdbc 5.0.0-og（maven central）。
- 字节面工具：`pg_read_binary_file('pg_xlog/...')` + encode hex + 服务端 position() 搜索
  （客户端管道会被 gsql 的 "total time" 页脚污染，勿用）。
- 容器现况：pg92-probe 运行中；og-native/og-probe 已停（复现命令见会话记录）。
