# vb-stream-wal-source 学习手册

> 面向新读者的代码流程学习手册（2026-10-14 编写，代码基线 = main `e2083fd`）。
> 五大主题：**WAL 获取方式 / catalog 一致性 / 断点续传 / 数据类型解析 / 物理复制槽推进**。
> 行号引用基于编写时点源码；细节文档另有模块 `vb-stream-wal-source/CLAUDE.md`（演进史与
> OG/VastBase 档差异）与 specs（见文末索引）——本手册聚焦"代码怎么跑"。

---

## 0. 模块定位：为什么要有 wal-source

engine（`vb-stream-engine`）走的是 **逻辑解码**：服务端起 walsender + pgoutput 插件，
在**主库 CPU** 上把 WAL 解成逻辑变更再发给客户端。wal-source 走的是另一条路——
**"伪备库"直解**：

- 不建备库、不占主库逻辑解码 CPU；
- 客户端用 **物理复制流**（`START_REPLICATION PHYSICAL`）收**原始 WAL 字节**；
- 纯 Java 按 REL_STABLE 源码头文件转录的**版本化布局描述符**（`WalLayout`）解析记录；
- 自己重放 pg_attribute/pg_class 等目录表的 WAL 记录，维护 **as-of catalog 字典**——
  相当于"伪装备库"：有备库的字典视图，没有备库的存储。

可行性由 spike 钉死（`docs/wal-direct-decode-spike.md`，19 场景 27 条发现）；v2 起 DML
面经**双路对拍 IT**（engine 逻辑解码路 vs wal 直解路，按 xid 交集逐行 diff 空）证明两路
CDC 输出逐字节等价。

包结构（`org.vastdata.vbstream.walsource`）：

| 包 | 职责 |
|---|---|
| `layout` | 版本化布局常量 + WAL 记录/heap 家族/元组解码（纯函数） |
| `receive` | 物理复制流接收 + 字节流走读状态机 + 槽管理 + OG 双通道字节源 |
| `replay` | catalog 一致性同步（JDBC 引导 + ctid 重放引擎 + 自愈） |
| `state` | 检查点持久化（VBWS 单文件格式） |
| `api` | `WalSource` 全装配门面 + `CatalogSnapshot` as-of 查询契约 |
| `changes` | v2 DML 面：白名单过滤 → 事务组装 → TOAST 重组 → 值渲染 → 输出 |

**单线程直通执行模型**：接收 → 解析 → 重放/解码 → 周期落盘全部发生在同一个
`wal-receiver` 线程（sink 同步执行）。这是很多"天然一致性"的根源——检查点取当前已
消费 LSN 即天然一致点，无需快照冻结（`CatalogSynchronizer.java:65-71`）。

---

## 1. 全局架构：一条 WAL 记录的旅程

```text
PostgreSQL 17/18
  │ 物理复制流 START_REPLICATION PHYSICAL <lsn>（pgjdbc，不带 SLOT）
  ▼
WalStreamReceiver（wal-receiver 线程）
  │ readPending drain 轮询；断流指数退避重连 1s→16s×5
  ▼
WalStreamWalker（字节流走读状态机）
  │ chunk 拼接 → 页导航（pageaddr 锚定）→ 记录头 → 跨页 contrecord 缝合
  │ 产出强类型 WalRecord（lsn/xid/rmid/info/blocks/main）
  ▼ sink 单 pass 分发（次序固定：先 catalog 后 DML —— as-of 正确性）
  ├─① CatalogSynchronizer.applyRecord
  │    ├─ appliedLsn 过滤线（end ≤ 前沿直接跳过）
  │    ├─ CatalogReplay.applyCatalogRecord（仅 watched 目录表的块）
  │    │    PRUNE → INPLACE → attr/class/nsp/partition 行事件（固定次序）
  │    │    → 维护 CatalogStores 四字典（ctid 键控）
  │    │    → 截断更新三级重建 / SelfHealer 末态采纳自愈
  │    └─ maybeCheckpoint（30s 或 1000 行事件先到为准）
  │         ├─ StateStore.checkpoint（.part→fsync→原子 rename）
  │         └─ pg_replication_slot_advance（按 floor 封顶）
  └─② postApply → ChangeStream.onRecord（DML 面，vb.wal.dml=true 时）
       ├─ TableFilter：relfilenode → 表身份（relkind/系统模式/白名单三道闸）
       ├─ XactGrouper：heap I/U/D 行入桶累积（到达期解码+渲染）
       │    XACT 终态（COMMIT/ABORT/PREPARE/...）驱动发射/弃桶/挂起
       ├─ ToastAssembler：toast chunk 采集 + external 指针重组 + pglz/lz4 解压
       ├─ DiskValueRenderer：磁盘值 → PG text（17 类型）
       ▼
       OutputRenderer → CDC logger（org.vastdata.vbstream.walsource.cdc）
       TXN-BEGIN / 逐行 / TXN-END（复刻 engine ConsoleRenderer 格式）
```

装配入口是 `WalSource.start()`（`api/WalSource.java:440-478`）：

1. 普通 JDBC URL 建 SQL 会话（此后归同步器独占——probe/槽推进复用）；
2. 查 `server_version_num` → `WalLayouts.forServer(versionNum, versionText, relnameAttlen)`
   分发布局（17/18 直取；9 万段 OG 撞号经 version() 文本判别；VastBase 再经
   `pg_class.relname` attlen=128 探测）——未注册版本启动期 ISE；
3. 命中 OG 系（`instanceof WalLayoutVOpenGauss`，VastBase 子类同走）转 `startOpenGauss`
   （DML 面请求在场时先过库级兼容模式护栏，白名单 {'PG','A'}）；
4. `CatalogSynchronizer.start(...)` 一次调用装配槽管理 + 引导/续传决策 + 接收线程 +
   检查点/槽推进生命周期；DML 面注入两个接缝：
   - `postApply`（`Consumer<WalRecord>`）：接收线程惰性建 `ChangeStream` 后逐条喂入——
     **先施加字典后解码行**，as-of 次序天然成立；
   - `pendingFloor`（`LongSupplier`）：DML 待决桶下界，供检查点封顶/续传回退（§4.4）。

---

## 2. WAL 获取方式

### 2.1 PG 17/18 主路径：pgjdbc 物理复制流

`WalStreamReceiver`（`receive/WalStreamReceiver.java`）：

**建流**（`openStream`，:306-318）：
- JDBC URL 必须带 `?replication=database&assumeMinServerVersion=9.4`——缺后者 replication
  参数被驱动**静默丢弃**（:25-26）；
- `getReplicationAPI().replicationStream().physical().withStartPosition(lsn)
  .withStatusInterval(1s).start()`；
- **slot 不参与流**：pgjdbc 42.7.13 物理流构造器无槽选项，实际命令是
  `START_REPLICATION PHYSICAL <lsn>`（不带 SLOT，javap 核实）。所以 restart_lsn 不随
  消费推进，保留窗口靠检查点后 SQL 主动推槽（§6）；
- 起点页对齐下取整：`fromLsn & ~(walBlockSize-1)`（:194）——walker 页导航假设流首是
  页头；多收的页首段记录由上游 appliedLsn 过滤线消化。

**消费循环**（`drainLoop`，:336-364）：`readPending()` 非阻塞取一条——null（缓冲取尽）
先做反馈判定再睡 100ms；有 chunk 拷贝字节喂 `walker.feed(getLastReceiveLSN(), bytes)`，
刷新 `consumedLsn` volatile 镜像。这是 **drain 形态**（连续读到 null 才睡），不是
"每轮一条 + 固定睡"——后者会把读取上限钉死在 ~10 msg/s（引擎 1.7 实测踩坑）。

**LSN 反馈**（`sendFeedback`，:374-380）：距上次反馈 ≥5s 或新交付 ≥1000 条即
`setFlushedLSN(walker.consumedLsn()) + forceUpdateStatus()`——只设 flushed 且取 walker
消费前沿（非接收游标）。当前流不附槽，此反馈主要维持 standby status 协议节拍。

**断流重连**（`runReceiveLoop`，:246-294）：SQLException 指数退避 1s/2s/4s/8s/16s，连续
>5 次写 `terminalFailure` 后线程退出（宿主必须轮询该面，Main 每 10s 检测后 exit 1）；
重连起点 = `walker.consumedLsn()`（记录对齐位点，walker 页内直通路接受这种重入点）；
成功收到过数据即清零失败计数。**RuntimeException（解析 ISE）是确定性失败——不重连**。

### 2.2 WalStreamWalker：字节流走读状态机

`WalStreamWalker`（`receive/WalStreamWalker.java`）把"块边界不按页对齐、记录可跨页"
的字节流切成一条条完整记录。核心状态：`carry`（跨 chunk 缝合缓冲，锚定于
`carryStartLsn`）、`dataEndLsn`（自维护数据锚）、`expectedPageAddr`（页连续性锚）。

**feed 衔接**（:161-196）：chunk 起点 = 自维护锚（非 0 时），否则回落
`chunkEndLsn - data.length`；与 carry 尾不衔接且 carry 非空 → WARN + 丢 carry
（`carryDrops++`，重连续传重发覆盖 carry 的预期形态）。

**页导航**（:224-297）：`cur & (walBlockSize-1) == 0` 即页边界。页头分长短档
（`xlp_info` 的 XLP_LONG_HEADER 位）；锚定判据三项：magic 对 + pageaddr 页对齐 +
`pageaddr == expectedPageAddr == cur`（首头只要求页对齐——pageaddr 是唯一权威，可纠
调用方游标漂移）。**孤立续体**（XLP_FIRST_IS_CONTRECORD）`orphanSkips++` 跳过。

**失配重锚**（`resyncAt`，:440-477）：坏 magic/pageaddr 失配 → 自失配点**逐字节扫描**
下一个合法页头（magic 对 + 页对齐 + 与游标推算地址差 ≤2 页）；命中则 arraycopy 左移
抹除错位字节、`resyncs++`；dropped>0 另计 `lossyResyncs`（真丢字节面）。

**记录头可跨页**（Task 10 修复，:307-329）：xlog.c 只保证记录起点处本页剩余 ≥4B——
24B 记录头本体可自页尾最后 4..23 字节起跨页，头余部经续体页缝合（三项全验：magic +
FIRST_IS_CONTRECORD + contrecord 页 pageaddr == curL）。页内 `totLen==0` 判零垫。

**自维护锚 vs getLastReceiveLSN**（竞速修正③）：`getLastReceiveLSN()` 随服务端
keepalive 的 walEnd **跳变**，采信它会假再同步甚至静默跳段——所以它只作首块锚与漂移
观测；连接内字节地址锚由 walker 的 `dataEndLsn` 承担；每次 openStream 后
`resetAnchor()` 清零（首块不能预锚请求位：服务端自请求位读到首个记录边界才起发）。

产出：`WalRecordParser.parse(rec, recLsn, layout)` → 强类型 `WalRecord` 交付 sink。

### 2.3 版本判别与布局分发

`WalLayouts.forServer(versionNum, versionText, relnameAttlen)`（`layout/WalLayouts.java`）：
- 17 → `WalLayoutV17`、18 → `WalLayoutV18`；
- 9 万段（90204 与 vanilla 9.2 撞号）：version() 文本含 "openGauss" → OG 档；无
  branding 时经已建会话探测 `pg_class.relname` attlen——128 → VastBase 档
  （`WalLayoutVVastbase`，探测 SQL 见 `WalSource.RELNAME_PROBE_SQL`）；64/不可得落
  vanilla 9.2 拒绝面。**探测在建槽之前**（ISE 先于槽副作用）。

### 2.4 OG/VastBase 档：双通道字节源

OG 上 pgjdbc 的 replication API 物理流不可用（`replication=database` 被硬路由进逻辑
发送循环），故有 `WalByteSource` 推模式接口体系（`vb.wal.og.source` 两通道）：

- **file（缺省）**：sidecar `pg_receivexlog` 子进程落盘目录（`ReceivexlogProcess`——
  managed 自拉起/external 只读两模式；密码只经 PGPASSWORD 环境变量；监督线程指数退避
  重拉、稳定运行 60s 清零计数）+ `FileWalSource` 目录 tail。**坑**：`.partial` 是
  预分配 16MB 全零文件（文件长度 ≠ 接收进度）——交付上限 = **末非零字节 + 1** 前向扫描
  （`FileWalSource.java:387-413`，旧逻辑静默丢 ~760 条尾记录）；walker 侧对整零页容忍
  （`tolerateAllZeroPages()`）。
- **direct**：`DirectWalSource` 裸 v3 协议客户端（`direct/V3ProtocolChannel` +
  `OgAuth`）——startup packet **`replication=true`** 形态 → 三认证面（码 5 md5 /
  码 10-3.0 RFC5802 签名 / 码 11 md5+sha256 混合——含 "ecdfecefade" 尾缀与 10000 迭代
  实锚，opengauss-jdbc 的实现恰在码 11 恒被拒）→ IDENTIFY_SYSTEM →
  `START_REPLICATION [SLOT "name"] %X/%X`（可带 SLOT——与 pgjdbc 主路径的关键差异）
  → 'w'/'k' 解析。OG 私有帧形：'k' 恰 33B；'r' standby status 恒 **65B**（多
  peer_role=3/peer_state=7 两 OG 尾缀；`V3ProtocolChannel.encodeStandbyReply`，
  :530-546）；块锚 = dataStart + 载荷长（非 walEnd）。续传**段对齐**下取整（16MB），
  重叠段 at-least-once 由上游过滤线消化。

VastBase 档在 receive 包零专用代码——`instanceof WalLayoutVOpenGauss` 门控命中同一
`startOpenGauss` 装配。

### 2.5 离线回放档（2026-10-15）

库不可访问（宕机/网络隔离）时的第三种字节源形态：**凭本地检查点 + WAL 段快照目录
离线续传**（段目录 = 拷 pg_xlog 或复制流按 LSN 落段），仅 VastBase 档——检查点是
唯一状态源与入口凭证（缺失/损坏 ISE 附"先在线运行"指引；pgVersion≠6 经 5/17/18
已知档位探针甄别后 ISE 附"本版仅 VastBase"——冷启动离线非目标）。

- **入口**：库 API `WalSource.startOffline()`（与 start 共用单程闸门、两路互斥）或
  Main `-Dvb.wal.mode=offline`（mode 是 Main 层直读键，不进门面配置/walconfig
  白名单）+ `vb.wal.offline.dir`（offline 档必填）/`vb.wal.offline.endLsn`（可选
  交付上界，到达即提前停）。
- **装配**（`CatalogSynchronizer.startOffline`，与 startOg 骨架同构、零 SQL 面——无
  会话/槽/引导/护栏）：restoreInto 灌回四字典 → appliedLsn 种子 = stored lsn（未对齐
  续传严格过滤线）→ 流起点 = min(stored, floor)（dmlFloorLsn 回退语义——重放段重建
  待决桶）→ `ChangeStream` 以 sqlConn=null 装配（纯回放禁回查档，TOAST probe 禁用）
  → sink 组合 applyRecord → postApply → maybeCheckpoint 次序与在线逐字同构。
- **字节源**（`FileWalSource.staticDirectory`，与 tail 档共存——tail 档零改动）：
  启动期一次性审计三项（起点段在场/段号连续/tli 一致；tail 档"静默跳后继段等落盘"
  语义在快照目录 = 静默丢数据，必须显式拒绝）；按清单序读尽即**确定性 EOF**（不轮询
  增长、无 rename 等待）；撕裂尾天然消化——半条记录的字节停在 walker carry 里，
  appliedLsn 只随完整记录推进，EOF 停机即弃 carry。
- **衔接在线**：检查点同目录续写，离线/在线双向互通——库修复后同 state 目录在线
  `start()` 即续传（stored ≥ P₀ 天然通过）；EOF 经 `eofReached()` 观测（在线档恒
  false），Main 据此 close 落最终检查点优雅退出（exit 0）。

---

## 3. catalog 一致性

问题：解码用户表行需要知道"这张表当时有哪些列、什么类型、行在哪"——即 pg_attribute /
pg_class 等目录的 **as-of 视图**。物理流里目录表自身的变更也是普通 heap 记录，所以要
自己重放。

### 3.1 四张 watched 字典与 ctid 重放模型

`replay/CatalogReplay` 只 watch 四张目录（注册面 = 引导期回填的物理 relfilenode，块
引用按 `fork==0 && relNode==watched` 匹配，:1576-1586）：

| 表 | 词典行模型 | 用途 |
|---|---|---|
| pg_attribute | `AttrRow(attrelid, attname, atttypid, attnum, attisdropped)` | 列字典（含 dropped 占位） |
| pg_class | `ClassRow(relOid, relname, relnamespace, ..., relfilenode, reltoastrelid, relkind)` | 关系身份 + relfilenode/toast 跟踪 |
| pg_namespace | `NspRow(nspOid, nspname)` | schema 名解析（v2 第三腿） |
| pg_partition | `PartitionRow(partOid, relname, parttype, parentid, relfilenode, reltoastrelid)` | OG 原生分区路由（M3 第四腿） |

**ctid 重放模型**（spike 发现 16）：行字典以**物理位置 (block, offnum) 折成的 u64 键**
索引——INSERT = upsert、DELETE = 按位删、UPDATE = 先删旧位再插新位（与 heap 自身的
MVCC 移行同构）。JDBC 引导快照带 ctid 种子，重放 upsert 语义使**重叠窗口幂等**。

### 3.2 JDBC 一致性引导（CatalogBootstrap.bootstrap，L159-198）

装配序：**① 建槽取 consistent_point P₀ → ② 引导得 B → ③ 流起点 max(P₀, B)**（P₀ 先取
是为了建槽自身的 WAL 写入落在引导快照之前）。

RR 单事务内步骤（次序是正确性的核心）：
1. `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`；
2. **首句读 `pg_current_wal_flush_lsn()` 得 B——必须先于任何种子查询**（C1 修复）：
   RR 隔离级不冻结 WAL 位点函数；若种子后再读，(S,B] 窗口内并发提交的 DDL 效果
   "既不在种子快照、WAL 记录又被 appliedLsn 过滤线永久跳过"——attr 行丢失且无自愈
   通道。先读保证 B ≤ 快照时点 S；
3. 回填四目录自身的 relfilenode（`pg_relation_filenode('pg_xxx'::regclass)`）；
4. 种子查询（共享同一时点快照）：attr 取 `attnum > 0`（系统列不进字典）；class
   **不限 relkind 全表**（toast/index 行也要——tracked toast 与 INPLACE 对象都是
   pg_class 行）；ctid 文本折键；
5. 定位 tracked 表行位（多命中取最小 oid，确定性）。

**引导窗口为何重放而非跳过**：(B,S] 段记录的效果一部分已含在种子快照、一部分不含
（在途事务：INSERT 已 flush ≤ B 而 commit 晚于快照）。所以引导路径的 appliedLsn 种子 =
**页对齐流起点**（重叠段一律重放消化，ctid upsert 幂等收敛，小段自愈 probe 噪声属
预期）；而**续传/forced 路径种子 = stored lsn 未对齐值**（字典已施加到该点，严格过滤
——部分记录类二次施加非幂等，如对已被 INPLACE 失效的 tail 做截断重建会产出零字段的行）。
这就是"**两级种子语义**"（`CatalogSynchronizer.java:600-609`）。

### 3.3 单记录施加流程（CatalogReplay.applyCatalogRecord，L721-824）

次序固定：**① PRUNE → ② INPLACE → ③ attr 行事件 → ④ class 行事件 → ⑤ nsp →
⑥ partition**。理由：PRUNE 压实搬行是最底层的行位迁移，须先于一切行事件重定位；
INPLACE 原地改写 relfilenode/toast 且**置 tail 失效**（后续截断重建被迫走值编码而非
splice 到陈旧 tail）。

各 opcode 解析（`heapEvents` L466-568，几何见 §5）：
- **INSERT**：data 路径 `[5B 头][tail]` 解码落键；**image 路径**（catalog 变更 FPW 记录
  **不含 tuple data**——`RelationIsLogicallyLogged` 排除 catalog，spike 发现 14）从
  页镜像按行号解；
- **UPDATE/HOT_UPDATE**：flags 含 `XLH_UPDATE_TRUNCATION` 走截断重建（§3.4）；
  **UPD 必删旧 tail**；
- **MULTI_INSERT**（catalog 单行插入也走此形态）：entry 起点 2 对齐、`datalen` 只计
  tail、步进 `SHORTALIGN(7+datalen)`；
- **INPLACE**（TRUNCATE/ANALYZE 对 pg_class 行的原位改写——relfilenode 跟踪的来源）：
  直读数据区 relfilenode@88 / reltoastrelid@108(V17)/112(V18)；
- **PRUNE**：redirected 对 (from,to) 逐对重定位行/tail（否则 autovacuum 压实后 ctid
  跟踪断链——spike 发现 18 实测撞上）。

### 3.4 截断更新的三级重建（reconstructTruncatedUpdate，L1098-1155）

heap UPDATE 未变更的首/尾列字节不落盘（prefix/suffix truncation，仅
`wal_level < logical` 可达——与 CONTAINS_OLD 记录在 heapam.c 门互斥，双计数恒 0 是
逻辑流下的回归哨兵）。重建三级决策：

1. **值编码重建**（pg_class/nsp/partition 面）：prefix ≤ 读区上界时由已知旧行重编码
   前缀 + 记录中段拼装；**后缀读区回填**——后缀零填充仅在读区之外合法，越界时必须把
   `[零填充起点, 读区末尾)` 段从旧行读区编码**回填精确值**（盲零填充在 RENAME 链上
   产出零字段行——spike 后期实测坑）；
2. **rawTail splice**（通用档）：旧 tail 前 prefix 字节 + 记录中段 + 旧 tail 末尾
   suffix 字节；
3. **自愈/skip**：旧行与 tail 皆无时走精确采纳（pg_class 面）或 skip + 计数。

### 3.5 末态回填 / 精确采纳自愈（SelfHealer）

断链窗口（丢页/重发窗口）内旧行不可得时，核心断言：**更新必移行位——"JDBC 末态仍居
记录 new 位"的行即该记录施加后的精确状态**，整行采纳（行值全来自探测行，无拼装时序
问题）。探测经 `JdbcProbeImpl` 按 ctid 点查（复用引导会话，单写者串行）；该位无行
（已再迁移）= 拒绝，skip 计数、链由该 oid 末条记录追平后收敛。

**语义边界：末态回填而非历史重建**——丢页窗口内的中间代际不可恢复，v1 只承诺 catalog
**末态**正确（对拍面即末态全等）。早期的"候选枚举 + 中段值校验"形态经对抗性 IT 实证
废弃（拼装产出时代错位混合行）。

PRUNE redirect 的 from 位无行（链断形态）同样按 **to 位精确采纳物化**（终审收敛洞
修复）；nsp/partition 面无探测通道，DEBUG + miss 计数后放弃（fail-safe 漏数据非错
数据）。per-oid 连续采纳拒绝 ≥3 登记 `staleOids` + WARN——观测面，"待下轮引导"落在
重启路径。

### 3.6 竞速鲁棒性三修正（核心质量设计，spike 实测通过率 ~1/3 的三根刺）

1. **引导一致性**（§3.2 的 C1：首句读 flush LSN + 两级种子语义）；
2. **ctid 寻址精确采纳自愈**（§3.5 的末态回填）；
3. **pageaddr 锚定 + 自维护数据锚**（§2.2——页头 pageaddr 是页内导航唯一权威；
   字节地址锚由 walker 自维护，`getLastReceiveLSN()` 只作观测）。

---

## 4. 断点续传

### 4.1 VBWS 检查点文件格式（state/StateStore.java）

单文件 `wal-source-state.bin`，大端序，三段式：

```text
┌─ header 24B ──────────────────────────────────────────────┐
│ magic 'VBWS' 4B │ formatVersion u16(=4) │ pgVersion u16   │
│ lsn u64（=appliedLsn 已施加前沿）│ headerLen u32(=24)     │
│ headerCrc32 u32（覆盖 header 前 20B）                     │
├─ body 变长 ───────────────────────────────────────────────┤
│ ① attr 表   [ctid u64 | attrelid | atttypid | attnum u32 │
│              | attisdropped u8 | attname UTF]×n           │
│ ② class 表  [ctid | 8×u64(oid/namespace/.../toastrelid)  │
│              | relname UTF | relkind UTF]×n               │
│ ③ nsp 表    [ctid | nspOid | nspname UTF]×n   (v2 增)    │
│ ④ partition 表 …                              (v4 增)    │
│ ⑤ 四张 raw tail 表 [ctid | len u32 | bytes]（截断 splice │
│    的旧值面）                                             │
│ ⑥ 标量：tracked 双 ctid + 四 watched relfilenode          │
│ ⑦ staleOids / interestRelOids 两 oid 集                   │
│ ⑧ dmlFloorLsn u64（DML 待决桶重放下界，v3 增，0=无桶）    │
├─ footer 12B ──────────────────────────────────────────────┤
│ lsn u64（header 复述，双验）│ fileCrc32 u32（全文件）     │
└───────────────────────────────────────────────────────────┘
```

- **CRC 双验**：header CRC 快速识别版本面损坏；全文件 CRC 覆盖除自身外全部字节；
- **formatVersion 演进**：v1 基础面 → v2 增 nsp+relkind → v3 增 dmlFloorLsn → v4 增
  partition。**旧版一律拒载不迁移**（宁可重引导，不可错位窗口重放）；
- **pgVersion 错配拒载**：与 `layout.majorVersion()` 同源（17 写 17 / 18 写 18 / OG=5 /
  VastBase=6）——跨版本/跨档位的检查点不互认；
- **解码防御**：先验后解（最短 36B → 全文件 CRC → magic/版本/头部 CRC → 逐表"垃圾
  计数防线"[rowCount×最小行宽 > 文件长即损坏] → footer lsn 双验 + 尾部余量恰余 4B）。

### 4.2 原子写协议（StateStore.checkpoint，L161-181）

内存一次性序列化 → 建目录 → 写 `.part`（TRUNCATE_EXISTING 覆写半成品）→
`ch.force(true)` fsync → 同目录 `Files.move(ATOMIC_MOVE, REPLACE_EXISTING)` 原子换名
（不支持原子 move 的文件系统回落普通 move + WARN）。父目录无 fsync——崩溃窗口内最坏
残留 `.part` 或旧正名文件，均走拒载/覆写路径，安全侧不损数据。**契约：无 `.part`
后缀的正名文件即完整检查点；`.part` 永不 load。** 损坏/版本不符一律 ERROR + empty
（不部分载入、不上抛），回落全新引导由 caller 决定。

### 4.3 触发时机与天然一致点

双节拍先到为准（`CatalogSynchronizer.maybeCheckpoint`，:773-786）：**距上次落盘 ≥30s
（vb.wal.state.interval.ms）或 新增 catalog 行事件 ≥1000（vb.wal.state.events）**。
事件计数取 catalog 行事件（非原始 WAL 记录数——非 catalog 记录靠墙钟兜底）。

单线程直通模型下 `maybeCheckpoint` 在 `applyRecord` 之后以 `appliedLsn` 落盘——**天然
一致点，无需快照冻结**。post-apply 分发档的 sink 次序固定为 **施加 → DML 组装 →
检查点**——检查点必须后于组装，floor 才含本记录新建的待决桶。`stop()` 在 join 接收
线程后做最终 best-effort 检查点（冻结态一致点）。

### 4.4 续传恢复流程（decideResumeOrBootstrap，L670-701）

state 启用时：先 `ensureSlot` 取 P₀（restart_lsn）→ `load()` 成功且 **stored lsn ≥ P₀**
→ 灌回字典（先清空再回填，metrics 从 0 起），start = stored lsn，跳过引导；
**stored lsn < P₀**（槽保留窗口已越前）→ WARN 回落全新引导。

**dmlFloorLsn 的语义**（XactGrouper.pendingFloorLsn，:1248-1256）：= 当前待决桶
（进行中普通事务 + 2PC 挂起桶）**首记录 lsn 的最小值**，无桶为 0。为何需要：检查点
LSN（catalog 已施加前沿）可以越过挂起桶的行记录所在页——续传只按 stored lsn 起流则
重放段够不到桶内行，COMMIT_PREPARED 到达即无桶**静默零发射**（at-most-once 缺陷）。
修复三面：
- 检查点随 lsn 落盘 floor（StateStore v3）；
- 槽推进目标按 floor 封顶（§6）；
- 续传流起点取 `min(stored, floor)` 页对齐回退——重放段重建桶后终态补发；
  **catalog 过滤线仍 = stored lsn**（字典不二次施加）。

三者合成 at-least-once 闭环：**崩溃最坏重复重放已落盘之后的窗口，永不静默丢发射。**

---

## 5. 数据类型解析（记录层 → 元组层 → 值层）

### 5.1 记录层：WalRecordParser

vanilla 记录头 24B（`tot_len u32@0 | xid u32@4 | prev u64@8 | info u8@16 | rmid u8@17
| pad | crc u32@20`），偏移经 layout 注入（OG 档 32B——term u32@4 前置整体后移）。

两阶段走读（`WalRecordParser.java:102-234`）：
- **头区**：自 `recordHeaderSize()` 逐块 id 推进。块头（id≤32）内嵌
  `id | fork_flags | data_length u16`；fork_flags 位 HAS_IMAGE=0x10 / HAS_DATA=0x20 /
  SAME_REL=0x80（置位省定位器）；镜像头现代形态 5B（imageLen/holeOffset/bimgInfo）；
  定位器 `spc/db/relNode` 三 u32（OG 14B 含 extra_flag）+ blkno u32（OG 后随
  last_lsn u64）；标记头：253 origin 跳 5B、**252 TOPLEVEL_XID 读 u32**（子事务归并键）、
  255/254 DATA_SHORT/LONG 是 main 终止头。终止判据双通道：撞终止头，或剩余字节恰等于
  已声明 datatotal（无 main data 的记录如 XLOG_FPI 没有终止头）；
- **载荷区**：按块 id 序 `[image][data]` 回填绝对偏移，main 殿后；不变量收口
  `mainOff + mainLen == totLen`（不等即布局错配 ISE）。

产出 `WalRecord`（lsn/totLen/xid/toplevelXid/rmid/info/blocks/mainOff/mainLen/raw）；
`effXid()` 在有 252 标记时归并到顶层 xid。

### 5.2 heap 家族视图（HeapViews）

rmid：RM_XACT_ID=1 / RM_HEAP2_ID=9 / RM_HEAP_ID=10；opcode = `info & 0x70`：

| 形态 | main 布局（vanilla） | 备注 |
|---|---|---|
| INSERT (0x00) | `offnum u16 + flags u8`（3B） | OG INIT_PAGE 变体前多 pd_xid_base u64 |
| UPDATE/HOT (0x20/0x40) | 前 14B：old_xmax/old_offnum/old_infobits/flags/new_xmax/new_offnum | oldBlock 按 fork==0 且 ≠块0 选取；CONTAINS_OLD_TUPLE=0x04（FULL 前像拼 main 尾）/ OLD_KEY=0x08（拒绝面——非键列全 null 拼装会错值）；PREFIX/SUFFIX_FROM_OLD=0x20/0x40 截断位 |
| DELETE (0x10) | 8B：xmax/offnum/infobits/flags | CONTAINS_OLD=0x02\|0x04 时前像在 main 尾 |
| MULTI_INSERT (0x50, HEAP2) | `flags + pad + ntuples u16 + offsets[]`；INIT_PAGE 置位时 offsets 省略（offnum=i+1） | entry = `[datalen u16][5B 头][tail]` |
| INPLACE (0x70) | 只取 offnum u16 | V18 main 20B（V17 2B）——两版唯一 heap 结构差异 |
| PRUNE (HEAP2) | `reason + flags [+冲突水位]`；块0 data 按 XLHP 位序：freeze plans → redirected(n+2n) → nowdead → nowunused | OG 9.2 系 CLEAN 撞号经 `layout.legacyHeap2Clean()` 门控 |

**截断 UPDATE 拼装**（`TruncatedUpdateSplice`）：块 data 布局
`[prefix u16?][suffix u16?][5B 头]` 随后；prefix 段自**旧元组自己的 t_hoff** 起取
（旧/新位图宽可不同）、suffix 段自旧元组字节**末端**取——照服务端 `heap_xlog_update`
回放语义实源钉。

### 5.3 FPW 页镜像（PageImages）

CHECKPOINT 后某页首写必带全页镜像（FPW）。`rebuild()`：压缩位任一置位先 ISE——
**这就是 `wal_compression=off` 运维前提的原因**（PageImages 不带解压面）。HAS_HOLE 时
镜像存 head + tail 两段，洞区（`pd_lower..pd_upper`）零补，重建页长 = imageLen+holeLen。
镜像**是变更后状态**（redo 对带镜像块跳过 apply）——新行/旧行都在页内。ItemId 走读：
自页偏移 24 起每项 4B，位段 `lp_off:15 | lp_flags:2 | lp_len:15`（**LP_NORMAL==1**，
不是 2）；catalog 的 FPW 记录不含 tuple data，行值只能从镜像按 offsets 提取（发现 14）。

### 5.4 元组解码（TupleDecoder）

WAL 载荷形态 `[xl_heap_header 5B][自 offset 23 起字节]`：infomask2/infomask/t_hoff；
`natts = infomask2 & 0x7FF`；HASNULL 置位时 t_bits@23 起 null 位图 `(natts+7)/8` 字节；
**datum 对齐相对 tupleStart**（tuple 可坐在缓冲任意偏移——不能按外层绝对位置对齐）。

**varlena 四形态**（`varlenaStart` 落位 = PG `att_align_pointer` 同面）：
1. 1B 头（tag bit0=1）：总长 `(b0>>1)&0x7F`；
2. **1B external 指针（首字节恰 0x01）**：18B，委派 `VarlenaResolver.resolveExternal`；
3. 4B 头（tag 00）：总长 `u32le>>>2`；
4. 4B 行内压缩（tag 0x02）：剥头后 `[u32 tcinfo=原长低30位|方法<<30][压缩流]`，委派
   `decompressInlineCompressed`。

**external TOAST 指针 18B 布局**（ToastAssembler.java:19-33）：
`[0x01][vartag 0x12][va_rawsize u32][va_extinfo u32][va_valueid u32][va_toastrelid u32]`
——rawsize 含 4B 头；extinfo 低 30 位 extsize + 高 2 位方法（0=pglz / 1=lz4）；
压缩判定 `extsize < rawsize-4`（"we never use compression unless it actually saves
space"）。

**VarlenaResolver 接缝**：layout 层只识别形态并推进游标；external 拼装/解压/回查兜底由
changes 包 `ToastAssembler implements TupleDecoder.VarlenaResolver` 承担——解压算力驻
changes 层、layout 保持格式分发职责单一。**oidhdr 虚拟槽**（OG）：9.2 式元组 oid 在头垫
区 `[t_hoff-4, t_hoff)`，词典首位 "oidhdr" 时值注入槽 0、不占 natts。

### 5.5 WalLayout：V17 vs V18 关键差异

| 常量 | V17 | V18 |
|---|---|---|
| 页魔数 | 0xD116 | 0xD118 |
| sizeOfHeapInplace main | 2B | 20B（增 dbId/tsId/nmsgs） |
| pg_attribute 词典 | 26 项 | 25 项（V18 删 attcacheoff） |
| pg_class 词典 | 33 项 | 34 项（增 relallfrozen） |
| pg_class reltoastrelid 数据区偏移 | 108 | 112 |

**自检锚：两版 toast 偏移差必须恰等于单列宽 4B**（V17 首版曾把 108 误算 104——漏计
relfilenode 自占 [88,92)；单测钉死此不变量使同类漏计离线即红）。其余（页 8192、记录头
24、sizeOfHeapUpdate 14、relfilenode@88）两版一致。OG/VastBase 档差异（记录头 32B、
词典列集、name128、heap 前缀 21/19 等）见模块 CLAUDE.md 差异表。

### 5.6 TOAST 重组（ToastAssembler）

- **chunk 采集**：toast 关系（relkind 't'）的 heap 记录解码为三列
  `[chunk_id, chunk_seq, chunk_data]`，归集
  `Map<toastRelfilenode, Map<valueid, TreeMap<seq,bytes>>>`（TreeMap 使乱序到达自然
  有序、重复 seq 后到覆盖）；
- **重组**（`resolveExternal` 共核）：指针头校验 → 解 rawsize/extinfo/valueid/toastrelid
  → 按 toastrelid 直查归集面（未命中按 valueid 全域兜底扫描——valueid 全局唯一保无
  歧义）→ **空面判定先于方法位检查**：归集面空且非 INSERT 视图即返回
  `UNCHANGED_TOAST_MARKER`（渲 `<toast-unchanged>`，对齐 engine pgoutput 的 'u'——
  服务端 reorder buffer 的 toast hash 每条已施加变更后即重置，**回查反而产出 engine
  没有的值破坏对拍**）；归集面非空但不完整才走 JDBC probe 回查合并，仍缺降级
  `toast-unavailable` + WARN 不 fail 流；
- **解压分派 `inflate`**：方法 0 → `Pglz.decompress`（pg_lzcompress.c 逐位转录：
  control byte 8 条目 LSB 先行、位 0 字面量、位 1 match 2B tag、off 倍增分段回拷）；
  方法 1 → `Lz4.decompress`（lz4-java safeDecompressor 裸 block 形态）；未知方法码
  ISE；未压缩判定 `extsize == rawsize-4` 直接拼接；
- **清窗全局**：每条用户表行施加后 `resetToastWindow`、五终态后 `clear()`（valueid
  全局唯一使全清安全）；
- **insertTupleResolver 视图**（2026-10-13 flake 根因修复）：INSERT 新元组的 external
  值必为本事务新写（valueid 新分配无跨事务复用），空面即重放切断或清窗、engine 对照面
  持真值，**probe 回查补全**——两类空面成因一次关死；UPDATE 后像维持空面哨兵
  （'u' 对齐——未变列不可探测判别）。

### 5.7 值渲染（DiskValueRenderer，17 类型首发矩阵）

分派入口 `render(decoded, typeOid)`：

| 类型族 | 磁盘格式 → text 规则 |
|---|---|
| bool (16) | `t`/`f`（bool_out，非 `::text` 的 true） |
| int2/4/8、oid 等 | Number → 十进制（oid u32 无符号语义） |
| float4/8 (700/701) | **必须经 PgFloatFormat**：最短往返位串（BigDecimal 逐精度试探）+ %g 定点门限（float8 exp∈[-4,15) / float4 [-4,6)）+ `d[.ddd]e±XX` 指数恒带符号——补 `Double.toString` 的 `1.0E20` 分叉；前提会话 extra_float_digits=1 |
| numeric (1700) | 首 u16 flag 三路（特殊值 NaN/±Inf、短格式 0x8000 头 2B、长格式头 4B）；digit groups 逐 u16 base-10000，值 = Σ digit[i]×10000^(weight−i) |
| text/varchar/bpchar/name/char/json | String 原文透传 |
| bytea (17) | `\x` + 小写 hex |
| date (1082) | 4B i32 天，epoch 2000-01-01 |
| time (1083) | 8B i64 当日微秒；微秒去尾零；24:00:00 闭上端特判 |
| timetz (1266) | 12B = i64 微秒 + i32 西正秒区偏移；输出反号三档 |
| timestamp (1114) | 8B i64 微秒（epoch 2000）；空格分隔、小数去尾零 |
| timestamptz (1184) | 8B i64 UTC 微秒按 JVM 默认时区渲染 + 数字偏移后缀（+08 / +05:30 / +00） |
| uuid (2950) | 16B 网络序 → 8-4-4-4-12 小写连字符 |
| **矩阵外** | `0x` + hex 降级 + 每 oid WARN 一次（输出可读性降但流不断） |
| dropped 列 | `∅` 占位；NULL 渲 `NULL` 字面 |

定长载荷长度不符 ISE（走读错位信号）。列 kind 由 typeOid 经 `DecodeKinds.forTypeOid`
单源派生，未知 oid 解码即 ISE（"宁可拒解不猜格式"）。

---

## 6. 物理复制槽推进

这是"伪备库"路线的一个独特问题：**物理流本身不附槽**（pgjdbc 的
`START_REPLICATION PHYSICAL` 不带 SLOT——驱动无槽选项），所以 restart_lsn 不会随消费
推进。槽在本模块的角色是 **WAL 保留窗口的载体 + 位点锚**。

### 6.1 建槽：immediately_reserve（PhysicalSlotManager）

- vanilla：`SELECT * FROM pg_create_physical_replication_slot(?, true)`——第二参
  **immediately_reserve=true** 使 restart_lsn 自建槽时刻非 NULL。未 reserve 的槽
  restart_lsn 为 NULL，`pg_replication_slot_advance` 直接被服务端拒绝
  （"never previously reserved WAL"）——**建槽必须 `(?, true)`**；
- 复用查询：`SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=? AND
  slot_type='physical'`（双条件）；restart_lsn NULL（存量未 reserve 槽）回落当前
  flush LSN；42710 duplicate_object 竞态静默吸收走复用路径；
- 新建路径返回值取 `pg_current_wal_flush_lsn()`——**用 flush 而非 write**：write 位点
  可能超前 flush，用它起流会被服务端拒绝（spike 实证）；
- OG 变体：第二参是 **dummy_standby**（false，不可照抄 vanilla 的 true）；位点函数
  `pg_current_xlog_location()`（9.2 系命名，无 flush 档）。

### 6.2 推进时机与目标：检查点持久化成功之后、按 floor 封顶

`CatalogSynchronizer.persistCheckpoint/advanceSlot`（:804-849）：

```text
每次检查点：
  store.checkpoint(stores, lsn, floor)   ← ① 先持久化成功
  lastCheckpointLsn = lsn
  SELECT pg_replication_slot_advance(?, <目标>::pg_lsn)   ← ② 才推槽
    目标 = floor > 0 && floor < lsn ? floor : lsn          ← 按 floor 封顶
```

- **次序（先落盘后推进）**：崩溃最坏重复重放已落盘之后的窗口，不丢状态；
- **floor 封顶**：槽保留窗口必须覆盖 DML 待决桶的重放段——越程推进会让续传起流点
  之前的 WAL 被回收、挂起 2PC 桶永久不可重建；
- **失败 WARN 不中断**：推进是保留窗口的优化而非正确性前提（续传正确性不依赖槽状态
  ——重连起点取接收器消费前沿）；`max_slot_wal_keep_size` 兜底磁盘；
- **advanceLsnCast 门控**（:143-150）：PG 档缺省 `"::pg_lsn"` cast；**OG 无 pg_lsn
  类型**，`startOg` 装配期覆写为空串 text 直传——否则每检查点一条 ERROR 槽恒不推。

观测面：`lastCheckpointLsn()` 与 `lastSlotAdvanceLsn()` 相等即"落盘后必推过"（Main
周期行 `ckpt= adv=` 数据源）。

---

## 7. DML 事务组装与输出（changes 包）

### 7.1 TableFilter：三道闸

relfilenode（块头只携带段号）经 as-of 快照解析表身份：①relkind ∈ {'r','p'}（OG 档改
parttype 语义）；②非系统模式（pg_catalog/pg_toast/information_schema）；③白名单
（空 = 全放行）。miss 一律 Optional.empty() 静默跳过（噪声表属常态）。OG 分区身份链：
heap 记录路由到**分区自己的 relfilenode**（pg_partition 'p' 行，不在三表字典）——
`partitionOf` 命中即走**母表身份**链（属主优先不回退），白名单母表名/分区名双匹配。

### 7.2 XactGrouper：事务组装状态机

- **提交时批量发射**（控制器裁定，spec §4 偏差记档）：heap I/U/D 行**到达期解码+
  渲染**入桶累积，COMMIT/COMMIT_PREPARED 时 Begin → 过滤后行 → End 批量回调——
  aborted 子事务过滤与逐行即时回调不可同时成立（子回滚到达时已发出的行无法撤回）；
- **子事务归并**：记录头 xid 是产生行的（子）事务；252 TOPLEVEL_XID 标记优先归并
  顶层；映射双通道（行记录到达时顺路收割 subToTop + ASSIGNMENT 兜底）；子回滚把子
  xid 记入桶的 abortedSubxids、发射期扣减；顶层 ABORT 整桶弃（onAborted 零输出）；
- **2PC**：PREPARE 挂起桶（记 gid，零发射）；COMMIT_PREPARED 按 main 的 twophase
  chunk xid（非记录头 xid）发射 twoPhase=true + gid；挂起桶跨重启安全（§4.4 floor）；
- **TableMeta 缓存**：按 relfilenode 缓存 resolve 结果；失效策略 = 字典 watched 表上的
  heap 记录到达即**全清**（ADD/DROP/EXCHANGE 后首行即按新身份渲染）；
- **XACT main 布局**（REL_18 实源钉）：commit/abort 最小 main = 裸 xact_time i64；
  HAS_INFO(0x80) 时 xinfo u32@8 + 块链（subxacts 4+4n / twophase xid+gid 等）；
  PREPARE main = 72B 两阶段状态文件头（gid@72 按 gidlen 截取，非 NUL 结尾）。

### 7.3 OutputRenderer：格式复刻契约

输出契约复刻 engine `ConsoleRenderer` 事务块格式（`TXN-BEGIN xid=... kind=...
changes=N` / `  [seq] INSERT schema.table BEFORE=... AFTER=[...]` / `TXN-END`），
走 CDC 专用 logger `org.vastdata.vbstream.walsource.cdc`（INFO）。头行 changes 是
End 终值（Begin 只缓存头字段开桶，End 时整桶 flush）；值截 64 字符附
`...(NB)`；dropped ∅ / NULL 字面同形。**双路对拍 IT（DualPathParityIT）以此格式为
契约**：engine 逻辑解码路 vs wal 直解路按 xid 交集逐行 diff 空。

---

## 8. 配置面与观测面速查

配置三层合并（宿主层）：工作目录 `walconfig.properties` 基础值（十六键白名单，外键
WARN 忽略）→ `-Dvb.wal.*` 同键覆盖 → 门面构造器默认值兜底；`-Dvb.config=<路径>` 整份
替换。核心键：

| 键 | 默认 | 语义 |
|---|---|---|
| vb.wal.host/port/db/user/pass/slot | localhost:5432/postgres/postgres/postgres/wal_source | 连接与槽 |
| vb.wal.state.dir | 空 = 禁用 | 检查点目录（启用后含续传/落盘/槽推进整个生命周期面） |
| vb.wal.state.interval.ms / events | 30000 / 1000 | 双节拍先到为准 |
| vb.wal.tables / vb.wal.dml | 空=true | DML 白名单 / 开关（false 回纯 catalog 形态） |
| vb.wal.og.source / rx.mode / rx.dir / rx.bin / jdbc.prefix | file / managed / wal-og-queue / pg_receivexlog / jdbc:postgresql: | OG 档五键（PG 17/18 不读） |

观测面（Main 每 10s 周期行）：
`smoke: lsn=… records=N resyncs=N reconnects=N censusTop3=[…]` + state 启用时
`ckpt=LSN adv=LSN`（相等即"落盘后必推过"）+ DML 启用时 `dml=[buckets=N rows=M
partMiss=K]`。接收器终态死亡（5 次重连失败/解析 ISE）经 `receiverTerminalFailure()`
检测 → ERROR + exit 1。

---

## 9. 关键不变量清单（改代码前默诵）

1. **施加次序固定** PRUNE → INPLACE → attr → class → nsp → partition（单记录内）；
2. **sink 次序固定** catalog apply → DML onRecord → 检查点（as-of 与 floor 正确性）；
3. **两级种子**：引导 = 页对齐流起点（重叠段重放消化）/ 续传 = stored lsn（严格过滤）；
4. **检查点先落盘、后推槽；槽推进按 floor 封顶**；
5. **`mainOff + mainLen == totLen`**（记录布局自检）；**V17/V18 toast 偏移差恰 4B**；
6. **datum 对齐相对 tupleStart**；ItemId `LP_NORMAL==1`；块内载荷序 `[页镜像][block
   data]`；
7. **wal_compression=off 是运维前提**（压缩 FPW ISE）；DML 面实际前提
   **wal_level=logical**（REPLICA IDENTITY FULL 前像）；
8. **一个检查点目录同一时刻仅一个活实例**（文件锁未做，运维契约）；
9. 字节地址锚 = walker 自维护数据末位（**勿采信 getLastReceiveLSN**——keepalive 跳变）；
10. 流起点/终点必须用 flush 位点（write 可能超前 flush，START_REPLICATION 拒绝）。

## 10. 已知限制（摘要，全文见模块 CLAUDE.md）

- `wal_level=replica` 的 UPDATE 系统性缺行（前缀省略 + 无旧元组不可重建）→ liveness
  guard 有痕跳过；
- STREAMED kind 分叉（engine 流式驱逐形态 vs wal 侧恒 NORMAL）→ parity 环境禁驱逐规避；
- 矩阵外类型（enum/域/jsonb/组合）`0x` 降级；dropped 列两路分叉（对拍矩阵避开）；
- VACUUM FULL/CLUSTER 的 DML 语义未承诺；NSP redirect 缺行静默跳过（fail-safe 方向）；
- UPDATE 改写宽列 + 恰逢重放切断的残余窄缝（无本地判别式）；
- 长挂起 2PC + 频繁重启 → 挂起桶整段重放（功能正确、成本线性增长）。

## 11. 延伸阅读索引

| 文档 | 内容 |
|---|---|
| `docs/wal-direct-decode-spike.md` | 先导实验：可行性 + 27 条发现（很多布局坑的原始出处） |
| `docs/superpowers/specs/2026-10-05-wal-source-module-design.md` | v1 设计（catalog 同步 + WAL 接收，竞速三修正、两级种子） |
| `docs/superpowers/specs/2026-10-06-wal-source-v2-design.md` | v2 DML 面设计（changes 包、双路对拍） |
| `docs/superpowers/specs/2026-10-11/12/13-wal-source-opengauss-m1/m2/m3-design.md` | OG 档三里程碑（双通道/XACT 接缝/分区第四字典） |
| `docs/superpowers/specs/2026-10-13-wal-source-vastbase-adaptation-design.md` | VastBase G100 档（第四布局档） |
| `docs/superpowers/specs/2026-10-15-wal-source-offline-replay-design.md` | 离线回放档（2026-10-15：检查点 + 段快照目录离线续传，§2.5） |
| `docs/wal-source-offline-replay-spike.md` | 离线回放三轮 spike（含 parseSegNo 段号 bug 实证） |
| `docs/wal-source-opengauss-adaptation-research.md` | OG 适配调研（含 VastBase 勘误表） |
| `vb-stream-wal-source/CLAUDE.md` | 模块全量文档：演进史、OG/VastBase 差异表、已知限制详版 |
| `vb-stream-wal-source/README.md` | 快速开始（含 openGauss 快速开始） |
