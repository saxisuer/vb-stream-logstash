# vb-stream-wal-source — WAL 直解源（伪备库 catalog 同步 + WAL 接收 + 用户表 DML）

不创建备库、不占主库逻辑解码 CPU 的"伪装备库"路线：pgjdbc **物理复制流**接收原始 WAL
字节，纯 Java 按 REL_STABLE 源码头文件转录的版本布局描述符解析记录，自维护
pg_attribute/pg_class 的 **ctid 重放 as-of 字典**。v1（2026-10-05 spec）交付两件事——
**A. catalog 同步**（JDBC 一致性引导 + WAL 重放维护两张目录表快照，含 ctid、
relfilenode、toast 映射、dropped 占位语义）与 **B. WAL 接收**（物理流接收、解析、
位点管理、检查点持久化）；**v2（2026-10-06 spec）在其上完成用户表 DML 解码与事务
组装**（changes 包——XactGrouper 事务组装 / TOAST external 重组 / engine 格式复刻
输出），并以**双路对拍 IT**（engine 逻辑解码路 vs wal 直解路，六场景 + 干扰矩阵 +
CREATE SCHEMA）证明两路 CDC 输出逐字节等价。设计全文见
`docs/superpowers/specs/2026-10-05-wal-source-module-design.md` 与
`docs/superpowers/specs/2026-10-06-wal-source-v2-design.md`（先导实验
`docs/wal-direct-decode-spike.md` + `spike/wal-parse` throwaway）。

**依赖方向刻意收窄**：运行依赖仅 pgjdbc + slf4j-api，零 engine/Chronicle/logback
依赖——将来是 engine（或连接器）依赖本模块，不是反过来（v2 裁定：两模块是**平级的
解析工具**，输出格式对齐靠双路对拍测试钉死、不靠代码共享；engine 仅以 test-scope
依赖出现在对拍 harness）。包结构 `org.vastdata.vbstream.walsource` 下
`layout`/`receive`/`replay`/`state`/`api`/`changes` 六包 + 顶层冒烟 `Main`。

## 组件（五组件 + api 门面，全部面向接口）

| 组件 | 位置 | 职责要点 |
|---|---|---|
| `WalLayout`/`WalLayouts` | `layout` | 版本化布局常量（`WalLayoutV17`/`WalLayoutV18`，逐常量标注 REL_STABLE 出处）；`forServerVersion` 按 `server_version_num` 分发，未注册版本/显式错配布局启动期 ISE fail-fast |
| `WalStreamReceiver` + `WalStreamWalker` + `PhysicalSlotManager` | `receive` | 物理复制流接收（readPending drain 轮询 + 断流指数退避重连 1s→16s、5 次失败停机）；walker 是字节流走读状态机（chunk 拼接/contrecord 缝合/**pageaddr 锚定协议**）；槽管理（`immediately_reserve=true` 建槽，restart_lsn 自建即保留——推进通道与保留窗口同时成立） |
| `WalRecordParser` + `WalRecord`/`BlockRef`/`HeapViews`/`PageImages`/`TupleDecoder`/`DecodeKinds` | `layout` | 记录头/block 头/heap 家族（INS/UPD/DEL/HOT/INPLACE/MULTI_INSERT/PRUNE）→ 强类型 record，纯函数；FPW 页镜像重建（hole 拼接 → ItemId 走读 → 页内 tuple 解码） |
| `CatalogSynchronizer` + `CatalogReplay`/`CatalogStores`/`CatalogBootstrap`/`CatalogRow`/`SelfHealer`/`JdbcProbe(Impl)`/`HeapEvent`/`Reconstruction` | `replay` | 通用 ctid 重放引擎（watched 表注册制、relfilenode/toast 跟踪、JDBC 一致性引导、竞速三修正）；单记录施加次序固定 PRUNE → INPLACE → attr 行事件 → class 行事件 |
| `StateStore` + `StateConfig` | `state` | 检查点持久化：单文件 `wal-source-state.bin`（magic 'VBWS' + header/ footer 双 CRC + lsn 双验；formatVersion 3 增 `dmlFloorLsn`——DML 待决桶重放下界，Task 11），全量序列化 `.part` → fsync → 原子 rename；损坏/版本不符（含 pgVersion 与 layout 错配——写与 load 校验同 `layout.majorVersion()`，17 写 17、18 写 18）拒载回落全新引导（安全侧：宁可重引导，不可错位窗口重放） |
| `WalSource`（api 门面）+ `CatalogSnapshot` | `api` | 一次 `start()` 装配全管线（SQL 会话 → 版本分发 → 同步器），`consumedLsn()`/`metrics()`/`lastCheckpointLsn()`/`lastSlotAdvanceLsn()`/`resumedFromState()` 观测面——v2 起接收 sink 单 pass 分发两消费者（catalog 先施加、DML 后解码，as-of 次序天然成立）；v2 DML 配置面 `vb.wal.tables`/`vb.wal.dml` 与发射计数观测同在此门面 |
| `ChangeStream` + `XactGrouper`/`TableFilter`/`ToastAssembler`/`ToastProbe(Impl→JdbcToastProbe)`/`Pglz`/`DiskValueRenderer`/`PgFloatFormat`/`OutputRenderer`/`ChangeOutputListener` | `changes` | **v2 DML 面**（详见下节"v2 DML 面"）——TableFilter 白名单过滤 → XactGrouper 事务组装（提交时批量发射）→ TOAST 重组三形态（external 未压缩/pglz/行内压缩）+ 窗口前指针回查兜底 → 磁盘格式值渲染 PG text → OutputRenderer 复刻 engine `ConsoleRenderer` 事务块格式输出 CDC logger |

单线程直通执行模型（接收 → 解析 → 重放/解码 → 周期落盘全在 wal-receiver 线程，sink
同步执行——检查点取当前已消费 LSN 即天然一致点，无需快照冻结）；组件接口化留解耦位。

## 竞速鲁棒性三修正（核心质量设计，spike 实测通过率 ~1/3 的三根刺）

1. **引导一致性**：建/复用物理槽记 consistent_point P₀ → REPEATABLE READ 单事务内
   **首句读 `pg_current_wal_flush_lsn()`（B，先于任何种子查询——终审 C1 修复：原
   "种子后再读"形态下 (S,B] 并发提交的 DDL 效果既不在种子快照、WAL 记录又被
   appliedLsn 过滤线永久跳过）** → 两表全行种子（快照时点 S ≥ B）→ 流从 max(P₀, B)
   起收且**引导路径 appliedLsn 过滤线种子 = 页对齐流起点**（快照字典无"已施加"记录，
   流交付的重叠段——(B,S] 窗口 + 在途事务跨 B 的记录[INSERT 已 flush ≤ B 而 commit
   晚于快照，实测不重放会偶发丢 attr 行]——一律重放：ctid 键控 upsert 幂等 + 截断
   更新末态采纳收敛，引导后小段自愈 probe 噪声属预期；续传/forced 路径过滤线仍 =
   stored/forced lsn 未对齐值）；已施加前沿种子化——引导后首条记录施加前
   `snapshot().lsn()` 即引导一致点而非 0。
2. **ctid 寻址精确采纳自愈（末态回填语义）**：截断更新未知 oldCtid 时按**记录 new 位**
   探测 JDBC 末态行——更新必移行位，"末态仍居 new 位"的行即该记录施加后的精确状态，
   整行采纳（行值全来自探测行，无拼装时序问题）；该位无行（已再迁移）skip 计数、链由
   该 oid 末条记录追平后收敛——**终审补：PRUNE redirect 的 from 位无行时按 to 位精确
   采纳物化**（"末条记录是 redirect"的断链形态：种子行无 raw tail，截断更新采纳因行已被
   压实搬迁至 redirect 目标位而拒绝，旧版 redirect 对缺行 no-op 使该行永久丢失——终审
   全量复跑 ~1/5 套次实测命中；物化语义与截断更新采纳同源，attr 面 attnum≤0 系统列不落）。
   **语义边界：末态回填而非历史重建**——丢页/断链窗口内的
   中间代际不可恢复，v1 只承诺 catalog **末态**正确（对拍面即末态全等），不承诺逐记录
   时点正确。spike/Task 12 的"候选枚举 + 中段值校验"形态经对抗性 IT 实证废弃（候选
   前缀源是断链窗口内历史快照，拼装产出时代错位混合行）。per-oid 连续采纳拒绝 ≥3
   登记 staleOids + WARN（观测面；"待下轮引导"落在重启路径，进程内热重引导 v1 不做）。
3. **pageaddr 锚定 + 自维护数据锚**：页头 `xlp_pageaddr` 是页内导航唯一权威——状态机
   维护期望页地址逐页校验，失配在块内扫描下一个合法页头（magic 对 + pageaddr 页对齐
   且与游标推算地址差 ≤2 页）重锚，resyncs 计数；**字节地址锚由 walker 自维护数据末位
   承担**（连接内字节流严格连续），`getLastReceiveLSN()` 只作首块/漂移观测——它随
   服务端 keepalive 的 walEnd 跳变，采信它会假再同步甚至静默跳段。`resetAnchor()` 在
   （重）连接建立时清零，首块回落调用方游标锚（首块不能预锚请求位：服务端自请求位
   读到首个记录边界才起发，预锚实测挂起/假再同步）。良性/恶性分列：空页跳过
   （dropped==0）只计 resyncs，丢字节（dropped>0）另计 lossyResyncs。

## v2 DML 面（changes 包，2026-10-06 spec）

**包结构**（`org.vastdata.vbstream.walsource.changes`）：

| 类 | 职责 |
|---|---|
| `ChangeStream` | DML 门面：单入口 `onRecord(WalRecord)`，WalSource sink 分发的第二消费者（catalog 同步器之后）；内建组装三件套装配（ToastAssembler probe 取 SQL 会话建 JdbcToastProbe，null 连接 = 纯回放禁回查档） |
| `XactGrouper` | 事务组装状态机：heap I/U/D 行入桶累积（到达期解码 + 渲染），XACT 终态驱动发射/弃桶/挂起；子事务归并双通道（TOPLEVEL_XID 收割优先 + ASSIGNMENT 兜底）；TableMeta 按 relfilenode 缓存、字典三表 heap 记录到达即全清失效 |
| `TableFilter` | relkind 'r'/'p' + `vb.wal.tables` 白名单（`schema.table` 逗号分隔，空 = 全放行）；从 v1 三表字典 as-of 解析表身份与列序 |
| `ToastAssembler` | TOAST 重组：toast 关系 chunk 行采集（valueid→seq→bytes TreeMap）+ external 18B 指针剥解（rawsize/extinfo/valueid/toastrelid）+ pglz 解压 + 窗口前指针 JDBC 回查兜底 + unchanged-TOAST 哨兵；实现 `TupleDecoder.VarlenaResolver` 接缝（external 指针 + 行内压缩 varlena 同一解压面） |
| `Pglz` | PG pglz 解压纯移植（`pg_lzcompress.c` 主循环逐行转录，control byte 分组 + match 回拷，L705-790 源码锚） |
| `DiskValueRenderer` | 磁盘格式值 → PG text（17 类型首发矩阵：bool/int2/int4/int8/float4/float8/numeric/text/varchar/bpchar/json/bytea/date/time/timetz/timestamp/timestamptz/uuid；矩阵外 `hex:` + WARN 一次、dropped 列 ∅） |
| `PgFloatFormat` | PG 风格浮点格式化器（Ryū 最短往返 + %g 定点门限——`1e+20`/`0.0001`/`-0`，补 `Double.toString` 的 `1.0E20` 分叉；前提会话 `extra_float_digits=1` 缺省档） |
| `OutputRenderer` | pending 缓冲 + TXN-BEGIN/逐行/TXN-END 输出（**engine `ConsoleRenderer` 格式复刻契约**——头行 changes 终值在 End 组装、值截 64 附 `...(NB)`、dropped ∅/NULL 字面同形）；onAborted 丢桶零输出 |
| `ChangeOutputListener` | 中立事件接口：`BatchBegin`/`RowChange`/`BatchEnd`(expectedChanges)/`BatchAborted` |

**双路对拍契约与执行机制**：输出格式以 engine `ConsoleRenderer` 为**契约源**（streaming
形态的 TXN-BEGIN/逐行/TXN-END 三段样板 + 行值截断规则），改动需双端同步（debezium19
双线同步约定同模式）；执行机制 = `DualPathParityIT`——同一 PG 18 容器、同一批 DML，
engine 逻辑解码路（in-process `PgReplicationSession + TransactionAssembler +
ConsoleRenderer`，`EnginePathRunner` 挂 logback ListAppender 捕获 CDC logger）与 wal
直解路（`WalSource` DML 面 + `OutputRenderer`）各自独立槽位捕获输出，**按 xid 求交集后
逐行 diff 断言空**（两路起停窗口造成的事务集合差由交集归一化吸收；头行 changes 语义差
——engine 取过滤前到达面/wal 取桶记账——按 "engine.changes==行数 && wal.changes≥行数"
归一化；parse 对未知行（engine 生命周期控制行/两路观测行）显式拒绝不静默吞）。六场景
矩阵：①基础 DML 六形态（单行/多语句/交错/回滚零输出/SAVEPOINT 子事务剔除/截断中段列
UPDATE）②17 类型边界值矩阵 ③TOAST 三存储形态 + 重启后未变列指针双路同渲染
`<toast-unchanged>` ④事务内 ADD COLUMN 前后段 as-of ⑤2PC 四形态（挂起零输出/弃桶/
COMMIT PREPARED/挂起重启续传）⑥普通 DML 中途停续（at-least-once 重发双路一致）；
矩阵外补充：流内 CREATE SCHEMA（nsp 字典面）+ 干扰矩阵（catalog 风暴线程下场景 1/TOAST
复跑 ×2）。parity 环境定 `logical_decoding_work_mem=64MB` 禁驱逐（两侧都走 NORMAL 形态）。

**关键机制**：

- **提交时批量发射**（spec §4"即时发射"的落地偏差，控制器裁定记档）：行在桶内累积、
  COMMIT/COMMIT_PREPARED 时 Begin→过滤后行→End 按序批量回调——aborted 子事务过滤与
  逐行即时回调不可同时成立（子回滚到达时已发出的行无法撤回）；输出缓冲堆 O(事务)
  （与 engine block 模式同级），落盘化是 v3 升级路径。
- **pendingFloorLsn 跨检查点 at-least-once**（Task 11 修复）：检查点 LSN 可越过挂起
  （或进行中）桶的行记录所在页——XactGrouper 暴露 `pendingFloorLsn()`（待决桶首记录
  lsn 最小值），检查点随 lsn 落盘（**StateStore formatVersion 3 增 `dmlFloorLsn` 字段**）、
  槽推进按 floor 封顶、续传流起点取 min(stored, floor) 页对齐（catalog 过滤线仍 =
  stored 不二次施加字典）——重放段重建桶后终态补发，关死"检查点越过待决桶→续传静默
  零发射"的 at-most-once 缺陷。
- **unchanged-TOAST 哨兵**（Task 10 值面裁定，REL_18 源码钉）：服务端 reorder buffer
  的 toast hash 每条已施加变更后即重置（decode.c `clear_toast_afterwards`），未变列
  指针 engine 恒发 pgoutput `'u'`——wal 侧 ToastAssembler 归集面空即返回
  `UNCHANGED_TOAST_MARKER`（**不回查**——回查会产出 engine 没有的值破坏对拍），渲染
  `<toast-unchanged>` 同形；归集面非空但不完整才走 probe 回查合并（角色收窄为补全窗口
  内新写值缺口），回查后仍缺降级 `toast-unavailable` + WARN 不 fail 流。**清窗是全局的**
  （镜像服务端逐变更重置但无 per-txn hash）：XactGrouper 每条用户表行施加后
  `resetToastWindow`、五终态后 `clear()`——valueid 全局唯一（oid 计数器单调）使全清安全。
- **截断 UPDATE**：FULL 身份（CONTAINS_OLD_TUPLE）经 `TruncatedUpdateSplice`（layout 包）
  旧元组字节拼装重建（Task 8.5）；非 FULL 保留 liveness guard（行级跳过 + WARN/表 +
  skipped 计数）。逻辑流下省略门与 CONTAINS_OLD 互斥（heapam.c 实源钉），双计数恒 0 作
  回归哨兵。

**已知限制与 v3 方向**：

- **replica 形态 UPDATE 系统性缺行**：`wal_level=replica`（无逻辑日志）下用户表 UPDATE
  走前缀/后缀省略且 DEFAULT 身份无旧元组——重建不可达，liveness guard 有痕跳过。DML 面
  的实际前提是 **`wal_level=logical`**（README 已限定）；跨记录行态重建（省略段 + 前后
  记录关联）是 v3 或非目标声明，需用户裁定。
- **STREAMED kind 分叉**：engine 逻辑解码流式驱逐形态输出 `kind=STREAMED` + 行尾
  `[streamed xid=N]`，wal 侧恒 NORMAL（WAL 无驱逐概念）——parity 环境禁驱逐规避；真
  流式形态对齐需 wal 侧模拟驱逐语义，v3 议。
- **矩阵外类型**：首发 17 类型集外的列（enum/域/jsonb/组合类型等）`hex:` 降级 + WARN
  ——双路对拍在该集外会 diff，文档化差异面。
- **lz4**：字典面 attcompression=='l' 启动期 WARN + 运行期撞 lz4（归集面非空）ISE
  fail-fast；无解压面。空归集面的 lz4 未变列指针同走哨兵（Task 13 顺手修）。
- **大事务输出缓冲 O(事务)**：pending 桶攒行文本至提交，与 engine block 模式同级；
  落盘化（格式层/file）是 v3 升级路径。
- **清窗全局 vs per-txn 窄缝**（v3 合并条目）：全局清窗使交错事务 B 的行夹在 A 的
  chunk 写入与引用行之间时 A 的窗口被清（服务端按事务分 hash 不受影响），A 行多见
  一个 `<toast-unchanged>` 占位——对拍面已知分叉；同类窄缝：①目录三表行不经
  resetToastWindow（catalog 行无 TOAST 面，理论无值面影响）②重放窗口内字典 as-of
  断裂边界（同事务 DDL+挂起+检查点跨页+重启四重叠加）③**重发渲染用字典持久化末态
  而非行记录时刻态**——跨停机点 DDL + DDL 前行落重发页的四重叠加窄缝会判"重发块
  不等"（检测面假红，非静默错值）。v3 方向合并为"重放段按记录 lsn 重建字典 as-of +
  已发射前沿持久化过滤线 + per-txn toast 窗口"。
- **长挂起 + 频重启重放放大**：挂起 2PC 桶跨每次重启整段重放（floor 不前进），无护栏
  ——桶快照持久化是 v3 方向。
- **NSP redirect 缺行无采纳通道**：pg_namespace 面 PRUNE redirect 缺行放弃物化（无
  ctid 探测通道）——DEBUG + `nspRedirectMisses` 计数观测（Task 13 补），仅全新引导
  自愈；触发需 nsp 链断 + schema DDL，fail-safe 方向（miss 跳过非错数据）。
- **干扰矩阵无 CHECKPOINT 面**：DualPathParityIT 干扰线程 = autovacuum toggle +
  ANALYZE 循环（catalog 风暴）；随机 CHECKPOINT 干扰面归 v1 承载（`WalAdversarialIT`
  已有），parity 侧未叠加以保基线稳定。



- **pg_class 行内偏移 88/108-112 与"两版差恰 4B"自检锚**：pg_class 行数据区
  relfilenode@88（V17/V18 同）、reltoastrelid@108（V17）/112（V18）——INPLACE 直读与
  截断重建的读区几何都由 layout 这两偏移锚定。V17 首版曾把 108 误算 104（漏计
  relfilenode 自占 [88,92) 4B），被 17 对拍 IT 偶然掩盖（收尾全行 UPD 把误读值修复
  回去）——**V17 与 V18 的 toast 偏移差必须恰等于单列宽 4B**，单测钉死此不变量使同类
  漏计离线即红。
- **锚定漂移根因 = getLastReceiveLSN 随 keepalive 跳变**（见三修正 ③）——排查流错位
  先看 walker 自维护锚是否被绕过。
- **`pg_stat_replication` 视图谓词下推自杀**：`SELECT ... FROM pg_stat_replication
  WHERE pg_terminate_backend(pid)` 会因视图谓词下推杀全场含自己——规避写法是先物化
  pid 再按值 terminate（IT 已落地该形态）。
- **RENAME 截断更新的 suffix 读区回填**：截断 suffix 零填充的合法条件是**数据区判据**
  （零填充起点 ≥ 读区末尾），越界时必须把 [零填充起点, min(dataLen, 读区末尾)) 段从
  旧行**读区编码回填精确值**（suffix 截断省略段与旧元组逐字节相同是其定义）——首版
  纯零填充在 RENAME 链上产出零字段行（relnamespace/reltype/relowner/relam 全 0）。
- **一个检查点目录同一时刻仅一个活实例**：旧实例后 stop 会以陈旧状态覆盖新实例的
  检查点（文件锁后续任务）——"一目录一活实例"是隐含运维契约。
- **续传重放窗口非幂等 → LSN 过滤线（种子按字典来源两级）**：接收器流起点按页对齐
  **下取整**，续传时 stored lsn 所在页的前段记录会被重发；`apply()` 对记录末尾 ≤
  已施加前沿（单调）的直接跳过——窗口内部分记录类二次施加非幂等（对 tail 已被
  INPLACE 失效的行做截断重建会产出部分零字段的行）。前沿种子分级：**续传/
  forced = stored/forced lsn（未对齐，严格过滤）**；**全新引导 = 页对齐
  流起点（快照字典无"已施加"记录，重叠段重放消化，终审 C1）**。
- **未 reserve 的物理槽不可推进**：`pg_create_physical_replication_slot` 缺省
  `immediately_reserve=false` → restart_lsn 为 NULL，`pg_replication_slot_advance` 直接
  拒绝——建槽必须 `(?, true)`。
- **pgjdbc 42.7.13 物理流无槽选项**：`START_REPLICATION PHYSICAL <lsn>` 不带 SLOT，
  流本身不附槽——restart_lsn 不随消费推进，保留窗口靠检查点后经 SQL 会话
  `pg_replication_slot_advance` 主动推 + `max_slot_wal_keep_size` 兜底。续传正确性
  不受影响（重连起点取接收器消费前沿，不依赖槽状态）。
- **wal_compression=off 是运维前提**：PageImages 不解压压缩 FPW——CHECKPOINT 后首写
  必带页镜像，压缩 FPW 记录会 ISE（v1 非目标）。

## 已知限制

- **接收器终态死亡进程面**：接收器 5 次重连失败/解析 ISE 后线程自行退出（ERROR），
  冒烟 `Main` 周期行经 `WalSource.receiverTerminalFailure()` 检测到终态 → ERROR 一行 +
  `System.exit(1)`（对齐引擎 fail-fast，终审 I2）；库形态宿主须自行轮询该面。
- **56KB 服务端侧地址跳变未根因**：高负载风暴下 ~5-10%/rep 触发服务端侧流形态跳变
  （空页跳过为主、偶发丢一页 + 56KB 地址差，疑 pgjdbc/walsender 高负载投递形态）——
  walker 每次 pageaddr 重锚自愈且对拍全等未破；IT 断言 resyncs ≤ 2 容差 +
  lossyResyncs 观测面；丢页场景有专门 IT 验证**末态追平**（ctid 精确采纳 + INPLACE
  全行改写 + PRUNE 陈旧清扫三机制把断链追平）。若 18 侧对抗 IT 未来出现"快照侧缺行"
  假红，应回查 replay 链（attr INSERT 在页竞争窗口的到达/解码）而非继续靠次序规避。
- **17 风暴交织丢 INSERT 跟进项**：干扰风暴与 DDL 并发交织时 1/4 执行丢一条 ADD
  COLUMN 的 pg_attribute INSERT 事件——17 矩阵裁定 DDL 先行、风暴后置规避。**根因面
  注记（Task 13 勾销核对）**：该丢失形态与引导一致性 C1 修复前的窗口同形（引导
  "(S,B] 并发提交的 DDL 效果既不在种子快照、WAL 记录又被 appliedLsn 过滤线永久跳过"
  ——风暴推高跨窗口在途事务概率），C1（首句读 flush LSN）+ Task 11 pendingFloorLsn
  后两个吞记录通道均已闭合；17 风暴矩阵的次序规避保留、专项复验未重跑，复验绿后可
  勾销本条。
- **压缩 FPW 不支持**：pglz/lz4/zstd 压缩页镜像一律 ISE——运维前提 `wal_compression=off`
  （CHECKPOINT 后首写必带页镜像）；external/行内压缩 varlena 已在 v2 由
  `ToastAssembler` 重组解压支持（lz4 压缩值除外，见 v2 限制）。
- **VACUUM FULL 全表重发的 CDC 语义**：v1 非目标——重放面按新 relfilenode 跟踪字典，
  但用户表 DML 语义未承诺。
- **two_phase 记录**：v1 不消费；v2（Task 5）起 XactGrouper 已实现 PREPARE 挂起/
  COMMIT_PREPARED 发射（kind=TWO_PHASE + gid，归属键 = main 的 twophase chunk xid）/
  ABORT_PREPARED 弃桶，**挂起桶跨检查点安全（Task 11 修复）**：检查点 lsn 可越过挂起
  （或进行中）桶的行记录所在页——XactGrouper 暴露 `pendingFloorLsn`（待决桶首记录
  lsn 最小值），检查点把它随 lsn 落盘（StateStore formatVersion 3 `dmlFloorLsn`）、
  槽推进按 floor 封顶、续传流起点取 min(stored, floor) 页对齐（catalog 过滤线仍 =
  stored，不二次施加字典）——重放段重建桶后终态补发；对拍验收
  `DualPathParityIT` 2PC 四形态（挂起零输出/弃桶/COMMIT PREPARED/挂起重启续传）。

## 配置面（`vb.wal.*` 十一键，`WalSource`/`StateConfig` 单一来源）

| 键 | 默认 | 语义 |
|---|---|---|
| `vb.wal.host` | `localhost` | PG 主机 |
| `vb.wal.port` | `5432` | PG 端口（非数字构造期 NumberFormatException） |
| `vb.wal.db` | `postgres` | 库名 |
| `vb.wal.user` | `postgres` | 用户（须有 REPLICATION 权限） |
| `vb.wal.pass` | `postgres` | 密码 |
| `vb.wal.slot` | `wal_source` | 物理复制槽名 |
| `vb.wal.state.dir` | 缺省 = 禁用 | 检查点目录（空串/空白同禁用；启用后含续传/周期落盘/槽推进整个生命周期面） |
| `vb.wal.state.interval.ms` | `30000` | 周期检查点间隔毫秒（≤0 构造期 IAE） |
| `vb.wal.state.events` | `1000` | catalog 行事件数阈值（与间隔先到为准；≤0 IAE） |
| `vb.wal.tables` | 缺省/空 = 全放行 | DML 输出面表白名单（逗号分隔 `schema.table`；relkind 'r'/'p' 持久用户表） |
| `vb.wal.dml` | `true` | DML 输出开关（false 回纯 v1 catalog 形态——ChangeStream 不装配；非 true/false 构造期 IAE） |

冒烟 Main 周期行（10s，logger `org.vastdata.vbstream.walsource.Main`）：
`smoke: lsn=… records=N resyncs=N reconnects=N censusTop3=[rmid/XX=count,…]`，state
启用时追加 `ckpt=LSN adv=LSN`（二者相等即"落盘后必推过"，落后即推进失败被 WARN
吞掉的形态），DML 面启用时追加 `dml=[buckets=N rows=M]`（ChangeStream 会话累计发射
计数——过滤后实付口径）。启动失败 ERROR + exit 1；接收器终态死亡（5 次重连失败/解析
ISE）周期行检测 → ERROR + exit 1；Ctrl-C hook 优雅 close（含最终 best-effort 检查点 +
槽推进）。DML 数据输出走 CDC 专用 logger `org.vastdata.vbstream.walsource.cdc`（INFO，
与 engine 的 `org.vastdata.vbstream.cdc` 分流惯例同构）。

## 测试面

`mvn test -pl vb-stream-wal-source` 单命令全跑（surefire 显式补 `**/*IT.java`）：
**245 用例 = 离线 206 + Testcontainers IT 39（需本机 Docker）**。

- **离线**（秒级）：`layout` 手造字节走读（WalRecordParserTest 12 / TupleDecoderTest 15
  / HeapViewsTest 12 / PageImagesTest 6 / WalLayoutV17Test 9 / WalLayoutV18Test 9——含
  "两版 toast 偏移差恰 4B"自检锚）；`receive` WalStreamWalkerTest 12（锚定协议含
  mid-stream 漂移免疫）；`replay` CatalogReplayTest 15 / CatalogSynchronizerTest 9（含
  引导前沿种子过滤线——种子 lsn 不吞引导窗口记录，终审 C1 离线锚）/ SelfHealerTest
  4；`state` StateStoreTest 9（往返/CRC 拒载/版本拒载/pgVersion 参数化错配拒载——
  17 写 17、18 写 18，终审 I1；formatVersion 3 dmlFloorLsn 往返）/ StateConfigTest
  5；`changes`（v2 DML 面：DiskValueRendererTest 18 / XactGrouperTest 20 /
  ToastAssemblerTest 12 / OutputRendererTest 7 / TableFilterTest 7 / PgFloatFormatTest
  7 / PglzTest 5 / ChangeStreamTest 7）；`api` WalSourceConfigTest 5；ModuleBuildTest 1。
- **IT**（postgres:18 `WalTestEnv`/`Interference`/`ParityEnv` + postgres:17
  `Wal17TestEnv` 矩阵，`WalSyncItBase` 对拍器 = 停流后 ctid 键控行字典 vs JDBC
  REPEATABLE READ 实查**逐行全等**）：`DualPathParityIT` 18（v2 双路对拍——engine
  逻辑解码路 in-process vs wal 直解路 DML 面同容器对拍，xid 交集逐行 diff 空：场景
  1 系基础 DML 六形态 + 类型矩阵边界值 + DDL-in-txn as-of + TOAST 三存储形态/重启
  unchanged + 2PC 四形态 + **普通 DML 中途停续（Task 12——at-least-once 重发双路一致，
  段一同页事务整桶幂等重发由重复块容忍面吸收）+ 流内 CREATE SCHEMA nsp 字典面
  （Task 4 延期清账）+ 干扰矩阵（Task 12——catalog 风暴线程[autovacuum toggle +
  ANALYZE 循环]下场景 1/TOAST 复跑 @RepeatedTest(2)）**）、`WalReceiveTest` 6（接收
  锚定 + 杀后端原地重连 + 首连失败）、`WalAdversarialIT` 3（autovacuum 拉满/ANALYZE
  风暴/随机 CHECKPOINT 干扰下 DDL 全场景对拍，@RepeatedTest(2) + RENAME 最小复现）、
  `WalLifecycleIT` 5（检查点续传/损坏回落全新引导/丢页末态追平 resyncs==0/门面全装配/
  引导窗口并发 DDL 不丢行）、`Wal17SyncIT` 7（17 对抗精简版 + 生命周期 + 跨版本分发
  拒绝与 V18 注入 fail-fast 零副作用）。
