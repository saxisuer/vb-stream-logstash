# vb-stream-wal-source — WAL 直解源（伪备库 catalog 同步 + WAL 接收 + 用户表 DML）

不创建备库、不占主库逻辑解码 CPU 的"伪装备库"路线：pgjdbc **物理复制流**接收原始 WAL
字节，纯 Java 按 REL_STABLE 源码头文件转录的版本布局描述符解析记录，自维护
pg_attribute/pg_class 的 **ctid 重放 as-of 字典**。v1（2026-10-05 spec）交付两件事——
**A. catalog 同步**（JDBC 一致性引导 + WAL 重放维护两张目录表快照，含 ctid、
relfilenode、toast 映射、dropped 占位语义）与 **B. WAL 接收**（物理流接收、解析、
位点管理、检查点持久化）；**v2（2026-10-06 spec）在其上完成用户表 DML 解码与事务
组装**（changes 包——XactGrouper 事务组装 / TOAST external 重组 / engine 格式复刻
输出），并以**双路对拍 IT**（engine 逻辑解码路 vs wal 直解路，六场景 + 干扰矩阵 +
CREATE SCHEMA）证明两路 CDC 输出逐字节等价；**openGauss 5.0.0 适配 M1（2026-10-11
spec，2026-10-12 落地）**在 v1 catalog 面上增第三布局档——判别（90204 撞号经
version() 文本消解）→ OG 布局描述符 → 双通道收流（sidecar 文件 tail / 裸 v3 直连）
→ catalog 引导/重放 OG 适配（见"openGauss 5.0.0 档"节；**M2（2026-10-12 spec，同日落地）
DML 面收官**——兼容模式护栏 + XACT/MULTI_INSERT/列压缩/分区四接缝 + v2 六场景双路对拍
全量验收，见"openGauss 5.0.0 档 M2"节；**M3（2026-10-13 spec，同日落地）A 兼容模式
开门 + 分区捕获**——白名单放宽 {'PG','A'} + pg_partition 第四 watched 字典（分区行
正常捕获、母表身份输出）+ 白名单双匹配 + StateStore v4 + A 档全量复验与分区专项
对拍，见"openGauss 5.0.0 档 M3"节；**VastBase G100 V3.0 适配（2026-10-13 spec，同日
T0-T7 收官）第四布局档 `WalLayoutVVastbase`**——OG 6.0.0 内核发行版，记录头与 OG 同构
(heap UPDATE/DELETE 前缀 21/19 分叉) + name128 词典几何 + relname 探测判别 + heap 前缀
21/19 与 oradate 重映射两接缝 + 手动档
真库 IT 三组（catalog 对拍/DML 对拍/生命周期），见"VastBase G100 档"节）。设计全文见
`docs/superpowers/specs/2026-10-05-wal-source-module-design.md`、
`docs/superpowers/specs/2026-10-06-wal-source-v2-design.md` 与
`docs/superpowers/specs/2026-10-11-wal-source-opengauss-m1-design.md` 与
`docs/superpowers/specs/2026-10-12-wal-source-opengauss-m2-design.md` 与
`docs/superpowers/specs/2026-10-13-wal-source-opengauss-m3-design.md` 与
`docs/superpowers/specs/2026-10-13-wal-source-vastbase-adaptation-design.md`（M2 DML 面已收官，
2026-10-12 同日落地——兼容模式护栏[PG 档 only，A/B 档启动期 ISE 附 M3/M4 指引] +
XACT/MULTI_INSERT/列压缩/分区四接缝 + v2 六场景双路对拍[基准路=OG 逻辑解码]全量验收，
见"openGauss 5.0.0 档 M2"节；M3 已收官 2026-10-13——A 档开门 + 分区捕获，见
"openGauss 5.0.0 档 M3"节；先导实验
`docs/wal-direct-decode-spike.md` + OG 调研
`docs/wal-source-opengauss-adaptation-research.md`）。

**依赖方向刻意收窄**：运行依赖 pgjdbc + slf4j-api + lz4-java（TOAST lz4 压缩值解压，
2026-10-07 lz4 支持落地引入）+ **opengauss-jdbc（runtime，openGauss 档 SQL 会话——sha256
认证兼容；包名 `org.opengauss.jdbc` 系与 vanilla pgjdbc 同 classpath 共存，DriverManager
按 URL 前缀分流，M1 Task 6）** + logback-classic（**runtime** 绑定 + `src/main/resources/`
logback.xml，2026-10-09 自 test-scope 提升——此前 main 面直接跑冒烟 Main 因 slf4j 无绑定
NOP 静默零日志；runtime 不进编译面，库形态宿主自带绑定/配置时以其 classpath 为准），
零 engine/Chronicle 依赖——将来是 engine（或连接器）依赖本模块，不是反过来（v2 裁定：两模块是**平级的
解析工具**，输出格式对齐靠双路对拍测试钉死、不靠代码共享；engine 仅以 test-scope
依赖出现在对拍 harness）。包结构 `org.vastdata.vbstream.walsource` 下
`layout`/`receive`/`replay`/`state`/`api`/`changes` 六包 + 顶层冒烟 `Main`。

## 组件（五组件 + api 门面，全部面向接口）

| 组件 | 位置 | 职责要点 |
|---|---|---|
| `WalLayout`/`WalLayouts` | `layout` | 版本化布局常量（`WalLayoutV17`/`WalLayoutV18`/`WalLayoutVOpenGauss`/`WalLayoutVVastbase`，逐常量标注 REL_STABLE/og-plan/真库实测出处）；`forServerVersion` 按 `server_version_num` 分发，未注册版本/显式错配布局启动期 ISE fail-fast；OG 档 90204 与 vanilla 9.2 撞号，经 `forServer(int, String)` 以 version() 文本判别；VastBase 档再撞号（无 branding），经 `forServer(int, String, int)` 三参版以 relname attlen 探测判别（见"VastBase G100 档"节） |
| `WalStreamReceiver` + `WalStreamWalker` + `PhysicalSlotManager` | `receive` | 物理复制流接收（readPending drain 轮询 + 断流指数退避重连 1s→16s、5 次失败停机）；walker 是字节流走读状态机（chunk 拼接/contrecord 缝合/**pageaddr 锚定协议**）；槽管理（`immediately_reserve=true` 建槽，restart_lsn 自建即保留——推进通道与保留窗口同时成立；OG 变体 `ensureSlotOg`：第二参 dummy_standby） |
| `WalByteSource` + `FileWalSource`/`ReceivexlogProcess`/`DirectWalSource`（+ direct 子包 `V3ProtocolChannel`/`OgAuth`） | `receive` | **OG 档双通道字节源（M1，详见"openGauss 5.0.0 档"节）**——推模式字节源接口（`start(LongConsumer, BiConsumer<Long,byte[]>)`）；file 通道=sidecar pg_receivexlog 落目录 + FileWalSource tail（managed 自管子进程 / external 只读既有目录）；direct 通道=裸 v3 协议客户端直连收流 |
| `WalRecordParser` + `WalRecord`/`BlockRef`/`HeapViews`/`PageImages`/`TupleDecoder`/`DecodeKinds` | `layout` | 记录头/block 头/heap 家族（INS/UPD/DEL/HOT/INPLACE/MULTI_INSERT/PRUNE）→ 强类型 record，纯函数；FPW 页镜像重建（hole 拼接 → ItemId 走读 → 页内 tuple 解码） |
| `CatalogSynchronizer` + `CatalogReplay`/`CatalogStores`/`CatalogBootstrap`/`CatalogRow`/`SelfHealer`/`JdbcProbe(Impl)`/`HeapEvent`/`Reconstruction` | `replay` | 通用 ctid 重放引擎（watched 表注册制、relfilenode/toast 跟踪、JDBC 一致性引导、竞速三修正）；单记录施加次序固定 PRUNE → INPLACE → attr 行事件 → class 行事件 |
| `StateStore` + `StateConfig` | `state` | 检查点持久化：单文件 `wal-source-state.bin`（magic 'VBWS' + header/ footer 双 CRC + lsn 双验；formatVersion 3 增 `dmlFloorLsn`——DML 待决桶重放下界，Task 11），全量序列化 `.part` → fsync → 原子 rename；损坏/版本不符（含 pgVersion 与 layout 错配——写与 load 校验同 `layout.majorVersion()`，17 写 17、18 写 18）拒载回落全新引导（安全侧：宁可重引导，不可错位窗口重放） |
| `WalSource`（api 门面）+ `CatalogSnapshot` | `api` | 一次 `start()` 装配全管线（SQL 会话 → 版本分发 → 同步器），`consumedLsn()`/`metrics()`/`lastCheckpointLsn()`/`lastSlotAdvanceLsn()`/`resumedFromState()` 观测面——v2 起接收 sink 单 pass 分发两消费者（catalog 先施加、DML 后解码，as-of 次序天然成立）；v2 DML 配置面 `vb.wal.tables`/`vb.wal.dml` 与发射计数观测同在此门面；**OG 档（M1）**判别命中 `instanceof WalLayoutVOpenGauss`（VastBase 子类同命中，vastbase T4）时转 `startOpenGauss`（`vb.wal.og.*` 五键——见"openGauss 5.0.0 档"节） |
| `ChangeStream` + `XactGrouper`/`TableFilter`/`ToastAssembler`/`ToastProbe(Impl→JdbcToastProbe)`/`Pglz`/`Lz4`/`DiskValueRenderer`/`PgFloatFormat`/`OutputRenderer`/`ChangeOutputListener` | `changes` | **v2 DML 面**（详见下节"v2 DML 面"）——TableFilter 白名单过滤 → XactGrouper 事务组装（提交时批量发射）→ TOAST 重组三形态（external 未压缩/pglz/lz4/行内压缩）+ 窗口前指针回查兜底 → 磁盘格式值渲染 PG text → OutputRenderer 复刻 engine `ConsoleRenderer` 事务块格式输出 CDC logger |

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
| `ToastAssembler` | TOAST 重组：toast 关系 chunk 行采集（valueid→seq→bytes TreeMap）+ external 18B 指针剥解（rawsize/extinfo/valueid/toastrelid）+ pglz/lz4 双方法解压（`inflate` 分派面）+ 窗口前指针 JDBC 回查兜底 + unchanged-TOAST 哨兵 + **INSERT 新元组空面回查视图**（`insertTupleResolver`——重放窗口切断 chunk 的补全通道，2026-10-13 flake 根因修复）；实现 `TupleDecoder.VarlenaResolver` 接缝（external 指针 + 行内压缩 varlena 同一解压面） |
| `Pglz` | PG pglz 解压纯移植（`pg_lzcompress.c` 主循环逐行转录，control byte 分组 + match 回拷，L705-790 源码锚） |
| `Lz4` | lz4 裸 block 解压（lz4-java safeDecompressor 定长目标形态；PG `toast_decompress_datum`→`LZ4_decompress_safe` 同格式，JNI 优先纯 Java回落；2026-10-07 随 lz4 值面支持引入） |
| `DiskValueRenderer` | 磁盘格式值 → PG text（17 类型首发矩阵：bool/int2/int4/int8/float4/float8/numeric/text/varchar/bpchar/json/bytea/date/time/timetz/timestamp/timestamptz/uuid；矩阵外 `0x` + WARN 一次、dropped 列 ∅） |
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

**场景支持矩阵**（v2 终局盘点——支持 = 双路对拍验收过；不支持 = 触发时的处理方式）：

| 场景 | 支持状态 | 不支持时的处理方式 |
|---|---|---|
| **INSERT / UPDATE / DELETE**（wal_level=logical + REPLICA IDENTITY FULL） | ✅ 双路对拍逐字节等价 | — |
| **事务组装**：普通/交错/SAVEPOINT 子事务回滚剔除/整体回滚零输出 | ✅ 场景 1 | — |
| **两阶段提交**：PREPARE 挂起/COMMIT PREPARED 发射/ROLLBACK PREPARED 弃/挂起重启续传 | ✅ 场景 5（kind=TWO_PHASE + gid 双路一致） | — |
| **TOAST**：external 重组/pglz 与 lz4 解压/行内压缩/UPDATE 未变列 `<toast-unchanged>`/重启后窗口前指针 probe 回查 | ✅ 场景 3（三存储形态 + 重启回查）+ lz4 专项场景（`SET COMPRESSION lz4` 双形态，2026-10-07） | — |
| **类型矩阵**：bool/int2/4/8/float4/8/numeric/text/varchar/bpchar/json/bytea/date/time/timetz/timestamp(tz)/uuid | ✅ 场景 2（17 类型 + 边界值 + 浮点 PG 风格格式化） | — |
| **DDL-in-txn**：事务内 ADD COLUMN 前后段 as-of 渲染 | ✅ 场景 4 | — |
| **RENAME/TRUNCATE**：catalog 行跟踪（v1 面） | ✅ v1 对拍 | — |
| **CREATE SCHEMA**：流内 nsp 字典构建 | ✅ 专项场景 | — |
| **生命周期**：检查点续传/损坏回落/丢页末态追平/at-least-once 重发 | ✅ 场景 6 + v1 生命周期 | — |
| **PG 17 + PG 18 双版本** | ✅ 全矩阵 | — |
| **表白名单**（vb.wal.tables）/ relkind 过滤 | ✅ 离线 | — |
| **UPDATE 未变列前像**（REPLICA IDENTITY FULL） | ✅ 前像恒完整（服务端 toast_flatten_tuple） | — |
| **replica 形态 UPDATE**（wal_level=replica） | ❌ v3 或非目标 | liveness guard 行级跳过 + WARN/表 + skipped 计数——**系统性缺行**（README 限定 DML 需 logical） |
| **截断 UPDATE 非 FULL 身份**（wal_level=logical + DEFAULT/KI 身份） | ⚠️ 理论不可达 | 互斥门保证 logical 流下恒 CONTAINS_OLD（heapam.c 实源）——liveness guard 防御面，双计数恒 0 哨兵 |
| **lz4 压缩值**（external/行内） | ✅ 2026-10-07 落地（lz4-java） | —（未知方法码仍 ISE；解压损坏 ISE 同 pglz 契约） |
| **矩阵外类型**（enum/域/jsonb/组合等） | ❌ 部分支持 | `0x` 十六进制降级 + WARN 一次/oid——输出可读性降但流不断；双路对拍在该集外会 diff |
| **dropped 列**（DROP COLUMN 后新 INSERT） | ⚠️ 两路分叉 | wal 渲 `∅` 占位 vs engine pgoutput 'R' 不发 dropped 列——对拍矩阵刻意避开含 dropped 列场景 |
| **压缩 FPW**（wal_compression 非 off） | ❌ v1 前提外 | ISE fail-fast——运维前提 `wal_compression=off` |
| **VACUUM FULL / CLUSTER** | ❌ v1 非目标 | 字典面跟踪新 relfilenode 但 DML 语义未承诺（整表重发形态） |
| **STREAMED kind**（logical_decoding_work_mem 驱逐形态） | ⚠️ 差异面 | wal 侧恒 NORMAL、engine 侧 kind=STREAMED+行尾缀——parity 环境禁驱逐规避；真流式对齐 v3 |
| **NSP redirect 缺行**（schema DDL 竞态窄面） | ⚠️ v2 限制 | 静默跳过（DEBUG + nspRedirectMisses 计数）→ 该 schema 表从输出消失——fail-safe 方向（漏数据非错数据）、重启全新引导自愈；v3 加精确采纳通道 |
| **跨停机点 DDL + DDL 前行落重发页**（四重叠加窄缝） | ⚠️ v3 方向 | 重发块渲染用字典末态非行时刻态——对拍判"不等"（假红非静默错值）；v3 按记录 lsn 重建 as-of |
| **交错事务 B 清掉 A 的 toast 窗口 / 重放窗口切断 chunk** | ⚠️ UPDATE 面 v3 方向（**INSERT 面已修**，2026-10-13：空面 probe 回查，`insertTupleResolver` 视图） | UPDATE 行多见 `<toast-unchanged>`（engine 发真值）——fail-safe 方向已知分叉；v3 per-txn 窗口 |
| **长挂起 2PC + 频繁重启** | ⚠️ 无护栏 | 挂起桶整段重放（floor 不前进）——功能正确但重放成本线性增长；桶快照持久化 v3 |

**已知限制与 v3 方向**：

- **replica 形态 UPDATE 系统性缺行**：`wal_level=replica`（无逻辑日志）下用户表 UPDATE
  走前缀/后缀省略且 DEFAULT 身份无旧元组——重建不可达，liveness guard 有痕跳过。DML 面
  的实际前提是 **`wal_level=logical`**（README 已限定）；跨记录行态重建（省略段 + 前后
  记录关联）是 v3 或非目标声明，需用户裁定。
- **STREAMED kind 分叉**：engine 逻辑解码流式驱逐形态输出 `kind=STREAMED` + 行尾
  `[streamed xid=N]`，wal 侧恒 NORMAL（WAL 无驱逐概念）——parity 环境禁驱逐规避；真
  流式形态对齐需 wal 侧模拟驱逐语义，v3 议。
- **矩阵外类型**：首发 17 类型集外的列（enum/域/jsonb/组合类型等）`0x` 降级 + WARN
  ——双路对拍在该集外会 diff，文档化差异面。
- **dropped 列对拍分叉**：wal 侧渲 `∅` 占位（磁盘布局 dropped 列仍在元组内占位），
  engine 侧 pgoutput 'R' 关系消息不发 dropped 列——两路列集分叉，对拍矩阵刻意避开
  含 dropped 列的场景（含 DROP COLUMN 后新 INSERT 的形态）。
- **lz4 值面已支持**（2026-10-07）：`SET COMPRESSION lz4` 列（PG 14+）的 external/
  行内压缩值经 lz4-java 裸 block 解压（`Lz4` 面），双路对拍专项场景验收；原
  attcompression=='l' 启动 WARN + ISE 拒绝面已删（lz4Guard 从未接线且语义已废）。
- **大事务输出缓冲 O(事务)**：pending 桶攒行文本至提交，与 engine block 模式同级；
  落盘化（格式层/file）是 v3 升级路径。
- **清窗全局 vs per-txn 窄缝**（v3 合并条目；**INSERT 面已修**，2026-10-13 对拍 flake
  根因修复）：空面成因两类——①全局清窗使交错事务 B 的行夹在 A 的 chunk 写入与引用行
  之间时 A 的窗口被清（服务端按事务分 hash 不受影响）②**重启续传重放窗口切断**——
  续传流起点页对齐下取整，页边界落在最后 chunk 记录与 heap 行之间时 chunk 不重发
  （实测复现 DualPathParityIT TOAST 重启续传场景：重发 INSERT 渲 `<toast-unchanged>`
  vs engine 首交付真值 → 重发块不等）。**修复 = INSERT 新元组专用解析视图**
  （`ToastAssembler.insertTupleResolver` + `XactGrouper.insertDecoder`）：INSERT 的
  external 值必为本事务新写（valueid 新分配无跨事务复用），空面即重放切断或清窗、
  engine 对照面持真值，probe 回查补全（值已提交可见）——两类成因一次关死；UPDATE
  后像维持空面哨兵（'u' 对齐——未变列不可探测判别），残余窄缝 = UPDATE **改写**宽列
  且恰逢重放切断（无本地判别式，对拍矩阵不构造）。同类窄缝：①目录三表行不经
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

## openGauss 5.0.0 档（M1 catalog 面，2026-10-11 spec / 2026-10-12 落地）

**范围**：v1 catalog 同步面（引导 + 重放 + 检查点/槽推进）+ WAL 接收双通道；DML 面已随
M2 落地（见下节）——`ogCatalogOnly` WARN 降级位退役（M2 T1），listener 在场且
dml 开着时 postApply/pendingFloor 经 `startOg` 接缝档与 PG 17/18 路同构注入（惰性
ChangeStream）+ 库级兼容模式护栏（详述见下节"兼容模式限制"）。存储引擎
只做 ASTORE；版本只做 5.0.0（90204 撞号经
`forServer(int, String)` 的 version() 文本判别）。调研全文见
`docs/wal-source-opengauss-adaptation-research.md`（文首有 M1/M2 落地状态与勘误回填）。

**判别与布局差异要点**（`WalLayoutVOpenGauss`，逐常量标注 og-plan 容器实测锚）：
页魔数 0xD074 / 短页头 24B 含私有 `xlp_total_len@20` / **记录头 32B**（term u32@4
前置——单机实测恒 0x1，xid@8/info@24/rmid@25/bucket_id@26）/ 块引用消费序 =
定位器 14B（RelFileNodeOld 12B + extra_flag u16）+ blkno u32 + last_lsn u64、镜像头
仅 4B 洞字段（无压缩面）/ heap main 老结构 + LogCSN（sizeOfHeap **13/11/2**）/ catalog
元组 oid 在 9.2 式元组头（[t_hoff-4,t_hoff)，`TupleDecoder` 虚拟 "oidhdr" 槽注入）/ 
**pg_class 词典 40 列**（非 41——oid 不占数据列）、relfilenode@**84**/reltoastrelid@**116**
（relpages/reltuples float8 加宽净 8B）/relkind@**143**、全槽位较 vanilla 前移一位 /
pg_attribute 词典 24 项（attstattarget 回 attnum 4、attnum 槽 5）/ PRUNE 是 9.2 系
XLOG_HEAP2_CLEAN 单 opcode 与 vanilla 撞号——经 `legacyHeap2Clean()` 门控分派 /
块 id 高位扩展位（0x80 bucket/段页式、0x40 TDE）启动期 ISE fail-fast / walker 容忍
整零页（`tolerateAllZeroPages()`——pg_receivexlog 落盘段对跳过页/段尾零区零填充补位）。

**收流双通道（`vb.wal.og.source`）**：
- **file（缺省）**：sidecar `pg_receivexlog`（`ReceivexlogProcess`——managed 自拉起
  子进程[stop×拉起竞态锁内互斥、监督线程守护化] / external 只读既有目录）落盘
  `rx.dir` + `FileWalSource` 目录 tail。**坑**：pg_receivexlog 预分配 16MB 全零
  `.partial`（文件长度 ≠ 接收进度）——交付上限 = 最后非零字节 +1 前向扫描（旧逻辑
  静默丢 ~760 条尾记录，对拍实证）。
- **direct**：`DirectWalSource` 裸 v3 协议客户端（`V3ProtocolChannel`/`OgAuth`）——
  startup packet **`replication=true`** 形态（非 pgjdbc 的 replication=database；两驱动
  的复制 API 只会 database 形态故不可用）→ 认证（OG sha256 挑战[码 11，应答含 iteration
  尾缀] + md5 + 码 10-3.0 签名三面）→ IDENTIFY_SYSTEM → START_REPLICATION（可带 SLOT）→
  'w'/'k' 解析。**OG 私有帧形**：'k' keepalive 恰 **33B**（walprotocol.h，真流闭环锚）；
  反馈 'r' standby status 恒 **65B**（多 peer_role u32=3 / peer_state u32=7 两 OG 尾缀）；
  块锚 = dataStart + 载荷长（非 walEnd）。断线重连面：CopyDone / 畸形 'k' 帧归可重试
  IOException。心跳线程周期 = statusInterval 之半。
- **部署硬前提**：openGauss pg_hba 的 `all` 不匹配 replication 伪库——replication 用户需
  显式 `host replication <user> 0.0.0.0/0 md5` 行（测试环境幂等追加 + reload 兜底）。

**catalog 引导/重放 OG 适配**（Task 7）：LSN 函数无 flush 档（`pg_current_xlog_location()`
write 位点，B 取值面同 C1 语义）；种子 SQL 的 OG 列单投影（41 列 SQL 面 vs 40 列 WAL 面
口径差已 javadoc）；**槽推进 cast 门控**——OG 无 `pg_lsn` 类型（`?::pg_lsn` 每检查点一条
ERROR、槽恒不推），`CatalogSynchronizer.advanceLsnCast` 缺省 `"::pg_lsn"` 保 PG 档字节
等价、`startOg` 装配期覆写空串（text 直传即通）；**存储引擎护栏**（种子面 + 流内
INS/UPD 双面，识别即 ISE 含 relname/oid）：列存（relcudescrelid/reldeltarelid≠0）、
分区（parttype≠'n'——M2 修复 B 起 **parttype='p' 分区母表行放宽**入字典，见 M2 节）、
段页式/bucket（relbucket≠null≠0）、UStore（reloptions 含
storage_type=ustore/segment=on）——护栏读区越界零填充段按不可判跳过（防假阳性）；
系统列 attnum 诊断下界经 layout 取（vanilla -6 / OG -8——tableoid/xc_node_id 是 -7/-8，
2026-10-12 实修防误报"词典错位"WARN）。

**验收**（OG IT 5 类，`opengauss/opengauss:5.0.0` Testcontainers `OgTestEnv`）：
`OgCatalogSyncIT`（65 轮干扰 + 宽 DDL 对拍，断言面 vanilla 同源逐行全等）、`OgLifecycleIT`
三用例（检查点续传/损坏回落/门面全装配 og.source=direct 两轮启停）、`OgInstanceSurvivalIT`
（存活红线 5/5 双 kill 向量——服务端 walsender 终止 + 客户端裸 socket 关闭，每轮 SELECT 1 +
日志无 gaussdb exit）、`OgGoldenWireIT`（黄金 wire 对拍——'k'=33/'r'=65 私有帧形）、
`OgDualChannelParityIT`（双通道对拍——file vs direct 同窗口记录序列全等
  [lsn/totLen/rmid/info]，窗口按两路覆盖较小者归一）。

**已知限制（OG 档增量，详版记档见各任务报告；~~MULTI_INSERT 解析即 ISE~~ 已勾销——M2 T3
转录真布局解除，见下节；~~`ogCatalogOnly` WARN 降级~~ 已勾销——M2 T1 退役，DML 面 OG 档
同构装配）**：
- **MultiXactId 变体未锚**：xmax 为 MultiXactId 时 UPDATE/DELETE 走全量 SizeOfHeap
  （u64 xmax 布局，前缀漂移）——常量锚常态逻辑日志形态，变体到达即偏移错位 ISE。
- **挂 replconninfo 的 HA 形态不在支持面**（"build walsender 自杀"雷区）——仅单机/
  无 replconninfo 实例验证。
- **write 位点起流语义**：OG 无 flush 位点函数，对拍目标前沿取 write 位点——write ≥
  flush 使引导窗口 S ≥ B(write) 更窄（物理流自 flushed 起发，重发窗口两检查点 IT 闭环
  无丢失）；槽推进对 write 档 restart_lsn 的观察面已验（推进成功才断言落位）。
- **`advanceLsnCast` 直写字段**（唯一写点 startOg）：未来新增非 startOg 的 OG 装配路径
  会静默回落失败 cast——新装配路径须同步覆写（Task 9 记档）。

## openGauss 5.0.0 档 M2（DML 面，2026-10-12 spec / 2026-10-12 落地）

**范围**：v2 DML 面在 OG 档全量开门——`vb.wal.tables`/`vb.wal.dml` 两键直接生效（配置零
新增）；v2 六场景（基础 DML 六形态/17 类型边界值/TOAST 三形态+重启 unchanged/DDL-in-txn
as-of/2PC 四形态/中途停续）经双路对拍在 PG 兼容库 m2pg 全量验收，v2 changes 包状态机/
TOAST/输出渲染结构零改动（OG 差异经四个接缝吸收，沿 M1 档位门控纪律）。

**兼容模式限制**（M2 时点口径——A 档限制已随 M3 放行退役，B/'C' 维持 ISE）：DML 面
支持 PG 兼容模式库（`DBCOMPATIBILITY 'PG'`；'C' 为 PG 兼容历史
别名——5.0.0 仍接受建库且存储值与 'PG' 相异，未在 M2 验证面）；库级护栏探测
`pg_database.datcompatibility`（og-plan 实测：postgres 缺省库='A' 即 Oracle 风格、'PG'
库='PG'），可判定的白名单外档位（M2 时为 A/B/C，**M3 起 'A' 放行、余 B/'C'**）启动期
ISE 附档位与 M4 指引、不可判定（无行/空值/SQLException）WARN 放行；**护栏只在 DML
请求在场时探测**——catalog 同步面与兼容
模式无关（M1 全部 OG IT 即跑在 'A' 档缺省库上全绿），显式 `-Dvb.wal.dml=false` 等价声明
catalog-only 意图不 ISE。

**对拍形态（PARITY=LOGICAL，T0 spike 裁定——`docs/wal-source-opengauss-m2-spike.md`）**：
基准路 = opengauss-jdbc `logical()` in-process 逻辑流（proto_version=1，publication 须先于
被解码事务存在）+ 归一化器（`it/OgParityBaseline.Normalizer`，spike §2 布局逐字段独立
新写——**不共享 engine PgOutputDecoder**：OG pgoutput 两处帧级差异 B 帧 xid Int64 /
TupleData Int16 ncols 前缀；显式拒绝 'T'/两阶段族帧）；wal 路 = `WalSource` DML 面
direct 通道；按 xid 交集逐行 diff 空（`DualPathParityIT` 形态复刻：expectedChanges 语义
归一化 + 重复块全等容忍 + 交集外窗口差 INFO 留痕）。**认证死角（T6 实测锚，driver trace +
服务端日志）**：gaussdb 走逻辑复制流撞**码 11**（AUTH_REQ_MD5_SHA256 混合挑战——
opengauss-jdbc 的码 11 应答缺 ITERATION_STRING 尾缀且用 2048 旧迭代数，真 5.0.0 服务端
恒拒），且 OG 复制连接在 pg_hba **只匹配 `replication` 关键字行**（库 `all`/库名行均不救）；
解法 = 专用角色 `m2par_base`（口令 sha256 存储）+ pg_hba 首插
`host replication m2par_base all sha256`（码 10 RFC5802 驱动实现正确；角色限定——gaussdb
不经此行，M1 DirectWalSource 的码 11 验证面不变）。spike 路②在 og-plan"可行"系容器口令
已 md5 化所致，与真部署面有出入（spike 文档已回填）。

**OG 差异接缝（T2-T7 落地）**：

- **XACT**（T2，`OgXactView`[layout] + XactGrouper 接缝）：四记录族 + 五 opcode 转录；
  **OG 流无 252 TOPLEVEL_XID 标记**（源码钉死）——子事务归并改**终态 subxacts 经
  mergeSubBuckets 补偿**（输出时序等价实证）；映射缺位子回滚多发一条 BatchAborted
  （onAborted 零输出，对拍无感）；COMMIT_COMPACT(0x60) 按源码推导布局解析兜底；
  RM_XACT_ID=1 同 vanilla；2PC 完整可用（max_prepared_transactions=200）。
- **MULTI_INSERT**（T3，`HeapViews.MultiInsertView.parseOg` OG 分支——M1 ISE 解除）：main =
  [INIT_PAGE 前置 pd_xid_base u64]flags u8 + isCompressed bool@1 + ntuples u16@2 +
  offsets（INIT_PAGE 省略）+ LogCSN 殿后（九条真字节语料收口；任务链候选锚"info 0x30"系
  误读——实测 0xD0[=0x50|INIT_PAGE]×15 + 0x50×2）；entry 几何 OG==vanilla（2 对齐 +
  SHORTALIGN(7+datalen)，datalen 只计 tail）；isCompressed≠0（页字典压缩表）ISE 属 M2
  范围外 fail-fast。**修复 A（T7，v2 存量真 bug）**：XactGrouper entry walk 原按
  `off += 2+datalen` 步进与真几何差 5B——第二 entry 起错位，手造测试按 5+tail 自洽编码
  掩盖；修为真几何 + 手造 DSL 编码口径连动 + COPY 100 行对拍场景闭环。
- **列压缩**（T4，`OgColumnCompression`[changes]）：DELTA/DICT/PREFIX/NUMSTR 四算法 +
  meta 走读纯函数（pagecompress.cpp 转录）**零接线**——**attcmprmode 列压缩在 OG 5.0.0
  行存面 DDL 天然不可达**（三路 DDL 语法全拒 + tablecmds.cpp 双护栏 + 全库目录扫三源；
  普通表 attcmprmode∈{0,127}），压缩形态实为元组级 HEAP_COMPRESSED 0x0010 + 页字典
  （非 varlena tag——spec 接缝预设推翻）；M3 警示：A 档列存走 compress_kits CU 族
  不同源勿复用本类。
- **分区**（T5，TableFilter OG 档判别闸 + 修复 B）：路由 = heap INSERT 落**分区自身
  relfilenode**（pg_partition 'p' 行，range/list 四锚 32809/32810/32821/32822，母表零
  命中）；**分区子表不在 pg_class**（无 pg_class/pg_attribute 行，列挂母表 oid；母表
  relkind='r' + parttype 值域恰 {'n','p'}）——"子表独立匹配白名单"前提被推翻。落地 =
  三参构造器 openGaussDialect 门控（relkind 'r' 收窄 + parttype 闸/未知 miss/不可判
  fail-open，V17/V18 行为零变化）；**修复 B（T7，R1 裁定）**：M1 存储护栏对
  parttype='p' 的 ISE 放宽（种子/流内双面——母表行入字典无害）。**M2 时点的"分区面 =
  不砖 + 有痕跳过而非捕获（R1 裁定）"已随 M3 退役**——M3 pg_partition 第四 watched
  字典落地后分区 DML 行正常捕获（母表身份输出，见"openGauss 5.0.0 档 M3"节）：分区
  relfilenode 不在 v1 三表字典的 miss 面由第四字典身份链接管；M2 终审 I1 的引导 seed
  节流 WARN 已在 M3 T2 删除（捕获语义下跳过指引是过时噪声），逐 relfilenode 字典 miss
  的 DEBUG（`XactGrouper.resolveCached`，缓存后不重复）与 Main 周期行 `dml=` 发射计数
  保留（miss 是噪声表常态）；有痕跳过仅余链断/DROP 后迟到行两 fail-safe 形态。
- **映射目录段号直认**（T7 修复 C，`CatalogSynchronizer.reOidOf`）：OG 的 pg_attribute/
  pg_class 是 relation-mapped——pg_class 行 relfilenode **列值为 0**，物理段号只经
  `pg_relation_filenode()` 可得，线性反查对两目录物理段号恒 miss（in-txn DDL 后行按陈旧
  词典解码 ISE 的根因）；修复 = 反查前与引导期回填的两 watched 段号比对，命中直认固定
  oid 1249/1259。vanilla 侧两目录同为映射目录（常态初映射段号==oid，直认与 oid 直认
  同值同果；目录重写后只增命中不改既有命中——顺带修 vanilla 潜在盲区）。直认段号是引导
  期回填值，VACUUM FULL/CLUSTER 重写后与 M1 块匹配面同样陈旧（v1 已声明非目标）。

**对拍基准路已知限制（服务端能力差，非接缝错位）**：

- **OG 逻辑解码器对事务内 DDL 的后续行静默丢弃**（og-plan 投探实锤——帧序恰
  `B..R(旧列)..I(首行)..C`，DDL 后新事务以新列 R 帧正常解码）：DDL-in-txn 场景重塑为
  双侧精确断言（wal 路三行逐字面——前段两列/后段三列/UPDATE 前像新列渲 NULL；基准路
  DDL 事务块恰一行钉丢弃面；DDL 后控制事务 full parity），断言强度不降。
- **2PC 基准路形态（proto v1）**：无 two_phase 槽选项，PREPARE→COMMIT PREPARED 以普通
  B..C 块出现（解码推迟到 COMMIT PREPARED 时点——prepared xid 块可出现在更晚普通事务
  块之后；ROLLBACK PREPARED 零帧）——归一化块模型不含 kind/gid，两阶段语义断言走
  `WalCapture.begins()` 事件面（twoPhase+gid 精确断言）。
- **值面分叉两处（OG 与 vanilla 的渲染面差异，双路一致由 parity 承载——场景内已避开；
  M3 A 档复验证 PG/A 双档 17 类型逐字面全等，date 面经底层 timestamp 化天然一致，见
  M3 节）**：
  ①float 文本输出 (0,1) 区间前导零省略 + float4 缺省 6 位有效数字；②UPDATE 未变
  external TOAST 列 OG 恒发完整重写值（heap_update 重写 toast 值），非 vanilla 的 'u'
  占位。
- **DEFAULT 身份前像分叉**：OG 的 'K' 段是 ncols=全列宽 + 'n' 占位形态（vanilla 'K' 只发
  键列）——基准路缺席占位不入像 vs wal 路尾列补 null 渲 NULL，对拍场景用 FULL 身份规避。

**验收**（OG IT 增至 26 用例，`OgTestEnv` 扩展三兼容档库 m2pg/m2a/m2b + 基准路角色）：
`OgParitySmokeIT` 1（T6 冒烟——单事务 I→U→D 三行双路 diff 空 + 字面锚）、`OgDmlParityIT`
13（v2 六场景 OG 档全量：1a-1f 基础六形态含 COPY 100 行 MULTI_INSERT 真几何/2 类型矩阵
边界值/3a+3b TOAST 三形态与重启 unchanged/4 DDL-in-txn 双侧精确断言/5a+5b 2PC 挂起弃桶
COMMIT PREPARED 与挂起重启续传/6 中途停续 state.dir 两轮）、`OgDmlOgSpecificIT` 3
（attcmprmode 护栏——压缩 DDL 服务端拒绝判别串 + attcmprmode∈{0,127} 库侧钉 + 对拍
不受扰；分区 fail-safe 母表白名单内/外两形态——修复 B 端到端锚，分区行零泄漏）、
`OgCompatGuardIT` 2（m2a/m2b 档 ISE 三断言[档位值/M3-M4 指引/PG 建库指引] + 槽副作用
缺席——ISE 先于建槽）；M1 五组 IT 零改动全绿。OgDmlOgSpecificIT 以 `new OgDmlParityIT()`
实例复用包内共享骨架（无 JUnit 生命周期介入——后者若引入 @BeforeEach 依赖会静默漂移，
记档备查）。

## openGauss 5.0.0 档 M3（A 兼容模式 + 分区捕获，2026-10-13 spec / 同日落地）

**范围**：兼容模式护栏白名单放宽为 {'PG','A'}（B/'C' 维持启动期 ISE 附 M4 指引；
ASTORE 限定不变）+ **pg_partition 第四 watched 字典**（与 attr/class/nsp 同权：
bootstrap 第四表种子 + WAL ctid 重放 + stores 第四容器 + **StateStore formatVersion 4**
[v3 拒载回落全新引导，既有语义] + 身份链进 CatalogSnapshot）——分区 DML 行从 M2 R1
裁定的"不砖 + 有痕跳过"升级**正常捕获**（PG 档分区捕获顺带受益——T5 实测两档分区为
同一套原生机制，vanilla `PARTITION OF` 语法在 OG 拒）。spike 锚定全文
`docs/wal-source-opengauss-m3-spike.md`。

**第四字典要点**（spike 锚①/锚④ + T1 kinds 推导）：pg_partition **32 列单表自带
relfilenode@数据区+84 / reltoastrelid@116**（oid 经 9.2 式元组头——oidhdr 虚拟槽机制
沿用，t_hoff=32；**无 pg_partition_fn 等伴生结构**，watched 面单表）；词典 =
`CatalogRow.PartitionRow.KINDS` 33 项（oidhdr + 32 列，值编码读区 120B 定宽段）；
行身份由 parttype@64 区分（'r'=母表行 relfilenode 恒 0 不入路由面 / 'p'=分区行）；
分区 DDL 五形态（CREATE/ADD/DROP/MERGE/SPLIT/EXCHANGE——**含 MERGE/SPLIT**，OG 5.0.0
语法全支持）全部**常规 heap 记录族**（INS/HOT_UPD/非HOT UPD/DELETE/INPLACE），零特殊
段——ctid 重放/末态采纳/截断重建全复用 v1 机制；自身 relfilenode=14318≠oid 9016，
watched 注册按物理段号（`relOidOf` 第三直认，沿 M2 修复 C 映射目录段号先例）。

**分区身份链与白名单双匹配**（T2/T5）：

- **PARTITION_NAME=mother（spike 锚②，M3 spec §0/§3 预设被实测反转）**：OG pgoutput
  逻辑流对分区 DML 的 R/I 帧报**母表身份**（relId=母表 oid、母表列集——分区名全程零
  出现，A 档与 PG 档采样同形）——TableMeta 身份=母表（`schema.母表名` + 母表 pg_attribute
  列序），wal 路与基准路天然逐字节同形，对拍归一化器免映射分派。
- 解析链：heap 记录（分区 relfilenode）→ 三表 miss → `CatalogSnapshot.partitionOf`
  （第四字典 'p' 行）→ parentid → 母表身份/列序。**resolve 次序 = OG 档第四字典属主
  优先且不回退**（T5 修复 `9d98f38`：EXCHANGE 换绑段号的 oid 碰撞——OG 建表
  relfilenode==oid、换绑双向对调段号使分区 'p' 行新段号恰等于交换对方普通表的 oid，
  三表反查 miss 后 oid 直认兜底误命中普通表；修为 partitionOf 命中即走母表身份链，
  链断 fail-safe 跳过同样不回退——"链断不回退三表"语义收紧）。
- **白名单双匹配（与 vanilla 语义的有意分叉——记档）**：**母表名匹配 = 基准形态**
  （写 `public.t_range` 覆盖其全部分区——分区滚动运维的正解写法）；分区名匹配 =
  兼容增强（写 `public.p1` 只命中 p1 路由的行，不外溢 p2）；实现为**运行期双查**
  （先母表全名、未中再分区名）——分区集是活的（流内 DDL 增删），静态预展开在首个
  分区 DDL 后即过期。vanilla 档（V17/V18）无视分区链零变化。
- **ToastAssembler 零改动**（T0 锚③ 预想的反查面经代码核实不存在）：分区自有 toast
  关系 `pg_toast_part_<分区relfilenode>`（挂 pg_partition 'p' 行 reltoastrelid@116，
  母表恒 0）的 pg_class 't' 行在不限 relkind 的种子面内，chunk 键 = toast relfilenode
  本身 + external 指针自携 va_toastrelid——既有机制天然覆盖，离线 + IT 双钉。
- 链断两形态 fail-safe 有痕跳过（沿 nsp redirect 先例）：第四字典命中 'p' 行但
  parentid 悬空（引导竞速理论窄缝）/ DROP PARTITION 后迟到行——**WARN 恰一次** +
  `partitionChainMisses` 计数（递增不重复告警）+ Main 冒烟行 `partMiss=` 透出，行照常
  跳过不砖流；TableMeta 缓存失效钩子扩第四字典（pg_partition heap 记录到达即全清，
  EXCHANGE 换绑后旧身份缓存即时作废）。

**A 档（Oracle 风格兼容库）能力**（T3/T5）：

- 护栏白名单 {'PG','A'}（`isDmlSupportedCompatMode`）；A 档 v2 全场景复验 = `OgDmlParityIT`
  13 场景参数化 PG/A 双库同套（场景体收成单方法双档委派，**场景代码零复制**、PG 档
  方法名与断言逐字节不变；槽名 A 档附 `_a` 后缀防跨库同名互踩）；A 档基准路
  （opengauss-jdbc 逻辑流收帧 + 归一化）端到端可用——认证/pgoutput 帧面/逻辑槽建删
  均无 A 档分叉。
- **date 探针终态（T5 fix round 1）**：A 档 DDL 关键字 `date` 映射 **atttypid=1114
  （底层 timestamp，8B 存时段）**——Oracle DATE 整秒粒度（微秒输入服务端截断）；wal
  路按列 typeOid（pg_attribute as-of 字典供数）走 timestamp 渲染、基准路 timestamp_out
  同形——**时段零真值损失、双路天然逐字节一致、无需任何归一化**（T5 首版"date 输出
  方言后缀归一化"前提被证伪——`pg_catalog.date` 直引[真 1082，A 档库仍可建]实测渲
  PG 同形 `2026-10-07`，首版所见"零时段后缀"全部来自 timestamp_out——该归一化分支
  是死代码已整体移除；直引 1082 同形结论无 committed 锚，探针记档）。档位语义分叉在
  **输入侧**（A 档存时段 vs PG 档带时段输入静默截断为日）——服务端行为，wal 源忠实
  转储无决策面；A 专形 IT 四面字面钉（时段完整存取/纯日期 00:00:00/微秒截断/UPDATE
  双像含时段）。
- A 档 `''` 空串入库即 NULL（服务端语义，两路同见，对拍天然吸收）。
- A 档列存表（`ORIENTATION=COLUMN`）引导种子面存储护栏 ISE（ASTORE 限定，真容器
  用例三断言）；次序差记档：兼容档护栏先于建槽（零槽副作用），存储护栏在引导期
  （槽已建——IT finally 兜底清槽 + 删列存表，残留会砖 A 档对拍 IT 的引导）。

**验收**（OG IT 增至 58 用例，`OgTestEnv` m2a 升格 A 档全量场景库 + `CompatMode`
参数化三入口）：`OgPartitionIT` 16（分区专项 PG+A 双档：range/list 捕获对拍——两前提
**显式 FOR TABLE 母表**[OG 的 FOR ALL TABLES publication 不含分区表] + 母表
REPLICA IDENTITY FULL 传播分区行；白名单四形态[母表名基准/分区名不外溢/双命中并集/
都不中——每形态独立全量 reset，防共表连续跑的跨用例重放尾窗污染桶计数]；流内
ADD·DROP PARTITION 竞速 as-of；EXCHANGE 后 TableMeta 失效双向身份钉；UPDATE 跨分区
迁移 DELETE+INSERT 两行形态）、`OgDmlParityIT` 27（= M2 13 场景 × PG/A + A 专形
date 1）、`OgCompatGuardIT` 3（m2b B 档 ISE + 槽副作用缺席 / A 档放行 / A 档列存表
ISE）、`OgParitySmokeIT` 2（单事务冒烟 + 分区冒烟）；M1 五组 IT 零改动全绿。

**已知限制（M3 增量）**：B/'C' 档整库不支持（ISE 附 M4 指引——B 档 MySQL 风格归
M4、'C' 为 PG 兼容历史别名未验证）；列存/段页式/bucket/UStore **长期非目标**（列存
CU 族 WAL 是独立大项，除非呈报）；跨分区 UPDATE 迁移 = DELETE+INSERT 两行（语义
自然，专项 IT 已钉）；"链断不回退三表"语义收紧后的真实链断 + oid 碰撞叠加形态极窄，
记档不另设 IT。

## VastBase G100 档（第四布局档，2026-10-13 spec / 同日 T0-T7 收官）

**范围**：VastBase G100 **V3.0（Build 9，OG 6.0.0 内核，2026-03-31）** 的 catalog 同步面
+ DML 面——与 OG 档同构的完整能力（四字典 / 检查点续传 / direct 通道[真库实测，file 通道
形态同 OG 未专项验] / DML 输出 / 分区捕获[第四字典沿用 M3] / A 档[真库恒 A 档，M3 白名单
内]）。设计 `docs/superpowers/specs/2026-10-13-wal-source-vastbase-adaptation-design.md`
+ spike `docs/wal-source-vastbase-adaptation-spike.md` + 实施计划
`docs/superpowers/plans/2026-10-13-vastbase-adaptation.md`。**仅认 V3.0**：判别命中
（relname attlen=128）但版本面不认的其他 OG 系发行版仍拒（64B name 的无 branding 发行版
走 vanilla 9.2 ISE 不变）。

**判别（第二面 = relname 探测）**：`version()` 报 `PostgreSQL 9.2.4 ...` **无任何
openGauss/VastBase branding**（branding 只在 `v$version` 视图）且 `server_version_num=90204`
——与 vanilla 9.2 / OG 三方撞号。`WalLayouts.forServer(int, String, int)` 三参重载：9 万段
+ 含 "openGauss" 文本 → OG 档（两参版语义不变）；9 万段 + 无 branding + **经已建 SQL 会话
探测 `pg_class.relname` attlen** → 128 = VastBase 档 / 64 或探测失败 = vanilla 9.2 ISE 不变
（消息附探测值）。探测在建槽**之前**（M2 护栏纪律：ISE 先于槽副作用）、只跑一次（9 万段
无 branding 才触发，17/18 档零触碰）。OG 装配分支门控 `instanceof WalLayoutVOpenGauss` 化
（子类命中同一 `startOpenGauss`——OG 变体建槽/bootstrap OG 种子/direct 通道全沿用）。

**词典几何差异表**（`WalLayoutVVastbase extends WalLayoutVOpenGauss`，全部列序/attlen
2026-10-13 真库实查锚——spec 附录 A）：

| 面 | OG 5.0.0 | VastBase G100 | 形态 |
|---|---|---|---|
| NAMEDATALEN（name kind） | 64（`name`） | **128（`name128`，T1 新 kind——128B 定宽/对齐 'c'/剥尾零）** | 四词典含 name 列者全加宽 |
| pg_class 词典 | oidhdr + 40 = 41 | **oidhdr + 42 = 43**（槽 24 插 `relhassecids`；尾增 `relminmxid`/`relrewrite`） | 读区偏移统一 +64B |
| pg_attribute 词典 | 24 | **29**（槽 19/20 插 `attalias`(name128)/`attisquoted`；尾增 `attidentity`/`attextraopt`/`attdroppedname`） | 投影槽位（attnum@5/dropped@14 等）全在分叉点前——AttrRow 零改动 |
| pg_partition / pg_namespace 词典 | 32+oidhdr / 7+oidhdr | **列集零分叉，仅 name → name128**（读区偏移同 +64） | M3 第四字典沿用 |
| pg_class 读区偏移 | relfilenode@84 / reltoastrelid@116 / relkind@143 | **148 / 180 / 207**（name 128 + oid×5；自检锚"与 OG 差恰 64B"） | INPLACE 直读/截断重建读区 |
| heap UPDATE/DELETE main 前缀 | 13 / 11（老结构 + LogCSN） | **21 / 19**（再 + **xid u64 殿后**，无条件在场——T6 真库实测锚；INIT_PAGE 变体仍 +8 前缀 pd_xid_base，`XactGrouper.ogInitShift` 同型适用） | 记录层唯一分叉（spec §0.2 "记录层全兼容"修正为"记录头全兼容，heap 载荷 xid 宽度分叉"） |
| DML 类型 oid | — | **9040（oradate，A 档 `date` 关键字私有类型）→ 1114（timestamp）**：`WalLayout#remapDmlTypeOid` 接缝（缺省恒等，VastBase 覆写；`TableFilter.assembleMeta` 经接缝归一——catalog 面 atttypid 保持 9040 原值不受扰） | 磁盘表示与 timestamp 同构（8B epoch-2000 微秒），DecodeKinds/渲染零改动 |
| 检查点档位 | majorVersion=5 | **majorVersion=6**（v5 检查点在本档拒载回落全新引导——判别落点的排他证据） | StateStore 档位隔离 |

**验收（手动档 IT，`-Dvb.it.vastbase=true`，连真库 172.16.107.133:55433/test_a——非
Testcontainers 长活共享环境；常规 `mvn test` 全 skip）**：`VastCatalogSyncIT` 2（干扰 + 全
DDL 场景四字典 vs JDBC REPEATABLE READ 逐行全等——对拍器包内自持复刻 `WalSyncItBase` 语义
+ pg_partition 行级/身份链两面 + 门面判别链 v6 检查点排他锚）、`VastDmlParityIT` 15（v2
六场景 + A 档 date 专形 + **COPY 批量 MULTI_INSERT 真几何[T7 补]**——基准路 = opengauss-jdbc
逻辑流 + `OgParityBaseline` 复用[T0 spike 裁定"同形"：认证 md5 码 5 一次过、B 帧 xid Int64/
TupleData Int16 ncols 前缀两处帧级差异原样成立]、DDL-in-txn 双块全量 parity[VastBase 解码器
不丢弃 DDL 后续行——与 OG 的静默丢弃裁定反转]、2PC/中途停续 state.dir 两轮）、`VastLifecycleIT`
3（检查点续传两轮启停对拍 / state 截断回落全新引导 / 门面全装配 direct 通道两轮启停——对拍
断言复用 `VastCatalogSyncIT.assertVastCatalogMatchesJdbc` 包内共享面）。环境/连接参数经
`-Dvb.it.vastbase.{host,port,db,user,pass}` 可覆盖（缺省真库实测值，`VastTestEnv` 单一来源）。

**已知限制（VastBase 档增量）**：①**仅 V3.0（Build 9）**——其他版本线不承诺，64B name 无
branding 发行版仍拒（探测假阳性面：未来 NAMEDATALEN=128 的其他发行版会被误认——spec §8
记档可接受）；②**uuid/numeric 输出函数方言**（值面裁定，沿 OG float 先例避开 + 记档）：
`uuid_out` 渲 RAW 风格（32 位大写 hex 无连字符）vs wal 路 `DiskValueRenderer` PG 风格（小写
带连字符）——同一 16B 值的两种合法文本形态，对拍矩阵不收 uuid 列（离线渲染测试覆盖）；
`numeric_out` 剥小数尾零（Oracle NUMBER 语义，`.1234567890` → `.123456789`）——矩阵取无尾零
scale 值避开；方言渲染分支（wal 路按档切文本风格）记档归后续任务；③INIT_PAGE + CONTAINS_OLD
的 UPDATE/DELETE 前像偏移是**离线手造字节锚**（OgXactGrouperSeamTest 双档用例——真库该
角落未独立构造，推导形态与 OG INSERT INIT_PAGE 探针同型）；④file 通道（pg_receivexlog
sidecar）未在 VastBase 专项验证（direct 通道真库全链验证）。



- **~~对拍 IT 满负载偶发（非回归，2026-10-12 M2 T1 记档）~~ 已修（2026-10-13 双 flake
  根因修复，均实测复现钉）**：①`DualPathParityIT` TOAST 重发场景——根因是**重启续传
  重放窗口切断**（续传起点页对齐下取整，页边界落在最后 chunk 记录与 heap 行之间时
  chunk 不重发 → 重发 INSERT 空面渲 `<toast-unchanged>` vs engine 首交付真值；单跑
  稳定绿是全新容器的确定性 WAL 相位恰好落安全区，满负载下背景写入漂移相位入危险区），
  修复 = INSERT 新元组空面 probe 回查（`ToastAssembler.insertTupleResolver`，见"清窗
  全局 vs per-txn 窄缝"条目），场景内 WAL 相位填充钉该几何 + ToastAssemblerTest 三
  用例钉语义；②`OgDualChannelParityIT` boundary 断言——根因是追平等待窗内**服务端
  背景写入**（autoanalyze 等）使两路各收到 boundary 之后的记录，旧断言
  `upper <= boundary + 8` 在良性越界（+0x30~+0x140 一条背景记录）假红；修复 = 覆盖面
  断言改自下覆盖（min 末记录末尾距 boundary ≤ 两页余量 + 双路前沿诊断输出），对拍
  窗口按 upper 归一排除 boundary 后记录的构造不变。
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
  （CHECKPOINT 后首写必带页镜像）；external/行内压缩 varlena 的**值面**已由
  `ToastAssembler` 重组解压支持（pglz + lz4 双方法，2026-10-07 补 lz4）。
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

## 配置面（`vb.wal.*` 十一基础键 + openGauss 档五键，`WalSource`/`StateConfig` 单一来源）

**冒烟面三层合并**（2026-10-12 起，仅宿主层——`WalSource` 库 API 不动）：工作目录
`walconfig.properties` 基础值（键名与 `-D` 参数完全同形；按十六键白名单过滤，白名单外
键 WARN 忽略；文件/系统属性空串值均视为未设）→ `-Dvb.wal.*` 系统属性同键覆盖 → 门面
构造器默认值兜底。默认文件不存在即纯 `-D` 形态（与文件支持前行为一致）；`-Dvb.config=
<路径>` 整份替换默认查找（指向不存在/不可读/内容坏启动期 ISE fail-fast；该键是保留键
不进门面配置）。实现在 `WalConfigFile`（顶层，与 `Main` 平级，模板
`src/docker/walconfig-pg.properties` / `walconfig-og.properties`）。

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
| `vb.wal.og.source` | `file` | **OG 档**收流通道：`file`=sidecar pg_receivexlog 落目录 + FileWalSource tail；`direct`=DirectWalSource 裸 v3 直连。非法值构造期 IAE；PG 17/18 档不读本键 |
| `vb.wal.og.rx.mode` | `managed` | OG file 通道 sidecar 管理：`managed`=自拉起 pg_receivexlog 子进程；`external`=只读既有落盘目录（进程运维自担）。非法值 IAE |
| `vb.wal.og.rx.dir` | `wal-og-queue` | OG file 通道落盘目录（managed 的 `-D` 参数值；相对工作目录）。显式空白构造期 IAE |
| `vb.wal.og.rx.bin` | `pg_receivexlog` | OG managed 模式的 pg_receivexlog 可执行文件（走 PATH）。显式空白构造期 IAE |
| `vb.wal.og.jdbc.prefix` | `jdbc:postgresql:` | OG 档 SQL 会话驱动 URL 前缀：`jdbc:opengauss:`=opengauss-jdbc（sha256，OG 部署**显式设置**）；缺省保 `jdbc:postgresql:`=vanilla pgjdbc（+md5，须 pg_hba 预埋 md5 行）——**缺省不换前缀的理由**：是否 OG 档在建连后才知道，默认换前缀会让 PG 17/18 档零配置变化下改走 opengauss-jdbc（违反 PG 档零回归）。非法值 IAE |

冒烟 Main 周期行（10s，logger `org.vastdata.vbstream.walsource.Main`）：
`smoke: lsn=… records=N resyncs=N reconnects=N censusTop3=[rmid/XX=count,…]`，state
启用时追加 `ckpt=LSN adv=LSN`（二者相等即"落盘后必推过"，落后即推进失败被 WARN
吞掉的形态），DML 面启用时追加 `dml=[buckets=N rows=M partMiss=K]`（ChangeStream 会话
累计发射计数——过滤后实付口径；`partMiss`= OG 档分区身份链断 fail-safe 跳过计数
[vanilla 恒 0]，M3 T6 透出）。启动失败 ERROR + exit 1；接收器终态死亡（5 次重连失败/解析
ISE）周期行检测 → ERROR + exit 1；Ctrl-C hook 优雅 close（含最终 best-effort 检查点 +
槽推进）。DML 数据输出走 CDC 专用 logger `org.vastdata.vbstream.walsource.cdc`（INFO，
与 engine 的 `org.vastdata.vbstream.cdc` 分流惯例同构）。

## 测试面

`mvn test -pl vb-stream-wal-source` 单命令全跑（surefire 显式补 `**/*IT.java`）：
**517 用例 = 离线 398 + Testcontainers IT 99（PG 41 + openGauss 58；需本机 Docker）+
VastBase 手动档真库 IT 20（`it/vastbase` 包——常规跑全 skip，`@EnabledIfSystemProperty`
计入 Tests run/Skipped；Windows 开发机另有环境性存量 7 例已知记档：FileWalSourceTest
6[.partial rename 撞 Windows 文件锁] + OgDualChannelParityIT 1[file 通道 sidecar 追平
超时]——pg_receivexlog 文件通道族在 Windows 的已知限制，非回归）**
（2026-10-13 双 flake 根因修复 +3：ToastAssemblerTest INSERT 视图空面回查三用例）
（2026-10-13 M3 收口 surefire 逐类对账口径；2026-10-13 vastbase T7 收口同口径重对——
离线 377→396 净增 19：vastbase 线 18[TupleDecoder +2 name128 / TableFilter +1 /
WalLayoutsOgDiscriminator +1 三参 / WalLayoutVVastbase 10 / VastProbeDiscriminator 2 /
OgXactGrouperSeam +2] + OgPartitionDictionary 9→10 的 M3 期漏记 1 一并校准 + 手动档 20）。

- **离线**（秒级）：`layout` 手造字节走读（WalRecordParserTest 12 / TupleDecoderTest 17
  / HeapViewsTest 12 / PageImagesTest 6 / WalLayoutV17Test 9 / WalLayoutV18Test 9——含
  "两版 toast 偏移差恰 4B"自检锚；**OG 档**：OgWalRecordParserTest 4（真实语料 283 条
  全窗走读 + 四锚记录 + prev 链自洽）/ OgPruneViewTest 6（9.2 系 CLEAN 老格式 + 撞号
  门控）/ WalLayoutVOpenGaussTest 5 / WalLayoutsOgDiscriminatorTest 6（90204 撞号
  文本判别 + vastbase T4 三参探测）/ **OgXactViewTest 6（M2 T2：XACT 四记录族手造 + 语料锚——含无 252
  TOPLEVEL_XID 的终态 subxacts 归并面）/ OgMultiInsertViewTest 4（M2 T3：九条真 COPY
  语料收口 + 手造变体）**）；`receive` WalStreamWalkerTest 14（锚定协议含 mid-stream 漂移免疫 +
  OG 整零页容忍）/ FileWalSourceTest 12（目录 tail + .partial 预分配全零的前向扫描
  交付上限 + **log/seg 段名语义锚——4GB 边界标准名寻址与续传选段两用例[2026-10-15
  修复 `parseSegNo` 直拼 bug：段号 = log × (0x100000000/段尺寸) + seg，seg 字段值域
  校验越界跳过；spike 第三轮真库 pg_xlog 实证 M1 以来全部 OG 测试在 log=0 区间从未
  暴露，lsn ≥ 4GB 寻址错乱——修前 file 通道在该形态不可用，见 spike 文档第三轮节]**）
  / direct 子包（**M1 裸 v3 客户端**：V3ProtocolChannelTest 6 / OgAuthTest 6
  （sha256 码 11/码 10-3.0 签名/md5 三认证面）/ DirectWalSourceStreamTest 6 /
  DirectWalSourceFeedbackTest 2（65B 'r' 反馈 + 心跳））；`replay` CatalogReplayTest
  16（含 attnum 下界 vanilla 零回归锚）/ CatalogSynchronizerTest 10（含引导前沿种子
  过滤线——种子 lsn 不吞引导窗口记录，终审 C1 离线锚 + M2 T7 修复 C 映射目录段号直认
  接缝锚）/ SelfHealerTest 4 / **OG 档**：
  OgCatalogReplayTest 11（头 oid/护栏/OG 词典/attnum -7/-8 合法下界——②b 已随修复 B
  反转为 parttype='p' 放行断言）/ OgCatalogBootstrapSqlTest 4 / OgCatalogBootstrapSeedWarnTest
  3（M3 T2 连动：捕获语义下分区母表 seed **零 WARN**——'p' 行入字典锚 + vanilla 零
  WARN 锚；M2 终审 I1 的节流 WARN 断言随语义退役更新，强度等）/ **OgPartitionDictionaryTest
  10（M3 T1 第四字典：kinds 词典镜像 og-plan pg_attribute 实查/INS·UPD·DEL·PRUNE·INPLACE
  五形态投影[含 EXCHANGE 跨页换段号与截断重建读区回填]/bootstrap 种子 SQL/vanilla 未回填
  段号第四腿 inert 零回归锚）**；`state` StateStoreTest 12
  （往返/CRC 拒载/版本拒载/
  pgVersion 参数化错配拒载——17 写 17、18 写 18，终审 I1；formatVersion 3 dmlFloorLsn
  往返 + OG 档 majorVersion=5；**M3：formatVersion 4 第四字典序列化往返 + v3 检查点
  拒载回落全新引导**）/ StateConfigTest 5；`changes`（v2 DML 面：
  DiskValueRendererTest 18 / XactGrouperTest 22（含 M2 T7 修复 A 的 MULTI_INSERT
  SHORTALIGN 真几何锚）/ ToastAssemblerTest 16（含 lz4 双形态
  解压往返与未知方法码 ISE）/ OutputRendererTest 7 / TableFilterTest 17（M2 T5 OG 档
  parttype 判别闸——V17/V18 用例零改动零回归；**M3：分区身份链四用例[母表身份/白名单
  双匹配含分区名不外溢负例/链断 WARN 恰一次+计数/vanilla 无视分区链] + EXCHANGE 换绑
  段号 oid 碰撞属主优先双向锚，T5 修复**）/
  PgFloatFormatTest 7 / PglzTest 5 / Lz4Test 6（lz4 裸 block：手造黄金字节 +
  库压缩器往返 + 截断/长度不符 ISE）/ ChangeStreamTest 7；**OG 档（M2）**：
  OgXactGrouperSeamTest 7（OG XACT 接缝——终态 subxacts 归并/COMMIT_COMPACT/2PC 面，
  T2；**+2 INIT_PAGE + CONTAINS_OLD 的 UPDATE/DELETE 旧元组偏移角落——OG 13/11+8 与
  VastBase 21/19 常态 + INIT_PAGE 29/27 双档，`ogInitShift` 的 +8 面（vastbase T7 补钉，
  Fixture 布局参数化）**）/ OgColumnCompressionTest 21（四算法黄金向量 + meta 走读 + 失败
  负例，T4）；
  **OgPartitionIdentityTest 5（M3 T2：OG 布局手造记录喂真 XactGrouper——分区行母表
  身份发射/第四字典 heap 记录全清缓存/分区自有 toast chunk 重组[ToastAssembler 零改动
  锚]/链断 fail-safe/partitionOf 缺省 empty[vanilla no-op 锚]）**）；
  `api` WalSourceConfigTest
  5 / OgWalSourceConfigTest 14（OG 五键解析/缺省值/IAE 面 + 全装配门面 README/Main
  形态 DML listener 装配锚[伪 Driver 顶替 DriverManager 驱动 start() 全路径，M2 T1
  反转 M1 的 WARN 降级断言——PG 档库 dmlEnabled 保持 true]）/ OgCompatModeGuardTest 4
  （兼容模式护栏：档位判定面 PG/A/B/C——**M3 起 'A' 白名单内**[原 'A' ISE 用例换 'B'
  输入 + M4 指引连动] + 不可判定 fail-open + 全路径白名单外档位 ISE 先于槽副作用 +
  'A' 档伪 Driver 全路径 DML 装配放行）；顶层 WalConfigFileTest 7（2026-10-12 配置文件层——白名单过滤/空串
  未设/缺文件空集与 vb.config fail-fast/保留键不入 props/Main 三层合并次序）；
  ModuleBuildTest 1。
- **IT**（postgres:18 `WalTestEnv`/`Interference`/`ParityEnv` + postgres:17
  `Wal17TestEnv` 矩阵，`WalSyncItBase` 对拍器 = 停流后 ctid 键控行字典 vs JDBC
  REPEATABLE READ 实查**逐行全等**）：`DualPathParityIT` 20（v2 双路对拍——engine
  逻辑解码路 in-process vs wal 直解路 DML 面同容器对拍，xid 交集逐行 diff 空：场景
  1 系基础 DML 六形态 + 类型矩阵边界值 + DDL-in-txn as-of + TOAST 三存储形态/重启
  unchanged + 2PC 四形态 + **普通 DML 中途停续（Task 12——at-least-once 重发双路一致，
  段一同页事务整桶幂等重发由重复块容忍面吸收）+ 流内 CREATE SCHEMA nsp 字典面
  （Task 4 延期清账）+ 干扰矩阵（Task 12——catalog 风暴线程[autovacuum toggle +
  ANALYZE 循环]下场景 1/TOAST 复跑 @RepeatedTest(2)）** + lz4 压缩列双形态值面
  （2026-10-07：`SET COMPRESSION lz4` external/行内 lz4，`pg_column_compression`
  钉前提）、`WalReceiveTest` 6（接收
  锚定 + 杀后端原地重连 + 首连失败）、`WalAdversarialIT` 3（autovacuum 拉满/ANALYZE
  风暴/随机 CHECKPOINT 干扰下 DDL 全场景对拍，@RepeatedTest(2) + RENAME 最小复现）、
  `WalLifecycleIT` 5（检查点续传/损坏回落全新引导/丢页末态追平 resyncs==0/门面全装配/
  引导窗口并发 DDL 不丢行）、`Wal17SyncIT` 7（17 对抗精简版 + 生命周期 + 跨版本分发
  拒绝与 V18 注入 fail-fast 零副作用）；**openGauss 档 58 用例**（`opengauss/opengauss:5.0.0`
  `OgTestEnv` 单例——M1 五组：`OgCatalogSyncIT` 1 / `OgLifecycleIT` 3 /
  `OgInstanceSurvivalIT` 1 / `OgGoldenWireIT` 1 / `OgDualChannelParityIT` 1；**M2 四组
  （2026-10-12）**：`OgParitySmokeIT`（对拍冒烟）/ `OgDmlParityIT`（v2 六场景 OG
  档全量对拍）/ `OgDmlOgSpecificIT` 3（attcmprmode 护栏 + 分区捕获两形态——M3 连动
  反转为母表身份捕获断言）/ `OgCompatGuardIT`（B 档启动期 ISE + 槽副作用缺席）；
  **M3（2026-10-13）**：`OgParitySmokeIT` 增至 2（+分区冒烟）/ `OgDmlParityIT` 增至
  27（13 场景参数化 PG/A 双库 + A 专形 date）/ `OgCompatGuardIT` 增至 3（+A 档列存表
  ISE）/ 新增 `OgPartitionIT` 16（分区专项 PG+A 双档——详见"openGauss 5.0.0 档 M3"
  节验收段））；**VastBase 手动档 20 用例（2026-10-13 vastbase T5-T7，`it/vastbase` 包
  `VastTestEnv` 真库静态单例——非 Testcontainers，`-Dvb.it.vastbase=true` 才跑，常规
  `mvn test` 全 skip）**：`VastCatalogSyncIT` 2（T5：干扰 DDL 四字典对拍 + 门面判别链
  v6 排他锚——对拍器包内自持复刻 `WalSyncItBase` 语义）、`VastDmlParityIT` 15（T6 六
  场景 + A 档 date 专形 + T7 补 COPY 批量 MULTI_INSERT 真几何——基准路复用
  `OgParityBaseline`）、`VastLifecycleIT` 3（T7：检查点续传两轮启停对拍 / state 截断
  回落全新引导 / 门面全装配 direct 通道两轮启停——详见"VastBase G100 档"节验收段）。
