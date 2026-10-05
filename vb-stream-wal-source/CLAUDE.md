# vb-stream-wal-source — WAL 直解源（伪备库 catalog 同步 + WAL 接收）

不创建备库、不占主库逻辑解码 CPU 的"伪装备库"路线：pgjdbc **物理复制流**接收原始 WAL
字节，纯 Java 按 REL_STABLE 源码头文件转录的版本布局描述符解析记录，自维护
pg_attribute/pg_class 的 **ctid 重放 as-of 字典**。v1（2026-10-05 spec）只做两件事——
**A. catalog 同步**（JDBC 一致性引导 + WAL 重放维护两张目录表快照，含 ctid、
relfilenode、toast 映射、dropped 占位语义）与 **B. WAL 接收**（物理流接收、解析、
位点管理、检查点持久化）；用户表 DML 解码与 engine 汇入、TOAST external 重组、
two_phase 消费均属 v2 非目标。设计全文见
`docs/superpowers/specs/2026-10-05-wal-source-module-design.md`（先导实验
`docs/wal-direct-decode-spike.md` + `spike/wal-parse` throwaway）。

**依赖方向刻意收窄**：运行依赖仅 pgjdbc + slf4j-api，零 engine/Chronicle/logback
依赖——将来是 engine（或连接器）依赖本模块，不是反过来。包结构
`org.vastdata.vbstream.walsource` 下 `layout`/`receive`/`replay`/`state`/`api` 五包 +
顶层冒烟 `Main`。

## 组件（五组件 + api 门面，全部面向接口）

| 组件 | 位置 | 职责要点 |
|---|---|---|
| `WalLayout`/`WalLayouts` | `layout` | 版本化布局常量（`WalLayoutV17`/`WalLayoutV18`，逐常量标注 REL_STABLE 出处）；`forServerVersion` 按 `server_version_num` 分发，未注册版本/显式错配布局启动期 ISE fail-fast |
| `WalStreamReceiver` + `WalStreamWalker` + `PhysicalSlotManager` | `receive` | 物理复制流接收（readPending drain 轮询 + 断流指数退避重连 1s→16s、5 次失败停机）；walker 是字节流走读状态机（chunk 拼接/contrecord 缝合/**pageaddr 锚定协议**）；槽管理（`immediately_reserve=true` 建槽，restart_lsn 自建即保留——推进通道与保留窗口同时成立） |
| `WalRecordParser` + `WalRecord`/`BlockRef`/`HeapViews`/`PageImages`/`TupleDecoder`/`DecodeKinds` | `layout` | 记录头/block 头/heap 家族（INS/UPD/DEL/HOT/INPLACE/MULTI_INSERT/PRUNE）→ 强类型 record，纯函数；FPW 页镜像重建（hole 拼接 → ItemId 走读 → 页内 tuple 解码） |
| `CatalogSynchronizer` + `CatalogReplay`/`CatalogStores`/`CatalogBootstrap`/`CatalogRow`/`SelfHealer`/`JdbcProbe(Impl)`/`HeapEvent`/`Reconstruction` | `replay` | 通用 ctid 重放引擎（watched 表注册制、relfilenode/toast 跟踪、JDBC 一致性引导、竞速三修正）；单记录施加次序固定 PRUNE → INPLACE → attr 行事件 → class 行事件 |
| `StateStore` + `StateConfig` | `state` | 检查点持久化：单文件 `wal-source-state.bin`（magic 'VBWS' + header/ footer 双 CRC + lsn 双验），全量序列化 `.part` → fsync → 原子 rename；损坏/版本不符（含 pgVersion 与 layout 错配——写与 load 校验同 `layout.majorVersion()`，17 写 17、18 写 18）拒载回落全新引导（安全侧：宁可重引导，不可错位窗口重放） |
| `WalSource`（api 门面）+ `CatalogSnapshot` | `api` | 一次 `start()` 装配全管线（SQL 会话 → 版本分发 → 同步器），`consumedLsn()`/`metrics()`/`lastCheckpointLsn()`/`lastSlotAdvanceLsn()`/`resumedFromState()` 观测面——v2 engine 的接入点 |

单线程直通执行模型（接收 → 解析 → 重放 → 周期落盘全在 wal-receiver 线程，sink 同步
执行——检查点取当前已消费 LSN 即天然一致点，无需快照冻结）；组件接口化留解耦位。

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

## 关键坑位（实现/运维必读）

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
  COLUMN 的 pg_attribute INSERT 事件（页竞争窗口）——17 矩阵裁定 DDL 先行、风暴后置
  规避，根因未深挖（Task 16 报告疑虑节记档）。
- **压缩 FPW / external varlena 不支持**：pglz/lz4/zstd 压缩镜像与外部 TOAST 指针
  （0x01 短指针 / tag 0x02 压缩头）一律 ISE——前者要 `wal_compression=off`，后者 v1
  两张 catalog 表行内无超界值（v2 重组）。
- **VACUUM FULL 全表重发的 CDC 语义**：v1 非目标——重放面按新 relfilenode 跟踪字典，
  但用户表 DML 语义未承诺。
- **two_phase（PREPARE/COMMIT PREPARED）记录不消费**：v1 非目标。

## 配置面（`vb.wal.*` 九键，`WalSource`/`StateConfig` 单一来源）

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

冒烟 Main 周期行（10s，logger `org.vastdata.vbstream.walsource.Main`）：
`smoke: lsn=… records=N resyncs=N reconnects=N censusTop3=[rmid/XX=count,…]`，state
启用时追加 `ckpt=LSN adv=LSN`（二者相等即"落盘后必推过"，落后即推进失败被 WARN
吞掉的形态）。启动失败 ERROR + exit 1；接收器终态死亡（5 次重连失败/解析 ISE）周期
行检测 → ERROR + exit 1；Ctrl-C hook 优雅 close（含最终 best-effort 检查点 + 槽推进）。

## 测试面

`mvn test -pl vb-stream-wal-source` 单命令全跑（surefire 显式补 `**/*IT.java`）：
**131 用例 = 离线 110 + Testcontainers IT 21（需本机 Docker）**。

- **离线**（秒级）：`layout` 手造字节走读（WalRecordParserTest 12 / TupleDecoderTest 14
  / HeapViewsTest 12 / PageImagesTest 6 / WalLayoutV17Test 9 / WalLayoutV18Test 9——含
  "两版 toast 偏移差恰 4B"自检锚）；`receive` WalStreamWalkerTest 11（锚定协议含
  mid-stream 漂移免疫）；`replay` CatalogReplayTest 12 / CatalogSynchronizerTest 8（含
  引导前沿种子过滤线——种子 lsn 不吞引导窗口记录，终审 C1 离线锚）/ SelfHealerTest
  4；`state` StateStoreTest 7（往返/CRC 拒载/版本拒载/pgVersion 参数化错配拒载——
  17 写 17、18 写 18，终审 I1）+ StateConfigTest 5；ModuleBuildTest 1。
- **IT**（postgres:18 `WalTestEnv`/`Interference` + postgres:17 `Wal17TestEnv` 双矩阵，
  `WalSyncItBase` 对拍器 = 停流后 ctid 键控行字典 vs JDBC REPEATABLE READ 实查**逐行
  全等**）：`WalReceiveTest` 6（接收锚定 + 杀后端原地重连 + 首连失败）、
  `WalAdversarialIT` 3（autovacuum 拉满/ANALYZE 风暴/随机 CHECKPOINT 干扰下 DDL 全
  场景对拍，@RepeatedTest(2) + RENAME 最小复现）、`WalLifecycleIT` 5（检查点续传/
  损坏回落全新引导/丢页末态追平 resyncs==0/门面全装配/引导窗口并发 DDL 不丢行——
  终审 C1 复测 + Task 16 跟进项第一嫌疑验证，17 侧不复刻[已知风暴丢 INSERT 跟进项
  会混淆归因，18 侧已验]）、`Wal17SyncIT` 7（17 对抗精简版 + 生命周期 + 跨版本分发
  拒绝与 V18 注入 fail-fast 零副作用）。
