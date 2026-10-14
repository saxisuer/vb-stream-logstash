# WAL 离线重放先导实验（spike）结论 — 2026-10-14

> **场景**（用户提出）：VastBase 库先经 wal-source 解码，作业暂停后物理复制槽未删、WAL
> 未删；一段时间后**库不可访问**。若从数据库拿到 WAL 日志（pg_xlog 段文件）到指定
> 文件夹——能否凭**本地已持久化的 catalog 检查点**直接离线解析该文件夹里的 WAL 续传
> DML？
>
> 代码：`spike/offline-replay/`（**throwaway**，不进 Maven 模块树；SqlRunner /
> OfflineReplaySpike / ParityDiff / WalSegmentRecorder 四件 + sql/ 数据脚本）。
> **两轮验证**：①openGauss 5.0.0 容器（机制链路）；②**VastBase G100 真库**
> 172.16.107.133:55433/test_a（第四布局档实测，见文末专节）。

## 结论：链路成立（PASS——离线重放与在线基准 CDC 输出逐字节全等）

以 opengauss:5.0.0 容器近似 VastBase（receive/checkpoint 链路同构，VastBase 档在
receive 包零专用代码；缺省 postgres 库即 A 档，贴近 VastBase 真库恒 A 档）：

| 验证面 | 结果 |
|---|---|
| 在线基准（direct 通道 Main 收流全程）vs 离线重放（拷出 pg_xlog + 旧检查点） | **按 xid 交集 6 块逐行全等，diff 空** |
| at-least-once 重叠段（检查点快照取在第一批中段，stored lsn=0/0234D010 < 前沿） | 重放段完整重建第一批尾部事务 + 第二批全部，**零丢零错** |
| TOAST external ~6.4KB 不可压缩值（md5 链） | 无 SQL probe 下**全值输出**（chunk 全在 WAL 内，重组/解压零库依赖） |
| DDL-in-txn as-of（事务内 ADD COLUMN） | 前段行 3 列 / 后段行 5 列（含 extra=777）——离线字典重放正确 |
| 2PC（PREPARE→COMMIT PREPARED） | kind=TWO_PHASE gid=spike2pc 同形 |
| 回滚事务 | 零输出（两路同） |
| 段尾未写区（pg_xlog 拷出段的预分配零尾 ~12.6MB） | `zeroPageSkips=1621`（OG 档整零页容忍面），**resyncs=0 / lossyResyncs=0 / carryDrops=0** |
| 离线检查点推进 | 重放结束落盘 `applied=0/02355150` = 在线最终前沿——**后续修复库可无缝切回在线** |

## 验证设计

```text
① 容器在线：direct 通道跑 Main（opengauss-jdbc SQL 面）收流
   第一批（建表+4 事务）→ 等检查点 ckpt=0/0234D010 落盘 → 快照 state → state.c1
   第二批（IUD 事务/TOAST/DDL-in-txn/回滚/2PC）→ 收完（dml=[buckets=6 rows=13]）
② 模拟死亡：kill Main → docker kill 容器（非优雅）→ docker cp pg_xlog → wal-dir/
   （段 000000010000000000000001/2 各 16MB 完成段；第二批已被 direct 通道收过 = 已 flush）
③ 挡路点实证：现状门面 external 模式启动 → 连接拒绝 exit 1（第一步 DriverManager）
④ 离线 harness（绕门面手工装配）：
   StateStore.load(state.c1 副本, pgVersion=5) → restoreInto（6289 attr 行/796 class 行）
   → WalLayouts.forServer(90204, "openGauss..", -1) → CatalogReplay(healer=null)
   → ChangeStream(sqlConn=null, probe 禁用) → FileWalSource(4 参 external 构造)
   → WalStreamWalker(过滤线=stored lsn 严格档) → EOF（前沿 15s 停滞）→ 最终检查点落盘
⑤ ParityDiff：两 log 按 xid 交集逐块 diff → PASS
```

## 发现清单（正式特性的资产）

1. **挡路点实证**：`WalSource.start()` 第一步 `DriverManager.getConnection` 即死
   （连接拒绝 → exit 1）——external 字节源与检查点都齐也到不了装配。其余挡路点
   （版本判别需活库 / `ensureSlotOg` 需活库 / `stored ≥ P₀` 校验依赖槽）在 harness
   绕行面得到间接验证。
2. **检查点恢复面完全公开**：`StateStore(dir, pgVersion).load()` + `StoredState.
   restoreInto(stores)` + `CatalogStores` 全公开 getter——离线装配只需 ~100 行 adapter。
3. **SnapshotView 依赖面零门面**：`CatalogSynchronizer.SnapshotView` 的全部实现基于
   `CatalogStores` public API——离线 `CatalogSnapshot` adapter 照抄即可（本 spike 的
   `SpikeSnapshot`）。**正式化时建议把这个 adapter 上提为 replay 包的公开类**（离线
   档与 SnapshotView 同源防漂移）。
4. **pg_xlog 拷贝目录即合法 file 源**：段名 24-hex 同构、`archive_status/` 等非段项
   静默跳过、完成段零尾由 `tolerateAllZeroPages()` 消化（1621 页零跳过、零 resync）。
   pg_receivexlog 落盘与服务器 pg_xlog 对 FileWalSource 无形态差。
5. **TOAST probe 缺席的主路径完好**：external 值 chunk 全在 WAL 内时零降级全值输出；
   probe 只在"归集面空/不完整"的窄缝才被需要（见"风险"）。
6. **XactGrouper 无过滤线是 at-least-once 的正面资产**：重放段（检查点 lsn 之前的
   旧事务）整段重建发射，与在线基准逐字节等——离线路对"检查点旧了多少"完全不敏感。
7. **EOF 判定形态**：库死文件不生长 → 前沿停滞（15s）即读尽；FileWalSource tail
   线程永续等待是离线档需要显式终止语义的根因。
8. **离线推进检查点 → 在线无缝衔接**：离线结束时 `applied` 追平在线最终前沿并落盘；
   库修复后切回在线时槽 restart_lsn 仍在老位置（离线推不了槽）≤ 新 stored lsn——
   续传校验天然通过。

## 正式特性改造点（若立项"离线回放档"）

1. **离线装配入口**（如 `vb.wal.og.mode=offline` 或独立工厂）：跳过 SQL 会话/版本
   判别/`ensureSlot`；**布局档从检查点恢复**——`StateStore` header 存了 pgVersion
   （OG=5/VastBase=6）但 `StoredState` 不携带，需 load 后回读（或扩展 StoredState）；
   `pgVersion → WalLayouts` 工厂入参映射要补（5→90204+openGauss 文本、6→VastBase 探测值）。
2. **`stored ≥ P₀` 校验的离线替代**：文件覆盖性启动期校验（stored lsn 所在段必须存在
   于目录；缺段时 `ceilingKey` **静默跳到后继段** = 丢数据，必须 fail-fast）。
3. **EOF 终止语义**：显式选项（读尽退出码/回调），替代 harness 的前沿停滞启发式。
4. **槽推进跳过**：离线无库可推，跳过省周期 WARN。
5. **兼容模式护栏豁免**：检查点在场即视为当时已过护栏（离线不探测 datcompatibility）。
6. **healer/probe 双 null 装配**已在组件面支持（spike 直接用），仅需装配入口固化。

## 已知限制与风险（离线模式的固有边界）

- **INSERT 重放切断窄缝无回查**：probe 不可用时，若段对齐起点恰好切在 INSERT 的
  toast chunk 与 heap 行之间，宽列渲 `<toast-unchanged>`（真值不可得，fail-safe 方向）；
  UPDATE 后像不受影响（OG/VastBase 的 heap_update 重写 toast 值）。
- **段覆盖缺口静默跳过**（改造点 2 未做前）：目录缺中间段 → 直接续读后继段。
- **timeline 混入**：tli 不参与寻址且同段号合并不辨 tli——主备切换过的环境拷 WAL 前
  需清理旧 tli 段（单机形态恒 tli=1 无此虑）。
- **崩溃尾部不完整记录**：walker 走到段尾等 carry（表现为停滞）；现状不校验记录 CRC，
  极端位翻转会以布局 ISE 暴露。
- **vanilla PG 17/18 档无此能力**：文件字节源仅 OG 系（VastBase/openGauss）file 通道
  形态；段 16MB/页 8KB 假设（库改过 wal_segment_size/block_size 需参数化）。

## 复现

```bash
# 容器（已清理，重跑需重起）：
docker run -d --name wal-offline-og -e GS_PASSWORD='OgTest#2026' -p 55440:5432 opengauss/opengauss:5.0.0
# 在线阶段（Main，direct 通道 + opengauss-jdbc SQL 面 + 10s 检查点）：
java -cp "vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" \
  -Dvb.wal.host=localhost -Dvb.wal.port=55440 -Dvb.wal.user=gaussdb -Dvb.wal.pass='OgTest#2026' \
  -Dvb.wal.slot=offline_spike -Dvb.wal.state.dir=spike/offline-replay/state \
  -Dvb.wal.state.interval.ms=10000 -Dvb.wal.tables=public.t_spike \
  -Dvb.wal.og.source=direct -Dvb.wal.og.jdbc.prefix=jdbc:opengauss: \
  org.vastdata.vbstream.walsource.Main > spike/offline-replay/online.log 2>&1 &
java -cp "spike/offline-replay/classes;vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" \
  SqlRunner spike/offline-replay/sql/batch1.sql        # → 等 ckpt 落盘 → cp -r state state.c1
java ... SqlRunner spike/offline-replay/sql/batch2.sql # → 等收完 → kill Main
docker kill wal-offline-og && docker cp wal-offline-og:/var/lib/opengauss/data/pg_xlog spike/offline-replay/wal-dir
# 离线阶段 + 对拍：
java -cp "spike/offline-replay/classes;vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" \
  OfflineReplaySpike spike/offline-replay/wal-dir spike/offline-replay/state.offline public.t_spike \
  > spike/offline-replay/offline.log 2>&1
java -cp "spike/offline-replay/classes" ParityDiff spike/offline-replay/online.log spike/offline-replay/offline.log
```

（Windows 备注：宿主 SQL 面必须 opengauss-jdbc——OG hba=md5 + sha256 存储命中认证码 11，
vanilla pgjdbc 过不去；log 文件为 GBK 字节，ParityDiff 按 ISO-8859-1 透传读入。）

---

## 第二轮：VastBase G100 真库验证（2026-10-14 同日，PASS）

**环境**（用户提供：172.16.107.133，pgdata=`/home/hongye/data/v3_A`；连接
172.16.107.133:55433/test_a，用户 test/Tester123，rolreplication=t，A 档库）：

| 探测面 | 实测值 | 判别意义 |
|---|---|---|
| version() | `PostgreSQL 9.2.4 ...`（无 branding） | 与 vanilla 9.2/OG 三方撞号 |
| pg_class.relname attlen | **128** | 三参判别命中 `WalLayoutVVastbase`（name128 词典） |
| hongye/hongye@50 | `FATAL: Forbid remote connection with initial user` | **hongye 是初始用户**——OG 系内核禁初始用户远程 TCP（凭据有效但策略拒绝，服务器本地 gsql 才能登录） |

**验证流程与第一轮同构**，WAL 段获取改为 **`WalSegmentRecorder`**（spike 新增）：
DirectWalSource（v3 自研客户端，VastBase 真库码面已验证）收流 → 按 LSN 稀疏写入
16MB 段文件（24-hex 段名与 pg_xlog 同形态）→ 离线 harness 用 FileWalSource external
读同一目录。**真库不可杀**——"库死亡"语义由"停 Main + 独立收集的段文件"承载（离线
输入面与拷 pg_xlog 等价：FileWalSource 只认段文件形态）。

**结果**（`spike/offline-replay-vb/`）：

| 验证面 | 结果 |
|---|---|
| 对拍（在线 direct 全程 vs 离线 c1 检查点重放） | **6 块逐行全等，diff 空** |
| 布局档恢复 | `WalLayoutVVastbase`（三参判别）+ StateStore pgVersion=6 检查点档位校验通过 |
| 字典灌回 | attrRows=13292 / classRows=1267（name128 词典） |
| TOAST ~6.4KB external | 真值输出（非哨兵） |
| DDL-in-txn as-of | 前段 3 列 / 后段 5 列（extra=777） |
| 2PC | kind=TWO_PHASE gid=vbspike2pc |
| 重放计数 | records=82、resyncs=0、lossyResyncs=0、carryDrops=0（recorder 段无预分配零尾，zeroPageSkips=0） |
| 离线检查点推进 | applied=2/071BF288（离线结束时落盘，衔接面成立） |

**真库轮新增发现**：

1. **libpq 侧车兼容性坑（file 通道 VastBase 部署风险）**：OG 5.0.0 容器内
   `pg_receivexlog`（libpq 客户端）对 VastBase 6.0.0 内核的 replication 认证握手
   失败（`Invalid username/password,login denied`——服务端拒绝客户端应答，非密码
   错误；同凭据 SQL 会话/direct 通道均一次通过）。**libpq 版本交叉的 sha256 挑战
   应答不兼容**疑因（与 opengauss-jdbc 码 11 缺 ITERATION_STRING 尾缀被拒的 M2 T6
   发现同族）。替代 = 自研 `WalSegmentRecorder`（DirectWalSource 落段）——也说明
   正式"离线档"若配 file 通道 sidecar，VastBase 部署须自带配套版本 pg_receivexlog
   或走 direct 采集。
2. `pg_hba_file_rules` 视图在 VastBase 上查询异常（列名/形态差异，spike 未深究）；
   hba 排障可走服务器本地文件直读。
3. 真库背景写入使离线 applied 前沿略超在线基准末尾（共享库的正常形态，交集对拍
   归一化吸收）。
4. **验证后清理**：t_vbspike 表与 vb_offline_spike 槽均已 drop（共享环境无残留）。

真库轮复现：`sql/vb-batch1.sql` / `vb-batch2.sql` / `vb-cleanup.sql` +
`WalSegmentRecorder`（先起 recorder 再起 Main——c1 检查点所在段须在采集范围内），
harness 第 4 参传 `vastbase` 切第四布局档。
