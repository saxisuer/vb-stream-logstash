# vb-stream-wal-source openGauss 5.0.0 适配 M1 设计（catalog 同步面）

日期：2026-10-11 · 状态：已与用户逐节确认（brainstorming 五节全过）
前置调研：`docs/wal-source-opengauss-adaptation-research.md`（实测锚与根因链全录）
里程碑序列：**M1（本 spec）**= 收流双通道（直连客户端已按用户裁定 B 并入）+ layout 解析 +
catalog 维护对拍全绿；M2 = DML/changes 面（另立 spec）。

## 0. 范围裁定（用户确认）

- **存储引擎只做 ASTORE**（传统 PG 布局行表）；UStore/列存/段页式/bucket 表启动期
  fail-fast 拒绝，流中遇到块引用扩展位 fail-fast。
- **版本只做 openGauss 5.0.0**；3.x/6.x/7.x 后期单独立项；vanilla 9.2 非目标。
- **收流双通道都进 M1**（用户裁定方案 B）：sidecar 文件源 + Java 直连客户端，
  互为对拍基准。
- **同模块**：openGauss 作为 `vb-stream-wal-source` 的 layout 版本档进（沿 V17/V18
  多版本描述符模式），不开新模块。
- **SQL 面驱动**：openGauss 档默认 `org.opengauss:opengauss-jdbc`（sha256 兼容，
  `jdbc:opengauss:` URL）；vanilla pgjdbc + md5 亦可（`jdbc:postgresql:`，运维自担）。
  DriverManager 按 URL 前缀分流，代码零分支。

  > **勘误注记（2026-10-12 Task 6 实施裁定，Task 10 回写）**：上述"OG 档默认
  > opengauss-jdbc"与实施形态不符——**SQL 会话先于版本判别建立**（判别依赖会话内
  > `current_setting('server_version_num')` 查询），建连时无法得知是否 OG 档；若缺省
  > URL 前缀是 `jdbc:opengauss:`，PG 17/18 档会在零配置变化下改走 opengauss-jdbc，
  > 违反"PG 档零回归"硬约束。实施形态：新增 `vb.wal.og.jdbc.prefix` 键（缺省
  > `jdbc:postgresql:` 保 PG 档零感知），**openGauss 部署显式设 `jdbc:opengauss:`**
  > 选 sha256 驱动。§7 配置键清单相应增第五键。

## 1. 架构总览

```
                    ┌─ PG 17/18 档（现状，零改动）
WalSource.start() ──┤
                    └─ openGauss 5.0.0 档（新）：
                        版本判别：server_version_num=90204 且 version() 含 "openGauss"
                        → WalLayoutVOpenGauss（magic 0xD074/页头 24·40B/记录头 32B/
                          块引用扩展位/catalog 41·24 列偏移——全部调研实测锚）
                        SQL 会话：jdbc:opengauss:（sha256）/ jdbc:postgresql:（md5）
                        收流（WalByteSource 新抽象，配置二选一）：
                        ├─ FileWalSource：sidecar pg_receivexlog 落目录
                        │    managed（默认，自拉起子进程）/ external（只读目录）
                        └─ DirectWalSource：replication=true 薄客户端
                             （IDENTIFY_SYSTEM → START_REPLICATION 段对齐 →
                              65B 'r' 反馈 + 心跳；黄金对拍验收）
                        共享管线（复用）：WalStreamWalker（零改动）→
                        CatalogSynchronizer（三修正零改动）→ StateStore → 对拍验收
```

**为什么是这个形态（调研结论摘要）**：openGauss 把物理复制流视为 HA 体系——
`replication=database` 形态被 walsender.cpp:471 硬路由进逻辑发送循环（空上下文即死），
walsender 死亡会触发 HA 状态机使整个实例退出；合法物理客户端只有 `replication=true`
形态。pgjdbc/opengauss-jdbc 的 replication API 只会 database 形态，故不可用，
需自研薄客户端；`pg_receivexlog`（发行版自带，pg_receivewal 旧名）是现成合法客户端，
作 sidecar 保底 + 黄金基准。

## 2. 组件明细

| 组件 | 包 | 动作 | 职责要点 |
|---|---|---|---|
| `WalLayoutVOpenGauss` | layout | 新增 | OG 全部格式常量，逐常量标注 openGauss-server v5.0.0 源码出处；catalog 偏移含"读区差恒定"式自检锚 |
| `WalLayouts` | layout | 改动 | 判别器：`forServer(int, String)`——90204+version() 含 "openGauss" → OG 档；纯 vanilla 9.2 拒绝（ISE 附指引）；OG 其他主版本拒绝（ISE 附版本号） |
| `WalRecordParser`/`HeapViews`/`PruneView` | layout | 改动 | 块引用 OG 分派（现代式 + 扩展位：`BKID_HAS_BUCKET_OR_SEGPAGE(0x80)`/`BKID_HAS_TDE_PAGE(0x40)` 命中即 ISE）；OG PRUNE 格式转录实现（见 §6 任务 T4） |
| `WalByteSource` | receive | 新增 | 推模式接口：`start(字节回调)`/`stop()`/前沿观测；字节回调携带锚（文件名+偏移 / CopyData 位点），与 `walker.feed(chunkEndLsn, bytes)` 对齐 |
| `FileWalSource` + `ReceivexlogProcess` | receive | 新增 | 目录 tail（段发现/.partial 增长/完成 rename/断点续读；锚=段号×16MB+读偏移，页头 pageaddr 接管纠偏）+ managed 子进程管理（拉起/优雅终止/死检测重拉） |
| `DirectWalSource` | receive | 新增 | v3 裸协议：`replication=true` startup（**md5 + OG sha256 挑战**——从 opengauss-jdbc 移植，sha256 是服务端默认不可回避）→ IDENTIFY_SYSTEM → xlogpos 段对齐 → START_REPLICATION [SLOT] → 'w'/'k' 解析 → 65B 'r' 反馈 + 心跳线程 → 断线重连 |
| `CatalogBootstrap` | replay | 改动 | 种子 SQL 列单适配 OG 41/24 列；B 函数 `pg_current_xlog_location()`（无 flush 档） |
| `CatalogReplay`/`CatalogSynchronizer` | replay | 改动 | 偏移全经 layout 取（现有模式）；三修正逻辑零改动；UStore/列存/bucket 识别后拒绝 |
| `WalSource` 配置面 | api | 改动 | 新键：`vb.wal.og.source=file|direct`、`vb.wal.og.rx.mode=managed|external`、`vb.wal.og.rx.dir`、`vb.wal.og.rx.bin`（默认 pg_receivexlog）（实施期另增第五键 `vb.wal.og.jdbc.prefix`，见 §0 勘误注记） |
| `StateStore` | state | 改动 | pgVersion 校验面加 OG 档（formatVersion 兼容扩展） |

`WalStreamWalker` 零改动：页头 24B、rem_len@16 与现代版同构；记录头 32B 走
`layout.recordHeaderSize()`；magic 走 `layout.pageMagic()`——全部已参数化。

## 3. 数据流

**启动次序**：SQL 会话 → 判别器 → `ensureSlot`（OG 变体：`pg_create_physical_replication_slot
(slotname, false)`，第二参为 dummy_standby——与 vanilla immediately_reserve 语义不同，
restart_lsn 保留行为实测后定是否用 `_extern` 变体）→ state 续传/引导决策 → bootstrap
（RR 单事务首句读 `pg_current_xlog_location()`，OG 无 flush 档；write 位点起流由服务端
等到 flush 自然收敛——调研注记）→ 收流起源（file/direct 按 `vb.wal.og.source`）→
字节回调 → walker → sink（applyCatalogRecord + postApply 预留）→ maybeCheckpoint。

**流中**：两通道字节流同构（"带绝对 LSN 锚的字节块"）。DirectWalSource 心跳线程每
**statusInterval/2**（客户端配置参数，与 receivexlog 的 standby_message_timeout 语义同）
发 65B 'r'（`replyRequested=true`，字段布局：
receive/write/flush/apply/applyRead 各 8B + peer_role/peer_state 各 4B + sendTime 8B +
replyRequested 1B + replyFlags 4B，C 对齐垫按实测钉）；FileWalSource 无心跳（sidecar 自管）。

**停机**：Direct 优雅 CopyDone + socket close（红线：实例存活）；File managed SIGTERM
子进程等退出；之后走现有 stop() 最终 best-effort 检查点 + `pg_replication_slot_advance`。

## 4. 错误处理

**fail-fast（启动期/流中，宁拒不错）**：vanilla 9.2 ISE；OG 非 5.0.0 主版本 ISE（附版本号）；
UStore/列存（relcudescrelid≠0）/段页式/bucket 表拒绝；流中块引用扩展位命中 ISE（附 LSN）；
`rx.dir`/`rx.bin` 缺失构造期 IAE；Direct 认证失败 ISE 附指引。

**重试/降级**：Direct 断线指数退避重连（1s→16s×5，重连起点 = consumedLsn 段对齐下取整），
连续失败走 `receiverTerminalFailure` 终态契约；File managed 子进程死→退避重拉 + WARN；
external 无新数据仅等待（观测行见停滞前沿）；解析 ISE 终态死亡（现有契约）。

**已知限制（文档化承诺）**：OG 物理槽 restart_lsn 保留窗口行为未实测完（T6）；
write 位点起流在极端写入停顿下的表现；OG PRUNE 格式是唯一未字节实测的记录族（T4）；
挂了 replconninfo 的 HA 形态实例不在支持面（"build walsender 自杀"雷区，文档标注）。

## 5. 测试与验收

- **离线单测**：`WalLayoutVOpenGaussTest`（含读区差恒定自检锚）；parser OG 档手造字节 +
  **真实语料**（调研 sidecar 落的 WAL 段做 fixture，含实测锚记录）；DirectWalSource 协议
  编码单测（65B 'r'/startup/命令字节）。
- **黄金对拍**：strace 抓 `pg_receivexlog` 真实 wire bytes 存 fixture，断言 DirectWalSource
  连接序列/反馈字节逐字节等价。
- **双通道对拍 IT**：同容器同写入序列，File 与 Direct 两路字节流等价（LSN 对齐 + diff 空）。
- **功能对拍 IT**（`opengauss/opengauss:5.0.0` Testcontainers，multi-arch）：catalog 末态
  对拍（复用 `WalSyncItBase` 形态：DDL 场景后 ctid 字典 vs JDBC RR 实查逐行全等）；生命周期
  （续传/损坏回落/断线重连）；**实例存活红线**（反复连/断/杀客户端 N 轮实例每轮存活）；
  干扰精简版。
- **回归**：PG 17/18 现有 253 用例零改动零回归（receive 老路不动是硬约束）。

**验收标准**：①OG 容器 catalog 字典末态对拍全等；②双通道字节等价 + Direct 黄金对拍过；
③实例存活红线过；④17/18 零回归；⑤UStore/列存/扩展位 fail-fast 有测试钉死。

## 6. 任务分解（writing-plans 展开的基础骨架）

- T1 `WalLayoutVOpenGauss` + 判别器 + 自检锚单测
- T2 `WalByteSource` 接口 + `FileWalSource`/`ReceivexlogProcess`（managed/external）
- T3 块引用 OG 分派 + 扩展位 fail-fast + 真实语料 parser 测试
- T4 OG PRUNE 格式核对（gitee 源码转录 + 实测字节验证）与实现
- T5 `DirectWalSource`：v3 startup（md5+sha256 挑战移植）→ 握手 → 流循环 → 反馈心跳 → 重连
- T6 OG 槽生命周期实测（建槽语义/advance/保留窗口）+ `ensureSlot` OG 变体
- T7 `CatalogBootstrap`/`CatalogReplay` OG 适配（列单/偏移/LSN 函数）+ fail-fast 扫描
- T8 `StateStore`/`WalSource` 配置面接线
- T9 黄金对拍 fixture 采集 + 双通道对拍 IT
- T10 catalog 末态对拍 IT + 生命周期 IT + 实例存活红线 IT + 干扰精简版

依赖序：T1→T3/T4；T2 与 T5 并行；T7 依赖 T1/T4；T9 依赖 T2/T5；T10 收口。

## 7. 依赖与配置面变更

- pom 增 `org.opengauss:opengauss-jdbc`（runtime，openGauss 档 SQL 面；与 vanilla pgjdbc
  包名无冲突共存）；测试增 Testcontainers 镜像 `opengauss/opengauss:5.0.0`（multi-arch 已确认）。
- 配置键（openGauss 档增量）：`vb.wal.og.source`（默认 file）、`vb.wal.og.rx.mode`
  （默认 managed）、`vb.wal.og.rx.dir`（默认 wal-og-queue）、`vb.wal.og.rx.bin`
  （默认 pg_receivexlog）、`vb.wal.og.jdbc.prefix`（默认 `jdbc:postgresql:`——SQL 会话
  驱动 URL 前缀分流，实施期 Task 6 增设，见 §0 勘误注记）。PG 17/18 档配置键与行为
  零变化。
