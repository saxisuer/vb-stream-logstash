# vb-stream-wal-source 离线回放档（offline replay）设计

- 日期：2026-10-15
- 状态：已实施（2026-10-15，feature/offline-replay 分支 commits c300194..c93ddd9）
- 先导：`docs/wal-source-offline-replay-spike.md`（三轮 spike——OG 容器机制链路 / VastBase
  真库 / 真 pg_xlog 段直读，全部 PASS；潜伏 bug `FileWalSource.parseSegNo` 已于 2026-10-15
  修复，commit `8e32702`）
- 关联：模块 CLAUDE.md（v1/v2/M1-M3/VastBase 档各节）、学习手册
  `docs/wal-source-learning-guide.md`

## 0. 背景与场景边界

**场景（用户提出 + spike 验证）**：VastBase 库经 wal-source 在线解析一段时间后作业暂停
（检查点已落盘、槽未删）；之后**库不可访问**（宕机/网络隔离）。若从数据库侧拿到 WAL
段文件（拷 pg_xlog 或复制流落段）到指定目录——凭**本地已持久化的 catalog 检查点**
离线续传解析 DML，直至 WAL 耗尽。

**场景支持矩阵**：

| 场景 | 支持状态 | 说明 |
|---|---|---|
| 先在线解析，后切离线（含离线中崩溃再离线、离线→在线回切、多次交替） | ✅ 本设计主场景 | 检查点是唯一状态源，双向互通是设计保证 |
| 首次启动即离线（冷启动） | ❌ 非目标 | 无活库无法做 JDBC 引导；纯流内重建需 WAL 窗含全表生命周期 + 四 watched 段号（离线不可得）。后续方向"半冷启动 meta 形态"（§8） |
| OG 5.0.0 / vanilla PG 17/18 档离线 | ❌ 非目标（本版） | 档位校验即 ISE；装配结构留扩展位 |

**约束（用户明确定下的需求限制）**：不允许一开始就离线解析——离线档的入口凭证就是
一份合法检查点，检查点缺失即 fail-fast（这同时是上面冷启动不可支持的技术落点）。

## 1. 已定决策（brainstorming 收敛记录）

| 决策点 | 结论 |
|---|---|
| 布局档范围 | 仅 VastBase（检查点 pgVersion=6） |
| 入口形态 | 库 API（`WalSource.startOffline`）+ 冒烟 Main 配置面双面 |
| 终止语义 | 确定性 EOF（静态目录清单读尽）+ 可选 endLsn 上界；无停滞启发式 |
| 检查点写向 | 同一 state 目录续写，离线/在线双向互通 |
| 装配架构 | 方案 A：`CatalogSynchronizer` 增无库装配档，复用运行期管线零复制 |

## 2. 架构总览

```
WalSource（api 门面）
  ├─ start()          在线档（现状零改动）
  └─ startOffline()   离线档（新）：无 SQL 会话 / 无槽 / 无引导 / 无护栏探测
        │
        ▼ CatalogSynchronizer 无库档（新静态工厂）
        │   复用运行期管线（零复制）：
        │   applyRecord 过滤线 / maybeCheckpoint 节拍 / persistCheckpoint+floor /
        │   snapshot() as-of 视图（ChangeStream 快照与在线路同源）
        ▼ FileWalSource 静态目录档（新形态）
            启动期一次性目录审计（覆盖/连续/tli）→ 按清单序读尽 → 确定性 EOF
```

spike 的手写 harness（~100 行 adapter）在本设计下**全部消失**：装配进同步器后，
检查点节拍、过滤线、快照视图、advanceSlot 空保护（null 连接 no-op）全是现成的。
spike 建议的"SnapshotView 上提公开类"也随之不需要——离线路经 `sync.snapshot()`
取视图，与在线 OG 路同一条接线（`WalSource.changeStreamOrInit` 同构）。

## 3. 组件改动

### 3.1 `StateStore` / `StoredState`（state 包）

- `StoredState` 扩展携带 `pgVersion`（现只在文件 header 校验面，load 后不可得）——
  load 时回读填充，不改 VBWS 文件格式（header 本就含该字段）
- 离线档位判定入口：`pgVersion == 6` → VastBase；其他值由调用方报 ISE

### 3.2 `CatalogSynchronizer`（replay 包）——无库装配档

新静态工厂 `startOffline(...)`（签名供实施计划细化，核心参数：stateDir、walDir、
tables 白名单、可选 endLsn、DML listener）：

1. `StateStore(stateDir, 6)`：`load()` 必须成功——缺失/损坏/拒载 ISE（附"离线无法
   引导，需先在线运行产生检查点"指引）；`pgVersion != 6` ISE（OG=5 / 17 / 18 各附
   明确档位指引）
2. `restoreInto` 灌回字典；`appliedLsn` 种子 = stored lsn（续传严格过滤线，非页对齐）
3. 流起点 = `min(stored, floor)`（dmlFloorLsn 回退语义，Task 11 原样）
4. 布局 = `WalLayoutVVastbase.INSTANCE`（档位从检查点判定，无 relname 探测）
5. `CatalogReplay(layout, TupleDecoder)` healer=null（纯回放禁回查档）；DML 面启用时
   `ChangeStream(sync.snapshot(), null, layout, tables, listener)`——sqlConn=null 即
   probe 禁用（组件既有 null 语义，spike 实证）
6. sink 组合与 startOg 同构：每记录 `applyRecord` → `postApply`(ChangeStream) →
   `maybeCheckpoint`（floor 含新开桶不滞后一条记录）
7. 槽推进：`advanceConnection` null → `advanceSlot` 既有 no-op（零改动）；兼容模式
   护栏：离线不探测（检查点在场即视为当时已过闸）

EOF 收尾：`eofReached()` 由字节源终态驱动 → 门面 Main 循环检测后走既有 `stop()` 路径
（最终检查点落盘，`persistCheckpoint` 复用）。

### 3.3 `FileWalSource`（receive 包）——静态目录档

新增静态档案构造（与 tail 档共存；tail 档行为零改动——在线路不触碰）。三点分叉：

**启动期一次性目录审计**（静态目录是快照，可确定性审计；任一不过即 ISE fail-fast）：
- **段覆盖**：流起点（`min(stored, floor)` 页对齐）所在段必须在场——现 tail 档
  `ceilingKey` 静默跳后继段的语义（等待落盘）在静态档不可接受（静默丢数据）
- **段号连续性**：起点段至末段之间不允许缺口
- **tli 一致性**：目录内全部段名 tli 必须一致；混杂即 ISE 并列出冲突 tli 值（真库
  pg_xlog 混 275 个历史分支段的正解——tail 档不做本校验，同段号不同 tli 的合并
  歧义在静态档必须显式拒绝）

**读尽即 EOF**：按启动期清单序读全段，不轮询增长、无 rename 等待；`.partial` 段的
已接收前沿（末非零字节+1）语义保留（拷出的活跃段可能是预分配 partial）；可选
`endLsn` 上界——交付前沿到达即提前 EOF。EOF 后 tail 线程自然退出并置终态位。

**撕裂尾天然消化**：末段半条记录的字节会被交付但停在 walker carry 里不产出
`WalRecord`——`appliedLsn` 只随完整记录推进，EOF 停机即弃 carry，检查点停在最后
一条完整记录。无专门处理面。

### 3.4 `WalSource`（api 包）与 Main / WalConfigFile（顶层）

- 库 API：`WalSource.startOffline(...)` 返回门面实例——观测面（`consumedLsn()` /
  `metrics()` / `lastCheckpointLsn()`）与在线同构，新增 `eofReached()` 终态位与
  `terminalFailure()` 透传
- Main 新键三枚（`WalConfigFile` 白名单连动扩容）：
  - `vb.wal.mode`：`online`（缺省）| `offline`（非法值构造期 IAE）
  - `vb.wal.offline.dir`：WAL 段目录（offline 模式必填，缺失 ISE）
  - `vb.wal.offline.endLsn`：可选上界（十六进制 LSN 文本，如 `2/071BF288`）
- offline 模式下在线专用键（host/port/db/user/pass/slot/og.*）忽略；state 三键
  （dir/interval/events）与 DML 两键（tables/dml）照常生效
- Main 周期行复用（smoke 统计 + dml 发射计数）；EOF 检测后优雅 close + exit 0；
  启动失败 / 终态死亡 → ERROR + exit 1（与在线同契约）

## 4. 数据流（离线一次运行的完整生命周期）

```
startOffline
  → StateStore.load（pgVersion=6 校验）→ restoreInto（四字典 as-of stored）
  → FileWalSource 静态档构造（目录审计 fail-fast）
  → tail 线程按清单读段 → walker.feed（pageaddr 锚定自纠）
      → applyRecord（≤ appliedLsn 跳过——重放段过滤线）
      → ChangeStream.onRecord（重放段事务幂等重建，at-least-once）
      → maybeCheckpoint（30s/1000 events 节拍，floor 封顶检查点，同目录续写）
  → EOF（清单读尽 / endLsn 到达）
  → stop()：最终检查点落盘（applied = 离线末态）
后续：库修复 → 同 state 目录 start() → stored ≥ P₀ 天然通过（spike 实证衔接面）
```

at-least-once 语义与在线续传一致：重放段（`[min(stored,floor), stored]`）整段重建
发射，与在线基准逐字节全等（spike 两轮实证）。

## 5. 错误处理汇总

| 形态 | 行为 |
|---|---|
| 检查点缺失/损坏/拒载 | 启动期 ISE，附采集指引（先在线运行） |
| 检查点 pgVersion ≠ 6 | 启动期 ISE（OG=5 附"本版仅 VastBase"指引；17/18 同） |
| 起点段缺失 / 段号缺口 / tli 混杂 | 启动期 ISE（目录审计，消息含期望与实况） |
| `vb.wal.mode=offline` 而 `offline.dir` 缺失 | 构造期/启动期 ISE |
| 解析确定性 ISE（非撕裂尾） | tail 线程终态死亡 → `terminalFailure()`（既有契约），Main exit 1 |
| 运行中段文件消失（目录被动） | 静态档无暂态重试——终态失败（与 tail 档分叉：快照目录不容突变） |
| TOAST probe 缺席的 INSERT 空面窄缝 | 宽列渲 `<toast-unchanged>`（fail-safe 已知限制，沿 spike 记档；UPDATE 后像不受影响——OG 系 heap_update 重写 toast 值） |

## 6. 测试面

**离线单测（零 Docker，秒级）**：
- `StateStoreTest` 增：`StoredState.pgVersion` 回读往返
- `FileWalSourceTest` 增静态档用例组：确定性 EOF / 起点段缺失 fail-fast / 段号缺口
  fail-fast / tli 混杂 fail-fast / endLsn 上界提前停 / partial 段前沿 + 撕裂尾容忍
- 装配路径单测：无检查点 ISE / pgVersion=5 与 17 的 ISE 指引 / 手造检查点 + 手造段
  目录的全链冒烟（记录送达 + EOF + 最终检查点落盘断言）

**真库手动档 IT（`-Dvb.it.vastbase=true` 族，新增 `VastOfflineIT`，约 4 用例）**：
在线基准（direct 通道 Main 收流 + SqlRunner 负载）→ 停 → 段目录离线重放 → 对拍：
1. 在线 vs 离线按 xid 交集逐块全等（spike 流程 IT 化——对拍器复用 spike `ParityDiff`
   思路，断言面进 JUnit）
2. 离线→在线回切：两轮启停对拍（离线检查点回灌在线续传）
3. 离线中崩溃再离线：kill 离线进程 → 二次离线续传对拍
4. endLsn 上界：指定中间位点截停 + 检查点落在该位

**段目录获取工具**：spike 的 `WalSegmentRecorder`（DirectWalSource 收流按 LSN 落段）
上移为 `it/vastbase` 测试树工具类——真库不可杀，IT 的"库死亡"语义由"停 Main +
recorder 独立收集的段目录"承载（spike 第二轮形态）。

**验收标准**：离线单测全绿 + 手动档 IT 4 用例全绿 + 既有 517 用例零回归
（在线档路径零改动，回归面收敛在 `FileWalSource` 静态档新增分支与 `StoredState`
字段扩展）。

## 7. 非目标（记档）

- **冷启动离线**（§0 技术根因）；后续方向"半冷启动"：人工提供引导 meta（四 watched
  段号 + 起始 LSN + 覆盖表生命周期的段目录）——wal-bench 语料形态的产品化，前提
  脆弱（窗口完整性无法自证），另立任务再议
- **OG 5.0.0 / vanilla PG 17/18 档离线**：档位校验 ISE 即扩展位；vanilla 档需先把
  PG 档装配的字节源参数化（receiver 路径 → sourceFactory 接缝），工作量大不做
- **离线期槽推进 / 兼容模式护栏**：无库天然跳过（在线回切时槽 restart_lsn ≤ 新
  stored，校验天然通过）
- **段尺寸/页大小参数化**：沿 FileWalSource 既有参数面（16MB/8KB 假设），不新增

## 8. 风险与开放问题

- **TOAST INSERT 空面窄缝**（§5 末行）：probe 缺席下重放切断形态渲哨兵——fail-safe
  方向已知限制，与在线档 v3 方向（per-txn toast 窗口）合并解决
- **真库背景写入**：离线 applied 前沿可能略超在线基准末尾（共享库常态）——IT 对拍
  按 xid 交集归一化吸收（spike 已处理）
- **`vb.wal.mode` 与 `vb.wal.og.source` 的组合语义**：offline 模式下 og.* 全忽略
  （离线字节源只认 `offline.dir`）——文档与配置校验双面钉死，不引入组合歧义
