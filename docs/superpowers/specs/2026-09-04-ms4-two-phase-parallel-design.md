# MS4 设计：two_phase 两阶段 IT + parallel 端到端 + R5 存量槽预检

日期：2026-09-04
状态：已批准（用户选定方案 A：预检放 `ensureSlot` + 单个 `TwoPhaseIT` 四场景）

## 1. 背景与范围

MS4 原始定义（`2026-09-01-debezium-connector-postgres-stream-design.md` §10）：two_phase + parallel——挂起池、建槽参数、重启 prepared 续传、parallel 校验；R5。验收：两阶段 IT 全绿。

MS1~MS3.5 已顺带落掉大半：协议层两阶段消息解析（`TwoPhaseParsers`）、组装器挂起池（`currentPrepareTx` + `preparedByGid`，5 种两阶段消息处理器）、建槽参数（幂等建槽带 two_phase + START_REPLICATION 槽选项）、parallel 校验（PARALLEL 档未开 two_phase 启动期 fail-fast）。

**本期实际范围 = 严格三件**（用户拍板）：

1. 两阶段 IT（验收主体）
2. parallel 档端到端验收——以 StreamPrepare 场景承载（用户拍板，不另做全套双档跑）
3. R5：存量槽 two_phase 不匹配的客户端预检（用户拍板客户端预检方案，非包装服务端错、非自动重建槽）

YAGNI 排除项：MBean prepared 挂起数暴露（MS5）、全套 IT 双档跑、自动删槽重建（违背 at-least-once）。

## 2. R5 客户端预检（唯一主代码改动）

`ReplicationSession.ensureSlot` 静态工作体内，SQLState 42710（槽已存在）分支从"WARN 后直接复用"改为**先查后定**：

- 增查 `SELECT two_phase FROM pg_replication_slots WHERE slot_name = ?`：
  - **匹配**（`config.twoPhase()` == 槽属性）→ WARN 复用（文案去掉"否则 start 时由服务端报错"提示，因不匹配已到不了这）
  - **不匹配** → 抛 `IllegalStateException`，文案含：槽名、槽现状、配置期望、迁移指引（需 `DROP SLOT` 重建，会丢确认位点、重启后 PG 从更早位点重发——at-least-once 语义不破）
  - **行不存在**（查询为空）→ 保持 WARN 复用原路径（防 42710 与目录可见性之间的竞态兜底，极端情况留给 start 的服务端报错）
- `IllegalStateException` 属运行时异常，`throws SQLException` 签名不变；沿既有 fail-fast 路径（任务失败、保留槽位，Connect 框架重试）
- 单测：静态工作体的假 `Connection` 替身扩两条分支（匹配复用 / 不匹配抛错），沿用现有离线单测锚定 SQL 契约的范式

选择预检放 `ensureSlot` 而非 `PostgresStreamConnector.validate` 的理由：预检挂在唯一建槽入口上，embedded IT、Connect 运行、未来任何装配方式自动覆盖，无需额外接缝；validate 期需独立 DB 连接生命周期且 embedded IT 走不到 validate 全路径。

新建槽必然带 two_phase，无假阴性窗口（预检只在槽已存在时触发）。

## 3. `TwoPhaseIT` 四场景（验收主体）

挂 `StreamITBase`（embedded engine + 文件 offset 存储 + sink await 骨架），每场景独立槽 + `@AfterEach` 收敛（`stopEngineAndDropSlot`）。翻译源：引擎 `TwoPhaseTransactionTest` 三场景 + 设计文档点名的"重启 prepared 续传"。

1. **prepareThenCommitPrepared**（`slot.streaming=on`、`slot.two.phase=true`）：`PREPARE TRANSACTION` 后挂起期断言 **Kafka 零记录** → `COMMIT PREPARED` → 全量记录按序落 Kafka、计数钉死
2. **prepareThenRollbackPrepared**（on 档）：PREPARE → 挂起 → `ROLLBACK PREPARED` → 全程零记录（弃桶路径）
3. **largePreparedStreamPrepare**（**`slot.streaming=parallel`**）：500 行 × `repeat('z',4096)`（2MB > 64kB work_mem，必触发流式）单事务 `PREPARE` → 以 StreamPrepare 收尾入挂起池 → `COMMIT PREPARED` → 500 条全落 Kafka。**此场景同时验收 parallel 档端到端与流式两阶段路径**（引擎 IT 默认即 PARALLEL 档跑通的翻译）
4. **restartPreparedResume**：PREPARE 挂起期停引擎 → 断言零记录且 offset 未推进 → 重启引擎（two_phase 槽重发未决 prepared 的 `BeginPrepare..Prepare`）→ `COMMIT PREPARED` → 记录补齐、offset 推进越过 CommitPrepared 位点。复用 `RestartSemanticsIT` 的停/起重启骨架（文件 offset 存储）

流式数据构造经验照搬：阈值按 TOAST 压缩后大小记账，`repeat('z',4096)` 可压缩载荷靠总量触发（引擎同款方案已实测 500 行必触发）。

## 4. 错误处理与边界

- 场景 4 的重发窗口：prepared 挂起期间 PG 不推进该槽 confirmed_flush（未输出），重启必然整段重发——组装器 `preparedByGid` 按 gid 匹配幂等吸收，断言不依赖"恰好一次"
- await 超时口径与其他 IT 一致（60s 级，fail 即测试失败，不 flaky 等待）
- `max_prepared_transactions=16` 容器已配，多场景串行不冲突
- 孤儿 prepared 清理：每场景 `@AfterEach` 若有未决 prepared 先 `ROLLBACK PREPARED` 再删槽（防脏状态污染后续场景与同容器其他 IT）

## 5. 验收标准

- `mvn test -pl vb-stream-connector-postgres-stream` 全绿（存量 218 + 新增 4 IT + R5 单测 2 分支）
- CLAUDE.md 连接器段与 IT 清单补 MS4 记档
