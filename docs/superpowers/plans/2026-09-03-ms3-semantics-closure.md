# MS3 语义闭环 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐流式 CDC 语义闭环:Truncate 发射(vanilla 对齐)、aborted 子事务剔除/同事务 DDL asOf/半事务停机与重启三情况的端到端 IT、数组列已知限制钉子。

**Architecture:** MS2 已交付 FrontierCap/ReaderUnblocked/重启冒烟 IT 与全部管道;本里程碑一个发射特性(Truncate,`skipped.operations` 门控照 vanilla)+ 三组语义 IT(翻译引擎 `DecoupledPipelineTest` 场景)+ 限制记档。LogicalMsg 发射**延期**(需 messages=true 槽选项 + 非事务消息即时前沿推进点,动 End 锚定模型,单独设计)。

**Tech Stack:** 既有栈(Debezium 3.6.1.Final/connector IT 基建/Testcontainers postgres:18)。

**Spec:** `docs/superpowers/specs/2026-09-01-debezium-connector-postgres-stream-design.md` §5.6(重启三情况)、§6(D7 停机语义)、§10 MS3。

## Global Constraints

- 零 `import org.vastdata.vbstream`;引擎源码只读参照;javadoc 全覆盖;slf4j 禁 System.out;commit 末尾 `Co-Authored-By: Claude <noreply@anthropic.com>`;验证 `clean`;IT 需 Docker
- **Truncate 照 vanilla**(3.6.1 sources 实证):每表一条记录走普通 `dispatchDataChangeEvent`;key=null、op="t"(truncate envelope,无 before/after、无 tombstone、普通 data topic);**门控 = `skipped.operations`(继承自 CommonConnectorConfig,默认 "t" = 跳过——我们默认不发,`skipped.operations=none` 才发)**;被 table-filter 过滤的表剔除、全过滤零发射;CASCADE/RESTART_IDENTITY 选项**丢弃**(vanilla 同款,记档我们 TruncateChange 保留了此信息属超集)
- **LogicalMsg 延期记档**:不加 `messages=true` 槽选项(vanilla 仅 PG14+ 加;加了不消费白白下发);延期理由与设计要点(专属 topic `<logical.name>.message`/prefix 过滤/非事务消息即时前沿推进点与 End 锚定的交互)写入 known-limitations
- **数组/未知类型列维持 fail-fast**:vanilla 依赖发射线程活连接(`PgArray` 惰性解析)且失败 WARN+null 静默丢值;我们 R3 约束下 fail-fast 更安全——口径升级"已知限制"并修正 `TypeRegistryColumnValueMapper` javadoc 与 vanilla 事实的出入(vanilla 未知类型是照发或**静默 null 不抛**)
- 流式数据构造照旧:16KB 不可压缩载荷、全局 rb->size=64kB 阈值、行间跨秒

**参照速查**:**ENG-IT** = `vb-stream-engine/src/test/java/org/vastdata/vbstream/it`(DecoupledPipelineTest 场景骨架);**CONN-IT** = `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it`(StreamPgTestEnv/StreamITBase/既有四 IT);**DBZ361** = sources jar 解包(参照 `/tmp/dbz361-pg` 若存,否则重新解包)。

---

### Task 1: Truncate 发射

**Files:**
- Modify: `.../stream/DispatcherTransactionListener.java`(TruncateChange 分支:现 DEBUG 跳过 → 发射)
- Modify: `.../stream/RowChangeEmitter.java`(或新建 `TruncateEmitter`):emitTruncateRecord = `tableSchema.getEnvelopeSchema().truncate(sourceInfo, clock)` → `receiver.changeRecord(partition, tableSchema, TRUNCATE, null, envelope, offset, null)`(key=null)
- Test: `DispatcherTransactionListenerTest` 补 2 用例(默认 skipped.operations 含 t → 零 dispatch;skipped.operations=none → 逐表 dispatch、每表一个 emitter);`CONN-IT/EndToEndStreamedTxIT` 或新建 `TruncateIT` 补 1 用例(TRUNCATE 表 → `skipped.operations=none` 下收到 op="t" 记录 key=null;默认配置零记录)

**Interfaces:**
- Consumes: `TruncateChange(relations, options, streamXid, seq)`(既有);`config.getSkippedOperations()`(CommonConnectorConfig 继承,无需新 Field)
- Produces: Truncate 发射语义(后续里程碑无依赖,独立交付)

**实现要点**:listener 的 TruncateChange 分支——`if (config.getSkippedOperations().contains(Envelope.Operation.TRUNCATE)) { DEBUG; return; }`;否则逐 relation 经 `resolver.resolve(oid, seq)` 取 asOf Table + 版本安装(同 RowChange 路径)→ `dispatcher.dispatchDataChangeEvent(partition, tableId, truncateEmitter)`;options 丢弃(vanilla 对齐,javadoc 记我们保留超集);多表 = 多条记录(dispatcher 内部 filter 自动剔过滤表)。

- [ ] TDD 五步(单测红→绿→IT)→ commit `feat: MS3 Truncate 发射——skipped.operations 门控照 vanilla(默认跳过,none 才发;每表一条 key=null op=t;选项位丢弃对齐)`

---

### Task 2: aborted 子事务剔除端到端 IT

**Files:**
- Create: `CONN-IT/StreamAbortFilterIT.java`

**Interfaces:**
- Consumes: 既有 IT 基建(StreamPgTestEnv/StreamITBase)
- Produces: MS3 验收项之一

**场景**(翻译 ENG-IT/DecoupledPipelineTest 场景①的 abort 部分,L122-204):单连接流式大事务(6 行×16KB 不可压缩,行间 sleep)——**第 6 行前 SAVEPOINT sp1 → 插 3 行(同样 16KB,确保子事务也流式)→ ROLLBACK TO sp1 → 再插 1 行 → COMMIT**;断言:topic 收到 7 条 INSERT(6+1,被回滚 3 行**不存在**);事务元数据 BEGIN/END 齐;`event_count`/数据计数不含被回滚行。Javadoc 论证流式构造(阈值 64kB 全局,跨秒分批)与 'A'(StreamAbort)消息必然到达(低阈值下总字节数保证)。

- [ ] IT 先红后绿 → commit `feat: MS3 aborted 子事务剔除端到端 IT——SAVEPOINT 回滚行不进 Kafka,存活行完整`

---

### Task 3: 同事务 DDL asOf IT

**Files:**
- Create: `CONN-IT/InTxnDdlAsOfIT.java`

**场景**(翻译 DecoupledPipelineTest 场景②,L225-290):表 t(id int PK, c1 text);单连接流式大事务——插 3 行(2 列形态,16KB 载荷)→ `ALTER TABLE t ADD COLUMN c2 text` → 插 2 行(3 列形态)→ COMMIT;断言:5 条 INSERT;**前 3 条 value schema 2 列、后 2 条 3 列**(asOf 版本渲染——"按最新版渲染必红");c2 值正确;事务元数据单事务块。

**实现要点**:DDL 触发新 'R'(同 oid 新版本)→ registry 追加 → 桶内前后段各自 asOf;版本安装路径(listener 的 tableFor 比较)在 DDL 边界各装一次。若 'R' 未在流中到达(ALTER 在事务外的形态)不属本场景——javadoc 写明场景边界。

- [ ] IT 先红后绿 → commit `feat: MS3 同事务 DDL asOf IT——前后段各按变更时刻表结构渲染,列数分界正确`

---

### Task 4: 半事务停机与重启三情况 IT

**Files:**
- Create: `CONN-IT/RestartSemanticsIT.java`(两到三个用例)

**场景**(spec §5.6 三情况;MS2 场景③只覆盖了"offset 落后→重发"一角):
1. **半事务停机**(D7 主验收):流式大事务进行中(部分流段已发、未 COMMIT)→ **不排干停机**(`stopConnector()` 走 D7 路径)→ 断言:该事务零记录或仅见部分(无 END 即下游可过滤)→ **重启 engine(offset 文件保留)** → COMMIT 事务(若未提交)或依赖重发 → 断言:完整事务(BEGIN+全行+END)最终达,**重复头行允许**(at-least-once 文档口径,Set 断言)
2. **Connect offset 落后于前沿**:停机→PG 侧已有更多已输出事务→重启→已输出事务重发→断言并集语义(重复不去重)
3. **无缝续传**(常态):停机→重启→新事务正常输出、旧的不重复(槽 confirmed_flush 已推进过停机点)

Javadoc 写清三情况各自的机制依据(前沿封顶/confirmed_flush candidate/offset 滞后)。

- [ ] IT 先红后绿 → commit `feat: MS3 重启三情况 IT——半事务停机 D7→重发补齐 + offset 落后重复(并集)+ 无缝续传`

---

### Task 5: 数组/未知类型已知限制钉子

**Files:**
- Modify: `.../stream/TypeRegistryColumnValueMapper.java`(javadoc:未知类型 vanilla 实为照发或静默 null 不抛——修正"抛"的错误表述;数组列 fail-fast 口径升级为"已知限制")
- Create/Modify: known-limitations 记档(并入 `docs/superpowers/specs/2026-09-02-ms2-r1-r3-audit.md` 新增"已知限制与延期"节,或独立小文档)
- Test: `TypeRegistryColumnValueMapperTest` 补 1 用例(数组类型经真供给器形态 → DebeziumException 穿透 fail-fast,不静默 null)

**记档内容**:数组列(vanilla 依赖发射线程活连接 PgArray 惰性解析、失败 WARN+null;可行支持路径=reader 旁路代转或 consumer 期短连接,破坏 R1/R3,真需求出现再议);LogicalMsg 延期(槽选项/专属 topic/即时前沿推进点设计要点);Truncate 选项位超集(我们保留 CASCADE/RESTART_IDENTITY 信息但发射丢弃,对齐 vanilla)。

- [ ] 单测红→绿 + 文档 → commit `docs: MS3 已知限制记档——数组列 fail-fast 口径/LogicalMsg 延期设计要点/Truncate 选项位超集 + javadoc 勘误`

---

### Task 6: 收尾——文档与全量验收

**Files:**
- Modify: 根 `CLAUDE.md` connector 行(追加 MS3 内容);`.../stream/CLAUDE.md`(若已存在模块级,补 Truncate/重启语义节)
- 全量验收

- [ ] `mvn clean test` 三段 SUCCESS(引擎 177 + connector 191+新增;IT 需 Docker)
- [ ] commit `docs: MS3 收官——CLAUDE.md 模块行更新`

---

## 验收汇总(对照 spec §10 MS3)

- [x] aborted 过滤端到端(SAVEPOINT 回滚行不进 Kafka)
- [x] DDL asOf(同事务前后段列数分界正确)
- [x] 重启三情况(半事务 D7/offset 落后重复/无缝续传)
- [x] Truncate 发射(`skipped.operations` 门控,默认跳过对齐 vanilla)
- [x] 数组/未知类型限制记档 + LogicalMsg 延期记档
- [x] 全量绿;零引擎 import

## 备注

- FrontierCap/ReaderUnblocked 已在 MS2 交付(spec §10 MS3 项的既成部分)
- LogicalMsg 发射延期至独立里程碑(设计要点已记 Task 5)
