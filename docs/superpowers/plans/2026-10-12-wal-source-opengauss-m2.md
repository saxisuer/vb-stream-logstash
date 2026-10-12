# openGauss 5.0.0 适配 M2 实施计划（PG 兼容模式 DML 面）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 openGauss 5.0.0 PG 兼容模式库上交付 DML/changes 面（v2 六场景矩阵全量 + attcmprmode 列压缩 + parttype 分区），M1 面与 PG 17/18 零回归。

**Architecture:** changes 包主干零结构改动（XactGrouper/ToastAssembler/DiskValueRenderer/OutputRenderer 复用），OG 差异经三接缝注入（XACT 解析 / MULTI_INSERT / TableFilter 分区语义），沿 M1 的 layout 档位门控纪律。验收形态由 T0 spike 裁定（OG 逻辑解码路双路对拍 vs JDBC 实查断言式）。

**Tech Stack:** Java 17 + Maven；依赖零新增；Testcontainers 复用 `opengauss/opengauss:5.0.0`（扩展建 PG/A/B 兼容库）。

**Spec:** `docs/superpowers/specs/2026-10-12-wal-source-opengauss-m2-design.md`（执行者须同读）

## Global Constraints

- 方法名英文 camelCase；javadoc 每函数（含测试辅助）；禁止 FQN 内联；slf4j `{}` 占位符。
- **PG 17/18 与 M1 面零回归**：v2 changes 包现有用例断言零改动；M1 全部 OG IT 零改动；接口扩展只允许 default/档位门控。
- 范围 fail-fast：非 PG 兼容档库 ISE；未转录分区路由形态 ISE；attcmprmode 解压失败 ISE；MULTI_INSERT 布局不自洽 ISE；XACT 字段越界/opcode 未注册 ISE（附 LSN）。
- 常量/偏移标注 OG v5.0.0 源码出处（本地克隆 `/tmp/og-src` 或 gitee v5.0.0）+ 语料实测锚——沿 M1 方法论。
- 每任务完成即 commit+push（中文信息 + Co-Authored-By: Claude 尾注）。
- 验证编译 `mvn -pl vb-stream-wal-source clean test-compile`；全量 `mvn -pl vb-stream-wal-source clean test`（当前基线 350 用例）。
- 可复用容器 `og-plan`（opengauss:5.0.0 端口 15700）；gsql 用法与 hex 防污染口径同 M1（grep 纯 hex 行）。

---

### Task 0: Spike——openGauss 逻辑解码路可用性探测（裁定验收形态）

**Files:**
- Create: `docs/wal-source-opengauss-m2-spike.md`（结论文档——三路探测记录 + 选型 + 输出样例；**唯一入库产物**）
- 探针代码 throwaway（/tmp 下，不入 git）

**Interfaces:**
- Produces: 验收形态裁定（写进结论文档首行）：`PARITY=LOGICAL`（双路对拍，附路选型与输出格式样例）或 `PARITY=ASSERT`（断言式降级）；后续 T6/T7 依此分派。

- [ ] **Step 1: 建 PG 兼容库 + 基础面探测**

```bash
docker exec -u opengauss og-plan bash -c 'export LD_LIBRARY_PATH=/usr/local/opengauss/lib; \
  gsql -d postgres -c "CREATE DATABASE m2pg DBCOMPATIBILITY '\''PG'\''"'
# 探测逻辑解码面：SHOW wal_level; SELECT count(*) FROM pg_proc WHERE proname LIKE '%logical%slot%';
# 确认 pg_create_logical_replication_slot 与插件目录（pgoutput 在不在？test_decoding？）
```

- [ ] **Step 2: 三路探测**（每路：起逻辑槽 → 写 DML → 收输出 → 记录形态/坑）
  ①`pg_recvlogical`（容器内二进制，`--start -f -` 形态）②opengauss-jdbc `logical()` API
  （`jdbc:opengauss:...?replication=database` + getReplicationAPI().replicationStream().logical()——
  逻辑路是 AM_WAL_DB_SENDER 正确路由，理论可行）③vanilla pgjdbc logical（对照组）。
  每路记录：能否建槽/起流/收到变更/输出格式（插件原生格式样例 hex+可读形态）/断开行为。
- [ ] **Step 3: 写结论**——选型（优先 ②in-process 对拍器最贴 v2 形态；①次之；③预期不行）+
  归一化器设计输入（输出格式样例：事务边界/行/值形态怎么对到 v2 TXN 块）；不可行写
  `PARITY=ASSERT` 与断言式基座设计要点。commit（仅结论文档）。

---

### Task 1: 启动期兼容模式护栏 + 撤销 M1 WARN 降级

**Files:**
- Modify: `api/WalSource.java`（`startOpenGauss`：sql_compatibility 探测 + 非 PG 档 ISE；`ogCatalogOnly` 降级位退役——DML listener 正常装配）
- Test: `api/OgWalSourceConfigTest`（扩展）+ 新 `api/OgCompatModeGuardTest`

**Interfaces:**
- Consumes: M1 的 `startOpenGauss`/`ogCatalogOnly` 位与 DML listener 注入路径。
- Produces: OG 档 DML 装配与 PG 17/18 路同构；`isPgCompatMode(Connection)` 探测方法（探测 SQL 以 T0/本任务实测钉死——候选 `SELECT datcompatibility FROM pg_database WHERE datname = current_database()`，返回 'A'/'B'/'C'/'PG' 形态实测定）。

- [ ] **Step 1: 实测探测 SQL**（og-plan 的 m2pg 库 + postgres 库——注意 postgres 缺省库的兼容档是什么，**M1 的容器在缺省库上跑过全部 IT——若缺省库非 PG 档，M1 IT 会因新护栏翻红，须实测后定护栏白名单语义**）
- [ ] **Step 2: 失败测试**——伪 Connection 返回非 PG 档 → startOpenGauss ISE 含档位与 M3/M4 指引；PG 档 → ChangeStream 装配（ogCatalogOnly 退役后 M1 的 WARN 降级测试反转为装配断言）
- [ ] **Step 3: 实现至过** + M1 OG IT 全量回归（护栏不能误伤）
- [ ] **Step 4: Commit**（`feat(wal-source): OG 档兼容模式护栏 + DML listener 装配撤销降级`）

---

### Task 2: OgXactParser 转录 + 语料锚

**Files:**
- Create: `layout/OgXactView.java`（或经现有 XACT 视图类 OG 分派——先读 v2 的 XACT 解析入口：`changes/XactGrouper` 内 XACT 记录解析处，产物形态对齐它）
- Modify: 对应解析入口（OG 格式分派注入）
- Test: `layout/OgXactViewTest.java`（手造 + 语料）

**Interfaces:**
- Produces: OG 五 opcode（COMMIT/ABORT/PREPARE/COMMIT_PREPARED/ABORT_PREPARED——HTUP/xact 转录，**opcode 值与 vanilla 可能不同，源码+语料双锚**）→ 现有 XactGrouper 状态机消费的事件（发射/弃桶/挂起 + 子事务归并 TOPLEVEL_XID/ASSIGNMENT）。

- [ ] **Step 1: 源码转录**（`/tmp/og-src`：`grep -rn "xl_xact_commit" src/include/ | head` 定位头文件——OG 把 xact 结构挪进了 `src/include/access/rmgr/` 或留在 htup.h，以 grep 为准；转录四记录族字段布局 + 五 opcode + 子事务段格式）
- [ ] **Step 2: 语料采集**——og-plan/m2pg 写入序列（普通提交/回滚/SAVEPOINT 子事务/2PC 四形态——`PREPARE TRANSACTION 'g'` 可用性顺带实测，不可用则 2PC 场景记档降级）+ hex 采锚（M1 方法）
- [ ] **Step 3: 失败测试**（手造四族 + 语料锚断言：xid/子事务列表/opcode 分派/2PC gid）→ 实现至过 → XactGrouper 离线回归（v2 20 用例零改动绿）
- [ ] **Step 4: Commit**

---

### Task 3: MULTI_INSERT OG 分支（M1 ISE 解除）

**Files:**
- Modify: `layout/HeapViews.java`（MultiInsertView OG 解析分支）
- Test: 扩展 `OgWalRecordParserTest` + `OgPruneViewTest` 风格新用例

**Interfaces:**
- Consumes: M1 的 MULTI_INSERT ISE 位与 Task 8 候选锚（rmid=9/info 0x30 ×15，COPY 200 行场景语料在 M1 语料容器）。
- Produces: MultiInsertView OG 产物（ntuples/offsets/entry 走读），XactGrouper 逐 entry 展开零改动消费。

- [ ] **Step 1: 语料补锚**——m2pg 库 COPY 大批量（JDBC CopyManager STDIN 形态——M1 Task 8 已验证服务端文件 COPY 被 OG 禁）采 MULTI_INSERT 记录字节 + INIT_PAGE/跨页 entry 变体
- [ ] **Step 2: 源码转录**（xl_heap_multi_insert OG 布局——htup.h；与 vanilla 差异：是否带 CSN/分区扩展位）
- [ ] **Step 3: 失败测试**（语料锚解析断言 + M1 的 ISE 测试**反转为解析断言**）→ 实现至过 → Commit

---

### Task 4: OgColumnCompression（attcmprmode 列压缩解压）

**Files:**
- Create: `changes/OgColumnCompression.java`
- Test: `changes/OgColumnCompressionTest.java`（黄金向量 + 失败负例）

**Interfaces:**
- Consumes: varlena 接缝（`TupleDecoder.VarlenaResolver` 同族——挂接点沿 ToastAssembler 的 pglz/lz4 分派面旁路或前置，实施时按 v2 接缝现状最小侵入定并 javadoc）。
- Produces: attcmprmode 压缩值的解压面（字节→原值）；解压失败 ISE（区别于 TOAST 值面契约——列压缩失败=格式错位硬失败）。

- [ ] **Step 1: 源码定位**（`/tmp/og-src`：`grep -rn "attcmprmode" src/include/ src/gausskernel/ | head`——定位压缩算法实现文件与格式；OG 列压缩的算法族（lz 变体？）与档位语义转录）
- [ ] **Step 2: 实测采锚**——m2pg 建 `COMPRESSION` 列表（各档），INSERT 大值（>阈值触发压缩）采 WAL 字节 + JDBC 原值 → 黄金向量对
- [ ] **Step 3: 失败测试**（黄金向量解压断言 + 损坏字节 ISE 负例）→ 实现至过 → Commit
- [ ] 注记：若某档算法转录成本超预期（如闭源/复杂），该档 ISE + 文档列明已支持档（spec §4 允许），报告裁定过程

---

### Task 5: 分区路由实测 + TableFilter PG 档语义

**Files:**
- Modify: `changes/TableFilter.java`（parttype 判别 + 白名单物理表匹配）
- Test: `changes/TableFilterTest`（扩展 OG 用例）

**Interfaces:**
- Produces: OG 档表过滤规则——relkind 'r' 且 parttype∈{'n','p'}；分区子表（行物理所在）独立匹配白名单，母表不承载行不在匹配面；**路由规则实测锚定**（INSERT 落子表 relfilenode 则规则成立，落母表则反转——spec §3）。

- [ ] **Step 1: 路由实测**——m2pg 建 range 分区表 + INSERT，采 WAL 看 heap 记录的 relfilenode 落母表还是子表；list 同测。结论写 javadoc + 测试锚。
- [ ] **Step 2: 失败测试**（parttype 判别/白名单子表匹配/母表放行语义/默认全放行母子都过）→ 实现至过（经 layout 档位门控，V17/V18 零变化）→ Commit

---

### Task 6: 对拍路装配（按 T0 选型）+ OgTestEnv 扩展

**Files:**
- Modify: `it/OgTestEnv.java`（建 m2pg/m2a/m2b 三库 + 按选型装配对拍基准路）
- Create: `it/OgParityBaseline.java`（路 A：逻辑解码基准 runner——opengauss-jdbc in-process 或 pg_recvlogical sidecar，按 T0；路 B：JDBC 实查断言基座——已知 DML 的预期构造器）
- Test: `it/OgParitySmokeIT.java`（单场景冒烟——INSERT/UPDATE/DELETE 三行，验证对拍 machinery 本身）

**Interfaces:**
- Consumes: T0 选型结论（`docs/wal-source-opengauss-m2-spike.md` 首行）。
- Produces: `OgParityBaseline`（供 T7 全场景复用的基准路接口：写 DML → 双路捕获 → 归一化 → diff）。

- [ ] **Step 1: 按 T0 实现基准路**（路 A 含归一化器：OG 逻辑输出格式 → v2 TXN 块对齐面，T0 样例为据）
- [ ] **Step 2: 冒烟 IT 过**（三行 DML 双路/diff 空 或 断言式三行全对）→ Commit

---

### Task 7: v2 六场景 IT 全量 + OG 特有面 + 护栏

**Files:**
- Test: `it/OgDmlParityIT.java`（或断言式 `OgDmlAssertionIT`——按 T0/T6 命名）+ `it/OgDmlOgSpecificIT.java` + `it/OgCompatGuardIT.java`

**Interfaces:**
- Consumes: T6 的 `OgParityBaseline`/断言基座；T2-T5 的解析/过滤/解压面。

- [ ] **Step 1: v2 六场景**（沿 `DualPathParityIT` 场景配方逐个翻译到 m2pg 库：基础 DML 六形态/17 类型边界值/TOAST 三形态+重启 unchanged/DDL-in-txn as-of/2PC 四形态[PREPARE 可用性以 T2 实测为准，不可用该场景记档跳过]/中途停续）——**对拍断言不许放宽**，错位按排障流程修接缝
- [ ] **Step 2: OG 特有面 IT**——attcmprmode 各支持档值往返/分区 range+list 捕获/白名单母-子两形态
- [ ] **Step 3: 护栏 IT**——m2a/m2b 库 fail-fast 各一用例
- [ ] **Step 4: 全量回归**（350 基线 + 新增全绿；M1 IT 零改动）→ Commit

---

### Task 8: 文档同步与收口

**Files:**
- Modify: 模块 `CLAUDE.md`/`README.md`（M2 节：DML 面 OG 档能力矩阵/兼容模式限制[PG 档 only，A/B ISE]/attcmprmode 支持档/分区语义/对拍形态与结论）、根 `CLAUDE.md`/README 模块行与测试计数、调研报告 M2 落地状态与勘误回填、M1 已知限制清单勾销（MULTI_INSERT/`ogCatalogOnly` 降级两条）

- [ ] **Step 1: 文档同步**（测试计数与 surefire 对账；技术事实抽查与代码一致——M1 Task 10 的对账方法复用）
- [ ] **Step 2: 收口验证**——`mvn -pl vb-stream-wal-source clean test` 全量绿 + 计数一致 → Commit

---

## Self-Review 记录

- **Spec 覆盖**：spec §2 组件表→T1-T6 逐行；§5 验收六条→T6/T7/T8；§6 T0-T8 与本计划对齐（T0 spike 前置裁定验收形态=spec §0 C 路线的落点）。无遗漏。
- **占位符**：探测 SQL/转录定位均给出候选与 grep 命令（实证推导标记，沿 M1 已验证方法）；T0 选型分支 T6/T7 均有双路径落法。无 TBD。
- **类型一致性**：`OgParityBaseline` 于 T6 定义、T7 消费；`isPgCompatMode` 于 T1 定义、护栏测试消费；MULTI_INSERT/XACT 产物均指向"现有状态机零改动消费"的既有接缝（v2 XactGrouper/MultiInsertView 消费面）。
- **风险注记**：①postgres 缺省库兼容档未知——T1 Step 1 显式处理（M1 IT 依赖缺省库，护栏语义须实测后定）；②PREPARE TRANSACTION 可用性未验——T2 实测，不可用则 2PC 场景记档降级（spec §4 允许）。
