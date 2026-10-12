# openGauss 5.0.0 适配 M3 实施计划（A 兼容模式 + pg_partition 第四字典）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** openGauss A 兼容模式 DML 面开门 + pg_partition 第四 watched 字典落地分区捕获（PG 档顺带受益），既有全部面零回归。

**Architecture:** 第四字典与 attr/class/nsp 同权（方案①：bootstrap 种子 + WAL ctid 重放 + stores 容器 + StateStore formatVersion 4 + 身份链进 CatalogSnapshot）；分区行输出=分区名（T0 采样终审）；白名单双匹配（分区名 OR 母表名）；护栏白名单 {'PG','A'}。

**Tech Stack:** Java 17 + Maven；依赖零新增；Testcontainers 复用 `opengauss/opengauss:5.0.0`（m2a 升格 A 档全量场景库 + IT 矩阵兼容档参数化）。

**Spec:** `docs/superpowers/specs/2026-10-13-wal-source-opengauss-m3-design.md`（执行者须同读；spec §0 的范围裁定与 §1 根因背景是本计划的前提）

## Global Constraints

- 方法名英文 camelCase；javadoc 每函数（含测试辅助）；禁止 FQN 内联；slf4j `{}` 占位符。
- **既有全部用例断言零改动**（v2 changes/M1/M2/PG 17/18）——接口扩展只允许 default/档位门控；"M2 分区跳过语义退役"仅指 OG 档新增正常路径，既有测试文件不改断言。
- ASTORE 限定维持：A 档列存表（relcudescrelid≠0）ISE；B/'C' 档库 ISE（附 M4 指引）。
- 常量/偏移标注 OG v5.0.0 源码出处 + 实测锚（M1/M2 方法论）。
- 每任务完成即 commit+push（中文信息 + Co-Authored-By: Claude <noreply@anthropic.com>）。
- 验证编译 `mvn -pl vb-stream-wal-source clean test-compile`；全量 `mvn -pl vb-stream-wal-source clean test`（当前基线 419）。
- 可复用容器 `og-plan`（m2pg[PG 档]/m2a[A 档]/m2b[B 档] 库在）；gsql/hex 防污染口径同前。

---

### Task 0: 实测锚定——分区字典几何 + 基准路命名采样

**Files:**
- Create: `docs/wal-source-opengauss-m3-spike.md`（结论文档——唯一入库产物；探针 throwaway）

**Interfaces:**
- Produces（喂 T1/T2/T4）: ①`pg_partition` 列集与分区 relfilenode 来源列（列名/类型/数据区偏移——第四套 kinds 的推导源）②基准路分区行表名输出形态（`PARTITION_NAME=partition|mother` 裁定行——T4 归一化器分派依据）③TOAST 关系挂分区还是母表（T2 的 ToastAssembler 匹配面依据）④分区 DDL 记录族盘点（CREATE/DROP/EXCHANGE/MERGE/SPLIT 在 m2pg/m2a 各触发一次采 WAL——记录形态清单 + 未转录结构记档）。

- [ ] **Step 1: pg_partition 列集与 relfilenode 出处**

```bash
docker exec -u opengauss og-plan bash -c 'export LD_LIBRARY_PATH=/usr/local/opengauss/lib; \
  gsql -d m2pg -t -A -c "SELECT a.attnum,a.attname,t.typname,a.attlen FROM pg_attribute a \
  JOIN pg_type t ON a.atttypid=t.oid WHERE a.attrelid='\''pg_partition'\''::regclass AND a.attnum>0 ORDER BY a.attnum"'
# 建分区表（若 T5 的已滚）→ INSERT → 采 WAL 对照 pg_partition 行，把 T5 锚的 32809 等值
# 落到列级（哪一列存分区 relfilenode——预期 relfilenode 列或 parentid+伴生结构，以实测为准）
# 同款实测 TOAST：分区表带大字段 → toast 关系的 pg_class 行挂母表 oid 还是分区（reltoastrelid
# 查母表行 + toast chunk 记录的 relfilenode 归属对照）
```

- [ ] **Step 2: 基准路分区行命名采样**——opengauss-jdbc 逻辑流（沿 M2 spike 路②形态）对分区 INSERT 采 R 帧，读表名（预期分区名——用户记忆+vanilla 缺省双背书；反转则 `PARTITION_NAME=mother` + 映射记档）。A 档库（m2a）同采一次（两档一致性验证）。
- [ ] **Step 3: 分区 DDL 记录族盘点**——m2pg/m2a 各执行 CREATE/DROP/EXCHANGE PARTITION（MERGE/SPLIT 若语法支持则加），采 pg_partition 上的 heap 记录形态清单（INS/UPD/DEL 常规 vs 特殊段），未转录结构记档。
- [ ] **Step 4: 写结论 + commit**（仅结论文档，中文信息+尾注）。

---

### Task 1: 第四字典三件套（layout kinds + bootstrap + replay + stores + StateStore v4）

**Files:**
- Modify: `layout/WalLayoutVOpenGauss.java`（或独立 kinds 载体沿 NspRow 先例——实施按 M2 NspRow.kinds() 模式定）+ 新 `replay/CatalogRow.PartitionRow`（沿 NspRow 模式）
- Modify: `replay/CatalogBootstrap.java`（第四表种子）、`replay/CatalogReplay.java`（WatchedLeg.PARTITION 腿）、`replay/CatalogStores.java`（第四容器 + relfilenode 回填 + 序列化）、`state/StateStore.java`（formatVersion 4）
- Test: `layout/OgPartitionKindsTest.java`（或并入）、`replay/OgPartitionDictionaryTest.java`

**Interfaces:**
- Consumes: T0 的①列集/偏移锚与④记录族盘点。
- Produces: `CatalogStores.partitionRows()`（ctid 键控 PartitionRow 容器）+ `pgPartitionRelfilenode()` 回填；`CatalogRow.PartitionRow`（分区 relname/parentid/relfilenode 关键字段——列集按 T0）；`StateStore` v4 检查点（四字典序列化，v3 拒载回落——既有语义）；`CatalogReplay` 第四腿施加（次序在 nsp 之后，固定）。

- [ ] **Step 1: 失败测试**——第四字典离线往返：手造字节（T0 锚几何）种子→重放（INS/UPD/DEL/PRUNE 施加）→ stores 断言；StateStore v4 序列化往返 + v3 拒载回落。
- [ ] **Step 2: 跑测试确认失败**（`mvn -pl vb-stream-wal-source clean test -Dtest='OgPartitionDictionaryTest,StateStoreTest'`）
- [ ] **Step 3: 实现至过**——沿 NspRow/第三表先例整段复制适配；kinds 列集与偏移逐项标注 T0 锚；bootstrap 第四表种子 SQL（同事务）；WatchedLeg.PARTITION（heap 事件 + INPLACE/截断重建按 T0④ 盘点形态，未转录段 ISE+记档）。
- [ ] **Step 4: 全量离线回归 + commit**（`feat(wal-source): pg_partition 第四 watched 字典三件套 + StateStore v4`）

---

### Task 2: 身份链与分区捕获面

**Files:**
- Modify: `api/CatalogSnapshot.java`（`partitionOf(long relfilenode) → Optional<PartitionIdentity{relname, parentid}>` default 方法 + `relOidOf`/`schemaOf` 链挂接——第四字典 miss 后母表链）、`changes/TableFilter.java`（分区行解析 + 白名单双匹配）、`changes/XactGrouper.java`（TableMeta 失效第四调用点 + M2 分区 miss DEBUG 退役）
- Test: `changes/OgPartitionIdentityTest.java`、`changes/TableFilterTest`（OG 用例扩展）

**Interfaces:**
- Consumes: T1 的 `partitionRows()`/PartitionRow；T0 的③TOAST 挂靠（挂分区则 ToastAssembler 的 relfilenode 匹配面扩第四字典——本任务顺带）。
- Produces: 分区行完整捕获链（TableFilter.resolve 四步——spec §3）；TableMeta 身份=分区名（schema.分区名）、列序=母表 columnsOf(parentid)；白名单双匹配（分区名 OR 母表名）；链断 fail-safe 有痕跳过（WARN 一次 + skipped 计数，沿 nsp 先例）。

- [ ] **Step 1: 失败测试**——身份链解析（分区 relfilenode→母表身份/链断 fail-safe/白名单双匹配四形态[分区名命中/母表名命中/双命中/都不中]/TableMeta 分区名身份+母表列序）；TableMeta 失效（第四字典 heap 记录到达即清——EXCHANGE 场景）；TOAST 挂靠按 T0③（挂分区则分区 TOAST 匹配用例）。
- [ ] **Step 2: 确认失败 → Step 3: 实现至过**（vanilla 路径零变化——分区链仅 OG 档进入；V17/V18 的 TableFilter 行为字节级不变锚）。
- [ ] **Step 4: 全量离线回归 + commit**（`feat(wal-source): 分区身份链 + 白名单双匹配 + TableMeta 分区捕获`）

---

### Task 3: 护栏放宽 + T4 遗留记档关闭

**Files:**
- Modify: `api/WalSource.java`（兼容模式白名单 {'PG','A'}；B/'C' ISE 文案 M4 指引）、`changes/OgColumnCompression.java`（三 minor javadoc 记档关闭——numstrMeta attlen 校验/DELTA bytes=0 dropped 语义/deltaBytes≤attlen 注"列存场景待未来项"）
- Test: `api/OgCompatModeGuardTest`（扩展：A 放行两用例）

- [ ] **Step 1: 失败测试**（A 档放行装配 / B·C ISE 文案）→ **Step 2: 实现至过** → **Step 3: javadoc 记档**（不修代码——T4 三 minor 的接线语境已实证消失）→ **Step 4: 全量 + commit**（`feat(wal-source): A 档兼容模式放行 + T4 遗留 minor 记档关闭`）

---

### Task 4: 对拍基座分区归一化 + OgTestEnv A 档升格

**Files:**
- Modify: `it/OgParityBaseline.java`（分区行命名归一化——按 T0②：`PARTITION_NAME=partition` 直通 / `=mother` 映射+记档注释）、`it/OgTestEnv.java`（m2a 升格 A 档全量场景库：表/槽/publication 预建；IT 矩阵兼容档参数化辅助——`CompatMode PG|A` 维度让同套场景双库跑）
- Test: `it/OgParitySmokeIT`（分区冒烟扩展：分区表 INSERT/UPDATE/DELETE 三行双路 diff 空）

**Interfaces:**
- Consumes: T0②裁定行；T2 捕获链。
- Produces: 兼容档参数化跑场景的 IT 基建（T5 消费）。

- [ ] **Step 1: 冒烟失败测试**（分区表三行对拍）→ **Step 2: 归一化 + env 升格实现至过** → **Step 3: commit**

---

### Task 5: A 档全量复验 IT + 分区专项 IT + 护栏 IT

**Files:**
- Test: `it/OgDmlParityIT`（扩展：兼容档参数化——六场景+COPY+TOAST 于 m2a 重跑）、`it/OgPartitionIT.java`（新：分区专项）、`it/OgCompatGuardIT`（扩展：m2b B 档 ISE 已有，加 A 档列存表 ISE）

**Interfaces:**
- Consumes: T4 参数化基建；T2 捕获链。

- [ ] **Step 1: A 档全量**——六场景+COPY 场景+TOAST 全形态在 m2a 重跑（参数化形态，场景代码零复制）；**对拍断言不许放宽**（A 档差异面按排障修——Oracle 系类型若 WAL 存储面有例外实测即现）
- [ ] **Step 2: 分区专项**（PG+A 双档）：range/list 捕获（输出命名对齐基准路）/白名单四形态/流内 CREATE·DROP PARTITION 竞速（as-of）/EXCHANGE 后 TableMeta 失效/UPDATE 跨分区迁移两行形态/基准路对拍 diff 空
- [ ] **Step 3: 护栏**——m2b B 档 ISE（既有）+ A 档列存表 ISE（ORIENTATION=COLUMN 建表触发）
- [ ] **Step 4: 全量回归**（419 基线+新增全绿；M1+M2 零改动）→ **Step 5: commit**

---

### Task 6: 文档同步与收口

**Files:**
- Modify: 模块 `CLAUDE.md`/`README.md`（M3 节：A 档能力/分区捕获[白名单双匹配语义+与 vanilla 分叉记档]/B·C 与列存 ISE/命名对齐结论）、根 `CLAUDE.md`/README 模块行与测试计数、调研报告 M3 段（勘误回填——分区捕获从"记档 M3"到落地的状态行）、M2 spec 勘误注记补一条（"分区跳过语义在 M3 退役"）、`docs/wal-source-opengauss-m2-spike.md` 若有 M3 相关出入回填

- [ ] **Step 1: 文档同步**（surefire 逐类对账——沿 M1/M2 收口方法；技术事实抽查与代码一致）
- [ ] **Step 2: 收口验证**——`mvn -pl vb-stream-wal-source clean test` 全量绿 + 计数一致 → commit

---

## Self-Review 记录

- **Spec 覆盖**：spec §2 组件表→T1-T4 逐行；§5 验收六条→T4/T5/T6；§6 T0-T6 对齐。无遗漏。
- **占位符**：T0 四个锚定物均带可执行命令/方法（实证推导标记）；`PARTITION_NAME` 双分支在 T4 有双落法。无 TBD。
- **类型一致性**：`partitionOf→PartitionIdentity` 于 T2 定义、T5 消费；`partitionRows()` 于 T1 定义、T2 消费；`CompatMode PG|A` 于 T4 定义、T5 消费。
- **风险注记**：①分区 relfilenode 来源列未知（T0 Step 1 显式锚定——若 pg_partition 无直接列，伴生结构如 pg_partition_fn/枚举关系推导须 T0 记档并可能扩 watched 面）；②A 档 Oracle 系类型的 WAL 存储面差异未验（T5 全量复验实测即现，按排障修）；③MERGE/SPLIT PARTITION 语法在 5.0.0 的支持面（T0 Step 3 盘点，不支持则记录族自然收窄）。
