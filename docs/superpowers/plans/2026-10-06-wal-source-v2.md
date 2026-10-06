# vb-stream-wal-source v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** wal-source v2——用户表 DML 解码（含 TOAST/pglz）、事务组装（含 2PC）、engine 格式复刻输出，以双路对拍 IT 证明与 pgoutput 路径逐字节等价。

**Architecture:** changes 包新组件平级接入 v1 单线程直通（sink 分发：catalog 先应用、DML 后解码）；XactGrouper 即时发射 + BatchAborted 通知；OutputRenderer pending 缓冲保证回滚零输出。零模块依赖（engine 仅 test-scope 供对拍 harness 使用）。

**Tech Stack:** Java 17 + Maven；pgjdbc + slf4j（运行）；junit + logback + testcontainers + engine（test）。

**Spec:** `docs/superpowers/specs/2026-10-06-wal-source-v2-design.md`

## Global Constraints

- 方法名英文 camelCase；import 简名禁 FQN 内联；每函数 javadoc（布局/算法注明源码出处）；slf4j 禁 System.out；TDD 每任务 commit+push
- **输出格式契约源**：engine `ConsoleRenderer`（`vb-stream-engine/src/main/java/org/vastdata/vbstream/ConsoleRenderer.java`）——精确格式（本计划 §契约 已钉，实现以引擎源码为准、变更需双端同步）
- 双路对拍 IT 的 parity 环境必须 `logical_decoding_work_mem` 调高（默认 64MB——**禁驱逐**：WAL 侧无法探测 server 端 STREAMED 形态，kind 只能 NORMAL/TWO_PHASE，驱逐事务的 kind 字段是已知差异面）
- 依赖纪律：wal-source 的 pom **main-scope 永零 engine 依赖**；engine 类只出现在 test-scope（对拍 harness）
- 对拍 IT 需 `--add-opens`（engine 的 Chronicle mmap）——surefire argLine 照 engine pom 抄
- 离线单测零 Docker；IT 用 Testcontainers

## 契约（双路对拍目标格式——实现与断言的唯一样板）

```
TXN-BEGIN xid=773 kind=NORMAL gid=null commitLsn=0x46f1234 commitTs=2026-10-06T05:11:22.123456Z changes=3
  [1] INSERT public.t1 BEFORE=- AFTER=[id=1, s=hello, f=1.5]
  [2] UPDATE public.t1 BEFORE=[id=1] AFTER=[id=1, s=world]
  [3] DELETE public.t1 BEFORE=[id=1] AFTER=-
TXN-END   xid=773
```

精确规则（源：ConsoleRenderer.java:108-157/165-197/282-284、TransactionEvent.java:47、DmlKind.java、TransactionKind.java）：
- 头行：`"TXN-BEGIN xid={} kind={} gid={} commitLsn=0x{} commitTs={} changes={}"`——kind 枚举名（NORMAL/TWO_PHASE；WAL 侧永不 STREAMED）；gid null（非 2PC）或 prepare 记录中的 gid；commitLsn=`Long.toHexString`（无补零）；commitTs=`Instant.toString()`（ISO-8601）；changes=expectedChanges（**aborted 过滤前**的桶记账）
- 变更行：`"  [{}] {}"` 前缀 + `"%s %s BEFORE=%s AFTER=%s%s"`——DML 枚举名；表 `schema.table`；tuple=`List.toString()`（`[name=value, ...]`）；BEFORE/AFTER 无则 `-`；suffix 恒空（WAL 侧无 streamXid）
- 值渲染：`NULL` / `<toast-unchanged>` / 文本截断 64（`s.substring(0,64) + "...(" + s.length() + "B)"`）
- 尾行：`"TXN-END   xid={}"`（TXN-END 后**三个空格**）
- 头/尾/变更行均经 CDC logger（wal-source 侧 logger 名 `org.vastdata.vbstream.walsource.cdc`，对拍 harness 各自捕获后只取消息体比对）

---

## MS1：值面

### Task 1: Pglz 解压移植

**Files:**
- Create: `.../walsource/changes/Pglz.java`
- Test: `.../walsource/changes/PglzTest.java`

**Interfaces:**
- Produces: `static byte[] decompress(byte[] src, int expectedRawSize)`——TOAST chunk 内 pglz 块解压；输入为 pglz 数据段（不含 varlena 头与 rawsize 前缀——那些由 ToastAssembler 剥离）；结果长度 != expectedRawSize 抛 ISE

- [ ] **Step 1: 失败测试**——三用例：①纯字面量流（control byte 0x00 + 8 字面量 + 续）解出原文；②含 match（control 位 1 + u16 匹配码：12-bit offset + 4-bit (len-3)？以实际 pg_lzcompress.c 为准）往返；③截断输入抛 ISE。pglz 块格式锚（转录源 `src/common/pg_lzcompress.c`，REL_18_STABLE——任务内 curl 拉取核对）：control byte 8 个条目 LSB 先行、位 0=字面量(拷 1 字节)、位 1=匹配(u16：低 12 位 offset、高 4 位 length-`PGLZ_MIN_MATCH`+?——**以源码宏 `PGLZ_IS_LITERAL`/匹配编码为准逐位转录，javadoc 注明行号**）
- [ ] **Step 2: 红** → **Step 3: 实现**（~百行，纯函数）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source-v2): pglz 解压移植（pg_lzcompress.c 转录）`

### Task 2: DiskValueRenderer 类型矩阵

**Files:**
- Create: `.../walsource/changes/DiskValueRenderer.java`
- Test: `.../walsource/changes/DiskValueRendererTest.java`

**Interfaces:**
- Consumes: v1 `DecodeKinds`（11 oid）、`TupleDecoder`（磁盘格式解码→Object[]）
- Produces: `static String render(Object decoded, long typeOid)`——解码值 → PG text 表示（pgoutput text 模式对齐）；`static String renderColumn(int attnum, Object decoded, ColumnMeta col)`——`name=value` 形态（对拍 tuple 用，截断 64 规则在 OutputRenderer 层）。渲染规则（对齐 engine `BinaryValueDecoder` 的 text 形态）：
  - bool → `t`/`f`（磁盘 0/1 字节，**不是** true/false）
  - int2/4/8 → 十进制；float4/8 → PG 风格（`1.5`；NaN→`NaN`、Infinity→`Infinity`）
  - numeric → 磁盘 base-10000 变长格式（sign/dscale/weight/digits）→ 十进制字符串（精度无损）
  - text/varchar → 原文；bytea → `\x` + 小写 hex
  - date → 天数（epoch 2000-01-01）→ `yyyy-MM-dd`；time → 微秒 → `HH:mm:ss.ffffff`（尾零**不截**——对齐 PG text 输出？**以双路对拍实测裁定**，实现期用 docker `SELECT ...::text` 钉样本，javadoc 记锚）
  - timestamp/timestamptz → 微秒 → `yyyy-MM-dd HH:mm:ss.ffffff`（tz 带 `+00`？**同样实测钉**）；uuid → 连字符小写
  - 矩阵外 oid → `0x` + hex + WARN 一次（logger 类级）
- `record ColumnMeta(int attnum, String name, long typeOid, boolean dropped)`（dropped → 值渲染 `∅`）

- [ ] **Step 1: 失败测试**——首发 17 类型（bool/int2/int4/int8/float4/float8/numeric/text/varchar/bytea/date/time/timestamp/timestamptz/uuid + NULL 路径 + dropped）逐一断言 text 形态；**时间类样本以 docker 实测钉**（`SELECT '2026-10-06 12:34:56.123456'::timestamp::text` 等——把期望值写死进测试 javadoc 与断言）
- [ ] **Step 2: 红** → **Step 3: 实现**（类型矩阵逐个攻，numeric base-10000 最重）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source-v2): 磁盘格式→PG text 渲染矩阵（首发 17 类型）`

### Task 3: ToastAssembler

**Files:**
- Create: `.../walsource/changes/ToastAssembler.java`、`.../changes/ToastProbe.java`（JDBC 回查接口）
- Test: `.../walsource/changes/ToastAssemblerTest.java`

**Interfaces:**
- Consumes: Task 1 `Pglz`、v1 `TupleDecoder`（external 指针 18B 短 varlena：`[0x01][0x12][rawsize u32][extinfo u32][valueid u32][toastrelid u32]`，spike 发现 20）、v1 `CatalogStores`（toast relfilenode 经 pg_class reltoastrelid 链）
- Produces:
  - `void onChunkRow(long toastRelfilenode, Object[] chunkRow)`——chunk 三列（chunk_id oid/chunk_seq int4/chunk_data bytea）按 valueid 归集 `TreeMap<seq,byte[]>`
  - `String resolveExternal(byte[] src, int off)`——剥 18B 指针：extsize=`extinfo&0x3FFFFFFF`；`extsize < rawsize-4` → pglz（拼全部 chunk 后 `Pglz.decompress`，**压缩 TOAST 的 rawsize/块布局：每 chunk_data = u32 原始长 + pglz 段——以实测钉**）；否则原文拼接（UTF-8）；chunk 缺失 → `ToastProbe.fetch(toastRelid, valueid)` 回查（接口：`Map<Long,byte[]> fetchChunks(long toastOid, long valueid)`，实现 `JdbcToastProbe(Connection)` 查 `pg_toast.pg_toast_<oid>`）；仍缺 → 返回 `toast-unavailable` + WARN
  - `void lz4Guard(byte attcompression, String relName)`——`'l'` 列 WARN 一次；运行期 extinfo 高位方法位 == lz4（`extinfo>>30` 编码以实测钉）→ ISE fail-fast

- [ ] **Step 1: 失败测试**——①未压缩 external：3 chunk 拼装 + extsize 校验；②pglz external：手造压缩块（Task 1 用例复用）解压往返；③chunk 缺失 → stub probe 回查命中；④回查也缺 → `toast-unavailable`；⑤lz4 列 guard
- [ ] **Step 2: 红** → **Step 3: 实现** → **Step 4: 绿** → **Step 5: Commit** `feat(wal-source-v2): TOAST 重组——chunk 采集/external 拼装/pglz/JDBC 回查兜底`

---

## MS2：事务组装

### Task 4: 事件接口 + 表过滤 + TableMeta

**Files:**
- Create: `.../walsource/changes/ChangeOutputListener.java`、`.../changes/TableMeta.java`、`.../changes/TableFilter.java`
- Test: `.../walsource/changes/TableFilterTest.java`

**Interfaces:**
- Produces:
  - `record TableMeta(long relOid, String schema, String table, List<ColumnMeta> columns)`（`columnsOf` 有序含 dropped 占位——v1 `CatalogSnapshot` 直供）
  - `interface ChangeOutputListener { void onBegin(BatchBegin b); void onRow(RowChange r); void onEnd(BatchEnd e); void onAborted(BatchAborted a); }`
  - `record BatchBegin(long xid, boolean twoPhase, String gid, long commitLsn, long endLsn, Instant commitTs, long expectedChanges)`（对拍契约字段齐）
  - `record RowChange(TableMeta table, DmlKind dml, Map<String,Object> before, Map<String,Object> after)`（值=DiskValueRenderer 解码后对象；`DmlKind` 为 wal-source 自有 enum INSERT/UPDATE/DELETE——**不 import engine**）
  - `record BatchEnd(long xid, long emittedChanges)` / `record BatchAborted(long xid)`
  - `class TableFilter(CatalogSnapshot snapshot, Set<String> whitelist)`：`Optional<TableMeta> resolve(long relNodeOrOid)`——relkind 'r'/'p' 且（白名单空或 schema.table 命中）；toast/catalog/索引排除
- 注意：v1 `CatalogSnapshot` 需补 `relkindOf`/`schemaOf`（pg_class relkind/relnamespace→nspname 需 pg_namespace？**裁定：v1 字典只有 pg_attribute/pg_class 两表，schema 名解析需 pg_namespace——字典面扩第三表 pg_namespace（oid→nspname，行模型一行 record）**，catalog 注册制 v1 已备）

- [ ] **Step 1: 失败测试**——TableFilter：relkind 过滤/白名单/miss；TableMeta 组装含 dropped 占位
- [ ] **Step 2-5: 红→实现→绿→Commit** `feat(wal-source-v2): 变更事件契约 + 表过滤 + TableMeta（字典扩 pg_namespace）`

### Task 5: XactGrouper 状态机

**Files:**
- Create: `.../walsource/changes/XactGrouper.java`
- Test: `.../walsource/changes/XactGrouperTest.java`

**Interfaces:**
- Consumes: Task 3/4；v1 `HeapViews`（insert/update/delete/multi-insert）、`WalRecord.effXid()`（spike 发现 25）、HeapOps 的 `XLH_UPDATE_CONTAINS_OLD`/`XLH_DELETE_CONTAINS_OLD`
- Produces: `class XactGrouper(CatalogSnapshot snapshot, DiskValueRenderer r, ToastAssembler t, ChangeOutputListener out)`：
  - `void onRecord(WalRecord rec)`——heap 记录：TableFilter resolve → 解码 new tuple（data 路径优先，FPW image 路径 v1 PageImages）→ TOAST external 经 assembler → before 元组（replica identity 语义：UPDATE/DELETE 的 old tuple 在 main data 尾，spike 发现 12；无则 before 空）→ `out.onRow` **即时发射**（桶记账 expectedChanges++）
  - XACT 记录（v1 `HeapOps.RM_XACT_ID`，opcode 见 spike 发现 25 表）：COMMIT→`onEnd`（emittedChanges=过滤后实付——aborted 子 xid 的行扣减）；ABORT→`onAborted`；PREPARE→挂起（记 gid/xid——**gid 从 prepare 记录 main data 的 gid 字符串（NUL 结尾）解出**）；COMMIT_PREPARED→`onEnd`（kind=TWO_PHASE、gid）；ABORT_PREPARED→`onAborted`；ASSIGNMENT→子 xid 映射兜底
  - **before 元组的 aborted 归属**：aborted 子事务过滤=发射期按"行的来源 xid 已 abort"扣减（v1 spike S8 的 tagged 模型：RowChange 增 originXid 内部记账、输出面不暴露）
  - commit 字段：commitLsn=记录 LSN；endLsn=LSN+MAXALIGN(totLen)；commitTs=main data 的 `xact_time`（i64 微秒 epoch2000 → Instant）；expectedChanges=桶记账
- 测试用 WalBytes 合成记录序列（v1 测试 DSL 扩 XACT 记录 helper：`WalBytes.xactRecord(op, xid, ...)`）

- [ ] **Step 1: 失败测试**——①单事务 INSERT×3→COMMIT：事件序 Begin/3×Row/End、expected/emitted 一致；②交错两桶不串；③ABORT→onAborted 零 End；④子事务 SAVEPOINT：aborted 子行扣减、expected=过滤前；⑤PREPARE 挂起→COMMIT_PREPARED 发射（gid 解出）；⑥ABORT_PREPARED 弃；⑦TOPLEVEL_XID 归并
- [ ] **Step 2-5** → Commit `feat(wal-source-v2): XactGrouper——即时发射/abort 通知/子事务归并/2PC 挂起`

---

## MS3：输出与双路基座

### Task 6: OutputRenderer（pending 缓冲 + 格式复刻）

**Files:**
- Create: `.../walsource/changes/OutputRenderer.java`
- Test: `.../walsource/changes/OutputRendererTest.java`

**Interfaces:**
- Consumes: Task 4 事件接口 + §契约 格式串
- Produces: `class OutputRenderer implements ChangeOutputListener`（构造持 slf4j logger `org.vastdata.vbstream.walsource.cdc`）：
  - `onBegin/onRow` → 追加到 per-xid pending `List<String>`（头行 changes 字段在 Begin 时未知——**pending 攒行文本，头行在 End 时以最终 expectedChanges 组装**）
  - `onEnd` → flush：头行 + 行文本 + 尾行逐条 `CDC.info`；`onAborted` → 丢弃 pending（零输出）
  - 行渲染：`"  [" + seq + "] " + dml + " " + schema + "." + table + " BEFORE=" + tuple + " AFTER=" + tuple`——tuple 用 `List<String>` toString（`name=value` 项，值截断 64 规则在此层）
- [ ] **Step 1: 失败测试**——断言捕获 logger 输出与 §契约 样板**逐字符相等**（含 TXN-END 三空格、`gid=null`、无补零 hex）；abort 零输出；多事务顺序
- [ ] **Step 2-5** → Commit `feat(wal-source-v2): OutputRenderer——pending 缓冲 + engine 格式复刻契约`

### Task 7: ChangeStream 接线 + Main DML 面

**Files:**
- Create: `.../walsource/changes/ChangeStream.java`
- Modify: `.../walsource/api/WalSource.java`（sink 分发）、`.../walsource/Main.java`（DML 输出面）、`.../walsource/replay/CatalogSynchronizer.java`（暴露 sink 分发点或经 WalSource 重组）
- Test: `.../walsource/changes/ChangeStreamTest.java` + 既有回归

**Interfaces:**
- Produces: `class ChangeStream(CatalogSnapshot snapshot, ChangeOutputListener out)`：`void onRecord(WalRecord rec)`——TableFilter/ToastAssembler/XactGrouper 组装编排（内部建三件套）；WalSource.start 后 sink = `rec -> { synchronizer.apply(rec); changeStream.onRecord(rec); }`（顺序保证 as-of）；Main 增 `vb.wal.tables`（白名单，默认空）与 DML 输出开关 `vb.wal.dml`（默认 true——false 回纯 v1 catalog 形态）
- [ ] **Step 1: 失败测试**——ChangeStream 端到端离线：合成记录序列（catalog INS + DML + commit）→ 事件流断言；Main 无新测试（装配），手工冒烟记档
- [ ] **Step 2-5** → Commit `feat(wal-source-v2): ChangeStream 接线 + Main DML 输出面（vb.wal.tables 白名单）`

### Task 8: 双路对拍基建 + 场景 1

**Files:**
- Modify: `vb-stream-wal-source/pom.xml`（**test-scope** 依赖 vb-stream-engine + surefire argLine add-opens 照 engine pom 抄）
- Create: `.../walsource/it/DualPathParityIT.java`、`.../walsource/it/ParityEnv.java`、`.../walsource/it/EnginePathRunner.java`
- Test: 即 DualPathParityIT

**Interfaces:**
- Produces:
  - `ParityEnv`：postgres:18 单例（`wal_level=logical`、`logical_decoding_work_mem=64MB` 禁驱逐、publication `vb_parity` FOR ALL TABLES 重建、两路各用独立 slot）；`List<String> walLines(...)` 捕获 wal-source 输出（logback 临时 appender 挂 `...walsource.cdc`，跑完摘除）
  - `EnginePathRunner`：in-process 跑 engine 管线（`PgReplicationSession` + `TransactionAssembler` + `ConsoleRenderer` 直挂——engine Main 的装配减 JVM），logback appender 挂 `org.vastdata.vbstream.cdc` 捕获；启停 API
  - `DualPathParityIT`：场景 1（基础 DML：单行/多行/交错/回滚零输出/子事务 SAVEPOINT）——每场景：清表 → 两路各自 slot 起流 → 执行 DDL/DML → 停流 → **两路捕获行序列 diff 断言空**（xid/LSN/时间戳字段**逐字节也对齐**？——**裁定：xid 与 commitLsn 双路必然一致（同一 server 同一事务），commitTs 一致（同一提交记录），唯一不稳定面是两路起停窗口造成的事务集合差——对拍断言前先按 xid 求交集，且断言交集非空 + 两侧窗口外事务集合说明**。若 commitTs 纳秒尾差（engine 走 pgoutput commit_ts 毫秒 vs WAL 记录微秒）——实测裁定，必要时归一化并记档）
- [ ] **Step 1: 红**（IT 无基建失败）→ **Step 2: 实现基建 + 场景 1 绿** → **Step 3: Commit** `feat(wal-source-v2): 双路对拍基建 + 场景1 基础 DML 逐字节等价`

### Task 9: 场景 2（类型矩阵）+ 场景 4（DDL-in-txn）

**Files:**
- Test: `DualPathParityIT` 增两场景方法

- [ ] **Step 1**：场景 2——17 类型建表 + 边界值行（负数/NULL/微秒/UTF-8 π/长文本截断 64）INSERT/UPDATE/DELETE 全 DML，双路 diff 空（时间类型渲染差在此暴露并修 DiskValueRenderer）
- [ ] **Step 2**：场景 4——事务内 ADD COLUMN 前后段 as-of 渲染双路 diff 空（catalog 先应用顺序的正确性端到端验证）
- [ ] **Step 3: 绿 + Commit** `feat(wal-source-v2): 对拍场景2 类型矩阵 + 场景4 DDL-in-txn as-of`

---

## MS4：对抗收官

### Task 10: 场景 3（TOAST 三形态）

- [ ] 场景 3：①不可压缩宽值（md5 链）external 重组；②可压缩宽值（repeat 'x'）pglz 解压；③重启后未变列指针 JDBC 回查（wal-source stop → 续传 → UPDATE 不改宽列 → 回查路径命中）。双路 diff 空（engine 侧宽值同为 text 解码——对齐天然）。Commit `feat(wal-source-v2): 对拍场景3 TOAST 三形态（重组/pglz/回查）`

### Task 11: 场景 5（2PC 四形态）

- [ ] 场景 5：PREPARE 挂起期输出零/弃桶/COMMIT PREPARED 发射（gid 双路一致——engine TWO_PHASE gid 同源 prepare 记录）/挂起期重启续传（wal-source 检查点跨 PREPARE 位点）。双路 diff 空。Commit `feat(wal-source-v2): 对拍场景5 2PC 四形态`

### Task 12: 场景 6（生命周期续传对齐）+ 干扰全矩阵

- [ ] 场景 6：wal-source 中途 stop/续传，全程输出 vs engine 全程输出按 xid 交集 diff 空（at-least-once 重复语义两侧一致——续传重发的事务双路都完整出现）
- [ ] 干扰矩阵：场景 1/3 在 Interference 容器变体复跑（autovacuum 拉满 + DDL 噪声）×2 重复。Commit `feat(wal-source-v2): 对拍场景6 生命周期 + 干扰矩阵全绿`

### Task 13: 文档归档 + 全仓收官

- [ ] 模块 CLAUDE.md/README 增 v2 节（changes 包结构/双路对拍契约与执行机制/已知差异面：STREAMED kind、矩阵外类型、大事务输出缓冲 O(事务)）；根 CLAUDE.md 模块行与 Main 冒烟段更新（`vb.wal.tables`/`vb.wal.dml` 键）；v1 spec 已知限制项勾销核对（17 丢 INSERT 在 C1 已修后补验说明）
- [ ] 全仓回归 `mvn clean test`（ConnectPluginIT assembly 前置惯例）→ Commit + push `docs(wal-source-v2): v2 文档归档 + 根索引收官`

---

## Self-Review 记录

- **Spec 覆盖**：§3 结构→T3/T4/T5/T7；§4 Grouper→T5；§5 值面→T1/T2/T3；§6 六场景→T8(1)/T9(2,4)/T10(3)/T11(5)/T12(6+干扰)；§7 MS 对齐。非目标无任务侵入。
- **占位扫描**：时间类型渲染的两处"实测裁定"是设计决策点（非占位）——测试步骤明确要求 docker 钉样本；pglz 匹配编码"以源码宏为准"同性质。
- **类型一致**：`ToastAssembler/Pglz/DiskValueRenderer/ColumnMeta/TableMeta/ChangeOutputListener(BatchBegin/RowChange/BatchEnd/BatchAborted)/TableFilter/XactGrouper/OutputRenderer/ChangeStream/ParityEnv/EnginePathRunner` 跨任务签名逐一核对一致；`DmlKind` 为 wal-source 自有 enum（零 engine import），对拍输出经枚举名对齐。
