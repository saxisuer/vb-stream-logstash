# vb-stream-wal-source v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 生产可用的"伪备库"模块：物理复制流接收 WAL + 自维护 catalog 字典（pg_attribute/pg_class ctid 重放）+ 状态持久化，PG 17/18 双版本。

**Architecture:** 单线程直通（接收→解析→重放→周期落盘），五组件接口化（WalLayout/WalStreamReceiver/WalRecordParser/CatalogSynchronizer/StateStore），输出契约为 v2 engine 汇入预留。布局常量按版本 descriptor 化，全部从 REL_STABLE 源码转录。

**Tech Stack:** Java 17 + Maven 多模块；运行依赖仅 pgjdbc + slf4j-api；测试 JUnit 6 + Testcontainers（postgres:17/18）。

**Spec:** `docs/superpowers/specs/2026-10-05-wal-source-module-design.md`（含 27 条 spike 发现 `docs/wal-direct-decode-spike.md`；可运行参照实现 `spike/wal-parse/WalParseSpike.java`）

## Global Constraints

- 方法名英文 camelCase；类型引用一律 import 简名（禁 FQN 内联）；每个函数 javadoc（布局常量须注明源码出处文件）
- 日志一律 slf4j（`LOG.info/warn/error` + `{}` 占位）；主代码与测试禁 System.out（冒烟 Main 的用户面输出除外，走 logger）
- 模块运行依赖仅 `org.postgresql:postgresql` + `slf4j-api`；测试依赖 junit-jupiter + logback-classic + testcontainers
- 未知 WAL 布局/版本一律 fail-fast（IllegalStateException），不做试探解析
- 每任务完成即 commit + push；集成测试需本机 Docker
- 离线单测零 Docker 秒级；IT 用 Testcontainers 自管容器（禁依赖 src/docker 常驻实例）
- 所有布局数值在 javadoc 标注 `// <源头文件>`（如 `// xlog_internal.h (REL_18_STABLE)`）

---

## MS1：模块骨架 + 布局与解析层

### Task 1: 模块骨架

**Files:**
- Modify: `pom.xml`（`<modules>` 尾部加 `<module>vb-stream-wal-source</module>`）
- Create: `vb-stream-wal-source/pom.xml`
- Create: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/package-info.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/ModuleBuildTest.java`

**Interfaces:**
- Produces: Maven 模块 `org.vastdata:vb-stream-wal-source`，包根 `org.vastdata.vbstream.walsource`

- [ ] **Step 1: 写失败测试**（验证模块接入聚合构建）

```java
package org.vastdata.vbstream.walsource;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** 模块接入冒烟：包存在且构建连通（真实断言留给后续任务，此处钉 groupId 约定）。 */
class ModuleBuildTest {
    @Test
    void modulePackageIsRooted() {
        assertEquals("org.vastdata.vbstream.walsource", ModuleBuildTest.class.getPackageName());
    }
}
```

- [ ] **Step 2: 跑测试验证失败**

Run: `mvn test -pl vb-stream-wal-source`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 建模块**——pom 完全对齐 `vb-stream-file-format/pom.xml` 模板（parent + artifactId + 注释定位），依赖改为：

```xml
<dependencies>
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>${postgresql.version}</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>${slf4j-api.version}</version>
    </dependency>
    <dependency>
        <groupId>ch.qos.logback</groupId>
        <artifactId>logback-classic</artifactId>
        <version>1.5.18</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>${junit-jupiter.version}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers-postgresql</artifactId>
        <version>${testcontainers.version}</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

`package-info.java` 一行 javadoc：模块定位（spec §4）。parent `<modules>` 加行。

- [ ] **Step 4: 全仓回归**：`mvn test -q`（新模块测试绿、其余模块不受扰）
- [ ] **Step 5: Commit**：`feat(wal-source): 模块骨架——聚合接入 + 最小依赖面`

---

### Task 2: Lsn 工具 + WalLayout 接口 + WalLayoutV18

**Files:**
- Create: `.../walsource/layout/Lsn.java`
- Create: `.../walsource/layout/WalLayout.java`
- Create: `.../walsource/layout/WalLayoutV18.java`
- Create: `.../walsource/layout/WalLayouts.java`（版本分发）
- Test: `.../walsource/layout/WalLayoutV18Test.java`

**Interfaces:**
- Produces:
  - `record LsnUtil` 不可实例；`static String format(long)`（`%X/%08X`）、`static long parse(String)`
  - `interface WalLayout`：`int pageMagic(); int walBlockSize(); int shortPageHeaderSize(); int longPageHeaderSize(); int recordHeaderSize(); int sizeOfHeapUpdate(); int sizeOfHeapDelete(); int sizeOfHeapInplace(); String[] pgAttributeKinds(); String[] pgClassKinds(); int pgClassRelfilenodeDataOffset(); int pgClassReltoastrelidDataOffset(); boolean supports(int pgVersionNum); int majorVersion();`
  - `static WalLayout WalLayouts.forServerVersion(int pgVersionNum)`——未知大版本抛 ISE
  - `WalLayoutV18` 常量：`pageMagic=0xD118`、`walBlockSize=8192`、`shortPageHeaderSize=24`、`longPageHeaderSize=40`、`recordHeaderSize=24`、`sizeOfHeapUpdate=14`、`sizeOfHeapDelete=8`；`pgClassRelfilenodeDataOffset=88`、`pgClassReltoastrelidDataOffset=112`；kinds 数组照抄 spike `PGATTR_KINDS`/`PGCLASS_KINDS`（spike 文件已在仓内，逐行搬并保留出处注释）

- [ ] **Step 1: 失败测试**——`pageMagic==0xD118`、`supports(180000)` 真 / `supports(170000)` 假、`forServerVersion(190000)` 抛 ISE、`Lsn.format/parse` 往返（`"0/46F00000"`）、两 kinds 数组长度 25/34 且 `pgClassKinds()[7]`/`[13]` 是 `"oid"`（偏移自检）

- [ ] **Step 2: 验证失败** → **Step 3: 实现**（kinds 从 spike 静态数组逐行复制；每常量 javadoc 标源码头文件）→ **Step 4: `mvn test -pl vb-stream-wal-source -Dtest=WalLayoutV18Test` 绿** → **Step 5: Commit** `feat(wal-source): WalLayout descriptor（V18）+ 版本分发 fail-fast`

---

### Task 3: WalRecord 记录走读 + 字节测试基建

**Files:**
- Create: `.../walsource/layout/WalRecord.java`、`.../layout/BlockRef.java`、`.../layout/WalRecordParser.java`
- Test: `.../walsource/layout/WalRecordParserTest.java`、`.../walsource/layout/WalBytes.java`（测试源码根，手造字节 DSL）

**Interfaces:**
- Produces:
  - `record WalRecord(long lsn, int totLen, int xid, int toplevelXid, int rmid, int info, List<BlockRef> blocks, int mainOff, int mainLen, byte[] raw)`，`long effXid()`
  - `record BlockRef(int fork, int blockNo, long spc, long db, long relNode, boolean hasImage, int imageOff, int imageLen, int bimgInfo, int holeOffset, int holeLen, boolean hasData, int dataOff, int dataLen)`
  - `static WalRecord WalRecordParser.parse(byte[] rec, long lsn, WalLayout layout)`
  - `WalBytes`（test DSL）：`WalBytes.record(rmid, info, xid)` → `.block(fork, spc, db, relNode, blockNo)`（可选 `.image(bytes, holeOff, bimgInfo)` / `.data(bytes)`）→ `.toplevel(xid)` → `.main(bytes)` → `byte[] build()`；内部处理 [image][data] 载荷序、datatotal 终止判据、MAXALIGN

- [ ] **Step 1: 失败测试**（四用例，全部经 WalBytes 造字节再 parse 断言）：
  1. 单 block（heap insert 形态）+ main data：blocks==1、dataOff/mainOff 自洽（`mainOff+mainLen==raw.length`）
  2. 无 main data（仅 image，XLOG_FPI 形态）：`mainLen==0` 且终止判据=datatotal（spike 发现 2）
  3. TOPLEVEL_XID(252) 标记：`toplevelXid` 读出、`effXid()` 归并（spike 发现 25）
  4. SAME_REL：第二 block 复用前一 locator
- [ ] **Step 2: 验证失败** → **Step 3: 实现**——走读逻辑照抄 spike `parseRecord`（锚：`static ParsedRecord parseRecord`），把硬编码常量改为 layout 注入；补 padding 断言。**Step 4: 绿** → **Step 5: Commit** `feat(wal-source): WAL 记录/block 头走读 + WalBytes 测试 DSL`

---

### Task 4: heap 家族视图

**Files:**
- Create: `.../walsource/layout/HeapViews.java`（六个 record + 解析工厂）
- Test: `.../walsource/layout/HeapViewsTest.java`

**Interfaces:**
- Produces（全部 `static XxxView parse(WalRecord r, WalLayout l)`，字段与 spike 语义一致）：
  - `record HeapInsertView(int offnum, int flags)`
  - `record HeapUpdateView(int oldOffnum, int newOffnum, int flags, int oldBlockNo)`（oldBlockNo=按 fork==0 非 b0 块选择，spike VM-block 教训）
  - `record HeapDeleteView(int offnum, int flags)`
  - `record HeapInplaceView(int offnum)`（含后续 16B 头，main 里的 dbId/tsId/nmsgs 跳过）
  - `record MultiInsertView(int ntuples, int offsetsOff)`（offsets 数组访问器 `int offsetAt(int i)`；含 C-padding：ntuples@mainOff+2，spike 发现 3）
  - `record PruneView(int flags, int dataOff)`（分段访问器：`List<Integer> redirectedPairs()` / `List<Integer> nowdead()` / `List<Integer> nowunused()`，按 spike 发现 18 的布局走读，含 freeze-plans 跳过 12B/plan）
  - 常量类 `HeapOps`（rmid + opcode + XLH_* flags，照抄 spike 常量区）

- [ ] **Step 1: 失败测试**——WalBytes 造 update 记录断言 old/new offnum 与 flags=0x60；multi_insert 记录断言 ntuples=3 与 offsets；prune 记录（freeze 1 plan + 2 redirected 对 + 3 dead）断言三段列表；toplevel 无关性
- [ ] **Step 2: 失败** → **Step 3: 实现**（spike 对应分支逐段移植，layout 注入 sizeOfHeapUpdate 等）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): heap 家族记录视图（insert/update/delete/inplace/multi-insert/prune）`

---

### Task 5: tuple 解码层

**Files:**
- Create: `.../walsource/layout/TupleDecoder.java`、`.../layout/DecodeKinds.java`
- Test: `.../walsource/layout/TupleDecoderTest.java`

**Interfaces:**
- Produces:
  - `DecodeKinds`：`static String forTypeOid(long oid)`（16/18/19/20/21/23/25/26/700/701/1114 映射，未知抛 ISE）；字面常量 `"dropped"`、`"skip"`
  - `class TupleDecoder`（构造持 `WalLayout`）：
    - `Object[] decodePayload(byte[] raw, int off, String[] kinds)`——WAL 载荷形态 `[xl_heap_header 5B][从 tuple offset 23 起字节]`
    - `Object[] decodePageTuple(byte[] page, int lpOff, String[] kinds)`——页内完整 tuple
    - `Object[] decodeEntry(byte[] raw, int entryOff, String[] kinds)`——multi-insert entry（含 2B datalen 前缀，spike 教训）
  - 对齐相对 tuple 起点（spike 发现 7）；varlena：1B 头 `(b>>1)&0x7F`、4B 头 `u32le>>>2`（发现 21）；name 64B NUL 截断；bool/char/oid/int2/int4/int8/float4/float8/timestamp（epoch 2000 微秒）渲染与 spike `decodeTupleData` 一致；`dropped` 空 case（发现 26）

- [ ] **Step 1: 失败测试**——WalBytes 造 tuple 载荷：6 列含 UTF-8 π 文本/float8/bool/timestamp 微秒往返；null bitmap（含 dropped 列 null 占位，natts> live）；4B varlena 头（7000 长度 → `u32le>>>2` 校验，防 BE 回归）；name 63 字节 + NUL；entry 形态 datalen 前缀偏移 +2
- [ ] **Step 2: 失败** → **Step 3: 实现**（spike `decodeTupleData`/`tupleFromPayload` 移植 + kinds 参数化）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): 磁盘格式 tuple 解码（对齐/位图/varlena 双头/dropped 占位）`

---

### Task 6: FPW 页镜像重建

**Files:**
- Create: `.../walsource/layout/PageImages.java`
- Test: `.../walsource/layout/PageImagesTest.java`

**Interfaces:**
- Produces: `class PageImages`：
  - `static byte[] rebuild(WalRecord r, BlockRef b)`——hole 拼接（`HAS_HOLE`：head+zeros+tail；无 hole：整页拷贝；压缩镜像抛 ISE）
  - `static int linePointerOffset(byte[] page, int offnum)`——ItemId `lp_off:15/lp_flags:2@15/lp_len:15@17`，LP_NORMAL==1（发现 4），非 LP_NORMAL 抛 ISE

- [ ] **Step 1: 失败测试**——WalBytes 造带 hole 镜像（head 72B + tail 16B，holeLen=8192-88）：重建后 `pd_lower@12`/`pd_upper@14` 可读；linePointerOffset 对自造 ItemId（`0x00809d00`→lp_off 7424、len 64、flags 1）断言
- [ ] **Step 2: 失败** → **Step 3: 实现**（spike `rebuildPage`/位段移植）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): FPW 页镜像重建与 line pointer 提取`

---

## MS2：流接收

### Task 7: WalStreamWalker（纯状态机）

**Files:**
- Create: `.../walsource/receive/WalStreamWalker.java`、`.../receive/WalStreamMetrics.java`
- Test: `.../receive/WalStreamWalkerTest.java`

**Interfaces:**
- Consumes: Task 2/3（WalLayout、WalRecordParser）
- Produces:
  - `class WalStreamWalker`（构造持 layout + parser + `Consumer<WalRecord> sink`）：
    - `void feed(long chunkEndLsn, byte[] data)`——绝对 LSN 游标 + carry 拼接 + contrecord 拼接 + 流首孤立续体跳过 + **页头 pageaddr 连续性锚定**：每页校验 `pageaddr == expectedPageAddr`；失配→在合并缓冲内向前扫下一个合法页头（magic 且 pageaddr 与游标容差 ≤2 页）再同步，`metrics.resyncs++` 并 WARN；超容差抛 ISE（spike 发现 23b 的正式解）
    - `long consumedLsn()`
  - `class WalStreamMetrics`：`LongAdder records/resyncs/contrecords/orphanSkips` + census `Map<String,Long>`（`rmid/info&0xF0` 计数），只读快照方法

- [ ] **Step 1: 失败测试**（WalBytes 扩展：整页拼装 helper `WalBytes.page(long pageAddr, ...records)`）：
  1. 单页多记录顺序产出、consumedLsn 推进
  2. 记录跨页（8KB FPW）→ contrecord 拼接完整
  3. chunk 任意切分（16 字节粒度逐段 feed）结果与一次 feed 全等
  4. 孤立续体页被跳过、计数
  5. pageaddr 失配 32B → 再同步产出后续记录且 `resyncs==1`；失配 3 页 → ISE
- [ ] **Step 2: 失败** → **Step 3: 实现**（spike `WalWalker` 移植 + pageaddr 锚定改造：spike 的 `WAL gap` 异常路径换成扫描再同步）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): WAL 流走读状态机——pageaddr 锚定 + 受控再同步`

---

### Task 8: 物理槽 + pgjdbc 接收器 + IT 环境

**Files:**
- Create: `.../walsource/receive/PhysicalSlotManager.java`、`.../receive/WalStreamReceiver.java`
- Test: `.../walsource/it/WalTestEnv.java`、`.../walsource/it/WalReceiveIT.java`

**Interfaces:**
- Consumes: Task 7（WalStreamWalker）
- Produces:
  - `class PhysicalSlotManager(Connection c)`：`long ensureSlot(String name)`——不存在则 `pg_create_physical_replication_slot` 并返回创建时 `pg_current_wal_flush_lsn()`；存在则查 `pg_replication_slots.confirmed_flush_lsn`（物理槽为 `restart_lsn`，以 restart_lsn 作续传起点）返回；带 42710 竞态重查
  - `class WalStreamReceiver`：构造 `(String url, String user, String pass, WalLayout layout, String slotName)`；`void start(Consumer<WalRecord> sink, long fromLsn)`——复制连接（`replication=database&assumeMinServerVersion=9.4`）→ `physical().withStartPosition(...)` → readPending drain 循环（`getLastReceiveLSN().asLong()` 传 walker.feed）→ 周期 `setFlushedLSN(consumedLsn)+forceUpdateStatus`；断流自动重连（从 consumedLsn，指数退避，上限 5 次）；`long consumedLsn(); void stop()`
  - `WalTestEnv`：单例 `postgres:18` 容器（`wal_level=logical`（对齐仓库惯例，物理流兼容）、`max_replication_slots=16`、`max_wal_senders=16`），`newSqlConnection()`/`jdbcUrl()`/replication 变体 URL、`pgWaldump(startLsn, endLsn)`（容器 exec，返回文本行流）

- [ ] **Step 1: 失败 IT**——建槽 → 起 receiver（sink 收集）→ JDBC 会话执行 `CREATE TABLE/DROP` → 等 census 含对应 heap2 multi-insert → 与 `WalTestEnv.pgWaldump` 同窗口 census 对比（rmid 维度计数一致）→ 断点：kill 容器连接（`PG.execInContainer("pg_ctl ... stop -m fast)"` 不可用则断网：直接 close 底层——用 stop/start 容器法）→ 重连后续传（consumedLsn 不回退）→ `resyncs==0`
- [ ] **Step 2: 验证失败（红）** → **Step 3: 实现**（spike main 的连接段落移植 + 重连循环新增）→ **Step 4: `mvn test -pl vb-stream-wal-source -Dtest=WalReceiveIT` 绿** → **Step 5: Commit** `feat(wal-source): 物理槽管理 + pgjdbc 接收器（重连续传）+ census 对拍 IT`

---

### Task 9: 冒烟 Main（接收形态）

**Files:**
- Create: `.../walsource/Main.java`、`.../walsource/api/WalSource.java`（门面最小版：仅接收面）

**Interfaces:**
- Produces: `class WalSource implements AutoCloseable`：`void start()`（ensureSlot→receiver.start，sink 记 census/位点）、`long consumedLsn()`、`WalStreamMetrics metrics()`；`Main` 读 `-Dvb.wal.*`（host/port/db/user/pass/slot/stateDir）起门面，10s 周期 INFO 一行（census 摘要 + consumedLsn + resyncs），Ctrl-C hook 优雅停

- [ ] **Step 1: 无新单测（纯装配），手工冒烟**：`docker compose` 常驻 PG 上起 Main（对齐 CLAUDE.md 运行节命令形态）、另一会话 DDL、观察 census 行推进、`Ctrl-C` 干净退出
- [ ] **Step 2: Commit** `feat(wal-source): WalSource 门面（接收面）+ 冒烟 Main`

---

## MS3：catalog 同步

### Task 10: 重放引擎核心（纯逻辑）

**Files:**
- Create: `.../walsource/replay/CatalogRow.java`（含 `AttrRow`/`ClassRow` record）、`.../replay/CatalogReplay.java`、`.../replay/HeapEvent.java`、`.../replay/Reconstruction.java`
- Test: `.../walsource/replay/CatalogReplayTest.java`

**Interfaces:**
- Consumes: Task 4/5/6（HeapViews/TupleDecoder/PageImages）
- Produces:
  - `record AttrRow(long attrelid, String attname, long atttypid, int attnum, boolean attisdropped)`
  - `record ClassRow(long relOid, String relname, long relnamespace, long reltype, long reloftype, long relowner, long relam, long relfilenode, long reltoastrelid)`，方法 `byte[] encodeFirstSevenCols()`（88B 值编码，spike 发现 24）
  - `record HeapEvent(int op, long oldCtid, long newCtid, Object[] row)`，op = INS/DEL/UPD
  - `class CatalogReplay`（构造持 layout + TupleDecoder）：
    - `List<HeapEvent> heapEvents(WalRecord r, long watchedRelfilenode, String[] kinds, Map<Long,?> rawTailStore)`——INS/DEL/UPD（含截断值重建、prefix>88 拒绝走 rawTail→null→skip）/MULTI_INSERT（data 与 image 双路径）/INPLACE（pg_class 88/112 直读，仅 watched==pgClass 时）
    - `void applyPrune(WalRecord r, CatalogStores stores)`——redirect 重定位/dead/unused（spike `replayPrune` 移植）
    - `CatalogStores`（Task 11 定义，本任务以接口消费：`attrRows()/classRows()/rawAttrTails()/rawClassTails()` 四 map + tracked 双 long）

- [ ] **Step 1: 失败测试**（WalBytes 造记录序列，断言 stores 终态）：
  1. INS→map 有行（data 路径）；FPW-only INS→image 路径解出同值（发现 14/15）
  2. UPD 非截断→ctid 移动、旧 key 消失（UPD 必删旧——spike 教训）
  3. UPD 截断 prefix=88→值编码重建全等；prefix=96 且 rawTail 有→splice 重建；两者皆无→skip 不抛
  4. MULTI_INSERT 3 行 + INIT_PAGE 形态（offsets 省略）
  5. INPLACE 后 relfilenode/reltoastrelid 更新（发现 17/19）
  6. PRUNE：2 redirect 对重定位 tracked、3 dead 移除、freeze 段跳过正确（发现 18）
- [ ] **Step 2: 失败** → **Step 3: 实现**（spike `heapEvents`/`reconstructClassTruncated`/`replayPrune`/`replayInplace` 移植；删除 `pg_read_binary_file` 兜底——spec §6）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): catalog ctid 重放引擎——全形态 + 值编码重建`

---

### Task 11: CatalogSynchronizer（引导 + 组装）

**Files:**
- Create: `.../walsource/replay/CatalogStores.java`、`.../replay/CatalogBootstrap.java`、`.../replay/CatalogSynchronizer.java`、`.../walsource/api/CatalogSnapshot.java`
- Test: `.../walsource/replay/CatalogSynchronizerTest.java`（纯逻辑部分）

**Interfaces:**
- Consumes: Task 10
- Produces:
  - `class CatalogStores`：四 map + `trackedTableCtid/trackedToastCtid`（long[] holder 形态）+ `interestRelOids` 注册
  - `class CatalogBootstrap(Connection c, WalLayout layout)`：`long bootstrap(CatalogStores stores)`——单 REPEATABLE READ 事务：`pg_relation_filenode('pg_attribute'/'pg_class')`、两张 watched 表全行含 ctid（`WHERE attrelid>0`（pg_attribute 按 attnum>-7 取系统列内最小集——v1 取 `attnum>0 AND relkind IN ('r','t')`? 不：**全表**，字典面覆盖全部关系）、pg_class `WHERE relkind IN ('r','v','m','t','i')`? **全表**）、caller interest 的 relid 行（含其 toast）——返回引导完成时的 `pg_current_wal_flush_lsn()`（spec §6①顺序：槽 P₀ 已在 caller 建好，bootstrap 返回值与 P₀ 取 max 再页对齐）
  - `interface CatalogSnapshot`：`long lsn(); List<Column> columnsOf(long relOid); OptionalLong relfilenodeOf(long relOid); OptionalLong toastOf(long relOid);`（`record Column(int attnum, String name, long typeOid, boolean dropped)`）
  - `class CatalogSynchronizer`：`static CatalogSynchronizer start(Connection sql, Consumer<WalRecord> pump ...)`——v1 组装序：ensureSlot(P₀)→bootstrap→receiver.start(sink=synchronizer.apply)→`void apply(WalRecord)`（prune→inplace→pgattr events→pgclass events+tracked 跟随→[v2 预留：用户表]）→`CatalogSnapshot snapshot()`
- [ ] **Step 1: 失败测试**（stubs：fake Connection 不可行——bootstrap 的 JDBC 部分留 IT；本任务测组装逻辑）：`apply` 顺序正确性（prune 先于 events）、snapshot 的 columnsOf 含 dropped 占位（attnum 序）、relfilenodeOf 取 tracked 链当前值
- [ ] **Step 2: 失败** → **Step 3: 实现**（bootstrap SQL 照 spike main 种子段扩展为全表）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): CatalogSynchronizer 组装 + REPEATABLE READ 一致性引导 + CatalogSnapshot 契约`

---

### Task 12: 自愈校验（中段新值对 JDBC 末态）

**Files:**
- Create: `.../replay/JdbcProbe.java`、`.../replay/SelfHealer.java`
- Test: `.../walsource/replay/SelfHealerTest.java`

**Interfaces:**
- Consumes: Task 10（CatalogReplay 截断分支需插入 hook）
- Produces:
  - `interface JdbcProbe`：`ClassRow currentClassRow(long relOid)`（`SELECT ctid::text, relname, relnamespace, reltype, reloftype, relowner, relam, relfilenode, reltoastrelid FROM pg_class WHERE oid=?`）
  - `class SelfHealer`：`Optional<Reconstructed> validate(Reconstructed candidate, ClassRow candidateOf, JdbcProbe probe)`——校验规则（spec §6②）：重建行与 `probe.currentClassRow(relOid)` 的 `relfilenode/relpages? (v1 取 relfilenode 与 reltoastrelid)` **任一相等且 ctid 等于记录 new 位**才采纳；失败→WARN+stale 标记（stores.staleOids 集合）。废除 OID 自校验（spike 发现 23a）
  - `CatalogReplay` 构造注入 `SelfHealer`（可 null=禁用）

- [ ] **Step 1: 失败测试**——stub probe：①中段值与末态一致→采纳+tracked 修位；②不一致→empty+stale 记录；③连续两次失败不升级为异常
- [ ] **Step 2: 失败** → **Step 3: 实现** → **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): 截断自愈校验——中段新值对 JDBC 末态（废 OID 恒真校验）`

---

### Task 13: 对抗性对拍 IT（PG 18）

**Files:**
- Test: `.../walsource/it/WalAdversarialIT.java`、`.../walsource/it/Interference.java`

**Interfaces:**
- Consumes: Task 8/11/12 全量
- Produces: 验收证据（spec §8 对拍矩阵）

- [ ] **Step 1: 写失败 IT**——结构：
  1. `WalTestEnv` 起干扰容器变体（`Interference.tunedContainer()`：`autovacuum_vacuum_cost_delay=0`、`autovacuum_vacuum_threshold=1`、`autovacuum_analyze_threshold=1`）
  2. `CatalogSynchronizer.start`（slot+bootstrap+receiver）
  3. DDL 场景序列（独立连接）：建表→批量 INSERT/UPDATE/DELETE 制死元组→ADD COLUMN→ADD COLUMN DEFAULT→DROP COLUMN→RENAME→TRUNCATE→再 TRUNCATE→建带 toast 的表并写宽行→并发第二连接交错 DDL（两线程 CountDownLatch 对齐）→CHECKPOINT×3
  4. 干扰线程：循环 `UPDATE pg_stat...`? 不——循环造 catalog 死元组：独立连接反复 `ALTER TABLE t SET (autovacuum_enabled)` 切换（合法地改 pg_class）+ `ANALYZE` 风暴，8 秒
  5. 停流→snapshot()→JDBC REPEATABLE READ 实查两表全行→**逐行全等断言**（ctid、attrname/typid/attnum/dropped、relfilenode/toast 映射、列序）——diff 失败时打印双方差异行（spec §10 可诊断性）
  6. 断言 `metrics().resyncs()==0`
  7. 复跑两次同 IT 方法（`@RepeatedTest(2)`，竞态覆盖）
- [ ] **Step 2: 红** → **Step 3: 迭代修复重放器至绿**（预期主要修：tracked 断链场景由 SelfHealer 覆盖；PRUNE 边界；本步骤允许在 replay 内修 bug，但**不得放宽断言**）→ **Step 4: 绿 + `mvn test -pl vb-stream-wal-source` 全绿** → **Step 5: Commit** `feat(wal-source): 对抗性 catalog 对拍 IT——autovacuum/交错 DDL/CHECKPOINT 下全等`

---

## MS4：持久化 + V17 + 收官

### Task 14: StateStore

**Files:**
- Create: `.../walsource/state/StateStore.java`、`.../state/StoredState.java`
- Test: `.../walsource/state/StateStoreTest.java`

**Interfaces:**
- Produces:
  - `record StoredState(long lsn, List<AttrRow> attrs, List<ClassRow> classes, long trackedTableCtid, long trackedToastCtid)`
  - `class StateStore(Path dir)`：`void checkpoint(CatalogStores stores, long lsn)`——序列化 `.part`（header `VBWS`+formatVersion=1+pgVersion+lsn+CRC32 → attr 表 → class 表 → footer lsn+CRC32）→ `FileChannel.force` → `Files.move(ATOMIC_MOVE)`；`Optional<StoredState> load()`——CRC/版本/lsn 双验，任一不符 → `Optional.empty()` + ERROR 日志（回落重引导由 caller 决定）；`boolean exists()`

- [ ] **Step 1: 失败测试**——roundtrip 全等（含 tracked ctid）；翻一位字节→empty；改 formatVersion→empty；`.part` 残留不 load；连写两次（旧 checkpoint 被覆盖原子替换）
- [ ] **Step 2: 失败** → **Step 3: 实现**（参照 reader 模块 `.part→rename` 范式；DataOutputStream + CRC32）→ **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): StateStore 原子持久化——VBWS 格式/CRC 双验/拒载回落`

---

### Task 15: 生命周期 IT（重启/续传/损坏回落）

**Files:**
- Test: `.../walsource/it/WalLifecycleIT.java`
- Modify: `.../replay/CatalogSynchronizer.java`（接入 StateStore：`start` 先 load 成功→以 stored lsn 续传跳过 bootstrap；失败→全新引导；运行中按 `vb.wal.state.interval.ms`(默认 30000)/`vb.wal.state.events`(默认 1000) 落盘）

**Interfaces:**
- Consumes: Task 13/14

- [ ] **Step 1: 失败 IT**——①跑半程 DDL→stop→重启（load 续传）→剩余 DDL→对拍全等（复用 Task 13 的场景与对拍断言，抽公共基类 `WalSyncItBase`）；②状态文件截断 3 字节→重启走全新引导且对拍仍全等；③重启后 `consumedLsn() >= stored lsn`
- [ ] **Step 2: 红** → **Step 3: 实现 + 至绿** → **Step 4: Commit** `feat(wal-source): 检查点续传生命周期 IT + 损坏回落全新引导`

---

### Task 16: WalLayoutV17 + 17 矩阵 + 跨版本拒绝

**Files:**
- Create: `.../layout/WalLayoutV17.java`
- Test: `.../layout/WalLayoutV17Test.java`、`.../it/Wal17TestEnv.java`、`.../it/Wal17SyncIT.java`
- Modify: `.../layout/WalLayouts.java`（注册 17）

**Interfaces:**
- Produces: `WalLayoutV17 implements WalLayout`——**转录步骤硬性要求**：curl 拉 REL_17_STABLE 头文件（`xlog_internal.h/xlogrecord.h/heapam_xlog.h/pg_attribute.h/pg_class 布局以活库 17 容器实查`），逐常量比对 V18 输出差异表（已知至少：`pageMagic`、`pg_attribute` attstattarget 位置——spike 发现 16），javadoc 标 `// REL_17_STABLE`

- [ ] **Step 1: 差异转录**——fetch 源码 + `Wal17TestEnv`（postgres:17 容器同 Interference 参数）+ 实查 pg_attribute/pg_class 列序 → 写 `WalLayoutV17Test` 钉死差异断言（pageMagic=0xD116 以实际头文件为准，测试与实现同源提交）
- [ ] **Step 2: `Wal17SyncIT`**——Task 13 对拍基类在 17 环境复跑（`@RepeatedTest(2)`）+ 生命周期基类复跑
- [ ] **Step 3: 跨版本拒绝**——`WalLayoutsTest` 补 `forServerVersion(160000)` 抛 ISE；IT：用 V18 layout 对 17 容器 start → 启动即 ISE
- [ ] **Step 4: 绿** → **Step 5: Commit** `feat(wal-source): WalLayoutV17 转录 + 17 对拍/生命周期矩阵 + 跨版本 fail-fast`

---

### Task 17: 文档归档 + 全仓收官

**Files:**
- Create: `vb-stream-wal-source/CLAUDE.md`、`vb-stream-wal-source/README.md`
- Modify: 根 `CLAUDE.md`（模块列表一行 + 源码结构一节 + 运行 Main 段落）、`docs/wal-direct-decode-spike.md`（头部加"已立项，正式模块见…"注记）

**Interfaces:** 无代码

- [ ] **Step 1: 模块 CLAUDE.md**——定位/组件图/竞速三修正机制/已知限制（VACUUM FULL 重发语义、压缩 external 未支持、two_phase 未消费）/坑位（锚定协议、INPLACE 偏移）
- [ ] **Step 2: README**——快速开始（docker + Main 参数表 + 配置键 `vb.wal.*` 全表）/架构节（spec 链接）/验收说明
- [ ] **Step 3: 全仓回归** `mvn test -q`（含既有 224+272+276 用例不回归）→ **Step 4: Commit + push** `docs(wal-source): 模块文档归档 + 根索引收官`

---

## Self-Review 记录

- **Spec 覆盖**：§2 目标 A→Task 10/11/13、B→Task 7/8/9；§5 五组件→Task 2-8/10-11/14；§6 三修正→Task 11(①)/12(②)/7(③)+Task 10 删页读；§7→Task 14/15；§8 矩阵→Task 13/15/16；§9 里程碑→MS1=Task1-6、MS2=7-9、MS3=10-13、MS4=14-17。无缺口
- **占位扫描**：无 TBD/"适当处理"；所有代码步骤有实码或精确移植锚（spike 在仓内，行号稳定于 commit `2b5e29e`）
- **类型一致**：`WalRecord/WalLayout/CatalogStores/AttrRow/ClassRow/Reconstructed/JdbcProbe/StoredState` 各任务引用名与定义任务逐一核对一致
