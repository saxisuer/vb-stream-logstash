# wal-source 离线回放档（offline replay）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** VastBase 库不可访问时，凭本地检查点 + WAL 段文件目录离线续传解析 DML，直至 WAL 耗尽（确定性 EOF）。

**Architecture:** 方案 A——`CatalogSynchronizer` 增无库装配档 `startOffline`（复用运行期管线零复制），`FileWalSource` 增静态目录档（启动期审计 + 按清单读尽即 EOF），`WalSource` 增 `startOffline()` 门面 + Main 三配置键。spike 手写 harness 全部消失。

**Tech Stack:** Java 17、JUnit 6（Jupiter）、既有模块零新依赖（无 Docker 离线单测 + VastBase 真库手动档 IT）。

**Spec:** `docs/superpowers/specs/2026-10-15-wal-source-offline-replay-design.md`（先导 spike：`docs/wal-source-offline-replay-spike.md`）

## Global Constraints

- **在线档路径零改动**：`WalSource.start()`/`startOpenGauss()`/`CatalogSynchronizer.start/startOg`/`FileWalSource` tail 档（既有两构造器路径）逐字节不变——回归面收敛在新增分支。
- 方法名英文 camelCase；类型引用 import 简名禁 FQN 内联；日志一律 slf4j + `{}` 占位；每个函数（含私有/测试辅助）javadoc 逻辑描述（职责/关键步骤/边界/线程约束）。
- 既有 **517 用例零回归**（`mvn test -pl vb-stream-wal-source` 全绿；Windows 存量 7 例环境性 skip 不计）。
- 检查点文件格式 **VBWS formatVersion 不变**（v4，`StoredState.pgVersion` 是回读填充非序列化面）。
- 每任务一次 commit（消息含 `(offline-replay)` 尾标）。

---

### Task 1: StoredState 扩展 pgVersion 回读

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/state/StoredState.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/state/StateStore.java:433-437`（decode 尾部构造）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/state/StateStoreTest.java`

**Interfaces:**
- Consumes: 无（既有 `StateStore.decode` 已读 `storedPgVersion` 仅作校验）
- Produces: `StoredState.pgVersion()` → `int`（record 组件，**追加在组件列表末位**——既有 canonical 构造调用点仅 decode 一处主代码 + 测试，尾部追加破坏面最小；后续 Task 3 据此判定 VastBase 档 `== 6`）

- [ ] **Step 1: 写失败测试**（StateStoreTest 增一用例——写 6 档检查点后 load 回读 pgVersion==6；同时断言另一档 5 也回读）：

```java
/**
 * pgVersion 回读往返（离线档位判定的供数面，spec §3.1）：写入时 header 的 pgVersion
 * 在 load 后可经 StoredState 组件取得——decode 回读填充、不改文件格式。
 */
@Test
void storedStateCarriesPgVersionAfterRoundTrip() throws Exception {
    for (int pg : new int[]{5, 6, 18}) {
        Path dir = Files.createTempDirectory("vbws-pgv");
        StateStore store = new StateStore(dir, pg);
        store.checkpoint(new CatalogStores(), 0x100L, 0L);
        StoredState st = new StateStore(dir, pg).load().orElseThrow();
        assertEquals(pg, st.pgVersion(), "pgVersion 应自 header 回读");
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=StateStoreTest`
Expected: 编译失败——`StoredState` 无 `pgVersion()` 组件。

- [ ] **Step 3: 最小实现**——`StoredState` record 组件列表**末位**追加 `int pgVersion`（javadoc：`@param pgVersion 检查点档位（layout.majorVersion()——17/18/OG=5/VastBase=6；load 自 header 回读填充，不参与序列化）`）；`StateStore.decode` 尾部构造调用补末参 `storedPgVersion`。全仓搜 `new StoredState(` 修齐其余调用点（若有测试直造）。

- [ ] **Step 4: 跑测试确认通过**（含既有 StateStoreTest 全部用例零回归）

Run: `mvn test -pl vb-stream-wal-source -Dtest=StateStoreTest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/state/ vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/state/StateStoreTest.java
git commit -m "feat(wal-source): StoredState 扩展 pgVersion 回读——decode 自 header 填充不改 VBWS 格式，离线档位判定的供数面 (offline-replay)"
```

---

### Task 2: FileWalSource 静态目录档（审计 + 读尽 EOF）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/receive/FileWalSource.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/receive/FileWalSourceTest.java`

**Interfaces:**
- Consumes: 既有 `listSegments()`/`parseSegNo(String, long)`/`PARTIAL_SUFFIX`/`CHUNK_LIMIT`/`POLL_INTERVAL_MS` 与 partial 前沿扫描逻辑（静态档复用扫描思路）。
- Produces:
  - `public static FileWalSource staticDirectory(Path dir, long segmentSize, long fromLsn, int walBlockSize, long endLsn)`（`endLsn == 0` = 无上界；返回实例与 tail 档同 `start/stop/consumedLsn/terminalFailure` 契约）
  - `public boolean eofReached()`（tail 档恒 false；静态档清单读尽/endLsn 到达后 true）

- [ ] **Step 1: 写失败测试**（FileWalSourceTest 增静态档用例组 6 例；段文件用 `RandomAccessFile.setLength` 稀疏造，内容手写页字节可全零——OG 系 walker 零页容忍由消费侧测，本面只测字节交付与终态。先给核心 3 例，其余 3 例同型）：

```java
/**
 * 静态档确定性 EOF：单段目录（手造 3 页字节 + 稀疏尾）读尽后 eofReached 置位、
 * sink 收到的字节总量 = 文件已写区（无轮询等待——读尽即终态）。
 */
@Test
void staticDirectoryReachesDeterministicEof() throws Exception {
    Path dir = Files.createTempDirectory("static-seg");
    writeSegment(dir, "000000010000000000000003", 3 * 8192 + 4096);   // 辅助：稀疏文件前 N 字节写非零
    FileWalSource src = FileWalSource.staticDirectory(dir, SEG, 0L, 8192, 0L);
    List<byte[]> chunks = new ArrayList<>();
    src.start(lsn -> { }, (end, b) -> chunks.add(b));
    awaitTrue(src::eofReached, 10_000);                              // 辅助：轮询上限断言
    src.stop();
    long total = chunks.stream().mapToLong(b -> b.length).sum();
    assertEquals(3 * 8192 + 4096, total, "已写字节全量交付");
}

/**
 * 静态档起点段缺失 fail-fast：fromLsn 落段 5 而目录只有段 3——启动期 ISE（tail 档
 * ceilingKey 静默跳后继段语义在静态档不可接受，spec §3.3）。
 */
@Test
void staticDirectoryRejectsMissingStartSegment() throws Exception {
    Path dir = Files.createTempDirectory("static-miss");
    writeSegment(dir, "000000010000000000000003", 8192);
    FileWalSource src = FileWalSource.staticDirectory(dir, SEG, 5L * SEG, 8192, 0L);
    IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> src.start(lsn -> { }, (end, b) -> { }));
    assertTrue(e.getMessage().contains("段 5"), "消息含期望段号: " + e.getMessage());
}

/**
 * 静态档 tli 混杂 fail-fast：目录混入 tli=2 段名——ISE 列出冲突 tli（真库 pg_xlog
 * 混历史分支段的正解，spec §3.3）。
 */
@Test
void staticDirectoryRejectsMixedTimeline() throws Exception {
    Path dir = Files.createTempDirectory("static-tli");
    writeSegment(dir, "000000010000000000000003", 8192);
    writeSegment(dir, "000000020000000000000004", 8192);
    FileWalSource src = FileWalSource.staticDirectory(dir, SEG, 0L, 8192, 0L);
    IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> src.start(lsn -> { }, (end, b) -> { }));
    assertTrue(e.getMessage().contains("tli"), "消息含 tli 冲突: " + e.getMessage());
}
```

（另 3 例同型补齐：`staticDirectoryRejectsSegmentGap`——段 3、5 缺 4 即 ISE 消息含缺口；`staticDirectoryStopsAtEndLsn`——endLsn 设段内偏移，交付总量恰到该 LSN；`staticDirectoryPartialSegmentFrontier`——`.partial` 段交付上限 = 末非零字节+1，其后稀疏零区不交付。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=FileWalSourceTest`
Expected: 编译失败——无 `staticDirectory`/`eofReached`。

- [ ] **Step 3: 实现**——`FileWalSource` 增：

```java
/** 静态目录档标志（true = 启动期审计 + 清单读尽即 EOF，无轮询）。 */
private final boolean staticMode;

/** 静态档可选上界（0 = 无；交付前沿到达即 EOF）。 */
private final long endLsn;

/** 静态档读尽终态位（volatile——tail 线程置位，宿主轮询）。 */
private volatile boolean eofReached;

/**
 * 静态目录档工厂（离线回放，spec §3.3）：目录是快照（拷 pg_xlog / recorder 落段），
 * 启动期一次性审计（段覆盖/段号连续/tli 一致，任一不过 ISE）后按清单序读尽即 EOF。
 * endLsn 上界可选（0 = 无）。
 */
public static FileWalSource staticDirectory(Path dir, long segmentSize, long fromLsn,
        int walBlockSize, long endLsn) { /* 委派私有五参构造 + staticMode=true */ }
```

`start(...)` 内 `staticMode` 分支：先 `auditStaticDirectory()`（调用线程同步执行、fail-fast）→ 起 `wal-file-static` 线程跑 `runStaticLoop()`。tail 档路径（`process` 检查、`runTailLoop`）不动。`auditStaticDirectory()`：自扫目录文件名解析 (tli, segNo) 对（tli 取 `[0,8)`、segNo 走 `parseSegNo`）；三项校验（清单空 ISE / tli 不一致 ISE 列冲突值 / `fromLsn>0` 时期望段 `floorDiv(fromLsn, segmentSize)` 必须恰在场且至末段逐 +1 无缺口，缺即 ISE 含期望与实况）。`runStaticLoop()`：按审计清单序，每段开句柄→上限（`.partial` = 自尾向头扫描末非零字节+1 缓存复用既有 `scannedDataEnd` 思路；完成段 = 文件长）→ 自首段 `fromLsn` 段内偏移页对齐下取整起，按 `CHUNK_LIMIT` 分块交付（`chunkEndLsnSink`/`chunkSink` 同 tail 契约；`endLsn>0` 且块末超界则截断交付并跳出）→ 段尽换下一段；清单尽置 `eofReached=true` 自然退出。段文件消失（`NoSuchFileException`）静态档**不重试**——置 `terminalFailure` 退出（快照目录不容突变，spec §5）。

- [ ] **Step 4: 跑测试确认通过**（FileWalSourceTest 全组含既有 tail 档用例零回归）

Run: `mvn test -pl vb-stream-wal-source -Dtest=FileWalSourceTest`
Expected: PASS（新增 6 例 + 既有用例全绿）

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/receive/FileWalSource.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/receive/FileWalSourceTest.java
git commit -m "feat(wal-source): FileWalSource 静态目录档——启动期审计（段覆盖/连续/tli）+ 清单读尽确定性 EOF + 可选 endLsn 上界，tail 档路径零改动 (offline-replay)"
```

---

### Task 3: CatalogSynchronizer.startOffline 无库装配档

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogSynchronizer.java`（新增静态工厂 + `eofReached` 透传）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/replay/CatalogOfflineTest.java`（新建）

**Interfaces:**
- Consumes: Task 1 `StoredState.pgVersion()`；Task 2 `FileWalSource.staticDirectory(...)`/`eofReached()`；既有 `WalLayoutVVastbase.INSTANCE`、私有全参构造、`snapshot()`、`pendingFloor`/`maybeCheckpoint`/`persistCheckpoint`（复用零改动）。
- Produces:
  - `public static CatalogSynchronizer startOffline(StateConfig state, Path walDir, long endLsn, Set<String> tables, ChangeOutputListener out)`（`state` 须 enabled；`out` 为 null = 纯 catalog 形态）
  - `public boolean eofReached()`（透传字节源终态；在线档恒 false）

- [ ] **Step 1: 写失败测试**（`CatalogOfflineTest` 新建，零 Docker——手造检查点 + 手造全零段文件的全链冒烟 + 三个 ISE 面）：

```java
/**
 * 无检查点 ISE 附采集指引（spec §5：离线无法引导——冷启动非目标，入口凭证就是
 * 一份合法检查点）。
 */
@Test
void offlineWithoutCheckpointFailsWithGuidance() {
    Path stateDir = Files.createTempDirectory("off-nostate");
    Path walDir = Files.createTempDirectory("off-wal");
    IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> CatalogSynchronizer.startOffline(cfg(stateDir), walDir, 0L, Set.of(), null));
    assertTrue(e.getMessage().contains("先在线运行"), "附采集指引: " + e.getMessage());
}

/**
 * 检查点 pgVersion=5（OG 档）在离线档 ISE 附档位指引（spec §3.2 第 1 步——本版仅
 * VastBase=6）。
 */
@Test
void offlineRejectsNonVastbaseCheckpoint() throws Exception {
    Path stateDir = Files.createTempDirectory("off-og");
    new StateStore(stateDir, 5).checkpoint(new CatalogStores(), 0x100L, 0L);
    IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> CatalogSynchronizer.startOffline(cfg(stateDir), Files.createTempDirectory("off-wal"), 0L, Set.of(), null));
    assertTrue(e.getMessage().contains("仅支持 VastBase"), "附档位指引: " + e.getMessage());
}

/**
 * 全链冒烟：手造 6 档检查点 + 手造全零段（OG 系零页容忍消化）——装配成功、EOF 到达、
 * 最终检查点落盘（spec §6 装配路径单测）。
 */
@Test
void offlineAssemblyReachesEofAndPersistsCheckpoint() throws Exception {
    Path stateDir = Files.createTempDirectory("off-ok");
    Path walDir = Files.createTempDirectory("off-wal");
    long lsn = 5L * SEG + 0x1234;                                  // 段 5 内
    new StateStore(stateDir, 6).checkpoint(new CatalogStores(), lsn, 0L);
    try (RandomAccessFile f = new RandomAccessFile(
            walDir.resolve("000000010000000000000005").toFile(), "rw")) {
        f.setLength(SEG);                                          // 稀疏全零段
    }
    CatalogSynchronizer sync = CatalogSynchronizer.startOffline(
            cfg(stateDir), walDir, 0L, Set.of(), null);
    awaitTrue(sync::eofReached, 15_000);
    sync.stop();                                                   // 最终检查点落盘
    assertTrue(new StateStore(stateDir, 6).load().isPresent(), "离线检查点续写同目录");
}
```

（`cfg(dir)` 辅助构造 `new StateConfig(dir, 30000, 1000)`；`SEG = 16L<<20`；`awaitTrue` 轮询辅助。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=CatalogOfflineTest`
Expected: 编译失败——无 `startOffline`。

- [ ] **Step 3: 实现**——`CatalogSynchronizer` 增静态工厂（照 `startOg` 骨架，spec §3.2 七步）：

```java
public static CatalogSynchronizer startOffline(StateConfig state, Path walDir,
        long endLsn, Set<String> tables, ChangeOutputListener out) {
    // ① StateStore(state.dir(), 6).load() 必须 present——empty ISE（附"离线无法引导，
    //    需先在线运行产生检查点"指引）；st.pgVersion()!=6 ISE（OG=5/17/18 各附档位指引）
    // ② restoreInto(stores)；appliedLsn 种子 = stored lsn（续传严格过滤线）
    // ③ 流起点 = min(stored, floor)（dmlFloorLsn 回退语义同 startOg :499-505）
    // ④ layout = WalLayoutVVastbase.INSTANCE
    // ⑤ sync = 私有全参构造(stores, new CatalogReplay(layout, new TupleDecoder(layout)),
    //    receiver=null, slotName=null, state, stateStore, advanceConnection=null)
    //    ——advanceSlot 既有 null 空保护 no-op（:831-834 零改动）
    // ⑥ out != null 时：ChangeStream cs = new ChangeStream(sync.snapshot(), null, layout,
    //    tables, out)（sqlConn=null → probe 禁用）；pendingFloor = cs::pendingFloorLsn
    //    （离线装配无 JMM 窗——源线程由本方法内 start，构造序天然安全）
    // ⑦ walker sink 组合同 startOg :510-516（applyRecord → postApply → maybeCheckpoint）
    //    + FileWalSource.staticDirectory(walDir, DEFAULT_SEGMENT_SIZE, streamStart,
    //      layout.walBlockSize(), endLsn) + source.start；sync.eofSource = source
}
```

`public boolean eofReached()`：透传 `byteSource instanceof FileWalSource f && f.eofReached()`（在线档 receiver/其他源恒 false）。类 javadoc 补离线档一段。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl vb-stream-wal-source -Dtest=CatalogOfflineTest`
Expected: PASS（3 例）

- [ ] **Step 5: 全量回归**

Run: `mvn test -pl vb-stream-wal-source`
Expected: 全绿（离线单测零 Docker 面不含既有 IT 的 Docker 依赖变化）

- [ ] **Step 6: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogSynchronizer.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/replay/CatalogOfflineTest.java
git commit -m "feat(wal-source): CatalogSynchronizer.startOffline 无库装配档——检查点 pgVersion=6 校验/严格过滤线/floor 回退/healer+probe 双 null，复用运行期管线零复制 + eofReached 透传 (offline-replay)"
```

---

### Task 4: WalSource.startOffline 门面 + Main 三键 + WalConfigFile 白名单

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/WalSource.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/Main.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/WalConfigFile.java`（白名单 +2 键）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/api/WalSourceConfigTest.java`、`WalConfigFileTest.java`

**Interfaces:**
- Consumes: Task 3 `CatalogSynchronizer.startOffline(...)`/`eofReached()`；`WalSource` 既有 `state`/`tables`/`dmlOut` 字段与 `dmlEnabled()`。
- Produces:
  - `WalSource.KEY_OFFLINE_DIR = "vb.wal.offline.dir"`（构造解析，缺省 null）
  - `WalSource.KEY_OFFLINE_END_LSN = "vb.wal.offline.endLsn"`（十六进制 LSN 文本如 `2/071BF288`；非法值构造期 IAE）
  - `public void startOffline()`、`public boolean eofReached()`、`public Path offlineDir()`
  - Main 分派键 `vb.wal.mode`（`online` 缺省 | `offline`；非法值启动期 ISE）+ EOF 检测（`eofReached()` → INFO + close + exit 0）

- [ ] **Step 1: 写失败测试**（WalSourceConfigTest 增 3 例）：

```java
/** offline.dir 键解析（缺省 null——startOffline 时 ISE；显式设置即存档）。 */
@Test
void offlineDirKeyParsed() {
    WalSource s = new WalSource(propsWith(WalSource.KEY_OFFLINE_DIR, "D:/wal"));
    assertEquals(Path.of("D:/wal"), s.offlineDir());
    assertNull(new WalSource(new Properties()).offlineDir());
}

/** offline.endLsn 非法十六进制 LSN 文本构造期 IAE（fail-fast 优于运行期跳错位）。 */
@Test
void offlineEndLsnRejectsGarbage() {
    Properties p = propsWith(WalSource.KEY_OFFLINE_END_LSN, "not-an-lsn");
    assertThrows(IllegalArgumentException.class, () -> new WalSource(p));
}

/** mode=offline 而 offline.dir 缺失——startOffline 启动期 ISE（spec §5）。 */
@Test
void startOfflineWithoutDirFails() {
    WalSource s = new WalSource(new Properties());   // 无 offline.dir
    IllegalStateException e = assertThrows(IllegalStateException.class, s::startOffline);
    assertTrue(e.getMessage().contains("vb.wal.offline.dir"));
}
```

（`propsWith` 现有辅助或同型新建；WalConfigFileTest 增两键白名单断言。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalSourceConfigTest`
Expected: 编译失败——无 `KEY_OFFLINE_DIR`/`startOffline`。

- [ ] **Step 3: 实现**——`WalSource` 增两键常量 + 构造器解析（`offlineDir` 可 null；`offlineEndLsn` 经 `Lsn.parse` 缺省 0）；`startOffline()`：`started/closed` 单程闸门复用 → `offlineDir == null` ISE → `sync = CatalogSynchronizer.startOffline(state, offlineDir, offlineEndLsn, tables, dmlOut)`（`dmlEnabled()` 为 false 时传 null——纯 catalog 形态）→ `started = true` + INFO 一行（offline 档、目录、起点）；`eofReached()` 透传 `sync == null ? false : sync.eofReached()`。**不建 SQL 会话、不关 sqlConnection**（close() 的 sqlConnection null 安全已是现状）。`Main`：`collectConfig` 补两键拷贝；main 里读 `vb.wal.mode`（缺省 online，非法值 ISE）分派 `startOffline()`/`start()`；周期循环分片醒来增 `if (source.eofReached()) { LOG.info("离线回放 EOF，优雅退出"); source.close(); return; }`（在线档 `eofReached()` 恒 false——行为零变化）。`WalConfigFile.KNOWN_KEYS` 增 `KEY_OFFLINE_DIR`/`KEY_OFFLINE_END_LSN`（`vb.wal.mode` 是 Main 层分派键不入门面白名单——**不进 Properties**，Main 直读 `System.getProperty`）。

- [ ] **Step 4: 跑测试确认通过**（WalSourceConfigTest + WalConfigFileTest 全组）

Run: `mvn test -pl vb-stream-wal-source -Dtest='WalSourceConfigTest,WalConfigFileTest'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/
git commit -m "feat(wal-source): WalSource.startOffline 门面 + Main vb.wal.mode/offline.dir/offline.endLsn 三键 + EOF 优雅退出（exit 0），在线档循环零变化 (offline-replay)"
```

---

### Task 5: VastOfflineIT 真库手动档 4 用例 + WalSegmentRecorder 上移

**Files:**
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/WalSegmentRecorder.java`（自 `spike/offline-replay/WalSegmentRecorder.java` 上移：包声明改 `...it.vastbase`、凭据参数化自 `VastTestEnv`、javadoc 补测试工具定位）
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastOfflineIT.java`

**Interfaces:**
- Consumes: `VastTestEnv`（host/port/user/pass/db 与 `@EnabledIfSystemProperty(vb.it.vastbase=true)` 族契约）、Task 4 `WalSource.startOffline()`/`eofReached()`、`OutputRenderer`/`ChangeOutputListener`（CDC 行捕获用 List 版 listener）。
- Produces: 手动档 IT 4 用例（常规 `mvn test` 全 skip；`-Dvb.it.vastbase=true` + `-Dvb.it.vastbase.{host,port,db,user,pass}` 覆盖连接）。

- [ ] **Step 1: 写 IT**（4 用例骨架——完整断言面如下；真库不可杀，"库死亡"由"停 Main/WalSource + recorder 独立收集的段目录"承载，spike 第二轮形态）：

```java
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastOfflineIT {
    // 用例 1 在线 vs 离线按 xid 交集逐块全等（spike 流程 IT 化）：
    //   recorder 起收集（VastTestEnv 段目录）→ 门面 start()（direct 通道）在线跑两批 DDL/DML
    //   → 快照检查点 → 收完停 → startOffline 重放 → 两路 CDC 行（List<ChangeOutputListener>
    //   捕获）按 xid 交集逐行 assertEquals
    @Test void offlineReplayMatchesOnlineBaseline() { ... }

    // 用例 2 离线→在线回切：离线跑至 EOF（检查点推进）→ 同 state 目录再 start()（direct）
    //   → 写新数据 → 全部可见——双向互通（spike 实证衔接面）
    @Test void offlineToOnlineResumeInteroperates() { ... }

    // 用例 3 离线中崩溃再离线：离线中途 stop（未 EOF）→ 二次 startOffline 自离线检查点
    //   续传 → 对拍全等（at-least-once）
    @Test void offlineCrashAndRelayoutRepeatsConsistently() { ... }

    // 用例 4 endLsn 上界：指定在线前沿的中间位点 → 离线在该位截停 + lastCheckpointLsn
    //   ≤ endLsn 且 eofReached
    @Test void offlineEndLsnBoundsReplay() { ... }
}
```

（对拍归一化：两路 `ChangeOutputListener` 事件面收集 `RowChange` 渲染文本按 `BatchBegin.xid` 分桶取交集逐行断言——spike `ParityDiff` 思路进 JUnit，不落文件。）

- [ ] **Step 2: 编译 + 离线单测全量回归**（本机无真库——IT 断言面归手动档）

Run: `mvn test -pl vb-stream-wal-source -Dtest='!VastOfflineIT'`
Expected: 编译通过、全绿（VastOfflineIT skip 计数 +4）

- [ ] **Step 3: （用户环境）手动档验证**

Run: `mvn test -pl vb-stream-wal-source -Dtest=VastOfflineIT -Dvb.it.vastbase=true`
Expected: 4 用例 PASS（真库 172.16.107.133:55433/test_a；测试自建 `t_offline_it` 表与专用槽、finally 清理）

- [ ] **Step 4: Commit**

```bash
git add vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/
git commit -m "test(wal-source): VastOfflineIT 真库手动档 4 用例——在线/离线对拍全等 + 双向回切 + 崩溃再离线 + endLsn 截停；WalSegmentRecorder 上移测试树 (offline-replay)"
```

---

### Task 6: 文档收口

**Files:**
- Modify: `vb-stream-wal-source/CLAUDE.md`（新"离线回放档"节 + 配置表 +3 键 + 测试面对账）
- Modify: `vb-stream-wal-source/README.md`（快速开始补 offline 模式一段）
- Modify: `CLAUDE.md`（根：wal-source 段一句 + 测试用例数更新）
- Modify: `docs/superpowers/specs/2026-10-15-wal-source-offline-replay-design.md`（状态行 → 已实施）
- Modify: `docs/wal-source-learning-guide.md`（§2.4 后补"离线回放档"小节）

- [ ] **Step 1: 更新五处文档**——离线档节内容照 spec §0-§5 收口（场景矩阵/装配架构/审计三项/EOF 语义/错误表/已知限制 TOAST 窄缝）；测试面数字按实际对账（离线单测 +N、手动档 +4）。

- [ ] **Step 2: 全量回归 + 模块测试计数对账**

Run: `mvn test -pl vb-stream-wal-source`
Expected: 全绿；计数与文档一致

- [ ] **Step 3: Commit + push**

```bash
git add CLAUDE.md vb-stream-wal-source/CLAUDE.md vb-stream-wal-source/README.md docs/
git commit -m "docs(wal-source): 离线回放档文档收口——模块/根 CLAUDE.md/README/spec 状态/学习手册五面 + 测试计数对账 (offline-replay)"
git push
```

---

## Self-Review 记录

- **Spec 覆盖**：§3.1→Task 1；§3.3→Task 2；§3.2→Task 3；§3.4→Task 4（Main 三键 + EOF exit 0 + `og.*` 离线忽略——Task 4 Step 3 的 mode 分派天然不触碰 og 键）；§4 数据流→Task 3/4 组合；§5 错误表→Task 2/3/4 各 ISE 面；§6 测试面→Task 1-5（离线单测 + 手动档 IT）；§7/§8 非目标与风险无实施面。
- **占位符扫描**：Task 5 IT 用例体为结构骨架（4 方法签名 + 流程注释 + 断言锚）——手动档 IT 的完整体量在执行时按 `VastLifecycleIT` 既有骨架展开，断言面已钉（xid 交集逐行/endLsn 截停/双向回切）。其余任务代码块完整。
- **类型一致性**：`staticDirectory(Path,long,long,int,long)`/`eofReached()`/`startOffline` 双签名（CatalogSynchronizer 5 参 / WalSource 0 参）跨任务一致；`StoredState.pgVersion()` 末位组件约定全计划一致。
