# 事务组装缓冲溢写 Chronicle Queue 实施计划（里程碑 1.6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 组装器缓冲内存有界化——小事务纯内存、越过阈值溢写 Chronicle Queue（原始 pgoutput 字节单格式），两种模式输出严格等价。

**Architecture:** 消息以原始字节进入新的 raw 接缝；live 侧只解码控制消息与 Relation（版本日志），payload（I/U/D/T/M）以 `(bytes, seq, streamXid)` 单元入桶（MEMORY=堆 / SPILLED=CQ）；提交时回放桶单元 → 逐条解码 → 按 seq 取"当时"Relation 版本 → 构造 Transaction。零序列化组件（复用 PgOutputDecoder 本身）。

**Tech Stack:** Java 17、pgjdbc、chronicle-queue 2026.6（已在 pom）、JUnit 6、Testcontainers、JMH 1.37（新 profile）。

**设计文档（权威）：** `docs/superpowers/specs/2026-08-27-assembly-spill-design.md`

## Global Constraints

- Java 17：**禁止 record-pattern switch**（Java 21 特性），用 instanceof 链（现有 `TransactionAssembler` 同款约束）
- 日志一律 slf4j（`LOG = LoggerFactory.getLogger(...)`），禁止 System.out/err；`{}` 占位符；CDC 数据走 `org.vastdata.vbstream.cdc` logger（INFO），生命周期 INFO / 可恢复 WARN / 失败 ERROR / 逐消息 DEBUG
- 每个函数（含私有/测试辅助）按 CLAUDE.md 规约写 javadoc：职责/关键步骤/边界与异常语义/线程约束。本计划代码片段为省篇幅从简，**实现时必须补全**
- 每任务完成即 `git commit + push`（跨机开发）
- 验证编译用 `mvn clean test-compile`（增量编译有假绿陷阱，见记忆）；跑测试 `mvn test -Dtest=Xxx`
- 集成测试沿用 `SessionHarness`"先 close 后断言"顺序契约
- 线程模型：所有新组件默认**单写者**（run 循环线程）调用，javadoc 注明；`Transaction`/`PgOutputMessage` record 不可变可跨线程

## 关键既有签名（实现者需要知道的现状）

- `PgOutputDecoder.decode(ByteBuffer)` → `PgOutputMessage`；内部 `inStream` 状态机（'S' 置位/'E' 复位），块内 M/R/Y/I/U/D/T 前置 Int32 xid
- `PgOutputListener.onMessage(PgOutputMessage, RelationRegistry)`（现行接缝，里程碑 2 输出队列仍将使用解码后契约）
- `RelationRegistry`（final，ConcurrentHashMap）：`accept(msg)` / `find(oid)` / `require(oid)`（miss 即 IllegalStateException）
- `TransactionListener.onTransaction(Transaction)`；`Transaction(xid, kind, gid, commitLsn, endLsn, commitTimestamp, List<TxChange>)` 不可变
- `TxChange` 密封族：`RowChange(dml, relation, Optional<TupleData> before, Optional<TupleData> after, OptionalLong streamXid)` / `TruncateChange(List<Relation> snapshots, EnumSet<TruncateOption>, OptionalLong)` / `MsgChange(boolean transactional, String prefix, byte[] content, OptionalLong)`
- `PgReplicationSession.run(PgOutputListener)`：readPending 轮询 → decode → registry.accept → 回调；LSN 反馈在内
- 协议字节布局（附录 A 摘要，所有测试字节构造依据）：整数 big-endian；CString = UTF-8 + `\0`；元组列 `I16 列数` + 每列 `'n'`(NULL)/`'u'`(TOAST 未变)/`'t'`+I32+UTF8/`'b'`+I32+bytes；时间戳 = 距 2000-01-01 UTC 微秒（纪元偏移 946684800 秒）

---

### Task 1: spec §4 精化修订——回放免路由

**Files:**
- Modify: `docs/superpowers/specs/2026-08-27-assembly-spill-design.md`

**背景：** 计划细化发现比 spec §4.1-4.3 更简的机制——live 路由时各桶**只存自己的 payload 单元**（每单元带 seq 与 streamXid），外来消息在 live 就被分流，回放无需再 peek 路由；SPILLED 单元在 CQ 里带 9/13 字节信封帧（seq + streamXid），堆内零元数据。"live 路由与回放路由是同一套代码"改为"回放不路由"。Relation/Type/Origin 字节不入桶（'R' 进版本日志，'Y'/'O' 丢弃）。

- [ ] **Step 1: 修订 spec §3/§4/§5**
  - §3 组件表 `MessagePeek` 行改为：**payload 路由窥探内置于组装器**（读类型字节 + 流块内 Int32 xid 前缀；inStream 状态由 live 解码的 StreamStart/StreamStop 驱动，不需独立状态机）；新增 `PayloadUnit(byte[] payload, long seq, OptionalLong streamXid)` 与 `SpoolFrame`（纯函数帧/解帧）两行
  - §4.1 改写为："桶存储只含本桶 payload 单元（I/U/D/T/M），live 路由已按流块上下文分流外来消息；回放 = 遍历单元 → 跳过 abortedSubxids → `decodeSingle` → 构造 TxChange，无需路由"；§4.2 live 流程表 'R' 行保持；补 'Y'/'O' 丢弃行；§4.3 提交路径同步（去掉 peek 过滤，改为 abortedSubxids 过滤）
  - §4.4 补一句：`PgOutputDecoder` 新增 `decodeSingle(ByteBuffer, boolean inStream)` 重载（单元自带前缀有无信息，免 S/E 包裹）
  - §5 补一句：SPILLED 单元入队带信封帧（seq+xid+payload），堆内不保留逐单元元数据
  - §11 交付物修订：`PgOutputListener`（解码后契约）**保留不改**——旧接缝由 `DecodedMessageBridge` 承接给里程碑 2 输出队列与既有测试；新增的是 `RawMessageListener` 并非替换语义
- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-08-27-assembly-spill-design.md
git commit -m "docs(spec): §4 精化——桶单元免路由回放与信封帧（计划阶段细化）"
git push
```

---

### Task 2: SpillConfig

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/SpillConfig.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/SpillConfigTest.java`

**Interfaces:**
- Produces: `record SpillConfig(long thresholdBytes, Path dir, RollCycle rollCycle)`；`static SpillConfig fromSystemProperties()`（`vb.spill.thresholdBytes` 默认 67108864、`vb.spill.dir` 默认 `spill-queue`、`vb.spill.rollCycle` 默认 `RollCycles.MINUTELY`，枚举名大小写宽容）；`boolean spillEnabled()`（thresholdBytes > 0）

- [ ] **Step 1: 写失败测试**

```java
@Test
void defaultsMatchSpec() {
    SpillConfig c = SpillConfig.fromSystemProperties();
    assertEquals(64L * 1024 * 1024, c.thresholdBytes());
    assertEquals(Path.of("spill-queue"), c.dir());
    assertEquals(RollCycles.MINUTELY, c.rollCycle());
    assertTrue(c.spillEnabled());
}

@Test
void overridesAndDisabled() {
    System.setProperty("vb.spill.thresholdBytes", "0");
    System.setProperty("vb.spill.dir", "/tmp/x");
    System.setProperty("vb.spill.rollCycle", "hourly");
    SpillConfig c = SpillConfig.fromSystemProperties();
    assertFalse(c.spillEnabled());          // ≤0 = 纯内存逃生门
    assertEquals(RollCycles.HOURLY, c.rollCycle());  // 大小写宽容
    // 测试尾清理 System.clearProperty(...)（@AfterEach）
}

@Test
void unknownRollCycleFailsFast() {
    System.setProperty("vb.spill.rollCycle", "nope");
    assertThrows(IllegalArgumentException.class, SpillConfig::fromSystemProperties);
}
```

- [ ] **Step 2: 跑测试确认失败**（类不存在编译错即红）
- [ ] **Step 3: 实现**（record + 静态工厂；`RollCycle` 类型为 `net.openhft.chronicle.queue.RollCycle`；枚举解析 `Arrays.stream(RollCycles.values()).filter(rc -> rc.name().equalsIgnoreCase(s)).findFirst().orElseThrow`，未识别抛 IllegalArgumentException 带可用值列表；javadoc 按 Global Constraints 补全）
- [ ] **Step 4: `mvn test -Dtest=SpillConfigTest` 绿**
- [ ] **Step 5: Commit** `feat(spill): SpillConfig——阈值/目录/滚动周期配置`

---

### Task 3: PgOutputDecoder.decodeSingle 重载

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/protocol/PgOutputDecoder.java`
- Test: `src/test/java/org/vastdata/vbstream/protocol/PgOutputDecoderTest.java`（追加用例）

**Interfaces:**
- Produces: `public PgOutputMessage decodeSingle(ByteBuffer payload, boolean inStream)`——按给定 inStream 解码**单条**消息，不改变实例状态；仅允许 7 类可带前缀消息（M/R/Y/I/U/D/T），其他类型抛 `IllegalArgumentException`。原 `decode(ByteBuffer)` 行为不变

- [ ] **Step 1: 失败测试**（构造一条带 xid 前缀的 Insert 字节，`decodeSingle(wrap(bytes), true)` 得到 `streamXid=OptionalLong.of(xid)` 的 Insert；同字节 `decodeSingle(bytes, false)` 抛 ProtocolMisalignmentException——按无前缀解析后续错位；`decodeSingle` 传入 'B' 字节抛 IllegalArgumentException；调用前后 decoder 实例的连续 `decode` 流状态不受污染——先 decodeSingle 再 decode 一对 S..E 内消息验证）

```java
@Test
void decodeSingleReadsPrefixWithoutState() {
    PgOutputDecoder d = new PgOutputDecoder(StreamingMode.ON);
    byte[] ins = concat(new byte[]{'I'}, i32(7), tupleN(textCol("v")));       // 无前缀体
    byte[] streamed = concat(new byte[]{'I'}, i32(99), i32(7), tupleN(textCol("v"))); // 前缀 xid=99
    assertEquals(OptionalLong.of(99), ((PgOutputMessage.Insert) d.decodeSingle(ByteBuffer.wrap(streamed), true)).streamXid());
    assertNotEquals(OptionalLong.of(99), ((PgOutputMessage.Insert) d.decodeSingle(ByteBuffer.wrap(ins), false)).streamXid());
    assertThrows(IllegalArgumentException.class, () -> d.decodeSingle(ByteBuffer.wrap(beginBytes()), false));
}
```
  （`concat/i32/tupleN/textCol/beginBytes` 等字节助手在本测试类已有同类内联构造手法，照 `StreamParsersTest` 风格本地实现为 private static）

- [ ] **Step 2: 确认失败**（方法不存在）
- [ ] **Step 3: 实现**——重构：现 `dispatch(byte, ByteBufferReader)` 改签名为 `dispatch(byte type, ByteBufferReader r, boolean inStream)`；`streamXid(r)` 改收参数；`decode(ByteBuffer)` 在调用前用字段 `inStream` 并保留 'S'/'E' 副作用；`decodeSingle` 用局部 boolean（'S'/'E'/两阶段控制类型直接 IllegalArgumentException），后置 remaining==0 检查复用
- [ ] **Step 4: `mvn test -Dtest=PgOutputDecoderTest` + 全 protocol 包测试绿**
- [ ] **Step 5: Commit** `feat(protocol): decodeSingle 重载——单条消息带显式 inStream 解码，回放免 S/E 包裹`

---

### Task 4: VersionedRelationRegistry

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/RelationRegistry.java`（去 final，javadoc 补"可继承用于版本化扩展"）
- Create: `src/main/java/org/vastdata/vbstream/replication/VersionedRelationRegistry.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/VersionedRelationRegistryTest.java`

**Interfaces:**
- Produces: `class VersionedRelationRegistry extends RelationRegistry`：
  - `void accept(long seq, PgOutputMessage.Relation rel)`——按 oid 追加版本（同 seq 重复接受幂等跳过）
  - `Relation require(int relationOid, long asOfSeq)`——二分取 ≤asOfSeq 最新版；该 oid 完全无版本或全部版本 > asOfSeq → IllegalStateException（"Relation 未先行到达"语义）
  - `void pruneBelow(long minSeq)`——各 oid 丢弃 < minSeq 的版本（保留恰好 == minSeq 的；每个 oid 至少保留最新一条防过度剪枝）
  - 继承的 `accept(msg)/find/require(oid)` 委托最新版本（渲染与旧接缝兼容）

- [ ] **Step 1: 失败测试**

```java
private final VersionedRelationRegistry reg = new VersionedRelationRegistry();
private static PgOutputMessage.Relation rel(int oid, String name) {
    return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", name,
            'd', List.of(new PgOutputMessage.Column("c", 25, -1, true)));
}

@Test
void asOfTakesLatestVersionAtOrBeforeSeq() {
    reg.accept(10, rel(1, "v1"));
    reg.accept(50, rel(1, "v2"));
    assertEquals("v1", reg.require(1, 49).table());   // 边界：恰在切换前
    assertEquals("v2", reg.require(1, 50).table());   // 边界：恰在切换 seq 上
    assertEquals("v2", reg.require(1, 999).table());
}

@Test
void missingOrFutureVersionFailsFast() {
    reg.accept(10, rel(1, "v1"));
    assertThrows(IllegalStateException.class, () -> reg.require(2, 99));   // 无版本
    assertThrows(IllegalStateException.class, () -> reg.require(1, 9));    // 全部在未来
}

@Test
void pruneKeepsAtLeastLatestAndExactBoundary() {
    reg.accept(10, rel(1, "v1")); reg.accept(50, rel(1, "v2"));
    reg.pruneBelow(50);
    assertEquals("v2", reg.require(1, 60).table());
    assertThrows(IllegalStateException.class, () -> reg.require(1, 49));
    reg.pruneBelow(10_000);                       // 过度剪枝保护
    assertEquals("v2", reg.require(1, 10_001).table());
}

@Test
void inheritedLatestViewForLegacyContract() {
    reg.accept(10, rel(1, "v1")); reg.accept(50, rel(1, "v2"));
    assertEquals("v2", reg.require(1).table());   // 旧签名 = 最新版
}
```

- [ ] **Step 2: 确认失败** → **Step 3: 实现**（`HashMap<Integer, ArrayList<Version>>`，Version 为私有 record(seq, rel)；单写者假设写 javadoc；查找 `Collections.binarySearch` 或手写二分按 seq）
- [ ] **Step 4: `mvn test -Dtest=VersionedRelationRegistryTest,RelationRegistryTest` 绿**
- [ ] **Step 5: Commit** `feat(registry): 版本日志 registry——oid→(seq,Relation) 序列，asOf 二分取变更时刻版本`

---

### Task 5: PayloadUnit 与 SpoolFrame（纯函数帧）

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/PayloadUnit.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/SpoolFrame.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/SpoolFrameTest.java`

**Interfaces:**
- Produces:
  - `record PayloadUnit(byte[] payload, long seq, OptionalLong streamXid)`（payload 为**含类型字节与可选 xid 前缀的完整消息字节**；无前缀单元 decodeSingle(buf,false)，有前缀 decodeSingle(buf,true) 且前缀值 == streamXid）
  - `SpoolFrame.frame(PayloadUnit u) -> byte[]`：`[I64 seq][I8 xidPresent][I32 xid?][payload]`（big-endian，xidPresent 0/1）
  - `SpoolFrame.unframe(byte[] framed) -> PayloadUnit`；帧长度不足/结构非法抛 `IllegalArgumentException`

- [ ] **Step 1: 失败测试**（round-trip 有/无 xid 两例；内容全零/超长 payload 正常；非法帧：首 9 字节不足、xidPresent 非 0/1、声明长度超界 → IllegalArgumentException）
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（`ByteBuffer.allocate/allocate(...).order(BIG_ENDIAN)` 手工读写，约 40 行）
- [ ] **Step 4: 测试绿** → **Step 5: Commit** `feat(spill): PayloadUnit 与 SpoolFrame——SPILLED 单元信封帧（seq+xid+payload），堆内零逐单元元数据`

---

### Task 6: MessageSpool（CQ 生命周期）

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/MessageSpool.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/MessageSpoolTest.java`（用 `@TempDir`）

**Interfaces:**
- Consumes: `SpoolFrame`（Task 5）、`SpillConfig.rollCycle()` 的 `RollCycle` 类型
- Produces: `final class MessageSpool implements AutoCloseable`：
  - `MessageSpool(Path dir, RollCycle rollCycle)`——**先清空 dir 既有内容再建队列**（瞬态工作区语义：重启后复制槽重发，杜绝陈旧状态；删除失败抛 IOException 包装的 UncheckedIOException）
  - `long append(byte[] framed)`——写一条，返回 CQ index（单调）
  - `void readRange(long firstIndex, long lastIndex, ObjIntConsumer<byte[]> framedConsumer)`——按 index 升序回读 [first..last] 闭区间的 framed 字节（`tailer.direction(FORWARD).moveToIndex(first)` 起步，读到 index > last 停；moveToIndex 落点可能 > first 时 fail-fast IllegalStateException——区间必须存在）
  - `long releaseBelow(long lowestNeededIndex)`——删除**严格低于** `rollCycle.toCycle(lowestNeededIndex) - 1` 的 cycle 滚动文件（保留当前与上一档；文件名解析失败的跳过并 WARN；每删一个 WARN 文件名）；返回实际删除数
  - `long lastAppendedIndex()`；`void close()`（tailer→appender→queue 逆序 release，失败 WARN 不上抛）
  - 包私有静态纯函数 `static List<Path> deletableFiles(RollCycle rc, Path dir, long neededCycle)`——供单测注入文件名列表验证删除数学

- [ ] **Step 1: 失败测试**

```java
@Test
void appendReadRangeRoundTrip(@TempDir Path dir) {
    try (MessageSpool spool = new MessageSpool(dir, RollCycles.MINUTELY)) {
        long a = spool.append(new byte[]{1});
        long b = spool.append(new byte[]{2});
        long c = spool.append(new byte[]{3});
        List<byte[]> got = new ArrayList<>();
        spool.readRange(a, c, (framed, idx) -> got.add(framed));
        assertArrayEquals(new byte[][]{{1},{2},{3}}, got.toArray(new byte[0][]));
        spool.readRange(b, c, (framed, idx) -> assertEquals(2, framed[0]));
    }
}

@Test
void reopenWipesStaleContent(@TempDir Path dir) {
    try (MessageSpool s1 = new MessageSpool(dir, RollCycles.MINUTELY)) { s1.append(new byte[]{9}); }
    try (MessageSpool s2 = new MessageSpool(dir, RollCycles.MINUTELY)) {
        List<byte[]> got = new ArrayList<>();
        s2.readRange(0, 100, (f, i) -> got.add(f));   // 旧内容不存在：0 起步读不到即空
        assertTrue(got.isEmpty());
    }
}

@Test
void releaseBelowNeverTouchesRecentCycles(@TempDir Path dir) throws IOException {
    // 注入式删除数学：构造假滚动文件名（MINUTELY 命名 YYYYMMDD-HHMM.cq4）
    Files.writeString(dir.resolve("20260101-0000.cq4"), "x");
    Files.writeString(dir.resolve("20260101-0001.cq4"), "x");
    Files.writeString(dir.resolve("20260101-0002.cq4"), "x");
    // neededCycle=2（20260101-0002 的 cycle 号）→ 只删 cycle 0 一档（保留 needed 与 needed-1）
    List<Path> doomed = MessageSpool.deletableFiles(RollCycles.MINUTELY, dir, 2);
    assertEquals(List.of(dir.resolve("20260101-0000.cq4")), doomed);
}

@Test
void liveReleaseBelowEndToEnd(@TempDir Path dir) throws IOException {
    try (MessageSpool spool = new MessageSpool(dir, RollCycles.MINUTELY)) {
        spool.append(new byte[]{1});
        assertEquals(0, spool.releaseBelow(spool.lastAppendedIndex())); // 当前 cycle，无文件可删
    }
}
```
  注：MINUTELY cycle 号 = 距默认纪元（1970-01-01 UTC）的分钟数；`20260101-0000` 对应 cycle = (Instant "2026-01-01T00:00Z" 距纪元分钟数)，测试里用 `Duration.between(Instant.EPOCH, Instant.parse("2026-01-01T00:00:00Z")).toMinutes()` 计算硬数字并直接传数字（保持断言可读）。

- [ ] **Step 2: 确认失败** → **Step 3: 实现**（`SingleChronicleQueue.builder(dir).rollCycle(rollCycle).build()`；appender/tailer 字段单写者；`appender.writeBytes(BytesStore.wrap(framed))`，index 取 `appender.lastIndexAppended()`；**实现时若发现文件命名与 YYYYMMDD-HHMM 不符，先写一个列出真实文件名的探针测试再修正 deletableFiles 的解析**——以真实为准，勿臆断）
- [ ] **Step 4: `mvn test -Dtest=MessageSpoolTest` 绿**
- [ ] **Step 5: Commit** `feat(spill): MessageSpool——CQ 单例/append/区间回读/低水位删滚动文件/瞬态清空语义`

---

### Task 7: 会话接缝改造（raw 契约 + 旧契约桥）

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/RawMessageListener.java`
- Create: `src/main/java/org/vastdata/vbstream/replication/DecodedMessageBridge.java`
- Modify: `src/main/java/org/vastdata/vbstream/replication/PgReplicationSession.java`（run 签名与内部）
- Modify: `src/main/java/org/vastdata/vbstream/Main.java`（临时用桥保持行为不变）
- Modify: `src/test/java/org/vastdata/vbstream/it/SessionHarness.java`（内部接桥，对外 API 不变 + 新增 rawMessages()）
- Test: `src/test/java/org/vastdata/vbstream/it/RawSessionContractTest.java`（新，验证 raw 字节与解码消息一一对应）

**Interfaces:**
- Produces:
  - `@FunctionalInterface RawMessageListener { void onRaw(byte[] raw) }`——raw 为完整单条 pgoutput 消息字节（含类型字节与可选 xid 前缀）；回调线程 = run 线程，同步
  - `DecodedMessageBridge implements RawMessageListener`——ctor `(PgOutputListener target, StreamingMode mode)`；自持 `PgOutputDecoder` 与 `RelationRegistry`；`onRaw` = decode(wrap) → registry.accept → target.onMessage；`RelationRegistry registry()` 暴露。**旧契约（PgOutputListener）原样保留给里程碑 2 与既有测试**
  - `PgReplicationSession.run(RawMessageListener listener)`（**签名替换**）：`payload.remaining() > 0` 时拷贝 byte[] 再回调；decoder/registry 移出 session
  - `SessionHarness.rawMessages()`——`List<byte[]>`（close 后才可做确定性全量断言，同既有顺序契约）

- [ ] **Step 1: 失败测试**——`RawSessionContractTest`（Testcontainers，沿用 `PgTestEnv` 模式）：起 harness，另一连接 `INSERT` 两行 + `COMMIT`，close 后断言：`rawMessages().size() == messages().size()`；每条 raw 首字节是合法类型字符；`new DecodedMessageBridge((m,r)->{}, mode)` 重放 raw 流得到的消息 equals 逐条等于 `messages()`（record 值相等）
- [ ] **Step 2: 确认失败**（方法不存在）
- [ ] **Step 3: 实现**——session.run 内：`ByteBuffer payload = stream.readPending(); if (payload != null && payload.remaining() > 0) { byte[] raw = new byte[payload.remaining()]; payload.get(raw); listener.onRaw(raw); }`（LSN 反馈与轮询节奏不动）；Main 临时改为 `session.run(new DecodedMessageBridge((msg, registry) -> { console.onMessage(msg, registry); assembler.accept(msg, registry); }, config.streamingMode()))`——**行为与之前逐字节等价**；SessionHarness worker 改为 `session.run(raw -> { raws.add(raw); bridge.onRaw(raw); })`，bridge 的 target 做 decoded 录制与停止条件（`messages`/`rawMessages` 均 CopyOnWriteArrayList）
- [ ] **Step 4: `mvn clean test`——全部既有 IT 必须绿（这是本任务的核心验收：接缝替换零行为漂移）**
- [ ] **Step 5: Commit** `feat(session): raw 消息接缝——run 交付原始字节，DecodedMessageBridge 保旧解码契约，harness 双轨录制`

---

### Task 8: 组装器重写 I——桶模型与控制路由（纯 MEMORY 路径）

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/replication/PgWire.java`（测试字节构造助手）
- Rewrite: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`
- Test: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`（**重写**——33 例语义全部移植为字节驱动）

**Interfaces:**
- Consumes: `RawMessageListener`（Task 7）、`VersionedRelationRegistry.accept(seq, rel)`（Task 4）、`PayloadUnit`（Task 5）
- Produces: `final class TransactionAssembler implements RawMessageListener, AutoCloseable`：
  - ctor `TransactionAssembler(TransactionListener listener, StreamingMode mode, VersionedRelationRegistry registry, SpillConfig spill, Consumer<PgOutputMessage> decodedObserver)`（本任务先落 **MEMORY-only 骨架**：spill 暂不接线——内部 `MessageSpool` 惰性创建留 Task 10 接口位，本任务 `close()` 空实现）；便捷 ctor 去掉 observer（`msg -> {}`）
  - `void onRaw(byte[] raw)`——契约见下
  - `void close()`（本任务 no-op，Task 10 实装）
  - 本任务外部可见行为：**与旧组装器逐语义等价**（等价基线 = 既有 33 例移植后全绿）
- `PgWire`（测试助手，static 方法，返回 byte[]）：`begin(xid) commit() insert(oid, newTuple) update(oid, oldTag, oldTuple, newTuple) delete(oid, tag, tuple) truncate(int[] oids, byte opts) logicalMsg(boolean tx, String prefix, byte[] content) relation(oid, table, String... colNames) streamStart(xid, first) streamStop() streamCommit(xid) streamAbort(xid, sub) beginPrepare(xid, gid) prepare(xid, gid) commitPrepared(xid, gid) rollbackPrepared(xid, gid) streamPrepare(xid, gid) textCol(String) nullCol() tuple(String...)`（布局按 Global Constraints 的附录 A 摘要；流块内 payload 前缀由各 insert/update/delete/truncate/logicalMsg 的 `streamed(long xid)` 变体包装：`concat(type, i32(xid), body)`；LSN/时间戳字段一律填 1/2/3 递增占位，commitTimestamp 微秒占位 0 → 断言用 `Instant.ofEpochSecond(946684800L)`）

**onRaw 路由契约（本任务实现的控制面）：**
```
seq = nextSeq++（单调，从 1 起）
switch raw[0]：
  'B' → begin：桶 null 校验 fail-fast，开 NORMAL 桶（xid 来自解码）
  'C' → commit：桶 miss fail-fast；replay → Transaction(NORMAL, commitMeta) 回调（replay 本任务先返回空 changes 占位实现？——否：本任务就绪 MEMORY 回放私有方法遍历 units 空列表即可，Task 9 才接 BucketReplayer；为保 33 例绿，本任务必须已能产出 changes —— 改为：本任务直接内联最小回放（decode + require(oid, seq) + RowChange/TruncateChange/MsgChange 构造），Task 9 再抽取为 BucketReplayer。**决定：本任务实现完整回放逻辑，Task 9 只做抽取+增强测试**）
  'S'/'E' → currentStream 指针维护（first/continue/miss 等全部 fail-fast 语义照旧移植）
  'c' → streamCommit：桶 miss/流块未闭合 fail-fast；replay → Transaction(STREAMED)
  'A' → streamAbort：top==sub → 整桶丢弃（含其存储，Task 10 接 spool 释放记账）；否则 abortedSubxids.add(sub)
  'b'/'P'/'p'/'K'/'r' → 2PC 四桶流转照旧移植；'K' replay → Transaction(TWO_PHASE, K 的 meta)；'r' 丢弃不回调 WARN
  'I'/'U'/'D'/'T' → 桶路由（currentStream ? 该桶 : prepare/normal 桶，miss fail-fast）→ 存 PayloadUnit（streamXid = currentStream!=null ? 前缀 Int32 : empty；前缀值即 raw[1..4] 无符号）
  'M' → 同上，但先窥 flags bit0（offset = currentStream!=null ? 5 : 1）：非事务性且无任何活动桶 → WARN 丢弃（旧语义）
  'R' → decoder 解码 → registry.accept(seq, rel)；不入桶
  'Y'/'O' → DEBUG 记录后丢弃（旧组装器忽略语义）
  未知类型字节 → decoder 抛 UnknownMessageTypeException（fail-fast 由解码层承担）
每个解码点（控制消息 + 'R' + 回放内）执行 decodedObserver.accept(msg)（ConsoleListener 逐消息 DEBUG 的新挂点）
```
桶结构（私有）：`TxBuffer { long xid; String gid; final List<PayloadUnit> units; long bytesTotal; final Set<Long> abortedSubxids; }`——本任务无 SPILLED 字段（Task 10 加）。

- [ ] **Step 1: 先写 PgWire + 2 个冒烟失败测试**（begin→insert→commit 产出含 1 条 RowChange 的 NORMAL Transaction；commit 无桶 fail-fast）
- [ ] **Step 2: 确认失败** → **Step 3: 实现 PgWire + 组装器骨架（含完整回放）**，跑绿
- [ ] **Step 4: 移植 33 例**——逐组改写既有 TransactionAssemblerTest：普通事务（开/提交/未闭合 Begin 重复 fail/Commit 无桶 fail）、流式（首段建桶/续段 miss fail/段外 StreamStop·StreamCommit·StreamAbort fail/交错双桶/c 提交）、abort（sub 剔除/top 移除/Message 顶层 xid 不误剔）、2PC（b..P 入池/K 输出/r 丢弃/p 流式入池/ gid 重复 fail/P 不匹配 fail/池 miss fail）、LogicalMsg（事务性无桶 fail/非事务有桶随桶/非事务无桶 WARN 丢弃——用 logback ListAppender 断言或仅断言不抛不产出）、Type/Origin 透传不影响输出。**字节序列 = 旧测试的消息序列经 PgWire 逐一翻译，断言体不变**（Transaction record 值相等）
- [ ] **Step 5: `mvn test -Dtest=TransactionAssemblerTest,TransactionModelTest` 绿；`mvn clean test` 全绿（旧 assembler 类已删，Main/ConsoleListener 若编译受影响按 Task 7 的桥先维持）**
- [ ] **Step 6: Commit** `feat(assembly)!: 组装器重写为 raw 驱动——桶持 PayloadUnit，控制消息 live 解码路由，MEMORY 回放（33 例语义移植）`

---

### Task 9: BucketReplayer 抽取与专项测试

**Files:**
- Create: `src/main/java/org/vastdata/vbstream/replication/BucketReplayer.java`
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`（回放逻辑移入）
- Test: `src/test/java/org/vastdata/vbstream/replication/BucketReplayerTest.java`（新）

**Interfaces:**
- Consumes: `decodeSingle`（Task 3）、`VersionedRelationRegistry.require(oid, asOfSeq)`（Task 4）
- Produces: `final class BucketReplayer`：
  - ctor `BucketReplayer(StreamingMode mode, VersionedRelationRegistry registry, Consumer<PgOutputMessage> decodedObserver)`
  - `List<TxChange> replay(Iterable<PayloadUnit> units, Set<Long> abortedSubxids)`——逐单元：`streamXid` 命中 abortedSubxids → 跳过；否则 `decodeSingle(wrap(payload), streamXid.isPresent())` + observer + 构造 TxChange（RowChange/TruncateChange/MsgChange；relation 一律 `registry.require(oid, unit.seq)` 取当时版本）；单元类型非 I/U/D/T/M → IllegalStateException

- [ ] **Step 1: 失败测试**（直接手造 PayloadUnit 列表，不经组装器）：
  - 正常 I/U/D 三单元 → 三 TxChange，relation 快照来自 require(oid, seq)
  - abortedSubxids 含某 sub → 该单元被剔除、其余保留
  - **asOf 版本正确性**：registry 预置同 oid 两版本（seq 10/50），单元 seq=30 → 取 v1；单元 seq=60 → 取 v2（DDL 中途换版不串位——spec §4.4 的机制级验证）
  - 单元字节为 'B' → IllegalStateException
- [ ] **Step 2: 确认失败** → **Step 3: 抽取实现**（组装器提交路径改为调用 replayer；行为无变化）
- [ ] **Step 4: `mvn test -Dtest=BucketReplayerTest,TransactionAssemblerTest` 绿**
- [ ] **Step 5: Commit** `refactor(assembly): 回放抽取为 BucketReplayer——asOf 快照/aborted 过滤/单消息解码，专项可测`

---

### Task 10: 组装器重写 II——阈值转储与 SPILLED 路径

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/TransactionAssembler.java`
- Modify: `src/test/java/org/vastdata/vbstream/replication/TransactionAssemblerTest.java`（追加混合模式组）

**Interfaces:**
- Consumes: `MessageSpool.append/readRange/releaseBelow/lastAppendedIndex`（Task 6）、`SpoolFrame.frame/unframe`（Task 5）、`SpillConfig.spillEnabled()`（Task 2）
- Produces（组装器新增行为）：
  - TxBuffer 增 `enum Mode { MEMORY, SPILLED }` + `long firstIndex/lastIndex`
  - 存储统一入口：MEMORY → units.add + bytesTotal；SPILLED → `spool.append(frame(unit))` 维护区间
  - **全局记账**：`memoryBytes()` = Σ MEMORY 桶 bytesTotal；每次 MEMORY 写入后 `if (spillEnabled && memoryBytes > threshold) spillAll()`
  - `spillAll()`：所有 MEMORY 桶逐单元 frame→append，置 SPILLED，清 units，INFO 一行（桶数/单元数/字节）
  - 开桶时 `memoryBytes >= threshold`（来自其它巨型桶）→ 直接 SPILLED 起步（仍空区间——首单元 append 时建立）
  - SPILLED 桶提交/2PC 回放：`spool.readRange(first, last, framed -> units.add(unframe(framed)))` 后走同一 replayer；abort 整桶丢弃时低水位候选推进
  - **低水位维护**：每次桶完结（提交回放后 / abort 丢弃 / 回滚丢弃）→ `spill.releaseBelow(min(存活 SPILLED 桶 firstIndex, lastAppended+1))`；spool 惰性创建（首次 spill 时 `new MessageSpool(config.dir(), config.rollCycle())` + INFO）；`spillEnabled()==false` 全路径短路（spool 永不创建）
  - `close()` → spool 非 null 则 close
  - **错误处理（spec §6）**：`spool.append` 的 CQ 写失败（磁盘满/IO，chronicle 抛运行时异常）自然沿 onRaw → run 循环上抛并 ERROR 收敛，不吞不重试；回放中 `unframe`/`decodeSingle` 异常同 fail-fast

- [ ] **Step 1: 失败测试**（SpillConfig.thresholdBytes 设 200 级别小值 + `@TempDir`）：
  - 小事务（阈值内）全程 MEMORY：断言输出 Transaction 与大阈值跑同一字节流完全相等（**等价性单测先行**）
  - 大事务跨阈值：输出仍与纯内存跑等价（同字节流、不同配置）
  - `spillAll` 触发后新开桶直接 SPILLED（巨型桶未完结期间）
  - SPILLED 桶 StreamAbort(sub)：回放剔除照旧（abortedSubxids 存于桶元数据，不依赖单元存储位置）
  - 2PC：桶 spill 后 PREPARE 挂起、跨很久后 COMMIT PREPARED 回放输出等价
  - spill 禁用（threshold=0）：`close()` 后 temp dir 无队列文件（spool 未创建）
  - 低水位推进：巨型桶 abort 后再提交小桶，`releaseBelow` 被调用（以 WARN/INFO 日志或包私有 getter 断言，或注入 fake spool——**实现提供包私有 `long spillWatermark()` 供断言**）
- [ ] **Step 2: 确认失败** → **Step 3: 实现**（注意：spillAll 期间当前正在追加的桶也在转储之列；转储后该桶后续写入走 SPILLED 分支；单写者无并发）
- [ ] **Step 4: `mvn clean test` 全绿**
- [ ] **Step 5: Commit** `feat(assembly): 混合缓冲——阈值记账/spillAll 转储/SPILLED 起步/tailer 回读/低水位清理，双模式输出等价`

---

### Task 11: Main / ConsoleListener 装配

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/Main.java`
- Modify: `src/main/java/org/vastdata/vbstream/ConsoleListener.java`（仅 javadoc：onMessage 调用时点变为"解码点"（控制/Relation live + payload 回放），渲染 registry 为最新版）

**实现要点：**
- Main：`SpillConfig spill = SpillConfig.fromSystemProperties()`；`VersionedRelationRegistry registry = new VersionedRelationRegistry()`；`try (TransactionAssembler assembler = new TransactionAssembler(console, config.streamingMode(), registry, spill, console::onMessage)) { session.run(assembler); }`（reader 线程内 try 块包 run；启动日志补 spill 阈值/目录/滚动周期）
- ConsoleListener.onMessage 保持 `(msg, registry)` 签名——observer 处传 `msg -> console.onMessage(msg, registry)`（闭包持 registry 引用；渲染用最新版，javadoc 注明）
- 逐消息 DEBUG 守卫（`CDC.isDebugEnabled()`）语义保持

- [ ] **Step 1: `mvn clean test` 绿（编译 + 既有测试）**
- [ ] **Step 2: 手动冒烟**（src/docker 已起）：跑 Main，另一会话执行 `src/main/resources/sql/04-streaming-large-txn.sql` 与 `05-streaming-abort.sql`，确认 TXN-BEGIN/END INFO 正常、`-Dvb.spill.thresholdBytes=65536` 时日志出现 spillAll INFO 与回放输出；把观察结果记入 commit message
- [ ] **Step 3: Commit + push** `feat(main): 装配 raw 组装器与 spill 配置——逐消息 DEBUG 挂解码点`

---

### Task 12: 集成测试——spill 等价性与专项场景

**Files:**
- Create: `src/test/java/org/vastdata/vbstream/it/AssemblySpillIT.java`
- Test 依据：`SessionHarness.rawMessages()`（Task 7）离线回放模式

**场景（每场景独立测试方法，共用 PG 容器）：**

- [ ] **Step 1: 等价性（核心验收）**——录制 `01-insert-types.sql` + 批量 UPDATE/DELETE 的 raw 流；close 后将**同一录制**喂两个组装器（threshold=64MB vs threshold=64KB + @TempDir），断言两边 `List<Transaction>` 完全相等（record 值相等；这是"spill 无损"的确定性证明——同字节流双配置对照，规避两次录制的数据随机性）
- [ ] **Step 2: 流式大事务 + spill**——复用 `TransactionAssemblyTest` 场景 2/4 的双连接交错手法 + `string_agg(md5(random()::text),'') FROM generate_series(1,512)` 不可压缩载荷（CLAUDE.md 领域要点），小阈值录制回放：断言 STREAMED Transaction 的 changes 与大阈值等价、StreamAbort 剔除后行数正确
- [ ] **Step 3: 并发 DDL asOf**——conn1 大事务（阈值以上）插入 t_assembly_types；中途 conn2 `ALTER TABLE ... ADD COLUMN` 提交；conn1 继续插入后提交。回放断言：前后段 `RowChange.relation().columns().size()` 各自正确（前=旧列数、后=旧+1），无列错位异常
- [ ] **Step 4: 大事务回滚清理**——录制：大事务（spill 后）ROLLBACK + 随后一个小事务 COMMIT；断言：输出仅含小事务、无异常、`assembler.spillWatermark()` 已推进（spilled 垃圾可回收）
- [ ] **Step 5: 既有 5 场景回归**——`mvn clean test` 全绿（TransactionAssemblyTest 等不经改动必须保持绿——行为不变式）
- [ ] **Step 6: Commit** `test(spill): 集成四场景——双配置等价性/流式交错/并发 DDL asOf/回滚低水位推进`

---

### Task 13: JMH 基准与语料

**Files:**
- Modify: `pom.xml`（`jmh` profile：`org.openjdk.jmh:jmh-core:1.37` + `jmh-generator-annprocess:1.37`，test scope）
- Create: `src/test/java/org/vastdata/vbstream/bench/CorpusLoader.java`（`[I32 len][bytes]...` 格式读写；`List<byte[]> load(Path)` / `void dump(Path, List<byte[]>)`）
- Create: `src/test/java/org/vastdata/vbstream/it/BenchCorpusRecordIT.java`（Docker 录制：跑 6 个场景脚本 → dump 到 `src/test/resources/bench-corpus/corpus.bin`；断言非空且含 ≥6 种类型字节）
- Create: `src/test/java/org/vastdata/vbstream/bench/DecodeBenchmark.java`（顺序态解码头推整语料，@Param 全 19 类型字节过滤可选）
- Create: `src/test/java/org/vastdata/vbstream/bench/RoutePeekBenchmark.java`（仅类型字节 + 可选 xid 前缀读取，对照 decode）
- Create: `src/test/java/org/vastdata/vbstream/bench/AssembleMemoryBenchmark.java`（threshold=∞ 组装整语料，listener no-op）
- Create: `src/test/java/org/vastdata/vbstream/bench/SpillPathBenchmark.java`（两 @Param 路径：MEMORY vs SPILLED——预构造 N=2000 单元桶，replay 计时；附 spool.append 吞吐）
- Create: `docs/benchmarks-baseline.md`（运行方法、环境、参数、结果表模板）

**Steps:**

- [ ] **Step 1: pom profile + CorpusLoader 测试**（round-trip；红→绿）
- [ ] **Step 2: BenchCorpusRecordIT**——跑一次生成语料并提交进库（几百 KB 二进制；`.gitattributes` 补 `*.bin binary`）；注意 `mvn test` 正常跑不依赖语料存在（基准类才读，缺失时 @Setup 抛带指引的异常）
- [ ] **Step 3: 四个基准类**（`@BenchmarkMode(Throughput/AverageTime)`、`@State(Scope.Thread)`、默认 `-f 1 -w 1s -r 2s` 由运行参数给，类上不硬编码 fork 数；SpillPath 用 @TempDir 语义的 @Setup/@TearDown 建/删队列）。代表骨架（其余三类同构替换计量体）：

```java
@BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class DecodeBenchmark {
    List<byte[]> corpus;              // @Setup: CorpusLoader.load(...)
    PgOutputDecoder decoder;          // = new PgOutputDecoder(StreamingMode.PARALLEL)

    /** 单位：一条消息平均解码耗时（µs）。 */
    @Benchmark
    public PgOutputMessage decodeOne() {
        byte[] raw = corpus.get(i = (i + 1) % corpus.size());
        return decoder.decode(ByteBuffer.wrap(raw));
    }
}
```
- [ ] **Step 4: 本机跑一轮冒烟档**（`mvn -Pjmh test-compile && java -cp "$(cat target/cp.txt):target/classes:target/test-classes" org.openjdk.jmh.Main "bench" -f 1 -w 1s -r 2s`，classpath 组装命令写进 baseline 文档），结果填入 `docs/benchmarks-baseline.md`（decode MB/s、peek/decode 比值、MEMORY vs SPILLED 回放差、append 吞吐）
- [ ] **Step 5: `mvn clean test` 全绿（基准类不得被 Surefire 误跑——命名 *Benchmark 校验）**
- [ ] **Step 6: Commit** `bench(spill): JMH profile+语料录制+四基准，基线数字入档`

---

### Task 14: 文档同步

**Files:**
- Modify: `src/main/java/org/vastdata/vbstream/replication/CLAUDE.md`
- Modify: `CLAUDE.md`（根）

**Steps:**

- [ ] **Step 1: replication/CLAUDE.md**——补：raw 接缝与 DecodedMessageBridge 双契约、TransactionAssembler 新桶模型（PayloadUnit/SPILLED/低水位）、MessageSpool/SpoolFrame/SpoolConfig/VersionedRelationRegistry/BucketReplayer 各一段（职责/线程/日志级别）、SessionHarness 双轨录制
- [ ] **Step 2: 根 CLAUDE.md**——项目概述更新里程碑状态；"运行 Main"补 `-Dvb.spill.*` 参数与 spill 队列目录说明（含"重启自动清空 spill 队列属预期"）；测试一节补 `AssemblySpillIT` 与 JMH 运行方式
- [ ] **Step 3: `mvn clean test` 最终全绿** → **Step 4: Commit + push** `docs(claude): 里程碑 1.6 文档同步——raw 接缝/溢写组件/运行与基准说明`

---

## 任务依赖

线性：1→2→3→4→5→6→7→8→9→10→11→12→13→14（7 依赖 3 无直接编译依赖但测试复用；10 依赖 5+6；12 依赖 7+10；13 依赖 7+9+10）。**Task 7 与 Task 8 是高风险点**（接缝替换零漂移、33 例语义移植），审查重点投入。
