# wal-source 热路径性能优化（#3/#6/#7/#1/#2）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消掉 wal-source 热路径上五处经三路性能审查证实的分配/拷贝/线性扫开销——接收零拷贝(#3)、TupleDecoder 逐列分配(#6)、census 每记录现拼键(#7)、字典 as-of 查询线性扫(#1)、缓存失效粗粒度+toastRels 无失效钩子(#2)——全部**行为逐字节不变**（对拍 IT 钉死）。

**Architecture:** 五项互相独立可单独提交，仅两处顺序依赖：Task 3（parser 偏移重载）先于 Task 4（walker 零拷贝消费它），Task 5（stores 索引+epoch）先于 Task 6（XactGrouper 失效接线）。#3 引入一个**契约面变更**：`WalRecord.raw` 语义从"恰为记录字节"放宽为"包含记录的宿主缓冲（偏移量绝对）"——已核实全部 `.raw()` 消费点（HeapViews/OgXactView/PageImages/XactGrouper/CatalogReplay）都以 `mainOff()/dataOff()/imageOff()` 等记录自带偏移为基准索引，绝对偏移下零改动；`tailOf`/`Arrays.copyOfRange` 均显式拷贝无滞留引用。#1/#2 全部在 CatalogStores 单写者模型内加二级索引与代际戳，弱一致读契约（CHM 无 CME）保持。

**Tech Stack:** Java 17 + Maven + JUnit 6；模块 `vb-stream-wal-source`（零新依赖）。

**Spec:** 本计划即 spec（源于 2026-10-17 三路性能审查会话；对拍契约源 = 模块 CLAUDE.md「双路对拍契约」节 + engine ConsoleRenderer 格式复刻契约）。

## Global Constraints

- **行为逐字节不变**：缺省档输出与服务器文本同形是对拍锚；全部离线测试 561 用例与 parity IT（DualPathParityIT 20 + OgDmlParityIT 27 + OgPartitionIT 16 + VastBase 手动档跳过）必须全绿。
- 方法名英文 camelCase；类型引用 import 简名禁止 FQN 内联；日志走 slf4j `{}` 占位符；每个函数（含私有）javadoc 说清职责/步骤/边界/线程约束。
- 测试命令须带 `-pl vb-stream-wal-source`（多模块仓 `-Dtest` 必须带 `-pl`）。
- 每任务完成即 `git add` 相关文件 + commit（消息格式仿仓内惯例 `perf(wal-source): ...`）+ push（跨机开发约定）。commit 尾加 `Co-Authored-By: Claude <noreply@anthropic.com>`。
- 已核实的安全前提（实现中如被推翻立即停手上报）：三路 `feed` 调用方（WalStreamReceiver.drainLoop / FileWalSource.pumpOnce / DirectWalSource.streamLoop）**每 chunk 新建 byte[] 且喂后不复用**；`resyncAt` 原地左移的落点区间起点 ≥ 已发射记录末尾（两调用点 dropFrom=pos 均如此）。

---

### Task 1: census 预计算键表 + LongAdder 值（#7）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/receive/WalStreamMetrics.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/receive/WalStreamWalkerTest.java`（census 键形已有断言则不动）；新增断言进 `WalStreamWalkerTest` 或独立小测试

**Interfaces:**
- Consumes: `WalRecord.rmid()/info()`（既有）
- Produces: `countRecord(WalRecord)`/`censusSnapshot()` 签名与输出**完全不变**（键形 `"rmid/hex(info&0xF0)"`、快照 `Map<String,Long>` 不可变）——Main 冒烟行 censusTop3 零改动。

**背景**：现 `countRecord` 每记录执行 `rec.rmid() + "/" + Integer.toHexString(...)`（StringBuilder + String）+ CHM `merge` 装箱 `Long.valueOf(1L)`。键空间封闭（rmid≤255 × 高 nibble 16 档）。改为懒填充 `String[256][16]` 键表 + `ConcurrentHashMap<String, LongAdder>` 计数（增量无装箱）。

- [ ] **Step 1: 写失败测试**（在 WalStreamWalkerTest 追加，或若该类无 census 断言则新建 `WalStreamMetricsTest`）：

```java
@Test
void censusKeyFormatAndCountSurvivePrecomputedTable() {
    WalStreamMetrics m = new WalStreamMetrics();
    WalRecord r = new WalRecord(0, 24, 1, 0, 10, (byte) 0xA0, List.of(), 24, 0, new byte[24]);
    m.countRecord(r);
    m.countRecord(r);
    m.countRecord(new WalRecord(0, 24, 1, 0, 10, (byte) 0xB0, List.of(), 24, 0, new byte[24]));
    Map<String, Long> snap = m.censusSnapshot();
    assertEquals(2L, snap.get("10/a0"));
    assertEquals(1L, snap.get("10/b0"));
    assertEquals(2, snap.size());
}
```

（若新建文件，包 `org.vastdata.vbstream.walsource.receive`，import 用简名。）

- [ ] **Step 2: 跑测试确认编译失败**（现行为其实能过——本任务输出不变，测试是**回归锚**而非失败驱动；确认新测试在现状下 PASS 即可继续，这是行为保持型重构的 TDD 变体）：

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalStreamMetricsTest`
Expected: PASS（若新建）——锚定改造前后同绿。

- [ ] **Step 3: 实现**——`WalStreamMetrics` 字段与方法替换：

```java
private final ConcurrentHashMap<String, LongAdder> census = new ConcurrentHashMap<>();
/** 懒填充键表 [rmid][高nibble]——键空间封闭（≤256×16），单写者线程填充后热路径零拼接。 */
private final String[][] censusKeys = new String[256][16];

public void countRecord(WalRecord rec) {
    records.increment();
    int rmid = rec.rmid();
    int nib = rec.info() & 0xF0;
    String key = censusKeys[rmid][nib >> 4];
    if (key == null) {
        key = rmid + "/" + Integer.toHexString(nib);
        censusKeys[rmid][nib >> 4] = key;
    }
    census.computeIfAbsent(key, k -> new LongAdder()).increment();
}

public Map<String, Long> censusSnapshot() {
    Map<String, Long> out = new java.util.HashMap<>(census.size());   // 实现处 import java.util.HashMap 用简名
    census.forEach((k, v) -> out.put(k, v.sum()));
    return Map.copyOf(out);
}
```

javadoc 更新：类 javadoc census 段与两方法 javadoc 写明懒填充键表 + LongAdder（单写者填充、快照弱一致语义不变）。

- [ ] **Step 4: 跑模块离线测试确认无回归**：

Run: `mvn test -pl vb-stream-wal-source -Dtest='WalStreamMetricsTest,WalStreamWalkerTest,ModuleBuildTest'`
Expected: PASS

- [ ] **Step 5: Commit & push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/receive/WalStreamMetrics.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/receive/
git commit -m "perf(wal-source): census 每记录字符串现拼改懒填充键表+LongAdder 计数——键形与快照输出不变（rmid/hex 高 nibble 封闭键空间，热路径零拼接零装箱）"
```

---

### Task 2: TupleDecoder 删逐列 int[] 游标与 null 位图拷贝（#6）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java`
- Test: 既有 `TupleDecoderTest` 17 用例即回归锚（行为不变型重构）；`decodeTupleData` 的 varlena 三分支与 HASNULL 位图路径已被既有用例覆盖（含 external/inline-compressed 经 ToastAssemblerTest 端到端）

**Interfaces:**
- Consumes: 既有三入口 `decodePayload/decodePageTuple/decodeEntry` 签名不变。
- Produces: 私有方法签名变化不影响任何外部调用方。`VarlenaResolver` 接口**不动**。

**设计**：varlena 游标改"先算终点再取值"——新增私有静态 `varlenaEnd(src, c, resolver)` 返回整个 varlena 之后的首偏移（一次头走读），值读取器改收 `(src, c)` 不再持 `int[] next`；null 位图不再拷出，直接在 `src` 上判位。

- [ ] **Step 1: 先跑现状基线**：

Run: `mvn test -pl vb-stream-wal-source -Dtest='TupleDecoderTest,ToastAssemblerTest,XactGrouperTest'`
Expected: PASS（记下用例数，改造后须同数同绿）。

- [ ] **Step 2: 实现**——`decodeTupleData` 内：

位图段替换（删 `bitmap` 数组）：
```java
// 原: byte[] bitmap = null; if (hasNull) { bitmap = new byte[...]; System.arraycopy(...); }
int bitsOff = hasNull ? tupleStart + TUPLE_BITS_OFFSET : -1;
```
列循环 null 判替换：
```java
if (hasNull && (src[bitsOff + (i >> 3)] & (1 << (i & 7))) == 0) {
    vals[i + base] = null;
    continue;
}
```

varlena 三 case 替换：
```java
case "text" -> {
    c = varlenaStart(src, c, tupleStart);
    int end = varlenaEnd(src, c, externalResolver);
    vals[i + base] = readTextDatum(src, c, end);
    c = end;
}
case "bytea", "numeric" -> {
    c = varlenaStart(src, c, tupleStart);
    int end = varlenaEnd(src, c, externalResolver);
    vals[i + base] = readDatumBytes(src, c, end);
    c = end;
}
case "skip" -> {
    c = varlenaStart(src, c, tupleStart);
    c = varlenaEnd(src, c, externalResolver);
}
```

新增 `varlenaEnd`（javadoc 说明四形态终点推导：1B 头短 varlena 总长 `(b0>>1)&0x7F`；external 短指针恒 18B；4B 头（plain 与压缩同面）总长 `u32>>>2`；resolver 为 null 时 0x01/压缩 tag 由值读取器的既有拒绝面兜住，本方法只算几何不抛）：
```java
private static int varlenaEnd(byte[] src, int c, VarlenaResolver resolver) {
    int b0 = src[c] & 0xFF;
    if ((b0 & VARLENA_TAG_1B) != 0) {
        return c + (b0 == VARLENA_TAG_1B_EXTERNAL ? EXTERNAL_POINTER_SIZE : (b0 >> 1) & 0x7F);
    }
    return c + (u32(src, c) >>> 2);
}
```

值读取器去 `int[] next`（返回值照旧、终点与 varlenaEnd 同源）：
```java
private String readTextDatum(byte[] src, int c, int end) {
    if (isExternalPointer(src, c)) {
        return new String(resolveExternalBytes(src, c), StandardCharsets.UTF_8);   // resolveExternalBytes 内部推 18B，end 断言一致
    }
    if (isInlineCompressed(src, c)) {
        return new String(decompressInlineBytes(src, c), StandardCharsets.UTF_8); // 内部 total=u32>>>2，与 end 同源
    }
    return readVarlenaText(src, c);   // 静态化：内部自读头，返回串；游标推进交给调用方的 end
}
```
`readDatumBytes` 同型。`readVarlenaText/readVarlenaBytes` 删 `int[] next` 参数（内部逻辑不变、删 `next[0]=c+len` 行）；`skipVarlena` 整个删除（case 直接用 varlenaEnd）；`decompressInlineBytes/resolveExternalBytes` 删 `next` 参数（保留 total 推进逻辑于返回前不需——调用方用 end）。

**注意**：`readVarlenaText` 保留 `rejectNonPlainVarlena` 调用与 ISE 消息逐字不变（v1 拒绝面）；类 javadoc 中"并发安全（实例无共享可变状态）"仍成立——全部改动为静态纯函数。

- [ ] **Step 3: 跑基线同套测试**：

Run: `mvn test -pl vb-stream-wal-source -Dtest='TupleDecoderTest,ToastAssemblerTest,XactGrouperTest,ChangeStreamTest,OgXactGrouperSeamTest,OgPartitionIdentityTest'`
Expected: PASS 且用例数与 Step 1 相同。

- [ ] **Step 4: Commit & push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java
git commit -m "perf(wal-source): TupleDecoder 逐列 int[] 游标改 varlenaEnd 终点前置+null 位图 src 原地判位——每 text/numeric/bytea/skip 列一次堆分配与每 HASNULL 行一次位图拷贝归零，行为不变（拒绝面/ISE 消息逐字保留）"
```

---

### Task 3: WalRecordParser 偏移重载 + WalRecord.raw 契约放宽（#3a）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalRecordParser.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalRecord.java`（仅 javadoc）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalRecordParserTest.java` 追加一用例

**Interfaces:**
- Consumes: 既有 `parse(byte[] rec, long lsn, WalLayout layout)`。
- Produces: 新重载 `public static WalRecord parse(byte[] buf, int recOff, long lsn, WalLayout layout)`——`buf` 为宿主缓冲、`recOff` 记录头起点；**返回的 `BlockRef.imageOff()/dataOff()` 与 `mainOff` 为 buf 内绝对偏移**，`raw()` 即 buf。旧三参签名委托新重载（off=0），既有调用点与全部测试零改动。

- [ ] **Step 1: 写失败测试**（WalRecordParserTest 追加；沿用该文件既有的手造记录辅助）：

```java
@Test
void offsetOverloadYieldsAbsoluteOffsetsIntoHostBuffer() {
    byte[] rec = <该测试类既有的最小合法记录构造>;              // 例：既有用例中 24B 头 + 一块 data 的构造
    byte[] host = new byte[rec.length + 64];
    System.arraycopy(rec, 0, host, 64, rec.length);
    WalRecord base = WalRecordParser.parse(rec, 100L, layout);
    WalRecord off = WalRecordParser.parse(host, 64, 100L, layout);
    assertEquals(base.totLen(), off.totLen());
    assertEquals(base.xid(), off.xid());
    assertEquals(base.rmid(), off.rmid());
    assertEquals(base.info(), off.info());
    assertEquals(base.mainOff() + 64, off.mainOff());
    assertSame(host, off.raw());
    assertEquals(base.blocks().size(), off.blocks().size());
    for (int i = 0; i < base.blocks().size(); i++) {
        BlockRef b = base.blocks().get(i);
        BlockRef o = off.blocks().get(i);
        assertEquals(b.dataOff() < 0 ? -1 : b.dataOff() + 64, o.dataOff());
        assertEquals(b.imageOff() < 0 ? -1 : b.imageOff() + 64, o.imageOff());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalRecordParserTest`
Expected: 编译失败（无四参重载）。

- [ ] **Step 3: 实现**——`parse` 主体整体改为四参版：`int totLen = u32(buf, recOff + TOTLEN_OFFSET);` `int pos = recOff + layout.recordHeaderSize();` 头区所有 `rec[x]` 读点改 `buf[recOff + x]` 相对基准（实现方式：局部 `int base = recOff;`，全部 `rec[...]` → `buf[base + ...]`，`u16(rec, p)` → `u16(buf, p)`（p 已是绝对））；载荷区回填的 `cur` 自 `pos` 起即绝对——imageOff/dataOff/mainOff 自然绝对。错误消息中的 `rec[rmidOff]` 改 `buf[recOff + rmidOff]`（消息文本不变）。`new WalRecord(lsn, totLen, ..., patched, mainOff, mainLen, buf)`——raw 传 buf。旧签名加一行委托：

```java
public static WalRecord parse(byte[] rec, long lsn, WalLayout layout) {
    return parse(rec, 0, lsn, layout);
}
```

`WalRecord` javadoc 更新（组件 `raw`/`mainOff`/`blocks` 三处注释）：raw 是**宿主缓冲**（记录可位于任意偏移；调用方持有、约定只读、walker 零拷贝优化引入——记录字节为 `raw[<记录自带偏移> .. +totLen)`），mainOff/imageOff/dataOff 为 raw 内绝对偏移。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl vb-stream-wal-source -Dtest='WalRecordParserTest,OgWalRecordParserTest,HeapViewsTest,PageImagesTest'`
Expected: PASS（既有用例走 off=0 委托零变化）。

- [ ] **Step 5: 全模块离线冒烟（消费面零改动验证）**

Run: `mvn test -pl vb-stream-wal-source -Dtest='!*,??*IT' -DfailIfNoTests=false` 或直接 `mvn test -pl vb-stream-wal-source -Dtest='TupleDecoderTest,XactGrouperTest,CatalogReplayTest,CatalogSynchronizerTest,WalStreamWalkerTest'`
Expected: PASS

- [ ] **Step 6: Commit & push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalRecordParser.java vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalRecord.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalRecordParserTest.java
git commit -m "perf(wal-source): WalRecordParser 增 (buf,recOff) 偏移重载——WalRecord.raw 契约放宽为宿主缓冲+绝对偏移（单页记录免缝合拷贝的前置；旧三参签名 off=0 委托，存量调用与测试零改动）"
```

---

### Task 4: WalStreamWalker 零拷贝 feed + 单页记录免缝合（#3b）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/receive/WalStreamWalker.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/receive/WalStreamWalkerTest.java` 追加两用例

**Interfaces:**
- Consumes: Task 3 的 `WalRecordParser.parse(buf, recOff, lsn, layout)`。
- Produces: `feed(long, byte[])` 签名不变；**契约面新增两条**（feed javadoc 写明）：① 喂入数组 feed 后不得复用/改写——walker 可能原地移位（resync）且已交付 `WalRecord.raw` 可能引用其字节；② 稳态（carry 空、记录完整落在本 chunk）零合并分配。

- [ ] **Step 1: 写契约锚测试**（WalStreamWalkerTest 追加；复用该类既有页/记录构造辅助）：

```java
@Test
void steadyStateFeedIsZeroCopyAndRecordReferencesHostChunk() {
    // 造一页含 ≥1 条完整记录的页（复用既有辅助），整页一次 feed
    List<WalRecord> out = new ArrayList<>();
    WalStreamWalker w = new WalStreamWalker(LAYOUT, new WalStreamMetrics(), out::add);
    byte[] page = <既有辅助造页>;
    w.feed(pageAddr + page.length, page);
    assertFalse(out.isEmpty());
    assertSame(page, out.get(0).raw());   // 契约锚：单页记录直接引用宿主 chunk
}

@Test
void leftoverOnlyChunkIsHeldWithoutCopy() {
    // 造半条记录（页头 + 记录头截断）作首 chunk，补齐作次 chunk，记录仍应完整交付
    ...  // 复用既有 piecewise 用例形态：断言交付记录字段正确即可（行为不变锚）
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalStreamWalkerTest`
Expected: `steadyStateFeedIsZeroCopy...` FAIL（现实现 rec 是缝合副本 ≠ page 引用）。

- [ ] **Step 3: 实现 feed 零拷贝化**——`feed` 主体替换（锚定/衔接校验段逐字保留）：

```java
if (carry.length == 0) {
    carryStartLsn = chunkStart;
}
byte[] buf;
if (carry.length == 0) {
    buf = data;   // 稳态零拷贝：直接解析调用方数组（契约：feed 后不得复用/改写 data）
} else {
    buf = new byte[carry.length + data.length];
    System.arraycopy(carry, 0, buf, 0, carry.length);
    System.arraycopy(data, 0, buf, carry.length, data.length);
}
sawAny = true;
ParseResult result = parse(buf, carryStartLsn);
int leftover = result.len() - result.consumed();
if (leftover == 0) {
    carry = EMPTY;
} else if (result.consumed() == 0 && result.len() == buf.length) {
    carry = buf;   // 整块未消费（首块即半记录）：直接持有免拷——resync 收缩(len<length)时不走本分支
} else {
    carry = new byte[leftover];
    System.arraycopy(buf, result.consumed(), carry, 0, leftover);
}
carryStartLsn = result.endLsn();
dataEndLsn = carryStartLsn + carry.length;
```

feed javadoc 增两段：契约变更（data 喂后不复用/可能被原地移位/WalRecord 可能引用）与稳态零拷贝说明（三路调用方已核实均为一次性数组）。

- [ ] **Step 4: 实现单页记录免缝合**——`parse` 的记录缝合段：在 `totLen` 合法性校验后、缝合循环前插快路：

```java
if (totLen <= pageRemain && len - pos >= totLen) {
    // 单页完整记录（绝大多数）：偏移直读零缝合（WalRecord.raw 即本缓冲）
    WalRecord parsed = WalRecordParser.parse(buf, pos, recLsn, layout);
    metrics.countRecord(parsed);
    sink.accept(parsed);
    pos += totLen;
    cur += totLen;
    <落入下方共享 MAXALIGN 垫块>;
    continue;
}
// 跨页记录：缝合进一次性数组（罕见形态，逐次新建防复用撞已交付记录）
byte[] rec = new byte[totLen]; ...（原缝合循环与 emit 逻辑不动）
```

垫块处理：把既有"记录起点 MAXALIGN 垫齐"段提为快慢两路共享（快路 continue 前执行同一 pad 段；或在快路内联同判据——两处同型即可，但注意快路 continue 前必须完成 pad 或落回循环顶重算 pageOff——**推荐**：快路只推进 pos/cur 到记录尾后 `break` 出快路、让循环尾的共享 pad 段处理，实现时按最小改动形态落地并在 javadoc 说明）。

- [ ] **Step 5: 跑 walker 全部用例**

Run: `mvn test -pl vb-stream-wal-source -Dtest='WalStreamWalkerTest,FileWalSourceTest,DirectWalSourceStreamTest,V3ProtocolChannelTest'`
Expected: PASS（WalStreamWalkerTest 14 用例含锚定协议/resync/整零页/跨页缝合全场景 + 新增 2）。

- [ ] **Step 6: 跑离线全量（消费面回归）**

Run: `mvn test -pl vb-stream-wal-source -Dtest='CatalogReplayTest,CatalogSynchronizerTest,CatalogOfflineTest,StateStoreTest,ChangeStreamTest'`
Expected: PASS

- [ ] **Step 7: Commit & push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/receive/WalStreamWalker.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/receive/WalStreamWalkerTest.java
git commit -m "perf(wal-source): walker feed 稳态零拷贝（carry 空直读调用方数组/整块未消费免拷持有/余量才拷）+ 单页记录偏移直读免缝合——每 chunk merge 分配与每记录 rec 拷贝归零；feed 契约面升级：喂后数组不得复用（可能原地移位/WalRecord 引用其字节），三路调用方已核实均为一次性数组"
```

---

### Task 5: CatalogStores 二级索引 + SnapshotView 查询 O(1) 化（#1）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogStores.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogSynchronizer.java`（SnapshotView 六查询）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogReplay.java`（增量写点换包装方法）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogBootstrap.java`（种子末尾 rebuildIndexes）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/state/StoredState.java`（restoreInto 末尾 rebuildIndexes）
- Test: 新建 `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/replay/CatalogStoresIndexTest.java`

**Interfaces:**
- Produces（CatalogStores 新公共面，后续 Task 6 也用）:
  - `void putAttrRow(long ctid, CatalogRow.AttrRow row)` / `void removeAttrRow(long ctid)`（class/nsp/partition 同型四对）
  - `void rebuildIndexes()`（清空并自四张主字典全量重建索引 + 代际戳自增——bootstrap/restore bulk 装载后调用一次）
  - 查询面：`CatalogRow.ClassRow classByOid(long)`、`ClassRow classByFilenode(long)`、`List<CatalogRow.AttrRow> attrsByRel(long)`、`CatalogRow.NspRow nspByOid(long)`、`CatalogRow.PartitionRow partitionByFilenode(long)`（均可能 null/empty）
  - `long shapeEpoch()`（Task 6 接线，本任务先落字段与 bump）

**索引设计**（全部 CHM 保弱一致读、无 CME；单写者线程维护）：
- `classOidIndex: CHM<Long, ClassRow>`（键 relOid）——relfilenodeOf/toastOf/relkindOf/nameOf/schemaOf 首段与 relOidOf 的 oid 直认腿
- `classFilenodeIndex: CHM<Long, ClassRow>`（键 relfilenode，**0 键不入**——映射目录行 relfilenode 列值恒 0，入表会把 relOidOf(0) 从"任意命中"变"恒命中"）——relOidOf 反查腿
- `nspOidIndex: CHM<Long, NspRow>`
- `attrRelIndex: CHM<Long, CopyOnWriteArrayList<AttrRow>>`（键 attrelid；DDL 稀有故 COW 拷贝可接受，读侧快照迭代无 CME）——columnsOf
- `partitionFilenodeIndex: CHM<Long, PartitionRow>`（键 relfilenode，0 不入）——partitionOf（'p' 判定保留在查询侧）

**多行同键语义（记档的收紧）**：同 oid/relfilenode 的陈旧 ctid 副本共存窗口内，线性扫返回"CHM 迭代序任意首配"、索引返回"最后 put 者"——后者即最新代际，是更正确的选择；repairTracked/PRUNE 收敛后两者一致。parity IT 钉行为不变。

- [ ] **Step 1: 写失败测试**（CatalogStoresIndexTest；手造 CatalogRow 四类行——构造参数照 CatalogRow record 定义取自源码）：

```java
@Test
void indexesAnswerSameAsLinearScanAcrossPutRemoveRebuild() {
    CatalogStores s = new CatalogStores();
    // 造 3 张表（含 1 张 dropped 列）+ 2 nsp + 1 分区 'p' 行，经 putXxxRow 装载
    ... s.putClassRow(1L << 32 | 1, classRowA); ...
    assertEquals(classRowA, s.classByOid(classRowA.relOid()));
    assertEquals(classRowA, s.classByFilenode(classRowA.relfilenode()));
    assertEquals(2, s.attrsByRel(relOidA).size());   // 含 dropped 占位
    assertNull(s.classByFilenode(0));                 // 0 键不入表
    // remove 后索引同步消失；旧值换 ctid 重 put 后索引指向新行
    s.removeClassRow(oldCtid);
    assertNull(s.classByOid(relOidA));
    // rebuildIndexes 与增量维护同果
    s.rebuildIndexes();
    assertEquals(classRowA, s.classByOid(relOidA));
    assertTrue(s.shapeEpoch() > 0);
}

@Test
void epochBumpsOnEveryRowMutation() {
    CatalogStores s = new CatalogStores();
    long e0 = s.shapeEpoch();
    s.putAttrRow(1L, attrRow);
    assertEquals(e0 + 1, s.shapeEpoch());
    s.removeAttrRow(1L);
    assertEquals(e0 + 2, s.shapeEpoch());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=CatalogStoresIndexTest`
Expected: 编译失败（无新方法）。

- [ ] **Step 3: 实现 CatalogStores**——五索引字段 + shapeEpoch（`private volatile long shapeEpoch;` + `public long shapeEpoch()`）+ 八包装方法（put/remove 各四；put：主 map put + 索引 upsert + epoch++；remove：`old = map.remove(ctid)`，old 非 null 时各索引按"索引现值 == old 才移除"防误删后写行，epoch++）+ `rebuildIndexes()`（清五索引、遍历四主 map 全量重建、epoch++）+ 五查询方法。类 javadoc 增索引节（单写者维护/弱一致读/多行同键语义收紧记档）。

- [ ] **Step 4: SnapshotView 六查询换索引**：
  - `columnsOf`: `List<AttrRow> rows = stores.attrsByRel(relOid); rows 为 null → 空表；否则投影 + sort（排序保留）`
  - `relfilenodeOf/toastOf/relkindOf/nameOf`: `ClassRow row = stores.classByOid(relOid); null → empty`
  - `schemaOf`: classByOid → `NspRow nsp = stores.nspByOid(row.relnamespace())`
  - `relOidOf`: watched 三段直认保留在前 → `ClassRow r = stores.classByFilenode(k); r != null → of(r.relOid())` → `classByOid(k) != null → of(k)` → empty
  - `partitionOf`: `PartitionRow row = stores.partitionByFilenode(relfilenode); row != null && "p".equals(row.parttype()) → ...`
  - 各方法 javadoc 更新（删"线性扫描"表述，注明索引面与语义收紧注记）。

- [ ] **Step 5: 写点迁移**——`grep -n "Rows()\.put\|Rows()\.remove" src/main/java/.../replay/CatalogReplay.java` 列出的全部行（约 658/694/728-738/754-763/797-802/815-818 等 attr/class/nsp/partition 四族）换 `stores.putXxxRow/ removeXxxRow`（尾部 map 仍直接操作——tail 无索引）。`StoredState.restoreInto` 四 putAll 后加 `stores.rebuildIndexes()`。`CatalogBootstrap` 种子循环保持直接 put、种子完成点加一次 `stores.rebuildIndexes()`。

- [ ] **Step 6: 跑测试**

Run: `mvn test -pl vb-stream-wal-source -Dtest='CatalogStoresIndexTest,CatalogReplayTest,CatalogSynchronizerTest,CatalogOfflineTest,OgCatalogReplayTest,OgPartitionDictionaryTest,SelfHealerTest,StateStoreTest,TableFilterTest,OgPartitionIdentityTest'`
Expected: PASS

- [ ] **Step 7: Commit & push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/ vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/state/StoredState.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/replay/CatalogStoresIndexTest.java
git commit -m "perf(wal-source): CatalogStores 五二级索引（oid/filenode/attrelid/nspOid/partitionFilenode）+写点包装方法+bulk rebuild——SnapshotView 六查询从字典值域线性扫降 O(1)（宽 schema 万级行下缓存 miss 路径数量级改善）；同键陈旧副本'最后 put 者胜'语义收紧记档；shapeEpoch 字段随行变更自增（Task 6 接线）"
```

---

### Task 6: shapeEpoch 缓存失效接线 + toastRels 失效钩子（#2）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/CatalogSnapshot.java`（default 方法）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogSynchronizer.java`（SnapshotView 覆写）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/XactGrouper.java`（resolveCached 入口 epoch 检查 + toastRels 清）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/XactGrouperTest.java` 追加一用例（StubSnapshot 加可变 epoch 字段）

**Interfaces:**
- Consumes: Task 5 的 `CatalogStores.shapeEpoch()`。
- Produces: `CatalogSnapshot.default long shapeEpoch() { return 0L; }`（默认 0 = 恒不过期——测试四个 Stub/FakeSnapshot 零改动即兼容）；`SnapshotView` 覆写返回真值。

**设计**：`resolveCached` 入口一次 `long epoch = snapshot.shapeEpoch()` 比对 `seenShapeEpoch` 字段——不等则 `metaCache.clear() + toastRels.clear()` 并同步。**保留** isShapeCatalogOid 分支的原 clear()（字典 heap 记录到达的直接信号，双保险且测试 Stub 无需改）；epoch 机制净新增覆盖面 = bootstrap/restore/PRUNE/INPLACE 等不经 shape-oid 记录的字典变更 + toastRels（今日**完全无失效钩子**、长会话单调增长，本任务是其唯一清理通道）。

- [ ] **Step 1: 写失败测试**（XactGrouperTest；该类 StubSnapshot 加字段 + 覆写）：

```java
// StubSnapshot 内加:
private long shapeEpoch;
@Override public long shapeEpoch() { return shapeEpoch; }
void bumpShapeEpoch() { shapeEpoch++; }

@Test
void staleEpochClearsMetaCacheAndToastRelsLazily() {
    // 既有 Fixture 形态：先解析一张表（缓存命中路径断言），随后 stub.bumpShapeEpoch()，
    // 再喂同表行——断言重新走了一次 resolve（可用改 Stub 数据后行值随之变化来观测：
    // bump 前后 StubSnapshot 返回不同列集，旧缓存若未被清则仍按旧列渲染）
    ...
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=XactGrouperTest`
Expected: 新用例 FAIL（现无 epoch 机制，旧缓存仍在）。

- [ ] **Step 3: 实现**——`CatalogSnapshot` 加 default `shapeEpoch()`（javadoc：字典形态代际戳，行变更/重建时自增；默认 0 = 恒不过期（测试替身兼容）；消费方按"值变即全清缓存"语义使用）。`SnapshotView` 覆写 `return stores.shapeEpoch();`。`XactGrouper` 加字段 `private long seenShapeEpoch = Long.MIN_VALUE;`，`resolveCached` 首行插入：

```java
long epoch = snapshot.shapeEpoch();
if (epoch != seenShapeEpoch) {
    metaCache.clear();
    toastRels.clear();   // 今日无失效钩子的补面：TRUNCATE/重写换段号后旧映射与 miss-亦-缓存条目随之清理
    seenShapeEpoch = epoch;
    LOG.debug("字典代际变化（epoch {}），TableMeta/toast 缓存全清", epoch);
}
```

类 javadoc 缓存策略段更新（全清触发键两条：shape-oid 记录到达[直接信号，保留] + 代际戳变化[兜底全覆盖]）。

- [ ] **Step 4: 跑测试**

Run: `mvn test -pl vb-stream-wal-source -Dtest='XactGrouperTest,ChangeStreamTest,TableFilterTest,OgXactGrouperSeamTest,OgPartitionIdentityTest,OgWalSourceConfigTest'`
Expected: PASS

- [ ] **Step 5: Commit & push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/CatalogSnapshot.java vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogSynchronizer.java vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/XactGrouper.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/XactGrouperTest.java
git commit -m "perf(wal-source): 缓存失效接代际戳——CatalogSnapshot.shapeEpoch() 默认 0 恒不过期（替身零改动）+ resolveCached 入口一次比对懒全清 metaCache/toastRels；toastRels 首获失效钩子（长会话单调增长关闭），bootstrap/restore/PRUNE/INPLACE 等字典变更全覆盖，shape-oid 直接信号保留双保险"
```

---

### Task 7: 全量验证 + 文档收口

**Files:**
- Modify: `vb-stream-wal-source/CLAUDE.md`（组件表/关键机制段各加一句性能面注记）
- Modify: 根 `CLAUDE.md`（wal-source 行追加简注，一行以内）

- [ ] **Step 1: 全模块测试（含 Docker IT——PG 41 + OG 58，对拍锚）**

Run: `mvn test -pl vb-stream-wal-source`
Expected: 567 用例（561 + 新增 6）全绿，Tests run 计数与失败明细贴进任务报告。

- [ ] **Step 2: 显式复核对拍 IT 细目**

Run: `mvn test -pl vb-stream-wal-source -Dtest='DualPathParityIT,WalAdversarialIT,WalLifecycleIT,Wal17SyncIT'`
Expected: PASS（干扰矩阵下 resync/缝合/字典风暴全场景——#3/#4/#5 的行为不变主锚）。

- [ ] **Step 3: 文档收口**——模块 CLAUDE.md：WalStreamWalker 行加"稳态零拷贝（feed 契约：喂后数组不得复用）"、CatalogStores 行加"五二级索引+代际戳"、XactGrouper 行缓存策略句更新、测试面计数 561→567 与新增用例注记；根 CLAUDE.md wal-source 源码根行追加一句。照仓内既有行文密度（中文、密集注记）。

- [ ] **Step 4: Commit & push**

```bash
git add vb-stream-wal-source/CLAUDE.md CLAUDE.md
git commit -m "docs(wal-source): 热路径性能优化五项收口——walker 零拷贝契约/字典二级索引/代际戳失效注记入档，测试面 561→567"
```

---

## Self-Review 结论

- **覆盖核对**：#7→Task 1、#6→Task 2、#3（a 契约面 + b walker）→Task 3/4、#1→Task 5、#2→Task 6，验证与文档→Task 7。审查清单其余项（H1 渲染/StateStore/浮点等）**刻意不在本计划**——用户只点 3/6/7/1/2。
- **类型一致性**：`varlenaEnd(src,c,resolver)`、`parse(buf,recOff,lsn,layout)`、`putXxxRow/removeXxxRow/rebuildIndexes/classByOid/.../shapeEpoch()` 各任务间签名已互检一致。
- **风险与已核实前提**：`WalRecord.raw` 契约放宽的消费面安全性（全部偏移消费 + tail 拷贝 + resync 落点 ≥ 已发射末尾 + 三路 feed 一次性数组）已在计划前逐点核实并写入 Global Constraints；`relOidOf(0)` 在 OG 档从"任意命中映射目录 oid"变 empty（损坏流形态，两者皆走跳过路径）为唯一记档行为差。
