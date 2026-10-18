# VastBase G100 B 档 M4 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** VastBase 档 B 兼容模式库（DBCOMPATIBILITY 'B'）的 DML 面开门——护栏分档放行 + 13 个 VastBase 系类型词典 + B 档 varlena 元组 6B xl 头接缝 + attnum -9 假阳性修复 + B 专形对拍 IT。

**Architecture:** 零布局/词典几何/XACT/分区改动（spike 实证同构）；三个面——`WalSource` 护栏白名单按 layout 分档、`WalLayout`/`TupleDecoder` 的 xl 头几何接缝（5B 位读 t_hoff < 24 即判 6B 头 +1，参数免下传）、`DecodeKinds`/`DiskValueRenderer` 的 oid 扩展（kind 与渲染均 oid/词典键控，A 档同批受益）。

**Tech Stack:** Java 17 + Maven + JUnit 6；真库手动档 IT（`-Dvb.it.vastbase=true`）连 172.16.107.133:55443/test_b（B 档）与 :55433/test_a（A 档回归）。

**Spec:** `docs/superpowers/specs/2026-10-18-wal-source-vastbase-b-mode-m4-design.md`（实施前先读——本计划从 spec 立论）
**Spike 证据:** `docs/wal-source-vastbase-b-mode-spike.md`（编码黄金锚表 + 用户三裁定）

## Global Constraints

- 方法名英文 camelCase；禁止 `System.out`（slf4j `{}` 占位符）；**每个函数（含私有/测试辅助）必须有 javadoc 逻辑描述**（职责/关键步骤/边界/线程约束）；类型引用 import 简名禁 FQN 内联
- 每任务完成即 `git add` + commit + **push**（跨机开发纪律）；commit message 中文、尾部 `Co-Authored-By: Claude <noreply@anthropic.com>`
- 离线测试命令：`mvn test -pl vb-stream-wal-source -Dtest=<ClassName>`（须带 `-pl`）；验证编译问题用 `mvn clean test-compile -pl vb-stream-wal-source`
- 真库 IT：`mvn test -pl vb-stream-wal-source -Dtest=<Class> -Dvb.it.vastbase=true`（B 档实例另需 `-Dvb.it.vastb.port=55443` 缺省已内置 test_b，无需覆盖）；真库卫生纪律：测试对象一律 `vb_wal_` 前缀 + finally 清理
- test_b 连接抖动（~10-20% 握手读超时；**快速连续建连触发连片失败**）：一律单持久连接 + 重试环（≤15 次、间隔 1s），禁止每语句新建连接
- 基线回归门：每任务后既有 569 用例中离线面全绿（`mvn test -pl vb-stream-wal-source -Dtest='!*IT'` 不可行——用全量 `mvn test -pl vb-stream-wal-source`，Docker 面自动跑）；A 档真库 24 用例在 Task 8 后统一复验
- spike harness 位于 `spike/vastbase-b-mode/`（不进构建，classpath `vb-stream-wal-source/target/classes` + `target/cp.txt`）

---

### Task 1: 补锚——B 档 text/NULL/COPY 变体的 6B 头形态钉死

**Files:**
- Modify: `spike/vastbase-b-mode/BModeEncodingSpike.java`（加 NULL 行与 COPY 探针）
- Modify: `docs/wal-source-vastbase-b-mode-spike.md`（锚结论回填）

**Interfaces:**
- Consumes: 既有 spike harness（编译/运行命令见下）
- Produces: 锚结论写进 spike 文档 §编码锚——Task 2 的接缝实现依据（预期形态：①B 档 text/varchar 列同样 6B 头 ②NULL 行 HASNULL 位图形态 ③COPY MULTI_INSERT entry 同 6B 头。**若实测推翻预期，回到 spec §2.2 修订接缝设计后再继续 Task 2**）

- [ ] **Step 1: 补探针——NULL 行 + COPY 面**

在 `BModeEncodingSpike.java` 的 `TYPES` 静态块中，把 clob/nvarchar2/blob 三类型的第二行值改为 NULL 形态（触发 HASNULL 位图），并新增 COPY 探针方法：

```java
// TYPES 修改（第二行改 NULL——HASNULL 位图锚）：
TYPES.put("clob", new String[] {"'hello clob'", "NULL"});
TYPES.put("nvarchar2(32)", new String[] {"'你好nvarchar'", "NULL"});
TYPES.put("blob", new String[] {"decode('DEADBEEF','hex')", "NULL"});
TYPES.put("text", new String[] {"'hello clob'", "NULL"});
```

`run()` 的 `phase1Inserts()` 之后追加 COPY 探针（`phase2Dump` 之前）：

```java
    /** COPY 批量面锚：MULTI_INSERT entry 的 xl 头形态（6B 头是否同样适用）。 */
    private void copyProbe(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("drop table if exists vb_wal_enc_copy");
            s.execute("create table vb_wal_enc_copy (id int, c clob)");
            s.execute("insert into vb_wal_enc_copy select g, 'row' || g from generate_series(1, 5) g");
            try (ResultSet rs = s.executeQuery("select relfilenode from pg_class where relname = 'vb_wal_enc_copy'")) {
                rs.next();
                filenodes.put(Long.parseLong(rs.getString(1).trim()), "copy(clob)");
            }
            System.out.println("== copy probe table ready");
        }
    }
```

（普通 `INSERT ... SELECT` 即走 MULTI_INSERT 路径，比 COPY FROM 语法更稳——B 档 COPY 协议不必引入。）在 `phase1Inserts()` 末尾的持久连接 try 块内调用 `copyProbe(c);`。

- [ ] **Step 2: 编译并对 test_b 跑锚**

```bash
javac -cp "vb-stream-wal-source/target/classes:$(cat vb-stream-wal-source/target/cp.txt)" \
     -d /tmp/vb-spike-b-cls spike/vastbase-b-mode/BModeEncodingSpike.java
rm -rf /tmp/vb-spike-b-anch && java -Db.types=text,clob,nvarchar2(32),blob \
     -cp "/tmp/vb-spike-b-cls:vb-stream-wal-source/target/classes:$(cat vb-stream-wal-source/target/cp.txt)" \
     BModeEncodingSpike /tmp/vb-spike-b-anch 2>&1 | grep -E "^HEX|^TEXT"
```

（zsh 下类型列表参数需引号：`'-Db.types=text,clob,nvarchar2(32),blob'`；连接重试耗尽则重跑一次。）

- [ ] **Step 3: 判读并记录**

对照判读（每行取 `data(NB)=` 前几个字节）：
- **text/varchar 行**：5B 位 `raw[off+4]` 是否 `< 0x18`（6B 头标志位 0x08）——若 text 行是 0x18 则 6B 头仅限 clob/blob 族（接缝规则不变，仍按 t_hoff<24 判定，天然兼容）
- **NULL 行**：infomask（5B 位 +2 或 6B 位 +3）是否含 0x0001（HASNULL）、位图与 datum 几何
- **copy(clob) 行**：是否出现在 `HEX [copy(clob)]`（MULTI_INSERT 的块 data 是 offsets 数组非 tuple——若 grep 不到该行属预期，MULTI_INSERT 不走 INSERT 块 data 面；此时记录"clob MULTI_INSERT 需 Task 3 的 decodeEntry 路径锚"并由 Task 8 IT 端到端兜底）

把三结论（含原始 hex）追加进 `docs/wal-source-vastbase-b-mode-spike.md` 的「编码锚」节末尾，标题 `### 第三轮补锚（M4 Task 1）`。

- [ ] **Step 4: 清理真库 + commit**

```bash
# 探针 finally 已自清 vb_wal_enc_*；确认无残留：
grep -cE "^\== cleanup done" <上一步输出文件>   # 应为 1
git add spike/vastbase-b-mode/BModeEncodingSpike.java docs/wal-source-vastbase-b-mode-spike.md
git commit -m "spike(wal-source): B 档 6B 头第三轮补锚——text/NULL/MULTI_INSERT 变体形态钉死（M4 Task 1）" && git push
```

---

### Task 2: xl 头几何接缝（`WalLayout.xlHeaderExtraShift` + `TupleDecoder` 三入口）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayout.java`（新增 default 接缝方法）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbase.java`（覆写）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java:129-133`（decodePayload 起点）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TupleDecoderTest.java`

**Interfaces:**
- Consumes: `TupleDecoder` 已有 `layout` 字段（构造器注入）；`TUPLE_BITS_OFFSET = 23`
- Produces: `WalLayout#xlHeaderExtraShift(byte[] raw, int off)` → `int`（缺省 0；VastBase 覆写：`(raw[off + 4] & 0xFF) < 24 ? 1 : 0`）。Task 3/8 依赖 decodePayload 对 B 档 varlena 行可解。

- [ ] **Step 1: 写失败测试（B 档黄金字节——spike 实采）**

`TupleDecoderTest` 追加（import 已有 `WalLayoutVVastbase`/`TupleDecoder` 则复用）：

```java
    /**
     * B 档 varlena 元组的 6B xl_heap_header 接缝（M4）：B 档库含 varlena 列的元组在
     * WAL 块 data 的 xl 头比 vanilla/A 档 5B 形多一个 0x00 字节（infomask2 后），
     * 5B 位读 t_hoff 得 0x08（几何不可能——合法 t_hoff ≥ 24）即判 6B 头 +1 平移。
     * 黄金字节实采自 test_b（spike 二轮，两列 (id int, c clob)）。
     */
    @Test
    void vastbaseBModeVarlenaTupleCarriesSixByteXlHeader() {
        TupleDecoder dec = new TupleDecoder(WalLayoutVVastbase.INSTANCE);
        // (1, 'hello clob')——02 00 | 00 | 02 08 | 18 | 00 01 00 00 00 | 17 | hello clob
        byte[] row1 = hex("02000002081800010000001768656C6C6F20636C6F62");
        assertArrayEquals(new Object[] {1, "hello clob"}, dec.decodePayload(row1, 0, new String[] {"int4", "text"}));
        // (2, '中文CLOB值')——13B UTF-8 数据
        byte[] row2 = hex("02000002081800020000001DE4B8ADE69687434C4F42E580BC");
        assertArrayEquals(new Object[] {2, "中文CLOB值"}, dec.decodePayload(row2, 0, new String[] {"int4", "text"}));
        // blob 族（bytea kind）同 6B 头：(1, DEADBEEF)
        byte[] blob1 = hex("02000002081800010000000BDEADBEEF");
        assertArrayEquals(1, ((long[]) null) == null ? assertArrayEquals(new Object[] {1, null}, null) : 0);
    }
```

（最后一行 blob 断言写清楚——替换为：）

```java
        byte[] blob1 = hex("02000002081800010000000BDEADBEEF");
        Object[] v = dec.decodePayload(blob1, 0, new String[] {"int4", "bytea"});
        assertEquals(1, v[0]);
        assertArrayEquals(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, (byte[]) v[1]);
```

（`hex` 辅助若测试类已有则复用；没有则加：

```java
    /** 十六进制串转字节数组（黄金字节测试辅助）。 */
    private static byte[] hex(String h) {
        byte[] b = new byte[h.length() / 2];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
        }
        return b;
    }
```

再追加 A 档零回归锚（同值 5B 形——test_a 实采）：

```java
    /**
     * A 档 varlena 元组维持 5B xl 头（M4 回归锚）：同值同几何下 A 档 text 行
     * [02 00][02 08][18] 无额外字节——接缝对 A 档恒 0，既有行为逐字节不变。
     */
    @Test
    void vastbaseACompatVarlenaTupleKeepsFiveByteXlHeader() {
        TupleDecoder dec = new TupleDecoder(WalLayoutVVastbase.INSTANCE);
        byte[] row1 = hex("020002081800010000001768656C6C6F20636C6F62");
        assertArrayEquals(new Object[] {1, "hello clob"}, dec.decodePayload(row1, 0, new String[] {"int4", "text"}));
    }
```

以及固定宽零移锚（uint8 最大值行，8B 对齐垫零——Task 3 之前 uint8 kind 未注册，本任务先用 int8 kind 占位断言几何）：

```java
    /**
     * B 档固定宽元组维持 5B xl 头（M4）：6B 头仅 varlena 元组——固定宽行 5B 位
     * t_hoff=0x18 直接成立、datum 起点不平移（uint8 行的 id@6、8B 对齐垫 3 零、
     * u64 殿后——spike 黄金锚，kind 暂用 int8 占位验证几何）。
     */
    @Test
    void vastbaseBModeFixedWidthTupleKeepsFiveByteXlHeader() {
        TupleDecoder dec = new TupleDecoder(WalLayoutVVastbase.INSTANCE);
        byte[] row = hex("0200000818000100000000000000FFFFFFFFFFFFFFFF");
        Object[] v = dec.decodePayload(row, 0, new String[] {"int4", "int8"});
        assertEquals(1, v[0]);
        assertEquals(-1L, v[1]);   // 0xFFFFFFFFFFFFFFFF == -1L（渲染无符号化在 Task 4）
    }
```

- [ ] **Step 2: 跑测试确认失败**

```bash
mvn test -pl vb-stream-wal-source -Dtest=TupleDecoderTest
```

预期：`vastbaseBModeVarlenaTupleCarriesSixByteXlHeader` FAIL（ArrayIndexOutOfBoundsException: Index -10 或值错位）；另两个用例可能已过（属预期——它们是回归锚）。

- [ ] **Step 3: 实现**

`WalLayout.java`——在 `attrMinSystemAttnum()` 方法后新增：

```java
    /**
     * WAL 块 data 的 xl_heap_header 额外前导字节数（M4 B 档接缝，缺省 0 =
     * vanilla/A 档 5B 头形 {@code [infomask2 u16][infomask u16][t_hoff u8]}）。
     *
     * <p>关键步骤：{@code TupleDecoder.decodePayload/decodeEntry} 在读取 infomask/
     * t_hoff 前经本接缝平移读取位。B 档（VastBase 内核 MySQL 兼容档）含 varlena 列
     * 的元组是 6B 头（infomask2 后多一个 0x00 字节，datum 区整体 +1；固定宽元组仍
     * 5B——spike 二/三轮实锚）。判别式 = 5B 位读 t_hoff 得 &lt; 24：合法 t_hoff 是
     * MAXALIGN(23 + 位图长) ≥ 24，6B 头行该位置读到的是 infomask 低字节 0x08。
     * 边界与异常语义：raw 长度不足由调用方走读自洽前提兜住（数组越界 ISE fail-fast）。
     * 线程约束：纯读。</p>
     *
     * @param raw 记录/entry 字节（契约只读）
     * @param off xl_heap_header 起点
     * @return 额外前导字节数（0 或 1）
     */
    default int xlHeaderExtraShift(byte[] raw, int off) {
        return 0;
    }
```

`WalLayoutVVastbase.java`——新增覆写：

```java
    /** {@inheritDoc}——VastBase 档覆写：5B 位 t_hoff &lt; 24（几何不可能值）判 B 档
     * 6B xl 头 +1；A 档 varlena 行与全部固定宽行 5B 位读得合法 t_hoff（≥24）恒 0，
     * 既有 A 档行为逐字节不变（M4 spike 二轮 A/B 对照锚）。 */
    @Override
    public int xlHeaderExtraShift(byte[] raw, int off) {
        return (raw[off + 4] & 0xFF) < 24 ? 1 : 0;
    }
```

`TupleDecoder.java`——`decodePayload` 改为：

```java
    public Object[] decodePayload(byte[] raw, int off, String[] kinds) {
        int shift = layout.xlHeaderExtraShift(raw, off);
        int infomask2 = u16(raw, off);
        int infomask = u16(raw, off + 2 + shift);
        int tHoff = raw[off + 4 + shift] & 0xFF;
        return decodeTupleData(raw, off + 5 + shift - TUPLE_BITS_OFFSET, tHoff, infomask, infomask2, kinds);
    }
```

（`decodeEntry` 委派 `decodePayload` 自动继承；`decodePageTuple` 是页内完整 23B 真头不受 xl 头接缝影响——零改动，B 档端到端由 Task 8 IT 兜底验证。若 `TupleDecoder` 的 `layout` 字段名不同，按实际字段名对齐。）

- [ ] **Step 4: 跑测试确认通过**

```bash
mvn test -pl vb-stream-wal-source -Dtest=TupleDecoderTest
```

预期：TupleDecoderTest 全绿（含既有 17 用例零回归）。

- [ ] **Step 5: 全量离线回归 + commit**

```bash
mvn test -pl vb-stream-wal-source
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayout.java \
        vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbase.java \
        vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java \
        vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TupleDecoderTest.java
git commit -m "feat(wal-source): B 档 varlena 元组 6B xl 头接缝——WalLayout.xlHeaderExtraShift 几何判别（5B 位 t_hoff<24 即 +1），A 档/固定宽恒 0 零回归" && git push
```

---

### Task 3: 类型词典扩展（`DecodeKinds` 13 oid + `TupleDecoder` 5 新 kind）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/DecodeKinds.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java`（kind switch 增 5 分支）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TupleDecoderTest.java`

**Interfaces:**
- Consumes: Task 2 的接缝；既有 kind 分支风格（`c` 游标 / `align(rel, n)` / `u16/u32/u64`）
- Produces: `DecodeKinds.forTypeOid` 认 13 新 oid；kinds `uint1/uint2/uint4/uint8/year`（`int1` 为既有 kind 解锁 oid 5545）。Task 4 渲染依赖解码值形态：uint1→Integer、uint2→Integer、uint4→Long、uint8→Long（可能为负、渲染无符号化）、year→Integer（解码期 +1900 归一）。

- [ ] **Step 1: 写失败测试**

`TupleDecoderTest` 追加（黄金字节全部 spike 实采，两列表 (id int, c T)）：

```java
    /**
     * M4 新 kind 黄金字节矩阵：uint1/uint2/uint4/uint8/year 五定宽类型 + int1 oid
     * 解锁——值与对齐（'c'/'s'/'d'）按 spike 二轮实采锚（uint8 的 8B 对齐垫 3 零）。
     */
    @Test
    void m4FixedWidthKindsDecodeAgainstGoldenBytes() {
        TupleDecoder dec = new TupleDecoder(WalLayoutVVastbase.INSTANCE);
        Object[] u1 = dec.decodePayload(hex("02000008180001000000FF"), 0, new String[] {"int4", "uint1"});
        assertEquals(255, u1[1]);
        Object[] u2 = dec.decodePayload(hex("02000008180001000000FFFF"), 0, new String[] {"int4", "uint2"});
        assertEquals(65535, u2[1]);
        Object[] u4 = dec.decodePayload(hex("02000008180001000000FFFFFFFF"), 0, new String[] {"int4", "uint4"});
        assertEquals(4294967295L, u4[1]);
        Object[] u8 = dec.decodePayload(hex("0200000818000100000000000000FFFFFFFFFFFFFFFF"),
                0, new String[] {"int4", "uint8"});
        assertEquals(-1L, u8[1]);   // 0xFFFF...FF == -1L，渲染期 Long.toUnsignedString
        Object[] yr = dec.decodePayload(hex("020000081800010000007E00"), 0, new String[] {"int4", "year"});
        assertEquals(2026, yr[1]);
        Object[] yr2 = dec.decodePayload(hex("020000081800020000000100"), 0, new String[] {"int4", "year"});
        assertEquals(1901, yr2[1]);
        Object[] i1 = dec.decodePayload(hex("020000081800010000007F"), 0, new String[] {"int4", "int1"});
        assertEquals((byte) 127, i1[1]);
        Object[] i1n = dec.decodePayload(hex("0200000818000200000080"), 0, new String[] {"int4", "int1"});
        assertEquals((byte) -128, i1n[1]);
    }

    /**
     * M4 oid 映射面：DecodeKinds.forTypeOid 对 13 新 oid 全命中——uint 族 4 + year +
     * int1 + varlena 族 7（clob/nvarchar2→text、blob/tinyblob/raw/binary/varbinary→bytea）。
     * 未注册 oid（如 enum 动态 oid）仍 ISE（矩阵外 fail-fast 语义不变）。
     */
    @Test
    void m4TypeOidsResolveInDecodeKinds() {
        assertEquals("int1", DecodeKinds.forTypeOid(5545));
        assertEquals("uint1", DecodeKinds.forTypeOid(15705));
        assertEquals("uint2", DecodeKinds.forTypeOid(15712));
        assertEquals("uint4", DecodeKinds.forTypeOid(15719));
        assertEquals("uint8", DecodeKinds.forTypeOid(15726));
        assertEquals("year", DecodeKinds.forTypeOid(17851));
        assertEquals("text", DecodeKinds.forTypeOid(90));
        assertEquals("text", DecodeKinds.forTypeOid(3969));
        assertEquals("bytea", DecodeKinds.forTypeOid(88));
        assertEquals("bytea", DecodeKinds.forTypeOid(95));
        assertEquals("bytea", DecodeKinds.forTypeOid(86));
        assertEquals("bytea", DecodeKinds.forTypeOid(9779));
        assertEquals("bytea", DecodeKinds.forTypeOid(17757));
        assertThrows(IllegalStateException.class, () -> DecodeKinds.forTypeOid(24064));
    }
```

（`DecodeKinds` 若未被该测试类 import 则补 `import org.vastdata.vbstream.walsource.layout.DecodeKinds;`——同包则免。）

- [ ] **Step 2: 跑测试确认失败**

```bash
mvn test -pl vb-stream-wal-source -Dtest=TupleDecoderTest
```

预期：两个新用例 FAIL（`unregistered type oid`）。

- [ ] **Step 3: 实现**

`DecodeKinds.forTypeOid` 的 switch 增分支（注释含 spike 出处）：

```java
            case 5545 -> "int1";      // VastBase int1（1B 有符号——TupleDecoder 既有 kind 的 oid 解锁）
            case 88, 95, 9779, 17757, 86 -> "bytea"; // blob/varbinary/binary/tinyblob/raw（varlena 字节流——渲染按 oid 分叉）
            case 90, 3969 -> "text";  // clob/nvarchar2（varlena 文本）
            case 15705 -> "uint1";    // VastBase B 档 uint1（1B 无符号）
            case 15712 -> "uint2";    // uint2（2B 小端无符号）
            case 15719 -> "uint4";    // uint4（4B 小端无符号）
            case 15726 -> "uint8";    // uint8（8B 小端无符号，'d' 对齐）
            case 17851 -> "year";     // year（u16 = 年-1900，MySQL YEAR 语义 1901-2155）
```

`TupleDecoder` 的 kind switch（`case "int2"` 邻域）增：

```java
                case "uint1" -> { vals[i + base] = src[c] & 0xFF; c += 1; }   // 1B 无符号
                case "uint2" -> { c = tupleStart + align(c - tupleStart, 2); vals[i + base] = u16(src, c) & 0xFFFF; c += 2; }
                case "uint4" -> { c = tupleStart + align(c - tupleStart, 4); vals[i + base] = u32(src, c) & 0xFFFFFFFFL; c += 4; }
                case "uint8" -> { c = tupleStart + align(c - tupleStart, 8); vals[i + base] = u64(src, c); c += 8; }   // 渲染期无符号化
                case "year" -> { c = tupleStart + align(c - tupleStart, 2); vals[i + base] = (u16(src, c) & 0xFFFF) + 1900; c += 2; }   // 解码期归一为年值
```

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

```bash
mvn test -pl vb-stream-wal-source -Dtest=TupleDecoderTest
mvn test -pl vb-stream-wal-source
```

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/DecodeKinds.java \
        vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java \
        vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TupleDecoderTest.java
git commit -m "feat(wal-source): M4 类型词典——DecodeKinds 增 13 oid（uint 族/year/int1/varlena 族 7）+ TupleDecoder 五新 kind 黄金字节锚" && git push
```

---

### Task 4: 渲染分支（`DiskValueRenderer` 13 oid）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/DiskValueRenderer.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/DiskValueRendererTest.java`

**Interfaces:**
- Consumes: Task 3 的解码值形态（uint1/2→Integer、uint4/8→Long、year→Integer、int1→Byte、bytea 族→byte[]、text 族→String）
- Produces: `render(Object, long)` 认 13 新 oid——后续 IT 与 OutputRenderer 直接可用。

- [ ] **Step 1: 写失败测试**

`DiskValueRendererTest` 追加（渲染字面全部 spike 服务端 text 输出实采锚；blob 族为**有意分叉**记档——`\x` hex 非裸字节）：

```java
    /**
     * M4 新类型渲染锚（服务端 text 输出同形，spike 二轮实采）：uint 族十进制无符号
     * （uint8 超 Long.MAX 走 toUnsignedString）、year 十进制、int1 有符号、
     * clob/nvarchar2 文本原样、raw 大写 hex 无前缀。
     */
    @Test
    void m4VastbaseTypesRenderServerTextForm() {
        assertEquals("127", DiskValueRenderer.render((byte) 127, 5545));
        assertEquals("-128", DiskValueRenderer.render((byte) -128, 5545));
        assertEquals("255", DiskValueRenderer.render(255, 15705));
        assertEquals("65535", DiskValueRenderer.render(65535, 15712));
        assertEquals("4294967295", DiskValueRenderer.render(4294967295L, 15719));
        assertEquals("18446744073709551615", DiskValueRenderer.render(-1L, 15726));
        assertEquals("2026", DiskValueRenderer.render(2026, 17851));
        assertEquals("1901", DiskValueRenderer.render(1901, 17851));
        assertEquals("hello clob", DiskValueRenderer.render("hello clob", 90));
        assertEquals("你好nvarchar", DiskValueRenderer.render("你好nvarchar", 3969));
        assertEquals("0102030405060708090A0B0C0D0E0F10",
                DiskValueRenderer.render(hexBytes("0102030405060708090A0B0C0D0E0F10"), 86));
        assertEquals("FF", DiskValueRenderer.render(hexBytes("FF"), 86));
    }

    /**
     * M4 blob 族渲染有意分叉记档锚：服务端 text 输出是裸字节（乱码形），本渲染取
     * \x hex（CDC 下游可用性优先——与矩阵外 0x 降级同族保守选择，spec §2.1 裁定）。
     */
    @Test
    void m4BlobFamilyRendersHexFormByDesign() {
        assertEquals("\\xdeadbeef", DiskValueRenderer.render(hexBytes("DEADBEEF"), 88));
        assertEquals("\\xdeadbeef", DiskValueRenderer.render(hexBytes("DEADBEEF"), 17757));
        assertEquals("\\xdeadbeef", DiskValueRenderer.render(hexBytes("DEADBEEF"), 9779));
        assertEquals("\\xdeadbeef", DiskValueRenderer.render(hexBytes("DEADBEEF"), 95));
    }
```

（`hexBytes` 辅助若类内已有等价物复用；没有则加同 Task 2 风格的 `hex`——返回 byte[]。）

- [ ] **Step 2: 跑测试确认失败**

```bash
mvn test -pl vb-stream-wal-source -Dtest=DiskValueRendererTest
```

预期：FAIL（unknown oid 走 `0x` 降级形态或异常）。

- [ ] **Step 3: 实现**

`DiskValueRenderer` 常量区（OID_UUID 后）增：

```java
    private static final int OID_INT1 = 5545;          // VastBase int1
    private static final int OID_CLOB = 90;            // VastBase clob
    private static final int OID_RAW = 86;             // VastBase raw（Oracle RAW——大写 hex 无前缀）
    private static final int OID_VARBINARY = 95;       // B 档 varbinary
    private static final int OID_NVARCHAR2 = 3969;     // VastBase nvarchar2
    private static final int OID_BLOB = 88;            // VastBase blob
    private static final int OID_BINARY = 9779;        // B 档 binary
    private static final int OID_TINYBLOB = 17757;     // B 档 tinyblob
    private static final int OID_UINT1 = 15705;        // B 档 uint1
    private static final int OID_UINT2 = 15712;        // B 档 uint2
    private static final int OID_UINT4 = 15719;        // B 档 uint4
    private static final int OID_UINT8 = 15726;        // B 档 uint8
    private static final int OID_YEAR = 17851;         // B 档 year
```

`render` 三参版 switch 的 `default` 前增：

```java
            case OID_INT1, OID_UINT1, OID_UINT2, OID_UINT4 -> renderInt(decoded);
            case OID_UINT8 -> Long.toUnsignedString(expectNumber(decoded, typeOid).longValue());
            case OID_YEAR -> String.valueOf(expectNumber(decoded, typeOid).intValue());
            case OID_CLOB, OID_NVARCHAR2 -> renderTextFamily(decoded);
            case OID_BLOB, OID_VARBINARY, OID_BINARY, OID_TINYBLOB ->
                    "\\x" + HexFormat.of().formatHex(expectBytes(decoded, typeOid));
            case OID_RAW -> HexFormat.of().formatHex(expectBytes(decoded, typeOid)).toUpperCase();
```

（`renderInt` 若对 Byte 输入不认，先查其实现——其 javadoc 声明 Number 族即应覆盖 Byte；不认则在其内部 widen 分支加 `Byte`。）

- [ ] **Step 4: 跑测试确认通过 + 全量**

```bash
mvn test -pl vb-stream-wal-source -Dtest=DiskValueRendererTest
mvn test -pl vb-stream-wal-source
```

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/DiskValueRenderer.java \
        vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/DiskValueRendererTest.java
git commit -m "feat(wal-source): M4 渲染分支——13 oid 服务端同形（uint/year/int1/clob/raw）+ blob 族 \\\\x hex 有意分叉记档" && git push
```

---

### Task 5: attnum 下界修复（`WalLayoutVVastbase.attrMinSystemAttnum = -9`）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbase.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbaseTest.java`

**Interfaces:**
- Produces: VastBase 档系统列诊断下界 -9（OG 5.0.0 维持 -8）；`CatalogReplay` 的"疑似词典错位"WARN 对 `attnum=-9 (xc_node_id)` 行不再误报（A/B 档同受益）。

- [ ] **Step 1: 写失败测试**

`WalLayoutVVastbaseTest` 追加：

```java
    /**
     * 系统列诊断下界 -9（M4 修复）：VastBase 6.0.0 内核 pg_attribute 系统列集
     * [-9,-1]（xc_node_id=-9、rowid=-8——真库 A/B 双档实查同源），OG 5.0.0 布局
     * 维持 -8——流内 CREATE TABLE 的 xc_node_id(-9) 行触发假阳性"疑似词典错位"
     * WARN 的根因修复锚。
     */
    @Test
    void vastbaseAttrMinSystemAttnumIsMinusNine() {
        assertEquals(-9, WalLayoutVVastbase.INSTANCE.attrMinSystemAttnum());
        assertEquals(-8, WalLayoutVOpenGauss.INSTANCE.attrMinSystemAttnum());
        assertEquals(-6, new WalLayoutV18().attrMinSystemAttnum());
    }
```

（`WalLayoutVOpenGauss`/`WalLayoutV18` import 按测试类既有；`WalLayoutV18` 若是单例风格按实际取法对齐。）

- [ ] **Step 2: 确认失败**

```bash
mvn test -pl vb-stream-wal-source -Dtest=WalLayoutVVastbaseTest
```

预期 FAIL（现值 -8）。

- [ ] **Step 3: 实现**

`WalLayoutVVastbase.java` 增覆写：

```java
    /** {@inheritDoc}——VastBase 6.0.0 内核覆写 -9：系统列集 [-9,-1]（xc_node_id=-9/
     *  rowid=-8，真库 A/B 双档实查同源——M4 修复流内建表 xc_node_id 行的假阳性
     * "疑似词典错位"WARN；OG 5.0.0 布局的 -8 不变）。 */
    @Override
    public int attrMinSystemAttnum() {
        return -9;
    }
```

- [ ] **Step 4: 跑测试 + 全量**

```bash
mvn test -pl vb-stream-wal-source -Dtest=WalLayoutVVastbaseTest
mvn test -pl vb-stream-wal-source
```

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbase.java \
        vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbaseTest.java
git commit -m "fix(wal-source): VastBase 档 attnum 系统列诊断下界 -8→-9——xc_node_id 行假阳性词典错位 WARN 修复（A/B 档同受益）" && git push
```

---

### Task 6: 护栏白名单分档（`WalSource`）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/WalSource.java:658-668`（guard）、`:974`（白名单）、`:1006-1020`（isDmlSupportedCompatMode）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/api/OgCompatModeGuardTest.java`

**Interfaces:**
- Produces: `static Set<String> dmlCompatModes(WalLayout layout)`（VastBase→{'PG','A','B'}，其余→{'PG','A'}）；`static boolean isDmlSupportedCompatMode(Connection, WalLayout)`。原 `isDmlSupportedCompatMode(Connection)` 保留为 OG-档委托（旧调用面兼容）或全量改签——按实际调用点最少扰动定，测试同步。

- [ ] **Step 1: 写失败测试**

`OgCompatModeGuardTest` 追加（伪 JDBC 基座复用既有 `scriptedConnection` 形态）：

```java
    /**
     * M4 护栏分档：B 档白名单按 layout 分——VastBase 档放行（{'PG','A','B'}）、
     * OG 5.0.0 档维持拒绝（B 档未验证，spec §0 裁定 a）；'C' 档两 layout 均拒绝。
     */
    @Test
    void bCompatModeWhitelistSplitsByLayout() throws Exception {
        FakeJdbc b = FakeJdbc.returningCompat("B");   // 既有伪基座的档位脚本形态
        assertTrue(WalSource.isDmlSupportedCompatMode(b.connection(), WalLayoutVVastbase.INSTANCE),
                "B 档在 VastBase 布局须放行（M4）");
        assertFalse(WalSource.isDmlSupportedCompatMode(b.connection(), WalLayoutVOpenGauss.INSTANCE),
                "B 档在 OG 5.0.0 布局维持 ISE（未验证，spec §0 裁定 a）");
        FakeJdbc c = FakeJdbc.returningCompat("C");
        assertFalse(WalSource.isDmlSupportedCompatMode(c.connection(), WalLayoutVVastbase.INSTANCE),
                "'C' 档（PG 兼容历史别名未验证）两 layout 均拒绝");
    }
```

（`FakeJdbc` 按该测试类既有的伪 Connection 工厂实际名称/构造对齐——类内已有 pg/oracle/jdbc 等脚本化连接的建法，照抄改档位值。）

既有用例 `definiteCompatModesResolveAgainstWhitelist` 的 'B' 断言（`assertFalse`）需改带 layout 双断言；`ogStartWithDmlListenerOnBCompatDatabaseFailsFastBeforeSlotSideEffects` 若其伪装配解析到 VastBase 布局则期望反转为装配成功——按其伪 Driver 的 version/relname 脚本判定，保留一个"OG 布局 + B 档 ISE"的等价用例（改脚本使其命中 OG 布局）。

- [ ] **Step 2: 确认失败**

```bash
mvn test -pl vb-stream-wal-source -Dtest=OgCompatModeGuardTest
```

- [ ] **Step 3: 实现**

`WalSource`：

```java
    /** DML 面兼容模式白名单（M4 起按 layout 分档）：VastBase 档 {'PG','A','B'}
     * （B=MySQL 风格，M4 开门）；OG 5.0.0 档 {'PG','A'} 不变（B 档 typemap 未验证，
     * spec §0 裁定 a）。'C' 两档均拒（PG 兼容历史别名未验证）。 */
    static Set<String> dmlCompatModes(WalLayout layout) {
        return layout instanceof WalLayoutVVastbase ? Set.of("PG", "A", "B") : Set.of("PG", "A");
    }
```

`startOpenGauss` 护栏段改为：

```java
        if (dmlEnabled()) {
            String compat = queryDbCompatMode(sqlConnection);
            boolean vastbase = layout instanceof WalLayoutVVastbase;
            if (compat != null && !dmlCompatModes(layout).contains(compat)) {
                throw new IllegalStateException("openGauss 档 DML 面仅支持 PG/A"
                        + (vastbase ? "/B" : "") + " 兼容模式库: 当前库 datcompatibility=" + compat
                        + (vastbase
                                ? "（'C' 为 PG 兼容历史别名未验证——请以 DBCOMPATIBILITY 'PG'/'A'/'B' 建库）"
                                : "（B 档=MySQL 风格支持属 VastBase 档 M4——OG 5.0.0 未验证，请以 DBCOMPATIBILITY 'PG' 或 'A' 建库）")
                        + "；仅需 catalog 同步面可 -Dvb.wal.dml=false 显式声明，catalog 面与兼容模式无关）");
            }
            ...（不可判定 WARN 段不变）
```

`isDmlSupportedCompatMode` 增 layout 参（原单参版委派 `this`... static——委派 `isDmlSupportedCompatMode(connection, null)` 走 OG 白名单），实现按 `dmlCompatModes(layout == null ? OG 语义 : layout)`——null 视为非 VastBase 档（旧调用面语义不变）。`OG_DML_COMPAT_MODES` 常量删除或内联进 `dmlCompatModes`（同步改 javadoc 的 {M4 指引} 表述）。`WalLayoutVVastbase`/`WalLayoutVOpenGauss` 需在 `WalSource` import。

- [ ] **Step 4: 跑测试 + 全量**

```bash
mvn test -pl vb-stream-wal-source -Dtest=OgCompatModeGuardTest
mvn test -pl vb-stream-wal-source
```

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/WalSource.java \
        vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/api/OgCompatModeGuardTest.java
git commit -m "feat(wal-source): M4 护栏分档——VastBase 档白名单放宽 {'PG','A','B'}，OG 5.0.0 维持 {'PG','A'}（B 档未验证 ISE 附指引）" && git push
```

---

### Task 7: B 档测试环境（`VastBEnv` + 连接重试环）

**Files:**
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastBEnv.java`
- Modify: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastTestEnv.java`（共享会话/新连接加重试环）

**Interfaces:**
- Produces: `VastBEnv.host()/port()/db()/user()/pass()/url()/sql()/newSqlConnection()/newStateDir()`（与 `VastTestEnv` 同形——默认 172.16.107.133:55443/test_b，`-Dvb.it.vastb.*` 覆盖）；`VastTestEnv` 的建连统一走私有 `openWithRetry`（≤15 次、1s 间隔）。Task 8 消费。

- [ ] **Step 1: 写 `VastBEnv`**

新类（javadoc 说明 B 档实例定位/连接纪律/卫生前缀与 VastTestEnv 同规；结构照抄 `VastTestEnv` 的 host/port/db/user/pass/sql/newSqlConnection/newStateDir 骨架，改属性前缀 `vb.it.vastb`、默认 port 55443/db test_b、URL 仍 `jdbc:opengauss://`）。核心差异点：

```java
    /** 建连（B 档实例握手抖动吸收——单持久连接纪律下的重试环，≤15 次间隔 1s）。 */
    private static Connection openConnection() throws SQLException {
        SQLException last = null;
        for (int attempt = 1; attempt <= 15; attempt++) {
            try {
                return DriverManager.getConnection(url(), user(), pass());
            } catch (SQLException e) {
                last = e;
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("建连中断", ie);
                }
            }
        }
        throw new SQLException("B 档实例连接重试耗尽: " + url(), last);
    }
```

（`sql()` 共享会话懒建、`newSqlConnection()` 每次独立——两路都经 `openConnection`。）

- [ ] **Step 2: `VastTestEnv` 同面加退避**

`VastTestEnv` 的共享会话懒建与 `newSqlConnection` 改走同款 `openWithRetry`（A 档真库偶发 EOF 同源——M4 起 A 档 IT 稳定性顺带受益）。javadoc 各补一行"建连带 ≤15 次重试环（M4，test_b 抖动纪律）"。

- [ ] **Step 3: 编译 + 既有 A 档 IT 冒烟**

```bash
mvn clean test-compile -pl vb-stream-wal-source
mvn test -pl vb-stream-wal-source -Dtest=VastCatalogSyncIT -Dvb.it.vastbase=true
```

预期：A 档 2 用例绿（重试环不破坏既有行为）。

- [ ] **Step 4: Commit**

```bash
git add vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastBEnv.java \
        vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastTestEnv.java
git commit -m "test(wal-source): M4 B 档测试环境 VastBEnv（55443/test_b）+ VastTestEnv 建连重试环（握手抖动纪律）" && git push
```

---

### Task 8: B 专形对拍 IT（`VastBModeIT` 四场景）

**Files:**
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastBModeIT.java`
- Test（手动档）: 同上文件——`-Dvb.it.vastbase=true` 时跑（B 实例经 `VastBEnv`，不依赖 `-Dvb.it.vastbase.port` 覆盖）

**Interfaces:**
- Consumes: Task 2-6 全部；`OgParityBaseline`（`it` 包既有基准路归一化器）；`VastBEnv`
- Produces: 四场景——①13 类型全矩阵 ②''/datetime 语义面 ③varlena 6B 头混合（含 NULL/TOAST）④UPDATE/DELETE 前像。验收 = spec §2.5。

- [ ] **Step 1: 写 IT 骨架 + 场景①（失败态：Task 6 前会 ISE——本任务时点已放行，预期真失败在值面）**

类骨架（对拍 harness 参照 `VastDmlParityIT` 的双路形态浓缩；基准路 = opengauss-jdbc 逻辑流 + `OgParityBaseline`，wal 路 = `WalSource` DML 面 direct 通道；**blob 族（88/95/9779/17757）四类型因渲染有意分叉不进双路对拍——走 wal 路字面断言**）：

```java
/**
 * VastBase B 档 DML 专形对拍 IT（M4，手动档 {@code -Dvb.it.vastbase=true}）：连
 * {@link VastBEnv}（172.16.107.133:55443/test_b，B 兼容档）。四场景——①13 类型全
 * 矩阵（双路对拍收 server-text 同形 9 型 + blob 族 4 型 wal 路字面锚）②'' 保留与
 * datetime 微秒截断语义面 ③varlena 6B 头混合（多列/NULL/TOAST 宽值）④UPDATE/DELETE
 * 前像（uint/year 的新类型前像渲染）。基准路 = opengauss-jdbc 逻辑流 +
 * OgParityBaseline 归一化（M3 先例：A 档 pgoutput 帧面无分叉，B 档同预期）；wal 路
 * = WalSource DML 面（vb.wal.og.source=direct）。卫生：vb_wal_b 前缀 + finally 清理。
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastBModeIT {
    private static final String SCHEMA = "vb_wal_bm";
    private static final String PUBLICATION = "vb_wal_bm_pub";
    private static final long AWAIT_MS = 60_000;
    // …（槽管理/WalSource 生命周期/await/exec 辅助——照 VastDmlParityIT 对应私有面
    //     浓缩复制，连接全部换 VastBEnv；此处不重复罗列，实施时以该类 816-900 行
    //     的 slots()/cleanupScenario()/await()/exec() 为模板）
```

场景①测试体（值全部 spike 锚）：

```java
    /** 场景①：13 类型全矩阵——双路对拍（int1/uint×4/year/clob/nvarchar2/raw 九型
     * server-text 同形）+ blob 族四型 wal 路字面锚（\x 有意分叉记档面）。 */
    @Test
    void bModeTypeMatrixParityAndBlobLiterals() throws Exception {
        String ddl = "create table %s.t_types (id int primary key, c_i1 int1, c_u1 int1 unsigned,"
                + " c_u2 smallint unsigned, c_u4 int unsigned, c_u8 bigint unsigned, c_yr year,"
                + " c_cl clob, c_nv nvarchar2(32), c_rw raw(16), c_bl blob, c_tb tinyblob,"
                + " c_bi binary(4), c_vb varbinary(8))";
        // 行 1 边界值 / 行 2 零值（INSERT→UPDATE→DELETE 三态，双路按 xid 交集 diff）
        // blob 族断言（wal 路 OutputRenderer 形态）：
        //   c_bl=\xdeadbeef  c_tb=\x00ff10  c_bi=\xaabbccdd  c_vb=\x11223344
        //   c_rw=0102030405060708090A0B0C0D0E0F10（raw 与服务端同形，参与双路对拍）
        ...
    }
```

（**实施说明**：`...` 处的双路 harness 调用按 `VastDmlParityIT.typeMatrixBoundaryRowsParityAcrossPaths`（297-356 行）的场景体浓缩复制——reset/DML/两路起停/xid 交集 diff 的既有样板；断言面按上表字面。）

- [ ] **Step 2: 场景②③④**

```java
    /** 场景②：B 档语义面——'' 保留（vs A 档 NULL）与 datetime 微秒截断（typmod=0
     * 存整秒）。wal 路字面断言：c_vc= （空串非 NULL）+ c_dt=2026-10-18 12:30:45。 */
    @Test
    void bModeEmptyStringPreservedAndDatetimeSecondPrecision() { ... }

    /** 场景③：varlena 6B 头混合——多 varlena 列/NULL 混排/TOAST 宽值（clob 大值
     * external 重组）。双路对拍（值面全 text 同形）。 */
    @Test
    void bModeVarlenaMixedNullsAndToastWideValues() { ... }

    /** 场景④：UPDATE/DELETE 前像——uint8/year/blob 列的前像渲染（REPLICA IDENTITY
     * FULL；uint8 前像无符号、blob 前像 \x）。 */
    @Test
    void bModeUpdateDeleteBeforeImagesWithNewTypes() { ... }
```

（场景②③④场景体同场景①样板：②表 `(id int, c_vc varchar(32), c_dt datetime)`，INSERT `('','...123' 微秒)`，断言 wal 路 `c_vc=` 空串与 `c_dt` 整秒；③表 `(id int, a clob, b nvarchar2(64), c blob)` 三 varlena 列，行含 NULL 混排 + 32KB clob（`repeat('x', 32768)`，TOAST external）；④表 = 场景①列子集，UPDATE 前像断言含 `c_u8=18446744073709551615`/`c_yr=2026`/`c_bl=\xdeadbeef`。）

- [ ] **Step 3: 跑 B 档 IT**

```bash
mvn test -pl vb-stream-wal-source -Dtest=VastBModeIT -Dvb.it.vastbase=true
```

预期：四场景绿。**失败排查次序**：值面 diff → 查渲染/解码锚；`Index -10` 类 → 查 6B 头接缝触发面（回 Task 1 锚）；基准路连接失败 → 重试环（VastBEnv 已带）。

- [ ] **Step 4: A 档回归复验（24 用例全量）**

```bash
mvn test -pl vb-stream-wal-source -Dtest='Vast*IT' -Dvb.it.vastbase=true
```

预期：既有 VastCatalogSyncIT/VastDmlParityIT/VastLifecycleIT/VastOfflineIT 24 用例 + 新 VastBModeIT 4 = 28 绿。

- [ ] **Step 5: Commit**

```bash
git add vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastBModeIT.java
git commit -m "test(wal-source): M4 B 专形对拍 IT 四场景——13 类型矩阵/'' 保留与 datetime 截断/varlena 6B 混合/新类型前像（手动档 test_b）" && git push
```

---

### Task 9: 文档收口

**Files:**
- Modify: `vb-stream-wal-source/CLAUDE.md`（M4 节 + 测试面对账）
- Modify: `vb-stream-wal-source/README.md`（B 档支持面）
- Modify: `CLAUDE.md`（根——wal-source 段 M4 一句）
- Modify: `docs/wal-source-vastbase-b-mode-spike.md`（若 Task 1 锚有勘误回填则已含）

- [ ] **Step 1: 模块 CLAUDE.md 增「VastBase 档 M4（B 兼容模式，2026-10-18）」节**

内容四段（对照 spec/实现实测回填）：范围（白名单分档/13 类型/6B 头接缝/-9 修复）；类型表（oid/kind/渲染——spec §2.1 表 + blob 族分叉记档）；6B 头接缝机制（t_hoff<24 判别 + A 档恒 0）；验收（VastBModeIT 四场景 + 用例数对账——离线/手动档增数按实际）。测试面节同步总数（569 → 实际值）。

- [ ] **Step 2: README + 根 CLAUDE.md**

README「openGauss 5.0.0 快速开始」后补一段 B 档说明：VastBase 档 B 兼容库 DML 面支持（M4）、类型清单、blob 族渲染形态说明。根 CLAUDE.md 的 wal-source 模块描述句追加 M4 短句（对照既有 M1/M2/M3 句式）。

- [ ] **Step 3: 全量回归 + commit**

```bash
mvn test -pl vb-stream-wal-source
git add vb-stream-wal-source/CLAUDE.md vb-stream-wal-source/README.md CLAUDE.md docs/
git commit -m "docs(wal-source): M4 收口——模块 CLAUDE.md M4 节/README B 档支持面/根 CLAUDE.md 同步" && git push
```

---

## Self-Review 记录

- **Spec 覆盖**：§2.1（Task 3/4）、§2.2（Task 1/2）、§2.3（Task 6）、§2.4（Task 5）、§2.5（Task 7/8）、§6 骨架九任务一一对应（Task 7 合并了骨架 7 的 env 与重试环；骨架 9 = Task 9）。§4 错误处理散在各任务边界语义。§2.1 的"raw 新 kind"在计划中改为复用 bytea kind + oid 键控渲染——渲染器本就 oid 键控，少一个 kind 是实现简化，spec 意图（raw 渲染分叉）不变。
- **占位符扫描**：Task 8 场景②③④以精确 DDL/值/断言描述 + 既有样板行号指代（浓缩复制非新设计），骨架 harness 部分指明模板行号——实施者可执行；其余任务代码完整。
- **类型一致性**：`xlHeaderExtraShift(byte[], int)` 签名 Task 2 定义并自用；`dmlCompatModes(WalLayout)`/`isDmlSupportedCompatMode(Connection, WalLayout)` Task 6 定义、测试同签；`VastBEnv` 访问器面 Task 7 定义、Task 8 消费。
