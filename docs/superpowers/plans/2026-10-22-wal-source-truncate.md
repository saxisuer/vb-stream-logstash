# vb-stream-wal-source TRUNCATE DML 支持 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** wal-source 识别 `XLOG_HEAP_TRUNCATE`（vanilla：RM_HEAP_ID opcode 0x30）WAL 记录并作为事务内事件发射，输出与 engine pgoutput 路逐字节同形——四布局档全支持。

**Architecture:** 记录视图归 layout 层（`HeapViews.TruncateView`，四档几何经 layout 分派）；`XactGrouper.dispatchHeap` 头部加 case（先于 blocks-empty 早退），逐 oid 经新 `TableFilter.resolveByOid` 解析表身份后与行同构入桶（sealed `BucketEntry` 泛化）；提交期 `emitBucket` 按 recLsn 序发射结构化 `TruncateEvent`（`ChangeOutputListener.onTruncate` 新回调）；`OutputRenderer` 渲染 `TRUNCATE [schema.t, ...] options=[...]` 与 engine `ConsoleRenderer.java:148-151` 逐字节同形。桶机制（回滚/子回滚/2PC/floor/离线回放/at-least-once）零新增状态全白拿。

**Tech Stack:** Java 17 + Maven + JUnit 6；Testcontainers（postgres:18 / opengauss:5.0.0）；VastBase 真库手动档（172.16.107.133）。

**Spec:** `docs/superpowers/specs/2026-10-22-wal-source-truncate-design.md`（含 2026-10-22 勘误——vanilla 侧源码实锚；本计划Task 0 的 spike 锚定结果是 spec §4 开放面的唯一裁决来源，**执行 Task 1 前必须先完成 Task 0**）

## Global Constraints

- Java 17（record pattern switch 的严格模式不可用——instanceof 链）；方法名一律英文 camelCase；类型引用 import 简名禁 FQN 内联；日志走 slf4j `{}` 占位符禁 System.out；**每个函数含私有/测试辅助必须有 javadoc 逻辑描述**（职责/步骤/边界/线程约束）
- wal-source main 面零 engine import（模块依赖方向单向——对齐只靠对拍测试）；测试面 engine 仅以 test-scope 进对拍 harness
- 测试命令：`mvn test -pl vb-stream-wal-source -Dtest=XxxTest`（多模块后 -Dtest 须带 -pl）；全模块回归 `mvn test -pl vb-stream-wal-source`
- 每个任务完成即 commit + push（跨机开发纪律）；commit message 首词 `feat(wal-source)`/`test(wal-source)`/`docs(wal-source)`
- 常量必须标注出处（REL_STABLE 源码行/og-plan 实测/真库实测——沿 `HeapOps` 既有 javadoc 风格）
- Windows 开发机：bash shell；Docker Desktop 本地可用（IT 需）；VastBase 真库 IT 以 `-Dvb.it.vastbase=true` 手动跑
- vanilla 几何已实锚（REL_17/18_STABLE `heapam_xlog.h` + Task 0 spike 黄金字节，17/18 逐字同）：opcode 0x30、`{Oid dbId; uint32 nrelids; uint8 flags; Oid relids[]}`、CASCADE=1<<0 / RESTART_SEQS=1<<1——**relids 起点已裁定 @12**（`Oid relids[]` 4 对齐使 offsetof=12；@9-11 是未初始化栈垫垃圾，**禁止读/禁止校验为零**）；`mainLen == 12 + 4×nrelids` 精确成立（序列重启走前置 RM_SEQ 记录，**无 relids 尾随**——heapam_xlog.h "followed by sequence relids" 注释是 pre-PG10 残留，spike §1.1 勘误）
- **Task 0 spike 裁定**（`docs/wal-source-truncate-spike.md` 裁决表）：TRUNCATE 捕获是 **vanilla 17/18 独有**能力——OG 5.0.0 与 VastBase 双双裁决 C（WAL 无专用记录[纯 catalog 手术] + 逻辑流不发 'T' 帧）；Task 6/7 已按此重塑为"零事件不砖流 + 截后 DML 字典正确"断言与记档

---

### Task 0: spike——四档 XLOG_HEAP_TRUNCATE 记录形态锚定

**Files:**
- Create: `docs/wal-source-truncate-spike.md`（spike 报告，仓内既有 spike 文档风格）
- Create: `spike/truncate/`（throwaway harness 与黄金字节 dump，gitignore 外的未跟踪残留可接受——沿 `spike/offline-replay/` 先例）

**Interfaces:**
- Produces: spike 文档四节（§vanilla18/§pg17/§OG/§VastBase）——每节含：rmid/info、main 区 hex dump + 逐字段标注、relids 起点 @9/@12 裁定、flags 位实测、基准路 'T' 帧样本；§裁决表（Task 6 消费：OG 系归 heap 0x30 同构 / XLOG 老 rmgr / 其他）

**步骤：**

- [ ] **Step 1: PG 18 采集**——起本地库并制造覆盖面

```bash
cd src/docker && docker compose up -d && cd ../..
# 建表 + 全形态 TRUNCATE（psql 容器内执行；记录每语句前 pg_current_wal_insert_lsn()）
docker exec -i src-docker-postgres-1 psql -U postgres -d postgres <<'SQL'
CREATE TABLE t_tr1(id int4, v text);
CREATE TABLE t_tr2(id int4);
SELECT pg_current_wal_insert_lsn();
TRUNCATE t_tr1;                                     -- 单表 flags=0
SELECT pg_current_wal_insert_lsn();
TRUNCATE t_tr1, t_tr2;                              -- 多表一语句
SELECT pg_current_wal_insert_lsn();
TRUNCATE t_tr1 RESTART IDENTITY;                    -- RESTART_SEQS 位（若无序列则 flags 仍可采）
SELECT pg_current_wal_insert_lsn();
BEGIN; INSERT INTO t_tr1 VALUES(1,'a'); TRUNCATE t_tr2; ROLLBACK;   -- 事务性 + 回滚记录在场
SQL
```

- [ ] **Step 2: dump harness**——throwaway 单测（test 树外放 `spike/truncate/`，用 `mvn -q test-compile` 后 java -cp 跑，或临时塞进 test 树再删）。核心：用既有 `WalSegmentRecorder`（`it/vastbase` 测试树工具——DirectWalSource 收流按 LSN 落段）收 PG 18 流，`WalRecordParser.parse` 走读，过滤 `(rmid==10 && (info & 0x70)==0x30)` 打印 `lsn/mainLen/hex(main)`：

```java
// 骨架：收段 → 逐记录过滤 dump（真实装配参考 VastOfflineIT 对 WalSegmentRecorder 的用法）
for (WalRecord r : parsed) {
    if (r.rmid() == 10 && (r.info() & 0x70) == 0x30) {
        System.out.printf("lsn=%s mainLen=%d main=%s%n", Long.toHexString(r.lsn()), r.mainLen(),
                HexFormat.of().formatHex(java.util.Arrays.copyOfRange(r.raw(), r.mainOff(), r.mainOff() + r.mainLen())));
    }
}
```

  先用 `pg_waldump`（`docker exec src-docker-postgres-1 pg_waldump -p $PGDATA -s <起点lsn>`）定位 TRUNCATE 记录行（desc 形如 `TRUNCATE (nrelids=...)`）交叉验证 nrelids/flags 读数。
  **预期**（源码推断，待字节裁决）：`main = [dbId u32][nrelids u32][flags u8][relid u32×n @9 起]`——若首 relid 读出的 oid 恰等于被截表的 pg_class.oid（`SELECT 't_tr1'::regclass::oid`）则 @9 成立，否则试 @12。

- [ ] **Step 3: PG 17 对照**——`docker run -d --name pg17-trunc -e POSTGRES_PASSWORD=postgres -p 55499:5432 postgres:17` + wal_level=logical（`docker exec pg17-trunc psql -c "ALTER SYSTEM SET wal_level=logical" -c "SELECT pg_reload_conf()"` 后重启）重复 Step 1/2，确认 17≡18（预期同构——两版头文件逐字同）。

- [ ] **Step 4: OG 5.0.0 采集**——起 `opengauss/opengauss:5.0.0` 容器（装配配方抄 `OgTestEnv`：密码/hba replication 行/`DBCOMPATIBILITY 'PG'` 建库——见 `vb-stream-wal-source/src/test/java/.../it/OgTestEnv.java`），重复 TRUNCATE 语句族 + dump。**未知全开放**：rmid 是 10（heap）还是 0（XLOG）？payload 键是 oid 还是别的几何？TRUNCATE 与 pg_class 换段号记录（9.2 pre-12 语义）相对次序？把 walker 解析失败的记录 totLen/rmid/info 也 dump 出来（OG 记录头 32B，用 `WalLayoutVOpenGauss.INSTANCE` 解析）。

- [ ] **Step 5: OG/VastBase 基准路 'T' 帧采样**——throwaway harness 用 opengauss-jdbc 逻辑流（装配抄 `OgParityBaseline`——proto_version=1、publication 前置、专用角色/hba sha256 行的坑全在该类注释里）收 TRUNCATE 事务，打印收到的每帧 type 字节：**'T' 帧到没到、报什么 oid/身份（母表 or 分区）、options 位**。分区内再跑一次 `TRUNCATE 母表`（OG 建 range 分区表配方抄 `OgPartitionIT`——显式 FOR TABLE 母表 publication + 母表 REPLICA IDENTITY FULL 两前提）。VastBase 真库（172.16.107.133:55433/test_a，连接纪律抄 `VastTestEnv`）重复 Step 4/5。

- [ ] **Step 6: engine 路渲染样本**——PG 18 上跑 engine 逻辑解码路收同批 TRUNCATE（装配抄 `DualPathParityIT.EnginePathRunner`——in-process `PgReplicationSession + ConsoleRenderer` + ListAppender 捕 CDC logger），采集 `TRUNCATE [...] options=[...]` 行字面（含 CASCADE/RESTART 双位形态的 toString 序）——对拍锚的契约源样本。

- [ ] **Step 7: 写 spike 文档并 commit**——四节 + 裁决表（Task 6 的实现分支依据）；黄金字节以 hex 块入文档。`git add docs/wal-source-truncate-spike.md && git commit -m "docs(wal-source): TRUNCATE spike——四档记录形态锚定" && git push`

---

### Task 1: HeapOps 常量 + HeapViews.TruncateView（vanilla 几何）

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/HeapOps.java`（增三常量）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/HeapViews.java`（增嵌套 `TruncateView` record）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TruncateViewTest.java`（新建）

**Interfaces:**
- Consumes: `WalRecord`（record：`rmid()/info()/mainOff()/mainLen()/raw()`）；`WalBytes.record(rmid, info, xid).main(bytes).build()`（test 树 DSL）
- Produces: `HeapOps.XLOG_HEAP_TRUNCATE`（=0x30）、`HeapOps.XLH_TRUNCATE_CASCADE`（=1）、`HeapOps.XLH_TRUNCATE_RESTART_SEQS`（=2）、`HeapViews.TruncateView(int nrelids, long[] relids, int flags)` + `static TruncateView parse(WalRecord r, WalLayout layout)`

- [ ] **Step 1: 写失败测试**——`TruncateViewTest`（手造字节，黄金值 = Task 0 Step 2 的真库 dump；若 spike 裁定 relids@12，`buildMain` 与断言随调）：

```java
package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import static org.junit.jupiter.api.Assertions.*;

/** {@link HeapViews.TruncateView} 的失败先行测试——vanilla 几何（REL_17/18_STABLE
 * heapam_xlog.h + spike 黄金字节，@12 裁定）：[dbId u32@0][nrelids u32@4][flags u8@8]
 * [3B 未初始化栈垫@9-11][relid u32×n @12 起]；错配 rmid/opcode 抛 ISE；
 * mainLen ≠ 12+4nrelids 抛 ISE（错位自检）。 */
class TruncateViewTest {

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 手造 main：12B 头（含 @9-11 未初始化栈垫——spike 实测非零垃圾，视图不读）+ relids 数组。 */
    private byte[] buildMain(int flags, long... relids) {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 16385L);                        // dbId（视图不消费，占位真值）
        put32(m, relids.length);                 // nrelids
        m.write(flags);                          // flags u8@8
        m.write(0xe4); m.write(0xdb); m.write(0x61);   // @9-11 栈垫（spike 黄金样本同形——非零）
        for (long rel : relids) put32(m, rel);   // relids @12 起（spike 裁定）
        return m.toByteArray();
    }

    private static void put32(ByteArrayOutputStream m, long v) {
        for (int i = 0; i < 4; i++) m.write((int) (v >>> (8 * i)) & 0xFF);
    }

    @Test
    void parsesSingleTableNoOptions() {
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(buildMain(0, 24600L)).build(), 0x100000L, layout);
        HeapViews.TruncateView v = HeapViews.TruncateView.parse(r, layout);
        assertEquals(1, v.nrelids());
        assertArrayEquals(new long[]{24600L}, v.relids());
        assertEquals(0, v.flags());
    }

    @Test
    void parsesMultiTableWithBothOptionBits() {
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100)
                .main(buildMain(HeapOps.XLH_TRUNCATE_CASCADE | HeapOps.XLH_TRUNCATE_RESTART_SEQS,
                        24600L, 24601L, 24602L)).build(), 0x100000L, layout);
        HeapViews.TruncateView v = HeapViews.TruncateView.parse(r, layout);
        assertEquals(3, v.nrelids());
        assertArrayEquals(new long[]{24600L, 24601L, 24602L}, v.relids());
        assertEquals(3, v.flags());
    }

    @Test
    void rejectsMismatchedRmidOrOpcode() {
        WalRecord notHeap = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP2_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(buildMain(0, 1L)).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(notHeap, layout));
    }

    @Test
    void rejectsRelidsBeyondMainLen() {
        // nrelids=2 但 main 只有 1 个 relid 的空间
        byte[] bad = buildMain(0, 24600L);
        bad[4] = 2;
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(bad).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(r, layout));
    }

    @Test
    void rejectsMainTooShort() {
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(new byte[5]).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(r, layout));
    }

    @Test
    void rejectsTrailingBytesBeyondDeclaredNrelids() {
        // nrelids=1 但 main 携带 2 个 relid 的空间——精确自检（mainLen == 12+4n）拒绝
        byte[] bad = buildMain(0, 24600L, 24601L);
        bad[4] = 1;
        WalRecord r = WalRecordParser.parse(WalBytes.record(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_TRUNCATE, 100).main(bad).build(), 0x100000L, layout);
        assertThrows(IllegalStateException.class, () -> HeapViews.TruncateView.parse(r, layout));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=TruncateViewTest`
Expected: FAIL——`TruncateView`/`XLOG_HEAP_TRUNCATE` 符号不存在（编译错）

- [ ] **Step 3: 实现**——`HeapOps` 增常量（javadoc 标 REL_17/18_STABLE 出处 + spike 文档指针）：

```java
/** heapam_xlog.h：XLOG_HEAP_TRUNCATE（opcode 位段 0x30——事务性 TRUNCATE 的记录族，
 *  DELETE 0x10 与 HOT_UPDATE 0x40 之间；REL_17/18_STABLE 逐字同，2026-10-22 实锚。
 *  载荷 xl_heap_truncate{Oid dbId; uint32 nrelids; uint8 flags; Oid relids[]}——
 *  relids 是 <b>oid 数组</b>@12 起（@9-11 是未初始化栈垫，spike 字节裁定）；写点
 *  tablecmds.c ExecuteTruncateGuts，仅 wal_level≥logical 时写（源码 Assert）。
 *  OG 系 9.2 内核无此记录（TRUNCATE 纯 catalog 手术——spike 裁决 C，
 *  docs/wal-source-truncate-spike.md）。 */
public static final int XLOG_HEAP_TRUNCATE = 0x30;

/** heapam_xlog.h：XLH_TRUNCATE_CASCADE（1&lt;&lt;0——级联截子表/分区）。 */
public static final int XLH_TRUNCATE_CASCADE = 1 << 0;

/** heapam_xlog.h：XLH_TRUNCATE_RESTART_SEQS（1&lt;&lt;1——RESTART IDENTITY；该位时
 *  relids 数组尾随待重启序列 oid——解析面只消费前 nrelids 个）。 */
public static final int XLH_TRUNCATE_RESTART_SEQS = 1 << 1;
```

`HeapViews` 增嵌套 record（几何已 spike 裁定 @12）：

```java
/** XLOG_HEAP_TRUNCATE 记录视图：被截关系 oid 数组 + flags 位图（vanilla 几何，
 * REL_17/18_STABLE 实锚 + spike 黄金字节 @12 裁定）。
 *
 * @param nrelids 被截关系数
 * @param relids  pg_class oid 数组（payload 序保持——engine relationOids 同源序）
 * @param flags   原样位图（{@link HeapOps#XLH_TRUNCATE_CASCADE}/
 *                {@link HeapOps#XLH_TRUNCATE_RESTART_SEQS}）
 */
record TruncateView(int nrelids, long[] relids, int flags) {

    /** relids 数组起点（SizeOfHeapTruncate=12——Oid relids[] 4 对齐使 offsetof=12；
     * @9-11 是未初始化栈垫垃圾，禁止读/禁止校验为零，spike §1 勘误）。 */
    private static final int RELIDS_OFF = 12;

    /**
     * 解析一条 truncate 记录的 main data：校验 rmid==RM_HEAP_ID 且 opcode==
     * XLOG_HEAP_TRUNCATE（错配 ISE）；走读 [dbId u32@0][nrelids u32@4][flags u8@8]
     * [3B 栈垫][relid u32×n@12 起]。边界与异常语义：mainLen &lt; RELIDS_OFF 或
     * mainLen != RELIDS_OFF + 4×nrelids 抛 ISE——精确等式自检（PG 17/18 序列重启
     * 走前置 RM_SEQ 记录、relids 无尾随，spike §1.1 勘误）；纯函数无 IO。
     *
     * @param r      走读完成的记录（raw 契约只读）
     * @param layout 版本布局（签名与既有视图对齐；OG 系无 truncate 记录——spike
     *               裁决 C，恒走 vanilla 分支）
     * @return 视图
     */
    static TruncateView parse(WalRecord r, WalLayout layout) {
        requireOp(r, HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_TRUNCATE);
        if (r.mainLen() < RELIDS_OFF) {
            throw new IllegalStateException("truncate main too short: " + r.mainLen());
        }
        byte[] raw = r.raw();
        int off = r.mainOff();
        int n = (int) u32(raw, off + 4);
        if (r.mainLen() != RELIDS_OFF + 4L * n) {
            throw new IllegalStateException("truncate 几何错位: nrelids=" + n
                    + ", mainLen=" + r.mainLen() + "（期望 " + (RELIDS_OFF + 4L * n) + "）");
        }
        long[] relids = new long[n];
        for (int i = 0; i < n; i++) {
            relids[i] = u32(raw, off + RELIDS_OFF + 4 * i);
        }
        return new TruncateView(n, relids, raw[off + 8] & 0xFF);
    }
}
```

  注：`requireOp`/`u32` 若在 `HeapViews` 内为私有，将 `u32` 提为包内静态（或视图内内联四字节小端读）——与既有视图同风格即可。Task 1 Step 1 期望值随 @12 连动：n=1 → mainLen=16。

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl vb-stream-wal-source -Dtest=TruncateViewTest`
Expected: PASS（5 用例）

- [ ] **Step 5: Commit**——`git add -A vb-stream-wal-source && git commit -m "feat(wal-source): TruncateView 记录视图 + heap truncate 常量（vanilla 几何 REL_STABLE 实锚 + spike 黄金字节）" && git push`

---

### Task 2: 契约扩展 TruncateOption/TruncateEvent/onTruncate + 全实现方 + 渲染

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/ChangeOutputListener.java`（嵌套 enum + record + 抽象回调）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/OutputRenderer.java`（`onTruncate` 实现 + `renderTruncate`）
- Modify（接口新抽象方法的实现方，编译面全清）: `XactGrouperTest`（RecordingListener@~924）、`ChangeStreamTest`（RecordingListener@~392）、`OgPartitionIdentityTest`（@~382）、`OgXactGrouperSeamTest`（@~523）+ `it` 包捕获监听器（`rg "implements ChangeOutputListener" vb-stream-wal-source/src -l` 兜底清点）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/OutputRendererTest.java`（增用例）

**Interfaces:**
- Consumes: `TableMeta`（`schema()/table()`）
- Produces: `ChangeOutputListener.TruncateOption`（enum，声明序 CASCADE→RESTART_IDENTITY）、`ChangeOutputListener.TruncateEvent(long xid, long commitLsn, long lsn, List<TableMeta> tables, Set<TruncateOption> options)` + 两参便捷构造、`void onTruncate(TruncateEvent event)`

- [ ] **Step 1: 写失败测试**——`OutputRendererTest` 增两用例（沿该文件既有 ListAppender 挂 CDC logger 的 @BeforeEach 形态）：

```java
/** TRUNCATE 行渲染字面锚——engine ConsoleRenderer renderChange 的 TruncateChange 分支
 * L148-151 逐字节同形：`TRUNCATE [public.t1, public.t2] options=[...]`。 */
@Test
void truncateEventRendersEngineIdenticalLine() {
    OutputRenderer renderer = new OutputRenderer();
    renderer.onBegin(new ChangeOutputListener.BatchBegin(9L, false, null, 0x100L, 0x108L, Instant.EPOCH, 1));
    renderer.onTruncate(new ChangeOutputListener.TruncateEvent(
            List.of(meta("public", "t1"), meta("public", "t2")),
            EnumSet.of(ChangeOutputListener.TruncateOption.CASCADE)));
    renderer.onEnd(new ChangeOutputListener.BatchEnd(9L, 1, 1));
    assertEquals(List.of(
            "TXN-BEGIN xid=9 kind=NORMAL gid=null commitLsn=0x100 commitTs=1970-01-01T00:00:00Z changes=1",
            "  [1] TRUNCATE [public.t1, public.t2] options=[CASCADE]",
            "TXN-END   xid=9"), cdcLines());
}

/** 双选项位 toString 序锚——EnumSet 声明序（CASCADE 在前），与 engine 同形。 */
@Test
void truncateBothOptionsRenderInDeclarationOrder() {
    OutputRenderer renderer = new OutputRenderer();
    renderer.onBegin(new ChangeOutputListener.BatchBegin(9L, false, null, 0x100L, 0x108L, Instant.EPOCH, 1));
    renderer.onTruncate(new ChangeOutputListener.TruncateEvent(
            List.of(meta("public", "t1")),
            EnumSet.allOf(ChangeOutputListener.TruncateOption.class)));
    renderer.onEnd(new ChangeOutputListener.BatchEnd(9L, 1, 1));
    assertEquals("  [1] TRUNCATE [public.t1] options=[CASCADE, RESTART_IDENTITY]", cdcLines().get(1));
}
```

  （`meta(...)`/`cdcLines()` 沿该测试文件既有辅助；若无 `meta` 辅助则内联 `new TableMeta(oid, schema, name, List.of())`。）

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=OutputRendererTest`
Expected: FAIL——`TruncateEvent` 符号不存在（编译错）

- [ ] **Step 3: 实现契约**——`ChangeOutputListener` 增（嵌套于既有文件，javadoc 按仓惯例）：

```java
/** TRUNCATE 选项位（wal-source 自有枚举——声明序与 engine TruncateOption 一致，
 * EnumSet.toString 渲染同形；不 import 引擎，DmlKind 同惯例）。 */
enum TruncateOption {

    /** heapam_xlog.h XLH_TRUNCATE_CASCADE（1<<0——级联截子表/分区）。 */
    CASCADE,

    /** heapam_xlog.h XLH_TRUNCATE_RESTART_SEQS（1<<1——RESTART IDENTITY）。 */
    RESTART_IDENTITY
}

/**
 * 单语句 TRUNCATE 事件（一条语句可截多表——payload relids 数组，过滤后命中表集）。
 *
 * <p>位址三字段语义与 {@link RowChange} 同源：发射层（Grouper）在 End 发射期填充
 * （到达期 commitLsn 未知占位 0）。{@code options} 经 EnumSet 拷贝——迭代序恒为
 * 声明序（渲染 toString 与 engine 逐字节同形的保证，Set.copyOf 迭代序无规格保证）。</p>
 *
 * @param xid        事务归属 id（与 Begin/End 同源；发射时填充）
 * @param commitLsn  提交记录 lsn（与 BatchBegin.commitLsn 同值；发射时填充）
 * @param lsn        truncate 记录起始 LSN（发射时填充）
 * @param tables     过滤后命中的表身份（payload 序保持；至少一表——全 miss 不产事件）
 * @param options    选项位集
 */
record TruncateEvent(long xid, long commitLsn, long lsn, List<TableMeta> tables,
                     Set<TruncateOption> options) {

    /**
     * 两参便捷构造（位址三字段占位 0）：手造事件的单测/宿主装配面。
     *
     * @param tables   表身份集
     * @param options  选项位集
     */
    TruncateEvent(List<TableMeta> tables, Set<TruncateOption> options) {
        this(0L, 0L, 0L, tables, options);
    }

    /** 紧凑构造器：tables 防御拷贝；options 经 EnumSet 拷贝保声明序迭代。 */
    TruncateEvent {
        tables = List.copyOf(tables);
        options = options.isEmpty()
                ? java.util.EnumSet.noneOf(TruncateOption.class)
                : java.util.EnumSet.copyOf(options);
    }
}

/**
 * 单语句 TRUNCATE 回调（回放线程；Begin 之后、End 之前，按 WAL 序——与 onRow
 * 同缝互排）。回滚事务的 truncate 随桶丢弃，不回调。
 *
 * @param event truncate 事件
 */
void onTruncate(TruncateEvent event);
```

  注意：import 面按仓规用简名（`EnumSet` 加 import，不内联 FQN——上面代码里 `java.util.EnumSet` 是为计划自包含，落码时改 import）。

- [ ] **Step 4: 实现方全清**——`OutputRenderer` 增：

```java
/** 单语句 TRUNCATE（契约源：engine ConsoleRenderer renderChange 的 TruncateChange
 * 分支 L148-151——{@code TRUNCATE [schema.t, ...] options=[...]}，表集 List.toString、
 * options EnumSet.toString）。无开桶 ISE（同 onRow 契约）。线程约束：单写者。 */
@Override
public void onTruncate(TruncateEvent event) {
    if (current == null) {
        throw new IllegalStateException("onTruncate 先于 onBegin 到达（契约：Begin 先于本事务任何事件）");
    }
    current.rows.add(renderTruncate(event));
}

/** truncate 行文本：表集映 schema.table 后 List.toString，options 直接 toString
 * （EnumSet 声明序）。线程约束：单写者线程。 */
private String renderTruncate(TruncateEvent event) {
    List<String> names = new ArrayList<>();
    for (TableMeta t : event.tables()) {
        names.add(t.schema() + "." + t.table());
    }
    return "TRUNCATE %s options=%s".formatted(names, event.options());
}
```

  四个测试 RecordingListener + it 包捕获监听器各加空实现或记录实现（RecordingListener 形态）：

```java
@Override
public void onTruncate(ChangeOutputListener.TruncateEvent event) {
    events.add("TRUNCATE " + event.tables().stream()
            .map(t -> t.schema() + "." + t.table()).toList() + " options=" + event.options());
}
```

  it 包监听器（对拍捕获面）以同样记录实现接入（Task 5 的 parse 面消费）。

- [ ] **Step 5: 跑测试确认通过 + 全模块编译面清**

Run: `mvn test -pl vb-stream-wal-source -Dtest=OutputRendererTest && mvn -q compile -pl vb-stream-wal-source`
Expected: PASS；`mvn test-compile` 无残留 `onTruncate` 未实现编译错（`rg "implements ChangeOutputListener" -l` 逐个核对）

- [ ] **Step 6: Commit**——`git commit -m "feat(wal-source): ChangeOutputListener 增 TruncateEvent/onTruncate 契约 + OutputRenderer engine 同形渲染"` + push

---

### Task 3: XactGrouper——resolveByOid + dispatchHeap case + 桶条目泛化 + 事务组装

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/TableFilter.java`（`resolveByOid` + `resolveThreeTable` refactor）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/XactGrouper.java`（dispatchHeap case / BucketEntry 泛化 / `appendTruncateRecord` / `emitBucket` 分派 / `emittedTruncates` 计数）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/XactGrouperTest.java`（Fixture 增 `truncate(...)` 构建器 + 用例群）；`TableFilterTest`（`resolveByOid` 用例）

**Interfaces:**
- Consumes: Task 1 的 `HeapViews.TruncateView.parse(rec, layout)`/常量；Task 2 的 `TruncateEvent/TruncateOption/onTruncate`
- Produces: `TableFilter.resolveByOid(long oid)` → `Optional<TableMeta>`；`XactGrouper.emittedTruncates()` → long（Task 4 穿线）；sealed `BucketEntry`（私有面）

- [ ] **Step 1: 写失败测试**——`XactGrouperTest` Fixture 增构建器（置于既有 `insert(...)` 旁）：

```java
/** 一条 TRUNCATE 记录（RM_HEAP_ID opcode 0x30，零块引用；main = 12B 头[含 @9-11
 * 栈垫] + relids 数组——relids 传 pg_class oid，与 stub 快照的 table 键同值；
 * spike @12 黄金几何）。 */
WalRecord truncate(int xid, int toplevel, int flags, long... relids) {
    ByteArrayOutputStream m = new ByteArrayOutputStream();
    put32(m, 16385L);                        // dbId（视图不消费）
    put32(m, relids.length);                 // nrelids
    m.write(flags);                          // flags u8@8
    m.write(0xe4); m.write(0xdb); m.write(0x61);   // @9-11 栈垫（非零垃圾——视图不读）
    for (long rel : relids) put32(m, rel);   // relids @12 起（spike 裁定）
    return feed(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_TRUNCATE, xid)
            .toplevel(toplevel).main(m.toByteArray()).build());
}
```

  用例群（每个一 @Test，方法名行为描述式；测试类常量区增 `private static final long MOTHER_REL = 24603L;`——分区母表 oid，relkind 'p'，stub 快照按 'p' 注册）：

```java
/** TRUNCATE 与行同桶按 WAL 序发射，expected 含 truncate 条目（对齐 engine unitCount）。 */
@Test
void truncateEmitsInBucketOrderAndCountsExpected() {
    Fixture fx = fixture();
    fx.insert(100, 0, 1, "a");
    fx.truncate(100, 0, 0, USER_REL);
    fx.commit(100);
    assertEquals(List.of(
            "BEGIN xid=100 2p=false gid=null exp=2",
            "ROW INSERT public.t_stream before=null after={id=1, name=a}",
            "TRUNCATE [public.t_stream] options=[]",
            "END xid=100 emitted=2 exp=2"), fx.listener.events);
}

/** 多表一语句：payload 序全量入列；选项双位 toString 声明序。 */
@Test
void truncateMultiTableWithBothOptionsKeepsPayloadOrder() {
    Fixture fx = fixture();
    fx.truncate(100, 0, HeapOps.XLH_TRUNCATE_CASCADE | HeapOps.XLH_TRUNCATE_RESTART_SEQS,
            USER_REL, WIDE_REL);
    fx.commit(100);
    assertEquals(List.of(
            "BEGIN xid=100 2p=false gid=null exp=1",
            "TRUNCATE [public.t_stream, public.t_wide] options=[CASCADE, RESTART_IDENTITY]",
            "END xid=100 emitted=1 exp=1"), fx.listener.events);
}

/** 回滚整桶弃——truncate 随桶零输出。 */
@Test
void truncatedTransactionDiscardsTruncateWithBucket() {
    Fixture fx = fixture();
    fx.truncate(300, 0, 0, USER_REL);
    fx.abort(300, 0);
    assertEquals(List.of("ABORTED xid=300"), fx.listener.events);
}

/** SAVEPOINT 子回滚扣减 truncate 条目——expected=过滤前、emitted=过滤后。 */
@Test
void savepointSubTruncateDeductedLikeRows() {
    Fixture fx = fixture();
    fx.insert(5, 0, 1, "top");
    fx.truncate(7, 5, 0, USER_REL);        // 子事务 truncate（TOPLEVEL 标记归并到 5）
    fx.abort(7, 5);
    fx.commit(5);
    assertEquals(List.of(
            "BEGIN xid=5 2p=false gid=null exp=2",
            "ROW INSERT public.t_stream before=null after={id=1, name=top}",
            "END xid=5 emitted=1 exp=2"), fx.listener.events);
}

/** 白名单 per-table 过滤——部分命中事件只携命中表（对齐 pgoutput per-relation 语义）。 */
@Test
void whitelistFiltersTruncatePerTable() {
    StubSnapshot snapshot = new StubSnapshot();
    snapshot.table(USER_REL, "public", "t_stream", "r",
            new CatalogSnapshot.Column(1, "id", 23, false));
    snapshot.table(WIDE_REL, "public", "t_wide", "r",
            new CatalogSnapshot.Column(1, "id", 23, false));
    RecordingListener listener = new RecordingListener();
    XactGrouper grouper = new XactGrouper(snapshot, java.util.Set.of("public.t_stream"),
            layout, new ToastAssembler(null), listener);
    Fixture fx = new Fixture(snapshot, new ToastAssembler(null), grouper, listener);
    fx.truncate(100, 0, 0, USER_REL, WIDE_REL);
    fx.commit(100);
    assertEquals(List.of(
            "BEGIN xid=100 2p=false gid=null exp=1",
            "TRUNCATE [public.t_stream] options=[]",
            "END xid=100 emitted=1 exp=1"), listener.events);
}

/** 全 miss（oid 不在字典）零记账零追加——对齐 pgoutput nrelids==0 不发帧。 */
@Test
void truncateAllTablesMissedEmitsNothing() {
    Fixture fx = fixture();
    fx.truncate(100, 0, 0, 24999L);
    fx.commit(100);
    assertEquals(List.of(), fx.listener.events);
}

/** truncate-only 事务建桶——pendingFloorLsn 覆盖（重放下界源）。 */
@Test
void truncateOnlyTransactionSetsPendingFloor() {
    Fixture fx = fixture();
    WalRecord tr = fx.truncate(100, 0, 0, USER_REL);
    assertEquals(tr.lsn(), fx.grouper.pendingFloorLsn());
}

/** relkind='p' 分区母表从事件滤除（spike §5.2-2——pgoutput 'T' 帧面母表缺席，
 * WAL relids 含母表；wal 路同构滤除）。 */
@Test
void partitionedMotherOidFilteredFromTruncateEvent() {
    StubSnapshot snapshot = new StubSnapshot();
    snapshot.table(MOTHER_REL, "public", "t_part_m", "p",
            new CatalogSnapshot.Column(1, "i", 23, false));
    snapshot.table(USER_REL, "public", "t_stream", "r",
            new CatalogSnapshot.Column(1, "id", 23, false));
    RecordingListener listener = new RecordingListener();
    XactGrouper grouper = new XactGrouper(snapshot, null, layout, new ToastAssembler(null), listener);
    Fixture fx = new Fixture(snapshot, new ToastAssembler(null), grouper, listener);
    fx.truncate(100, 0, 0, MOTHER_REL, USER_REL);   // payload 序 [母表, 普通表]
    fx.commit(100);
    assertEquals(List.of(
            "BEGIN xid=100 2p=false gid=null exp=1",
            "TRUNCATE [public.t_stream] options=[]",
            "END xid=100 emitted=1 exp=1"), listener.events);
}
```

  `TableFilterTest` 增 `resolveByOid` 两用例：①oid 直查命中（stub 三表在册的普通表）；②**反查碰撞防御锚**——构造 relfilenode 反查会先命中的键值时 `resolveByOid` 仍按 oid 判（沿该文件既有 StubSnapshot 形态手造字典行）。

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=XactGrouperTest,TableFilterTest`
Expected: FAIL——`truncate(...)` 构建器/`resolveByOid` 不存在（编译错）

- [ ] **Step 3: 实现 TableFilter.resolveByOid + refactor**——`resolveThreeTable` 的 `relOidOf` 后半段抽出：

```java
/**
 * 按关系 oid 直查表身份（truncate 载荷键是 oid——服务端 decode.c 即 oid 直开关系）：
 * 三道闸（relkind[OG 档含 parttype]/系统模式/白名单）与 {@link #resolveThreeTable}
 * 同构，但<b>不经 relOidOf 的 relfilenode 反查</b>——反查优先在 EXCHANGE 换绑段号/
 * oid 数值碰撞面会产出错误属主（M3 T5 已证该碰撞类真实存在）。
 *
 * <p>边界与异常语义：任一闸不过或字典缺行返回 empty（不抛）。线程约束：并发安全
 * （持不可变引用，同 {@link #resolve}）。</p>
 *
 * @param oid 关系 oid（pg_class.oid）
 * @return 表身份 + 列序；miss 为 empty
 */
public Optional<TableMeta> resolveByOid(long oid) {
    Optional<String> relkind = snapshot.relkindOf(oid);
    if (relkind.isEmpty() || !isUserTableKind(relkind.get(), oid)) {
        return Optional.empty();
    }
    Optional<String> schema = snapshot.schemaOf(oid);
    if (schema.isEmpty() || SYSTEM_SCHEMAS.contains(schema.get())) {
        return Optional.empty();
    }
    Optional<String> name = snapshot.nameOf(oid);
    if (name.isEmpty()) {
        return Optional.empty();
    }
    if (!whitelist.isEmpty() && !whitelist.contains(schema.get() + "." + name.get())) {
        return Optional.empty();
    }
    return Optional.of(assembleMeta(oid, schema.get(), name.get()));
}
```

  `resolveThreeTable` 改为 `relOidOf` + `return resolveByOid(oid);`（行为零变化——既有 TableFilterTest 17 用例全绿作回归锚）。注意 OG 档 `isUserTableKind` 内的 parttype 闸以 oid 查询——本方法 OG 语义自然成立。

- [ ] **Step 4: 实现 XactGrouper**——四处改动：

  ① `dispatchHeap` 头部（`XactGrouper.java:697` 起，**case 置于 blocks-empty 早退之前**）：

```java
private void dispatchHeap(WalRecord rec) {
    int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
    if (op == HeapOps.XLOG_HEAP_TRUNCATE) {
        appendTruncateRecord(rec);   // 零块引用的事务性 TRUNCATE——先于 blocks 早退
        return;
    }
    if (rec.blocks().isEmpty()) {
        return;
    }
    if (op == HeapOps.XLOG_HEAP_INPLACE) {
        return;   // ...原注释保留
    }
    // ...原体不动（原 blocks 空判后置的 INPLACE 判随 op 前移，行为等价）
```

  ② `appendTruncateRecord`（javadoc：职责/全 miss 零记账语义/单写者）：

```java
/**
 * 事务性 TRUNCATE 记录入桶：TruncateView 解析 → 逐 oid resolveByOid 过滤 →
 * 命中表集（payload 序）入桶；全 miss 零记账零追加（对齐 pgoutput 服务端
 * nrelids==0 不发 'T' 帧语义）；桶不存在则新建并记 firstLsn（truncate-only
 * 事务的 pendingFloorLsn 覆盖）。低频记录不走 metaCache（oid 键与 filenode 键
 * 混入会碰撞，CatalogStores 五索引 O(1) 直查本就廉价）。线程约束：单写者。
 *
 * @param rec truncate 记录（RM_HEAP_ID opcode 0x30）
 */
private void appendTruncateRecord(WalRecord rec) {
    HeapViews.TruncateView v = HeapViews.TruncateView.parse(rec, layout);
    long origin = rec.xid() & 0xFFFFFFFFL;
    if (origin == 0) {
        return;   // 无事务归属（truncate 恒事务性，防御面同 heap 行）
    }
    long top = effectiveTop(rec, origin);
    List<TableMeta> tables = new ArrayList<>(v.relids().length);
    for (long oid : v.relids()) {
        // spike §5.2-2：pgoutput 'T' 帧剔除 relkind='p' 分区母表（母表无存储不发布，
        // WAL relids=[m,p1,p2] vs engine 渲 [p1,p2]）——wal 路同构滤除，否则对拍必 diff
        if ("p".equals(snapshot.relkindOf(oid).orElse(""))) {
            continue;
        }
        filter.resolveByOid(oid).ifPresent(tables::add);
    }
    if (tables.isEmpty()) {
        return;
    }
    EnumSet<ChangeOutputListener.TruncateOption> options = EnumSet.noneOf(ChangeOutputListener.TruncateOption.class);
    if ((v.flags() & HeapOps.XLH_TRUNCATE_CASCADE) != 0) {
        options.add(ChangeOutputListener.TruncateOption.CASCADE);
    }
    if ((v.flags() & HeapOps.XLH_TRUNCATE_RESTART_SEQS) != 0) {
        options.add(ChangeOutputListener.TruncateOption.RESTART_IDENTITY);
    }
    Bucket b = buckets.get(top);
    if (b == null) {
        b = new Bucket(top, rec.lsn());
        buckets.put(top, b);
    }
    b.entries.add(new TruncateEntry(origin, rec.lsn(), tables, options));
}
```

  ③ 桶条目泛化（`Bucket.rows` → `Bucket.entries`，全引用点连动——含 `mergeSubBuckets`/`emitBucket`/`onPrepare` 等，`rg "rows" XactGrouper.java` 逐点核对）：

```java
/** 桶内条目公共面：来源 xid（aborted 过滤键）+ 记录 lsn（发射排序键）。 */
private sealed interface BucketEntry permits RowEntry, TruncateEntry {
    /** 行/事件来源事务 id（记录头 xid——aborted 过滤的私有记账键）。 */
    long originXid();
    /** 所在记录 lsn（emitBucket 稳定排序键——DML 与 truncate 混排按 WAL 序）。 */
    long recLsn();
}

private record RowEntry(long originXid, long recLsn, ChangeOutputListener.RowChange row)
        implements BucketEntry {
}

/** 桶内 truncate 条目：表集与选项在到达期定格（as-of），发射期补位址三字段。 */
private record TruncateEntry(long originXid, long recLsn, List<TableMeta> tables,
        EnumSet<ChangeOutputListener.TruncateOption> options) implements BucketEntry {
}
```

  ④ `emitBucket` 分派（排序/过滤/计数连动）：

```java
long expected = b.entries.size();
b.entries.sort(Comparator.comparingLong(BucketEntry::recLsn));
// ...Begin 不变
long emitted = 0;
long emittedTruncates = 0;
for (BucketEntry e : b.entries) {
    if (b.abortedSubxids.contains(e.originXid())) {
        continue;
    }
    if (e instanceof RowEntry re) {
        ChangeOutputListener.RowChange r = re.row();
        out.onRow(new ChangeOutputListener.RowChange(b.xid, commitLsn, e.recLsn(),
                r.table(), r.op(), r.before(), r.after()));
        emitted++;
    } else if (e instanceof TruncateEntry te) {
        out.onTruncate(new ChangeOutputListener.TruncateEvent(b.xid, commitLsn, e.recLsn(),
                te.tables(), te.options()));
        emitted++;
        emittedTruncates++;
    }
}
out.onEnd(new ChangeOutputListener.BatchEnd(b.xid, emitted, expected));
emittedBuckets++;
emittedRows += emitted - emittedTruncates;
this.emittedTruncates += emittedTruncates;
```

  （`this.emittedTruncates` 字段 `private volatile long` + getter `emittedTruncates()`——javadoc 同 `emittedRows()` 形态；`emitBucket` 局部变量改名避免与字段互踩，落码时以编译器为准。）

- [ ] **Step 5: 跑测试确认通过 + 存量全绿**

Run: `mvn test -pl vb-stream-wal-source -Dtest=XactGrouperTest,TableFilterTest,OutputRendererTest,ChangeStreamTest,OgXactGrouperSeamTest,OgPartitionIdentityTest`
Expected: PASS（新增 7+2 用例绿；存量零回归——桶泛化是本次最大回归面）

- [ ] **Step 6: Commit**——`git commit -m "feat(wal-source): XactGrouper truncate 入桶——dispatchHeap case/resolveByOid/BucketEntry 泛化/emitBucket 分派（桶机制全家桶白拿）"` + push

---

### Task 4: 计数穿线——ChangeStream/WalSource/Main

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/changes/ChangeStream.java`（`emittedTruncates()` 透传 + `onRecord` javadoc 连动）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/WalSource.java`（`dmlEmittedTruncates()`——形态抄 `dmlEmittedRows()`@792）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/Main.java`（smoke 行 `Main.java:248-252` 增 `trunc=`）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/changes/ChangeStreamTest.java`（增用例）

**Interfaces:**
- Consumes: Task 3 的 `XactGrouper.emittedTruncates()`
- Produces: `ChangeStream.emittedTruncates()`、`WalSource.dmlEmittedTruncates()`、Main smoke 行 `dml=[buckets=N rows=M partMiss=K trunc=T]`

- [ ] **Step 1: 写失败测试**——`ChangeStreamTest` 增（沿该文件既有 RecordingListener/StubSnapshot 形态）：

```java
/** 计数穿线：truncate 发射后 emittedTruncates 递增且不并入 emittedRows。 */
@Test
void truncateCounterThreadedThroughWithoutPollutingRowCount() {
    // 沿本文件既有装配（ChangeStream + stub 快照 + RecordingListener）
    // 喂 [truncate 记录 + commit] 后断言：
    // assertEquals(1L, stream.emittedTruncates());
    // assertEquals(0L, stream.emittedRows());
    // 落码时按本文件既有喂入手造记录的辅助形态展开（WalBytes 同 XactGrouperTest）
}
```

  （`ChangeStreamTest` 已有手造喂入面——复用其形态，断言面如上两行。）

- [ ] **Step 2: 跑测试确认失败**——`mvn test -pl vb-stream-wal-source -Dtest=ChangeStreamTest` → FAIL（`emittedTruncates()` 符号不存在）

- [ ] **Step 3: 实现三件**——`ChangeStream` 透传（javadoc 抄 `emittedRows()` 形态）：

```java
/** DML 观测面：已发射 TRUNCATE 事件数（不并入 emittedRows——行口径保持纯净；
 * 透传 {@link XactGrouper#emittedTruncates()}）。
 * @return 会话累计发射 truncate 事件数（任意线程可读） */
public long emittedTruncates() {
    return grouper.emittedTruncates();
}
```

  `WalSource`（抄 `dmlEmittedRows()` 的 null 保护形态）：

```java
/** DML 观测面：已发射 TRUNCATE 事件数（DML 面未装配/尚无记录为 0）。
 * @return 会话累计发射 truncate 事件数 */
public long dmlEmittedTruncates() {
    ChangeStream cs = changeStream;
    return cs == null ? 0L : cs.emittedTruncates();
}
```

  `Main.java:249-251`：

```java
String dml = source.dmlEnabled()
        ? " dml=[buckets=" + source.dmlEmittedBuckets()
                + " rows=" + source.dmlEmittedRows()
                + " partMiss=" + source.dmlPartitionChainMisses()
                + " trunc=" + source.dmlEmittedTruncates() + "]"
        : "";
```

- [ ] **Step 4: 跑测试确认通过**——`mvn test -pl vb-stream-wal-source -Dtest=ChangeStreamTest` → PASS

- [ ] **Step 5: Commit**——`git commit -m "feat(wal-source): emittedTruncates 计数穿线——ChangeStream/WalSource/Main smoke 行 trunc="` + push

---

### Task 5: vanilla 对拍 IT——DualPathParityIT TRUNCATE 场景

**Files:**
- Modify: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/DualPathParityIT.java`（增场景 + 对拍 parse 面 TRUNCATE 行）

**Interfaces:**
- Consumes: 本类既有双路对拍骨架（engine in-process 路 + wal 直解路、按 xid 交集逐行 diff——**落码前先读本类任一既有场景**掌握装配/断言/归一化手法）；Task 0 Step 6 的 engine 渲染样本
- Produces: TRUNCATE 场景（对拍断言空 diff）+ 对拍器对 `TRUNCATE [...] options=[...]` 行的 parse/归一化（options 集内排序归一——防 `Set.copyOf` 迭代序无规格面）

- [ ] **Step 1: 起容器写场景**——`cd src/docker && docker compose up -d`；在本类增场景方法（沿既有场景的事务构造→双路捕获→diff 断言骨架），SQL 覆盖面：

```sql
-- 场景内容（publication 前置/表清理沿既有场景形态）：
CREATE TABLE t_tr_p1(id int4, v text);
CREATE TABLE t_tr_p2(id int4);
CREATE TABLE t_tr_m(i int4) PARTITION BY RANGE (i);       -- 分区母表 + 两分区
CREATE TABLE t_tr_pa PARTITION OF t_tr_m FOR VALUES FROM (0) TO (100);
CREATE TABLE t_tr_pb PARTITION OF t_tr_m FOR VALUES FROM (100) TO (200);
INSERT INTO t_tr_p1 VALUES (1,'a'), (2,'b');
INSERT INTO t_tr_m VALUES (1), (150);
-- 单事务混排：INSERT → TRUNCATE t_tr_p1 → INSERT（次序钉）
BEGIN; INSERT INTO t_tr_p1 VALUES(3,'c'); TRUNCATE t_tr_p1; INSERT INTO t_tr_p1 VALUES(4,'d'); COMMIT;
-- 多表一语句
TRUNCATE t_tr_p1, t_tr_p2;
-- 选项位
TRUNCATE t_tr_p2 RESTART IDENTITY;
-- 分区母表
TRUNCATE t_tr_m;
-- 回滚零输出
BEGIN; TRUNCATE t_tr_p1; ROLLBACK;
```

  断言：两路按 xid 交集逐行 diff 空（既有归一化：expectedChanges 语义 + 重复块容忍）；TRUNCATE 行在两路都出现且字面全等。

- [ ] **Step 2: 对拍 parse 面认 TRUNCATE 行**——本类（或其行解析辅助）对 `TRUNCATE \[...\] options=\[...\]` 增分支：表列表原样、options 集内**排序归一**后比对（**spike §5.2-1 实证必须**：engine 侧 `TruncateChange` 的 `Set.copyOf` 迭代序 = 身份哈希探查序，双位形态实测渲 `[RESTART_IDENTITY, CASCADE]` 且 JVM run 间可漂移；wal 侧 EnumSet 恒声明序 `[CASCADE, RESTART_IDENTITY]`——归一化是对拍可行的前提而非保险）。场景断言面补充：**分区母表 TRUNCATE 断言只出叶子**（`[public.t_tr_pa, public.t_tr_pb]`——母表 relkind 'p' 两路皆滤）；RESTART 场景含 owned 序列（RM_SEQ 记录前置于 heap truncate、wal 路透明——spike §1.1）；engine 侧 TRUNCATE 事务 `changes=1`（一条语句一个变更单元，与 wal 桶一个 TruncateEntry 同口径）。

- [ ] **Step 3: 跑场景确认通过**

Run: `mvn test -pl vb-stream-wal-source -Dtest=DualPathParityIT`
Expected: PASS（既有 20 用例零回归 + 新场景绿；需本机 Docker）

- [ ] **Step 4: Commit**——`git commit -m "test(wal-source): DualPathParityIT TRUNCATE 场景——混排次序/多表/选项位/分区母表/回滚，对拍 parse 面同构"` + push

---

### Task 6: OG 档"TRUNCATE 双路不可见"语义钉（spike 裁决 C 重塑）

**Files:**
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/OgDmlParityIT.java`（增 TRUNCATE 零事件场景，PG 档参数化——m2pg 库）
- Modify: `vb-stream-wal-source/README.md` 与 `vb-stream-wal-source/CLAUDE.md` 的 OG 档支持面段（"TRUNCATE 对 OG 系双路不可见"记档——与 Task 8 合并执行亦可，此处只落测试）

**Interfaces:**
- Consumes: **spike 裁决 C（OG 记录面+帧面双缺席）**——TRUNCATE 在 OG 上是纯 catalog 手术（rmid=10 零 0x30），逻辑流空 B..C 块；`RESTART IDENTITY` 被 OG SQL 层拒绝（`ERROR: PGXC does not support RESTART IDENTITY yet`）
- Produces: OG 档 TRUNCATE 语义 = **零事件、不砖流、截后 DML 字典正确**（正式记档的行为锚，非缺陷）

- [ ] **Step 1: OgDmlParityIT 增场景**——沿既有场景骨架（PG 档 m2pg 库）：

```sql
-- 场景断言三面：
-- ① TRUNCATE 事务两路零变更（wal 路无桶零发射；基准路空 B..C 块不进对拍面）——对拍 diff 空
-- ② 截后 INSERT 正常捕获且 TableMeta 正确（catalog 手术换段号由既有字典面跟踪——
--    端到端钉"TRUNCATE 后字典不倒"）
-- ③ 全程无 ISE/无砖流（TRUNCATE 语句族的 WAL 记录流经 grouper 全静默）
TRUNCATE vb_tr_og1;
INSERT INTO vb_tr_og1 VALUES (...);   -- 截后行正常双路对拍
TRUNCATE vb_tr_og1, vb_tr_og2;        -- 多表同形
```

- [ ] **Step 2: 跑 OG 对拍**

Run: `mvn test -pl vb-stream-wal-source -Dtest='OgDmlParityIT,OgParitySmokeIT'`
Expected: PASS（需本机 Docker；既有用例零回归；新场景绿=零事件语义钉死）

- [ ] **Step 3: Commit**——`git commit -m "test(wal-source): OG 档 TRUNCATE 双路不可见语义钉——零事件不砖流 + 截后 DML 字典正确（spike 裁决 C）"` + push

---

### Task 7: VastBase 手动档 IT（spike 裁决 C 重塑）+ 真库槽健康前置检查

**Files:**
- Modify: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastDmlParityIT.java`（增 TRUNCATE 零事件场景——同 Task 6 三面断言，A 档语法适配）
- Modify: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastOfflineIT.java`（既有在线/离线对拍用例的在线窗口 SQL 加一条 `TRUNCATE`——离线重放同样零事件，对拍仍全等）

**Interfaces:**
- Consumes: spike 裁决 C（VastBase 记录面实锚——rmid=10 全窗零 0x30；帧面按 OG 同内核推定，未复验记档）；`VastTestEnv` 连接纪律
- Produces: 手动档 vastbase 族 +1~2 用例；VastBase TRUNCATE 语义记档

- [ ] **Step 0: 真库槽子系统健康前置检查（必做）**——spike §5.3 sticky-slot 事故残留 4 槽（vb_wal_spike_tr2-tr5_slot，drop 报 already active）。先试 `SELECT slot_name, active FROM pg_replication_slots` + 重试 DROP；**若新建逻辑槽仍撞 already active：报告 BLOCKED 附 pg_replication_slots 快照**，等真库重启窗口清理后再执行本任务（不硬闯——避免叠加毒化）。物理槽（direct 通道）不受影响可先行。

- [ ] **Step 1: VastDmlParityIT 增场景**——Task 6 三面断言的 A 档形态（`TRUNCATE vb_wal_tr1` 零事件 + 截后 INSERT 母表/普通表身份正确）。

- [ ] **Step 2: VastOfflineIT 连动**——在线窗口加 `TRUNCATE`，离线重放对拍仍全等（truncate 零事件的离线一致性）。

- [ ] **Step 3: 手动跑**

Run: `mvn test -pl vb-stream-wal-source -Dtest='VastDmlParityIT,VastOfflineIT' -Dvb.it.vastbase=true`
Expected: PASS（需真库可达；既有用例零回归）

- [ ] **Step 4: Commit**——`git commit -m "test(wal-source): VastBase 手动档 TRUNCATE 零事件语义钉 + 离线面一致（spike 裁决 C）"` + push

---

### Task 8: 文档收口 + 全量回归

**Files:**
- Modify: `vb-stream-wal-source/CLAUDE.md`（v2 DML 面节：`ChangeOutputListener` 行、场景支持矩阵增 TRUNCATE 行、对拍契约段、测试面对账）
- Modify: `vb-stream-wal-source/README.md`（支持面/DML 输出面提 TRUNCATE；`trunc=` 观测键）
- Modify: 根 `CLAUDE.md`（wal-source 段落一句记账——沿 M4 同位置惯例）
- Modify: `docs/superpowers/specs/2026-10-22-wal-source-truncate-design.md`（状态行改"已实施"；spike 裁决回填勘误：①§2.1 relids@12（@9-11 未初始化栈垫不读）+ 无序列尾随（"followed by sequence relids"系 pre-PG10 残留注释）②§0"全档"裁定被服务端事实反转——OG/VastBase 裁决 C 双路不可见、TRUNCATE 收缩为 vanilla 17/18 独有能力、记档而非实现 ③写点勘误：tablecmds.c ExecuteTruncateGuts 非 heapam.c；仅 wal_level≥logical 时写）

- [ ] **Step 1: 全量回归**

Run: `mvn test -pl vb-stream-wal-source`
Expected: 离线 + Testcontainers IT 全绿（Windows 环境性存量 7 例 skip/已知记档面不动）；用例数对账（离线 +N / IT +M）写进 CLAUDE.md 测试面

- [ ] **Step 2: 文档四处收口**——按上表落笔（测试数按实际对账；spike 分支结论回填 spec）

- [ ] **Step 3: Commit + push**——`git commit -m "docs(wal-source): TRUNCATE 支持收口——模块 CLAUDE.md/README/根 CLAUDE.md 记账 + spec 状态回填"` + push

---

## 自审记档（writing-plans Self-Review）

1. **Spec 覆盖**：§2.1→Task 1（+Task 6 OG 分支）；§2.2→Task 3；§2.3→Task 2；§2.4→Task 2 Step 4；§2.5→Task 3④/Task 4；§3.1→Task 3（resolveByOid/全 miss）；§3.2 桶白拿→Task 3 用例群（回滚/子回滚）+ Task 5/6/7 IT；§3.3 fail-safe→Task 1（越界 ISE/未知位忽略）+ Task 3（全 miss）；§3.4→Task 0 Step 5 + Task 6 分支 C；§4→Task 0 六步；§5→Task 1/2/3/4/5/6/7 测试面。无缺口。
2. **占位符扫描**：Task 4 Step 1 的用例体是"沿既有形态展开"的半骨架（断言面完整、装配面引用本文件既有辅助）——该文件已有同构装配，非占位；Task 5/6/7 的场景骨架引用既有 IT 类方法（repo 存在，执行者读文件后落码）。其余步骤均含全量代码/命令/期望。
3. **类型一致性**：`TruncateView(int nrelids, long[] relids, int flags)` 全文一致；`TruncateEvent` 五参/两参构造一致；`BucketEntry.originXid()/recLsn()` 与 emitBucket 消费一致；`resolveByOid(long)` 签名一致；计数链 `emittedTruncates()`（grouper→ChangeStream→WalSource `dmlEmittedTruncates()`）一致。
