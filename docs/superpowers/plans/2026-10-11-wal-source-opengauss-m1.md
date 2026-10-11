# openGauss 5.0.0 适配 M1 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `vb-stream-wal-source` 在 openGauss 5.0.0（ASTORE）上完成 WAL 收流双通道（sidecar 文件源 + Java 直连）与 catalog 同步，JDBC 对拍全绿，PG 17/18 零回归。

**Architecture:** openGauss 作为 `WalLayout` 版本档进同模块；新增 `WalByteSource` 推模式抽象承载双通道，`WalStreamWalker`/`CatalogSynchronizer`/`StateStore` 复用零改或经 default 方法扩展。直连客户端为裸 v3 协议实现（`replication=true` 形态——pgjdbc 系驱动的 `replication=database` 形态在 openGauss 上被硬路由进逻辑发送循环导致实例自杀，见 spec §1）。

**Tech Stack:** Java 17 + Maven；新增依赖 `org.opengauss:opengauss-jdbc`（runtime，SQL 面 sha256）；Testcontainers 镜像 `opengauss/opengauss:5.0.0`（multi-arch）。

**Spec:** `docs/superpowers/specs/2026-10-11-wal-source-opengauss-m1-design.md`（本计划从 spec 立论，执行者须同读）

## Global Constraints

- 方法名一律英文 camelCase；**禁止**中文方法名与全限定类名内联（CLAUDE.md 规约）。
- 每个函数（含私有/测试辅助）必须有 javadoc 逻辑描述（职责/关键步骤/边界/线程约束）。
- 日志一律 slf4j `{}` 占位符；禁止 System.out/err。
- layout 常量逐个标注 openGauss-server v5.0.0 源码出处（文件+结构体/宏名），沿 REL_STABLE 惯例。
- **PG 17/18 现有 253 用例零改动零回归**：`WalLayout` 接口扩展只允许 `default` 方法；`WalStreamReceiver`（pgjdbc 路）不动。
- 范围 fail-fast：vanilla 9.2 / OG 非 5.0.x 主版本 / UStore / 列存（relcudescrelid≠0）/ 段页式 / bucket 表 / 块引用扩展位（id&0x80、id&0x40）一律 ISE 或启动期拒绝。
- 实测锚事实（调研钉死，代码注释引用）：页 magic `0xD074`；短页头 24B（magic@0/info@2/tli@4/pageaddr@8/rem_len@16/**total_len@20**）；长页头 40B；记录头 32B（tot_len@0/term@4 恒 0x80000001 单机/xid@8/pad@12/prev@16/info@24/rmid@25/bucket_id@26/crc@28）；heap rmid=10 与 vanilla 同；`xl_heap_update`（htup.h）= 9.3+ 现代格式，`XLH_UPDATE_PREFIX_FROM_OLD=1<<5`、`SUFFIX=1<<6`、`CONTAINS_OLD_TUPLE=1<<2` 与 vanilla 同值；RelFileNode 编码 16B（spc4/db4/rel4/bucketNode int2/opt uint2）。
- 每任务完成即 `git commit + push`（commit message 中文、带 Co-Authored-By: Claude 尾注）。
- 验证编译用 `mvn -pl vb-stream-wal-source clean test-compile`（防增量假绿）。

---

### Task 1: `WalLayoutVOpenGauss` 描述符 + 版本判别器

**Files:**
- Create: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayoutVOpenGauss.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayout.java`（追加 default 方法）
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayouts.java`（新增 `forServer(int, String)`）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalLayoutVOpenGaussTest.java`
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalLayoutsOgDiscriminatorTest.java`

**Interfaces:**
- Consumes: 现有 `WalLayout` 接口（全部既有方法）。
- Produces: `WalLayoutVOpenGauss.INSTANCE`（单例）；`WalLayouts.forServer(int pgVersionNum, String versionText)`——后续所有任务的 layout 来源；`WalLayout` 新 default 方法：`int recordXidOffset()`、`int recordInfoOffset()`、`int recordRmidOffset()`、`int relNodeEncodedSize()`、`boolean rejectBlockIdExtensionBits()`。

- [ ] **Step 1: 推导 kinds 词典（一次性，产出抄进代码）**

起 OG 容器跑（复用调研容器或新起，见 Task 9 的 `OgTestEnv` 亦可提前建）：

```bash
docker run -d --name og-plan -e GS_PASSWORD=openGauss@123 -p 15700:5432 opengauss/opengauss:5.0.0
# 等就绪后（~60s）：
docker exec -u opengauss og-plan bash -c 'export LD_LIBRARY_PATH=/usr/local/opengauss/lib; \
  /usr/local/opengauss/bin/gsql -d postgres -t -A -c "SELECT a.attnum,a.attname,t.typname \
  FROM pg_attribute a JOIN pg_type t ON a.atttypid=t.oid \
  WHERE a.attrelid=$$pg_class$$::regclass AND a.attnum>0 ORDER BY a.attnum"'
# 同款查询跑 $$pg_attribute$$::regclass
```

预期：pg_class **41 行**、pg_attribute **24 行**（列清单见调研报告 §2.1）。kind 映射规则
照抄 `WalLayoutV18` 的推导口径（`name/oid/int4/int2/float4/bool/char/xid`，varlena 列记
`skip`）；记下 attisdropped、reltoastrelid、relkind 的 attnum（推导锚：reltoastrelid
attnum=12、attisdropped attnum=15、relkind attnum=21）。

- [ ] **Step 2: 写失败测试（描述符常量 + 自检锚）**

```java
// WalLayoutVOpenGaussTest.java 核心断言（javadoc 完整、方法英文命名，下同）
@Test
void pageAndRecordHeaderConstantsMatchEmpiricalAnchors() {
    WalLayout og = WalLayoutVOpenGauss.INSTANCE;
    assertEquals(0xD074, og.pageMagic(), "xlog_basic.h XLOG_PAGE_MAGIC, 段首字节实测 74 d0");
    assertEquals(24, og.shortPageHeaderSize(), "MAXALIGN(sizeof XLogPageHeaderData)=24, rem_len@16+total_len@20");
    assertEquals(40, og.longPageHeaderSize());
    assertEquals(32, og.recordHeaderSize(), "实测 prev@16 8 对齐垫 4B, crc@28");
    assertEquals(8, og.recordXidOffset());
    assertEquals(24, og.recordInfoOffset());
    assertEquals(25, og.recordRmidOffset());
    assertEquals(16, og.relNodeEncodedSize(), "relfilenode.h: spc/db/rel + bucketNode int2 + opt uint2");
    assertTrue(og.rejectBlockIdExtensionBits());
    assertTrue(og.supports(90204));
    assertEquals(5, og.majorVersion(), "OG 以 PG 大版本口径报 5——由判别器保证仅 OG 5.0.x 到达");
}

@Test
void classReadRegionDriftFromV17IsExactlyFiveExtraOidColumns() {
    // 自检锚（沿"两版 toast 偏移差恰 4B"惯例）：OG 较 V17 在 reltoastrelid 后多
    // reltoastidxid/reldeltarelid/reldeltaidx/relcudescrelid/relcudescidx 五个 oid
    assertEquals(108, WalLayoutV17.INSTANCE.pgClassReltoastrelidDataOffset());
    assertEquals(108, WalLayoutVOpenGauss.INSTANCE.pgClassReltoastrelidDataOffset(),
        "OG reltoastrelid 数据区偏移与 V17 同值（V18 因 relallfrozen 是 112）");
    assertEquals(20, WalLayoutVOpenGauss.INSTANCE.pgClassRelkindDataOffset()
        - WalLayoutV17.INSTANCE.pgClassRelkindDataOffset(),
        "relkind 读区差 = 五个 4B 列（列存四件 + reltoastidxid）");
}
```

kinds 数量断言：`assertEquals(41, og.pgClassKinds().length); assertEquals(24, og.pgAttributeKinds().length);`

- [ ] **Step 3: 跑测试确认失败**

Run: `mvn -pl vb-stream-wal-source clean test -Dtest=WalLayoutVOpenGaussTest`
Expected: 编译失败（类不存在）。

- [ ] **Step 4: 实现 `WalLayoutVOpenGauss` + 接口 default + 判别器**

接口追加（**全部 default**，V17/V18 零改动）：

```java
/** WAL 记录头 xl_xid 偏移（V17/18=4；OG=8——xlog_basic.h XLogRecord，实测）。 */
default int recordXidOffset() { return 4; }
/** 记录头 xl_info 偏移（V17/18 沿现实现取值抄入；OG=24）。 */
default int recordInfoOffset() { return 8; }
/** 记录头 xl_rmid 偏移（V17/18 沿现实现抄入；OG=25）。 */
default int recordRmidOffset() { return 9; }
/** 块引用中 RelFileNode 编码宽度（V17/18=12B 三 oid；OG=16B 含 bucketNode/opt）。 */
default int relNodeEncodedSize() { return 12; }
/** 块引用 id 高位扩展（bucket/segpage=0x80、TDE=0x40）是否 fail-fast（仅 OG true）。 */
default boolean rejectBlockIdExtensionBits() { return false; }
```

（先读 `WalRecordParser.java` 头字段与块引用读取处的现实现，把 V17/V18 实际偏移抄进
default 值——**不要凭记忆写**。）

`WalLayouts` 追加：

```java
/**
 * 带版本文本的分发（openGauss 判别）：server_version_num=90204 且 version() 含
 * "openGauss 5.0." 返回 OG 档；90204 无 openGauss 标记＝vanilla 9.2 非目标 ISE；
 * openGauss 其他主版本 ISE（附 version 文本）。判别依据：openGauss 全系版本报
 * 90204（与 vanilla 9.2 撞号），唯一区分面是 version() 文本（实测 5.0.0 形如
 * "(openGauss 5.0.0 build ...) compiled at ..."）。
 */
public static WalLayout forServer(int pgVersionNum, String versionText) {
    if (pgVersionNum >= 180000 && pgVersionNum < 190000) { return WalLayoutV18.INSTANCE; }
    if (pgVersionNum >= 170000 && pgVersionNum < 180000) { return WalLayoutV17.INSTANCE; }
    if (pgVersionNum / 10000 == 9) {
        if (versionText != null && versionText.contains("openGauss")) {
            if (versionText.contains("openGauss 5.0.")) {
                return WalLayoutVOpenGauss.INSTANCE;
            }
            throw new IllegalStateException("openGauss 版本不在支持面（当前仅 5.0.x）: " + versionText);
        }
        throw new IllegalStateException("vanilla PG 9.2 非目标版本（无复制槽/无逻辑日志，"
            + "见 docs/wal-source-opengauss-adaptation-research.md）: " + versionText);
    }
    throw new IllegalStateException("unsupported server version " + pgVersionNum + ": " + versionText);
}
```

判别器测试：90204+"(openGauss 5.0.0 build…)"→OG；90204+"PostgreSQL 9.2.4"→ISE 含
"非目标"；90204+"(openGauss 6.0.0"→ISE 含版本；180002→V18 不受影响；旧
`forServerVersion(int)` 行为零变化。

- [ ] **Step 5: 全量测试 + 提交**

Run: `mvn -pl vb-stream-wal-source clean test -Dtest='WalLayout*'`
Expected: 新测试全绿 + 旧 WalLayoutV17/V18Test 不变绿。

```bash
git add -A vb-stream-wal-source && git commit -m "feat(wal-source): WalLayoutVOpenGauss 描述符与 openGauss 5.0.0 判别器"
```

---

### Task 2: `WalRecordParser` OG 块引用分派 + 扩展位 fail-fast + 真实语料测试

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalRecordParser.java`
- Create: `vb-stream-wal-source/src/test/resources/ogcorpus/wal-000000010000000000000002.bin`（真实语料）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/OgWalRecordParserTest.java`

**Interfaces:**
- Consumes: Task 1 的 `recordXidOffset()/recordInfoOffset()/recordRmidOffset()/relNodeEncodedSize()/rejectBlockIdExtensionBits()`。
- Produces: `WalRecordParser.parse` 对 OG 记录正确产出（`WalRecord.rmid()==10`、xid、块链 `BlockRef`——`relNode()` 含 OG 16B 编码的 relNode 位段）。

- [ ] **Step 1: 采集真实语料**

调研容器（或 Task 1 的 og-plan）：

```bash
docker exec -u opengauss og-plan bash -c 'export LD_LIBRARY_PATH=/usr/local/opengauss/lib; \
  gsql -d postgres -t -A -c "CREATE TABLE IF NOT EXISTS t_corpus(id int primary key, v text); \
  INSERT INTO t_corpus VALUES (1,repeat(chr(98),100)); UPDATE t_corpus SET v=repeat(chr(99),100) WHERE id=1;"'
docker exec og-plan bash -c 'cat > /tmp/dump.sql <<EOF
SELECT encode(pg_read_binary_file((SELECT pg_xlogfile_name(pg_current_xlog_insert_location()))),'"'"'base64'"'"');
EOF'
# base64 段名与内容取回后解码入 test/resources/ogcorpus/（另存段名与锚 LSN 到同目录 anchor.txt：
# 记录调研报告实测锚记录——INSERT(info=0x80 INIT_PAGE)/INSERT(0x00)/HOT_UPDATE(0x40, 含新 cccc×100
# 与旧 bbbb×100 相距 56B) 的期望 LSN）
```

（实施注记：若段 >1MB 可只 dump 记录所在页窗口 `substring(... from N for 8192*k)` 起始
页对齐；语料二进制不进 git 的话以 base64 文本存 `.b64` 并在测试内解码。）

- [ ] **Step 2: 写失败测试**

```java
@Test
void parsesRealOgCorpusAnchoredRecords() throws Exception {
    byte[] seg = loadCorpus();                       // 语料加载辅助（javadoc 注明来源与锚文件）
    WalLayout og = WalLayoutVOpenGauss.INSTANCE;
    List<WalRecord> recs = walkSegmentRecords(seg, og);   // 测试内迷你 walker：页头跳过+记录链走读
    // 断言锚（值来自 anchor.txt，实施时以实测 LSN 替换占位常量——语料采集时即写入）：
    // ① 存在 rmid=10、info&0x70==0x00 的 INSERT，且记录字节含 "cccc"100 连
    // ② 存在 rmid=10、info&0x70==0x40 的 HOT_UPDATE，同一记录内含新旧两段 100 连
    // ③ 全部记录 tot_len 恰好链式衔接（走读收敛在段末/最后页尾）
}

@Test
void blockRefExtensionBitsFailFast() {
    // 手造记录：块引用 id 字节置 0x80（HAS_BUCKET_OR_SEGPAGE）→ ISE 消息含 LSN 与 0x80；
    // 置 0x40（TDE）同理
}
```

- [ ] **Step 3: 跑测试确认失败**（`mvn -pl vb-stream-wal-source clean test -Dtest=OgWalRecordParserTest`）

- [ ] **Step 4: 实现 parser 适配**

改 `WalRecordParser`：头字段（xid/info/rmid）偏移从 layout default 方法取；块引用
RelFileNode 读宽从 `relNodeEncodedSize()` 取（OG 16B：spc/db/rel u32×3 + bucket u16 +
opt u16——bucket/opt 读入但不参与匹配，仅 `fork==0 && relNode 匹配` 判据不变）；
记录头之后进入块引用链前，OG 档先校验 `id & 0xC0 == 0`（`rejectBlockIdExtensionBits()`
为 true 时违例 ISE——消息含记录 LSN 与违例位）。**V17/V18 路径字节行为零变化**（default
值即现值——Step 4 前先核对现实现抄准）。

- [ ] **Step 5: 全量 + 提交**（含 V17/V18 parser 旧测试回归确认）

---

### Task 3: OG PRUNE/CLEAN 格式转录与 `PruneView` 适配

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/HeapViews.java`（PruneView OG 解析分支）
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/OgPruneViewTest.java`

**Interfaces:**
- Consumes: Task 1 layout；OG 源码事实（本地克隆 `/tmp/og-src` 或 gitee v5.0.0）。
- Produces: `PruneView.parse(record, OG layout)` 产出 redirected/nowdead/nowunused 段（字段集以转录结果为准——OG 的 XLOG_HEAP2 opcode 名与段布局**尚未实测**，本任务即核对任务）。

- [ ] **Step 1: 转录源格式**

```bash
cd /tmp/og-src && grep -n "XLOG_HEAP2_" src/include/access/htup.h | head; \
  sed -n '/typedef struct xl_heap_prune\|typedef struct xl_heap_clean/,+20p' src/include/access/htup.h
# 若本地克隆已清：git clone --depth 1 --branch v5.0.0 --filter=blob:none --sparse \
#   https://gitee.com/opengauss/openGauss-server.git（sparse: src/include/access/htup.h）
```

把 opcode 值与结构体字段（预期：OG 沿用 9.5+ 的 xl_heap_clean/xl_heap_prune 家族，
`nredirected/ndead/nunused` 段布局——**以转录结果为准写 javadoc 出处**）。

- [ ] **Step 2: 失败测试**——手造字节按转录格式构造 PRUNE 记录，断言 `redirectedPairs()/nowdead()/nowunused()` 与 `CatalogReplay.applyPrune` 施加后的字典行迁移（复用 `CatalogReplayTest` 的施加断言风格）。
- [ ] **Step 3: 确认失败 → 实现 PruneView OG 分支（经 layout 或 opcode 集判定）→ 过 → 提交**

---

### Task 4: `WalByteSource` 接口 + `FileWalSource`/`ReceivexlogProcess`

**Files:**
- Create: `receive/WalByteSource.java`、`receive/FileWalSource.java`、`receive/ReceivexlogProcess.java`
- Test: `receive/FileWalSourceTest.java`（离线：临时目录手写段文件模拟增长/rename/续读）

**Interfaces:**
- Produces:

```java
/**
 * openGauss 档 WAL 字节源（推模式）：start 后在源线程回调字节块（携带"本块末字节之后的
 * 绝对 LSN"锚——与 WalStreamWalker.feed(chunkEndLsn, bytes) 对齐），stop 幂等。
 * 两实现：FileWalSource（sidecar 目录 tail）/ DirectWalSource（Task 5）。
 */
public interface WalByteSource extends AutoCloseable {
    /** 起流：chunk 回调在源线程同步执行；锚语义同 walker.feed。 */
    void start(LongConsumer chunkEndLsnSink, BiConsumer<Long, byte[]> chunkSink);
    /** 幂等停机（managed 模式含子进程终止）。 */
    void stop();
    /** 消费前沿（已交付字节末尾的 LSN；未 start 为 0）。 */
    long consumedLsn();
}
```

- `ReceivexlogProcess`：`ProcessBuilder` 拉起 `pg_receivexlog -D dir [-h host -p port -U user] [--slot name]`（密码经 `PGPASSWORD` 环境变量传入，**不经命令行**）；子进程退出监控→指数退避重拉（1s→16s，5 次终态失败——复用接收器退避形态）；stop 时 SIGTERM + 3s 等待 + SIGKILL。
- `FileWalSource`：tail 目录——按段文件名（`%08X%08X%08X`）发现与排序；`.partial` 增长读（记录上次读偏移，File.length 轮询 50ms；读到稳定长度即交付，尾部不足一页留待下轮——walker awaitMore 语义天然消化）；`.partial`→去后缀 rename 检测（读到段尾后切下一段）；锚 = `段号×16MB + 文件内偏移`（首个页头 pageaddr 随后接管纠偏——与调研"自维护数据锚"同构）；断点续传起点 = consumedLsn 所在段。

**Steps（TDD 循环）：**
- [ ] 失败测试 ①：临时目录预置 16MB 段文件（手造：合法页头+记录链——复用 `WalRecordParserTest` 的 MsgBuilder 风格造 2 页），start 后断言收到全部记录、consumedLsn 前进到段尾。
- [ ] 确认失败 → 实现 `FileWalSource`（external 模式，无子进程）→ 过。
- [ ] 失败测试 ②：`.partial` 增长场景（测试线程分两次写文件）+ rename 切段场景，断言零丢零重（记录序列连续）。
- [ ] 实现增长/rename 检测 → 过。
- [ ] 失败测试 ③：`ReceivexlogProcess` 用假二进制（`/bin/sh -c 'sleep 3600'` 包装）验证拉起/环境变量/终止链（**不测真 pg_receivexlog**——真程序归 Task 9 IT）。
- [ ] 实现子进程管理 → 全量（离线）→ 提交。

---

### Task 5: `DirectWalSource` 裸 v3 协议客户端

**Files:**
- Create: `receive/direct/V3ProtocolChannel.java`（socket + startup + 认证 + 'Q' 命令 + CopyBoth 帧读写）
- Create: `receive/direct/OgAuth.java`（md5 + OG sha256 挑战，自 opengauss-jdbc `ConnectionFactoryImpl`/`MD5` 逻辑移植——反编译参考 `/tmp/og-jdbc.jar` 或 gitee openGauss JDBC 仓源码）
- Create: `receive/DirectWalSource.java`（实现 Task 4 的 `WalByteSource`：握手/流循环/反馈心跳/重连）
- Test: `receive/direct/V3ProtocolChannelTest.java`（对本地伪服务器 `ServerSocket` 的手写 v3 应答做单测）、`receive/DirectWalSourceFeedbackTest.java`（65B 'r' 编码断言）

**Interfaces:**
- Produces: `DirectWalSource`（`WalByteSource` 实现；构造参数 host/port/user/pass/slotName/statusIntervalMs）。

**关键实现事实（全部调研/spec 实锚，实现注释引用）：**
- startup 参数：`replication=true`、无 dbname（streamutil.cpp:175-176 形态）；'Q' 简单查询协议发命令。
- 握手：`'Q' IDENTIFY_SYSTEM` → DataRow 解析 systemid/tli/xlogpos → 起点 = `xlogpos 段对齐下取整` → `'Q' START_REPLICATION [SLOT "x"] %X/%X` → 期待 CopyBothResponse('W')。
- 反馈消息（65B，walprotocol.h StandbyReplyMessage，C 对齐垫按实测）：

```java
// 'r' + receive/write/flush/apply/applyRead 各 u64 + peer_role i32(=2 STANDBY_MODE)
//     + peer_state i32(=4 BUILDING_STATE) + sendTime i64(μs 自 2000-01-01)
//     + replyRequested u8 + pad3 + replyFlags u32(=0)
// 写入用 ByteBuffer LITTLE_ENDIAN.allocate(65)——字段次序与宽度以
// DirectWalSourceFeedbackTest 逐字节断言钉死（黄金样本来自 receivelog.cpp sendReplyToSender）
```

- 心跳：独立守护线程每 `statusIntervalMs/2` 发 replyRequested=true。
- 'w' 帧：跳过 `WalDataMessageHeader`（OG 扩展布局——`dataStart` u64/walEnd u64/…以 xlog_basic.h `WalDataMessageHeader` 转录，Step 内先 grep 本地源）；载荷字节交付 sink，锚 = walEnd。'k' 帧仅推进观测。
- 断线：指数退避重连（起点 = consumedLsn 段对齐下取整），5 次失败置终态失败（`WalByteSource` 增加 `Optional<Throwable> terminalFailure()` default 方法）。

**Steps：**
- [ ] 失败测试 ①（V3ProtocolChannel 对伪服务器）：startup→AuthenticationMD5 流程（伪服务器回 md5 挑战）→ 'Q' 命令 DataRow 解析 → CopyBoth 'W' 进入。实现至过。
- [ ] 失败测试 ②：OgAuth sha256 挑战（伪服务器回 AuthenticationSASL/OG 私有码流——以 opengauss-jdbc 移植的算法对拍其已知向量；如无公开向量则以"伪服务器用相同算法校验客户端响应"闭环）。实现至过。
- [ ] 失败测试 ③：65B 反馈逐字节断言（常量黄金样本硬编码于测试）。实现至过。
- [ ] 失败测试 ④：流循环——伪服务器推 'w'/'k' 帧，断言 sink 收到载荷与锚、心跳线程按间隔发 'r'（伪服务器侧记录收包时间）。实现 `DirectWalSource` 至过。
- [ ] 失败测试 ⑤：断线重连（伪服务器主动 close 一次），断言重连起点段对齐与字节零丢。实现至过 → 全量离线 → 提交。

---

### Task 6: OG 槽生命周期 + 配置面/依赖接线

**Files:**
- Modify: `receive/PhysicalSlotManager.java`（OG 变体建槽）、`api/WalSource.java`（OG 档装配 + 新配置键）、`state/StateStore.java`（pgVersion 面 OG 档）、`pom.xml`（`org.opengauss:opengauss-jdbc` 5.0.0-og，runtime）
- Test: `api/OgWalSourceConfigTest.java`（离线）、IT 归 Task 9

**Interfaces:**
- Produces: 配置键 `vb.wal.og.source=file|direct`（默认 file）、`vb.wal.og.rx.mode=managed|external`（默认 managed）、`vb.wal.og.rx.dir`（默认 `wal-og-queue`）、`vb.wal.og.rx.bin`（默认 `pg_receivexlog`）；`WalSource.start()` 内：判别器 `forServer(versionNum, version())` → OG 档装配 SQL 会话（`jdbc:opengauss:` 前缀 URL 由 DriverManager 分流）+ `WalByteSource` 按 source 键注入 sink 组合（walker.feed → apply → maybeCheckpoint——与现 `WalStreamReceiver` 路相同的 sink 组合次序）。

**Steps：**
- [ ] 失败测试：配置解析（缺省值/非法值 IAE：`vb.wal.og.source` 非 file|direct）；OG 档与 PG 档互斥装配（17/18 走老路断言零新分支触碰）。实现至过。
- [ ] 失败测试：`PhysicalSlotManager.ensureSlotOg`（伪 SQL 应答——建槽语句形态断言：`SELECT * FROM pg_create_physical_replication_slot(?, false)`，第二参为 **dummy_standby 非 immediately_reserve**；restart_lsn NULL 回落 flush 语义沿现实现）。实现至过。
- [ ] pom 依赖 + `StateStore` OG 档标识（formatVersion 兼容扩展，沿 Task 15 的 v3 先例）。离线全量 → 提交。

---

### Task 7: catalog 引导/重放 OG 适配

**Files:**
- Modify: `replay/CatalogBootstrap.java`（OG 列单 SQL + `pg_current_xlog_location()`）、`replay/CatalogReplay.java`（偏移经 layout 已天然生效；UStore/列存 fail-fast 扫描）
- Test: `replay/OgCatalogBootstrapSqlTest.java`（离线：SQL 文本/列单断言）、fail-fast 单测

**Steps：**
- [ ] 失败测试 ①：OG 档 bootstrap SQL 含 41 列投影（显式列名——含 `relcudescrelid/reldeltarelid/parttype/relcmprs` 等 OG 列）与 `pg_current_xlog_location`（非 wal 命名、无 flush 档）；实现（layout/档位门控分支）至过。
- [ ] 失败测试 ②：UStore/列存 fail-fast——`relcudescrelid != 0` 或 parttype 异常的 class 行到达时 `CatalogReplay` 抛 ISE（消息含 relname/oid）；实现扫描钩子至过。
- [ ] 全量离线（17/18 回归重点看 CatalogReplayTest/SynchronizerTest）→ 提交。

---

### Task 8: 黄金 wire 对拍 fixture + 双通道对拍 IT

**Files:**
- Create: `src/test/resources/golden/receivexlog-wire.txt`（strace 抓包解析产物：连接后客户端→服务端的命令与反馈字节序列）
- Create: `it/OgTestEnv.java`（Testcontainers `opengauss/opengauss:5.0.0` 单例：GS_PASSWORD、gaussdb 用户 + REPLICATION 权限、建表辅助；wait 策略 = JDBC 重试，**镜像默认 sha256 无需 md5 化**——SQL 走 opengauss-jdbc、sidecar 与 DirectWalSource 均原生支持）
- Test: `it/OgGoldenWireIT.java`、`it/OgDualChannelParityIT.java`

**Steps：**
- [ ] 采集黄金样本（调研方法复用）：`strace -f -e trace=sendto,write,read -s 200 -x pg_receivexlog ...` 抓会话前 30s，提取客户端发出的字节（startup 参数、IDENTIFY_SYSTEM、START_REPLICATION、'r' 帧），整理为行文本 fixture（每行：方向/字节 hex/注释）。
- [ ] 失败测试：`OgGoldenWireIT`——DirectWalSource 连真容器，伪断言其发出的命令序列与 fixture 等价（'r' 因 sendTime 时变，比对除 sendTime 外字段）；实例存活。实现/修至过。
- [ ] 失败测试：`OgDualChannelParityIT`——同容器同写入序列（INSERT/UPDATE/DDL 各若干），File（managed，容器内无 pg_receivexlog 二进制时降级 external——**注意**：镜像含 `/usr/local/opengauss/bin/pg_receivexlog`，managed 需挂宿主二进制不可行，IT 一律用 external 形态：测试内自行 exec 容器内 pg_receivexlog 落到挂载卷）与 Direct 两路各自收流至同一边界 LSN，断言两路 walker 交付的记录序列（LSN+totLen+rmid+info）全等。实现至过 → 提交。

---

### Task 9: catalog 末态对拍 IT + 生命周期 IT + 实例存活红线 IT

**Files:**
- Test: `it/OgCatalogSyncIT.java`（复用 `WalSyncItBase` 对拍器形态：DDL 场景后 ctid 字典 vs JDBC REPEATABLE READ 实查逐行全等）、`it/OgLifecycleIT.java`、`it/OgInstanceSurvivalIT.java`

**Steps：**
- [ ] 失败测试 ①（catalog 对拍）：场景 = 建表/加列/RENAME/TRUNCATE/ANALYZE/随机 CHECKPOINT 序列（沿 `WalAdversarialIT` 配方裁剪），对拍断言逐行全等（attr/class/nsp 三表）。实现修至过——**这里预期会暴露 OG 偏移/格式的残余错位，按排障流程修 layout/PruneView，不许放宽断言**。
- [ ] 失败测试 ②（生命周期）：检查点续传（stop→写 DDL→start 续传对拍）；损坏回落（改坏 state 文件字节→重启全新引导）。实现修至过。
- [ ] 失败测试 ③（**实例存活红线**）：Direct 档循环 5 轮"连→收 3s→杀连接"，每轮后断言实例仍接受 SQL（`SELECT 1`）且日志无 `Gaussdb exit`。实现修至过——**此测试不过则 M1 不算完成**。
- [ ] 全仓库 `mvn -pl vb-stream-wal-source clean test`（17/18+OG 全绿）→ 提交。

---

### Task 10: 文档同步与收口

**Files:**
- Modify: `vb-stream-wal-source/CLAUDE.md`、`vb-stream-wal-source/README.md`、根 `CLAUDE.md`（模块行/依赖面/配置键/测试计数）、`docs/wal-source-opengauss-adaptation-research.md`（M1 落地状态回填）

**Steps：**
- [ ] 文档同步（新组件/配置键表/已知限制——replconninfo HA 形态不在支持面、PRUNE 实测口径、双通道对拍说明）；测试计数更新（含 OG IT 数）。
- [ ] 收口验证：`mvn -pl vb-stream-wal-source clean test` 全绿 + 根 README 路线提及 OG M1；commit + push。

---

## Self-Review 记录

- **Spec 覆盖**：spec §2 组件表逐行对应 Task 1-7；§5 验收五条对应 Task 8/9/10；§6 T1-T10 与本计划 Task 1-10 对齐（T5 拆为 Task 5 的五步）。无遗漏。
- **占位符**：语料锚 LSN 与黄金 'r' 样本标注"采集时写入"——属一次性实测产物（计划给出采集命令与断言形态），非 TBD；其余步骤均含代码/命令。
- **类型一致性**：`WalByteSource.start(LongConsumer, BiConsumer<Long,byte[]>)` 于 Task 4 定义、Task 5/8 消费；`WalLayouts.forServer(int, String)` 于 Task 1 定义、Task 6 消费；layout default 方法名在 Task 1/2 一致。
