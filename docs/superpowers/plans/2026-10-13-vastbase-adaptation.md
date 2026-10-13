# VastBase G100 适配实施计划(第四布局档)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 vb-stream-wal-source 支持 VastBase G100 V3.0(OG 6.0.0 内核)的 catalog 同步 + DML 面——新增第四布局档 `WalLayoutVVastbase` 吸收 NAMEDATALEN 128 与 catalog 列集差异。

**Architecture:** `WalLayoutVVastbase extends WalLayoutVOpenGauss` 只覆写差异面(词典/读区偏移/护栏槽位/name 宽度/majorVersion),记录层常量全部继承(132019 条记录零 resync 实证兼容);判别经 `WalSource.start()` 的 relname attlen 探测第二面;词典版本分派沿 `NspRow.kinds(WalLayout)` 既有模式(不上移接口);验收 = 离线单测 + `-Dvb.it.vastbase=true` 手动档 IT 连真库(172.16.107.133:55433/test_a)+ T0 spike 定对拍基座。

**Tech Stack:** Java 17 + Maven + JUnit 6;运行依赖 pgjdbc + opengauss-jdbc(`jdbc:opengauss:` 前缀连真库)+ lz4-java。

**Spec:** `docs/superpowers/specs/2026-10-13-wal-source-vastbase-adaptation-design.md`(实测锚定数据在 spec 附录 A——词典数组的黄金参照)

## Global Constraints

- 方法名一律英文 camelCase;类型引用 import 后用简名,禁止 FQN 内联(唯一例外:同文件真实命名冲突)
- 日志走 slf4j(`private static final Logger LOG = LoggerFactory.getLogger(Xxx.class)`),禁止 System.out/System.err(主代码与测试同规;临时探针类放 `target/probe/` 不入库)
- 每个函数(含私有/测试辅助)必须有 javadoc 逻辑描述——职责/关键步骤/边界异常/线程约束;词典数组逐行注释锚定列名与实测出处
- 单测命令形态:`mvn test -pl vb-stream-wal-source -Dtest=XxxTest`(多模块下 -Dtest 须带 -pl)
- 手动档 IT:`@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")`,常规 `mvn test` 跳过(参照 `Pg17DefaultWorkMemTest` 模式)
- 真库卫生:测试表/槽一律 `vb_wal_` 前缀,用例 finally 清理(DROP TABLE IF EXISTS + pg_drop_replication_slot);**不触碰既有业务表(orders/snapshot/gs_* 等)与既有逻辑槽(sync_orders_slot/vb_e2e_*)**
- 真库连接参数:`172.16.107.133:55433/test_a`,用户 `test`/`Tester123`(经系统属性 `vb.it.vastbase.host/port/db/user/pass` 可覆盖,缺省即此);JDBC 前缀 `jdbc:opengauss:`
- 每任务完成即 `git commit` + `git push`(跨机开发纪律)
- Windows 环境:Java classpath 分隔符 `;`;bash 下路径用 `/c/Users/浦晟/...` 形态

---

### Task 0: T0 spike——逻辑流基准路探测(真库,非 TDD)

**Files:**
- Create: `docs/wal-source-vastbase-adaptation-spike.md`(探测结论文档)
- Create(临时,不入库): `target/probe/VastLogicalSpike.java`

**Interfaces:**
- Produces: spike 结论(四项:认证/槽与 publication/帧形态/分区身份)——Task 6 的对拍基座形态依据;结论回填 spec §6

- [ ] **Step 1: 写临时探测类**(探测 opengauss-jdbc `logical()` 连 VastBase 的可行性)

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.opengauss.jdbc.PgConnection;
import org.opengauss.replication.PGConnection;
import org.opengauss.replication.fluent.ChainedStreamBuilder;

/**
 * 一次性 spike 工具(不入库):验证 opengauss-jdbc logical() 逻辑流对 VastBase G100
 * 的可用性——建槽/publication、收流 dump 帧字节、认证面观察。
 */
public final class VastLogicalSpike {
    public static void main(String[] args) throws Exception {
        // ① 建 publication + 逻辑槽(显式 FOR TABLE——M3 锚:OG 的 FOR ALL TABLES 不含分区表)
        try (Connection c = DriverManager.getConnection(
                "jdbc:opengauss://172.16.107.133:55433/test_a", "test", "Tester123");
             Statement st = c.createStatement()) {
            st.execute("DROP PUBLICATION IF EXISTS vb_spike_pub");
            st.execute("CREATE TABLE vb_spike_t(id int primary key, v text)");
            st.execute("ALTER TABLE vb_spike_t REPLICA IDENTITY FULL");
            st.execute("CREATE PUBLICATION vb_spike_pub FOR TABLE vb_spike_t");
            st.execute("SELECT pg_drop_replication_slot('vb_spike_slot')");
            st.execute("SELECT * FROM pg_create_logical_replication_slot('vb_spike_slot', 'pgoutput')");
        }
        // ② 复制流连接(URL replication=database + assumeMinServerVersion——M2 spike 同形)
        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "Tester123");
        props.setProperty("assumeMinServerVersion", "9.4");
        try (PgConnection conn = (PgConnection) DriverManager.getConnection(
                "jdbc:opengauss://172.16.107.133:55433/test_a?replication=database", props)) {
            PGConnection repl = conn.getReplicationAPI();
            try (var stream = repl.replicationStream().logical()
                    .withSlotName("vb_spike_slot")
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", "vb_spike_pub")
                    .start()) {
                // ③ 写一事务三行,轮询收帧 dump hex
                try (Connection w = DriverManager.getConnection(
                        "jdbc:opengauss://172.16.107.133:55433/test_a", "test", "Tester123");
                     Statement st = w.createStatement()) {
                    st.execute("INSERT INTO vb_spike_t VALUES (1,'spike'),(2,'row2')");
                    st.execute("UPDATE vb_spike_t SET v='upd' WHERE id=1");
                    st.execute("DELETE FROM vb_spike_t WHERE id=2");
                }
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline) {
                    byte[] msg = stream.readPending();
                    if (msg != null && msg.length > 0) {
                        System.out.printf("FRAME %d bytes: %s%n", msg.length, hex(msg, Math.min(msg.length, 64)));
                    }
                    Thread.sleep(200);
                }
            }
        }
    }

    static String hex(byte[] b, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) sb.append(String.format("%02x ", b[i]));
        return sb.toString();
    }
}
```

注意:`org.opengass.*` 包名以模块 classpath 里 opengauss-jdbc 实际包名为准(`org.opengauss.jdbc.PgConnection` / `org.opengauss.replication.*`——与 M2 spike `OgParityBaseline` 的 import 对照,文件在 `vb-stream-wal-source/src/test/java/.../it/OgParityBaseline.java`);编译期 import 错则照该文件改正。**Step 2: 跑探测并记录四项结论**

```bash
javac -encoding UTF-8 -d target/probe target/probe/VastLogicalSpike.java
java -cp "target/probe;vb-stream-wal-source/target/classes;$(cat vb-stream-wal-source/target/cp.txt)" VastLogicalSpike
```

记录(全部写入 spike 文档):
1. **认证**:logical() 复制流连接是否成功;失败则记异常类型与认证码(对照 M2 码 11 死角——`docs/wal-source-opengauss-m2-spike.md` 认证节)。若撞码 11:记"需专用角色 + hba sha256 行",列出生效 hba 行要求请用户在服务器侧配置后复测
2. **槽/publication**:CREATE PUBLICATION / pg_create_logical_replication_slot 是否成功
3. **帧形态**:B/R/I/U/D/C 帧字节——重点锚定:B 帧 xid 是否 Int64(8B)、'R' Relation 帧列定义段、TupleData 是否 Int16 ncols 前缀(与 OG 归一化器 `OgParityBaseline.Normalizer` 的两处帧级差异逐项比对)
4. **分区身份**:追加 `CREATE TABLE vb_spike_p(id int) PARTITION BY RANGE(id)(PARTITION p1 VALUES LESS THAN(10))` + publication 加母表 + 插行,观察 R/I 帧的 relname 是母表还是分区(PARTITION_NAME=mother 语义)

- [ ] **Step 3: 清理 spike 痕迹**(真库卫生)

```java
// VastSql 工具或追加到探测类:DROP PUBLICATION vb_spike_pub; DROP TABLE vb_spike_t[, vb_spike_p]; SELECT pg_drop_replication_slot('vb_spike_slot');
```

- [ ] **Step 4: 写 spike 文档** `docs/wal-source-vastbase-adaptation-spike.md`

结构参照 `docs/wal-source-opengauss-m2-spike.md`:目的/探测环境/四项结论(每项附原始字节或异常栈)/对拍基座裁定(帧同形→复用 Normalizer;异形→列出 Normalizer 增量分支点;逻辑流不可用→验收降级为字典对拍 + DML 冒烟)。

- [ ] **Step 5: Commit + push**

```bash
git add docs/wal-source-vastbase-adaptation-spike.md
git commit -m "docs(spike): VastBase 逻辑流基准路探测——认证/槽/帧形态/分区身份四锚定(vastbase T0)"
git push
```

---

### Task 1: TupleDecoder `name128` kind

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java`(`case "name"` 分支后,约 232-243 行)
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TupleDecoderTest.java`

**Interfaces:**
- Produces: 词典 kind 字符串 `"name128"`——128B 定宽、NUL 截断、渲染 String;Task 3 的三个词典数组引用

- [ ] **Step 1: 写失败测试**(TupleDecoderTest 增用例;手造字节走 `decodePayload` 公共入口,与现有用例同形)

```java
/**
 * name128 kind:VastBase G100 的 name 定宽 128B(NAMEDATALEN 加宽)——
 * 尾零截断、非 ASCII 名完整解出、对齐 'c' 无额外填充。
 */
@Test
void name128KindDecodesFixedWidthAndTruncatesAtNul() {
    // kinds 单列词典;元组数据区 128B:前缀 UTF-8 名 + NUL + 零填充
    byte[] tupleData = new byte[200];
    byte[] name = "vb_wal_probe表".getBytes(StandardCharsets.UTF_8);
    System.arraycopy(name, 0, tupleData, 0, name.length); // 无需写 NUL——数组已全零
    TupleDecoder d = new TupleDecoder();
    Object[] vals = d.decodePayload(tupleData, 0, new String[]{"name128"});
    assertEquals("vb_wal_probe表", vals[0]);
}
```

(具体入口签名以 `TupleDecoderTest` 现有用例为准——若 `decodePayload` 非 public 或参数形不同,照现有用例的手造元组辅助方法构造完整元组头后解码;断言不变。)

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=TupleDecoderTest`
Expected: FAIL——`name128` 落 default 分支抛 ISE(未注册 kind)

- [ ] **Step 3: 实现 name128 分支**(TupleDecoder `decodeTupleData` 的 kind switch,`case "name"` 紧后)

```java
case "name128" -> {  // VastBase G100 NameData：定宽 128B（NAMEDATALEN 加宽，'c' 对齐同 name），NUL 截断
    String s = new String(src, c, 128, StandardCharsets.UTF_8);
    int nul = s.indexOf('\0');
    vals[i + base] = nul >= 0 ? s.substring(0, nul) : s;
    c += 128;
}
```

(与 `case "name"` 现有实现逐行对照后平行书写;若 name 分支用了别的截断惯用法,保持同风格。)

- [ ] **Step 4: 跑测试确认通过**

Run: `mvn test -pl vb-stream-wal-source -Dtest=TupleDecoderTest`
Expected: PASS(含既有全部用例零回归)

- [ ] **Step 5: Commit + push**

```bash
git add vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/TupleDecoder.java vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/TupleDecoderTest.java
git commit -m "feat(wal-source): TupleDecoder name128 kind——VastBase NAMEDATALEN 128 定宽解码(vastbase T1)"
git push
```

---

### Task 2: 词典版本感知扩展 + nameDatumWidth(零行为重构)

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayout.java`(接口尾部新增 default)
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogRow.java`(NspRow.kinds(WalLayout) 增分支;PartitionRow 增 kinds(WalLayout) 重载)
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogReplay.java:220`(partitionDecodeKinds 定格点改调版本感知重载)
- Test: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/replay/OgPartitionDictionaryTest.java`(增 vanilla/OG 零回归锚)

**Interfaces:**
- Produces: `WalLayout.nameDatumWidth()`(default 64;Task 3 覆写 128);`CatalogRow.PartitionRow.kinds(WalLayout)`(vanilla/OG 返现 KINDS,VastBase 返 name128 变体——Task 3 接线);`NspRow.kinds(WalLayout)` VastBase 分支位(Task 3 接线)

- [ ] **Step 1: 写失败测试**(OgPartitionDictionaryTest 增零回归锚;VastBase 分支的断言留 Task 3 一并落——本任务只锁重构不改行为)

```java
/**
 * 词典版本感知重构零回归锚:PartitionRow.kinds(WalLayout) 对 vanilla(V17/V18)
 * 与 OG 档均返回与既有静态 KINDS 逐项相等——重构仅引入分派点,词典内容零变化。
 */
@Test
void partitionKindsVersionAwareReturnsExistingDictionariesUnchanged() {
    assertArrayEquals(CatalogRow.PartitionRow.kinds(), CatalogRow.PartitionRow.kinds(WalLayoutV17.INSTANCE));
    assertArrayEquals(CatalogRow.PartitionRow.kinds(), CatalogRow.PartitionRow.kinds(WalLayoutV18.INSTANCE));
    assertArrayEquals(CatalogRow.PartitionRow.kinds(), CatalogRow.PartitionRow.kinds(WalLayoutVOpenGauss.INSTANCE));
}
```

(若 `PartitionRow.kinds()` 现为 private 则先提为包可见;`WalLayoutV17/V18` import 简名。)

- [ ] **Step 2: 跑测试确认失败**(方法不存在编译红即视为失败确认)

Run: `mvn test -pl vb-stream-wal-source -Dtest=OgPartitionDictionaryTest`

- [ ] **Step 3: 实现三处**

① `WalLayout` 接口尾部(`majorVersion()` 前)加:

```java
/**
 * name 类型(NameData)在本档的定宽字节数。
 *
 * <p>V17/V18/OG 5.0 均为 64(NAMEDATALEN 缺省);VastBase G100 加宽至 128
 * (live 实查 pg_attribute 的 name 列 attlen=128,spec §0.2)。消费面:nsp/partition
 * 词典分派与读区重建几何的 name 差计算。缺省 64。</p>
 *
 * @return name 定宽字节数(64 / VastBase 128)
 */
default int nameDatumWidth() {
    return 64;
}
```

② `CatalogRow.NspRow.kinds(WalLayout)`(约 335-345 行)增 VastBase 分支占位(词典常量 Task 3 落):

```java
public static String[] kinds(WalLayout layout) {
    if (layout instanceof WalLayoutVVastbase) {
        return VAST_KINDS.clone();
    }
    return layout.openGaussDialect() ? OG_KINDS.clone() : KINDS.clone();
}
```

同 record 内新增常量(Task 3 填充内容前先以 OG_KINDS 同内容占位会破坏 TDD——**本任务直接一步到位**,见 ③):

```java
/** VastBase G100 pg_namespace 词典(8 项 = oidhdr + 七数据列,nspname 128B)。 */
private static final String[] VAST_KINDS = {
        TupleDecoder.OIDHDR_KIND,   // 元组头 oid(虚拟槽)
        "name128",   // nspname(VastBase NAMEDATALEN 128)
        "oid",    // nspowner
        "int8",   // nsptimeline
        "skip",   // nspacl
        "char",   // in_redistribution
        "bool",   // nspblockchain
        "oid",    // nspcollation
};
```

③ `CatalogRow.PartitionRow` 增版本感知重载(词典 = 现 KINDS 33 项复制、槽 1 `"name"`→`"name128"`,逐行注释照抄并改 relname 行注):

```java
/**
 * pg_partition 列解码词典(版本感知):VastBase G100 档返回 name128 变体
 * (32 数据列与 OG 5.0 逐列同名同序——live 实查,spec 附录 A;仅 relname 加宽),
 * 其余档返回既有 OG 词典(vanilla 无此表,返回值不被消费)。防御副本。
 */
public static String[] kinds(WalLayout layout) {
    if (layout instanceof WalLayoutVVastbase) {
        return VAST_KINDS.clone();
    }
    return KINDS.clone();
}
```

`VAST_KINDS` 常量 = 复制现 `KINDS` 数组全文后仅改槽 1(注释 `// relname(VastBase NAMEDATALEN 128)`);`instanceof WalLayoutVVastbase` 引用的类 Task 3 才建——**本任务与 Task 3 的类型依赖**:为保持每任务独立可编译,`WalLayoutVVastbase` 类骨架(空覆写版)在本任务一并落地(仅类声明 + INSTANCE + majorVersion 占位),完整词典/偏移归 Task 3。

④ `CatalogReplay.java:220` 改:`partitionDecodeKinds = CatalogRow.PartitionRow.kinds(layout);`

- [ ] **Step 4: 跑测试确认通过 + 全量离线零回归**

Run: `mvn test -pl vb-stream-wal-source -Dtest=OgPartitionDictionaryTest && mvn test -pl vb-stream-wal-source`
Expected: 全绿(476 用例;Testcontainers IT 若本机 Docker 未起则按既有跳过形态)

- [ ] **Step 5: Commit + push**

```bash
git add -A vb-stream-wal-source/src
git commit -m "refactor(wal-source): nsp/partition 词典版本感知分派 + WalLayout.nameDatumWidth——VastBase 分支位预埋,词典内容零变化(vastbase T2)"
git push
```

---

### Task 3: WalLayoutVVastbase 布局类 + 读区重建 VastBase 分支

**Files:**
- Create: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbase.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/replay/CatalogRow.java`(NspRow.readRegionLen/encodeReadRegion(WalLayout)、PartitionRow.READ_REGION 相关、ClassRow.encodeReadRegion(WalLayout)(161 行)各增 VastBase 分支;AttrRow 截断重建面核查)
- Test: Create `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalLayoutVVastbaseTest.java`

**Interfaces:**
- Consumes: `"name128"` kind(Task 1)、`nameDatumWidth()`(Task 2)
- Produces: `WalLayoutVVastbase.INSTANCE`(Task 4 判别返回它;`openGaussDialect()` 继承 true 使 startOpenGauss 装配自动工作);`majorVersion()=6`

- [ ] **Step 1: 写失败测试**

```java
/**
 * VastBase G100(OG 6.0.0 内核)第四布局档:词典几何/读区偏移/护栏槽位——
 * 全部锚定 2026-10-13 真库实测(spec 附录 A)。
 */
class WalLayoutVVastbaseTest {

    @Test
    void dictionariesMatchProbedColumnSets() {
        WalLayout v = WalLayoutVVastbase.INSTANCE;
        assertEquals(43, v.pgClassKinds().length);      // oidhdr + 42
        assertEquals(29, v.pgAttributeKinds().length);  // 无 oidhdr
        assertEquals(33, CatalogRow.PartitionRow.kinds(v).length);  // oidhdr + 32
        assertEquals(8, CatalogRow.NspRow.kinds(v).length);         // oidhdr + 7
    }

    @Test
    void pgClassDictionarySlotsAnchorProbedColumnOrder() {
        String[] k = WalLayoutVVastbase.INSTANCE.pgClassKinds();
        assertEquals("name128", k[1]);    // relname
        assertEquals("oid", k[7]);        // relfilenode(attnum 7——含 oidhdr 槽=attnum)
        assertEquals("oid", k[12]);       // reltoastrelid(attnum 12)
        assertEquals("char", k[21]);      // relkind(attnum 21)
        assertEquals("bool", k[25]);      // relhassecids(attnum 25,VastBase 插入)
        assertEquals("char", k[33]);      // parttype(attnum 33,OG 32)
        assertEquals("int8", k[41]);      // relminmxid(attnum 41)
        assertEquals("oid", k[42]);       // relrewrite(attnum 42)
    }

    @Test
    void pgAttributeDictionarySlotsAnchorProbedColumnOrder() {
        String[] k = WalLayoutVVastbase.INSTANCE.pgAttributeKinds();
        assertEquals("name128", k[1]);   // attname
        assertEquals("int2", k[5]);      // attnum(槽 5 == attrAttnumIndex,分叉点前)
        assertEquals("bool", k[14]);     // attisdropped(槽 14 == attrDroppedIndex)
        assertEquals("name128", k[19]);  // attalias(VastBase 插入)
        assertEquals("bool", k[20]);     // attisquoted(VastBase 插入)
        assertEquals("name128", k[28]);  // attdroppedname(尾列)
    }

    /** 自检锚(沿"两版 toast 偏移差恰 4B"先例):VastBase 与 OG 的 pg_class 读区偏移差恰 64B。 */
    @Test
    void readOffsetsDifferFromOpenGaussByExactly64() {
        WalLayout og = WalLayoutVOpenGauss.INSTANCE;
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        assertEquals(64, vb.pgClassRelfilenodeDataOffset() - og.pgClassRelfilenodeDataOffset());
        assertEquals(64, vb.pgClassReltoastrelidDataOffset() - og.pgClassReltoastrelidDataOffset());
        assertEquals(64, vb.pgClassRelkindDataOffset() - og.pgClassRelkindDataOffset());
        assertEquals(128, vb.nameDatumWidth());
    }

    /** 分叉点后护栏槽位 +1;分叉点前不变;majorVersion=6;supports 覆盖 90204。 */
    @Test
    void guardSlotIndexesAndVersionIdentity() {
        WalLayout vb = WalLayoutVVastbase.INSTANCE;
        assertEquals(32, vb.classParttypeIndex());     // OG 31 + 1
        assertEquals(35, vb.classReloptionsIndex());   // OG 34 + 1
        assertEquals(38, vb.classRelbucketIndex());    // OG 37 + 1
        assertEquals(13, vb.classReldeltarelidIndex()); // 分叉点前不变
        assertEquals(15, vb.classRelcudescrelidIndex());
        assertEquals(6, vb.majorVersion());
        assertTrue(vb.supports(90204));
        assertTrue(vb.openGaussDialect());             // 继承——startOpenGauss 装配面
        assertEquals(5, vb.attrAttnumIndex());         // 继承(分叉点前)
        assertEquals(14, vb.attrDroppedIndex());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**(类不存在/常量缺)

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalLayoutVVastbaseTest`

- [ ] **Step 3: 实现 WalLayoutVVastbase 完整版**(替换 Task 2 的骨架)

```java
package org.vastdata.vbstream.walsource.layout;

/**
 * VastBase G100 V3.0(基于 openGauss 6.0.0 内核,2026-03-31 Build 9)第四布局档:
 * 记录层与 OG 5.0.0 全同构(页魔数/记录头/heap 尺寸/块引用——132019 条真实 WAL
 * 记录零 resync 实证,spec §0.2),差异集中在 catalog 词典几何——
 * ① NAMEDATALEN 64→128(name 类型全 catalog 加宽,读区偏移统一 +64B);
 * ② pg_class 42 列(OG 40):attnum 25 插 relhassecids,尾部增 relminmxid/relrewrite;
 * ③ pg_attribute 29 列(OG 24):attnum 20/21 插 attalias/attisquoted,尾部增
 * attidentity/attextraopt/attdroppedname。
 * 全部列序/attlen 锚定 2026-10-13 真库实查(spec 附录 A)。判别经 relname attlen
 * 探测(WalLayouts 三参重载);majorVersion=6 使 v5 检查点拒载回落全新引导。
 * 线程约束:无状态单例,并发安全。
 */
public final class WalLayoutVVastbase extends WalLayoutVOpenGauss {

    /** 单例。 */
    public static final WalLayoutVVastbase INSTANCE = new WalLayoutVVastbase();

    /** pg_attribute 词典(29 项,无 oidhdr——元组无 oid),列序 2026-10-13 实查。 */
    private static final String[] PG_ATTRIBUTE_KINDS = {
            "oid",     // attrelid
            "name128", // attname(VastBase NAMEDATALEN 128)
            "oid",     // atttypid
            "int4",    // attstattarget(PG≤16 位次)
            "int2",    // attlen
            "int2",    // attnum                  <-- 槽 5(继承 attrAttnumIndex)
            "int4",    // attndims
            "int4",    // attcacheoff
            "int4",    // atttypmod
            "bool",    // attbyval
            "char",    // attstorage
            "char",    // attalign
            "bool",    // attnotnull
            "bool",    // atthasdef
            "bool",    // attisdropped           <-- 槽 14(继承 attrDroppedIndex)
            "bool",    // attislocal
            "int1",    // attcmprmode
            "int4",    // attinhcount
            "oid",     // attcollation            ---- 分叉点(槽 19 起)----
            "name128", // attalias(VastBase 插入,实查 typid 19/attlen 128)
            "bool",    // attisquoted(VastBase 插入)
            "skip",    // attacl
            "skip",    // attoptions
            "skip",    // attfdwoptions
            "skip",    // attinitdefval
            "int1",    // attkvtype
            "char",    // attidentity(VastBase 插入,typid 18)
            "int8",    // attextraopt(VastBase 插入,typid 20/attlen 8)
            "name128", // attdroppedname(VastBase 插入)
    };

    /** pg_class 词典(oidhdr + 42 项 = 43,数组槽 = attnum——oidhdr 前导)。 */
    private static final String[] PG_CLASS_KINDS = {
            TupleDecoder.OIDHDR_KIND, // 元组头 oid(9.2 式,继承 classOidInTupleHeader)
            "name128", // relname(VastBase NAMEDATALEN 128)
            "oid",     // relnamespace
            "oid",     // reltype
            "oid",     // reloftype
            "oid",     // relowner
            "oid",     // relam
            "oid",     // relfilenode            <-- attnum 7
            "oid",     // reltablespace
            "float8",  // relpages
            "float8",  // reltuples
            "int4",    // relallvisible
            "oid",     // reltoastrelid          <-- attnum 12
            "oid",     // reltoastidxid
            "oid",     // reldeltarelid          <-- 物理槽 13(护栏,分叉前)
            "oid",     // reldeltaidx
            "oid",     // relcudescrelid         <-- 物理槽 15(护栏,分叉前)
            "oid",     // relcudescidx
            "bool",    // relhasindex
            "bool",    // relisshared
            "char",    // relpersistence
            "char",    // relkind                <-- attnum 21
            "int2",    // relnatts
            "int2",    // relchecks
            "bool",    // relhasoids             ---- 分叉点(槽 24/attnum 25 起)----
            "bool",    // relhassecids(VastBase 插入)
            "bool",    // relhaspkey
            "bool",    // relhasrules
            "bool",    // relhastriggers
            "bool",    // relhassubclass
            "int1",    // relcmprs
            "bool",    // relhasclusterkey
            "bool",    // relrowmovement
            "char",    // parttype               <-- attnum 33(OG 32;物理槽 32)
            "int4",    // relfrozenxid
            "skip",    // relacl
            "text",    // reloptions             <-- 物理槽 35(护栏;UStore/段页式 sniff 面)
            "char",    // relreplident
            "int8",    // relfrozenxid64
            "oid",     // relbucket              <-- 物理槽 38(护栏)
            "skip",    // relbucketkey
            "int8",    // relminmxid             <-- attnum 41(OG 40)
            "oid",     // relrewrite(VastBase 新增尾列)
    };

    /** 私有构造器:单例描述符。 */
    private WalLayoutVVastbase() {
    }

    /** {@inheritDoc}——29 项(OG 24 + attalias/attisquoted 尾部三列,槽 19 起分叉)。 */
    @Override
    public String[] pgAttributeKinds() {
        return PG_ATTRIBUTE_KINDS.clone();
    }

    /** {@inheritDoc}——43 项(oidhdr + 42;槽 24 起分叉,读区偏移 +64)。 */
    @Override
    public String[] pgClassKinds() {
        return PG_CLASS_KINDS.clone();
    }

    /** {@inheritDoc}——148(relname 128 + 5×oid;OG 84 + 64,实查列宽推演)。 */
    @Override
    public int pgClassRelfilenodeDataOffset() {
        return 148;
    }

    /** {@inheritDoc}——180(148 + reltablespace 4 + float8×2 d 对齐 + relallvisible 4)。 */
    @Override
    public int pgClassReltoastrelidDataOffset() {
        return 180;
    }

    /** {@inheritDoc}——207(180 + oid×5 + bool×2 + char;类注释锚,投影走词典槽 21)。 */
    @Override
    public int pgClassRelkindDataOffset() {
        return 207;
    }

    /** {@inheritDoc}——32(attnum 33;relhassecids 插入使 OG 31 + 1——流内存储护栏判别面)。 */
    @Override
    public int classParttypeIndex() {
        return 32;
    }

    /** {@inheritDoc}——35(OG 34 + 1;UStore/段页式 reloptions sniff 判别面)。 */
    @Override
    public int classReloptionsIndex() {
        return 35;
    }

    /** {@inheritDoc}——38(OG 37 + 1;bucket/段页式判别面)。 */
    @Override
    public int classRelbucketIndex() {
        return 38;
    }

    /** {@inheritDoc}——128(VastBase NAMEDATALEN 加宽;V17/V18/OG 64)。 */
    @Override
    public int nameDatumWidth() {
        return 128;
    }

    /** {@inheritDoc}——6(档号延续:OG=5;v5 检查点在本档拒载回落全新引导)。 */
    @Override
    public int majorVersion() {
        return 6;
    }

    /** {@inheritDoc}——90204(VastBase 与 OG/vanilla 9.2 撞号;档位判别在 WalLayouts 探测)。 */
    @Override
    public boolean supports(int pgVersionNum) {
        return pgVersionNum == 90204;
    }
}
```

- [ ] **Step 4: 读区重建面 VastBase 分支**(CatalogRow 三个行模型)

① `NspRow.readRegionLen(WalLayout)`(359-361 行)与 `encodeReadRegion(WalLayout)`(420 行区):VastBase 分支读区长 **144**(= OG 80 + 64:nspname@0 128 + nspowner@132(4) + nsptimeline@136(8))——实现:`layout instanceof WalLayoutVVastbase ? 144 : (openGaussDialect ? 80 : 72)`,encodeReadRegion 的 VastBase 分支照 OG 分支结构写 name128(128B 定宽 + 零填充)。

② `PartitionRow` 读区(READ_REGION_LEN=120 为 OG 值,经版本感知方法消费处同 nsp 模式):VastBase **184**(= 120 + 64,relname 加宽);`encodeReadRegion(WalLayout)` 增 VastBase 分支(relname 编 128B)。

③ `ClassRow.encodeReadRegion(WalLayout)`(161 行):核查其 OG 分支读区长与 name 编码宽度,VastBase 分支 = OG 值 + 64、name 编 128B(数值与结构照 161 行 OG 分支逐段平移)。

④ **AttrRow 核查**:grep AttrRow 截断重建/读区消费(`CatalogReplay` attr 腿的 `TruncatedUpdateSplice` 消费),若 attname 以 64B 硬编码参与值编码重建,同步参数化(经 `layout.nameDatumWidth()`);无读区面则记 javadoc 注记。

- [ ] **Step 5: 跑测试确认通过 + 全量**

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalLayoutVVastbaseTest && mvn test -pl vb-stream-wal-source`
Expected: 全绿

- [ ] **Step 6: Commit + push**

```bash
git add -A vb-stream-wal-source/src
git commit -m "feat(wal-source): WalLayoutVVastbase 第四布局档——name128 词典几何/读区偏移 +64/护栏槽位 +1/读区重建分支(vastbase T3)"
git push
```

---

### Task 4: 判别三参重载 + WalSource 探测 + instanceof 门控

**Files:**
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/layout/WalLayouts.java`
- Modify: `vb-stream-wal-source/src/main/java/org/vastdata/vbstream/walsource/api/WalSource.java`(start() 429-440 行区)
- Test: Modify `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/layout/WalLayoutsOgDiscriminatorTest.java`;Create `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/api/VastProbeDiscriminatorTest.java`

**Interfaces:**
- Consumes: `WalLayoutVVastbase.INSTANCE`(Task 3)
- Produces: `WalLayouts.forServer(int, String, int)`(三参重载;两参版语义不变);`WalSource.start()` 对 VastBase 自动判别(建槽前 ISE 纪律)

- [ ] **Step 1: 写失败测试**

WalLayoutsOgDiscriminatorTest 增:

```java
/**
 * 三参重载 VastBase 分支:90204 + 无 openGauss branding + relname attlen=128
 * → VastBase 档;attlen=64 → vanilla 9.2 ISE(消息含探测值);两参版行为不变。
 */
@Test
void threeArgOverloadRoutesVastbaseByProbedNameWidth() {
    String vanillaLike = "PostgreSQL 9.2.4 on x86_64-pc-linux-gnu, compiled by g++ (GCC) 7.3.0, 64-bit";
    assertSame(WalLayoutVVastbase.INSTANCE, WalLayouts.forServer(90204, vanillaLike, 128));
    IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> WalLayouts.forServer(90204, vanillaLike, 64));
    assertTrue(ex.getMessage().contains("128") || ex.getMessage().contains("64")); // 探测值透传可诊断
    assertSame(WalLayoutVOpenGauss.INSTANCE,
            WalLayouts.forServer(90204, "(openGauss 5.0.0 build x)", 64));        // branding 优先于探测
    assertThrows(IllegalStateException.class,
            () -> WalLayouts.forServer(90204, vanillaLike, -1));                   // 探测不可得仍拒
}
```

新建 VastProbeDiscriminatorTest(探测查询构造面——探测 SQL 常量与解析函数单测,不连库):

```java
/**
 * WalSource 的 relname 探测面:SQL 常量形状 + attlen 解析(128/64/无行)三分支。
 * 真库探测路径归手动档 IT(VastCatalogSyncIT)——本类只锚 SQL 与解析纯函数。
 */
class VastProbeDiscriminatorTest {
    @Test
    void probeSqlTargetsPgClassRelnameAttlen() {
        assertTrue(WalSource.RELNAME_PROBE_SQL.contains("attlen")
                && WalSource.RELNAME_PROBE_SQL.contains("'pg_class'::regclass")
                && WalSource.RELNAME_PROBE_SQL.contains("'relname'"));
    }
}
```

(`RELNAME_PROBE_SQL` 设为 public static final;解析函数 `parseRelnameAttlen(ResultSet)`——128/64/无行→OptionalInt 形态按实现落测试。)

- [ ] **Step 2: 跑测试确认失败**

Run: `mvn test -pl vb-stream-wal-source -Dtest=WalLayoutsOgDiscriminatorTest,VastProbeDiscriminatorTest`

- [ ] **Step 3: 实现**

① WalLayouts 加三参重载(两参版委派三参版、探测值传 -1 保持现行为):

```java
/**
 * 带版本文本与 name 宽度探测的分发(VastBase 判别)。
 *
 * <p>关键步骤:与两参版同判据处理 17/18 区间与 openGauss branding;9 万段无
 * openGauss 字样时按 pgClassRelnameAttlen 分支——128 返回 VastBase 档
 * (VastBase G100 的 version() 报 vanilla 形态,branding 只在 v$version,唯一
 * 稳定区分面是 NAMEDATALEN 探测,spec §4);64/其他/不可得(-1)落 vanilla 9.2
 * ISE(消息附探测值)。边界与异常语义:探测值不影响 branding 命中路径(优先)。
 * 线程约束:静态纯函数。</p>
 */
public static WalLayout forServer(int pgVersionNum, String versionText, int pgClassRelnameAttlen) {
    if (pgVersionNum >= 180000 && pgVersionNum < 190000) {
        return WalLayoutV18.INSTANCE;
    }
    if (pgVersionNum >= 170000 && pgVersionNum < 180000) {
        return WalLayoutV17.INSTANCE;
    }
    if (pgVersionNum / 10000 == 9) {
        if (versionText != null && versionText.contains("openGauss")) {
            if (versionText.contains("openGauss 5.0.")) {
                return WalLayoutVOpenGauss.INSTANCE;
            }
            throw new IllegalStateException("openGauss 版本不在支持面(当前仅 5.0.x): " + versionText);
        }
        if (pgClassRelnameAttlen == 128) {
            return WalLayoutVVastbase.INSTANCE;
        }
        throw new IllegalStateException("vanilla PG 9.2 非目标版本(无复制槽/无逻辑日志,见 "
                + "docs/wal-source-opengauss-adaptation-research.md;VastBase 探测 relname attlen="
                + pgClassRelnameAttlen + "): " + versionText);
    }
    throw new IllegalStateException("unsupported server version " + pgVersionNum + ": " + versionText);
}
```

原两参方法体改为 `return forServer(pgVersionNum, versionText, -1);`。

② WalSource:start() 436 行前插入探测,门控改 instanceof:

```java
// VastBase 判别第二面(spec §4):9 万段无 openGauss branding 时探测 pg_class.relname
// 的 attlen——VastBase G100 的 version() 报 vanilla 形态无 branding,唯一稳定区分面是
// NAMEDATALEN 加宽(128);64/不可得落 vanilla 9.2 拒绝面(真 vanilla 零扰动)。
// 探测在建槽之前(ISE 先于槽副作用,M2 护栏纪律)。
int relnameAttlen = -1;
if (versionNum >= 90_000 && versionNum < 100_000
        && (versionText == null || !versionText.contains("openGauss"))) {
    relnameAttlen = queryRelnameAttlen(sqlConnection);
}
WalLayout layout = WalLayouts.forServer(versionNum, versionText, relnameAttlen);
if (layout instanceof WalLayoutVOpenGauss) {   // 原实例相等改 instanceof——VastBase 子类同走 OG 装配
    startOpenGauss(layout);
    return;
}
```

`queryRelnameAttlen(Connection)`(私有,RELNAME_PROBE_SQL 常量执行 + 无行/SQLException → -1 记 DEBUG 不中断——探测失败落既有 vanilla ISE 面消息可诊断)+ `public static final String RELNAME_PROBE_SQL = "SELECT attlen FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relname'"`。

- [ ] **Step 4: 跑测试确认通过 + 全量 + OgWalSourceConfigTest 零回归锚核查**

Run: `mvn test -pl vb-stream-wal-source`
Expected: 全绿(OG 档既有判别/装配测试零回归——两参语义不变)

- [ ] **Step 5: Commit + push**

```bash
git add -A vb-stream-wal-source/src
git commit -m "feat(wal-source): relname attlen 探测判别 + forServer 三参重载 + OG 装配门控 instanceof 化(vastbase T4)"
git push
```

---

### Task 5: VastTestEnv + VastCatalogSyncIT(手动档真库 IT)

**Files:**
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastTestEnv.java`
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastCatalogSyncIT.java`
- Modify: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/package-info.java`(若需要)

**Interfaces:**
- Consumes: `WalSource`(api 门面,判别自动 VastBase 档)
- Produces: `VastTestEnv` 静态单例(`sql(String...)` 执行器 + `params()` 连接参数 + 表/槽前缀常量)——Task 6/7 复用

- [ ] **Step 1: 写 VastTestEnv**

```java
/**
 * VastBase G100 真库测试环境(手动档 -Dvb.it.vastbase=true):直连 172.16.107.133:55433/test_a
 * (opengauss-jdbc 前缀),静态单例——非 Testcontainers,库是长活真环境,纪律:测试表/槽
 * 一律 vb_wal_ 前缀 + 用例自清,不触碰既有业务表与逻辑槽(orders/sync_orders_slot 等)。
 * 连接参数经 -Dvb.it.vastbase.{host,port,db,user,pass} 可覆盖,缺省真库实测值。
 */
public final class VastTestEnv {
    /** 测试对象前缀(表/槽/publication 共用)。 */
    public static final String PREFIX = "vb_wal_";
    private static volatile Connection shared;
    private VastTestEnv() {
    }
    /** 环境地址(缺省真库)。 */
    public static String host() {
        return System.getProperty("vb.it.vastbase.host", "172.16.107.133");
    }
    // port()/db()/user()/pass() 同形——缺省 55433/test_a/test/Tester123
    /** JDBC URL(opengauss-jdbc 前缀——sha256 认证兼容)。 */
    public static String jdbcUrl() {
        return "jdbc:opengauss://" + host() + ":" + port() + "/" + db();
    }
    /** 共享 SQL 会话(懒建单连接;测试全部跑在单线程 JUnit 方法内,无并发争用)。 */
    public static synchronized Connection sql() throws SQLException {
        if (shared == null || shared.isClosed()) {
            shared = DriverManager.getConnection(jdbcUrl(), user(), pass());
        }
        return shared;
    }
    /** 语句序列执行器(DDL/DML;SELECT 场景各 IT 自持 Statement)。 */
    public static void sql(String... statements) throws SQLException {
        try (Statement st = sql().createStatement()) {
            for (String s : statements) {
                st.execute(s);
            }
        }
    }
}
```

- [ ] **Step 2: 写 VastCatalogSyncIT**(断言面翻译自 `OgCatalogSyncIT`——干扰 DDL 序列下字典 vs JDBC REPEATABLE READ 实查逐行全等)

```java
/**
 * VastBase catalog 同步对拍(手动档):干扰 DDL 序列(CREATE/ALTER ADD/DROP COLUMN/
 * RENAME/TRUNCATE/CREATE SCHEMA,含分区 ADD/DROP——M3 面)下,WalSource 四字典
 * 末态 vs JDBC REPEATABLE READ 实查逐行全等。断言器复用 {@code WalSyncItBase}
 * 的对拍语义(REPEATABLE READ 单事务首句读 flush LSN 同 C1 纪律);连接形态
 * 换 VastTestEnv 直连(非容器)。
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastCatalogSyncIT {
    @Test
    void adversarialDdlSequenceEndsInParity() throws Exception {
        // ① 建干扰表集(vb_wal_ 前缀)+ WalSource 起(state 目录 = target 下独立子目录,
        //    用例级 wipe——Windows 管道纪律:独立子目录)
        // ② 干扰序列:ALTER ADD/DROP COLUMN ×N、RENAME、TRUNCATE、CREATE SCHEMA vb_wal_s、
        //    分区表 CREATE/ADD/DROP PARTITION(pg_partition 第四字典面)
        // ③ 停流;REPEATABLE READ 实查 pg_attribute/pg_class/pg_namespace/pg_partition
        //    (投影列照 OgCatalogSyncIT 的查询面翻译)
        // ④ 字典快照逐行 diff 空(ctid 键控,RelFileNode/toast/oid 断言同源)
        // ⑤ finally:DROP 表/schema + 槽
    }
}
```

(骨架步骤 ①-⑤ 的具体实现**逐段对照 `OgCatalogSyncIT` 与其用的对拍器基类翻译**——那两个文件是断言面的权威源;差异仅:环境直连、表集独立、无容器生命周期。每个 @Test 方法 javadoc 说明场景;干扰循环次数缩至 20 轮(真库轻量纪律)。)

- [ ] **Step 3: 跑手动档 IT**

Run: `mvn test -pl vb-stream-wal-source -Dtest=VastCatalogSyncIT -Dvb.it.vastbase=true`
Expected: PASS——四字典对拍全等(这是 VastBase 词典几何的端到端验收)

- [ ] **Step 4: 常规 mvn test 确认跳过**

Run: `mvn test -pl vb-stream-wal-source`(不带开关)
Expected: VastCatalogSyncIT skipped(EnabledIfSystemProperty 生效),其余全绿

- [ ] **Step 5: Commit + push**

```bash
git add vb-stream-wal-source/src/test
git commit -m "test(wal-source): VastTestEnv + VastCatalogSyncIT 真库 catalog 对拍手动档(vastbase T5)"
git push
```

---

### Task 6: VastDmlParityIT(双路对拍,T0 结论驱动)

**Files:**
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastDmlParityIT.java`
- Modify(按 T0 结论): `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/OgParityBaseline.java`(若帧异形,Normalizer 增 VastBase 分支)

**Interfaces:**
- Consumes: T0 spike 结论(帧形态/认证);`VastTestEnv`(Task 5);`OgParityBaseline.Normalizer`(M2 归一化器)
- Produces: VastBase 档 DML 面双路对拍验收

- [ ] **Step 1: 按 T0 结论定基座形态**

- **帧同形**(T0 结论 B 帧/TupleData 与 OG 逐字节同形):基座复用 `OgParityBaseline`,仅连接参数换 `VastTestEnv`、槽名/publication 用 `vb_wal_` 前缀
- **帧异形**:Normalizer 增 VastBase 分支(差异点按 T0 记录的字节锚逐处分支,门控 `layout instanceof` 或独立参数——沿"帧级差异两处"的既有注记格式)
- **逻辑流不可用**(认证死角且服务器侧无法配 hba):本任务**降级**为单路字面验证——WalSource DML 面跑通六场景,事务块输出(TXN-BEGIN/行/TXN-END)与手工 INSERT/UPDATE/DELETE 的期望字面逐行断言(OutputRendererTest 的期望形态手写),并在 javadoc 记降级原因;同时 spec §6 回填

- [ ] **Step 2: 写对拍用例**(场景面 = OgDmlParityIT 精简版)

场景:①基础 DML 六形态(单行/多语句/交错/回滚/SAVEPOINT/截断中段 UPDATE——REPLICA IDENTITY FULL)②17 类型矩阵边界值(A 档 date=timestampr 形态含时段)③TOAST 三形态(external 大值)④DDL-in-txn as-of ⑤2PC 四形态⑥中途停续(state.dir 两轮)。每场景:基准路(逻辑流)与 wal 路各自独立槽捕获 → 按 xid 交集逐行 diff 空。

```java
/**
 * VastBase DML 双路对拍(手动档):逻辑流基准路(opengauss-jdbc logical(),T0 spike
 * 结论形态)vs WalSource 直解路,六场景按 xid 交集逐行 diff 空——OgDmlParityIT
 * 场景面的真库翻译。A 档专面:date 列底层 timestamp(1114)含时段(M3 探针语义
 * 预期同 OG);'' 入库即 NULL(两路同见天然吸收)。
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastDmlParityIT { /* 场景方法逐个翻译,骨架步骤同 OgDmlParityIT */ }
```

(实现逐段对照 `OgDmlParityIT` 翻译;环境直连差异同 Task 5;每场景 finally 清理。)

- [ ] **Step 3: 跑手动档 IT**

Run: `mvn test -pl vb-stream-wal-source -Dtest=VastDmlParityIT -Dvb.it.vastbase=true`
Expected: PASS(六场景 diff 空;A 档专面字面锚过)

- [ ] **Step 4: Commit + push**

```bash
git add vb-stream-wal-source/src/test
git commit -m "test(wal-source): VastDmlParityIT 六场景双路对拍——T0 结论基座形态(vastbase T6)"
git push
```

---

### Task 7: VastLifecycleIT + 文档收口

**Files:**
- Create: `vb-stream-wal-source/src/test/java/org/vastdata/vbstream/walsource/it/vastbase/VastLifecycleIT.java`
- Modify: `vb-stream-wal-source/CLAUDE.md`(VastBase 档节)
- Modify: `vb-stream-wal-source/README.md`(VastBase 快速开始节)
- Modify: `CLAUDE.md`(根,wal-source 段落补 VastBase 一句)
- Modify: `docs/wal-source-opengauss-adaptation-research.md`(VastBase 段勘误/补记——判别第二面与词典几何差异)
- Modify: `docs/superpowers/specs/2026-10-13-wal-source-vastbase-adaptation-design.md`(执行偏差回填:护栏槽位 +1 覆写补充、nsp/partition 词典分派沿 CatalogRow 而非上移接口——见本计划 Task 2)

**Interfaces:**
- Consumes: 前序全部;Produces: 收口文档与最终验收

- [ ] **Step 1: 写 VastLifecycleIT**(翻译自 `OgLifecycleIT` 三用例:检查点续传两轮启停 / state 损坏回落全新引导 / 门面全装配 og.source=direct 冒烟)

```java
/** VastBase 生命周期(手动档):续传/损坏回落/全装配直连冒烟——OgLifecycleIT 真库翻译。 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastLifecycleIT { /* 三用例逐段翻译,state 目录 target 下独立子目录 + 用例级 wipe */ }
```

- [ ] **Step 2: 跑手动档全部 VastBase IT + 离线全量**

Run: `mvn test -pl vb-stream-wal-source -Dtest='Vast*IT' -Dvb.it.vastbase=true && mvn test -pl vb-stream-wal-source`
Expected: 全绿

- [ ] **Step 3: 文档收口**(五处)

① 模块 CLAUDE.md 增"VastBase G100 档"节:判别(relname 探测)/词典几何差异表(name128/42/29 列/偏移 +64)/手动档 IT 开关与连接参数/已知限制(仅 V3.0、其他 OG 系发行版仍拒);测试计数对账(surefire 逐类数,四处文档口径一致)
② README 增"VastBase G100 快速开始"节(前置:opengauss 前缀 + wal_level=logical + REPLICATION 权限;示例命令)
③ 根 CLAUDE.md wal-source 段补一句(第四布局档 + spec 链接)
④ 调研文档补 VastBase 段(2026-10-13 实测发现回填)
⑤ spec 执行偏差注记(护栏槽位覆写补充 + 词典分派形态)

- [ ] **Step 4: Commit + push**

```bash
git add -A
git commit -m "test+docs(wal-source): VastLifecycleIT + VastBase 档文档收口——模块/根/README/调研/spec 偏差回填(vastbase T7)"
git push
```

---

## 自审记录(计划完成后已核对)

1. **Spec 覆盖**:spec §2(布局档/词典上移)→ T2/T3(执行裁定:分派沿 CatalogRow 版本感知先例而非接口上移,T7 回填 spec);§3(词典几何)→ T3 数组全文;§4(判别)→ T4;§5(零改动断言)→ T5/T6 验证面;§6(T0)→ T0;§7(验收)→ T1-T7 各测试步骤;§8(风险)→ T6 降级路径。**spec 遗漏补齐**:护栏槽位三覆写(T3)——spec §3.1 未列,T7 回填。
2. **Placeholder 扫描**:T5/T6 的"逐段对照 OgXxxIT 翻译"指向仓内权威源文件 + 骨架步骤与差异点说明(非占位——翻译类任务的完整复刻会在计划里失真且违反 DRY,翻译源在仓);其余任务代码全文给出。
3. **类型一致**:`name128`/`nameDatumWidth()`/`forServer(int,String,int)`/`RELNAME_PROBE_SQL`/`kinds(WalLayout)` 各任务引用与定义处签名一致;`WalLayoutVVastbase.INSTANCE` 单例形态与 `WalLayoutVOpenGauss.INSTANCE` 同构。
