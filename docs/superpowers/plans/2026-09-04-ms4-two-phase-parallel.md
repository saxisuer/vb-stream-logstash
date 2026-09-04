# MS4 实现计划：two_phase 两阶段 IT + parallel 端到端 + R5 存量槽预检

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落 MS4 验收——两阶段 IT 四场景全绿（含 parallel 档 StreamPrepare 端到端、重启 prepared 续传）+ R5 存量槽 two_phase 不匹配的客户端预检。

**Architecture:** 主代码唯一改动在 `ReplicationSession.ensureSlot`（42710 复用分支前增查 `pg_replication_slots.two_phase`，不匹配抛带迁移指引的 `IllegalStateException`）；验收主体是新增 `TwoPhaseIT`（挂既有 `StreamITBase` embedded-engine 基座），四场景翻译自引擎 `TwoPhaseTransactionTest` + 设计文档点名的重启 prepared 续传。

**Tech Stack:** Java 17 + Maven 多模块、Debezium 3.6.1 embedded async engine 测试基座（`AbstractAsyncEngineConnectorTest`）、Testcontainers postgres:18、JDK 动态代理假 Connection（零第三方 mock）。

**Spec:** `docs/superpowers/specs/2026-09-04-ms4-two-phase-parallel-design.md`

## Global Constraints

- 日志一律 slf4j（`private static final Logger LOG = LoggerFactory.getLogger(Xxx.class)`），禁止 System.out/System.err；消息用 `{}` 占位符
- **每个函数（含私有方法与测试辅助方法）必须有 javadoc 逻辑描述**：职责/关键步骤/边界与异常语义/线程约束
- 测试基于 JUnit Jupiter；IT 需本机 Docker（Testcontainers）
- 每任务完成即 `git commit` 并 `git push origin worktree-ms35-logical-msg-guard`（跨电脑开发约定）；commit message 尾加 `Co-Authored-By: Claude <noreply@anthropic.com>`
- 单测命令形态：`mvn test -pl vb-stream-connector-postgres-stream -Dtest=类名`（多模块后 `-Dtest` 必带 `-pl`）
- 工作目录：仓库根即当前 worktree 根（所有路径相对它）

---

### Task 1: R5 存量槽 two_phase 预检（TDD 单测先行）

**Files:**
- Modify: `vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/ReplicationSession.java`（ensureSlot 的 catch 分支 + 新静态方法，约 96-122 行区域）
- Test: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/ReplicationSessionTest.java`

**Interfaces:**
- Consumes: 既有静态接缝 `ReplicationSession.ensureSlot(Connection sqlConnection, Parameters config) throws SQLException`；`Parameters.twoPhase()`/`slotName()`
- Produces: 新静态方法 `static void verifySlotTwoPhaseMatches(Connection sqlConnection, Parameters config) throws SQLException`（包私有）；42710 分支行为变更——不匹配抛 `IllegalStateException`（运行时异常，`throws SQLException` 签名不变）

- [ ] **Step 1: 改造既有 42710 用例并写两条失败新用例**

既有用例 `ensureSlotSwallowsDuplicateObjectButRethrowsOtherSqlStates` 的 42710 子块**必须移除**（旧假件对一切 executeQuery 抛 42710，预检查询也会中招，行为已变）。改造后该用例只保留成功路径与 42P01 上抛两段：

```java
    /**
     * ensureSlot 的 SQL 契约与异常语义:建槽 SQL 文本与两个绑定参数固定(第 4 参 twoPhase
     * 随槽声明);executeQuery 正常返回走成功路径;非 42710 的 SQLException 原样上抛。
     * 42710 复用语义(含 R5 预检三分支)由 {@link #ensureSlotRejectsExistingSlotWithMismatchedTwoPhase}
     * 与 {@link #ensureSlotReusesExistingSlotWhenTwoPhaseMatchesOrRowAbsent} 以目录查询脚本假件覆盖。
     */
    @Test
    void ensureSlotSwallowsDuplicateObjectButRethrowsOtherSqlStates() {
        List<String> okSql = new ArrayList<>();
        assertDoesNotThrow(() -> ReplicationSession.ensureSlot(
                fakeConnection(null, okSql, new ArrayList<>()), parameters(StreamingMode.ON, true, 10)),
                "executeQuery 正常返回时应走成功路径(只需副作用)");
        assertEquals(List.of("SELECT pg_create_logical_replication_slot(?, 'pgoutput', false, ?)"), okSql,
                "建槽 SQL 文本固定(第 3 参临时槽=false、第 4 参 two_phase 绑定)");

        SQLException thrown = assertThrows(SQLException.class, () -> ReplicationSession.ensureSlot(
                fakeConnection("42P01", new ArrayList<>(), new ArrayList<>()), parameters(StreamingMode.ON, true, 10)),
                "非 42710 的 SQLException 必须原样上抛");
        assertEquals("42P01", thrown.getSQLState(), "上抛的异常不得被吞改");
    }
```

新增两个用例 + 新假件 helper（与既有 `fakeConnection` 并列）：

```java
    /**
     * R5 预检:槽已存在(42710)且目录中 two_phase=false 与配置 true 不匹配 → 启动期抛
     * IllegalStateException,文案须含 DROP SLOT 迁移指引(PG 不允许后改槽属性,重插槽会
     * 丢确认位点——报错把代价说清,用户自行决策)。
     */
    @Test
    void ensureSlotRejectsExistingSlotWithMismatchedTwoPhase() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> ReplicationSession.ensureSlot(
                slotLookupConnection(false), parameters(StreamingMode.ON, true, 10)),
                "two_phase 属性与配置不匹配的存量槽必须在启动期拒绝");
        assertTrue(e.getMessage().contains("DROP SLOT"), "报错应带 DROP SLOT 迁移指引: " + e.getMessage());
        assertTrue(e.getMessage().contains("vb_cdc_slot"), "报错应点名槽名: " + e.getMessage());
    }

    /**
     * R5 预检:两处放行形态——①槽已存在且 two_phase 匹配(true==true)→ WARN 复用不抛;
     * ②目录查询行不存在(slotTwoPhase=null,42710 与目录可见性之间的竞态兜底)→ 复用不抛,
     * 极端情况留给 start 时服务端报错。
     */
    @Test
    void ensureSlotReusesExistingSlotWhenTwoPhaseMatchesOrRowAbsent() {
        assertDoesNotThrow(() -> ReplicationSession.ensureSlot(
                slotLookupConnection(true), parameters(StreamingMode.ON, true, 10)),
                "槽已存在且 two_phase 匹配应复用而非失败");
        assertDoesNotThrow(() -> ReplicationSession.ensureSlot(
                slotLookupConnection(null), parameters(StreamingMode.ON, true, 10)),
                "目录行不存在的竞态兜底应复用而非失败");
    }
```

```java
    /**
     * 构造"建槽报 42710 + 目录查询返回脚本行"的假 Connection(R5 预检路径专用,JDK 动态
     * 代理,零第三方 mock)。关键步骤:prepareStatement 按 SQL 文本分流——含
     * pg_create_logical_replication_slot 的语句 executeQuery 抛 SQLState 42710(槽已存在),
     * 含 pg_replication_slots 的语句返回脚本 ResultSet。脚本语义由 slotTwoPhase 驱动:
     * Boolean.TRUE/FALSE → next()=true 且 getBoolean(1)=该值(槽存在、属性即该值);
     * null → next()=false(目录行不存在)。其余接口方法经 fallback 回落。
     *
     * @param slotTwoPhase 目录查询的脚本返回值;null 表示行不存在
     * @return 可直接交给 ReplicationSession.ensureSlot 静态接缝的假 Connection
     */
    private static Connection slotLookupConnection(Boolean slotTwoPhase) {
        ResultSet lookupResult = (ResultSet) Proxy.newProxyInstance(
                ReplicationSessionTest.class.getClassLoader(),
                new Class<?>[]{ ResultSet.class },
                (p, m, a) -> switch (m.getName()) {
                    case "next" -> slotTwoPhase != null;
                    case "getBoolean" -> Boolean.TRUE.equals(slotTwoPhase);
                    default -> fallback(m.getReturnType());
                });
        PreparedStatement creator = (PreparedStatement) Proxy.newProxyInstance(
                ReplicationSessionTest.class.getClassLoader(),
                new Class<?>[]{ PreparedStatement.class },
                (p, m, a) -> {
                    if (m.getName().equals("executeQuery")) {
                        throw new SQLException("fake duplicate_object", "42710");
                    }
                    return fallback(m.getReturnType());
                });
        PreparedStatement lookup = (PreparedStatement) Proxy.newProxyInstance(
                ReplicationSessionTest.class.getClassLoader(),
                new Class<?>[]{ PreparedStatement.class },
                (p, m, a) -> m.getName().equals("executeQuery") ? lookupResult : fallback(m.getReturnType()));
        return (Connection) Proxy.newProxyInstance(
                ReplicationSessionTest.class.getClassLoader(),
                new Class<?>[]{ Connection.class },
                (p, m, a) -> {
                    if (m.getName().equals("prepareStatement")) {
                        String sql = (String) a[0];
                        if (sql.contains("pg_create_logical_replication_slot")) {
                            return creator;
                        }
                        if (sql.contains("pg_replication_slots")) {
                            return lookup;
                        }
                    }
                    return fallback(m.getReturnType());
                });
    }
```

同步更新类 javadoc 中"ensureSlot 的 42710 复用语义"一句为"ensureSlot 的 SQL 契约 + R5 预检三分支"。

- [ ] **Step 2: 跑单测确认新用例失败（编译失败也算失败形态）**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=ReplicationSessionTest`
Expected: FAIL——`verifySlotTwoPhaseMatches` 尚不存在；且 42710 旧行为下 `ensureSlotRejectsExistingSlotWithMismatchedTwoPhase` 不会抛 IllegalStateException（不匹配仍静默复用）

- [ ] **Step 3: 实现 ensureSlot 预检分支**

`ReplicationSession.ensureSlot` 静态工作体的 catch 分支替换：

```java
        } catch (SQLException e) {
            if (SQLSTATE_DUPLICATE_OBJECT.equals(e.getSQLState())) {
                verifySlotTwoPhaseMatches(sqlConnection, config);
            } else {
                throw e;
            }
        }
```

新增静态方法（放在 ensureSlot 之后）：

```java
    /**
     * 责任:R5 存量槽 two_phase 预检——建槽撞 42710(槽已存在)时,查 pg_replication_slots
     * 的 two_phase 列与配置比对,不匹配即启动期拒绝(PG 不允许后改槽属性,留给 START_
     * REPLICATION 的服务端报错对用户不可读)。
     * 关键步骤:预编译目录查询(槽名绑定)→ 行不存在直接 WARN 复用(42710 与目录可见性
     * 之间的竞态兜底,极端情况留给 start 时服务端报错)→ 行存在则取 two_phase 比对:
     * 匹配 WARN 复用,不匹配抛 IllegalStateException——文案含槽名/槽现状/配置期望/DROP
     * SLOT 迁移指引(重插槽丢确认位点、重启后从更早位点重发,at-least-once 语义不变)。
     * 边界:SQLException 原样上抛(目录查询失败属基础设施异常,不走复用);IllegalStateException
     * 属运行时异常,经既有 fail-fast 路径任务失败、保留槽位。
     * 线程约束:与 open/start 同为装配方串行调用。
     *
     * @param sqlConnection 已建立的普通 SQL 连接(建槽同一连接复用)
     * @param config        含 slotName/twoPhase 的参数包
     * @throws SQLException 目录查询失败原样上抛
     */
    static void verifySlotTwoPhaseMatches(Connection sqlConnection, Parameters config) throws SQLException {
        try (PreparedStatement ps = sqlConnection.prepareStatement(
                "SELECT two_phase FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, config.slotName());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    LOG.warn("复制槽 {} 已存在但目录中不可见（罕见竞态），直接复用；属性不匹配时由 start 时服务端报错",
                            config.slotName());
                    return;
                }
                boolean slotTwoPhase = rs.getBoolean(1);
                if (slotTwoPhase != config.twoPhase()) {
                    throw new IllegalStateException("复制槽 " + config.slotName() + " 的 two_phase=" + slotTwoPhase
                            + " 与配置 two_phase=" + config.twoPhase()
                            + " 不匹配：PG 不允许后改槽属性，需 DROP SLOT 重建（会丢确认位点，"
                            + "重启后 PG 从更早位点重发未输出事务，at-least-once 语义不变）");
                }
                LOG.warn("复制槽 {} 已存在且 two_phase 匹配，直接复用", config.slotName());
            }
        }
    }
```

同步更新 `ensureSlot` 实例形态与静态工作体的 javadoc（删"否则 start 时由服务端报错"句，改为指向 `verifySlotTwoPhaseMatches` 的预检语义）。

- [ ] **Step 4: 跑单测确认全绿**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=ReplicationSessionTest`
Expected: PASS 全部用例

- [ ] **Step 5: Commit & push**

```bash
git add vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/ReplicationSession.java \
        vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/ReplicationSessionTest.java
git commit -m "feat(ms4): R5 存量槽 two_phase 客户端预检——42710 复用前查目录比对，不匹配启动期拒绝并附 DROP SLOT 迁移指引

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin worktree-ms35-logical-msg-guard
```

---

### Task 2: TwoPhaseIT 骨架 + 场景① prepareThenCommitPrepared

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java`

**Interfaces:**
- Consumes: `StreamITBase` 的 `baseConfig(slot, pub, pipeDir)`（默认 slot.streaming=parallel + two_phase=true，**本场景须覆盖 `slot.streaming=on`**）、`consumeRecordsUnchecked(n)`、`recordsForTopic(records, topic)`、`describe(records)`、`stopEngineAndDropSlot(slot)`；`StreamPgTestEnv.execSql/newSqlConnection/awaitWalsender/dropSlotQuietly`
- Produces: 类级夹具与 helper（`createFixture`/`prepareRows`/`insertSentinelRow`/`dataIds`/`rollbackPreparedQuietly`），Task 3-5 复用（同文件内直接调用）

- [ ] **Step 1: 写 TwoPhaseIT 完整骨架 + 场景① 测试方法**

```java
package org.vastdata.debezium.connector.postgresql.stream.it;

import io.debezium.config.Configuration;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS4 两阶段提交端到端验收(四场景):①PREPARE 挂起期零发射、COMMIT PREPARED 后全量落
 * Kafka(on 档);②ROLLBACK PREPARED 弃桶全程零发射(on 档);③流式大事务 PREPARE 以
 * StreamPrepare 收尾、COMMIT PREPARED 后 500 行全达(<b>parallel 档端到端验收</b>——
 * StreamPrepare 是 PARALLEL×two_phase 独有路径);④prepared 挂起期停机、重启重发
 * BeginPrepare..Prepare、COMMIT PREPARED 后补齐(offset 推进越过 CommitPrepared 位点)。
 *
 * <p>发射语义依据:组装器 {@code preparedByGid} 挂起池——PREPARE 后桶挂起零交付,
 * CommitPrepared 才交接回放(BEGIN+数据+END),RollbackPrepared 直接弃桶。挂起期"零发射"
 * 断言以哨兵事务证管道存活(仅凭等若干秒无记录是弱断言)。
 *
 * <p>夹具约定:表 t_2pc_ms4(id int PK + payload text)与 publication 预建;独立槽
 * {@code ms4_twophase} 前后清删;已知 gid 集 @BeforeEach/@AfterEach 双向 ROLLBACK PREPARED
 * 自愈(用例中途失败留下的未决 prepared 会持锁阻塞下一用例的 TRUNCATE 夹具)。管道目录
 * @TempDir 绝对路径。需要本机 Docker。
 */
class TwoPhaseIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms4_twophase";

    /** 两阶段验收数据表。 */
    private static final String TABLE = "t_2pc_ms4";

    /** publication 名(单表)。 */
    private static final String PUB = "pub_2pc_ms4";

    /** 数据记录 topic(DefaultTopicNamingStrategy)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 哨兵行 id 起始(prepared 事务数据行用 1..N 小 id 空间,哨兵用 900+ 隔离)。 */
    private static final int SENTINEL_ID_FROM = 901;

    /** 本类跨用例可能出现的全部 gid(夹具自愈与用例尾清理的统一清单)。 */
    private static final String[] ALL_GIDS = { "gid_c1", "gid_rb", "gid_big", "gid_restart" };

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir
    Path pipeDir;

    /**
     * 每用例前自愈:①清残留槽(上次异常退出留下的同名槽从旧 confirmed_flush_lsn 续传,
     * 静默吞掉建流前的写入使记录断言失真);②回滚残留未决 prepared(JVM 中途被杀时留下,
     * 持有的表锁会阻塞本用例夹具的 TRUNCATE)。均幂等。
     */
    @BeforeEach
    void cleanResidualSlotAndPrepared() {
        rollbackPreparedQuietly(ALL_GIDS);
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 每用例后清理:先回滚本用例可能残留的未决 prepared(中途断言失败的兜底),再先停引擎
     * 后删槽(次序见基类 {@link #stopEngineAndDropSlot})。
     */
    @AfterEach
    void rollbackPreparedAndDropSlot() {
        rollbackPreparedQuietly(ALL_GIDS);
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 夹具:建表、建 publication(单表)、清空历史数据。DDL/TRUNCATE 先于建槽执行——不产生
     * 解码输出,不污染记录计数;publication 预建是 start 的边界(无自动建,缺失即建流报错)。
     */
    private void createFixture() throws SQLException {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS " + PUB,
                "CREATE PUBLICATION " + PUB + " FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /**
     * 单事务插 rows 行后 PREPARE TRANSACTION(两阶段挂起构造):裸语句管理事务
     * (autocommit 连接上 BEGIN..PREPARE,引擎 TwoPhaseTransactionTest 同款——PREPARE
     * TRANSACTION 不能在 JDBC 显式事务内执行)。载荷 "前缀-id" 可预期。
     *
     * @param gid    两阶段事务名
     * @param idFrom 起始 id(含),逐行 +1
     * @param rows   行数
     * @param prefix 载荷前缀(实际载荷 "前缀-id")
     * @throws SQLException SQL 失败原样上抛
     */
    private static void prepareRows(String gid, int idFrom, int rows, String prefix) throws SQLException {
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            try (Statement st = c.createStatement()) {
                st.execute("BEGIN");
                for (int i = 0; i < rows; i++) {
                    st.execute("INSERT INTO " + TABLE + " VALUES (" + (idFrom + i) + ", '" + prefix + "-" + (idFrom + i) + "')");
                }
                st.execute("PREPARE TRANSACTION '" + gid + "'");
            }
        }
    }

    /**
     * 自动提交单行哨兵插入(挂起期"零发射"断言的管道存活证据):普通小事务,立即提交立即可消费。
     *
     * @param id 哨兵行 id(用 900+ 空间与 prepared 数据行隔离)
     * @throws SQLException SQL 失败原样上抛
     */
    private static void insertSentinelRow(int id) throws SQLException {
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (" + id + ", 'sentinel-" + id + "')");
    }

    /**
     * 静默回滚未决 prepared(夹具自愈):该 gid 已无未决事务(已 COMMIT/ROLLBACK PREPARED
     * 或从未创建)时 SQL 报 42704,吞掉属预期——"quietly" 习语,不打日志不失败。
     *
     * @param gids 待清理的 gid 清单
     */
    private static void rollbackPreparedQuietly(String... gids) {
        for (String gid : gids) {
            try {
                StreamPgTestEnv.execSql("ROLLBACK PREPARED '" + gid + "'");
            }
            catch (SQLException e) {
                // gid 无未决 prepared 事务——静默跳过
            }
        }
    }

    /**
     * 从数据 topic 记录提取 id 集(after 为 null 的防御跳过):零发射/全集断言的收集面。
     *
     * @param records 全部到达记录(跨轮次)
     * @return 出现过的 id 集(重复收敛为单值)
     */
    private static Set<Integer> dataIds(List<SourceRecord> records) {
        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : recordsForTopic(records, TOPIC)) {
            Struct after = ((Struct) r.value()).getStruct("after");
            if (after != null) {
                ids.add(after.getInt32("id"));
            }
        }
        return ids;
    }

    /**
     * 场景①(on 档):PREPARE 挂起期零发射、COMMIT PREPARED 后全量落 Kafka。关键步骤:
     * start(slot.streaming=on 覆盖 baseConfig 的 parallel 默认)→ PREPARE 3 行('gid_c1')
     * → 哨兵行 901 消费 3 条(BEGIN+1 数据+END,证明管道活着)且恰无 prepared 数据行
     * → COMMIT PREPARED → 消费 5 条(BEGIN+3 数据+END)→ 断言 ids 恰 {1,2,3}、payload
     * 全文相等、END event_count=3。
     * 边界:挂起期若凑不齐哨兵 3 条即 fail(管道死与零发射的区分是本场景的断言核心)。
     */
    @Test
    void prepareThenCommitPreparedEmitsOnlyAfterCommitPrepared() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, PUB, pipeDir).with("slot.streaming", "on").build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        prepareRows("gid_c1", 1, 3, "c1");
        insertSentinelRow(SENTINEL_ID_FROM);
        List<SourceRecord> duringPending = consumeRecordsUnchecked(3);
        assertEquals(3, duringPending.size(), "挂起期哨兵事务 BEGIN+1 数据+END 应到达: " + describe(duringPending));
        assertEquals(Set.of(SENTINEL_ID_FROM), dataIds(duringPending),
                "PREPARE 挂起期应零发射(prepared 数据行不得早于 COMMIT PREPARED 出现)");

        StreamPgTestEnv.execSql("COMMIT PREPARED 'gid_c1'");
        List<SourceRecord> emitted = consumeRecordsUnchecked(5);
        assertEquals(5, emitted.size(), "COMMIT PREPARED 后 BEGIN+3 数据+END 共 5 条应到达: " + describe(emitted));
        assertEquals(Set.of(1, 2, 3), dataIds(emitted), "prepared 事务 3 行应全量到达");
        for (SourceRecord r : recordsForTopic(emitted, TOPIC)) {
            Struct after = ((Struct) r.value()).getStruct("after");
            assertEquals("c1-" + after.getInt32("id"), after.getString("payload"), "payload 应全文相等");
        }
        assertTrue(hasEndWithEventCount(emitted, 3), "事务 topic 应有 event_count=3 的 END: " + describe(emitted));
    }

    /**
     * 事务 topic 是否存在指定 event_count 的 END 记录(完整事务边界信号;其他事务的 END
     * 不影响命中)。status 字段缺失的记录(理论不出现)跳过。
     *
     * @param records    全部到达记录
     * @param eventCount 期望的 END 事件计数(数据记录数)
     * @return 存在为 true
     */
    private static boolean hasEndWithEventCount(List<SourceRecord> records, long eventCount) {
        for (SourceRecord r : recordsForTopic(records, TX_TOPIC)) {
            Struct value = (Struct) r.value();
            if ("END".equals(value.getString("status"))
                    && Long.valueOf(eventCount).equals(value.getInt64("event_count"))) {
                return true;
            }
        }
        return false;
    }
}
```

- [ ] **Step 2: 跑场景① 验证通过**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=TwoPhaseIT`
Expected: PASS（需 Docker）。若挂起期出现 prepared 行或 COMMIT PREPARED 后记录缺失，为真实缺陷——用 `describe()` 输出定位（挂起池语义见 `StreamedTransactionAssembler.preparedByGid`），修复后重跑

- [ ] **Step 3: Commit & push**

```bash
git add vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java
git commit -m "test(ms4): 两阶段 IT 场景①——PREPARE 挂起期零发射(哨兵证活)、COMMIT PREPARED 后全量落 Kafka

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin worktree-ms35-logical-msg-guard
```

---

### Task 3: 场景② ROLLBACK PREPARED 弃桶

**Files:**
- Modify: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java`（类内新增一个 @Test）

**Interfaces:**
- Consumes: Task 2 的全部夹具/helper（同文件直接调用）

- [ ] **Step 1: 写场景② 测试方法**

```java
    /**
     * 场景②(on 档):ROLLBACK PREPARED 弃桶全程零发射。关键步骤:start(on 档)→ PREPARE
     * 2 行('gid_rb')→ 哨兵 902 消费 3 条且恰无 prepared 行 → ROLLBACK PREPARED → 哨兵
     * 903 消费 3 条 → 断言两轮合计数据 ids 恰 {902,903}(弃桶后也无补发)且不存在
     * event_count=2 的 END(RollbackPrepared 不驱动事务块)。
     * 边界:第二轮哨兵证"回滚处理后管道仍活",零发射贯穿挂起期与回滚后两段。
     */
    @Test
    void rollbackPreparedDiscardsBucketWithZeroEmission() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, PUB, pipeDir).with("slot.streaming", "on").build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        prepareRows("gid_rb", 1, 2, "rb");
        insertSentinelRow(902);
        List<SourceRecord> first = consumeRecordsUnchecked(3);
        assertEquals(Set.of(902), dataIds(first), "挂起期应仅哨兵行到达");

        StreamPgTestEnv.execSql("ROLLBACK PREPARED 'gid_rb'");
        insertSentinelRow(903);
        List<SourceRecord> second = consumeRecordsUnchecked(3);
        assertEquals(3, second.size(), "回滚后哨兵事务应照常到达(管道存活): " + describe(second));

        List<SourceRecord> all = new java.util.ArrayList<>(first);
        all.addAll(second);
        assertEquals(Set.of(902, 903), dataIds(all), "弃桶语义:prepared 两行全程零发射(挂起期与回滚后均无)");
        assertTrue(!hasEndWithEventCount(all, 2), "RollbackPrepared 弃桶不得产生 event_count=2 的事务 END");
    }
```

- [ ] **Step 2: 跑场景② 验证通过**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=TwoPhaseIT`
Expected: 两个场景全 PASS

- [ ] **Step 3: Commit & push**

```bash
git add vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java
git commit -m "test(ms4): 两阶段 IT 场景②——ROLLBACK PREPARED 弃桶全程零发射(挂起期与回滚后双段哨兵证活)

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin worktree-ms35-logical-msg-guard
```

---

### Task 4: 场景③ 流式大事务 StreamPrepare（parallel 档端到端验收）

**Files:**
- Modify: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java`（类内新增一个 @Test）

**Interfaces:**
- Consumes: Task 2 夹具/helper；`baseConfig` 的默认 `slot.streaming=parallel`（**本场景不覆盖**，正是 parallel 验收）

- [ ] **Step 1: 写场景③ 测试方法**

```java
    /**
     * 场景③(<b>parallel 档端到端验收</b>):流式大事务 PREPARE 以 StreamPrepare 收尾、
     * COMMIT PREPARED 后 500 行全达。关键步骤:start(baseConfig 默认 parallel + two_phase,
     * 不覆盖——StreamPrepare 是 PARALLEL×two_phase 独有路径,引擎 TwoPhaseTransactionTest
     * 场景三的翻译)→ 单事务 500 行×repeat('z',4096)(2MB,远超 64kB work_mem 必触发流式)
     * PREPARE('gid_big')→ 立即 COMMIT PREPARED → 消费 502 条(BEGIN+500 数据+END)→
     * 断言数据 ids 恰 1000..1499、END event_count=500。
     * 边界:载荷可压缩但 2MB 总量靠 rb->size 全局记账必触发流式驱逐(引擎同款方案实测);
     * 断言面是黑盒记录(StreamPrepare 消息路径由全量到达间接验收),不逐字节验载荷。
     */
    @Test
    void largePreparedTransactionStreamsUnderParallelAndEmitsOnCommitPrepared() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, PUB, pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            try (Statement st = c.createStatement()) {
                st.execute("BEGIN");
                for (int i = 0; i < 500; i++) {
                    st.execute("INSERT INTO " + TABLE + " VALUES (" + (1000 + i) + ", repeat('z', 4096))");
                }
                st.execute("PREPARE TRANSACTION 'gid_big'");
                st.execute("COMMIT PREPARED 'gid_big'");
            }
        }

        List<SourceRecord> all = consumeRecordsUnchecked(502);
        assertEquals(502, all.size(), "BEGIN+500 数据+END 共 502 条应到达: " + describe(all));
        Set<Integer> expected = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            expected.add(1000 + i);
        }
        assertEquals(expected, dataIds(all), "流式两阶段事务 500 行应全量落 Kafka(parallel 档端到端)");
        assertTrue(hasEndWithEventCount(all, 500), "事务 topic 应有 event_count=500 的 END: " + describe(all));
    }
```

注意 `describe` 对 500 条记录的输出量——若断言失败消息过长，可在断言消息里只传 `all.size()` 概要（断言本身保留全集比较）。

- [ ] **Step 2: 跑场景③ 验证通过**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=TwoPhaseIT`
Expected: 三场景全 PASS。500×4KB 载荷消费耗时约 10-30s 属正常

- [ ] **Step 3: Commit & push**

```bash
git add vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java
git commit -m "test(ms4): 两阶段 IT 场景③——parallel 档流式大事务 StreamPrepare 收尾,COMMIT PREPARED 后 500 行全量落 Kafka

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin worktree-ms35-logical-msg-guard
```

---

### Task 5: 场景④ prepared 挂起期停机重启续传

**Files:**
- Modify: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java`（类内新增一个 @Test）

**Interfaces:**
- Consumes: Task 2 夹具/helper；`StreamPgTestEnv.confirmedFlushLsn(slot)`；基座 offset 文件单用例内跨 start/stop 保留的性质（`StreamITBase` 类 javadoc）；Awaitility `await`（模块已用，见 `RestartSemanticsIT` import 形态）

- [ ] **Step 1: 写场景④ 测试方法（import 补 `org.awaitility.Awaitility.await`、`java.time.Duration`、`java.util.ArrayList`）**

```java
    /**
     * 场景④:prepared 挂起期停机 → 重启重发 BeginPrepare..Prepare → COMMIT PREPARED 后
     * 补齐(offset 推进越过 CommitPrepared 位点)。关键步骤:start(on 档)→ 暖场哨兵 904
     * 消费 3 条取边界 LSN → PREPARE 3 行('gid_restart')→ 不变量锚点:confirmed_flush ≤
     * 暖场边界(CommitPrepared 未达 → 前沿钉在暖场 End → 服务端采纳不可能越过——重启重发
     * 区间必然覆盖整个 prepared 事务,确定性非时序)→ stopConnector → 停机期排空断言零
     * 残留 → 同一 offset 文件重启 → COMMIT PREPARED → await 轮询:ids 覆盖 {1,2,3}(暖场
     * 行允许重发重复,并集口径)、END event_count=3。
     * 边界:重发到达总数不确定(暖场事务是否重发取决于停机前服务端采纳进度),用非阻塞
     * 排空轮询而非按数消费;preparedByGid 按 gid 匹配幂等吸收重发的 BeginPrepare..Prepare。
     */
    @Test
    void preparedTxSurvivesRestartAndEmitsOnCommitPrepared() throws Exception {
        createFixture();
        var config = baseConfig(SLOT, PUB, pipeDir).with("slot.streaming", "on").build();
        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        insertSentinelRow(904);
        List<SourceRecord> warm = consumeRecordsUnchecked(3);
        assertEquals(Set.of(904), dataIds(warm), "暖场哨兵应到达并给前沿一个已推进锚点");
        long warmBoundary = ((Number) recordsForTopic(warm, TOPIC).get(0)
                .sourceOffset().get("lsn_commit")).longValue();

        prepareRows("gid_restart", 1, 3, "restart");
        assertTrue(StreamPgTestEnv.confirmedFlushLsn(SLOT) <= warmBoundary,
                "prepared 挂起期 confirmed_flush 应被前沿封顶钉在暖场边界之内"
                        + "(CommitPrepared 未达前沿不推进,重启重发区间必然覆盖整个 prepared 事务)");

        stopConnector();
        List<SourceRecord> residual = new java.util.ArrayList<>();
        drainArrivedRecords(residual);
        assertTrue(residual.isEmpty(), "停机期排空应零残留(prepared 桶未交接,零发射): " + describe(residual));

        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        StreamPgTestEnv.execSql("COMMIT PREPARED 'gid_restart'");

        List<SourceRecord> seen = new java.util.ArrayList<>();
        await("重启后 prepared 事务经 COMMIT PREPARED 补齐")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    drainArrivedRecords(seen);
                    Set<Integer> union = dataIds(seen);
                    union.add(904); // 暖场行:停机前已见,允许(不强制)由重发补充
                    assertEquals(Set.of(1, 2, 3, 904), union,
                            "prepared 3 行必达(重启重发+COMMIT PREPARED 补齐),暖场行允许重发: " + describe(seen));
                    assertTrue(hasEndWithEventCount(seen, 3),
                            "prepared 事务应有 END 到达且 event_count=3: " + describe(seen));
                });
    }
```

- [ ] **Step 2: 跑场景④ 验证通过**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=TwoPhaseIT`
Expected: 四场景全 PASS

- [ ] **Step 3: Commit & push**

```bash
git add vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/TwoPhaseIT.java
git commit -m "test(ms4): 两阶段 IT 场景④——prepared 挂起期停机重启续传,前沿封顶锚点+COMMIT PREPARED 后整事务补齐

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin worktree-ms35-logical-msg-guard
```

---

### Task 6: 全量回归 + R5 真库 IT + 文档记档

**Files:**
- Create: `vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/SlotTwoPhaseMismatchIT.java`
- Modify: `CLAUDE.md`（根，connector 源码结构段与 IT 清单）；`vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/CLAUDE.md`（若列有 IT 清单，grep `IT` 确认）

**Interfaces:**
- Consumes: Task 1 的预检行为（真库端到端验证）；`StreamITBase` 基座

- [ ] **Step 1: 写 R5 真库 IT（预检的启动期拒绝面——单测假件覆盖不到真 42710 与目录行）**

```java
package org.vastdata.debezium.connector.postgresql.stream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.nio.file.Path;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * MS4 R5 真库验收:存量槽 two_phase 属性与配置不匹配时启动期拒绝(带 DROP SLOT 迁移
 * 指引)。单测(ReplicationSessionTest 的目录脚本假件)锚定分支语义,本类补真库面——
 * 真 42710 路径 + pg_replication_slots 真目录行。场景:先以 two_phase=false 建槽(模拟
 * MS3 之前的存量槽)→ 以 two_phase=true 配置启动引擎 → 启动失败且异常链文案含槽名与
 * DROP SLOT 指引 → 槽未被删(拒绝不得有副作用)。需要本机 Docker。
 */
class SlotTwoPhaseMismatchIT extends StreamITBase {

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "ms4_slot_mismatch";

    /** 每用例独立的管道目录。 */
    @TempDir
    Path pipeDir;

    /** 每用例前清残留槽(幂等)。 */
    @BeforeEach
    void cleanResidualSlot() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /** 每用例后清理:先停引擎(启动失败时为幂等 no-op)再删槽。 */
    @AfterEach
    void dropSlot() {
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 场景:two_phase=false 存量槽 × two_phase=true 配置 → 启动期拒绝。关键步骤:SQL
     * 直接建 two_phase=false 槽(第 4 参 false)→ 以 baseConfig(two_phase=true 默认)
     * start → 断言 CompletionCallback 收到的异常链文案含槽名与 DROP SLOT(经
     * engine 启动路径异步上报,轮询记录消费线程的失败信号);失败后槽仍在(拒绝零副作用)。
     * 边界:启动失败必须发生(60s 内无失败信号即 fail——静默复用是 R5 要修的 bug 形态)。
     */
    @Test
    void mismatchedExistingSlotFailsStartupWithMigrationHint() throws Exception {
        StreamPgTestEnv.execSql(
                "SELECT pg_create_logical_replication_slot('" + SLOT + "', 'pgoutput', false, false)");
        try {
            start(PostgresStreamConnector.class,
                    baseConfig(SLOT, "pub_2pc_ms4", pipeDir).with("slot.streaming", "on").build());
            // start 返回后引擎异步跑任务——预检异常经 CompletionCallback 上报;轮询消费
            // 队列不可用(无记录),以引擎停止 + 异常链断言。Awaitility 轮询引擎的停止信号:
            awaitEngineFailureContaining(SLOT + "' 的 two_phase=false");
            fail("two_phase 不匹配的存量槽必须启动失败(静默复用是 R5 修复前的 bug 形态)");
        }
        catch (AssertionError expected) {
            // awaitEngineFailureContaining 命中后 fail() 抛出的 AssertionError 即预期出口
        }
        assertTrue(slotExists(SLOT), "启动拒绝不得有删槽副作用");
    }
```

写法说明（执行者按基座实际能力落地，不强求与上面草稿逐字一致）：`AbstractAsyncEngineConnectorTest.start(...)` 的失败上报面——若基座提供 `engineException`/CompletionCallback 记录（查阅 `AbstractAsyncEngineConnectorTest` 源码的失败断言方法，如 `waitForEngineToStop()` + 异常捕获），用其原生方法替代 `awaitEngineFailureContaining` 草稿；断言核心只有两条：**①启动失败发生（不得静默复用）；②异常文案含槽名与 DROP SLOT**。`slotExists(slot)` 用 `SELECT count(*) FROM pg_replication_slots WHERE slot_name=?` 实现（`StreamPgTestEnv.newSqlConnection` 直查）。若该 IT 因基座失败信号不可达而无法稳定断言（异步异常吞掉），降级方案：直接调 `ReplicationSession`（open + ensureSlot 对真库）断言 IllegalStateException——同样覆盖真 42710 + 真目录行，测试形态更朴素但断言等价；在测试 javadoc 里注明降级原因。

- [ ] **Step 2: 跑 R5 真库 IT 验证通过**

Run: `mvn test -pl vb-stream-connector-postgres-stream -Dtest=SlotTwoPhaseMismatchIT`
Expected: PASS

- [ ] **Step 3: 全模块回归**

Run: `mvn test -pl vb-stream-connector-postgres-stream`
Expected: 全绿；记录总用例数（存量 218 + 本计划新增 ≈ 7，以实际输出为准）

- [ ] **Step 4: CLAUDE.md 记档**

根 `CLAUDE.md` 两处：
1. connector 源码结构段（"**MS3.5 已落…**"句后）追加 MS4 句，形态参照既有里程碑句式：
   > **MS4 已落 two_phase 语义闭环**——两阶段 IT 四场景（挂起期零发射/弃桶/parallel 档 StreamPrepare 大事务/挂起期停机重启续传）+ R5 存量槽预检（42710 复用前查 `pg_replication_slots.two_phase`，不匹配启动期拒绝附 DROP SLOT 迁移指引）
2. IT 清单"`LogicalMsgIT` 逻辑消息三场景（…）"之后插入：
   > `TwoPhaseIT` 两阶段四场景（PREPARE 挂起零发射/ROLLBACK PREPARED 弃桶/parallel 档 StreamPrepare 大事务全量落 Kafka/prepared 挂起期停机重启续传）、`SlotTwoPhaseMismatchIT` 存量槽 two_phase 不匹配启动期拒绝
3. 用例计数"218 用例含 `it` 包 16 用例"按 Step 3 实际数字更新

模块 `stream/CLAUDE.md` 若列 IT 清单同步补（先 `grep -n "IT" 该文件` 确认）。

- [ ] **Step 5: Commit & push**

```bash
git add CLAUDE.md vb-stream-connector-postgres-stream/src/main/java/org/vastdata/debezium/connector/postgresql/stream/CLAUDE.md \
        vb-stream-connector-postgres-stream/src/test/java/org/vastdata/debezium/connector/postgresql/stream/it/SlotTwoPhaseMismatchIT.java
git commit -m "test(ms4)+docs: R5 真库验收(不匹配存量槽启动期拒绝零副作用)+ MS4 收官记档

Co-Authored-By: Claude <noreply@anthropic.com>"
git push origin worktree-ms35-logical-msg-guard
```

---

## 自审记录

- **Spec 覆盖**：§2 R5 预检 → Task 1（单测）+ Task 6 Step 1（真库 IT）；§3 四场景 → Task 2-5；§4 边界（前沿封顶锚点、gid 幂等吸收、prepared 清理自愈）→ Task 2 夹具 + Task 5；§5 验收 → Task 6
- **占位符**：Task 6 Step 1 的 `awaitEngineFailureContaining` 是待执行者按基座实际失败断言面落地的草稿名，附了明确的降级路径与断言核心——不算 TBD（两条断言①失败发生②文案含 DROP SLOT 是硬要求）
- **类型一致性**：`verifySlotTwoPhaseMatches(Connection, Parameters)` 在 Task 1 定义与调用一致；`slotLookupConnection(Boolean)`、`prepareRows(gid, idFrom, rows, prefix)`、`dataIds(List<SourceRecord>)`、`hasEndWithEventCount(List, long)` 各任务引用签名一致
