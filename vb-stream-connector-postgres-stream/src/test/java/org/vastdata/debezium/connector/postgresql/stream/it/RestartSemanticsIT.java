package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS3 重启三情况验收(spec §5.6 重启三情况 + D7 半事务停机语义)——同一 stop→restart
 * 动作在"停机时流式状态 × 服务端确认位点进度"两轴上的三种形态:
 * <ol>
 *   <li><b>半事务停机(D7 主验收)</b>:流式大事务进行中(部分 Stream 段已下发、事务未
 *       COMMIT)不排干停机({@code stopConnector()} → 任务 doStop → 流式源 stopStreaming:
 *       session.close → reader.join → assembler.shutdownFast,不排干 CQ)→ 停机时该事务
 *       <b>零记录</b>(本连接器回放只在提交交接点发生——BEGIN 也不会提前发射,残段形态比
 *       "BEGIN 已见 END 未达"更弱污染,下游零过滤负担)→ 重启(同一 offset 文件)→ PG 重发
 *       <b>整事务</b>(BEGIN+全行+END 最终全达)。机制依据(①前沿封顶):End 未达 → 输出前沿
 *       钉在前一事务边界 → 客户端确认经 min(已收到,前沿) 封顶 → 槽 confirmed_flush 钉住
 *       → 重启重放区间必然覆盖整个半事务 WAL。确定性手段:单连接 JDBC 由测试侧控制"先
 *       stop 再 COMMIT"(不在 engine 里竞态);停机前断言 confirmed_flush ≤ 前一已输出
 *       事务的边界 LSN(前沿封顶的不变量,非时序碰运气)。</li>
 *   <li><b>重发窗口(停机时确认位点落后于已输出前沿)</b>:已提交事务输出完毕后<b>立即</b>
 *       停机 → 停机窗口再写一个已提交事务(槽保留 WAL)→ 重启 → 断言<b>并集语义</b>:新
 *       事务必达、停机前已见事务允许重复(at-least-once 文档化口径,Set 断言不去重)。
 *       机制依据(②重启锚点的滞后):本连接器不复刻 vanilla 的 WalPositionLocator 与
 *       Connect offset LSN 搜索——重启时 START_REPLICATION 从 INVALID_LSN 起,服务端按
 *       槽 confirmed_flush 续发;而 confirmed_flush 的服务端采纳(candidate 机制)只在
 *       解码推进时发生——停机前若输出后无后续 WAL 活动,confirmed_flush 冻结在旧值
 *       (客户端确认不丢失但空闲期不落库,Diag 实证的行为),重放起点落在已输出事务之前
 *       → 重发窗口。重复是否实际发生取决于停机前最后一刻的服务端采纳进度(candidate
 *       行为非本连接器可控),故只断言并集不断言重复计数;实际观察到的重复经日志留痕。
 *       Connect offset 文件在本测试基座下不构成滞后源:embedded engine 停机时 commit
 *       offsets 落盘(AsyncEmbeddedEngine.stopSourceTasks),文件恢复的是 offsetContext
 *       元数据而非流式起点。</li>
 *   <li><b>无缝续传(常态)</b>:停机前<b>驱动服务端采纳</b>——在非 publication 表上写
 *       WAL 触发解码推进,轮询直到 confirmed_flush ≥ 已输出事务边界(candidate 机制:下次
 *       WAL 活动使 confirmed_flush 一步跳到客户端已确认的前沿)→ 停机 → 停机窗口写新
 *       事务 → 重启 → 断言<b>新事务到达且旧事务零重发</b>(硬断言,机制保证):confirmed_flush
 *       已越过旧事务提交记录 → 服务端重放起点在其后 → 旧事务对解码器不可见,非时序侥幸。
 *       机制依据(③confirmed_flush 已推进):重放起点 = 槽确认位点 > 停机点前的最后输出
 *       事务。</li>
 * </ol>
 *
 * <p><b>三情况的分界本质</b>:重放起点(服务端 confirmed_flush)与已输出前沿(客户端
 * 确认值)之间的距离——半事务时前沿被 End 锚定钉死在前一事务(距离 ≥ 一个整事务);停机
 * 即断时服务端未采纳(距离 = 已输出但未采纳的事务);常态停机前服务端已采纳(距离 = 0)。
 * 前两者 PG 重发已见内容(at-least-once 头部/整事务重复,下游按事务元数据幂等收敛),
 * 第三者无缝。
 *
 * <p>夹具约定:数据表(id int PK + payload text)与 WAL 触发表(不在 publication,写入产
 * WAL 被解码但不产记录)由 IT 预建;publication 单表;独立槽 {@code ms3_restart_sem}
 * 前后清删;管道目录 @TempDir 绝对路径(重启时 wipe-on-open 自清,重启前在途半事务数据
 * 弃桶属 D7 预期)。需要本机 Docker。与 MS2 {@code EndToEndStreamedTxIT} 场景③的关系:
 * 彼处是"停机窗口写入后重启,并集到达"的单角冒烟,本类补齐三情况形态——尤其①的
 * 停机期断言(半事务零发射)与③的无重复硬断言(服务端采纳驱动的确定性)。
 */
class RestartSemanticsIT extends StreamITBase {

    private static final Logger LOG = LoggerFactory.getLogger(RestartSemanticsIT.class);

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms3_restart_sem";

    /** 数据表名(publication 单表,三情况的数据记录断言面)。 */
    private static final String TABLE = "t_restart_sem";

    /** WAL 触发表名(刻意不在 publication:写入产 WAL 推进解码/candidate 采纳,但不产记录)。 */
    private static final String SCRATCH = "t_restart_scratch";

    /** 数据记录 topic(DefaultTopicNamingStrategy:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 场景①流式大事务的起始 id(101..106,与暖场/停机窗口写入的小事务 id 空间隔离)。 */
    private static final int BIG_ID_FROM = 101;

    /** 场景①流式大事务行数:6 行×16KB 不可压缩载荷,行间 sleep——进行中必然多次越过 64kB 驱逐阈值。 */
    private static final int BIG_ROWS = 6;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open——重启即弃 D7 半事务在途桶)。 */
    @TempDir
    Path pipeDir;

    /** 场景③驱动 candidate 采纳时的 WAL 触发表自增序号(测试线程独占,单用例内递增)。 */
    private int scratchSeq = 0;

    /**
     * 每用例前清残留槽:上次异常退出留下的同名槽会从旧 confirmed_flush_lsn 续传,
     * 静默吞掉建流前的写入使记录断言失真。幂等。
     */
    @BeforeEach
    void cleanResidualSlot() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /** 每用例后清理:先停引擎再删槽(次序见基类 {@link #stopEngineAndDropSlot})。 */
    @AfterEach
    void dropSlot() {
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 夹具:建数据表与 WAL 触发表(均 id int PK + payload text,后者不入 publication)、
     * 建 publication(单表)并清空两表。DDL/TRUNCATE 先于建槽执行——不产生解码输出,
     * 不污染记录计数;publication 预建是 start 的边界(无自动建,缺失即建流报错)。
     */
    private void createFixture() throws SQLException {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, payload text)",
                "CREATE TABLE IF NOT EXISTS " + SCRATCH + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_restart",
                "CREATE PUBLICATION pub_restart FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE,
                "TRUNCATE " + SCRATCH);
    }

    /**
     * 场景①(半事务停机,D7 主验收):流式大事务进行中不排干停机 → 停机期零发射 →
     * COMMIT → 重启(同一 offset 文件)→ 整事务(BEGIN+全行+END)最终全达。
     * 关键步骤:start → 暖场小事务(id=1)输出并恰好消费(BEGIN+1 数据+END 三条,证明管线
     * 活着并给前沿一个已推进的锚点)→ 单连接开大事务逐行插 6×16KB(行间 sleep 250ms,
     * 进行中必然流式下发)→ 停机前锚点断言:confirmed_flush ≤ 暖场事务边界(前沿封顶
     * 不变量:End 未达 → 前沿钉在暖场 End → 客户端确认封顶 → 服务端确认不可能越过;
     * 非时序碰运气)→ sleep 1s 让最后的流段抵达 reader → stopConnector(D7 路径,不排干,
     * 在途桶随下次引擎启动 wipe-on-open 弃掉)→ 停机期断言:排空消费队列零残留(本连接器
     * 回放只在提交交接点发生,BEGIN 也不会提前发射——残段形态零记录)→ 引擎已停时 COMMIT
     * 大事务(测试侧单连接,顺序确定)→ 同一 offset 文件重启 → await 轮询排空累计:大事务
     * id 并集覆盖全部 6 行(暖场行允许重发重复)、payload 全文相等、事务 topic 存在
     * event_count=6 的 END。
     * 边界:停机期断言在 stopConnector 返回后做——引擎停稳后消费队列冻结,排空即终态;
     * 重启后到达总数不确定(暖场事务是否重发取决于停机前服务端采纳进度),故用非阻塞排空
     * 轮询而非按数消费。
     */
    @Test
    void halfOpenTxStopBeforeCommitYieldsNoEmissionAndFullResendAfterRestart() throws Exception {
        createFixture();
        var config = baseConfig(SLOT, "pub_restart", pipeDir).build();
        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        // 暖场小事务:给输出前沿一个已推进锚点(其边界 LSN 后续作 confirmed_flush 封顶断言)
        Map<Integer, String> expected = new HashMap<>();
        expected.putAll(insertCommittedRows(1, 1, "warm"));
        List<SourceRecord> warm = consumeRecordsUnchecked(3);
        List<SourceRecord> warmData = recordsForTopic(warm, TOPIC);
        assertEquals(Set.of(1), dataIds(warmData), "暖场事务应恰一条数据记录到达(BEGIN+数据+END 三条已消费)");
        long warmBoundary = commitLsnOf(warmData);

        Random rnd = new Random();
        try (Connection tx = StreamPgTestEnv.newSqlConnection()) {
            tx.setAutoCommit(false);
            try (PreparedStatement ps = tx.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, ?)")) {
                for (int i = 0; i < BIG_ROWS; i++) {
                    int id = BIG_ID_FROM + i;
                    String payload = StreamPgTestEnv.incompressiblePayload(rnd);
                    expected.put(id, payload);
                    ps.setInt(1, id);
                    ps.setString(2, payload);
                    ps.executeUpdate();
                    Thread.sleep(250); // 跨秒分批给服务端驱逐窗口:进行中流式下发的构造前提
                }
            }
            // 前沿封顶不变量锚点:大事务 End 未达 → 前沿钉在暖场边界 → 服务端 confirmed_flush
            // (=客户端确认=min(已收到,前沿) 的采纳值)不可能越过暖场边界——确定性,非时序
            assertTrue(StreamPgTestEnv.confirmedFlushLsn(SLOT) <= warmBoundary,
                    "半事务进行中 confirmed_flush 应被前沿封顶钉在暖场事务边界之内"
                            + "(End 未达前沿不推进,重启重放区间必然覆盖整个半事务)");
            Thread.sleep(1000); // 让最后的流段抵达 reader(断言不依赖,贴近真实 D7 停机形态)

            stopConnector(); // D7:不排干——半事务在途桶不交付,已收流段弃掉

            // 停机期断言:引擎已停,消费队列冻结,排空即终态——半事务零发射(数据/BEGIN/END 均无)
            List<SourceRecord> residual = new ArrayList<>();
            drainArrivedRecords(residual);
            assertTrue(residual.isEmpty(),
                    "半事务停机时残段形态应为零记录(回放只在提交交接点发生,BEGIN 也不提前发射;"
                            + "下游可按 BEGIN-无-END 过滤的更强形态是本连接器常态): " + describe(residual));

            tx.commit(); // 引擎已停,COMMIT 顺序确定——"先 stop 再 COMMIT"消除竞态
        }

        // 同一 offset 文件重启(基座只在 @BeforeEach 删 offset 文件,单用例内跨 start/stop 保留)
        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        // 整事务最终全达:重复头行允许(暖场行可能随重放窗口重发,at-least-once 文档口径,Set 并集)
        List<SourceRecord> seen = new ArrayList<>();
        await("半事务停机重启:整事务(BEGIN+全行+END)最终完整到达")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    drainArrivedRecords(seen);
                    Set<Integer> ids = collectValidatedDataIds(seen, expected);
                    Set<Integer> union = new HashSet<>(ids);
                    union.add(1); // 暖场行:停机前已见,允许(不强制)在并集中由重发补充
                    assertEquals(expected.keySet(), union,
                            "重启后并集应覆盖暖场行 + 大事务全部 6 行(暖场重发允许,大事务必达): " + describe(seen));
                    assertTrue(hasEndWithEventCount(seen, BIG_ROWS),
                            "大事务应有 END 到达且 event_count=" + BIG_ROWS + "(完整事务边界补齐): " + describe(seen));
                });
    }

    /**
     * 场景②(重发窗口,并集语义):已提交事务输出后立即停机(服务端 confirmed_flush 大概率
     * 未采纳到前沿——空闲期不落库)→ 停机窗口写入新已提交事务 → 重启 → 断言并集:新事务
     * 必达、停机前已见事务允许重复。
     * 关键步骤:start → 单事务 3 行(id 1..3)提交并恰好消费 5 条(BEGIN+3 数据+END,记录
     * 停机前已见集与停机时 confirmed_flush 供诊断)→ 立即 stopConnector(不给服务端
     * candidate 采纳留 WAL 活动)→ 停机窗口写 2 行(id 4,5,槽保留 WAL)→ 同一 offset 文件
     * 重启 → await 轮询排空累计:已见集 ∪ 重启后到达集 = 全部 5 行,payload 全文相等。
     * 边界:重复数不确定(取决于停机前最后一刻服务端采纳进度,candidate 行为非连接器可控),
     * 故只断言并集不断言计数;观察到的重复经 INFO 日志留痕(非断言面)。
     */
    @Test
    void immediateStopAfterOutputRedeliversAtLeastOnceWithUnionSemantics() throws Exception {
        createFixture();
        var config = baseConfig(SLOT, "pub_restart", pipeDir).build();
        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        Map<Integer, String> expected = new LinkedHashMap<>(insertCommittedRows(1, 3, "lag"));
        List<SourceRecord> first = consumeRecordsUnchecked(5);
        Set<Integer> preStopIds = dataIds(recordsForTopic(first, TOPIC));
        assertEquals(Set.of(1, 2, 3), preStopIds, "停机前应先消费到首批 3 行(重启基线)");
        long confirmedAtStop = StreamPgTestEnv.confirmedFlushLsn(SLOT);

        stopConnector(); // 输出后立即停机:confirmed_flush 采纳大概率未跟上前沿(重发窗口的成因)

        expected.putAll(insertCommittedRows(4, 2, "lag")); // 停机窗口写入,槽保留 WAL

        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        List<SourceRecord> seen = new ArrayList<>();
        await("重启后并集覆盖停机窗口新事务(旧事务重复允许)")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    drainArrivedRecords(seen);
                    Set<Integer> postRestartIds = collectValidatedDataIds(seen, expected);
                    Set<Integer> union = new HashSet<>(preStopIds);
                    union.addAll(postRestartIds);
                    assertEquals(expected.keySet(), union,
                            "并集语义:停机窗口新事务(id 4,5)必达,停机前已见事务(1..3)允许重发重复: " + describe(seen));
                });
        Set<Integer> redelivered = new HashSet<>(collectValidatedDataIds(seen, expected));
        redelivered.retainAll(preStopIds);
        LOG.info("场景②观察:停机时 confirmed_flush={}, 停机前已见事务重发 id 集={}(at-least-once 头部重复,"
                + "是否发生取决于停机前服务端采纳进度,非断言面)", confirmedAtStop, redelivered);
    }

    /**
     * 场景③(无缝续传,常态):停机前驱动服务端采纳 confirmed_flush 到已输出前沿 → 停机 →
     * 停机窗口写新事务 → 重启 → 断言新事务到达且旧事务<b>零重发</b>(硬断言,机制保证)。
     * 关键步骤:start → 3 行事务(id 1..3)提交并消费 5 条,取其边界 LSN(lsn_commit)→
     * 驱动采纳:循环在非 publication 触发表写一行(产 WAL 被解码推进 candidate)并轮询
     * confirmed_flush ≥ 边界(candidate 机制:下次 WAL 活动使 confirmed_flush 一步跳到
     * 客户端已确认前沿;30s 内未达即 fail-fast——该机制是引擎 FrontierCapTest 已实证的
     * 服务端行为,不达属环境异常)→ stopConnector → 停机窗口写 2 行(id 4,5)→ 同一
     * offset 文件重启 → await 轮询排空累计:数据 id 集<b>恰为</b> {4,5}(旧事务零重发:
     * 重放起点 = confirmed_flush ≥ 旧事务提交记录末端 → 旧事务对重启解码器不可见——
     * 确定性机制保证,非时序侥幸)且存在 event_count=2 的 END。
     * 边界:旧事务若重发即判红(无缝语义被破坏);触发表写入不在 publication,重启重放
     * 区间覆盖它们但零输出,不干扰恰集断言。
     */
    @Test
    void restartAfterConfirmedFlushCaughtUpIsSeamlessWithoutDuplicates() throws Exception {
        createFixture();
        var config = baseConfig(SLOT, "pub_restart", pipeDir).build();
        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        Map<Integer, String> expected = new LinkedHashMap<>(insertCommittedRows(1, 3, "seam"));
        List<SourceRecord> first = consumeRecordsUnchecked(5);
        List<SourceRecord> firstData = recordsForTopic(first, TOPIC);
        assertEquals(Set.of(1, 2, 3), dataIds(firstData), "停机前应先消费到首批 3 行");
        long txBoundary = commitLsnOf(firstData);

        // 驱动服务端采纳:非 publication 表写 WAL → 解码推进 → candidate 使 confirmed_flush
        // 跳到客户端已确认前沿(= 首批事务边界)
        assertTrue(awaitConfirmedFlushCaughtUp(txBoundary),
                "停机前应能驱动 confirmed_flush 采纳到已输出事务边界(candidate 机制,环境异常即 fail)");

        stopConnector();
        expected.putAll(insertCommittedRows(4, 2, "seam")); // 停机窗口写入

        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        List<SourceRecord> seen = new ArrayList<>();
        await("无缝续传:停机窗口新事务到达,旧事务零重发")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    drainArrivedRecords(seen);
                    Set<Integer> ids = collectValidatedDataIds(seen, expected);
                    assertEquals(Set.of(4, 5), ids,
                            "重启后数据记录应恰为停机窗口新事务两行(旧事务零重发:重放起点已越过其提交记录,"
                                    + "机制保证的无缝形态): " + describe(seen));
                    assertTrue(hasEndWithEventCount(seen, 2),
                            "新事务应有 END 到达且 event_count=2: " + describe(seen));
                });
    }

    /**
     * 单事务内插入若干小载荷行并提交(普通小事务,走非流式路径——三情况的暖场/停机窗口
     * 写入用;载荷 "前缀-id" 可预期)。与 {@link StreamPgTestEnv#insertIncompressibleRows}
     * 的分工:那边是流式大事务构造,这边只要已提交的确定性小事务。
     *
     * @param idFrom        起始 id(含),逐行 +1
     * @param rows          行数
     * @param payloadPrefix 载荷前缀(实际载荷 "前缀-id")
     * @return id → 实际插入载荷(记录值断言的期望源,保持插入序)
     * @throws SQLException 插入失败原样上抛
     */
    private Map<Integer, String> insertCommittedRows(int idFrom, int rows, String payloadPrefix) throws SQLException {
        Map<Integer, String> payloads = new LinkedHashMap<>();
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, ?)")) {
                for (int i = 0; i < rows; i++) {
                    String payload = payloadPrefix + "-" + (idFrom + i);
                    payloads.put(idFrom + i, payload);
                    ps.setInt(1, idFrom + i);
                    ps.setString(2, payload);
                    ps.executeUpdate();
                }
            }
            finally {
                c.commit();
            }
        }
        return payloads;
    }

    /**
     * 轮询驱动并等待服务端把 confirmed_flush 采纳到 {@code targetLsn}:每轮先在非
     * publication 触发表写一行(产 WAL → walsender 解码推进 → candidate 机制得以把
     * confirmed_flush 一步跳到客户端已确认前沿),再查槽位点。30s 超时返回 false
     * (调用方 fail-fast:该机制是引擎 FrontierCapTest 已实证的服务端行为,不达属环境
     * 异常而非产品语义)。仅测试线程调用。
     *
     * @param targetLsn 期望追平的目标 LSN(已输出事务边界)
     * @return 追平为 true;超时为 false
     * @throws SQLException           触发表写入/槽查询失败原样上抛
     * @throws InterruptedException sleep 被中断:异常原样上抛、中断位不复位
     *                                (勘误:早先写"恢复中断位上抛"与实际不符——本方法
     *                                直接传播 Thread.sleep 的异常,无恢复逻辑;测试放弃)
     */
    private boolean awaitConfirmedFlushCaughtUp(long targetLsn) throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            scratchSeq++;
            StreamPgTestEnv.execSql("INSERT INTO " + SCRATCH + " VALUES (" + scratchSeq + ", 'flush-trigger')");
            if (StreamPgTestEnv.confirmedFlushLsn(SLOT) >= targetLsn) {
                return true;
            }
            Thread.sleep(200);
        }
        return false;
    }

    /**
     * 从一批数据记录提取 id 集(不校验载荷——停机前已见集的收集面,校验留给
     * {@link #collectValidatedDataIds})。after 为 null 的记录(理论不出现,防御)跳过。
     *
     * @param data 数据 topic 的记录列表
     * @return 出现过的 id 集(重复收敛为单值)
     */
    private static Set<Integer> dataIds(List<SourceRecord> data) {
        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : data) {
            Struct after = ((Struct) r.value()).getStruct("after");
            if (after != null) {
                ids.add(after.getInt32("id"));
            }
        }
        return ids;
    }

    /**
     * 收集并校验:遍历全部到达记录中的数据 topic 记录,断言 op=c、after 非空、id 在期望表内
     * 且 payload 全文相等(重复到达各自独立校验——at-least-once 下同一 id 多条都必须值正确),
     * 返回到达 id 集(重复收敛)。
     *
     * @param records  全部到达记录(跨轮次累计)
     * @param expected id → 期望载荷
     * @return 到达过的 id 集
     */
    private static Set<Integer> collectValidatedDataIds(List<SourceRecord> records, Map<Integer, String> expected) {
        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : recordsForTopic(records, TOPIC)) {
            Struct value = (Struct) r.value();
            assertEquals("c", value.getString("op"), "应全为 INSERT(op=c)");
            Struct after = value.getStruct("after");
            assertNotNull(after, "INSERT 记录应有 after 结构");
            Integer id = after.getInt32("id");
            assertTrue(expected.containsKey(id), "未知 id 到达: " + id);
            assertEquals(expected.get(id), after.getString("payload"), "payload 应全文相等(id=" + id + ")");
            ids.add(id);
        }
        return ids;
    }

    /**
     * 取首批数据记录的事务边界 LSN(任意一条的 {@code lsn_commit}:同事务内六面一致,
     * 见 {@code EndToEndStreamedTxIT} 场景①的 offset 三面一致断言——此处只取不重复断)。
     *
     * @param data 同一事务的数据记录(非空)
     * @return 事务边界 LSN(数值形态)
     */
    private static long commitLsnOf(List<SourceRecord> data) {
        return ((Number) data.get(0).sourceOffset().get("lsn_commit")).longValue();
    }

    /**
     * 事务 topic 是否存在指定 event_count 的 END 记录:并集/轮询断言的"完整事务边界补齐"
     * 信号——重复到达的其他事务 END(如重发窗口里的旧事务)不影响命中。status 字段缺失
     * 的记录(理论不出现)跳过。
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
