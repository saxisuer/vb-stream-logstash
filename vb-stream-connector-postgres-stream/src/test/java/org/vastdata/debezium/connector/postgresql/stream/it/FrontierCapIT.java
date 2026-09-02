package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 反馈语义验收的连接器形态(引擎 it 包 FrontierCapTest 的翻译):未输出事务钉住槽
 * confirmed_flush_lsn,输出放行并补 WAL 活动后越过封顶。构造与 {@link ReaderUnblockedIT}
 * 同款(小队列 + 阻塞消费者,差异仅在断言面):T0 热身事务直通(前沿&gt;0 是 cap 生效
 * 前提——前沿 0 = 无 cap)→ 取 {@code pg_current_wal_insert_lsn()} 为 before 锚点 →
 * 提交目标事务 T(锚点间唯一 WAL 活动,before &lt; T.endLsn ≤ after 恒成立)→ T 的输出
 * 在队列满处阻塞(consumer 线程停摆,End 未处理,前沿冻结在 T0.endLsn ≤ before)→
 * 等 ≥2 个反馈周期(本测试 1s×2)断言 confirmed ≤ before → 放行排干 → 补 WAL 活动
 * (T2)触发服务端解码推进(candidate 机制:confirmed_flush 落库需要解码活动,空闲期
 * 不推进)→ 断言 confirmed &gt; before。
 *
 * <p>关键机理:阻塞期间客户端反馈 = min(已收到, 前沿=T0.endLsn ≤ before)——无论
 * 服务端何时采纳,确认值被前沿钉在 before 之前;若无 End 锚定封顶,vanilla 的
 * received 直推路径会在 T 解码完成时把 confirmed 推到 ≥ T.endLsn &gt; before,
 * 第一段必红(非恒真断言)。夹具:独立槽 {@code frontier_cap_it} 前后清删(残留槽
 * 续传旧位点会破坏"T 是锚点间唯一 WAL 活动"的前提);表/publication 先于建槽;
 * 管道 @TempDir。需要本机 Docker。
 */
class FrontierCapIT extends StreamITBase {

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "frontier_cap_it";

    /** 数据表名。 */
    private static final String TABLE = "t_cap";

    /** 阻塞前的放行条数:T0 热身事务的 BEGIN + 1 数据 + END。 */
    private static final int BLOCK_AFTER = 3;

    /** 目标事务 T 的行数(40 行 + BEGIN/END = 42 条记录,远超队列+在途缓冲,保证 End 不被 dispatch)。 */
    private static final int BIG_TX_ROWS = 40;

    /** 全部已消费记录的确定数:T0 三条 + T 四十二条。 */
    private static final int TOTAL_RECORDS = 3 + (BIG_TX_ROWS + 2);

    /** 每用例独立的管道目录(瞬态工作区)。 */
    @TempDir
    Path pipeDir;

    /**
     * 每用例前清残留槽:残留同名槽从旧 confirmed_flush_lsn 续传,锚点语义
     * (T 是 before/after 之间唯一 WAL 活动)即被破坏。幂等。
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
     * 未输出事务钉住 confirmed_flush(第一段)+ 输出后越过封顶(第二段)。
     * 关键步骤:夹具 → 阻塞消费者形态 start → 写 T0 并等 sink==3(热身直通,前沿&gt;0)
     * → before 锚点 → 写 T(40 行单事务)→ 等 blockedStarted(T 的 BEGIN 到达,输出
     * 停摆)→ sleep ≥2 个反馈周期后断言 confirmed ≤ before(封顶钉在 T 之前)→ 放行,
     * 等 sink==45(T 的 End 已 dispatch,前沿=T.endLsn&gt;before)→ 写 T2 触发解码推进,
     * 轮询断言 confirmed &gt; before。边界:after ≥ before 自洽护栏;release 闩 finally
     * 兜底(断言中途抛出不拖住引擎停机)。
     */
    @Test
    void unflushedTransactionHoldsConfirmedFlush() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int)",
                "DROP PUBLICATION IF EXISTS pub_cap_it",
                "CREATE PUBLICATION pub_cap_it FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockedStarted = new CountDownLatch(1);
        List<SourceRecord> sink = new CopyOnWriteArrayList<>();
        try {
            start(PostgresStreamConnector.class,
                    withSmallQueue(baseConfig(SLOT, "pub_cap_it", pipeDir)).build(),
                    loggingCompletion(), null,
                    blockingConsumerAt(BLOCK_AFTER, release, blockedStarted, sink), false);
            StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

            // 热身 T0:直通使前沿 > 0——cap 生效前提(前沿 0 = 无 cap)
            StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (0)");
            await("T0 三条记录先放行").atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                    .until(() -> sink.size() == BLOCK_AFTER);

            // 锚点与目标事务:before < T.endLsn ≤ after 恒成立(T 是两锚点间唯一 WAL 活动)
            long before = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_insert_lsn()");
            insertBigTx(1);
            long after = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_insert_lsn()");
            assertTrue(after >= before, "自洽护栏: after=" + after + " before=" + before);

            assertTrue(blockedStarted.await(20, java.util.concurrent.TimeUnit.SECONDS),
                    "目标事务首条记录未到达——输出路径未进入阻塞(20s)");
            Thread.sleep(2_200);   // ≥ 2 个反馈周期(1s×2):给服务端充足的采纳窗口

            // 第一段:输出阻塞期间,反馈被前沿(=T0.endLsn ≤ before)封顶,确认不得越过
            long confirmedBlocked = StreamPgTestEnv.confirmedFlushLsn(SLOT);
            assertTrue(confirmedBlocked <= before,
                    "未输出事务应钉住 confirmed_flush: confirmed=" + confirmedBlocked
                            + " before=" + before);

            release.countDown();
            // 等 T 输出完成:全部 45 条到达(T 的 End 已处理,前沿=T.endLsn>before 恒成立)
            await("放行后全部记录排干").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                    .until(() -> sink.size() >= TOTAL_RECORDS);

            // 第二段:补一次 WAL 活动触发解码推进(confirmed_flush 落库条件——空闲期
            // 不推进),轮询断言越过封顶(≥ 语义,慢机容忍)
            StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (999)");
            await("输出后 confirmed_flush 越过封顶").atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> StreamPgTestEnv.confirmedFlushLsn(SLOT) > before);
        }
        finally {
            release.countDown();
        }
    }

    /**
     * 单事务批量插入 N 行(小载荷,快路径——本测试不关心 T 是否流式,只关心其输出
     * 被阻塞在队列满处)。行序 idFrom..idFrom+rows-1。
     *
     * @param idFrom 起始 id
     */
    private void insertBigTx(int idFrom) throws Exception {
        try (var c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (var ps = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?)")) {
                for (int i = 0; i < BIG_TX_ROWS; i++) {
                    ps.setInt(1, idFrom + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            finally {
                c.commit();
            }
        }
    }
}
