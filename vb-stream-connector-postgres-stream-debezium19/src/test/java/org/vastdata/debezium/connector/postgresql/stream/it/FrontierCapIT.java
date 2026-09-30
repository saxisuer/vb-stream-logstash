package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 反馈语义验收的连接器形态(引擎 it 包 FrontierCapTest 的翻译):未输出事务钉住槽
 * confirmed_flush_lsn,输出放行并补 WAL 活动后越过封顶。构造与 {@link ReaderUnblockedIT}
 * 同款(小队列 + 阻塞消费者,差异仅在断言面,阻塞安装位置亦同——1.9.7 基座 start 无
 * 消费者注入口,本类自建 embedded 引擎把阻塞消费者装进引擎线程的 handleBatch,
 * task.poll 停摆 → ChangeEventQueue 满 → 连接器 consumer 线程在 enqueue 阻塞,详见
 * 彼处类 javadoc):T0 热身事务直通(前沿&gt;0 是 cap 生效前提——前沿 0 = 无 cap)→
 * 取 {@code pg_current_wal_insert_lsn()} 为 before 锚点 → 提交目标事务 T(锚点间唯一
 * WAL 活动,before &lt; T.endLsn ≤ after 恒成立)→ T 的输出在队列满处阻塞(consumer
 * 线程停摆,End 未处理,前沿冻结在 T0.endLsn ≤ before)→ 等 ≥2 个反馈周期(本测试
 * 1s×2)断言 confirmed ≤ before → 放行排干 → 补 WAL 活动(T2)触发服务端解码推进
 * (candidate 机制:confirmed_flush 落库需要解码活动,空闲期不推进)→ 断言
 * confirmed &gt; before。
 *
 * <p>关键机理:阻塞期间客户端反馈 = min(已收到, 前沿=T0.endLsn ≤ before)——无论
 * 服务端何时采纳,确认值被前沿钉在 before 之前;若无 End 锚定封顶,vanilla 的
 * received 直推路径会在 T 解码完成时把 confirmed 推到 ≥ T.endLsn &gt; before,
 * 第一段必红(非恒真断言)。夹具:独立槽 {@code frontier_cap_it} 前后清删(残留槽
 * 续传旧位点会破坏"T 是锚点间唯一 WAL 活动"的前提);表/publication 先于建槽;
 * 管道 {@code @TempDir}。需要本机 Docker。
 */
class FrontierCapIT extends StreamITBase {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierCapIT.class);

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
    @TempDir(cleanup = CleanupMode.NEVER)
    Path pipeDir;

    /** 自建阻塞引擎的实例(run 期间非 null;每用例一次 start,不重启)。 */
    private DebeziumEngine<SourceRecord> blockingEngine;

    /** 自建阻塞引擎的线程(run 阻塞至停机;与 blockingEngine 同生命周期)。 */
    private Thread blockingEngineThread;

    /** 自建引擎完成闩:run() 返回(正常或异常)后 CompletionCallback countDown。 */
    private volatile CountDownLatch blockingEngineStopped = new CountDownLatch(1);

    /**
     * 每用例前清残留槽与残留 offset:残留槽续传旧位点会破坏锚点语义(T 是 before/after
     * 之间唯一 WAL 活动);自建引擎的 offset 文件与基座文件分立,基座
     * {@code initializeConnectorTestFramework} 删不到它,由本类自清(跨用例残留会让
     * 新建槽的流起点错位)。幂等。
     */
    @BeforeEach
    void cleanResidualSlotAndOffset() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
        try {
            Files.deleteIfExists(blockingOffsetFilePath());
        }
        catch (IOException e) {
            throw new IllegalStateException("自建引擎旧 offset 文件删除失败: " + blockingOffsetFilePath(), e);
        }
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
            startBlockingEngine(PostgresStreamConnector.class,
                    withSmallQueue(baseConfig(SLOT, "pub_cap_it", pipeDir)).build(),
                    blockingConsumerAt(BLOCK_AFTER, release, blockedStarted, sink));
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

            assertTrue(blockedStarted.await(20, TimeUnit.SECONDS),
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

    /**
     * 以自定义逐条消费者启动<b>本类自建的</b> embedded 引擎(基座 {@code start} 无消费者
     * 注入口,阻塞构造必须落在引擎线程上,见类 javadoc 与 {@link ReaderUnblockedIT}
     * 的安装位置论证)。关键步骤:offset 文件父目录就绪 → 配置叠加引擎五件套(与基座
     * start 同款:name/connector.class/offset.storage+文件/offset.flush.interval.ms=100
     * ——文件与基座的分立,本类 {@code @BeforeEach} 自清)→ {@code EmbeddedEngine.create()}
     * 装配(CompletionCallback 打日志 + countDown 完成闩,对应 3.6.1 翻译源的
     * loggingCompletion/assertOnFailure=false 形态;ChangeConsumer 逐条
     * consumer.accept → markProcessed,批尾 markBatchFinished——阻塞发生在引擎线程的
     * handleBatch 内,task.poll 随之停摆)→ 专用线程 run。
     * 边界:引擎构造失败原样上抛(装配错误 fail-fast)。
     *
     * @param connectorClass 连接器实现类
     * @param config         连接器配置(引擎面属性由本方法注入)
     * @param consumer       逐条记录消费者(阻塞构造经 {@link #blockingConsumerAt} 生成)
     */
    private void startBlockingEngine(Class<? extends SourceConnector> connectorClass, Configuration config,
            Consumer<SourceRecord> consumer) {
        Path offsetFile = blockingOffsetFilePath();
        try {
            Files.createDirectories(offsetFile.getParent());
        }
        catch (IOException e) {
            throw new IllegalStateException("offset 文件父目录创建失败: " + offsetFile.getParent(), e);
        }
        Configuration effective = config.edit()
                .with("name", "stream-it-blocking-engine")
                .with("connector.class", connectorClass.getName())
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetFile.toString())
                .with("offset.flush.interval.ms", 100)
                .build();
        blockingEngineStopped = new CountDownLatch(1);
        blockingEngine = EmbeddedEngine.create()
                .using(effective)
                .using((success, message, error) -> {
                    if (!success) {
                        LOG.error("阻塞形态 embedded 引擎异常停机: {}", message, error);
                    }
                    blockingEngineStopped.countDown();
                })
                .notifying((List<SourceRecord> batch, DebeziumEngine.RecordCommitter<SourceRecord> committer) -> {
                    for (SourceRecord record : batch) {
                        consumer.accept(record);
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .using(getClass().getClassLoader())
                .build();
        blockingEngineThread = new Thread(blockingEngine::run, "stream-it-blocking-engine");
        blockingEngineThread.setDaemon(false);
        blockingEngineThread.start();
    }

    /**
     * 覆写基座停机入口:先收敛本类自建引擎再走基座路径(基座引擎从未 start,恒 no-op)。
     * 基座清理链({@code stopEngineAndDropSlot} 的"先停引擎后删槽"次序与 @AfterEach
     * 兜底)全部经本方法路由到自建引擎,次序语义不变。
     */
    @Override
    protected void stopConnector() {
        stopBlockingEngine();
        super.stopConnector();
    }

    /**
     * 请求自建引擎停机并等待退出:release 闩已由用例 finally 放行(阻塞消费者退出
     * await)→ {@code close()} 触发 run 循环退出;引擎从未建(null)为 no-op,已停
     * (闩已开)幂等跳过 close。等待语义与基座 {@code stopConnector} 同款(60s 卡死
     * 探测 AssertionError)。
     */
    private void stopBlockingEngine() {
        DebeziumEngine<SourceRecord> current = blockingEngine;
        if (current == null) {
            return;
        }
        if (blockingEngineStopped.getCount() > 0) {
            try {
                current.close();
            }
            catch (Exception e) {
                throw new IllegalStateException("blocking engine close failed", e);
            }
        }
        if (blockingEngineThread == null) {
            return;
        }
        try {
            if (!blockingEngineStopped.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("阻塞形态引擎 60s 内未停机(close 未生效或 run 卡死)");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        try {
            blockingEngineThread.join(TimeUnit.SECONDS.toMillis(60));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * 本类自建引擎的 offset 文件路径:模块 target/it-offsets/ 下按测试类简单名加
     * {@code -blocking} 后缀(与基座 offset 文件分立,互不删除;跨用例残留由本类
     * {@code @BeforeEach} 清理)。
     *
     * @return offset 文件路径(surefire 工作目录即模块 basedir 相对)
     */
    private Path blockingOffsetFilePath() {
        return Path.of("target", "it-offsets", getClass().getSimpleName() + "-blocking.dat");
    }
}
