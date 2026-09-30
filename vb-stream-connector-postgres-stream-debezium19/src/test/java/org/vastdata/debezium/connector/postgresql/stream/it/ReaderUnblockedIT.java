package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.data.Struct;
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
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 解耦头名验收的连接器形态(引擎 it 包 ReaderUnblockedTest 的翻译,断言面换服务端
 * 可观测列):输出路径阻塞期间 reader 线程持续从复制流接收——"读不被输出阻塞"目标
 * 的端到端验证。构造:小队列(max.queue.size=8/max.batch.size=4)+ 阻塞消费者
 * (前 3 条=T0 热身事务的 BEGIN/数据/END 放行,第 4 条起阻塞)→ T0 消费完成后写入
 * 40 行大事务 T:连接器 consumer 线程 dispatch 到队列满即阻塞(输出前沿冻结在
 * T0.endLsn),而 reader 线程照常 drain 复制流并把 T 的消息落管道。
 *
 * <p><b>1.9.7 线的阻塞安装位置(与 3.6.1 的差异,语义等价)</b>:3.6.1 基座的
 * {@code start} 六参重载把自定义消费者装进引擎的记录处理线程;新基座 {@code start}
 * 无消费者注入口(ChangeConsumer 固定为入队基座记录队列,无界不阻塞)。故本类
 * <b>自建 embedded 引擎</b>({@link #startBlockingEngine},引擎五件套与基座 start 同款)
 * 装入 {@link #blockingConsumerAt} 的逐条消费者——1.9.7 同步引擎的 {@code run()}
 * 在引擎线程上串行做 {@code task.poll()} → {@code handleBatch},消费者在第 4 条阻塞
 * 即引擎停摆 poll → 任务 ChangeEventQueue(max.queue.size)填满 → 连接器 consumer
 * 线程在 dispatch 的 enqueue 上阻塞。与 3.6.1 异步引擎经 dispatch 线程阻塞殊途同归:
 * 阻塞点都在"任务队列的消费端",reader 线程两侧均不受影响。停机经
 * {@link #stopConnector()} 覆写先收敛自建引擎(幂等),基座清理链
 * ({@code stopEngineAndDropSlot}/@AfterEach 兜底)零感知复用。
 *
 * <p><b>阻塞窗口的 reader 活性断言面(为何选 reply_time)</b>:引擎侧可直接数
 * reader 的 onRaw 回调,连接器侧无此插桩;候选观测——①walsender 的 sent_lsn:服务端
 * 写 socket 即推进,socket 缓冲可吸收小流量,"没在读"也能推进,不严格;②槽
 * confirmed_flush_lsn 钉住:那是 FrontierCapIT 的断言面(前沿封顶),不证活性;
 * ③<b>pg_stat_replication.reply_time</b>(客户端最近一次 standby status 的服务端
 * 收时):status 由 <b>reader 线程的 run 循环</b>按反馈周期(本测试 1s)内联发送,
 * reader 停摆则 status 停发、reply_time 冻结——输出阻塞期间的 reply_time 持续推进
 * 即"reader 循环活着"的严格证明。阻塞窗口取 ≥3s(≥2 个反馈周期,非恒真)。
 *
 * <p>放行后断言排干契约:全部已消费记录数恰 45(T0=BEGIN+1+END,T=BEGIN+40+END),
 * 数据记录恰覆盖 41 行不丢不重(引擎测试"放行后排干输出 == 提交数"的同义)。
 * 夹具:独立槽 {@code reader_unblocked_it} 前后清删;表/publication 先于建槽;管道
 * {@code @TempDir}。需要本机 Docker。
 */
class ReaderUnblockedIT extends StreamITBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReaderUnblockedIT.class);

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "reader_unblocked_it";

    /** 数据表名。 */
    private static final String TABLE = "t_unblock";

    /** 数据记录 topic。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 阻塞前的放行条数:T0 热身事务的 BEGIN + 1 数据 + END。 */
    private static final int BLOCK_AFTER = 3;

    /** 大事务 T 的行数(40 行 + BEGIN/END = 42 条记录,远超队列+在途缓冲,保证 consumer 阻塞)。 */
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
     * 每用例前清残留槽与残留 offset:残留同名槽从旧 confirmed_flush_lsn 续传,静默吞
     * 掉建流前写入使记录计数失真;自建引擎的 offset 文件与基座文件分立,基座
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
     * 输出阻塞期间 reader 持续接收(reply_time 推进)+ 放行后排干全量(不丢不重)。
     * 关键步骤:夹具 → 阻塞消费者形态 start → 写 T0 并等 sink==3(热身放行,前沿&gt;0)→
     * 写 T(40 行单事务)→ 等 blockedStarted(T 的 BEGIN 即全局第 4 条到达,输出路径
     * 已停摆)→ 取 reply_time 基线 → 等 ≥3s(≥2 个反馈周期)断言 reply_time 严格推进
     * (reader 循环活着;若读被输出阻塞,status 停发,reply_time 冻结,本断言必红)→
     * 放行 → 等全部 45 条到达 → 断言数据记录恰 41 行全覆盖。边界:release 闩 finally
     * 兜底 countDown(断言中途抛出也不留卡死的记录处理线程拖住引擎停机)。
     */
    @Test
    void readerStaysAliveWhileOutputPathIsBlocked() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_unblock_it",
                "CREATE PUBLICATION pub_unblock_it FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockedStarted = new CountDownLatch(1);
        List<SourceRecord> sink = new CopyOnWriteArrayList<>();
        try {
            startBlockingEngine(PostgresStreamConnector.class,
                    withSmallQueue(baseConfig(SLOT, "pub_unblock_it", pipeDir)).build(),
                    blockingConsumerAt(BLOCK_AFTER, release, blockedStarted, sink));
            StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

            // 热身 T0:1 行事务,3 条记录放行——前沿>0(此后 T 的输出阻塞才冻结得住确认)
            StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (0, 'warmup')");
            await("T0 三条记录先放行").atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                    .until(() -> sink.size() == BLOCK_AFTER);

            // 大事务 T:40 行单事务(连接器 replay 后 BEGIN+40+END=42 条,队列 8+在途≈16
            // 必然装满 → consumer 线程阻塞;reader 侧不受影响持续接收)
            try (var c = StreamPgTestEnv.newSqlConnection()) {
                c.setAutoCommit(false);
                try (var ps = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, 'x')")) {
                    for (int i = 1; i <= BIG_TX_ROWS; i++) {
                        ps.setInt(1, i);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                finally {
                    c.commit();
                }
            }
            // T 的首条记录(BEGIN,全局第 4 条)到达即输出路径停摆
            assertTrue(blockedStarted.await(20, TimeUnit.SECONDS),
                    "T 的首条记录未到达——输出路径未进入阻塞(20s)");

            Timestamp replyAtBlock = replyTime();
            assertNotNull(replyAtBlock, "阻塞起点应有活跃 walsender 的 reply_time");
            // 阻塞窗口 ≥3s:reader 循环按 1s 反馈周期持续发 status → reply_time 推进
            Thread.sleep(3_200);
            Timestamp replyAfterWindow = replyTime();
            assertNotNull(replyAfterWindow, "阻塞窗口内 walsender 应仍活跃");
            assertTrue(replyAfterWindow.after(replyAtBlock),
                    "输出阻塞期间 reply_time 应推进(reader 线程仍在轮询发 status): "
                            + replyAtBlock + " -> " + replyAfterWindow);
        }
        finally {
            release.countDown();
        }

        // 放行排干:全部 45 条到达(重复容忍面不涉及——单引擎单槽,正常路径恰量)
        await("放行后全部记录排干").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertTrue(sink.size() >= TOTAL_RECORDS,
                        "排干应到齐 " + TOTAL_RECORDS + " 条: " + sink.size()));
        assertEquals(TOTAL_RECORDS, sink.size(), "排干后记录总数恰为提交量(不丢不多)");

        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : sink) {
            if (TOPIC.equals(r.topic())) {
                Struct after = ((Struct) r.value()).getStruct("after");
                assertNotNull(after);
                ids.add(after.getInt32("id"));
            }
        }
        assertEquals(BIG_TX_ROWS + 1, ids.size(), "数据记录应覆盖全部 41 行(0.." + BIG_TX_ROWS + ")");
    }

    /**
     * 以自定义逐条消费者启动<b>本类自建的</b> embedded 引擎(基座 {@code start} 无消费者
     * 注入口,阻塞构造必须落在引擎线程上,见类 javadoc 的安装位置论证)。关键步骤:
     * offset 文件父目录就绪 → 配置叠加引擎五件套(与基座 start 同款:name/connector.class/
     * offset.storage+文件/offset.flush.interval.ms=100——文件与基座的分立,本类
     * {@code @BeforeEach} 自清)→ {@code EmbeddedEngine.create()} 装配(CompletionCallback 打日志
     * + countDown 完成闩,对应 3.6.1 翻译源的 loggingCompletion/assertOnFailure=false 形态;
     * ChangeConsumer 逐条 consumer.accept → markProcessed,批尾 markBatchFinished——
     * 阻塞发生在引擎线程的 handleBatch 内,task.poll 随之停摆)→ 专用线程 run。
     * 边界:引擎构造失败原样上抛(装配错误 fail-fast);重复调用未先停属用例编写错误,
     * 旧线程泄漏由 60s 停机断言兜底暴露。
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

    /**
     * 槽的活跃 walsender 的 reply_time(pg_stat_replication):客户端最近一次 standby
     * status 的服务端收时。无活跃 walsender 或列为 NULL 返回 null(调用方按环境异常处理)。
     */
    private static Timestamp replyTime() throws Exception {
        try (var c = StreamPgTestEnv.newSqlConnection();
             var ps = c.prepareStatement(
                     "SELECT r.reply_time FROM pg_stat_replication r "
                             + "JOIN pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = ?")) {
            ps.setString(1, SLOT);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getTimestamp(1) : null;
            }
        }
    }
}
