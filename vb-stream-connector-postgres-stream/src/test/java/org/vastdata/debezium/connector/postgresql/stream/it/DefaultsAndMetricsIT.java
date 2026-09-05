package org.vastdata.debezium.connector.postgresql.stream.it;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.debezium.config.Configuration;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS5 集成面验收 IT(缺省配置注入 + snapshot.mode 拒绝 + 指标观测),三场景:
 * <ol>
 *   <li><b>缺省注入</b>({@code defaultsYieldNoDataSnapshotAndTransactionMetadata}):配置
 *       不含 snapshot.mode 与 provide.transaction.metadata 两键(Embedded engine 经
 *       {@code SourceConnector.taskConfigs} 取任务配置——恰是 {@link PostgresStreamConnector#
 *       taskConfigs(int)} 的注入必经点),启动后写小事务 → 数据记录到达 <b>且</b> 事务元数据
 *       topic 出现 BEGIN/END 记录(注入 provide.transaction.metadata=true 的验收面),全程
 *       零 op="r"(注入 no_data——若默认回落父 Field 的 initial,建流前的种子行会被快照读出);
 *       本场景不能用 baseConfig 的该两键——Configuration 无删键,故自组最小配置复制 baseConfig
 *       语义但排除两键({@link #defaultsInjectionConfig});</li>
 *   <li><b>快照模式拒绝</b>({@code snapshotModeInitialFailsStartup}):显式 snapshot.mode=
 *       initial → 引擎启动失败(CompletionCallback + Awaitility 模式,复用
 *       {@code SlotTwoPhaseMismatchIT} 的既有形态),异常链含 "no_data only"(REST 校验被
 *       绕过时的构造器 fail-fast 兜底);</li>
 *   <li><b>指标观测</b>({@code metricsObservableAfterTraffic}):baseConfig 启动 → 写若干
 *       事务消费到 → 断言 {@code StreamThroughputMetrics} 的 10s 统计 tick INFO 行可观测
 *       (吞吐行出现且输出段非零——流式源的 metrics 实例是 execute 内的私有字段,IT 不可达
 *       {@code throughputMetrics()};MBean 属性亦经 bridge 预计算且需两 tick 窗口,故以
 *       日志 ListAppender 为实际观测路径——路径选择记档 task-5-report.md);</li>
 * </ol>
 * 夹具:场景①③各建单表 + 单表 publication(场景②失败于任务装配期,publication 占位即可);
 * 每场景独立槽名前后清删。需要本机 Docker。
 */
class DefaultsAndMetricsIT extends StreamITBase {

    /** 场景①专用复制槽名。 */
    private static final String SLOT_DEFAULTS = "ms5_defaults";

    /** 场景②专用复制槽名(实际不会被创建——启动失败于任务装配期,清删是防漏兜底)。 */
    private static final String SLOT_INITIAL = "ms5_snap_initial";

    /** 场景③专用复制槽名。 */
    private static final String SLOT_METRICS = "ms5_metrics";

    /** 场景②的 publication 占位名(任务装配期即拒绝,实际不被消费)。 */
    private static final String PUB_PLACEHOLDER = "pub_ms5_placeholder";

    /** 场景①数据表。 */
    private static final String TABLE_DEFAULTS = "t_ms5_defaults";

    /** 场景③数据表。 */
    private static final String TABLE_METRICS = "t_ms5_metrics";

    /** 场景①的 topic 前缀(独立于 baseConfig 的 ms2it,断言 topic 名自洽不与其他场景串台)。 */
    private static final String PREFIX_DEFAULTS = "ms5def";

    /** 场景①数据 topic(DefaultTopicNamingStrategy:前缀.schema.table)。 */
    private static final String TOPIC_DEFAULTS = PREFIX_DEFAULTS + ".public." + TABLE_DEFAULTS;

    /** 场景①事务元数据 topic(注入验收面:&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC_DEFAULTS = PREFIX_DEFAULTS + TX_TOPIC_SUFFIX;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir
    Path pipeDir;

    /**
     * 指标三行(吞吐/分布/峰值)的打点 logger 名:TransactionConsumer 是包私有类,IT 包
     * 不能引类字面量,按 SLF4J 的名字寻址(slf4j getLogger(String) 与 getLogger(Class) 的
     * logger 名等价——均取全限定类名)。
     */
    private static final String METRICS_LOGGER_NAME =
            "org.vastdata.debezium.connector.postgresql.stream.TransactionConsumer";

    /**
     * 每用例前清三个场景的残留槽(幂等):上次异常退出留下的同名槽会从旧
     * confirmed_flush_lsn 续传,静默吞掉建流前的写入使记录断言失真。
     */
    @BeforeEach
    void cleanResidualSlots() {
        StreamPgTestEnv.dropSlotQuietly(SLOT_DEFAULTS);
        StreamPgTestEnv.dropSlotQuietly(SLOT_INITIAL);
        StreamPgTestEnv.dropSlotQuietly(SLOT_METRICS);
    }

    /**
     * 每用例后清理:先停引擎再删槽(次序语义见基类 {@link #stopEngineAndDropSlot};
     * 未启动/启动失败时停引擎为幂等 no-op)。
     */
    @AfterEach
    void dropSlots() {
        stopEngineAndDropSlot(SLOT_DEFAULTS);
        stopEngineAndDropSlot(SLOT_INITIAL);
        stopEngineAndDropSlot(SLOT_METRICS);
    }

    /**
     * 场景①:缺省配置(无 snapshot.mode / provide.transaction.metadata 两键)经
     * taskConfigs 注入后等价于 no_data + 事务元数据开。关键步骤:建表并<b>预插种子行</b>
     * (若默认回落 initial,种子行会被快照读出成 op="r"——零 op="r" 断言的素材)→ 建单表
     * publication → 以自组最小配置启动(配置面恰好缺省两键,embedded engine 取任务配置的
     * 必经点即注入点)→ 等 walsender 挂上 → 单事务插两行 → consume(4)(注入
     * provide.transaction.metadata=true 下恰 BEGIN + 2 数据 + END)→ 断言:数据 topic 恰
     * 2 条且 op 全 "c";事务元数据 topic 恰 BEGIN/END 一对且 END 计数 = 2(实付数);
     * 全部到达记录零 op="r"(无快照记录)。边界:凑不齐 4 条即 fail(注入失败/引擎未达均
     * 属失败而非跳过)。
     */
    @Test
    void defaultsYieldNoDataSnapshotAndTransactionMetadata() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE_DEFAULTS + "(id int PRIMARY KEY, v text)",
                "TRUNCATE " + TABLE_DEFAULTS,
                "INSERT INTO " + TABLE_DEFAULTS + " VALUES (100, 'seed-before-start')",
                "DROP PUBLICATION IF EXISTS pub_ms5_defaults",
                "CREATE PUBLICATION pub_ms5_defaults FOR TABLE " + TABLE_DEFAULTS);

        start(PostgresStreamConnector.class, defaultsInjectionConfig().build());
        StreamPgTestEnv.awaitWalsender(SLOT_DEFAULTS, 20_000);
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (java.sql.Statement st = c.createStatement()) {
                st.execute("INSERT INTO " + TABLE_DEFAULTS + " VALUES (1, 'a')");
                st.execute("INSERT INTO " + TABLE_DEFAULTS + " VALUES (2, 'b')");
            }
            finally {
                c.commit();
            }
        }

        List<SourceRecord> all = consumeRecordsUnchecked(4);
        assertEquals(4, all.size(), "注入事务元数据后单事务恰 BEGIN+2 数据+END: " + describe(all));
        List<SourceRecord> data = recordsForTopic(all, TOPIC_DEFAULTS);
        List<SourceRecord> txMeta = recordsForTopic(all, TX_TOPIC_DEFAULTS);
        assertEquals(2, data.size(), "数据记录恰 2 条(两行 INSERT)");
        assertEquals(2, txMeta.size(), "事务元数据记录恰一对 BEGIN/END(注入 provide.transaction.metadata=true)");

        Set<String> ops = new HashSet<>();
        for (SourceRecord r : data) {
            ops.add(((Struct) r.value()).getString("op"));
        }
        assertEquals(Set.of("c"), ops, "数据记录全为流式 INSERT(op=c)");
        for (SourceRecord r : all) {
            if (r.value() instanceof Struct s && s.schema().field("op") != null) {
                assertFalse("r".equals(s.getString("op")),
                        "缺省注入 no_data:零快照记录(op=r),种子行不被快照读出: " + describe(all));
            }
        }
        Struct begin = (Struct) txMeta.get(0).value();
        Struct end = (Struct) txMeta.get(1).value();
        assertEquals("BEGIN", begin.getString("status"), "首条事务块为 BEGIN");
        assertEquals("END", end.getString("status"), "次条事务块为 END");
        assertEquals(2L, end.getInt64("event_count"), "END 的 event_count=实付数据数(2)");
    }

    /**
     * 场景②:显式 snapshot.mode=initial 被启动期拒绝。关键步骤:以 baseConfig(已含
     * no_data)叠加 {@code .with("snapshot.mode", "initial")} 覆盖 → start 注入捕获
     * CompletionCallback → 硬断言失败信号 60s 内到达(轮询回调置位)→ 断言失败且异常链
     * 渲染文本含 "no_data only"(REST 校验被绕过时构造器 fail-fast 的兜底文案)。
     * 边界:start() 返回仅代表 latch 放行,失败回调可能更晚,故由 Awaitility 承载断言
     * (与 SlotTwoPhaseMismatchIT 同形态);注入层只 putIfAbsent,显式 initial 原样透传
     * 到任务侧构造器——本场景正是该透传语义的验收面。
     */
    @Test
    void snapshotModeInitialFailsStartup() throws Exception {
        AtomicBoolean completionReached = new AtomicBoolean(false);
        AtomicBoolean succeeded = new AtomicBoolean(true);
        AtomicReference<String> message = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT_INITIAL, PUB_PLACEHOLDER, pipeDir)
                        .with("snapshot.mode", "initial").build(),
                (success, msg, err) -> {
                    completionReached.set(true);
                    succeeded.set(success);
                    message.set(msg);
                    error.set(err);
                },
                null);

        await("snapshot.mode=initial 必须启动失败(残余到运行期才是缺陷)")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilTrue(completionReached);
        assertFalse(succeeded.get(), "引擎必须以失败告终,不得成功运行");
        String chain = renderThrowableChain(message.get(), error.get());
        assertTrue(chain.contains("no_data only"),
                "失败异常链应含 no_data only 拒绝文案(当前链: " + chain + ")");
    }

    /**
     * 场景③:有流量后指标观测面可观测。关键步骤:建表 + publication → baseConfig 启动 →
     * 等 walsender → 三个自动提交 INSERT(各成一小事务,共 BEGIN/END 三对 + 3 数据
     * = 9 条)→ consume(9) 证明流量已完整走完 slot→组装→回放→输出链路 → 挂
     * ListAppender 轮询等 10s 统计 tick:吞吐行("吞吐:"前缀)出现且输出段非零
     * (不含 "0.0 rec/s"——流量窗口的差分必然非零)→ 峰值行("峰值:")的输出段非 n/a
     * (会话峰值不随窗口翻页归零)。观测路径:流式源的 {@code StreamThroughputMetrics}
     * 实例是 execute 内私有字段,IT 不可达 {@code throughputMetrics()}/{@code totals()},
     * 故以 TransactionConsumer 的 INFO 三行为准(tick 同点触发 bridge 预计算,日志行出现
     * 即指标链路在跑)。边界:appender 挂在写流量之后、等 tick 之前——首个含流量的窗口
     * 报告必然非零;若流量恰好横跨 tick 边界则后一窗口承接,45s 轮询覆盖多窗。
     */
    @Test
    void metricsObservableAfterTraffic() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE_METRICS + "(id int PRIMARY KEY, v text)",
                "TRUNCATE " + TABLE_METRICS,
                "DROP PUBLICATION IF EXISTS pub_ms5_metrics",
                "CREATE PUBLICATION pub_ms5_metrics FOR TABLE " + TABLE_METRICS);

        start(PostgresStreamConnector.class, baseConfig(SLOT_METRICS, "pub_ms5_metrics", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT_METRICS, 20_000);
        StreamPgTestEnv.execSql(
                "INSERT INTO " + TABLE_METRICS + " VALUES (1, 'm1')",
                "INSERT INTO " + TABLE_METRICS + " VALUES (2, 'm2')",
                "INSERT INTO " + TABLE_METRICS + " VALUES (3, 'm3')");
        List<SourceRecord> consumed = consumeRecordsUnchecked(9);
        assertEquals(9, consumed.size(), "三事务各 BEGIN+数据+END 共 9 条(流量完整走完输出链路): "
                + describe(consumed));

        ListAppender<ILoggingEvent> appender = attachInfoCapture();
        try {
            await("10s 统计 tick 的吞吐行应出现且输出段非零(指标观测面)")
                    .atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(500))
                    .until(() -> infoLines(appender).stream()
                            .anyMatch(line -> line.startsWith("吞吐:") && !line.contains("0.0 rec/s")));
            assertTrue(infoLines(appender).stream().anyMatch(
                            line -> line.startsWith("峰值:") && !line.contains("输出=n/a")),
                    "峰值行输出段非 n/a(会话峰值留存,不随窗口翻页消失): "
                            + infoLines(appender));
        }
        finally {
            ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(METRICS_LOGGER_NAME))
                    .detachAppender(appender);
        }
    }

    /**
     * 场景①的自组最小配置:逐项复制 {@link #baseConfig} 的流式验收语义,但<b>不含</b>
     * snapshot.mode 与 provide.transaction.metadata 两键——两键的缺省值正是本场景的验收
     * 对象(注入路径),配置里带了就验不到注入。为什么不用 baseConfig 再删键:
     * Configuration/Builder 无删键 API,{@code with(name, null)} 也非删除语义,只能自组。
     * topic.prefix 用独立的 ms5def,数据/事务元数据 topic 断言不与 baseConfig 前缀串台。
     *
     * @return 已填基础项(缺省两键除外)的 Configuration.Builder(未 build)
     */
    private Configuration.Builder defaultsInjectionConfig() {
        return Configuration.create()
                .with("database.hostname", StreamPgTestEnv.PG.getHost())
                .with("database.port", StreamPgTestEnv.PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
                .with("database.dbname", StreamPgTestEnv.PG.getDatabaseName())
                .with("database.user", StreamPgTestEnv.PG.getUsername())
                .with("database.password", StreamPgTestEnv.PG.getPassword())
                .with("topic.prefix", PREFIX_DEFAULTS)
                .with("slot.name", SLOT_DEFAULTS)
                .with("publication.name", "pub_ms5_defaults")
                .with("slot.streaming", "parallel")
                .with("slot.two.phase", true)
                .with("slot.feedback.interval.ms", 1000)
                .with("pipe.dir", pipeDir.toAbsolutePath().toString());
    }

    /**
     * 挂载 INFO 捕获器到指标三行的打点 logger({@link #METRICS_LOGGER_NAME};吞吐/分布/峰值的
     * 实际打点处),调用方 try/finally 摘除防泄漏到其他用例。ListAppender 自身不过滤
     * 级别——INFO 过滤由 {@link #infoLines} 侧做。
     *
     * @return 已 start 的捕获器
     */
    private static ListAppender<ILoggingEvent> attachInfoCapture() {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(METRICS_LOGGER_NAME))
                .addAppender(appender);
        return appender;
    }

    /**
     * 就地快照捕获器中的 INFO 格式化消息:logback 的 doAppend 对 appender 实例加锁写,
     * 读侧同锁复制(logback 3.x ListAppender.list 为裸 ArrayList,无锁读有竞态)。
     *
     * @param appender 已挂载的捕获器
     * @return 当前已捕获 INFO 消息的副本(到达序)
     */
    private static List<String> infoLines(ListAppender<ILoggingEvent> appender) {
        List<String> out = new ArrayList<>();
        synchronized (appender) {
            for (ILoggingEvent e : appender.list) {
                if (e.getLevel() == Level.INFO) {
                    out.add(e.getFormattedMessage());
                }
            }
        }
        return out;
    }
}
