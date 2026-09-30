package org.vastdata.debezium.connector.postgresql.stream.it;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.config.Configuration;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 集成面验收 IT(缺省配置注入 + snapshot.mode 拒绝 + 指标观测,1.9.7 线),三场景:
 * <ol>
 *   <li><b>缺省注入</b>({@code defaultsYieldNoDataSnapshotAndTransactionMetadata}):配置
 *       不含 snapshot.mode 与 provide.transaction.metadata 两键(embedded engine 经
 *       {@code SourceConnector.taskConfigs} 取任务配置——恰是 {@link PostgresStreamConnector#
 *       taskConfigs(int)} 的注入必经点),启动后写小事务 → 数据记录到达 <b>且</b> 事务元数据
 *       topic 出现 BEGIN/END 记录(注入 provide.transaction.metadata=true 的验收面),全程
 *       零 op="r"(注入 never——若缺注入,任务侧启动校验/构造器即 fail-fast 拒绝,本场景
 *       连启动都过不去);本场景不能用 baseConfig 的该两键——Configuration 无删键,故自组
 *       最小配置复制 baseConfig 语义但排除两键({@link #defaultsInjectionConfig});</li>
 *   <li><b>快照模式拒绝</b>({@code snapshotModeInitialFailsStartup}):显式 snapshot.mode=
 *       initial → 引擎启动失败(基座失败通道 + awaitEngine 形态,复用
 *       {@code SlotTwoPhaseMismatchIT} 的既有形态),双面断言:链面含任务装配期拒绝的
 *       笼统文案("Error configuring an instance of PostgresStreamConnectorTask")、
 *       日志面(BaseSourceTask 的 ERROR 校验行)含 "never only"——1.9.7 的
 *       validateAndRecord 把校验问题记日志后抛<b>无 cause</b> 的笼统异常,拒绝文案
 *       不进链(3.6.1 异步引擎的链面在 1.9.7 不可达,注入层只 putIfAbsent,显式
 *       initial 原样透传);</li>
 *   <li><b>指标观测</b>({@code metricsObservableAfterTraffic}):baseConfig 启动 → 写若干
 *       事务消费到 → 断言 {@code StreamThroughputMetrics} 的 10s 统计 tick INFO 行可观测
 *       (吞吐行出现且输出段非零——流式源的 metrics 实例是 execute 内的私有字段,IT 不可达
 *       {@code throughputMetrics()};MBean 属性亦经 bridge 预计算且需两 tick 窗口,故以
 *       日志 ListAppender 为实际观测路径);</li>
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
    @TempDir(cleanup = CleanupMode.NEVER)
    Path pipeDir;

    /**
     * 指标三行(吞吐/分布/峰值)的打点 logger 名:TransactionConsumer 是包私有类,IT 包
     * 不能引类字面量,按 SLF4J 的名字寻址(slf4j getLogger(String) 与 getLogger(Class) 的
     * logger 名等价——均取全限定类名;1.9.7 线同类同包,打点名与 3.6.1 版一致)。
     */
    private static final String METRICS_LOGGER_NAME =
            "org.vastdata.debezium.connector.postgresql.stream.TransactionConsumer";

    /**
     * 任务装配期配置校验问题的 ERROR 打点 logger:1.9.7 的 {@code BaseSourceTask.start}
     * 经 {@code validateAndRecord} 把逐条校验问题记到该 logger(ERROR)后抛无 cause 的
     * 笼统 ConnectException——拒绝文案的日志面(场景②断言用),链面拿不到原文。
     */
    private static final String BASE_SOURCE_TASK_LOGGER_NAME = "io.debezium.connector.common.BaseSourceTask";

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
     * taskConfigs 注入后等价于 never + 事务元数据开。关键步骤:建表并<b>预插种子行</b>
     * (若注入缺失,snapshotMode() 经父 Field 默认回落 initial——任务侧启动校验/构造器
     * 即 fail-fast,本用例在启动段失败;种子行是注入正常路径下"零 op=r"断言的素材)→ 建单表
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
            try (Statement st = c.createStatement()) {
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
                        "缺省注入 never:零快照记录(op=r),种子行不被快照读出: " + describe(all));
            }
        }
        Struct begin = (Struct) txMeta.get(0).value();
        Struct end = (Struct) txMeta.get(1).value();
        assertEquals("BEGIN", begin.getString("status"), "首条事务块为 BEGIN");
        assertEquals("END", end.getString("status"), "次条事务块为 END");
        assertEquals(2L, end.getInt64("event_count"), "END 的 event_count=实付数据数(2)");
    }

    /**
     * 场景②:显式 snapshot.mode=initial 被启动期拒绝。<b>1.9.7 线的拒绝信号面(与
     * 3.6.1 的差异)</b>:任务装配期的配置校验失败经 {@code BaseSourceTask.start} 的
     * {@code validateAndRecord} 把逐条校验问题记 <b>ERROR 日志</b>(logger
     * {@code io.debezium.connector.common.BaseSourceTask})后抛<b>无 cause</b> 的笼统
     * {@code ConnectException("Error configuring an instance of ...")}——拒绝文案
     * "never only" 不进异常链(3.6.1 异步引擎的链面在 1.9.7 不可达,EmbeddedEngine
     * sources 实证)。故断言双面:<b>链面</b>失败信号非空且含任务装配期拒绝的笼统文案
     * (fail-fast 发生点),<b>日志面</b>挂 ListAppender 捕获 BaseSourceTask 的 ERROR 行
     * 断言含 "never only"(实际拒绝文案——本连接器 validateSnapshotMode 的输出,
     * 1.9.7 线文案,3.6.1 线为 "no_data only")。
     * 关键步骤:挂捕获器 → 以 baseConfig(已含 never)叠加 {@code .with("snapshot.mode",
     * "initial")} 覆盖 → 预声明预期失败后 start → awaitEngine 等引擎停机(失败即返回)→
     * 双面断言 → finally 摘捕获器。
     * 边界:注入层只 putIfAbsent,显式 initial 原样透传到任务侧校验——本场景正是该
     * 透传语义的验收面;失败发生于引擎线程,由 awaitEngine 的完成闩承载等待。
     */
    @Test
    void snapshotModeInitialFailsStartup() throws Exception {
        ListAppender<ILoggingEvent> appender = attachCaptureTo(BASE_SOURCE_TASK_LOGGER_NAME);
        try {
            expectEngineFailure();
            start(PostgresStreamConnector.class,
                    baseConfig(SLOT_INITIAL, PUB_PLACEHOLDER, pipeDir)
                            .with("snapshot.mode", "initial").build());
            awaitEngine();
            Throwable failure = engineFailure();
            assertNotNull(failure, "引擎必须以失败告终(残余到运行期才是缺陷)");
            String chain = renderThrowableChain(null, failure);
            assertTrue(chain.contains("Error configuring an instance of PostgresStreamConnectorTask"),
                    "失败应发生于任务装配期(1.9.7 校验失败的笼统链面文案;当前链: " + chain + ")");
            assertTrue(logLinesAtLevel(appender, Level.ERROR).stream().anyMatch(line -> line.contains("never only")),
                    "拒绝文案 never only 应经 BaseSourceTask 的 ERROR 校验日志出现(当前 ERROR 行: "
                            + logLinesAtLevel(appender, Level.ERROR) + ")");
        }
        finally {
            ((Logger) LoggerFactory.getLogger(BASE_SOURCE_TASK_LOGGER_NAME)).detachAppender(appender);
        }
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
            ((Logger) LoggerFactory.getLogger(METRICS_LOGGER_NAME)).detachAppender(appender);
        }
    }

    /**
     * 场景①的自组最小配置:逐项复制 {@link #baseConfig} 的流式验收语义,但<b>不含</b>
     * snapshot.mode 与 provide.transaction.metadata 两键——两键的缺省值正是本场景的验收
     * 对象(注入路径),配置里带了就验不到注入。为什么不用 baseConfig 再删键:
     * Configuration/Builder 无删键 API,{@code with(name, null)} 也非删除语义,只能自组。
     * 逻辑名键为 <b>database.server.name</b>(1.9.7 线——topic.prefix 是 2.0+ 键),值用
     * 独立的 ms5def,数据/事务元数据 topic 断言不与 baseConfig 前缀串台。
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
                .with("database.server.name", PREFIX_DEFAULTS)
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
     * 级别——INFO 过滤由 {@link #infoLines} 侧做(logback 的 logger 有效级别 INFO 由
     * logback-test.xml 的 org.vastdata.debezium 配置保证)。
     *
     * @return 已 start 的捕获器
     */
    private static ListAppender<ILoggingEvent> attachInfoCapture() {
        return attachCaptureTo(METRICS_LOGGER_NAME);
    }

    /**
     * 就地快照捕获器中的 INFO 格式化消息:委托 {@link #logLinesAtLevel} 的 INFO 档。
     *
     * @param appender 已挂载的捕获器
     * @return 当前已捕获 INFO 消息的副本(到达序)
     */
    private static List<String> infoLines(ListAppender<ILoggingEvent> appender) {
        return logLinesAtLevel(appender, Level.INFO);
    }

    /**
     * 挂载捕获器到指定名字的 logger(按名字寻址——打点类多为不可引类字面量的包私有/
     * 上游类),调用方 try/finally 摘除防泄漏到其他用例。ListAppender 自身不过滤级别。
     *
     * @param loggerName 目标 logger 的全限定名
     * @return 已 start 的捕获器
     */
    private static ListAppender<ILoggingEvent> attachCaptureTo(String loggerName) {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ((Logger) LoggerFactory.getLogger(loggerName)).addAppender(appender);
        return appender;
    }

    /**
     * 就地快照捕获器中指定级别的格式化消息:logback 的 doAppend 对 appender 实例加锁写,
     * 读侧同锁复制(logback ListAppender.list 为裸 ArrayList,无锁读有竞态)。
     *
     * @param appender 已挂载的捕获器
     * @param level    目标级别(如 INFO/ERROR)
     * @return 当前已捕获该级别消息的副本(到达序)
     */
    private static List<String> logLinesAtLevel(ListAppender<ILoggingEvent> appender, Level level) {
        List<String> out = new ArrayList<>();
        synchronized (appender) {
            for (ILoggingEvent e : appender.list) {
                if (e.getLevel() == level) {
                    out.add(e.getFormattedMessage());
                }
            }
        }
        return out;
    }
}
