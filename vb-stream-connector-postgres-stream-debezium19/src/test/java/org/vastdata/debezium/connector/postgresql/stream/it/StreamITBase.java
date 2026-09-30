package org.vastdata.debezium.connector.postgresql.stream.it;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.config.Configuration;

/**
 * 连接器 IT 公共基类(1.9.7 线):继承自建 Jupiter 基座 {@link AbstractStreamIT}
 * (3.6.1 线的 {@code AbstractAsyncEngineConnectorTest} 是 JUnit 4 工件无法继承,
 * 基座方法面已按同名对齐——start/stopConnector/consumeRecords/consumeRecordsByTopic
 * /initializeConnectorTestFramework/drainArrivedRecords),提供三件事:
 * <ol>
 *   <li>{@code initializeFramework()} 每用例前清场(删 offset 文件、清消费队列、
 *       重置失败通道)——基座 {@code start()} 会自动补 {@code name}、
 *       {@code connector.class}、{@code offset.storage}+{@code offset.storage.file.filename}
 *       (模块 target 下按测试类命名,单用例内跨 start/stop 保留,重启续传断言依赖此
 *       性质)与 {@code offset.flush.interval.ms}=100ms(及时落盘,同上);</li>
 *   <li>{@link #baseConfig} 组装连接器最小配置:数据库四件套取自 {@link StreamPgTestEnv}
 *       单例容器,流式档位 parallel + two_phase + 事务元数据开启 + 快照 never
 *       (1.9.7 线:逻辑名键为 <b>database.server.name</b>——topic.prefix 是 2.0+ 键,
 *       1.9.7 的 RelationalBaseSourceConnector 不识别,缺键即启动失败;snapshot.mode
 *       无 no_data 档,本连接器同名替换 Field 仅接受 never),pipe.dir 用绝对路径
 *       (相对路径按工作目录解析,跨机器不确定);</li>
 *   <li>每用例后兜底 {@code stopConnector()}:测试路径中途抛出时收敛引擎,避免残留
 *       walsender 占住复制槽(基座幂等,已停时直接返回)。</li>
 * </ol>
 * 槽/publication 由各 IT 自建自清(start 无守门:publication 不预建,建流即报错;
 * 残留槽从旧 confirmed_flush 续传会吞掉先于建流的写入,故 {@code @BeforeEach} 清删是
 * it 包习语)。需要本机 Docker。
 */
abstract class StreamITBase extends AbstractStreamIT {

    /** 事务元数据 topic 的后缀(Debezium TransactionMonitor 约定:&lt;prefix&gt;.transaction)。 */
    static final String TX_TOPIC_SUFFIX = ".transaction";

    /**
     * 每用例前初始化测试基座:删旧 offset 文件、清空消费队列、重置失败通道。必须先于
     * start 调用(基座契约),本基类独占该 @BeforeEach 位,子类清理逻辑另挂同注解方法。
     */
    @BeforeEach
    void initializeFramework() {
        initializeConnectorTestFramework();
    }

    /**
     * 收集恰好 {@code expected} 条记录(基座 {@code consumeRecords(int, Consumer)} 的
     * 收集形态)。3.6.1 线经免校验四参重载绕开 VerifyRecord 的 JDK 17 链接期 Confluent
     * 依赖问题;自建基座本就无 VerifyRecord 面,语义收敛为:60s 截止凑数,超时返回
     * 已到达的(基座同 3.6.1 不越权抛错),由调用方断言数量,记录结构断言由各 IT 自持。
     *
     * @param expected 期望到达的记录数(超时返回已到达的,由调用方断言数量)
     * @return 按到达序排列的记录列表
     */
    protected List<SourceRecord> consumeRecordsUnchecked(int expected) throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        consumeRecords(expected, out::add);
        return out;
    }

    /**
     * 按 topic 过滤记录({@code SourceRecords.recordsForTopic} 的列表形态替身,
     * 与 {@link #consumeRecordsUnchecked} 配套):保持到达序。
     *
     * @param records 全部记录
     * @param topic   目标 topic
     * @return 该 topic 的记录子列表(保持序;无命中为空列表)
     */
    protected static List<SourceRecord> recordsForTopic(List<SourceRecord> records, String topic) {
        List<SourceRecord> out = new ArrayList<>();
        for (SourceRecord r : records) {
            if (topic.equals(r.topic())) {
                out.add(r);
            }
        }
        return out;
    }

    /**
     * 每用例后兜底停引擎:断言中途抛出时也收敛(引擎停 → 任务 stop → 流式源
     * stopStreaming 断复制流),否则残留 walsender 会占住槽使下个用例的 drop 失败。
     * 已停(用例经 {@link #stopEngineAndDropSlot} 收敛过)时为幂等 no-op。注意
     * JUnit 的超类 @AfterEach 在子类之后跑——次序敏感的清理必须走
     * {@link #stopEngineAndDropSlot}(先停引擎后删槽),本方法只是防漏兜底。
     */
    @AfterEach
    void stopEngineQuietly() {
        stopConnector();
    }

    /**
     * 次序敏感的用例尾清理:先 {@code stopConnector()}(引擎停 → 任务 doStop → 流式源
     * stopStreaming:session.close → reader.join → assembler.shutdownFast),等引擎
     * 完全退出后再删槽。若先删槽,dropSlotQuietly 的 pg_terminate_backend 会杀掉仍在
     * 跑的 walsender,reader 的复制流读出 EOF 被当作失败上报(ERROR 噪声 + 停机路径
     * 走错分支);先停引擎则 reader 随 session.close 的 isClosed 守卫干净退出。
     *
     * @param slotName 本用例的槽名
     */
    protected void stopEngineAndDropSlot(String slotName) {
        stopConnector();
        StreamPgTestEnv.dropSlotQuietly(slotName);
    }

    /**
     * 组装连接器最小可用配置(流式验收形态)。项:数据库四件套 +
     * database.server.name(1.9.7 线的逻辑名键——2.0+ 才改名 topic.prefix,值沿用
     * ms2it 使 IT 内的 topic 常量字面量跨线不变)+ slot.name + publication.name +
     * snapshot.mode=never(本连接器同名替换 Field 的唯一合法值)+ slot.streaming=parallel +
     * slot.two.phase=true + provide.transaction.metadata=true + pipe.dir(绝对路径,
     * MessagePipe wipe-on-open,每次引擎启动自清)+ slot.feedback.interval.ms(默认 1s,
     * 阻塞类断言的观察窗口需要亚十秒反馈周期)。派生配置(如 max.queue.size 的阻塞
     * 构造)由调用方在返回的 Builder 上继续叠加。
     *
     * @param slotName    复制槽名(测试类专用,前后清删)
     * @param publication publication 名(IT 预建)
     * @param pipeDir     管道目录的绝对路径(每测试类独立,瞬态工作区)
     * @return 已填基础项的 Configuration.Builder(未 build,留调用方扩展)
     */
    protected Configuration.Builder baseConfig(String slotName, String publication, Path pipeDir) {
        return Configuration.create()
                .with("database.hostname", StreamPgTestEnv.PG.getHost())
                .with("database.port", StreamPgTestEnv.PG.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
                .with("database.dbname", StreamPgTestEnv.PG.getDatabaseName())
                .with("database.user", StreamPgTestEnv.PG.getUsername())
                .with("database.password", StreamPgTestEnv.PG.getPassword())
                .with("database.server.name", "ms2it")
                .with("slot.name", slotName)
                .with("publication.name", publication)
                .with("snapshot.mode", "never")
                .with("slot.streaming", "parallel")
                .with("slot.two.phase", true)
                .with("provide.transaction.metadata", true)
                .with("slot.feedback.interval.ms", 1000)
                .with("pipe.dir", pipeDir.toAbsolutePath().toString());
    }

    /**
     * 阻塞消费构造器:前 {@code blockAfter} 条记录照常收集进 sink,第
     * {@code blockAfter+1} 条到达时置位 {@code blockedStarted} 并 await {@code release}
     * ——输出路径(engine 的记录处理线程 → 任务 ChangeEventQueue 满 → 连接器 consumer
     * 线程在 dispatch 的 enqueue 上阻塞)从该点停摆,而 reader 线程不受影响(解耦头名
     * 验收的构造核心)。线程约束:accept 由 engine 的记录处理线程串行调用,sink 用
     * CopyOnWriteArrayList 供测试线程轮询读。
     *
     * @param blockAfter     放行的记录条数(其后第一条触发阻塞)
     * @param release        放行闩(测试在断言完阻塞窗口后 countDown)
     * @param blockedStarted 阻塞已进入的信号(第 blockAfter+1 条到达时置位)
     * @param sink           已消费记录的收集列表(测试线程读)
     * @return 可交给基座 {@code consumeRecords(int, Consumer)} 的逐条消费者
     */
    protected Consumer<SourceRecord> blockingConsumerAt(
            int blockAfter, CountDownLatch release, CountDownLatch blockedStarted, List<SourceRecord> sink) {
        return record -> {
            sink.add(record);
            if (sink.size() == blockAfter + 1) {
                blockedStarted.countDown();
                try {
                    release.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // engine 停机中断:放行退出,不留卡死线程
                }
            }
        };
    }

    /**
     * 阻塞类 IT 的派生配置项:小队列/小批量——下游阻塞后,任务侧 ChangeEventQueue
     * (容量 max.queue.size)加上在途批次(max.batch.size + engine 内部少量缓冲)
     * 很快装满,连接器 consumer 线程随即在 enqueue 上阻塞(前沿冻结)。若用默认
     * 8192/2044 需要万级记录才能填满,阻塞点不可达。
     *
     * @param builder 基础配置 Builder(baseConfig 的返回值)
     * @return 已叠加小队列参数的 Builder
     */
    protected Configuration.Builder withSmallQueue(Configuration.Builder builder) {
        return builder.with("max.queue.size", 8).with("max.batch.size", 4);
    }

    /**
     * 记录的一行摘要(失败消息诊断面):topic/op(或事务 status)/txId/键/事务边界
     * lsn——不打印值体(16KB 载荷会刷爆输出)。空值各段以 "-" 占位;数据记录与
     * 事务元数据记录的字段面不同,各自按存在的字段取。
     *
     * @param records 到达记录
     * @return 每条一行的摘要列表
     */
    protected static List<String> describe(List<SourceRecord> records) {
        List<String> out = new ArrayList<>();
        for (SourceRecord r : records) {
            String detail;
            if (r.value() instanceof Struct s && s.schema().field("op") != null) {
                String txId = s.getStruct("source") != null && s.getStruct("source").getInt64("txId") != null
                        ? String.valueOf(s.getStruct("source").getInt64("txId")) : "-";
                detail = "op=" + s.getString("op") + " txId=" + txId;
            }
            else if (r.value() instanceof Struct s && s.schema().field("status") != null) {
                detail = "tx=" + s.getString("id") + " status=" + s.getString("status");
            }
            else {
                detail = "value=" + r.value();
            }
            out.add(r.topic() + " " + detail + " key=" + r.key()
                    + " lsn_commit=" + r.sourceOffset().get("lsn_commit"));
        }
        return out;
    }

    /**
     * 渲染失败回调的完整异常链文本(回调 msg + 沿 cause 链逐层消息拼接):引擎侧失败被
     * 层层包装(如 ConnectException → IllegalStateException 原文,或默认值校验的多层
     * 包装),槽名/拒绝文案/DROP SLOT 指引等关键信息常在链尾——单层 getMessage 断言
     * 会漏,必须全链拼接后做包含断言。
     *
     * @param message 回调的消息参数(可能为 null)
     * @param error   回调的异常参数(可能为 null)
     * @return msg 与各层 cause 消息以 " | " 连接的文本(null 段跳过)
     */
    protected static String renderThrowableChain(String message, Throwable error) {
        StringBuilder sb = new StringBuilder(String.valueOf(message));
        Throwable t = error;
        while (t != null) {
            sb.append(" | ").append(t.getMessage());
            t = t.getCause();
        }
        return sb.toString();
    }
}
