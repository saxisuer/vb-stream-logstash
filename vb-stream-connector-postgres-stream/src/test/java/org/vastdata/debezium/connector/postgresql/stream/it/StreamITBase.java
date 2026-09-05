package org.vastdata.debezium.connector.postgresql.stream.it;

import io.debezium.config.Configuration;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * 连接器 IT 公共基类:继承 Debezium 3.6.1 测试基座 {@link AbstractAsyncEngineConnectorTest}
 * (tests classifier 提供——async 引擎是 3.x 唯一实现),提供三件事:
 * <ol>
 *   <li>{@code initializeConnectorTestFramework()} 每用例前清场(删 offset 文件、重建
 *       consumedLines 队列)——{@code start()} 会自动补 {@code name=testing-connector}、
 *       {@code connector.class}、{@code offset.storage.file.filename}(模块 target 下,
 *       单用例内跨 start/stop 保留,重启续传断言依赖此性质)与 offset flush 间隔;</li>
 *   <li>{@link #baseConfig} 组装连接器最小配置:数据库四件套取自 {@link StreamPgTestEnv}
 *       单例容器,流式档位 parallel + two_phase + 事务元数据开启 + 快照 no_data
 *       (3.6.1 的 SnapshotMode 枚举已无 never,no_data 是"只流式不快照"的现行值),
 *       pipe.dir 用绝对路径(相对路径按工作目录解析,跨机器不确定);</li>
 *   <li>每用例后兜底 {@code stopConnector()}:测试路径中途抛出时收敛引擎,避免残留
 *       walsender 占住复制槽(基座幂等,已停时只打一行日志)。</li>
 * </ol>
 * 槽/publication 由各 IT 自建自清(Task 7 的 start 无守门:publication 不预建,建流即报错;
 * 残留槽从旧 confirmed_flush 续传会吞掉先于建流的写入,故 {@code @BeforeEach} 清删是
 * it 包习语)。需要本机 Docker。
 */
abstract class StreamITBase extends AbstractAsyncEngineConnectorTest {

    /** 事务元数据 topic 的后缀(Debezium TransactionMonitor 约定:&lt;prefix&gt;.transaction)。 */
    static final String TX_TOPIC_SUFFIX = ".transaction";

    /**
     * 每用例前初始化测试基座:删旧 offset 文件、重建消费队列。必须先于 start 调用
     * (基座契约),本基类独占该 @BeforeEach 位,子类清理逻辑另挂同注解方法。
     */
    @BeforeEach
    void initializeFramework() {
        initializeConnectorTestFramework();
    }

    /**
     * 收集恰好 {@code expected} 条记录(基座 {@code consumeRecordsByTopic} 的免校验替身)。
     * 为什么不用基座原方法:其消费路径每条记录都过 {@code VerifyRecord.isValid},
     * 而该类在 JDK 17 下<b>链接期</b>即失败——方法字节码的调试局部变量表引用了
     * Confluent 的 MockSchemaRegistryClient,验证器解析局部变量类型要加载该类;
     * 坐标不在 Central(vanilla 自家测试经 Confluent 私仓带入),本模块不引私仓,
     * 故以基座的免校验四参重载(consumeRecords(n, maxWaits, consumer, check=false))
     * 复刻同语义,记录结构断言由各 IT 自持(与 VerifyRecord 的检查面等价或更强)。
     *
     * @param expected 期望到达的记录数(超时返回已到达的,由调用方断言数量)
     * @return 按到达序排列的记录列表
     */
    protected List<SourceRecord> consumeRecordsUnchecked(int expected) throws InterruptedException {
        List<SourceRecord> out = new java.util.ArrayList<>();
        consumeRecords(expected, waitTimeForRecordsAfterNulls(), out::add, false);
        return out;
    }

    /**
     * 就地取尽当前已到达的记录(非阻塞,不等待):直排基座的 consumedLines 队列
     * (engine 默认消费者写入),供 await 轮询式断言累计消费——记录总数不确定的
     * 场景(at-least-once 重复数未知)不能用按数消费。
     *
     * @param out 累计输出列表(方法把当前队列内容追加进来)
     */
    protected void drainArrivedRecords(List<SourceRecord> out) {
        consumedLines.drainTo(out);
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
        List<SourceRecord> out = new java.util.ArrayList<>();
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
     * 组装连接器最小可用配置(流式验收形态)。项:数据库四件套 + topic.prefix +
     * slot.name + publication.name + snapshot.mode=no_data + slot.streaming=parallel +
     * slot.two.phase=true + provide.transaction.metadata=true + pipe.dir(绝对路径,
     * MessagePipe wipe-on-open,每次引擎启动自清) + slot.feedback.interval.ms(默认 1s,
     * 阻塞类断言的观察窗口需要亚十秒反馈周期)。派生配置(如 max.queue.size 的阻塞
     * 构造)由调用方在返回的 Builder 上继续叠加。
     *
     * @param slotName    复制槽名(测试类专用,前后清删)
     * @param publication publication 名(IT 预建)
     * @param pipeDir     管道目录的绝对路径(每测试类独立,瞬态工作区)
     * @return 已填基础项的 Configuration.Builder(未 build,留调用方扩展)
     */
    protected Configuration.Builder baseConfig(String slotName, String publication, java.nio.file.Path pipeDir) {
        return Configuration.create()
                .with("database.hostname", StreamPgTestEnv.PG.getHost())
                .with("database.port", StreamPgTestEnv.PG.getMappedPort(org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT))
                .with("database.dbname", StreamPgTestEnv.PG.getDatabaseName())
                .with("database.user", StreamPgTestEnv.PG.getUsername())
                .with("database.password", StreamPgTestEnv.PG.getPassword())
                .with("topic.prefix", "ms2it")
                .with("slot.name", slotName)
                .with("publication.name", publication)
                .with("snapshot.mode", "no_data")
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
     * @return 可交给 {@code start(Class, Configuration, CompletionCallback, Predicate, Consumer, boolean)} 的逐条消费者
     */
    protected java.util.function.Consumer<SourceRecord> blockingConsumerAt(
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
     * (容量 max.queue.size)加上在途批次(max.batch.size + async 引擎内部少量缓冲)
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
        List<String> out = new java.util.ArrayList<>();
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
