package org.vastdata.debezium.connector.postgresql.stream.it;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;

/**
 * 1.9.7 线自建 Jupiter IT 基座(替代 3.6.1 的 {@code AbstractAsyncEngineConnectorTest}
 * ——后者是 JUnit 4 编译的 tests classifier 工件,Jupiter 测试无法继承;1.9.7 亦无
 * 3.x 的 async 引擎形态,同步 {@link EmbeddedEngine} 即官方实现)。
 *
 * <p><b>1.9.7 API 形态(javap 实证,与 3.x 的两处关键差异)</b>:①1.9.7 无
 * {@code io.debezium.engine.format.Connect}(2.0 才引入),故不走
 * {@code DebeziumEngine.create(格式)} 工厂——{@link EmbeddedEngine} 本身就实现
 * {@code DebeziumEngine<SourceRecord>},{@link EmbeddedEngine#create()} 返回的
 * {@code EmbeddedEngine.Builder} 即记录类型为裸 {@link SourceRecord} 的 builder;
 * ②{@code ChangeConsumer} 在 1.9.7 是 {@link DebeziumEngine} 的嵌套接口
 * {@link DebeziumEngine.ChangeConsumer}(2.x 才提为顶层类),其
 * {@code handleBatch(List&lt;SourceRecord&gt;, RecordCommitter)} 的元素即裸记录
 * (3.x 的 {@code RecordChangeEvent<SourceRecord>} 包装层在 1.9.7 不存在)。
 *
 * <p>职责:内装同步 embedded 引擎,专用线程 run({@code run()} 阻塞至引擎停机);
 * {@link DebeziumEngine.ChangeConsumer} 把批记录逐条入队并
 * {@code markProcessed}/{@code markBatchFinished} 手动 offset 记账;对外暴露与
 * 3.6.1 官方基座同名的最小方法面({@code start}/{@code stopConnector}/
 * {@code consumeRecords}/{@code consumeRecordsByTopic} +
 * {@link #initializeConnectorTestFramework()}),使后续 IT 翻译保持 import 级。
 * 停机/失败语义三分({@code AbstractStreamITContractTest} 钉子):
 * <b>stopConnector/awaitEngine 是纯停机等待</b>(未 start 为 no-op、已停幂等、
 * 卡死 60s 探测,<b>不隐式抛引擎失败</b>);<b>失败断言是显式动作</b>
 * ({@link #assertNoEngineFailure()},预期失败经 {@link #expectEngineFailure()}
 * 豁免);<b>失败信号是可读数据</b>({@link #engineFailure()},供用例断言异常链)。
 *
 * <p>引擎属性由基座统一注入(调用方 Configuration 只写连接器配置):{@code name}
 * (EmbeddedConfig 必填)、{@code connector.class}、{@code offset.storage}+
 * {@code offset.storage.file.filename}(FileOffsetBackingStore + 模块 target 下
 * 按测试类命名的文件——单用例内跨 start/stop 保留,重启续传断言依赖此性质;
 * {@link #initializeConnectorTestFramework()} 每用例删除)与
 * {@code offset.flush.interval.ms}(压到 100ms——重启续传断言要求 offset 及时落盘,
 * 1.9.7 默认 60s 会吞掉停机窗口前的提交)。
 *
 * <p>线程约束:start/stop/consume 均由测试线程调用;引擎线程只做 run 与记录入队;
 * {@code records} 队列入队(引擎线程)与 poll/清空(测试线程)均线程安全。
 */
public abstract class AbstractStreamIT {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamIT.class);

    /** 引擎输出的记录队列(ChangeConsumer 入队,测试线程 poll;3.6.1 基座 consumedLines 的对应物)。 */
    private final LinkedBlockingQueue<SourceRecord> records = new LinkedBlockingQueue<>();

    /** 引擎实例(run 期间非 null;每次 start 重建——单用例内多次 start/stop 的重启形态)。 */
    private DebeziumEngine<SourceRecord> engine;

    /** 引擎线程(每次 start 重建,与 engine 同生命周期)。 */
    private Thread engineThread;

    /** 完成闩:当前引擎 run() 返回(正常或异常,CompletionCallback 后 countDown);每次 start 重建。 */
    private volatile CountDownLatch stopped = new CountDownLatch(1);

    /** 引擎失败信号(成功/未启动为 null;经 CompletionCallback 捕获,volatile 供测试线程读)。 */
    private volatile Throwable engineFailure;

    /**
     * 预期失败标记({@link #expectEngineFailure()} 置位,须先于 start 调用):置位后
     * {@link #assertNoEngineFailure()}、{@link #consumeRecords(int, Consumer)} 的失败
     * 快速通道对捕获的失败<b>不再抛出</b>——失败断言权移交用例本身(经
     * {@link #engineFailure()} 取信号自断异常链);"启动期拒绝"类用例
     * (SlotTwoPhaseMismatchIT 形态)用例断言通过后 teardown 才能干净收敛。
     */
    private volatile boolean failureExpected;

    /**
     * 启动连接器:补齐引擎属性后构造 embedded 引擎并在专用线程 run。
     * <p>关键步骤:offset 文件父目录就绪 → 调用方配置 edit 后叠加引擎五件套
     * (name/connector.class/offset.storage/offset.storage.file.filename/
     * offset.flush.interval.ms——基座权威注入,调用方同名键被覆盖)→ 重置失败
     * 信号与完成闩(同用例内第二次 start 即重启形态的干净通道)→ builder 装配
     * (Configuration + CompletionCallback + ChangeConsumer + 测试类类加载器)
     * → 专用线程 run。</p>
     * <p>边界:offset 文件路径按测试类命名({@link #offsetFilePath()}),同模块多测试类
     * 并行不互踩;引擎构造失败原样上抛(配置缺失属装配错误,fail-fast)。</p>
     *
     * @param connectorClass 连接器实现类
     * @param config         连接器配置(连接器属性为主;引擎面属性由基座注入)
     */
    protected void start(Class<? extends SourceConnector> connectorClass, Configuration config) {
        Path offsetFile = offsetFilePath();
        try {
            Files.createDirectories(offsetFile.getParent());
        }
        catch (IOException e) {
            throw new IllegalStateException("offset 文件父目录创建失败: " + offsetFile.getParent(), e);
        }
        Configuration effective = config.edit()
                .with("name", "stream-it-engine")
                .with("connector.class", connectorClass.getName())
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetFile.toString())
                .with("offset.flush.interval.ms", 100)
                .build();
        engineFailure = null;
        stopped = new CountDownLatch(1);
        engine = EmbeddedEngine.create()
                .using(effective)
                .using((success, message, error) -> {
                    if (!success) {
                        engineFailure = error;
                        LOG.error("embedded 引擎异常停机: {}", message, error);
                    }
                    stopped.countDown();
                })
                .notifying((List<SourceRecord> batch, DebeziumEngine.RecordCommitter<SourceRecord> committer) -> {
                    for (SourceRecord record : batch) {
                        records.add(record);
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .using(getClass().getClassLoader())
                .build();
        engineThread = new Thread(engine::run, "stream-it-engine");
        engineThread.setDaemon(false);
        engineThread.start();
    }

    /**
     * 请求停机并等待引擎线程退出:{@code close()} 触发优雅停止({@code run()} 从 poll
     * 循环退出走 finally 的 connector stop/offset flush/CompletionCallback),闩到后 join。
     * <p>幂等与零成本路径:引擎已停(闩已开)时跳过 close 直接返回;引擎<b>从未
     * start</b>(engine 字段 null)时纯 no-op 立即返回(3.6.1 基座同语义——teardown
     * 对未启动引擎打日志即返回,不等待不抛错)。stopConnector <b>不隐式抛引擎失败</b>
     * (失败面归 {@link #assertNoEngineFailure()} 显式调用,见其 javadoc)。</p>
     *
     * <p>边界:等待 60s 仍未停即 AssertionError(引擎卡死属被测缺陷,与失败无关)。</p>
     */
    protected void stopConnector() {
        DebeziumEngine<SourceRecord> current = engine;
        if (current == null) {
            return; // 从未 start:teardown 兜底路径,纯 no-op
        }
        if (stopped.getCount() > 0) {
            try {
                current.close();
            }
            catch (Exception e) {
                throw new IllegalStateException("engine close failed", e);
            }
        }
        awaitEngine();
    }

    /**
     * 等待当前引擎停机:<b>纯等待语义,不做失败断言</b>——完成闩 60s 截止,未停即
     * AssertionError(引擎卡死探测,与失败信号无关);闩开即 join 引擎线程收尾。
     * 引擎从未 start 时直接返回(无可等)。失败路径用例的标准节奏:
     * {@code expectEngineFailure(); start(...); awaitEngine(); engineFailure()} 断言链。
     * 中断恢复中断位上抛。
     */
    protected void awaitEngine() {
        if (engineThread == null) {
            return; // 从未 start:无引擎可等
        }
        try {
            if (!stopped.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("engine 60s 内未停机(close 未生效或 run 卡死)");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        Thread runner = engineThread;
        if (runner != null) {
            try {
                runner.join(TimeUnit.SECONDS.toMillis(60));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * 断言引擎未因失败停机(长跑 IT 中途探测用;失败时抛 AssertionError 携原始异常)。
     * <b>预期失败豁免</b>:先经 {@link #expectEngineFailure()} 置位的用例,本方法对
     * 捕获到的失败不再抛出(失败断言权已移交用例,经 {@link #engineFailure()} 自取
     * 信号断异常链)——这是"启动期拒绝"类用例(SlotTwoPhaseMismatchIT 形态)用例
     * 断言通过后 teardown 干净收敛的前提。
     */
    protected void assertNoEngineFailure() {
        if (engineFailure != null && !failureExpected) {
            throw new AssertionError("engine failed", engineFailure);
        }
    }

    /**
     * 声明本用例预期引擎以失败告终(必须先于 {@code start()} 调用):置位预期失败
     * 标记——其后 {@link #assertNoEngineFailure()} 与 {@code @AfterEach} 兜底停机对
     * CompletionCallback 捕获的失败不再连带抛出;失败信号经 {@link #engineFailure()}
     * 暴露,由用例自行断言(典型:渲染异常链断言槽名/DROP SLOT 迁移指引)。
     * 标记在 {@link #initializeConnectorTestFramework()} 每用例重置。
     */
    protected void expectEngineFailure() {
        failureExpected = true;
    }

    /**
     * 当前引擎失败信号的读取面(CompletionCallback 捕获的原始异常,成功/未启动/尚未
     * 失败为 null)。失败路径用例经 Awaitility 轮询本方法或在 {@link #awaitEngine()}
     * 之后直接读取,再自行断言异常链内容。
     *
     * @return 捕获的失败异常;无失败信号为 null
     */
    protected Throwable engineFailure() {
        return engineFailure;
    }

    /**
     * 每用例前清场(与 3.6.1 基座同名同契约,由 {@code StreamITBase} 的
     * {@code @BeforeEach} 调用):清空记录队列、删除旧 offset 文件(残留 offset 会让
     * 重启类断言跨用例串台)、重置失败通道(失败信号与预期失败标记,下一用例 start
     * 前的干净基线)。必须先于 start 调用。
     */
    protected void initializeConnectorTestFramework() {
        records.clear();
        engineFailure = null;
        failureExpected = false;
        stopped = new CountDownLatch(1);
        try {
            Files.deleteIfExists(offsetFilePath());
        }
        catch (IOException e) {
            throw new IllegalStateException("旧 offset 文件删除失败: " + offsetFilePath(), e);
        }
    }

    /**
     * 消费至少 {@code minRecords} 条记录并对每条执行副作用(断言用),返回实付数。
     * <p>关键步骤:100ms 粒度 poll 队列,凑够即返回;60s 截止后原样返回已到达数
     * (3.6.1 基座同语义——超时由调用方断言数量,基座不越权抛错,记录总数不确定的
     * at-least-once 场景也复用同一方法);poll 间隙若引擎已携失败停机则立即抛出
     * (不等满 60s,失败诊断优先)。</p>
     *
     * @param minRecords 期望到达的最少记录数
     * @param consumer   每条记录的副作用(收集/断言;可为空操作)
     * @return 实际取到的记录数(超时返回已到达数)
     */
    protected int consumeRecords(int minRecords, Consumer<SourceRecord> consumer) throws InterruptedException {
        int consumed = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (consumed < minRecords) {
            SourceRecord record = records.poll(100, TimeUnit.MILLISECONDS);
            if (record != null) {
                consumed++;
                consumer.accept(record);
            }
            else if (System.nanoTime() > deadline) {
                return consumed;
            }
            else if (engineFailure != null && !failureExpected) {
                throw new AssertionError("engine 携失败停机,记录凑不齐(期望 " + minRecords
                        + ",已到 " + consumed + ")", engineFailure);
            }
        }
        return consumed;
    }

    /**
     * 消费至少 {@code minRecords} 条记录(无逐条副作用)。
     *
     * @param minRecords 期望到达的最少记录数
     * @return 实际取到的记录数(超时返回已到达数)
     */
    protected int consumeRecords(int minRecords) throws InterruptedException {
        return consumeRecords(minRecords, r -> {
        });
    }

    /**
     * 就地取尽当前已到达的记录(非阻塞,不等待):直排基座记录队列,供 await 轮询式
     * 断言累计消费——记录总数不确定的场景(at-least-once 重复数未知)不能用按数消费。
     * 单用例内跨多次 start(重启形态)队列不清空,累计语义与 3.6.1 的 consumedLines 一致。
     *
     * @param out 累计输出列表(方法把当前队列内容追加进来,保持到达序)
     */
    protected void drainArrivedRecords(List<SourceRecord> out) {
        records.drainTo(out);
    }

    /**
     * 消费 {@code numRecords} 条并按主题聚合(方法名与 3.6.1 基座对齐;聚合器为本基座
     * 嵌套 {@link SourceRecords})。
     *
     * @param numRecords 期望到达的记录数
     * @return 按主题分桶的记录集
     */
    protected SourceRecords consumeRecordsByTopic(int numRecords) throws InterruptedException {
        SourceRecords grouped = new SourceRecords();
        consumeRecords(numRecords, grouped::add);
        return grouped;
    }

    /**
     * 本用例的 offset 文件路径:模块 target/it-offsets/ 下按测试类简单名命名
     * (target 已 gitignore;测试类粒度使同模块并行测试类不互踩,单用例内跨
     * start/stop 保留、跨用例由 {@link #initializeConnectorTestFramework()} 删除)。
     *
     * @return offset 文件绝对或相对模块 basedir 的路径(surefire 工作目录即模块 basedir)
     */
    private Path offsetFilePath() {
        return Path.of("target", "it-offsets", getClass().getSimpleName() + ".dat");
    }

    /** 每用例后兜底停机(幂等),防引擎线程与 walsender 跨用例泄漏。 */
    @AfterEach
    void afterEachStop() {
        stopConnector();
    }

    /**
     * 按主题聚合的记录集(方法面与 3.6.1 基座 {@code SourceRecords} 对齐:主题分桶
     * 保持到达序,查询 API 为 {@link #recordsForTopic} 与 {@link #allRecords})。
     */
    protected static final class SourceRecords {

        /** 主题 → 该主题的到达序记录桶(LinkedHashMap 保持主题首现序)。 */
        private final Map<String, List<SourceRecord>> byTopic = new LinkedHashMap<>();

        /**
         * 记录一条到其主题桶。
         *
         * @param record 到达的记录
         */
        void add(SourceRecord record) {
            byTopic.computeIfAbsent(record.topic(), t -> new ArrayList<>()).add(record);
        }

        /**
         * 取指定主题的全部记录。
         *
         * @param topic 目标主题
         * @return 该主题的记录列表(保持到达序;无记录返回空列表)
         */
        public List<SourceRecord> recordsForTopic(String topic) {
            return byTopic.getOrDefault(topic, List.of());
        }

        /**
         * 全部主题的全部记录展平(保持到达序)。
         *
         * @return 展平记录列表
         */
        public List<SourceRecord> allRecords() {
            return byTopic.values().stream().flatMap(List::stream).toList();
        }
    }
}
