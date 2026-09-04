package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 流式变更源:coordinator 线程上的<b>监督壳</b>——把 MS2 管道(复制会话 + 异步组装器 +
 * reader/consumer 双线程)装配成 Debezium 的 {@link StreamingChangeEventSource} 形态。
 * 与 vanilla {@code PostgresStreamingChangeEventSource} 的结构对照(差异记档):
 * <ul>
 *   <li>vanilla 在本线程内联处理复制消息(walsender 解码 + dispatcher 直调);本连接器把
 *       读取/组装/回放解耦——本类只负责装配、生命周期监督与停机次序,事件交付全部在
 *       reader 线程('R' enrich,main JDBC 连接独占——R3)与 consumer 线程
 *       (DispatcherTransactionListener,R1)</li>
 *   <li><b>End 锚定 flush</b>(MS2 口径):复制会话的 LSN 反馈经输出前沿封顶
 *       ({@code session.run(assembler, frontier::get)})——前沿只在事务 End 之后单调推进,
 *       故确认位点"只前进不后退"由前沿单调性与 min 封顶共同保证;vanilla 的
 *       commitOffset→flushLsn 直推路径不复刻(见 {@link #commitOffset} 的口径注记)</li>
 *   <li>心跳:监督循环空转周期发 {@code dispatchHeartbeatEventAlsoToIncrementalSnapshot}
 *       (vanilla 在无消息迭代发——本连接器消息到达在 reader 线程,监督壳不可见,按周期发;
 *       heartbeat.interval 关闭时为 no-op);<b>仅监督线程调用</b>(R1)</li>
 *   <li>WAL 位点搜索(WalPositionLocator)/xmin 刷新/keep-alive 线程不复刻——重启续传锚定
 *       复制槽确认位点(START_REPLICATION 从 INVALID_LSN 起,服务端按槽续发),xmin
 *       MS2 不启(默认 0)</li>
 * </ul>
 *
 * <p>线程约束:execute/init/commitOffset/close 由 coordinator 线程串行调用;reader 与
 * consumer 线程由 execute 创建、{@link #stopStreaming()} 收敛(次序:session.close →
 * reader.join(5s) → assembler.shutdownFast——D7 快速停机,已提交未输出事务由复制槽
 * 重发,at-least-once)。reader/consumer 失败经 {@link #fail(Throwable)} 汇聚到
 * errorHandler(vanilla 同款出口;停机中 stopping 已置时忽略——shutdownFast 砸中在途回放
 * 属正常收敛,不上报,Connect 记 STOPPED 而非 FAILED)。
 */
public class PostgresStreamStreamingChangeEventSource
        implements StreamingChangeEventSource<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresStreamStreamingChangeEventSource.class);

    /** pgoutput 协议版本(引擎同款默认:v4,流式与 two_phase 需 ≥2)。 */
    private static final int PROTO_VERSION = 4;

    /** 监督循环周期(ms):isRunning 轮询 + 心跳发射节拍。 */
    private static final long SUPERVISE_INTERVAL_MILLIS = 200L;

    /** 停机时 reader 线程的 join 上限(ms):空轮间歇 ≤100ms,超时属异常路径。 */
    private static final long READER_JOIN_TIMEOUT_MILLIS = 5_000L;

    private final PostgresStreamConnectorConfig connectorConfig;
    private final PostgresEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final StreamPostgresSchema schema;
    /** main JDBC 连接('R' enrich 的元数据源;execute 装配后由 reader 线程独占,R3)。 */
    private final PostgresConnection mainConnection;
    private final TypeRegistry typeRegistry;

    /** 生效 offset(init 或 execute 置位;事务边界写点在 consumer 线程的 listener)。 */
    private volatile PostgresOffsetContext effectiveOffset;

    /** 失败标志(reader/consumer 任一失败置位,监督循环随即退出)。 */
    private final AtomicBoolean failed = new AtomicBoolean();

    /** 停机标志(stopStreaming 置位——reader 的会话关闭异常不按失败上报)。 */
    private volatile boolean stopping;

    /** 停机序列只跑一次的守卫。 */
    private final AtomicBoolean stopped = new AtomicBoolean();

    /** reader 线程(名 vb-pgoutput-reader,执行 session.run)。 */
    private Thread readerThread;

    /** 复制会话(open/ensureSlot/start 在 execute,run 归 reader 线程)。 */
    private ReplicationSession session;

    /** 异步组装器(consumer 线程随构造启动)。 */
    private StreamedTransactionAssembler assembler;

    /** 管线吞吐与分布指标(MS5:execute 装配点创建并注入组装器,四点插桩共用;Task 4 的
     *  bridge 以 {@link #throughputMetrics()} 为读源。volatile——coordinator 写、任意线程读;
     *  execute 前为 null)。 */
    private volatile StreamThroughputMetrics throughputMetrics;

    /**
     * 构造流式源(装配延迟到 init/execute)。
     *
     * @param connectorConfig 连接器配置(会话参数/管道参数/流式档位的来源)
     * @param dispatcher      事件出口
     * @param errorHandler    失败出口(reader/consumer 失败汇聚于此)
     * @param clock           时间戳时钟
     * @param schema          schema 组件(listener 版本安装目标)
     * @param mainConnection  main JDBC 连接(元数据 enrich 源)
     * @param typeRegistry    共享类型注册表(值映射的类型真源)
     */
    public PostgresStreamStreamingChangeEventSource(PostgresStreamConnectorConfig connectorConfig,
                                                    PostgresEventDispatcher<TableId> dispatcher,
                                                    ErrorHandler errorHandler, Clock clock,
                                                    StreamPostgresSchema schema,
                                                    PostgresConnection mainConnection,
                                                    TypeRegistry typeRegistry) {
        this.connectorConfig = Objects.requireNonNull(connectorConfig, "connectorConfig");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.schema = Objects.requireNonNull(schema, "schema");
        this.mainConnection = Objects.requireNonNull(mainConnection, "mainConnection");
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
    }

    /**
     * 责任:初始化——offsetContext 为 null(首启无存量 offset)时经
     * {@code PostgresOffsetContext.initialContext} 从 main 连接读当前 xlog 位点建初始上下文
     * (vanilla init 同款;此时 reader 线程尚未创建,与 main 连接的 reader 独占不冲突——
     * 时序独占)。读后随即 commit 收敛 main 连接(autoCommit=false)上被
     * {@code txid_current()} 强制分配了 XID 的只读事务——不收敛则复制会话在另一条连接上的
     * {@code pg_create_logical_replication_slot} 会为等解码一致点而等它,自死锁。
     * 边界:库查询失败抛 ConnectException(initialContext 语义);commit 失败同样
     * ConnectException(fail-fast)。
     */
    @Override
    public void init(PostgresOffsetContext offsetContext) {
        this.effectiveOffset = offsetContext != null ? offsetContext : initialOffsetWithCommit();
    }

    /**
     * 责任:装配并监督 MS2 管道直至停机/失败。关键步骤:offset 定形(execute 入参优先)→
     * 建/开/启动复制会话(open→ensureSlot→start,幂等建槽)→ 建吞吐指标实例(MS5,首窗
     * 基线取当前 nanoTime)→ 建异步组装器(listener =
     * DispatcherTransactionListener,包私有 11 参构造注入 listener 侧表解析接缝与吞吐指标
     * ——tableResolver 必须与 listener 构造时的<b>同一实例</b>,consumer 每桶 bind 的与
     * listener 读的经它共享)→ 起
     * vb-pgoutput-reader 线程跑 {@code session.run(assembler, frontier::get)}(LSN 反馈按
     * 输出前沿封顶 = End 锚定)→ 监督循环(isRunning && !failed,空转周期发心跳)→
     * 退出后走停机次序。边界:装配/监督期任何 Throwable 经 {@link #fail} 汇聚
     * errorHandler(vanilla 同款——异常不外泄给 coordinator,经 queue.poll 呈报);
     * reader/consumer 失败置 failed 收敛监督循环。
     */
    @Override
    public void execute(ChangeEventSourceContext context, PostgresPartition partition,
                        PostgresOffsetContext offsetContext) {
        if (offsetContext != null) {
            this.effectiveOffset = offsetContext;
        }
        try {
            PostgresOffsetContext offset = this.effectiveOffset != null ? this.effectiveOffset : initialOffsetWithCommit();
            this.effectiveOffset = offset;

            session = new ReplicationSession(sessionParameters());
            session.open();
            session.ensureSlot();
            session.start();

            AtomicLong frontier = new AtomicLong();
            BucketTableResolver tableResolver = BucketTableResolver.snapshotBacked();
            DispatcherTransactionListener listener = new DispatcherTransactionListener(
                    partition, offset, dispatcher, schema, clock, connectorConfig,
                    new TypeRegistryColumnValueMapper(typeRegistry,
                            connectorConfig.getConfig().getBoolean(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES)),
                    tableResolver);
            this.throughputMetrics = new StreamThroughputMetrics(System.nanoTime());
            assembler = new StreamedTransactionAssembler(listener, connectorConfig.streamingMode(),
                    new VersionedRelationRegistry(),
                    new RelationTableFactory(RelationMetadataSource.jdbc(mainConnection,
                            typeRegistry, connectorConfig.getColumnFilter())),
                    Path.of(connectorConfig.pipeDir()), connectorConfig.rollCycle(),
                    (msg, view) -> { }, frontier,
                    () -> fail(new DebeziumException("consumer 回放失败,已 fail-fast(细节见 transaction-consumer 的 ERROR 日志)")),
                    tableResolver, this.throughputMetrics);

            readerThread = new Thread(() -> runReader(frontier), "vb-pgoutput-reader");
            readerThread.setDaemon(false);
            readerThread.start();
            LOG.info("流式管道已启动: 槽={} publication={} streaming={} twoPhase={}",
                    connectorConfig.slotName(), connectorConfig.publicationName(),
                    connectorConfig.streamingMode(), connectorConfig.twoPhase());

            supervise(context, partition);
        }
        catch (Throwable t) {
            fail(t);
        }
        finally {
            stopStreaming();
        }
    }

    /**
     * 责任:监督循环——isRunning 且未失败期间周期发心跳并让出 CPU。关键步骤:心跳经
     * {@code dispatchHeartbeatEventAlsoToIncrementalSnapshot}(仅本监督线程调用,R1;
     * heartbeat.interval 关闭时内部 no-op);sleep 被中断即恢复中断位退出(停机请求)。
     * 边界:failed 由 reader/consumer 失败路径异步置位,退出后交停机次序收敛。
     */
    private void supervise(ChangeEventSourceContext context, PostgresPartition partition) {
        while (context.isRunning() && !failed.get()) {
            try {
                dispatcher.dispatchHeartbeatEventAlsoToIncrementalSnapshot(partition, effectiveOffset);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                Thread.sleep(SUPERVISE_INTERVAL_MILLIS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * 责任:reader 线程主体——跑复制会话消息循环至流关闭/异常。边界:run 抛出的任何
     * Throwable:停机中(stopping)属正常收敛只 DEBUG;否则经 {@link #fail} 上报。
     *
     * @param frontier 输出前沿(会话 LSN 反馈封顶的读源)
     */
    private void runReader(AtomicLong frontier) {
        try {
            session.run(assembler, frontier::get);
        }
        catch (Throwable t) {
            if (stopping) {
                LOG.debug("reader 随停机退出: {}", t.getMessage());
            }
            else {
                fail(t);
            }
        }
    }

    /** 最近一次 consumer 失败的暂存位(onFailure 无参回调,失败细节已由 consumer ERROR 留痕)。 */
    private volatile Throwable lastFailure;

    /**
     * 责任:失败汇聚——置 failed(收敛监督循环)并上报 errorHandler(vanilla 出口;
     * Connect runtime 经 queue.poll 感知后停任务)。停机中(stopping 已置)的一切失败
     * 按正常收敛忽略(DEBUG 留痕):D7 的 shutdownFast 关管道会砸中在途回放,consumer 的
     * onFailure→fail() 若无此守卫会把正常停机当真失败上报——Connect 记 FAILED 而非
     * STOPPED(终审修复;runReader 的同款守卫由此统一收敛到本方法)。
     * 边界:多次调用可能重复上报(reader 与 consumer 同时失败时),errorHandler 以
     * 首报为准不影响停机语义。
     */
    private void fail(Throwable t) {
        if (stopping) {
            LOG.debug("停机期失败忽略(正常收敛的一部分): {}", t.getMessage());
            return;
        }
        failed.set(true);
        lastFailure = t;
        LOG.error("流式源失败,停机收敛: {}", t.getMessage(), t);
        errorHandler.setProducerThrowable(t);
    }

    /**
     * 责任:停机次序(D7,幂等):session.close() 断流(reader 的 run 循环 ≤100ms 内退出,
     * 此时 stopping 已置,会话关闭异常不按失败上报)→ reader.join(5s)(超时 WARN 放行,
     * reader 非守护)→ assembler.shutdownFast()(毒丸 + interrupt,不排干——未输出事务
     * 由复制槽重发)。边界:execute 未装配完成(部分字段 null)时按已有的收敛;
     * join 被中断恢复中断位继续shutdownFast(不因停机中断丢掉快速停机)。
     */
    private synchronized void stopStreaming() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        stopping = true;
        if (session != null) {
            session.close();
        }
        if (readerThread != null) {
            try {
                readerThread.join(READER_JOIN_TIMEOUT_MILLIS);
                if (readerThread.isAlive()) {
                    LOG.warn("reader 线程 {}ms 内未退出,放行(非守护线程,随 JVM 收敛)", READER_JOIN_TIMEOUT_MILLIS);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("停机 join reader 被中断,继续快速停机");
            }
        }
        if (assembler != null) {
            assembler.shutdownFast();
        }
    }

    /**
     * 责任:响应框架的 offset 提交(vanilla :498-562 的简化)。口径(MS2 End 锚定):
     * 取 offset 的 lsn_commit(缺省回落 lsn_proc)推进内部<b>单调水位</b>(只前进不后退,
     * AtomicLong max)——服务端 LSN 确认不由此直推,而由复制会话的反馈封顶
     * (min(已收到, 输出前沿))承担:前沿只在事务 End 后推进,故"只前进不后退"由前沿
     * 单调性保证,本水位记录框架已确认位置(诊断/后续里程碑的 flush 收紧依据)。
     * 边界:offset 为 null、两个 LSN 键缺失或非数值直接忽略(DEBUG);跨线程并发调用
     * (Connect 提交线程)只碰 AtomicLong,安全。
     */
    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        if (offset == null) {
            return;
        }
        Long commitLsn = asLong(offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
        Long processedLsn = asLong(offset.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
        long lsn = commitLsn != null ? commitLsn : (processedLsn != null ? processedLsn : -1L);
        if (lsn <= 0L) {
            LOG.debug("offset 提交无可用 LSN,忽略: {}", offset);
            return;
        }
        long previous = committedWatermark.getAndAccumulate(lsn, Math::max);
        LOG.debug("框架 offset 提交: lsn_commit={}（单调水位 {}→{}；服务端确认由输出前沿封顶承担）",
                lsn, previous, Math.max(previous, lsn));
    }

    /** 框架已确认位点的单调水位(只前进不后退;End 锚定 flush 的诊断面)。 */
    private final AtomicLong committedWatermark = new AtomicLong();

    /**
     * 责任:取当前生效 offset(coordinator 的 signal 处理器上下文用)。边界:init 前为 null。
     */
    @Override
    public PostgresOffsetContext getOffsetContext() {
        return effectiveOffset;
    }

    /**
     * 责任:取管线吞吐与分布指标(MS5,只读访问口——Task 4 的 MBean bridge 读源之一:
     * 六计数 totals 快照作窗口差分分子)。返回的是组装器构造注入的同一实例,四点插桩
     * (slot 读取/组装/回放字节/输出+分布)与 consumer 的 10s 报告 tick 全部落在这上面。
     * 边界:execute 装配前为 null(指标随管道一起诞生,调用方须以 execute 完成为前提)。
     */
    public StreamThroughputMetrics throughputMetrics() {
        return throughputMetrics;
    }

    /**
     * 责任:收敛资源(close 协议)。边界:execute 未跑(装配失败在 start 阶段)时各字段
     * null 安全跳过;幂等(stopped 守卫)。
     */
    @Override
    public void close() {
        stopStreaming();
    }

    /**
     * 责任:从配置组装会话参数(host/port/database/user/password 取 JDBC 配置面,
     * slot/publication/proto/streaming/twoPhase/feedback/messages 取本模块配置面)。
     * 边界:纯函数;proto 固定 v4(流式与 two_phase 需 ≥2,引擎同款默认)。
     */
    private ReplicationSession.Parameters sessionParameters() {
        var jdbc = connectorConfig.getJdbcConfig();
        return new ReplicationSession.Parameters(jdbc.getHostname(), jdbc.getPort(), jdbc.getDatabase(),
                jdbc.getUser(), jdbc.getPassword(), connectorConfig.slotName(),
                connectorConfig.publicationName(), PROTO_VERSION, connectorConfig.streamingMode(),
                connectorConfig.twoPhase(), connectorConfig.feedbackIntervalSeconds(),
                connectorConfig.messagesEnabled());
    }

    /**
     * 责任:建初始 offset 并随即 commit 收敛 main 连接上的读事务。initialContext 的
     * {@code txid_current()} 会给 main 连接(autoCommit=false)强制分配 XID,不提交则
     * 该事务悬置到 doStop——复制会话在另一条连接上的 {@code pg_create_logical_
     * replication_slot} 为等解码一致点会等所有进行中 XID 事务,连接器自死锁
     * (Task 8 IT 首跑实测)。线程约束:调用点在 reader 线程创建之前(时序独占,R3)。
     * 边界:查询/commit 失败抛 ConnectException(fail-fast,装配失败由 execute 的
     * fail 汇聚或 init 直抛)。
     */
    private PostgresOffsetContext initialOffsetWithCommit() {
        PostgresOffsetContext initial = PostgresOffsetContext.initialContext(connectorConfig, mainConnection, clock);
        try {
            mainConnection.commit();
        }
        catch (java.sql.SQLException e) {
            throw new org.apache.kafka.connect.errors.ConnectException(
                    "Failed to commit the initial offset read on the main connection", e);
        }
        return initial;
    }

    /**
     * 责任:宽松数值转换——Number 直取 long,其余(含 null)返回 null。
     */
    private static Long asLong(Object value) {
        return value instanceof Number number ? number.longValue() : null;
    }
}
