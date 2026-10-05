package org.vastdata.vbstream.walsource.receive;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * pgjdbc 物理复制流接收器：后台线程维持 WAL 字节流，经 {@link WalStreamWalker} 解析为
 * {@link WalRecord} 序列交付 sink，断流自动从消费前沿重连。
 *
 * <p>移植自 spike {@code WalParseSpike.main} 的连接/feed 段（spike/wal-parse），重连循环为本任务
 * 新增。数据面：复制 URL 带 {@code replication=database&assumeMinServerVersion=9.4}（缺后者
 * replication 参数被驱动静默丢弃）→ {@code replicationStream().physical().withStartPosition(...)
 * .withStatusInterval(1s).start()} → readPending drain 轮询（null 空轮睡 100ms——spike 节拍；
 * 每轮一条+固定睡会把读取上限钉死 ~10 msg/s，见引擎 1.7 吐噬踩坑）→ chunk 字节喂
 * walker（地址锚由 walker 自维护数据末位承担——{@code getLastReceiveLSN()} 会随
 * keepalive 的 walEnd 跳变，采信它会在 keepalive/data 交错时假再同步，Task 13
 * 对拍 IT 实证）。起点按页边界下取整（walker 页导航假设流首是页头；
 * 多收的页首段记录由调用方按 LSN 窗口过滤）。</p>
 *
 * <p><strong>已知限制（pgjdbc 42.7.13）</strong>：物理流构造器无槽选项——发出的命令是
 * {@code START_REPLICATION PHYSICAL <lsn>}（不带 SLOT，spike 注记 + javap 核实）。slotName
 * 当前仅作日志/身份标识与 {@link PhysicalSlotManager} 的位点锚载体，流本身不附槽：槽的
 * restart_lsn 不会随消费推进、WAL 保留由 {@code max_slot_wal_keep_size} 兜底；槽绑流
 * （手工协议命令或驱动升级）留待生命周期任务裁定。续传正确性不受影响——重连起点取本接收器
 * 的消费前沿（walker.consumedLsn），不依赖槽状态。</p>
 *
 * <p>断流重连：IO 异常（SQLException）→ 从 consumedLsn 重连，指数退避 1s/2s/4s/8s/16s；
 * 连续失败 5 次后 ERROR 停机（流数据损坏的 RuntimeException 属确定性失败，重连无益——
 * ERROR 后线程退出，不吞异常面）。成功收到数据即重置失败计数（连接级抖动不累计成停机）。
 * 重连起点契约（与 walker 双侧约定）：consumedLsn 是<strong>记录对齐</strong>（上条记录
 * MAXALIGN 后）而非页对齐位点——walker 的页内直通路（首 chunk 不在页界时直接走页内
 * 记录头分支）恰好接受这种重入点，依赖正是 consumedLsn 恰落在记录边界的语义保证；若
 * 断流时 carry 挂着半条记录，服务端自 consumedLsn（= carry 首字节）重发，feed 的衔接
 * 校验会走一次 carry 丢弃+重锚（carryDrops 计数），数据不丢不重。首次建流即失败
 * （walker 从未 feed）时 resume 保留页对齐初值而非 0 哨兵。
 * 周期反馈：每 5s 或每 1000 条 {@code setFlushedLSN(consumedLsn) + forceUpdateStatus}。</p>
 *
 * <p>线程约束：接收线程（名 wal-receiver，非守护）独占 walker/feed 与流游标读写；
 * {@link #consumedLsn()} 经 volatile 镜像供任意线程读取；{@link #stop()} 可从任意线程调用
 * （幂等：置停机位 + 关流关连接解除阻塞 + join 3s 超时）。</p>
 */
public final class WalStreamReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(WalStreamReceiver.class);

    /** 重连退避基数（1s），逐次翻倍。 */
    private static final long RECONNECT_BACKOFF_BASE_MS = 1_000;

    /** 重连退避上限（16s——第 5 次退避档）。 */
    private static final long RECONNECT_BACKOFF_MAX_MS = 16_000;

    /** 连续重连失败上限，超出后 ERROR 停机。 */
    private static final int MAX_RECONNECT_ATTEMPTS = 5;

    /** 周期反馈间隔（毫秒）——flush LSN 上报下限频率。 */
    private static final long FEEDBACK_INTERVAL_MS = 5_000;

    /** 周期反馈的记录数阈值（条）。 */
    private static final long FEEDBACK_RECORD_THRESHOLD = 1_000;

    /** readPending 空轮的睡眠（毫秒）——drain 轮询节拍（spike 实证形态）。 */
    private static final long IDLE_POLL_SLEEP_MS = 100;

    /** stop() 对接收线程的 join 超时（毫秒）。 */
    private static final long STOP_JOIN_TIMEOUT_MS = 3_000;

    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String pass;
    private final WalLayout layout;
    private final String slotName;

    /** 接收器自有计数器（Task 7 产物）——{@link #metrics()} 暴露只读观测。 */
    private final WalStreamMetrics metrics = new WalStreamMetrics();

    /** 走读器（start 时创建，接收线程独占）。 */
    private WalStreamWalker walker;

    /** 消费前沿的跨线程镜像——接收线程每次 feed 后刷新，任意线程读。 */
    private volatile long consumedLsn;

    /** 停机请求位——stop() 置位，接收线程各循环边界检查。 */
    private volatile boolean stopRequested;

    /** start/stop 的单程闸门（synchronized 方法互斥保护）。 */
    private boolean started;
    private boolean stopped;

    /** 接收线程句柄（start 时创建）。 */
    private Thread worker;

    /** 活跃复制流与其连接——接收线程独占写，stop() 经 closeStreamQuietly 从外部线程关闭。 */
    private volatile PGReplicationStream stream;
    private volatile Connection connection;

    /**
     * 装配一个接收器（内部拼复制 URL）。
     *
     * @param host     PG 主机
     * @param port     PG 端口
     * @param database 库名
     * @param user     用户（须有 REPLICATION 权限）
     * @param pass     密码
     * @param layout   版本布局描述符（walker 的页/记录尺寸来源）
     * @param slotName 槽名（当前不附流——见类 javadoc 已知限制；作身份标识与位点锚）
     */
    public WalStreamReceiver(String host, int port, String database, String user, String pass,
                             WalLayout layout, String slotName) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.pass = pass;
        this.layout = layout;
        this.slotName = slotName;
    }

    /**
     * 计数器观测面（records/resyncs/contrecords/orphanSkips/carryDrops/census）。
     *
     * @return 本接收器内建的计数器实例（生命周期与实例同寿）
     */
    public WalStreamMetrics metrics() {
        return metrics;
    }

    /**
     * 已消费到的绝对 LSN 游标前沿（walker.consumedLsn 的跨线程镜像）。
     *
     * <p>start 后即为页对齐起点；每次 chunk 解析后推进到"已 emit 记录的末尾"——不含 carry
     * 中等待拼装的半条记录/页。断流重连与 Task 9+ 位点持久化以本值为续传起点。</p>
     *
     * @return 下一个未消费字节的 LSN；未 start 时为 0
     */
    public long consumedLsn() {
        return consumedLsn;
    }

    /**
     * 同步启动后台接收线程（wal-receiver，非守护）。
     *
     * <p>关键步骤：起点 LSN 按页边界下取整（walker 页导航假设）→ 创建 walker（sink 直接
     * 作其交付回调，回调在接收线程内同步执行）→ 起线程即返回（首连接在接收线程内建立，
     * 连接失败走重连循环不阻塞调用方）。边界与异常语义：重复 start 抛 ISE；fromLsn 必须
     * 落在服务端有效 WAL 区间内（非法位点在首个 START_REPLICATION 处 SQLException，重连
     * 5 次后 ERROR 停机）；已 stop 的实例再 start 抛 ISE（生命周期单程——新会话新建实例，
     * 续传位点经 consumedLsn 交接）。</p>
     *
     * @param sink    已解析记录的交付回调（接收线程内同步执行——慢 sink 直接拖慢收流，背压语义）
     * @param fromLsn 起始 LSN（典型 {@link PhysicalSlotManager#ensureSlot(String)} 的返回值）
     */
    public synchronized void start(Consumer<WalRecord> sink, long fromLsn) {
        if (stopped) {
            throw new IllegalStateException("receiver already stopped — create a new instance for a new session");
        }
        if (started) {
            throw new IllegalStateException("receiver already started");
        }
        started = true;
        long aligned = fromLsn & ~((long) layout.walBlockSize() - 1);
        walker = new WalStreamWalker(layout, metrics, sink);
        consumedLsn = aligned;
        worker = new Thread(() -> runReceiveLoop(aligned), "wal-receiver");
        worker.setDaemon(false);
        worker.start();
        LOG.info("WAL 接收器启动: slot={} 起点 {}（页对齐 {}）", slotName, Lsn.format(fromLsn), Lsn.format(aligned));
    }

    /**
     * 幂等停机：置停机位 → 关流关连接（解除接收线程阻塞）→ join 3s。
     *
     * <p>关键步骤：synchronized 双检幂等（重复调用为 no-op）；closeStreamQuietly 先关流再关
     * 连接（接收线程正在 readPending 时被关连接会抛 SQLException，接收循环以 stopRequested
     * 位先行短路吞掉）。join 超时则 interrupt 接收线程（打断重连退避睡）后 WARN——停机位
     * 保证其随后自行退出。边界：未 start 时调用合法（仅置位）。</p>
     */
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        stopRequested = true;
        closeStreamQuietly();
        if (worker != null) {
            try {
                worker.join(STOP_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (worker.isAlive()) {
                // join 超时（多半卡在重连退避睡）——interrupt 提前打断睡，循环顶的
                // stopRequested 位随即令其退出
                worker.interrupt();
                LOG.warn("wal-receiver 线程 {}ms 内未退出——已 interrupt（停机位已置）", STOP_JOIN_TIMEOUT_MS);
            }
        }
        LOG.info("WAL 接收器停机: slot={} 消费前沿 {}", slotName, Lsn.format(consumedLsn));
    }

    /**
     * 接收主循环（接收线程体）：建流 → drain → 异常重连。
     *
     * <p>关键步骤：外层 for 受 stopRequested 约束；每次迭代 openStream(resume) → drainLoop
     * （正常返回仅因停机）；SQLException 走重连分支——失败计数自增、WARN 记退避、resume
     * 切到 walker 消费前沿、睡退避后重试，连续超 {@link #MAX_RECONNECT_ATTEMPTS} 次 ERROR
     * 停机；drainLoop 期间真收到过数据（fedAny）则计数清零。RuntimeException（walker 解析
     * ISE 等）属确定性数据失败——ERROR 记录后直接退出，不做无益重连。finally 逐轮关闭
     * 流/连接防句柄泄漏。</p>
     *
     * @param startLsn 首轮起始 LSN（已页对齐）
     */
    private void runReceiveLoop(long startLsn) {
        long resume = startLsn;
        int failures = 0;
        long backoff = RECONNECT_BACKOFF_BASE_MS;
        while (!stopRequested) {
            try {
                openStream(resume);
                boolean fedAny = drainLoop();
                if (fedAny) {
                    failures = 0;
                    backoff = RECONNECT_BACKOFF_BASE_MS;
                }
                return; // drainLoop 正常返回 = 停机请求
            } catch (SQLException e) {
                if (stopRequested) {
                    return; // stop() 关连接引起的 SQLException 属停机路径
                }
                failures++;
                if (failures > MAX_RECONNECT_ATTEMPTS) {
                    LOG.error("WAL 流连续重连失败 {} 次，接收器停机（最后错误）", failures, e);
                    return;
                }
                metrics.reconnects.increment();
                // resume 兜底（High-1）：walker.consumedLsn() 在从未 feed 时为 0 哨兵——
                // 首次 openStream 即失败或首 feed 前断流若取 0 会以 0/0 重连（服务端拒绝
                // →5 次假停机）或按当前位点起流跳过启动窗口，故保留页对齐初值不动
                long frontier = walker.consumedLsn();
                if (frontier != 0) {
                    resume = frontier;
                    consumedLsn = frontier;
                }
                LOG.warn("WAL 流中断（第 {}/{} 次），{}ms 后从 {} 重连: {}", failures, MAX_RECONNECT_ATTEMPTS,
                        backoff, Lsn.format(resume), e.getMessage());
                sleepQuiet(backoff);
                backoff = Math.min(backoff * 2, RECONNECT_BACKOFF_MAX_MS);
            } catch (RuntimeException e) {
                LOG.error("WAL 流数据解析失败（确定性错误，不重连）——接收器停机，前沿 {}",
                        Lsn.format(walker.consumedLsn()), e);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                closeStreamQuietly();
            }
        }
    }

    /**
     * 建立物理复制流（复制连接 + START_REPLICATION PHYSICAL）。
     *
     * <p>URL 参数两件套缺一不可：{@code replication=database} 声明复制模式、
     * {@code assumeMinServerVersion=9.4} 防驱动静默丢弃 replication 参数（领域要点）。
     * 状态回执间隔 1s（keepalive 层面），流游标经 {@code stream.getLastReceiveLSN()} 读。</p>
     *
     * @param startLsn 起始 LSN（已页对齐）
     * @throws SQLException 连接或 START_REPLICATION 失败
     */
    private void openStream(long startLsn) throws SQLException {
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database
                + "?replication=database&assumeMinServerVersion=9.4";
        connection = DriverManager.getConnection(url, user, pass);
        PGConnection pgc = connection.unwrap(PGConnection.class);
        stream = pgc.getReplicationAPI().replicationStream()
                .physical()
                .withStartPosition(LogSequenceNumber.valueOf(startLsn))
                .withStatusInterval(1, TimeUnit.SECONDS)
                .start();
        // 数据锚清零（重连/首连同式）：本连接首块回落调用方游标锚，其后各 chunk 自锚
        // （免疫 getLastReceiveLSN 的 keepalive walEnd 跳变——Task 13 实证的假再同步根因）
        walker.resetAnchor();
        LOG.info("WAL 物理流已建立: slot={} 起点 {}", slotName, Lsn.format(startLsn));
    }

    /**
     * drain 轮询循环：readPending 取尽缓冲、空轮睡 100ms、周期反馈 flush LSN。
     *
     * <p>关键步骤：读到 chunk → 拷贝字节 → walker.feed（流游标 getLastReceiveLSN 作 chunk 锚）
     * → 刷新 consumedLsn 镜像 → 反馈判定（距上次反馈 ≥5s 或距上次反馈点新收 ≥1000 条）。
     * 空轮睡 100ms 前做同款反馈判定（空闲期也保底 5s 上报）。返回仅两种形态：stopRequested
     * 置位（正常）或异常上抛（交重连循环）。</p>
     *
     * @return 本次连接期间是否真收到过数据（重连失败计数的清零依据）
     * @throws SQLException          流读/反馈失败（交重连）
     * @throws InterruptedException  停机睡被中断
     */
    private boolean drainLoop() throws SQLException, InterruptedException {
        long lastFeedbackAt = System.currentTimeMillis();
        long recordsAtFeedback = metrics.records.sum();
        boolean fedAny = false;
        while (!stopRequested) {
            ByteBuffer msg = stream.readPending();
            if (msg == null) {
                if (System.currentTimeMillis() - lastFeedbackAt >= FEEDBACK_INTERVAL_MS) {
                    sendFeedback();
                    lastFeedbackAt = System.currentTimeMillis();
                    recordsAtFeedback = metrics.records.sum();
                }
                Thread.sleep(IDLE_POLL_SLEEP_MS);
                continue;
            }
            byte[] bytes = new byte[msg.remaining()];
            msg.get(bytes);
            walker.feed(stream.getLastReceiveLSN().asLong(), bytes);
            consumedLsn = walker.consumedLsn();
            fedAny = true;
            if (metrics.records.sum() - recordsAtFeedback >= FEEDBACK_RECORD_THRESHOLD
                    || System.currentTimeMillis() - lastFeedbackAt >= FEEDBACK_INTERVAL_MS) {
                sendFeedback();
                lastFeedbackAt = System.currentTimeMillis();
                recordsAtFeedback = metrics.records.sum();
            }
        }
        return fedAny;
    }

    /**
     * 上报 flush 确认：setFlushedLSN(consumedLsn) + forceUpdateStatus。
     *
     * <p>当前不附槽（类 javadoc 已知限制），本反馈主要维持 standby status 协议节拍与
     * pg_stat_replication 观测面；槽绑流后即成为 restart_lsn 推进通道。</p>
     *
     * @throws SQLException 反馈失败（交重连）
     */
    private void sendFeedback() throws SQLException {
        PGReplicationStream s = stream;
        if (s != null) {
            s.setFlushedLSN(LogSequenceNumber.valueOf(walker.consumedLsn()));
            s.forceUpdateStatus();
        }
    }

    /**
     * 静默关闭当前流与连接（WARN 吞异常——清理路径不得掩盖主异常/停机）。
     */
    private void closeStreamQuietly() {
        PGReplicationStream s = stream;
        Connection c = connection;
        stream = null;
        connection = null;
        if (s != null) {
            try {
                s.close();
            } catch (SQLException e) {
                LOG.warn("关闭复制流失败（忽略）: {}", e.getMessage());
            }
        }
        if (c != null) {
            try {
                c.close();
            } catch (SQLException e) {
                LOG.warn("关闭复制连接失败（忽略）: {}", e.getMessage());
            }
        }
    }

    /**
     * 不可中断优先的退避睡（中断仅提前结束本轮流——停机位在循环顶再查）。
     *
     * @param ms 睡眠毫秒
     */
    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
