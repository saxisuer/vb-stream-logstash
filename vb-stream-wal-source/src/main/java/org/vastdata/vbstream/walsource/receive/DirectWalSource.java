package org.vastdata.vbstream.walsource.receive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.receive.direct.V3ProtocolChannel;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

/**
 * openGauss 物理复制裸 v3 协议字节源（{@link WalByteSource} 的 Direct 实现，Task 5）：
 * 不经 pgjdbc/opengauss-jdbc 的 replication API（openGauss 的
 * {@code replication=database} 形态被服务端硬路由进逻辑发送循环，物理流不可用），以
 * {@link V3ProtocolChannel} 裸客户端直连——'Q' IDENTIFY_SYSTEM → 起点 = xlogpos 段对齐
 * 下取整（pg_receivexlog.cpp:265 同式）→ 'Q' START_REPLICATION → CopyBoth 'w' 载荷逐块
 * 交付 sink。
 *
 * <p>关键机制（全部对齐 pg_receivexlog 参考实现 receivelog.cpp）：</p>
 * <ol>
 *   <li><strong>锚语义</strong>：块锚 = {@code dataStart + 载荷长}（本块末字节之后的绝对
 *       LSN，与 walker.feed 对齐）——<b>非</b>帧头 walEnd（那是发送端 WAL 末观测值，
 *       可越前于已收字节；采信会虚抬前沿，重连即跳字节。walEnd 只进观测面）。</li>
 *   <li><strong>'k' keepalive</strong>：仅推进观测；replyRequested 置位时立即回 'r'
 *       （replyRequested=false 档——receivelog.cpp:789 形态）。</li>
 *   <li><strong>心跳</strong>：每连接独立守护线程，每 {@code statusIntervalMs/2} 发
 *       replyRequested=true 的 'r'（checkForReceiveTimeout/createHeartbeatTimer 形态——
 *       心跳必须在 CopyBoth 之后才发，故线程随连接起止）。</li>
 *   <li><strong>'m' 配置帧</strong>：服务端 GaussDB 配置推送——整体跳过不断流。</li>
 *   <li><strong>断线重连</strong>：指数退避 1s→16s，连续 {@code maxAttempts} 次失败置
 *       终态失败（工作线程退出 + terminalFailure 暴露）；重连起点 = consumedLsn 段对齐
 *       下取整（重叠段 at-least-once 重发，上游 LSN 过滤线消化）；稳定运行 ≥60s 失败
 *       计数清零（与 ReceivexlogProcess/WalStreamReceiver 同形）。</li>
 * </ol>
 *
 * <p>线程约束：工作线程（wal-direct-rx，非守护）独占通道读与 sink 回调；心跳线程
 * （wal-direct-hb，守护）只写——与读线程的 'r' 写经通道 sendLock 互斥。
 * {@code consumedLsn()} 经 AtomicLong 任意线程读；{@code stop()} 任意线程幂等。</p>
 *
 * <p>边界与异常语义：sink 的 RuntimeException 属确定性失败——置 terminalFailure 后
 * ERROR 退出（不重试，与 FileWalSource 同契约）；认证失败/ErrorResponse/协议错位按
 * 连接失败计入退避面（服务端拒绝是可恢复的运维面——改配置/起服务后重连可自愈）。</p>
 */
public final class DirectWalSource implements WalByteSource {

    private static final Logger LOG = LoggerFactory.getLogger(DirectWalSource.class);

    /** 重连退避上限（毫秒）。 */
    private static final long BACKOFF_MAX_MS = 16_000;

    /** 稳定运行阈值（毫秒）——连接存活超此时长后失败计数清零。 */
    private static final long STABLE_RUN_RESET_MS = 60_000;

    /** stop() 对工作/心跳线程的 join 超时（毫秒）。 */
    private static final long STOP_JOIN_TIMEOUT_MS = 3_000;

    /** 心跳周期下限（毫秒）——statusIntervalMs/2 的兜底，防零/负值忙转。 */
    private static final long MIN_HEARTBEAT_MS = 50;

    private final String host;
    private final int port;
    private final String user;
    private final String pass;
    private final String slotName;
    private final long statusIntervalMs;

    /**
     * 首连起点注入（M1 Task 6 装配缝）：{@code WalSource} openGauss 档把引导一致点
     * （ensureSlot P₀ / bootstrap B / 检查点 stored lsn）传进本源——首连不采纳
     * IDENTIFY_SYSTEM 的 xlogpos（它是服务端当前末观测位点，晚于引导一致点时采纳即跳过
     * (B, xlogpos] 窗口记录——引导一致性 C1 的 OG 面）。0 = 现状 IDENTIFY_SYSTEM 档；
     * 断流重连恒自 consumedLsn 续传（注入值只作用于首连）。
     */
    private final long startLsn;

    private final long segmentSize;
    private final int readTimeoutMs;
    private final long backoffBaseMs;
    private final int maxAttempts;

    /** 消费前沿镜像（已交付字节末尾的 LSN）——工作线程写，任意线程读。 */
    private final AtomicLong consumedLsn = new AtomicLong();

    /** 停机请求位——stop() 置位，工作/心跳循环边界检查。 */
    private volatile boolean stopRequested;

    /** 终态失败原因：volatile 发布，宿主经 terminalFailure() 轮询。 */
    private volatile Throwable terminalFailure;

    /** 当前连接通道——工作线程换装，心跳线程读（sendCopyData 内部 sendLock 防写交错）。 */
    private volatile V3ProtocolChannel channel;

    /** 当前连接的心跳线程——工作线程管理生命周期。 */
    private Thread heartbeatThread;

    /** start/stop 的单程闸门（synchronized 方法互斥保护）。 */
    private boolean started;
    private boolean stopped;

    /** 工作线程句柄（start 时创建）。 */
    private Thread worker;

    /** 交付回调（start 时注入，工作线程独占调用）。 */
    private LongConsumer chunkEndLsnSink;
    private BiConsumer<Long, byte[]> chunkSink;

    /**
     * 生产档装配（段尺寸 16MB、读超时 = max(statusIntervalMs×4, 30s)、退避 1s→16s、
     * 5 次失败终态）。
     *
     * @param host             服务端主机
     * @param port             服务端端口
     * @param user             用户（须有 REPLICATION 权限）
     * @param pass             密码
     * @param slotName         物理复制槽名
     * @param statusIntervalMs 反馈心跳的 statusInterval（毫秒，心跳周期 = 其一半）
     * @param startLsn         首连起点（0 = IDENTIFY_SYSTEM xlogpos 档；&gt;0 = 注入值段对齐
     *                         下取整起流——WalSource OG 档引导一致点的传递面，M1 Task 6）
     */
    public DirectWalSource(String host, int port, String user, String pass,
                           String slotName, long statusIntervalMs, long startLsn) {
        this(host, port, user, pass, slotName, statusIntervalMs, FileWalSource.DEFAULT_SEGMENT_SIZE,
                (int) Math.max(statusIntervalMs * 4, 30_000), 1_000, 5, startLsn);
    }

    /**
     * 测试档装配（段尺寸/读超时/退避参数化——离线测试不睡真实秒级、段几何可缩小）。
     *
     * @param host             服务端主机
     * @param port             服务端端口
     * @param user             用户
     * @param pass             密码
     * @param slotName         槽名
     * @param statusIntervalMs 反馈心跳的 statusInterval（毫秒）
     * @param segmentSize      WAL 段尺寸（字节——重连起点的对齐粒度）
     * @param readTimeoutMs    通道读超时（毫秒，≤0 无限阻塞）
     * @param backoffBaseMs    重连退避基数（逐次翻倍封顶 16s）
     * @param maxAttempts      连续失败上限（达限置终态）
     * @param startLsn         首连起点（0 = IDENTIFY_SYSTEM xlogpos 档；&gt;0 = 注入值优先）
     */
    public DirectWalSource(String host, int port, String user, String pass, String slotName,
                    long statusIntervalMs, long segmentSize, int readTimeoutMs,
                    long backoffBaseMs, int maxAttempts, long startLsn) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.slotName = slotName;
        this.statusIntervalMs = statusIntervalMs;
        this.segmentSize = segmentSize;
        this.readTimeoutMs = readTimeoutMs;
        this.backoffBaseMs = backoffBaseMs;
        this.maxAttempts = maxAttempts;
        this.startLsn = startLsn;
    }

    /**
     * 消费前沿（已交付字节末尾的 LSN；未 start / 未交付为 0）。
     *
     * @return 末块锚（dataStart + 载荷长）
     */
    @Override
    public long consumedLsn() {
        return consumedLsn.get();
    }

    /**
     * 终态失败观测面：重连耗尽 / sink 确定性异常后返回根因；运行中/正常停机/未 start
     * 为 empty。宿主应周期轮询本面，见终态即 fail-fast。
     *
     * @return 终态根因；非终态为 empty
     */
    @Override
    public Optional<Throwable> terminalFailure() {
        return Optional.ofNullable(terminalFailure);
    }

    /**
     * 工作线程是否已终止（终态失败/停机后的终止性验收锚）。
     *
     * @return 工作线程不在运行为 true
     */
    public boolean isTerminated() {
        Thread w = worker;
        return w == null || !w.isAlive();
    }

    /**
     * 同步启动工作线程（wal-direct-rx，非守护）——首连失败不抛调用方，走退避/终态面。
     *
     * <p>边界：重复 start / 停机后再 start 抛 ISE（生命周期单程，新会话新实例）；
     * chunk 回调在工作线程同步执行（慢 sink 拖慢收流——背压语义与 FileWalSource 一致）。</p>
     *
     * @param chunkEndLsnSink 前沿通知：每块交付后以本块末锚回调
     * @param chunkSink       字节块交付回调（第一参 = 本块末字节之后的绝对 LSN）
     */
    @Override
    public synchronized void start(LongConsumer chunkEndLsnSink, BiConsumer<Long, byte[]> chunkSink) {
        if (stopped) {
            throw new IllegalStateException("source already stopped — create a new instance for a new session");
        }
        if (started) {
            throw new IllegalStateException("source already started");
        }
        started = true;
        this.chunkEndLsnSink = chunkEndLsnSink;
        this.chunkSink = chunkSink;
        worker = new Thread(this::runWorkerLoop, "wal-direct-rx");
        worker.setDaemon(false);
        worker.start();
        LOG.info("WAL 直连源启动: {}:{} user={} 槽 {} 段尺寸 {} 心跳 {}ms",
                host, port, user, slotName, segmentSize, heartbeatPeriodMs());
    }

    /**
     * 幂等停机：置停机位 → 关当前通道（解除工作线程阻塞读）→ join 工作线程 3s（超时
     * interrupt 兜底）——心跳线程由工作线程的连接清理路径自行收割。
     */
    @Override
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        stopRequested = true;
        closeChannelQuietly();
        Thread w = worker;
        if (w != null) {
            try {
                w.join(STOP_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (w.isAlive()) {
                w.interrupt();
                LOG.warn("wal-direct-rx 线程 {}ms 内未退出——已 interrupt（停机位已置）", STOP_JOIN_TIMEOUT_MS);
            }
        }
        LOG.info("WAL 直连源停机: 消费前沿 {}", Lsn.format(consumedLsn.get()));
    }

    /**
     * {@link AutoCloseable#close} 的无检收窄——委托 {@link #stop()}（幂等）。
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * 工作主循环（线程体）：连接 + 流接收 → 失败退避重连 / 终态停机。
     *
     * <p>关键步骤：每轮 {@link #runSingleConnection}（IOException = 连接级失败）；停机位
     * 置位下的异常直接返回（stop 路径）；稳定运行 ≥{@value #STABLE_RUN_RESET_MS}ms 先清零
     * 计数；失败累计到 {@code maxAttempts} 置 terminalFailure 后 ERROR 退出；退避睡可被
     * interrupt 提前结束（循环顶再查停机位）。sink 的 RuntimeException 属确定性失败——
     * 置终态退出不重试。finally 收心跳线程 + 关通道防泄漏。</p>
     */
    private void runWorkerLoop() {
        long backoff = backoffBaseMs;
        int failures = 0;
        while (!stopRequested) {
            long connectedAt = System.currentTimeMillis();
            try {
                runSingleConnection();
                // 正常返回的唯一来源 = streamLoop 循环顶见停机位（stop() 路径）——直接收工。
                // CopyDone 不落此路径：streamLoop 对其抛 IOException 走可重试重连（审查
                // 修复 1——静默 return 会让源停更而宿主无终态可查）。
                return;
            } catch (IOException e) {
                if (stopRequested) {
                    return;   // stop() 主动关通道的读异常——正常停机路径
                }
                if (System.currentTimeMillis() - connectedAt >= STABLE_RUN_RESET_MS) {
                    failures = 0;
                    backoff = backoffBaseMs;
                }
                failures++;
                if (failures >= maxAttempts) {
                    terminalFailure = new IllegalStateException(
                            "直连 WAL 流连续失败 " + failures + " 次，放弃重连", e);
                    LOG.error("WAL 直连源终态停机（连续失败 {} 次）", failures, e);
                    return;
                }
                LOG.warn("直连 WAL 流失败（第 {}/{} 次），{}ms 后重连: {}",
                        failures, maxAttempts, backoff, e.toString());
            } catch (RuntimeException e) {
                terminalFailure = e;
                LOG.error("WAL 直连源交付失败（确定性错误）——源停机，前沿 {}",
                        Lsn.format(consumedLsn.get()), e);
                return;
            } finally {
                stopHeartbeat();
                closeChannelQuietly();
            }
            sleepQuiet(backoff);
            backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
        }
    }

    /**
     * 单连接生命周期：建连认证 → IDENTIFY_SYSTEM → 起点对齐（续传优先 consumedLsn，首连
     * 依次取注入 startLsn（Task 6 装配缝——引导一致点优先于服务端观测位点）、都缺才取
     * xlogpos；段对齐下取整）→ START_REPLICATION → CopyBoth 进入 → 起心跳 → 流循环
     * 至断连/CopyDone。
     *
     * @throws IOException 连接/认证/命令/流读失败（工作循环的失败面）
     */
    private void runSingleConnection() throws IOException {
        V3ProtocolChannel ch = new V3ProtocolChannel(host, port, user, pass,
                "vb-wal-source", 30_000, readTimeoutMs);
        channel = ch;
        ch.connect();
        String[] row = ch.identifySystem();
        long resumeBase = consumedLsn.get() > 0 ? consumedLsn.get()
                : startLsn > 0 ? startLsn : Lsn.parse(row[2]);
        long start = resumeBase - resumeBase % segmentSize;
        ch.startPhysicalReplication(slotName, start);
        LOG.info("直连流已起: systemid={} tli={} 续传基点 {} 流起点 {}",
                row[0], row[1], Lsn.format(resumeBase), Lsn.format(start));
        startHeartbeat(ch);
        streamLoop(ch);
    }

    /**
     * 流循环（工作线程体）：逐 CopyData 帧分派——'w' 剥 64B 头交付载荷（锚 =
     * dataStart+载荷长）、'k' 帧长校验后观测 + 即答 'r'、'm' 跳过、未知帧 WARN 不断流。
     *
     * <p>边界：空载荷 'w'（帧长恰 65）跳过——无字节可交付、锚不推进；'k' 帧长 ≠33 抛
     * IOException（协议字节错位属可重试面——重连段对齐重发即纠错，勿落入 RuntimeException
     * 终态面）；readCopyData 返回 null（服务端 CopyDone）抛 IOException 走可重试重连
     * （审查修复 1：静默返回会令源停更而宿主无终态可查）。循环顶停机位退出是唯一的
     * 正常返回（stop() 路径）。</p>
     *
     * @param ch 当前连接通道
     * @throws IOException 流读/反馈写失败
     */
    private void streamLoop(V3ProtocolChannel ch) throws IOException {
        while (!stopRequested) {
            byte[] msg = ch.readCopyData();
            if (msg == null) {
                // 服务端 CopyDone：物理复制流的 copy 会话被服务端正常结束（时间线切换/对端
                // 重启等）——按<b>可重试中断</b>处理（与 C 参考客户端收 CopyDone 后由外层
                // 重连同构）。不得静默 return：工作线程无声终止会让源停更而宿主无终态可查
                // （本类 javadoc 警告的空转形态）。
                throw new IOException("服务端 CopyDone——copy 会话被服务端结束，按可重试中断重连");
            }
            if (msg.length == 0) {
                continue;
            }
            switch (msg[0]) {
                case 'w' -> {
                    if (msg.length <= 1 + V3ProtocolChannel.WAL_HEADER_SIZE) {
                        LOG.debug("空载荷 'w' 帧——跳过");
                        continue;
                    }
                    long dataStart = V3ProtocolChannel.walDataStart(msg);
                    byte[] payload = V3ProtocolChannel.walPayload(msg);
                    long anchor = dataStart + payload.length;
                    chunkSink.accept(anchor, payload);
                    chunkEndLsnSink.accept(anchor);
                    consumedLsn.set(anchor);
                }
                case 'k' -> {
                    // 帧长下界先行校验（C 客户端 receivelog.cpp:785 的 r != 33 → error 同构）：
                    // 未校验即取 frame[25] 的 replyRequested 位会被畸形帧触发 AIOOBE，落入
                    // RuntimeException 终态面——协议字节错位属环境/传输类失败，契约上走
                    // IOException 可重试面（重连后段对齐重发，服务端即纠错源）。
                    if (msg.length != 33) {
                        throw new IOException("畸形 keepalive 帧长 " + msg.length + "（期望 33）——协议错位，断流重连");
                    }
                    if (V3ProtocolChannel.keepaliveReplyRequested(msg)) {
                        ch.sendStandbyReply(consumedLsn.get(), false);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("keepalive: walEnd={}", Lsn.format(V3ProtocolChannel.walEndOfKeepalive(msg)));
                    }
                }
                case 'm' -> LOG.debug("服务端配置帧 'm'——跳过（receivelog.cpp 'm' 分支）");
                default -> LOG.warn("未知复制帧类型 '{}'——跳过（{} 字节）", (char) msg[0], msg.length);
            }
        }
    }

    /**
     * 起当前连接的心跳线程（wal-direct-hb，守护）：每 {@code statusIntervalMs/2} 发
     * replyRequested=true 的 'r'——写失败静默（断流由读面感知并重连，心跳只作保活）。
     *
     * @param ch 当前连接通道
     */
    private void startHeartbeat(V3ProtocolChannel ch) {
        heartbeatThread = new Thread(() -> {
            while (!stopRequested && channel == ch) {
                try {
                    ch.sendStandbyReply(consumedLsn.get(), true);
                } catch (IOException e) {
                    LOG.debug("心跳写失败（断流由读面感知）: {}", e.toString());
                }
                sleepQuiet(heartbeatPeriodMs());
            }
        }, "wal-direct-hb");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    /**
     * 收割心跳线程（连接清理路径——interrupt + join，超时依赖守护位兜底）。
     */
    private void stopHeartbeat() {
        Thread hb = heartbeatThread;
        heartbeatThread = null;
        if (hb != null) {
            hb.interrupt();
            try {
                hb.join(STOP_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 心跳周期（毫秒）= max(statusIntervalMs/2, {@value #MIN_HEARTBEAT_MS})。
     */
    private long heartbeatPeriodMs() {
        return Math.max(statusIntervalMs / 2, MIN_HEARTBEAT_MS);
    }

    /**
     * 静默关当前通道（幂等清理路径）。
     */
    private void closeChannelQuietly() {
        V3ProtocolChannel ch = channel;
        if (ch != null) {
            ch.close();
            channel = null;
        }
    }

    /**
     * 可中断优先的退避睡（中断仅提前结束本轮流——停机位在循环顶再查）。
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
