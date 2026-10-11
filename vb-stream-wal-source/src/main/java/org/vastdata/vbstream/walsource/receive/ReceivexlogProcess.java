package org.vastdata.vbstream.walsource.receive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * pg_receivexlog sidecar 子进程管理器（managed 模式，openGauss M1）：把 WAL 落盘成段
 * 文件目录的外部进程的生命周期管起来，供 {@link FileWalSource} tail 同一目录。
 *
 * <p>关键机制：</p>
 * <ol>
 *   <li><strong>拉起面</strong>：ProcessBuilder 拼命令 {@code pg_receivexlog -D dir
 *       [-h host -p port -U user] [--slot name]}——<b>密码只经 PGPASSWORD 环境变量
 *       传入，绝不经命令行</b>（命令行参数对同机所有用户可见于 ps）；stderr 合并入
 *       stdout 并在监督线程内逐行排空（不排空会塞满管道缓冲区把子进程写阻塞）。</li>
 *   <li><strong>退出监控 + 退避重拉</strong>：监督线程（名 wal-rx-supervisor）以合并流
 *       EOF + waitFor 感知子进程退出（pg_receivexlog 预期长驻，任何退出都是异常），
 *       指数退避重拉 1s→2s→4s→8s→16s（复用 {@link WalStreamReceiver} 的退避形态），
 *       连续失败 5 次置终态失败停机；子进程稳定运行 ≥{@value #STABLE_RUN_RESET_MS}ms
 *       后失败计数清零（连接级抖动不累计成停机）。IOException（二进制缺失/权限）同计
 *       失败面。</li>
 *   <li><strong>stop 链</strong>：SIGTERM（{@code destroy()}）→ 3s 等待 →
 *       SIGKILL（{@code destroyForcibly()}）。</li>
 * </ol>
 *
 * <p>线程约束：监督线程独占子进程拉起/回收；{@code processAlive()} 经 volatile 句柄
 * 供任意线程读；{@code start()/stop()} synchronized 单程闸门（幂等停机）。终态失败经
 * {@link #terminalFailure()} 暴露——宿主周期轮询，见终态即 fail-fast（与接收器/
 * FileWalSource 的终态死亡契约同构）。</p>
 */
public final class ReceivexlogProcess {

    private static final Logger LOG = LoggerFactory.getLogger(ReceivexlogProcess.class);

    /** 重连退避基数（1s），逐次翻倍——与 WalStreamReceiver 同形。 */
    private static final long BACKOFF_BASE_MS = 1_000;

    /** 重连退避上限（16s）。 */
    private static final long BACKOFF_MAX_MS = 16_000;

    /** 连续失败上限（>5 次置终态）——与 WalStreamReceiver 同形。 */
    private static final int MAX_ATTEMPTS = 5;

    /** 稳定运行阈值（毫秒）——存活超过该时长的子进程退出后失败计数清零。 */
    private static final long STABLE_RUN_RESET_MS = 60_000;

    /** stop() 对子进程 SIGTERM 后的等待上限（毫秒），超时 SIGKILL。 */
    private static final long STOP_WAIT_MS = 3_000;

    private final String bin;
    private final Path dir;
    private final String host;
    private final int port;
    private final String user;
    private final String pass;
    private final String slotName;
    private final long backoffBaseMs;
    private final int maxAttempts;

    /** 拉起计数（观测面，AtomicInteger 供监督线程写/任意线程读）。 */
    private final AtomicInteger launches = new AtomicInteger();

    /** 当前子进程句柄——监督线程写，processAlive()/stop() 跨线程读。 */
    private volatile Process process;

    /** 停机请求位——stop() 置位，监督循环边界检查。 */
    private volatile boolean stopRequested;

    /** 终态失败原因（连续重拉耗尽）：volatile 发布，任意线程经 terminalFailure() 轮询。 */
    private volatile Throwable terminalFailure;

    /** start/stop 的单程闸门（synchronized 方法互斥保护）。 */
    private boolean started;
    private boolean stopped;

    /** 监督线程句柄（start 时创建）。 */
    private Thread worker;

    /**
     * 生产档装配（退避 1s→16s、5 次终态）。
     *
     * @param bin      pg_receivexlog 可执行文件路径
     * @param dir      段落盘目录（-D 参数值，与 FileWalSource 监视目录同源）
     * @param host     服务端主机（null = 不传 -h，走客户端缺省）
     * @param port     服务端端口（&le;0 = 不传 -p）
     * @param user     用户（null = 不传 -U）
     * @param pass     密码（经 PGPASSWORD 环境变量传入；null = 不设）
     * @param slotName 复制槽名（null = 不传 --slot）
     */
    public ReceivexlogProcess(String bin, Path dir, String host, int port, String user, String pass,
                              String slotName) {
        this(bin, dir, host, port, user, pass, slotName, BACKOFF_BASE_MS, MAX_ATTEMPTS);
    }

    /**
     * 测试档装配（退避参数可缩小——离线测试不睡真实秒级）。
     *
     * @param bin           pg_receivexlog 可执行文件路径
     * @param dir           段落盘目录
     * @param host          服务端主机（null = 不传）
     * @param port          服务端端口（&le;0 = 不传）
     * @param user          用户（null = 不传）
     * @param pass          密码（null = 不设环境变量）
     * @param slotName      槽名（null = 不传）
     * @param backoffBaseMs 退避基数（毫秒，逐次翻倍封顶 {@value #BACKOFF_MAX_MS}）
     * @param maxAttempts   连续失败上限（超出置终态）
     */
    ReceivexlogProcess(String bin, Path dir, String host, int port, String user, String pass,
                       String slotName, long backoffBaseMs, int maxAttempts) {
        this.bin = bin;
        this.dir = dir;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.slotName = slotName;
        this.backoffBaseMs = backoffBaseMs;
        this.maxAttempts = maxAttempts;
    }

    /**
     * 当前子进程是否存活（volatile 句柄读，任意线程）。
     *
     * @return 子进程存活为 true；未拉起/已退出为 false
     */
    public boolean processAlive() {
        Process p = process;
        return p != null && p.isAlive();
    }

    /**
     * 累计拉起次数（观测面）。
     *
     * @return 监督线程启动以来 ProcessBuilder.start 的次数
     */
    public int launches() {
        return launches.get();
    }

    /**
     * 终态失败观测面：连续重拉耗尽后返回根因（进程退出链的最后一个根因包装/拉起
     * IOException）；运行中/正常停机/未 start 为 empty。
     *
     * @return 终态根因；非终态为 empty
     */
    public Optional<Throwable> terminalFailure() {
        return Optional.ofNullable(terminalFailure);
    }

    /**
     * 同步启动监督线程（wal-rx-supervisor，非守护）——子进程在监督线程内首次拉起。
     *
     * <p>边界与异常语义：重复 start / 停机后再 start 抛 ISE（生命周期单程，新会话新实例）；
     * 首次拉起失败不抛给调用方——走监督循环的退避/终态面（宿主轮询 terminalFailure）。</p>
     */
    public synchronized void start() {
        if (stopped) {
            throw new IllegalStateException("process manager already stopped — create a new instance for a new session");
        }
        if (started) {
            throw new IllegalStateException("process manager already started");
        }
        started = true;
        worker = new Thread(this::runSupervisorLoop, "wal-rx-supervisor");
        worker.setDaemon(false);
        worker.start();
        LOG.info("pg_receivexlog 监督线程启动: bin={} dir={}", bin, dir);
    }

    /**
     * 幂等停机：SIGTERM（destroy）→ 3s 等待 → SIGKILL（destroyForcibly）→ join 监督线程。
     *
     * <p>关键步骤：置停机位（监督循环退出 + 子进程退出不计失败）→ 终止当前子进程
     * （监督线程阻塞的合并流读随进程退出 EOF 解除）→ join 3s（监督线程可能正在退避睡，
     * 超时 interrupt 提前打断）。</p>
     */
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        stopRequested = true;
        Process p = process;
        if (p != null) {
            p.destroy();
            try {
                if (!p.waitFor(STOP_WAIT_MS, TimeUnit.MILLISECONDS)) {
                    p.destroyForcibly();
                    LOG.warn("pg_receivexlog {}ms 内未随 SIGTERM 退出——已 SIGKILL", STOP_WAIT_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (worker != null) {
            try {
                worker.join(STOP_WAIT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (worker.isAlive()) {
                worker.interrupt();
                LOG.warn("wal-rx-supervisor 线程 {}ms 内未退出——已 interrupt（停机位已置）", STOP_WAIT_MS);
            }
        }
        LOG.info("pg_receivexlog 子进程停机: dir={} 累计拉起 {} 次", dir, launches.get());
    }

    /**
     * 监督主循环（线程体）：拉起 → 排空输出并等退出 → 退避重拉 / 终态停机。
     *
     * <p>关键步骤：每轮 buildProcessBuilder().start() 后排空合并输出流（阻塞至 EOF =
     * 子进程退出）再 waitFor 收割退出码；stopRequested 置位下的退出属停机路径直接
     * 返回；异常退出计失败——稳定运行超阈值先清零计数，超 {@code maxAttempts} 置
     * terminalFailure 后 ERROR 退出；IOException（拉起失败/输出读失败——后者先
     * destroy 防双活）同计失败面。退避睡可被 interrupt 提前结束（循环顶停机位再查）。</p>
     */
    private void runSupervisorLoop() {
        long backoff = backoffBaseMs;
        int failures = 0;
        while (!stopRequested) {
            long startedAt = System.currentTimeMillis();
            try {
                Process p = buildProcessBuilder().start();
                process = p;
                int n = launches.incrementAndGet();
                LOG.info("pg_receivexlog 已拉起（第 {} 次）: {} -D {}", n, bin, dir);
                drainOutput(p);
                int exit = p.waitFor();
                if (stopRequested) {
                    return;   // stop() 主动终止的正常退出
                }
                long ranFor = System.currentTimeMillis() - startedAt;
                if (ranFor >= STABLE_RUN_RESET_MS) {
                    failures = 0;
                    backoff = backoffBaseMs;
                }
                failures++;
                if (failures > maxAttempts) {
                    terminalFailure = new IllegalStateException("pg_receivexlog 连续退出 " + failures
                            + " 次（最后 exit=" + exit + "），放弃重拉");
                    LOG.error("pg_receivexlog 连续失败 {} 次后终态停机（最后 exit={}）", failures, exit);
                    return;
                }
                LOG.warn("pg_receivexlog 异常退出（exit={}，第 {}/{} 次），{}ms 后重拉", exit, failures,
                        maxAttempts, backoff);
            } catch (IOException e) {
                destroyCurrentQuietly();   // 输出读失败时子进程可能仍活——防重拉双活
                failures++;
                if (failures > maxAttempts) {
                    terminalFailure = e;
                    LOG.error("pg_receivexlog 拉起/输出失败 {} 次后终态停机", failures, e);
                    return;
                }
                LOG.warn("pg_receivexlog 拉起失败（第 {}/{} 次），{}ms 后重试: {}", failures, maxAttempts,
                        backoff, e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            sleepQuiet(backoff);
            backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
        }
    }

    /**
     * 拼命令与环境：位置参数按需附加（null/非正数不传对应参数）；密码只进
     * PGPASSWORD 环境变量（命令行会进 ps，硬禁）；redirectErrorStream 合并两流
     * 供监督线程单点排空。
     *
     * @return 就绪的 ProcessBuilder（未 start）
     */
    private ProcessBuilder buildProcessBuilder() {
        List<String> cmd = new ArrayList<>();
        cmd.add(bin);
        cmd.add("-D");
        cmd.add(dir.toString());
        if (host != null) {
            cmd.add("-h");
            cmd.add(host);
        }
        if (port > 0) {
            cmd.add("-p");
            cmd.add(Integer.toString(port));
        }
        if (user != null) {
            cmd.add("-U");
            cmd.add(user);
        }
        if (slotName != null) {
            cmd.add("--slot");
            cmd.add(slotName);
        }
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (pass != null) {
            pb.environment().put("PGPASSWORD", pass);
        }
        pb.redirectErrorStream(true);
        return pb;
    }

    /**
     * 排空子进程合并输出（逐行 DEBUG 日志）——读到 EOF 即子进程退出，兼作退出感知的
     * 阻塞等待点。
     *
     * @param p 目标子进程
     * @throws IOException 流读失败（监督循环计失败面）
     */
    private static void drainOutput(Process p) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.debug("pg_receivexlog: {}", line);
            }
        }
    }

    /**
     * 静默终止当前子进程（清理路径不抛——生命周期兜底）。
     */
    private void destroyCurrentQuietly() {
        Process p = process;
        if (p != null && p.isAlive()) {
            p.destroy();
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
