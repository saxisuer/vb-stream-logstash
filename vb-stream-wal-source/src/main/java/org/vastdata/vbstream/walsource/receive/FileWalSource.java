package org.vastdata.vbstream.walsource.receive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

/**
 * WAL 段文件目录 tail 字节源（{@link WalByteSource} 的 File 实现，openGauss M1）：
 * 监视 pg_receivexlog 落盘目录，把段文件字节以推模式逐块交付。
 *
 * <p>关键机制（与 WalStreamWalker 的 feed/锚定协议对齐）：</p>
 * <ol>
 *   <li><strong>段发现与排序</strong>：按段文件名（{@code %08X%08X%08X}——tli/段号高/段号低，
 *       可带 {@code .partial} 后缀）扫描目录，段号（高 32 位左移或低 32 位）为序消费；
 *       同段完成文件与 partial 并存时取完成文件。external 模式目录里"既有完成段又有
 *       新 partial"的任意组合按段号序全量消化，已读过的（段号低于游标的）跳过。</li>
 *   <li><strong>.partial 增长读</strong>：记录上次读偏移，交付上界 = <b>已接收前沿</b>——
 *       OG pg_receivexlog 对 .partial 预分配整段全零文件、数据原地写入，文件长度≠接收
 *       进度，前沿经"末非零字节 + 1"前向扫描（{@link #receivedFrontier}，缓存增量扫）
 *       ；完成段按文件全长读。轮询 {@value #POLL_INTERVAL_MS}ms；尾部不足一条记录/一页
 *       的字节照发——walker 的 awaitMore 语义天然消化半记录，本源不自行攒块。</li>
 *   <li><strong>rename 切段</strong>：pg_receivexlog 写满段后把 {@code X.partial} rename
 *       为 {@code X}——当前段读尽（偏移到达段尺寸，或文件为完成名）且目录出现更高段号
 *       文件时切到下一段从 0 起读；rename 窗口（partial 已消失、完成名未上列表）由
 *       重扫兜底等待。</li>
 *   <li><strong>锚语义</strong>：块锚 = {@code 段号 × 段尺寸 + 文件内偏移}（本块末字节之后的
 *       绝对 LSN，与 walker.feed 的 chunkEndLsn 同义）。文件名推算的锚只是粗对齐——
 *       首个页头 pageaddr 随后在 walker 侧接管纠偏（与接收器"首块回落调用方游标锚"
 *       机制同构，本源不做纠偏）。</li>
 *   <li><strong>断点续传</strong>：构造入参 fromLsn（0 = 从目录最早段起）；起点 = fromLsn
 *       所在段、段内偏移按页边界下取整（walker 页导航假设与
 *       {@link WalStreamReceiver} 同式），重放段由上游 LSN 过滤线消化
 *       （at-least-once）。</li>
 * </ol>
 *
 * <p>线程约束：段游标/读句柄为 tail 线程（名 wal-file-tail，非守护）独占；
 * {@link #consumedLsn()} 经 volatile 镜像供任意线程读；{@link #stop()} 可从任意线程调用
 * （幂等；managed 模式连带终止子进程）。{@link WalByteSource#start(LongConsumer, BiConsumer)}
 * 的 chunk 回调在 tail 线程同步执行——慢 sink 直接拖慢收流，背压语义与接收器一致。</p>
 *
 * <p>边界与异常语义：sink 抛 RuntimeException（walker 解析 ISE 等）属确定性失败——ERROR
 * 记录后 tail 线程退出并置 terminalFailure（宿主经 {@link #terminalFailure()} 轮询，
 * 见接收器终态死亡契约）；文件系统 IO 异常视为暂态（WARN 后下轮重扫重试——rename 竞态
 * 窗口的 NoSuchFile 属预期）。目录为空/起始段未出现时静默等待（managed 模式子进程正在
 * 拉流；external 模式等待对端落盘）。</p>
 */
public final class FileWalSource implements WalByteSource {

    private static final Logger LOG = LoggerFactory.getLogger(FileWalSource.class);

    /** 生产档段尺寸：openGauss / PG 缺省 wal_segment_size = 16MB（xlog_basic.h，实测段文件几何）。 */
    public static final long DEFAULT_SEGMENT_SIZE = 16L * 1024 * 1024;

    /** 增长轮询间隔（毫秒）——File.length 探测节拍。 */
    private static final long POLL_INTERVAL_MS = 50;

    /** 单块交付上限（字节）——满 16MB 段一次性出现时分块交付，界住 chunk 数组与 sink 瞬态。 */
    private static final int CHUNK_LIMIT = 1024 * 1024;

    /** .partial 已接收前沿的扫描块尺寸（字节）——空闲期每轮一块的快路径粒度。 */
    private static final int FRONTIER_SCAN_BLOCK = 64 * 1024;

    /** stop() 对 tail 线程的 join 超时（毫秒）。 */
    private static final long STOP_JOIN_TIMEOUT_MS = 3_000;

    /** pg_receivexlog 的完成中段后缀。 */
    private static final String PARTIAL_SUFFIX = ".partial";

    /** 段文件名长度（24 个十六进制字符 = tli 8 + 段号高 8 + 段号低 8）。 */
    private static final int SEGMENT_NAME_LENGTH = 24;

    private final Path dir;
    private final long segmentSize;
    private final long fromLsn;
    private final int walBlockSize;

    /** managed 模式的 sidecar 子进程管理器；external 模式为 null（纯 tail，无子进程）。 */
    private final ReceivexlogProcess process;

    /** 消费前沿的跨线程镜像——tail 线程每块交付后刷新，任意线程读。 */
    private volatile long consumedLsn;

    /** 停机请求位——stop() 置位，tail 循环边界检查。 */
    private volatile boolean stopRequested;

    /** 终态失败原因（sink 确定性异常）：volatile 发布，宿主经 terminalFailure() 轮询。 */
    private volatile Throwable terminalFailure;

    /** start/stop 的单程闸门（synchronized 方法互斥保护）。 */
    private boolean started;
    private boolean stopped;

    /** tail 线程句柄（start 时创建）。 */
    private Thread worker;

    /** 交付回调（start 时注入，tail 线程独占调用）。 */
    private LongConsumer chunkEndLsnSink;
    private BiConsumer<Long, byte[]> chunkSink;

    // ---- 以下段游标状态为 tail 线程独占 ----

    /** 当前消费段号（-1 = 尚未选定初始段）。 */
    private long currentSegNo = -1;

    /** 当前段文件路径（随 .partial→完成段 rename 切换）。 */
    private Path currentPath;

    /** 当前段文件是否 .partial 名（切段终局的判据之一）。 */
    private boolean currentIsPartial;

    /** 当前段内已读偏移（字节）。 */
    private long currentOffset;

    /** 当前段文件的读句柄（随段切换/rename 重开）。 */
    private RandomAccessFile input;

    /**
     * .partial 文件"已接收前沿"的扫描缓存（Task 8 修复）：OG pg_receivexlog 对
     * .partial 段预分配整 16MB 零文件、数据按接收序原地写入——文件长度恒为段尺寸，
     * 不能作数据前沿。本值 = 上轮前向扫描得到的"末非零字节 + 1"（单调推进；adoptSegment
     * 重置 0），下轮扫描自此起（空闲期一轮一块的快路径）。完成段（rename 后）不走本面。
     */
    private long scannedDataEnd;

    /** scannedDataEnd 所属段号（adoptSegment 判定缓存复用还是重置；-1 未采纳过）。 */
    private long lastScannedSegNo = -1;

    /**
     * external 模式装配（无子进程——落盘目录由外部 pg_receivexlog 进程维护）。
     *
     * @param dir          段文件目录（pg_receivexlog -D 指向的目录）
     * @param segmentSize  段尺寸（字节，须 2 的幂倍 8KB；生产 16MB，测试可参数化缩小）
     * @param fromLsn      断点续传起点（0 = 从目录最早段起读）
     * @param walBlockSize 页大小（续传段内偏移的页对齐下取整基准，来自 WalLayout）
     */
    public FileWalSource(Path dir, long segmentSize, long fromLsn, int walBlockSize) {
        this(dir, segmentSize, fromLsn, walBlockSize, null);
    }

    /**
     * managed 模式装配：start 时先拉起 sidecar 子进程再起 tail 线程，stop 时先停 tail
     * 再终止子进程。
     *
     * @param dir          段文件目录
     * @param segmentSize  段尺寸（字节）
     * @param fromLsn      断点续传起点（0 = 从目录最早段起读）
     * @param walBlockSize 页大小（页对齐下取整基准）
     * @param process      子进程管理器（null 退化为 external 模式）
     */
    public FileWalSource(Path dir, long segmentSize, long fromLsn, int walBlockSize,
                         ReceivexlogProcess process) {
        this.dir = dir;
        this.segmentSize = segmentSize;
        this.fromLsn = fromLsn;
        this.walBlockSize = walBlockSize;
        this.process = process;
    }

    /**
     * 终态失败观测面（对齐接收器终态死亡契约）：tail 线程因 sink 确定性异常或 managed
     * 模式 sidecar 终态失败（连续重拉耗尽，Task 6 接线）自行退出后返回根因；运行中/
     * 正常停机/未 start 为 empty。宿主应周期轮询本面，见终态即 fail-fast——tail 线程
     * 非守护，不感知即进程空转且源停更。
     *
     * @return 终态根因；非终态为 empty
     */
    @Override
    public Optional<Throwable> terminalFailure() {
        return Optional.ofNullable(terminalFailure);
    }

    /**
     * 已交付字节末尾的 LSN 消费前沿（跨线程镜像）。
     *
     * @return 末块锚（段号×段尺寸+段内末偏移）；未交付过为 0
     */
    @Override
    public long consumedLsn() {
        return consumedLsn;
    }

    /**
     * 同步启动 tail 线程（wal-file-tail，非守护）；managed 模式先拉起子进程。
     *
     * <p>关键步骤：闸门校验（重复 start / 停机后再 start 抛 ISE——生命周期单程，新会话
     * 新实例）→ 注入回调 → process.start()（managed）→ 起线程即返回（目录为空等暂态在
     * 线程内静默等待，不阻塞调用方）。chunk 回调在 tail 线程同步执行。</p>
     *
     * @param chunkEndLsnSink 前沿通知：每块交付后以本块末锚回调（供宿主前沿记账；纯
     *                        文件源无"无字节伴随的 LSN 推进"，恒与 chunkSink 同拍）
     * @param chunkSink       字节块交付回调（第一参 = 本块末字节之后的绝对 LSN，语义同
     *                        walker.feed 的 chunkEndLsn）
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
        if (process != null) {
            process.start();
        }
        worker = new Thread(this::runTailLoop, "wal-file-tail");
        worker.setDaemon(false);
        worker.start();
        LOG.info("WAL 文件源启动: dir={} 段尺寸 {} 起点 {}（{} 模式）", dir, segmentSize,
                Lsn.format(fromLsn), process != null ? "managed" : "external");
    }

    /**
     * 幂等停机：置停机位 → join tail 线程 3s（超时 interrupt 兜底）→ managed 模式终止
     * 子进程 → 关读句柄。
     *
     * <p>边界：未 start 时调用合法（仅置位）；tail 线程卡在 sink 长回调时 join 超时
     * interrupt 提前打断，停机位保证其随后自行退出。</p>
     */
    @Override
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        stopRequested = true;
        if (worker != null) {
            try {
                worker.join(STOP_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (worker.isAlive()) {
                worker.interrupt();
                LOG.warn("wal-file-tail 线程 {}ms 内未退出——已 interrupt（停机位已置）", STOP_JOIN_TIMEOUT_MS);
            }
        }
        if (process != null) {
            process.stop();
        }
        LOG.info("WAL 文件源停机: dir={} 消费前沿 {}", dir, Lsn.format(consumedLsn));
    }

    /**
     * {@link AutoCloseable#close} 的无检收窄——委托 {@link #stop()}（幂等）。
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * tail 主循环（线程体）：目录 pump → sidecar 终态检查 → 无进展睡
     * {@value #POLL_INTERVAL_MS}ms。
     *
     * <p>关键步骤：每轮 pumpOnce（段发现/增量读/切段，见其 javadoc）；IO 异常 WARN 后
     * 重试（rename 竞态暂态）；sink 的 RuntimeException 属确定性失败——置 terminalFailure
     * 后 ERROR 退出（不吞异常面，与接收器同构）。managed 模式每轮检查 sidecar 的终态
     * 失败面（Task 4 遗留接线债，Task 6 吸纳）：监督线程放弃重拉后目录不会再有新段，
     * tail 若继续空转即"源停更而宿主无终态可查"——见终态即包装传播为源级终态失败并
     * 停机（根因链保留 sidecar 异常）。finally 关句柄防泄漏。</p>
     */
    private void runTailLoop() {
        try {
            while (!stopRequested) {
                boolean progressed = false;
                try {
                    progressed = pumpOnce();
                } catch (IOException e) {
                    LOG.warn("段目录读暂态失败（下轮重扫）: {}", e.toString());
                } catch (RuntimeException e) {
                    terminalFailure = e;
                    LOG.error("WAL 文件源交付失败（确定性错误）——源停机，前沿 {}",
                            Lsn.format(consumedLsn), e);
                    return;
                }
                if (sidecarTerminalSeen()) {
                    return;
                }
                if (!progressed && !stopRequested) {
                    sleepQuiet(POLL_INTERVAL_MS);
                }
            }
        } finally {
            closeInputQuietly();
        }
    }

    /**
     * managed 模式的 sidecar 终态检查：监督线程置终态失败（连续重拉耗尽）即把失败
     * 包装传播为本源的 terminalFailure 并请求停机。
     *
     * <p>关键步骤：external 模式（process null）恒 false；sidecar 终态在场时置
     * terminalFailure（IllegalStateException 包装，cause 保留原根因）+ ERROR 日志
     * （fail-fast 轮询契约——宿主经 {@link #terminalFailure()} 感知）。线程约束：tail
     * 线程单写者。</p>
     *
     * @return sidecar 终态已传播（tail 应退出）为 true
     */
    private boolean sidecarTerminalSeen() {
        ReceivexlogProcess proc = process;
        if (proc == null) {
            return false;
        }
        Optional<Throwable> failure = proc.terminalFailure();
        if (failure.isEmpty()) {
            return false;
        }
        terminalFailure = new IllegalStateException(
                "pg_receivexlog sidecar 终态失败——目录不会再有新段，WAL 文件源联动停机", failure.get());
        LOG.error("pg_receivexlog sidecar 终态失败——WAL 文件源联动停机，前沿 {}",
                Lsn.format(consumedLsn), failure.get());
        return true;
    }

    /**
     * 单轮推进：尽可能多地把新字节交付出去，返回本轮是否有进展。
     *
     * <p>关键步骤：① 未定初始段先选段（无候选返回 false 等待）；② 解析现段文件句柄
     * （.partial→完成段 rename 后换句柄，段文件消失则等待重扫）；③ 有增量字节则读
     * [{@code currentOffset}, len) 交付（单块上限 {@value #CHUNK_LIMIT} 字节——满段分
     * 块逐轮交付）；④ 无增量时评估切段（现段读尽且终局、且目录出现更高段号文件 →
     * 换段从 0 起）。</p>
     *
     * @return 交付了字节或完成切段为 true；需等待为 false
     * @throws IOException 目录扫描或段读失败（rename 竞态窗的 NoSuchFile 属预期暂态）
     */
    private boolean pumpOnce() throws IOException {
        if (currentSegNo < 0 && !chooseInitialSegment()) {
            return false;
        }
        if (!resolveCurrentFile()) {
            return false;
        }
        // .partial 的交付上界是"已接收前沿"而非文件长度（Task 8 修复，见 receivedFrontier）
        long dataLen = currentIsPartial ? receivedFrontier(input.length()) : input.length();
        long available = dataLen - currentOffset;
        if (available > 0) {
            int n = (int) Math.min(available, CHUNK_LIMIT);
            byte[] buf = new byte[n];
            input.seek(currentOffset);
            input.readFully(buf);
            long chunkEnd = currentSegNo * segmentSize + currentOffset + n;
            chunkSink.accept(chunkEnd, buf);
            chunkEndLsnSink.accept(chunkEnd);
            consumedLsn = chunkEnd;
            currentOffset += n;
            return true;
        }
        return switchToNextSegmentIfFinal();
    }

    /**
     * .partial 文件的已接收数据前沿（末非零字节 + 1）——Task 8 修复（双通道对拍 IT
     * 实证）：OG pg_receivexlog 把 .partial 段<b>预分配成整段尺寸的全零文件</b>（strace
     * 实锚：建文件即连发 8192B 全零 write；段几何恒满 16MB），WAL 数据按接收序原地写入
     * ——文件长度与接收进度无关，按长度 tail 会把未写区的预分配零当数据交付，walker
     * 跳零页冲到段尾后，后续原地写入的真记录永不重读（尾部记录静默丢失，对拍窗口缺
     * 760 条的实锚形态）。前沿 = 末非零字节 + 1：写序单调 + 预分配零先在，末非零即
     * 已写区上界；完成段（rename 后）的零尾是真数据，不走本面。
     *
     * <p>关键步骤：自上轮前沿缓存（{@code scannedDataEnd}，adoptSegment 归零）按
     * {@value #FRONTIER_SCAN_BLOCK}B 块前向扫描——块内自尾向头找非零字节，命中则前沿
     * 推进到该字节 + 1 并继续下一块；首见全零块即停（空闲期每轮一块的快路径，成本
     * O(新数据 + 一块)）。边界与异常语义：已知限制——若文件存在"中段永久零区 + 其后
     * 更高偏移真数据"（服务端跳页的补位零形态，实测未出现——OG 客户端按预分配写序
     * 落盘），前向扫描会在零区前停住；该形态表现为 tail 停滞（可观测）而非错位，出现
     * 时按服务端 dataStart 侧信道再扩。线程约束：tail 线程单写者。</p>
     *
     * @param fileLen 文件当前长度
     * @return 已接收前沿（文件为空/全零为 0）
     * @throws IOException 块读失败
     */
    private long receivedFrontier(long fileLen) throws IOException {
        if (fileLen <= 0 || scannedDataEnd > fileLen) {
            scannedDataEnd = 0;   // 防御：文件不得回缩（异常形态归零重扫）
        }
        long frontier = scannedDataEnd;
        long p = scannedDataEnd;
        byte[] block = new byte[FRONTIER_SCAN_BLOCK];
        while (p < fileLen) {
            int n = (int) Math.min(FRONTIER_SCAN_BLOCK, fileLen - p);
            input.seek(p);
            input.readFully(block, 0, n);
            int lastNonZero = -1;
            for (int i = n - 1; i >= 0; i--) {
                if (block[i] != 0) {
                    lastNonZero = i;
                    break;
                }
            }
            if (lastNonZero < 0) {
                break;   // 全零块：已写区上界定格
            }
            frontier = p + lastNonZero + 1;
            p += n;
        }
        scannedDataEnd = frontier;
        return frontier;
    }

    /**
     * 选定初始段：fromLsn&gt;0 取目录中段号 ≥ ⌊fromLsn/段尺寸⌋ 的最小者（所在段被保留
     * 窗口清掉时自然落向后继段），段内起点 = fromLsn 段内偏移的页对齐下取整；fromLsn==0
     * 取目录最早段。
     *
     * <p>边界：目录为空 / 无 ≥ 起点段的文件 → false（等待外部落盘）。</p>
     *
     * @return 是否已采纳初始段
     * @throws IOException 目录扫描失败
     */
    private boolean chooseInitialSegment() throws IOException {
        NavigableMap<Long, Segment> segments = listSegments();
        if (segments.isEmpty()) {
            return false;
        }
        Long initialSeg;
        long offsetInSeg = 0;
        if (fromLsn > 0) {
            long wantedSeg = fromLsn / segmentSize;
            initialSeg = segments.ceilingKey(wantedSeg);
            if (initialSeg == null) {
                return false;   // 起始段尚未落盘（managed 子进程拉流中/external 对端未写）
            }
            if (initialSeg == wantedSeg) {
                offsetInSeg = (fromLsn % segmentSize) & ~((long) walBlockSize - 1);
            }
        } else {
            initialSeg = segments.firstKey();
        }
        adoptSegment(segments.get(initialSeg), offsetInSeg);
        LOG.info("WAL 文件源初始段: 段号 {} 段内起点 {}", initialSeg,
                Lsn.format(initialSeg * segmentSize + offsetInSeg));
        return true;
    }

    /**
     * 解析现段文件句柄：目录重扫后同段号文件名变化（.partial → 完成段 rename）则换
     * 句柄（偏移保留——rename 不改内容）；段号消失（异常清理）则等待下轮。
     *
     * @return 现段句柄是否可用
     * @throws IOException 目录扫描失败
     */
    private boolean resolveCurrentFile() throws IOException {
        Segment cur = listSegments().get(currentSegNo);
        if (cur == null) {
            return false;   // 现段文件暂不可见（rename 窗口/人为清理）——重扫等待
        }
        if (!cur.path().equals(currentPath)) {
            adoptSegment(cur, currentOffset);   // rename：同段号换名换句柄，偏移原样
        }
        return true;
    }

    /**
     * 切段评估：现段已读尽且内容终局（偏移到达段尺寸，或文件为完成名——.partial 尚在
     * 增长期不算终局），且目录出现更高段号的文件 → 采纳后继段从 0 起读。
     *
     * <p>关键步骤：终局判据（不足则 false 等增长/rename）→ higherKey 取后继 →
     * adoptSegment(seg, 0)。</p>
     *
     * @return 是否完成切段
     * @throws IOException 目录扫描失败
     */
    private boolean switchToNextSegmentIfFinal() throws IOException {
        boolean finalName = !currentIsPartial;
        boolean reachedEnd = currentOffset >= segmentSize;
        if (!finalName && !reachedEnd) {
            return false;   // partial 未写满：写入方仍在写，等待增长
        }
        NavigableMap<Long, Segment> segments = listSegments();
        Long next = segments.higherKey(currentSegNo);
        if (next == null) {
            return false;   // 后继段未落盘
        }
        adoptSegment(segments.get(next), 0);
        return true;
    }

    /**
     * 采纳一段为消费对象：关旧句柄 → 记段号/路径/partial 标记 → 以给定偏移开新句柄。
     *
     * @param segment       目标段（目录列表项）
     * @param offsetInSeg   段内起始偏移（初始段续传为页对齐下取整值，其余为 0）
     * @throws IOException 打开段文件失败
     */
    private void adoptSegment(Segment segment, long offsetInSeg) throws IOException {
        closeInputQuietly();
        currentSegNo = segment.segNo();
        currentPath = segment.path();
        currentIsPartial = segment.partial();
        currentOffset = offsetInSeg;
        // 前沿扫描缓存随段重置（新段从 0 起扫；rename 换名同段则保留偏移、缓存重扫无害）
        if (currentSegNo != lastScannedSegNo) {
            scannedDataEnd = 0;
            lastScannedSegNo = currentSegNo;
        }
        input = new RandomAccessFile(currentPath.toFile(), "r");
        LOG.debug("采纳段文件: {} 偏移 {}", currentPath.getFileName(), offsetInSeg);
    }

    /**
     * 扫描目录构造段号→段项的有序映射：文件名须为 24 个十六进制字符（可带
     * {@code .partial} 后缀），其余项静默跳过；同段号完成文件与 partial 并存取完成文件
     * （rename 末窗的中间态）。tli 段不参与寻址（地址空间按段号×段尺寸展开）。
     *
     * @return 按段号升序的段映射（空目录返回空映射）
     * @throws IOException 目录流打开/迭代失败
     */
    private NavigableMap<Long, Segment> listSegments() throws IOException {
        TreeMap<Long, Segment> out = new TreeMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                boolean partial = name.endsWith(PARTIAL_SUFFIX);
                if (partial) {
                    name = name.substring(0, name.length() - PARTIAL_SUFFIX.length());
                }
                if (name.length() != SEGMENT_NAME_LENGTH) {
                    continue;
                }
                Long segNo = parseSegNo(name);
                if (segNo == null) {
                    LOG.debug("跳过非段文件: {}", p.getFileName());
                    continue;
                }
                Segment candidate = new Segment(segNo, p, partial);
                out.merge(segNo, candidate,
                        (a, b) -> a.partial() && !b.partial() ? b : a);
            }
        }
        return out;
    }

    /**
     * 解析 24 字符段名的段号：字符 [8,16) 为段号高 32 位（左移 32 位），[16,24) 为低
     * 32 位；[0,8) 是时间线，不参与寻址（地址空间按段号×段尺寸展开）。
     *
     * @param name 24 个十六进制字符的段名（已剥 .partial）
     * @return 绝对段号；含非十六进制字符返回 null
     */
    private static Long parseSegNo(String name) {
        try {
            long hi = Long.parseUnsignedLong(name.substring(8, 16), 16);
            long lo = Long.parseUnsignedLong(name.substring(16), 16);
            return (hi << 32) | lo;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 静默关闭当前段句柄（清理路径不抛——IO 吞为 DEBUG）。
     */
    private void closeInputQuietly() {
        if (input != null) {
            try {
                input.close();
            } catch (IOException e) {
                LOG.debug("关段句柄失败（忽略）: {}", e.getMessage());
            }
            input = null;
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

    /**
     * 目录扫描的一项：段号与其文件路径（含 .partial 标记）。
     *
     * @param segNo   绝对段号（高 32 位左移或低 32 位，寻址 = 段号×段尺寸）
     * @param path    段文件路径
     * @param partial 是否 .partial 名（写入方尚未 rename 的完成中段）
     */
    private record Segment(long segNo, Path path, boolean partial) {
    }
}
