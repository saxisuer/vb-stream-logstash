package org.vastdata.vbstream.walsource.it.vastbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 真库 WAL 段记录器（测试工具，offline-replay Task 5——{@code spike/offline-replay/
 * WalSegmentRecorder} 的上移形态）：{@link DirectWalSource} 裸 v3 协议直连收流（无槽），
 * 把字节按 LSN 写进 16MB 段文件（24-hex 标准段名：tli + log + seg，段号语义 = log ×
 * (2^32/段尺寸) + seg——与 {@code FileWalSource.parseSegNo} 修复后的编码对齐）——
 * VastBase 真库的 pg_receivexlog 替代（OG 5.0.0 容器 libpq 对 VastBase 6.0 内核认证
 * 握手不兼容，模块自研 v3 客户端在真库已验证，spike 第二/三轮实证）。落段目录即
 * {@code FileWalSource.staticDirectory} 离线回放档的输入快照。
 *
 * <p><strong>与 spike 版的差异（测试工具化改造）</strong>：①凭据自 {@link VastTestEnv}
 * 取（{@code -Dvb.it.vastbase.*} 可覆盖——spike 版 main 参数硬编码）；②生命周期由测试
 * 方法持有（{@link #close()} 幂等停源——真库不可杀，"库死亡"由停在线 WalSource + 本
 * 记录器独立收集的段目录承载，spike 第二轮形态）；③新增 {@link #recordedFrontierLsn()}
 * 暴露已落盘前沿，供测试等待段目录覆盖目标位点后再停源转离线（避免竞速截尾）。
 * startLsn=0 时 DirectWalSource 首连取 IDENTIFY xlogpos 段对齐——覆盖当前段头；跨段
 * chunk 分段写；seek 空洞补零（walker 零区容忍面消化）。</p>
 *
 * <p>线程约束：{@link #start} 在测试线程调用；数据回调在 DirectWalSource 工作线程
 * 单写者执行（{@link #write} synchronized 双保险）；{@link #recordedFrontierLsn} 与
 * {@link #terminalFailure()} 任意线程可读（前者目录扫描容忍文件长度的瞬时旧值，后者
 * volatile 透传）。</p>
 */
final class WalSegmentRecorder implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WalSegmentRecorder.class);

    /** 段尺寸（字节）——与真库 wal_segment_size 及 FileWalSource.DEFAULT_SEGMENT_SIZE 同档（spike 实证）。 */
    private static final long SEGMENT_SIZE = 16L * 1024 * 1024;

    /** 每 log 段位（2^32 / 段尺寸 = 256）——段名第三字段的值域上界（标准 PG 编码）。 */
    private static final long SEGMENTS_PER_XLOG_ID = 0x100000000L / SEGMENT_SIZE;

    private final Path dir;
    private final DirectWalSource source;
    private final AtomicBoolean stopped = new AtomicBoolean();

    private WalSegmentRecorder(Path dir, DirectWalSource source) {
        this.dir = dir;
        this.source = source;
    }

    /**
     * 启动记录器（测试线程）：建目录 → 裸 v3 直连（凭据 {@link VastTestEnv}，无槽、
     * 心跳 10s、startLsn=0 IDENTIFY 档）→ 注入落盘回调。
     *
     * <p>边界与异常语义：目录创建失败 ISE（测试前置残缺 fail-fast）；首连失败由
     * DirectWalSource 内部重连（5 次耗尽置终态，经 {@link #terminalFailure()} 观测）
     * ——不在本方法同步抛出。</p>
     *
     * @param walDir 段落盘目录（建议 {@link VastTestEnv#newStateDir} 独立子目录）
     * @return 已运行的记录器（调用方负责 {@link #close()}）
     */
    static WalSegmentRecorder start(Path walDir) {
        try {
            Files.createDirectories(walDir);
        } catch (IOException e) {
            throw new IllegalStateException("WAL 段目录创建失败: " + walDir, e);
        }
        DirectWalSource src = new DirectWalSource(VastTestEnv.host(), VastTestEnv.port(),
                VastTestEnv.user(), VastTestEnv.pass(), null, 10_000L, 0L);
        WalSegmentRecorder recorder = new WalSegmentRecorder(walDir, src);
        src.start(lsn -> { }, (endLsn, bytes) -> recorder.write(endLsn - bytes.length, bytes));
        LOG.info("WAL 段记录器启动: dir={}（16MB 标准段名，DirectWalSource 无槽裸流）", walDir);
        return recorder;
    }

    /**
     * 已落盘前沿（LSN）：扫描段目录，取各段"段起始 LSN + 文件长度"的最大值——记录器
     * 自首连段头起连续写、无预分配，文件末即前沿（seek 空洞的零填充只出现在覆盖区间
     * 内部，不影响末位语义）。
     *
     * <p>边界与异常语义：目录无段文件/不可读归 0（由等待方继续轮询）；段名非法的文件
     * 跳过（目录里不应有其他文件，防御面）。线程约束：任意线程可读。</p>
     *
     * @return 已落盘字节末尾的 LSN；尚无段文件为 0
     */
    long recordedFrontierLsn() {
        long frontier = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                Long segNo = parseSegNo(p.getFileName().toString());
                if (segNo == null) {
                    continue;
                }
                long end = segNo * SEGMENT_SIZE + Files.size(p);
                frontier = Math.max(frontier, end);
            }
        } catch (IOException e) {
            return 0;
        }
        return frontier;
    }

    /**
     * 透传 DirectWalSource 终态失败（重连耗尽/落盘回调未捕获异常）——测试 await 超时
     * 消息的诊断面。
     *
     * @return 终态根因；运行中/正常停机为 empty
     */
    Optional<Throwable> terminalFailure() {
        return source.terminalFailure();
    }

    /**
     * 幂等停源：首个调用者停 DirectWalSource（工作线程退出、socket 关闭），重复调用
     * no-op——测试 finally 与 AfterEach 双清理面安全。
     */
    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            source.stop();
            LOG.info("WAL 段记录器停止: dir={} 已落盘前沿 {}", dir, Lsn.format(recordedFrontierLsn()));
        }
    }

    /**
     * 把 {@code [start, start+len)} 字节按 16MB 段切分写入对应段文件对应偏移（spike
     * 逻辑原样上移）：段名 = tli(1) + log + seg（段号 = floorDiv(abs, 段尺寸) 后标准
     * 编码）；跨段 chunk 在段边界分段写；RandomAccessFile seek 天然补零空洞。
     *
     * <p>边界与异常语义：IO 失败抛 {@link IllegalStateException}（回调线程未捕获即
     * DirectWalSource 终态——测试经 {@link #terminalFailure()} 观测，fail-fast 优于
     * 静默丢段）。线程约束：synchronized——DirectWalSource 单写者回调，双保险。</p>
     *
     * @param start 载荷起始 LSN
     * @param data  载荷字节
     */
    private synchronized void write(long start, byte[] data) {
        long pos = 0;
        while (pos < data.length) {
            long abs = start + pos;
            long segNo = Math.floorDiv(abs, SEGMENT_SIZE);
            long inSeg = abs - segNo * SEGMENT_SIZE;
            int len = (int) Math.min(data.length - pos, SEGMENT_SIZE - inSeg);
            String name = String.format("%08X%08X%08X", 1L,
                    segNo / SEGMENTS_PER_XLOG_ID, segNo % SEGMENTS_PER_XLOG_ID);
            try (RandomAccessFile f = new RandomAccessFile(dir.resolve(name).toFile(), "rw")) {
                f.seek(inSeg);
                f.write(data, (int) pos, len);
            } catch (IOException e) {
                throw new IllegalStateException("WAL 段落盘失败: " + name, e);
            }
            pos += len;
        }
    }

    /**
     * 段名解析（{@code FileWalSource.parseSegNo} 同语义的镜像）：24 位大写 hex =
     * tli[0,8) + log[8,16) + seg[16,24)，段号 = log × 256 + seg；seg 值域越界判非段
     * 文件（null）。
     *
     * @param name 文件名
     * @return 段号；非法段名为 null
     */
    private static Long parseSegNo(String name) {
        if (name.length() != 24 || !name.matches("[0-9A-F]{24}")) {
            return null;
        }
        long log = Long.parseLong(name.substring(8, 16), 16);
        long seg = Long.parseLong(name.substring(16, 24), 16);
        if (seg >= SEGMENTS_PER_XLOG_ID) {
            return null;
        }
        return log * SEGMENTS_PER_XLOG_ID + seg;
    }
}
