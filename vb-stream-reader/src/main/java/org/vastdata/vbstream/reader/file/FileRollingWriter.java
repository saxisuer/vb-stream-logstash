package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.format.FileNaming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 滚动文件写入器（单目录形态）：内部记录 → data 目录内 {@code .part} 半成品 →
 * (COMMIT 处评估切分) → fsync → <b>同目录</b>原子 rename 去掉 {@code .part} 后缀。
 *
 * <p>核心约束（与 offset 联动构成"端到端不丢数据"契约，消费方是 {@link FileChangeConsumer}）：
 * <ul>
 *   <li>文件只在 COMMIT 边界切分，保证每个文件自包含完整事务</li>
 *   <li>条数（roll.max-records）或时间（roll.interval-ms，距上次 publish）先到先触发，
 *       评估点同样只在 COMMIT 处</li>
 *   <li>publish = finish(格式自封尾——binary 写 FOOTER + CRC，sql flush + fsync)
 *       + <b>同目录</b>原子 rename 去后缀——同目录 rename 天然同分区，不存在跨分区失败
 *       窗口；之后调用方可安全推进 offset</li>
 *   <li>启动时只清理 data 目录内 {@code .part} 残留半成品（那些数据 offset 未推进，
 *       源端会重发——删除是安全的）；完成文件与位点文件等非 {@code .part} 文件一律不动
 *       （data 目录里不带 {@code .part} 后缀的数据文件即完整文件）</li>
 * </ul>
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 FileRollingWriter（2026-09-11 快照，
 * 格式经 {@link OutputFormat} 注入；2026-09-16 对齐对方仓 tmp 目录合并进 data 目录的
 * refactor——.part 同目录暂存，tmp-dir 配置随之移除）。
 */
final class FileRollingWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FileRollingWriter.class);

    /** 写入中文件的暂存后缀（publish = 同目录 rename 去掉本后缀）。 */
    private static final String PART_SUFFIX = ".part";

    private final String task;
    private final Path dataDir;
    private final int rollMaxRecords;
    private final long rollIntervalMs;
    private final OutputFormat format;
    private final Clock clock;

    private EventFileWriter current;
    private Path currentPartPath;
    private long currentSeq;
    private int eventsInCurrent;
    private long lastPublishMs;

    /**
     * 责任：初始化目录与序号状态。关键步骤：建 data 目录 → 清理 {@code .part} 残留半成品
     * （未 publish = offset 未推进 = 源端会重发，删除安全；非 {@code .part} 文件不动）→
     * {@link FileNaming#nextSeq} 扫数据目录恢复序号。边界：目录不可建/不可清抛 IOException
     * （启动期 fail-fast）。
     */
    FileRollingWriter(String task, Path dataDir,
                      int rollMaxRecords, long rollIntervalMs, OutputFormat format, Clock clock) throws IOException {
        this.task = task;
        this.dataDir = dataDir;
        this.rollMaxRecords = rollMaxRecords;
        this.rollIntervalMs = rollIntervalMs;
        this.format = format;
        this.clock = clock;
        Files.createDirectories(dataDir);
        cleanStaleParts();
        this.currentSeq = FileNaming.nextSeq(dataDir, task);
        this.lastPublishMs = clock.millis();
        LOG.info("落地文件写入器就绪: task={}, dataDir={}, format={}, 起始 seq={}",
                task, dataDir, format, currentSeq);
    }

    /**
     * 责任：只清理 data 目录内 {@code .part} 残留半成品（每删除一个 WARN 留痕——这些是
     * 未 publish 的半成品）；完成文件与位点文件等非 {@code .part} 文件一律不动。
     */
    private void cleanStaleParts() throws IOException {
        try (Stream<Path> files = Files.list(dataDir)) {
            for (Path p : files.toList()) {
                if (p.getFileName().toString().endsWith(PART_SUFFIX)) {
                    Files.deleteIfExists(p);
                    LOG.warn("清理 data 目录残留 .part 半成品文件: {}", p);
                }
            }
        }
    }

    void onBegin(TransactionMarker marker, SourceRecord raw) throws IOException {
        writer().writeBegin(marker, raw);
    }

    /** 责任：写一条数据事件；eventsInCurrent 计数自增（COMMIT 处的条数切分判据）。 */
    void onEvent(SourceRecord record) throws IOException {
        writer().writeEvent(record);
        eventsInCurrent++;
    }

    /** 责任：写 COMMIT 并评估是否切分；publish 成功返回已落地文件路径，否则 empty。 */
    Optional<Path> onCommit(TransactionMarker marker, SourceRecord raw) throws IOException {
        writer().writeCommit(marker, raw);
        if (shouldRoll()) {
            return Optional.of(publish());
        }
        return Optional.empty();
    }

    /** 切分判据：条数上限或距上次 publish 的时长先到先触发；无当前文件（空事务）不切。 */
    private boolean shouldRoll() {
        if (current == null) {
            return false;
        }
        boolean byCount = eventsInCurrent >= rollMaxRecords;
        boolean byTime = clock.millis() - lastPublishMs >= rollIntervalMs;
        return byCount || byTime;
    }

    /**
     * 责任：发布当前文件——finish（格式自封尾）+ close + <b>同目录原子 rename</b> 去掉
     * {@code .part} 后缀，重置当前文件状态并推进 seq。边界：rename 失败抛 IOException
     * （offset 未推进，调用方停引擎，重启后源端重放）。
     */
    private Path publish() throws IOException {
        current.finish();
        long crcRecords = current.recordCount();
        current.close();
        current = null;

        Path target = dataDir.resolve(FileNaming.fileName(task, currentSeq, LocalDateTime.now(clock),
                format.extension()));
        Files.move(currentPartPath, target, StandardCopyOption.ATOMIC_MOVE);
        lastPublishMs = clock.millis();
        LOG.info("落地文件已发布: {} (记录数={})", target, crcRecords);
        currentSeq++;
        eventsInCurrent = 0;
        return target;
    }

    /** 责任：惰性建当前文件（首个事件/BEGIN 到达才在 data 目录开 .part 半成品，写入器按注入格式分派）。 */
    private EventFileWriter writer() throws IOException {
        if (current == null) {
            currentPartPath = partPath(currentSeq);
            current = format.newWriter(currentPartPath, (int) currentSeq, task);
        }
        return current;
    }

    /** data 目录内的半成品路径：正式文件名 + {@code .part} 后缀（publish 时同目录 rename 去后缀）。 */
    private Path partPath(long seq) {
        return dataDir.resolve(FileNaming.fileName(task, seq, LocalDateTime.now(clock),
                format.extension()) + PART_SUFFIX);
    }

    /**
     * 责任：停机清理——未 publish 的当前文件直接丢弃（offset 未推进，重启后源端重发），
     * 删除其 {@code .part} 半成品。边界：删除失败 WARN 吸收（残留只占磁盘，下次启动
     * cleanStaleParts 补删）。
     */
    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
            try {
                Files.deleteIfExists(currentPartPath);
            } catch (IOException e) {
                LOG.warn("关闭时清理 .part 半成品失败: {}", currentPartPath, e);
            }
        }
    }
}
