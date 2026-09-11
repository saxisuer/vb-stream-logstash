package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.reader.format.FileNaming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 滚动文件写入器：内部记录 → tmp 文件 → (COMMIT 处评估切分) → fsync → 原子 rename 到数据目录。
 *
 * <p>核心约束（与 offset 联动构成"端到端不丢数据"契约，消费方是 {@link FileChangeConsumer}）：
 * <ul>
 *   <li>文件只在 COMMIT 边界切分，保证每个文件自包含完整事务</li>
 *   <li>条数（roll.max-records）或时间（roll.interval-ms，距上次 publish）先到先触发，
 *       评估点同样只在 COMMIT 处</li>
 *   <li>publish = finish(写 FOOTER + CRC) + fsync + 原子 rename，之后调用方可安全推进 offset</li>
 *   <li>启动时清空 tmp 目录残留（那些数据 offset 未推进，源端会重发——删除是安全的）</li>
 * </ul>
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 FileRollingWriter（2026-09-11 快照，
 * 格式工厂收敛为 VBFG 单形态）。
 */
final class FileRollingWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FileRollingWriter.class);

    private final String task;
    private final Path dataDir;
    private final Path tmpDir;
    private final int rollMaxRecords;
    private final long rollIntervalMs;
    private final Clock clock;

    private EventFileWriter current;
    private Path currentTmpPath;
    private long currentSeq;
    private int eventsInCurrent;
    private long lastPublishMs;

    /**
     * 责任：初始化目录与序号状态。关键步骤：建数据/tmp 目录 → 清空 tmp 残留（未 publish =
     * offset 未推进 = 源端会重发，删除安全）→ {@link FileNaming#nextSeq} 扫数据目录恢复序号。
     * 边界：目录不可建/不可清抛 IOException（启动期 fail-fast）。
     */
    FileRollingWriter(String task, Path dataDir, Path tmpDir,
                      int rollMaxRecords, long rollIntervalMs, Clock clock) throws IOException {
        this.task = task;
        this.dataDir = dataDir;
        this.tmpDir = tmpDir;
        this.rollMaxRecords = rollMaxRecords;
        this.rollIntervalMs = rollIntervalMs;
        this.clock = clock;
        Files.createDirectories(dataDir);
        Files.createDirectories(tmpDir);
        cleanTmp();
        this.currentSeq = FileNaming.nextSeq(dataDir, task);
        this.lastPublishMs = clock.millis();
        LOG.info("落地文件写入器就绪: task={}, dataDir={}, 起始 seq={}", task, dataDir, currentSeq);
    }

    /** 责任：删除 tmp 目录全部残留（每删除一个 WARN 留痕——这些是未 publish 的半成品）。 */
    private void cleanTmp() throws IOException {
        try (Stream<Path> files = Files.list(tmpDir)) {
            for (Path p : files.toList()) {
                Files.deleteIfExists(p);
                LOG.warn("清理残留 tmp 半成品文件: {}", p);
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
     * 责任：发布当前文件——finish（FOOTER + CRC）+ close + <b>原子 rename</b> 到数据目录，
     * 重置当前文件状态并推进 seq。边界：rename 失败抛 IOException（offset 未推进，
     * 调用方停引擎，重启后源端重放）。
     */
    private Path publish() throws IOException {
        current.finish();
        long crcRecords = current.recordCount();
        current.close();
        current = null;

        Path target = dataDir.resolve(FileNaming.fileName(task, currentSeq, LocalDateTime.now(clock), "bin"));
        Files.move(currentTmpPath, target, StandardCopyOption.ATOMIC_MOVE);
        lastPublishMs = clock.millis();
        LOG.info("落地文件已发布: {} (记录数={})", target, crcRecords);
        currentSeq++;
        eventsInCurrent = 0;
        return target;
    }

    /** 责任：惰性建当前文件（首个事件/BEGIN 到达才开 tmp .part 文件）。 */
    private EventFileWriter writer() throws IOException {
        if (current == null) {
            currentTmpPath = tmpPath(currentSeq);
            current = new VbfgEventWriter(currentTmpPath, (int) currentSeq, task);
        }
        return current;
    }

    private Path tmpPath(long seq) {
        return tmpDir.resolve(FileNaming.fileName(task, seq, LocalDateTime.now(clock), "bin") + ".part");
    }

    /**
     * 责任：停机清理——未 publish 的当前文件直接丢弃（offset 未推进，重启后源端重发），
     * 删除其 tmp .part 文件。边界：删除失败 WARN 吸收（残留只占磁盘，下次启动 cleanTmp 补删）。
     */
    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
            try {
                Files.deleteIfExists(currentTmpPath);
            } catch (IOException e) {
                LOG.warn("关闭时清理 tmp 文件失败: {}", currentTmpPath, e);
            }
        }
    }
}
