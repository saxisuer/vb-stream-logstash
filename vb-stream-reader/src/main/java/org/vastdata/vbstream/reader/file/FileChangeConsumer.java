package org.vastdata.vbstream.reader.file;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.reader.SinkConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Optional;

/**
 * CDC 记录落地 consumer——{@link ChangeConsumer} 的批形态实现：解析 envelope → 写滚动文件
 * （{@link FileRollingWriter}），offset 与文件 publish <b>严格联动</b>。
 *
 * <p>核心契约（端到端不丢数据）：{@code markProcessed} 逐条登记，但
 * {@code markBatchFinished()}（真正推进 offset/slot）只在 {@link FileRollingWriter#onCommit}
 * 返回已 publish 文件后调用——数据完整落到数据目录之前，offset 绝不推进；写文件 IO 异常
 * 直接抛出使引擎停止（offset 不动，重启后源端重放）。每事务 COMMIT 后 INFO 一行摘要到
 * CDC 专用 logger（file 形态的可观测面——不逐条刷屏，publish 行由 FileRollingWriter 另出）。
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 CaptureConsumer（2026-09-11 快照）。
 *
 * <p>线程约束：handleBatch 由 engine 任务轮询线程按提交序串行调用（ORDERED 处理器）；
 * 单实例单线程使用。抛 InterruptedException 会停引擎（契约）。
 */
public final class FileChangeConsumer implements ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>>, AutoCloseable {

    /** CDC 专用 logger（与 LogChangeConsumer 同一分离通道，可独立调级）。 */
    private static final Logger CDC = LoggerFactory.getLogger("org.vastdata.vbstream.reader.cdc");

    private static final Logger LOG = LoggerFactory.getLogger(FileChangeConsumer.class);

    private final FileRollingWriter writer;

    /**
     * 责任：从配置建立滚动写入器（目录/tmp 清理/seq 恢复都在其构造内完成）。
     * 边界：构造抛 IOException（目录不可用）由调用方按启动失败处理。
     */
    public FileChangeConsumer(SinkConfig config) throws IOException {
        this.writer = new FileRollingWriter(config.task(), config.dataDir(), config.tmpDir(),
                config.rollMaxRecords(), config.rollIntervalMs(), Clock.systemUTC());
    }

    /**
     * 责任：消费一批变更事件——解析事务边界驱动滚动写入器，publish 后才放行 offset。
     * 关键步骤：循环内 {@code event.value()} 取原始 {@link SourceRecord}（null 为 tombstone
     * 兜底跳过）→ 事务元数据记录（{@link EnvelopeParser#isTransactionMetadata} 结构特征
     * 识别）按 BEGIN/END 分流到 {@code onBegin}/{@code onCommit}，数据记录走 {@code onEvent}
     * → {@code committer.markProcessed(event)} 逐条登记；本批内有任一文件 publish 才在批尾
     * {@code markBatchFinished()} 推进 offset。边界：写文件 IO 失败包装 UncheckedIOException
     * 上抛（引擎停止、offset 未推进、重启重放——不吞）；InterruptedException 原样上抛。
     *
     * @param records  本批事件（Connect 格式直通，value 即 SourceRecord）
     * @param committer offset 记账器
     * @throws InterruptedException 停机中断（引擎契约，原样上抛）
     */
    @Override
    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records,
                            RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        boolean published = false;
        long events = 0;
        String lastTxid = null;
        try {
            for (ChangeEvent<SourceRecord, SourceRecord> changeEvent : records) {
                SourceRecord record = changeEvent.value();
                if (record == null) {
                    continue; // tombstone（连接器配置已关，兜底）
                }
                if (EnvelopeParser.isTransactionMetadata(record)) {
                    TransactionMarker marker = EnvelopeParser.parseTransaction(record);
                    lastTxid = marker.txid();
                    if (marker.begin()) {
                        writer.onBegin(marker, record);
                    } else {
                        Optional<Path> file = writer.onCommit(marker, record);
                        if (file.isPresent()) {
                            CDC.info("TXN-END txid={} events={} 落地={}", marker.txid(), events, file.get());
                            published = true;
                        } else {
                            CDC.info("TXN-END txid={} events={} 落地=暂存(未达切分阈值)", marker.txid(), events);
                        }
                        events = 0;
                    }
                } else {
                    writer.onEvent(record);
                    events++;
                }
                committer.markProcessed(changeEvent);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写落地文件失败，停止引擎（offset 未推进，重启后重放）", e);
        }
        if (published) {
            // 数据已完整 publish，现在才允许推进 offset/slot
            committer.markBatchFinished();
        }
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
    }

    /**
     * 责任：停机关闭滚动写入器——未 publish 的 tmp 文件直接丢弃（offset 未推进，重启后
     * 源端重发）。边界：关闭失败 WARN 吸收（停机路径不被次生异常掩盖；残留 tmp 下次启动
     * cleanTmp 补删）。
     */
    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            LOG.warn("关闭落地文件写入器失败: {}", e.getMessage());
        }
    }
}
