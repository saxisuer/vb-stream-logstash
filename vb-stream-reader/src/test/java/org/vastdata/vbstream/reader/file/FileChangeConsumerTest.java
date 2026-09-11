package org.vastdata.vbstream.reader.file;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.reader.SinkConfig;
import org.vastdata.vbstream.reader.format.BeginRecord;
import org.vastdata.vbstream.reader.format.ChangeFileReader;
import org.vastdata.vbstream.reader.format.CommitRecord;
import org.vastdata.vbstream.reader.format.EventRecord;
import org.vastdata.vbstream.reader.format.Record;
import org.vastdata.vbstream.reader.format.TableDefRecord;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.vastdata.vbstream.reader.file.ConnectTestRecords.dataRecord;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.event;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.txRecord;

/**
 * {@link FileChangeConsumer} 的 offset 联动单测(零 PG):手造事务元数据与数据
 * {@link SourceRecord} 驱动 handleBatch,断言核心契约——<b>markBatchFinished 只在本批有文件
 * publish 后调用</b>(数据完整落地前 offset 绝不推进)、tombstone 跳过、已发布文件可被
 * {@link ChangeFileReader} 完整读回(RoundTrip 闭环)。记录手造见 {@link ConnectTestRecords}。
 */
class FileChangeConsumerTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /**
     * 场景:publish 后放行 offset——roll.max-records=1(首个 COMMIT 即切分),一批
     * [BEGIN, event, END] 消费后:数据目录恰一个 .bin 文件;文件经 ChangeFileReader 读回
     * 记录序 [TABLE_DEF, BEGIN, EVENT, COMMIT] 且 complete(EVENT 值与手造记录对齐);
     * committer 三条 markProcessed、恰一次 markBatchFinished。
     */
    @Test
    void publishedBatchAdvancesOffsetAndFileRoundTrips() throws Exception {
        Path dataDir = dir.resolve("data");
        try (FileChangeConsumer consumer = new FileChangeConsumer(
                new SinkConfig(SinkConfig.Mode.FILE, dataDir, dir.resolve("tmp"), "t", 1, 60_000L))) {
            RecordingCommitter committer = new RecordingCommitter();
            consumer.handleBatch(List.of(
                    event(txRecord("BEGIN", "769")),
                    event(dataRecord(1, "v1")),
                    event(txRecord("END", "769"))), committer);

            assertEquals(3, committer.processed, "逐条 markProcessed");
            assertEquals(1, committer.batchFinished, "有 publish 才 markBatchFinished(offset 联动核心)");
            List<Path> files = binFiles(dataDir);
            assertEquals(1, files.size(), "首个 COMMIT 即切分发布一个文件");

            List<Class<? extends Record>> kinds = new ArrayList<>();
            EventRecord roundTripped = null;
            try (ChangeFileReader reader = ChangeFileReader.open(files.get(0))) {
                for (Record rec : reader) {
                    kinds.add(rec.getClass());
                    if (rec instanceof EventRecord e) {
                        roundTripped = e;
                    }
                }
                assertTrue(reader.complete(), "读到 FOOTER,CRC/记录数校验通过");
            }
            assertEquals(List.of(BeginRecord.class, TableDefRecord.class, EventRecord.class, CommitRecord.class),
                    kinds, "记录序 BEGIN/TABLE_DEF/EVENT/COMMIT(TABLE_DEF 随首个事件写入,故在 BEGIN 后)");
            assertEquals(1L, roundTripped.values()[0], "EVENT 的 id 列值往返(INT 列手造 INT32 归一 long)");
            assertEquals("v1", roundTripped.values()[1], "EVENT 的 payload 列值往返");
            assertArrayEquals(new Object[]{1L, "v1"}, roundTripped.values(), "EVENT 全列往返");
        }
    }

    /**
     * 场景:未达切分阈值不推进 offset——roll 上限大,同批 [BEGIN, event, END] 消费后无文件
     * 落地、markBatchFinished 不调用(markProcessed 照常——逐条记账不受影响);数据在 tmp
     * .part 中暂存,close 丢弃(重启后源端重放,不丢数据)。
     */
    @Test
    void unpublishedBatchHoldsOffsetAndCloseDiscardsTmp() throws Exception {
        Path dataDir = dir.resolve("data");
        Path tmpDir = dir.resolve("tmp");
        FileChangeConsumer consumer = new FileChangeConsumer(
                new SinkConfig(SinkConfig.Mode.FILE, dataDir, tmpDir, "t", 1000, 60_000L));
        RecordingCommitter committer = new RecordingCommitter();
        consumer.handleBatch(List.of(
                event(txRecord("BEGIN", "770")),
                event(dataRecord(2, "v2")),
                event(txRecord("END", "770"))), committer);

        assertEquals(3, committer.processed, "markProcessed 照常逐条登记");
        assertEquals(0, committer.batchFinished, "无 publish 绝不 markBatchFinished");
        assertTrue(binFiles(dataDir).isEmpty(), "未达阈值不落地数据目录");
        consumer.close();
        assertTrue(binFiles(dataDir).isEmpty(), "close 丢弃未 publish 的 tmp(不丢数据——offset 未推进)");
        try (DirectoryStream<Path> ls = Files.newDirectoryStream(tmpDir)) {
            assertTrue(!ls.iterator().hasNext(), "tmp 目录在 close 后已空");
        }
    }

    /** 场景:tombstone(value=null 的 ChangeEvent)兜底跳过——不写文件也不记账。 */
    @Test
    void tombstoneSkipped() throws Exception {
        try (FileChangeConsumer consumer = new FileChangeConsumer(
                new SinkConfig(SinkConfig.Mode.FILE, dir.resolve("data"), dir.resolve("tmp"), "t", 1, 60_000L))) {
            RecordingCommitter committer = new RecordingCommitter();
            consumer.handleBatch(List.of(event(null)), committer);
            assertEquals(0, committer.processed, "tombstone 不记账");
            assertEquals(0, committer.batchFinished, "无 publish 不推进");
        }
    }

    /** 记账侧 fake:只计数 markProcessed/markBatchFinished(offset 联动断言的观测面)。 */
    private static final class RecordingCommitter implements RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> {
        int processed;
        int batchFinished;

        @Override
        public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record) {
            processed++;
        }

        @Override
        public void markBatchFinished() {
            batchFinished++;
        }

        @Override
        public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record,
                                  DebeziumEngine.Offsets offsets) {
            markProcessed(record);
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return null;   // fake 只被测试直接驱动,engine 侧的 offsets 构造不进本断言面
        }
    }

    /** 数据目录下已发布 .bin 文件清单。 */
    private static List<Path> binFiles(Path dataDir) throws IOException {
        try (DirectoryStream<Path> ls = Files.newDirectoryStream(dataDir, "*.bin")) {
            List<Path> files = new ArrayList<>();
            for (Path p : ls) {
                files.add(p);
            }
            return files;
        }
    }
}
