package org.vastdata.vbstream.reader.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.dataRecord;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.marker;

/**
 * {@link FileRollingWriter} 的滚动与 publish 语义单测(零 PG):时间切分(注入时钟推进,
 * 切分评估点只在 COMMIT)、seq 从数据目录恢复(重启续号)、data 目录 .part 残留清理
 * (只删半成品,非 .part 文件一律不动)。记录手造见 {@link ConnectTestRecords};
 * 事务边界记录的 raw 载荷 VBFG 写入器不消费(只取 marker 字段),传 null 即可。
 */
class FileRollingWriterTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /** 可推进的测试时钟(时间切分用例的时钟控制面)。 */
    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(0);

        void advanceMillis(long ms) {
            now = now.plusMillis(ms);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /**
     * 场景:时间切分——条数上限放大,距上次 publish 不足 interval 的 COMMIT 不切;时钟推进
     * 越过 interval 后的下一个 COMMIT 触发 publish,文件名形态
     * {@code <task>-<seq 16位零填充>-<yyyyMMddHHmmss>.bin}。
     */
    @Test
    void timeBasedRollPublishesOnCommitAfterInterval() throws IOException {
        MutableClock clock = new MutableClock();
        try (FileRollingWriter w = new FileRollingWriter("tk", dir.resolve("data"),
                1000, 10_000L, OutputFormat.BINARY, clock)) {
            // 第一个事务(距启动 0ms,时间未到不切)
            w.onBegin(marker(true, "1"), null);
            w.onEvent(dataRecord(1, "v"));
            assertFalse(w.onCommit(marker(false, "1"), null).isPresent(), "时间未到不切");

            clock.advanceMillis(10_001L);
            // 第二个事务的 COMMIT 越过 interval → publish
            w.onBegin(marker(true, "2"), null);
            w.onEvent(dataRecord(2, "v"));
            Optional<Path> published = w.onCommit(marker(false, "2"), null);
            assertTrue(published.isPresent(), "越时后的 COMMIT 触发 publish");
            assertTrue(published.get().getFileName().toString().matches("tk-\\d{16}-\\d{14}\\.bin"),
                    "文件名形态 <task>-<seq 16位零填充>-<时间戳>.bin: " + published.get().getFileName());
        }
    }

    /**
     * 场景:seq 恢复——数据目录预置高 seq 文件(上次会话产物),新构造的写入器从 max+1
     * 续号(下一个 publish 的文件名序号),不依赖额外状态文件。
     */
    @Test
    void seqRestoredFromDataDir() throws IOException {
        Path dataDir = dir.resolve("data");
        Files.createDirectories(dataDir);
        Files.writeString(dataDir.resolve("tk-0000000000000042-20260101000000.bin"), "old");
        try (FileRollingWriter w = new FileRollingWriter("tk", dataDir,
                1, 60_000L, OutputFormat.BINARY, Clock.systemUTC())) {
            w.onBegin(marker(true, "1"), null);
            w.onEvent(dataRecord(1, "v"));
            Optional<Path> published = w.onCommit(marker(false, "1"), null);
            assertTrue(published.isPresent(), "roll.max-records=1 首 COMMIT 即切分");
            assertTrue(published.get().getFileName().toString().startsWith("tk-0000000000000043-"),
                    "seq 恢复为目录最大 42 + 1 = 43: " + published.get().getFileName());
        }
    }

    /**
     * 场景:data 目录 .part 残留清理——预置混合目录(上次异常退出的 .part 半成品 + 已发布
     * 完整文件 + 位点文件 offsets.dat),构造即只删 .part(那些数据 offset 未推进、源端会
     * 重发,删除安全);完成文件与位点等非 .part 文件一律不动。
     */
    @Test
    void partResidueCleanedWhileOtherFilesKept() throws IOException {
        Path dataDir = dir.resolve("data");
        Files.createDirectories(dataDir);
        Path part = dataDir.resolve("tk-0000000000000001-20260101000000.bin.part");
        Path published = dataDir.resolve("tk-0000000000000002-20260101000000.bin");
        Path offsets = dataDir.resolve("offsets.dat");
        Files.writeString(part, "half");
        Files.writeString(published, "done");
        Files.writeString(offsets, "offsets");
        try (FileRollingWriter ignored = new FileRollingWriter("tk", dataDir,
                1, 60_000L, OutputFormat.BINARY, Clock.systemUTC())) {
            assertFalse(Files.exists(part), "构造后 .part 半成品已清");
            assertTrue(Files.exists(published), "已发布完成文件不动");
            assertTrue(Files.exists(offsets), "位点等非 .part 文件不动");
        }
    }

    /**
     * 场景:格式分派——SQL 格式的 publish 产物后缀 .sql、文件名前段命名规则与 binary 相同
     * (字典序 = 消费顺序与格式无关)。
     */
    @Test
    void sqlFormatPublishesSqlSuffixFile() throws IOException {
        try (FileRollingWriter w = new FileRollingWriter("tk", dir.resolve("data"),
                1, 60_000L, OutputFormat.SQL, Clock.systemUTC())) {
            w.onBegin(marker(true, "1"), null);
            w.onEvent(dataRecord(1, "v"));
            Optional<Path> published = w.onCommit(marker(false, "1"), null);
            assertTrue(published.isPresent(), "roll.max-records=1 首 COMMIT 即切分");
            assertTrue(published.get().getFileName().toString().matches("tk-\\d{16}-\\d{14}\\.sql"),
                    "SQL 格式文件后缀 .sql: " + published.get().getFileName());
            assertTrue(Files.exists(published.get()), "已 publish 文件落在数据目录");
        }
    }

    /**
     * 场景:SQL 格式文件内容形态——头注释 + 裸 BEGIN;/COMMIT; 包住语句(publish 后读回)。
     */
    @Test
    void sqlFormatFileContainsBeginStatementCommit() throws IOException {
        try (FileRollingWriter w = new FileRollingWriter("tk", dir.resolve("data"),
                1, 60_000L, OutputFormat.SQL, Clock.systemUTC())) {
            w.onBegin(marker(true, "1"), null);
            w.onEvent(dataRecord(1, "v"));
            w.onCommit(marker(false, "1"), null);
        }
        try (DirectoryStream<Path> ls = Files.newDirectoryStream(dir.resolve("data"), "*.sql")) {
            Path published = ls.iterator().next();
            String content = Files.readString(published);
            assertTrue(content.startsWith("-- vb-stream task=tk seq="), "头注释带 task 与 seq: " + content);
            assertTrue(content.contains("BEGIN;\n"), "事务边界裸 BEGIN;");
            assertTrue(content.contains("COMMIT;\n"), "事务边界裸 COMMIT;");
        }
    }
}
