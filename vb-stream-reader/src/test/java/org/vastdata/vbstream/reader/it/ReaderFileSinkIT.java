package org.vastdata.vbstream.reader.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.reader.EngineLifecycle;
import org.vastdata.vbstream.reader.ReaderProperties;
import org.vastdata.vbstream.reader.SinkConfig;
import org.vastdata.vbstream.reader.file.FileChangeConsumer;
import org.vastdata.vbstream.format.BeginRecord;
import org.vastdata.vbstream.format.ChangeFileReader;
import org.vastdata.vbstream.format.ColumnDef;
import org.vastdata.vbstream.format.CommitRecord;
import org.vastdata.vbstream.format.EventRecord;
import org.vastdata.vbstream.format.Op;
import org.vastdata.vbstream.format.Record;
import org.vastdata.vbstream.format.TableDef;
import org.vastdata.vbstream.format.TableDefRecord;
import org.vastdata.vbstream.format.TypeCode;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * file 输出形态端到端 IT:{@link FileChangeConsumer} 经 {@link EngineLifecycle}(Main 生产
 * 同款装配路径)起 embedded 引擎加载 PostgresStreamConnector,Testcontainers 真 PG 多类型列
 * 写入 → VBFG 落地文件(roll.max-records=1,首事务即 publish)→ 移植的
 * {@link ChangeFileReader} 读回断言:记录序 [TABLE_DEF, BEGIN, EVENT, COMMIT] + FOOTER 完整
 * (CRC/记录数通过)、表定义与列类型对齐(含时间类型的字符串化路径)、EVENT 值与写入对齐、
 * BEGIN/COMMIT 的 txid 同源。验收面:落地链路(publish 后才推进 offset)与 VBFG 格式
 * (与 vb-cdc-file-transform 的 cdc-sink 消费端互通)在真库上的闭环。
 */
class ReaderFileSinkIT {

    @TempDir(cleanup = CleanupMode.NEVER)
    static Path tempDir;

    /** 当前测试方法的活动槽名(@AfterEach 清理)。 */
    private String slotName;

    @AfterEach
    void cleanSlot() {
        if (slotName != null) {
            ReaderPgEnv.dropSlotQuietly(slotName);
        }
    }

    /**
     * 场景 端到端落地闭环:建表(六列——INT/TEXT/BOOL/FLOAT64/DATE/TIMESTAMP,锚定时间类型
     * 字符串化路径)→ 起引擎(file consumer,roll.max-records=1)→ 等文件落地 → 停机 →
     * 读回断言记录序/表定义/值/事务标记。offset 联动(markBatchFinished 只在 publish 后)
     * 在 file 包单测锚定,此处验收真库全链路。
     *
     * @throws Exception 环境异常(容器/SQL)或断言超时原样上抛
     */
    @Test
    void fileSinkProducesVbfgFileReadableBack() throws Exception {
        String table = "t_reader_file";
        slotName = "reader_file_slot";
        ReaderPgEnv.execSql(
                "DROP TABLE IF EXISTS " + table,
                "CREATE TABLE " + table + "(id INT PRIMARY KEY, payload TEXT, flag BOOLEAN,"
                        + " score DOUBLE PRECISION, created DATE, ts TIMESTAMP)",
                "DROP PUBLICATION IF EXISTS pub_reader_file",
                "CREATE PUBLICATION pub_reader_file FOR TABLE " + table);

        Path dataDir = Files.createTempDirectory(tempDir, "cdc-files");
        Path tmpDir = Files.createTempDirectory(tempDir, "cdc-tmp");
        Path offsetFile = Files.createTempFile(tempDir, "offsets-file", ".dat");
        try (FileChangeConsumer consumer = new FileChangeConsumer(new SinkConfig(SinkConfig.Mode.FILE,
                dataDir, tmpDir, "readerit", 1, 60_000L))) {
            EngineLifecycle lifecycle = EngineLifecycle.start(
                    baseProps(slotName, "pub_reader_file",
                            Files.createTempDirectory(tempDir, "pipe"), offsetFile),
                    consumer);
            try {
                ReaderPgEnv.awaitWalsender(slotName, 30_000);
                ReaderPgEnv.execSql("INSERT INTO " + table + " VALUES (1, 'hello', true, 3.5,"
                        + " '2026-09-11', '2026-09-11 10:20:30.123456')");
                awaitPublishedFile(dataDir, 30_000);
            }
            finally {
                assertFalse(lifecycle.shutdown(60_000), "停机非失败");
            }
        }

        List<Path> files = binFiles(dataDir);
        assertEquals(1, files.size(), "首事务即切分恰发布一个文件");
        List<Record> records = new ArrayList<>();
        try (ChangeFileReader reader = ChangeFileReader.open(files.get(0))) {
            for (Record rec : reader) {
                records.add(rec);
            }
            assertTrue(reader.complete(), "读到 FOOTER——CRC/记录数校验通过");
            assertEquals("readerit", reader.sourceDb(), "文件头 sourceDb 即 task 名");
        }
        assertEquals(4, records.size(), "BEGIN + TABLE_DEF + EVENT + COMMIT");

        long txid = ((BeginRecord) records.get(0)).txid();
        TableDef def = ((TableDefRecord) records.get(1)).tableDef();
        assertEquals(table, def.table());
        assertEquals("public", def.schema());
        assertEquals(List.of("id"), def.keyColumns(), "key 列取自 record key 结构");
        assertEquals(List.of("id", "payload", "flag", "score", "created", "ts"),
                def.columns().stream().map(ColumnDef::name).toList());
        assertEquals(List.of(TypeCode.INT, TypeCode.STRING, TypeCode.BOOL, TypeCode.FLOAT64,
                TypeCode.DATE, TypeCode.TIMESTAMP),
                def.columns().stream().map(ColumnDef::type).toList(), "列类型对齐(时间类型经字符串化路径)");

        BeginRecord begin = (BeginRecord) records.get(0);
        EventRecord event = (EventRecord) records.get(2);
        CommitRecord commit = (CommitRecord) records.get(3);
        assertEquals(Op.CREATE, event.op());
        // 值对齐:int 归一 long、时间字符串化(MicroTimestamp → 'yyyy-MM-dd HH:mm:ss.SSSSSS',
        // Date → 'yyyy-MM-dd')
        assertEquals(1L, event.values()[0]);
        assertEquals("hello", event.values()[1]);
        assertEquals(Boolean.TRUE, event.values()[2]);
        assertEquals(3.5, event.values()[3]);
        assertEquals("2026-09-11", event.values()[4]);
        assertEquals("2026-09-11 10:20:30.123456", event.values()[5]);
        assertTrue(begin.txid() > 0, "BEGIN txid 为纯数字解析(连接器事务 id 形态)");
        assertEquals(begin.txid(), commit.txid(), "BEGIN/COMMIT 的 txid 同源");
        assertTrue(commit.timestamp() > 0, "COMMIT 时间戳取事务元数据 ts_ms");
    }

    /**
     * 轮询等待数据目录出现已发布 .bin 文件(100ms 间隔;超时 AssertionError——publish 链路
     * 未走通属装配/链路异常,不静默放过)。
     *
     * @param dataDir      落地数据目录
     * @param timeoutMillis 超时毫秒
     * @throws InterruptedException sleep 被中断:恢复中断位上抛
     * @throws java.io.IOException 目录扫描失败上抛
     */
    private static void awaitPublishedFile(Path dataDir, long timeoutMillis)
            throws InterruptedException, java.io.IOException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (binFiles(dataDir).isEmpty()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(timeoutMillis + "ms 内无落地文件发布(链路异常)");
            }
            Thread.sleep(100);
        }
    }

    /** 数据目录下已发布 .bin 文件清单。 */
    private static List<Path> binFiles(Path dataDir) throws java.io.IOException {
        try (DirectoryStream<Path> ls = Files.newDirectoryStream(dataDir, "*.bin")) {
            List<Path> files = new ArrayList<>();
            for (Path p : ls) {
                files.add(p);
            }
            return files;
        }
    }

    /**
     * 组装 IT 专用引擎配置——与 {@link ReaderEndToEndIT} 的 baseProps 同款(容器地址/绝对
     * 路径/亚秒反馈与每批 offset 落盘),topic.prefix 用独立值防串。
     *
     * @param slot       复制槽名(每方法独立)
     * @param publication publication 名(已预建含目标表)
     * @param pipeDir    连接器 CQ 管道目录(瞬态)
     * @param offsetFile 引擎 offset 文件
     * @return 完整引擎/连接器配置
     */
    private static Properties baseProps(String slot, String publication, Path pipeDir, Path offsetFile) {
        Properties props = new Properties();
        props.setProperty("connector.class", ReaderProperties.CONNECTOR_CLASS);
        props.setProperty("name", "reader-file-it");
        props.setProperty("database.hostname", ReaderPgEnv.PG.getHost());
        props.setProperty("database.port", String.valueOf(ReaderPgEnv.PG.getMappedPort(
                org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT)));
        props.setProperty("database.dbname", ReaderPgEnv.PG.getDatabaseName());
        props.setProperty("database.user", ReaderPgEnv.PG.getUsername());
        props.setProperty("database.password", ReaderPgEnv.PG.getPassword());
        props.setProperty("topic.prefix", "readerfileit");
        props.setProperty("slot.name", slot);
        props.setProperty("publication.name", publication);
        props.setProperty("pipe.dir", pipeDir.toAbsolutePath().toString());
        props.setProperty("slot.feedback.interval.ms", "1000");
        props.setProperty("offset.storage.file.filename", offsetFile.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        return props;
    }
}
