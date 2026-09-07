package org.vastdata.vbstream.reader.it;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.reader.EngineLifecycle;
import org.vastdata.vbstream.reader.ReaderProperties;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * reader 端到端 IT:经 {@link EngineLifecycle}(Main 生产同款装配路径)起 embedded 引擎
 * 加载 PostgresStreamConnector,Testcontainers 真 PG 写入 → ChangeConsumer 回调收数断言。
 * 两场景:①端到端——单事务 3 行 INSERT 断言数据记录(op=c)+ 事务元数据 BEGIN/END 对 +
 * 零 op=r(snapshot 钉死 no_data);②重启无重复——同槽同 offset 文件两轮运行,第一轮已收
 * 且槽确认位已推进的事务第二轮不重发,新写入恰收新记录(offset 文件接线验收)。
 *
 * <p>习语(连接器 IT 同源):每方法独立槽名/publication/表名(容器共享,残留槽会从旧
 * confirmed_flush 续传吞数据);start 后写数据前必等 walsender 挂上(建流完成汇合点);
 * offset.flush.interval.ms=0(每批落盘,断言窗口快)与 slot.feedback.interval.ms=1000
 * (亚秒反馈,槽确认位及时推进——场景二的安全停机前置)。
 */
class ReaderEndToEndIT {

    /** 事务元数据 topic 名(provide.transaction.metadata 默认 true 注入,topic.prefix 之后)。 */
    private static final String TX_TOPIC = "readerit.transaction";

    @TempDir(cleanup = CleanupMode.NEVER)
    static Path tempDir;

    /** 当前测试方法的活动槽名(@AfterEach 清理)。 */
    private String slotName;

    /**
     * 收尾:清理槽(幂等,WARN 吸收)。表/publication 留库无害(每方法独立名,不互相干扰)。
     */
    @AfterEach
    void cleanSlot() {
        if (slotName != null) {
            ReaderPgEnv.dropSlotQuietly(slotName);
        }
    }

    /**
     * 场景一 端到端:建表/publication → 起引擎 → 单事务 INSERT 3 行 → 断言回调收到
     * 3 条数据记录(全 op=c,id 齐)+ 1 对 BEGIN/END 事务元数据 + 零 op=r → 停机断言非失败。
     * 验收面:Main 生产装配路径(DebeziumEngine.create(Connect.class) + ChangeConsumer)
     * 在真 PG 上把 CDC 记录与事务块完整送达回调。
     *
     * @throws Exception 环境异常(容器/SQL)或断言超时原样上抛
     */
    @Test
    void endToEndDeliversDataAndTransactionMetadata() throws Exception {
        String table = "t_reader_e2e";
        slotName = "reader_e2e_slot";
        ReaderPgEnv.execSql(
                "DROP TABLE IF EXISTS " + table,
                "CREATE TABLE " + table + "(id INT PRIMARY KEY, payload TEXT)",
                "DROP PUBLICATION IF EXISTS pub_reader_e2e",
                "CREATE PUBLICATION pub_reader_e2e FOR TABLE " + table);

        RecordingConsumer consumer = new RecordingConsumer();
        EngineLifecycle lifecycle = EngineLifecycle.start(
                baseProps(slotName, "pub_reader_e2e",
                        Files.createTempDirectory(tempDir, "pipe"), offsetFile("e2e")),
                consumer);
        try {
            ReaderPgEnv.awaitWalsender(slotName, 30_000);
            ReaderPgEnv.execSql("INSERT INTO " + table + " VALUES (1,'a1'),(2,'a2'),(3,'a3')");

            consumer.awaitRecords(5, 60_000);
            List<SourceRecord> data = consumer.dataRecords();
            assertEquals(3, data.size(), "数据记录恰 3 条(事务元数据另计)");
            assertEquals("readerit.public." + table, data.get(0).topic(), "数据 topic 命名");
            data.forEach(r -> assertEquals("c", opOf(r), "全部为 INSERT(无快照 op=r)"));
            assertEquals(3, data.stream().map(ReaderEndToEndIT::idOf).distinct().count(), "id 不重复");

            assertEquals(List.of("BEGIN", "END"), consumer.txStatuses(),
                    "事务元数据恰一对 BEGIN/END(顺序即交付序)");
        }
        finally {
            assertFalse(lifecycle.shutdown(60_000), "停机非失败(回调无 error)");
        }
    }

    /**
     * 场景二 重启无重复(offset 文件接线验收):第一轮收 1 行 → 等槽 confirmed_flush 越过
     * 已收事务(连接器 1s 反馈周期内推进——安全停机前置,不等则服务端重发属合法 at-least-once
     * 而非断言目标)→ 停机(final commitOffsets 落盘)→ 同槽同 offset 文件重启 → 静默窗口
     * 断言零重发 → 再插 1 行恰收新记录。
     *
     * @throws Exception 环境异常(容器/SQL)或断言超时原样上抛
     */
    @Test
    void restartResumesWithoutDuplicate() throws Exception {
        String table = "t_reader_restart";
        slotName = "reader_restart_slot";
        ReaderPgEnv.execSql(
                "DROP TABLE IF EXISTS " + table,
                "CREATE TABLE " + table + "(id INT PRIMARY KEY, payload TEXT)",
                "DROP PUBLICATION IF EXISTS pub_reader_restart",
                "CREATE PUBLICATION pub_reader_restart FOR TABLE " + table);

        Path pipeDir1 = Files.createTempDirectory(tempDir, "pipe1");
        Path pipeDir2 = Files.createTempDirectory(tempDir, "pipe2");
        Path offsetFile = offsetFile("restart");

        RecordingConsumer consumer = new RecordingConsumer();
        EngineLifecycle first = EngineLifecycle.start(
                baseProps(slotName, "pub_reader_restart", pipeDir1, offsetFile), consumer);
        try {
            ReaderPgEnv.awaitWalsender(slotName, 30_000);
            ReaderPgEnv.execSql("INSERT INTO " + table + " VALUES (1,'first')");
            consumer.awaitRecords(3, 60_000);

            String confirmedBaseline = consumer.maxLsnText();
            awaitStandbyFlush(confirmedBaseline);
        }
        finally {
            assertFalse(first.shutdown(60_000), "第一轮停机非失败");
        }

        RecordingConsumer secondConsumer = new RecordingConsumer();
        EngineLifecycle second = EngineLifecycle.start(
                baseProps(slotName, "pub_reader_restart", pipeDir2, offsetFile), secondConsumer);
        try {
            ReaderPgEnv.awaitWalsender(slotName, 30_000);
            Thread.sleep(5_000);
            assertEquals(0, secondConsumer.records.size(),
                    "重启静默窗口无旧事务重发(槽确认位已越过第一轮事务)");

            ReaderPgEnv.execSql("INSERT INTO " + table + " VALUES (2,'second')");
            secondConsumer.awaitRecords(3, 60_000);
            assertEquals(List.of("BEGIN", "END"), secondConsumer.txStatuses());
            assertEquals(1, secondConsumer.dataRecords().size(), "第二轮恰收新 1 行数据记录");
        }
        finally {
            assertFalse(second.shutdown(60_000), "第二轮停机非失败");
        }
    }

    /**
     * 组装 IT 专用引擎配置——与 {@link ReaderProperties} 生产透传同键,但取值全部确定性
     * (容器地址/绝对路径/亚秒反馈与每批 offset 落盘)。关键步骤:connector.class 用
     * {@link ReaderProperties#CONNECTOR_CLASS} 同源常量;database.* 取容器映射;
     * offset.flush.interval.ms=0(每批落盘)与 slot.feedback.interval.ms=1000(槽确认位
     * 及时推进——重启场景安全停机前置)。
     *
     * @param slot       复制槽名(每方法独立)
     * @param publication publication 名(已预建含目标表)
     * @param pipeDir    连接器 CQ 管道目录(瞬态,每轮独立目录)
     * @param offsetFile 引擎 offset 文件(重启场景两轮共用同一路径)
     * @return 完整引擎/连接器配置
     */
    private static Properties baseProps(String slot, String publication, Path pipeDir, Path offsetFile) {
        Properties props = new Properties();
        props.setProperty("connector.class", ReaderProperties.CONNECTOR_CLASS);
        props.setProperty("name", "reader-it");
        props.setProperty("database.hostname", ReaderPgEnv.PG.getHost());
        props.setProperty("database.port", String.valueOf(ReaderPgEnv.PG.getMappedPort(
                org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT)));
        props.setProperty("database.dbname", ReaderPgEnv.PG.getDatabaseName());
        props.setProperty("database.user", ReaderPgEnv.PG.getUsername());
        props.setProperty("database.password", ReaderPgEnv.PG.getPassword());
        props.setProperty("topic.prefix", "readerit");
        props.setProperty("slot.name", slot);
        props.setProperty("publication.name", publication);
        props.setProperty("pipe.dir", pipeDir.toAbsolutePath().toString());
        props.setProperty("slot.feedback.interval.ms", "1000");
        props.setProperty("offset.storage.file.filename", offsetFile.toAbsolutePath().toString());
        props.setProperty("offset.flush.interval.ms", "0");
        return props;
    }

    /**
     * 在 tempDir 下建引擎 offset 文件(预创建空文件,两轮运行共用同一路径)。
     *
     * @param tag 场景标签(文件名防撞)
     * @return 空文件的绝对路径
     * @throws java.io.IOException 创建失败上抛
     */
    private static Path offsetFile(String tag) throws java.io.IOException {
        Path file = Files.createTempFile(tempDir, "offsets-" + tag, ".dat");
        return file;
    }

    /**
     * 轮询等待 walsender 采纳的客户端 flush 位点覆盖基线(上限 30s,超时 AssertionError——
     * 反馈不推进属装配异常,不静默放过)。判定面取 pg_stat_replication.flush_lsn 而非槽目录
     * confirmed_flush:后者空闲库不落库(Diag 实证,引擎 CLAUDE.md 记档),前者是 status 包
     * 采纳的即时面。基线取<b>已收记录自身 lsn 的最大值</b>(连接器事务边界 offset 双写——
     * 即已输出事务的 endLsn),反馈确认值按输出前沿封顶恰好能推进到它:若取
     * pg_current_wal_lsn()(WAL 末端)则永远无法覆盖(前沿只到事务尾)。
     *
     * @param baselineLsn 基线 LSN 文本("X/Y" 形态,已收事务的 endLsn)
     * @throws InterruptedException sleep 被中断:恢复中断位上抛
     * @throws SQLException 位点查询失败上抛
     */
    private void awaitStandbyFlush(String baselineLsn) throws InterruptedException, SQLException {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!ReaderPgEnv.standbyFlushCovers(slotName, baselineLsn)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("槽 " + slotName + " 的 standby flush_lsn 30s 内未覆盖基线 "
                        + baselineLsn + "(LSN 反馈未推进——装配异常)");
            }
            Thread.sleep(200);
        }
    }

    /**
     * LSN long → "X/Y" 文本(高/低各 32 位十六进制)——pg_lsn 输入的硬性格式要求,
     * 单段十六进制会被服务端拒绝(首跑实测)。
     *
     * @param lsn LSN long 值(Debezium offset 的数值形态)
     * @return "X/Y" 形态文本
     */
    private static String lsnText(long lsn) {
        return Long.toHexString(lsn >>> 32) + "/" + Long.toHexString(lsn & 0xffffffffL);
    }

    /**
     * 数据记录的 op 字段(value Struct 顶层);非 Struct 或字段缺失返回 "?"(断言随即失败,
     * 失败信息里可见形态)。
     *
     * @param record 数据记录
     * @return op 字符串(c/u/d/t/r 之一,或 "?")
     */
    private static String opOf(SourceRecord record) {
        if (record.value() instanceof Struct struct && struct.get("op") != null) {
            return String.valueOf(struct.get("op"));
        }
        return "?";
    }

    /**
     * 数据记录的 id 列值(after 块第一个字段——表定义首列)。
     *
     * @param record 数据记录
     * @return after 块首字段值(缺失为 null)
     */
    private static Object idOf(SourceRecord record) {
        if (record.value() instanceof Struct struct) {
            Struct after = struct.getStruct("after");
            if (after != null) {
                return after.get("id");
            }
        }
        return null;
    }

    /**
     * 收集型 ChangeConsumer——记录全收(CopyOnWriteArrayList,handleBatch 在引擎轮询线程
     * 串行调用,并发面仅为可见性)并逐条 markProcessed/markBatchFinished 与生产 consumer
     * 同款记账(offset 提交语义不变,断言面才与 Main 等价)。
     */
    private static final class RecordingConsumer implements ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

        /** 全量收数面(awaitRecords 轮询读取,引擎线程写)。 */
        final List<SourceRecord> records = new CopyOnWriteArrayList<>();

        /**
         * 收一记一,批尾收口(记账语义与 LogChangeConsumer 相同,仅渲染换成收集)。
         *
         * @param events   本批事件
         * @param committer offset 记账器
         * @throws InterruptedException 停机中断契约(原样上抛)
         */
        @Override
        public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> events,
                                RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
                throws InterruptedException {
            for (ChangeEvent<SourceRecord, SourceRecord> event : events) {
                records.add(event.value());
                committer.markProcessed(event);
            }
            committer.markBatchFinished();
        }

        /**
         * 轮询等待累计记录数达标(100ms 间隔;超时 AssertionError 带当前进度)。
         *
         * @param expected      期望总数(数据 + 事务元数据)
         * @param timeoutMillis 超时毫秒
         * @throws InterruptedException sleep 被中断:恢复中断位上抛
         */
        void awaitRecords(int expected, long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
            while (records.size() < expected) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("等待 " + expected + " 条记录超时(" + timeoutMillis
                            + "ms),当前 " + records.size() + " 条: " + records);
                }
                Thread.sleep(100);
            }
        }

        /**
         * 数据记录(非事务 topic)——op 断言面。
         *
         * @return 数据记录列表(到达序)
         */
        List<SourceRecord> dataRecords() {
            return records.stream().filter(r -> !r.topic().equals(TX_TOPIC)).toList();
        }

        /**
         * 事务元数据记录的 status 序列(BEGIN/END 断言面,到达序)。
         *
         * @return status 字符串列表;非事务记录被过滤,事务记录缺 status 落 "null"
         */
        List<String> txStatuses() {
            return records.stream()
                    .filter(r -> r.topic().equals(TX_TOPIC))
                    .map(r -> r.value() instanceof Struct s ? String.valueOf(s.get("status")) : "null")
                    .toList();
        }

        /**
         * 已收记录自身 lsn 的最大值转 "X/Y" 文本——连接器事务边界 offset 双写(记录的
         * lsn = 所属事务 endLsn),最大值即最新已输出事务的 endLsn,作 standby flush
         * 覆盖判定的基线(反馈确认值按输出前沿封顶恰好推进到它)。
         * 边界:无带数值 lsn 的记录抛 IllegalStateException(调用点必在 awaitRecords
         * 之后,记录必带 lsn;非数值形态属契约异常,fail-fast 不猜测)。
         *
         * @return 最新已输出事务 endLsn 的 "X/Y" 文本
         */
        String maxLsnText() {
            return lsnText(records.stream()
                    .map(r -> r.sourceOffset() == null ? null : r.sourceOffset().get("lsn"))
                    .filter(Number.class::isInstance)
                    .mapToLong(v -> ((Number) v).longValue())
                    .max()
                    .orElseThrow(() -> new IllegalStateException("无带数值 lsn 的记录(调用点应在 awaitRecords 之后)")));
        }
    }
}
