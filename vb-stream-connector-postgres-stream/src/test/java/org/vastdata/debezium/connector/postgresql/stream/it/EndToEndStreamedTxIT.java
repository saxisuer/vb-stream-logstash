package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS2 验收主角:流式大事务经 embedded engine 端到端进 Kafka 记录(spec §10 MS2 验收行
 * "流式大事务(parallel 模式)记录完整进 Kafka,事务元数据 BEGIN/END 齐,per-record
 * offset=事务 endLsn")。场景四段:①parallel 档下 6×16KB 不可压缩载荷单事务(行间
 * sleep)→ 6 条 INSERT 记录值完整 + 事务 topic BEGIN/END + source.lsn 组内一致且
 * =事务边界(endLsn,offset 三面一致);②普通小事务 INSERT + UPDATE(非流式路径的
 * 同管线回归);③重启续传冒烟(停引擎→停机窗口写入→重启→已提交事务最终全量到达,
 * at-least-once 允许重复);④亚秒反馈冒烟(slot.feedback.interval.ms=500,Task 1 账本)。
 *
 * <p><b>流式路径论证(为何 6×16KB 必然 STREAMED)</b>:pgoutput 的流式下发由 PG 服务端
 * reorder buffer 驱动——进行中事务的变更元组按 <b>TOAST 压缩后字节数</b>累入全局
 * {@code rb->size},越过 {@code logical_decoding_work_mem=64kB}(容器 command 定死)即
 * 驱逐出队,以 StreamStart/Stream* 块对进行中事务边收边发(引擎 it 包实测先例:
 * TransactionAssemblyTest 场景 4/StreamedTransactionTest)。16KB 十六进制载荷每行
 * pglzip 压不动(实测 pg_column_size 存满),5 行即越阈值;行间 sleep 1200ms 保证驱逐
 * 窗口在事务提交前到来(不 sleep 的批量写入会被 PG 延迟到提交后一次性回放)。
 * 连接器侧对 STREAMED/NORMAL 无独立可观测面(TransactionKind 不进记录),故以构造
 * 论证 + 断言记录数/值完整性/offset 语义为主(brief 备注认可该口径)。
 *
 * <p>夹具约定:表(id int PK + payload text,无数组列——数组/未知类型在值映射层
 * fail-fast 属 MS3 面)与 publication 由 IT 预建(Task 7 的 start 无守门/自动建);
 * 独立槽 {@code e2e_streamed_tx} 前后清删;管道目录 @TempDir 绝对路径。需要本机 Docker。
 */
class EndToEndStreamedTxIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "e2e_streamed_tx";

    /** 数据表名(publication 单表)。 */
    private static final String TABLE = "t_e2e";

    /** 数据记录 topic(DefaultTopicNamingStrategy:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir
    Path pipeDir;

    /**
     * 每用例前清残留槽:上次异常退出留下的同名槽会从旧 confirmed_flush_lsn 续传,
     * 静默吞掉建流前的写入使记录断言失真。幂等。
     */
    @BeforeEach
    void cleanResidualSlot() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /** 每用例后清理:先停引擎再删槽(次序见基类 {@link #stopEngineAndDropSlot})。 */
    @AfterEach
    void dropSlot() {
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 夹具:建表(无数组列)与 publication,并清空历史数据。DDL 先于建槽执行——
     * DDL 不产生解码输出,不污染记录计数;publication 预建是 Task 7 start 的边界
     * (无自动建,缺失即建流报错)。
     */
    private void createFixture() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_e2e",
                "CREATE PUBLICATION pub_e2e FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /**
     * 场景①:parallel 档流式大事务(6×16KB,行间 1.2s)完整进 Kafka 记录。
     * 关键步骤:start 引擎 → 单连接事务逐行插 6 行并 COMMIT → consumeRecordsByTopic(8)
     * (6 数据 + BEGIN + END)→ 断言:数据 topic 恰 6 条 INSERT、after 结构 id/payload
     * 与插入值逐一相等(payload 16KB 全文,TOAST 不可变值哨兵不出现)、source.lsn 六条
     * 同值、sourceOffset 的 lsn_proc/lsn_commit 与 source.lsn 相等(事务边界 offset 的
     * 三面一致)、事务 topic BEGIN/END 事件计数齐(harness 助手断言 END 的
     * data_collections 恰本表 6 条)。
     * 边界:consumeRecordsByTopic(8) 在消费超时内凑不齐即 fail(基座默认超时,
     * 大事务回放慢属失败而非跳过)。
     */
    @Test
    void streamedLargeTxReachesKafkaRecordsWithTransactionMetadata() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_e2e", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        Map<Integer, String> expected = StreamPgTestEnv.insertIncompressibleRows(TABLE, 1, 6, 1200);

        List<SourceRecord> all = consumeRecordsUnchecked(8);
        List<SourceRecord> data = recordsForTopic(all, TOPIC);
        assertEquals(8, all.size(), "6 数据 + BEGIN + END 共 8 条记录应到达: " + describe(all));
        assertEquals(6, data.size(), "流式大事务应产出恰 6 条 INSERT 记录: " + describe(all));

        Long txBoundaryLsn = null;
        Set<Integer> seenIds = new HashSet<>();
        for (SourceRecord r : data) {
            assertEquals(TOPIC, r.topic(), "记录 topic 应为数据表 topic");
            Struct value = (Struct) r.value();
            assertEquals("c", value.getString("op"), "应全为 INSERT(op=c)");
            Struct after = value.getStruct("after");
            assertNotNull(after, "INSERT 记录应有 after 结构");
            Integer id = after.getInt32("id");
            assertTrue(expected.containsKey(id), "未知 id 到达: " + id);
            assertEquals(expected.get(id), after.getString("payload"),
                    "16KB payload 应全文相等(id=" + id + ",TOAST 值不得截断/哨兵化)");
            assertTrue(seenIds.add(id), "id 重复到达: " + id);

            Long lsn = value.getStruct("source").getInt64("lsn");
            assertNotNull(lsn, "source 块应有 lsn");
            if (txBoundaryLsn == null) {
                txBoundaryLsn = lsn;
            }
            assertEquals(txBoundaryLsn, lsn, "同一事务内 source.lsn 应恒为事务边界 endLsn");
            assertEquals(txBoundaryLsn.longValue(), ((Number) r.sourceOffset().get("lsn_proc")).longValue(),
                    "per-record offset 的 lsn_proc 应=事务边界(source.lsn)");
            assertEquals(txBoundaryLsn.longValue(), ((Number) r.sourceOffset().get("lsn_commit")).longValue(),
                    "per-record offset 的 lsn_commit 应=事务边界(updateCommitPosition 双写)");
        }
        assertEquals(expected.keySet(), seenIds, "六条记录应恰好覆盖全部插入行");

        List<SourceRecord> tx = recordsForTopic(all, TX_TOPIC);
        assertEquals(2, tx.size(), "事务元数据应恰 BEGIN+END 两条");
        assertTransactionBlock(tx.get(0), tx.get(1), 6L);
        // 事务块事件与数据记录共用同一 offsetContext——其 offset 同锚事务边界
        for (SourceRecord r : tx) {
            assertEquals(txBoundaryLsn.longValue(), ((Number) r.sourceOffset().get("lsn_commit")).longValue(),
                    "事务块事件 offset 的 lsn_commit 应与数据记录同锚事务边界");
        }
    }

    /**
     * 事务块 BEGIN/END 的语义断言(基座 assertBeginTransaction/assertEndTransaction 的
     * 直断替身):基座助手额外断言 offset 的 transaction_id == value.id——那是 MySQL 的
     * 事务 id 语义;PG 的 PostgresTransactionMonitor 按设计给记录 id 附加 ":lsn" 后缀
     * (prepareTxKey/Value 统一 adjustTxId),与 offset 里的裸 id 恒差一个后缀,助手在
     * PG 事务记录上必红(vanilla PG IT 不用这对助手断 id)。此处断言真实语义:BEGIN
     * 的 status/event_count(空)、END 的 status/id 与 BEGIN 一致/event_count=行数/
     * data_collections 恰本表 N 条。
     *
     * @param beginRecord  事务块首记录
     * @param endRecord    事务块尾记录
     * @param eventCount   期望的事件计数(数据记录数)
     */
    private static void assertTransactionBlock(SourceRecord beginRecord, SourceRecord endRecord, long eventCount) {
        Struct begin = (Struct) beginRecord.value();
        assertEquals("BEGIN", begin.getString("status"), "首条事务块应为 BEGIN");
        org.junit.jupiter.api.Assertions.assertNull(begin.getInt64("event_count"),
                "BEGIN 的 event_count 应为空(计数在 END 给出)");
        Struct end = (Struct) endRecord.value();
        assertEquals("END", end.getString("status"), "末条事务块应为 END");
        assertEquals(begin.getString("id"), end.getString("id"), "END 与 BEGIN 的事务 id 应一致");
        assertEquals(eventCount, end.getInt64("event_count"), "END 的事件计数应等于数据记录数");
        List<Struct> collections = end.getArray("data_collections");
        org.junit.jupiter.api.Assertions.assertEquals(1, collections.size(),
                "单表事务的 data_collections 应恰一项");
        assertEquals("public." + TABLE, collections.get(0).getString("data_collection"),
                "data_collections 应记数据表 id(schema.table)");
        assertEquals(eventCount, collections.get(0).getInt64("event_count"), "分表计数应等于数据记录数");
    }

    /**
     * 场景②:普通小事务(INSERT + UPDATE 两语句)同管线正确——非流式路径的端到端回归。
     * 关键步骤:start → 单事务 INSERT id=1 + UPDATE id=1 → consume(4)(BEGIN+2 数据+END)
     * → 断言 op 序列 c/u、UPDATE 记录 after 为新值且 before 为旧值(信封完整性)。
     */
    @Test
    void smallTxInsertAndUpdateRoundTrip() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_e2e", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (1, 'before-value')");
                 PreparedStatement upd = c.prepareStatement("UPDATE " + TABLE + " SET payload='after-value' WHERE id=1")) {
                ins.executeUpdate();
                upd.executeUpdate();
            }
            finally {
                c.commit();
            }
        }

        List<SourceRecord> all = consumeRecordsUnchecked(4);
        List<SourceRecord> data = recordsForTopic(all, TOPIC);
        assertEquals(2, data.size());
        Struct insert = (Struct) data.get(0).value();
        assertEquals("c", insert.getString("op"));
        assertEquals("before-value", insert.getStruct("after").getString("payload"));
        Struct update = (Struct) data.get(1).value();
        assertEquals("u", update.getString("op"), "第二条应为 UPDATE");
        // REPLICA IDENTITY DEFAULT 下 UPDATE 不带旧元组('O' 消息缺席),before 为 null
        // 属预期;after 的新值才是载荷断言面(before 面 REPLICA IDENTITY FULL 场景归 MS3+)
        org.junit.jupiter.api.Assertions.assertNull(update.getStruct("before"),
                "DEFAULT replica identity 下 UPDATE 的 before 应为 null");
        assertEquals("after-value", update.getStruct("after").getString("payload"),
                "UPDATE after 应为新值");
        List<SourceRecord> tx = recordsForTopic(all, TX_TOPIC);
        assertEquals(2, tx.size());
        assertTransactionBlock(tx.get(0), tx.get(1), 2L);
    }

    /**
     * 场景③:重启续传冒烟(at-least-once 口径):停引擎→停机窗口写入→重启(offset 文件
     * 单用例内保留——基座只在 @BeforeEach 删)→断言停机窗口前后的已提交事务最终全部
     * 到达。重复(同 id 多条)属 at-least-once 文档化行为,不去重逐条;引擎 A 停机前已
     * 消费的行是否重发取决于服务端 confirmed_flush 的采纳进度,不强制。关键步骤:
     * 引擎 A 消费 3 行确认运行(每事务 BEGIN+数据+END,按数消费 15 条覆盖 3 事务)→
     * stopConnector(槽保留,walsender 随任务停机断开)→ 停机窗口写 2 行 → 引擎 B 同
     * offset 文件重启 → 再写 2 行 → await 轮询非阻塞排空累计,直到两阶段 id 并集
     * 覆盖全部 7 行(总数不确定——重复数未知,不能用按数消费)。
     */
    @Test
    void restartResumesAndDeliversAllCommittedRowsAtLeastOnce() throws Exception {
        createFixture();
        var config = baseConfig(SLOT, "pub_e2e", pipeDir).build();
        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        Map<Integer, String> expected = new HashMap<>();
        expected.putAll(StreamPgTestEnv.insertIncompressibleRows(TABLE, 1, 3, 200));
        Set<Integer> phaseAIds = new HashSet<>();
        for (SourceRecord r : recordsForTopic(consumeRecordsUnchecked(15), TOPIC)) {
            phaseAIds.add(((Struct) r.value()).getStruct("after").getInt32("id"));
        }
        assertEquals(Set.of(1, 2, 3), phaseAIds, "引擎 A 应先消费到首批 3 行(重启基线)");

        stopConnector();
        // 停机窗口写入:槽保留 WAL,重启后从 confirmed_flush(输出前沿封顶)续发
        expected.putAll(StreamPgTestEnv.insertIncompressibleRows(TABLE, 4, 2, 200));

        start(PostgresStreamConnector.class, config);
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        expected.putAll(StreamPgTestEnv.insertIncompressibleRows(TABLE, 6, 2, 200));

        // at-least-once:停机窗口与重启后的行(4..7)必须最终到达;引擎 A 已消费的行
        // (1..3)可能重发也可能不重发(取决于服务端 confirmed_flush 是否已采纳到前沿)——
        // 断言口径:两阶段并集覆盖全部已提交 id,重复允许;消费用非阻塞排空(总数不确定)
        List<SourceRecord> seen = new java.util.ArrayList<>();
        await("重启后全部已提交行到达").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    drainArrivedRecords(seen);
                    Set<Integer> postRestartIds = new HashSet<>();
                    for (SourceRecord r : seen) {
                        if (TOPIC.equals(r.topic())) {
                            Struct value = (Struct) r.value();
                            if (value.getStruct("after") != null) {
                                Integer id = value.getStruct("after").getInt32("id");
                                assertEquals(expected.get(id), value.getStruct("after").getString("payload"),
                                        "重启后记录值应与插入值一致(id=" + id + ")");
                                postRestartIds.add(id);
                            }
                        }
                    }
                    Set<Integer> union = new HashSet<>(phaseAIds);
                    union.addAll(postRestartIds);
                    assertEquals(expected.keySet(), union, "两阶段并集应覆盖全部已提交行(重复允许)");
                });
    }

    /**
     * 场景④(亚秒反馈冒烟,Task 1 账本项):slot.feedback.interval.ms=500 → 整除换算
     * 0 秒 → run 循环每轮 forceUpdateStatus(间隔计时永不满)+ withStatusInterval(0 秒)
     * ——一轮跑通:取写入前基线,写一行,消费完整个事务(BEGIN+数据+END 三条,End
     * 处理完毕前沿才推进)后,断言服务端在 5s 内采纳客户端 flush 位点越过基线
     * (pg_stat_replication.flush_lsn 观测面)。默认 10s 节流下 5s 窗口必不满足,
     * 本断言非恒真。
     */
    @Test
    void subSecondFeedbackSmoke() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, "pub_e2e", pipeDir).with("slot.feedback.interval.ms", 500).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        String baseline = lsnText("SELECT pg_current_wal_insert_lsn()");
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (99, 'feedback')");
        assertEquals(1, recordsForTopic(consumeRecordsUnchecked(3), TOPIC).size(),
                "反馈冒烟的事务应完整到达(BEGIN+数据+END)");

        await("亚秒反馈:服务端数秒内采纳客户端 flush 位点")
                .atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(200))
                .until(() -> StreamPgTestEnv.standbyFlushBeyond(SLOT, baseline));
    }

    /**
     * 查询单值 LSN 的文本形态(基线锚点用,保留 "X/Y" 形态直接喂 SQL 侧 pg_lsn 比较)。
     * 无结果行抛 ISE(锚点查询必然有值,缺失即环境异常)。
     */
    private static String lsnText(String sql) throws Exception {
        try (Connection c = StreamPgTestEnv.newSqlConnection();
             PreparedStatement st = c.prepareStatement(sql);
             ResultSet rs = st.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("LSN 查询无结果: " + sql);
            }
            return rs.getString(1);
        }
    }
}
