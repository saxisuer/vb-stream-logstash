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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS3 aborted 子事务剔除端到端验收:流式大事务内的 SAVEPOINT 子事务回滚——被回滚行
 * <b>不进 Kafka</b>,存活行完整到达(引擎 it 包 {@code DecoupledPipelineTest} 场景① abort
 * 部分的连接器翻译,单连接形态)。机制链:PG 在 ROLLBACK TO SAVEPOINT 时对已流式下发的
 * 被弃子事务发 StreamAbort('A', top-xid + sub-xid,parallel 档附 abortLsn/timestamp)→
 * 组装器把 subxid 记入桶的 abortedSubxids → 回放期 {@code BucketReplayer} 命中
 * abortedSubxids 的单元直接跳过(不解码不交付)→ topic 只见存活行。
 *
 * <p><b>场景构造</b>:单连接单事务——6 行×16KB 不可压缩载荷(行间 sleep 250ms)→
 * {@code SAVEPOINT sp1} → 3 行(同样 16KB 不可压缩,行间 sleep)→
 * {@code ROLLBACK TO SAVEPOINT sp1} → 尾行(id=999)→ COMMIT。存活 6+1=7 行,被回滚
 * 3 行(id=101..103)。
 *
 * <p><b>流式构造论证(为何 'A' 必然到达,场景非空转)</b>:驱逐阈值是全局
 * {@code rb->size} > {@code logical_decoding_work_mem=64kB}(容器 command 定死),按变更
 * 元组 TOAST 压缩后字节数记账;16KB 十六进制载荷 pglz 压不动(实测存满 16384),总量
 * 6+3+1 行 ×16KB=160KB 必然多次越过阈值;行间跨秒 sleep 保证驱逐发生在事务进行中
 * (walsender 追平后的批量写入会被整体延迟到提交后回放,流式路径不触发)。关键的第二
 * 段论证(引擎 DecoupledPipelineTest 场景① 实测先例:<b>驱逐后水位清零</b>——已流式
 * 下发的变更从内存记账扣除,子事务写入必须自行重新越阈值才被流式下发,否则 ROLLBACK TO
 * 被 PG 静默丢弃、'A' 永不下发):主循环 4 行×16KB 恰好压线 64kB 触发首次驱逐(16384×4
 * =65536 加每变更记账开销必过线),残留 2 行(32KB)仍在账上;子事务 3 行累计 48KB
 * 单独不足,但叠加残留后第 2 行(32+32)即再越 64kB——即使首次驱逐边界后移到第 5 行
 * (残留仅 16KB),第 3 行(16+48)也必过线(残留只可能是 16 或 32KB:第 5、6 行合计
 * 32KB 撑不起一次独立驱逐,不存在"第 6 行才驱逐、残留 0"的边界)。故子事务变更确已
 * 流式下发,ROLLBACK TO 时 PG 必发 StreamAbort(top, sub)——连接器侧对 'A' 无独立
 * 可观测面,以本构造论证 + 断言面为准(brief 认可口径;另经过滤失效变异运行证实被回滚
 * 行确会漏到记录面,即流式与过滤链路均被本场景真实穿过)。
 *
 * <p>断言面:恰 9 条记录(7 数据 + BEGIN + END);数据 topic 恰 7 条 INSERT、id 集合
 * 恰为 {1..6, 999}(被回滚 101..103 在任何数据记录中不出现——集合相等即排除);存活行
 * payload 逐一全文相等;事务 topic BEGIN/END 各一、END 的 event_count=7、
 * data_collections 恰本表一项且分表计数=7(数据计数不含被回滚行)。
 *
 * <p>夹具约定:表(id int PK + payload text)与 publication 预建(DDL 先于建槽,不产生
 * 解码输出);独立槽 {@code ms3_abort_filter} 前后清删;管道目录 @TempDir 绝对路径。
 * 需要本机 Docker。
 */
class StreamAbortFilterIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms3_abort_filter";

    /** 数据表名(publication 单表)。 */
    private static final String TABLE = "t_abort_filter";

    /** 数据记录 topic(DefaultTopicNamingStrategy:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 主循环行数:4 行×16KB 恰压 64kB 阈值触发首次驱逐,残留 2 行作子事务越线的记账底座。 */
    private static final int MAIN_ROWS = 6;

    /** 子事务行数:3×16KB=48KB,叠加主循环残留(16~32KB)必再越 64kB——被回滚行确已流式下发。 */
    private static final int SUB_ROWS = 3;

    /** 被回滚子事务行的起始 id(101..103,断言面以集合相等排除)。 */
    private static final int ROLLED_BACK_ID_FROM = 101;

    /** 尾行 id(ROLLBACK TO 之后插入,证明子事务回滚不截断后续变更)。 */
    private static final int TAIL_ID = 999;

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
     * 夹具:建表与 publication,并清空历史数据。DDL/TRUNCATE 先于建槽执行——不产生
     * 解码输出,不污染记录计数;publication 预建是 start 的边界(无自动建,缺失即建流报错)。
     */
    private void createFixture() throws SQLException {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_abort",
                "CREATE PUBLICATION pub_abort FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /**
     * 场景主体:流式大事务的子事务回滚剔除。关键步骤:start → 单连接单事务(6 行不可压缩
     * → SAVEPOINT → 3 行 → ROLLBACK TO → 尾行 → COMMIT,行间 sleep 见类 javadoc 构造
     * 论证)→ consume(9)→ 断言:恰 9 条记录、数据恰 7 条 INSERT 且 id 集合恰为存活集
     * (被回滚行零出现)、payload 全文相等、事务块 BEGIN/END 各一且 END 计数面(总量与
     * 分表)均为 7。
     * 边界:凑不齐 9 条即 fail(消费超时内事务未达属失败而非跳过);被回滚行若漏出,
     * 总量与 id 集合两道断言先后判红。
     */
    @Test
    void rolledBackSubtransactionRowsAreFilteredWhileSurvivingRowsReachKafka() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_abort", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        Map<Integer, String> expected = insertStreamedTxWithRolledBackSubxact();

        List<SourceRecord> all = consumeRecordsUnchecked(2 + MAIN_ROWS + 1);
        assertEquals(2 + MAIN_ROWS + 1, all.size(),
                "BEGIN + 7 条 INSERT(6 存活 + 尾行) + END 共 9 条记录应到达: " + describe(all));

        Set<Integer> seenIds = new java.util.HashSet<>();
        for (SourceRecord r : recordsForTopic(all, TOPIC)) {
            assertEquals(TOPIC, r.topic(), "数据记录 topic 应为数据表 topic");
            Struct value = (Struct) r.value();
            assertEquals("c", value.getString("op"), "应全为 INSERT(op=c)");
            Struct after = value.getStruct("after");
            assertNotNull(after, "INSERT 记录应有 after 结构");
            Integer id = after.getInt32("id");
            assertTrue(expected.containsKey(id), "未知 id 到达(被回滚行混入?): " + id);
            assertEquals(expected.get(id), after.getString("payload"),
                    "payload 应全文相等(id=" + id + ")");
            assertTrue(seenIds.add(id), "id 重复到达: " + id);
        }
        assertEquals(expected.keySet(), seenIds,
                "数据记录应恰覆盖存活 id 集(6 主循环行 + 尾行,被回滚 101..103 零出现): " + describe(all));

        List<SourceRecord> tx = recordsForTopic(all, TX_TOPIC);
        assertEquals(2, tx.size(), "事务元数据应恰 BEGIN+END 两条");
        assertTransactionBlock(tx.get(0), tx.get(1), MAIN_ROWS + 1L);
    }

    /**
     * 执行场景事务并返回存活行的期望载荷:单连接显式事务内——主循环 {@link #MAIN_ROWS}
     * 行不可压缩载荷(行间 sleep 给服务端流式驱逐窗口)→ SAVEPOINT sp1 → 子事务
     * {@link #SUB_ROWS} 行(同样不可压缩,同样 sleep)→ ROLLBACK TO SAVEPOINT sp1(触发
     * PG 下发 StreamAbort)→ 尾行(小载荷,ROLLBACK TO 后插入的存活行)→ commit。
     * 被回滚行的载荷不进期望表(它们不得出现在任何记录中,无需期望值)。
     *
     * @return id → 实际插入的载荷文本(存活行:1..6 与 999)
     * @throws SQLException           插入失败原样上抛
     * @throws InterruptedException sleep 被中断:恢复中断位上抛(测试放弃)
     */
    private Map<Integer, String> insertStreamedTxWithRolledBackSubxact() throws SQLException, InterruptedException {
        Map<Integer, String> expected = new LinkedHashMap<>();
        Random rnd = new Random();
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, ?)");
                 Statement st = c.createStatement()) {
                for (int i = 1; i <= MAIN_ROWS; i++) {
                    insertRow(ps, expected, i, StreamPgTestEnv.incompressiblePayload(rnd));
                    Thread.sleep(250);
                }
                st.execute("SAVEPOINT sp1");
                for (int i = 0; i < SUB_ROWS; i++) {
                    insertRow(ps, null, ROLLED_BACK_ID_FROM + i, StreamPgTestEnv.incompressiblePayload(rnd));
                    Thread.sleep(250);
                }
                st.execute("ROLLBACK TO SAVEPOINT sp1");
                insertRow(ps, expected, TAIL_ID, "tail-after-rollback");
            }
            finally {
                c.commit();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        return expected;
    }

    /**
     * 经预编译语句插一行并按需登记期望载荷:期望表为 null 时只插入不登记(被回滚行——
     * 它们不得出现在任何记录中,期望表只收存活行)。
     *
     * @param ps       预编译 INSERT(列序 id, payload)
     * @param expected 存活行期望登记表(可为 null——被回滚行不登记)
     * @param id       行 id
     * @param payload  行载荷
     * @throws SQLException 插入失败原样上抛
     */
    private static void insertRow(PreparedStatement ps, Map<Integer, String> expected, int id, String payload)
            throws SQLException {
        ps.setInt(1, id);
        ps.setString(2, payload);
        ps.executeUpdate();
        if (expected != null) {
            expected.put(id, payload);
        }
    }

    /**
     * 事务块 BEGIN/END 的语义断言(与 {@code EndToEndStreamedTxIT} 同款的直断替身,基座
     * 助手在 PG 事务记录上必红——id 带 ":lsn" 后缀的语义差异见彼处 javadoc):BEGIN 的
     * status/event_count(空)、END 的 status/id 与 BEGIN 一致/event_count=存活数据记录数
     * (aborted 剔除后的实付数,被回滚行不计)/data_collections 恰本表一项且分表计数同。
     *
     * @param beginRecord 事务块首记录
     * @param endRecord   事务块尾记录
     * @param eventCount  期望的事件计数(aborted 剔除后的存活数据记录数)
     */
    private static void assertTransactionBlock(SourceRecord beginRecord, SourceRecord endRecord, long eventCount) {
        Struct begin = (Struct) beginRecord.value();
        assertEquals("BEGIN", begin.getString("status"), "首条事务块应为 BEGIN");
        assertNull(begin.getInt64("event_count"), "BEGIN 的 event_count 应为空(计数在 END 给出)");
        Struct end = (Struct) endRecord.value();
        assertEquals("END", end.getString("status"), "末条事务块应为 END");
        assertEquals(begin.getString("id"), end.getString("id"), "END 与 BEGIN 的事务 id 应一致");
        assertEquals(eventCount, end.getInt64("event_count"),
                "END 的事件计数应为 aborted 剔除后的存活数(被回滚行不计)");
        List<Struct> collections = end.getArray("data_collections");
        assertEquals(1, collections.size(), "单表事务的 data_collections 应恰一项");
        assertEquals("public." + TABLE, collections.get(0).getString("data_collection"),
                "data_collections 应记数据表 id(schema.table)");
        assertEquals(eventCount, collections.get(0).getInt64("event_count"),
                "分表计数应为 aborted 剔除后的存活数");
    }
}
