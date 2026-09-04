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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * MS3 同事务 DDL 的 asOf 版本渲染端到端验收:流式大事务<b>中段</b>的 {@code ALTER TABLE
 * ADD COLUMN}——前段行按旧表结构(2 列)、后段行按新表结构(3 列)各自渲染,同一条
 * value schema 的列数在 DDL 边界正确分界(引擎 it 包 {@code DecoupledPipelineTest}
 * 场景② 的连接器翻译,单连接流式形态)。
 *
 * <p><b>机制链(断言依据)</b>:事务内 DDL 使 pgoutput 对同 oid 重发新版本 Relation
 * ('R',列集 3 列)→ 组装器 {@code VersionedRelationRegistry} 按 seq 单调追加版本,
 * 交接的桶快照({@code RelationSnapshot})同时含两版 → 回放期 {@code BucketReplayer}
 * 按单元自身 seq 经 {@code RelationSnapshot.require(oid, seq)} 二分取"变更时刻生效"
 * 的版本,{@code RowChange.relation} 即该版本的 wire Relation → listener 侧
 * {@code DispatcherTransactionListener.resolveAndInstall} 经
 * {@code BucketTableResolver.resolve(oid, seq)} 取同版本 Debezium {@code Table} 并做
 * 版本安装(值相等短路,DDL 边界各装一次——{@code applySchemaChangesForTable} 重建
 * TableSchema)→ dispatch。Kafka Connect 的 value schema 是 dispatch 时经 dispatcher
 * 的 schemaFor(<b>当前安装版</b>)生成的:前 3 条 dispatch 时安装的是 2 列版、后 2 条
 * 是 3 列版,故前段 after 结构恰 2 字段。<b>按最新版渲染前段的实现必红</b>:那样的
 * 前段记录 after 是 3 列(c2 为 null 填充)——字段数断言直接抓漏(经 asOf 失效变异
 * 运行证实,见任务报告)。
 *
 * <p><b>场景边界</b>:ALTER 在<b>事务内</b>才走本场景——'R' v2 随流式块携带 xid 前缀
 * 到达,与 v1 同桶共存,asOf 取版才有物可考;事务外 DDL 的 'R'(无前缀、桶外)不属
 * 本场景。另注 'R' enrich 边界:reader 期的 JDBC 元数据补齐(optional/默认值/PK)读
 * 的是 main 连接的 catalog 视图,彼时事务未提交、c2 不在视图内——按名 miss 回落
 * optional=true(vanilla 口径),而列集以 wire 'R' 为真源,3 列版本不受影响(列数
 * 断言同时覆盖此点:若 enrich 意外吞列,后段字段数即错)。
 *
 * <p><b>场景构造</b>:单连接单事务——插 3 行(2 列形态,c1=16KB 不可压缩载荷,行间
 * sleep 250ms)→ {@code ALTER TABLE t ADD COLUMN c2 text} → 插 2 行(3 列形态,c2
 * 有值,同样 16KB 载荷 + sleep)→ COMMIT。
 *
 * <p><b>流式构造论证(为何 'R' v2 必然于流式块内到达,场景非空转)</b>:驱逐阈值是
 * 全局 {@code rb->size} ≥ {@code logical_decoding_work_mem=64kB}(容器 command 定死),
 * 按变更元组 TOAST 压缩后字节数记账,16KB 十六进制载荷 pglz 压不动(实测存满
 * 16384)。DDL 前最大累积 3×16KB=48KB,加记账开销仍在 64kB 之下——<b>首次驱逐不可能
 * 先于 DDL</b>(不存在"前段先单独成块、后段 NORMAL 尾巴"的边界形态);第 4 行使总量
 * ≥64KB+记账开销必触发驱逐,已累积的行 1..3(v1)、'R' v2、行 4(v2)同在该流式块
 * 内到达;行 5(残留 16KB)不再越阈,以提交时的末段流块收尾。行间跨秒 sleep 保证
 * 驱逐发生在事务进行中(walsender 追平后的批量写入会被整体延迟到提交后回放,流式
 * 路径不触发)。
 *
 * <p>断言面:恰 7 条记录(5 数据 + BEGIN + END);数据 topic 恰 5 条 INSERT 且按
 * id=1..5 顺序到达(桶回放按 seq 单调,单事务内记录有序);<b>前 3 条 after 结构
 * 恰 2 字段且无 c2 字段、后 2 条恰 3 字段且 c2 值等于写入值</b>(asOf 版本渲染的
 * 连接器可观测面);全部 c1 载荷全文相等;事务 topic BEGIN/END 各一、END 的
 * event_count=5、data_collections 恰本表一项且分表计数=5。
 *
 * <p>夹具约定:表(id int PK + c1 text)与 publication 预建(DDL 先于建槽,不产生
 * 解码输出);因场景自身 ADD COLUMN,夹具走 DROP/CREATE 自愈(仅 TRUNCATE 清不掉
 * 上次运行留下的 c2 列);独立槽 {@code ms3_ddl_asof} 前后清删;管道目录 @TempDir
 * 绝对路径。需要本机 Docker。
 */
class InTxnDdlAsOfIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms3_ddl_asof";

    /** 数据表名(publication 单表)。 */
    private static final String TABLE = "t_ddl_asof";

    /** 数据记录 topic(DefaultTopicNamingStrategy:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** DDL 前段行数(2 列形态;3×16KB=48KB &lt; 64kB,首驱逐不可能先于 DDL)。 */
    private static final int PRE_ROWS = 3;

    /** 总行数(DDL 后 2 行为 3 列形态;第 4 行起总量 ≥64kB 必触发驱逐)。 */
    private static final int TOTAL_ROWS = 5;

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
     * 夹具:重建表与 publication。关键步骤:DROP 再 CREATE(而非 CREATE IF NOT EXISTS
     * + TRUNCATE)——本场景的事务内 ADD COLUMN 会把 c2 永久留在表上,仅清数据无法恢复
     * 2 列初态,重跑会因列已存在而 ALTER 失败;DROP/CREATE 自愈且 DDL 先于建槽执行,
     * 不产生解码输出。publication 预建是 start 的边界(无自动建,缺失即建流报错)。
     */
    private void createFixture() throws SQLException {
        StreamPgTestEnv.execSql(
                "DROP PUBLICATION IF EXISTS pub_ddl",
                "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + "(id int PRIMARY KEY, c1 text)",
                "CREATE PUBLICATION pub_ddl FOR TABLE " + TABLE);
    }

    /**
     * 场景主体:流式大事务中段 DDL 的 asOf 版本渲染。关键步骤:start → 单连接单事务
     * (3 行 2 列形态 → ALTER ADD COLUMN c2 → 2 行 3 列形态 → COMMIT,行间 sleep 见
     * 类 javadoc 流式论证)→ consume(7)→ 断言:恰 7 条记录;数据恰 5 条 INSERT 按
     * id 顺序到达,<b>前 3 条 after 恰 2 字段且无 c2、后 2 条恰 3 字段且 c2 值正确</b>
     * (asOf 分界——按最新版渲染的实现在此必红);全部 c1 载荷全文相等;事务块
     * BEGIN/END 各一且 END 计数面(总量与分表)均为 5。
     * 边界:凑不齐 7 条即 fail(消费超时内事务未达属失败而非跳过)。
     */
    @Test
    void inTransactionDdlRendersEachSegmentByItsOwnSchemaVersion() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_ddl", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        Map<Integer, String> expectedC1 = new LinkedHashMap<>();
        Map<Integer, String> expectedC2 = new LinkedHashMap<>();
        insertStreamedTxWithInTxnDdl(expectedC1, expectedC2);

        List<SourceRecord> all = consumeRecordsUnchecked(2 + TOTAL_ROWS);
        assertEquals(2 + TOTAL_ROWS, all.size(),
                "BEGIN + 5 条 INSERT + END 共 7 条记录应到达: " + describe(all));

        List<SourceRecord> data = recordsForTopic(all, TOPIC);
        assertEquals(TOTAL_ROWS, data.size(), "数据 topic 应恰 5 条记录: " + describe(all));
        for (int i = 0; i < data.size(); i++) {
            final int idx = i;   // lambda 失败消息捕获用(循环变量非最终变量)
            SourceRecord r = data.get(i);
            assertEquals(TOPIC, r.topic(), "数据记录 topic 应为数据表 topic");
            Struct value = (Struct) r.value();
            assertEquals("c", value.getString("op"), "应全为 INSERT(op=c)");
            Struct after = value.getStruct("after");
            assertNotNull(after, "INSERT 记录应有 after 结构");
            Integer id = after.getInt32("id");
            assertEquals(idx + 1, id, "桶回放按 seq 单调,单事务内数据记录应按 id=1..5 顺序到达");
            assertEquals(expectedC1.get(id), after.getString("c1"),
                    "c1 载荷应全文相等(id=" + id + ")");

            // asOf 断言核心:DDL 前段按 v1(2 列,无 c2 字段)、后段按 v2(3 列,c2 有值)渲染
            boolean preDdl = idx < PRE_ROWS;
            int fieldCount = after.schema().fields().size();
            assertEquals(preDdl ? 2 : 3, fieldCount,
                    "变更 #" + idx + " after 结构字段数(前段旧/后段新): " + describe(all));
            if (preDdl) {
                assertNull(after.schema().field("c2"),
                        "DDL 前段记录的 schema 不应含 c2 字段(按最新版渲染即在此露馅): " + describe(all));
            }
            else {
                assertEquals(expectedC2.get(id), after.getString("c2"),
                        "DDL 后段记录的 c2 值应等于写入值(id=" + id + "): " + describe(all));
            }
        }

        List<SourceRecord> tx = recordsForTopic(all, TX_TOPIC);
        assertEquals(2, tx.size(), "事务元数据应恰 BEGIN+END 两条");
        assertTransactionBlock(tx.get(0), tx.get(1), TOTAL_ROWS);
    }

    /**
     * 执行场景事务并登记期望值:单连接显式事务内——前段 {@link #PRE_ROWS} 行 2 列形态
     * (id, c1=16KB 不可压缩载荷,行间 sleep 给服务端流式驱逐窗口)→
     * {@code ALTER TABLE ADD COLUMN c2 text}(触发 pgoutput 同 oid 重发 3 列版 'R')→
     * 后段 2 行 3 列形态(id, c1=16KB 不可压缩, c2=小载荷,同样 sleep——第 4 行起总量
     * 必越 64kB,'R' v2 于流式块内到达)→ commit。
     *
     * @param expectedC1 id → 实际插入的 c1 载荷文本(全文相等断言的期望源,调用方预建)
     * @param expectedC2 id → 实际插入的 c2 值文本(后段 c2 值断言的期望源,调用方预建)
     * @throws SQLException           插入/DDL 失败原样上抛
     * @throws InterruptedException sleep 被中断:恢复中断位上抛(测试放弃)
     */
    private void insertStreamedTxWithInTxnDdl(Map<Integer, String> expectedC1, Map<Integer, String> expectedC2)
            throws SQLException, InterruptedException {
        Random rnd = new Random();
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement pre = c.prepareStatement("INSERT INTO " + TABLE + " (id, c1) VALUES (?, ?)");
                 PreparedStatement post = c.prepareStatement("INSERT INTO " + TABLE + " (id, c1, c2) VALUES (?, ?, ?)");
                 Statement st = c.createStatement()) {
                for (int i = 1; i <= PRE_ROWS; i++) {
                    String payload = StreamPgTestEnv.incompressiblePayload(rnd);
                    pre.setInt(1, i);
                    pre.setString(2, payload);
                    pre.executeUpdate();
                    expectedC1.put(i, payload);
                    Thread.sleep(250);
                }
                st.execute("ALTER TABLE " + TABLE + " ADD COLUMN c2 text");
                for (int i = PRE_ROWS + 1; i <= TOTAL_ROWS; i++) {
                    String payload = StreamPgTestEnv.incompressiblePayload(rnd);
                    String c2 = "c2-" + i;
                    post.setInt(1, i);
                    post.setString(2, payload);
                    post.setString(3, c2);
                    post.executeUpdate();
                    expectedC1.put(i, payload);
                    expectedC2.put(i, c2);
                    Thread.sleep(250);
                }
            }
            finally {
                c.commit();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * 事务块 BEGIN/END 的语义断言(与 {@code StreamAbortFilterIT} 同款的直断替身):BEGIN
     * 的 status/event_count(空)、END 的 status/id 与 BEGIN 一致/event_count=数据记录数
     * (本场景无 aborted 剔除,计数即 5)/data_collections 恰本表一项且分表计数同。
     *
     * @param beginRecord 事务块首记录
     * @param endRecord   事务块尾记录
     * @param eventCount  期望的事件计数(数据记录数)
     */
    private static void assertTransactionBlock(SourceRecord beginRecord, SourceRecord endRecord, long eventCount) {
        Struct begin = (Struct) beginRecord.value();
        assertEquals("BEGIN", begin.getString("status"), "首条事务块应为 BEGIN");
        assertNull(begin.getInt64("event_count"), "BEGIN 的 event_count 应为空(计数在 END 给出)");
        Struct end = (Struct) endRecord.value();
        assertEquals("END", end.getString("status"), "末条事务块应为 END");
        assertEquals(begin.getString("id"), end.getString("id"), "END 与 BEGIN 的事务 id 应一致");
        assertEquals(eventCount, end.getInt64("event_count"), "END 的事件计数应为数据记录数");
        List<Struct> collections = end.getArray("data_collections");
        assertEquals(1, collections.size(), "单表事务的 data_collections 应恰一项");
        assertEquals("public." + TABLE, collections.get(0).getString("data_collection"),
                "data_collections 应记数据表 id(schema.table)");
        assertEquals(eventCount, collections.get(0).getInt64("event_count"), "分表计数应为数据记录数");
    }
}
