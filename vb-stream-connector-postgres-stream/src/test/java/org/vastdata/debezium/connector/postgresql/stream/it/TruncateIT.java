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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS3 Truncate 发射端到端验收:一条 {@code TRUNCATE t_a, t_b} 语句经 pgoutput 'T' 消息
 * → 组装 → 回放 → dispatcher,按 {@code skipped.operations} 门控(vanilla 语义:默认
 * "t" = 跳过,连接器<b>默认不发</b>;{@code none} 才发射)逐表产出 op="t" 记录——
 * <b>每张受影响表一条</b>,key=null、无 before/after、普通 data topic(非事务块 topic)。
 * 两场景:①none 配置下两表各收一条 op="t"(信封形态逐项断言);②默认配置下零发射
 * (以截断后的哨兵事务证明管道存活而 truncate 零记录)。
 *
 * <p>夹具约定:两张表(id int PK + v text)与双表 publication 预建;独立槽
 * {@code ms3_truncate} 前后清删;管道目录 @TempDir 绝对路径。TRUNCATE 是小事务,走
 * Begin..Commit 的 NORMAL 路径(非流式),provide.transaction.metadata=true 下每事务
 * 另有 BEGIN/END 两条事务块记录。需要本机 Docker。
 */
class TruncateIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms3_truncate";

    /** 受影响表 A。 */
    private static final String TABLE_A = "t_trunc_a";

    /** 受影响表 B(多表 TRUNCATE 的第二表)。 */
    private static final String TABLE_B = "t_trunc_b";

    /** 表 A 的数据记录 topic(DefaultTopicNamingStrategy)。 */
    private static final String TOPIC_A = "ms2it.public." + TABLE_A;

    /** 表 B 的数据记录 topic。 */
    private static final String TOPIC_B = "ms2it.public." + TABLE_B;

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
     * 夹具:建两张表与双表 publication,并清空历史数据。DDL 先于建槽执行——DDL 不产生
     * 解码输出,不污染记录计数;publication 必须覆盖两表(pgoutput 只为 publication 内
     * 的表发 'T' 消息)。
     */
    private void createFixture() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE_A + "(id int PRIMARY KEY, v text)",
                "CREATE TABLE IF NOT EXISTS " + TABLE_B + "(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_trunc",
                "CREATE PUBLICATION pub_trunc FOR TABLE " + TABLE_A + ", TABLE " + TABLE_B,
                "TRUNCATE " + TABLE_A + ", " + TABLE_B);
    }

    /**
     * 夹具动作:单事务向两表各插一行(explicit 事务——execSql 是自动提交,两条 INSERT
     * 会拆成两个事务,BEGIN+2 数据+END 的单哨兵事务需显式包拢)。哨兵确保两表的 'R'
     * 元数据先于截断到达且管道存活可观测。
     */
    private void insertSentinelRowsIntoBothTables() throws Exception {
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (java.sql.Statement st = c.createStatement()) {
                st.execute("INSERT INTO " + TABLE_A + " VALUES (1, 'a')");
                st.execute("INSERT INTO " + TABLE_B + " VALUES (1, 'b')");
            }
            finally {
                c.commit();
            }
        }
    }

    /**
     * 场景①(skipped.operations=none):两表 TRUNCATE 逐表发射。关键步骤:start(none
     * 配置)→ 单事务向两表各插一行(BEGIN+2 数据+END 共 4 条,哨兵确保 'R' 元数据先于
     * 截断到达且管道存活)→ 单语句 {@code TRUNCATE t_a, t_b} → consume(4)(BEGIN +
     * 2 条 truncate 记录 + END)→ 断言:恰 2 条数据记录、topic 各归各表、op="t"、
     * key=null、before/after 均无(vanilla truncate 信封无行镜像)。
     * 边界:凑不齐 4 条即 fail(消费超时内截断事务未达属失败而非跳过)。
     */
    @Test
    void truncateEmitsPerTableRecordWhenNotSkipped() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, "pub_trunc", pipeDir).with("skipped.operations", "none").build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        insertSentinelRowsIntoBothTables();
        assertEquals(4, consumeRecordsUnchecked(4).size(), "哨兵事务 BEGIN+2 数据+END 应到达");

        StreamPgTestEnv.execSql("TRUNCATE " + TABLE_A + ", " + TABLE_B);
        List<SourceRecord> all = consumeRecordsUnchecked(4);
        List<SourceRecord> truncA = recordsForTopic(all, TOPIC_A);
        List<SourceRecord> truncB = recordsForTopic(all, TOPIC_B);
        assertEquals(4, all.size(), "截断事务 BEGIN + 2 条 truncate 记录 + END 共 4 条: " + describe(all));
        assertEquals(1, truncA.size(), "表 A 恰一条 truncate 记录(每表一条)");
        assertEquals(1, truncB.size(), "表 B 恰一条 truncate 记录(每表一条)");
        for (SourceRecord r : List.of(truncA.get(0), truncB.get(0))) {
            Struct value = (Struct) r.value();
            assertEquals("t", value.getString("op"), "TRUNCATE 记录 op=t");
            assertNull(r.key(), "truncate 记录 key=null(截断无行键)");
            assertNull(value.getStruct("before"), "truncate 信封无 before");
            assertNull(value.getStruct("after"), "truncate 信封无 after");
        }
    }

    /**
     * 场景②(默认配置):skipped.operations 默认 "t"(CommonConnectorConfig 继承,
     * vanilla 同默认)→ TRUNCATE 零数据记录发射。关键步骤:start(无覆盖)→ 哨兵事务
     * 两表各插一行(4 条)→ 单语句双表 TRUNCATE → 表 A 哨兵 INSERT → consume(5)
     * → 断言:数据记录两轮合计恰 3 条(2+1)、全部 op="c"、零 op="t";截断事务本身
     * 在事务 topic 留一对<b>空</b> BEGIN/END(event_count=0)——vanilla 同款:门控只
     * 吞 'T' 数据消息,BEGIN/COMMIT 消息照常驱动事务块,零数据事件的空块仍发射
     * (TransactionMonitor.transactionStartedEvent 无数据事件守卫,3.6.1 sources 实证)。
     * 边界:仅凭"等若干秒无记录"是弱断言(可能管道死了)——以截断后哨兵事务完整到达
     * 为存活证据,截断事务被门控吞掉数据面才算语义成立。
     */
    @Test
    void truncateEmitsNothingUnderDefaultSkippedOperations() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_trunc", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        insertSentinelRowsIntoBothTables();
        List<SourceRecord> sentinel = consumeRecordsUnchecked(4);
        assertEquals(4, sentinel.size(), "首轮哨兵事务 BEGIN+2 数据+END 应到达");

        StreamPgTestEnv.execSql("TRUNCATE " + TABLE_A + ", " + TABLE_B);
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE_A + " VALUES (2, 'alive')");
        // 截断事务的空事务块(BEGIN+END)+ 哨兵事务(BEGIN+1 数据+END)共 5 条
        List<SourceRecord> after = consumeRecordsUnchecked(5);
        assertEquals(5, after.size(), "截断空块 2 条 + 哨兵事务 3 条应到达: " + describe(after));
        List<SourceRecord> emptyTx = recordsForTopic(after, "ms2it" + TX_TOPIC_SUFFIX);
        assertEquals(4, emptyTx.size(), "两事务的 BEGIN/END 各一对(截断空块 + 哨兵)");
        assertEquals("END", ((Struct) emptyTx.get(1).value()).getString("status"), "第二条事务块为 END(空块收尾)");
        assertEquals(0L, ((Struct) emptyTx.get(1).value()).getInt64("event_count"),
                "被门控截断的事务 END 计数为 0(零数据事件)");

        List<SourceRecord> data = new java.util.ArrayList<>();
        data.addAll(recordsForTopic(sentinel, TOPIC_A));
        data.addAll(recordsForTopic(sentinel, TOPIC_B));
        data.addAll(recordsForTopic(after, TOPIC_A));
        data.addAll(recordsForTopic(after, TOPIC_B));
        assertEquals(3, data.size(), "两轮哨兵合计恰 3 条数据记录(截断零贡献)");
        Set<String> ops = new HashSet<>();
        for (SourceRecord r : data) {
            ops.add(((Struct) r.value()).getString("op"));
        }
        assertEquals(Set.of("c"), ops, "默认配置下零 op=t 记录(TRUNCATE 被 skipped.operations 门控跳过)");
    }
}
