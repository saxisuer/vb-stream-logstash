package org.vastdata.debezium.connector.postgresql.stream.it;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * values.as.string 全串输出模式端到端验收:开关开启时 <b>值与 schema 双轨改形</b>——
 * after/key 的所有 Connect 字段恒 STRING、值为 pgoutput 文本原文(int/numeric/bool/
 * timestamp/timestamptz/text[] 全形态零类型化);数组列在类型化路径是 fail-fast 已知
 * 限制(vanilla PgArray 需活连接),全串模式原文照发<b>限制消解</b>。对照组:开关
 * 缺省(false)时 id 仍是 Integer、num 仍是 BigDecimal(vanilla 类型化行为不受影响,
 * 开关真正在门控)。两场景各一条 INSERT(BEGIN+1 数据+END 共 3 条记录)。
 *
 * <p>夹具约定:单表(id int PK + num numeric(12,4) + flag bool + ts timestamp +
 * tstz timestamptz + tags text[])与单表 publication 预建;独立槽 {@code ms_allstr}
 * 前后清删;管道目录 @TempDir 绝对路径。需要本机 Docker。
 */
class StringValuesIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms_allstr";

    /** 受影响表。 */
    private static final String TABLE = "t_allstr";

    /** 数据记录 topic(DefaultTopicNamingStrategy)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir(cleanup = CleanupMode.NEVER)
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
     * 夹具:建表(六列覆盖整型/定点小数/布尔/无时区时间/带时区时间/数组)与单表
     * publication,并清空历史数据。DDL 先于建槽执行——DDL 不产生解码输出,不污染
     * 记录计数;publication 覆盖该表(pgoutput 只为 publication 内的表发数据消息)。
     */
    private void createFixture() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "("
                        + "id int PRIMARY KEY, "
                        + "num numeric(12,4), "
                        + "flag boolean, "
                        + "ts timestamp, "
                        + "tstz timestamptz, "
                        + "tags text[])",
                "DROP PUBLICATION IF EXISTS pub_allstr",
                "CREATE PUBLICATION pub_allstr FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /**
     * 夹具动作:插一行已知值(explicit 事务包拢成单哨兵事务,BEGIN+1 数据+END)。
     * ts 无时区列的文本输出即存储原文(精确断言);tstz 的文本输出随 walsender 会话
     * 时区漂移,只断言类型不断言值。withArray=false 时 tags 置 NULL——类型化路径
     * (对照组)对非空数组列是 fail-fast 已知限制,对照组只验类型化不触雷。
     *
     * @param id       行主键
     * @param withArray 是否携带数组值(tags 列)
     * @throws Exception SQL 失败原样上抛
     */
    private void insertRow(int id, boolean withArray) throws Exception {
        try (java.sql.Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (java.sql.Statement st = c.createStatement()) {
                String tags = withArray ? "ARRAY['a','b']" : "NULL";
                st.execute("INSERT INTO " + TABLE + " VALUES ("
                        + id + ", 3.14, true, '2026-09-09 12:34:56.789012', "
                        + "'2026-09-09 12:34:56.789012+00', " + tags + ")");
            }
            finally {
                c.commit();
            }
        }
    }

    /**
     * 从数据记录取 after 结构(信封解包一步:record value 的 "after" 字段)。
     *
     * @param record 数据记录(op=c)
     * @return after 的 Struct(非 null)
     */
    private static Struct afterOf(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get("after");
    }

    /**
     * 场景①(开关开启):所有列 STRING schema + 文本原值。关键步骤:start(values.as.string
     * =true)→ 插行 → consume(3) → 断言:数据记录 after 的全部 6 个字段 schema.type
     * 均为 STRING、值逐列等值——id "1"、num "3.1400"(numeric(12,4) 按刻度补零)、
     * flag "t"、ts 即插入原文、tstz 是 String(值随时区漂移不断言)、tags "{a,b}"
     * (数组原文——类型化路径的 fail-fast 限制在此消解);key 结构的 id 字段同为
     * STRING 且值 "1"。
     */
    @Test
    void allValuesAndSchemasAreStringWhenEnabled() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, "pub_allstr", pipeDir).with("values.as.string", true).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        insertRow(1, true);

        List<SourceRecord> records = consumeRecordsUnchecked(3);
        assertEquals(3, records.size(), "BEGIN+1 数据+END 应到达");
        SourceRecord data = recordsForTopic(records, TOPIC).get(0);
        Struct after = afterOf(data);

        for (String field : new String[] { "id", "num", "flag", "ts", "tstz", "tags" }) {
            assertEquals(Schema.Type.STRING, after.schema().field(field).schema().type(),
                    "全串模式下字段 " + field + " 的 schema 必须是 STRING");
        }
        assertEquals("1", after.get("id"), "int 原文");
        assertEquals("3.1400", after.get("num"), "numeric(12,4) 按刻度补零的文本原文");
        assertEquals("t", after.get("flag"), "布尔 PG 文本形态");
        assertEquals("2026-09-09 12:34:56.789012", after.get("ts"), "timestamp 无时区列即存储原文");
        assertTrue(after.get("tstz") instanceof String, "timestamptz 值必须是 String(值随时区漂移不断言)");
        assertEquals("{a,b}", after.get("tags"), "数组列文本原文(类型化路径的 fail-fast 限制消解)");

        Struct key = (Struct) data.key();
        assertEquals(Schema.Type.STRING, key.schema().field("id").schema().type(), "key 列同为 STRING");
        assertEquals("1", key.get("id"), "key 值同文本原文");
    }

    /**
     * 场景②(开关缺省,对照组):vanilla 类型化行为不受影响——id 是 Integer、num 是
     * BigDecimal、schema 非 STRING。开关只在显式开启时改形,存量 IT 的类型化断言面
     * 以此默认为前提。tags 置 NULL:类型化路径对非空数组列是 fail-fast 已知限制,
     * 对照组只验类型化基准不触雷。
     */
    @Test
    void typedBehaviorUnchangedWhenSwitchAbsent() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_allstr", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);
        insertRow(2, false);

        List<SourceRecord> records = consumeRecordsUnchecked(3);
        assertEquals(3, records.size(), "BEGIN+1 数据+END 应到达");
        Struct after = afterOf(recordsForTopic(records, TOPIC).get(0));
        assertEquals(Integer.class, after.get("id").getClass(), "缺省时 id 仍是 Integer");
        assertEquals(BigDecimal.class, after.get("num").getClass(), "缺省时 num 仍是 BigDecimal");
        assertEquals(Schema.Type.INT32, after.schema().field("id").schema().type(), "缺省时 id schema 仍 INT32");
    }
}
