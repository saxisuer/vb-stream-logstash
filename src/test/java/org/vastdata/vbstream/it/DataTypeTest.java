package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 常见数据类型（时间/数字/字符串 + 布尔/uuid/jsonb/bytea）经 pgoutput 文本协议解码的端到端一致性。
 * 期望值不用硬编码，而以 PG 自身的文本输出（JDBC getString，与 pgoutput 用同一套类型输出函数）为
 * oracle，规避时区/格式化假设。binary publication 是里程碑 1 非目标，全部列应走 't' 文本种类。
 */
class DataTypeTest {

    private static final int COLUMN_COUNT = 19;

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_types");
    }

    @Test
    void commonTypesRoundTripAsText() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_types ("
                        // 时间
                        + "c_date date, c_time time, c_timetz timetz, c_ts timestamp,"
                        + " c_tstz timestamptz, c_interval interval,"
                        // 数字
                        + " c_small smallint, c_int integer, c_big bigint,"
                        + " c_num numeric(12,4), c_real real, c_double double precision,"
                        // 字符串
                        + " c_varchar varchar(64), c_text text, c_char char(8),"
                        // 其他常见
                        + " c_bool boolean, c_uuid uuid, c_jsonb jsonb, c_bytea bytea)",
                "DROP PUBLICATION IF EXISTS pub_types",
                "CREATE PUBLICATION pub_types FOR TABLE t_types");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_types", "pub_types"),
                msg -> msg instanceof PgOutputMessage.Commit)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_types VALUES ("
                            + "'2026-08-27', '12:34:56.789012', '12:34:56.789+08', '2026-08-27 10:20:30.123456',"
                            + " '2026-08-27 10:20:30.123456+08', '1 year 2 mons 3 days 04:05:06',"
                            + " 32767, 2147483647, 9223372036854775807,"
                            + " 12345.6789, 1.5, 2.718281828459045,"
                            + " 'hello varchar', 'hello text 世界', 'abc',"
                            + " true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\":1}', '\\xDEADBEEF')");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Insert insert = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert)
                    .map(m -> (PgOutputMessage.Insert) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Insert 消息"));
            assertTrue(insert.streamXid().isEmpty(), "顶层消息不应带 streamXid");
            assertEquals(COLUMN_COUNT, insert.newTuple().columns().size(), "列数应与表定义一致");

            // Relation 元数据（列名）与解码值按位置配对
            PgOutputMessage.Relation relation = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Relation r
                            && "t_types".equals(r.table()))
                    .map(m -> (PgOutputMessage.Relation) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Relation(t_types)"));
            Map<String, String> decodedByName = new HashMap<>();
            for (int i = 0; i < COLUMN_COUNT; i++) {
                TupleValue value = insert.newTuple().columns().get(i);
                decodedByName.put(relation.columns().get(i).name(), assertInstanceOf(TupleValue.Text.class, value,
                        relation.columns().get(i).name() + " 应为文本种类").value());
            }

            // PG 自身文本输出作 oracle
            List<String> expected = new ArrayList<>();
            try (Connection c = PgTestEnv.newSqlConnection();
                 ResultSet rs = c.createStatement().executeQuery("SELECT * FROM t_types")) {
                assertTrue(rs.next());
                for (int i = 1; i <= COLUMN_COUNT; i++) {
                    expected.add(rs.getString(i));
                }
            }
            for (int i = 0; i < COLUMN_COUNT; i++) {
                String column = relation.columns().get(i).name();
                assertEquals(expected.get(i), decodedByName.get(column),
                        column + " 解码值应与 PG 文本输出一致");
            }
        }
    }
}
