package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TruncateTest {

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_trunc");
    }

    /** spec 用例 7：TRUNCATE 选项位与多表 oid 列表。 */
    @Test
    void truncateDecodesOptionsAndMultipleOids() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_trunc1(id int PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS t_trunc2(id int PRIMARY KEY)",
                "DROP PUBLICATION IF EXISTS pub_trunc",
                "CREATE PUBLICATION pub_trunc FOR TABLE t_trunc1, t_trunc2",
                "TRUNCATE t_trunc1, t_trunc2");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_trunc", "pub_trunc"),
                msg -> msg instanceof PgOutputMessage.Truncate)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_trunc1 VALUES (1)",
                    "INSERT INTO t_trunc2 VALUES (2)",
                    "TRUNCATE t_trunc1, t_trunc2 RESTART IDENTITY CASCADE");
            harness.awaitTermination(Duration.ofSeconds(30));

            PgOutputMessage.Truncate truncate = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Truncate)
                    .map(m -> (PgOutputMessage.Truncate) m)
                    .findFirst().orElseThrow(() -> new AssertionError("应出现 Truncate 消息"));
            assertEquals(2, truncate.relationOids().length, "TRUNCATE 两张表应携带两个 oid");
            assertTrue(truncate.options().contains(TruncateOption.CASCADE), "应含 CASCADE: " + truncate.options());
            assertTrue(truncate.options().contains(TruncateOption.RESTART_IDENTITY), "应含 RESTART_IDENTITY");
        }
    }
}
