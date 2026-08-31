package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.DecodedMessageBridge;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * raw 会话契约集成测试：验证 run() 交付的原始字节与解码消息严格一一对应。
 *
 * <p>验证三角：①{@code rawMessages()} 与 {@code messages()} 逐条等长（一条 raw 恰好解码一条消息，
 * 不多不少）；②每条 raw 首字节是合法 pgoutput 类型字符（19 种之一，见 spec 附录 A）；
 * ③用全新 {@link DecodedMessageBridge} 顺序重放 raw 流，得到的消息序列与在线解码结果
 * record 值相等——证明"raw 字节完整保留了再解码所需的全部信息"（spill 回放路径的正确性根基）。
 */
class RawSessionContractTest {

    /** 19 种 pgoutput 消息的类型字符（spec 附录 A）：首字节合法性断言的白名单。 */
    private static final String VALID_TYPE_CHARS = "BCORYIUDTMSEcAbPKrp";

    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_raw");
    }

    @Test
    void rawBytesCorrespondOneToOneWithDecodedMessages() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_raw(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_raw",
                "CREATE PUBLICATION pub_raw FOR TABLE t_raw",
                "TRUNCATE t_raw");
        ReplicationConfig config = PgTestEnv.newConfig("slot_raw", "pub_raw");

        // 停止条件：首个 Commit（本用例只产生一个显式事务）。
        // 不用 try-with-resources 的声明形态：raw/decoded 等长断言必须 close 后做，harness 需在
        // close 之后仍在作用域——改用显式 try/finally（close 保证与既有 try-with-resources 等价）。
        AtomicInteger committedTxns = new AtomicInteger();
        SessionHarness harness = SessionHarness.start(
                config,
                msg -> msg instanceof PgOutputMessage.Commit && committedTxns.incrementAndGet() >= 1);
        try {
            insertTwoRowsAndCommit();
            harness.awaitTermination(Duration.ofSeconds(30));
        } finally {
            harness.close(); // close 后两列表不再增长，方可做确定性全量断言（同既有顺序契约）
        }

        List<PgOutputMessage> decoded = harness.messages();
        List<byte[]> raws = harness.rawMessages();

        // 锚定非退化：单事务两行 → Begin, Relation, Insert, Insert, Commit（协议保证 Relation 先于 DML）
        List<Class<?>> types = decoded.stream().<Class<?>>map(Object::getClass).toList();
        assertEquals(List.of(PgOutputMessage.Begin.class, PgOutputMessage.Relation.class,
                        PgOutputMessage.Insert.class, PgOutputMessage.Insert.class,
                        PgOutputMessage.Commit.class),
                types, "单事务两行应为 5 条消息序列: " + types);

        // ① 一一对应
        assertEquals(decoded.size(), raws.size(), "raw 条数应与解码消息条数一致");

        // ② 首字节合法类型字符
        for (byte[] raw : raws) {
            assertTrue(VALID_TYPE_CHARS.indexOf(raw[0]) >= 0,
                    "raw 首字节应为合法类型字符，实际: '" + (char) raw[0] + "' (0x"
                            + Integer.toHexString(raw[0] & 0xFF) + ")");
        }

        // ③ 离线重放：全新桥顺序喂 raw，消息序列 record 值相等
        List<PgOutputMessage> replayed = new ArrayList<>();
        DecodedMessageBridge replayBridge = new DecodedMessageBridge(
                (msg, registry) -> replayed.add(msg), config.streamingMode());
        for (byte[] raw : raws) {
            replayBridge.onRaw(raw);
        }
        assertEquals(decoded, replayed, "重放消息应与在线解码逐条 record 值相等");
    }

    /**
     * 另一连接显式事务写入两行后提交：一个事务恰好产生 Begin..Commit 一个完整序列，
     * 两行 Insert 共享同一 Relation 元数据消息。
     */
    private static void insertTwoRowsAndCommit() throws Exception {
        try (Connection c = PgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.executeUpdate("INSERT INTO t_raw VALUES (1, 'a')");
                st.executeUpdate("INSERT INTO t_raw VALUES (2, 'b')");
            }
            c.commit();
        }
    }
}
