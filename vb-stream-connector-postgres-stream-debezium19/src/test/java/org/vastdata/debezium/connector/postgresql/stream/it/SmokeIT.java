package org.vastdata.debezium.connector.postgresql.stream.it;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 1.9.7 线自建 IT 基座的端到端冒烟(Task 8 验收,<b>模块永久保留</b>——后续全套 IT
 * 之外的最小链路回声):3 行 INSERT 单事务 → 恰 3 条 op=c 数据记录 + 事务元数据
 * BEGIN/END 一对,共 5 条记录。这是一条链路全通的证明:{@link AbstractStreamIT}
 * 的 EmbeddedEngine 装配(1.9.7 无 format.Connect,经 EmbeddedEngine.create() 直取
 * 裸 SourceRecord)→ PostgresStreamConnector.taskConfigs 默认值注入 → 任务真装配
 * (PostgresConnection × pgjdbc 42.7.13——1.9.7 与新版驱动的兼容性风险点,spec §6.3)
 * → 复制槽自建 + pgoutput 流式源 → Kafka 记录面(数据 topic + 事务 topic)。
 * 夹具约定:表与 publication 预建,槽 {@code smoke_it} 前后清删。需要本机 Docker。
 */
class SmokeIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "smoke_it";

    /** 数据表名(publication 单表,id + payload 两列)。 */
    private static final String TABLE = "t_smoke";

    /** 数据记录 topic(DefaultTopicNamingStrategy:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;,prefix=ms2it)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open;NEVER——CQ mmap 在 Windows 上 held 句柄由重启自清)。 */
    @TempDir(cleanup = CleanupMode.NEVER)
    Path pipeDir;

    /**
     * 每用例前清残留槽 + 建夹具:上次异常退出留下的同名槽会从旧 confirmed_flush_lsn
     * 续传,静默吞掉建流前的写入;DDL 先于建槽执行不产生解码输出,不污染记录计数;
     * publication 预建是连接器 start 的边界(无自动建,缺失即建流报错)。
     */
    @BeforeEach
    void cleanSlotAndCreateFixture() throws Exception {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_smoke",
                "CREATE PUBLICATION pub_smoke FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /** 每用例后清理:先停引擎再删槽(次序见基类 {@link #stopEngineAndDropSlot})。 */
    @AfterEach
    void dropSlot() {
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 最小端到端:3 行 INSERT → 3 条 c 记录 + 事务元数据 BEGIN/END。
     * 关键步骤:start 引擎(parallel 档基座配置)→ 等 walsender 挂上(建流完成的
     * 可观测汇合点,先于写入——建流前写入可能落在 restart_lsn 之前被跳过)→
     * 单语句 3 行 INSERT(单事务)→ consumeRecordsUnchecked(5) → 断言:总数恰 5
     * (隐含零 op=r 快照记录)、数据 topic 恰 3 条且 op=c、after.id/payload 与插入值
     * 逐一相等、事务 topic 恰 BEGIN+END 两条(id 一致、END 的 event_count=3、
     * data_collections 恰本表 3 条)。
     * 边界:消费超时凑不齐 5 条即断言红(附 describe 全量摘要诊断)。
     */
    @Test
    void threeInsertsReachConsumerWithTransactionMetadata() throws Exception {
        start(PostgresStreamConnector.class, baseConfig(SLOT, "pub_smoke", pipeDir).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        StreamPgTestEnv.execSql("INSERT INTO " + TABLE
                + " VALUES (1, 'smoke-1'), (2, 'smoke-2'), (3, 'smoke-3')");

        List<SourceRecord> all = consumeRecordsUnchecked(5);
        assertEquals(5, all.size(), "3 数据 + BEGIN + END 共 5 条记录应到达: " + describe(all));

        List<SourceRecord> data = recordsForTopic(all, TOPIC);
        assertEquals(3, data.size(), "数据 topic 应恰 3 条 INSERT: " + describe(all));
        Set<Integer> seenIds = new HashSet<>();
        for (SourceRecord r : data) {
            assertEquals(TOPIC, r.topic(), "数据记录 topic 应为数据表 topic");
            Struct value = (Struct) r.value();
            assertEquals("c", value.getString("op"), "应全为 INSERT(op=c)");
            Struct after = value.getStruct("after");
            assertNotNull(after, "INSERT 记录应有 after 结构");
            Integer id = after.getInt32("id");
            assertTrue(id == 1 || id == 2 || id == 3, "未知 id 到达: " + id);
            assertEquals("smoke-" + id, after.getString("payload"), "payload 应与插入值相等(id=" + id + ")");
            assertTrue(seenIds.add(id), "id 重复到达: " + id);
        }
        assertEquals(Set.of(1, 2, 3), seenIds, "三条记录应恰好覆盖全部插入行");

        List<SourceRecord> tx = recordsForTopic(all, TX_TOPIC);
        assertEquals(2, tx.size(), "事务元数据应恰 BEGIN+END 两条: " + describe(all));
        Struct begin = (Struct) tx.get(0).value();
        assertEquals("BEGIN", begin.getString("status"), "首条事务块应为 BEGIN");
        Struct end = (Struct) tx.get(1).value();
        assertEquals("END", end.getString("status"), "末条事务块应为 END");
        assertEquals(begin.getString("id"), end.getString("id"), "END 与 BEGIN 的事务 id 应一致");
        assertEquals(3L, end.getInt64("event_count"), "END 的事件计数应等于数据记录数");
        List<Struct> collections = end.getArray("data_collections");
        assertEquals(1, collections.size(), "单表事务的 data_collections 应恰一项");
        assertEquals("public." + TABLE, collections.get(0).getString("data_collection"),
                "data_collections 应记数据表 id(schema.table)");
        assertEquals(3L, collections.get(0).getInt64("event_count"), "分表计数应等于数据记录数");
    }
}
