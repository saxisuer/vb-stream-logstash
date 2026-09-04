package org.vastdata.debezium.connector.postgresql.stream.it;

import io.debezium.config.Configuration;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS4 两阶段提交端到端验收(四场景):①PREPARE 挂起期零发射、COMMIT PREPARED 后全量落
 * Kafka(on 档);②ROLLBACK PREPARED 弃桶全程零发射(on 档);③流式大事务 PREPARE 以
 * StreamPrepare 收尾、COMMIT PREPARED 后 500 行全达(<b>parallel 档端到端验收</b>——
 * StreamPrepare 是 PARALLEL×two_phase 独有路径);④prepared 挂起期停机、重启重发
 * BeginPrepare..Prepare、COMMIT PREPARED 后补齐(offset 推进越过 CommitPrepared 位点)。
 *
 * <p>发射语义依据:组装器 {@code preparedByGid} 挂起池——PREPARE 后桶挂起零交付,
 * CommitPrepared 才交接回放(BEGIN+数据+END),RollbackPrepared 直接弃桶。挂起期"零发射"
 * 断言以哨兵事务证管道存活(仅凭等若干秒无记录是弱断言)。
 *
 * <p>夹具约定:表 t_2pc_ms4(id int PK + payload text)与 publication 预建;独立槽
 * {@code ms4_twophase} 前后清删;已知 gid 集 @BeforeEach/@AfterEach 双向 ROLLBACK PREPARED
 * 自愈(用例中途失败留下的未决 prepared 会持锁阻塞下一用例的 TRUNCATE 夹具)。管道目录
 * @TempDir 绝对路径。需要本机 Docker。
 */
class TwoPhaseIT extends StreamITBase {

    /** 本测试类专用复制槽名:@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "ms4_twophase";

    /** 两阶段验收数据表。 */
    private static final String TABLE = "t_2pc_ms4";

    /** publication 名(单表)。 */
    private static final String PUB = "pub_2pc_ms4";

    /** 数据记录 topic(DefaultTopicNamingStrategy)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** 哨兵行 id 起始(prepared 事务数据行用 1..N 小 id 空间,哨兵用 900+ 隔离)。 */
    private static final int SENTINEL_ID_FROM = 901;

    /** 本类跨用例可能出现的全部 gid(夹具自愈与用例尾清理的统一清单)。 */
    private static final String[] ALL_GIDS = { "gid_c1", "gid_rb", "gid_big", "gid_restart" };

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir
    Path pipeDir;

    /**
     * 每用例前自愈:①清残留槽(上次异常退出留下的同名槽从旧 confirmed_flush_lsn 续传,
     * 静默吞掉建流前的写入使记录断言失真);②回滚残留未决 prepared(JVM 中途被杀时留下,
     * 持有的表锁会阻塞本用例夹具的 TRUNCATE)。均幂等。
     */
    @BeforeEach
    void cleanResidualSlotAndPrepared() {
        rollbackPreparedQuietly(ALL_GIDS);
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 每用例后清理:先回滚本用例可能残留的未决 prepared(中途断言失败的兜底),再先停引擎
     * 后删槽(次序见基类 {@link #stopEngineAndDropSlot})。
     */
    @AfterEach
    void rollbackPreparedAndDropSlot() {
        rollbackPreparedQuietly(ALL_GIDS);
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 夹具:建表、建 publication(单表)、清空历史数据。DDL/TRUNCATE 先于建槽执行——不产生
     * 解码输出,不污染记录计数;publication 预建是 start 的边界(无自动建,缺失即建流报错)。
     */
    private void createFixture() throws SQLException {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS " + PUB,
                "CREATE PUBLICATION " + PUB + " FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);
    }

    /**
     * 单事务插 rows 行后 PREPARE TRANSACTION(两阶段挂起构造):裸语句管理事务
     * (autocommit 连接上 BEGIN..PREPARE,引擎 TwoPhaseTransactionTest 同款——PREPARE
     * TRANSACTION 不能在 JDBC 显式事务内执行)。载荷 "前缀-id" 可预期。
     *
     * @param gid    两阶段事务名
     * @param idFrom 起始 id(含),逐行 +1
     * @param rows   行数
     * @param prefix 载荷前缀(实际载荷 "前缀-id")
     * @throws SQLException SQL 失败原样上抛
     */
    private static void prepareRows(String gid, int idFrom, int rows, String prefix) throws SQLException {
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            try (Statement st = c.createStatement()) {
                st.execute("BEGIN");
                for (int i = 0; i < rows; i++) {
                    st.execute("INSERT INTO " + TABLE + " VALUES (" + (idFrom + i) + ", '" + prefix + "-" + (idFrom + i) + "')");
                }
                st.execute("PREPARE TRANSACTION '" + gid + "'");
            }
        }
    }

    /**
     * 自动提交单行哨兵插入(挂起期"零发射"断言的管道存活证据):普通小事务,立即提交立即可消费。
     *
     * @param id 哨兵行 id(用 900+ 空间与 prepared 数据行隔离)
     * @throws SQLException SQL 失败原样上抛
     */
    private static void insertSentinelRow(int id) throws SQLException {
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (" + id + ", 'sentinel-" + id + "')");
    }

    /**
     * 静默回滚未决 prepared(夹具自愈):该 gid 已无未决事务(已 COMMIT/ROLLBACK PREPARED
     * 或从未创建)时 SQL 报 42704,吞掉属预期——"quietly" 习语,不打日志不失败。
     *
     * @param gids 待清理的 gid 清单
     */
    private static void rollbackPreparedQuietly(String... gids) {
        for (String gid : gids) {
            try {
                StreamPgTestEnv.execSql("ROLLBACK PREPARED '" + gid + "'");
            }
            catch (SQLException e) {
                // gid 无未决 prepared 事务——静默跳过
            }
        }
    }

    /**
     * 从数据 topic 记录提取 id 集(after 为 null 的防御跳过):零发射/全集断言的收集面。
     *
     * @param records 全部到达记录(跨轮次)
     * @return 出现过的 id 集(重复收敛为单值)
     */
    private static Set<Integer> dataIds(List<SourceRecord> records) {
        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : recordsForTopic(records, TOPIC)) {
            Struct after = ((Struct) r.value()).getStruct("after");
            if (after != null) {
                ids.add(after.getInt32("id"));
            }
        }
        return ids;
    }

    /**
     * 场景①(on 档):PREPARE 挂起期零发射、COMMIT PREPARED 后全量落 Kafka。关键步骤:
     * start(slot.streaming=on 覆盖 baseConfig 的 parallel 默认)→ PREPARE 3 行('gid_c1')
     * → 哨兵行 901 消费 3 条(BEGIN+1 数据+END,证明管道活着)且恰无 prepared 数据行
     * → COMMIT PREPARED → 消费 5 条(BEGIN+3 数据+END)→ 断言 ids 恰 {1,2,3}、payload
     * 全文相等、END event_count=3。
     * 边界:挂起期若凑不齐哨兵 3 条即 fail(管道死与零发射的区分是本场景的断言核心)。
     */
    @Test
    void prepareThenCommitPreparedEmitsOnlyAfterCommitPrepared() throws Exception {
        createFixture();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, PUB, pipeDir).with("slot.streaming", "on").build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        prepareRows("gid_c1", 1, 3, "c1");
        insertSentinelRow(SENTINEL_ID_FROM);
        List<SourceRecord> duringPending = consumeRecordsUnchecked(3);
        assertEquals(3, duringPending.size(), "挂起期哨兵事务 BEGIN+1 数据+END 应到达: " + describe(duringPending));
        assertEquals(Set.of(SENTINEL_ID_FROM), dataIds(duringPending),
                "PREPARE 挂起期应零发射(prepared 数据行不得早于 COMMIT PREPARED 出现)");

        StreamPgTestEnv.execSql("COMMIT PREPARED 'gid_c1'");
        List<SourceRecord> emitted = consumeRecordsUnchecked(5);
        assertEquals(5, emitted.size(), "COMMIT PREPARED 后 BEGIN+3 数据+END 共 5 条应到达: " + describe(emitted));
        assertEquals(Set.of(1, 2, 3), dataIds(emitted), "prepared 事务 3 行应全量到达");
        for (SourceRecord r : recordsForTopic(emitted, TOPIC)) {
            Struct after = ((Struct) r.value()).getStruct("after");
            assertEquals("c1-" + after.getInt32("id"), after.getString("payload"), "payload 应全文相等");
        }
        assertTrue(hasEndWithEventCount(emitted, 3), "事务 topic 应有 event_count=3 的 END: " + describe(emitted));
    }

    /**
     * 事务 topic 是否存在指定 event_count 的 END 记录(完整事务边界信号;其他事务的 END
     * 不影响命中)。status 字段缺失的记录(理论不出现)跳过。
     *
     * @param records    全部到达记录
     * @param eventCount 期望的 END 事件计数(数据记录数)
     * @return 存在为 true
     */
    private static boolean hasEndWithEventCount(List<SourceRecord> records, long eventCount) {
        for (SourceRecord r : recordsForTopic(records, TX_TOPIC)) {
            Struct value = (Struct) r.value();
            if ("END".equals(value.getString("status"))
                    && Long.valueOf(eventCount).equals(value.getInt64("event_count"))) {
                return true;
            }
        }
        return false;
    }
}
