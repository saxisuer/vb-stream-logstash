package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 读取循环吞吐回归的连接器形态(引擎 it 包 ReaderThroughputTest 的翻译):
 * {@code ReplicationSession.run} 的消息取送必须是 drain 语义——非阻塞取尽当前缓冲
 * 的全部消息,而不是每轮取一条后固定 sleep 100ms(旧节拍把 slot 读取上限钉死在
 * ~10 msg/s,500 行事务 50+ 秒才收完,表现为"连接器看不到任何数据")。判定选 500
 * 行单事务:Begin + Relation + 500×Insert + Commit ≈ 503 条消息,旧节拍下确定性
 * 需 ~50s(100ms/条是硬性 sleep,不受机器快慢影响),35s 消费时限必红;drain 节拍
 * 下秒级完成,余量充足。归连接器断言面:500 条数据记录在 35s 消费超时内到齐
 * (setConsumeTimeout 支撑,凑不齐即基座断言失败)——consumer 侧的到达以 reader
 * 收完为前提,节拍退化直接反映为超时。事务元数据关闭(本测试只关心行数节拍,少
 * 两条记录少一个变量)。须独立槽 + 建流前清残留:残留槽会从旧 confirmed_flush
 * 重放历史流量,扭曲到达计时。需要本机 Docker。
 */
class ReaderThroughputIT extends StreamITBase {

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "reader_tp_it";

    /** 数据表名。 */
    private static final String TABLE = "t_tp";

    /** 目标行数(单事务)。 */
    private static final int ROWS = 500;

    /** 消费时限(秒):旧退化节拍 ~50s 必红,drain 节拍余量充足。 */
    private static final long CONSUME_TIMEOUT_SECONDS = 35;

    /** 每用例独立的管道目录(瞬态工作区)。 */
    @TempDir
    Path pipeDir;

    /** 每用例前清残留槽:残留槽重放历史流量会扭曲到达计时。幂等。 */
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
     * 500 行单事务在 35s 内完整到达消费面。关键步骤:夹具 → start(流式 parallel、
     * 无事务元数据)→ 单事务批量写 500 行 → setConsumeTimeout(35s) 后按数消费,
     * 凑不齐 500 条即基座断言失败(消息含 topic 归集,恰量断言数据 topic 500 条)。
     * 容器内无其他写者,建流后唯一写流量即本事务(先建流后写——先建槽后写会把
     * 事务 WAL 留在 restart_lsn 之前,槽直接跳过不重放,引擎 it 包同习语)。
     */
    @Test
    void readerDrainsBufferedMessagesWithoutPerMessageThrottle() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_tp_it",
                "CREATE PUBLICATION pub_tp_it FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);

        start(PostgresStreamConnector.class,
                baseConfig(SLOT, "pub_tp_it", pipeDir).with("provide.transaction.metadata", false).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        try (var c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (var ps = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, 'tp')")) {
                for (int i = 1; i <= ROWS; i++) {
                    ps.setInt(1, i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            finally {
                c.commit();
            }
        }

        setConsumeTimeout(CONSUME_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        // 免校验消费(VerifyRecord 在 JDK 17 链接期引用 Confluent 类,见基类替身 javadoc);
        // 单次空轮等满 35s(pollTimeoutInMs)即放弃,凑不齐由下方断言钉死
        List<SourceRecord> arrived = new java.util.ArrayList<>();
        consumeRecords(ROWS, 1, arrived::add, false);
        assertEquals(ROWS, recordsForTopic(arrived, "ms2it.public." + TABLE).size(),
                "500 行事务应在 " + CONSUME_TIMEOUT_SECONDS + "s 内全部到达(节拍退化即超时)");
    }
}
