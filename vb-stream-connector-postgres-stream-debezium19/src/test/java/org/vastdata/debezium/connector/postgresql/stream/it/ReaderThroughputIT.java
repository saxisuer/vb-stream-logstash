package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 读取循环吞吐回归的连接器形态(引擎 it 包 ReaderThroughputTest 的翻译):
 * {@code ReplicationSession.run} 的消息取送必须是 drain 语义——非阻塞取尽当前缓冲
 * 的全部消息,而不是每轮取一条后固定 sleep 100ms(旧节拍把 slot 读取上限钉死在
 * ~10 msg/s,500 行事务 50+ 秒才收完,表现为"连接器看不到任何数据")。判定选 500
 * 行单事务:Begin + Relation + 500×Insert + Commit ≈ 503 条消息,旧节拍下确定性
 * 需 ~50s(100ms/条是硬性 sleep,不受机器快慢影响),35s 消费时限必红;drain 节拍
 * 下秒级完成,余量充足。归连接器断言面:500 条数据记录在 35s 窗口内到齐——基座
 * {@code consumeRecords} 的截止是写死的 60s(3.6.1 基座的 setConsumeTimeout 面
 * 不存在),故本用例自持 35s 窗口:Awaitility atMost 驱动 {@code drainArrivedRecords}
 * 轮询累计,窗口内凑不齐即断言失败——consumer 侧的到达以 reader 收完为前提,节拍
 * 退化直接反映为超时。事务元数据关闭(本测试只关心行数节拍,少两条记录少一个变
 * 量)。须独立槽 + 建流前清残留:残留槽会从旧 confirmed_flush 重放历史流量,扭曲
 * 到达计时。需要本机 Docker。
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
    @TempDir(cleanup = CleanupMode.NEVER)
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
     * 无事务元数据)→ 单事务批量写 500 行 → 自持 35s 窗口轮询:每轮非阻塞排空基座
     * 记录队列累计到达集 + assertNoEngineFailure 快速失败探测,窗口内数据 topic 凑齐
     * 500 条即过(消息含 topic 归集,恰量断言数据 topic 500 条;窗口耗尽由 Awaitility
     * 以最后一次断言失败收场)。
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

        // 自持 35s 窗口(基座 consumeRecords 截止写死 60s,无法表达本判定的时限):
        // 非阻塞排空累计 + 快速失败探测,凑不齐 500 条由断言在窗口边界钉死
        List<SourceRecord> arrived = new ArrayList<>();
        await("500 行事务在 35s 消费时限内到齐")
                .atMost(Duration.ofSeconds(CONSUME_TIMEOUT_SECONDS)).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    drainArrivedRecords(arrived);
                    assertNoEngineFailure();
                    assertEquals(ROWS, recordsForTopic(arrived, "ms2it.public." + TABLE).size(),
                            "500 行事务应在 " + CONSUME_TIMEOUT_SECONDS + "s 内全部到达(节拍退化即超时)");
                });
    }
}
