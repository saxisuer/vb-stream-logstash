package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.time.Duration;

/**
 * 读取循环吞吐回归（2026-08-31 吞吐冒烟踩坑的锚定用例）：{@code PgReplicationSession.run}
 * 的消息取送必须是 drain 语义——非阻塞取尽当前缓冲的全部消息，而不是每轮取一条后固定
 * sleep 100ms（旧节拍把 slot 读取上限钉死在 ~10 msg/s，5 万行大事务需 90+ 分钟才收完，
 * 表现为"Main 看不到任何数据"）。回归判定选 500 行单事务：Begin + Relation + 500×Insert
 * + Commit ≈ 503 条消息，旧节拍下确定性需 ~50s（100ms/条是硬性 sleep，不受机器快慢影响），
 * 35s 时限必红；drain 节拍下秒级完成，余量 10 倍以上。须用独立槽 + 建流前清残留：本用例
 * 对流起点敏感，残留槽会从旧 confirmed_flush 重放历史流量，扭曲到达计时。
 */
class ReaderThroughputTest {

    @BeforeEach
    @AfterEach
    void cleanup() {
        PgTestEnv.dropSlotQuietly("slot_tp");
    }

    /**
     * 500 行单事务在 35s 内录制到提交消息：停止条件取 Commit 或 StreamCommit 任一（测试容器
     * logical_decoding_work_mem=64kB，500 行小事务可能越过阈值走流式路径，提交消息形态随之
     * 二选一——首跑实测 StreamStart 出现即流式；只等 Commit 会永不满足）。容器内无其他写者，
     * 建流后唯一写流量即本事务。写流量在建流后发生——先建槽后写会把事务 WAL 留在 restart_lsn
     * 之前，槽直接跳过不重放（NormalTransactionTest 同习语）。断言只依赖 awaitTermination 的
     * 超时语义：超时即"节拍退化"失败，消息直方图随异常输出供诊断。
     */
    @Test
    void readLoopDrainsBufferedMessagesWithoutPerMessageThrottle() throws Exception {
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_tp(id int PRIMARY KEY, v text)",
                "DROP PUBLICATION IF EXISTS pub_tp",
                "CREATE PUBLICATION pub_tp FOR TABLE t_tp",
                "TRUNCATE t_tp");
        try (SessionHarness harness = SessionHarness.start(
                PgTestEnv.newConfig("slot_tp", "pub_tp"),
                msg -> msg instanceof PgOutputMessage.Commit
                        || msg instanceof PgOutputMessage.StreamCommit)) {
            PgTestEnv.execSql(
                    "INSERT INTO t_tp SELECT g, 'tp' FROM generate_series(1, 500) AS g");
            harness.awaitTermination(Duration.ofSeconds(35));
        }
    }
}
