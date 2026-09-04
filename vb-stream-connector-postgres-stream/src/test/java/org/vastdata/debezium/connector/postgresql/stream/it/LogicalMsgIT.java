package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS3.5 心跳推进验收(spec §3.4 场景 1,"全发完了"):config 开 {@code slot.messages=true}
 * 后,<b>纯非事务消息流</b>(连发 {@code pg_logical_emit_message(false,'heartbeat',...)},
 * 全程无任何<b>并发/在途</b>表事务)应把槽 confirmed_flush_lsn 推进越过暖场边界——钉
 * "空闲库不钉死"的原始动机:最后一笔事务输出完毕后库转入空闲,前沿唯一推进点(End 处理
 * 完毕)永不触发,反馈被 capFeedback 的前沿封顶钉死在末笔事务 endLsn,槽确认位点冻结
 * (WAL 无限保留,只剩 {@code max_slot_wal_keep_size} 兜底删档)——除非非事务消息即时
 * 推进前沿(T1/T2 接线)。
 *
 * <p><b>为何必须有暖场事务 T0(实测结论,防恒真)</b>:{@code capFeedback} 在前沿 ≤0
 * (尚未有任何事务输出)时视为无 cap、直推已收到值——fresh 槽零事务形态下 confirmed_flush
 * 经 received 路径自走,断言对门控关闭也绿(本 IT 开发期实测:messages=false 的首轮即绿,
 * 恒真被否)。故先用一笔小事务 T0 拉起前沿(T0.endLsn &gt; 0 = cap 生效),此后库转入
 * 纯空闲(仅心跳消息,无任何表活动)——门控关闭时前沿冻结在 T0.endLsn ≤ 边界,断言必红
 * (先红后绿的"红");门控开启时消息把前沿推到末条消息位 &gt; 边界,断言绿。T0 已 DONE,
 * 恰构成 spec §3.4 的"全发完了"状态:无 pending 桶、无在途事务。
 *
 * <p><b>机制依据(spec §3.4 状态枚举,"全发完了"场景)</b>:消息 X 即时推进到
 * {@code safeMessageAdvance(X, handedOff)} 的安全性由三态覆盖——①已输出事务
 * (DONE,前沿 ≥ 其 endLsn):前沿单调 max,消息推进不回退已覆盖区间;②在途未提交
 * (live 桶,endLsn 未知):其 commit 在 WAL 序上必然 &gt; X,重启经 restart_lsn 整体重发,
 * 完整到达;③X 之后的新事务:LSN &gt; X,重放覆盖。T0 完毕后无 pending 桶,safe = X
 * 本身即安全上限:所有已解码内容都已交付确认,确认到 X 不跳过任何未送达内容。
 *
 * <p><b>断言面(实测结论)</b>:PG 的 confirmed_flush_lsn 由 walsender 在收到 standby
 * status 时经 LogicalConfirmReceivedLocation 直接采纳客户端 flush 位——非事务消息即时
 * 下发即时解码,不需要 commit 触发解码推进,主断言直接锚 {@code pg_replication_slots
 * .confirmed_flush_lsn} 轮询越过暖场边界即可绿(无需退 {@code standbyFlushBeyond} 的
 * pg_stat_replication.flush 面;机制证据见任务报告)。
 *
 * <p><b>边界取值的安全性</b>:暖场边界 = 首条消息前的 {@code pg_current_wal_insert_lsn()}
 * (最后一条 WAL 记录的结束位)。首条消息记录的 LSN 可能恰等于边界(下一条记录起始 =
 * 上一条结束,无页填充时零间隙),故连发 4 条——确认值最终锚<b>末条</b>消息 LSN,末条
 * 记录严格大于首条(记录长度非零),&gt; 边界恒成立,单条消息的 off-by-zero 不影响断言。
 *
 * <p>夹具:独立槽 {@code logical_msg_it} 前后清删(残留槽续传旧位点破坏"边界后唯一
 * WAL 活动是消息"的前提);表/publication 先于建槽(pgoutput 协议硬性要求
 * publication_names);管道 @TempDir。需要本机 Docker。
 */
class LogicalMsgIT extends StreamITBase {

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "logical_msg_it";

    /** 暖场表名(T0 单行写入拉起前沿;publication 挂名用)。 */
    private static final String TABLE = "t_msg";

    /** T0 暖场事务的记录数:1 数据 + 事务元数据 BEGIN/END 共 3 条。 */
    private static final int WARMUP_RECORDS = 3;

    /** 心跳消息条数:≥2 保证末条消息 LSN 严格大于暖场边界(见类 javadoc 边界段)。 */
    private static final int HEARTBEATS = 4;

    /** 消息间隔毫秒:&gt; 1s 反馈周期(baseConfig 的 slot.feedback.interval.ms=1000),
     * 让每条消息后都有独立反馈周期走过,观察窗口不依赖单次 status 包的时序运气。 */
    private static final long INTER_MESSAGE_MILLIS = 1500;

    /** 每用例独立的管道目录(瞬态工作区,引擎启动 wipe-on-open)。 */
    @TempDir
    Path pipeDir;

    /**
     * 每用例前清残留槽:残留同名槽从旧 confirmed_flush_lsn 续传,"边界后唯一 WAL 活动
     * 是心跳消息"的锚点前提即被破坏。幂等。
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
     * 纯非事务消息流(末笔事务输出后的空闲库)推进 confirmed_flush 越过消息位。
     * 关键步骤:夹具(表/publication 预建)→ 开 {@code slot.messages=true} start 引擎 →
     * 等 walsender 挂上(建流完成)→ 暖场事务 T0(单行 INSERT)并等 3 条记录消费完毕
     * (End 已处理,前沿 = T0.endLsn &gt; 0 = cap 生效;此后库转入纯空闲)→ 取
     * {@code pg_current_wal_insert_lsn()} 为暖场边界 → 连发 4 条非事务心跳(间隔 1.5s
     * 走过多个反馈周期,无任何表活动)→ 轮询断言 confirmed_flush &gt; 边界。每条消息的
     * reader 侧路径:'M' 下发 → routeLogicalMsg 无桶非事务分支 INFO 留痕 + 前沿 max
     * 推进到 min(msgLsn, 无 pending = msgLsn)→ 下轮反馈 capFeedback(min(已收到, 前沿))
     * = msgLsn → 服务端采纳进 confirmed_flush。
     * 边界:边界后无表事务/无 DML——门控关闭时('M' 不下发)前沿冻结在 T0.endLsn ≤ 边界,
     * 断言必红(非恒真;开发期实测证伪过"fresh 槽零事务"形态,见类 javadoc)。
     */
    @Test
    void idleDatabaseHeartbeatAdvancesConfirmedFlush() throws Exception {
        StreamPgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(id int)",
                "DROP PUBLICATION IF EXISTS pub_msg_it",
                "CREATE PUBLICATION pub_msg_it FOR TABLE " + TABLE,
                "TRUNCATE " + TABLE);

        start(PostgresStreamConnector.class,
                baseConfig(SLOT, "pub_msg_it", pipeDir).with("slot.messages", true).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        // 暖场 T0:输出完毕即前沿 = T0.endLsn > 0(cap 生效),库转入"全发完了"的空闲态
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (0)");
        List<SourceRecord> warmup = consumeRecordsUnchecked(WARMUP_RECORDS);
        assertTrue(warmup.size() >= WARMUP_RECORDS,
                "暖场事务 3 条记录(数据 + BEGIN + END)未到达: " + describe(warmup));

        // 暖场边界:此后所有 WAL 活动仅心跳消息(pg_logical_emit_message 自成一条 WAL 记录)
        long boundary = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_insert_lsn()");
        for (int i = 1; i <= HEARTBEATS; i++) {
            StreamPgTestEnv.execSql(
                    "SELECT pg_logical_emit_message(false, 'heartbeat', 'hb-" + i + "')");
            Thread.sleep(INTER_MESSAGE_MILLIS);
        }

        // 主断言面:pg_replication_slots.confirmed_flush_lsn(实测纯消息流即可落库,
        // 见类 javadoc 断言面段);> 语义,末条消息 LSN 严格大于边界
        await("空闲库心跳流 confirmed_flush 越过暖场边界").atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> StreamPgTestEnv.confirmedFlushLsn(SLOT) > boundary);

        // 自洽护栏:边界必在当前 WAL 尾之前或相等(WAL 插入位单调性健全性检查)
        assertTrue(StreamPgTestEnv.lsnOf("SELECT pg_current_wal_insert_lsn()") >= boundary,
                "自洽护栏: WAL 插入位不应回退");
    }
}
