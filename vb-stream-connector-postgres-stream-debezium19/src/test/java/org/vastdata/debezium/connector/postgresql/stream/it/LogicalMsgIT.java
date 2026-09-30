package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 心跳推进验收(spec §3.4 状态枚举 / §4.2 场景 1-3):config 开 {@code slot.messages=true}
 * 后非事务 'M' 即时推进前沿的两个面——
 * <b>场景 1("全发完了")</b>:<b>纯非事务消息流</b>(连发
 * {@code pg_logical_emit_message(false,'heartbeat',...)},全程无任何<b>并发/在途</b>表事务)
 * 应把槽 confirmed_flush_lsn 推进越过暖场边界——钉"空闲库不钉死"的原始动机:最后一笔
 * 事务输出完毕后库转入空闲,前沿唯一推进点(End 处理完毕)永不触发,反馈被 capFeedback
 * 的前沿封顶钉死在末笔事务 endLsn,槽确认位点冻结(WAL 无限保留,只剩
 * {@code max_slot_wal_keep_size} 兜底删档)——除非非事务消息即时推进前沿(T1/T2 接线)。
 *
 * <p><b>场景 2+3(crash 注入主验收 + 状态②重复语义,单用例合并,取舍见该用例 javadoc)</b>:
 * 已提交事务 T 停在输出中段时发心跳——护栏(safeMessageAdvance,L8 全有或全无)应使
 * confirmed_flush <b>完全静止在暖场边界</b>(等值钉死):存在未输出(pending)桶即一个
 * 字节都不推进。部分推进形态(旧 min(commitLsn) 语义)会把 confirmed 抬到 T 的 commit
 * 位(&gt; 暖场边界)→ 等值断言红;直推 msgLsn 形态到消息位 → 同红;"消息路径整体是否
 * 存在"由场景 1(纯消息流必须推进)承担,两场景合起来钉完整语义——随后停机重启,T 整事务重发、尾部不丢(若对 restart_lsn/confirmed_flush
 * 语义理解有误,重启断言必红——它是 spec §3.4 状态③/②断言的实证,不做纯机制断言)。
 *
 * <p><b>1.9.7 线的三处基座接缝适配(语义等价,与 3.6.1 版的差异)</b>:
 * <ol>
 *   <li><b>阻塞消费者安装位置</b>(场景 2+3):3.6.1 基座六参 {@code start} 把自定义消费者
 *       装进引擎记录处理线程;新基座 {@code start} 无消费者注入口(ChangeConsumer 固定
 *       入队基座无界记录队列,永不阻塞)。故本类经 {@link #startBlockingEngine} 自建
 *       embedded 引擎装 {@link #blockingConsumerAt} 的逐条消费者(与 ReaderUnblockedIT
 *       同款——1.9.7 同步引擎的 {@code run()} 在引擎线程串行 poll→handleBatch,消费者
 *       在第 6 条阻塞即输出路径停摆,reader 线程不受影响);</li>
 *   <li><b>offset 文件同路径</b>:自建引擎的 offset 文件取基座同一路径
 *       ({@code target/it-offsets/<测试类>.dat},见 {@link #baseOffsetFilePath})——
 *       源测试"同一 offset 文件重启"的续传语义载体(重启经基座 {@code start} 读取恰此
 *       文件);基座 {@code @BeforeEach} 清场删的也是它,跨用例残留自清;</li>
 *   <li><b>停机中断等待压短</b>:1.9.7 {@code EmbeddedEngine.stop()} 在 interrupt 引擎
 *       线程前先等引擎自身完成闩 {@code debezium.embedded.shutdown.pause.before.interrupt.ms}
 *       (默认 <b>5 分钟</b>)——场景 2+3 的 crash 注入在记录线程 park 中调停机,默认值
 *       使停机调用本身停滞 5 分钟。{@code @BeforeAll} 经该官方系统属性压到 1s,复刻
 *       3.6.1 异步引擎 recordService.shutdownNow 的即时中断收敛语义(JVM 全局副作用:
 *       同 surefire JVM 的其他 IT 引擎停机中断等待同被压短——正常停机的引擎完成闩
 *       毫秒级打开,不受影响)。</li>
 * </ol>
 * 管道目录:场景 2+3 的两次引擎 start 各用独立子目录(engine-a/engine-b)——Windows 下
 * Chronicle mmap 句柄异步释放使同目录立即复用的 wipe-on-open 撞句柄翻车
 * (DecoupledEquivalenceTest 2026-09-08 先例;POSIX 上两种形态等价);续传语义载体是
 * 槽 + Connect offset 文件,与管道目录身份无关。
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
 * <p><b>场景 2+3 的断言锚点(等值钉死,L8)</b>:暖场边界 = T0 输出完毕后取
 * {@code pg_current_wal_insert_lsn()}——无其他 WAL 活动时恰等于 T0.endLsn,即护栏冻结
 * 期间前沿的驻留位。等值断言 {@code confirmed_flush == 暖场边界} 比旧开区间双侧断言更强
 * (钉死精确值):两个退化形态(部分推进 → confirmed 抬到 pending 桶 commitLsn &gt; 边界;
 * 直推 msgLsn → 到消息位 &gt; 边界)都必然撞红,非恒真;且断言前先越过两个完整反馈周期
 * (sleep 3s &gt; 反馈周期 1s),给"会被抬走"的形态留足暴露窗口,瞬时通过不构成假绿。
 * 上界锚(commit 记录结束位)保留为诊断对照值,不参与断言;历史 off-by-one 论证
 * (commitLsn vs endLsn,设计 L5→L8)见 spec 决策记录与护栏 javadoc 的"历史决策"段,
 * 行为级硬验收(重启尾部不丢)不变。
 *
 * <p>夹具:独立槽 {@code logical_msg_it} 前后清删(残留槽续传旧位点破坏"边界后唯一
 * WAL 活动是消息"的前提);表/publication 先于建槽(pgoutput 协议硬性要求
 * publication_names);管道 @TempDir。需要本机 Docker。
 */
class LogicalMsgIT extends StreamITBase {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalMsgIT.class);

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "logical_msg_it";

    /** 暖场表名(T0 单行写入拉起前沿;publication 挂名用)。 */
    private static final String TABLE = "t_msg";

    /** 数据记录 topic(DefaultTopicNamingStrategy:&lt;prefix&gt;.&lt;schema&gt;.&lt;table&gt;)。 */
    private static final String TOPIC = "ms2it.public." + TABLE;

    /** 事务元数据 topic(&lt;prefix&gt;.transaction)。 */
    private static final String TX_TOPIC = "ms2it" + TX_TOPIC_SUFFIX;

    /** T0 暖场事务的记录数:1 数据 + 事务元数据 BEGIN/END 共 3 条。 */
    private static final int WARMUP_RECORDS = 3;

    /** 场景 2+3 阻塞前的放行条数:T0 三条 + T 的 BEGIN + 前 2 行数据 = 6——第 6 条
     * (T 的第 2 行)到达即 park,构成"T 已部分输出(BEGIN+2 行在下游)、尾部滞留"的
     * 状态②形态(比"完全未输出"更强:停机前下游已有头部)。 */
    private static final int BLOCK_AFTER = 5;

    /** 场景 2+3 的大事务 T 行数:40 行 + BEGIN/END = 42 条记录,远超小队列(8)+在途批量
     * (4)的容量——consumer 线程 dispatch 到队列满即阻塞,End 永不 dispatch、前沿钉在
     * T0.endLsn(ReaderUnblockedIT 已证的手法)。 */
    private static final int GUARD_ROWS = 40;

    /** 场景 2+3 的大事务 T 起始 id(101..140,与暖场 id=0 隔离)。 */
    private static final int GUARD_ID_FROM = 101;

    /** 心跳消息条数:≥2 保证末条消息 LSN 严格大于暖场边界(见类 javadoc 边界段)。 */
    private static final int HEARTBEATS = 4;

    /** 消息间隔毫秒:&gt; 1s 反馈周期(baseConfig 的 slot.feedback.interval.ms=1000),
     * 让每条消息后都有独立反馈周期走过,观察窗口不依赖单次 status 包的时序运气。 */
    private static final long INTER_MESSAGE_MILLIS = 1500;

    /**
     * 每用例独立的管道目录父目录:场景 2+3 的两次引擎 start(自建阻塞引擎 + 基座重启)
     * 各用其下独立子目录(engine-a/engine-b)——Windows 下 Chronicle mmap 句柄异步释放
     * 使同目录立即复用的 wipe-on-open 撞句柄翻车(类 javadoc 接缝适配段);场景 1 单
     * 引擎直接用父目录。
     */
    @TempDir(cleanup = CleanupMode.NEVER)
    Path pipeDir;

    /** 自建阻塞引擎的实例(run 期间非 null;场景 2+3 每用例一次 start,不重启)。 */
    private DebeziumEngine<SourceRecord> blockingEngine;

    /** 自建阻塞引擎的线程(run 阻塞至停机;与 blockingEngine 同生命周期)。 */
    private Thread blockingEngineThread;

    /** 自建引擎完成闩:run() 返回(正常或异常)后 CompletionCallback countDown。 */
    private volatile CountDownLatch blockingEngineStopped = new CountDownLatch(1);

    /**
     * 压短 1.9.7 引擎停机的中断等待:EmbeddedEngine.stop() 在 interrupt 引擎线程前先等
     * 引擎完成闩,系统属性 {@code debezium.embedded.shutdown.pause.before.interrupt.ms}
     * 默认 5 分钟——场景 2+3 的 crash 注入在记录线程 park 中停机,默认值使停机调用停滞
     * 5 分钟(1.9.7 sources 实证)。压到 1s 复刻 3.6.1 异步引擎 recordService.
     * shutdownNow 的即时中断收敛;正常停机(完成闩毫秒级打开)不受影响。JVM 全局副作用
     * 见类 javadoc 接缝适配段第 3 条。须先于任何 stop 调用执行(@BeforeAll 保证)。
     */
    @BeforeAll
    static void shortenEngineShutdownInterruptPause() {
        System.setProperty("debezium.embedded.shutdown.pause.before.interrupt.ms", "1000");
    }

    /**
     * 每用例前清残留槽:残留同名槽从旧 confirmed_flush_lsn 续传,"边界后唯一 WAL 活动
     * 是心跳消息"的锚点前提即被破坏。幂等(offset 文件由基座 @BeforeEach 清场删同一路径,
     * 见 {@link #baseOffsetFilePath})。
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
     * 夹具:DROP+CREATE 重建数据表与 publication(单表)。**DROP 而非 IF NOT EXISTS 的
     * 自愈考量**:跨用例/复用容器上跑过旧版夹具的表会让 IF NOT EXISTS 静默 no-op、随后
     * 的列数不符 INSERT 报错;DROP+CREATE 每次按当前模式重建,自愈。publication 同步
     * DROP+CREATE(挂名表必须存在,建槽的边界)。DDL 先于建槽执行——不产生解码输出,
     * 不污染记录计数。
     */
    private void createFixture() throws SQLException {
        StreamPgTestEnv.execSql(
                "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + "(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_msg_it",
                "CREATE PUBLICATION pub_msg_it FOR TABLE " + TABLE);
    }

    /**
     * 纯非事务消息流(末笔事务输出后的空闲库)推进 confirmed_flush 越过消息位。
     * 关键步骤:夹具(表/publication 预建)→ 开 {@code slot.messages=true} start 引擎 →
     * 等 walsender 挂上(建流完成)→ 暖场事务 T0(单行 INSERT)并等 3 条记录消费完毕
     * (End 已处理,前沿 = T0.endLsn &gt; 0 = cap 生效;此后库转入纯空闲)→ 取
     * {@code pg_current_wal_insert_lsn()} 为暖场边界 → 连发 4 条非事务心跳(间隔 1.5s
     * 走过多个反馈周期,无任何表活动)→ 轮询断言 confirmed_flush &gt; 边界。每条消息的
     * reader 侧路径:'M' 下发 → routeLogicalMsg 无桶非事务分支 INFO 留痕 + 前沿 max
     * 推进到 msgLsn(L8 全有或全无:无 pending 桶 → msgLsn)→ 下轮反馈 capFeedback(min(已收到, 前沿))
     * = msgLsn → 服务端采纳进 confirmed_flush。
     * 边界:边界后无表事务/无 DML——门控关闭时('M' 不下发)前沿冻结在 T0.endLsn ≤ 边界,
     * 断言必红(非恒真;开发期实测证伪过"fresh 槽零事务"形态,见类 javadoc)。
     */
    @Test
    void idleDatabaseHeartbeatAdvancesConfirmedFlush() throws Exception {
        createFixture();

        start(PostgresStreamConnector.class,
                baseConfig(SLOT, "pub_msg_it", pipeDir).with("slot.messages", true).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        // 暖场 T0:输出完毕即前沿 = T0.endLsn > 0(cap 生效),库转入"全发完了"的空闲态
        StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (0, 'warm')");
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

    /**
     * 场景 2+3(crash 注入主验收 + 状态②重复语义,<b>单用例合并</b>):已提交事务 T 停在
     * 输出中段(BEGIN+2 行已到下游,尾部滞留)时发非事务心跳 → 护栏(L8 全有或全无:存在 pending 桶即不推进)使 confirmed_flush
     * 完全静止在暖场边界(等值钉死) → 停机(不排干)→ 同一 offset 文件重启 →
     * T 整事务重发(BEGIN+全行+END,尾部不丢;重复头行允许 Set 口径)。
     * <b>合并取舍(spec §4.2 场景 2/3 → 一流程)</b>:场景 3 的"部分输出后停机重启"与
     * 场景 2 的"crash 注入"共享同一构造(阻塞使 T 停在中段)——独立成两用例只在"是否发
     * 心跳"上分叉,而两者的重启面(整事务重发 + 并集断言)完全同构;单用例以阻塞点设在
     * BEGIN 之后 2 行(而非 BEGIN 处)同时满足两者的强形态(场景 3 的字面要求"部分行已
     * dispatch"),少跑一轮 30-60s 的 IT 且断言面零损失。
     *
     * <p><b>关键步骤</b>:夹具 → 小队列 + {@code slot.messages=true} + 阻塞消费者形态
     * start(自建阻塞引擎,管道 engine-a 子目录)→ 暖场 T0(单行,3 条记录放行——前沿
     * = T0.endLsn &gt; 0,cap 生效)→ 取 {@code pg_current_wal_insert_lsn()} 为暖场边界
     * (双侧断言的下界)→ 40 行单事务 T 提交(小队列装满后 consumer 线程阻塞在 dispatch
     * 的 enqueue 上,End 永不 dispatch,T 滞留未输出完;reader 线程照常收心跳)→ 等 park
     * 信号(全局第 6 条 = T 的第 2 行,停机前下游已有 T 的 BEGIN+2 行)→ 取 COMMIT 后、
     * 心跳前的 WAL 插入位为诊断对照锚(见类 javadoc 锚点段)→ 发一条非事务心跳(reader
     * 即时路径:safeMessageAdvance 见 pending 桶 T → 冻结,前沿保持 T0.endLsn 不动——旧
     * 部分推进语义会抬到 T.commitLsn,等值断言即红)→ 等值稳态断言
     * {@code confirmed_flush == 暖场边界},且先 sleep 3s(两个反馈周期)再钉死——部分
     * 推进/直推 msgLsn 两个退化形态都把 confirmed 抬离边界,必红;稳态即"confirmed 已
     * 落库"(实测 walsender 直接采纳 standby flush,轮询到即落库)→ <b>crash 注入点</b>:
     * stopConnector(D7 快速停机,不排干;引擎记录线程 park 在第 6 条上,由引擎停机的
     * interrupt——1s 压短的 shutdown pause 后——中断收敛,见类 javadoc 接缝适配段)→
     * finally 放行闩 → 同一 offset 文件重启(基座 start,管道 engine-b 子目录)→ await
     * 轮询排空累计:T 的全部 40 行必达(payload 全文相等,重复各自独立校验)、
     * event_count ≥ 40 的 END 必达(尾部不丢/事务边界补齐)、停机前已见集(暖场行 +
     * T 的前 2 行)∪ 重启后到达集 = 期望全集(状态②重复语义:整事务重发使头部重复,
     * 允许,Set 口径收敛)。
     *
     * <p><b>END 计数取 ≥ 而非 ==(开发期实测形态)</b>:中段停机重启的重发携带两种
     * 重复形态——①重发流对 T 头部若干行(实测恰为停机前 offset 落盘的在途事务事件
     * 计数,本例 3 行:id 101,102,103)重复下发;②事务元数据 END 的 event_count 从
     * offset 装载的在途计数续算(实测 3 + 40 = 43,而非 40)——两者皆是 at-least-once
     * 文档口径内的重复(下游按事务元数据幂等收敛),非数据错误;故 END 断言取
     * event_count ≥ 40(每行至少计一次),行覆盖取 Set 并集(重复允许),精确重复数
     * 与计数 inflation 取决于停机时 offset 落盘点,不作断言面。
     *
     * <p><b>若对 restart_lsn/confirmed_flush 语义理解有误此用例必红</b>(spec §3.4 状态③
     * 断言的实证,硬验收):停机时 confirmed_flush = T.commitLsn(commit 记录起始位)——
     * 只有"commit 记录<b>结束位</b> ≤ confirmed 的事务才可跳过"的服务端语义才能保证 T
     * 整体重发;若服务端按 commit 起始位判跳过,T 的 38 行尾部在重启后永不再达,并集断言
     * 红——这正是护栏取 commitLsn 而非 endLsn(设计 L5 off-by-one 修正)的行为级检验。
     * 边界:重复头行(暖场行/T 的前 2 行)是否随重放再现有随机性(取决于停机前服务端
     * candidate 采纳的精确落点),故断言并集不断言重复计数,实际重复经断言消息留痕。
     */
    @Test
    void crashInjectionGuardPinsUnoutputTxAndRestartResendsWholeTx() throws Exception {
        createFixture();

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockedStarted = new CountDownLatch(1);
        List<SourceRecord> sink = new CopyOnWriteArrayList<>();
        var config = withSmallQueue(baseConfig(SLOT, "pub_msg_it", pipeDir.resolve("engine-a")))
                .with("slot.messages", true).build();
        Map<Integer, String> expected = new LinkedHashMap<>();
        try {
            startBlockingEngine(PostgresStreamConnector.class, config,
                    blockingConsumerAt(BLOCK_AFTER, release, blockedStarted, sink));
            StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

            // 暖场 T0:3 条记录放行(前沿 = T0.endLsn > 0,此后 T 的输出阻塞才冻结得住确认)
            expected.put(0, "warm");
            StreamPgTestEnv.execSql("INSERT INTO " + TABLE + " VALUES (0, 'warm')");
            await("T0 三条记录先放行").atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                    .until(() -> sink.size() == WARMUP_RECORDS);

            // 双侧断言的下界:此后 T 的全部 WAL 记录(含 commit)必在其后
            long warmBoundary = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_insert_lsn()");

            // 大事务 T:40 行单事务提交——小队列装满后输出路径停摆,T 滞留未输出完(状态②)
            expected.putAll(insertCommittedTx(GUARD_ID_FROM, GUARD_ROWS, "guard"));
            assertTrue(blockedStarted.await(20, TimeUnit.SECONDS),
                    "T 的第 6 条记录(BEGIN+前 2 行)未到达——输出路径未进入阻塞(20s)");
            assertEquals(BLOCK_AFTER + 1, sink.size(),
                    "停机前部分输出形态:T 的 BEGIN+2 行应在下游(park 在第 6 条): " + describe(sink));

            // 双侧断言的上界锚:COMMIT 之后、心跳之前的 WAL 插入位 = commit 记录结束位
            // (诊断面:等值断言失败时对照 confirmed 是否被抬到此位之上——部分推进形态的去向)
            long commitEndAnchor = StreamPgTestEnv.lsnOf("SELECT pg_current_wal_insert_lsn()");

            // 非事务心跳:LSN 在 T 的 commit 之后——护栏可见性的激励(reader 即时推进路径)
            StreamPgTestEnv.execSql("SELECT pg_logical_emit_message(false, 'heartbeat', 'crash-guard')");

            // 先等 confirmed 采纳到暖场边界(T0 输出后的稳态:前沿 = T0.endLsn = 暖场边界,
            // 反馈周期 1s,等待期远大于它)——随后越过完整反馈周期再钉死<b>等值</b>:
            // 护栏全有或全无(L8)——存在 pending 桶(T 未输出完)即完全不推进,confirmed_flush
            // 恒等暖场边界。部分推进形态(旧 min(commitLsn) 语义)会在一个反馈周期内把
            // confirmed 抬到 T 的 commit 位(> 暖场边界)→ 等值断言红;直推 msgLsn 形态
            // confirmed 到消息位(≥ commit 结束锚)→ 同红——等值钉死比旧开区间双侧断言更强。
            await("confirmed_flush 先采纳到暖场边界(T0 稳态)")
                    .atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250))
                    .until(() -> StreamPgTestEnv.confirmedFlushLsn(SLOT) == warmBoundary);
            Thread.sleep(3000);   // > 反馈周期(1000ms)×2:给任何"会被抬走"的形态留足暴露窗口
            assertEquals(warmBoundary, StreamPgTestEnv.confirmedFlushLsn(SLOT),
                    "护栏冻结:存在 pending 桶(T 未输出完)confirmed_flush 完全静止"
                            + "(若被抬走即部分推进形态复发;诊断对照 commitEndAnchor=" + commitEndAnchor + ")");

            // crash 注入点:输出中段停机(D7 不排干,已提交未输出事务由复制槽重发)。
            // 稳态断言通过即 confirmed 已落库;记录线程 park 由引擎停机的 interrupt 收敛
            // (1s 压短的 shutdown pause,类 javadoc 接缝适配段第 3 条)
            stopConnector();
        }
        finally {
            release.countDown(); // 兜底放行:断言中途抛出也不留卡死的记录线程拖住清理
        }

        // 同一 offset 文件重启(基座 start 读 target/it-offsets 下的基座路径文件——与自建
        // 引擎同路径,单用例内跨 start/stop 保留;管道换 engine-b 子目录,类 javadoc)
        start(PostgresStreamConnector.class, withSmallQueue(
                baseConfig(SLOT, "pub_msg_it", pipeDir.resolve("engine-b")))
                .with("slot.messages", true).build());
        StreamPgTestEnv.awaitWalsender(SLOT, 20_000);

        // 整事务重发 + 并集断言(状态②):T 全部 40 行必达(尾部不丢)、END 补齐;
        // 停机前已见集(暖场行 + T 的前 2 行)允许重复,Set 口径收敛
        List<SourceRecord> seen = new ArrayList<>();
        await("重启后滞留事务整事务重发(BEGIN+全行+END),并集覆盖期望全集")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    drainArrivedRecords(seen);
                    Set<Integer> arrived = collectValidatedDataIds(seen, expected);
                    Set<Integer> union = new HashSet<>(dataIds(recordsForTopic(sink, TOPIC))); // 停机前已见:{0, 101, 102}
                    union.addAll(arrived);
                    assertEquals(expected.keySet(), union,
                            "并集语义:T 的 40 行必达(尾部不丢),停机前已见行允许重发重复: " + describe(seen));
                    assertTrue(hasEndWithEventCountAtLeast(seen, GUARD_ROWS),
                            "T 应有 END 到达且 event_count>=" + GUARD_ROWS
                                    + "(尾部不丢的边界补齐信号;计数含 offset 在途续算的重复,见用例 javadoc): "
                                    + txTopicSummary(seen) + " || " + describe(seen));
                    assertTrue(hasBegin(seen), "T 重发应含 BEGIN(整事务形态): " + describe(seen));
                });
    }

    /**
     * 以自定义逐条消费者启动<b>本类自建的</b> embedded 引擎(基座 {@code start} 无消费者
     * 注入口,阻塞构造必须落在引擎线程上,与 ReaderUnblockedIT 同款——差异是 offset 文件
     * 取基座同一路径而非 -blocking 后缀分立:本类的重启经基座 {@code start} 读同一文件,
     * "同一 offset 文件重启"的续传语义载体,见 {@link #baseOffsetFilePath})。关键步骤:
     * offset 文件父目录就绪 → 配置叠加引擎五件套(与基座 start 同款:name/connector.class/
     * offset.storage+文件/offset.flush.interval.ms=100)→ {@code EmbeddedEngine.create()}
     * 装配(CompletionCallback 失败打日志 + countDown 完成闩,对应 3.6.1 翻译源的
     * loggingCompletion/assertOnFailure=false 形态;ChangeConsumer 逐条 consumer.accept →
     * markProcessed,批尾 markBatchFinished——阻塞发生在引擎线程的 handleBatch 内,
     * task.poll 随之停摆)→ 专用线程 run。
     * 边界:引擎构造失败原样上抛(装配错误 fail-fast);重复调用未先停属用例编写错误,
     * 旧线程泄漏由 60s 停机断言兜底暴露。
     *
     * @param connectorClass 连接器实现类
     * @param config         连接器配置(引擎面属性由本方法注入)
     * @param consumer       逐条记录消费者(阻塞构造经 {@link #blockingConsumerAt} 生成)
     */
    private void startBlockingEngine(Class<? extends SourceConnector> connectorClass, Configuration config,
            Consumer<SourceRecord> consumer) {
        Path offsetFile = baseOffsetFilePath();
        try {
            Files.createDirectories(offsetFile.getParent());
        }
        catch (IOException e) {
            throw new IllegalStateException("offset 文件父目录创建失败: " + offsetFile.getParent(), e);
        }
        Configuration effective = config.edit()
                .with("name", "stream-it-blocking-engine")
                .with("connector.class", connectorClass.getName())
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetFile.toString())
                .with("offset.flush.interval.ms", 100)
                .build();
        blockingEngineStopped = new CountDownLatch(1);
        blockingEngine = EmbeddedEngine.create()
                .using(effective)
                .using((success, message, error) -> {
                    if (!success) {
                        LOG.error("阻塞形态 embedded 引擎异常停机: {}", message, error);
                    }
                    blockingEngineStopped.countDown();
                })
                .notifying((List<SourceRecord> batch, DebeziumEngine.RecordCommitter<SourceRecord> committer) -> {
                    for (SourceRecord record : batch) {
                        consumer.accept(record);
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .using(getClass().getClassLoader())
                .build();
        blockingEngineThread = new Thread(blockingEngine::run, "stream-it-blocking-engine");
        blockingEngineThread.setDaemon(false);
        blockingEngineThread.start();
    }

    /**
     * 覆写基座停机入口:先收敛本类自建引擎再走基座路径(基座引擎仅在场景 2+3 的重启段
     * start 过;先停自建再停基座,两引擎若有未停均被收敛)。基座清理链
     * ({@code stopEngineAndDropSlot} 的"先停引擎后删槽"次序与 @AfterEach 兜底)全部经
     * 本方法路由,次序语义不变。
     */
    @Override
    protected void stopConnector() {
        stopBlockingEngine();
        super.stopConnector();
    }

    /**
     * 请求自建引擎停机并等待退出:crash 注入形态下记录线程 park 在阻塞消费者内——
     * {@code close()} → {@code stop()} 先置空 runningThread 再等引擎完成闩(已由
     * {@code @BeforeAll} 压短到 1s,默认 5 分钟会使停机调用停滞)后 interrupt 引擎线程,
     * park 的 {@code release.await()} 抛 InterruptedException 收敛退出(1.9.7 sources
     * 实证,类 javadoc 接缝适配段)。引擎从未建(null)为 no-op,已停(闾已开)幂等跳过
     * close;等待语义与基座 {@code stopConnector} 同款(60s 卡死探测 AssertionError)。
     */
    private void stopBlockingEngine() {
        DebeziumEngine<SourceRecord> current = blockingEngine;
        if (current == null) {
            return;
        }
        if (blockingEngineStopped.getCount() > 0) {
            try {
                current.close();
            }
            catch (Exception e) {
                throw new IllegalStateException("blocking engine close failed", e);
            }
        }
        if (blockingEngineThread == null) {
            return;
        }
        try {
            if (!blockingEngineStopped.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("阻塞形态引擎 60s 内未停机(close 未生效或 run 卡死)");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        try {
            blockingEngineThread.join(TimeUnit.SECONDS.toMillis(60));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /**
     * 本类自建引擎的 offset 文件路径:<b>刻意取基座 {@code AbstractStreamIT} 的同一路径</b>
     * (target/it-offsets/ 下按测试类简单名命名)——场景 2+3 的重启经基座 {@code start}
     * 读取的恰是此文件,"同一 offset 文件重启"的续传语义载体(与 ReaderUnblockedIT 的
     * -blocking 后缀分立不同,那是单引擎无重启形态);跨用例残留由基座
     * {@code initializeConnectorTestFramework} 的 @BeforeEach 清场删除(删的正是本路径)。
     * 路径拼法与基座私有 {@code offsetFilePath()} 保持一致,若基座改命名约定须同步此处。
     *
     * @return offset 文件路径(surefire 工作目录即模块 basedir 相对)
     */
    private Path baseOffsetFilePath() {
        return Path.of("target", "it-offsets", getClass().getSimpleName() + ".dat");
    }

    /**
     * 单事务内插入若干小载荷行并提交(普通小事务,走非流式路径——场景 2+3 的大事务 T
     * 构造用:载荷小但行数多,靠记录条数而非数据量装满小队列)。载荷 "前缀-id" 可预期。
     *
     * @param idFrom        起始 id(含),逐行 +1
     * @param rows          行数
     * @param payloadPrefix 载荷前缀(实际载荷 "前缀-id")
     * @return id → 实际插入载荷(记录值断言的期望源,保持插入序)
     * @throws SQLException 插入失败原样上抛
     */
    private Map<Integer, String> insertCommittedTx(int idFrom, int rows, String payloadPrefix) throws SQLException {
        Map<Integer, String> payloads = new LinkedHashMap<>();
        try (Connection c = StreamPgTestEnv.newSqlConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + TABLE + " VALUES (?, ?)")) {
                for (int i = 0; i < rows; i++) {
                    String payload = payloadPrefix + "-" + (idFrom + i);
                    payloads.put(idFrom + i, payload);
                    ps.setInt(1, idFrom + i);
                    ps.setString(2, payload);
                    ps.executeUpdate();
                }
            }
            finally {
                c.commit();
            }
        }
        return payloads;
    }

    /**
     * 从一批数据记录提取 id 集(不校验载荷——停机前已见集的收集面,校验留给
     * {@link #collectValidatedDataIds})。after 为 null 的记录(理论不出现,防御)跳过。
     *
     * @param data 数据 topic 的记录列表
     * @return 出现过的 id 集(重复收敛为单值)
     */
    private static Set<Integer> dataIds(List<SourceRecord> data) {
        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : data) {
            Struct after = ((Struct) r.value()).getStruct("after");
            if (after != null) {
                ids.add(after.getInt32("id"));
            }
        }
        return ids;
    }

    /**
     * 收集并校验:遍历全部到达记录中的数据 topic 记录,断言 op=c、after 非空、id 在期望表内
     * 且 payload 全文相等(重复到达各自独立校验——at-least-once 下同一 id 多条都必须值正确),
     * 返回到达 id 集(重复收敛)。
     *
     * @param records  全部到达记录(跨轮次累计)
     * @param expected id → 期望载荷
     * @return 到达过的 id 集
     */
    private static Set<Integer> collectValidatedDataIds(List<SourceRecord> records, Map<Integer, String> expected) {
        Set<Integer> ids = new HashSet<>();
        for (SourceRecord r : recordsForTopic(records, TOPIC)) {
            Struct value = (Struct) r.value();
            assertEquals("c", value.getString("op"), "应全为 INSERT(op=c)");
            Struct after = value.getStruct("after");
            assertNotNull(after, "INSERT 记录应有 after 结构");
            Integer id = after.getInt32("id");
            assertTrue(expected.containsKey(id), "未知 id 到达: " + id);
            assertEquals(expected.get(id), after.getString("payload"), "payload 应全文相等(id=" + id + ")");
            ids.add(id);
        }
        return ids;
    }

    /**
     * 事务 topic 是否存在 event_count ≥ {@code minCount} 的 END 记录:"尾部不丢/完整
     * 事务边界补齐"的信号——取 ≥ 而非 == 是因为中段停机重启后 END 计数从 offset 装载的
     * 在途计数续算(实测 3+40=43,见用例 javadoc),≥ 保证每行至少计一次。重复到达的
     * 其他事务 END(如重放窗口里的旧事务)不影响命中。status/event_count 字段缺失的
     * 记录(理论不出现)跳过。
     *
     * @param records  全部到达记录
     * @param minCount END 事件计数的下界(数据记录数)
     * @return 存在为 true
     */
    private static boolean hasEndWithEventCountAtLeast(List<SourceRecord> records, long minCount) {
        for (SourceRecord r : recordsForTopic(records, TX_TOPIC)) {
            Struct value = (Struct) r.value();
            if ("END".equals(value.getString("status"))
                    && value.getInt64("event_count") != null
                    && value.getInt64("event_count") >= minCount) {
                return true;
            }
        }
        return false;
    }

    /**
     * 事务 topic 记录的一行摘要(status + event_count + 事务 id)——END 计数断言失败的
     * 诊断面:直接展开 event_count 实际值,不必从 describe 的字段面反推。
     *
     * @param records 全部到达记录
     * @return 事务 topic 每条一行的摘要列表
     */
    private static List<String> txTopicSummary(List<SourceRecord> records) {
        List<String> out = new ArrayList<>();
        for (SourceRecord r : recordsForTopic(records, TX_TOPIC)) {
            Struct value = (Struct) r.value();
            out.add(r.topic() + " tx=" + value.getString("id") + " status=" + value.getString("status")
                    + " event_count=" + value.getInt64("event_count"));
        }
        return out;
    }

    /**
     * 事务 topic 是否存在任意 BEGIN 记录:整事务重发形态的最弱信号(BEGIN 与 END 配对
     * 的完整计数由 {@link #hasEndWithEventCountAtLeast} 承担,此处只排除"只有数据行、无事务
     * 边界"的残段形态)。status 字段缺失的记录(理论不出现)跳过。
     *
     * @param records 全部到达记录
     * @return 存在为 true
     */
    private static boolean hasBegin(List<SourceRecord> records) {
        for (SourceRecord r : recordsForTopic(records, TX_TOPIC)) {
            Struct value = (Struct) r.value();
            if ("BEGIN".equals(value.getString("status"))) {
                return true;
            }
        }
        return false;
    }
}
