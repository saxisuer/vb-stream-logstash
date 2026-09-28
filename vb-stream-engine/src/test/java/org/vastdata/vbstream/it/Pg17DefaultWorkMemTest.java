package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleValue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PG 17 出厂默认 logical_decoding_work_mem=64MB 档的流式触发与子事务回滚实证（手动档，
 * {@code -Dvb.it.pg17.defaultmem=true} 才运行——单次 ~1-2 分钟且需额外拉起一个 postgres:17
 * 容器 {@link Pg17DefaultMemTestEnv}，不进常规回归）。现有 {@link Pg17CompatTest} 的流式场景
 * 全部在 64kB 压低阈值下构造，本组回答"不调参的默认档，78MB 大事务能否照常触发流式驱逐、
 * 已流式子事务回滚是否仍发 StreamAbort（含 parallel 附加字段）"。断言面与 Pg17CompatTest
 * 同一定位：纯协议层（SessionHarness 录制消息的存在性/计数），子事务剔除属组装器职责、
 * 已在 PG 18 侧 TransactionAssemblyTest/DecoupledPipelineTest 覆盖。
 */
@EnabledIfSystemProperty(named = "vb.it.pg17.defaultmem", matches = "true")
class Pg17DefaultWorkMemTest {

    private static final Logger LOG = LoggerFactory.getLogger(Pg17DefaultWorkMemTest.class);

    /** 本组槽名——@AfterAll 统一清理。 */
    private static final String SLOT = "slot17_def";

    /**
     * 不可压缩<b>且不 TOAST</b> 的行载荷：32 个不同 md5 拼接 = 1024 hex 字符 ≈1KB（行总尺寸
     * <2KB TOAST 阈值，行内存储）。这是本场景的关键构造（四轮实测 + debug2 日志定位）：16KB
     * TOAST 载荷会让每行产生 toast chunk 的 partial change，驱逐时刻事务几乎恒带
     * RBTXN_HAS_PARTIAL_CHANGE 标——ReorderBufferCheckMemoryLimit 因此<b>走 spill-to-disk
     * 分支而非 streaming 分支</b>（LargestStreamableTopTXN 跳过 partial 事务），spilled
     * 子事务的补发流撞上 ROLLBACK TO 的 concurrent-abort 检测被中断，StreamAbort 永不
     * 发射；不 TOAST 的行无 partial 标，驱逐回到 streaming 分支。1KB 行的代价是行数放大
     * 16 倍（同等字节量下）。
     */
    private static final String RAND_PAYLOAD =
            "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 32))";

    /** 主段批量参数：70 批 × 1000 行 × 1KB ≈ 70MB——较 64MB 阈值留 9% 余量。 */
    private static final int MAIN_BATCHES = 70;
    private static final int MAIN_ROWS_PER_BATCH = 1000;

    /**
     * 子事务段批量参数：70 批 × 1000 行 ≈ 70MB，与主段同量级——子段须<b>自行</b>越过 64MB 阈值
     * 被 streaming 驱逐下发，其行先发给下游、后被 StreamAbort 通知丢弃（流式子事务剔除的协议
     * 形态；子事务打 RBTXN_IS_STREAMED 标是 abort 通知的前提，打标只发生在驱逐段收尾的
     * TruncateTXN）。
     */
    private static final int SUB_BATCHES = 70;
    private static final int SUB_ROWS_PER_BATCH = 1000;

    /**
     * 驱动段批量参数：66 批 × 1000 行 ≈ 66MB——<b>ROLLBACK TO 之后、COMMIT 之前</b>写入（top 下
     * 新子事务，非被回滚的那个），触发 aborted 标记之后的越限驱逐段。机制：aborted 标记后新解码
     * 的同子事务行不再进 rb，ROLLBACK TO 时 rb 里该子事务未发送的余量行须由下一次驱逐携带发出
     * （先发后弃），段尾打标，DecodeAbort 到达 xl_xact_abort 记录时据标发射 StreamAbort。
     */
    private static final int DRIVE_BATCHES = 66;
    private static final int DRIVE_ROWS_PER_BATCH = 1000;

    /** 批间停顿：给 walsender 解码窗口——walsender 已追平时单语句快写的大事务不触发流式（整段提交后回放）。 */
    private static final long BATCH_PAUSE_MILLIS = 250;

    /** 防漂移双守卫：容器大版本必须真是 17，且 logical_decoding_work_mem 必须是出厂默认 64MB——否则场景跑偏成假验证。 */
    @BeforeAll
    static void containerMustBePg17AtDefaultWorkMem() throws Exception {
        assertEquals(17, Pg17DefaultMemTestEnv.majorVersion(), "容器应为 PostgreSQL 17");
        assertEquals("64MB", Pg17DefaultMemTestEnv.workMemSetting(),
                "logical_decoding_work_mem 应为出厂默认 64MB（本组验证对象）");
    }

    @AfterAll
    static void cleanupSlot() {
        Pg17DefaultMemTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 默认 64MB work_mem 档：~200MB 单事务（1KB 不 TOAST 行 × ~20 万行）触发流式分段
     * （StreamStart/StreamCommit），SAVEPOINT 段回滚产生 StreamAbort 且 parallel 附加字段
     * （abortLsn/abortTimestamp）非 empty——Pg17CompatTest 同名场景的量级放大版（64kB 档
     * 逐行 + 75ms；本档分批 1000 行 + 250ms，20 万行逐行不可行）。断言四层：①StreamStart 存在
     * （默认档确实触发流式——未触发则后续断言全空转，此为前置）；②StreamAbort 存在且两
     * 附加字段非 empty（PG 17 按 parallel 形态附加）；③行数按 id 区间：主段/驱动段（未回滚）
     * 全量到达，被回滚子段"先发后弃"——部分到达（aborted 标记前已驱逐的行照发、标记后未发送
     * 的行不再发送，剔除执行归下游的 StreamAbort 通知）；④尾行 (99999,'tail') 在录制中（回滚后
     * 仍有存活输出）。边界：写入循环的 InterruptedException 直接上抛（测试线程不该被中断）。
     */
    @Test
    void defaultWorkMemStreamsLargeTransactionAndSubtransactionAbortCarriesParallelFields() throws Exception {
        Pg17DefaultMemTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_v17_def(id int PRIMARY KEY, payload text)",
                "DROP PUBLICATION IF EXISTS pub_v17_def",
                "CREATE PUBLICATION pub_v17_def FOR TABLE t_v17_def",
                "TRUNCATE t_v17_def");
        try (SessionHarness harness = SessionHarness.start(
                Pg17DefaultMemTestEnv.newConfig(SLOT, "pub_v17_def"),
                msg -> msg instanceof PgOutputMessage.StreamCommit || msg instanceof PgOutputMessage.Commit)) {
            writeLargeTransactionWithAbortedSubtransaction();
            harness.awaitTermination(Duration.ofSeconds(240)); // 会话/解码异常经 failure 通道在此上抛（总数据 ~156MB，预算放宽）

            // 证据收集（StreamAbort 缺失排障 Phase 1）：消息类型直方图 + 流块内 Insert.streamXid 去重
            //（主段行前缀应为 top xid、子段行应为 sp1 的 subxid——后者缺失即"子段行未被流式下发"的直接证据）
            Map<String, Long> histogram = harness.messages().stream()
                    .collect(Collectors.groupingBy(m -> m.getClass().getSimpleName(), Collectors.counting()));
            Set<Long> streamXids = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.Insert i && i.streamXid().isPresent())
                    .map(m -> ((PgOutputMessage.Insert) m).streamXid().getAsLong())
                    .collect(Collectors.toSet());
            LOG.info("证据直方图: {}", histogram);
            LOG.info("流块内 Insert.streamXid 去重: {}", streamXids);

            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.StreamStart),
                    "78MB 单事务在默认 64MB work_mem 下应触发流式分段");
            PgOutputMessage.StreamAbort abort = harness.messages().stream()
                    .filter(m -> m instanceof PgOutputMessage.StreamAbort)
                    .map(m -> (PgOutputMessage.StreamAbort) m)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("子事务回滚应产生 StreamAbort（未流式的子事务"
                            + "回滚被 PG 静默丢弃，场景将空转）"));
            assertTrue(abort.abortLsn().isPresent(), "parallel 模式 StreamAbort 应附加 abortLsn");
            assertTrue(abort.abortTimestamp().isPresent(), "parallel 模式 StreamAbort 应附加 abortTimestamp");
            assertTrue(harness.messages().stream().anyMatch(m -> m instanceof PgOutputMessage.StreamCommit),
                    "流式大事务应以 StreamCommit 收尾");
            // 行数按 id 区间断言（剔除语义的精确口径）：主段/驱动段未回滚必须全量到达；被回滚子段
            // 是"先发后弃"——aborted 标记前已随驱逐段发出的行到达（>0），标记后未发送的行不再发送
            //（<全量），具体量随时序浮动故只卡区间，剔除的执行归下游（StreamAbort 通知）
            assertEquals(MAIN_BATCHES * MAIN_ROWS_PER_BATCH,
                    countInsertsByIdBetween(harness, 1, 70000), "主段（未回滚）应全量到达");
            long subArrived = countInsertsByIdBetween(harness, 100001, 170000);
            assertTrue(subArrived > 0 && subArrived <= SUB_BATCHES * SUB_ROWS_PER_BATCH,
                    "被回滚子段应部分到达（先发后弃）：实测 " + subArrived + " / " + SUB_BATCHES * SUB_ROWS_PER_BATCH);
            assertEquals(DRIVE_BATCHES * DRIVE_ROWS_PER_BATCH,
                    countInsertsByIdBetween(harness, 200001, 266000), "驱动段（未回滚）应全量到达");
            assertTrue(harness.messages().stream()
                            .filter(m -> m instanceof PgOutputMessage.Insert)
                            .map(m -> (PgOutputMessage.Insert) m)
                            .anyMatch(i -> "tail".equals(((TupleValue.Text) i.newTuple().columns().get(1)).value())),
                    "回滚后尾行 (99999,'tail') 应在录制中");
        }
    }

    /**
     * 在一个显式事务内写入带回滚子事务的大事务：主段分批写 4800 行 × 16KB（越过 64MB 触发流式），
     * SAVEPOINT sp1 内再写 4800 行（子段自行越限、随驱逐段下发），随后<b>等 walsender 发送位点追平
     * 子段末 WAL 位点</b>再 ROLLBACK TO——保证子段全部行已解码进 rb（64MB 大流量下 walsender 被
     * 下游消费速率流控钉住、解码进度落后于写入；不等则子段尾行在 aborted 标记后才被解码、直接
     * 跳过，rb 无余量，StreamAbort 永不发射——三轮实测定位）；再写 4200 行驱动段触发 aborted
     * 标记后的越限驱逐（携带余量行先发后弃、段尾发 StreamAbort，见 DRIVE_BATCHES 注释），补写
     * 尾行 (99999,'tail') 后 commit。id 区间：主段 1..70000、子段 100001..170000、驱动段
     * 200001..266000、尾行 99999（1KB 行放大行数后各段区间重排，尾行 id 须在主段区间外——
     * 旧场景的尾行 id 直接沿用会 PK 冲突，此为实测踩坑修正）。每批间 sleep {@link #BATCH_PAUSE_MILLIS}
     * 让 walsender 有解码窗口（驱逐发生在
     * 事务进行中）。边界：批间 InterruptedException 上抛终止测试（外部中断不该被吞）。
     */
    private static void writeLargeTransactionWithAbortedSubtransaction() throws Exception {
        try (Connection c = Pg17DefaultMemTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
            c.setAutoCommit(false);
            int nextId = 1;
            for (int batch = 0; batch < MAIN_BATCHES; batch++) {
                st.execute(batchInsert(nextId, MAIN_ROWS_PER_BATCH));
                nextId += MAIN_ROWS_PER_BATCH;
                Thread.sleep(BATCH_PAUSE_MILLIS);
            }
            st.execute("SAVEPOINT sp1");
            for (int batch = 0; batch < SUB_BATCHES; batch++) {
                st.execute(batchInsert(100001 + batch * SUB_ROWS_PER_BATCH, SUB_ROWS_PER_BATCH));
                Thread.sleep(BATCH_PAUSE_MILLIS);
            }
            awaitWalSentBeyond(currentWalLsn(), Duration.ofSeconds(120));
            st.execute("ROLLBACK TO SAVEPOINT sp1");
            for (int batch = 0; batch < DRIVE_BATCHES; batch++) {
                st.execute(batchInsert(200001 + batch * DRIVE_ROWS_PER_BATCH, DRIVE_ROWS_PER_BATCH));
                Thread.sleep(BATCH_PAUSE_MILLIS);
            }
            st.execute("INSERT INTO t_v17_def VALUES (99999, 'tail')");
            c.commit();
        }
    }

    /**
     * 解析 pg_lsn 文本形态（如 {@code "0/1A2B3C4D"}，高 32 位/低 32 位十六进制）为无符号 long 位点值
     * ——供 {@link #awaitWalSentBeyond} 的位点比较。
     */
    private static long parseLsn(String lsn) {
        String[] parts = lsn.split("/");
        return (Long.parseUnsignedLong(parts[0], 16) << 32) | Long.parseUnsignedLong(parts[1], 16);
    }

    /** 当前 WAL 末位点（{@code pg_current_wal_lsn()}）——子段写完后的追平目标锚点。 */
    private static long currentWalLsn() throws SQLException {
        try (Connection c = Pg17DefaultMemTestEnv.newSqlConnection();
             java.sql.ResultSet rs = c.createStatement().executeQuery("SELECT pg_current_wal_lsn()::text")) {
            rs.next();
            return parseLsn(rs.getString(1));
        }
    }

    /**
     * 轮询等待 walsender 发送位点（{@code pg_stat_replication} 的 {@code sent_lsn}，发送滞后于解码，
     * 故追平即解码已完成）越过指定位点——测试容器独占，取 max(sent_lsn) 即本复制流。500ms 轮询间隔，
     * 超时抛 AssertionError（fail-fast：追不平意味着会话/消费侧异常，后续断言无意义）。
     */
    private static void awaitWalSentBeyond(long targetLsn, Duration timeout) throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Connection c = Pg17DefaultMemTestEnv.newSqlConnection();
                 java.sql.ResultSet rs = c.createStatement().executeQuery("SELECT max(sent_lsn)::text FROM pg_stat_replication")) {
                if (rs.next() && rs.getString(1) != null && parseLsn(rs.getString(1)) >= targetLsn) {
                    return;
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("等待 walsender 发送位点追平子段末位点超时（" + timeout.toSeconds() + "s）");
    }

    /**
     * 统计录制中 id 落在 [from, to] 闭区间的流式 Insert 行数——按 id 区间核对各段到达量的断言辅助
     * （id 是 Insert 元组第 0 列的文本形态）。遍历整个录制列表，线性复杂度。
     */
    private static long countInsertsByIdBetween(SessionHarness harness, int from, int to) {
        return harness.messages().stream()
                .filter(m -> m instanceof PgOutputMessage.Insert)
                .map(m -> (PgOutputMessage.Insert) m)
                .filter(i -> {
                    int id = Integer.parseInt(((TupleValue.Text) i.newTuple().columns().get(0)).value());
                    return id >= from && id <= to;
                })
                .count();
    }

    /**
     * 拼一条多值 INSERT（[fromId, fromId+rows) 区间连续 id，每行载荷 {@link #RAND_PAYLOAD}）——
     * 分批写入的单批语句。语句文本量级 rows × ~90 字符（1000 行 ≈ 90KB），远低于 PG 语句长度限制。
     */
    private static String batchInsert(int fromId, int rows) {
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                values.append(',');
            }
            values.append('(').append(fromId + i).append(", ").append(RAND_PAYLOAD).append(')');
        }
        return "INSERT INTO t_v17_def VALUES " + values;
    }
}
