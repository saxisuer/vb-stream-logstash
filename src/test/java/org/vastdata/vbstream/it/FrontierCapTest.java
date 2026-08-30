package org.vastdata.vbstream.it;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.vastdata.vbstream.replication.StreamingToBlockAdapter;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 1.7 反馈语义验收（设计 §9.3/§5）：未输出事务不推进槽 confirmed_flush_lsn，输出后推进。
 * 两段式 + LSN 锚点策略（避免硬编码 T.endLsn）：先写热身事务 T0 正常输出——前沿 &gt;0 是 cap
 * 生效的前提（前沿 0 = 无 cap，首个事务输出前与 1.6 行为一致）；再取
 * {@code pg_current_wal_insert_lsn()} 为 before 锚点、提交目标事务 T（锚点间唯一 WAL 活动，
 * before &lt; T.endLsn ≤ after 恒成立），让 consumer 阻塞在 T 的输出回调里；等 ≥2 个反馈周期
 * （PgTestEnv 反馈 2s → sleep 5s）断言 confirmed ≤ before——封顶钉在 T 之前，严格强于
 * "confirmed &lt; T.endLsn"；放行排干后补一次 WAL 活动并轮询断言 confirmed &gt; before（前沿
 * 解封）。补 WAL 活动的依据：confirmed_flush_lsn 由 walsender 在**解码推进时**（candidate
 * 机制）落库，空闲期不推进（Diag 实证，见根 CLAUDE.md）——纯 sleep 等不来推进。
 * 夹具约定：独立槽 + 前后清删（it 包习语）；专用表/publication 先于建槽建立（DDL 不产生解码
 * 输出，T0/T/T2 即全部输出，阻塞定位用输出序数即可）；管道目录 target/frontier-cap-pipe。
 * 需要本机 Docker（PgTestEnv 单例容器）。
 */
class FrontierCapTest {

    /** 本测试类专用复制槽名：@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "frontier_cap";

    /** 目标事务 T 在输出序中的位置：第 1 个是热身 T0（直通），第 2 个是 T（阻塞），其后直通。 */
    private static final int TARGET_OUTPUT_ORDINAL = 2;

    /**
     * 每用例前清残留槽：上次运行可能异常退出留下同名槽，ensureSlot 复用旧槽从旧
     * confirmed_flush_lsn 续传，锚点语义（T 是阻塞窗口内唯一 WAL 活动）即被破坏。幂等。
     */
    @BeforeEach
    void cleanResidualSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    /** 每用例后清理本测试专用槽：先杀 walsender 再删，避免槽残留跨用例干扰（槽不存在时静默）。 */
    @AfterEach
    void dropSlot() {
        PgTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 未输出事务钉住 confirmed_flush（第一段）+ 输出后越过封顶（第二段）。
     * 关键步骤：异步组装器 + 按"输出序数 == 2 才阻塞"的 listener（2.0 起组装器回调为流式事件——
     * 序数语义是**事务**序数，经 {@link StreamingToBlockAdapter} 重组整块后计数阻塞，否则 Begin/
     * TxChange/End 每事件都会使序数前移；T0 热身直通使前沿 &gt;0、T 阻塞、放行后的 T2 直通）
     * → 热身 T0 后轮询等前沿 &gt;0 → 取 before 锚点、提交 T、取 after
     * → 等 T 到达输出回调（consumer 已阻塞）→ sleep 5s（≥2 个反馈周期，给服务端充足的采纳
     * 窗口后断言"没有推进"）断言 confirmed ≤ before → 放行，轮询等前沿越过 before（T.endLsn
     * &gt; before 恒成立，即 T 已输出完成）→ 写 T2 触发解码推进，轮询断言 confirmed &gt; before。
     * 关键机理：阻塞期间反馈 = min(received, frontier=T0.endLsn ≤ before)——无论服务端何时采纳，
     * 确认值被前沿钉在 before 之前；若无 cap（1.6 行为），T 解码后到达的首个 status 包就会把
     * confirmed 推到 received ≥ T.endLsn，第一段必红（非恒真断言）。
     * 边界：reader 线程的 run 异常（close 触发的断连）捕获吞掉属预期；阻塞回调 20s latch 超时
     * 上限防异常路径拖住 close 的 join；after ≥ before 自洽护栏。
     */
    @Test
    void unflushedTransactionHoldsConfirmedFlush() throws Exception {
        // 夹具先于建槽：DDL 不产生解码输出，输出序数（1=T0、2=T、3=T2）与写入序一一对应
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_cap(id int)",
                "DROP PUBLICATION IF EXISTS pub_cap",
                "CREATE PUBLICATION pub_cap FOR TABLE t_cap",
                "TRUNCATE t_cap");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch targetBlocked = new CountDownLatch(1);
        AtomicLong frontier = new AtomicLong();
        AtomicInteger outputs = new AtomicInteger();
        ReplicationConfig config = PgTestEnv.newConfig(SLOT, "pub_cap");
        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            TransactionAssembler assembler = new TransactionAssembler(new StreamingToBlockAdapter(t -> {
                if (outputs.incrementAndGet() == TARGET_OUTPUT_ORDINAL) {   // T：目标事务，阻塞 consumer
                    targetBlocked.countDown();
                    try {
                        release.await(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }   // T0（热身）与 T2（触发解码推进）直通
            }), config.streamingMode(), new VersionedRelationRegistry(),
                    new PipeConfig(Path.of("target/frontier-cap-pipe"), LegacyRollCycles.MINUTELY),
                    (msg, view) -> { }, frontier, () -> { });
            Thread reader = new Thread(() -> {
                try {
                    session.run(assembler, frontier::get);
                } catch (Exception e) {
                    // 会话 close 触发的断连走这里，属预期（isClosed 守卫抛 SQLException 退出）
                }
            }, "pgoutput-reader");
            reader.setDaemon(true);
            reader.start();
            try {
                // 热身 T0：正常输出使前沿 > 0——cap 生效前提（前沿 0 = 无 cap，设计 §5）
                PgTestEnv.execSql("INSERT INTO t_cap VALUES (0)");
                awaitPredicate(Duration.ofSeconds(10), () -> frontier.get() > 0L,
                        () -> "热身事务 10s 未输出（前沿仍为 0，cap 未激活）: frontier=0x"
                                + Long.toHexString(frontier.get()));

                // 锚点与目标事务：before < T.endLsn ≤ after 恒成立（T 是两锚点间唯一 WAL 活动）
                long before = lsnOf("SELECT pg_current_wal_insert_lsn()");
                PgTestEnv.execSql("BEGIN; INSERT INTO t_cap VALUES (1); COMMIT;");
                long after = lsnOf("SELECT pg_current_wal_insert_lsn()");

                assertTrue(targetBlocked.await(10, TimeUnit.SECONDS),
                        "目标事务 10s 未到达输出回调（consumer 未阻塞）: outputs=" + outputs.get());
                Thread.sleep(5_000);   // ≥ 2 个反馈周期（2s×2）：给服务端充足的采纳窗口

                // 第一段：consumer 阻塞期间，反馈被前沿（=T0.endLsn ≤ before）封顶，确认不得越过
                long confirmedBlocked = confirmedFlushLsn();
                assertTrue(confirmedBlocked <= before,
                        "未输出事务应钉住 confirmed_flush: confirmed=0x" + Long.toHexString(confirmedBlocked)
                                + " before=0x" + Long.toHexString(before));

                release.countDown();
                // 等 T 输出完成：前沿 = T.endLsn > before 恒成立（放行即解封的直接观测）
                awaitPredicate(Duration.ofSeconds(10), () -> frontier.get() > before,
                        () -> "放行后目标事务 10s 未输出完成（前沿未越过 before）: frontier=0x"
                                + Long.toHexString(frontier.get()) + " before=0x" + Long.toHexString(before));

                // 第二段：补一次 WAL 活动触发解码推进（confirmed_flush 落库条件——空闲期不推进），
                // 轮询断言越过封顶（≥ 语义，慢机容忍）
                PgTestEnv.execSql("INSERT INTO t_cap VALUES (2)");
                awaitPredicate(Duration.ofSeconds(10), () -> confirmedFlushLsn() > before,
                        () -> "输出后 confirmed_flush 应越过封顶: confirmed=0x"
                                + Long.toHexString(confirmedFlushLsn()) + " before=0x" + Long.toHexString(before));
                assertTrue(after >= before, "自洽护栏: after=0x" + Long.toHexString(after)
                        + " before=0x" + Long.toHexString(before));
            } finally {
                session.close();        // 先关会话使 run 循环退出（外层 TWR 再关一次为幂等兜底）
                try {
                    reader.join(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                assembler.close();      // 再毒丸排干（T2 已提交未输出也不丢）后关管道
            }
        }
    }

    /** 条件轮询：每 250ms 检查一次，超时抛 AssertionError（消息由 describe 惰性提供）。条件与描述均可抛检查异常。 */
    private interface ThrowingCondition {
        boolean test() throws Exception;
    }

    /** 断言失败消息的惰性提供者（可抛检查异常，如 SQL 查询失败）。 */
    private interface ThrowingDescribe {
        String get() throws Exception;
    }

    /**
     * 轮询等待条件成立（NormalTransactionTest 同款习语）：每 250ms 检查一次，条件不成立且仍在
     * 超时窗口内则继续；超时抛 AssertionError（消息惰性求值——其中可能再查一次当前值辅助定位）。
     * 相比裸 sleep 的优势：慢机上条件早成立即早返回、条件迟成立也不误报，只有真超时才失败。
     */
    private static void awaitPredicate(Duration timeout, ThrowingCondition condition,
                                       ThrowingDescribe describe) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.test()) {
            assertTrue(System.nanoTime() < deadline, describe.get());
            Thread.sleep(250);
        }
    }

    /**
     * 查询单值 LSN 并解析为 long：经 {@link PgTestEnv#newSqlConnection} 执行 SQL，结果以十六进制
     * "X/Y" 字符串形态经 {@link LogSequenceNumber#valueOf(String)} 解析（NormalTransactionTest 的
     * LSN 解析先例同款）。无结果行或值为 null 抛 IllegalStateException（锚点查询必然有值，
     * 缺失即环境异常，fail-fast 不猜测）。
     */
    private static long lsnOf(String sql) throws Exception {
        try (Connection c = PgTestEnv.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next() || rs.getString(1) == null) {
                throw new IllegalStateException("LSN 查询无结果行或为 null: " + sql);
            }
            return LogSequenceNumber.valueOf(rs.getString(1)).asLong();
        }
    }

    /**
     * 当前槽的 confirmed_flush_lsn（"0/0" 经 valueOf 解析为 0）：SQL 函数与槽列查询统一走
     * {@link #lsnOf} 助手。
     */
    private static long confirmedFlushLsn() throws Exception {
        return lsnOf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='" + SLOT + "'");
    }
}
