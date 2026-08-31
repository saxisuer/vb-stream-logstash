package org.vastdata.vbstream.it;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.replication.StreamingToBlockAdapter;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.PipeConfig;
import org.vastdata.vbstream.replication.ReplicationConfig;
import org.vastdata.vbstream.replication.Transaction;
import org.vastdata.vbstream.replication.TransactionAssembler;
import org.vastdata.vbstream.replication.VersionedRelationRegistry;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 1.7 解耦头名验收（设计 §9.3）：consumer 阻塞在输出回调期间，reader 持续从复制流接收消息——
 * 这是"读不被输出阻塞"这一里程碑目标本身的端到端验证。构造：**异步**组装器（真实双线程）+
 * onTransaction 内 await latch 的阻塞 listener（2.0 起组装器回调为流式事件——经
 * {@link StreamingToBlockAdapter} 重组整块后进入阻塞块，阻塞点即 End 封箱回调，consumer 线程
 * 同样停摆，验收语义不变）；reader 线程（pgoutput-reader）的 raw 回调先计数
 * 再喂组装器；阻塞窗口内继续写入并断言接收计数增长；放行后等 reader 追平，再按 Main 关闭次序
 * （会话 → 组装器毒丸排干）收尾，断言输出事务数 == 提交数（排干不丢不重）。
 * 夹具约定：独立槽 {@code reader_unblocked} + 前后清删（it 包习语）；专用表/publication 先于
 * 建槽建立（DDL 不产生解码输出，避免混入断言计数）；管道目录 target/reader-unblocked-pipe
 * （MessagePipe wipe-on-open 自清）。需要本机 Docker（PgTestEnv 单例容器）。
 */
class ReaderUnblockedTest {

    /** 本测试类专用复制槽名：@BeforeEach 清残留与 @AfterEach drop 统一引用。 */
    private static final String SLOT = "reader_unblocked";

    /** 本用例提交的事务总数（阻塞前后各写 5 个 autocommit 单语句事务）。 */
    private static final int COMMITTED_TXNS = 10;

    /**
     * 每用例前清残留槽：上次运行可能异常退出留下同名槽，ensureSlot 会复用旧槽并从其
     * confirmed_flush_lsn 续传，静默吞掉先于建流写入的事务使计数断言失真。dropSlotQuietly 幂等。
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
     * consumer 阻塞期间 reader 持续接收（头名断言）+ 放行后排干输出全量（排干契约）。
     * 关键步骤：异步组装器起 transaction-consumer 线程 → reader 线程内 session.run 双参重载
     * （raw 先计数再喂组装器，反馈按前沿封顶）→ 先写 5 个事务并等首个输出到达（consumer 已
     * 阻塞在第一个事务回调里）→ 阻塞期间再写 5 个事务，轮询断言接收计数严格增长（若读被输出
     * 阻塞，计数停在阻塞点，本断言必红——非恒真）→ 放行 latch，轮询等全部 10 个事务输出
     * （reader 追平 + consumer 排干）→ finally 按 Main 次序收尾：先关会话（run 循环 ≤100ms
     * 经 isClosed 退出）→ join reader（消除 onRaw 与组装器 close 的并发窗口——close 契约要求
     * 不可与 onRaw 并发）→ 组装器毒丸排干后关管道。
     * 边界：reader 线程的 run 异常（close 触发的断连）被捕获吞掉属预期；阻塞回调设 20s latch
     * 超时上限，防异常路径下 consumer 永久卡死拖住 close 的 join；输出列表用
     * CopyOnWriteArrayList（consumer 线程写、测试线程轮询读）。
     */
    @Test
    void readerContinuesWhileConsumerBlocked() throws Exception {
        // 夹具先于建槽：DDL（建表/建 publication）不产生解码输出，10 个事务即全部输出
        PgTestEnv.execSql(
                "CREATE TABLE IF NOT EXISTS t_unblock(id int, v text)",
                "DROP PUBLICATION IF EXISTS pub_unblock",
                "CREATE PUBLICATION pub_unblock FOR TABLE t_unblock",
                "TRUNCATE t_unblock");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstOutput = new CountDownLatch(1);
        AtomicLong frontier = new AtomicLong();
        AtomicLong received = new AtomicLong();
        List<Transaction> out = new CopyOnWriteArrayList<>();
        ReplicationConfig config = PgTestEnv.newConfig(SLOT, "pub_unblock");
        try (PgReplicationSession session = new PgReplicationSession(config)) {
            session.open();
            session.ensureSlot();
            session.start();
            TransactionAssembler assembler = new TransactionAssembler(new StreamingToBlockAdapter(t -> {
                out.add(t);
                firstOutput.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }), config.streamingMode(), new VersionedRelationRegistry(),
                    new PipeConfig(Path.of("target/reader-unblocked-pipe"), LegacyRollCycles.MINUTELY),
                    (msg, view) -> { }, frontier, () -> { });
            Thread reader = new Thread(() -> {
                try {
                    session.run(raw -> {
                        received.incrementAndGet();
                        assembler.onRaw(raw);
                    }, frontier::get);
                } catch (Exception e) {
                    // 会话 close 触发的断连走这里，属预期（isClosed 守卫抛 SQLException 退出）
                }
            }, "pgoutput-reader");
            reader.setDaemon(true);
            reader.start();
            try {
                for (int i = 0; i < 5; i++) {
                    PgTestEnv.execSql("BEGIN; INSERT INTO t_unblock VALUES (" + i + ", 'x'); COMMIT;");
                }
                assertTrue(firstOutput.await(10, TimeUnit.SECONDS),
                        "首个事务 10s 未输出（consumer 未进入阻塞回调）");
                long atBlock = received.get();
                for (int i = 5; i < 10; i++) {                          // 阻塞期间继续写 5 个事务
                    PgTestEnv.execSql("BEGIN; INSERT INTO t_unblock VALUES (" + i + ", 'y'); COMMIT;");
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (received.get() <= atBlock && System.nanoTime() < deadline) {
                    Thread.sleep(50);
                }
                assertTrue(received.get() > atBlock,
                        "consumer 阻塞期间 reader 未继续接收（读被输出阻塞）: received="
                                + received.get() + " atBlock=" + atBlock);
                release.countDown();
                // 放行后等 reader 追平 + consumer 排干：全部已提交事务输出（轮询而非裸 sleep，
                // 慢机容忍；超时则收尾后的计数断言以明确消息失败）
                long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (out.size() < COMMITTED_TXNS && System.nanoTime() < drainDeadline) {
                    Thread.sleep(50);
                }
            } finally {
                session.close();        // 先关会话使 run 循环退出（外层 TWR 再关一次为幂等兜底）
                try {
                    reader.join(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                assembler.close();      // 再毒丸排干（已提交未输出的事务不丢）后关管道
            }
            assertEquals(COMMITTED_TXNS, out.size(),
                    "排干后应输出全部已提交事务（一个不少、一个不多）: out=" + out.size()
                            + " received=" + received.get());
        }
    }
}
