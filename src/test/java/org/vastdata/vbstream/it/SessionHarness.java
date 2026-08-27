package org.vastdata.vbstream.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.time.Duration;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** 在守护线程跑复制会话并录制消息，直到满足停止条件或超时。 */
public final class SessionHarness implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHarness.class);

    private final PgReplicationSession session;
    private final List<PgOutputMessage> messages = new CopyOnWriteArrayList<>();
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile Exception failure;

    private SessionHarness(PgReplicationSession session, Predicate<PgOutputMessage> stopCondition) {
        this.session = session;
        Thread worker = new Thread(() -> {
            try {
                session.run((msg, registry) -> {
                    messages.add(msg);
                    if (stopCondition.test(msg)) {
                        done.countDown();
                    }
                });
            } catch (Exception e) {
                failure = e;
                done.countDown();
            }
        }, "pgoutput-reader");
        worker.setDaemon(true);
        worker.start();
    }

    public static SessionHarness start(ReplicationConfig config,
                                       Predicate<PgOutputMessage> stopCondition) throws Exception {
        PgReplicationSession session = new PgReplicationSession(config);
        try {
            session.open();
            session.ensureSlot();
            session.start();
        } catch (Exception e) {
            session.close(); // 防中途失败泄漏连接（Task 10 审查修正）
            throw e;
        }
        LOG.info("会话 harness 已启动: 槽={}", config.slotName());
        return new SessionHarness(session, stopCondition);
    }

    public List<PgOutputMessage> messages() {
        return messages;
    }

    /**
     * 等待停止条件达成（或会话线程失败）。
     * 超时抛 AssertionError：消息带录制条数与类型直方图——流式用例单条消息载荷可达 16KB，
     * 逐消息 toString 会让失败输出膨胀到 MB 级，直方图既能看出收到了多少，又能直接定位
     * "未收到某类消息"的失败模式；会话线程的异常作为原因上抛。
     */
    public void awaitTermination(Duration timeout) throws InterruptedException {
        if (!done.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new AssertionError("等待复制消息超时，已收到 " + messages.size() + " 条，类型分布: "
                    + histogram());
        }
        if (failure != null) {
            throw new AssertionError("复制会话异常", failure);
        }
    }

    /**
     * 录制消息的类型直方图（类名 → 计数，TreeMap 按类名排序）。
     * 超时诊断专用：避免逐消息 toString 的大载荷爆炸，且比原始列表更直接暴露
     * "收到一堆 Insert 但没有 StreamStart/Commit"之类的分布性失败。
     */
    private String histogram() {
        return messages.stream()
                .collect(Collectors.groupingBy(
                        m -> m.getClass().getSimpleName(),
                        TreeMap::new,
                        Collectors.counting()))
                .toString();
    }

    @Override
    public void close() {
        session.close();
        LOG.info("会话 harness 已关闭: 槽={} 共录制 {} 条消息", session.config().slotName(), messages.size());
    }
}
