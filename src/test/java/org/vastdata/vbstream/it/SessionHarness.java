package org.vastdata.vbstream.it;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.PgReplicationSession;
import org.vastdata.vbstream.replication.ReplicationConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/** 在守护线程跑复制会话并录制消息，直到满足停止条件或超时。 */
public final class SessionHarness implements AutoCloseable {

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
        return new SessionHarness(session, stopCondition);
    }

    public List<PgOutputMessage> messages() {
        return messages;
    }

    public void awaitTermination(Duration timeout) throws InterruptedException {
        if (!done.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new AssertionError("等待复制消息超时，已收到: " + messages);
        }
        if (failure != null) {
            throw new AssertionError("复制会话异常", failure);
        }
    }

    @Override
    public void close() {
        session.close();
    }
}
