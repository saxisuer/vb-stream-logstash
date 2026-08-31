package org.vastdata.vbstream.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.replication.DecodedMessageBridge;
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

/** 在守护线程跑复制会话并双轨录制（raw 字节 + 解码消息），直到满足停止条件或超时。 */
public final class SessionHarness implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHarness.class);

    private final PgReplicationSession session;
    private final List<PgOutputMessage> messages = new CopyOnWriteArrayList<>();
    private final List<byte[]> rawMessages = new CopyOnWriteArrayList<>();
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile Exception failure;

    /**
     * 私有构造：绑定会话与停止条件，启动守护读取线程双轨录制。
     * 接线：session 交付 raw（先录 rawMessages）→ 桥解码（decoder/registry 归桥所有）→
     * target 录 decoded 并测停止条件。raw 先于 decoded 入列表，录制中途两列表条数可能
     * 相差正在解码的一条；无解码异常时 close 后两列表等长——解码抛异常的那条 raw 已入列
     * 而对位 decoded 缺席（异常随即成为 failure 终止读取线程），等长性从此不成立。
     * 线程内命中停止条件或会话抛异常都只 countDown latch——异常记入 failure，
     * 由 awaitTermination 统一上抛，不在本方法同步暴露。
     */
    private SessionHarness(PgReplicationSession session, Predicate<PgOutputMessage> stopCondition) {
        this.session = session;
        DecodedMessageBridge bridge = new DecodedMessageBridge((msg, registry) -> {
            messages.add(msg);
            if (stopCondition.test(msg)) {
                done.countDown();
            }
        }, session.config().streamingMode());
        Thread worker = new Thread(() -> {
            try {
                session.run(raw -> {
                    rawMessages.add(raw);
                    bridge.onRaw(raw);
                });
            } catch (Exception e) {
                failure = e;
                done.countDown();
            }
        }, "pgoutput-reader");
        worker.setDaemon(true);
        worker.start();
    }

    /**
     * 建立复制会话（open → ensureSlot → start）并包装为 harness，返回时读取线程已在录制。
     * 契约：任一建流环节失败即关闭已建立的会话（防连接泄漏）后原样上抛，不返回半成品 harness。
     */
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

    /**
     * 已录制消息的实时视图。
     * 契约：停止条件仅 countDown latch——awaitTermination 返回后读取线程仍会持续追加，
     * 直至 close() 才停；返回 CopyOnWriteArrayList，遍历线程安全（迭代为创建时快照），
     * 但要做确定性的全量断言必须先 close() 再读（本测试类"先 close 后断言"的既定顺序，
     * 未来作者新增用例需遵守此顺序约束，否则可能读到仍在增长的列表）。
     */
    public List<PgOutputMessage> messages() {
        return messages;
    }

    /**
     * 已录制 raw 消息字节的实时视图，与 {@link #messages()} 同序一一对应（每条 raw 恰好
     * 是对位解码消息的完整字节）。
     * 契约：同 messages()——确定性全量断言必须先 close() 再读，且 close 后无解码异常时
     * 与 messages() 等长（解码异常会使 raw 侧多出最后一条未解码字节，见构造方法说明）。
     */
    public List<byte[]> rawMessages() {
        return rawMessages;
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

    /**
     * 关闭复制会话：终止读取线程的阻塞读，close 返回后消息列表不再增长，
     * 此后方可对录制流做确定性的断言/离线回放；同时记录录制总数便于日志对账。
     */
    @Override
    public void close() {
        session.close();
        LOG.info("会话 harness 已关闭: 槽={} 共录制 {} 条消息 / {} 条 raw",
                session.config().slotName(), messages.size(), rawMessages.size());
    }
}
