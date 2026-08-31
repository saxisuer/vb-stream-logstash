package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 读取循环纯函数单测：capFeedback（前沿封顶）与 drainPending（缓冲取尽）。
 * drainPending 是 2026-08-31 吞吐冒烟踩坑的修复锚点——旧形态每轮只取一条消息 + 固定
 * 100ms sleep（上限 ~10 msg/s，大事务被拖到分钟级），drain 语义由本类经 fake 流锚定：
 * 一次调用取尽当前缓冲的全部消息，节拍不再与消息条数线性耦合。
 */
class PgReplicationSessionTest {

    /** 前沿 ≤0（尚未有任何事务输出，含负数防御值）不封顶：反馈值原样返回已收到的 LSN。 */
    @Test
    void zeroOrNegativeFrontierMeansNoCap() {
        assertEquals(500L, PgReplicationSession.capFeedback(500L, 0L));
        assertEquals(500L, PgReplicationSession.capFeedback(500L, -1L));
    }

    /** 正前沿取 min 封顶；前沿不会超过已收到（防御性取 min，超过时返回已收到值——不得确认未收到的位点）。 */
    @Test
    void positiveFrontierCapsToMinimum() {
        assertEquals(300L, PgReplicationSession.capFeedback(500L, 300L));
        assertEquals(300L, PgReplicationSession.capFeedback(300L, 500L));  // 前沿不会超过已收到，防御性取 min
    }

    /**
     * drainPending 一次调用取尽缓冲全部消息：500 条按序逐条回调、内容逐条等值、返回 true——
     * 调用方一轮即可搬空积压，不被"每条一拍"的节拍钉死。
     */
    @Test
    void drainPendingTakesAllBufferedMessagesInOneCall() throws Exception {
        FakeReplicationStream stream = new FakeReplicationStream();
        List<byte[]> received = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            stream.pending.add(ByteBuffer.wrap(("msg-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        boolean any = PgReplicationSession.drainPending(stream, received::add);
        assertTrue(any, "有缓冲消息时必须返回 true");
        assertEquals(500, received.size(), "一次调用必须取尽全部缓冲消息");
        for (int i = 0; i < 500; i++) {
            assertEquals("msg-" + i, new String(received.get(i), StandardCharsets.UTF_8), "第 " + i + " 条内容/顺序不符");
        }
    }

    /** 空缓冲一轮：返回 false、零回调——调用方据此判定空转并进入 sleep 间歇。 */
    @Test
    void drainPendingReturnsFalseWhenNothingBuffered() throws Exception {
        FakeReplicationStream stream = new FakeReplicationStream();
        List<byte[]> received = new ArrayList<>();
        assertFalse(PgReplicationSession.drainPending(stream, received::add), "空缓冲必须返回 false");
        assertTrue(received.isEmpty(), "空缓冲不得回调");
    }

    /** remaining()==0 的零载荷防御跳过：不回调但同样被消费掉，drain 继续取后续真实消息并正常终止。 */
    @Test
    void drainPendingSkipsZeroLengthPayload() throws Exception {
        FakeReplicationStream stream = new FakeReplicationStream();
        stream.pending.add(ByteBuffer.wrap(new byte[0]));
        stream.pending.add(ByteBuffer.wrap(new byte[] {1, 2}));
        List<byte[]> received = new ArrayList<>();
        assertTrue(PgReplicationSession.drainPending(stream, received::add));
        assertEquals(1, received.size(), "零载荷必须被跳过、真实消息必须到达");
        assertArrayEquals(new byte[] {1, 2}, received.get(0));
    }

    /**
     * 测试用假复制流：预置消息队列供 readPending 按序消费、取尽返回 null（非阻塞语义），
     * 其余接口方法与 drainPending 无关，空实现/抛异常即可。仅供 PgReplicationSessionTest
     * 同包直连，不模拟网络与协议。
     */
    private static final class FakeReplicationStream implements PGReplicationStream {

        private final Queue<ByteBuffer> pending = new ArrayDeque<>();

        @Override
        public ByteBuffer readPending() {
            return pending.poll();
        }

        @Override
        public ByteBuffer read() {
            throw new UnsupportedOperationException("drainPending 只走 readPending，阻塞读不参与测试");
        }

        @Override
        public LogSequenceNumber getLastReceiveLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public LogSequenceNumber getLastFlushedLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public LogSequenceNumber getLastAppliedLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public void setFlushedLSN(LogSequenceNumber flushed) {
            // drainPending 不触碰反馈状态，空实现
        }

        @Override
        public void setAppliedLSN(LogSequenceNumber applied) {
            // 同上
        }

        @Override
        public void forceUpdateStatus() {
            // 同上
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            // 测试不关流
        }
    }
}
