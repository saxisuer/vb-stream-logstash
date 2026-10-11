package org.vastdata.vbstream.walsource.receive;

import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

/**
 * openGauss 档 WAL 字节源（推模式）：start 后在源线程回调字节块（携带"本块末字节之后的
 * 绝对 LSN"锚——与 WalStreamWalker.feed(chunkEndLsn, bytes) 对齐），stop 幂等。
 * 两实现：FileWalSource（sidecar 目录 tail）/ DirectWalSource（Task 5）。
 */
public interface WalByteSource extends AutoCloseable {
    /** 起流：chunk 回调在源线程同步执行；锚语义同 walker.feed。 */
    void start(LongConsumer chunkEndLsnSink, BiConsumer<Long, byte[]> chunkSink);
    /** 幂等停机（managed 模式含子进程终止）。 */
    void stop();
    /** 消费前沿（已交付字节末尾的 LSN；未 start 为 0）。 */
    long consumedLsn();
}
