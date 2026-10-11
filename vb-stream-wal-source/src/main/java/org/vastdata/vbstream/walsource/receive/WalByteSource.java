package org.vastdata.vbstream.walsource.receive;

import java.util.Optional;
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

    /**
     * 终态失败观测面（openGauss M1 Task 5 追加）：源线程因确定性错误（重连耗尽/sink
     * 确定性异常）自行退出后返回根因；运行中/正常停机/未 start 为 empty。宿主应周期
     * 轮询本面，见终态即 fail-fast——源停更而进程空转。默认 empty（实现按需覆写）。
     *
     * @return 终态根因；非终态为 empty
     */
    default Optional<Throwable> terminalFailure() {
        return Optional.empty();
    }
}
