package org.vastdata.vbstream.replication;

/**
 * 整块事务消费契约（1.7 契约保留改名，2.0 起非默认——{@code vb.output.mode=block} 时经
 * {@link StreamingToBlockAdapter} 启用）。原子性在 block 模式下保留：适配器攒齐才转发，
 * 中途失败下游零输出（1.7 原子交付语义的逃生门）。
 *
 * <p>线程约束：调用线程 = consumer 线程（异步装配——Main 形态为 transaction-consumer 线程；
 * 同步测试形态为调用方线程），与 {@link StreamingTransactionListener} 一致。
 */
@FunctionalInterface
public interface BlockTransactionListener {

    /**
     * 收到一个已确认提交的完整事务（BLOCK 模式：适配器 End 重组后转发；ROLLBACK 路径不回调）。
     *
     * @param transaction 不可变事务单元
     */
    void onTransaction(Transaction transaction);
}
