package org.vastdata.vbstream.replication;

/**
 * 事务消费契约：组装完成的原子事务到达即回调。
 *
 * <p>线程约束：调用线程 = TransactionAssembler 所在的 run 循环线程（同步执行，
 * 回调耗时直接拖慢消息循环与 LSN 反馈，实现方应快速返回或自行转交）。
 */
@FunctionalInterface
public interface TransactionListener {

    /**
     * 收到一个已确认提交的完整事务。
     *
     * @param transaction 不可变事务单元；ROLLBACK 路径不会回调
     */
    void onTransaction(Transaction transaction);
}
