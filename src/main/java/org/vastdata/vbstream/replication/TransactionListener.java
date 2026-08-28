package org.vastdata.vbstream.replication;

/**
 * 事务消费契约：组装完成的原子事务到达即回调。
 *
 * <p>线程约束：调用线程 = consumer 线程（异步装配——Main 形态为 transaction-consumer 线程；
 * 同步测试形态为调用方线程）。1.7 解耦后回调耗时不再拖慢读取路径（回放已在 consumer 线程），
 * 但仍应快速返回或自行转交——拖长会积压交接队列、推迟输出前沿与 LSN 反馈位点推进。
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
