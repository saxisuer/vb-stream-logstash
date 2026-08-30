package org.vastdata.vbstream.replication;

/**
 * 事务输出契约（2.0 流式主形态）：一个事务的事件序列
 * {@code Begin → TxChange* → End} 按序经单回调逐条交付，不再整块封箱
 * （1.7 的 {@code onTransaction(Transaction)} 整块契约保留改名为
 * {@link BlockTransactionListener}，经 {@link StreamingToBlockAdapter} 适配启用，非默认）。
 *
 * <p>契约注记（2.0 设计 §2）：
 * <ul>
 *   <li><b>单回调 = 单一背压点</b>：全部事件经同一个 {@code onEvent(TransactionEvent)} 顺序交付，
 *       下游在回调内的耗时/阻塞天然反压上游（消费器不继续回放下一事件），无需额外流控机制</li>
 *   <li><b>End 返回 = 下游确认完整消费</b>：下游必须在 {@link TransactionEvent.End End} 返回前
 *       完成落盘/投递；End 未达（中途异常/阻塞）则该事务不推进输出前沿，重启后整个事务经复制槽
 *       重发（at-least-once，下游可能见到重复头行）</li>
 * </ul>
 *
 * <p>线程约束：调用线程 = consumer 线程（异步装配——Main 形态为 transaction-consumer 线程；
 * 同步测试形态为调用方线程）。回调耗时只拖慢输出不阻塞读取，但仍应快速返回或自行转交——
 * 拖长会积压交接队列、推迟输出前沿与 LSN 反馈位点推进。
 */
@FunctionalInterface
public interface StreamingTransactionListener {

    /**
     * 收到一个事务输出事件（头 {@link TransactionEvent.Begin Begin} / 逐变更
     * {@link TxChange} / 尾 {@link TransactionEvent.End End}），按序流式回调。
     *
     * @param event 不可变事件值对象；ROLLBACK 路径不产生任何事件
     */
    void onEvent(TransactionEvent event);
}
