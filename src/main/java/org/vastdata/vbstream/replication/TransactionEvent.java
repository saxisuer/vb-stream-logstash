package org.vastdata.vbstream.replication;

import java.time.Instant;

/**
 * 事务输出事件族（2.0）：单回调流式交付，替代 1.7 的 {@code onTransaction(Transaction)} 整块契约。
 *
 * <p>职责：定义事务回放期对外交付的三种事件形态——事务头（{@link Begin}）、逐条变更
 * （{@link TxChange}）、事务尾（{@link End}）——回放器逐条解码逐条交付，不再把整事务的
 * 变更累积成 List 后封箱回调，回放期堆峰值从 O(事务大小) 降到 O(单条)。sealed permits
 * 在编译期钉死成员集，下游面向一种事件类型编程（未来 Logstash 集成的契约基座）。
 *
 * <p>契约注记（设计 §2/§3）：
 * <ul>
 *   <li><b>单回调 = 单一背压点</b>：全部事件经同一个 {@code onEvent(TransactionEvent)} 回调
 *       顺序交付，下游在回调内的耗时/阻塞天然反压上游（消费器不继续回放下一事件），
 *       无需额外流控机制</li>
 *   <li><b>End 返回 = 下游确认完整消费</b>：下游必须在 {@link End} 返回前完成落盘/投递；
 *       End 未达（中途异常/阻塞）则该事务不推进输出前沿，重启后整个事务经复制槽重发
 *       （at-least-once，下游可能见到重复头行）</li>
 * </ul>
 *
 * <p>时序保证：一个事务的事件序列恒为 {@code Begin → TxChange* → End}（空事务为
 * {@code Begin → End(0)}，协议合法）；中途失败 fail-fast 截断——已输出条数进 ERROR 日志，
 * End 永不发出。
 *
 * <p>线程约束：三个形态均为不可变值对象，可跨线程持有传递；事件的发出与消费发生在
 * transaction-consumer 线程（同步测试形态即调用线程），见 {@link TransactionListener}。
 */
public sealed interface TransactionEvent permits TransactionEvent.Begin, TransactionEvent.End, TxChange {

    /**
     * 事务头：一个事务交付序列的首个事件，回放开始前发出（下游先见元数据再见数据）。
     *
     * <p>组件语义与 {@link Transaction} 同名组件一致（2.0 起 Transaction 换角色为 block
     * 模式的重组值对象）；{@code expectedChanges} 来自 reader 桶记账的单元计数，是
     * aborted 过滤<b>前</b>的值——流式头行 {@code changes=N} 由此保持 1.7 输出格式。
     *
     * @param xid             事务 id：NORMAL 来自 Begin、STREAMED 来自 StreamStart、TWO_PHASE 来自 BeginPrepare/StreamPrepare
     * @param kind            事务形态
     * @param gid             两阶段事务的全局 id（非 null 当且仅当 kind=TWO_PHASE），其余 null
     * @param commitLsn       提交记录 LSN（Commit/StreamCommit/CommitPrepared 的对应字段）
     * @param endLsn          提交结束 LSN（输出前沿按此推进）
     * @param commitTimestamp 提交时间戳
     * @param expectedChanges 预期变更条数（aborted 过滤前）；实际交付数可能小于此值，见 {@link End}
     */
    record Begin(long xid, TransactionKind kind, String gid, long commitLsn, long endLsn,
                 Instant commitTimestamp, long expectedChanges) implements TransactionEvent { }

    /**
     * 事务尾：全部变更交付完后发出，标志该事务交付序列终结。
     *
     * <p>{@code emittedChanges} 为实际交付数（aborted 子事务过滤<b>后</b>）：
     * {@code emitted < expected} 合法——流式事务的 StreamAbort 子事务剔除属正常路径；
     * {@code emitted > expected} 属记账异常（下游校验应 fail-fast）。End 的返回即下游
     * 对完整消费的确认（见接口级契约注记）。
     *
     * @param xid            事务 id，与同序列 {@link Begin#xid()} 一致
     * @param emittedChanges 实际交付的变更条数（aborted 过滤后）
     */
    record End(long xid, long emittedChanges) implements TransactionEvent { }
}
