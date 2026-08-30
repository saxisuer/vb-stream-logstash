package org.vastdata.vbstream.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 流式→整块输出边界适配器（2.0 spec §2）：{@link TransactionEvent.Begin Begin} 开桶攒
 * {@link TxChange}，{@link TransactionEvent.End End} 封箱转发目标后丢弃；中途异常攒的内容
 * 随失败丢弃，目标零输出（block 模式恢复 1.7 原子交付语义——堆 O(事务)、半截事务永不外泄）。
 *
 * <p>与 {@link TransactionRecorder} 的差异：本类面向**下游转发**（End 即回调目标，事务级
 * 转发后丢弃，不累积历史）；录制器面向**测试断言**（全部产物驻留列表供事后核对）。两者对
 * 同一事件流的整块表达值相等（StreamingToBlockAdapterTest 的等价验收）。
 *
 * <p>边界：End 无匹配 Begin、Begin 内嵌 Begin、变更先于 Begin 均抛 {@link IllegalStateException}
 * （流合法性 fail-fast，原样上抛不转发）。
 *
 * <p>线程约束：非线程安全——consumer 线程单写者（与流式主契约同线程模型）。
 */
public final class StreamingToBlockAdapter implements StreamingTransactionListener {

    private final BlockTransactionListener target;
    private TransactionEvent.Begin open;
    private final List<TxChange> changes = new ArrayList<>();

    /**
     * 构造适配器。
     *
     * @param target 整块消费目标（End 封箱后同步调用，null 抛 NPE）
     */
    public StreamingToBlockAdapter(BlockTransactionListener target) {
        this.target = Objects.requireNonNull(target, "target");
    }

    /**
     * 责任：消费一个流式事件，维护单事务重组缓冲。关键步骤：Begin 校验无内嵌后开桶清缓冲；
     * TxChange 校验桶已开后攒入；End 校验 xid 匹配后以 Begin 元数据 + 攒集变更封箱
     * {@link Transaction} 转发目标并丢弃本事务状态。边界：三类流序违规抛 ISE（不转发）。
     * 线程：consumer 线程。
     */
    @Override
    public void onEvent(TransactionEvent event) {
        if (event instanceof TransactionEvent.Begin b) {
            if (open != null) {
                throw new IllegalStateException("Begin 内嵌 Begin: xid=" + b.xid());
            }
            open = b;
            changes.clear();
        } else if (event instanceof TransactionEvent.End e) {
            if (open == null || e.xid() != open.xid()) {
                throw new IllegalStateException("End 无匹配 Begin: xid=" + e.xid());
            }
            target.onTransaction(new Transaction(open.xid(), open.kind(), open.gid(),
                    open.commitLsn(), open.endLsn(), open.commitTimestamp(), List.copyOf(changes)));
            open = null;
            changes.clear();
        } else if (event instanceof TxChange c) {
            if (open == null) {
                throw new IllegalStateException("变更先于 Begin 到达");
            }
            changes.add(c);
        }
    }
}
