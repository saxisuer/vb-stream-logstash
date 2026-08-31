package org.vastdata.vbstream.replication;

import java.util.ArrayList;
import java.util.List;

/**
 * 事件流重组器（2.0 spec §2）：把流式事件攒回整块 {@link Transaction}——测试等价币
 * （既有 {@code List<Transaction>} 断言经它存活）与未来需要整块形态的下游共用。
 *
 * <p>校验语义：流合法性 fail-fast（End 无匹配 Begin、Begin 内嵌 Begin、变更先于 Begin 均
 * {@link IllegalStateException}）；End 对账 {@code emittedChanges <= expectedChanges} 且
 * {@code emittedChanges == 实收条数}——{@code emitted < expected} 合法（aborted 子事务
 * 过滤属正常路径），违约抛 ISE（End 处理时对账，{@link #transactions()} 访问器恒不抛）。
 *
 * <p>线程约束：非线程安全——单 consumer 线程（与流式主契约同线程模型；跨线程消费输出列表
 * 前需自建 happens-before，如组装器 close 的 join）。
 */
public final class TransactionRecorder implements StreamingTransactionListener {

    private final List<Transaction> transactions = new ArrayList<>();
    private TransactionEvent.Begin open;
    private final List<TxChange> changes = new ArrayList<>();

    /**
     * 责任：消费一个流式事件，按三形态分流。关键步骤：Begin 校验无内嵌后开桶清缓冲；
     * TxChange 校验桶已开后攒入；End 校验 xid 匹配与对账（见类 javadoc）通过后以 Begin
     * 元数据 + 攒集变更封箱 Transaction 入产物列表并闭桶。边界：流序违规与对账失败抛 ISE，
     * 未知事件类型（sealed 族之外的不可达形态）防御性抛 ISE。线程：consumer 线程。
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
            if (e.emittedChanges() > open.expectedChanges() || e.emittedChanges() != changes.size()) {
                throw new IllegalStateException("End 对账失败: xid=" + e.xid() + " emitted=" + e.emittedChanges()
                        + " expected=" + open.expectedChanges() + " received=" + changes.size());
            }
            transactions.add(new Transaction(open.xid(), open.kind(), open.gid(), open.commitLsn(),
                    open.endLsn(), open.commitTimestamp(), List.copyOf(changes)));
            open = null;
        } else if (event instanceof TxChange c) {
            if (open == null) {
                throw new IllegalStateException("变更先于 Begin 到达");
            }
            changes.add(c);
        } else {
            throw new IllegalStateException("未知事件类型: " + event.getClass());
        }
    }

    /** 已重组完成的事务列表（按完成序；End 封箱后可见——调用线程内即读即见）。 */
    public List<Transaction> transactions() {
        return transactions;
    }
}
