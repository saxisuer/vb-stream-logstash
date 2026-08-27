package org.vastdata.vbstream.replication;

import java.time.Instant;
import java.util.List;

/**
 * 一个已确认提交的完整事务（不可变原子单元，回调给 {@link TransactionListener}）。
 *
 * @param xid             事务 id：NORMAL 来自 Begin、STREAMED 来自 StreamStart、TWO_PHASE 来自 BeginPrepare/StreamPrepare
 * @param kind            事务形态
 * @param gid             两阶段事务的全局 id（非 null 当且仅当 kind=TWO_PHASE），其余 null
 * @param commitLsn       提交记录 LSN（Commit/StreamCommit/CommitPrepared 的对应字段）
 * @param endLsn          提交结束 LSN
 * @param commitTimestamp 提交时间戳
 * @param changes         事务内变更，按协议到达顺序
 */
public record Transaction(long xid, TransactionKind kind, String gid,
                          long commitLsn, long endLsn, Instant commitTimestamp,
                          List<TxChange> changes) {

    /** 防御性拷贝：changes 收集为不可变 List，回调后调用方持有的源缓冲不再影响本对象；changes 为 null 或含 null 元素时抛 NullPointerException（List.copyOf 行为）。 */
    public Transaction {
        changes = List.copyOf(changes);
    }
}
