package org.vastdata.vbstream.replication;

/** 事务形态：普通（Begin..Commit）、流式大事务（StreamStart..StreamCommit）、两阶段（BeginPrepare/StreamPrepare 后经 CommitPrepared 确认）。 */
public enum TransactionKind {
    /** 普通事务：变更整体缓冲，Commit 后一次输出。 */
    NORMAL,
    /** 流式大事务：越过 logical_decoding_work_mem 被驱逐流式，StreamCommit 后一次输出。 */
    STREAMED,
    /** 两阶段提交：PREPARE 后挂起，COMMIT PREPARED 才输出（ROLLBACK PREPARED 丢弃）。 */
    TWO_PHASE
}
