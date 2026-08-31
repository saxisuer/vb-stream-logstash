package org.vastdata.vbstream.replication;

/**
 * 桶生命周期状态（1.7 设计 §3.1）：{@link TxBuffer#state} 的取值域。写侧归属按状态划分——
 * reader 只写 LIVE（记账期）并把桶推进到 HANDED_OFF（交接即冻结），HANDED_OFF 之后的两个状态
 * 由 consumer 线程独占写入；跨线程可见性由 {@code TxBuffer.state} 的 volatile 保证。
 */
enum BucketState {
    /** reader 记账中（含 2PC 挂起池里等待 COMMIT/ROLLBACK PREPARED 的桶）——桶字段只有 reader 线程触碰。 */
    LIVE,
    /** 已交接（快照与封箱元数据冻结、入交接队列），待 consumer 回放——firstIndex 仍钉住 CQ 删除低水位。 */
    HANDED_OFF,
    /** consumer 回放输出中——管道段仍会被 readRange 读回，低水位维度与 HANDED_OFF 同（不可回收）。 */
    OUTPUTTING,
    /** 输出完成——桶可从交接记账中清理，管道段不再约束删除低水位（低于水位的滚动文件可删）。 */
    DONE
}
