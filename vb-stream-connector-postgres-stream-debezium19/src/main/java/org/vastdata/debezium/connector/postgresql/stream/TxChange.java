package org.vastdata.debezium.connector.postgresql.stream;

import java.util.OptionalLong;

/**
 * 事务内一条变更的密封基接口。实现自带 streamXid 与 seq 组件(与接口方法同名,record 自动实现)。
 * 引擎 {@code org.vastdata.vbstream.replication.TxChange} 的 1:1 重写(文字参照,非依赖)
 * <b>加一个已文档化偏差组件</b>:seq。
 *
 * <p>{@link TransactionEvent} 直接 permits 本接口(不包 Change 壳)——逐条变更即一个交付事件、
 * 零额外分配;自身仍是 sealed(RowChange/TruncateChange/MsgChange),两层 sealed 叠加。
 *
 * <p><b>偏差依据(seq,MS2 设计)</b>:connector 侧下游(Task 7 的
 * {@code DispatcherTransactionListener})需要按 (oid, seq) 从桶快照解析 asOf Table 做版本
 * 安装——引擎把变更时刻的 Relation 快照嵌入变更即自包含,而 Debezium 渲染视图(Table)
 * 在 listener 侧经 {@code RelationSnapshot.require(oid, seq)} 取版,变更必须自带 seq
 * (≡ 该消息的 CQ index,即 append 时的返回值)。
 */
public sealed interface TxChange extends TransactionEvent permits RowChange, TruncateChange, MsgChange {

    /**
     * 该变更所属(子)事务的 xid。
     *
     * <p>流式块内非空:DML/Truncate 消息的 xid 前缀 = 产生变更的(子)事务 xid,
     * Message 的前缀 = 顶层 xid;非流式块内的变更恒为 empty。
     * 供 StreamAbort(sub) 时按子事务剔除变更、下游追溯子事务归属。
     *
     * @return (子)事务 xid;非流式变更返回 empty
     */
    OptionalLong streamXid();

    /**
     * 该变更消息的序号(≡ Chronicle Queue index,即组装器 append 该消息字节时的返回值,
     * 起点随建队列时刻漂移,勿硬编码)。
     *
     * <p>下游以 (relationOid, seq) 为键经桶快照 {@code RelationSnapshot.require} 取
     * <b>变更时刻</b>的表定义(asOf 二分)——connector 相对引擎事件族的已文档化偏差组件。
     *
     * @return 消息序号(单调,同一管道内与消息位置一一对应)
     */
    long seq();
}
