package org.vastdata.vbstream.replication;

import java.util.OptionalLong;

/**
 * 事务内一条变更的密封基接口。实现自带 streamXid 组件（与接口方法同名，record 自动实现）。
 */
public sealed interface TxChange permits RowChange, TruncateChange, MsgChange {

    /**
     * 该变更所属（子）事务的 xid。
     *
     * <p>流式块内非空：DML/Truncate 消息的 xid 前缀 = 产生变更的（子）事务 xid，
     * Message 的前缀 = 顶层 xid（spec 附录 B.5）；非流式块内的变更恒为 empty。
     * 供 StreamAbort(sub) 时按子事务剔除变更、下游追溯子事务归属。
     *
     * @return （子）事务 xid；非流式变更返回 empty
     */
    OptionalLong streamXid();
}
