package org.vastdata.debezium.connector.postgresql.stream;

import java.util.Arrays;
import java.util.OptionalLong;

/**
 * pg_logical_emit_message 产生的事务内逻辑消息。引擎
 * {@code org.vastdata.vbstream.replication.MsgChange} 的 1:1 重写(文字参照,非依赖)
 * + <b>seq 偏差组件</b>(见 {@link TxChange#seq()})。
 *
 * @param transactional true=事务性消息(随事务缓冲、提交才输出);false=即时消息
 * @param prefix        消息前缀
 * @param content       消息字节内容
 * @param streamXid     所属(子)事务 xid,见 {@link TxChange#streamXid()}
 * @param seq           消息序号(CQ index),见 {@link TxChange#seq()}——connector 偏差组件
 */
public record MsgChange(boolean transactional, String prefix, byte[] content,
                        OptionalLong streamXid, long seq) implements TxChange {

    /** content 为 byte[] 组件,需值相等语义(record 默认对数组退化为引用相等),故显式 override。 */
    @Override
    public boolean equals(Object o) {
        return o == this || o instanceof MsgChange other
                && transactional == other.transactional
                && prefix.equals(other.prefix)
                && Arrays.equals(content, other.content)
                && streamXid.equals(other.streamXid)
                && seq == other.seq;
    }

    @Override
    public int hashCode() {
        int result = Boolean.hashCode(transactional);
        result = 31 * result + prefix.hashCode();
        result = 31 * result + Arrays.hashCode(content);
        result = 31 * result + streamXid.hashCode();
        result = 31 * result + Long.hashCode(seq);
        return result;
    }
}
