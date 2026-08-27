package org.vastdata.vbstream.replication;

import java.util.Arrays;
import java.util.OptionalLong;

/**
 * pg_logical_emit_message 产生的事务内逻辑消息。
 *
 * @param transactional true=事务性消息（随事务缓冲、提交才输出）；false=即时消息
 * @param prefix        消息前缀
 * @param content       消息字节内容
 * @param streamXid     所属（子）事务 xid，见 {@link TxChange#streamXid()}
 */
public record MsgChange(boolean transactional, String prefix, byte[] content,
                        OptionalLong streamXid) implements TxChange {

    /** content 为 byte[] 组件，需值相等语义（record 默认对数组退化为引用相等），故显式 override。 */
    @Override
    public boolean equals(Object o) {
        return o == this || o instanceof MsgChange other
                && transactional == other.transactional
                && prefix.equals(other.prefix)
                && Arrays.equals(content, other.content)
                && streamXid.equals(other.streamXid);
    }

    @Override
    public int hashCode() {
        int result = Boolean.hashCode(transactional);
        result = 31 * result + prefix.hashCode();
        result = 31 * result + Arrays.hashCode(content);
        result = 31 * result + streamXid.hashCode();
        return result;
    }
}
