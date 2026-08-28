package org.vastdata.vbstream.replication;

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * 事务组装桶的存储单元（spec §4）：一条**可直接回放**的 pgoutput 原始消息字节，外加定位元数据。
 *
 * <p>它是整个混合内存/溢写设计的支点——桶里存的不是解码后的 TxChange 对象，而是原始字节。
 * 这样 MEMORY 切到 SPILLED 就只是纯字节转储（加 {@link SpoolFrame} 信封帧直接写进
 * Chronicle Queue），自始至终只有一种存储表示。
 *
 * @param payload   完整的单条消息字节：含类型字节，流式块内的消息还带 Int32 xid 前缀。
 *                  消费契约：无前缀的单元用 {@code decodeSingle(wrap(payload), false)} 解码；
 *                  有前缀的用 {@code decodeSingle(wrap(payload), true)}，且解析出的前缀值
 *                  必须等于 streamXid
 * @param seq       组装器分配的全局单调序号（每条消息一个，控制消息与 Relation 也占号），
 *                  供 Relation 版本日志按 asOf 取"变更时刻"的版本
 * @param streamXid 所属（子）事务的 xid：有值表示这条消息在流式块内（payload 带 4 字节前缀），
 *                  为空表示在块外；取值范围是无符号 Int32（0..4294967295），超出会被
 *                  {@link SpoolFrame#frame} 拒绝
 */
public record PayloadUnit(byte[] payload, long seq, OptionalLong streamXid) {

    /**
     * 构造时的归一与校验：payload 为 null 直接 NPE；streamXid 为 null 归一成 empty（组装器可能
     * 传 null，与 RowChange 的宽容约定一致）。payload 数组按引用共享、不做拷贝（溢写热路径要
     * 避免双倍拷贝），所以调用方构造之后不得再改写这个数组。
     */
    public PayloadUnit {
        Objects.requireNonNull(payload, "payload 不能为 null");
        streamXid = streamXid == null ? OptionalLong.empty() : streamXid;
    }

    /**
     * payload 为 byte[] 组件，需值相等语义（record 默认 equals 对数组退化为引用相等，round-trip
     * 断言与桶内容比对会假阴性），故显式 override 为 {@link Arrays#equals(byte[], byte[])}。
     */
    @Override
    public boolean equals(Object o) {
        return o == this || o instanceof PayloadUnit other
                && seq == other.seq
                && streamXid.equals(other.streamXid)
                && Arrays.equals(payload, other.payload);
    }

    /** 与 {@link #equals} 配套的值语义 hashCode（{@link Arrays#hashCode(byte[])}）。 */
    @Override
    public int hashCode() {
        int result = Long.hashCode(seq);
        result = 31 * result + streamXid.hashCode();
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
