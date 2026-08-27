package org.vastdata.vbstream.replication;

import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * 事务组装桶的存储单元（spec §4）：一条**可直接回放**的 pgoutput 原始消息字节 + 定位元数据。
 * 整个混合内存/溢写设计的支点——桶里存的不是解码后的 TxChange 对象，而是原始字节，
 * 使 MEMORY→SPILLED 切换成为纯字节转储（加 {@link SpoolFrame} 信封帧直写 Chronicle Queue），
 * 自始至终只有一种存储表示。
 *
 * @param payload   完整单条消息字节：含类型字节与（流式块内时）其后的 Int32 xid 前缀。消费契约——
 *                  无前缀单元 {@code decodeSingle(ByteBuffer.wrap(payload), false)}；有前缀单元
 *                  {@code decodeSingle(ByteBuffer.wrap(payload), true)} 且解析出的前缀值 == streamXid
 * @param seq       MessageSpool 分配的全局单调序号，供 Relation 版本日志 asOf 取"变更时刻"版本
 * @param streamXid 所属（子）事务 xid：有值即"流式块内"（payload 带 4 字节前缀）、empty 即"块外"；
 *                  值域为无符号 Int32（0..4294967295），超出会被 {@link SpoolFrame#frame} fail-fast
 */
public record PayloadUnit(byte[] payload, long seq, OptionalLong streamXid) {

    /**
     * 归一与 fail-fast：payload 为 null 直接 NPE（帧层无从表示空引用）；streamXid 为 null 归一为
     * empty（组装器可能传 null，与 RowChange 的 null 宽容约定一致）。payload 数组按引用共享不复制
     * （溢写热路径避免双倍拷贝），调用方构造后不得再改写该数组。
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
