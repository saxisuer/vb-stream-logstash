package org.vastdata.debezium.connector.postgresql.stream.protocol;

/**
 * 未知消息类型字节（或 TupleData 未知列种类），fail-fast 不静默跳过。
 * 引擎侧同名异常的 1:1 重写（构造器签名与消息格式一致，含"剩余 N 字节"诊断——
 * 未知类型往往正是错位的首发现场，剩余量帮助判断吃偏了多少）。
 * RuntimeException：不做恢复尝试，直接终止该复制流的解码。
 */
public final class UnknownMessageTypeException extends RuntimeException {

    /**
     * 以未知类型字节与读原语当前状态构造，消息文本携带十六进制类型与剩余字节数。
     *
     * @param type 未识别的类型/种类字节（显示为字符与 0xXX 两种形态）
     * @param reader 当前读原语，仅取其 remaining 作诊断（不持有引用）
     */
    public UnknownMessageTypeException(byte type, WireReader reader) {
        super("未知 pgoutput 类型字节 '%s' (0x%02X)，剩余 %d 字节，可能消息错位"
                .formatted((char) type, type, reader.remaining()));
    }
}
