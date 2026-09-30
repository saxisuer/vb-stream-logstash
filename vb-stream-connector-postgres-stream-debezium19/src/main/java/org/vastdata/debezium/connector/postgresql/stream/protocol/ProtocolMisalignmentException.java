package org.vastdata.debezium.connector.postgresql.stream.protocol;

/**
 * 消息解析结束后仍剩余字节——字段序列与协议不符，必须立即暴露以防后续消息错位。
 * 引擎侧同名异常的 1:1 重写（构造器签名与消息格式一致，含"剩余 N 字节"诊断）。
 * RuntimeException（fail-fast）：不做恢复尝试，直接终止该复制流的解码。
 */
public final class ProtocolMisalignmentException extends RuntimeException {

    /**
     * 以消息类型字节与剩余字节数构造，消息文本携带十六进制类型与剩余量便于定位错位现场。
     *
     * @param type 触发校验的消息类型字节（显示为字符与 0xXX 两种形态）
     * @param leftover 解析完成后仍剩余的字节数（调用方保证 &gt;0 才抛）
     */
    public ProtocolMisalignmentException(byte type, int leftover) {
        super("消息 '%s' (0x%02X) 解析后剩余 %d 字节，字段布局与协议不符"
                .formatted((char) type, type, leftover));
    }
}
