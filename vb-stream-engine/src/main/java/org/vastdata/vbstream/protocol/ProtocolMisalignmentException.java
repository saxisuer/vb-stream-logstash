package org.vastdata.vbstream.protocol;

/** 消息解析结束后仍剩余字节——字段序列与协议不符，必须立即暴露以防后续消息错位。 */
public final class ProtocolMisalignmentException extends RuntimeException {

    public ProtocolMisalignmentException(byte type, int leftover) {
        super("消息 '%s' (0x%02X) 解析后剩余 %d 字节，字段布局与协议不符"
                .formatted((char) type, type, leftover));
    }
}
