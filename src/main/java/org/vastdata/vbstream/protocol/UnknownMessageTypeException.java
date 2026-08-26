package org.vastdata.vbstream.protocol;

/** 未知消息类型字节（或 TupleData 未知列种类），fail-fast 不静默跳过。 */
public final class UnknownMessageTypeException extends RuntimeException {

    public UnknownMessageTypeException(byte type, ByteBufferReader reader) {
        super("未知 pgoutput 类型字节 '%s' (0x%02X)，剩余 %d 字节，可能消息错位"
                .formatted((char) type, type, reader.remaining()));
    }
}
