package org.vastdata.vbstream.protocol;

import java.util.OptionalLong;

/** 族 1/2：事务边界与元数据消息。Task 4 实现具体解析。 */
final class NormalParsers {

    private NormalParsers() {
    }

    static PgOutputMessage.Begin begin(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Commit commit(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Origin origin(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Relation relation(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 4 实现");
    }

    static PgOutputMessage.Type type(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 4 实现");
    }
}
