package org.vastdata.vbstream.protocol;

import java.util.OptionalLong;

/** 族 1：DML 消息与 TupleData。Task 5 实现具体解析。 */
final class DmlParsers {

    private DmlParsers() {
    }

    static PgOutputMessage.Insert insert(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.Update update(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.Delete delete(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.Truncate truncate(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }

    static PgOutputMessage.LogicalMsg logicalMsg(ByteBufferReader r, OptionalLong streamXid) {
        throw new UnsupportedOperationException("Task 5 实现");
    }
}
