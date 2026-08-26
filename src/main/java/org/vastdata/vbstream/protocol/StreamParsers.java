package org.vastdata.vbstream.protocol;

/** 族 3：流式大事务控制消息。Task 6 实现具体解析。 */
final class StreamParsers {

    private StreamParsers() {
    }

    static PgOutputMessage.StreamStart start(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 6 实现");
    }

    static PgOutputMessage.StreamStop stop(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 6 实现");
    }

    static PgOutputMessage.StreamCommit commit(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 6 实现");
    }

    static PgOutputMessage.StreamAbort abort(ByteBufferReader r, StreamingMode mode) {
        throw new UnsupportedOperationException("Task 6 实现");
    }
}
