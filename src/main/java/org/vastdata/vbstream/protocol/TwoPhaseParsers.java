package org.vastdata.vbstream.protocol;

/** 族 4：两阶段提交消息。Task 7 实现具体解析。 */
final class TwoPhaseParsers {

    private TwoPhaseParsers() {
    }

    static PgOutputMessage.BeginPrepare beginPrepare(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.Prepare prepare(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.CommitPrepared commitPrepared(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.RollbackPrepared rollbackPrepared(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }

    static PgOutputMessage.StreamPrepare streamPrepare(ByteBufferReader r) {
        throw new UnsupportedOperationException("Task 7 实现");
    }
}
