package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * StreamParsers 族（'S'/'E'/'c'/'A' 流式大事务控制）的行为契约测试：引擎
 * {@code org.vastdata.vbstream.protocol.StreamParsersTest} 全部 6 用例的 1:1 翻写
 * （断言值不变，仅换解码器类名）。与 NormalParsersTest/DmlParsersTest 的替身接缝不同，
 * 本族消息的状态语义（inStream 前缀有无、abort 按模式分支）只有经解码器整路径才可观察，
 * 故经 {@link PgOutputStreamDecoder#decode(ByteBuffer)} 驱动——同时让每条消息的出口
 * 剩余字节检查在真实 decode() 路径下被覆盖（Task 4/5 遗留的出口闭合账在此清账）。
 * 注意：StreamAbort 的类型字节是 'A'（大写，协议 LOGICAL_REP_MSG_STREAM_ABORT 原值，
 * 引擎与 Debezium MessageType 一致；brief 字节表中的 'a' 为笔误）。
 */
class StreamParsersTest {

    /**
     * 验证意图：'S'/'E' 驱动解码器流块状态机往返——S 后块内 'I' 先读 Int32 xid 前缀
     * （streamXid 非空且值等于消息携带的 xid），E 复位后顶层 'I' 无前缀（empty）。
     * StreamStart 自身不消费前缀（firstSegment 是无条件字段）。
     */
    @Test
    void streamStartStopToggleInStreamState() throws IOException {
        PgOutputStreamDecoder decoder = new PgOutputStreamDecoder(StreamingMode.ON);
        Object streamStart = decoder.decode(new MsgBuilder().type('S').i32(505).i8(1).build());
        assertInstanceOf(PgOutputMessage.StreamStart.class, streamStart);
        assertEquals(505L, ((PgOutputMessage.StreamStart) streamStart).xid());
        assertTrue(((PgOutputMessage.StreamStart) streamStart).firstSegment());
        // inStream 置位后，流块内的 I 消息会先读 Int32 xid 前缀
        Object inBlock = decoder.decode(new MsgBuilder().type('I')
                .i32(505).i32(16385).i8('N').i16(0).build());
        assertInstanceOf(PgOutputMessage.Insert.class, inBlock);
        assertTrue(((PgOutputMessage.Insert) inBlock).streamXid().isPresent());
        assertEquals(505L, ((PgOutputMessage.Insert) inBlock).streamXid().getAsLong());

        decoder.decode(new MsgBuilder().type('E').build());
        Object afterStop = decoder.decode(new MsgBuilder().type('I')
                .i32(16385).i8('N').i16(0).build());
        assertFalse(((PgOutputMessage.Insert) afterStop).streamXid().isPresent());
    }

    /**
     * 验证意图：'c' StreamCommit 按序读取 I32 xid + I8 flags（读掉不建模——夹具含
     * 该字节，漏读会让 commitLsn 吃进 flags 拼出的错值）+ I64/I64 LSN + I64 微秒时间戳。
     * 夹具为 PG epoch 秒数的微秒形式（偏移 946684800s ≈ 30 年）：
     * 换算 = 2000-01-01 + 946684800s = 2029-12-31T00:00:00Z，覆盖 epoch 加法与微秒整除两条路径。
     */
    @Test
    void streamCommit() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('c')
                .i32(505).i8(0).i64(0x6000L).i64(0x7000L).i64(946684800_000_000L).build();
        PgOutputMessage.StreamCommit msg = (PgOutputMessage.StreamCommit)
                new PgOutputStreamDecoder(StreamingMode.ON).decode(payload);
        assertEquals(505L, msg.xid());
        assertEquals(0x6000L, msg.commitLsn());
        assertEquals(0x7000L, msg.endLsn());
        assertEquals(Instant.parse("2029-12-31T00:00:00Z"), msg.commitTimestamp());
    }

    /**
     * 验证意图：'A' StreamAbort 非 parallel 模式只读 I32 xid + I32 subxid，
     * abortLsn/abortTimestamp 整体缺席（empty）——与 PARALLEL 正例对称。
     */
    @Test
    void streamAbortWithoutParallelExtra() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A')
                .i32(505).i32(606).build();
        PgOutputMessage.StreamAbort msg = (PgOutputMessage.StreamAbort)
                new PgOutputStreamDecoder(StreamingMode.ON).decode(payload);
        assertEquals(505L, msg.xid());
        assertEquals(606L, msg.subxid());
        assertFalse(msg.abortLsn().isPresent());
        assertFalse(msg.abortTimestamp().isPresent());
    }

    /**
     * 验证意图：'A' StreamAbort 在 PARALLEL 模式继续读 I64 abortLsn + I64 abortTime
     * （微秒原值，刻意不转 Instant），两字段经 OptionalLong 非空暴露。
     */
    @Test
    void streamAbortWithParallelExtra() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A')
                .i32(505).i32(505).i64(0x8000L).i64(123_456L).build();
        PgOutputMessage.StreamAbort msg = (PgOutputMessage.StreamAbort)
                new PgOutputStreamDecoder(StreamingMode.PARALLEL).decode(payload);
        assertEquals(0x8000L, msg.abortLsn().getAsLong());
        assertEquals(123_456L, msg.abortTimestamp().getAsLong());
    }

    /**
     * 验证意图：PARALLEL 模式下附加字段缺 16 字节即 BufferUnderflow——abort 解析严格
     * 按构造时 StreamingMode 分支，错读/漏读都会让字节流错位而非静默通过。
     */
    @Test
    void streamAbortInParallelModeRequiresExtraBytes() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('A').i32(1).i32(1).build();
        assertThrows(java.nio.BufferUnderflowException.class,
                () -> new PgOutputStreamDecoder(StreamingMode.PARALLEL).decode(payload));
    }

    /**
     * 验证意图：'E' StreamStop 无字段——类型字节后消息体为空，出口剩余字节恰为 0。
     */
    @Test
    void streamStopIsFieldless() throws IOException {
        Object msg = new PgOutputStreamDecoder(StreamingMode.ON)
                .decode(new MsgBuilder().type('E').build());
        assertInstanceOf(PgOutputMessage.StreamStop.class, msg);
    }
}
