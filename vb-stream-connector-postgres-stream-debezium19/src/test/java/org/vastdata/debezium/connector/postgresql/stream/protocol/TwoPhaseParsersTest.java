package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TwoPhaseParsers 族（'b'/'P'/'K'/'r'/'p' 两阶段提交）的行为契约测试：引擎
 * {@code org.vastdata.vbstream.protocol.TwoPhaseParsersTest} 全部 5 用例的 1:1 翻写
 * （断言值不变，仅换解码器类名）。与 StreamParsersTest 同理经
 * {@link PgOutputStreamDecoder#decode(ByteBuffer)} 驱动——每条消息的出口剩余字节
 * 检查（含 flags 字节读掉不建模的偏移锚定）在真实 decode() 路径下被覆盖。
 */
class TwoPhaseParsersTest {

    /** 所有用例共用的解码器：模式对两阶段族无分支语义，取 PARALLEL 与引擎侧一致。 */
    private final PgOutputStreamDecoder decoder = new PgOutputStreamDecoder(StreamingMode.PARALLEL);

    /** PG epoch + 42 秒的微秒形式（注意：输入语义是 epoch 起的微秒数，勿再叠 epoch） */
    private static final long MICROS = 42_000_000L;

    /**
     * 验证意图：'b' BeginPrepare 五字段按序读取——I64 prepareLsn + I64 endLsn +
     * I64 prepareTs（PG 纪元微秒换算）+ I32 xid + CString gid；'b' 无 flags 字节
     * （与 'P' 的差异点），夹具不写该字节即是对无 flags 的反向锚定。
     */
    @Test
    void beginPrepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('b')
                .i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.BeginPrepare msg = (PgOutputMessage.BeginPrepare) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareLsn());
        assertEquals(0x200L, msg.endLsn());
        assertEquals(Instant.ofEpochSecond(946684800L + 42), msg.prepareTimestamp());
        assertEquals(909L, msg.xid());
        assertEquals("gid_1", msg.gid());
    }

    /**
     * 验证意图：'P' Prepare 首字节 flags 被读掉不建模——后续五字段（与 'p' 同构）
     * 的断言值锚定在 flags 之后的偏移上，漏读 1 字节即整体错位。
     */
    @Test
    void prepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('P')
                .i8(0).i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.Prepare msg = (PgOutputMessage.Prepare) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareLsn());
        assertEquals("gid_1", msg.gid());
    }

    /**
     * 验证意图：'K' CommitPrepared 的 flags 字节后 I64 commitLsn + I64 endLsn +
     * I64 commitTs + I32 xid + CString gid 按序读取。
     */
    @Test
    void commitPrepared() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('K')
                .i8(0).i64(0x300L).i64(0x400L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.CommitPrepared msg = (PgOutputMessage.CommitPrepared) decoder.decode(payload);
        assertEquals(0x300L, msg.commitLsn());
        assertEquals("gid_1", msg.gid());
    }

    /**
     * 验证意图：'r' RollbackPrepared 是唯一双时间戳消息——两个 I64 微秒时间戳的
     * 读取顺序不可交换：prepare=+42s 整，rollback=+42s 又 10µs（夹具刻意差 10µs
     * 拆穿换序后仍可能相等的取巧实现）。
     */
    @Test
    void rollbackPreparedHasTwoTimestamps() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('r')
                .i8(0).i64(0x100L).i64(0x500L).i64(MICROS).i64(MICROS + 10).i32(909).str("gid_1").build();
        PgOutputMessage.RollbackPrepared msg = (PgOutputMessage.RollbackPrepared) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareEndLsn());
        assertEquals(0x500L, msg.rollbackEndLsn());
        assertEquals(Instant.ofEpochSecond(946684800L + 42), msg.prepareTimestamp());
        assertEquals(Instant.ofEpochSecond(946684800L + 42, 10_000L), msg.rollbackTimestamp());
        assertEquals("gid_1", msg.gid());
    }

    /**
     * 验证意图：'p' StreamPrepare 与 'P' 消息体完全同构（flags + 五字段），经同一
     * 复用读取路径解析，仅构造的 record 类型不同——断言落在共同字段 prepareLsn/gid 上。
     */
    @Test
    void streamPrepare() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('p')
                .i8(0).i64(0x100L).i64(0x200L).i64(MICROS).i32(909).str("gid_1").build();
        PgOutputMessage.StreamPrepare msg = (PgOutputMessage.StreamPrepare) decoder.decode(payload);
        assertEquals(0x100L, msg.prepareLsn());
        assertEquals("gid_1", msg.gid());
    }
}
