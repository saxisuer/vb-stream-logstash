package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DmlParsers 族（'I'/'U'/'D'/'T'/'M' 与 TupleData）的行为契约测试：引擎
 * {@code org.vastdata.vbstream.protocol.DmlParsersTest} 全部 12 用例的 1:1 翻写
 * （断言值不变，仅换调用接缝——引擎经 decoder.decode 分发，本模块解码器属 Task 6，
 * 类型字节剥离以 {@link #body(ByteBuffer)} 替身代行，与 NormalParsersTest 同形；
 * 出口剩余字节检查替身 {@link #requireFullyConsumed(byte, WireReader)} 在 insert
 * 与 truncate 两用例钉住"读到消息体尽头"的语义——引擎侧由 decode 的 finish 检查隐式覆盖）。
 */
class DmlParsersTest {

    /**
     * 验证意图：'I' insert 的 I32 oid + 'N' 标记 + 三列 TupleData 按序读取——
     * 't' 列值取 I32 长度前缀载荷按 UTF-8 解码，'n' 列为 NULL 单例；末尾以出口
     * 检查替身断言三列读完后 reader 恰好耗尽（decoder finish 语义的 parser 层验证）。
     */
    @Test
    void insertWithTextAndNullColumns() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(16385).i8('N')
                .i16(3)
                .i8('t').bytes(utf8("a"))
                .i8('n')
                .i8('t').bytes(utf8("b"))
                .build();
        WireReader reader = body(payload);
        PgOutputMessage.Insert msg = DmlParsers.insert(reader, OptionalLong.empty());
        assertEquals(16385, msg.relationOid());
        assertEquals(3, msg.newTuple().columns().size());
        assertEquals(new TupleValue.Text("a"), msg.newTuple().columns().get(0));
        assertEquals(TupleValue.NULL, msg.newTuple().columns().get(1));
        assertEquals(new TupleValue.Text("b"), msg.newTuple().columns().get(2));
        requireFullyConsumed((byte) 'I', reader);
    }

    /**
     * 验证意图：'U' update 形态①——'K' 携带旧 key 元组（仅 key 列），随后 'N' 标记
     * 引出新元组；oldTuple 非空且为新元组独立解析（新值 "2" 不受旧 key "1" 干扰）。
     */
    @Test
    void updateWithKeyPrefixThenNewTuple() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('K').i16(1).i8('t').bytes(utf8("1"))  // 旧 key
                .i8('N').i16(1).i8('t').bytes(utf8("2"))  // 新值
                .build();
        PgOutputMessage.Update msg = DmlParsers.update(body(payload), OptionalLong.empty());
        assertTrue(msg.oldTuple().isPresent());
        assertEquals(1, msg.oldTuple().get().columns().size());
        assertEquals(new TupleValue.Text("2"), msg.newTuple().columns().get(0));
    }

    /**
     * 验证意图：'U' update 形态②——REPLICA IDENTITY FULL 时 'O' 携带完整旧行
     * （非仅 key 列），与 'K' 同一解析路径；新旧元组列数可不同（2 列旧行 + 1 列新行）。
     */
    @Test
    void updateWithFullOldRow() throws IOException {
        // REPLICA IDENTITY FULL：'O' 携带完整旧行（非仅 key 列），与 'K' 同一解析路径
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('O').i16(2).i8('t').bytes(utf8("1")).i8('t').bytes(utf8("old"))
                .i8('N').i16(1).i8('t').bytes(utf8("new"))
                .build();
        PgOutputMessage.Update msg = DmlParsers.update(body(payload), OptionalLong.empty());
        assertTrue(msg.oldTuple().isPresent());
        assertEquals(2, msg.oldTuple().get().columns().size());
        assertEquals(new TupleValue.Text("old"), msg.oldTuple().get().columns().get(1));
        assertEquals(new TupleValue.Text("new"), msg.newTuple().columns().get(0));
    }

    /**
     * 验证意图：'U' update 形态③——语句未携带旧值时无 'K'/'O' 前缀，首标记直接是 'N'，
     * oldTuple 为 Optional.empty（三形态中唯一的空旧元组分支）。
     */
    @Test
    void updateWithoutOldTuple() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385).i8('N').i16(1).i8('t').bytes(utf8("x")).build();
        PgOutputMessage.Update msg = DmlParsers.update(body(payload), OptionalLong.empty());
        assertEquals(Optional.empty(), msg.oldTuple());
    }

    /**
     * 验证意图：'K'/'O' 旧元组之后的标记必须是 'N'——再来一个 'O'（K/O 非法并存序列）
     * 不得静默跳过，fail-fast 抛 UnknownMessageTypeException。
     */
    @Test
    void updateOldTupleFollowedByWrongTagFailsFast() throws IOException {
        // 'K'/'O' 之后必须是 'N'：再来一个 'O'（K/O 非法并存序列）不得静默跳过
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385)
                .i8('K').i16(1).i8('t').bytes(utf8("1"))
                .i8('O')
                .build();
        assertThrows(UnknownMessageTypeException.class,
                () -> DmlParsers.update(body(payload), OptionalLong.empty()));
    }

    /**
     * 验证意图：'U' 首标记既非 'K'/'O' 也非 'N'（如 'X'）即未知元组标记，
     * fail-fast 抛 UnknownMessageTypeException 而非猜一种形态继续。
     */
    @Test
    void updateUnknownTagFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('U')
                .i32(16385).i8('X').build();
        assertThrows(UnknownMessageTypeException.class,
                () -> DmlParsers.update(body(payload), OptionalLong.empty()));
    }

    /**
     * 验证意图：'D' delete 的 I32 oid + 'K' 标记 + 旧元组按序读取，
     * 旧元组进 oldTuple 组件（delete 模型无新元组）。
     */
    @Test
    void deleteWithKey() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('D')
                .i32(16385).i8('K').i16(1).i8('t').bytes(utf8("9")).build();
        PgOutputMessage.Delete msg = DmlParsers.delete(body(payload), OptionalLong.empty());
        assertEquals(16385, msg.relationOid());
        assertEquals(new TupleValue.Text("9"), msg.oldTuple().columns().get(0));
    }

    /**
     * 验证意图：'D' 必带 'K' 或 'O' 旧元组标记——'N' 不是合法的 delete 标记
     * （insert 语义混进 delete 处必须拒绝），fail-fast 抛 UnknownMessageTypeException。
     */
    @Test
    void deleteWithoutKeyOrOldTupleTagFailsFast() throws IOException {
        // 'D' 必有 'K' 或 'O'：'N' 不是合法的 delete tag
        ByteBuffer payload = new MsgBuilder().type('D')
                .i32(16385).i8('N').build();
        assertThrows(UnknownMessageTypeException.class,
                () -> DmlParsers.delete(body(payload), OptionalLong.empty()));
    }

    /**
     * 验证意图：'T' truncate 的 I32 关系数 + I8 选项位 + N×I32 oid 按序读取——
     * 选项位 3 = bit0|bit1 = CASCADE|RESTART_IDENTITY 拼进 EnumSet，两张表 oid
     * 依声明顺序进数组；末尾以出口检查替身断言 oid 序列读完 reader 恰好耗尽。
     */
    @Test
    void truncateOptionsAndOids() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('T')
                .i32(2).i8(3).i32(100).i32(200).build(); // CASCADE|RESTART, 两张表
        WireReader reader = body(payload);
        PgOutputMessage.Truncate msg = DmlParsers.truncate(reader, OptionalLong.empty());
        assertEquals(java.util.EnumSet.of(TruncateOption.CASCADE, TruncateOption.RESTART_IDENTITY), msg.options());
        assertArrayEquals(new int[]{100, 200}, msg.relationOids());
        requireFullyConsumed((byte) 'T', reader);
    }

    /**
     * 验证意图：'M' logicalMsg 的 I8 flags + I64 lsn + CString prefix + I32 长度前缀
     * + content 按序读取——content 恰取长度前缀声明的字节数（多读会吞下一条消息的
     * 头部、少读会错位）；flags 只有 bit0 表示 transactional，其他位（如 bit1=2）
     * 不得误读为事务消息（位隔离）。
     */
    @Test
    void logicalMessageWithContentLength() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('M')
                .i8(1).i64(0x5000L).str("prefix").bytes(utf8("hello")).build();
        PgOutputMessage.LogicalMsg msg = DmlParsers.logicalMsg(body(payload), OptionalLong.empty());
        assertTrue(msg.transactional());
        assertEquals("prefix", msg.prefix());
        assertArrayEquals(utf8("hello"), msg.content());

        // flags 只有 bit0 表示 transactional：其他位（如 bit1）不得误读为事务消息
        ByteBuffer nonTxPayload = new MsgBuilder().type('M')
                .i8(2).i64(0x5000L).str("prefix").bytes(utf8("hello")).build();
        PgOutputMessage.LogicalMsg nonTx = DmlParsers.logicalMsg(body(nonTxPayload), OptionalLong.empty());
        assertFalse(nonTx.transactional());
    }

    /**
     * 验证意图：TupleData 无负载列种类——'u' 为 TOAST 未变单例（值不可得，非 NULL），
     * 'b' 为二进制形态取 I32 长度前缀原始字节（不按文本解码，值相等语义对拍 byte[]）。
     */
    @Test
    void unchangedToastAndBinaryValue() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(1).i8('N')
                .i16(2)
                .i8('u')
                .i8('b').bytes(new byte[]{1, 2})
                .build();
        PgOutputMessage.Insert msg = DmlParsers.insert(body(payload), OptionalLong.empty());
        assertEquals(TupleValue.UNCHANGED_TOAST, msg.newTuple().columns().get(0));
        assertEquals(new TupleValue.Binary(new byte[]{1, 2}), msg.newTuple().columns().get(1));
    }

    /**
     * 验证意图：TupleData 列种类字节超出 'n'/'u'/'t'/'b' 四值（如 'z'）即未知种类，
     * fail-fast 抛 UnknownMessageTypeException——静默跳过一列会让后续全部列错位。
     */
    @Test
    void unknownTupleKindFailsFast() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('I')
                .i32(1).i8('N').i16(1).i8('z').build();
        assertThrows(UnknownMessageTypeException.class,
                () -> DmlParsers.insert(body(payload), OptionalLong.empty()));
    }

    /**
     * 测试文本转 UTF-8 字节（'t' 列值与 LogicalMsg content 的线上形态）。
     *
     * @param s 待编码文本
     * @return UTF-8 字节序列
     */
    private static byte[] utf8(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * 剥去首字节类型字节后以 WireReader 包裹消息体——生产路径该职责属解码器
     * （Task 6：读类型字节 → 分发 parser），测试手动代行，使 parser 从消息体
     * 首字段读起，与字节格式表"类型字节已由 decoder 剥离"的口径一致。
     * NormalParsersTest 同形替身，两测试类各自持有（包内可见、不抽公共基类）。
     *
     * @param payload MsgBuilder 产出的完整消息（含首字节类型字节）
     * @return 从消息体首字段读起的读原语
     */
    private static WireReader body(ByteBuffer payload) {
        payload.get();
        return new WireReader(payload);
    }

    /**
     * 出口剩余字节检查的 parser 层替身，与引擎解码器 finish 同形：解析完成后
     * remaining 非 0 即抛 ProtocolMisalignmentException。实现在 Task 6 落进解码器，
     * 此处先行验证该检查所依赖的 reader 状态语义（读完为 0 / 多留即抛）。
     *
     * @param type 消息类型字节（异常诊断用）
     * @param reader 解析完成后的读原语
     */
    private static void requireFullyConsumed(byte type, WireReader reader) {
        if (reader.remaining() != 0) {
            throw new ProtocolMisalignmentException(type, reader.remaining());
        }
    }
}
