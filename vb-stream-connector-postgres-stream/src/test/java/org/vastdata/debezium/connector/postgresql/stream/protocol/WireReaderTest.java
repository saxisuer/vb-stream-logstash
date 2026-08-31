package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * WireReader 读原语的行为契约测试：big-endian 整型（含无符号掩码边界）、
 * CString 多字节 UTF-8、PG 纪元微秒换算（含负值），外加 MsgBuilder 字节序抽查。
 * 前三用例为引擎 ByteBufferReaderTest 的 1:1 翻写（断言值不变，仅换类名与包名），
 * 第四用例为本模块新增——防止测试基建自身字节序写反而把错误布局"测试成真"。
 */
class WireReaderTest {

    /**
     * 验证意图：整型字段一律按 big-endian 读取且宽度不串位，并且 readUnsignedInt
     * 的无符号掩码在边界值 0xFFFFFFFF 处必须得到 4294967295L 而非 -1——
     * 这是 xid 无符号语义（设计约束：xid 一律走 readUnsignedInt）的地基。
     */
    @Test
    void readsBigEndianIntegers() {
        // 1 + 2 + 4 + 4 + 4 = 15 字节（原计划 allocate(10) 容量不足，putInt(4) 时 BufferOverflow）
        ByteBuffer buf = ByteBuffer.allocate(15)
                .put((byte) 1).putShort((short) 2).putInt(3).putInt(4).putInt(0xFFFFFFFF);
        buf.flip();
        WireReader r = new WireReader(buf);
        assertEquals(1, r.readByte());
        assertEquals(2, r.readUnsignedShort());
        assertEquals(3, r.readInt());
        assertEquals(4L, r.readUnsignedInt()); // unsigned 32 位保进 long
        // 无符号掩码边界：0xFFFFFFFF 必须读成 4294967295 而非 -1（删掩码此断言即失败）
        assertEquals(4294967295L, r.readUnsignedInt());
    }

    /**
     * 验证意图：readString 按 CString（读到 \0 终止）读取，多字节 UTF-8（中文）
     * 必须完整解码不被截断，且终止符不计入内容——其后的下一字节仍可继续按字段读取。
     */
    @Test
    void readsCStringAsUtf8() {
        ByteBuffer buf = ByteBuffer.allocate(16).put("你好".getBytes(StandardCharsets.UTF_8))
                .put((byte) 0).put((byte) 'x');
        buf.flip();
        WireReader r = new WireReader(buf);
        assertEquals("你好", r.readString());
        assertEquals('x', r.readByte());
    }

    /**
     * 验证意图：pgMicrosToInstant 以 PG 纪元 2000-01-01 00:00:00 UTC
     * （= Unix 946684800 秒）换算微秒时间戳；负微秒走 floorDiv/floorMod
     * 向负无穷取整（epoch 前 1 微秒 = 1999-12-31T23:59:59.999999Z），
     * 用向零截断除法的实现此断言即失败。
     */
    @Test
    void convertsPgEpochMicrosToInstant() {
        // PG epoch 2000-01-01 00:00:00 UTC = Unix 946684800 秒；1 秒 = 1e6 微秒
        Instant expected = Instant.ofEpochSecond(946684800L + 100, 500_000_000L);
        assertEquals(expected, WireReader.pgMicrosToInstant(100_500_000L));
        // 负值：epoch 前 1 微秒——floorDiv/floorMod 不向零截断
        assertEquals(Instant.parse("1999-12-31T23:59:59.999999Z"), WireReader.pgMicrosToInstant(-1L));
    }

    /**
     * 验证意图：MsgBuilder 七种写入（type/i8/i16/i32/i64/str/bytes）拼出的字节序列
     * 与手拼 big-endian 期望逐字节一致——重点核对 bytes() 必须先写 I32 长度前缀再写内容
     * （前缀只计载荷长度，列种类字节不在其内）；随后用 WireReader 对同一缓冲全字段回读，
     * 顺带覆盖 readUnsignedByte/readLong/readBytes/remaining 的端到端一致性。
     */
    @Test
    void msgBuilderWritesBigEndianLayout() throws IOException {
        ByteBuffer built = new MsgBuilder()
                .type('M')
                .i8(0x74)
                .i16(0x1234)
                .i32(0x56789ABC)
                .i64(0x0102030405060708L)
                .str("hi")
                .bytes(new byte[]{(byte) 0xAA, (byte) 0xBB})
                .build();
        ByteBuffer expected = ByteBuffer.wrap(new byte[]{
                0x4D, // type 'M'
                0x74, // i8
                0x12, 0x34, // i16
                0x56, 0x78, (byte) 0x9A, (byte) 0xBC, // i32
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // i64
                0x68, 0x69, 0x00, // str "hi" + '\0'
                0x00, 0x00, 0x00, 0x02, // bytes 的 I32 长度前缀
                (byte) 0xAA, (byte) 0xBB // bytes 内容
        });
        assertEquals(expected, built);
        // 同一缓冲再走读侧：覆盖无符号单字节、64 位、定长字节段与剩余量查询
        WireReader r = new WireReader(built);
        assertEquals('M', r.readByte());
        assertEquals(0x74, r.readUnsignedByte());
        assertEquals(0x1234, r.readUnsignedShort());
        assertEquals(0x56789ABC, r.readInt());
        assertEquals(0x0102030405060708L, r.readLong());
        assertEquals("hi", r.readString());
        assertEquals(6, r.remaining()); // 4 字节长度前缀 + 2 字节内容
        // 定长字段的消费模式：先读 I32 长度前缀，再按前缀读载荷（与 parser 读 't'/'b' 列值一致）
        assertEquals(2, r.readInt());
        assertArrayEquals(new byte[]{(byte) 0xAA, (byte) 0xBB}, r.readBytes(2));
        assertEquals(0, r.remaining());
    }
}
