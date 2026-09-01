package org.vastdata.debezium.connector.postgresql.stream.protocol;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * NormalParsers 五类消息（B/C/O/R/Y）的行为契约测试：引擎
 * {@code org.vastdata.vbstream.protocol.NormalParsersTest} 全部 6 用例的 1:1 翻写
 * （断言值不变，仅换类名与调用接缝）。本模块解码器属 Task 6，类型字节剥离与出口
 * 剩余字节检查在测试内以同形替身代行——{@link #body(ByteBuffer)} 代行前者，
 * {@link #requireFullyConsumed(byte, WireReader)} 代行后者（"读完 relation 后 remaining==0、
 * 多留字节即抛 ProtocolMisalignmentException" 的语义在 parser 层直接断言 reader 状态）。
 */
class NormalParsersTest {

    /**
     * 验证意图：'B' Begin 三字段按序读取——commit_ts 为 PG epoch(2000-01-01) 起的微秒数，
     * 2_500_000µs = epoch+2.5s（勿再叠加 epoch 秒数）；xid 用高位值 0x80000001 钉住
     * 无符号语义（有符号读取会得负数，掩码进 long 才是 2147483649）。
     */
    @Test
    void begin() throws IOException {
        // commit_ts 为 PG epoch(2000-01-01) 起的微秒数：2_500_000µs = epoch+2.5s（勿再叠加 epoch 秒数）
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(0x1000L).i64(2_500_000L).i32(0x80000001).build(); // 高位 xid 钉住无符号语义
        PgOutputMessage.Begin msg = NormalParsers.begin(body(payload));
        assertEquals(0x1000L, msg.finalLsn());
        assertEquals(Instant.ofEpochSecond(946684800L + 2, 500_000_000L), msg.commitTimestamp());
        assertEquals(2147483649L, msg.xid());
    }

    /**
     * 验证意图：'C' Commit 的首字节 flags 必须被消费——commitLsn/endLsn 断言值锚定在
     * flags 之后的偏移上，漏读 1 字节即整体左移错位（commitLsn 会读成 flags 拼出的 0）。
     */
    @Test
    void commitConsumesLeadingFlagsByte() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('C')
                .i8(0).i64(0x2000L).i64(0x3000L).i64(1_000_000L).build();
        PgOutputMessage.Commit msg = NormalParsers.commit(body(payload));
        assertEquals(0x2000L, msg.commitLsn());
        assertEquals(0x3000L, msg.endLsn());
    }

    /**
     * 验证意图：'O' Origin 的 I64 LSN + CString name 按序读取，名字按 UTF-8 解码。
     */
    @Test
    void origin() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('O').i64(0x4000L).str("origin_a").build();
        PgOutputMessage.Origin msg = NormalParsers.origin(body(payload));
        assertEquals("origin_a", msg.originName());
    }

    /**
     * 验证意图：'R' Relation 头部四字段 + I16 列数 + 逐列 [flags(bit0=key) + name + typeId + typmod]
     * 按序读取，RelationColumn 以 record 值相等直接对拍（partOfKey 由列 flags bit0 揭示）；
     * 末尾以出口检查替身断言双列读完 reader 恰好耗尽——decoder finish 语义的 parser 层验证。
     */
    @Test
    void relationWithTwoColumns() throws IOException {
        MsgBuilder m = new MsgBuilder().type('R')
                .i32(16385).str("public").str("t_demo").i8('d')
                .i16(2)
                .i8(1).str("id").i32(23).i32(-1)   // int4, key, typmod=-1
                .i8(0).str("name").i32(25).i32(-1); // text, 非key
        WireReader reader = body(m.build());
        PgOutputMessage.Relation msg = NormalParsers.relation(reader, OptionalLong.empty());
        assertEquals(16385, msg.relationOid());
        assertEquals("t_demo", msg.table());
        assertEquals('d', msg.replicaIdentity());
        assertEquals(2, msg.columns().size());
        assertEquals(new RelationColumn("id", 23, -1, true), msg.columns().get(0));
        assertEquals(new RelationColumn("name", 25, -1, false), msg.columns().get(1));
        // 出口剩余字节检查语义：双列读完后 remaining==0（多留字节由下一用例反向钉住）
        requireFullyConsumed((byte) 'R', reader);
    }

    /**
     * 验证意图：'Y' Type 的 I32 oid + 双 CString（schema/name）按序读取。
     */
    @Test
    void typeMsg() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('Y').i32(16386).str("public").str("mytype").build();
        PgOutputMessage.Type msg = NormalParsers.type(body(payload), OptionalLong.empty());
        assertEquals("mytype", msg.name());
    }

    /**
     * 验证意图：消息体多留字节时 parser 自身不报错（它只管读到字段尽头），错位由
     * 出口剩余字节检查暴露——先在 parser 层直接断言 reader 状态（剩余恰为多写的 4 字节），
     * 再以同形替身验证该状态触发 ProtocolMisalignmentException（实现属 Task 6 decoder）。
     */
    @Test
    void leftoverBytesCauseMisalignment() throws IOException {
        ByteBuffer payload = new MsgBuilder().type('B')
                .i64(1).i64(2).i32(3).i32(99).build(); // 故意多 4 字节
        WireReader reader = body(payload);
        NormalParsers.begin(reader);
        assertEquals(4, reader.remaining());
        assertThrows(ProtocolMisalignmentException.class,
                () -> requireFullyConsumed(payload.get(0), reader));
    }

    /**
     * 剥去首字节类型字节后以 WireReader 包裹消息体——生产路径该职责属解码器
     * （Task 6：读类型字节 → 分发 parser），测试手动代行，使 parser 从消息体
     * 首字段读起，与字节格式表"类型字节已由 decoder 剥离"的口径一致。
     */
    private static WireReader body(ByteBuffer payload) {
        payload.get();
        return new WireReader(payload);
    }

    /**
     * 出口剩余字节检查的 parser 层替身，与引擎解码器 finish 同形：解析完成后
     * remaining 非 0 即抛 ProtocolMisalignmentException。实现在 Task 6 落进解码器，
     * 此处先行验证该检查所依赖的 reader 状态语义（读完为 0 / 多留即抛）。
     */
    private static void requireFullyConsumed(byte type, WireReader reader) {
        if (reader.remaining() != 0) {
            throw new ProtocolMisalignmentException(type, reader.remaining());
        }
    }
}
