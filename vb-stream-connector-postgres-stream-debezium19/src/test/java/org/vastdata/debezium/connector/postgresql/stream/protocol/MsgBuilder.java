package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 测试用 pgoutput 消息样本构造器，写入均为 big-endian。
 * 引擎测试域 org.vastdata.vbstream.protocol.MsgBuilder 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），供 Task 4-6 的 parser 族测试拼装消息字节。
 * 非线程安全：单测试方法内链式使用，不跨线程共享。
 */
public final class MsgBuilder {

    /** 未定长字节累积流：type/i8/str 等单字节与裸字节写入这里，build 时整体取出。 */
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    /** buf 之上的 big-endian 定宽写出视图：i16/i32/i64 与 bytes 的 I32 长度前缀经它写入以保证字节序。 */
    private final DataOutputStream out = new DataOutputStream(buf);

    /**
     * 写 1 字节消息类型字符（如 'B'/'I'/'S'），按协议位于消息体首字节。
     *
     * @param t pgoutput 消息类型字符，仅低 8 位被写出（ASCII 类型字节无损失）
     * @return this，支持链式续写
     */
    public MsgBuilder type(char t) {
        buf.write(t);
        return this;
    }

    /**
     * 写 1 字节有符号整数或种类字节（如 TupleData 列种类 't'、flags、firstSegment）。
     *
     * @param v 待写值，仅低 8 位被写出（调用方负责值域）
     * @return this，支持链式续写
     */
    public MsgBuilder i8(int v) {
        buf.write(v);
        return this;
    }

    /**
     * 写 2 字节 big-endian 整数（如 Relation 列数）。
     *
     * @param v 待写值，仅低 16 位被写出（调用方负责值域）
     * @return this，支持链式续写
     * @throws IOException DataOutputStream 写出失败（内存流实际不会发生，签名对齐引擎侧）
     */
    public MsgBuilder i16(int v) throws IOException {
        out.writeShort(v);
        return this;
    }

    /**
     * 写 4 字节 big-endian 整数（如 xid、relation oid、列值长度前缀）。
     *
     * @param v 待写值，仅低 32 位被写出（调用方负责值域）
     * @return this，支持链式续写
     * @throws IOException DataOutputStream 写出失败（内存流实际不会发生，签名对齐引擎侧）
     */
    public MsgBuilder i32(int v) throws IOException {
        out.writeInt(v);
        return this;
    }

    /**
     * 写 8 字节 big-endian 长整数（如 LSN、微秒时间戳）。
     *
     * @param v 待写的 64 位值
     * @return this，支持链式续写
     * @throws IOException DataOutputStream 写出失败（内存流实际不会发生，签名对齐引擎侧）
     */
    public MsgBuilder i64(long v) throws IOException {
        out.writeLong(v);
        return this;
    }

    /**
     * 写 null 结尾 UTF-8 字符串（CString，协议的 schema/表名/列名/gid 等编码形态）：
     * UTF-8 字节序列 + 1 字节 0x00 终止符。
     *
     * @param s 待写文本，不得含 U+0000（0x00 是终止符）
     * @return this，支持链式续写
     * @throws IOException DataOutputStream 写出失败（内存流实际不会发生，签名对齐引擎侧）
     */
    public MsgBuilder str(String s) throws IOException {
        buf.write(s.getBytes(StandardCharsets.UTF_8));
        buf.write(0);
        return this;
    }

    /**
     * 写带 I32 长度前缀的字节载荷：先 4 字节 big-endian 长度，再原始字节。
     * 专用于 't'/'b' 列值内容与 LogicalMsg content——注意长度前缀只计字节内容本身，
     * 列种类字节（'t'/'b'）不在载荷内，须另行 {@link #i8(int)} 写出。
     *
     * @param arr 载荷字节，长度写成前缀；空数组合法（前缀为 0）
     * @return this，支持链式续写
     * @throws IOException DataOutputStream 写出失败（内存流实际不会发生，签名对齐引擎侧）
     */
    public MsgBuilder bytes(byte[] arr) throws IOException {
        out.writeInt(arr.length);
        out.write(arr);
        return this;
    }

    /**
     * 结束构造并取出全部已写字节，position 归零待读。
     *
     * @return 新包裹的只读视图上的 heap ByteBuffer（position=0，可直接喂给 WireReader）
     * @throws IOException flush 失败（内存流实际不会发生，签名对齐引擎侧）
     */
    public ByteBuffer build() throws IOException {
        out.flush();
        return ByteBuffer.wrap(buf.toByteArray());
    }
}
