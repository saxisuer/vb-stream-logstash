package org.vastdata.debezium.connector.postgresql.stream.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * 按协议字节序（big-endian）逐字段读取 pgoutput 消息体的读原语。
 * 引擎 org.vastdata.vbstream.protocol.ByteBufferReader 的 1:1 重写
 * （设计决策 D2：重写不 import 引擎类），方法集合与字节级行为保持一致，
 * 是本 connector 协议包（Task 4-6 parser 族）唯一的底层读取工具。
 * 非线程安全：每个读线程对每条消息各建一个实例，不跨线程共享。
 */
public final class WireReader {

    /** PostgreSQL 时间纪元：2000-01-01 00:00:00 UTC 相对 Unix 纪元的秒数，pgoutput 时间戳的偏移基准。 */
    private static final long PG_EPOCH_SECONDS = 946684800L;

    /** 被读取的消息体缓冲，position 随逐字段读取推进，越界读由 ByteBuffer 抛 BufferUnderflowException。 */
    private final ByteBuffer buf;

    /**
     * 包裹一条 pgoutput 消息体开始逐字段读取。
     *
     * @param buf 已剥去复制协议封装的单条完整消息体（big-endian），从其当前 position 读起
     */
    public WireReader(ByteBuffer buf) {
        this.buf = buf;
    }

    /**
     * 查询尚未消费的字节数。一条消息解析完成后应为 0，非 0 即字段序列与协议不符
     * （上层据此抛 ProtocolMisalignmentException）。
     *
     * @return 底层缓冲的 remaining
     */
    public int remaining() {
        return buf.remaining();
    }

    /**
     * 读 1 字节有符号整数（消息类型字节、flags、种类字节等）。
     *
     * @return 读出的字节（-128..127）
     */
    public byte readByte() {
        return buf.get();
    }

    /**
     * 读 1 字节并按无符号解释（0..255）——flags、firstSegment 等位域字段避免符号位干扰。
     *
     * @return 高位补零后的无符号值
     */
    public int readUnsignedByte() {
        return buf.get() & 0xFF;
    }

    /**
     * 读 2 字节 big-endian 并按无符号解释（0..65535）——如 Relation 列数。
     *
     * @return 掩码到 int 的无符号值
     */
    public int readUnsignedShort() {
        return buf.getShort() & 0xFFFF;
    }

    /**
     * 读 4 字节 big-endian 有符号整数——oid、LSN 低 32 位、长度前缀等。
     *
     * @return 读出的 int 原值
     */
    public int readInt() {
        return buf.getInt();
    }

    /**
     * 读 4 字节 big-endian 并按无符号装入 long（0..4294967295）。
     * 事务号（xid）一律走此方法：32 位 xid 到达 0xFFFFFFFF 邻域时
     * 有符号 int 会变负数，掩码进 long 才能保住无符号序。
     *
     * @return 掩码到 long 的无符号值
     */
    public long readUnsignedInt() {
        return buf.getInt() & 0xFFFFFFFFL;
    }

    /**
     * 读 8 字节 big-endian 长整数——LSN、微秒时间戳原值等。
     *
     * @return 读出的 long 原值
     */
    public long readLong() {
        return buf.getLong();
    }

    /**
     * 读 null 结尾 UTF-8 字符串（CString）：逐字节累积到 0x00 为止，终止符不进内容，
     * 字节序列按 UTF-8 解码（schema/表名/列名/gid 等的线上形态）。
     *
     * @return 解码后的文本；消息体不含终止符时由底层缓冲抛 BufferUnderflowException 暴露错位
     */
    public String readString() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte b;
        while ((b = buf.get()) != 0) {
            out.write(b);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * 读定长字节段——'t'/'b' 列值内容、LogicalMsg content 等（长度前缀已由调用方读出）。
     *
     * @param len 段长，为 0 返回空数组；越界由底层缓冲抛 BufferUnderflowException
     * @return 长度恰为 len 的副本数组
     */
    public byte[] readBytes(int len) {
        byte[] arr = new byte[len];
        buf.get(arr);
        return arr;
    }

    /**
     * PG 微秒时间戳转 Instant：加上 PG 纪元 946684800 秒偏移后按秒+纳秒重表示。
     * 用 floorDiv/floorMod 拆分（向负无穷取整），负微秒（PG 纪元之前的时刻）
     * 也落在正确的前一秒内——向零截断的除法会把 -1 微秒算成 +999999 纳秒。
     *
     * @param micros 距 2000-01-01 00:00:00 UTC 的微秒数，可负
     * @return 对应的 UTC Instant
     */
    public static Instant pgMicrosToInstant(long micros) {
        long seconds = PG_EPOCH_SECONDS + Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
