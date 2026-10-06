package org.vastdata.vbstream.walsource.layout;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 磁盘格式 tuple 载荷的手造字节 DSL（测试源码根，与被测同包）——按 kinds 词典逐列
 * 链式写入值，输出三种消费形态：
 * <ul>
 *   <li>{@link #payload()}：WAL 载荷形态 {@code [xl_heap_header 5B][自 tuple offset 23
 *       起的位图与数据]}——喂 {@code TupleDecoder.decodePayload}；</li>
 *   <li>{@link #tuple()}：页内完整 tuple 形态（23B 头前缀 + 位图 + 数据）——嵌入
 *       假页喂 {@code decodePageTuple}；</li>
 *   <li>entry 形态由测试自行在 {@link #payload()} 前补 2B datalen u16。</li>
 * </ul>
 *
 * <p>t_hoff 按写入列数自算：{@code MAXALIGN(23 + bitmapLen)}（无 null 列时位图省略，
 * t_hoff=24）；列间对齐相对 tuple 起点（t_hoff 起算，spike 发现 7），与本 DSL 独立
 * 转录的第二份布局理解互为双源。"dropped" 列恒按 null 位写入（attisdropped 列存储
 * 恒 NULL，spike 发现 26），不占数据字节。</p>
 */
public final class TupleBytes {

    /** infomask 的 HEAP_HASNULL 位（htup_details.h）——存在 null 列即置位。 */
    private static final int HEAP_HASNULL = 0x0001;

    /** infomask2 的 natts 位掩码（HEAP_NATTS_MASK）。 */
    private static final int HEAP_NATTS_MASK = 0x07FF;

    /** HeapTupleHeader 的 t_bits 偏移（offsetof(t_bits)=23）。 */
    private static final int TUPLE_BITS_OFFSET = 23;

    /** 1B varlena 头可编码的总长上限（含头 1B，即数据 ≤126B）。 */
    private static final int VARLENA_1B_MAX_TOTAL = 127;

    /** 单列已排队待编码的值：对齐要求 + 编码字节（对齐相对 tuple 起点）。 */
    private record Cell(int align, byte[] bytes) {
    }

    private final String[] kinds;
    private final Cell[] cells;
    private final boolean[] isNull;
    private int cursor;

    /**
     * 起点：声明逐列 kinds 词典（与被测解码词典同面）。
     *
     * @param kinds 每列一个解码 kind
     * @return 可链式写入的本 DSL 实例
     */
    public static TupleBytes of(String... kinds) {
        return new TupleBytes(kinds);
    }

    /**
     * 私有构造：按 kinds 长度分配列槽，dropped 列即刻置 null 位。
     *
     * @param kinds 列 kind 数组
     */
    private TupleBytes(String[] kinds) {
        this.kinds = kinds.clone();
        this.cells = new Cell[kinds.length];
        this.isNull = new boolean[kinds.length];
        for (int i = 0; i < kinds.length; i++) {
            if (DecodeKinds.DROPPED.equals(kinds[i])) {
                isNull[i] = true;   // attisdropped 列存储恒 NULL
            }
        }
    }

    /**
     * 标记 0-based 列为 null（null 位图占位，不写数据字节）。
     *
     * @param cols 列号（可变参，可反复调用）
     * @return 本实例（链式）
     * @throws IllegalArgumentException 列号越界
     */
    public TupleBytes nullAt(int... cols) {
        for (int c : cols) {
            if (c < 0 || c >= kinds.length) {
                throw new IllegalArgumentException("column index " + c + " out of 0.." + (kinds.length - 1));
            }
            isNull[c] = true;
        }
        return this;
    }

    /**
     * int2 列（i16 小端）。
     *
     * @param v 16 位值
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 int2
     */
    public TupleBytes i2(int v) {
        return scalar("int2", 2, new byte[]{(byte) v, (byte) (v >>> 8)});
    }

    /**
     * int4 列（i32 小端）。
     *
     * @param v 32 位值
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 int4
     */
    public TupleBytes i32(int v) {
        return scalar("int4", 4, le32(v));
    }

    /**
     * int8 列（i64 小端）。
     *
     * @param v 64 位值
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 int8
     */
    public TupleBytes i64(long v) {
        return scalar("int8", 8, le64(v));
    }

    /**
     * oid 列（u32 小端）。
     *
     * @param v 32 位 oid
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 oid
     */
    public TupleBytes oid(long v) {
        return scalar("oid", 4, le32((int) v));
    }

    /**
     * float4 列（IEEE 754 位模式小端）。
     *
     * @param v 浮点值
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 float4
     */
    public TupleBytes f4(float v) {
        return scalar("float4", 4, le32(Float.floatToRawIntBits(v)));
    }

    /**
     * float8 列（IEEE 754 位模式小端）。
     *
     * @param v 双精度值
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 float8
     */
    public TupleBytes f8(double v) {
        return scalar("float8", 8, le64(Double.doubleToRawLongBits(v)));
    }

    /**
     * bool 列（单字节 0/1）。
     *
     * @param v 布尔值
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 bool
     */
    public TupleBytes bool(boolean v) {
        return scalar("bool", 1, new byte[]{(byte) (v ? 1 : 0)});
    }

    /**
     * "char" 列（单字节内部类型字节）。
     *
     * @param c 字符（低 8 位写出）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 char
     */
    public TupleBytes ch(char c) {
        return scalar("char", 1, new byte[]{(byte) c});
    }

    /**
     * name 列（NameData 定宽 64B，尾部 NUL 补齐）。
     *
     * @param s 名字（编码后须 ≤63B，第 64 字节保留 NUL）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 name，或名字超 63B
     */
    public TupleBytes name(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > 63) {
            throw new IllegalStateException("name longer than 63 bytes: " + b.length);
        }
        byte[] cell = new byte[64];
        System.arraycopy(b, 0, cell, 0, b.length);   // 其余保持 0（NUL）
        return scalar("name", 4, cell);
    }

    /**
     * timestamp 列（i64 微秒原值，epoch 2000 计）。
     *
     * @param micros 自 2000-01-01 起的微秒数
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 timestamp
     */
    public TupleBytes ts(long micros) {
        return scalar("timestamp", 8, le64(micros));
    }

    /**
     * text 列（varlena，按总长自动选 1B 头或 4B 头；4B 头 = 总长&lt;&lt;2 u32le）。
     *
     * @param s 文本（UTF-8 编码）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 text
     */
    public TupleBytes text(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (1 + b.length <= VARLENA_1B_MAX_TOTAL) {
            byte[] cell = new byte[1 + b.length];
            cell[0] = (byte) ((1 + b.length) << 1 | 0x01);   // 1B 头：总长<<1|tag
            System.arraycopy(b, 0, cell, 1, b.length);
            return scalar("text", 4, cell);
        }
        return text4Bytes(b);
    }

    /**
     * text 列写行内压缩 varlena（4B 头 tag 0x02 + u32 tcinfo + 纯字面量 pglz 流——
     * 真实 pglz 的合法子集：control byte 0x00 后跟 ≤8 个字面量字节逐组重复）。
     *
     * @param s 被压缩的原文（UTF-8）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 text
     */
    public TupleBytes compressedText(String s) {
        byte[] raw = s.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32(out, (4L + 4 + pglzLiteralSize(raw.length)) << 2 | 0x02);   // 4B_C 头：总长<<2|tag
        writeU32(out, raw.length);                                            // tcinfo：exhdrlen 低 30 位（pglz 方法=0）
        for (int off = 0; off < raw.length; off += 8) {
            out.write(0x00);                                                  // control byte：8 个字面量位
            out.write(raw, off, Math.min(8, raw.length - off));
        }
        return scalar("text", 4, out.toByteArray());
    }

    /**
     * 纯字面量 pglz 流的字节数（每 8 个字面量一组，每组前缀 1 控制字节）。
     */
    private static int pglzLiteralSize(int rawLen) {
        return rawLen + (rawLen + 7) / 8;
    }

    /**
     * date 列（i32 天数自 2000-01-01，小端，4 对齐）。
     *
     * @param days 自 2000-01-01 起的天数（可为负）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 date
     */
    public TupleBytes date(int days) {
        return scalar("date", 4, le32(days));
    }

    /**
     * time 列（i64 微秒自当日零点，小端，8 对齐）。
     *
     * @param micros 微秒数
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 time
     */
    public TupleBytes timeMicros(long micros) {
        return scalar("time", 8, le64(micros));
    }

    /**
     * timetz 列（i64 微秒 + i32 区偏移秒，共 12B 定宽，8 对齐）。
     *
     * @param micros      时间微秒
     * @param zoneSeconds 区偏移秒（PG TimeZoneADT.zone）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 timetz
     */
    public TupleBytes timetzMicros(long micros, int zoneSeconds) {
        byte[] cell = new byte[12];
        byte[] t = le64(micros);
        byte[] z = le32(zoneSeconds);
        System.arraycopy(t, 0, cell, 0, 8);
        System.arraycopy(z, 0, cell, 8, 4);
        return scalar("timetz", 8, cell);
    }

    /**
     * timestamptz 列（i64 微秒 epoch 2000 UTC，小端，8 对齐）。
     *
     * @param micros 自 2000-01-01T00:00:00Z 起微秒
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 timestamptz
     */
    public TupleBytes tstz(long micros) {
        return scalar("timestamptz", 8, le64(micros));
    }

    /**
     * uuid 列（16 字节网络序原文，'c' 对齐即 1 字节对齐）。
     *
     * @param b 16 字节 uuid
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 uuid
     */
    public TupleBytes uuidBytes(byte[] b) {
        if (b.length != 16) {
            throw new IllegalStateException("uuid must be 16 bytes: " + b.length);
        }
        return scalar("uuid", 1, b.clone());
    }

    /**
     * numeric 列（varlena 内容 = 已剥头的磁盘格式，恒 4B varlena 头包裹——首 u16 的
     * flag 位自证短/长/特殊格式）。
     *
     * @param content 磁盘格式内容字节（短格式 ≥2B / 长格式 ≥4B）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 numeric
     */
    public TupleBytes numericContent(byte[] content) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32(out, (4L + content.length) << 2);
        out.writeBytes(content);
        return scalar("numeric", 4, out.toByteArray());
    }

    /**
     * text 列写 18B external 短指针（0x01/0x12 + 四 u32——行内 TOAST 接线用例的
     * 手造形态，对齐 'i' 即 4）。
     *
     * @param rawsize    原始 varlena 总长（载荷 + 4B 头）
     * @param extsize    外部存储字节数（== chunk 拼接总长）
     * @param valueid    toast chunk_id
     * @param toastrelid toast 关系 oid
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 text
     */
    public TupleBytes externalPointer(long rawsize, long extsize, long valueid, long toastrelid) {
        byte[] cell = new byte[18];
        cell[0] = 0x01;
        cell[1] = 0x12;
        put32(cell, 2, rawsize);
        put32(cell, 6, extsize);
        put32(cell, 10, valueid);
        put32(cell, 14, toastrelid);
        return scalar("text", 4, cell);
    }

    /**
     * text 列（强制 4B varlena 头）——直击解码端 {@code u32le>>>2} 读长的防 BE 回归用例。
     *
     * @param s 文本（UTF-8 编码）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 text
     */
    public TupleBytes text4(String s) {
        return text4Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * bytea 列（varlena，按总长自动选 1B 头或 4B 头——与 {@link #text(String)} 同一
     * 头逻辑，载荷不解码直接写原文）。
     *
     * @param b 载荷字节（原样写入 varlena 载荷区）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 bytea
     */
    public TupleBytes bytes(byte[] b) {
        if (1 + b.length <= VARLENA_1B_MAX_TOTAL) {
            byte[] cell = new byte[1 + b.length];
            cell[0] = (byte) ((1 + b.length) << 1 | 0x01);   // 1B 头：总长<<1|tag
            System.arraycopy(b, 0, cell, 1, b.length);
            return scalar("bytea", 4, cell);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32(out, (4L + b.length) << 2);
        out.writeBytes(b);
        return scalar("bytea", 4, out.toByteArray());
    }

    /**
     * skip 列：写一个 varlena 供解码端走读消耗（值不物化）。
     *
     * @param s 被跳过的文本（UTF-8 编码）
     * @return 本实例（链式）
     * @throws IllegalStateException 下一待写列 kind 不是 skip
     */
    public TupleBytes skipVarlena(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (1 + b.length <= VARLENA_1B_MAX_TOTAL) {
            byte[] cell = new byte[1 + b.length];
            cell[0] = (byte) ((1 + b.length) << 1 | 0x01);
            System.arraycopy(b, 0, cell, 1, b.length);
            return scalar("skip", 4, cell);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32(out, (4L + b.length) << 2);
        out.writeBytes(b);
        return scalar("skip", 4, out.toByteArray());
    }

    /**
     * 数据区起点（自 tuple 起算的 t_hoff）：MAXALIGN(23 + bitmapLen)。
     *
     * @return t_hoff 字节数
     */
    public int tHoff() {
        return align(TUPLE_BITS_OFFSET + bitmapLen(), 8);
    }

    /**
     * WAL 载荷形态字节：{@code [infomask2 u16][infomask u16][t_hoff u8][位图][数据]}。
     *
     * <p>调用方解码入口：{@code decoder.decodePayload(bytes, 0, kinds)}。</p>
     *
     * @return 载荷字节数组
     * @throws IllegalStateException 存在既非 null 又未写入值的列（DSL 误用）
     */
    public byte[] payload() {
        byte[] data = dataRegion();
        byte[] out = new byte[5 + data.length];
        writeU16(out, 0, natts());
        writeU16(out, 2, hasNull() ? HEAP_HASNULL : 0);
        out[4] = (byte) tHoff();
        System.arraycopy(data, 0, out, 5, data.length);
        return out;
    }

    /**
     * 页内完整 tuple 形态字节：23B 头前缀（infomask2@18、infomask@20、t_hoff@22）
     * + 位图 + 数据。嵌入假页任意偏移即得 {@code decodePageTuple} 输入。
     *
     * @return tuple 字节数组（长度 = t_hoff + 数据区净长）
     * @throws IllegalStateException 存在既非 null 又未写入值的列（DSL 误用）
     */
    public byte[] tuple() {
        byte[] data = dataRegion();
        byte[] out = new byte[TUPLE_BITS_OFFSET + data.length];
        writeU16(out, 18, natts());
        writeU16(out, 20, hasNull() ? HEAP_HASNULL : 0);
        out[22] = (byte) tHoff();
        System.arraycopy(data, 0, out, TUPLE_BITS_OFFSET, data.length);
        return out;
    }

    /**
     * 位图 + 数据区（即 tuple offset 23 起的全部字节）。
     *
     * <p>关键步骤：逐列走写——null/dropped 列跳过；其余列先按类型对齐（相对 tuple
     * 起点，自 t_hoff 起算）补零再写字节。边界与异常语义：未写值的活列抛 ISE。</p>
     *
     * @return 自 offset 23 起的字节
     * @throws IllegalStateException 活列缺值
     */
    private byte[] dataRegion() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (hasNull()) {
            byte[] bitmap = new byte[bitmapLen()];
            for (int i = 0; i < kinds.length; i++) {
                if (!isNull[i]) {
                    bitmap[i / 8] |= (byte) (1 << (i % 8));   // 1=非 null
                }
            }
            out.writeBytes(bitmap);
        }
        int pos = tHoff();   // 下一列的 tuple 起点相对偏移（位图已含在内）
        for (int i = 0; i < kinds.length; i++) {
            if (isNull[i]) {
                continue;
            }
            Cell c = cells[i];
            if (c == null) {
                throw new IllegalStateException("column " + i + " (kind=" + kinds[i] + ") has no value and is not null");
            }
            int target = align(pos, c.align());   // 对齐相对 tuple 起点（spike 发现 7）
            while (TUPLE_BITS_OFFSET + out.size() < target) {
                out.write(0);   // 垫到目标 tuple 偏移
            }
            out.writeBytes(c.bytes());
            pos = target + c.bytes().length;
        }
        return out.toByteArray();
    }

    /**
     * 把下一待写活列排队为定宽标量，并校验 kind 匹配。
     *
     * @param expect 期望 kind
     * @param align  对齐要求（1/2/4/8）
     * @param bytes  编码字节
     * @return 本实例（链式）
     * @throws IllegalStateException 无待写活列或 kind 不匹配
     */
    private TupleBytes scalar(String expect, int align, byte[] bytes) {
        int i = nextLiveColumn();
        if (!expect.equals(kinds[i])) {
            throw new IllegalStateException("writer for " + expect + " but column " + i + " kind is " + kinds[i]);
        }
        cells[i] = new Cell(align, bytes);
        return this;
    }

    /**
     * 推进游标找到下一个非 null 且未写的列。
     *
     * @return 列号
     * @throws IllegalStateException 已无待写活列（写入次数超过活列数）
     */
    private int nextLiveColumn() {
        while (cursor < kinds.length && (isNull[cursor] || cells[cursor] != null)) {
            cursor++;
        }
        if (cursor >= kinds.length) {
            throw new IllegalStateException("no live column left to write");
        }
        return cursor;
    }

    /**
     * 强制 4B varlena 头写入 text/skip 列。
     *
     * @param b 文本字节
     * @return 本实例（链式）
     */
    private TupleBytes text4Bytes(byte[] b) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeU32(out, (4L + b.length) << 2);   // 4B 头：总长<<2，u32le（spike 发现 21）
        out.writeBytes(b);
        return scalar("text", 4, out.toByteArray());
    }

    /**
     * 是否存在任何 null 位（dropped 自动计入）。
     *
     * @return 有 null 列 true
     */
    private boolean hasNull() {
        for (boolean b : isNull) {
            if (b) {
                return true;
            }
        }
        return false;
    }

    /**
     * null 位图字节数（(natts+7)/8；无 null 列为 0——PG 不存位图）。
     *
     * @return 位图字节数
     */
    private int bitmapLen() {
        return hasNull() ? (kinds.length + 7) / 8 : 0;
    }

    /**
     * infomask2 中的 natts 位段值。
     *
     * @return natts | 0（本 DSL 不写其它 infomask2 位）
     */
    private int natts() {
        return kinds.length & HEAP_NATTS_MASK;
    }

    /**
     * 就地写 little-endian u16。
     *
     * @param a 目标数组
     * @param o 偏移
     * @param v 16 位值
     */
    private static void writeU16(byte[] a, int o, int v) {
        a[o] = (byte) v;
        a[o + 1] = (byte) (v >>> 8);
    }

    /**
     * 向流写 little-endian u32。
     *
     * @param out 目标流
     * @param v   32 位值
     */
    private static void writeU32(ByteArrayOutputStream out, long v) {
        out.write((int) (v & 0xFF));
        out.write((int) ((v >>> 8) & 0xFF));
        out.write((int) ((v >>> 16) & 0xFF));
        out.write((int) ((v >>> 24) & 0xFF));
    }

    /**
     * little-endian u32 字节。
     *
     * @param v 32 位值
     * @return 4 字节
     */
    private static byte[] le32(int v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }

    /**
     * 就地写 little-endian u32（external 指针四字段）。
     *
     * @param target 目标数组
     * @param offset 起始偏移
     * @param v      32 位值
     */
    private static void put32(byte[] target, int offset, long v) {
        target[offset] = (byte) (v & 0xFF);
        target[offset + 1] = (byte) ((v >>> 8) & 0xFF);
        target[offset + 2] = (byte) ((v >>> 16) & 0xFF);
        target[offset + 3] = (byte) ((v >>> 24) & 0xFF);
    }

    /**
     * little-endian u64 字节。
     *
     * @param v 64 位值
     * @return 8 字节
     */
    private static byte[] le64(long v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24),
                (byte) (v >>> 32), (byte) (v >>> 40), (byte) (v >>> 48), (byte) (v >>> 56)};
    }

    /**
     * MAXALIGN 向上取整（8 的倍数）。
     *
     * @param v 值
     * @param a 对齐要求（2 的幂）
     * @return 对齐后的值
     */
    private static int align(int v, int a) {
        return (v + a - 1) & ~(a - 1);
    }
}
