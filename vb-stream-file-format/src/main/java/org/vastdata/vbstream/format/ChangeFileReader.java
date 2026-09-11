package org.vastdata.vbstream.format;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

/**
 * VBFG 落地文件流式读取器。布局见 {@link ChangeFileWriter}。
 * 迭代到 FOOTER 时校验记录数与 CRC32；截断（记录中间 EOF）、损坏、CRC 不一致均抛
 * {@link IOException}（迭代侧包装为 {@link UncheckedIOException}），{@link #complete()}
 * 表示已读到 FOOTER（文件完整）。
 *
 * <p>Reader 是流式 {@code Iterator<Record>}，惰性读、常量内存。TABLE_DEF 按需注册到
 * defId 映射，EVENT/TRUNCATE 引用未注册的 defId 即损坏文件。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 ChangeFileReader（2026-09-11 快照，
 * 与 Writer 同步移植——作 RoundTrip 锚定与落地文件校验工具）。
 */
public final class ChangeFileReader implements AutoCloseable, Iterable<Record> {

    private final DataInputStream in;
    private final CRC32 crc = new CRC32();
    private final Map<Integer, TableDef> defs = new HashMap<>();
    private final int seq;
    private final String sourceDb;
    private boolean footerSeen;
    private long recordCount;

    /**
     * 私有构造：读并校验文件头（Magic/version/seq/sourceDb）。
     * 边界：Magic 不符（非 VBFG 文件）或 version 不等于 {@code ChangeFileWriter.VERSION}
     * 抛 IOException。
     */
    private ChangeFileReader(DataInputStream in) throws IOException {
        this.in = in;
        byte[] magic = new byte[ChangeFileWriter.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(magic, ChangeFileWriter.MAGIC)) {
            throw new IOException("非法文件头（非 VBFG 落地文件）");
        }
        int version = in.readUnsignedByte();
        if (version != ChangeFileWriter.VERSION) {
            throw new IOException("不支持的版本: " + version);
        }
        this.seq = (int) Varint.readUnsigned(in);
        this.sourceDb = readString(in);
    }

    /**
     * 责任：打开文件建立读取器。边界：文件头读失败（不存在/非 VBFG/版本不符）时关闭
     * 输入流后原样上抛（不留半开句柄）。
     */
    public static ChangeFileReader open(Path file) throws IOException {
        DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile()), 1 << 16));
        try {
            return new ChangeFileReader(in);
        } catch (IOException | RuntimeException e) {
            in.close();
            throw e;
        }
    }

    /** 文件头携带的序号（与文件名 seq 同源）。 */
    public int seq() {
        return seq;
    }

    /** 文件头携带的源库名（写入侧的 task 标识）。 */
    public String sourceDb() {
        return sourceDb;
    }

    /** 是否已读到 FOOTER（迭代完成的另一种判据——文件完整且 CRC 通过）。 */
    public boolean complete() {
        return footerSeen;
    }

    @Override
    public Iterator<Record> iterator() {
        return new Iterator<>() {
            private Record next;

            @Override
            public boolean hasNext() {
                if (next == null && !footerSeen) {
                    try {
                        next = readNext();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return next != null;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Record r = next;
                next = null;
                return r;
            }
        };
    }

    /**
     * 责任：读下一条记录。关键步骤：逐字节读 varint 长度前缀（是否计 CRC 待记录类型
     * 识别后决定）→ 读载荷 → 按类型字节分发解析；FOOTER 校验记录数与 CRC 后置
     * {@code footerSeen} 并返回 null（迭代终止）；其余记录的长度前缀与载荷都计入 CRC。
     * 边界：载荷中途 EOF 即截断文件，抛 IOException。
     */
    private Record readNext() throws IOException {
        if (footerSeen) {
            return null;
        }
        byte[] lenBytes = readVarintBytes();
        long len = decodeUnsigned(lenBytes);
        if (len > Integer.MAX_VALUE) {
            throw new IOException("记录长度超限: " + len);
        }
        byte[] payload = new byte[(int) len];
        try {
            in.readFully(payload);
        } catch (EOFException e) {
            throw new IOException("文件在记录中间截断", e);
        }

        DataInputStream p = new DataInputStream(new ByteArrayInputStream(payload));
        int type = p.readUnsignedByte();
        Record record = switch (type) {
            case ChangeFileWriter.T_TABLE_DEF -> readTableDef(p);
            case ChangeFileWriter.T_BEGIN ->
                    new BeginRecord(Varint.readZigzag(p), Varint.readZigzag(p));
            case ChangeFileWriter.T_EVENT -> readEvent(p);
            case ChangeFileWriter.T_COMMIT ->
                    new CommitRecord(Varint.readZigzag(p), Varint.readZigzag(p), Varint.readZigzag(p));
            case ChangeFileWriter.T_TRUNCATE -> new TruncateRecord(defs.get((int) Varint.readUnsigned(p)));
            case ChangeFileWriter.T_FOOTER -> readFooter(p);
            default -> throw new IOException("未知记录类型: " + type);
        };
        if (type != ChangeFileWriter.T_FOOTER) {
            // FOOTER 自身不进 CRC 与记录数计数；其余记录的长度前缀与载荷都计入
            crc.update(lenBytes);
            crc.update(payload);
            recordCount++;
        }
        return record;
    }

    /** 责任：解析 TABLE_DEF 并注册 defId 映射（key 下标还原为列名）。 */
    private Record readTableDef(DataInputStream p) throws IOException {
        int id = (int) Varint.readUnsigned(p);
        String db = readString(p);
        String schema = readString(p);
        String table = readString(p);
        int keyCount = (int) Varint.readUnsigned(p);
        List<Integer> keyIdx = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keyIdx.add((int) Varint.readUnsigned(p));
        }
        int colCount = (int) Varint.readUnsigned(p);
        List<ColumnDef> columns = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            columns.add(new ColumnDef(readString(p), TypeCode.of(p.readUnsignedByte())));
        }
        List<String> keys = keyIdx.stream().map(i -> columns.get(i).name()).toList();
        TableDef def = new TableDef(id, db, schema, table, keys, columns);
        defs.put(id, def);
        return new TableDefRecord(def);
    }

    /** 责任：解析 EVENT——null 位图先行，非 null 值按列序解码。边界：defId 未注册抛 IOException。 */
    private Record readEvent(DataInputStream p) throws IOException {
        int defId = (int) Varint.readUnsigned(p);
        Op op = Op.of(p.readUnsignedByte());
        TableDef def = defs.get(defId);
        if (def == null) {
            throw new IOException("事件引用了未定义的表定义 id: " + defId);
        }
        int n = def.columns().size();
        byte[] bitmap = new byte[(n + 7) >> 3];
        p.readFully(bitmap);
        Object[] values = new Object[n];
        for (int i = 0; i < n; i++) {
            boolean isNull = (bitmap[i >> 3] & (1 << (i & 7))) != 0;
            values[i] = isNull ? null : Values.read(p, def.columns().get(i).type());
        }
        return new EventRecord(defId, op, values);
    }

    /**
     * 责任：解析 FOOTER 并做完整性校验——记录数与实际迭代数一致、CRC32 与累计值一致，
     * 任一不符抛 IOException（传输/存储损坏信号）；通过则置 {@code footerSeen} 返回 null。
     */
    private Record readFooter(DataInputStream p) throws IOException {
        long count = Varint.readUnsigned(p);
        long expectedCrc = p.readLong();
        if (count != recordCount) {
            throw new IOException("记录数不一致: 期望 %d, 实际 %d".formatted(count, recordCount));
        }
        if (expectedCrc != crc.getValue()) {
            throw new IOException("CRC32 校验失败: 文件在传输或存储中损坏");
        }
        footerSeen = true;
        return null;
    }

    /** 逐字节读无符号 varint，返回原始字节（是否计入 CRC 由调用方在识别记录类型后决定）。 */
    private byte[] readVarintBytes() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(8);
        for (int shift = 0; shift < 64; shift += 7) {
            int b = in.read();
            if (b < 0) {
                throw new EOFException("文件在记录中间截断");
            }
            buf.write(b);
            if ((b & 0x80) == 0) {
                return buf.toByteArray();
            }
        }
        throw new IOException("varint 超过 64 位，数据可能损坏");
    }

    private static long decodeUnsigned(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < bytes.length; i++) {
            result |= (long) (bytes[i] & 0x7F) << (i * 7);
        }
        return result;
    }

    /** 责任：读长度前缀 UTF-8 字符串。边界：长度超 int 域视为损坏抛 IOException。 */
    private static String readString(DataInput in) throws IOException {
        long len = Varint.readUnsigned(in);
        if (len > Integer.MAX_VALUE) {
            throw new IOException("字符串长度超限: " + len);
        }
        byte[] bytes = new byte[(int) len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
