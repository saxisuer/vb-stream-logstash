package org.vastdata.vbstream.reader.format;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * VBFG 落地文件写入器。文件布局（变长整数见 {@link Varint}）：
 *
 * <pre>
 * [Magic "VBFG"][version][seq][sourceDb]          —— 文件头，不进 CRC
 * 记录流（每条 = [长度][记录类型 + 载荷]，全部计入 CRC32）:
 *   TABLE_DEF: defId, db, schema, table, keyColIdx[], cols[]{name, typeCode}
 *   BEGIN:     txid, lsn
 *   EVENT:     defId, op, nullBitmap, 非null值序列（按列序）
 *   COMMIT:    txid, lsn, ts
 *   TRUNCATE:  defId
 *   FOOTER:    recordCount, crc32                 —— 最后一条，自身不进 CRC
 * </pre>
 *
 * 同一表（结构未变）只写一条 TABLE_DEF，事件通过 defId 引用；列结构变化分配新 id。
 * 文件应写到临时目录，{@link #finish()} 完成 fsync 后由调用方原子 rename 到数据目录
 * （见 file 包 {@code FileRollingWriter} 的 publish 语义）。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 ChangeFileWriter（2026-09-11 快照）——
 * <b>字节布局跨仓契约，任何不兼容变更须递增 VERSION 并两仓同步</b>。
 */
public final class ChangeFileWriter implements AutoCloseable {

    static final byte[] MAGIC = {'V', 'B', 'F', 'G'};
    static final int VERSION = 1;

    static final int T_TABLE_DEF = 1;
    static final int T_BEGIN = 2;
    static final int T_EVENT = 3;
    static final int T_COMMIT = 4;
    static final int T_TRUNCATE = 5;
    static final int T_FOOTER = 6;

    private final FileOutputStream fos;
    private final DataOutputStream out;
    private final CRC32 crc = new CRC32();
    private final Map<TableDef, TableDef> defsInFile = new HashMap<>();
    private int nextDefId = 1;
    private long recordCount;
    private boolean finished;

    /**
     * 责任：打开目标文件并写文件头（Magic/version/seq/sourceDb——不进 CRC）。
     * 边界：文件已存在则从头部覆写（调用方约定写 tmp 目录的全新文件）。
     */
    public ChangeFileWriter(Path file, int seq, String sourceDb) throws IOException {
        this.fos = new FileOutputStream(file.toFile());
        this.out = new DataOutputStream(new java.io.BufferedOutputStream(fos));
        out.write(MAGIC);
        out.writeByte(VERSION);
        Varint.writeUnsigned(out, seq);
        writeString(out, sourceDb);
    }

    /** 责任：写 BEGIN 记录（txid/lsn 均 zigzag 有符号）。 */
    public void writeBegin(long txid, long lsn) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(24);
        DataOutputStream p = new DataOutputStream(payload);
        p.writeByte(T_BEGIN);
        Varint.writeZigzag(p, txid);
        Varint.writeZigzag(p, lsn);
        p.flush();
        writeRecord(payload.toByteArray());
    }

    /** 责任：写 COMMIT 记录（文件切分只发生在其后——每份文件自包含完整事务）。 */
    public void writeCommit(long txid, long lsn, long timestamp) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(28);
        DataOutputStream p = new DataOutputStream(payload);
        p.writeByte(T_COMMIT);
        Varint.writeZigzag(p, txid);
        Varint.writeZigzag(p, lsn);
        Varint.writeZigzag(p, timestamp);
        p.flush();
        writeRecord(payload.toByteArray());
    }

    /**
     * 责任：写 DML 事件——表定义首次出现（或结构变化）时自动先写 TABLE_DEF。
     * 边界：values 长度与 def.columns 不符抛 IAE（上游解析对齐性破坏的信号）；
     * values 中的 null 由 null 位图表达、不进值序列（列序严格对齐）。
     */
    public void writeEvent(TableDef def, Op op, Object[] values) throws IOException {
        if (values.length != def.columns().size()) {
            throw new IllegalArgumentException("列数不匹配: 表 %s 期望 %d 列, 实际 %d 列"
                    .formatted(def.table(), def.columns().size(), values.length));
        }
        TableDef canonical = resolveDef(def);

        ByteArrayOutputStream payload = new ByteArrayOutputStream(64);
        DataOutputStream p = new DataOutputStream(payload);
        p.writeByte(T_EVENT);
        Varint.writeUnsigned(p, canonical.id());
        p.writeByte(op.id());
        writeNullBitmap(p, values);
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                Values.write(p, canonical.columns().get(i).type(), values[i]);
            }
        }
        p.flush();
        writeRecord(payload.toByteArray());
    }

    /** 责任：写 TRUNCATE 记录（表定义按需先行注册）。 */
    public void writeTruncate(TableDef def) throws IOException {
        TableDef canonical = resolveDef(def);
        ByteArrayOutputStream payload = new ByteArrayOutputStream(8);
        DataOutputStream p = new DataOutputStream(payload);
        p.writeByte(T_TRUNCATE);
        Varint.writeUnsigned(p, canonical.id());
        p.flush();
        writeRecord(payload.toByteArray());
    }

    /**
     * 责任：写 FOOTER（记录数 + CRC）、flush 并 fsync——之后的 close 不再写任何内容。
     * 幂等：重复调用直接返回。
     */
    public void finish() throws IOException {
        if (finished) {
            return;
        }
        ByteArrayOutputStream payload = new ByteArrayOutputStream(20);
        DataOutputStream p = new DataOutputStream(payload);
        p.writeByte(T_FOOTER);
        Varint.writeUnsigned(p, recordCount);
        p.writeLong(crc.getValue());
        p.flush();
        // FOOTER 自身不进 CRC：长度前缀与载荷直接写
        byte[] bytes = payload.toByteArray();
        out.write(unsignedBytes(bytes.length));
        out.write(bytes);
        out.flush();
        fos.getFD().sync();
        finished = true;
    }

    /** 已写入的记录数（含 TABLE_DEF/BEGIN/EVENT/COMMIT/TRUNCATE，不含 FOOTER）。 */
    public long recordCount() {
        return recordCount;
    }

    /**
     * 责任：返回文件内已注册的规范定义（含 id）；未注册则先写 TABLE_DEF 并注册
     * （defId 从 1 起分配——文件内作用域）。TABLE_DEF 写失败转 UncheckedIOException
     * （签名约束：本方法被无 throws 的 resolveDef 调用链使用）。
     */
    private TableDef resolveDef(TableDef def) {
        TableDef existing = defsInFile.get(def);
        if (existing != null) {
            return existing;
        }
        TableDef withId = new TableDef(nextDefId++, def.db(), def.schema(), def.table(),
                def.keyColumns(), def.columns());
        try {
            writeTableDef(withId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        defsInFile.put(withId, withId);
        return withId;
    }

    /** 责任：写 TABLE_DEF 记录——key 列以下标表达（避免重复存列名）。 */
    private void writeTableDef(TableDef def) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream(128);
        DataOutputStream p = new DataOutputStream(payload);
        p.writeByte(T_TABLE_DEF);
        Varint.writeUnsigned(p, def.id());
        writeString(p, def.db());
        writeString(p, def.schema());
        writeString(p, def.table());
        Varint.writeUnsigned(p, def.keyColumns().size());
        for (String key : def.keyColumns()) {
            Varint.writeUnsigned(p, indexOfColumn(def, key));
        }
        Varint.writeUnsigned(p, def.columns().size());
        for (ColumnDef col : def.columns()) {
            writeString(p, col.name());
            p.writeByte(col.type().id());
        }
        p.flush();
        writeRecord(payload.toByteArray());
    }

    /** 责任：key 列名 → 列下标。边界：key 列不在列定义中抛 IAE（上游构造的 TableDef 自相矛盾）。 */
    private static int indexOfColumn(TableDef def, String column) {
        for (int i = 0; i < def.columns().size(); i++) {
            if (def.columns().get(i).name().equals(column)) {
                return i;
            }
        }
        throw new IllegalArgumentException("key 列 %s 不在表 %s 的列定义中".formatted(column, def.table()));
    }

    /** null 位图：bit i 置位表示第 i 列为 null（LSB 在前）。 */
    private static void writeNullBitmap(DataOutputStream p, Object[] values) throws IOException {
        byte[] bitmap = new byte[(values.length + 7) >> 3];
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                bitmap[i >> 3] |= (byte) (1 << (i & 7));
            }
        }
        p.write(bitmap);
    }

    /** 责任：写一条记录（长度前缀 + 载荷），两者都计入 CRC32，recordCount 自增。 */
    private void writeRecord(byte[] payload) throws IOException {
        byte[] lenBytes = unsignedBytes(payload.length);
        crc.update(lenBytes);
        crc.update(payload);
        out.write(lenBytes);
        out.write(payload);
        recordCount++;
    }

    private static byte[] unsignedBytes(long value) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(8);
        DataOutputStream d = new DataOutputStream(buf);
        Varint.writeUnsigned(d, value);
        return buf.toByteArray();
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Varint.writeUnsigned(out, bytes.length);
        out.write(bytes);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
