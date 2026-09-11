package org.vastdata.vbstream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ChangeFileWriter} → {@link ChangeFileReader} RoundTrip 单测——移植正确性的锚定面:
 * 全记录类型往返、null 位图、多表 defId 去重、文件头元数据、FOOTER 的记录数/CRC 完整性
 * 校验(含破坏检测)。布局契约见类 javadoc(与 vb-cdc-file-transform 的 cdc-file-format
 * 逐字节互通)。
 */
class ChangeFileIOTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /**
     * 场景:全记录类型 roundtrip——TABLE_DEF(自动)/BEGIN/EVENT(含 null 与全类型值)/
     * TRUNCATE/COMMIT 顺序写,读回记录序、值、defId 引用全等;文件头 seq/sourceDb 可读回;
     * 迭代到 FOOTER 后 complete() 为真。
     */
    @Test
    void roundTripFullFile() throws IOException {
        Path file = dir.resolve("rt.bin");
        TableDef def = TableDef.of("db1", "public", "t1",
                List.of("id"),
                List.of(new ColumnDef("id", TypeCode.INT), new ColumnDef("name", TypeCode.STRING),
                        new ColumnDef("flag", TypeCode.BOOL), new ColumnDef("score", TypeCode.FLOAT64),
                        new ColumnDef("ratio", TypeCode.FLOAT32), new ColumnDef("raw", TypeCode.BYTES),
                        new ColumnDef("ts", TypeCode.TIMESTAMP), new ColumnDef("tsz", TypeCode.TIMESTAMPTZ),
                        new ColumnDef("d", TypeCode.DATE), new ColumnDef("t", TypeCode.TIME),
                        new ColumnDef("iv", TypeCode.INTERVAL)));
        Object[] values = {42L, "hello 世界", true, 3.14, 1.5f, new byte[]{1, 2, (byte) 0xFF},
                "2026-09-11 10:00:00.000000", "2026-09-11T02:00:00.000000Z",
                "2026-09-11", "10:00:00.000000", "123456 microseconds"};
        Object[] withNulls = {7L, null, false, null, 0.5f, null, null, null, null, null, null};

        try (ChangeFileWriter w = new ChangeFileWriter(file, 9, "task-x")) {
            w.writeBegin(769L, 0x3A21C60L);
            w.writeEvent(def, Op.CREATE, values);
            w.writeEvent(def, Op.UPDATE, withNulls);
            w.writeTruncate(def);
            w.writeCommit(769L, 0x3A21D00L, 1_760_000_000_000L);
            w.finish();
        }

        try (ChangeFileReader r = ChangeFileReader.open(file)) {
            assertEquals(9, r.seq(), "文件头 seq");
            assertEquals("task-x", r.sourceDb(), "文件头 sourceDb");
            List<Record> records = new ArrayList<>();
            for (Record rec : r) {
                records.add(rec);
            }
            assertTrue(r.complete(), "读到 FOOTER 后 complete");
            assertEquals(6, records.size(), "BEGIN + TABLE_DEF + 2*EVENT + TRUNCATE + COMMIT 共 6 条(TRUNCATE 复用同表 def 不另写 TABLE_DEF)");
            // BEGIN 先写(writeBegin),TABLE_DEF 随首个事件自动写入——故在 BEGIN 之后
            BeginRecord begin = (BeginRecord) records.get(0);
            TableDefRecord td = (TableDefRecord) records.get(1);
            assertEquals(1, td.tableDef().id(), "defId 从 1 分配");
            assertTrue(td.tableDef().sameDefinition(def), "TABLE_DEF 业务定义往返一致");
            assertEquals(769L, begin.txid());
            assertEquals(0x3A21C60L, begin.lsn());
            // EVENT 1:全类型值
            EventRecord e1 = (EventRecord) records.get(2);
            assertEquals(Op.CREATE, e1.op());
            assertEquals(1, e1.tableDefId());
            assertArrayEquals(values, e1.values(), "全类型值往返");
            // EVENT 2:null 位图
            EventRecord e2 = (EventRecord) records.get(3);
            assertEquals(Op.UPDATE, e2.op());
            assertEquals(7L, e2.values()[0]);
            assertNull(e2.values()[1], "null 列经位图还原");
            assertEquals(false, e2.values()[2]);
            assertNull(e2.values()[3]);
            // TRUNCATE
            TruncateRecord tr = (TruncateRecord) records.get(4);
            assertEquals(1, tr.tableDef().id(), "TRUNCATE 复用同表 defId");
            // COMMIT 是第 6 条
            CommitRecord commit = (CommitRecord) records.get(5);
            assertEquals(769L, commit.txid());
            assertEquals(0x3A21D00L, commit.lsn());
            assertEquals(1_760_000_000_000L, commit.timestamp());
        }
    }

    /**
     * 场景:多表与结构变化——两表分别注册 defId 1/2;同表同结构去重(不再写 TABLE_DEF);
     * 列结构变化分配新 defId(3)。
     */
    @Test
    void defIdAllocationAndDedup() throws IOException {
        Path file = dir.resolve("defs.bin");
        TableDef t1 = TableDef.of("db", "public", "a", List.of(),
                List.of(new ColumnDef("x", TypeCode.INT)));
        TableDef t1Again = TableDef.of("db", "public", "a", List.of(),
                List.of(new ColumnDef("x", TypeCode.INT)));
        TableDef t2 = TableDef.of("db", "public", "b", List.of(),
                List.of(new ColumnDef("y", TypeCode.STRING)));
        TableDef t1V2 = TableDef.of("db", "public", "a", List.of(),
                List.of(new ColumnDef("x", TypeCode.INT), new ColumnDef("x2", TypeCode.INT)));
        try (ChangeFileWriter w = new ChangeFileWriter(file, 1, "s")) {
            w.writeEvent(t1, Op.CREATE, new Object[]{1L});
            w.writeEvent(t1Again, Op.CREATE, new Object[]{2L});   // 同结构去重
            w.writeEvent(t2, Op.CREATE, new Object[]{"v"});
            w.writeEvent(t1V2, Op.CREATE, new Object[]{3L, 4L});  // 结构变化新 id
            w.finish();
        }
        try (ChangeFileReader r = ChangeFileReader.open(file)) {
            List<Record> records = new ArrayList<>();
            for (Record rec : r) {
                records.add(rec);
            }
            // 记录序 = [TD a, E(t1), E(t1 复用), TD b, E(t2), TD a-v2, E(t1V2)] 共 7 条
            assertEquals(7, records.size(), "去重后 3 TABLE_DEF + 4 EVENT");
            assertEquals(1, ((EventRecord) records.get(1)).tableDefId(), "a 第二事件复用 defId 1");
            assertEquals(1, ((TableDefRecord) records.get(0)).tableDef().id());
            assertEquals(2, ((TableDefRecord) records.get(3)).tableDef().id(), "b 分配 defId 2");
            assertEquals(3, ((TableDefRecord) records.get(5)).tableDef().id(), "a 结构变化分配 defId 3");
        }
    }

    /**
     * 场景:CRC 完整性——发布后的文件字节被篡改(中段翻一个位),读侧迭代到 FOOTER 时抛
     * UncheckedIOException(CRC 校验失败)。
     */
    @Test
    void corruptedFileFailsCrcCheck() throws IOException {
        Path file = writeMinimalFile();
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length / 2] ^= 0x01;   // 中段翻位模拟传输/存储损坏
        Path corrupted = dir.resolve("corrupted.bin");
        Files.write(corrupted, bytes);
        try (ChangeFileReader r = ChangeFileReader.open(corrupted)) {
            assertThrows(RuntimeException.class, () -> {
                for (Record ignored : r) {
                    // 迭代到 FOOTER 校验
                }
            }, "损坏文件应抛(中段翻位可落在类型码/长度/CRC 任一处——IAE 或 IOException 包装)");
        }
    }

    /**
     * 场景:截断检测——文件在末条记录中间被截断(FOOTER 缺失),读侧抛 UncheckedIOException
     * ("文件在记录中间截断")且 complete() 为假。
     */
    @Test
    void truncatedFileRejected() throws IOException {
        Path file = writeMinimalFile();
        byte[] bytes = Files.readAllBytes(file);
        Path truncated = dir.resolve("truncated.bin");
        Files.write(truncated, java.util.Arrays.copyOf(bytes, bytes.length - 4));   // 掉尾 4 字节
        try (ChangeFileReader r = ChangeFileReader.open(truncated)) {
            assertThrows(UncheckedIOException.class, () -> {
                for (Record ignored : r) {
                    // 尝试读尽
                }
            }, "截断文件应抛 IOException");
            assertFalse(r.complete(), "未读到 FOOTER");
        }
    }

    /** 场景:非 VBFG 文件(Magic 不符)在 open 即抛。 */
    @Test
    void nonVbfgFileRejectedAtOpen() throws IOException {
        Path file = dir.resolve("plain.bin");
        Files.write(file, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        assertThrows(IOException.class, () -> ChangeFileReader.open(file));
    }

    /** 场景:列数与值数不匹配在写侧 fail-fast(上游对齐性破坏的信号)。 */
    @Test
    void columnCountMismatchFailsFast() throws IOException {
        Path file = dir.resolve("bad.bin");
        TableDef def = TableDef.of("db", "s", "t", List.of(), List.of(new ColumnDef("a", TypeCode.INT)));
        try (ChangeFileWriter w = new ChangeFileWriter(file, 1, "s")) {
            assertThrows(IllegalArgumentException.class, () ->
                    w.writeEvent(def, Op.CREATE, new Object[]{1L, 2L}));
        }
    }

    /** 写一个最小完整文件(BEGIN+EVENT+COMMIT+finish),供破坏类用例取材。 */
    private Path writeMinimalFile() throws IOException {
        Path file = dir.resolve("minimal.bin");
        TableDef def = TableDef.of("db", "s", "t", List.of(),
                List.of(new ColumnDef("x", TypeCode.INT), new ColumnDef("y", TypeCode.STRING)));
        try (ChangeFileWriter w = new ChangeFileWriter(file, 1, "s")) {
            w.writeBegin(1L, 100L);
            w.writeEvent(def, Op.CREATE, new Object[]{1L, "v"});
            w.writeCommit(1L, 200L, 1000L);
            w.finish();
        }
        return file;
    }
}
