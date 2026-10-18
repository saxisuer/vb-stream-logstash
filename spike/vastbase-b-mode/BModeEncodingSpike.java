import org.postgresql.util.PSQLException;
import org.vastdata.vbstream.walsource.layout.BlockRef;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.receive.FileWalSource;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.receive.WalStreamWalker;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * M4 前置编码锚 spike（throwaway）：B 档/VastBase 系目标类型（int1/uint1/uint2/uint4/
 * uint8/year/blob/tinyblob/raw/clob/nvarchar2/binary/varbinary）逐类型单列表——①已知值
 * 插入；②服务端 text 输出（::text 对拍锚）采集；③DirectWalSource 收段 + 静态档走读，
 * 对每表 heap INSERT 记录 dump 块 data hex（t_hoff 前后全量）——三方对齐推磁盘编码。
 */
public final class BModeEncodingSpike {

    private static final String HOST = System.getProperty("b.host", "172.16.107.133");
    private static final int PORT = Integer.parseInt(System.getProperty("b.port", "55443"));
    private static final String DB = System.getProperty("b.db", "test_b");
    private static final String USER = "test";
    private static final String PASS = "Tester123";
    private static final String PREFIX = "vb_wal_enc_";
    private static final long SEGMENT_SIZE = 16L * 1024 * 1024;
    private static final long SEGMENTS_PER_XLOG_ID = 0x100000000L / SEGMENT_SIZE;

    /** 类型 -> 两行已知值（第二行更刁钻）。 */
    private static final Map<String, String[]> TYPES = new LinkedHashMap<>();
    static {
        TYPES.put("text", new String[] {"'hello clob'", "'中文CLOB值'"});
        TYPES.put("varchar(32)", new String[] {"'你好nvarchar'", "''"});
        TYPES.put("bytea", new String[] {"decode('DEADBEEF','hex')", "decode('00','hex')"});
        TYPES.put("int1", new String[] {"127", "-128"});
        TYPES.put("uint1", new String[] {"255", "0"});
        TYPES.put("uint2", new String[] {"65535", "1"});
        TYPES.put("uint4", new String[] {"4294967295", "1"});
        TYPES.put("uint8", new String[] {"18446744073709551615", "1"});
        TYPES.put("year", new String[] {"2026", "1901"});
        TYPES.put("blob", new String[] {"decode('DEADBEEF','hex')", "decode('00','hex')"});
        TYPES.put("tinyblob", new String[] {"decode('00FF10','hex')", "decode('AA','hex')"});
        TYPES.put("raw(16)", new String[] {"decode('0102030405060708090A0B0C0D0E0F10','hex')", "decode('FF','hex')"});
        TYPES.put("clob", new String[] {"'hello clob'", "'中文CLOB值'"});
        TYPES.put("nvarchar2(32)", new String[] {"'你好nvarchar'", "''"});
        TYPES.put("binary(4)", new String[] {"decode('AABBCCDD','hex')", "decode('0000','hex')"});
        TYPES.put("varbinary(8)", new String[] {"decode('11223344','hex')", "decode('EE','hex')"});
        // -Db.types=csv 过滤（对照跑用——A 档 int1 值域不同等差异绕行）
        String filter = System.getProperty("b.types");
        if (filter != null) {
            var keep = java.util.Set.of(filter.split(","));
            TYPES.keySet().retainAll(keep);
        }
    }

    private final Path dir;
    private final WalLayout layout = WalLayoutVVastbase.INSTANCE;
    private final Map<Long, String> filenodes = new LinkedHashMap<>();
    private final Map<String, Integer> census = new TreeMap<>();

    public static void main(String[] args) throws Exception {
        new BModeEncodingSpike(Path.of(args.length > 0 ? args[0] : "/tmp/vb-spike-b-enc")).run();
    }

    private BModeEncodingSpike(Path dir) {
        this.dir = dir;
    }

    private void run() throws Exception {
        Files.createDirectories(dir);
        DirectWalSource src = new DirectWalSource(HOST, PORT, USER, PASS, null, 10_000L, 0L);
        src.start(lsn -> { }, (endLsn, bytes) -> write(endLsn - bytes.length, bytes));
        System.out.println("== recorder started");
        try {
            phase1Inserts();
            long target = queryLsn();
            for (int i = 0; i < 60; i++) {
                if (frontier() >= target) {
                    break;
                }
                Thread.sleep(500);
            }
        } finally {
            src.stop();
        }
        System.out.println("== recorder stopped, frontier=" + Lsn.format(frontier()));
        phase2Dump();
        cleanup();
    }

    private void phase1Inserts() throws Exception {
        try (Connection c = acquire()) {
            for (var e : TYPES.entrySet()) {
                String table = PREFIX + e.getKey().replaceAll("[^a-z0-9]", "");
                try (Statement s = c.createStatement()) {
                    s.execute("drop table if exists " + table);
                    s.execute("create table " + table + " (id int, c " + e.getKey() + ")");
                    s.execute("insert into " + table + " values (1, " + e.getValue()[0] + ")");
                    s.execute("insert into " + table + " values (2, " + e.getValue()[1] + ")");
                    try (ResultSet rs = s.executeQuery("select relfilenode from pg_class where relname = '"
                            + table + "'")) {
                        rs.next();
                        filenodes.put(Long.parseLong(rs.getString(1).trim()), e.getKey());
                    }
                    System.out.println("== " + e.getKey() + " filenode ok");
                }
            }
            try (Statement s = c.createStatement()) {
                for (var e : TYPES.entrySet()) {
                    String table = PREFIX + e.getKey().replaceAll("[^a-z0-9]", "");
                    try (ResultSet rs = s.executeQuery("select id, c::text from " + table + " order by id")) {
                        while (rs.next()) {
                            System.out.println("TEXT [" + e.getKey() + "] row" + rs.getString(1)
                                    + " = [" + rs.getString(2) + "]");
                        }
                    } catch (SQLException ex) {
                        System.out.println("TEXT [" + e.getKey() + "] ::text 失败: "
                                + String.valueOf(ex.getMessage()).replace('\n', ' '));
                    }
                }
            }
        }
    }

    private void phase2Dump() throws Exception {
        FileWalSource source = FileWalSource.staticDirectory(dir, SEGMENT_SIZE, 0,
                layout.walBlockSize(), 0);
        WalStreamWalker walker = new WalStreamWalker(layout, new WalStreamMetrics(), rec -> {
            census.merge("rmid=" + rec.rmid() + "/0x" + Integer.toHexString(rec.info()), 1, Integer::sum);
            if (rec.rmid() != HeapOps.RM_HEAP_ID
                    || (rec.info() & HeapOps.XLOG_XACT_OPMASK) != HeapOps.XLOG_HEAP_INSERT) {
                return;
            }
            for (BlockRef blk : rec.blocks()) {
                String type = filenodes.get(blk.relNode());
                if (type != null && blk.hasData()) {
                    byte[] raw = rec.raw();
                    int off = blk.dataOff();
                    int len = Math.min(72, raw.length - off);
                    System.out.println("HEX [" + type + "] lsn=" + Lsn.format(rec.lsn())
                            + " data(" + len + "B)=" + hex(raw, off, len));
                }
            }
        });
        source.start(lsn -> { }, (endLsn, bytes) -> walker.feed(endLsn, bytes));
        while (!source.eofReached() && source.terminalFailure().isEmpty()) {
            Thread.sleep(200);
        }
        source.terminalFailure().ifPresent(t -> {
            throw new IllegalStateException("静态档终态失败", t);
        });
        System.out.println("== census 键数 " + census.size() + "（全窗零异常 = 布局可用）");
    }

    private void cleanup() throws Exception {
        try (Connection c = acquire()) {
            try (Statement s = c.createStatement()) {
                for (var e : TYPES.entrySet()) {
                    s.execute("drop table if exists " + PREFIX + e.getKey().replaceAll("[^a-z0-9]", ""));
                }
            }
        }
        System.out.println("== cleanup done");
    }

    private static String hex(byte[] b, int off, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02X", b[off + i]));
        }
        return sb.toString();
    }

    // ---- 连接面 / 段写盘（同 BModeWalSpike）----

    private interface ConnFn<T> {
        T apply(Connection c) throws SQLException;
    }

    private Connection acquire() throws Exception {
        for (int attempt = 1; attempt <= 15; attempt++) {
            try {
                return DriverManager.getConnection(
                        "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DB, USER, PASS);
            } catch (PSQLException e) {
                System.out.println("   (conn attempt " + attempt + " fail)");
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("连接重试耗尽");
    }

    private <T> T withConn(ConnFn<T> fn) throws Exception {
        for (int attempt = 1; attempt <= 8; attempt++) {
            try (Connection c = DriverManager.getConnection(
                    "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DB, USER, PASS)) {
                return fn.apply(c);
            } catch (PSQLException e) {
                System.out.println("   (conn attempt " + attempt + " fail)");
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("连接重试耗尽");
    }

    private long queryLsn() throws Exception {
        return withConn(c -> {
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("select pg_current_xlog_location()::text")) {
                rs.next();
                String s2 = rs.getString(1);
                int slash = s2.indexOf('/');
                return (Long.parseLong(s2.substring(0, slash), 16) << 32)
                        | Long.parseLong(s2.substring(slash + 1), 16);
            }
        });
    }

    private long frontier() {
        long f = 0;
        try (var stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                Long segNo = parseSegNo(p.getFileName().toString());
                if (segNo != null) {
                    f = Math.max(f, segNo * SEGMENT_SIZE + Files.size(p));
                }
            }
        } catch (java.io.IOException e) {
            return 0;
        }
        return f;
    }

    private synchronized void write(long start, byte[] data) {
        long pos = 0;
        while (pos < data.length) {
            long abs = start + pos;
            long segNo = Math.floorDiv(abs, SEGMENT_SIZE);
            long inSeg = abs - segNo * SEGMENT_SIZE;
            int len = (int) Math.min(data.length - pos, SEGMENT_SIZE - inSeg);
            String name = String.format("%08X%08X%08X", 1L,
                    segNo / SEGMENTS_PER_XLOG_ID, segNo % SEGMENTS_PER_XLOG_ID);
            try (RandomAccessFile f = new RandomAccessFile(dir.resolve(name).toFile(), "rw")) {
                f.seek(inSeg);
                f.write(data, (int) pos, len);
            } catch (java.io.IOException e) {
                throw new IllegalStateException("段落盘失败: " + name, e);
            }
            pos += len;
        }
    }

    private static Long parseSegNo(String name) {
        if (name.length() != 24 || !name.matches("[0-9A-F]{24}")) {
            return null;
        }
        long log = Long.parseLong(name.substring(8, 16), 16);
        long seg = Long.parseLong(name.substring(16, 24), 16);
        return seg >= SEGMENTS_PER_XLOG_ID ? null : log * SEGMENTS_PER_XLOG_ID + seg;
    }
}
