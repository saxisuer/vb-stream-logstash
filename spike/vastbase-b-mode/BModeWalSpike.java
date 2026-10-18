import org.postgresql.util.PSQLException;
import org.vastdata.vbstream.walsource.changes.DiskValueRenderer;
import org.vastdata.vbstream.walsource.layout.BlockRef;
import org.vastdata.vbstream.walsource.layout.DecodeKinds;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.HeapViews;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * B 档（DBCOMPATIBILITY 'B'，MySQL 风格）VastBase G100 差异面 spike harness（throwaway）：
 * Phase 1 在线收集——DirectWalSource 裸 v3 直连收流落 16MB 段目录（WalSegmentRecorder
 * 测试树工具的 spike 简化版），同时经 JDBC 在 test_b 建探针表做 DDL/DML；Phase 2 离线
 * 走读——FileWalSource 静态目录档 + WalStreamWalker（VastBase 布局）全窗解析：①记录族
 * census（rmid/info 分布——布局错位会直接解析异常，walker/parse 异常即证据面）②探针表
 * heap 记录元组解码 + 渲染（新类型 oid 的 DecodeKinds/DiskValueRenderer 覆盖面探测）。
 * 连接抖动（test_b 实例握手间歇 EOF/读超时）经 withConnFn 重试环吸收。
 */
public final class BModeWalSpike {

    private static final String HOST = System.getProperty("b.host", "172.16.107.133");
    private static final int PORT = Integer.parseInt(System.getProperty("b.port", "55443"));
    private static final String DB = System.getProperty("b.db", "test_b");
    private static final String USER = System.getProperty("b.user", "test");
    private static final String PASS = System.getProperty("b.pass", "Tester123");
    private static final String TABLE = "vb_wal_spike_b";
    private static final long SEGMENT_SIZE = 16L * 1024 * 1024;
    private static final long SEGMENTS_PER_XLOG_ID = 0x100000000L / SEGMENT_SIZE;

    private final Path dir;
    private final WalLayout layout = WalLayoutVVastbase.INSTANCE;
    private final Map<String, Integer> census = new TreeMap<>();
    private final Map<String, Integer> probeCensus = new LinkedHashMap<>();
    private long[] probeOids;
    private String[] probeKinds;

    public static void main(String[] args) throws Exception {
        new BModeWalSpike(Path.of(args.length > 0 ? args[0] : "/tmp/vb-spike-b-wal")).run();
    }

    private BModeWalSpike(Path dir) {
        this.dir = dir;
    }

    private void run() throws Exception {
        Files.createDirectories(dir);
        DirectWalSource src = new DirectWalSource(HOST, PORT, USER, PASS, null, 10_000L, 0L);
        AtomicBoolean stopped = new AtomicBoolean();
        BiConsumer<Long, byte[]> writer = (endLsn, bytes) -> write(endLsn - bytes.length, bytes);
        src.start(lsn -> { }, writer);
        System.out.println("== recorder started, dir=" + dir);
        try {
            phase1DdlDml();
            long target = queryLsn();
            waitForFrontier(target);
        } finally {
            if (stopped.compareAndSet(false, true)) {
                src.stop();
            }
        }
        System.out.println("== recorder stopped, frontier=" + Lsn.format(frontier()));
        phase2Parse();
    }

    /** Phase 1：探针表 DDL + DML（B 档差异类型全覆盖：date/datetime/timestamp/float4/uint4/uint8/tinyblob）。 */
    private void phase1DdlDml() throws Exception {
        withConn(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("drop table if exists " + TABLE);
                s.execute("create table " + TABLE + " ("
                        + "id int primary key, c_date date, c_dt datetime, c_ts timestamp, c_f float,"
                        + "c_u4 int unsigned, c_u8 bigint unsigned, c_vc varchar(32), c_tb tinyblob)");
                System.out.println("== probe table created");
                }
                return null;
            });
        withConn(c -> {
            try (Statement s = c.createStatement()) {
                s.execute("insert into " + TABLE + " values (1, '2026-10-18', '2026-10-18 12:30:45.123',"
                        + " '2026-10-18 12:30:45.123', 3.14, 4294967295, 18446744073709551615, 'hello B',"
                        + " decode('00FF10', 'hex'))");
                s.execute("insert into " + TABLE + " values (2, '1999-12-31', '0001-01-01 00:00:01',"
                        + " '0001-01-01 00:00:01', 1.5e-8, 0, 1, '', decode('AA', 'hex'))");
                s.execute("update " + TABLE + " set c_vc = 'updated', c_f = 2.71 where id = 1");
                s.execute("delete from " + TABLE + " where id = 2");
                // 第二探针表：只含 DecodeKinds 已支持类型——值面渲染正确性验证
                s.execute("drop table if exists " + TABLE + "2");
                s.execute("create table " + TABLE + "2 ("
                        + "id int primary key, c_date date, c_dt datetime, c_ts timestamp, c_f float,"
                        + "c_vc varchar(32))");
                s.execute("insert into " + TABLE + "2 values (1, '2026-10-18', '2026-10-18 12:30:45.123',"
                        + " '2026-10-18 12:30:45.123', 3.14, 'hello B')");
                s.execute("insert into " + TABLE + "2 values (2, '1999-12-31', '0001-01-01 00:00:01',"
                        + " '0001-01-01 00:00:01', 1.5e-8, '')");
                s.execute("update " + TABLE + "2 set c_vc = 'updated', c_f = 2.71 where id = 1");
                s.execute("delete from " + TABLE + "2 where id = 2");
                System.out.println("== probe DML done");
                }
                return null;
            });
    }

    /** Phase 2：静态目录档全窗走读 + census + 探针表元组解码（两张探针表）。 */
    private void phase2Parse() throws Exception {
        var probes = new LinkedHashMap<Long, boolean[]>(); // filenode -> [isSupportedTable]
        long fn1 = queryLong("select relfilenode from pg_class where relname = '" + TABLE + "'");
        long fn2 = queryLong("select relfilenode from pg_class where relname = '" + TABLE + "2'");
        System.out.println("== probe relfilenodes: " + TABLE + "=" + fn1 + " " + TABLE + "2=" + fn2);
        TupleDecoder decoder = new TupleDecoder(layout);

        FileWalSource source = FileWalSource.staticDirectory(dir, SEGMENT_SIZE, 0,
                layout.walBlockSize(), 0);
        WalStreamWalker walker = new WalStreamWalker(layout, new WalStreamMetrics(), rec -> {
            census.merge("rmid=" + rec.rmid() + " info=0x" + Integer.toHexString(rec.info()), 1, Integer::sum);
            for (BlockRef blk : rec.blocks()) {
                if (blk.relNode() == fn1 || blk.relNode() == fn2) {
                    try {
                        selectProbe(blk.relNode() == fn2);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    decodeProbeRecord(rec, blk, decoder);
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
        System.out.println("== census (rmid/info -> count), 共 " + census.size() + " 键:");
        census.forEach((k, v) -> System.out.println("   " + k + " x" + v));
        System.out.println("== probe 记录族分布:");
        probeCensus.forEach((k, v) -> System.out.println("   " + k + " x" + v));
    }

    /** 切换当前探针表的列集（oids/kinds 供解码与渲染；同表连打缓存跳过）。 */
    private String lastProbeTable;
    private void selectProbe(boolean second) throws Exception {
        String table = TABLE + (second ? "2" : "");
        if (table.equals(lastProbeTable)) {
            return;
        }
        lastProbeTable = table;
        probeOids = withConnFn(c -> {
            var list = new java.util.ArrayList<Long>();
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(
                    "select atttypid from pg_attribute where attrelid = '" + table
                            + "'::regclass and attnum > 0 order by attnum")) {
                while (rs.next()) {
                    list.add(rs.getLong(1));
                }
            }
            return list;
        }).stream().mapToLong(Long::longValue).toArray();
        probeKinds = new String[probeOids.length];
        for (int i = 0; i < probeOids.length; i++) {
            try {
                probeKinds[i] = DecodeKinds.forTypeOid(probeOids[i]);
            } catch (IllegalStateException e) {
                probeKinds[i] = "UNKNOWN(" + probeOids[i] + ")";
            }
        }
        System.out.println("== [" + table + "] oids/kinds=" + describeColumns(table));
    }

    /** 探针表单记录解码：INSERT 取块 data 元组，UPDATE/DELETE 经 HeapViews 视图走位。 */
    private void decodeProbeRecord(WalRecord rec, BlockRef blk, TupleDecoder decoder) {
        try {
            if (rec.rmid() == HeapOps.RM_HEAP_ID) {
                int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
                probeCensus.merge("heap op=0x" + Integer.toHexString(op)
                        + (rec.info() != op ? " rawInfo=0x" + Integer.toHexString(rec.info()) : ""), 1, Integer::sum);
                switch (op) {
                    case HeapOps.XLOG_HEAP_INSERT -> {
                        if (blk.hasData()) {
                            Object[] vals = decoder.decodePayload(rec.raw(), blk.dataOff(), probeKinds);
                            System.out.println("   INSERT tuple: " + renderRow(vals));
                        }
                    }
                    case HeapOps.XLOG_HEAP_UPDATE, HeapOps.XLOG_HEAP_HOT_UPDATE -> {
                        HeapViews.HeapUpdateView v = HeapViews.HeapUpdateView.parse(rec, layout);
                        System.out.println("   UPDATE flags=0x" + Integer.toHexString(v.flags())
                                + " blk.hasData=" + blk.hasData());
                        if (blk.hasData()) {
                            Object[] vals = decoder.decodePayload(rec.raw(), blk.dataOff(), probeKinds);
                            System.out.println("   UPDATE new tuple: " + renderRow(vals));
                        }
                        if ((v.flags() & HeapOps.XLH_UPDATE_CONTAINS_OLD) != 0) {
                            Object[] old = decoder.decodePayload(rec.raw(),
                                    rec.mainOff() + layout.sizeOfHeapUpdate(), probeKinds);
                            System.out.println("   UPDATE old tuple: " + renderRow(old));
                        }
                    }
                    case HeapOps.XLOG_HEAP_DELETE -> {
                        HeapViews.HeapDeleteView v = HeapViews.HeapDeleteView.parse(rec, layout);
                        System.out.println("   DELETE flags=0x" + Integer.toHexString(v.flags()));
                        if ((v.flags() & HeapOps.XLH_DELETE_CONTAINS_OLD) != 0) {
                            Object[] old = decoder.decodePayload(rec.raw(),
                                    rec.mainOff() + layout.sizeOfHeapDelete(), probeKinds);
                            System.out.println("   DELETE old tuple: " + renderRow(old));
                        }
                    }
                    default -> { }
                }
            } else if (rec.rmid() == HeapOps.RM_HEAP2_ID
                    && (rec.info() & HeapOps.XLOG_XACT_OPMASK) == HeapOps.XLOG_HEAP2_MULTI_INSERT) {
                probeCensus.merge("heap2 MULTI_INSERT", 1, Integer::sum);
            }
        } catch (Exception e) {
            System.out.println("   !! probe 记录解码异常 info=0x" + Integer.toHexString(rec.info()) + ": " + e);
            probeCensus.merge("DECODE-FAIL info=0x" + Integer.toHexString(rec.info()), 1, Integer::sum);
        }
    }

    /** 渲染整行：新类型 oid 探测面——DecodeKinds/渲染器不认的列单独点名。 */
    private String renderRow(Object[] vals) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vals.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            try {
                sb.append(DiskValueRenderer.render(vals[i], probeOids[i]));
            } catch (Exception e) {
                sb.append("<render-fail oid=").append(probeOids[i]).append(": ")
                        .append(String.valueOf(e.getMessage()).replace('\n', ' ')).append(">");
            }
        }
        return sb.append("]").toString();
    }

    private String describeColumns(String table) throws Exception {
        return withConnFn(c -> {
            StringBuilder sb = new StringBuilder();
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(
                    "select a.attnum, a.attname, a.atttypid, t.typname from pg_attribute a"
                            + " join pg_type t on t.oid = a.atttypid where a.attrelid = '" + table
                            + "'::regclass and a.attnum > 0 order by a.attnum")) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        sb.append("; ");
                    }
                    first = false;
                    int idx = rs.getInt(1) - 1;
                    sb.append(rs.getString(2)).append('=').append(rs.getString(4))
                            .append('(').append(rs.getLong(3)).append(")->").append(probeKinds[idx]);
                }
            }
            return sb.toString();
        });
    }

    // ---- 连接面（test_b 握手抖动重试环）----

    private interface ConnFn<T> {
        T apply(Connection c) throws SQLException;
    }

    private void withConn(ConnFn<Void> fn) throws Exception {
        withConnFn(fn);
    }

    private <T> T withConnFn(ConnFn<T> fn) throws Exception {
        for (int attempt = 1; attempt <= 8; attempt++) {
            try (Connection c = DriverManager.getConnection(
                    "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DB, USER, PASS)) {
                return fn.apply(c);
            } catch (PSQLException e) {
                System.out.println("   (conn attempt " + attempt + " fail: "
                        + String.valueOf(e.getMessage()).replace('\n', ' ') + ")");
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("连接重试耗尽");
    }

    private long queryLong(String sql) throws Exception {
        return withConnFn(c -> {
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new SQLException("无行: " + sql);
                }
                return Long.parseLong(rs.getString(1).trim());
            }
        });
    }

    private long queryLsn() throws Exception {
        return withConnFn(c -> {
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("select pg_current_xlog_location()::text")) {
                rs.next();
                return parseLsnText(rs.getString(1));
            }
        });
    }

    private void waitForFrontier(long target) throws Exception {
        for (int i = 0; i < 60; i++) {
            if (frontier() >= target) {
                System.out.println("== frontier " + Lsn.format(frontier()) + " >= target " + Lsn.format(target));
                return;
            }
            Thread.sleep(500);
        }
        System.out.println("== WARN: frontier 追平超时 " + Lsn.format(frontier()) + " < " + Lsn.format(target));
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

    // ---- 段写盘（WalSegmentRecorder 简化内联）----

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

    private static long parseLsnText(String s) {
        int slash = s.indexOf('/');
        return (Long.parseLong(s.substring(0, slash), 16) << 32) | Long.parseLong(s.substring(slash + 1), 16);
    }
}
