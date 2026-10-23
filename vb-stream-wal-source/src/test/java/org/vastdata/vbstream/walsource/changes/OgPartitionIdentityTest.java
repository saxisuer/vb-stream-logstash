package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M3 Task 2 的分区身份链端到端验收（OG 布局手造记录喂真
 * {@code XactGrouper}）——T0 锚②语义重排的落地锚：分区 relfilenode（heap 记录
 * 路由锚）经第四字典（pg_partition 'p' 行）反查 → parentid 母表链 → <b>TableMeta
 * 身份 = 母表</b>（schema.母表名 + 母表 oid + 母表列集——基准路 pgoutput R/I 帧
 * 同形，锚② {@code PARTITION_NAME=mother}）。
 *
 * <p>断言面：①分区行以母表身份发射（值按母表列序解码）；②第四字典 heap 记录
 * （pg_partition 段号 14318，oid 直认 9016）到达即全清 TableMeta 缓存——EXCHANGE
 * 换挂母表后新行即时按新身份渲染；③分区自有 TOAST（锚③：pg_partition
 * reltoastrelid@116、toast relfilenode 在 pg_class 't' 行）chunk 采集 + 分区行
 * external 指针拼装端到端（ToastAssembler 零改动的最小侵入锚）；④分区身份链断裂
 * （parentid 悬空）fail-safe 跳过——零发射、零终态失败；⑤{@code partitionOf}
 * 缺省实现恒 empty（V17/V18/未引导第四腿的 no-op 锚）。</p>
 *
 * <p>白名单双匹配四形态与链断 WARN/计数面在 {@code TableFilterTest}（纯过滤层）
 * 覆盖；本类经组装器钉端到端。需既有 OG 布局手造形态（OgXactGrouperSeamTest
 * 同源记录拼装）。</p>
 */
class OgPartitionIdentityTest {

    /** 测试基准定位器三件组（OgXactGrouperSeamTest 同源）。 */
    private static final long SPC = 1663L;

    private static final long DB = 24581L;

    /** 分区母表：oid == relfilenode == 24500，public.t_prng（id int4 / name text）。 */
    private static final long MOTHER_REL = 24500L;

    /** EXCHANGE 后的新母表：oid == relfilenode == 24501，public.t_exch（同列形）。 */
    private static final long NEW_MOTHER_REL = 24501L;

    /** 分区 p1 的物理段号（heap 记录路由锚——spike 锚② m2pg 实测形态的离线同构）。 */
    private static final long PART_RELNODE = 32900L;

    /** 分区自有 toast 关系的 pg_class 行：oid == relfilenode == 32902（pg_toast_part_32900）。 */
    private static final long PART_TOAST_REL = 32902L;

    /** pg_partition 自身物理段号（T0 坑 2：14318 ≠ oid 9016——watched 直认面）。 */
    private static final long PG_PART_RELNODE = 14318L;

    /**
     * 场景①：分区 relfilenode 上的 heap INSERT 以<b>母表身份</b>发射——表名
     * public.t_prng（非分区名 p1）、relOid=母表 oid、值按母表列序（id/name）解码。
     * 锚② 裁定行：基准路 R/I 帧报母表身份，wal 路渲染分区名即对拍必 diff。
     */
    @Test
    void ogPartitionRowEmitsUnderMotherIdentityWithMotherColumns() {
        Fixture fx = fixture();
        fx.insertPartitionRow(100, 1, "range-a");
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=1",
                "ROW INSERT public.t_prng before=null after={id=1, name=range-a}",
                "END xid=100 emitted=1 exp=1"), fx.listener.events,
                "分区行须以母表身份（public.t_prng）发射，值按母表列序解码——锚② PARTITION_NAME=mother");
    }

    /**
     * 场景②（TableMeta 缓存失效第四调用点）：pg_partition 段号上的 heap 记录到达即
     * 全清 TableMeta 缓存——EXCHANGE 把分区段号换挂新母表后，同段号的后续行即时按
     * 新母表身份渲染（缓存残留会把新行继续渲染成旧母表——错值面）。
     */
    @Test
    void ogFourthDictionaryHeapRecordInvalidatesTableMetaCacheForExchange() {
        Fixture fx = fixture();
        fx.insertPartitionRow(100, 1, "before-exchange");
        fx.feedHeapInsert(555, PG_PART_RELNODE,
                TupleBytes.of("oid", "int4", "text").oid(9016L).i32(1).text("ddl").payload());
        // 模拟重放已施加 EXCHANGE：第四字典该分区段号改挂新母表（缓存全清后可见）
        fx.snapshot.partitions.put(PART_RELNODE,
                new CatalogSnapshot.PartitionIdentity("p1", NEW_MOTHER_REL));
        fx.insertPartitionRow(100, 2, "after-exchange");
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=2",
                "ROW INSERT public.t_prng before=null after={id=1, name=before-exchange}",
                "ROW INSERT public.t_exch before=null after={id=2, name=after-exchange}",
                "END xid=100 emitted=2 exp=2"), fx.listener.events,
                "第四字典 heap 记录（pg_partition 段号）到达即全清缓存——EXCHANGE 换挂后新行按新母表身份渲染");
    }

    /**
     * 场景③（T0 锚③：TOAST 挂分区）：分区自有 toast 关系（pg_class 't' 行 +
     * pg_partition.reltoastrelid@116 登记）的 chunk 行正常采集，分区行元组内的
     * external 指针（va_toastrelid=分区 toast 段号）经已归集 chunk 拼装原值——
     * ToastAssembler 匹配面零改动（chunk 键 = toast relfilenode 本身，不经
     * pg_class.reltoastrelid 反查）的最小侵入锚。
     */
    @Test
    void ogPartitionToastExternalPointerReassemblesViaPartitionToastChunks() {
        Fixture fx = fixture();
        byte[] value = "partition-toast-payload".getBytes(StandardCharsets.UTF_8);
        fx.toastChunks(100, 32902L, 777L, value, 7);
        byte[] tuple = TupleBytes.of("int4", "text")
                .i32(1).externalTextPointer(value.length + 4, value.length, 0, 777L, PART_TOAST_REL)
                .payload();
        fx.feedHeapInsert(100, PART_RELNODE, tuple);
        fx.commit(100);

        assertEquals(List.of(
                "BEGIN xid=100 2p=false gid=null exp=1",
                "ROW INSERT public.t_prng before=null after={id=1, name=partition-toast-payload}",
                "END xid=100 emitted=1 exp=1"), fx.listener.events,
                "分区行 external 指针经分区自有 toast 关系的 chunk 拼装原值（锚③：pg_toast_part_<node>）");
    }

    /**
     * 场景④（链断 fail-safe）：第四字典有分区行但 parentid 悬空（母表不在 pg_class
     * 字典——字典半损/窗口期 DDL 形态）——该分区行 fail-safe 跳过：零发射、零
     * 终态失败（异常不抛、流不断），痕量面在 TableFilter 的 WARN 一次 + 计数
     * （TableFilterTest 链断用例锚）。
     */
    @Test
    void ogPartitionChainBrokenRowSkipsFailSafeWithoutTerminalFailure() {
        Fixture fx = fixture();
        fx.snapshot.partitions.put(32905L,
                new CatalogSnapshot.PartitionIdentity("p_orphan", 24599L));   // 母表未登记
        fx.feedHeapInsert(100, 32905L,
                TupleBytes.of("int4", "text").i32(1).text("orphan").payload());
        fx.commit(100);

        assertEquals(List.of(), fx.listener.events, "链断分区行零发射（fail-safe 跳过，不砖流）");
    }

    /**
     * 场景⑤（缺省实现锚）：{@code CatalogSnapshot#partitionOf} 缺省恒 empty——
     * V17/V18 档（无 pg_partition）与未引导第四腿的快照实现不覆写即天然 no-op，
     * 分区链在 vanilla 档零侵入的最小面。
     */
    @Test
    void defaultPartitionOfIsEmptyWithoutFourthDictionary() {
        CatalogSnapshot bare = new CatalogSnapshot() {
            @Override
            public long lsn() {
                return 0;
            }

            @Override
            public List<Column> columnsOf(long relOid) {
                return List.of();
            }

            @Override
            public OptionalLong relfilenodeOf(long relOid) {
                return OptionalLong.empty();
            }

            @Override
            public OptionalLong toastOf(long relOid) {
                return OptionalLong.empty();
            }

            @Override
            public Optional<String> schemaOf(long relOid) {
                return Optional.empty();
            }

            @Override
            public Optional<String> relkindOf(long relOid) {
                return Optional.empty();
            }

            @Override
            public Optional<String> nameOf(long relOid) {
                return Optional.empty();
            }

            @Override
            public OptionalLong relOidOf(long relNodeOrOid) {
                return OptionalLong.empty();
            }
        };
        assertTrue(bare.partitionOf(32900L).isEmpty(),
                "partitionOf 缺省实现须恒 empty（vanilla/未引导第四腿 no-op 锚）");
    }

    // ---- 测试基建 ----

    /**
     * 逐用例装配：stub 快照（母表/新母表/分区 toast 关系 + 第四字典 + watched
     * 直认）+ OG layout 组装器 + 录音 listener。
     */
    private Fixture fixture() {
        StubSnapshot snapshot = new StubSnapshot();
        snapshot.table(MOTHER_REL, "public", "t_prng", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false));
        snapshot.table(NEW_MOTHER_REL, "public", "t_exch", "r",
                new CatalogSnapshot.Column(1, "id", 23, false),
                new CatalogSnapshot.Column(2, "name", 25, false));
        snapshot.table(PART_TOAST_REL, "pg_toast", "pg_toast_part_32900", "t");
        snapshot.partition(PART_RELNODE, "p1", MOTHER_REL);
        snapshot.watchedRelnode(PG_PART_RELNODE, 9016L);
        RecordingListener listener = new RecordingListener();
        XactGrouper grouper = new XactGrouper(snapshot, null, WalLayoutVOpenGauss.INSTANCE,
                new ToastAssembler(null), listener);
        return new Fixture(snapshot, grouper, listener);
    }

    /**
     * OG 布局记录喂入面——LSN 游标按 MAXALIGN 步进（行序断言的 lsn 真序源）。
     */
    private static final class Fixture {
        final StubSnapshot snapshot;
        final XactGrouper grouper;
        final RecordingListener listener;
        private long lsn = 0x400000L;

        Fixture(StubSnapshot snapshot, XactGrouper grouper, RecordingListener listener) {
            this.snapshot = snapshot;
            this.grouper = grouper;
            this.listener = listener;
        }

        /**
         * 分区段号上的一条用户表形态 INSERT（id/name 两列载荷——按母表列序解码）。
         *
         * @param xid   事务 id
         * @param id    首列值
         * @param name  次列值
         */
        void insertPartitionRow(int xid, int id, String name) {
            feedHeapInsert(xid, PART_RELNODE,
                    TupleBytes.of("int4", "text").i32(id).text(name).payload());
        }

        /**
         * 指定段号上的一条 OG heap INSERT：块引用带定位器 14B + blkno + last_lsn，
         * 新元组在块 data、main = SizeOfOldHeapInsert 3 + LogCSN 8（OgXactGrouperSeamTest
         * 同源形态）。
         *
         * @param xid     事务 id
         * @param relNode 目标段号
         * @param payload 新元组载荷（[xl_heap_header 5B][tail]）
         */
        void feedHeapInsert(int xid, long relNode, byte[] payload) {
            byte[] main = new byte[11];   // SizeOfOldHeapInsert 3 + LogCSN 8
            feed(record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, xid, relNode, payload, main));
        }

        /**
         * toast 关系上的一组 chunk 行（MULTI_INSERT 真 entry 几何：datalen 只计
         * tail、SHORTALIGN(7+datalen) 步进——XactGrouperTest.toastChunks 的 OG 同构）。
         *
         * @param xid      事务 id
         * @param toastRel toast 关系段号（chunk 归集键）
         * @param valueId  值 oid（chunk_id 列）
         * @param value    原值字节（按 chunkSize 切片）
         * @param chunkSize 单片字节数
         */
        void toastChunks(int xid, long toastRel, long valueId, byte[] value, int chunkSize) {
            List<byte[]> payloads = new ArrayList<>();
            for (int off = 0, seq = 0; off < value.length; off += chunkSize, seq++) {
                int len = Math.min(chunkSize, value.length - off);
                byte[] chunk = new byte[len];
                System.arraycopy(value, off, chunk, 0, len);
                payloads.add(TupleBytes.of("oid", "int4", "bytea")
                        .oid(valueId).i32(seq).bytes(chunk).payload());
            }
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            ByteArrayOutputStream main = new ByteArrayOutputStream();
            main.write(0);                                // flags u8@0
            main.write(0);                                // isCompressed bool@1
            put16(main, payloads.size());                 // ntuples u16@2
            for (int i = 0; i < payloads.size(); i++) {
                put16(main, i + 1);                       // offsets u16[]@4
            }
            put64(main, 0L);                              // LogCSN 殿后
            for (byte[] p : payloads) {
                put16(data, p.length - 5);                // datalen 只计 tail（真几何锚）
                data.writeBytes(p);
                if ((data.size() & 1) != 0) {
                    data.write(0);                        // SHORTALIGN 填充（奇长 entry）
                }
            }
            feed(record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, xid, toastRel,
                    data.toByteArray(), main.toByteArray()));
        }

        /** 一条 OG COMMIT（nsub=0——OgXactGrouperSeamTest.commit 同源裁剪）。 */
        void commit(long xid) {
            ByteArrayOutputStream m = new ByteArrayOutputStream();
            put64(m, 1_000_000L);        // xact_time
            put64(m, 42L);               // csn
            put64(m, 0L);                // xinfo
            put32(m, 0);                 // nrels
            put32(m, 0);                 // nsubxacts
            put32(m, 0);                 // nmsgs
            put32(m, 24581);             // dbId
            put32(m, 1663);              // tsId
            put32(m, 0);                 // nlibrary
            put64(m, 0);                 // RecentXmin 尾
            feed(xact(HeapOps.XLOG_XACT_COMMIT | HeapOps.OG_XLR_REL_COMPRESS, xid, m.toByteArray()));
        }

        /** 喂一条记录字节并推进 LSN 游标。 */
        private void feed(byte[] bytes) {
            WalRecord r = WalRecordParser.parse(bytes, lsn, WalLayoutVOpenGauss.INSTANCE);
            grouper.onRecord(r);
            lsn += ((long) r.totLen() + 7) / 8 * 8;
        }

        /**
         * 一条带块引用 + 块 data + main 的 OG heap/heap2 记录拼装（OgXactGrouperSeamTest
         * 的 INSERT 形态一般化——块头 = id 0 + HAS_DATA 0x20 + 定位器 14B + blkno +
         * last_lsn，DATA_SHORT 承载 main）。
         */
        private byte[] record(int rmid, int info, int xid, long relNode, byte[] data, byte[] main) {
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            headers.write(0);             // 块 id 0
            headers.write(0x20);          // BKPBLOCK_HAS_DATA, fork=0
            put16(headers, data.length);
            put32(headers, SPC);
            put32(headers, DB);
            put32(headers, relNode);
            put16(headers, 0);            // extra_flag u16
            put32(headers, 0);            // blkno
            put64(headers, 0);            // last_lsn u64
            headers.write(255);           // DATA_SHORT
            headers.write(main.length);
            byte[] hdr = headers.toByteArray();
            byte[] rec = new byte[32 + hdr.length + data.length + main.length];
            put32(rec, 0, rec.length);
            put64(rec, 8, xid);
            rec[24] = (byte) info;
            rec[25] = (byte) rmid;
            System.arraycopy(hdr, 0, rec, 32, hdr.length);
            System.arraycopy(data, 0, rec, 32 + hdr.length, data.length);
            System.arraycopy(main, 0, rec, 32 + hdr.length + data.length, main.length);
            return rec;
        }

        /** 一条 OG XACT 记录拼装（main ≤255B 用 DATA_SHORT，超长用 DATA_LONG）。 */
        private static byte[] xact(int info, long xid, byte[] main) {
            boolean shortHeader = main.length <= 255;
            int headerLen = shortHeader ? 2 : 5;
            byte[] rec = new byte[32 + headerLen + main.length];
            put32(rec, 0, rec.length);
            put64(rec, 8, xid);
            rec[24] = (byte) info;
            rec[25] = (byte) HeapOps.RM_XACT_ID;
            if (shortHeader) {
                rec[32] = (byte) 255;
                rec[33] = (byte) main.length;
            } else {
                rec[32] = (byte) 254;
                put32(rec, 33, main.length);
            }
            System.arraycopy(main, 0, rec, 32 + headerLen, main.length);
            return rec;
        }
    }

    /**
     * 事件录音 listener——扁平字符串面（与 XactGrouperTest 同形，断言事件序/内容）。
     */
    private static final class RecordingListener implements ChangeOutputListener {
        final List<String> events = new ArrayList<>();

        @Override
        public void onBegin(BatchBegin begin) {
            events.add("BEGIN xid=" + begin.xid() + " 2p=" + begin.twoPhase() + " gid=" + begin.gid()
                    + " exp=" + begin.expectedChanges());
        }

        @Override
        public void onRow(RowChange row) {
            events.add("ROW " + row.op() + " " + row.table().schema() + "." + row.table().table()
                    + " before=" + row.before() + " after=" + row.after());
        }

        /** TRUNCATE 事件录音：表集映 schema.table 的列表文本 + 选项集（与 onRow 同缝）。 */
        @Override
        public void onTruncate(TruncateEvent event) {
            events.add("TRUNCATE " + event.tables().stream()
                    .map(t -> t.schema() + "." + t.table()).toList() + " options=" + event.options());
        }

        @Override
        public void onEnd(BatchEnd end) {
            events.add("END xid=" + end.xid() + " emitted=" + end.emittedChanges()
                    + " exp=" + end.expectedChanges());
        }

        @Override
        public void onAborted(BatchAborted aborted) {
            events.add("ABORTED xid=" + aborted.xid());
        }
    }

    /**
     * 分区链感知的 catalog 快照 stub（OgXactGrouperSeamTest 的 StubSnapshot 扩第四
     * 字典两 map：partitionOf 应答 + watched 段号直认——SnapshotView 的 relOidOf
     * 直认接缝同构）。
     */
    private static final class StubSnapshot implements CatalogSnapshot {
        final Map<Long, String> relkindByOid = new HashMap<>();
        final Map<Long, String> schemaByOid = new HashMap<>();
        final Map<Long, String> nameByOid = new HashMap<>();
        final Map<Long, List<Column>> columnsByOid = new HashMap<>();
        final Map<Long, PartitionIdentity> partitions = new HashMap<>();
        final Map<Long, Long> watchedRelnodeToOid = new HashMap<>();

        /** 登记一张表（relfilenode 取身份映射）。 */
        void table(long oid, String schema, String name, String relkind, Column... cols) {
            relkindByOid.put(oid, relkind);
            schemaByOid.put(oid, schema);
            nameByOid.put(oid, name);
            columnsByOid.put(oid, List.of(cols));
        }

        /** 登记一条第四字典分区行（relfilenode → 分区名/母表 oid）。 */
        void partition(long relfilenode, String relname, long parentid) {
            partitions.put(relfilenode, new PartitionIdentity(relname, parentid));
        }

        /** 登记 watched 段号直认（pg_partition 段号 14318 → oid 9016 形态）。 */
        void watchedRelnode(long relnode, long oid) {
            watchedRelnodeToOid.put(relnode, oid);
        }

        @Override
        public long lsn() {
            return 0;
        }

        @Override
        public List<Column> columnsOf(long relOid) {
            return columnsByOid.getOrDefault(relOid, List.of());
        }

        @Override
        public OptionalLong relfilenodeOf(long relOid) {
            return relkindByOid.containsKey(relOid) ? OptionalLong.of(relOid) : OptionalLong.empty();
        }

        @Override
        public OptionalLong toastOf(long relOid) {
            return relkindByOid.containsKey(relOid) ? OptionalLong.of(0) : OptionalLong.empty();
        }

        @Override
        public Optional<String> schemaOf(long relOid) {
            return Optional.ofNullable(schemaByOid.get(relOid));
        }

        @Override
        public Optional<String> relkindOf(long relOid) {
            return Optional.ofNullable(relkindByOid.get(relOid));
        }

        @Override
        public Optional<String> nameOf(long relOid) {
            return Optional.ofNullable(nameByOid.get(relOid));
        }

        @Override
        public OptionalLong relOidOf(long relNodeOrOid) {
            Long direct = watchedRelnodeToOid.get(relNodeOrOid);
            if (direct != null) {
                return OptionalLong.of(direct);           // watched 段号直认（SnapshotView 同构）
            }
            return relkindByOid.containsKey(relNodeOrOid)
                    ? OptionalLong.of(relNodeOrOid) : OptionalLong.empty();
        }

        @Override
        public Optional<PartitionIdentity> partitionOf(long relfilenode) {
            return Optional.ofNullable(partitions.get(relfilenode));
        }
    }

    /** 向流写 little-endian u16。 */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }

    /** 向流写 little-endian u32。 */
    private static void put32(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 4; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /** 向流写 little-endian u64。 */
    private static void put64(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /** 就地写 little-endian u32。 */
    private static void put32(byte[] b, int offset, long v) {
        for (int i = 0; i < 4; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /** 就地写 little-endian u64。 */
    private static void put64(byte[] b, int offset, long v) {
        for (int i = 0; i < 8; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }
}
