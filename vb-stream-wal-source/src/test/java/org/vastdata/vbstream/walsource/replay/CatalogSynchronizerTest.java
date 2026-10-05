package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CatalogSynchronizer / CatalogSnapshot 契约（纯逻辑面）的失败先行测试——JDBC 引导
 * （CatalogBootstrap）部分留 Task 13 IT，本类只测组装逻辑：apply 委派重放引擎并推进
 * 消费前沿（两条记录验证终态正确性而非施加顺序本身）、columnsOf 含 dropped 占位按
 * attnum 序、relfilenodeOf 取 tracked 链当前值（PRUNE redirect → UPD 跨记录链条）。
 *
 * <p>记录拼装与 CatalogReplayTest 互为双源转录（WalBytes DSL 之上的 heap 家族便捷
 * 档）；种子行经行模型直接落 stores（引导后形态），不依赖 JDBC。</p>
 */
class CatalogSynchronizerTest {

    /** 测试用 pg_attribute relfilenode（与 pg_class 区分，块匹配面）。 */
    private static final long ATTR_RELNODE = 6101;

    /** 测试用 pg_class relfilenode。 */
    private static final long CLASS_RELNODE = 6102;

    /** 测试用表空间 oid（任意合法值，走读不校验）。 */
    private static final long SPC = 1663;

    /** 测试用数据库 oid（任意合法值）。 */
    private static final long DB = 16384;

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /**
     * 用例 1：apply 逐条委派重放并推进消费前沿——UPD 移动 tracked 行（行/尾/triple
     * 面终态正确）后 DEL 移除，snapshot().lsn() 每步推进到记录 MAXALIGN 后末尾；
     * metrics() 反映已施加事件数。
     */
    @Test
    void applyDelegatesReplayAndAdvancesFrontierAcrossRecords() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(0, 1);
        long newKey = CatalogReplay.ctidKey(2, 3);
        TupleBytes oldTuple = classTuple(100, "t1", 200, 0, 1);
        stores.classRows().put(oldKey, CatalogRow.ClassRow.fromDecoded(decode(oldTuple)));
        stores.rawClassTails().put(oldKey, tailOf(oldTuple));
        stores.trackedTableCtid(oldKey);
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout)));

        TupleBytes newTuple = classTuple(100, "t2", 205, 0, 1);
        WalRecord upd = rec(updateRecord(0, 0, 1, 2, 3, newTuple.payload()), 0x200000L);
        sync.apply(upd);
        assertEquals(new CatalogRow.ClassRow(100, "t2", 11, 12, 0, 10, 0, 205, 0),
                stores.classRows().get(newKey));
        assertNull(stores.classRows().get(oldKey), "UPD 后旧 ctid 行必须消失");
        assertEquals(newKey, stores.trackedTableCtid(), "tracked 须随 UPD 移动");
        long end1 = endLsn(upd);
        assertEquals(end1, sync.snapshot().lsn(), "前沿须推进到首条记录 MAXALIGN 后末尾");

        WalRecord del = rec(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, 1)
                .block(0, SPC, DB, CLASS_RELNODE, 2)
                .main(new byte[]{0, 0, 0, 0, 3, 0, 0, 0})
                .build(), end1);
        sync.apply(del);
        assertNull(stores.classRows().get(newKey), "DEL 后行必须消失");
        assertEquals(endLsn(del), sync.snapshot().lsn(), "前沿须随第二条记录继续推进");
        assertEquals(2L, sync.metrics().getOrDefault(CatalogStores.CatalogMetrics.REPLAYED, 0L));
    }

    /**
     * 用例 2：跨记录链条终态（prune 先于 events 的组合面）——PRUNE redirect 迁走
     * tracked 行位后，后续 UPD 自新行位继续搬迁并改 relfilenode：tracked 链不断、
     * relfilenodeOf 取链尾当前值（tracked 面与字典面同源）。
     */
    @Test
    void pruneThenUpdateChainKeepsTrackedAndRelfilenodeCurrent() {
        CatalogStores stores = freshStores();
        long start = CatalogReplay.ctidKey(7, 1);
        long afterPrune = CatalogReplay.ctidKey(7, 5);
        long afterUpdate = CatalogReplay.ctidKey(8, 2);
        TupleBytes seed = classTuple(100, "t1", 200, 0, 1);
        stores.classRows().put(start, CatalogRow.ClassRow.fromDecoded(decode(seed)));
        stores.rawClassTails().put(start, tailOf(seed));
        stores.trackedTableCtid(start);
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, 1);      // redirected: n=1
        put16(out, 1);
        put16(out, 5);      // (1 -> 5)
        WalRecord prune = rec(WalBytes.record(
                        HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_PRUNE_VACUUM_SCAN, 1)
                .block(0, SPC, DB, CLASS_RELNODE, 7)
                .data(out.toByteArray())
                .main(new byte[]{2, (byte) HeapOps.XLHP_HAS_REDIRECTIONS})
                .build(), 0x300000L);
        sync.apply(prune);
        assertEquals(afterPrune, stores.trackedTableCtid(), "tracked 须随 redirect 迁移");

        TupleBytes newTuple = classTuple(100, "t1", 777, 0, 1);
        WalRecord upd = rec(updateRecord(0, 7, 5, 8, 2, newTuple.payload()), endLsn(prune));
        sync.apply(upd);

        assertEquals(afterUpdate, stores.trackedTableCtid(), "tracked 链须延续到 UPD 后行位");
        CatalogSnapshot snapshot = sync.snapshot();
        assertEquals(OptionalLong.of(777L), snapshot.relfilenodeOf(100),
                "relfilenodeOf 取 tracked 链当前值（链尾 UPD 后的新 relfilenode）");
        assertEquals(OptionalLong.of(0L), snapshot.toastOf(100), "无 toast 关系的表 toastOf 为 0 值");
        assertEquals(endLsn(upd), snapshot.lsn());
    }

    /**
     * 用例 3：columnsOf 含 dropped 占位且按 attnum 升序——种子乱序落位（attnum 3/1/2）
     * 后快照仍按 1,2,3 序返回，dropped 位保真（DROP COLUMN 后的存储占位行不丢，spike
     * 发现 26 的字典语义）；他关系（relid 43）的行不串面；未知关系空表。
     */
    @Test
    void columnsOfReturnsDroppedPlaceholderInAttnumOrder() {
        CatalogStores stores = freshStores();
        stores.attrRows().put(CatalogReplay.ctidKey(0, 3),
                new CatalogRow.AttrRow(42, "........pg.dropped.3........", 23, 3, true));
        stores.attrRows().put(CatalogReplay.ctidKey(0, 1),
                new CatalogRow.AttrRow(42, "id", 23, 1, false));
        stores.attrRows().put(CatalogReplay.ctidKey(0, 2),
                new CatalogRow.AttrRow(42, "s", 25, 2, false));
        stores.attrRows().put(CatalogReplay.ctidKey(1, 1),
                new CatalogRow.AttrRow(43, "other", 23, 1, false));
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout)));

        List<CatalogSnapshot.Column> cols = sync.snapshot().columnsOf(42);
        assertEquals(3, cols.size());
        assertEquals(new CatalogSnapshot.Column(1, "id", 23, false), cols.get(0));
        assertEquals(new CatalogSnapshot.Column(2, "s", 25, false), cols.get(1));
        assertEquals(new CatalogSnapshot.Column(3, "........pg.dropped.3........", 23, true),
                cols.get(2), "dropped 占位须保留且置 dropped 位");
        assertTrue(sync.snapshot().columnsOf(9999).isEmpty(), "未知关系返回空表而非 null");
    }

    /**
     * 用例 4：relfilenodeOf/toastOf 对未知关系返回 empty（区别于"已知但值为 0"——
     * 后者是 of(0)，引导前 relfilenode 未登记的 oid 不臆造值）。
     */
    @Test
    void relfilenodeAndToastOfUnknownOidReturnEmpty() {
        CatalogStores stores = freshStores();
        stores.classRows().put(CatalogReplay.ctidKey(0, 1), new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 200, 0));
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout)));

        CatalogSnapshot snapshot = sync.snapshot();
        assertEquals(OptionalLong.empty(), snapshot.relfilenodeOf(12345));
        assertEquals(OptionalLong.empty(), snapshot.toastOf(12345));
        assertEquals(OptionalLong.of(200L), snapshot.relfilenodeOf(100));
    }

    /**
     * 用例 5：bootstrap 的 ctid 文本解析——"(block,off)" 形态折为 ctid 键（spike
     * {@code replaceAll("[() ]","").split(",")} 等价面，纯逻辑，JDBC 面留 IT）。
     */
    @Test
    void parseCtidKeyParsesPgTextForm() {
        assertEquals(CatalogReplay.ctidKey(0, 1), CatalogBootstrap.parseCtidKey("(0,1)"));
        assertEquals(CatalogReplay.ctidKey(123, 45), CatalogBootstrap.parseCtidKey("(123,45)"));
    }

    // ---- 测试基建：记录拼装（CatalogReplayTest 同源转录，双源互证） ----------------

    /**
     * 造一套已登记两 watched 目录 relfilenode 的空 stores。
     *
     * @return 新 CatalogStores 实例
     */
    private CatalogStores freshStores() {
        CatalogStores stores = new CatalogStores();
        stores.pgAttrRelfilenode(ATTR_RELNODE);
        stores.pgClassRelfilenode(CLASS_RELNODE);
        return stores;
    }

    /**
     * 字节 → 走读完成的 WalRecord（指定起始 LSN，前沿断言面消费它）。
     *
     * @param bytes WalBytes.build() 产物
     * @param lsn   本记录起始 LSN
     * @return 解析结果
     */
    private WalRecord rec(byte[] bytes, long lsn) {
        return WalRecordParser.parse(bytes, lsn, layout);
    }

    /**
     * 一条记录施加后的消费前沿：起点 + MAXALIGN(totLen)（与 walker 的记录对齐边界
     * 同式，synchronizer.apply 的前沿推进面）。
     *
     * @param r 已施加记录
     * @return 记录 MAXALIGN 后的末尾 LSN
     */
    private static long endLsn(WalRecord r) {
        return (r.lsn() + r.totLen() + 7) & ~7L;
    }

    /**
     * XLOG_HEAP_UPDATE 记录（跨页）：14B main + 块 0（新页）data + 块 1（旧页，
     * SAME_REL）无载荷——CatalogReplayTest 同构转录。
     *
     * @param flags     xl_heap_update flags
     * @param oldBlock  旧页块号
     * @param oldOffnum 旧行行号
     * @param newBlock  新页块号
     * @param newOffnum 新行行号
     * @param newData   新 tuple 载荷
     * @return 记录字节
     */
    private byte[] updateRecord(int flags, int oldBlock, int oldOffnum, int newBlock, int newOffnum, byte[] newData) {
        byte[] main = new byte[14];
        main[4] = (byte) oldOffnum;
        main[5] = (byte) (oldOffnum >>> 8);
        main[7] = (byte) flags;
        main[12] = (byte) newOffnum;
        main[13] = (byte) (newOffnum >>> 8);
        return WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 1)
                .block(0, SPC, DB, CLASS_RELNODE, newBlock)
                .data(newData)
                .sameRel(0, oldBlock)
                .main(main)
                .build();
    }

    /**
     * pg_class 全 34 列 tuple DSL（词典取 layout，值面按参数 + 固定填充）。
     *
     * @param oidV     relOid（列 1）
     * @param relname  relname（列 2）
     * @param filenode relfilenode（列 8，数据区偏移 88）
     * @param toast    reltoastrelid（列 14，数据区偏移 112）
     * @param relpages relpages（列 10）
     * @return 已写满全部列的 DSL
     */
    private TupleBytes classTuple(long oidV, String relname, long filenode, long toast, int relpages) {
        return TupleBytes.of(layout.pgClassKinds())
                .oid(oidV)
                .name(relname)
                .oid(11)             // relnamespace
                .oid(12)             // reltype
                .oid(0)              // reloftype
                .oid(10)             // relowner
                .oid(0)              // relam
                .oid(filenode)       // relfilenode @88
                .oid(0)              // reltablespace
                .i32(relpages)       // relpages @96
                .f4(0)               // reltuples
                .i32(0)              // relallvisible
                .i32(0)              // relallfrozen
                .oid(toast)          // reltoastrelid @112
                .bool(false)         // relhasindex
                .bool(false)         // relisshared
                .ch('p')             // relpersistence
                .ch('r')             // relkind
                .i2(2)               // relnatts
                .i2(0)               // relchecks
                .bool(false)         // relhasrules
                .bool(false)         // relhastriggers
                .bool(false)         // relhassubclass
                .bool(false)         // relrowsecurity
                .bool(false)         // relforcerowsecurity
                .bool(true)          // relispopulated
                .ch('d')             // relreplident
                .bool(false)         // relispartition
                .oid(0)              // relrewrite
                .i32(0)              // relfrozenxid
                .i32(0)              // relminmxid
                .skipVarlena("a")    // relacl
                .skipVarlena("b")    // reloptions
                .skipVarlena("c");   // relpartbound
    }

    /**
     * 用被测同源的 TupleDecoder 解码一个 tuple DSL 的 payload（种子行构造用）。
     *
     * @param tuple tuple DSL
     * @return 解码值行
     */
    private Object[] decode(TupleBytes tuple) {
        return new TupleDecoder(layout).decodePayload(tuple.payload(), 0, layout.pgClassKinds());
    }

    /**
     * tuple DSL 的 raw tail（payload 去 5B xl_heap_header）。
     *
     * @param tuple tuple DSL
     * @return tail 字节副本
     */
    private byte[] tailOf(TupleBytes tuple) {
        return Arrays.copyOfRange(tuple.payload(), 5, tuple.payload().length);
    }

    /**
     * 向流写 little-endian u16。
     *
     * @param out 目标流
     * @param v   16 位值
     */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }
}
