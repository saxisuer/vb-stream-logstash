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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        stores.classRows().put(oldKey, CatalogRow.ClassRow.fromDecoded(decode(oldTuple), layout));
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
        stores.classRows().put(start, CatalogRow.ClassRow.fromDecoded(decode(seed), layout));
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

    /**
     * 用例 6（审查修复回归，Minor②）：带端口的多宿主 URL 先切逗号再切冒号——
     * {@code //h1:5432,h2:5433/db} 取 h1:5432（原实现整段切冒号会把端口解析成
     * "5432,h2:5433" 抛 ISE）。
     */
    @Test
    void parseUrlTakesFirstHostOfMultiHostWithPorts() {
        CatalogSynchronizer.ConnInfo ci = CatalogSynchronizer.parseUrl(
                "jdbc:postgresql://h1:5432,h2:5433/mydb", "pg");
        assertEquals("h1", ci.host());
        assertEquals(5432, ci.port());
        assertEquals("mydb", ci.database());
        assertEquals("pg", ci.user());
        assertEquals("", ci.pass());
    }

    /**
     * 用例 7：URL 形态面——query 参数凭据优先于 metadata 回落、缺省端口/库回落、
     * 非 postgresql URL 与非数字端口 fail-fast。
     */
    @Test
    void parseUrlDerivesParamsCredentialsAndFailsFastOnMalformed() {
        CatalogSynchronizer.ConnInfo withParams = CatalogSynchronizer.parseUrl(
                "jdbc:postgresql://h/db?user=u&password=p", "pg");
        assertEquals("u", withParams.user());
        assertEquals("p", withParams.pass());
        assertEquals("h", withParams.host());
        assertEquals(5432, withParams.port());

        CatalogSynchronizer.ConnInfo bare = CatalogSynchronizer.parseUrl(
                "jdbc:postgresql://localhost:55432/postgres", "pg");
        assertEquals("pg", bare.user());
        assertEquals("", bare.pass());
        assertEquals("postgres", bare.database());

        CatalogSynchronizer.ConnInfo defaults = CatalogSynchronizer.parseUrl(
                "jdbc:postgresql:mydb", "pg");
        assertEquals("localhost", defaults.host());
        assertEquals(5432, defaults.port());
        assertEquals("mydb", defaults.database());
        assertEquals("pg", defaults.user());

        assertEquals("pg", CatalogSynchronizer.parseUrl("jdbc:postgresql:", "pg").database(),
                "全默认形态缺省库回落用户名");

        assertThrows(IllegalStateException.class,
                () -> CatalogSynchronizer.parseUrl("jdbc:mysql://h/db", "pg"),
                "非 postgresql URL 须 fail-fast");
        assertThrows(IllegalStateException.class,
                () -> CatalogSynchronizer.parseUrl("jdbc:postgresql://h:notaport/db", "pg"),
                "非数字端口须 fail-fast");
    }

    /**
     * 用例 8（终审 C1 离线判定）：start 返回的 lsn 作 appliedLsn 种子后，引导窗口
     * 记录不被过滤线吞掉——种子前沿（= 引导首句 flush LSN / stored lsn，B ≤ 快照
     * 时点 S）恰为重投递过滤线：记录末尾 ≤ 种子的（种子前重投递）被跳过且前沿不动；
     * 紧邻其后的首条记录（末尾 &gt; 种子，即 (B,S] 引导窗口内的记录）必须施加。
     * flush 读取次序的时序本身离线不可测，本用例锚定"种子 lsn 不吞引导窗口记录"的
     * 语义——若有人把种子化回退成"种子之后再读 flush"的旧形态（B &gt; S，窗口记录
     * 末尾 ≤ B 被过滤线吞掉），配合 IT 场景即暴露。
     */
    @Test
    void bootstrapSeededFrontierFiltersPreSeedButNotImmediateWindowRecord() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(0, 1);
        long newKey = CatalogReplay.ctidKey(2, 3);
        TupleBytes oldTuple = classTuple(100, "t1", 200, 0, 1);
        stores.classRows().put(oldKey, CatalogRow.ClassRow.fromDecoded(decode(oldTuple), layout));
        stores.rawClassTails().put(oldKey, tailOf(oldTuple));
        stores.trackedTableCtid(oldKey);

        // 种子 = start 返回的流起点（引导形态 max(P₀, B)）；一条"种子前"记录（末尾恰=种子）
        WalRecord preSeed = rec(updateRecord(0, 0, 1, 2, 3, classTuple(100, "stale", 999, 0, 1).payload()), 0x200000L);
        long seed = endLsn(preSeed);
        CatalogSynchronizer sync = new CatalogSynchronizer(stores,
                new CatalogReplay(layout, new TupleDecoder(layout)), seed);

        sync.apply(preSeed);
        assertEquals(oldKey, stores.trackedTableCtid(), "末尾 ≤ 种子的记录必须被过滤（不施加）");
        assertEquals("t1", stores.classRows().get(oldKey).relname(), "种子前记录施加会污染种子状态");
        assertEquals(seed, sync.snapshot().lsn(), "过滤不推进前沿（前沿单调不回退）");

        // 紧邻种子的窗口记录（起点恰 = 种子、末尾 > 种子）——引导窗口 (B,S] 内的形态，必须施加
        WalRecord window = rec(updateRecord(0, 0, 1, 2, 3, classTuple(100, "t2", 205, 0, 1).payload()), seed);
        sync.apply(window);
        assertEquals(new CatalogRow.ClassRow(100, "t2", 11, 12, 0, 10, 0, 205, 0),
                stores.classRows().get(newKey), "紧邻种子后的窗口记录必须施加（upsert 幂等消化重叠）");
        assertEquals(newKey, stores.trackedTableCtid(), "窗口记录施加后 tracked 须随 UPD 移动");
        assertEquals(endLsn(window), sync.snapshot().lsn());
        assertEquals(1L, sync.metrics().getOrDefault(CatalogStores.CatalogMetrics.REPLAYED, 0L),
                "仅窗口记录计入已施加事件");
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
