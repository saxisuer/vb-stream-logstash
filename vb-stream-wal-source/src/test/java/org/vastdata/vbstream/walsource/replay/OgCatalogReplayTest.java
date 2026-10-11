package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 档 catalog 重放的离线失败先行测试（Task 7 失败测试 ② + OG 偏移消费）——
 * 手造 OG 记录（32B 记录头 + 14B 定位器伴随 + last_lsn u64 + main 老结构
 * [xlrec][LogCSN 8B]，字节面与 og-plan 语料锚同源：INSERT main=11/INIT_PAGE 19、
 * UPDATE old@0/new@2/flags@4 + CSN、DELETE offnum@0/flags@2 + CSN、INPLACE 2B）经
 * {@link CatalogReplay#applyCatalogRecord} 施加，断言：
 *
 * <ul>
 *   <li>pg_class INS 解码：9.2 式元组头 oid（虚拟前导槽）注入值行槽 0，ClassRow
 *       投影（relfilenode 槽 6/reltoastrelid 槽 11/relkind 槽 20 + 值行位移）与
 *       int1 列（relcmprs）消耗正确；</li>
 *   <li>存储引擎护栏：列存（relcudescrelid/reldeltarelid≠0）、分区（parttype≠'n'）、
 *       段页式/bucket（relbucket≠null≠0）、UStore（reloptions 含
 *       storage_type=ustore）的 class 行到达即 ISE（消息含 relname/oid）——
 *       og-plan 实测判别面；</li>
 *   <li>pg_attribute OG 词典（attnum 槽 5）与 pg_namespace OG 七数据列（oid 头槽）
 *       的 INS 解码；</li>
 *   <li>INPLACE 数据区直读（OG 偏移 84/116）与 UPDATE 老结构字段位（行位移动 +
 *       tracked 跟随）。</li>
 * </ul>
 */
class OgCatalogReplayTest {

    /** 测试用 pg_attribute relfilenode（OG 档块匹配面）。 */
    private static final long ATTR_RELNODE = 2601;

    /** 测试用 pg_class relfilenode。 */
    private static final long CLASS_RELNODE = 2602;

    /** 测试用 pg_namespace relfilenode。 */
    private static final long NSP_RELNODE = 2603;

    /** 测试用表空间/库 oid（任意合法值）。 */
    private static final long SPC = 1663;
    private static final long DB = 14806;

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;
    private final CatalogReplay replay = new CatalogReplay(og, new TupleDecoder(og));

    /**
     * 用例 ①：pg_class INS data 路径——9.2 式元组头 oid（[t_hoff-4,t_hoff)）经虚拟
     * 前导槽注入值行槽 0，ClassRow 十列投影全对（含 float8 relpages/reltuples 消耗
     * 与 int1 relcmprs、parttype/'n' 等护栏列的正常值面）；raw tail 落 newCtid。
     */
    @Test
    void ogClassInsertDecodesHeaderOidIntoClassRow() {
        CatalogStores stores = freshStores();
        WalRecord rec = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                CLASS_RELNODE, 0, ogClassTuple(16389L, "og_t", 16395L, 16392L, 0L, 0L, 'n', null, null).payload())
                .mainData(insertMain(1, false)).build());

        replay.applyCatalogRecord(rec, stores);
        assertEquals(new CatalogRow.ClassRow(16389L, "og_t", 11L, 12L, 0L, 10L, 0L, 16395L, 16392L, "r"),
                stores.classRows().get(CatalogReplay.ctidKey(0, 1)),
                "头 oid + relfilenode@84/toast@116/relkind@143 的 OG 投影应全对");
        assertTrue(stores.rawClassTails().containsKey(CatalogReplay.ctidKey(0, 1)),
                "data 路径 INS 应落 raw tail");
    }

    /**
     * 用例 ②a：列存护栏——relcudescrelid≠0 的 class INS 行到达即 ISE，消息含
     * relname 与 oid（og-plan 实测：og_t_colstore 行 relcudescrelid=16447）；
     * 行不落字典。
     */
    @Test
    void columnStoreInsertTripsStorageEngineGuard() {
        CatalogStores stores = freshStores();
        WalRecord rec = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                CLASS_RELNODE, 0, ogClassTuple(16400L, "og_t_colstore", 5000L, 0L, 16447L, 0L, 'n', null, null).payload())
                .mainData(insertMain(1, false)).build());

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> replay.applyCatalogRecord(rec, stores));
        assertTrue(e.getMessage().contains("og_t_colstore"), "ISE 消息须含 relname: " + e.getMessage());
        assertTrue(e.getMessage().contains("16400"), "ISE 消息须含 oid: " + e.getMessage());
        assertTrue(e.getMessage().contains("relcudescrelid=16447"), e.getMessage());
        assertTrue(stores.classRows().isEmpty(), "护栏行不得落字典");
    }

    /**
     * 用例 ②b：分区/段页式 bucket/UStore 三护栏——parttype='p'（分区父表 relkind 仍
     * 'r'）、relbucket=2（段页式实测值）、reloptions 含 storage_type=ustore
     * （UStore 仅经 reloptions 可判，relam 恒 0）分别命中各自 ISE 判别串。
     */
    @Test
    void partitionBucketAndUstoreRowsTripStorageEngineGuard() {
        // parttype='p'（分区父表）
        assertGuardMessage(ogClassTuple(16410L, "og_t_part", 5001L, 0L, 0L, 0L, 'p', null, null),
                "og_t_part", "16410", "parttype=p");
        // relbucket=2（段页式表 SEGMENT=ON 实测）
        assertGuardMessage(ogClassTuple(16411L, "og_t_seg", 5002L, 0L, 0L, 0L, 'n', 2L, null),
                "og_t_seg", "16411", "relbucket=2");
        // reloptions 含 storage_type=ustore（UStore）
        assertGuardMessage(ogClassTuple(16412L, "og_t_ustore", 5003L, 0L, 0L, 0L, 'n', null,
                        "{orientation=row,storage_type=ustore,compression=no}"),
                "og_t_ustore", "16412", "storage_type=ustore");
    }

    /**
     * 用例 ③：pg_attribute OG 词典 INS——attnum 在槽 5（attstattarget@3 的 PG≤16
     * 位次）、attisdropped 槽 14、int1 attcmprmode@16/attkvtype@23 消耗正确，
     * AttrRow 五字段投影对。
     */
    @Test
    void ogAttrInsertDecodesWithShiftedAttnumSlot() {
        CatalogStores stores = freshStores();
        TupleBytes tuple = TupleBytes.of(og.pgAttributeKinds())
                .oid(16389)          // attrelid
                .name("v")           // attname
                .oid(25)             // atttypid
                .i32(-1)             // attstattarget（OG 位次：attnum 4）
                .i2(-1)              // attlen
                .i2(2)               // attnum（槽 5）
                .i32(0)              // attndims
                .i32(-1)             // attcacheoff
                .i32(-1)             // atttypmod
                .bool(true)          // attbyval
                .ch('i')             // attalign
                .ch('x')             // attstorage
                .bool(true)          // attnotnull
                .bool(false)         // atthasdef
                .bool(false)         // attisdropped（槽 14）
                .bool(true)          // attislocal
                .i1(0)               // attcmprmode（int1，OG 独有）
                .i32(0)              // attinhcount（OG int4）
                .oid(0)              // attcollation
                .nullAt(19, 20, 21)  // attacl/attoptions/attfdwoptions
                .skipVarlena("m")    // attinitdefval
                .i1(0);              // attkvtype（int1，OG 独有）
        WalRecord ins = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                ATTR_RELNODE, 3, tuple.payload()).mainData(insertMain(5, false)).build());

        replay.applyCatalogRecord(ins, stores);
        assertEquals(new CatalogRow.AttrRow(16389L, "v", 25L, 2, false),
                stores.attrRows().get(CatalogReplay.ctidKey(3, 5)),
                "attnum 须取槽 5（attstattarget 前移差），非 V17/V18 的槽 4");
    }

    /**
     * 用例 ④：pg_namespace OG 词典（七数据列 + 虚拟头 oid 槽）INS——NspRow 两字段
     * 投影对（nspOid 自头槽、nspname 自数据列 1），nsptimeline int8/in_redistribution
     * char/nspblockchain bool/nspcollation oid 消耗正确不抛。
     */
    @Test
    void ogNspInsertDecodesSevenColumnDict() {
        CatalogStores stores = freshStores();
        TupleBytes tuple = TupleBytes.of(CatalogRow.NspRow.kinds(og))
                .headerOid(501)
                .name("ogschema")
                .oid(10)             // nspowner
                .i64(1)              // nsptimeline（int8）
                .nullAt(4)           // nspacl
                .ch('n')             // in_redistribution
                .bool(false)         // nspblockchain
                .oid(100);           // nspcollation
        WalRecord ins = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                NSP_RELNODE, 0, tuple.payload()).mainData(insertMain(1, false)).build());

        replay.applyCatalogRecord(ins, stores);
        assertEquals(new CatalogRow.NspRow(501L, "ogschema"),
                stores.nspRows().get(CatalogReplay.ctidKey(0, 1)),
                "nspOid 应自 9.2 式元组头 oid（虚拟槽）解出");
    }

    /**
     * 用例 ⑤：INPLACE 数据区直读——OG 偏移 relfilenode@84/reltoastrelid@116（2B main
     * 仅 offnum），ClassRow 只更新两字段、raw tail 置失效。
     */
    @Test
    void ogInplaceReadsRelfilenodeAtOffsetEightyFour() {
        CatalogStores stores = freshStores();
        long key = CatalogReplay.ctidKey(0, 1);
        stores.classRows().put(key, new CatalogRow.ClassRow(16389L, "og_t", 11L, 12L, 0L, 10L, 0L, 16395L, 0L, "r"));
        stores.rawClassTails().put(key, new byte[]{1});

        TupleBytes fresh = ogClassTuple(16389L, "og_t", 777L, 888L, 0L, 0L, 'n', null, null);
        byte[] dataRegion = Arrays.copyOfRange(fresh.payload(),
                5 + (fresh.tHoff() - 23), fresh.payload().length);
        WalRecord rec = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INPLACE, 1,
                CLASS_RELNODE, 0, dataRegion).mainData(new byte[]{1, 0}).build());

        replay.applyCatalogRecord(rec, stores);
        assertEquals(new CatalogRow.ClassRow(16389L, "og_t", 11L, 12L, 0L, 10L, 0L, 777L, 888L, "r"),
                stores.classRows().get(key), "INPLACE 应按 OG 偏移 84/116 直读两字段");
        assertFalse(stores.rawClassTails().containsKey(key), "INPLACE 后 tail 须置失效");
    }

    /**
     * 用例 ⑥：UPDATE 老结构字段位——main [old_offnum u16@0][new_offnum u16@2]
     * [flags u8@4][LogCSN 8B]（语料锚 0x2359908 同形）：非截断跨页 UPD 行位移动、
     * 旧键消失、tracked 跟随（OG 与 vanilla 的字段位差异由本用例钉死）。
     */
    @Test
    void ogUpdateMovesCtidWithOldStructFieldOffsets() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        stores.classRows().put(oldKey, new CatalogRow.ClassRow(16389L, "og_t", 11L, 12L, 0L, 10L, 0L, 16395L, 0L, "r"));
        stores.trackedTableCtid(oldKey);

        TupleBytes moved = ogClassTuple(16389L, "og_t2", 16396L, 0L, 0L, 0L, 'n', null, null);
        WalRecord upd = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 1,
                CLASS_RELNODE, 5, moved.payload())
                .mainData(updateMain(1, 2, 0))
                .sameRelBlock(3)
                .build());
        replay.applyCatalogRecord(upd, stores);

        assertNull(stores.classRows().get(oldKey), "UPD 后旧 ctid 行须消失");
        assertEquals(new CatalogRow.ClassRow(16389L, "og_t2", 11L, 12L, 0L, 10L, 0L, 16396L, 0L, "r"),
                stores.classRows().get(newKey));
        assertEquals(newKey, stores.trackedTableCtid(), "tracked 须按 OG 字段位解出的 old/new 移动");
    }

    /**
     * 用例 ⑦（M1 已知限制锚）：OG 档 MULTI_INSERT 记录解析即 ISE——main 布局未语料
     * 锚定（og-plan 32KB 窗口 283 条零 multi_insert），fail-fast 优于盲解错位。
     */
    @Test
    void ogMultiInsertIsFailFastUntilLayoutAnchored() {
        CatalogStores stores = freshStores();
        WalRecord rec = rec(ogRecord(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, 1,
                ATTR_RELNODE, 0, new byte[]{0, 0}).mainData(new byte[]{0, 0, 2, 0}).build());
        assertThrows(IllegalStateException.class, () -> replay.applyCatalogRecord(rec, stores));
    }

    /**
     * 用例 ⑧（护栏边界）：reloptions 含 "segment=on"（段页式的 reloptions 侧判据，
     * 与 relbucket=2 双保险）与 reldeltarelid≠0（列存 delta 表）分别命中判别串；
     * 纯 astore 行（reloptions 无违例串、relbucket null）不触发护栏。
     */
    @Test
    void segmentOptionsAndDeltaTableTripGuardWhileAstorePasses() {
        assertGuardMessage(ogClassTuple(16413L, "og_t_seg2", 5004L, 0L, 0L, 0L, 'n', null,
                        "{orientation=row,segment=on,compression=no}"),
                "og_t_seg2", "16413", "segment=on");
        assertGuardMessage(ogClassTuple(16414L, "og_t_delta", 5005L, 0L, 0L, 16441L, 'n', null, null),
                "og_t_delta", "16414", "reldeltarelid=16441");

        CatalogStores stores = freshStores();
        WalRecord ok = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                CLASS_RELNODE, 0, ogClassTuple(16415L, "og_t_ok", 5006L, 0L, 0L, 0L, 'n', null,
                        "{orientation=row,compression=no}").payload())
                .mainData(insertMain(1, false)).build());
        assertDoesNotThrow(() -> replay.applyCatalogRecord(ok, stores));
        assertEquals(1, stores.classRows().size(), " astore 行应正常落字典");
    }

    // ---- 测试基建：OG 元组 DSL 与记录拼装 -----------------------------------------

    /**
     * 登记三 watched 目录 relfilenode 的空 stores。
     *
     * @return 新 CatalogStores
     */
    private CatalogStores freshStores() {
        CatalogStores stores = new CatalogStores();
        stores.pgAttrRelfilenode(ATTR_RELNODE);
        stores.pgClassRelfilenode(CLASS_RELNODE);
        stores.pgNspRelnode(NSP_RELNODE);
        return stores;
    }

    /**
     * 护栏断言便捷档：给定 OG class 元组 DSL 施加 INS 应抛 ISE，消息含三元组
     * （relname / oid / 判别串）。
     *
     * @param tuple     OG class 元组 DSL（未施加）
     * @param relname   期望 ISE 消息含的关系名
     * @param oid       期望 ISE 消息含的 oid
     * @param violation 期望 ISE 消息含的判别串
     */
    private void assertGuardMessage(TupleBytes tuple, String relname, String oid, String violation) {
        CatalogStores stores = freshStores();
        WalRecord rec = rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                CLASS_RELNODE, 0, tuple.payload()).mainData(insertMain(1, false)).build());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> replay.applyCatalogRecord(rec, stores));
        assertTrue(e.getMessage().contains(relname), "ISE 须含 relname: " + e.getMessage());
        assertTrue(e.getMessage().contains(oid), "ISE 须含 oid: " + e.getMessage());
        assertTrue(e.getMessage().contains(violation), "ISE 须含判别串 " + violation + ": " + e.getMessage());
    }

    /**
     * pg_class OG 增强词典（虚拟头 oid 槽 + 40 数据列）的全列元组 DSL——参数覆盖
     * 投影/护栏面字段，其余按 astore 常态填值（relcmprs=1、parttype/reloptions/
     * relbucket 为护栏判别列）。
     *
     * @param oidV      元组头 oid
     * @param relname   relname
     * @param filenode  relfilenode
     * @param toast     reltoastrelid
     * @param relcudesc relcudescrelid（列存主判别列，0 = 非）
     * @param reldelta  reldeltarelid（列存 delta 判别列，0 = 非）
     * @param parttype  parttype 单字符（'n' = 普通表）
     * @param relbucket relbucket（null = 普通表 NULL 列形态）
     * @param reloptions reloptions 原文（null = NULL 列形态）
     * @return 已写满全部列的 DSL
     */
    private TupleBytes ogClassTuple(long oidV, String relname, long filenode, long toast,
            long relcudesc, long reldelta, char parttype, Long relbucket, String reloptions) {
        String[] phys = og.pgClassKinds();
        String[] aug = new String[phys.length + 1];
        aug[0] = TupleDecoder.OIDHDR_KIND;
        System.arraycopy(phys, 0, aug, 1, phys.length);
        TupleBytes t = TupleBytes.of(aug)
                .headerOid(oidV)
                .name(relname)      // 数据列 0（relname@0，oid 在元组头）
                .oid(11)            // relnamespace
                .oid(12)            // reltype
                .oid(0)             // reloftype
                .oid(10)            // relowner
                .oid(0)             // relam
                .oid(filenode)      // relfilenode（槽 6，数据区 84）
                .oid(0)             // reltablespace
                .f8(0)              // relpages（float8 加宽）
                .f8(0)              // reltuples（float8 加宽）
                .i32(0)             // relallvisible
                .oid(toast)         // reltoastrelid（槽 11，数据区 116）
                .oid(0)             // reltoastidxid
                .oid(reldelta)      // reldeltarelid（护栏列）
                .oid(0)             // reldeltaidx
                .oid(relcudesc)     // relcudescrelid（护栏列）
                .oid(0)             // relcudescidx
                .bool(false)        // relhasindex
                .bool(false)        // relisshared
                .ch('p')            // relpersistence
                .ch('r')            // relkind（槽 20，数据区 143）
                .i2(2)              // relnatts
                .i2(0)              // relchecks
                .bool(true)         // relhasoids（9.2 式 oid 体系）
                .bool(false)        // relhaspkey
                .bool(false)        // relhasrules
                .bool(false)        // relhastriggers
                .bool(false)        // relhassubclass
                .i1(1)              // relcmprs（int1，OG 独有——astore 实测常态 1）
                .bool(false)        // relhasclusterkey
                .bool(false)        // relrowmovement
                .ch(parttype)       // parttype（护栏列）
                .i32(0)             // relfrozenxid
                .nullAt(34)         // relacl（skip 列 null；增强词典位 34 = 物理 33）
                .text(reloptions == null ? "z" : reloptions)   // reloptions（增强词典位 35；null 语义下方覆写）
                .ch('d')            // relreplident
                .i64(0)             // relfrozenxid64
                .oid(relbucket == null ? 0 : relbucket)        // relbucket（增强词典位 38；null 语义下方覆写）
                .nullAt(39)         // relbucketkey（int2vector skip；增强词典位 39）
                .i64(0);            // relminmxid（增强词典位 40）
        if (relbucket == null) {
            t.nullAt(38);
        }
        if (reloptions == null) {
            t.nullAt(35);
        }
        return t;
    }

    /**
     * OG INSERT main（老结构 + LogCSN）：[offnum u16][flags u8=0x10][CSN 8B]；
     * INIT_PAGE 变体前置 pd_xid_base u64（语料锚 0x2359550 main=19B）。
     *
     * @param offnum   新行行号
     * @param initPage true 前置 8B pd_xid_base（值 0 即可——视图只跳过）
     * @return main 字节
     */
    private static byte[] insertMain(int offnum, boolean initPage) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (initPage) {
            out.write(new byte[8], 0, 8);
        }
        put16(out, offnum);
        out.write(0x10);
        out.write(new byte[8], 0, 8);   // LogCSN（值不消费）
        return out.toByteArray();
    }

    /**
     * OG UPDATE main（老结构）：[old_offnum u16@0][new_offnum u16@2][flags u8@4]
     * [LogCSN 8B]（语料锚 0x2359908）。
     *
     * @param oldOffnum 旧行行号
     * @param newOffnum 新行行号
     * @param flags     xl_heap_update flags（0x04 = CONTAINS_OLD_TUPLE 等）
     * @return main 字节
     */
    private static byte[] updateMain(int oldOffnum, int newOffnum, int flags) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, oldOffnum);
        put16(out, newOffnum);
        out.write(flags);
        out.write(new byte[8], 0, 8);
        return out.toByteArray();
    }

    /**
     * 手造 OG 记录的字节拼装器——32B 记录头（xlog_basic.h XLogRecord：totLen@0/
     * term@4/xid@8/pad@12/prev@16/info@24/rmid@25/bucket@26/crc@28）+ 块头链
     * （id u8 + fork_flags u8 + data_length u16 + 定位器 12B + extra_flag u16 +
     * blkno u32 + last_lsn u64；SAME_REL 块省定位器伴随）+ DATA_SHORT 终止头 +
     * 载荷区（块 data 后 main 殿后）——与 OgWalRecordParserTest 的拼装器同源双写。
     */
    private static final class OgRecordBuilder {

        private final int rmid;
        private final int info;
        private final int xid;
        private final long relNode;
        private final int blockNo;
        private byte[] data = {};
        private byte[] mainData = {};
        private boolean sameRel;
        private int sameRelBlockNo;

        private OgRecordBuilder(int rmid, int info, int xid, long relNode, int blockNo) {
            this.rmid = rmid;
            this.info = info;
            this.xid = xid;
            this.relNode = relNode;
            this.blockNo = blockNo;
        }

        /**
         * 声明首块（id 0，fork 0）data 载荷。
         *
         * @param d 载荷字节
         * @return 本实例
         */
        private OgRecordBuilder data(byte[] d) {
            this.data = d;
            return this;
        }

        /**
         * 声明 main data（DATA_SHORT 终止头发射）。
         *
         * @param m main 字节
         * @return 本实例
         */
        private OgRecordBuilder mainData(byte[] m) {
            this.mainData = m;
            return this;
        }

        /**
         * 追加 SAME_REL 旧页块（id 1，无定位器伴随、无 data）。
         *
         * @param oldBlockNo 旧页块号
         * @return 本实例
         */
        private OgRecordBuilder sameRelBlock(int oldBlockNo) {
            this.sameRel = true;
            this.sameRelBlockNo = oldBlockNo;
            return this;
        }

        /**
         * 拼出完整 OG 记录字节（totLen 即数组长度，无 MAXALIGN 尾垫）。
         *
         * @return 记录字节
         */
        private byte[] build() {
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            // 首块：完整定位器 + extra_flag
            headers.write(0);
            headers.write(0x20);   // HAS_DATA, fork=0
            put16(headers, data.length);
            put32(headers, (int) SPC);
            put32(headers, (int) DB);
            put32(headers, (int) relNode);
            put16(headers, 0);     // extra_flag u16
            put32(headers, blockNo);
            headers.write(new byte[8], 0, 8);   // last_lsn u64
            payload.writeBytes(data);
            if (sameRel) {
                headers.write(1);
                headers.write(0x80);   // SAME_REL, fork=0, 无 data
                put16(headers, 0);
                put32(headers, sameRelBlockNo);
                headers.write(new byte[8], 0, 8);
            }
            headers.write(255);    // XLR_BLOCK_ID_DATA_SHORT
            headers.write(mainData.length);
            payload.writeBytes(mainData);
            int totLen = 32 + headers.size() + payload.size();
            ByteArrayOutputStream rec = new ByteArrayOutputStream(totLen);
            put32(rec, totLen);
            put32(rec, 1);         // term
            put32(rec, xid);
            put32(rec, 0);         // pad
            rec.write(new byte[8], 0, 8);   // prev
            rec.write(info);
            rec.write(rmid);
            put16(rec, 0);         // bucket_id
            put32(rec, 0);         // crc（parser 不校验）
            rec.writeBytes(headers.toByteArray());
            rec.writeBytes(payload.toByteArray());
            return rec.toByteArray();
        }
    }

    /**
     * OG 记录拼装入口（首块 = watched 关系、fork 0、带 data）。
     *
     * @param rmid    资源管理器 id
     * @param info    info 字节
     * @param xid     事务 id
     * @param relNode watched relfilenode
     * @param blockNo 首块块号
     * @param data    首块 data 载荷
     * @return 拼装器
     */
    private static OgRecordBuilder ogRecord(int rmid, int info, int xid, long relNode, int blockNo, byte[] data) {
        return new OgRecordBuilder(rmid, info, xid, relNode, blockNo).data(data);
    }

    /**
     * 字节 → 走读完成的 WalRecord（LSN 定值即可，重放面不消费）。
     *
     * @param bytes 拼装器产物
     * @return 解析结果
     */
    private WalRecord rec(byte[] bytes) {
        return WalRecordParser.parse(bytes, 0x3000000L, og);
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

    /**
     * 向流写 little-endian u32。
     *
     * @param out 目标流
     * @param v   32 位值
     */
    private static void put32(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 24) & 0xFF);
    }
}
