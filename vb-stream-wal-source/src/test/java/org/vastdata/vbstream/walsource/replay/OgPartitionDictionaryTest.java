package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.layout.HeapOps;
import org.vastdata.vbstream.walsource.layout.TupleBytes;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV17;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.layout.WalRecordParser;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M3 Task 1 第四 watched 字典（pg_partition）的离线失败先行测试——手造
 * OG 记录（字节面与 {@link OgCatalogReplayTest} 拼装器同源双写）按 T0 锚几何
 * （spike {@code docs/wal-source-opengauss-m3-spike.md} 锚①：32 数据列 + 9.2 式元组头
 * oid，relfilenode@数据区 84 / reltoastrelid@116 与 pg_class OG 偏移恰同值）经
 * {@link CatalogReplay#applyCatalogRecord} 施加，断言：
 *
 * <ul>
 *   <li>kinds 词典镜像 live 列布局（og-plan 容器 pg_attribute 于 pg_partition 32 行
 *       实查，2026-10-13：33 项 = 虚拟头 oid 槽 + 32 数据列，槽 7=relfilenode/
 *       槽 12=reltoastrelid）；</li>
 *   <li>INS 'p' 分区行与 'r' 母表行（relfilenode 恒 0，T0 锚①行身份双面）落
 *       partitionRows 字典 + raw tail；</li>
 *   <li>EXCHANGE/SPLIT 形态的 UPDATE（含截断值编码重建——prefix 落读区 120 内由旧行
 *       重编码）、DEL（DROP PARTITION 无块 data 形态）、OG CLEAN redirect（PRUNE 面）
 *       与 INPLACE（ADD PARTITION 母表统计面，84/116 直读）四事件形态的施加；</li>
 *   <li>bootstrap 第四表种子 SQL 的 OG 档投影（ctid/oid/relname/parttype/parentid/
 *       relfilenode/reltoastrelid 七表达式）与 vanilla 缺席（null = 不种子——
 *       vanilla 无 pg_partition 表）。</li>
 * </ul>
 */
class OgPartitionDictionaryTest {

    /** pg_partition 的物理 relfilenode（T0 锚②坑 2：14318 ≠ oid 9016——watched 面
     * 须按物理段号注册，与 M2 T7 修复 C 的映射目录段号直认机制同源注意）。 */
    private static final long PART_RELNODE = 14318;

    /** 测试用表空间/库 oid（任意合法值）。 */
    private static final long SPC = 1663;
    private static final long DB = 14806;

    /** T0 锚①实测母表 pg_class oid（t_m3_range）。 */
    private static final long MOTHER_OID = 32896;

    /** T0 锚①实测 p1 分区行 oid/relfilenode（INSERT 时同值）。 */
    private static final long P1_OID = 32900;

    /** T0 锚③实测 p1 分区自有 toast 关系 oid（pg_toast_part_32900）。 */
    private static final long P1_TOAST = 32902;

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;
    private final CatalogReplay replay = new CatalogReplay(og, new TupleDecoder(og));

    /**
     * 用例 ①：kinds 词典镜像 live 列布局（og-plan pg_attribute 于 pg_partition 32 行
     * 实查锚）——33 项 = 虚拟头 oid 槽 + 32 数据列；槽 7=relfilenode（attnum 7）、
     * 槽 12=reltoastrelid（attnum 12）与 T0 锚①数据区偏移 84/116 的定宽推算自洽
     * （relname 64B + parttype 1B + 垫 3 + parentid/rangenum/intervalnum 12B +
     * partstrategy 1B + 垫 3 = 84；后续与 pg_class OG 列 7-12 同宽对齐至 116）。
     */
    @Test
    void kindsDictMirrorsLiveColumnLayout() {
        String[] kinds = CatalogRow.PartitionRow.kinds();
        assertEquals(33, kinds.length, "虚拟头 oid 槽 + 32 数据列（live 实查 attnum 上界 32）");
        assertEquals(TupleDecoder.OIDHDR_KIND, kinds[0]);
        assertEquals("name", kinds[1], "relname attnum 1（name 64B 定宽）");
        assertEquals("char", kinds[2], "parttype attnum 2（'r'/'p' 行身份判别）");
        assertEquals("oid", kinds[3], "parentid attnum 3（母表 pg_class oid）");
        assertEquals("oid", kinds[7], "relfilenode attnum 7（值行槽 7——oidhdr 前导 +6）");
        assertEquals("oid", kinds[12], "reltoastrelid attnum 12（值行槽 12——oidhdr 前导 +11）");
        assertEquals("float8", kinds[9], "relpages attnum 9（float8 加宽，与 pg_class OG 同形）");
        assertEquals("skip", kinds[27], "reloptions attnum 27（_text 变长——路由面不消费）");
        assertEquals(120, CatalogRow.PartitionRow.READ_REGION_LEN,
                "读区 = relname..reltoastrelid 的 [0,120) 定宽段（值编码重建上界）");
    }

    /**
     * 用例 ②：INS 'p' 分区行 + 'r' 母表行——T0 锚①行身份双面（pg_partition 同时含
     * 母表行 parttype='r' relfilenode=0 与每分区一行 parttype='p'）经第四腿施加：
     * partOid 自 9.2 式元组头 oid 虚拟槽、relname@0/parttype@64/parentid@68/
     * relfilenode@84/reltoastrelid@116（T0 锚①六行已知值锚）投影全对；data 路径
     * INS 落 raw tail。
     */
    @Test
    void partitionAndMotherInsertsProjectAnchoredFields() {
        CatalogStores stores = freshStores();
        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                PART_RELNODE, 0, ogPartitionTuple(P1_OID, "p1", 'p', MOTHER_OID, P1_OID, P1_TOAST).payload())
                .mainData(insertMain(1)).build()), stores);
        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1,
                PART_RELNODE, 0, ogPartitionTuple(MOTHER_OID, "t_m3_range", 'r', MOTHER_OID, 0, 0).payload())
                .mainData(insertMain(2)).build()), stores);

        assertEquals(new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, P1_OID, P1_TOAST),
                stores.partitionRows().get(CatalogReplay.ctidKey(0, 1)),
                "分区行投影：头 oid + relname/parttype/parentid/relfilenode@84/reltoastrelid@116");
        assertEquals(new CatalogRow.PartitionRow(MOTHER_OID, "t_m3_range", "r", MOTHER_OID, 0, 0),
                stores.partitionRows().get(CatalogReplay.ctidKey(0, 2)),
                "母表行投影：parttype='r'、relfilenode/reltoastrelid 恒 0（T0 锚①）");
        assertTrue(stores.rawPartitionTails().containsKey(CatalogReplay.ctidKey(0, 1)),
                "data 路径 INS 应落 partition raw tail");
    }

    /**
     * 用例 ③：EXCHANGE/SPLIT 形态的跨页 UPDATE（T0 锚④：s1 行换 relfilenode 为源表
     * 段号、分区行不换身份只换路由）——旧行值不变仅 relfilenode 变，施加后旧 ctid
     * 行消失、新 ctid 落新值行。
     */
    @Test
    void updateMovesCtidAndSwapsRelfilenode() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        stores.partitionRows().put(oldKey, new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, P1_OID, P1_TOAST));

        TupleBytes moved = ogPartitionTuple(P1_OID, "p1", 'p', MOTHER_OID, 32973L, P1_TOAST);
        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 1,
                PART_RELNODE, 5, moved.payload())
                .mainData(updateMain(1, 2, 0))
                .sameRelBlock(3)
                .build()), stores);

        assertNull(stores.partitionRows().get(oldKey), "UPD 后旧 ctid 行须消失");
        assertEquals(new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, 32973L, P1_TOAST),
                stores.partitionRows().get(newKey), "EXCHANGE 后 relfilenode 换源表段号 32973");
    }

    /**
     * 用例 ④：截断 UPDATE 的值编码重建（读区 120 内 prefix 由旧行重编码 + 读区回填）
     * ——prefix=84（relfilenode 前界）：relname/parttype/parentid 取旧行读区、
     * relfilenode 取记录中段新值、suffix 起点越入读区时 reltoastrelid 段按旧行回填。
     */
    @Test
    void truncatedUpdateReconstructsFromReadRegionValueEncoding() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        stores.partitionRows().put(oldKey,
                new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, P1_OID, P1_TOAST));

        TupleBytes fresh = ogPartitionTuple(P1_OID, "whatever", 'p', MOTHER_OID, 33000L, P1_TOAST);
        int bitmapLen = fresh.tHoff() - 23;
        int dataLen = fresh.payload().length - 5 - bitmapLen;
        int prefix = 84;               // = relfilenode 数据区偏移，值编码重建上界
        int suffix = dataLen - 96;     // 零填充起点 96：读区 [96,120) 旧行回填
        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 1,
                PART_RELNODE, 5, ogTruncatedData(fresh, prefix, suffix))
                .mainData(updateMain(1, 2, HeapOps.XLH_UPDATE_TRUNCATION))
                .sameRelBlock(3)
                .build()), stores);

        assertNull(stores.partitionRows().get(oldKey), "截断 UPD 后旧 ctid 行须消失");
        assertEquals(new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, 33000L, P1_TOAST),
                stores.partitionRows().get(newKey),
                "prefix 段取旧行（relname/parttype/parentid）、中段取记录（relfilenode=33000）、"
                        + "读区回填段取旧行（reltoastrelid）");
    }

    /**
     * 用例 ⑤：DROP PARTITION 的 DELETE 形态（T0 锚④：单条、块无 data——catalog 无
     * replica identity 无旧元组）——行与 raw tail 均移除。
     */
    @Test
    void deleteDropsPartitionRowWithoutBlockData() {
        CatalogStores stores = freshStores();
        long key = CatalogReplay.ctidKey(0, 1);
        stores.partitionRows().put(key, new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, P1_OID, P1_TOAST));
        stores.rawPartitionTails().put(key, new byte[] {7, 7});

        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, 1,
                PART_RELNODE, 0, null)
                .mainData(deleteMain(1)).build()), stores);

        assertNull(stores.partitionRows().get(key), "DELETE 后行须移除");
        assertFalse(stores.rawPartitionTails().containsKey(key), "DELETE 后 tail 须移除");
    }

    /**
     * 用例 ⑥：OG CLEAN（9.2 系 XLOG_HEAP2_CLEAN）redirect 重定位 partition 行与
     * tail、dead 段移除行——第四腿的 PRUNE 施加面（T0 锚④：DDL 记录族沿 M1 既有
     * 页清理机制，无新格式）。
     */
    @Test
    void pruneRedirectRelocatesPartitionRowAndTail() {
        CatalogStores stores = freshStores();
        long from = CatalogReplay.ctidKey(0, 1);
        long to = CatalogReplay.ctidKey(0, 5);
        long dead = CatalogReplay.ctidKey(0, 7);
        CatalogRow.PartitionRow row = new CatalogRow.PartitionRow(P1_OID, "p1", "p", MOTHER_OID, P1_OID, P1_TOAST);
        stores.partitionRows().put(from, row);
        stores.partitionRows().put(dead, new CatalogRow.PartitionRow(32901L, "p2", "p", MOTHER_OID, 32901L, 32905L));
        stores.rawPartitionTails().put(from, new byte[] {9});

        ByteArrayOutputStream main = new ByteArrayOutputStream();
        put64(main, 4242L);         // latestRemovedXid（OG u64）
        put16(main, 1);             // nredirected
        put16(main, 1);             // ndead
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        put16(data, 1);             // redirect 1 -> 5
        put16(data, 5);
        put16(data, 7);             // dead
        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP2_ID, HeapOps.OG_XLOG_HEAP2_CLEAN, 1,
                PART_RELNODE, 0, data.toByteArray()).mainData(main.toByteArray()).build()), stores);

        assertEquals(row, stores.partitionRows().get(to), "redirect 1->5 重定位分区行");
        assertNull(stores.partitionRows().get(from), "旧位行须消失");
        assertNull(stores.partitionRows().get(dead), "dead 行须移除");
        assertArrayEquals(new byte[] {9}, stores.rawPartitionTails().get(to), "tail 随 redirect 重定位");
    }

    /**
     * 用例 ⑦：INPLACE（ADD PARTITION 收尾对母表行的统计面改写，T0 锚④）——数据区
     * 直读 relfilenode@84/reltoastrelid@116（与 pg_class OG 偏移恰同值，列 7-12 同宽
     * 对齐的巧合锚）重建行、raw tail 置失效。
     */
    @Test
    void inplaceReadsRelfilenodeAndToastAtPartitionOffsets() {
        CatalogStores stores = freshStores();
        long key = CatalogReplay.ctidKey(0, 1);
        stores.partitionRows().put(key, new CatalogRow.PartitionRow(MOTHER_OID, "t_m3_range", "r", MOTHER_OID, 0, 0));
        stores.rawPartitionTails().put(key, new byte[] {1});

        TupleBytes fresh = ogPartitionTuple(MOTHER_OID, "t_m3_range", 'r', MOTHER_OID, 33200L, 33201L);
        byte[] dataRegion = Arrays.copyOfRange(fresh.payload(),
                5 + (fresh.tHoff() - 23), fresh.payload().length);
        replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INPLACE, 1,
                PART_RELNODE, 0, dataRegion).mainData(new byte[] {1, 0}).build()), stores);

        assertEquals(new CatalogRow.PartitionRow(MOTHER_OID, "t_m3_range", "r", MOTHER_OID, 33200L, 33201L),
                stores.partitionRows().get(key), "INPLACE 应按 84/116 直读两字段重建行");
        assertFalse(stores.rawPartitionTails().containsKey(key), "INPLACE 后 tail 须置失效");
    }

    /**
     * 用例 ⑧：bootstrap 第四表种子 SQL——OG 档投影 ctid/oid/relname/parttype/
     * parentid/relfilenode/reltoastrelid 七表达式（T0 锚①喂 T1 的最少列集 + oid 经
     * 首列）；vanilla（V17/V18）返回 null = 不种子（vanilla 无 pg_partition 表，
     * 查询即 SQLException——档位门控是硬前提）。
     */
    @Test
    void partitionSeedSqlProjectsRoutingColumnsForOgOnly() {
        String sql = CatalogBootstrap.partitionSeedSql(og);
        assertEquals("SELECT ctid::text, oid, relname, parttype::text, parentid, relfilenode,"
                + " reltoastrelid FROM pg_partition", sql);
        assertNull(CatalogBootstrap.partitionSeedSql(WalLayoutV17.INSTANCE),
                "vanilla 无 pg_partition——不种子（null 门控）");
    }

    /**
     * 用例 ⑨：第四腿 inert 面——pgPartitionRelfilenode 未回填（0）时分区 relfilenode
     * 的记录施加为 no-op（vanilla 档天然如此；OG 档 bootstrap 前亦然），不抛不落。
     */
    @Test
    void unwatchedPartitionRelfilenodeIsNoOp() {
        CatalogStores stores = new CatalogStores();
        assertDoesNotThrow(() -> replay.applyCatalogRecord(rec(ogRecord(HeapOps.RM_HEAP_ID,
                HeapOps.XLOG_HEAP_INSERT, 1, PART_RELNODE, 0,
                ogPartitionTuple(P1_OID, "p1", 'p', MOTHER_OID, P1_OID, P1_TOAST).payload())
                .mainData(insertMain(1)).build()), stores));
        assertTrue(stores.partitionRows().isEmpty(), "未注册 watched 段号不得落行");
    }

    /**
     * 用例 ⑩：词典版本感知重构零回归锚——{@code PartitionRow.kinds(WalLayout)} 对
     * vanilla（V17/V18）与 OG 档均返回与既有静态 KINDS 逐项相等：vastbase Task 2
     * 的重构仅引入 VastBase 分派点（{@code instanceof WalLayoutVVastbase}），既有
     * 三档词典内容零变化（VastBase 变体断言归 Task 3 词典落地后一并锁）。
     */
    @Test
    void partitionKindsVersionAwareReturnsExistingDictionariesUnchanged() {
        assertArrayEquals(CatalogRow.PartitionRow.kinds(), CatalogRow.PartitionRow.kinds(WalLayoutV17.INSTANCE));
        assertArrayEquals(CatalogRow.PartitionRow.kinds(), CatalogRow.PartitionRow.kinds(WalLayoutV18.INSTANCE));
        assertArrayEquals(CatalogRow.PartitionRow.kinds(), CatalogRow.PartitionRow.kinds(WalLayoutVOpenGauss.INSTANCE));
    }

    // ---- 测试基建：OG 分区元组 DSL 与记录拼装（与 OgCatalogReplayTest 同源双写） --------

    /**
     * 登记第四 watched 目录 relfilenode 的空 stores。
     *
     * @return 新 CatalogStores
     */
    private CatalogStores freshStores() {
        CatalogStores stores = new CatalogStores();
        stores.pgPartitionRelfilenode(PART_RELNODE);
        return stores;
    }

    /**
     * pg_partition OG 词典（虚拟头 oid 槽 + 32 数据列）的全列元组 DSL——参数覆盖
     * 投影面字段（relname/parttype/parentid/relfilenode/reltoastrelid），其余列按
     * 常态填值（策略 'r' RANGE、变长段 22-27/30 全 null 位）。
     *
     * @param oidV     元组头 oid（分区/母表行自身 oid）
     * @param relname  relname
     * @param parttype 行身份（'p'=分区行 / 'r'=母表行）
     * @param parentid 母表 pg_class oid
     * @param filenode relfilenode（母表行恒 0）
     * @param toast    reltoastrelid（母表行恒 0）
     * @return 已写满全部列的 DSL
     */
    private TupleBytes ogPartitionTuple(long oidV, String relname, char parttype, long parentid,
            long filenode, long toast) {
        return TupleBytes.of(CatalogRow.PartitionRow.kinds())
                .headerOid(oidV)
                .name(relname)      // attnum 1 @0（oid 在元组头）
                .ch(parttype)       // attnum 2 @64
                .oid(parentid)      // attnum 3 @68
                .i32(0)             // rangenum @72
                .i32(0)             // intervalnum @76
                .ch('r')            // partstrategy @80
                .oid(filenode)      // relfilenode @84（槽 7）
                .oid(0)             // reltablespace @88
                .f8(0)              // relpages @96（float8 加宽）
                .f8(0)              // reltuples @104
                .i32(0)             // relallvisible @112
                .oid(toast)         // reltoastrelid @116（槽 12）
                .oid(0)             // reltoastidxid @120
                .oid(0)             // indextblid @124
                .bool(true)         // indisusable @128
                .oid(0)             // reldeltarelid @132
                .oid(0)             // reldeltaidx @136
                .oid(0)             // relcudescrelid @140
                .oid(0)             // relcudescidx @144
                .i32(0)             // relfrozenxid @148（xid32 4B）
                .i32(0)             // intspnum @152
                .nullAt(22, 23, 24, 25, 26, 27)   // partkey..reloptions 变长段全 null
                .i64(0)             // relfrozenxid64 @160（xid 8B）
                .i64(0)             // relminmxid @168（xid 8B）
                .nullAt(30)         // partkeyexpr text 变长
                .i32(0)             // partitionno @176
                .i32(0);            // subpartitionno @180
    }

    /**
     * OG INSERT main（老结构 + LogCSN）：[offnum u16][flags u8=0x10][CSN 8B]。
     *
     * @param offnum 新行行号
     * @return main 字节
     */
    private static byte[] insertMain(int offnum) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, offnum);
        out.write(0x10);
        out.write(new byte[8], 0, 8);   // LogCSN（值不消费）
        return out.toByteArray();
    }

    /**
     * OG DELETE main（老结构）：[offnum u16@0][flags u8@2][LogCSN 8B]（语料锚
     * 0x2359AD8 同形；无旧元组——catalog 无 replica identity）。
     *
     * @param offnum 被删行行号
     * @return main 字节
     */
    private static byte[] deleteMain(int offnum) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, offnum);
        out.write(0);
        out.write(new byte[8], 0, 8);
        return out.toByteArray();
    }

    /**
     * OG UPDATE main（老结构）：[old_offnum u16@0][new_offnum u16@2][flags u8@4]
     * [LogCSN 8B]（语料锚 0x2359908）。
     *
     * @param oldOffnum 旧行行号
     * @param newOffnum 新行行号
     * @param flags     xl_heap_update flags（XLH_UPDATE_TRUNCATION = 双截断位组）
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
     * 截断 UPDATE 的块 0 data（OG 档，flags 置双截断位组形态）：
     * [prefix u16][suffix u16][xl_heap_header 5B][bitmap+pad][数据区自 prefix 起、
     * 止于 len-suffix]——heapam 截断日志的 chunk 布局（与 OgCatalogReplayTest 的
     * ogTruncatedData 同构双写）。
     *
     * @param tuple  新行完整 tuple DSL（值供裁切）
     * @param prefix 前缀字节数（自旧行取）
     * @param suffix 后缀字节数（零填充段，重建期回填）
     * @return 块 data 字节
     */
    private static byte[] ogTruncatedData(TupleBytes tuple, int prefix, int suffix) {
        byte[] payload = tuple.payload();
        int bitmapLen = tuple.tHoff() - 23;
        byte[] dataRegion = Arrays.copyOfRange(payload, 5 + bitmapLen, payload.length);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, prefix);
        put16(out, suffix);
        out.write(payload, 0, 5);       // xl_heap_header
        out.write(payload, 5, bitmapLen);
        out.write(dataRegion, prefix, dataRegion.length - prefix - suffix);
        return out.toByteArray();
    }

    /**
     * 手造 OG 记录的字节拼装器——32B 记录头 + 块头链 + DATA_SHORT 终止头 + 载荷区
     * （块 data 后 main 殿后）；data 为 null 时首块无 HAS_DATA 位（DELETE 形态）。
     * 与 OgCatalogReplayTest 的拼装器同源双写。
     */
    private static final class OgRecordBuilder {

        private final int rmid;
        private final int info;
        private final int xid;
        private final long relNode;
        private final int blockNo;
        private byte[] data;
        private byte[] mainData = {};
        private boolean sameRel;
        private int sameRelBlockNo;

        private OgRecordBuilder(int rmid, int info, int xid, long relNode, int blockNo, byte[] data) {
            this.rmid = rmid;
            this.info = info;
            this.xid = xid;
            this.relNode = relNode;
            this.blockNo = blockNo;
            this.data = data;
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
            boolean hasData = data != null;
            ByteArrayOutputStream headers = new ByteArrayOutputStream();
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            headers.write(0);
            headers.write(hasData ? 0x20 : 0);   // HAS_DATA 位（OG 0x20），fork=0
            put16(headers, hasData ? data.length : 0);
            put32(headers, (int) SPC);
            put32(headers, (int) DB);
            put32(headers, (int) relNode);
            put16(headers, 0);     // extra_flag u16
            put32(headers, blockNo);
            headers.write(new byte[8], 0, 8);   // last_lsn u64
            if (hasData) {
                payload.writeBytes(data);
            }
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
            ByteArrayOutputStream recOut = new ByteArrayOutputStream(totLen);
            put32(recOut, totLen);
            put32(recOut, 1);      // term
            put32(recOut, xid);
            put32(recOut, 0);      // pad
            recOut.write(new byte[8], 0, 8);   // prev
            recOut.write(info);
            recOut.write(rmid);
            put16(recOut, 0);      // bucket_id
            put32(recOut, 0);      // crc（parser 不校验）
            recOut.writeBytes(headers.toByteArray());
            recOut.writeBytes(payload.toByteArray());
            return recOut.toByteArray();
        }
    }

    /**
     * OG 记录拼装入口（首块 = watched 关系、fork 0；data 为 null 表示无块 data）。
     *
     * @param rmid    资源管理器 id
     * @param info    info 字节
     * @param xid     事务 id
     * @param relNode watched relfilenode
     * @param blockNo 首块块号
     * @param data    首块 data 载荷（null = 无 data）
     * @return 拼装器
     */
    private static OgRecordBuilder ogRecord(int rmid, int info, int xid, long relNode, int blockNo, byte[] data) {
        return new OgRecordBuilder(rmid, info, xid, relNode, blockNo, data);
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

    /**
     * 向流写 little-endian u64。
     *
     * @param out 目标流
     * @param v   64 位值
     */
    private static void put64(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }
}
