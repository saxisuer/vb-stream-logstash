package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
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
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 截断自愈校验（SelfHealer + CatalogReplay 注入）的失败先行测试——stub probe
 * （oid → 末态行映射）替换 JDBC 面，经 {@link CatalogReplay#applyCatalogRecord}
 * 走真实截断重建路径后断言 stores 终态（spec §6②；spike 发现 23a：OID 恒真校验
 * 已废，鉴别力来自中段新值对 JDBC 末态）。
 *
 * <p>三用例：①中段 relfilenode 与末态一致且 ctid 匹配 → 采纳 + tracked 修位 +
 * 陈旧候选副本清除；②中段值不一致 → empty + 第二次连续失败起标 staleOids（第一次
 * 仅计数）；③连续失败不升级异常 + 采纳后连续计数清零。</p>
 */
class SelfHealerTest {

    /** 测试用 pg_class relfilenode（块匹配面）。 */
    private static final long CLASS_RELNODE = 6002;

    /** 测试用表空间 oid（任意合法值，走读不校验）。 */
    private static final long SPC = 1663;

    /** 测试用数据库 oid（任意合法值）。 */
    private static final long DB = 16384;

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /**
     * 用例 ①：未知 oldCtid 的截断更新——候选行（tracked 位上的旧 ClassRow）经值编码
     * 重建，中段 relfilenode=205 与 stub 末态一致且末态 ctid == 记录 new 位 → 采纳：
     * 行落 newCtid、tracked 修复到 new 位、陈旧候选副本（行 + tail）清除、
     * selfHealed 计数、skippedTruncated 不计。
     */
    @Test
    void midValuesMatchingEndStateAdoptsAndRepairsTracked() {
        CatalogStores stores = freshStores();
        long staleCtid = CatalogReplay.ctidKey(3, 1);
        long newCtid = CatalogReplay.ctidKey(5, 2);
        CatalogRow.ClassRow candidate = CatalogRow.ClassRow.fromDecoded(decode(classTuple(100, "t1", 200, 0, 1)));
        stores.classRows().put(staleCtid, candidate);
        stores.trackedTableCtid(staleCtid);
        stores.interestRelOids().add(100L);

        Map<Long, JdbcProbe.ProbedRow> endState = new HashMap<>();
        endState.put(100L, new JdbcProbe.ProbedRow(newCtid,
                new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 205, 0)));
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout), new SelfHealer(endState::get));

        TupleBytes newTuple = classTuple(100, "t1", 205, 0, 1);   // 列 1-7 与候选一致（prefix 区）
        WalRecord rec = rec(updateRecord(7, 9, 5, 2, truncatedData(newTuple, 88, 0)));
        replay.applyCatalogRecord(rec, stores);

        assertEquals(new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 205, 0),
                stores.classRows().get(newCtid), "采纳后重建行应落 newCtid");
        assertNull(stores.classRows().get(staleCtid), "陈旧候选副本必须清除（行已物理离开该位）");
        assertEquals(newCtid, stores.trackedTableCtid(), "tracked 须修复到记录 new 位");
        assertTrue(stores.rawClassTails().containsKey(newCtid), "重建 tail 应落 newCtid");
        assertFalse(stores.rawClassTails().containsKey(staleCtid), "陈旧候选 tail 必须清除");
        assertTrue(stores.staleOids().isEmpty(), "采纳不得标 stale");
        assertEquals(1L, stores.metrics().get(CatalogStores.CatalogMetrics.SELF_HEALED));
        assertEquals(0L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED));
    }

    /**
     * 用例 ②：中段 relfilenode=205 与末态 999 不一致（toast 双零无鉴别力）→ 拒绝：
     * 首次失败仅计数（staleOids 空、skippedTruncated=1、tracked 不动、无行落位）；
     * 第二次失败（另一未知 oldCtid 的同形态记录）起 staleOids 含该 oid（Set 去重）。
     */
    @Test
    void midValuesMismatchingEndStateMarksStaleOnSecondFailure() {
        CatalogStores stores = freshStores();
        long staleCtid = CatalogReplay.ctidKey(3, 1);
        long newCtid = CatalogReplay.ctidKey(5, 2);
        stores.classRows().put(staleCtid, CatalogRow.ClassRow.fromDecoded(decode(classTuple(100, "t1", 200, 0, 1))));
        stores.trackedTableCtid(staleCtid);

        Map<Long, JdbcProbe.ProbedRow> endState = new HashMap<>();
        endState.put(100L, new JdbcProbe.ProbedRow(newCtid,
                new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 999, 0)));   // filenode 不一致
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout), new SelfHealer(endState::get));

        byte[] data = truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0);
        replay.applyCatalogRecord(rec(updateRecord(7, 9, 5, 2, data)), stores);
        assertFalse(stores.staleOids().contains(100L), "首次失败仅计数，不标 stale");
        assertEquals(1L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED));
        assertNull(stores.classRows().get(newCtid), "拒绝不得落行");
        assertEquals(staleCtid, stores.trackedTableCtid(), "拒绝不得修 tracked");

        replay.applyCatalogRecord(rec(updateRecord(8, 4, 5, 2, truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0))), stores);
        assertTrue(stores.staleOids().contains(100L), "第二次连续失败须标 stale");
        assertEquals(2L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED));
    }

    /**
     * 用例 ③：连续三次失败不升级为异常（逐次 empty 拒绝，staleOids 去重仍 1 个）；
     * 随后一次成功采纳将连续失败计数清零——其后再失败一次不达 stale 门槛
     * （repeatedFailure=false，非升级路径）。
     */
    @Test
    void consecutiveFailuresDoNotEscalateAndAdoptionResetsCounter() {
        CatalogStores stores = freshStores();
        long staleCtid = CatalogReplay.ctidKey(3, 1);
        stores.classRows().put(staleCtid, CatalogRow.ClassRow.fromDecoded(decode(classTuple(100, "t1", 200, 0, 1))));
        stores.trackedTableCtid(staleCtid);

        Map<Long, JdbcProbe.ProbedRow> endState = new HashMap<>();
        long firstNewCtid = CatalogReplay.ctidKey(5, 2);
        endState.put(100L, new JdbcProbe.ProbedRow(firstNewCtid,
                new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 999, 0)));
        SelfHealer healer = new SelfHealer(endState::get);
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout), healer);

        byte[] data = truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0);
        int[][] olds = {{7, 9}, {8, 4}, {6, 7}};
        for (int[] old : olds) {
            assertDoesNotThrow(() -> replay.applyCatalogRecord(
                    rec(updateRecord(old[0], old[1], 5, 2, data)), stores),
                    "连续失败不得升级为异常");
        }
        assertEquals(1, stores.staleOids().size(), "staleOids 按 oid 去重");

        // 成功采纳：末态行换到与本记录 new 位一致且中段值吻合的形态
        long adoptCtid = CatalogReplay.ctidKey(9, 1);
        endState.put(100L, new JdbcProbe.ProbedRow(adoptCtid,
                new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 205, 0)));
        replay.applyCatalogRecord(rec(updateRecord(4, 4, 9, 1,
                truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0))), stores);
        assertEquals(adoptCtid, stores.trackedTableCtid(), "成功路径 tracked 须修复");

        // 计数已清零：再失败一次仅计数 1，不重复触发 stale 门槛
        replay.applyCatalogRecord(rec(updateRecord(2, 3, 6, 6,
                truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0))), stores);
        assertFalse(healer.repeatedFailure(100L), "采纳后连续计数清零，单次失败不达门槛");
        assertEquals(1, stores.staleOids().size());
    }

    // ---- 测试基建：记录拼装（CatalogReplayTest 同源自持副本） ----------------------

    /**
     * 造一套已登记 pg_class relfilenode 的空 stores。
     *
     * @return 新 CatalogStores 实例
     */
    private CatalogStores freshStores() {
        CatalogStores stores = new CatalogStores();
        stores.pgAttrRelfilenode(6001);
        stores.pgClassRelfilenode(CLASS_RELNODE);
        return stores;
    }

    /**
     * 字节 → 走读完成的 WalRecord（LSN 定值即可，重放面不消费）。
     *
     * @param bytes WalBytes.build() 产物
     * @return 解析结果
     */
    private WalRecord rec(byte[] bytes) {
        return WalRecordParser.parse(bytes, 0x100000L, layout);
    }

    /**
     * XLOG_HEAP_UPDATE 截断记录（跨页）：14B main（old_offnum u16@4、flags u8@7 置
     * 截断位组、new_offnum u16@12）+ 块 0（新页）data + 块 1（旧页，SAME_REL）无载荷
     * ——oldCtid 刻意取 stores 未登记位（未知旧行，自愈入口）。
     *
     * @param oldBlock  旧页块号（未知旧行所在）
     * @param oldOffnum 旧行行号
     * @param newBlock  新页块号
     * @param newOffnum 新行行号
     * @param newData   截断形态的新 tuple data
     * @return 记录字节
     */
    private byte[] updateRecord(int oldBlock, int oldOffnum, int newBlock, int newOffnum, byte[] newData) {
        byte[] main = new byte[14];
        main[4] = (byte) oldOffnum;
        main[5] = (byte) (oldOffnum >>> 8);
        main[7] = (byte) HeapOps.XLH_UPDATE_TRUNCATION;
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
     * 截断 UPDATE 的块 0 data（双 u16 前缀形态）：[prefix u16][suffix u16]
     * [xl_heap_header 5B][bitmap+pad][数据区自 t_hoff+prefix 起]——heapam.c 截断
     * 日志的 chunk 布局。
     *
     * @param tuple  新行完整 tuple DSL（值供裁切）
     * @param prefix 前缀字节数（自旧行取）
     * @param suffix 后缀字节数（自旧行取）
     * @return 块 data 字节
     */
    private byte[] truncatedData(TupleBytes tuple, int prefix, int suffix) {
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
     * pg_class 全 34 列 tuple DSL（词典取 layout，值面按参数 + 固定填充——与
     * CatalogReplayTest 同源，列 1-7 固定值保证 prefix 区候选无关）。
     *
     * @param oidV     relOid（列 1）
     * @param relname  relname（列 2）
     * @param filenode relfilenode（列 8，数据区偏移 88——中段校验面）
     * @param toast    reltoastrelid（列 14，数据区偏移 112——中段校验面）
     * @param relpages relpages（列 10，中段噪声面）
     * @return 已写满全部列的 DSL
     */
    private TupleBytes classTuple(long oidV, String relname, long filenode, long toast, int relpages) {
        return TupleBytes.of(layout.pgClassKinds())
                .oid(oidV)          // oid
                .name(relname)      // relname
                .oid(11)            // relnamespace
                .oid(12)            // reltype
                .oid(0)             // reloftype
                .oid(10)            // relowner
                .oid(0)             // relam
                .oid(filenode)      // relfilenode @88
                .oid(0)             // reltablespace
                .i32(relpages)      // relpages @96
                .f4(0)              // reltuples
                .i32(0)             // relallvisible
                .i32(0)             // relallfrozen
                .oid(toast)         // reltoastrelid @112
                .bool(false)        // relhasindex
                .bool(false)        // relisshared
                .ch('p')            // relpersistence
                .ch('r')            // relkind
                .i2(2)              // relnatts
                .i2(0)              // relchecks
                .bool(false)        // relhasrules
                .bool(false)        // relhastriggers
                .bool(false)        // relhassubclass
                .bool(false)        // relrowsecurity
                .bool(false)        // relforcerowsecurity
                .bool(true)         // relispopulated
                .ch('d')            // relreplident
                .bool(false)        // relispartition
                .oid(0)             // relrewrite
                .i32(0)             // relfrozenxid
                .i32(0)             // relminmxid
                .skipVarlena("a")   // relacl
                .skipVarlena("b")   // reloptions
                .skipVarlena("c");  // relpartbound
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
