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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 截断自愈（SelfHealer + CatalogReplay 注入）的失败先行测试——stub probe（ctid →
 * 末态行映射）替换 JDBC 面，经 {@link CatalogReplay#applyCatalogRecord} 走真实截断
 * 更新路径后断言 stores 终态（spec §6② 的 Task 13 收敛形态：ctid 寻址精确采纳；
 * Task 12 的候选枚举 + 中段值校验形态经对抗性 IT 实证存在时序身份混窗，已废——
 * <strong>采纳行值整行来自探测行</strong>是本组断言的核心面）。
 *
 * <p>三用例：①ctid 探测命中 → 精确采纳（探测行整行落位 + tracked 修复 + 陈旧副本
 * 清除）；②ctid 探测无行（行已再迁移，记录形态过时）→ 拒绝走 skip（不抛、不动
 * tracked）；③采纳撤回 stale 标记（tracked 断链态下 interest 最小 oid 归表位）。</p>
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
     * 用例 ①：未知 oldCtid 的截断更新——stub 末态仍居记录 new 位（ctid 探测命中）→
     * 精确采纳：行值<strong>整行来自探测行</strong>（relfilenode 205——与字典陈旧候选
     * 的 200 不同源，时代错位混窗由本断言钉死）、行落 newCtid、tracked 修复到 new 位、
     * 陈旧候选副本（行 + tail）清除、selfHealed 计数、skippedTruncated 不计。
     */
    @Test
    void ctidExactAdoptionLandsProbedRowAndRepairsTracked() {
        CatalogStores stores = freshStores();
        long staleCtid = CatalogReplay.ctidKey(3, 1);
        long newCtid = CatalogReplay.ctidKey(5, 2);
        CatalogRow.ClassRow candidate = CatalogRow.ClassRow.fromDecoded(decode(classTuple(100, "t1", 200, 0, 1)), layout);
        stores.classRows().put(staleCtid, candidate);
        stores.rawClassTails().put(staleCtid, new byte[]{1, 2, 3});
        stores.trackedTableCtid(staleCtid);
        stores.interestRelOids().add(100L);

        CatalogRow.ClassRow probed = new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 205, 0, "r");
        Map<String, JdbcProbe.ProbedRow> byCtid = new HashMap<>();
        byCtid.put("(5,2)", new JdbcProbe.ProbedRow(newCtid, probed));
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout),
                new SelfHealer(new StubProbe(byCtid)));

        WalRecord rec = rec(updateRecord(7, 9, 5, 2,
                truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0)));
        replay.applyCatalogRecord(rec, stores);

        assertEquals(probed, stores.classRows().get(newCtid), "采纳行须整行取自探测行（精确，无拼装）");
        assertNull(stores.classRows().get(staleCtid), "陈旧候选副本必须清除（行已物理离开该位）");
        assertEquals(newCtid, stores.trackedTableCtid(), "tracked 须修复到记录 new 位");
        assertFalse(stores.rawClassTails().containsKey(staleCtid), "陈旧候选 tail 必须清除");
        assertEquals(1L, stores.metrics().get(CatalogStores.CatalogMetrics.SELF_HEALED));
        assertEquals(0L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED));
    }

    /**
     * 用例 ②：ctid 探测无行（行已再迁移——记录形态过时）→ 采纳拒绝走 skip：不计
     * selfHealed、计 skippedTruncated、无行落位、tracked 不动、不抛异常（连续拒绝
     * 不升级——链由该 oid 末条记录追平后的精确采纳收敛）。
     */
    @Test
    void ctidVacantAtRecordPositionRejectsToSkip() {
        CatalogStores stores = freshStores();
        long staleCtid = CatalogReplay.ctidKey(3, 1);
        long newCtid = CatalogReplay.ctidKey(5, 2);
        stores.classRows().put(staleCtid, CatalogRow.ClassRow.fromDecoded(decode(classTuple(100, "t1", 200, 0, 1)), layout));
        stores.trackedTableCtid(staleCtid);
        stores.interestRelOids().add(100L);

        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout),
                new SelfHealer(new StubProbe(new HashMap<>())));
        byte[] data = truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0);
        assertDoesNotThrow(() -> replay.applyCatalogRecord(rec(updateRecord(7, 9, 5, 2, data)), stores),
                "探测无行的拒绝不得升级为异常");
        assertEquals(1L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED));
        assertEquals(0L, stores.metrics().get(CatalogStores.CatalogMetrics.SELF_HEALED));
        assertNull(stores.classRows().get(newCtid), "拒绝不得落行");
        assertEquals(staleCtid, stores.trackedTableCtid(), "拒绝不动 tracked");
    }

    /**
     * 用例 ③：tracked 断链态（字典无表行副本）下的采纳——interest 最小 oid 保守归
     * 表位（repairTracked 断链修复档）且撤回既往 stale 标记（链已修复，下轮引导
     * 不再按 stale 裁剪）。
     */
    @Test
    void adoptionClearsStaleMarkAndRepairsBrokenTracked() {
        CatalogStores stores = freshStores();
        long newCtid = CatalogReplay.ctidKey(9, 1);
        stores.interestRelOids().add(100L);
        stores.staleOids().add(100L);

        CatalogRow.ClassRow probed = new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 205, 0, "r");
        Map<String, JdbcProbe.ProbedRow> byCtid = new HashMap<>();
        byCtid.put("(9,1)", new JdbcProbe.ProbedRow(newCtid, probed));
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout),
                new SelfHealer(new StubProbe(byCtid)));

        replay.applyCatalogRecord(rec(updateRecord(4, 4, 9, 1,
                truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0))), stores);
        assertEquals(newCtid, stores.trackedTableCtid(), "tracked 断链态（无表行副本）下 interest 最小 oid 归表位");
        assertTrue(stores.staleOids().isEmpty(), "采纳须撤回 stale 标记");
    }

    /**
     * 用例 ④（审查裁定 3）：stale 轻量复活——同 oid（tracked 表面归因）连续 3 次采纳
     * 拒绝（ctid 探测无行）向 staleOids 登记（待下轮引导 = 重启路径）；其后一次成功
     * 采纳清零计数并撤回 stale 标记（再单次拒绝不重登）。
     */
    @Test
    void consecutiveAdoptRejectionsRegisterStaleUntilAdoptionClears() {
        CatalogStores stores = freshStores();
        long staleCtid = CatalogReplay.ctidKey(3, 1);
        long newCtid = CatalogReplay.ctidKey(5, 2);
        stores.classRows().put(staleCtid, CatalogRow.ClassRow.fromDecoded(decode(classTuple(100, "t1", 200, 0, 1)), layout));
        stores.trackedTableCtid(staleCtid);
        stores.interestRelOids().add(100L);

        Map<String, JdbcProbe.ProbedRow> byCtid = new HashMap<>();
        CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout),
                new SelfHealer(new StubProbe(byCtid)));
        byte[] data = truncatedData(classTuple(100, "t1", 205, 0, 1), 88, 0);
        int[][] olds = {{7, 9}, {8, 4}, {6, 7}};
        for (int[] old : olds) {
            replay.applyCatalogRecord(rec(updateRecord(old[0], old[1], 5, 2, data)), stores);
        }
        assertTrue(stores.staleOids().contains(100L), "连续 3 次拒绝须登记 stale（待下轮引导）");
        assertEquals(3L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED));

        // 成功采纳：ctid 探测命中 → 计数清零 + stale 撤回
        long adoptCtid = CatalogReplay.ctidKey(9, 1);
        byCtid.put("(9,1)", new JdbcProbe.ProbedRow(adoptCtid,
                new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 205, 0, "r")));
        replay.applyCatalogRecord(rec(updateRecord(4, 4, 9, 1, data)), stores);
        assertTrue(stores.staleOids().isEmpty(), "采纳须撤回 stale 标记");

        // 计数已清零：再单次拒绝不重登（连续计数从 0 起）
        replay.applyCatalogRecord(rec(updateRecord(2, 3, 6, 6, data)), stores);
        assertTrue(stores.staleOids().isEmpty(), "清零后单次拒绝不达门槛");
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
     * @param filenode relfilenode（列 8，数据区偏移 88）
     * @param toast    reltoastrelid（列 14，数据区偏移 112）
     * @param relpages relpages（列 10）
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

    /**
     * stub 探测器：ctid 文本 → pg_class 末态行点查（精确采纳面）；oid 点查与 attr 面
     * 点查恒 null（本类用例只触 class 面精确采纳——Task 13 收敛后候选枚举已废）。
     */
    private static final class StubProbe implements JdbcProbe {

        private final Map<String, ProbedRow> byCtid;

        /**
         * 构造 stub。
         *
         * @param byCtid ctid 文本（"(b,o)"）→ 末态行
         */
        StubProbe(Map<String, ProbedRow> byCtid) {
            this.byCtid = byCtid;
        }

        /**
         * oid 点查——本类不触，恒 null。
         *
         * @param relOid 关系 oid
         * @return 恒 null
         */
        @Override
        public ProbedRow currentClassRow(long relOid) {
            return null;
        }

        /**
         * attr 面 ctid 点查——本类不触，恒 null。
         *
         * @param ctidText ctid 文本形态
         * @return 恒 null
         */
        @Override
        public CatalogRow.AttrRow currentAttrRowByCtid(String ctidText) {
            return null;
        }

        /**
         * class 面 ctid 点查——精确采纳的 stub 数据面。
         *
         * @param ctidText ctid 文本形态
         * @return 映射行；无映射 null（采纳拒绝路径）
         */
        @Override
        public ProbedRow currentClassRowByCtid(String ctidText) {
            return byCtid.get(ctidText);
        }
    }
}
