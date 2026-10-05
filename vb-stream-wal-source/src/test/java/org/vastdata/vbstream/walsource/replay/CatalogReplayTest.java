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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * catalog ctid 重放引擎（CatalogReplay / CatalogStores）的失败先行测试——WalBytes /
 * TupleBytes 手造记录序列（与被测互为双源转录），经 {@link #replay} 全量施加到
 * {@link CatalogStores} 后断言终态。
 *
 * <p>断言面来自任务书 Step 1 六用例：INS data 路径与 FPI-only image 路径解出同值
 * （spike 发现 14/15）；UPD 非截断 ctid 移动 + 旧 key 消失（UPD 必删旧——spike 教训）
 * + DEL；UPD 截断 prefix=88 值编码重建全等（发现 24）/ prefix=96 rawTail splice /
 * 两者皆无 skip 不抛；MULTI_INSERT 3 行 INIT_PAGE（offsets 省略）+ image 路径带
 * offsets 数组；INPLACE relfilenode/reltoastrelid 88/112 直读（发现 17/19）；
 * PRUNE redirect 重定位 tracked + dead/unused 移除 + freeze 段跳过（发现 18）。</p>
 */
class CatalogReplayTest {

    /** 测试用 pg_attribute relfilenode（与 pg_class 区分，块匹配面）。 */
    private static final long ATTR_RELNODE = 6001;

    /** 测试用 pg_class relfilenode。 */
    private static final long CLASS_RELNODE = 6002;

    /** 测试用表空间 oid（任意合法值，走读不校验）。 */
    private static final long SPC = 1663;

    /** 测试用数据库 oid（任意合法值）。 */
    private static final long DB = 16384;

    private final WalLayout layout = WalLayoutV18.INSTANCE;
    private final CatalogReplay replay = new CatalogReplay(layout, new TupleDecoder(layout));

    /**
     * 用例 1a：INS data 路径——block data 携带 xl_heap_header + tuple，事件与
     * stores 终态（attrRows 行值 + rawAttrTails tail 落位）正确；4 参 facade
     * 签名直击（事件 op/newCtid/row 三面）。
     */
    @Test
    void insertDataPathPopulatesRowAndTail() {
        CatalogStores stores = freshStores();
        TupleBytes tuple = attrTuple(100, "c1", 23, 1, false);
        WalRecord rec = rec(insertRecord(ATTR_RELNODE, 0, 1, tuple.payload()));

        List<HeapEvent> events = replay.heapEvents(rec, ATTR_RELNODE, layout.pgAttributeKinds(), stores.rawAttrTails());
        assertEquals(1, events.size());
        assertEquals(HeapEvent.INS, events.get(0).op());
        assertEquals(CatalogReplay.ctidKey(0, 1), events.get(0).newCtid());
        assertEquals(100L, ((Number) events.get(0).row()[0]).longValue());
        assertEquals("c1", events.get(0).row()[1]);

        replay.applyCatalogRecord(rec, stores);
        assertEquals(new CatalogRow.AttrRow(100, "c1", 23, 1, false),
                stores.attrRows().get(CatalogReplay.ctidKey(0, 1)));
        assertTrue(stores.rawAttrTails().containsKey(CatalogReplay.ctidKey(0, 1)),
                "data 路径 INS 应在 newCtid 落 raw tail");
    }

    /**
     * 用例 1b：FPW-only INS image 路径——块只携带整页镜像（无 tuple data），重建页
     * 为变更后状态（发现 15），offnum 行解出与 data 路径同值（发现 14）；image 路径
     * 无 raw tail 可落。
     */
    @Test
    void insertFpwOnlyImagePathDecodesSameValues() {
        CatalogStores stores = freshStores();
        TupleBytes tuple = attrTuple(100, "c1", 23, 1, false);
        byte[] page = pageWithTuples(new int[]{2}, new TupleBytes[]{tuple});
        WalRecord rec = rec(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1)
                .block(0, SPC, DB, ATTR_RELNODE, 0)
                .image(page, 0, 0)
                .main(new byte[]{2, 0, 0})
                .build());

        replay.applyCatalogRecord(rec, stores);
        assertEquals(new CatalogRow.AttrRow(100, "c1", 23, 1, false),
                stores.attrRows().get(CatalogReplay.ctidKey(0, 2)),
                "image 路径应解出与 data 路径同值的行");
        assertFalse(stores.rawAttrTails().containsKey(CatalogReplay.ctidKey(0, 2)),
                "image 路径无 raw tail");
    }

    /**
     * 用例 2：UPD 非截断（跨页）——新行落新 ctid、旧 ctid 行与 tail 双消（UPD 必删旧，
     * spike 教训），tracked ctid 跟随移动；随后 DEL 施加在新 ctid 上行与 tail 双消。
     */
    @Test
    void updateMovesCtidDeletesOldKeyThenDeleteRemovesRow() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        TupleBytes oldTuple = classTuple(100, "t1", 200, 0, 1);
        stores.classRows().put(oldKey, CatalogRow.ClassRow.fromDecoded(decode(oldTuple)));
        stores.rawClassTails().put(oldKey, tailOf(oldTuple));
        stores.trackedTableCtid(oldKey);

        TupleBytes newTuple = classTuple(100, "t2", 205, 0, 1);
        WalRecord upd = rec(updateRecord(0, 3, 1, 5, 2, newTuple.payload()));
        replay.applyCatalogRecord(upd, stores);

        assertNull(stores.classRows().get(oldKey), "UPD 后旧 ctid 行必须消失");
        assertEquals(new CatalogRow.ClassRow(100, "t2", 11, 12, 0, 10, 0, 205, 0),
                stores.classRows().get(newKey));
        assertFalse(stores.rawClassTails().containsKey(oldKey), "UPD 后旧 raw tail 必须消失");
        assertTrue(stores.rawClassTails().containsKey(newKey));
        assertEquals(newKey, stores.trackedTableCtid(), "tracked ctid 须跟随 UPD 移动");

        WalRecord del = rec(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_DELETE, 1)
                .block(0, SPC, DB, CLASS_RELNODE, 5)
                .main(new byte[]{0, 0, 0, 0, 2, 0, 0, 0})
                .build());
        replay.applyCatalogRecord(del, stores);
        assertNull(stores.classRows().get(newKey), "DEL 后行必须消失");
        assertFalse(stores.rawClassTails().containsKey(newKey), "DEL 后 raw tail 必须消失");
    }

    /**
     * 用例 3a：UPD 截断 prefix=88 值编码重建（发现 24）——旧 ClassRow 已知时按
     * encodeReadRegion 重建前 88B（列 1-7 定宽区，读区编码的子段），解码全等；重建
     * tail 与独立拼装的期望字节（旧值编码 + 新行中段）逐字节一致（双源互证）。
     */
    @Test
    void truncatedUpdatePrefix88RebuildsByValueEncoding() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        TupleBytes oldTuple = classTuple(100, "t1", 200, 0, 1);
        CatalogRow.ClassRow oldRow = CatalogRow.ClassRow.fromDecoded(decode(oldTuple));
        stores.classRows().put(oldKey, oldRow);

        TupleBytes newTuple = classTuple(100, "t1", 201, 0, 1);   // 列 1-7 与旧行一致（prefix 区）
        WalRecord rec = rec(updateRecord(HeapOps.XLH_UPDATE_TRUNCATION,
                3, 1, 5, 2, truncatedData(newTuple, 88, 0)));
        replay.applyCatalogRecord(rec, stores);

        assertNull(stores.classRows().get(oldKey));
        assertEquals(new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 201, 0),
                stores.classRows().get(newKey), "prefix=88 应由已知行值编码重建出全行");

        // 期望 tail = [bitmap+pad] + 旧行读区编码前 88B + 新行中段（数据区 88 起）
        byte[] newTail = tailOf(newTuple);
        int bitmapLen = newTuple.tHoff() - 23;
        ByteArrayOutputStream expect = new ByteArrayOutputStream();
        expect.write(newTail, 0, bitmapLen);
        expect.write(oldRow.encodeReadRegion(), 0, 88);
        expect.write(newTail, bitmapLen + 88, newTail.length - bitmapLen - 88);
        assertArrayEquals(expect.toByteArray(), stores.rawClassTails().get(newKey));
    }

    /**
     * 用例 3d：UPD 截断 RENAME 形态（prefix 落 relname 区 + 后缀起点越入读区）的
     * <strong>读区回填</strong>（审查 High-1）——suffix 零填充区起点 &lt; 读区末尾
     * （reltoastrelid 末尾 116）时，重叠段必须按旧行读区编码回填精确值而非留零：
     * 旧行值模型 namespace/owner 与 relfilenode/reltoastrelid 均须完整回填（修复前
     * relnamespace..relam/relfilenode/reltoastrelid 全部被清零——Task 15 诊断的
     * RENAME 零字段行）。中段只携带 [prefix, 零填充起点) 的新 relname 尾段。
     */
    @Test
    void truncatedRenameBackfillsReadRegionFromOldRow() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        // 旧行值模型带非默认标识值（77/88/66 + filenode 200/toast 900）——回填判别面
        CatalogRow.ClassRow oldRow = new CatalogRow.ClassRow(100, "t1", 77, 88, 0, 66, 0, 200, 900);
        stores.classRows().put(oldKey, oldRow);

        TupleBytes newTuple = classTuple(100, "t1x", 201, 0, 1);
        int bitmapLen = newTuple.tHoff() - 23;
        int dataLen = newTuple.payload().length - 5 - bitmapLen;
        int prefix = 4 + 2;              // oid + relname 共有前缀 "t1"
        int suffix = dataLen - 68;       // 零填充起点钉在 relnamespace@68（越入读区）
        WalRecord rec = rec(updateRecord(HeapOps.XLH_UPDATE_TRUNCATION,
                3, 1, 5, 2, truncatedData(newTuple, prefix, suffix)));
        replay.applyCatalogRecord(rec, stores);

        // 中段只含新 relname 尾段;读区 [68,116) 全部来自旧行回填（suffix 截断的
        // 定义:省略段与旧元组逐字节相同）——filenode/toast 取旧行值而非新行 DSL 值
        assertEquals(new CatalogRow.ClassRow(100, "t1x", 77, 88, 0, 66, 0, 200, 900),
                stores.classRows().get(newKey), "RENAME 形态读区段须按旧行回填而非零填充");
        assertNull(stores.classRows().get(oldKey));
    }

    /**
     * 用例 3b：UPD 截断 prefix=96 且 rawTail 在——越出值编码区（&gt;88）走 rawTail
     * splice：bitmap + 旧 tail 前 96B + 记录中段 + 旧 tail 尾部 suffix 字节，
     * 解码出 relfilenode（prefix 区，未变）与 reltoastrelid/relpages（中段，已变）。
     */
    @Test
    void truncatedUpdatePrefix96SplicesRawTail() {
        CatalogStores stores = freshStores();
        long oldKey = CatalogReplay.ctidKey(3, 1);
        long newKey = CatalogReplay.ctidKey(5, 2);
        TupleBytes oldTuple = classTuple(100, "t1", 200, 0, 1);
        stores.classRows().put(oldKey, CatalogRow.ClassRow.fromDecoded(decode(oldTuple)));
        stores.rawClassTails().put(oldKey, tailOf(oldTuple));

        // 新行：列 1-7 与 relfilenode@88/tablespace@92 落在 prefix=96 区（不变），
        // relpages@96 与 reltoastrelid@112 在中段（1→7 / 0→900），尾部 8B 与旧行一致
        TupleBytes newTuple = classTuple(100, "t1", 200, 900, 7);
        WalRecord rec = rec(updateRecord(HeapOps.XLH_UPDATE_TRUNCATION,
                3, 1, 5, 2, truncatedData(newTuple, 96, 8)));
        replay.applyCatalogRecord(rec, stores);

        assertEquals(new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 200, 900),
                stores.classRows().get(newKey), "prefix=96 应由 rawTail splice 重建");
        assertNull(stores.classRows().get(oldKey));
        assertTrue(stores.rawClassTails().containsKey(newKey));
    }

    /**
     * 用例 3c：UPD 截断而旧行值与 rawTail 皆无——skip 不抛（未知 oldCtid 是窗口外
     * 噪声，spike 差异①：页读兜底已删），skippedTruncated 计数 +1、零行重放。
     */
    @Test
    void truncatedUpdateWithoutOldStateSkipsQuietly() {
        CatalogStores stores = freshStores();
        TupleBytes newTuple = classTuple(100, "t1", 201, 0, 1);
        WalRecord rec = rec(updateRecord(HeapOps.XLH_UPDATE_TRUNCATION,
                3, 1, 5, 2, truncatedData(newTuple, 88, 0)));

        assertDoesNotThrow(() -> replay.applyCatalogRecord(rec, stores));
        assertTrue(stores.classRows().isEmpty(), "skip 不得落任何行");
        assertTrue(stores.rawClassTails().isEmpty());
        assertEquals(1L, stores.metrics().snapshot()
                .getOrDefault(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED, 0L));
        assertEquals(0L, stores.metrics().snapshot()
                .getOrDefault(CatalogStores.CatalogMetrics.REPLAYED, 0L));
    }

    /**
     * 用例 4：MULTI_INSERT 3 行——data 路径 INIT_PAGE 形态（offsets 省略、offnum
     * 隐含 i+1，spike 发现 3）逐 entry 解码落位；image 路径带显式 offsets 数组
     * （乱序 3,1,2）按 offsets 取行。
     */
    @Test
    void multiInsertThreeRowsInitPageAndOffsetsImagePaths() {
        CatalogStores stores = freshStores();
        byte[] data = multiInsertData(List.of(
                attrTuple(100, "a1", 23, 1, false),
                attrTuple(100, "a2", 25, 2, true),
                attrTuple(100, "a3", 20, 3, false)));
        WalRecord init = rec(WalBytes.record(
                        HeapOps.RM_HEAP2_ID,
                        HeapOps.XLOG_HEAP2_MULTI_INSERT | HeapOps.XLOG_HEAP_INIT_PAGE, 1)
                .block(0, SPC, DB, ATTR_RELNODE, 2)
                .data(data)
                .main(new byte[]{1, 0, 3, 0})
                .build());
        replay.applyCatalogRecord(init, stores);

        assertEquals(new CatalogRow.AttrRow(100, "a1", 23, 1, false),
                stores.attrRows().get(CatalogReplay.ctidKey(2, 1)));
        assertEquals(new CatalogRow.AttrRow(100, "a2", 25, 2, true),
                stores.attrRows().get(CatalogReplay.ctidKey(2, 2)));
        assertEquals(new CatalogRow.AttrRow(100, "a3", 20, 3, false),
                stores.attrRows().get(CatalogReplay.ctidKey(2, 3)));
        assertEquals(3, stores.rawAttrTails().size(), "每 entry 应各落一条 raw tail");

        // image 路径：非 INIT_PAGE（offsets 显式），乱序 offsets 3,1,2 验证按数组取行
        TupleBytes t1 = attrTuple(200, "b1", 23, 1, false);
        TupleBytes t3 = attrTuple(200, "b3", 25, 3, false);
        TupleBytes t2 = attrTuple(200, "b2", 20, 2, false);
        byte[] page = pageWithTuples(new int[]{1, 2, 3}, new TupleBytes[]{t1, t2, t3});
        WalRecord imageRec = rec(WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_MULTI_INSERT, 1)
                .block(0, SPC, DB, ATTR_RELNODE, 4)
                .image(page, 0, 0)
                .main(new byte[]{0, 0, 3, 0, 3, 0, 1, 0, 2, 0})
                .build());
        replay.applyCatalogRecord(imageRec, stores);
        assertEquals(new CatalogRow.AttrRow(200, "b3", 25, 3, false),
                stores.attrRows().get(CatalogReplay.ctidKey(4, 3)), "offsets[0]=3 应取 b3");
        assertEquals(new CatalogRow.AttrRow(200, "b1", 23, 1, false),
                stores.attrRows().get(CatalogReplay.ctidKey(4, 1)), "offsets[1]=1 应取 b1");
        assertEquals(new CatalogRow.AttrRow(200, "b2", 20, 2, false),
                stores.attrRows().get(CatalogReplay.ctidKey(4, 2)), "offsets[2]=2 应取 b2");
    }

    /**
     * 用例 5：INPLACE——pg_class 88/112 数据区直读（发现 17/19）：ctid 不动、
     * ClassRow 只更新 relfilenode/reltoastrelid 两字段、raw tail 置失效（后续截断
     * 重建走值编码而非陈旧 tail，spike replayInplace 语义）。
     */
    @Test
    void inplaceUpdatesRelfilenodeAndToastInPlace() {
        CatalogStores stores = freshStores();
        long key = CatalogReplay.ctidKey(0, 1);
        TupleBytes oldTuple = classTuple(100, "t1", 200, 0, 1);
        stores.classRows().put(key, CatalogRow.ClassRow.fromDecoded(decode(oldTuple)));
        stores.rawClassTails().put(key, tailOf(oldTuple));

        TupleBytes newTuple = classTuple(100, "t1", 777, 888, 1);
        byte[] dataRegion = Arrays.copyOfRange(newTuple.payload(),
                5 + (newTuple.tHoff() - 23), newTuple.payload().length);
        byte[] main = new byte[20];       // xl_heap_inplace 固定前缀 20B，offnum u16@0
        main[0] = 1;
        WalRecord rec = rec(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INPLACE, 1)
                .block(0, SPC, DB, CLASS_RELNODE, 0)
                .data(dataRegion)
                .main(main)
                .build());
        replay.applyCatalogRecord(rec, stores);

        assertEquals(new CatalogRow.ClassRow(100, "t1", 11, 12, 0, 10, 0, 777, 888),
                stores.classRows().get(key), "INPLACE 应就地更新 relfilenode/reltoastrelid");
        assertFalse(stores.rawClassTails().containsKey(key), "INPLACE 后 raw tail 必须置失效");
        assertEquals(1L, stores.metrics().snapshot()
                .getOrDefault(CatalogStores.CatalogMetrics.INPLACE_UPDATES, 0L));
    }

    /**
     * 用例 6：PRUNE——freeze 段（nplans=1 + 12B plan）正确跳过后：2 对 redirect
     * 重定位行与 tracked（发现 18）、3 个 nowdead + 1 个 nowunused 移除行与 tail。
     */
    @Test
    void pruneRedirectsTrackedAndDropsDeadWithFreezeSkipped() {
        CatalogStores stores = freshStores();
        long a = CatalogReplay.ctidKey(7, 1);
        long b = CatalogReplay.ctidKey(7, 2);
        CatalogRow.ClassRow rowA = new CatalogRow.ClassRow(101, "ra", 11, 12, 0, 10, 0, 300, 0);
        CatalogRow.ClassRow rowB = new CatalogRow.ClassRow(102, "rb", 11, 12, 0, 10, 0, 301, 0);
        stores.classRows().put(a, rowA);
        stores.classRows().put(b, rowB);
        for (int off : new int[]{10, 11, 12, 13}) {
            stores.classRows().put(CatalogReplay.ctidKey(7, off),
                    new CatalogRow.ClassRow(200 + off, "d" + off, 11, 12, 0, 10, 0, off, 0));
        }
        byte[] tailA = tailOf(classTuple(101, "ra", 300, 0, 1));
        stores.rawClassTails().put(a, tailA);
        stores.trackedTableCtid(a);
        stores.trackedToastCtid(b);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, 1);                              // freeze: nplans=1
        out.write(new byte[14], 0, 14);             // 2B pad + 12B plan（跳过面）
        put16(out, 2);                              // redirected: n=2
        put16(out, 1);
        put16(out, 5);                              // (1 -> 5)
        put16(out, 2);
        put16(out, 6);                              // (2 -> 6)
        put16(out, 3);                              // nowdead: n=3
        put16(out, 10);
        put16(out, 11);
        put16(out, 12);
        put16(out, 1);                              // nowunused: n=1
        put16(out, 13);
        int flags = HeapOps.XLHP_HAS_FREEZE_PLANS | HeapOps.XLHP_HAS_REDIRECTIONS
                | HeapOps.XLHP_HAS_DEAD_ITEMS | HeapOps.XLHP_HAS_NOW_UNUSED_ITEMS;
        WalRecord rec = rec(WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_PRUNE_VACUUM_SCAN, 1)
                .block(0, SPC, DB, CLASS_RELNODE, 7)
                .data(out.toByteArray())
                .main(new byte[]{2, (byte) flags})
                .build());
        replay.applyCatalogRecord(rec, stores);

        assertEquals(rowA, stores.classRows().get(CatalogReplay.ctidKey(7, 5)), "redirect 1->5 应重定位 rowA");
        assertEquals(rowB, stores.classRows().get(CatalogReplay.ctidKey(7, 6)), "redirect 2->6 应重定位 rowB");
        assertNull(stores.classRows().get(a));
        assertNull(stores.classRows().get(b));
        for (int off : new int[]{10, 11, 12, 13}) {
            assertNull(stores.classRows().get(CatalogReplay.ctidKey(7, off)), "dead/unused 行必须移除");
        }
        assertEquals(CatalogReplay.ctidKey(7, 5), stores.trackedTableCtid(), "tracked 应随 redirect 移动");
        assertEquals(CatalogReplay.ctidKey(7, 6), stores.trackedToastCtid());
        assertArrayEquals(tailA, stores.rawClassTails().get(CatalogReplay.ctidKey(7, 5)), "tail 随 redirect 重定位");
        assertEquals(2L, stores.metrics().snapshot()
                .getOrDefault(CatalogStores.CatalogMetrics.PRUNE_REDIRECTS, 0L));
        assertEquals(4L, stores.metrics().snapshot()
                .getOrDefault(CatalogStores.CatalogMetrics.PRUNE_DROPPED, 0L));
        // attr 面不受本记录影响（块 relnode 属 pg_class）
        assertTrue(stores.attrRows().isEmpty());
    }

    /**
     * 用例 6b（回归，审查修复锚）：pg_attribute 的 PRUNE redirect 的 from 键与
     * trackedTableCtid <strong>数值相同</strong>——ctidKey 无关系判别、两目录块号键
     * 空间完全重叠，tracked 双 ctid 是 pg_class 行位：attr 分支不得搬移 tracked
     * （行本身照常重定位）。
     */
    @Test
    void attrPruneRedirectDoesNotMoveTrackedEvenOnKeyCollision() {
        CatalogStores stores = freshStores();
        long collideKey = CatalogReplay.ctidKey(9, 1);    // 数值上与 tracked 相同
        stores.attrRows().put(collideKey, new CatalogRow.AttrRow(100, "c1", 23, 1, false));
        stores.trackedTableCtid(collideKey);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        put16(out, 1);      // redirected: n=1
        put16(out, 1);
        put16(out, 4);      // (1 -> 4)，from 键 == trackedTableCtid
        WalRecord rec = rec(WalBytes.record(HeapOps.RM_HEAP2_ID, HeapOps.XLOG_HEAP2_PRUNE_VACUUM_SCAN, 1)
                .block(0, SPC, DB, ATTR_RELNODE, 9)
                .data(out.toByteArray())
                .main(new byte[]{2, (byte) HeapOps.XLHP_HAS_REDIRECTIONS})
                .build());
        replay.applyCatalogRecord(rec, stores);

        assertEquals(new CatalogRow.AttrRow(100, "c1", 23, 1, false),
                stores.attrRows().get(CatalogReplay.ctidKey(9, 4)), "attr 行本身应照常重定位");
        assertEquals(collideKey, stores.trackedTableCtid(),
                "tracked 是 pg_class 行位：attr redirect 数值命中不得搬移");
    }

    /**
     * 用例 3d（回归，审查修复锚）：pg_attribute 侧的截断 UPDATE skip 同样计入
     * skippedTruncated——指标面覆盖两目录（attr 侧无值编码路径，旧行值与 raw tail
     * 皆无即 skip）。
     */
    @Test
    void attrTruncatedUpdateSkipCountsInMetrics() {
        CatalogStores stores = freshStores();
        TupleBytes tuple = attrTuple(100, "c1", 23, 1, false);
        byte[] data = truncatedData(tuple, 88, 0);
        byte[] main = new byte[14];
        main[4] = 1;    // old_offnum
        main[7] = (byte) HeapOps.XLH_UPDATE_TRUNCATION;
        main[12] = 2;   // new_offnum
        WalRecord rec = rec(WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_UPDATE, 1)
                .block(0, SPC, DB, ATTR_RELNODE, 5)
                .data(data)
                .sameRel(0, 3)
                .main(main)
                .build());

        assertDoesNotThrow(() -> replay.applyCatalogRecord(rec, stores));
        assertTrue(stores.attrRows().isEmpty());
        assertEquals(1L, stores.metrics().get(CatalogStores.CatalogMetrics.SKIPPED_TRUNCATED),
                "attr 侧截断 skip 须计入 skippedTruncated");
    }

    // ---- 测试基建：记录拼装（WalBytes DSL 之上的 heap 家族便捷档） ----------------

    /**
     * 造一套已登记两 watched 目录 relfilenode 的空 stores（引导前其余字段为零值）。
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
     * 字节 → 走读完成的 WalRecord（LSN 定值即可，重放面不消费）。
     *
     * @param bytes WalBytes.build() 产物
     * @return 解析结果
     */
    private WalRecord rec(byte[] bytes) {
        return WalRecordParser.parse(bytes, 0x100000L, layout);
    }

    /**
     * XLOG_HEAP_INSERT 记录（data 路径）：3B main（offnum u16 + flags u8=0）+
     * 块 0 data = tuple 载荷（[xl_heap_header 5B][自 offset 23 起字节]）。
     *
     * @param relnode watched relfilenode
     * @param blockNo 块号
     * @param offnum  新行行号（1-based）
     * @param data    tuple 载荷（TupleBytes.payload()）
     * @return 记录字节
     */
    private byte[] insertRecord(long relnode, int blockNo, int offnum, byte[] data) {
        return WalBytes.record(HeapOps.RM_HEAP_ID, HeapOps.XLOG_HEAP_INSERT, 1)
                .block(0, SPC, DB, relnode, blockNo)
                .data(data)
                .main(new byte[]{(byte) offnum, (byte) (offnum >>> 8), 0})
                .build();
    }

    /**
     * XLOG_HEAP_UPDATE 记录（跨页）：14B main（old_offnum u16@4、flags u8@7、
     * new_offnum u16@12）+ 块 0（新页）data + 块 1（旧页，SAME_REL）无载荷。
     *
     * @param flags     xl_heap_update flags（截断位组等）
     * @param oldBlock  旧页块号
     * @param oldOffnum 旧行行号
     * @param newBlock  新页块号
     * @param newOffnum 新行行号
     * @param newData   新 tuple 载荷（完整或截断形态，由调用方拼装）
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
     * 截断 UPDATE 的块 0 data（双 u16 前缀形态，flags 置 PREFIX|SUFFIX 位组）：
     * [prefix u16][suffix u16][xl_heap_header 5B][bitmap+pad][数据区自 t_hoff+prefix
     * 起、止于 len-suffix]——heapam.c 截断日志的 chunk 布局（spike 移植锚）。
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
     * MULTI_INSERT 的块 0 data：逐 entry [datalen u16][xl_heap_header 5B][tail]，
     * entry 起点 2 对齐（spike 走读锚：datalen 只计 header 之后的 tail 字节）。
     *
     * @param tuples 逐行 tuple DSL
     * @return 块 data 字节
     */
    private byte[] multiInsertData(List<TupleBytes> tuples) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (TupleBytes tuple : tuples) {
            byte[] payload = tuple.payload();
            if (out.size() % 2 != 0) {
                out.write(0);
            }
            put16(out, payload.length - 5);
            out.writeBytes(payload);
        }
        return out.toByteArray();
    }

    /**
     * 拼一个含若干 tuple 的整页（FPW 镜像用）：24B 页头零值 + ItemId 数组
     * （lp_off|LP_NORMAL&lt;&lt;15|lp_len&lt;&lt;17，发现 4 位段）+ tuple 自页尾向下排布。
     *
     * @param offnums 各行行号（1-based，与 ItemId 数组位对应）
     * @param tuples  各行 tuple DSL（与 offnums 同序）
     * @return 8192 字节整页
     */
    private byte[] pageWithTuples(int[] offnums, TupleBytes[] tuples) {
        byte[] page = new byte[8192];
        int lpOff = 8192;
        for (int i = 0; i < offnums.length; i++) {
            byte[] tup = tuples[i].tuple();
            lpOff -= tup.length;
            int itemId = lpOff | (1 << 15) | (tup.length << 17);
            page[24 + (offnums[i] - 1) * 4] = (byte) itemId;
            page[25 + (offnums[i] - 1) * 4] = (byte) (itemId >>> 8);
            page[26 + (offnums[i] - 1) * 4] = (byte) (itemId >>> 16);
            page[27 + (offnums[i] - 1) * 4] = (byte) (itemId >>> 24);
            System.arraycopy(tup, 0, page, lpOff, tup.length);
        }
        return page;
    }

    /**
     * pg_attribute 全 25 列 tuple DSL（词典取 layout，值面按参数 + 固定填充）。
     *
     * @param relid   attrelid
     * @param name    attname
     * @param typid   atttypid
     * @param attnum  attnum
     * @param dropped attisdropped
     * @return 已写满全部列的 DSL（可取 payload/tuple/tHoff）
     */
    private TupleBytes attrTuple(long relid, String name, long typid, int attnum, boolean dropped) {
        return TupleBytes.of(layout.pgAttributeKinds())
                .oid(relid)          // attrelid
                .name(name)          // attname
                .oid(typid)          // atttypid
                .i2(-1)              // attlen
                .i2(attnum)          // attnum
                .i32(-1)             // atttypmod
                .i2(0)               // attndims
                .bool(true)          // attbyval
                .ch('i')             // attalign
                .ch('p')             // attstorage
                .ch(' ')             // attcompression
                .bool(false)         // attnotnull
                .bool(false)         // atthasdef
                .bool(false)         // atthasmissing
                .ch(' ')             // attidentity
                .ch(' ')             // attgenerated
                .bool(dropped)       // attisdropped
                .bool(true)          // attislocal
                .i2(0)               // attinhcount
                .oid(0)              // attcollation
                .i2(-1)              // attstattarget
                .skipVarlena("a")    // attacl
                .skipVarlena("b")    // attoptions
                .skipVarlena("c")    // attfdwoptions
                .skipVarlena("d");   // attmissingval
    }

    /**
     * pg_class 全 34 列 tuple DSL（词典取 layout，值面按参数 + 固定填充）。
     *
     * @param oidV     relOid（列 1）
     * @param relname  relname（列 2）
     * @param filenode relfilenode（列 8，数据区偏移 88）
     * @param toast    reltoastrelid（列 14，数据区偏移 112）
     * @param relpages relpages（列 10，数据区偏移 96——截断中段变量面）
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
     * 用被测同源的 TupleDecoder 解码一个 tuple DSL 的 payload（种子行构造用：
     * fromDecoded 与重放路径共享词典/解码器，种子与终态断言两侧各自独立拼装）。
     *
     * @param tuple tuple DSL
     * @return 解码值行
     */
    private Object[] decode(TupleBytes tuple) {
        return new TupleDecoder(layout).decodePayload(tuple.payload(), 0, layout.pgClassKinds());
    }

    /**
     * tuple DSL 的 raw tail（自 heap-tuple offset 23 起的字节 = payload 去掉 5B
     * xl_heap_header）——rawTailStore 的存储契约面。
     *
     * @param tuple tuple DSL
     * @return tail 字节
     */
    private byte[] tailOf(TupleBytes tuple) {
        byte[] payload = tuple.payload();
        return Arrays.copyOfRange(payload, 5, payload.length);
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
