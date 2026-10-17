package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CatalogStores} 五二级索引（oid/filenode/attrelid/nspOid/partitionFilenode）
 * 与写点包装方法（putXxxRow/removeXxxRow/rebuildIndexes）的契约测试——索引应答与
 * 主字典值域线性扫同果、put/remove/rebuild 全生命周期同步、同键陈旧副本的"最后
 * put 者胜"与 remove 防误删（索引现值 == old 才移除）、filenode 0 键不入、
 * shapeEpoch 随行变更单调自增。
 *
 * <p>手造 CatalogRow 四类行直接装载（与 CatalogBootstrap 种子后的形态同源）；
 * 纯离线单测，零 JDBC/Docker。</p>
 */
class CatalogStoresIndexTest {

    /**
     * 用例 1：索引应答与线性扫同果 + put/remove/rebuild 全生命周期同步——装载
     * 3 张 pg_class 行（含 1 张 relfilenode=0 的映射目录形态）+ 4 条 attr 行
     * （含 dropped 占位与他关系行）+ 2 条 nsp 行 + 母表/分区两行 pg_partition 后，
     * 六查询面逐一与主字典值域线性扫对拍；remove 后索引同步消失；同键两 ctid
     * 副本先后 put、先删旧副本时索引保住后写行（防误删判定）；rebuildIndexes
     * 与增量维护同果。
     */
    @Test
    void indexesAnswerSameAsLinearScanAcrossPutRemoveRebuild() {
        CatalogStores s = new CatalogStores();
        // ---- 装载：3 class + 4 attr + 2 nsp + 2 partition ----
        CatalogRow.ClassRow classRowA = new CatalogRow.ClassRow(1001, "t1", 2200, 1005, 0, 10, 2, 5001, 0, "r");
        CatalogRow.ClassRow classRowMapped = new CatalogRow.ClassRow(1249, "pg_attribute", 11, 2610, 0, 10, 2, 0, 0, "r");
        CatalogRow.ClassRow classRowC = new CatalogRow.ClassRow(1002, "t_part", 2200, 1006, 0, 10, 2, 5003, 0, "p");
        long ctidA = (1L << 32) | 1;
        long ctidMapped = (1L << 32) | 2;
        long ctidC = (1L << 32) | 3;
        s.putClassRow(ctidA, classRowA);
        s.putClassRow(ctidMapped, classRowMapped);
        s.putClassRow(ctidC, classRowC);
        CatalogRow.AttrRow attrId = new CatalogRow.AttrRow(1001, "id", 20, 1, false);
        CatalogRow.AttrRow attrDropped = new CatalogRow.AttrRow(1001, "........pg.dropped.2........", 0, 2, true);
        CatalogRow.AttrRow attrOtherRel = new CatalogRow.AttrRow(1002, "v", 25, 1, false);
        CatalogRow.AttrRow attrOtherTable = new CatalogRow.AttrRow(2001, "x", 23, 1, false);
        long attrCtid1 = (2L << 32) | 1;
        long attrCtid2 = (2L << 32) | 2;
        long attrCtid3 = (2L << 32) | 3;
        long attrCtid4 = (2L << 32) | 4;
        s.putAttrRow(attrCtid1, attrId);
        s.putAttrRow(attrCtid2, attrDropped);
        s.putAttrRow(attrCtid3, attrOtherRel);
        s.putAttrRow(attrCtid4, attrOtherTable);
        CatalogRow.NspRow nspPublic = new CatalogRow.NspRow(2200, "public");
        CatalogRow.NspRow nspCatalog = new CatalogRow.NspRow(11, "pg_catalog");
        s.putNspRow((3L << 32) | 1, nspPublic);
        s.putNspRow((3L << 32) | 2, nspCatalog);
        CatalogRow.PartitionRow motherR = new CatalogRow.PartitionRow(3000, "t_part", "r", 3000, 0, 0);
        CatalogRow.PartitionRow partP = new CatalogRow.PartitionRow(3001, "p1", "p", 3000, 5002, 0);
        s.putPartitionRow((4L << 32) | 1, motherR);
        s.putPartitionRow((4L << 32) | 2, partP);

        // ---- 索引应答与线性扫同果 ----
        assertEquals(classRowA, s.classByOid(1001));
        assertEquals(classRowMapped, s.classByOid(1249));
        assertNull(s.classByOid(9999));
        assertEquals(classRowA, s.classByFilenode(5001));
        assertEquals(classRowC, s.classByFilenode(5003));
        assertNull(s.classByFilenode(0), "映射目录行 relfilenode 列值 0 不得入 filenode 索引");
        for (long oid : new long[]{1001, 1249, 1002, 9999}) {
            assertEquals(linearClassByOid(s, oid), s.classByOid(oid), "classByOid(" + oid + ") 须与线性扫同果");
        }
        for (long filenode : new long[]{5001, 5003, 9999}) {
            assertEquals(linearClassByFilenode(s, filenode), s.classByFilenode(filenode),
                    "classByFilenode(" + filenode + ") 须与线性扫同果");
        }
        assertEquals(2, s.attrsByRel(1001).size(), "含 dropped 占位行");
        assertEquals(List.of(attrId, attrDropped), s.attrsByRel(1001));
        assertEquals(List.of(attrOtherRel), s.attrsByRel(1002));
        assertNull(s.attrsByRel(9999));
        assertEquals(2, linearAttrsByRel(s, 1001).size(), "索引面行集与线性扫同规模");
        assertEquals(nspPublic, s.nspByOid(2200));
        assertEquals(nspCatalog, s.nspByOid(11));
        assertNull(s.nspByOid(9999));
        assertEquals(partP, s.partitionByFilenode(5002));
        assertNull(s.partitionByFilenode(0), "母表 'r' 行 relfilenode 恒 0 不得入 partition filenode 索引");
        assertEquals(linearPartitionByFilenode(s, 5002), s.partitionByFilenode(5002));

        // ---- remove 后索引同步消失 ----
        s.removeClassRow(ctidA);
        assertNull(s.classByOid(1001));
        assertNull(s.classByFilenode(5001));
        s.removeAttrRow(attrCtid1);
        assertEquals(1, s.attrsByRel(1001).size(), "dropped 占位行仍在");

        // ---- 同键两 ctid 副本先后 put、先删旧：防误删判定保住后写行 ----
        CatalogRow.ClassRow staleCopy = new CatalogRow.ClassRow(2001, "t2", 2200, 0, 0, 10, 2, 6001, 0, "r");
        CatalogRow.ClassRow freshCopy = new CatalogRow.ClassRow(2001, "t2v2", 2200, 0, 0, 10, 2, 6001, 0, "r");
        long staleCtid = (5L << 32) | 1;
        long freshCtid = (5L << 32) | 2;
        s.putClassRow(staleCtid, staleCopy);
        s.putClassRow(freshCtid, freshCopy);
        assertEquals(freshCopy, s.classByOid(2001), "同 oid 双副本共存窗口内索引取最后 put 者");
        s.removeClassRow(staleCtid);
        assertEquals(freshCopy, s.classByOid(2001), "删除旧副本不得误删后写行的索引（索引现值 != old 不移除）");
        assertEquals(freshCopy, s.classByFilenode(6001));

        // ---- rebuildIndexes 与增量维护同果 ----
        s.rebuildIndexes();
        assertEquals(freshCopy, s.classByOid(2001));
        assertEquals(classRowMapped, s.classByOid(1249));
        assertNull(s.classByOid(1001), "已 remove 的行 rebuild 后不得还魂");
        assertNull(s.classByFilenode(0));
        assertEquals(partP, s.partitionByFilenode(5002));
        assertEquals(1, s.attrsByRel(1001).size());
        assertNotNull(s.nspByOid(2200));
        assertTrue(s.shapeEpoch() > 0);
    }

    /**
     * 用例 2：shapeEpoch 随每次行变更自增——put +1、remove（命中行）+1、no-op
     * remove 不动、rebuildIndexes +1；代际戳单调递增（Task 6 将以它作缓存代际键）。
     */
    @Test
    void epochBumpsOnEveryRowMutation() {
        CatalogStores s = new CatalogStores();
        long e0 = s.shapeEpoch();
        CatalogRow.AttrRow attrRow = new CatalogRow.AttrRow(1001, "id", 20, 1, false);
        s.putAttrRow(1L, attrRow);
        assertEquals(e0 + 1, s.shapeEpoch());
        s.removeAttrRow(999L);
        assertEquals(e0 + 1, s.shapeEpoch(), "no-op remove（无行）不是行变更，epoch 不动");
        s.removeAttrRow(1L);
        assertEquals(e0 + 2, s.shapeEpoch());
        s.putClassRow(2L, new CatalogRow.ClassRow(1001, "t1", 2200, 0, 0, 10, 2, 5001, 0, "r"));
        assertEquals(e0 + 3, s.shapeEpoch());
        s.removeClassRow(2L);
        assertEquals(e0 + 4, s.shapeEpoch());
        s.rebuildIndexes();
        assertEquals(e0 + 5, s.shapeEpoch(), "rebuildIndexes 亦自增代际戳");
    }

    // ---- 线性扫对照面（对拍锚：主字典值域过滤，即索引化前的查询形态） -------------

    /**
     * 线性扫对照：classRows 值域首配 relOid（数据无同 oid 副本时与索引唯一同果）。
     *
     * @param s   状态容器
     * @param oid 关系 oid
     * @return 首配行；无匹配 null
     */
    private static CatalogRow.ClassRow linearClassByOid(CatalogStores s, long oid) {
        for (CatalogRow.ClassRow row : s.classRows().values()) {
            if (row.relOid() == oid) {
                return row;
            }
        }
        return null;
    }

    /**
     * 线性扫对照：classRows 值域首配 relfilenode（0 键按索引"不入"语义跳过）。
     *
     * @param s         状态容器
     * @param filenode relfilenode
     * @return 首配行；无匹配/ filenode 0 null
     */
    private static CatalogRow.ClassRow linearClassByFilenode(CatalogStores s, long filenode) {
        if (filenode == 0) {
            return null;
        }
        for (CatalogRow.ClassRow row : s.classRows().values()) {
            if (row.relfilenode() == filenode) {
                return row;
            }
        }
        return null;
    }

    /**
     * 线性扫对照：attrRows 值域过滤 attrelid。
     *
     * @param s      状态容器
     * @param relOid 关系 oid
     * @return 匹配行列表
     */
    private static List<CatalogRow.AttrRow> linearAttrsByRel(CatalogStores s, long relOid) {
        List<CatalogRow.AttrRow> out = new ArrayList<>();
        for (CatalogRow.AttrRow row : s.attrRows().values()) {
            if (row.attrelid() == relOid) {
                out.add(row);
            }
        }
        return out;
    }

    /**
     * 线性扫对照：partitionRows 值域首配 relfilenode（0 键跳过）。
     *
     * @param s         状态容器
     * @param filenode 分区物理段号
     * @return 首配行；无匹配 null
     */
    private static CatalogRow.PartitionRow linearPartitionByFilenode(CatalogStores s, long filenode) {
        if (filenode == 0) {
            return null;
        }
        for (CatalogRow.PartitionRow row : s.partitionRows().values()) {
            if (row.relfilenode() == filenode) {
                return row;
            }
        }
        return null;
    }
}
