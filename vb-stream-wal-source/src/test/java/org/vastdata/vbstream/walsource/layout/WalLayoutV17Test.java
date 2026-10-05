package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WalLayoutV17 布局常量的失败先行测试——差异常量钉死（Task 16 转录锚：pageMagic
 * 0xD116、sizeOfHeapInplace 2、pg_attribute 26 项含 attcacheoff、pg_class 33 项
 * 缺 relallfrozen、reltoastrelid 偏移 104、两漂移槽位 17/12）、与 V18 的逐常量
 * diff 断言（哪些相同哪些不同——两描述符互为转录交叉核）、版本分发（170000-179999
 * 全区间）与跨版本拒绝（160000/169999/190000/0 抛 ISE）。
 *
 * <p>出处双源：REL_17_STABLE 头文件（xlog_internal.h / xlogrecord.h / heapam_xlog.h /
 * pg_attribute.h，两分支逐 struct diff）+ live postgres:17 容器实查（pg_attribute /
 * pg_class 列序，见 Wal17TestEnv 环境下 Wal17SyncIT 的端到端复核）。</p>
 */
class WalLayoutV17Test {

    private final WalLayout layout = WalLayoutV17.INSTANCE;

    /** 页魔数必须是 REL_17_STABLE 的 XLOG_PAGE_MAGIC 0xD116——与 V18 恰差低字节 2。 */
    @Test
    void pageMagicMatchesRel17Stable() {
        assertEquals(0xD116, layout.pageMagic());
    }

    /** 差异常量逐项钉死：inplace 前缀 2（V17 结构仅 offnum，无 PG 18 的失效消息内联）、
     *  reltoastrelid 数据区偏移 104（缺 relallfrozen 少 4B）、漂移槽位 17/12。 */
    @Test
    void versionDriftedConstantsMatchRel17Transcription() {
        assertEquals(2, layout.sizeOfHeapInplace());
        assertEquals(104, layout.pgClassReltoastrelidDataOffset());
        assertEquals(17, layout.attrDroppedIndex());
        assertEquals(12, layout.classToastRelidIndex());
    }

    /** 与 V18 一致的常量逐项钉死（两分支头文件逐 struct diff 实证）：页/记录头与
     *  heap update/delete 前缀、relfilenode 偏移 88。任一"顺手同步"漂移即红。 */
    @Test
    void sharedConstantsMatchV18() {
        WalLayout v18 = WalLayoutV18.INSTANCE;
        assertEquals(v18.walBlockSize(), layout.walBlockSize());
        assertEquals(8192, layout.walBlockSize());
        assertEquals(v18.shortPageHeaderSize(), layout.shortPageHeaderSize());
        assertEquals(v18.longPageHeaderSize(), layout.longPageHeaderSize());
        assertEquals(v18.recordHeaderSize(), layout.recordHeaderSize());
        assertEquals(v18.sizeOfHeapUpdate(), layout.sizeOfHeapUpdate());
        assertEquals(v18.sizeOfHeapDelete(), layout.sizeOfHeapDelete());
        assertEquals(v18.pgClassRelfilenodeDataOffset(), layout.pgClassRelfilenodeDataOffset());
        // 差异面照抄反向钉死：pageMagic / inplace / toast 偏移两版必须不同
        assertNotSame(0, layout.pageMagic() ^ v18.pageMagic());
        assertEquals(18, v18.sizeOfHeapInplace() - layout.sizeOfHeapInplace());
        assertEquals(8, v18.pgClassReltoastrelidDataOffset() - layout.pgClassReltoastrelidDataOffset());
    }

    /** 两 kinds 词典长度钉死 26/33，且与 V18 的关系恰为"单列差"：attr 多 attcacheoff@5、
     *  class 缺 relallfrozen@12——按插入/删除位逐槽比对，两描述符互为转录交叉核。 */
    @Test
    void kindsDictionariesDifferFromV18ByExactlyOneColumnEach() {
        String[] attr17 = layout.pgAttributeKinds();
        String[] attr18 = WalLayoutV18.INSTANCE.pgAttributeKinds();
        assertEquals(26, attr17.length);
        assertEquals(25, attr18.length);
        assertEquals("int4", attr17[5]);    // attcacheoff（V17 独有）
        for (int i = 0; i < 5; i++) {
            assertEquals(attr18[i], attr17[i], "att 词典槽 " + i + " 前段应同序");
        }
        for (int i = 5; i < attr18.length; i++) {
            assertEquals(attr18[i], attr17[i + 1], "att 词典槽 " + i + " 越过 attcacheoff 后应同序");
        }

        String[] cls17 = layout.pgClassKinds();
        String[] cls18 = WalLayoutV18.INSTANCE.pgClassKinds();
        assertEquals(33, cls17.length);
        assertEquals(34, cls18.length);
        assertEquals("oid", cls17[7]);      // relfilenode（与 V18 同槽）
        assertEquals("oid", cls17[12]);     // reltoastrelid（V18 为 13）
        assertEquals("oid", cls18[13]);
        for (int i = 0; i < 12; i++) {
            assertEquals(cls18[i], cls17[i], "class 词典槽 " + i + " 前段应同序");
        }
        // V18 槽 12 = relallfrozen（int4，V17 缺席）
        assertEquals("int4", cls18[12]);
        for (int i = 12; i < cls17.length; i++) {
            assertEquals(cls18[i + 1], cls17[i], "class 词典槽 " + i + " 越过 relallfrozen 后应同序");
        }
    }

    /** 词典自检锚：attr 槽位语义（attname 定长锚 / attstattarget 可空 int2 / 末列 skip）
     *  与 dropped 槽位的 kind 一致性（attisdropped 是 bool 列）。 */
    @Test
    void kindsSlotsHoldExpectedKinds() {
        String[] attr = layout.pgAttributeKinds();
        assertEquals("name", attr[1]);       // attname（NameData 定长锚）
        assertEquals("int2", attr[21]);      // attstattarget（varlen 区首列，可空）
        assertEquals("skip", attr[25]);      // 末列 attmissingval 为 varlena 跳过列
        assertEquals("bool", attr[layout.attrDroppedIndex()]);
        assertEquals("oid", layout.pgClassKinds()[0]);   // oid 首列（与 V18 同）
    }

    /** supports 按大版本区间判定：17 全区间真、相邻 16/18 假——跨版本交叉全 false。 */
    @Test
    void supportsAcceptsOnlyItsMajorVersion() {
        assertTrue(layout.supports(170000));
        assertTrue(layout.supports(179999));
        assertFalse(layout.supports(169999));
        assertFalse(layout.supports(180000));
        assertFalse(layout.supports(160000));
        assertFalse(WalLayoutV18.INSTANCE.supports(170000));
        assertFalse(layout.supports(180001));
    }

    /** majorVersion 自述 17；分发面把 170000-179999 全区间分发到 V17 描述符。 */
    @Test
    void forServerVersionDispatchesV17AcrossFullRange() {
        assertEquals(17, layout.majorVersion());
        assertEquals(17, WalLayouts.forServerVersion(170000).majorVersion());
        assertEquals(17, WalLayouts.forServerVersion(179999).majorVersion());
        assertEquals(WalLayoutV17.INSTANCE, WalLayouts.forServerVersion(170000));
    }

    /** 跨版本拒绝：16 及以下（含 160000）、19 及以上启动期 ISE fail-fast——V17 描述符
     *  对 18 区间同样拒绝，双向交叉即"伪布局注入不匹配"的离线等价断言（容器级伪布局
     *  注入不做：版本探测由 server_version_num 驱动，容器上 17 环境的正确分发由
     *  Wal17SyncIT 的布局断言承载）。 */
    @Test
    void forServerVersionRejectsForeignMajors() {
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(160000));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(169999));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(190000));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(0));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(-1));
    }

    /** kinds 数组每次返回防御副本：外部改写不得污染描述符的静态词典。 */
    @Test
    void kindsArraysAreDefensivelyCopied() {
        String[] first = layout.pgAttributeKinds();
        String[] second = layout.pgAttributeKinds();
        assertNotSame(first, second);
        first[5] = "corrupted";
        assertEquals("int4", layout.pgAttributeKinds()[5]);

        String[] c1 = layout.pgClassKinds();
        String[] c2 = layout.pgClassKinds();
        assertNotSame(c1, c2);
        c1[12] = "corrupted";
        assertEquals("oid", layout.pgClassKinds()[12]);

        // 词典内容完整自检（防御副本改写后仍逐槽一致）
        assertArrayEquals(WalLayoutV17.INSTANCE.pgClassKinds(), layout.pgClassKinds());
    }
}
