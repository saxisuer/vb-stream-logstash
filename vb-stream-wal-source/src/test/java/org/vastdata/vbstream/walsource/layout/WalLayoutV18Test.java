package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WalLayoutV18 布局常量与版本分发的失败先行测试。
 *
 * <p>断言面来自任务书 Step 1：pageMagic 0xD118、supports 只认 18 大版本、
 * forServerVersion 对未知大版本 fail-fast 抛 ISE、Lsn format/parse 互逆、
 * 两 kinds 数组长度 25/34 且 pgClass 偏移 7/13 处为 "oid"（列序自检锚）。
 * 其余布局常量（页头/记录头/heap 结构尺寸/pg_class 数据区偏移）逐项钉死，
 * 防止后续解析任务照抄时被无声漂移。</p>
 */
class WalLayoutV18Test {

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 页魔数必须是 REL_18_STABLE 的 XLOG_PAGE_MAGIC——错一位即全流拒读。 */
    @Test
    void pageMagicMatchesRel18Stable() {
        assertEquals(0xD118, layout.pageMagic());
    }

    /** 页/记录头与 heap 结构尺寸照抄 spike 实测锚：任一漂移都会让 walker 错位。 */
    @Test
    void layoutConstantsMatchSpikeTranscription() {
        assertEquals(8192, layout.walBlockSize());
        assertEquals(24, layout.shortPageHeaderSize());
        assertEquals(40, layout.longPageHeaderSize());
        assertEquals(24, layout.recordHeaderSize());
        assertEquals(14, layout.sizeOfHeapUpdate());
        assertEquals(8, layout.sizeOfHeapDelete());
        // MinSizeOfHeapInplace = offsetof(msgs)：offnum+pad+dbId+tsId+bool+pad+nmsgs 固定前缀
        assertEquals(20, layout.sizeOfHeapInplace());
        assertEquals(88, layout.pgClassRelfilenodeDataOffset());
        assertEquals(112, layout.pgClassReltoastrelidDataOffset());
    }

    /** supports 按大版本区间判定：18 全区间真、相邻 17/19 假。 */
    @Test
    void supportsAcceptsOnlyItsMajorVersion() {
        assertTrue(layout.supports(180000));
        assertTrue(layout.supports(189999));
        assertFalse(layout.supports(170000));
        assertFalse(layout.supports(179999));
        assertFalse(layout.supports(190000));
    }

    /** majorVersion 描述符自述 18，供运行期交叉校验连接端版本。 */
    @Test
    void majorVersionIs18() {
        assertEquals(18, layout.majorVersion());
    }

    /** forServerVersion 把 180000-189999 全区间分发到 V18 描述符。 */
    @Test
    void forServerVersionDispatchesV18AcrossFullRange() {
        assertEquals(18, WalLayouts.forServerVersion(180000).majorVersion());
        assertEquals(18, WalLayouts.forServerVersion(189999).majorVersion());
    }

    /** 未知大版本（190000 及以上、160000 及以下）启动期 ISE fail-fast，不得静默回退——
     *  170000 自 Task 16 起分发 V17，不再属拒绝面（17 断言面归 WalLayoutV17Test）。 */
    @Test
    void forServerVersionRejectsUnknownOrUnregisteredMajors() {
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(190000));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(160000));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(0));
    }

    /** Lsn.format 与 Lsn.parse 互逆：低/高段、零值往返后逐字符一致。 */
    @Test
    void lsnFormatParseRoundTrips() {
        assertEquals("0/46F00000", Lsn.format(Lsn.parse("0/46F00000")));
        assertEquals("16/B374D848", Lsn.format(Lsn.parse("16/B374D848")));
        // 低段恒补零至 8 位（与 PG pg_lsn 的 LSN_FORMAT_ARGS %X/%08X 同形），零不特判
        assertEquals("0/00000000", Lsn.format(Lsn.parse("0/0")));
        assertEquals(0x46F00000L, Lsn.parse("0/46F00000"));
        assertEquals("0/46F00000", Lsn.format(0x46F00000L));
    }

    /** 两 kinds 数组长度钉死 25/34，且 pg_class 偏移 7/13 是 "oid"——列序自检锚。 */
    @Test
    void kindsArraysHoldTranscribedLengthsAndOffsets() {
        String[] attr = layout.pgAttributeKinds();
        String[] cls = layout.pgClassKinds();
        assertEquals(25, attr.length);
        assertEquals(34, cls.length);
        assertEquals("oid", cls[7]);   // relfilenode
        assertEquals("oid", cls[13]);  // reltoastrelid
        assertEquals("name", attr[1]); // attname（NameData 定长锚）
        assertEquals("skip", attr[24]); // 末列 attmissingval 为 varlena 跳过列
    }

    /** kinds 数组每次返回防御副本：外部改写不得污染描述符的静态词典。 */
    @Test
    void kindsArraysAreDefensivelyCopied() {
        String[] first = layout.pgClassKinds();
        String[] second = layout.pgClassKinds();
        assertNotSame(first, second);
        first[7] = "corrupted";
        assertEquals("oid", layout.pgClassKinds()[7]);
    }
}
