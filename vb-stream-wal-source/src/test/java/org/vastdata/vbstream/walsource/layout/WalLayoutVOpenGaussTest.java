package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WalLayoutVOpenGauss 布局常量的失败先行测试。
 *
 * <p>断言面来自任务书 Step 2 + 2026-10-12 og-plan 容器（opengauss:5.0.0）字节级实测：
 * WAL 面锚（magic 0xD074 / 32B 记录头链式走读 prev 链全通）与 catalog 面锚
 * （pg_class 元组数据区 relname@0、relfilenode@84、reltoastrelid@116、relkind@143，
 * pg_attribute 元组 attrelid@0 起二十四定序——<b>任务书的 108/135 偏移与 41 列计数
 * 与实测冲突，按实测修正，勘误记见模块报告</b>：openGauss 的 relpages/reltuples 是
 * float8（非 vanilla int4/float4，读区 +8B）；catalog 元组 oid 在 9.2 式元组头
 * [t_hoff-4,t_hoff) 而非数据区首列（infomask2 natts=40 实锚），故 pgClassKinds 为
 * 40 项且 relfilenode 数据区偏移 84（V17/V18 的 88 口径含 oid 数据列）。</p>
 */
class WalLayoutVOpenGaussTest {

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    /**
     * 页/记录头常量逐项钉死实测锚：magic 0xD074（xlog_basic.h XLOG_PAGE_MAGIC，
     * 段首字节实测 74 d0）、短页头 24（pageaddr@8/rem_len@16/xlp_total_len@20）、
     * 长页头 40（sysid@24 u64）、记录头 32（实测 prev@16 8 对齐垫 4B、crc@28——
     * 15 条记录 MAXALIGN 链式走读 prev 链逐条自洽）、xid@8/info@24/rmid@25、
     * 块引用定位器伴随区 14B（xlogreader.cpp DecodeXLogRecord：RelFileNodeOld 12B +
     * extra_flag u16——Task 2 语料勘误：Task 1 的 16B 系 bucket 变体宽度，其块 id
     * 扩展位由 fail-fast 拒绝不走本宽度）、blkno 后 last_lsn u64、镜像头仅洞字段、
     * heap 前缀 13/11/2（sizeOfHeapUpdate/Delete/Inplace——语料锚）、扩展位
     * fail-fast、supports 90204、majorVersion 5。
     *
     * <p>任一漂移都会让页遍历/记录走读整体错位——错一位即全流拒读。</p>
     */
    @Test
    void pageAndRecordHeaderConstantsMatchEmpiricalAnchors() {
        assertEquals(0xD074, og.pageMagic(), "xlog_basic.h XLOG_PAGE_MAGIC, 段首字节实测 74 d0");
        assertEquals(24, og.shortPageHeaderSize(), "MAXALIGN(sizeof XLogPageHeaderData)=24, rem_len@16+total_len@20");
        assertEquals(40, og.longPageHeaderSize());
        assertEquals(32, og.recordHeaderSize(), "实测 prev@16 8 对齐垫 4B, crc@28, 链式走读 15 条全通");
        assertEquals(8, og.recordXidOffset());
        assertEquals(24, og.recordInfoOffset());
        assertEquals(25, og.recordRmidOffset());
        assertEquals(14, og.relNodeEncodedSize(),
                "DecodeXLogRecord 消费序 RelFileNodeOld 12B + extra_flag u16（语料 283 条实测）");
        assertEquals(8, og.blockRefLastLsnBytes(), "xloginsert.cpp 逐块 last_lsn u64");
        assertTrue(og.imageHeaderIsHoleOnly(), "XLogRecordBlockImageHeader 仅洞字段（无 bimg_info）");
        assertTrue(og.rejectBlockIdExtensionBits());
        assertTrue(og.supports(90204));
        assertFalse(og.supports(90205), "openGauss 全系恒报 90204，其他值不应到达本档");
        assertEquals(5, og.majorVersion(), "OG 以 PG 大版本口径报 5——由判别器保证仅 OG 5.0.x 到达");
    }

    /**
     * legacy 描述符经接口 default 取到的记录头偏移必须与 WalRecordParser 历内联值
     * 逐项相等（xid@4/info@16/rmid@17/定位器 12B/尾随 0/现代镜像头/扩展位不拒绝）
     * ——这是"V17/V18 路径字节行为零变化"的常量面锚，Task 2 起 parser 内联值
     * 换成 layout 取值，以此防漂移。
     */
    @Test
    void legacyLayoutsKeepParserInlineOffsetsAsDefaults() {
        for (WalLayout legacy : new WalLayout[] {WalLayoutV17.INSTANCE, WalLayoutV18.INSTANCE}) {
            assertEquals(4, legacy.recordXidOffset(), legacy.getClass().getSimpleName());
            assertEquals(16, legacy.recordInfoOffset(), legacy.getClass().getSimpleName());
            assertEquals(17, legacy.recordRmidOffset(), legacy.getClass().getSimpleName());
            assertEquals(12, legacy.relNodeEncodedSize(), legacy.getClass().getSimpleName());
            assertEquals(0, legacy.blockRefLastLsnBytes(), legacy.getClass().getSimpleName());
            assertFalse(legacy.imageHeaderIsHoleOnly(), legacy.getClass().getSimpleName());
            assertFalse(legacy.rejectBlockIdExtensionBits(), legacy.getClass().getSimpleName());
        }
    }

    /**
     * 自检锚（沿"两版 toast 偏移差恰 4B"惯例）：OG 读区相对 V17 的漂移由两个独立差异合成——
     * ①oid 数据列缺席（9.2 式元组头携带 oid，读区整体前移 4B）；②relpages/reltuples
     * 加宽 float8（int4/float4→float8/float8，读区后移 8B）；③reltoastrelid 后多五个
     * oid 列（列存四件 + reltoastidxid，仅影响 relkind 不影响 reltoastrelid）。
     * 净漂移：relfilenode 84（−4）、reltoastrelid 116（+8）、relkind 143（+28 = 8+20）。
     */
    @Test
    void classReadRegionDriftFromV17IsFloat8WideningPlusFiveExtraOidColumns() {
        assertEquals(88, WalLayoutV17.INSTANCE.pgClassRelfilenodeDataOffset());
        assertEquals(108, WalLayoutV17.INSTANCE.pgClassReltoastrelidDataOffset());
        assertEquals(115, WalLayoutV17.INSTANCE.pgClassRelkindDataOffset());
        // 实测锚：t_offprobe 元组字节 relfilenode=16389@84、reltoastrelid=16392@116、relkind='r'@143
        assertEquals(84, og.pgClassRelfilenodeDataOffset(),
                "OG oid 在元组头非数据区（infomask2 natts=40 实锚），数据区首列即 relname@0");
        assertEquals(116, og.pgClassReltoastrelidDataOffset(),
                "relpages/reltuples float8 加宽 8B（任务书 108 按 int4/float4 推导，与实测冲突）");
        assertEquals(143, og.pgClassRelkindDataOffset());
        assertEquals(8, og.pgClassReltoastrelidDataOffset()
                - WalLayoutV17.INSTANCE.pgClassReltoastrelidDataOffset(),
                "toast 读区差 = float8 加宽净 8B（oid 前移 4B 不越过 relfilenode 起点）");
        assertEquals(28, og.pgClassRelkindDataOffset() - WalLayoutV17.INSTANCE.pgClassRelkindDataOffset(),
                "relkind 读区差 = 五个 4B oid 列（列存四件 + reltoastidxid）+ float8 加宽 8B");
    }

    /**
     * kinds 数组长度与锚槽位钉死：pg_class 40 项（attnum 1-40 + 无 oid 数据列——
     * infomask2 低 11 位实测 40）、pg_attribute 24 项（relhasoids=f，元组亦无 oid）；
     * relfilenode 槽 6、reltoastrelid 槽 11（attnum 12）、relkind 槽 20（attnum 21）、
     * attisdropped 槽 14（attnum 15）——槽位比 V17/V18 整体前移一位（oid 列缺席所致）。
     */
    @Test
    void kindsArraysHoldEmpiricalCountsAndAnchorSlots() {
        String[] attr = og.pgAttributeKinds();
        String[] cls = og.pgClassKinds();
        assertEquals(40, cls.length);
        assertEquals(24, attr.length);
        assertEquals("name", cls[0], "数据区首列 relname（NameData 定长锚）");
        assertEquals("oid", cls[6], "relfilenode（V17/V18 槽 7，OG 前移一位）");
        assertEquals("oid", cls[11], "reltoastrelid（attnum 12）");
        assertEquals("char", cls[20], "relkind（attnum 21）");
        assertEquals("float8", cls[8], "relpages float8（OG 加宽，非 vanilla int4）");
        assertEquals("float8", cls[9], "reltuples float8（OG 加宽，非 vanilla float4）");
        assertEquals("bool", attr[14], "attisdropped（attnum 15）");
        assertEquals("name", attr[1], "attname（NameData 定长锚）");
        assertEquals("int1", attr[16], "attcmprmode（OG 独有 int1，解码分支随 catalog 任务补）");
    }

    /** kinds 数组每次返回防御副本：外部改写不得污染描述符的静态词典。 */
    @Test
    void kindsArraysAreDefensivelyCopied() {
        String[] first = og.pgClassKinds();
        String[] second = og.pgClassKinds();
        assertNotSame(first, second);
        first[6] = "corrupted";
        assertEquals("oid", og.pgClassKinds()[6]);
        String[] attrA = og.pgAttributeKinds();
        String[] attrB = og.pgAttributeKinds();
        assertNotSame(attrA, attrB);
        attrA[14] = "corrupted";
        assertEquals("bool", og.pgAttributeKinds()[14]);
    }
}
