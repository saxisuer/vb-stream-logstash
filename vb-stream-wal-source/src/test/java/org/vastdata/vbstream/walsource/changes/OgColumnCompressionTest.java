package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * openGauss attcmprmode 列压缩解压（{@link OgColumnCompression}）的失败先行测试。
 *
 * <p><b>语料锚说明（重要）</b>：openGauss 5.0.0 对行存表的压缩 DDL 三路全拒
 * （{@code CREATE TABLE ... COMPRESS} / {@code ALTER ... SET (compression=yes)} /
 * {@code WITH (compresslevel)}，"row-oriented table does not support compression"——
 * tablecmds.cpp 双护栏 + og-plan 容器 m2pg[PG 档] 与 postgres[A 档] 双库实测），
 * 故真实服务端压缩字节<b>天然不可采</b>；本测试黄金向量按压缩侧源码语义
 * （pagecompress.cpp NumstrCompress/DeltaCompress/PrefixCompress + SetCmprHeaderData
 * 逐行推导）手造——"PG 档 DDL 不可达、A 档留 M3"记档于 task-4-report。</p>
 *
 * <p>断言面六组：①DELTA（含 bytes=0 零消耗形态 / int2 回绕 / 未知 typeOid ISE /
 * dropped 列 null 语义）；②DICT（定宽项 / 单项零消耗 / varlena 项 / id 越界 ISE）；
 * ③PREFIX（varlena 后缀重建 4B 头 / cstring 形态 / len=0 ISE）；④NUMSTR（奇偶长
 * 双形态 varlena + cstring / 空载荷 ISE / 偏置零字节损坏 ISE）；⑤多列压缩头走读
 * （SetCmprHeaderData 布局：逐列 meta 串联，NONE 列单字节）；⑥meta 形态不符 ISE
 * （未注册 mode / DELTA 落在变宽列 / bytes 越界）。</p>
 */
class OgColumnCompressionTest {

    /** 便捷：ASCII 文本转字节（测试字面量可读性）。 */
    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    /** 便捷：拼字节（meta/值区构造）。 */
    private static byte[] bytes(int... ints) {
        byte[] out = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            out[i] = (byte) ints[i];
        }
        return out;
    }

    /**
     * 用例 1a：DELTA 基本形态——meta {@code [01][bytes=2][min=100 i32LE]} + 值区
     * 大端 2B 差值 100，解出 datum = min+diff = 200 的 4B 小端原值字节，值区恰耗 2B。
     */
    @Test
    void deltaDecodesBigEndianDiffPlusMin() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DELTA, 2, 100, 0, 0, 0), 4);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 23, 4, bytes(0x00, 0x64), 0);

        assertArrayEquals(bytes(200, 0, 0, 0), out.datum());
        assertEquals(2, out.compressedSize());
    }

    /**
     * 用例 1b：DELTA bytes=0 形态——全列同值时编码器 bytes=0，值区零消耗、
     * datum 恒为 meta 里的 MinVal（UncompressOneAttr DELTA 分支 L1412-1415）。
     */
    @Test
    void deltaZeroBytesConsumesNothingAndReturnsMin() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DELTA, 0, 7, 0, 0, 0), 4);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 23, 4, new byte[0], 0);

        assertArrayEquals(bytes(7, 0, 0, 0), out.datum());
        assertEquals(0, out.compressedSize());
    }

    /**
     * 用例 1c：DELTA int2 回绕——int16Plus 按 16 位两补回绕（plus 函数族注释
     * "we don't care overflow"），min=32767 + diff=2 → -32767（0x8001）。
     */
    @Test
    void deltaInt2WrapsAtSixteenBits() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DELTA, 1, 0xFF, 0x7F), 2);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 21, 2, bytes(2), 0);

        assertArrayEquals(bytes(0x01, 0x80), out.datum());
    }

    /**
     * 用例 1d：DELTA 未知 typeOid（g_number_types 表外）——格式错位硬失败 ISE
     * （C 侧 binarySearch 返回 NULL 后解引用即崩，Java 面显式 ISE 附 oid）。
     */
    @Test
    void deltaUnknownTypeOidFailsFast() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DELTA, 1, 1, 0, 0, 0), 4);

        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.uncompressAttr(meta, 25, 4, bytes(1), 0));
    }

    /**
     * 用例 1e：DELTA dropped 列（typeOid=0=InvalidOid）——C 语义"值不可计算，
     * 跳过继续走列"（UncompressOneAttr L1424-1429 注释）：datum 返 null、
     * compressedSize 仍按 bytes 推进（走列不受阻）。
     */
    @Test
    void deltaDroppedColumnYieldsNullButAdvances() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DELTA, 2, 0, 0, 0, 0), 4);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 0, 4, bytes(1, 2), 0);

        assertNull(out.datum());
        assertEquals(2, out.compressedSize());
    }

    /**
     * 用例 2a：DICT 定宽项——meta {@code [02][itemNum=2][item0=1][item1=99]}（各
     * 4B）+ 值区单字节 id=1，解出 item1 原值字节，值区恰耗 1B（DICT_ITEMID_SIZE）。
     * 值区嵌在 off=1（前面垫一字节）——同钉"值区偏移由调用方走列推进"的接口面。
     */
    @Test
    void dictFixedWidthItemById() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DICT, 2, 1, 0, 0, 0, 99, 0, 0, 0), 4);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 23, 4, bytes(0, 1), 1);

        assertArrayEquals(bytes(99, 0, 0, 0), out.datum());
        assertEquals(1, out.compressedSize());
    }

    /**
     * 用例 2b：DICT 单项形态——itemNum==1 时全列同值，值区零消耗（GetAttrCmprValSize
     * {@code (dictItemNum==1) ? 0 : DICT_ITEMID_SIZE}），datum 恒为 item0。
     */
    @Test
    void dictSingleItemConsumesNothing() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DICT, 1, 42, 0, 0, 0), 4);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 23, 4, new byte[0], 0);

        assertArrayEquals(bytes(42, 0, 0, 0), out.datum());
        assertEquals(0, out.compressedSize());
    }

    /**
     * 用例 2c：DICT 变宽项（attlen=-1）——字典项是完整 varlena（含头，itemSize =
     * VARSIZE_ANY）：item0 用 1B 短头、item1 用 4B 头，解出 item1 的完整 varlena
     * 字节（含头原样）。
     */
    @Test
    void dictVarlenaItemsKeepTheirHeaders() {
        // item0 "aa": 1B 头 0x07（总长 3：len<<1|1，tag 位置 1）+ "aa"；item1 "bbb": 4B 头（总长 7 >>> 2）+ "bbb"
        byte[] metaBytes = bytes(OgColumnCompression.CMPR_DICT, 2, 0x07, 'a', 'a', 0x1C, 0, 0, 0, 'b', 'b', 'b');
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(metaBytes, -1);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 25, -1, bytes(1), 0);

        assertArrayEquals(bytes(0x1C, 0, 0, 0, 'b', 'b', 'b'), out.datum());
        assertEquals(1, out.compressedSize());
    }

    /**
     * 用例 2d：DICT id 越界——值区 id ≥ itemNum 即格式错位（C 侧 dictItems+id 越界
     * 读，Java 面显式 ISE 附 id/itemNum）。
     */
    @Test
    void dictIdOutOfBoundsFailsFast() {
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(bytes(
                OgColumnCompression.CMPR_DICT, 2, 1, 0, 0, 0, 99, 0, 0, 0), 4);

        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.uncompressAttr(meta, 23, 4, bytes(5), 0));
    }

    /**
     * 用例 3a：PREFIX varlena 形态——meta 前缀 "abc"，值区是短头 varlena 后缀 "de"
     * （1B 头，压缩侧 SET_VARSIZE_SHORT），重建为 4B 头 varlena "abcde"（解压侧恒
     * SET_VARSIZE 4B 头——PrefixUncompress L800-810），值区消耗 = VARSIZE_ANY=3。
     */
    @Test
    void prefixRebuildsFourByteHeaderVarlena() {
        byte[] metaBytes = bytes(OgColumnCompression.CMPR_PREFIX, 3, 'a', 'b', 'c');
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(metaBytes, -1);

        byte[] value = bytes(0x07, 'd', 'e');   // 1B 头总长 3（len<<1|1）
        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 25, -1, value, 0);

        assertArrayEquals(bytes(0x24, 0, 0, 0, 'a', 'b', 'c', 'd', 'e'), out.datum());
        assertEquals(3, out.compressedSize());
    }

    /**
     * 用例 3b：PREFIX cstring 形态（attlen=-2）——值区后缀 "cd\0"，重建
     * {@code 前缀+后缀+'\0'}（datum_size = prefix.len + strlen + 1）。
     */
    @Test
    void prefixCstringFormAppendsSuffixAndNul() {
        byte[] metaBytes = bytes(OgColumnCompression.CMPR_PREFIX, 2, 'a', 'b');
        OgColumnCompression.CmprAttrMeta meta = OgColumnCompression.parseMeta(metaBytes, -2);

        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(meta, 1042, -2, bytes('c', 'd', 0), 0);

        assertArrayEquals(ascii("abcd\0"), out.datum());
        assertEquals(3, out.compressedSize());
    }

    /**
     * 用例 3c：PREFIX len=0——压缩侧 Assert(prefix->len>0)（空前缀不构成压缩），
     * Java 面 meta 解析即 ISE。
     */
    @Test
    void prefixZeroLengthFailsFastAtMetaParse() {
        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.parseMeta(bytes(OgColumnCompression.CMPR_PREFIX, 0), -1));
    }

    /**
     * 用例 4a：NUMSTR 奇数长 varlena——"123"：偶段 "12" 编为 {@code (1)|(2<<4)+1=
     * 0x22}（低 nibble 在前 + 偏置 1），尾位 3 编为 {@code 3|0xF0=0xF3}（高 nibble
     * 0xF 标记奇尾）；1B 头短 varlena 载荷 [22 F3]，解出 4B 头 varlena "123"。
     */
    @Test
    void numstrOddLengthVarlenaDecodes() {
        byte[] value = bytes(0x07, 0x22, 0xF3);   // 1B 头总长 3（len<<1|1）
        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(
                OgColumnCompression.numstrMeta(), 25, -1, value, 0);

        assertArrayEquals(bytes(0x1C, 0, 0, 0, '1', '2', '3'), out.datum());
        assertEquals(3, out.compressedSize());
    }

    /**
     * 用例 4b：NUMSTR 偶数长 varlena——"42"：{@code (4)|(2<<4)+1=0x25} 单字节载荷，
     * 解出 "42"（尾字节高 nibble 非 0xF → 双字符展开）。
     */
    @Test
    void numstrEvenLengthVarlenaDecodes() {
        byte[] value = bytes(0x05, 0x25);   // 1B 头总长 2（len<<1|1）
        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(
                OgColumnCompression.numstrMeta(), 25, -1, value, 0);

        assertArrayEquals(bytes(0x18, 0, 0, 0, '4', '2'), out.datum());   // 4B 头：总长 6<<2=0x18
        assertEquals(2, out.compressedSize());
    }

    /**
     * 用例 4c：NUMSTR cstring 形态（attlen=-2）——"907"：偶段 "90" 编 0x0A、尾位
     * 7 编 0xF7，cstring 尾部 '\0'（偏置 1 保证编码字节恒非零，strlen 可走）；
     * 解出 {@code "907\0"}。
     */
    @Test
    void numstrCstringFormDecodes() {
        byte[] value = bytes(0x0A, 0xF7, 0);
        OgColumnCompression.AttrVal out = OgColumnCompression.uncompressAttr(
                OgColumnCompression.numstrMeta(), 18, -2, value, 0);

        assertArrayEquals(ascii("907\0"), out.datum());
        assertEquals(3, out.compressedSize());
    }

    /**
     * 用例 4d：NUMSTR 空载荷——压缩侧对空串 outSize=0（不压，存原值），压缩形态
     * 里 srcLen==0 即格式错位 ISE（C 侧 Assert(srcLen > 0)）。
     */
    @Test
    void numstrEmptyPayloadFailsFast() {
        byte[] value = bytes(0x03);   // 1B 头总长 1（len<<1|1）：载荷空
        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.uncompressAttr(OgColumnCompression.numstrMeta(), 25, -1, value, 0));
    }

    /**
     * 用例 4e：NUMSTR 偏置零字节——合法编码器恒产 ≥1 的字节（"+1" 偏置），值区
     * 出现 0x00 配对字节即损坏，ISE 硬失败（Java 面在 C 语义之上的显式加固——
     * C 侧静默解出 '?' 乱字符，与"格式错位硬失败"契约不符故收紧）。
     */
    @Test
    void numstrZeroCodeByteFailsFast() {
        byte[] value = bytes(0x07, 0x00, 0xF3);   // 1B 头总长 3（len<<1|1）
        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.uncompressAttr(OgColumnCompression.numstrMeta(), 25, -1, value, 0));
    }

    /**
     * 用例 5：多列压缩头走读——SetCmprHeaderData 布局 = 逐列 meta 串联
     * （[DELTA 6B][PREFIX 5B][NONE 1B] 共 12B），NONE 列 mode=CMPR_NONE、
     * metaSize=1；整头解析逐列落位正确。
     */
    @Test
    void headerWalkLaysOutPerColumnMetas() {
        byte[] header = bytes(
                OgColumnCompression.CMPR_DELTA, 2, 100, 0, 0, 0,
                OgColumnCompression.CMPR_PREFIX, 3, 'a', 'b', 'c',
                OgColumnCompression.CMPR_NONE);
        int[] attlens = {4, -1, 4};

        OgColumnCompression.CmprAttrMeta[] metas = OgColumnCompression.parseCmprHeader(header, attlens);

        assertEquals(3, metas.length);
        assertEquals(OgColumnCompression.CMPR_DELTA, metas[0].mode());
        assertEquals(6, metas[0].metaSize());
        assertEquals(OgColumnCompression.CMPR_PREFIX, metas[1].mode());
        assertEquals(3, metas[1].prefixLen());
        assertArrayEquals(ascii("abc"), metas[1].prefix());
        assertEquals(OgColumnCompression.CMPR_NONE, metas[2].mode());
        assertEquals(1, metas[2].metaSize());
        assertEquals(12, OgColumnCompression.cmprHeaderSize(header, attlens));
    }

    /**
     * 用例 6a：未注册 mode 字节（如 0x7F=CMPR_UNDEF——catalog 里 toast/index 列的
     * "未定义"占位值）出现在压缩头——ISE 附 mode 值。
     */
    @Test
    void unknownModeByteFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.parseMeta(bytes(OgColumnCompression.CMPR_UNDEF), 4));
    }

    /**
     * 用例 6b：DELTA 落在变宽列——压缩侧只对定宽数字列选 DELTA（ChooseMethod
     * ForFixedLenType），attlen&lt;=0 即格式错位 ISE。
     */
    @Test
    void deltaOnVarlenaColumnFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.parseMeta(bytes(OgColumnCompression.CMPR_DELTA, 1, 1), -1));
    }

    /**
     * 用例 6c：DELTA bytes 越界——压缩侧 Assert(bytes &lt; 8)（g_max_value_in_bytes
     * 尺寸表），bytes&gt;7 即格式错位 ISE。
     */
    @Test
    void deltaBytesOutOfRangeFailsFast() {
        assertThrows(IllegalStateException.class,
                () -> OgColumnCompression.parseMeta(bytes(OgColumnCompression.CMPR_DELTA, 8, 0, 0, 0, 0), 4));
    }
}
