package org.vastdata.vbstream.walsource.changes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * pglz 解压（{@link Pglz#decompress}）的失败先行测试——手造压缩字节（依据
 * REL_18_STABLE {@code src/common/pg_lzcompress.c} 的 {@code pglz_decompress}
 * 解码侧逐位推导，与被测实现互为双源转录）。
 *
 * <p>断言面来自任务书 Step 1 三用例：①纯字面量流（control byte 0x00 + 8 字面量
 * + 续组）解出原文；②含 match 往返（control 位 1 + 2 字节匹配码：低 12 位
 * offset、低 nibble length-3——具体编码以源码行为准，见各用例的手造推导）；
 * ③截断输入抛 ISE。另补：扩展长度 match（length nibble 0x0F + 第三字节）与
 * 结果长度不匹配 expectedRawSize 的 ISE。</p>
 */
class PglzTest {

    /**
     * 用例 1（任务书 ①）：纯字面量流——10 字节原文分两组 control byte 编码：
     * 第一组 ctrl=0x00（8 个条目全为字面量位 0，LSB 先行）+ 字面量 'A'..'H'；
     * 第二组 ctrl=0x00 + 字面量 'I','J'（组内 sp 耗尽即止，后 6 个条目位闲置）。
     * 期望解出原文 "ABCDEFGHIJ"。
     */
    @Test
    void pureLiteralStreamDecodesToOriginal() {
        byte[] src = new byte[12];
        src[0] = 0x00;                                     // ctrl：8 条目全字面量
        System.arraycopy("ABCDEFGH".getBytes(StandardCharsets.US_ASCII), 0, src, 1, 8);
        src[9] = 0x00;                                     // 第二组 ctrl
        System.arraycopy("IJ".getBytes(StandardCharsets.US_ASCII), 0, src, 10, 2);

        byte[] out = Pglz.decompress(src, 10);

        assertArrayEquals("ABCDEFGHIJ".getBytes(StandardCharsets.US_ASCII), out);
    }

    /**
     * 用例 2（任务书 ②）：含 match 往返——ctrl=0x08（bit3=1：第 4 个条目是
     * match，前 3 条目字面量位 0）+ 字面量 'a','b','c' + 匹配码 2 字节：
     * b0=0x05（低 nibble 5 → len=5+3=8；高 nibble 0 → offset 高 4 位 0）、
     * b1=0x03（offset 低 8 位 → off=3）。off&lt;len 构成重叠拷贝（源码
     * off 倍增循环路径）：raw = "abc" + 从 dp-3 逐字节拷 8 个 = "abcabcabcab"
     * （3+8=11 字节，期望值按重复周期 3 独立手推）。
     */
    @Test
    void matchTagRoundTripsOverlappingCopy() {
        byte[] src = new byte[6];
        src[0] = 0x08;                                     // ctrl：条目 3 为 match
        src[1] = 'a';
        src[2] = 'b';
        src[3] = 'c';
        src[4] = 0x05;                                     // len nibble 5 → len 8；off 高 4 位 0
        src[5] = 0x03;                                     // off 低 8 位 → off 3

        byte[] out = Pglz.decompress(src, 11);

        assertArrayEquals("abcabcabcab".getBytes(StandardCharsets.US_ASCII), out);
    }

    /**
     * 用例 3（任务书 ③）：截断输入抛 ISE——合法流（同用例 2 布局）在匹配码第
     * 2 字节处截断，match tag 仅剩 1 字节，源码防护 {@code sp + 2 > srcend}
     * 判定损坏；Java 侧对应 ISE（fail-fast，不返回部分结果）。
     */
    @Test
    void truncatedMatchTagThrowsIllegalState() {
        byte[] src = {0x08, 'a', 'b', 'c', 0x05};          // 匹配码缺 b1

        assertThrows(IllegalStateException.class, () -> Pglz.decompress(src, 11));
    }

    /**
     * 补充用例：扩展长度 match——len nibble 0x0F 使 len=18，后随第三字节 10 使
     * len=28（源码 {@code if (len == 18) len += *sp++}）；b0=0x0F、b1=0x03、
     * 扩展字节 10，off=3 重叠拷贝 28 字节 = "abc"×9 + "a"，raw 共 3+28=31 字节。
     */
    @Test
    void extendedLengthMatchDecodesFullRepeat() {
        byte[] src = new byte[7];
        src[0] = 0x08;                                     // 条目 3 为 match
        src[1] = 'a';
        src[2] = 'b';
        src[3] = 'c';
        src[4] = 0x0F;                                     // len nibble 0x0F → len 18 待扩展
        src[5] = 0x03;                                     // off 3
        src[6] = 10;                                       // 扩展长度 → len 28

        byte[] out = Pglz.decompress(src, 31);

        String expected = "abc".repeat(10) + "a";          // 3 字面量 + 28 字节周期 3 拷贝（9 周期余 1）
        assertArrayEquals(expected.getBytes(StandardCharsets.US_ASCII), out);
    }

    /**
     * 补充用例：解出长度 != expectedRawSize 抛 ISE——8 个字面量解出 8 字节，
     * 传 expectedRawSize=7 迫使 {@code dp != destend}（对应源码
     * {@code check_complete} 完整性检查），接口契约要求 ISE 而非截断返回。
     */
    @Test
    void decodedLengthMismatchThrowsIllegalState() {
        byte[] src = new byte[9];
        src[0] = 0x00;
        System.arraycopy("ABCDEFGH".getBytes(StandardCharsets.US_ASCII), 0, src, 1, 8);

        assertThrows(IllegalStateException.class, () -> Pglz.decompress(src, 7));
    }
}
