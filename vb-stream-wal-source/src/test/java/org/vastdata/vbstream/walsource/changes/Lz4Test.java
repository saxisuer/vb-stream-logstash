package org.vastdata.vbstream.walsource.changes;

import net.jpountz.lz4.LZ4Factory;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * lz4 解压（{@link Lz4#decompress}）的失败先行测试——PG 侧 TOAST lz4 值是 liblz4
 * <b>裸 block 格式</b>（tuptoaster.c {@code toast_decompress_datum} →
 * {@code LZ4_decompress_safe}），与 lz4-java 的 safeDecompressor 同格式。
 *
 * <p>断言面四组：①手造最小 block 黄金字节（单 token 纯字面量）——与库压
 * 缩器互为双源钉格式；②lz4-java 压缩器往返（可压缩 repeat 型 + 固定种子
 * 伪随机不可压缩型——后者钉"无 match 段"的纯字面量长块）；③截断输入抛
 * ISE；④解出长度 != expectedRawSize 抛 ISE。另补 null/负参 IAE 防御面
 * （与 {@link Pglz} 契约同形）。</p>
 */
class Lz4Test {

    /**
     * 用例 1：手造黄金字节——lz4 block 单 token {@code 0x30}（<b>高 nibble</b>
     * 字面量长度 3、<b>低 nibble</b> match 数 0，无扩展长度字节、无 match 段）
     * 后随 3 字面量 "abc"，是合法完整 block；解出恰 "abc"。与用例 2 的库压缩器
     * 互为双源：本用例不依赖库的压缩行为，钉死 {@link Lz4} 与裸 block 格式的
     * 对齐（首版曾把两 nibble 写反成 0x03——低 nibble 3 解作 match 长度 7、
     * 且字面量段为空，库即报损坏）。
     */
    @Test
    void handCraftedLiteralBlockDecodesToOriginal() {
        byte[] block = {0x30, 'a', 'b', 'c'};

        byte[] out = Lz4.decompress(block, 3);

        assertArrayEquals("abc".getBytes(StandardCharsets.US_ASCII), out);
    }

    /**
     * 用例 2a：库压缩器往返——可压缩载荷（周期 16 的重复图案 65536 字节，
     * 形态对齐 TOAST 场景的 repeat 型宽值）：lz4-java fastCompressor 压缩后
     * {@link Lz4#decompress} 解回原文，expectedRawSize = 载荷长。
     */
    @Test
    void roundTripsCompressiblePayload() {
        byte[] raw = new byte[65536];
        byte[] pattern = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < raw.length; i++) {
            raw[i] = pattern[i % pattern.length];
        }
        byte[] compressed = LZ4Factory.fastestInstance().fastCompressor()
                .compress(raw, 0, raw.length);

        byte[] out = Lz4.decompress(compressed, raw.length);

        assertArrayEquals(raw, out);
    }

    /**
     * 用例 2b：不可压缩载荷往返——固定种子（42）伪随机 4096 字节：压缩后几乎
     * 必为"超长字面量"形态（token 扩展链 + 无 match 段），覆盖与 2a 不同的
     * block 分支；种子固定保证跨机器可复现。
     */
    @Test
    void roundTripsIncompressiblePayload() {
        byte[] raw = new byte[4096];
        new Random(42).nextBytes(raw);
        byte[] compressed = LZ4Factory.fastestInstance().fastCompressor()
                .compress(raw, 0, raw.length);

        byte[] out = Lz4.decompress(compressed, raw.length);

        assertArrayEquals(raw, out);
    }

    /**
     * 用例 3：截断输入抛 ISE——用例 1 的合法 block 砍掉末位字面量（block 不完
     * 整，safeDecompressor 判损坏），{@link Lz4} 须把库侧 LZ4Exception 包成
     * ISE（与 {@link Pglz} 的 fail-fast 契约同形，不返回部分结果）。
     */
    @Test
    void truncatedBlockThrowsIllegalState() {
        byte[] truncated = Arrays.copyOf(new byte[] {0x30, 'a', 'b', 'c'}, 3);

        assertThrows(IllegalStateException.class, () -> Lz4.decompress(truncated, 3));
    }

    /**
     * 用例 4：解出长度 != expectedRawSize 抛 ISE——用例 1 的 block 实际解出
     * 3 字节，传 4 迫使长度校验失败：库返回的解出字节数与期望不符时抛 ISE
     * （对应 {@link Pglz} 的 {@code dp != destend} 完整性检查）。
     */
    @Test
    void decodedLengthMismatchThrowsIllegalState() {
        byte[] block = {0x30, 'a', 'b', 'c'};

        assertThrows(IllegalStateException.class, () -> Lz4.decompress(block, 4));
    }

    /**
     * 补充用例：防御面——src 为 null / expectedRawSize 为负抛 IAE
     * （与 {@link Pglz#decompress} 的入参契约逐字同形）。
     */
    @Test
    void nullOrNegativeArgsThrowIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> Lz4.decompress(null, 3));
        assertThrows(IllegalArgumentException.class,
                () -> Lz4.decompress(new byte[] {0x30, 'a'}, -1));
    }
}
