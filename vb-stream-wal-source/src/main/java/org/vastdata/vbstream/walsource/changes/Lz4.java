package org.vastdata.vbstream.walsource.changes;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

/**
 * lz4 解压——PostgreSQL TOAST lz4 压缩值（tcinfo/extinfo 方法位
 * {@code TOAST_LZ4_COMPRESSION_ID=1}）的解压面，形态镜像 {@link Pglz}：
 * 纯函数入参 {@code (src, expectedRawSize)}、结果长度恰为期望、损坏抛
 * {@link IllegalStateException} fail-fast。
 *
 * <p><b>格式依据</b>：PG 侧 tuptoaster.c {@code toast_compress_datum} 对 lz4 列
 * 走 {@code pg_lz4compress} → liblz4 的 {@code LZ4_compress_default}，解压走
 * {@code toast_decompress_datum} → {@code LZ4_decompress_safe}——即 liblz4
 * <b>裸 block 格式</b>（token 序列：高 nibble 字面量长度/低 nibble match 数，
 * 无帧头、无内容校验）。lz4-java 的 {@link LZ4SafeDecompressor} 实现同一
 * 格式（JNI 优先、纯 Java 安全回落），原长已知（tcinfo 低 30 位）恰合
 * safe 形态的"定长目标缓冲"用法。</p>
 *
 * <p>线程约束：{@link LZ4Factory#fastestInstance()} 与其解压器实例线程安全
 * （lz4-java 契约），工厂单例静态持有；解压调用本身无共享可变状态。</p>
 */
final class Lz4 {

    /** 工厂与安全解压器单例——fastestInstance 按 JNI 可用性择优（不可用回落纯 Java）。 */
    private static final LZ4SafeDecompressor DECOMPRESSOR =
            LZ4Factory.fastestInstance().safeDecompressor();

    /** 工具类不可实例化。 */
    private Lz4() {
    }

    /**
     * 解压一段 lz4 裸 block 数据。
     *
     * <p>职责：把 liblz4 裸 block 解至 {@code expectedRawSize} 字节——目标
     * 缓冲按期望长度精确分配（TOAST 的 tcinfo 低 30 位承载原长，调用方
     * {@link ToastAssembler} 已解出传入），库解压 + 长度复核。</p>
     *
     * <p>关键步骤：防御式入参检查（null/负长度 IAE，与 {@link Pglz#decompress}
     * 逐字同形）→ safeDecompressor 解入定长目标 → 返回前复核实际解出长度
     * （库在目标未填满时可能正常返回，长度短于期望即流与声明原长不符）。</p>
     *
     * <p>边界与异常语义：数据损坏/截断抛出的 {@link LZ4Exception} 包成
     * {@link IllegalStateException}（fail-fast，与 pglz 契约同形，不返回部分
     * 结果）；解出长度 != expectedRawSize 同抛 ISE；成功返回数组长度恒为
     * expectedRawSize。</p>
     *
     * @param src             纯 lz4 block（4B tcinfo 头已由上层剥离）
     * @param expectedRawSize 解压结果的期望字节数（tcinfo 声明的原长）
     * @return 长度恰为 expectedRawSize 的解压字节
     * @throws IllegalArgumentException src 为 null 或 expectedRawSize 为负
     * @throws IllegalStateException    block 损坏/截断或解出长度与期望不符
     */
    static byte[] decompress(byte[] src, int expectedRawSize) {
        if (src == null) {
            throw new IllegalArgumentException("lz4 source must not be null");
        }
        if (expectedRawSize < 0) {
            throw new IllegalArgumentException("expectedRawSize must be >= 0, got " + expectedRawSize);
        }
        byte[] dest = new byte[expectedRawSize];
        int written;
        try {
            written = DECOMPRESSOR.decompress(src, 0, src.length, dest, 0);
        } catch (LZ4Exception e) {
            throw new IllegalStateException("lz4 corrupt: " + e.getMessage(), e);
        }
        if (written != expectedRawSize) {
            throw new IllegalStateException(
                    "lz4 corrupt: decoded " + written + " bytes (expected " + expectedRawSize + ")");
        }
        return dest;
    }
}
