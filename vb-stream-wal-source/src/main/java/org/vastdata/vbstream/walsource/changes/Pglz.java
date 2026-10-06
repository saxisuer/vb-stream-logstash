package org.vastdata.vbstream.walsource.changes;

/**
 * pglz 解压——PostgreSQL TOAST 压缩值（varlena tag {@code 0x01}，即
 * {@code VARTAG_COMPRESSED_PGLZ} 载荷的 pglz 数据段）的解压纯函数。
 *
 * <p>本类是 PostgreSQL {@code src/common/pg_lzcompress.c}（REL_18_STABLE）
 * 中 {@code pglz_decompress()}（L692-839，signature 于 L692 起）的逐位 Java
 * 转录，javadoc 内行号均锚定该文件。转录纪律：以源码宏展开与语句逻辑为准，
 * 不引入源码之外的语义。</p>
 *
 * <p>块格式（解压侧，源码行为归纳）：</p>
 * <ul>
 *   <li>control byte 一字节管 8 个条目、<b>LSB 先行</b>（L711 读 ctrl，
 *       L825 {@code ctrl >>= 1} 逐条右移）；</li>
 *   <li>位 0（未置位）＝字面量：从输入拷 1 字节到输出（L816-820）；
 *       注：REL_18 源码中<b>不存在</b> {@code PGLZ_IS_LITERAL} 宏——位判定
 *       就是裸的 {@code if (ctrl & 1)}（L716），置位走 match、未置位走字面量；</li>
 *   <li>位 1（置位）＝match：后随 2 字节匹配码（L727-746）——首字节
 *       <b>低 nibble 编 length-3</b>（L738 {@code len = (sp[0] &amp; 0x0f) + 3}）、
 *       <b>高 nibble 是 offset 的高 4 位</b>，次字节是 offset 低 8 位
 *       （L739 {@code off = ((sp[0] &amp; 0xf0) &lt;&lt; 4) | sp[1]}）；length
 *       nibble 为 {@code 0x0F}（即 len==18）时 tag 共 3 字节，第三字节是
 *       扩展长度 {@code len += *sp++}（L741-746）；</li>
 *   <li>输出侧回拷：从 {@code dp - off} 拷 len 字节到 {@code dp}，区域可重叠
 *       ——源码用"off 倍增 + 非重叠分段拷贝"避免 UB（L774-810），语义等价于
 *       逐字节回拷（周期为 off 的重复）；</li>
 *   <li>损坏判定（源码返回 -1 处，本类改抛 {@link IllegalStateException}
 *       fail-fast）：match tag 截断（L733-735）、扩展字节缺失（L742-744）、
 *       {@code off == 0} 或 {@code off} 越过输出已解前缀（L756-759，防死循环
 *       与越界）、以及完整性检查 {@code dp != destend || sp != srcend}
 *       （L832-833，{@code check_complete == true} 路径）。</li>
 * </ul>
 *
 * <p>线程约束：纯函数无共享可变状态，任意线程并发安全。输入约定：{@code src}
 * 是纯 pglz 数据段——varlena 头与 rawsize 前缀已由上层（ToastAssembler）剥离。</p>
 */
final class Pglz {

    /** 工具类不可实例化。 */
    private Pglz() {
    }

    /**
     * 解压一段 pglz 压缩数据。
     *
     * <p>职责：转录 {@code pglz_decompress()} 的主循环（L705 起）——外层按
     * control byte 分组（每组至多 8 条目，输入耗尽或输出写满即提前出组，L713），
     * 组内逐条目按 LSB 位分派字面量/match 两分支。</p>
     *
     * <p>关键步骤：match 分支先做 tag 完整性预检（2 字节、扩展时 3 字节），
     * 再解码 len/off 并做 off 合法性检查；随后 {@code len = Min(len, destend - dp)}
     * （L763，本实现中 destend - dp 即 {@code expectedRawSize - dp}）钳住不越界
     * 写，最后进入 off 倍增回拷循环。字面量分支直拷一字节。</p>
     *
     * <p>边界与异常语义：输入为 null 或 expectedRawSize 为负抛
     * {@link IllegalArgumentException}（防御式，源码由 slen/rawsize 前置保证）；
     * 数据损坏/截断/解出长度不等于 expectedRawSize 抛
     * {@link IllegalStateException}，绝不返回部分解压结果；成功时返回数组长度
     * 恒为 expectedRawSize。</p>
     *
     * @param src             纯 pglz 数据段（不含 varlena 头与 rawsize 前缀）
     * @param expectedRawSize 解压结果的期望字节数（TOAST 外部长度）
     * @return 长度恰为 expectedRawSize 的解压字节
     */
    static byte[] decompress(byte[] src, int expectedRawSize) {
        if (src == null) {
            throw new IllegalArgumentException("pglz source must not be null");
        }
        if (expectedRawSize < 0) {
            throw new IllegalArgumentException("expectedRawSize must be >= 0, got " + expectedRawSize);
        }
        byte[] dest = new byte[expectedRawSize];
        int sp = 0;
        int dp = 0;
        while (sp < src.length && dp < expectedRawSize) {
            int ctrl = src[sp++] & 0xFF;
            for (int item = 0; item < 8 && sp < src.length && dp < expectedRawSize; item++) {
                if ((ctrl & 1) != 0) {
                    if (sp + 2 > src.length) {
                        throw new IllegalStateException("pglz corrupt: match tag truncated at " + sp);
                    }
                    int b0 = src[sp] & 0xFF;
                    int b1 = src[sp + 1] & 0xFF;
                    int len = (b0 & 0x0F) + 3;
                    int off = ((b0 & 0xF0) << 4) | b1;
                    sp += 2;
                    if (len == 18) {
                        if (sp >= src.length) {
                            throw new IllegalStateException("pglz corrupt: extended length byte missing at " + sp);
                        }
                        len += src[sp++] & 0xFF;
                    }
                    if (off == 0 || off > dp) {
                        throw new IllegalStateException(
                                "pglz corrupt: bad offset " + off + " with " + dp + " bytes decoded");
                    }
                    len = Math.min(len, expectedRawSize - dp);
                    while (off < len) {
                        System.arraycopy(dest, dp - off, dest, dp, off);
                        len -= off;
                        dp += off;
                        off += off;
                    }
                    System.arraycopy(dest, dp - off, dest, dp, len);
                    dp += len;
                } else {
                    dest[dp++] = src[sp++];
                }
                ctrl >>= 1;
            }
        }
        if (dp != expectedRawSize || sp != src.length) {
            throw new IllegalStateException(
                    "pglz corrupt: decoded " + dp + " bytes (expected " + expectedRawSize
                            + "), consumed " + sp + " of " + src.length);
        }
        return dest;
    }
}
