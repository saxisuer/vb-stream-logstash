package org.vastdata.vbstream.walsource.layout;

/**
 * LSN（Log Sequence Number）文本/long 双向工具：PG 的显示形态 {@code "XXX/XXXXXXXX"}
 * （高 32 位段 / 低 32 位段，均十六进制、低段补零至 8 位）与打包成单个 long
 * （高 32 位段左移 32 位或上低段）的内部表示互转。
 *
 * <p>纯函数无状态；final 类 + 私有构造器防实例化（任务书原定零组件 record，
 * 但 Java 17 禁止对 record 规范构造器收窄访问权限，私有化编译不过，故以
 * 等价的不可实例 final 类承载）。后续 walker、位点持久化、START_REPLICATION
 * 起点换算均复用本工具，保证全模块 LSN 文本形态与 PG 服务端输出（如
 * {@code pg_current_wal_flush_lsn()}）逐字符一致。</p>
 */
public final class Lsn {

    /** 私有构造器：工具类不可实例，全部能力以静态方法提供。 */
    private Lsn() {
    }

    /**
     * 把打包 long 表示渲染为 PG 文本 LSN。
     *
     * <p>关键步骤：高 32 位（{@code value >>> 32}）以 {@code %X} 渲染（无前导零），
     * 低 32 位（{@code value & 0xFFFFFFFFL}）以 {@code %08X} 渲染（补零至 8 位），
     * 两段以 {@code '/'} 相连——与 pgjdbc {@code LogSequenceNumber} 及 psql 输出同形。
     * 零值渲染为 {@code "0/0"}。</p>
     *
     * @param value 打包 LSN（高 32 位段 = WAL 段号高位，低 32 位段 = 段内偏移）
     * @return PG 文本形态，如 {@code "0/46F00000"}
     */
    public static String format(long value) {
        return String.format("%X/%08X", value >>> 32, value & 0xFFFFFFFFL);
    }

    /**
     * 把 PG 文本 LSN 解析回打包 long——{@link #format(long)} 的严格逆。
     *
     * <p>关键步骤：按第一个 {@code '/'} 分段，两段各自按十六进制解析，高段左移 32 位
     * 与低段按位或。边界与异常语义：入参不含 {@code '/'}、有空段（前导或尾随）或段内
     * 含非十六进制字符时抛 {@link IllegalArgumentException}（含 {@code Long.parseLong}
     * 的 NumberFormatException 链）；不接受空串与 null（null 直接 NPE）。</p>
     *
     * @param text PG 文本形态 LSN，如 {@code "16/B374D848"}
     * @return 打包 long 表示
     */
    public static long parse(String text) {
        int slash = text.indexOf('/');
        if (slash <= 0 || slash == text.length() - 1) {
            throw new IllegalArgumentException("malformed LSN (expect HI/LO hex): " + text);
        }
        return (Long.parseLong(text.substring(0, slash), 16) << 32)
                | Long.parseLong(text.substring(slash + 1), 16);
    }
}
