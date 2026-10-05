package org.vastdata.vbstream.walsource.replay;

import org.vastdata.vbstream.walsource.layout.WalLayout;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 系统目录重放的行模型——pg_attribute / pg_class 两张 watched 目录各自的最小列投影
 * （as-of 字典查询与 relfilenode/toast 跟踪所需字段），按物理 ctid 键控存于
 * {@link CatalogStores}。两 record 以嵌套类型收拢进本文件（仓库嵌套类型惯例，
 * 参照 layout 包 HeapViews 的视图收纳方式）。
 *
 * <p>两 record 均提供 {@code fromDecoded(Object[], WalLayout)} 静态工厂：把
 * {@code TupleDecoder} 的解码值行投影为行模型。词典槽位与数据区偏移一样随大版本
 * 漂移（V17 vs V18：pg_attribute 多 attcacheoff 使 attisdropped 后移、pg_class
 * 缺 relallfrozen 使 reltoastrelid 前移），因此漂移槽位一律经 {@link WalLayout}
 * 的版本化访问器（{@code attrDroppedIndex()} / {@code classToastRelidIndex()}）
 * 取值，禁止跨版本硬编码；其余投影槽位（词典前段）V17/V18 同序。</p>
 */
public final class CatalogRow {

    /**
     * 工具类防实例化：两 record 经各自工厂/构造器进入。
     */
    private CatalogRow() {
    }

    /**
     * 一条重放中的 pg_attribute 行（按物理 ctid 键控）——as-of 字典面（列名/类型/
     * 列号/dropped 占位）的最小投影。
     *
     * @param attrelid    所属关系 oid
     * @param attname     列名（DROP COLUMN 后为 "........pg.dropped.N........"）
     * @param atttypid    类型 oid
     * @param attnum      列号
     * @param attisdropped 是否已删列（存储恒 NULL，字典须保留占位——spike 发现 26）
     */
    public record AttrRow(long attrelid, String attname, long atttypid, int attnum, boolean attisdropped) {

        /** 词典索引：attrelid（词典前段 V17/V18 同序——两版 diff 仅在 attnum 之后）。 */
        private static final int IX_ATTRELID = 0;

        /** 词典索引：attname。 */
        private static final int IX_ATTNAME = 1;

        /** 词典索引：atttypid。 */
        private static final int IX_ATTTYPEID = 2;

        /** 词典索引：attnum。 */
        private static final int IX_ATTNUM = 4;

        /**
         * 把 pg_attribute 解码值行投影为行模型。
         *
         * <p>关键步骤：按词典索引取值并做数值/布尔收敛（oid→Long、int2→Short、
         * bool→Boolean、name→String，映射面见 TupleDecoder 类 javadoc）；attisdropped
         * 槽位随版本漂移（V17=17 含 attcacheoff、V18=16），经 {@code layout.attrDroppedIndex()}
         * 取；dropped 判等用 {@code Boolean.TRUE.equals}（null 容错——理论不出现，防御）。
         * 边界与异常语义：索引越界/类型错配由 Number 强转裸抛（上游词典长度已由
         * 解码器校验）；线程约束：静态纯函数，并发安全。</p>
         *
         * @param vals   解码值行（与该版本 pgAttributeKinds() 词典同长）
         * @param layout 解码所用词典的版本布局描述符（漂移槽位来源）
         * @return 行模型
         */
        public static AttrRow fromDecoded(Object[] vals, WalLayout layout) {
            return new AttrRow(
                    ((Number) vals[IX_ATTRELID]).longValue(),
                    (String) vals[IX_ATTNAME],
                    ((Number) vals[IX_ATTTYPEID]).longValue(),
                    ((Number) vals[IX_ATTNUM]).intValue(),
                    Boolean.TRUE.equals(vals[layout.attrDroppedIndex()]));
        }
    }

    /**
     * 一条重放中的 pg_class 行（按物理 ctid 键控）——关系身份面与 relfilenode/toast
     * 跟踪面；列 3-7（relnamespace..relam）保留是为了截断更新的值编码重建（发现 24：
     * 列 1-7 全定宽共 88B，可由行值无损重编码）。
     *
     * @param relOid      关系 oid（列 1）
     * @param relname     关系名（列 2）
     * @param relnamespace 模式 oid（列 3）
     * @param reltype     行类型 oid（列 4）
     * @param reloftype   OF 类型 oid（列 5）
     * @param relowner    属主 oid（列 6）
     * @param relam       存取方法 oid（列 7）
     * @param relfilenode relfilenode（列 8，数据区偏移 88）
     * @param reltoastrelid toast 关系 oid（列 13/14 依版本——V17 槽 12 数据区偏移
     *                      108、V18 槽 13 偏移 112，两版差恰 4B 单列宽）
     */
    public record ClassRow(long relOid, String relname, long relnamespace, long reltype, long reloftype,
                           long relowner, long relam, long relfilenode, long reltoastrelid) {

        /** 词典索引：oid（live PG 18 pg_attribute 于 pg_class 的行序）。 */
        private static final int IX_OID = 0;

        /** 词典索引：relname。 */
        private static final int IX_RELNAME = 1;

        /** 词典索引：relnamespace。 */
        private static final int IX_RELNAMESPACE = 2;

        /** 词典索引：reltype。 */
        private static final int IX_RELTYPE = 3;

        /** 词典索引：reloftype。 */
        private static final int IX_RELOFTYPE = 4;

        /** 词典索引：relowner。 */
        private static final int IX_RELOWNER = 5;

        /** 词典索引：relam。 */
        private static final int IX_RELAM = 6;

        /** 词典索引：relfilenode（数据区偏移 88，INPLACE 直读锚）。 */
        private static final int IX_RELFILENODE = 7;

        /**
         * 编码<strong>读区</strong>的数据区字节（值编码——发现 24 的 88B 前缀区
         * 审查扩展：覆盖 {@code fromDecoded} 消费的全部九个投影列）。
         *
         * <p><strong>读区布局（数据区偏移，与 layout
         * {@code pgClassRelfilenodeDataOffset()/pgClassReltoastrelidDataOffset()} 锚定）</strong>：
         * oid@0（u32 小端）、relname@4（定宽 64B，UTF-8 + NUL 尾垫，超 63B 截断——与
         * NameData 存储一致）、relnamespace..relam@[68,88)（5 个 oid 各 u32）、
         * relfilenode@88（u32）、{@code [92, reltoastrelid 偏移)} 填零
         * （reltablespace/relpages/reltuples/relallvisible[/relallfrozen]——定宽且
         * 不被 {@code fromDecoded} 投影，零值合法；V18 五列 20B 至 112、V17 四列
         * 16B 至 108）、reltoastrelid@toastOff（u32）——合计
         * {@code reltoastrelidDataOffset()+4} 字节（V18 116 / V17 112）。
         * <strong>消费面</strong>：截断更新的 prefix 重编码（prefix ≤ 读区末尾均可由
         * 旧行无损重编码）与<strong>后缀读区回填</strong>（suffix 截断时被省略的尾段与
         * 读区的重叠段按本编码回填——后缀与旧元组逐字节相同是 suffix 截断的定义，
         * 回填即精确值；审查 High-1：此前盲零填充在 RENAME 形态（prefix 落 relname 区、
         * 后缀起点 &lt; 读区末尾）会把 relnamespace..relam 等读区列清零）。
         * 边界与异常语义：relname 超 63B 截断不抛（防御）；读区末尾之外的列不可由
         * 行值重编码，属零填充合法区（定宽不读值）。线程约束：纯函数，并发安全。</p>
         *
         * @param layout 版本布局描述符（toast 偏移锚定读区几何）
         * @return {@code layout.pgClassReltoastrelidDataOffset()+4} 字节数据区读区编码
         */
        public byte[] encodeReadRegion(WalLayout layout) {
            int toastOff = layout.pgClassReltoastrelidDataOffset();
            ByteArrayOutputStream out = new ByteArrayOutputStream(toastOff + 4);
            writeU32(out, relOid);
            byte[] n = relname.getBytes(StandardCharsets.UTF_8);
            out.write(n, 0, Math.min(63, n.length));
            for (int i = Math.min(63, n.length); i < 64; i++) {
                out.write(0);   // NUL 尾垫
            }
            writeU32(out, relnamespace);
            writeU32(out, reltype);
            writeU32(out, reloftype);
            writeU32(out, relowner);
            writeU32(out, relam);
            writeU32(out, relfilenode);
            // [92,toastOff) 定宽列（V17 四列 16B / V18 五列 20B）——不被投影, 零值合法
            out.write(new byte[toastOff - 92], 0, toastOff - 92);
            writeU32(out, reltoastrelid);
            return out.toByteArray();
        }

        /**
         * 把 pg_class 解码值行投影为行模型。
         *
         * <p>关键步骤与异常语义同 {@link AttrRow#fromDecoded(Object[], WalLayout)}
         * （reltoastrelid 槽位随版本漂移——V17=12、V18=13，经
         * {@code layout.classToastRelidIndex()} 取）；线程约束：静态纯函数，并发安全。</p>
         *
         * @param vals   解码值行（与该版本 pgClassKinds() 词典同长）
         * @param layout 解码所用词典的版本布局描述符（漂移槽位来源）
         * @return 行模型
         */
        public static ClassRow fromDecoded(Object[] vals, WalLayout layout) {
            return new ClassRow(
                    ((Number) vals[IX_OID]).longValue(),
                    (String) vals[IX_RELNAME],
                    ((Number) vals[IX_RELNAMESPACE]).longValue(),
                    ((Number) vals[IX_RELTYPE]).longValue(),
                    ((Number) vals[IX_RELOFTYPE]).longValue(),
                    ((Number) vals[IX_RELOWNER]).longValue(),
                    ((Number) vals[IX_RELAM]).longValue(),
                    ((Number) vals[IX_RELFILENODE]).longValue(),
                    ((Number) vals[layout.classToastRelidIndex()]).longValue());
        }

        /**
         * 向流写 little-endian u32。
         *
         * @param out 目标流
         * @param v   32 位值（仅低 32 位有效）
         */
        private static void writeU32(ByteArrayOutputStream out, long v) {
            out.write((int) (v & 0xFF));
            out.write((int) ((v >>> 8) & 0xFF));
            out.write((int) ((v >>> 16) & 0xFF));
            out.write((int) ((v >>> 24) & 0xFF));
        }
    }
}
