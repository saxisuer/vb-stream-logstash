package org.vastdata.vbstream.walsource.replay;

import org.vastdata.vbstream.walsource.layout.TupleDecoder;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 系统目录重放的行模型——pg_attribute / pg_class / pg_namespace 三张 watched 目录
 * （v1/v2）+ openGauss M3 扩链的第四张 pg_partition 各自的最小列投影（as-of 字典
 * 查询与 relfilenode/toast 跟踪所需字段），按物理 ctid 键控存于 {@link CatalogStores}。
 * 各 record 以嵌套类型收拢进本文件（仓库嵌套类型惯例，参照 layout 包 HeapViews 的
 * 视图收纳方式）。
 *
 * <p>pg_attribute / pg_class 两 record 提供 {@code fromDecoded(Object[], WalLayout)}
 * 静态工厂：把 {@code TupleDecoder} 的解码值行投影为行模型。词典槽位与数据区偏移
 * 一样随大版本漂移（V17 vs V18：pg_attribute 多 attcacheoff 使 attisdropped 后移、
 * pg_class 缺 relallfrozen 使 reltoastrelid 前移），因此漂移槽位一律经 {@link WalLayout}
 * 的版本化访问器（{@code attrDroppedIndex()} / {@code classToastRelidIndex()} /
 * {@code classRelkindIndex()}）取值，禁止跨版本硬编码；其余投影槽位（词典前段）
 * V17/V18 同序。pg_namespace 列序两版无漂移（oid/nspname/nspowner/nspacl 四列），
 * 词典经 {@link NspRow#kinds(WalLayout)} 版本感知分派（vanilla 档自持 4 项）。</p>
 */
public final class CatalogRow {

    /**
     * 工具类防实例化：三 record 经各自工厂/构造器进入。
     */
    private CatalogRow() {
    }

    /**
     * 一条重放中的 pg_attribute 行（按物理 ctid 键控）——as-of 字典面（列名/类型/
     * 列号/dropped 占位）的最小投影。
     *
     * <p><b>无值编码读区（vastbase T3 核查注记）</b>：attr 面不提供
     * {@code encodeReadRegion}——五字段投影无法重编码全列前缀（class/nsp/partition
     * 的读区重建前提是投影列覆盖定宽前段），截断更新走 rawTail splice / ctid 精确
     * 采纳两通道（{@code CatalogReplay.reconstructTruncatedUpdate} 的 attr 分支），
     * attname 不以 64B 硬编码参与任何值编码重建——故 VastBase 的 nameDatumWidth
     * 加宽在本 record 无读区消费面（词典宽度差异由 pgAttributeKinds() 的 name128
     * kind 承担，投影槽位全在词典分叉点槽 19 之前零漂移）。</p>
     *
     * @param attrelid    所属关系 oid
     * @param attname     列名（DROP COLUMN 后为 "........pg.dropped.N........"）
     * @param atttypid    类型 oid
     * @param attnum      列号
     * @param attisdropped 是否已删列（存储恒 NULL，字典须保留占位——spike 发现 26）
     */
    public record AttrRow(long attrelid, String attname, long atttypid, int attnum, boolean attisdropped) {

        /** 词典索引：attrelid（词典前段 V17/V18/OG 同序——各版 diff 仅在 atttypid 之后）。 */
        private static final int IX_ATTRELID = 0;

        /** 词典索引：attname。 */
        private static final int IX_ATTNAME = 1;

        /** 词典索引：atttypid。 */
        private static final int IX_ATTTYPEID = 2;

        // attnum 槽位随版本漂移（V17/V18=4、OG=5——OG 词典含 attstattarget@3 的
        // PG≤16 位次），经 layout.attrAttnumIndex() 取，禁止跨版本硬编码

        /**
         * 把 pg_attribute 解码值行投影为行模型。
         *
         * <p>关键步骤：按词典索引取值并做数值/布尔收敛（oid→Long、int2→Short、
         * bool→Boolean、name→String，映射面见 TupleDecoder 类 javadoc）；attisdropped
         * 与 attnum 槽位随版本漂移（V17=17/4 含 attcacheoff、V18=16/4、OG=14/5 含
         * attstattarget@3 前移），经 {@code layout.attrDroppedIndex()} /
         * {@code layout.attrAttnumIndex()} 取；dropped 判等用
         * {@code Boolean.TRUE.equals}（null 容错——理论不出现，防御）。
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
                    ((Number) vals[layout.attrAttnumIndex()]).intValue(),
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
     * @param relkind     relkind 单字符（列 17/18 依版本——V17 槽 16 偏移 115、V18 槽 17
     *                    偏移 119；v2 表过滤的分派键，投影恒裸字符如 "r"）
     */
    public record ClassRow(long relOid, String relname, long relnamespace, long reltype, long reloftype,
                           long relowner, long relam, long relfilenode, long reltoastrelid, String relkind) {

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
         * 审查扩展：覆盖 {@code fromDecoded} 消费的全部十个投影列——v2 增读区末列
         * relkind）。
         *
         * <p><strong>读区布局（数据区偏移，与 layout
         * {@code pgClassRelfilenodeDataOffset()/pgClassReltoastrelidDataOffset()/
         * pgClassRelkindDataOffset()} 锚定；name 定宽经 {@code nameDatumWidth()} 取）
         * </strong>，按 oid 形态分档：
         * <b>V17/V18（oid 是数据列）</b>：oid@0（u32 小端）、relname@4（定宽 64B，
         * UTF-8 + NUL 尾垫，超 63B 截断——与 NameData 存储一致）、
         * relnamespace..relam@[68,88)（5 个 oid 各 u32）、relfilenode@88（u32）、
         * {@code [92, reltoastrelid 偏移)} 填零（reltablespace/relpages/reltuples/
         * relallvisible[/relallfrozen]——定宽且不被投影，零值合法；V18 五列 20B 至
         * 112、V17 四列 16B 至 108）、reltoastrelid@toastOff（u32）、
         * {@code [toastOff+4, relkind 偏移)} 填零（三个单字节列——不投影，零值合法）、
         * relkind@relkindOff——合计 {@code relkindDataOffset()+1} 字节（V18 120 /
         * V17 116）。<b>openGauss（oid 在 9.2 式元组头，数据区首列即 relname@0）</b>：
         * relname@0（64B）、relnamespace..relam@[64,84)（5 oid）、relfilenode@84、
         * 填零 [88,116)（tablespace + float8 加宽的 pages/tuples + allvisible）、
         * reltoastrelid@116、填零 [120,143)（五 oid 列存四件 + reltoastidxid + 三
         * 单字节列）、relkind@143——合计 144 字节。<b>VastBase（OG 几何 + name128，
         * vastbase T3）</b>：relname@0（128B）、relnamespace..relam@[128,148)、
         * relfilenode@148、填零 [152,180)、reltoastrelid@180、填零 [184,207)、
         * relkind@207——合计 208 字节（= OG 144 + 64，name 加宽净效果；两个填零段
         * 宽度经 layout 偏移差自适应，无需独立分叉）。
         * <strong>消费面</strong>：截断更新的 prefix 重编码（prefix ≤ 读区末尾均可由
         * 旧行无损重编码）与<strong>后缀读区回填</strong>（suffix 截断时被省略的尾段与
         * 读区的重叠段按本编码回填——后缀与旧元组逐字节相同是 suffix 截断的定义，
         * 回填即精确值；审查 High-1：此前盲零填充在 RENAME 形态（prefix 落 relname 区、
         * 后缀起点 &lt; 读区末尾）会把 relnamespace..relam 等读区列清零）。
         * 边界与异常语义：relname 超定宽-1 截断不抛（防御）；读区末尾之外的列不可由
         * 行值重编码，属零填充合法区（定宽不读值）。线程约束：纯函数，并发安全。</p>
         *
         * @param layout 版本布局描述符（toast/relkind 偏移锚定读区几何 + name 宽度；
         *               OG 档经 {@code classOidInTupleHeader()} 分派无 oid 前缀形态）
         * @return {@code layout.pgClassRelkindDataOffset()+1} 字节数据区读区编码
         */
        public byte[] encodeReadRegion(WalLayout layout) {
            int toastOff = layout.pgClassReltoastrelidDataOffset();
            int relkindOff = layout.pgClassRelkindDataOffset();
            boolean oidHeader = layout.classOidInTupleHeader();
            int filenodeOff = layout.pgClassRelfilenodeDataOffset();
            int nameWidth = layout.nameDatumWidth();
            ByteArrayOutputStream out = new ByteArrayOutputStream(relkindOff + 1);
            if (!oidHeader) {
                writeU32(out, relOid);
            }
            byte[] n = relname.getBytes(StandardCharsets.UTF_8);
            out.write(n, 0, Math.min(nameWidth - 1, n.length));
            for (int i = Math.min(nameWidth - 1, n.length); i < nameWidth; i++) {
                out.write(0);   // NUL 尾垫
            }
            writeU32(out, relnamespace);
            writeU32(out, reltype);
            writeU32(out, reloftype);
            writeU32(out, relowner);
            writeU32(out, relam);
            writeU32(out, relfilenode);
            // [filenodeOff+4,toastOff) 定宽列（V17 四列 16B / V18 五列 20B / OG 与
            // VastBase 均 28B——tablespace + float8 pages/tuples + allvisible）——
            // 不被投影, 零值合法
            out.write(new byte[toastOff - filenodeOff - 4], 0, toastOff - filenodeOff - 4);
            writeU32(out, reltoastrelid);
            // [toastOff+4,relkindOff) 单字节/oid 列（V17/V18 三单字节列；OG/VastBase
            // 另含五个 4B oid 列——列存四件 + reltoastidxid）——不被投影, 零值合法
            out.write(new byte[relkindOff - toastOff - 4], 0, relkindOff - toastOff - 4);
            out.write(relkind == null || relkind.isEmpty() ? 0 : (byte) relkind.charAt(0));
            return out.toByteArray();
        }

        /**
         * 把 pg_class 解码值行投影为行模型。
         *
         * <p>关键步骤与异常语义同 {@link AttrRow#fromDecoded(Object[], WalLayout)}
         * （reltoastrelid/relkind 槽位随版本漂移——V17=12/16、V18=13/17，经
         * {@code layout.classToastRelidIndex()} / {@code layout.classRelkindIndex()}
         * 取；<b>OG 档</b>值行含虚拟头 oid 前导槽——物理词典槽位整体
         * +{@code layout.classValsSlotDelta()}，且前导槽恰使 IX_* 镜像槽位与
         * V17/V18 对齐——见 IX_OID 注记）；relkind 词典 kind 为 "char"——
         * {@code TupleDecoder} 解码为左引号单字符（{@code 'r'}），投影经
         * {@link #stripCharQuotes} 剥引号收敛为裸字符（与 JDBC/引导种子的
         * {@code getString} 形态一致，record equals 两侧同锚）。
         * 线程约束：静态纯函数，并发安全。</p>
         *
         * @param vals   解码值行（V17/V18 与 pgClassKinds() 同长；OG 与
         *               classDecodeKinds() 注入前导槽后同长 = 词典+1）
         * @param layout 解码所用词典的版本布局描述符（漂移槽位来源）
         * @return 行模型
         */
        public static ClassRow fromDecoded(Object[] vals, WalLayout layout) {
            // OG 档：虚拟头 oid 槽使前八列（oid..relfilenode）的值行槽位恰与 V17/V18
            // 的 IX_* 镜像对齐（零位移）；reltoastrelid/relkind 物理槽位较 vanilla 前移
            // 一位，值行槽位 = 访问器（物理槽位）+ classValsSlotDelta()
            int delta = layout.classValsSlotDelta();
            return new ClassRow(
                    ((Number) vals[IX_OID]).longValue(),
                    (String) vals[IX_RELNAME],
                    ((Number) vals[IX_RELNAMESPACE]).longValue(),
                    ((Number) vals[IX_RELTYPE]).longValue(),
                    ((Number) vals[IX_RELOFTYPE]).longValue(),
                    ((Number) vals[IX_RELOWNER]).longValue(),
                    ((Number) vals[IX_RELAM]).longValue(),
                    ((Number) vals[IX_RELFILENODE]).longValue(),
                    ((Number) vals[layout.classToastRelidIndex() + delta]).longValue(),
                    stripCharQuotes(vals[layout.classRelkindIndex() + delta]));
        }

        /**
         * "char" 词典列的解码值收敛：TupleDecoder 的 char 渲染是<strong>左引号
         * 单字符</strong>（{@code 'r}——仅前导引号），剥引号为裸字符（"r"）；已是
         * 裸形态（JDBC 探测/种子路径的 {@code getString} 产物）原样透传——两来源
         * （WAL 解码 / JDBC 实查）的 relkind 表示在此归一。
         *
         * @param v 解码槽位值（String 预期；null 防御透传）
         * @return 裸单字符（或 null）
         */
        private static String stripCharQuotes(Object v) {
            String s = v == null ? null : v.toString();
            if (s == null || s.isEmpty()) {
                return s;
            }
            if (s.charAt(0) == '\'') {
                s = s.substring(1);
            }
            if (!s.isEmpty() && s.charAt(s.length() - 1) == '\'') {
                s = s.substring(0, s.length() - 1);
            }
            return s;
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

    /**
     * 一条重放中的 pg_namespace 行（按物理 ctid 键控，v2 扩链第三 watched 目录）
     * ——schema 名解析的最小投影（pg_class.relnamespace → 本表 nspname，"public"
     * 等对拍契约面的 schema 源）。pg_namespace 是普通表、有自身 relfilenode，引导
     * 流程与两目录同构（种子 + 回填 relfilenode + 流内重放）。
     *
     * <p>列序 V17/V18 无漂移（pg_namespace.h：oid/nspname/nspowner/nspacl 四列），
     * 解码词典自持于 {@link #kinds()}，不经 layout 版本化。<b>openGauss（Task 7）：
     * pg_namespace 七数据列 + oid 在 9.2 式元组头</b>（live 实查 attnum 升序：
     * nspname name@1、nspowner oid@2、nsptimeline int8@3、nspacl@4、
     * in_redistribution char@5、nspblockchain bool@6、nspcollation oid@7——OG 词典
     * 经 {@link #kinds(WalLayout)} 取，首位注入虚拟头 oid 槽使 nspOid/nspname 的
     * 值行槽位（0/1）与 vanilla 对齐）。</p>
     *
     * @param nspOid  模式 oid（列 1）
     * @param nspname 模式名（列 2，定宽 name——V17/18/OG 64B / VastBase 128B）
     */
    public record NspRow(long nspOid, String nspname) {

        /**
         * 值编码<strong>读区</strong>长度（数据区偏移，自 t_hoff 起）：oid u32@0 +
         * nspname 定宽 64B@4 + nspowner u32@68 = 72B——截断更新值编码重建的合法
         * prefix 上界与后缀读区回填的读区末尾（语义同 ClassRow 读区，发现 24 审查
         * 扩展的 nsp 面）。nspowner 不在投影面，读区编码的 [68,72) 以 0 占位——
         * 投影/解码不消费该列，仅重建几何需要（与 ClassRow 读区 [92,toastOff)
         * 不投影列零占位同惯例）；nspacl varlena@72 不可值编码，prefix 越过 72
         * 落 rawTail splice。<b>OG 档读区长 80、VastBase 档 144</b>（经
         * {@link #readRegionLen(WalLayout)} 取）。
         */
        public static final int READ_REGION_LEN = 72;

        /** 词典索引：oid（列 1；OG 档为虚拟头 oid 槽——两形态槽位同为 0）。 */
        private static final int IX_OID = 0;

        /** 词典索引：nspname（列 2；OG 档数据列 1，值行槽位经前导槽同为 1）。 */
        private static final int IX_NSPNAME = 1;

        /** pg_namespace 列解码词典（4 项，V17/V18 同序——oid/name/oid/skip）。 */
        private static final String[] KINDS = {
                "oid",   // oid
                "name",  // nspname (NameData, 64B fixed)
                "oid",   // nspowner
                "skip",  // nspacl aclitem[]
        };

        /**
         * openGauss pg_namespace 列解码词典（8 项 = 虚拟头 oid 槽 + 七数据列，
         * live 实查列序——见 record javadoc；nsptimeline int8 读区对齐占位，
         * in_redistribution/nspblockchain/nspcollation 不投影仅消耗）。
         */
        private static final String[] OG_KINDS = {
                TupleDecoder.OIDHDR_KIND,   // 元组头 oid（虚拟槽）
                "name",   // nspname
                "oid",    // nspowner
                "int8",   // nsptimeline（OG 独有，attlen=8 实查）
                "skip",   // nspacl aclitem[]
                "char",   // in_redistribution（OG 独有）
                "bool",   // nspblockchain（OG 独有）
                "oid",    // nspcollation（OG 独有）
        };

        /**
         * VastBase G100 pg_namespace 词典（8 项 = 虚拟头 oid 槽 + 七数据列）：
         * 列集与 {@link #OG_KINDS} 同名同序（live 实查，vastbase spec 附录 A），
         * 仅 nspname 加宽为 name128（VastBase NAMEDATALEN 128）。
         */
        private static final String[] VAST_KINDS = {
                TupleDecoder.OIDHDR_KIND,   // 元组头 oid（虚拟槽）
                "name128",   // nspname（VastBase NAMEDATALEN 128）
                "oid",    // nspowner
                "int8",   // nsptimeline
                "skip",   // nspacl
                "char",   // in_redistribution
                "bool",   // nspblockchain
                "oid",    // nspcollation
        };

        /**
         * pg_namespace 列解码词典（防御副本——调用方改写不污染静态词典；V17/V18 档）。
         *
         * @return 列 kind 数组（4 项）
         */
        public static String[] kinds() {
            return KINDS.clone();
        }

        /**
         * pg_namespace 列解码词典（版本感知档，Task 7 OG 适配；vastbase Task 2 增
         * VastBase 分支）。
         *
         * <p>关键步骤：{@code layout instanceof WalLayoutVVastbase} 时返回 G100
         * name128 变体词典（8 项，列集与 OG 同名同序仅 nspname 加宽——判别先行于
         * openGaussDialect，VastBase 档继承 OG 方言位 true）；否则
         * {@code layout.openGaussDialect()} 时返回 OG 七数据列 + 虚拟头 oid 前导槽
         * 的 8 项词典（值行槽 0/1 与 vanilla 的 oid/nspname 对齐，
         * {@link #fromDecoded} 无需分版本）；否则返回 vanilla 4 项词典（与
         * {@link #kinds()} 同值）。防御副本。线程约束：纯函数。</p>
         *
         * @param layout 版本布局描述符（WalLayoutVVastbase / openGaussDialect 决定词典分派）
         * @return 列 kind 数组（V17/18=4 项 / OG=8 项含虚拟头 oid 槽 /
         *         VastBase=8 项含虚拟头 oid 槽且 nspname 为 name128）
         */
        public static String[] kinds(WalLayout layout) {
            if (layout instanceof WalLayoutVVastbase) {
                return VAST_KINDS.clone();
            }
            return layout.openGaussDialect() ? OG_KINDS.clone() : KINDS.clone();
        }

        /**
         * 值编码<strong>读区</strong>长度（版本感知档；vastbase T3 增 VastBase 分支）：
         * V17/V18=72（见 {@link #READ_REGION_LEN}）；OG=80——数据区 nspname@0
         * （64B）+ nspowner@64（4B）+ 垫 4B + nsptimeline int8@72（8 对齐）至 80，
         * nspacl varlena@80 不可值编码；<b>VastBase=144</b>（= OG 80 + 64）——
         * nspname@0（128B）+ nspowner@128（4B）+ 垫 4B + nsptimeline int8@136
         * （8 对齐）至 144，nspacl varlena@144 不可值编码。判别 {@code instanceof
         * WalLayoutVVastbase} 先行于 {@code openGaussDialect()}（VastBase 继承
         * OG 方言位 true，次序颠倒会误取 OG 读区）。
         *
         * @param layout 版本布局描述符
         * @return 读区字节数（V17/18=72，OG=80，VastBase=144）
         */
        public static int readRegionLen(WalLayout layout) {
            if (layout instanceof WalLayoutVVastbase) {
                return 144;
            }
            return layout.openGaussDialect() ? 80 : READ_REGION_LEN;
        }

        /**
         * 把 pg_namespace 解码值行投影为行模型。
         *
         * <p>关键步骤与异常语义同 {@link AttrRow#fromDecoded(Object[], WalLayout)}
         * （vanilla 无版本漂移槽位；OG 档经虚拟头 oid 前导槽使两投影槽位与 vanilla
         * 对齐——词典取 {@link #kinds(WalLayout)}）；线程约束：静态纯函数，并发安全。</p>
         *
         * @param vals 解码值行（与所用版本词典同长）
         * @return 行模型
         */
        public static NspRow fromDecoded(Object[] vals) {
            return new NspRow(
                    ((Number) vals[IX_OID]).longValue(),
                    (String) vals[IX_NSPNAME]);
        }

        /**
         * 编码<strong>读区</strong>的数据区字节（值编码——vanilla 档语义同
         * {@link ClassRow#encodeReadRegion(WalLayout)} 的 nsp 面）。
         *
         * <p>读区布局（数据区偏移）：oid@0（u32 小端）、nspname@4（定宽 64B，
         * UTF-8 + NUL 尾垫，超 63B 截断——与 NameData 存储一致）、nspowner@68
         * （u32，<strong>以 0 占位</strong>——投影面不含该列，见
         * {@link #READ_REGION_LEN} javadoc）。消费面：截断更新的 prefix 重编码与
         * 后缀读区回填。边界与异常语义：nspname 超 63B 截断不抛（防御）；线程
         * 约束：纯函数，并发安全。</p>
         *
         * @return {@value #READ_REGION_LEN} 字节数据区读区编码
         */
        public byte[] encodeReadRegion() {
            ByteArrayOutputStream out = new ByteArrayOutputStream(READ_REGION_LEN);
            out.write((int) (nspOid & 0xFF));
            out.write((int) ((nspOid >>> 8) & 0xFF));
            out.write((int) ((nspOid >>> 16) & 0xFF));
            out.write((int) ((nspOid >>> 24) & 0xFF));
            byte[] n = nspname.getBytes(StandardCharsets.UTF_8);
            out.write(n, 0, Math.min(63, n.length));
            for (int i = Math.min(63, n.length); i < 64; i++) {
                out.write(0);   // NUL 尾垫
            }
            out.write(new byte[4], 0, 4);   // nspowner@68：0 占位（不投影）
            return out.toByteArray();
        }

        /**
         * 编码<strong>读区</strong>的数据区字节（版本感知档，Task 7 OG 适配；
         * vastbase T3 起 name 定宽经 {@code layout.nameDatumWidth()} 参数化，
         * 读区总长与尾段零占位经 {@link #readRegionLen(WalLayout)} 收口）。
         *
         * <p><b>openGauss 读区布局</b>（数据区偏移，oid 在元组头）：nspname@0
         * （定宽 64B，NUL 尾垫）、nspowner@64（u32，0 占位——不投影）、垫 4B、
         * nsptimeline@72（int8，0 占位——不投影）——合计 80B。<b>VastBase</b>
         * （vastbase T3）：nspname@0（128B，NUL 尾垫）、nspowner@128（0 占位）、
         * 垫 4B、nsptimeline int8@136（0 占位）——合计 144B。vanilla 档委派
         * {@link #encodeReadRegion()}。<strong>vastbase T3 连带修正（OG 档）</strong>：
         * 原实现 name 后仅写 12B（nspowner 4 + 8B），实长 76 与声明读区 80 不符
         * ——prefix∈(76,80] 或回填触达 [76,80) 会数组越界；现按读区总长零占位
         * 收口（实长 == readRegionLen），越界窗口关死，[76,80) 属 nsptimeline
         * 上半段的 0 占位（不投影，解码消费恒无害）。
         * 边界与异常语义同 vanilla 档；线程约束：纯函数，并发安全。</p>
         *
         * @param layout 版本布局描述符（openGaussDialect/nameDatumWidth 决定读区几何）
         * @return 读区字节数据区编码（V17/18=72 / OG=80 / VastBase=144）
         */
        public byte[] encodeReadRegion(WalLayout layout) {
            if (!layout.openGaussDialect()) {
                return encodeReadRegion();
            }
            int nameWidth = layout.nameDatumWidth();
            int total = readRegionLen(layout);
            ByteArrayOutputStream out = new ByteArrayOutputStream(total);
            byte[] n = nspname.getBytes(StandardCharsets.UTF_8);
            out.write(n, 0, Math.min(nameWidth - 1, n.length));
            for (int i = Math.min(nameWidth - 1, n.length); i < nameWidth; i++) {
                out.write(0);   // NUL 尾垫
            }
            // [nameWidth,total)：nspowner（4B，0 占位）+ 垫 4B + nsptimeline int8
            // （0 占位）——不投影列，仅重建几何需要
            out.write(new byte[total - nameWidth], 0, total - nameWidth);
            return out.toByteArray();
        }
    }

    /**
     * 一条重放中的 pg_partition 行（按物理 ctid 键控，openGauss M3 Task 1 扩链
     * <b>第四 watched 目录</b>）——OG 原生分区路由字典的最小投影：分区 relfilenode
     * （heap 记录路由锚）→ parttype/parentid → 母表 pg_class oid 的映射链源
     * （spike 锚②：`PARTITION_NAME=mother`——wal 路表名解析须经本链归一到母表），
     * 加 reltoastrelid（spike 锚③：分区自有 toast 挂 'p' 行、母表面恒 0——
     * ToastAssembler 的 chunk 归属反查源）。
     *
     * <p><b>openGauss 5.0.0 专属表</b>（vanilla PG 无 pg_partition——第四腿仅在
     * OG 档激活）：32 数据列 + oid 在 9.2 式元组头（T0 锚①，og-plan live
     * pg_attribute 实查 2026-10-13：attnum 1-32 见 {@link #KINDS} 列序注）；行身份
     * 双面——parttype 'r'=母表行（relfilenode/reltoastrelid 恒 0）与 'p'=分区行。
     * 词典自持于 {@link #kinds()}（首位虚拟头 oid 槽使投影槽位与值行对齐），
     * VastBase G100 档经 {@link #kinds(WalLayout)} 返回 name128 变体（vastbase
     * Task 2——32 数据列与 OG 逐列同名同序，仅 relname 加宽）。
     * <b>无伴生结构</b>（OG 5.0.0 不存在 pg_partition_fn/
     * pg_partition_enum，T0 锚①），watched 面单表。</p>
     *
     * @param partOid     行自身 oid（元组头 oid 虚拟槽；分区行 INSERT 时与 relfilenode
     *                    同值，EXCHANGE 后换源表段号——以重放末态为准，T0 坑 3）
     * @param relname     分区/母表名（定宽 name——OG 64B / VastBase 128B）
     * @param parttype    行身份（'r'=母表行 / 'p'=分区行——与 pg_class.parttype
     *                    值域不同语义：这里区分行身份，投影恒裸字符）
     * @param parentid    母表 pg_class oid（母表行=自 oid）
     * @param relfilenode 分区物理段号（OG 数据区偏移 84 / VastBase 148；路由锚——
     *                    heap 记录落此段号）
     * @param reltoastrelid 分区自有 toast 关系 oid（OG 数据区偏移 116 / VastBase 180；
     *                    'r' 行恒 0）
     */
    public record PartitionRow(long partOid, String relname, String parttype,
                               long parentid, long relfilenode, long reltoastrelid) {

        /**
         * 值编码<strong>读区</strong>长度（<b>OG 5.0.0 几何</b>，数据区偏移自 t_hoff 起；
         * VastBase 档读区长经 {@link #readRegionLen(WalLayout)} 取——184，见其 javadoc）：
         * [0,120) 定宽段 = relname 64B@0 + parttype 1B@64 + 垫 3 + parentid u32@68 +
         * rangenum@72 + intervalnum@76 + partstrategy@80 + 垫 3 + relfilenode u32@84 +
         * reltablespace@88（0 占位——不投影）+ 垫 4 + relpages float8@96（0 占位）+
         * reltuples float8@104（0 占位）+ relallvisible@112（0 占位）+
         * reltoastrelid u32@116——截断更新值编码重建的合法 prefix 上界与后缀读区
         * 回填的读区末尾（语义同 {@link ClassRow}/{@link NspRow} 读区；T0 锚①
         * 数据区偏移 84/116 实测锚，与 pg_class OG 列 7-12 同宽对齐恰同值）。
         * 变长段 @156 起不可值编码，prefix 越过 120 落 rawTail splice。
         */
        public static final int READ_REGION_LEN = 120;

        /** 词典索引：元组头 oid（虚拟槽 0——9.2 式 [t_hoff-4,t_hoff) 垫区）。 */
        private static final int IX_PARTOID = 0;

        /** 词典索引：relname（attnum 1，数据区 @0；VastBase @0 宽 128）。 */
        private static final int IX_RELNAME = 1;

        /** 词典索引：parttype（attnum 2，OG 数据区 @64；VastBase @128）。 */
        private static final int IX_PARTTYPE = 2;

        /** 词典索引：parentid（attnum 3，OG 数据区 @68；VastBase @132）。 */
        private static final int IX_PARENTID = 3;

        /** 词典索引：relfilenode（attnum 7，OG 数据区 @84 / VastBase @148——虚拟槽使
         *  值行槽位 = attnum）。 */
        private static final int IX_RELFILENODE = 7;

        /** 词典索引：reltoastrelid（attnum 12，OG 数据区 @116 / VastBase @180）。 */
        private static final int IX_RELTOASTRELID = 12;

        /**
         * pg_partition 列解码词典（33 项 = 虚拟头 oid 槽 + 32 数据列），逐行转录自
         * live opengauss:5.0.0 服务端 pg_attribute 于 pg_partition 的行（attnum
         * 升序，2026-10-13 og-plan 容器实查，T0 锚①同源 SQL）：
         * relname name@1 / parttype char@2 / parentid oid@3 / rangenum int4@4 /
         * intervalnum int4@5 / partstrategy char@6 / relfilenode oid@7 /
         * reltablespace oid@8 / relpages float8@9 / reltuples float8@10 /
         * relallvisible int4@11 / reltoastrelid oid@12 / reltoastidxid oid@13 /
         * indextblid oid@14 / indisusable bool@15 / reldeltarelid oid@16 /
         * reldeltaidx oid@17 / relcudescrelid oid@18 / relcudescidx oid@19 /
         * relfrozenxid xid32@20 / intspnum int4@21 / partkey int2vector@22 /
         * intervaltablespace oidvector@23 / interval _text@24 / boundaries _text@25 /
         * transit _text@26 / reloptions _text@27 / relfrozenxid64 xid@28 /
         * relminmxid xid@29 / partkeyexpr text@30 / partitionno int4@31 /
         * subpartitionno int4@32。变长段 22-27/30 路由面不消费记 "skip"；
         * xid32/xid 沿 pg_class 词典的 int4/int8 口径（M1 Task 7 先例）。
         */
        private static final String[] KINDS = {
                TupleDecoder.OIDHDR_KIND,   // 元组头 oid（虚拟槽）
                "name",    // relname
                "char",    // parttype（'r'/'p' 行身份）
                "oid",     // parentid
                "int4",    // rangenum
                "int4",    // intervalnum
                "char",    // partstrategy（'r'/'l'）
                "oid",     // relfilenode          <-- 槽 7（OG 数据区 84 / VastBase 148）
                "oid",     // reltablespace
                "float8",  // relpages（float8——与 pg_class OG 同形加宽）
                "float8",  // reltuples
                "int4",    // relallvisible
                "oid",     // reltoastrelid        <-- 槽 12（OG 数据区 116 / VastBase 180）
                "oid",     // reltoastidxid
                "oid",     // indextblid
                "bool",    // indisusable
                "oid",     // reldeltarelid
                "oid",     // reldeltaidx
                "oid",     // relcudescrelid
                "oid",     // relcudescidx
                "int4",    // relfrozenxid（xid32 4B）
                "int4",    // intspnum
                "skip",    // partkey int2vector
                "skip",    // intervaltablespace oidvector
                "skip",    // interval _text
                "skip",    // boundaries _text
                "skip",    // transit _text
                "skip",    // reloptions _text
                "int8",    // relfrozenxid64（xid 8B）
                "int8",    // relminmxid（xid 8B）
                "skip",    // partkeyexpr text
                "int4",    // partitionno
                "int4",    // subpartitionno
        };

        /**
         * pg_partition 列解码词典的 VastBase G100 变体（33 项 = 虚拟头 oid 槽 +
         * 32 数据列）：与 {@link #KINDS} 逐列同名同序（live 实查，vastbase spec
         * 附录 A），仅 relname 加宽为 name128（VastBase NAMEDATALEN 128）——
         * 槽 1 之外与 OG 词典逐项相等。数据区几何 = OG 各偏移 +64（relname 加宽
         * 净效果，见 {@link #readRegionLen(WalLayout)}）。
         */
        private static final String[] VAST_KINDS = {
                TupleDecoder.OIDHDR_KIND,   // 元组头 oid（虚拟槽）
                "name128",  // relname（VastBase NAMEDATALEN 128）
                "char",    // parttype（'r'/'p' 行身份）
                "oid",     // parentid
                "int4",    // rangenum
                "int4",    // intervalnum
                "char",    // partstrategy（'r'/'l'）
                "oid",     // relfilenode          <-- 槽 7（VastBase 数据区 148；OG 84）
                "oid",     // reltablespace
                "float8",  // relpages（float8——与 pg_class OG 同形加宽）
                "float8",  // reltuples
                "int4",    // relallvisible
                "oid",     // reltoastrelid        <-- 槽 12（VastBase 数据区 180；OG 116）
                "oid",     // reltoastidxid
                "oid",     // indextblid
                "bool",    // indisusable
                "oid",     // reldeltarelid
                "oid",     // reldeltaidx
                "oid",     // relcudescrelid
                "oid",     // relcudescidx
                "int4",    // relfrozenxid（xid32 4B）
                "int4",    // intspnum
                "skip",    // partkey int2vector
                "skip",    // intervaltablespace oidvector
                "skip",    // interval _text
                "skip",    // boundaries _text
                "skip",    // transit _text
                "skip",    // reloptions _text
                "int8",    // relfrozenxid64（xid 8B）
                "int8",    // relminmxid（xid 8B）
                "skip",    // partkeyexpr text
                "int4",    // partitionno
                "int4",    // subpartitionno
        };

        /**
         * pg_partition 列解码词典（防御副本——调用方改写不污染静态词典）。OG 专属表
         * 无 vanilla 档变体：第四腿仅在 OG 引导回填 pgPartitionRelfilenode 后激活
         * （vanilla 恒 0 不匹配，词典不被消费）。
         *
         * @return 列 kind 数组（33 项含虚拟头 oid 槽）
         */
        public static String[] kinds() {
            return KINDS.clone();
        }

        /**
         * pg_partition 列解码词典（版本感知，vastbase Task 2）：VastBase G100 档返回
         * name128 变体（32 数据列与 OG 5.0 逐列同名同序——live 实查，vastbase spec
         * 附录 A；仅 relname 加宽），其余档返回既有 OG 词典（vanilla 无此表，返回值
         * 不被消费——第四腿激活面由 pgPartitionRelfilenode 门控）。防御副本。
         * 线程约束：纯函数。
         *
         * @param layout 版本布局描述符（WalLayoutVVastbase 决定词典分派）
         * @return 列 kind 数组（33 项含虚拟头 oid 槽；VastBase 档槽 1 为 name128）
         */
        public static String[] kinds(WalLayout layout) {
            if (layout instanceof WalLayoutVVastbase) {
                return VAST_KINDS.clone();
            }
            return KINDS.clone();
        }

        /**
         * 值编码<strong>读区</strong>长度（版本感知档，vastbase T3）：
         * VastBase=184（= {@link #READ_REGION_LEN} + 64）——数据区各列较 OG 几何
         * 整体 +64（relname 128B 加宽的净效果，64 是 8 的倍数使全部对齐不变）：
         * relname 128B@0 + parttype 1B@128 + 垫 3 + parentid u32@132 + rangenum@136 +
         * intervalnum@140 + partstrategy@144 + 垫 3 + relfilenode u32@148 +
         * reltablespace@152（0 占位）+ 垫 4 + relpages float8@160（0 占位）+
         * reltuples float8@168（0 占位）+ relallvisible@176（0 占位）+
         * reltoastrelid u32@180——变长段不可值编码，prefix 越过 184 落 rawTail
         * splice。其余档返回 OG 几何 120（vanilla 无此表，返回值不被消费）。
         *
         * @param layout 版本布局描述符（WalLayoutVVastbase 决定读区几何）
         * @return 读区字节数（OG=120，VastBase=184）
         */
        public static int readRegionLen(WalLayout layout) {
            return layout instanceof WalLayoutVVastbase ? READ_REGION_LEN + 64 : READ_REGION_LEN;
        }

        /**
         * 把 pg_partition 解码值行投影为行模型。
         *
         * <p>关键步骤与异常语义同 {@link AttrRow#fromDecoded(Object[], WalLayout)}
         * （虚拟头 oid 前导槽使 attnum n 的值行槽位恰为 n；parttype 词典 kind 为
         * "char"——解码为左引号单字符（{@code 'p}），投影经
         * {@link ClassRow#stripCharQuotes} 剥引号收敛为裸字符，与 JDBC/引导种子的
         * {@code getString} 形态一致，record equals 两侧同锚）。线程约束：静态纯
         * 函数，并发安全。</p>
         *
         * @param vals 解码值行（与 {@link #kinds()} 同长）
         * @return 行模型
         */
        public static PartitionRow fromDecoded(Object[] vals) {
            return new PartitionRow(
                    ((Number) vals[IX_PARTOID]).longValue(),
                    (String) vals[IX_RELNAME],
                    ClassRow.stripCharQuotes(vals[IX_PARTTYPE]),
                    ((Number) vals[IX_PARENTID]).longValue(),
                    ((Number) vals[IX_RELFILENODE]).longValue(),
                    ((Number) vals[IX_RELTOASTRELID]).longValue());
        }

        /**
         * 编码<strong>读区</strong>的数据区字节（值编码——<b>OG 5.0.0 几何</b>，语义同
         * {@link ClassRow#encodeReadRegion(WalLayout)} 的 partition 面，布局见
         * {@link #READ_REGION_LEN} javadoc）。消费面：截断更新的 prefix 重编码与
         * 后缀读区回填。边界与异常语义：relname 超 63B 截断不抛（防御）；
         * 不投影的定宽列（reltablespace/relpages/reltuples/relallvisible）以 0
         * 占位——仅重建几何需要；线程约束：纯函数，并发安全。
         *
         * @return {@value #READ_REGION_LEN} 字节数据区读区编码
         */
        public byte[] encodeReadRegion() {
            ByteArrayOutputStream out = new ByteArrayOutputStream(READ_REGION_LEN);
            byte[] n = relname.getBytes(StandardCharsets.UTF_8);
            out.write(n, 0, Math.min(63, n.length));
            for (int i = Math.min(63, n.length); i < 64; i++) {
                out.write(0);   // NUL 尾垫
            }
            out.write(parttype == null || parttype.isEmpty() ? 0 : (byte) parttype.charAt(0));   // parttype@64
            out.write(new byte[3], 0, 3);   // [65,68) 垫
            ClassRow.writeU32(out, parentid);        // parentid@68
            out.write(new byte[12], 0, 12); // [72,84) rangenum/intervalnum/partstrategy/垫：0 占位（不投影）
            ClassRow.writeU32(out, relfilenode);     // relfilenode@84
            out.write(new byte[28], 0, 28); // [88,116) 垫 4 + relpages/reltuples float8 + relallvisible：0 占位
            ClassRow.writeU32(out, reltoastrelid);   // reltoastrelid@116
            return out.toByteArray();
        }

        /**
         * 编码<strong>读区</strong>的数据区字节（版本感知档，vastbase T3）：
         * VastBase 分支 = OG 几何逐段 +64（布局见 {@link #readRegionLen(WalLayout)}
         * javadoc——relname 128B 定宽 + NUL 尾垫、parttype@128、parentid@132、
         * relfilenode@148、reltoastrelid@180，合计 184B）；其余档委派
         * {@link #encodeReadRegion()}。边界与异常语义同 OG 档（relname 超 127B
         * 截断不抛——防御）；线程约束：纯函数，并发安全。
         *
         * @param layout 版本布局描述符（WalLayoutVVastbase 决定读区几何与 name 宽度）
         * @return 读区字节数据区编码（OG=120 / VastBase=184）
         */
        public byte[] encodeReadRegion(WalLayout layout) {
            if (!(layout instanceof WalLayoutVVastbase)) {
                return encodeReadRegion();
            }
            int nameWidth = layout.nameDatumWidth();
            ByteArrayOutputStream out = new ByteArrayOutputStream(readRegionLen(layout));
            byte[] n = relname.getBytes(StandardCharsets.UTF_8);
            out.write(n, 0, Math.min(nameWidth - 1, n.length));
            for (int i = Math.min(nameWidth - 1, n.length); i < nameWidth; i++) {
                out.write(0);   // NUL 尾垫
            }
            out.write(parttype == null || parttype.isEmpty() ? 0 : (byte) parttype.charAt(0));   // parttype@128
            out.write(new byte[3], 0, 3);   // [129,132) 垫
            ClassRow.writeU32(out, parentid);        // parentid@132
            out.write(new byte[12], 0, 12); // [136,148) rangenum/intervalnum/partstrategy/垫：0 占位（不投影）
            ClassRow.writeU32(out, relfilenode);     // relfilenode@148
            out.write(new byte[28], 0, 28); // [152,180) 垫 4 + relpages/reltuples float8 + relallvisible：0 占位
            ClassRow.writeU32(out, reltoastrelid);   // reltoastrelid@180
            return out.toByteArray();
        }
    }
}
