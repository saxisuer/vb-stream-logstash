package org.vastdata.vbstream.walsource.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.TupleDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * TOAST 重组——external 短 varlena 指针的原值重建：chunk 采集（WAL 流内）+ external
 * 拼装 + pglz 解压 + 窗口前指针 JDBC 回查兜底，是 v2 DML 值面的宽值出口。
 *
 * <p><b>external 指针布局（18B 短 varlena，REL_18_STABLE {@code src/include/varatt.h}
 * L32-39 + 2026-10-07 docker PG 18.6 容器实测逐字节钉）</b>：</p>
 * <pre>
 * [0x01][0x12 VARTAG_ONDISK=18][va_rawsize u32 LE][va_extinfo u32 LE][va_valueid u32 LE][va_toastrelid u32 LE]
 * </pre>
 * <ul>
 *   <li>rawsize = 原始 varlena 总长（<b>含 4B 头</b>，即载荷+4）——实测压缩样本
 *       （38400 字节载荷）rawsize=38404；</li>
 *   <li>extinfo = extsize（外部存储字节数，== chunk 拼接总长）低 30 位 | 压缩方法
 *       高 2 位（{@code extinfo>>30}：0=pglz、1=lz4——lz4 实测样本 extinfo=0x40004E8A
 *       方法位=01）；压缩判定 {@code extsize < rawsize-4}（varatt.h L354-356，
 *       VARATT_EXTERNAL_IS_COMPRESSED——"we never use compression unless it actually
 *       saves space"，等号即未压缩）；</li>
 *   <li>valueid = toast 表内该值的唯一 oid（toast 表 chunk_id 列的值）；toastrelid =
 *       toast 关系 oid（注意：<b>toast 表名 {@code pg_toast_<主表oid>} 的后缀不是它</b>，
 *       见 {@link ToastProbe}）。</li>
 * </ul>
 *
 * <p><b>chunk 布局（实测钉）</b>：TOAST_MAX_CHUNK_SIZE=1996（10×1996+余量）；未压缩值
 * 的 chunk_data 拼接即原载荷（无前缀，实测 chunk0 首 4B "da9b" 即 md5 文本）；压缩值
 * 的 chunk 拼接整体 = <b>[u32 LE tcinfo = (rawsize-4) 低 30 位 | 方法&lt;&lt;30][pglz 流]</b>
 * ——4B 前缀是唯一头（落 chunk0 内，可被块边界切开），实测 pglz 样本 chunk0 首 4B
 * {@code 00960000}（LE 38400）、lz4 样本 {@code 00960040}（0x40009600）；两形态拼接
 * 总长均 == extsize。</p>
 *
 * <p><b>完整性判定、unchanged 对齐与回查兜底（Task 10 值面裁定，REL_18 源码钉）</b>：
 * chunk 按 valueid 归集 {@code TreeMap<seq,byte[]>}，完整当且仅当拼接总长 == extsize。
 * 三分支：</p>
 * <ul>
 *   <li><b>归集面空 → 返回 {@link #UNCHANGED_TOAST_MARKER}</b>（值不在窗口，不回查）
 *   ——对齐 engine 的 pgoutput wire 契约：'u'（LOGICALREP_COLUMN_UNCHANGED，proto.c
 *   L812-819——"Unchanged toasted datum ... a cheap check to avoid sending large
 *   values unnecessarily"，VARATT_IS_EXTERNAL_ONDISK 的列不发值）。服务端只有
 *   reorder buffer 的本事务 toast hash 命中才内联替换（reorderbuffer.c
 *   ReorderBufferToastReplace 按 valueid 查 {@code txn->toast_hash}），而
 *   <b>每条已施加的用户表变更后 hash 即重置</b>（decode.c 对 INSERT/UPDATE/DELETE/
 *   MULTI_INSERT 末行置 {@code clear_toast_afterwards=true} →
 *   ReorderBufferToastReset）——故"未变列指针"（同事务早先行消费过、或跨事务/重启
 *   窗口前）在 engine 侧恒为 'u'，wal 侧归集面空（{@code XactGrouper} 同式逐行清窗）
 *   恰同形，回查反而会产出 engine 没有的值、破坏双路对拍；</li>
 *   <li><b>归集面非空但不完整 → {@link ToastProbe} 回查合并</b>——probe 的对拍角色
 *   收窄为"补全窗口内的新写值缺口"（如 FPI 镜像形态的 chunk 记录采集面跳过后留下的
 *   部分归集——服务端解码面不受 FPI 形态影响、会发值，wal 侧须回查补齐）；</li>
 *   <li><b>回查后仍缺或 probe 抛异常 → {@code toast-unavailable} + WARN</b>（含
 *   valueid/toastrelid 上下文），<b>不 fail 整条流</b>（v2 设计 §5）。</li>
 * </ul>
 * <p>另注（旧元组免重组依据）：REPLICA IDENTITY FULL 的旧元组在 WAL 写入时已被
 * 服务端 {@code toast_flatten_tuple} 拍扁内联（heapam.c ExtractReplicaIdentity——
 * "When logging the entire old tuple, it very well could contain toasted columns.
 * If so, force them to be inlined"），外部指针不会出现在前像；压缩 external 拍扁后
 * 是行内压缩 varlena，走 {@link #decompressInlineCompressed} 同一面。压缩方法面：
 * pglz（方法位 0）经 {@link Pglz}、lz4（方法位 1，TOAST_LZ4_COMPRESSION_ID）经
 * {@link Lz4}（lz4-java 裸 block 解压）分派解压，两路契约同形（定长期望、损坏
 * ISE fail-fast）；未知方法位（varatt.h 未定义保留值）ISE——走读错位信号。</p>
 *
 * <p>线程约束：<b>单写者</b>（wal-receiver 线程：onChunkRow 与 resolveExternal 同线程
 * 调用）——内部 HashMap/HashSet 非线程安全，与 v1 replay 组件同假设。{@link #clear()}
 * 两处调用面：事务终态淘汰（aborted/已消费 chunk 防滞留）+ <b>逐行清窗</b>
 * （{@code XactGrouper} 在每条用户表行施加后调用——镜像服务端 clear_toast_afterwards
 * 的 hash 重置，见上）。{@link #resolveExternal} 返回 <b>byte[]</b>（Task 8
 * 保真裁定）——文本族由 {@code TupleDecoder} 解码 UTF-8、numeric/bytea 等非文本面
 * 字节直达渲染矩阵，不经 String 有损往返。</p>
 */
public final class ToastAssembler implements TupleDecoder.VarlenaResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ToastAssembler.class);

    /**
     * unchanged-TOAST 的渲染字面（engine {@code ConsoleRenderer} 的
     * {@code TupleValue.UnchangedToast} 分支同形——pgoutput 'u' 列的输出契约）。
     */
    public static final String UNCHANGED_TOAST_TEXT = "<toast-unchanged>";

    /**
     * <b>unchanged-TOAST 哨兵</b>（Task 10 值面裁定）——resolveExternal 在归集面空
     * （值不在当前窗口：未变列指针/跨事务/重启窗口前）时的返回，字节内容即
     * {@link #UNCHANGED_TOAST_TEXT} 的 UTF-8 形态（对齐 engine ConsoleRenderer 对
     * pgoutput 'u' 列的渲染字面）。
     *
     * <p><b>双路径交付设计</b>：text 族列经 {@code TupleDecoder.readTextDatum} 的
     * {@code new String(字节)} 交付——哨兵字节自然解码为渲染字面（与真值恰为该字面
     * 的输出形态天然全等，无歧义成本）；bytea/numeric 族列经字节直达——渲染层
     * （{@code XactGrouper.renderRow}）以<b>身份比较</b>（{@code ==}）识别本实例后
     * 改渲染字面（否则会被 bytea 的 hex 矩阵消费成错值）。真实值经 pglz/chunk 拼装
     * 产出，与共享哨兵实例无身份碰撞。</p>
     */
    public static final byte[] UNCHANGED_TOAST_MARKER =
            UNCHANGED_TOAST_TEXT.getBytes(StandardCharsets.UTF_8);

    /** 短外部 varlena 首 byte（VARATT_IS_1B_E：恰 0x01）。 */
    private static final int TAG_1B_EXTERNAL = 0x01;

    /** 外部指针 vartag（varatt.h L89：VARTAG_ONDISK=18=0x12）。 */
    private static final int VARTAG_ONDISK = 18;

    /** external 指针总长（1B 头 + 1B vartag + 4×u32）。 */
    private static final int POINTER_SIZE = 18;

    /** extinfo 的 extsize 位掩码（varatt.h L45-46：VARLENA_EXTSIZE_BITS=30）。 */
    private static final int EXTSIZE_MASK = 0x3FFFFFFF;

    /** extinfo 的方法位右移量（高 2 位）。 */
    private static final int EXTSIZE_BITS = 30;

    /** 压缩方法码：pglz（varatt.h 断言面 TOAST_PGLZ_COMPRESSION_ID）。 */
    private static final int METHOD_PGLZ = 0;

    /** 压缩方法码：lz4（TOAST_LZ4_COMPRESSION_ID=1，实测 lz4 样本方法位=01）。 */
    private static final int METHOD_LZ4 = 1;

    /** 回查仍缺的降级字面（v2 设计 §5：不 fail 整条流）。 */
    private static final String TOAST_UNAVAILABLE = "toast-unavailable";

    /** 降级字面的字节形态（{@link #resolveExternal} 的返回契约是 byte[]——渲染层按需解码）。 */
    private static final byte[] TOAST_UNAVAILABLE_BYTES =
            TOAST_UNAVAILABLE.getBytes(StandardCharsets.UTF_8);

    private final ToastProbe probe;

    /** chunk 归集：toast 关系键（WAL relfilenode 或 oid）→ valueid → seq → 字节。 */
    private final Map<Long, Map<Long, TreeMap<Integer, byte[]>>> chunksByToast = new HashMap<>();

    /**
     * 构造重组器。
     *
     * @param probe JDBC 回查兜底（null = 回查禁用——缺 chunk 直接降级
     *              {@code toast-unavailable}，适用于无 SQL 会话的纯回放形态）
     */
    public ToastAssembler(ToastProbe probe) {
        this.probe = probe;
    }

    /**
     * 采集一条 toast chunk 行——toast 关系 heap 记录经 v1 {@code TupleDecoder}
     * （词典三列 oid/int4/bytea）解出的值行按 valueid/seq 归集。
     *
     * <p>关键步骤：行形校验（Long/Integer/byte[] 三列，形不符 ISE fail-fast——走读
     * 错位信号）→ 按 toast 关系键取 valueid 字典 → TreeMap 按 seq 落位（乱序到达
     * 自然有序，重复 seq 后到覆盖）。边界与异常语义：chunkRow 为 null / 列数不足 /
     * 列类型不符抛 ISE；数据列零长合法（空值 chunk）。线程约束：单写者
     * （wal-receiver 线程）。</p>
     *
     * @param toastRelfilenode toast 关系的 WAL 记录 relfilenode（与关系 oid 无重写时
     *                         相等；分叉时 resolveExternal 走 valueid 兜底扫描）
     * @param chunkRow         三列值行：[Long chunk_id, Integer chunk_seq, byte[] chunk_data]
     * @throws IllegalStateException 行形态不符
     */
    public void onChunkRow(long toastRelfilenode, Object[] chunkRow) {
        if (chunkRow == null || chunkRow.length < 3
                || !(chunkRow[0] instanceof Long valueid)
                || !(chunkRow[1] instanceof Integer seq)
                || !(chunkRow[2] instanceof byte[] data)) {
            throw new IllegalStateException("toast chunk 行形态不符: "
                    + (chunkRow == null ? "null" : Arrays.toString(chunkRow)));
        }
        chunksByToast.computeIfAbsent(toastRelfilenode, k -> new HashMap<>())
                .computeIfAbsent(valueid, k -> new TreeMap<>())
                .put(seq, data);
    }

    /**
     * 剥 18B external 指针并重建原值<b>字节</b>——拼装 chunk、按需回查、pglz 解压，
     * <b>不做 UTF-8 解码</b>（Task 8 保真裁定：返回 byte[]，文本/字节解释权归渲染层——
     * numeric/bytea 不再经 String UTF-8 有损往返）。
     *
     * <p>关键步骤：①指针头校验（0x01/0x12 + 越界，形不符 ISE）→ 解 rawsize/extinfo/
     * valueid/toastrelid；②chunk 归集（键 = toastrelid，未命中再按 valueid 全域兜底
     * 扫描——主表重写后 relfilenode 与 oid 分叉的容错）；③<b>归集面空 → 返回
     * {@link #UNCHANGED_TOAST_MARKER}</b>（Task 10 值面裁定：值不在窗口——未变列指针
     * /跨事务/重启窗口前，engine 同形发 'u'，不回查；<b>空面判定先于方法位检查</b>
     * ——lz4 列的未变列指针同走哨兵，先查方法位会把该形态错杀成 ISE；依据与三分支
     * 语义见类 javadoc）；④非空时方法位判定——未知方法码（非 pglz/lz4 的保留值）
     * ISE fail-fast（走读错位信号）；⑤拼接总长 != extsize → probe 回查合并后复检，
     * 仍缺 → 降级 {@code toast-unavailable}；⑥未压缩（extsize == rawsize-4）拼接
     * 即载荷；压缩（&lt;）校验 chunk0 前 4B tcinfo 与指针一致（(rawsize-4)|方法&lt;&lt;30，
     * 不符 ISE）后剥 4B 经 {@link #inflate} 按方法位分派（pglz/lz4）解至 rawsize-4
     * 字节。</p>
     *
     * <p>边界与异常语义：src 过短/tag 不符/tcinfo 不符/pglz 数据损坏抛 ISE；归集面
     * 非空且回查（probe 为 null、查无、抛异常）仍不齐 → WARN（valueid/toastrelid
     * 上下文）并返回降级字面 {@code toast-unavailable} 的 UTF-8 字节（不 fail 流）。
     * 线程约束：单写者（与 {@link #onChunkRow} 同线程调用）。</p>
     *
     * @param src 完整源缓冲（记录/页字节，契约只读）
     * @param off 指针起点（external 指针是 1B 头短 varlena——<b>无 4 对齐保证</b>，
     *            按 {@code TupleDecoder.varlenaStart} 的落位判定直接字节起读）
     * @return 拼装解压后的原值字节；归集面空时 {@link #UNCHANGED_TOAST_MARKER}；
     *         窗口内缺口回查不可得时降级字节的 UTF-8 字节
     * @throws IllegalArgumentException src 为 null 或 off 越界
     * @throws IllegalStateException    指针形态/未知方法码/tcinfo/压缩流数据不符
     */
    public byte[] resolveExternal(byte[] src, int off) {
        if (src == null) {
            throw new IllegalArgumentException("source buffer must not be null");
        }
        if (off < 0 || off + POINTER_SIZE > src.length) {
            throw new IllegalArgumentException("external pointer out of bounds: off=" + off
                    + ", buffer=" + src.length);
        }
        if ((src[off] & 0xFF) != TAG_1B_EXTERNAL || (src[off + 1] & 0xFF) != VARTAG_ONDISK) {
            throw new IllegalStateException("不是 ondisk external 指针: hdr="
                    + (src[off] & 0xFF) + ", vartag=" + (src[off + 1] & 0xFF));
        }
        long rawsize = u32le(src, off + 2);
        long extinfo = u32le(src, off + 6);
        long valueid = u32le(src, off + 10);
        long toastrelid = u32le(src, off + 14);
        long extsize = extinfo & EXTSIZE_MASK;
        int method = (int) (extinfo >>> EXTSIZE_BITS);
        TreeMap<Integer, byte[]> collected = collectChunks(toastrelid, valueid);
        if (collected.isEmpty()) {
            // Task 10 值面裁定：归集面空 = 值不在当前窗口（未变列指针/跨事务/重启窗口前）
            // ——engine 的 pgoutput 同形发 'u'（proto.c LOGICALREP_COLUMN_UNCHANGED），
            // 回查反而会产出 engine 没有的值、破坏双路对拍；渲染层按 UNCHANGED_TOAST_TEXT 输出。
            // 空面判定先于方法位检查（Task 13 顺手修）：lz4 列的未变列指针同走哨兵——
            // 值不在窗口即无需解压，engine 同形发 'u'，先查方法位会把该形态错杀成 ISE
            return UNCHANGED_TOAST_MARKER;
        }
        if (method != METHOD_PGLZ && method != METHOD_LZ4) {
            throw new IllegalStateException("未知 TOAST 压缩方法码 " + method + ": valueid=" + valueid
                    + ", toastrelid=" + toastrelid);
        }
        RuntimeException probeFailure = null;
        if (totalSize(collected) != extsize && probe != null) {
            probeFailure = mergeFetched(toastrelid, valueid, collected);
        }
        if (totalSize(collected) != extsize) {
            // 单 WARN 点：回查失败携带异常堆栈，查无/仍缺打归集面计数
            if (probeFailure != null) {
                LOG.warn("TOAST chunk JDBC 回查失败，值降级为 toast-unavailable: valueid={}, "
                        + "toastrelid={}, extsize={}, rawsize={}", valueid, toastrelid, extsize, rawsize, probeFailure);
            } else {
                LOG.warn("TOAST chunk 不完整（回查后仍缺），值降级为 toast-unavailable: valueid={}, "
                        + "toastrelid={}, extsize={}, rawsize={}, 已归集块数={}", valueid, toastrelid, extsize,
                        rawsize, collected.size());
            }
            return TOAST_UNAVAILABLE_BYTES;
        }
        byte[] concat = concat(collected, (int) extsize);
        if (extsize < rawsize - 4) {
            if (concat.length < 4) {
                throw new IllegalStateException("压缩 TOAST 拼接缺 tcinfo 头: valueid=" + valueid);
            }
            int tcinfo = (int) u32le(concat, 0);
            int expected = (int) ((rawsize - 4) | ((long) method << EXTSIZE_BITS));
            if (tcinfo != expected) {
                throw new IllegalStateException("压缩 TOAST 头与指针不一致: tcinfo=" + tcinfo
                        + ", 期望=" + expected + ", valueid=" + valueid);
            }
            byte[] stream = new byte[concat.length - 4];
            System.arraycopy(concat, 4, stream, 0, stream.length);
            return inflate(method, stream, (int) (rawsize - 4));
        }
        return concat;
    }

    /**
     * 压缩流解压的公共分派面（external 拼接体与行内压缩 varlena 共用——两形态的
     * 压缩流同源，均是 tuptoaster.c 压缩产出面的裸流）。
     *
     * <p>关键步骤：按方法码分派——pglz（0）走 {@link Pglz}（PG 源码纯移植）、
     * lz4（1）走 {@link Lz4}（liblz4 裸 block，lz4-java 实现）；两者契约同形
     * （定长期望、损坏 ISE fail-fast）。边界与异常语义：未知方法码（varatt.h
     * 未定义的保留值）抛 ISE——走读错位/格式演进信号，绝不静默降级。线程约束：
     * 纯分派无共享状态。</p>
     *
     * @param method      压缩方法码（extinfo/tcinfo 高 2 位）
     * @param stream      纯压缩流（4B tcinfo 头已剥）
     * @param expectedLen 解压结果的期望字节数（tcinfo/指针声明的原长）
     * @return 长度恰为 expectedLen 的解压字节
     * @throws IllegalStateException 未知方法码或压缩流损坏
     */
    private static byte[] inflate(int method, byte[] stream, int expectedLen) {
        return switch (method) {
            case METHOD_PGLZ -> Pglz.decompress(stream, expectedLen);
            case METHOD_LZ4 -> Lz4.decompress(stream, expectedLen);
            default -> throw new IllegalStateException("未知 TOAST 压缩方法码 " + method);
        };
    }

    /**
     * 行内压缩 varlena 的解压面（{@link TupleDecoder.VarlenaResolver} 契约，Task 7
     * 行内接线）——payload 即剥去 4B varlena 头的
     * {@code [u32 tcinfo = exhdrlen 低 30 位 | 方法&lt;&lt;30][压缩流]}，与 external
     * 压缩值的 chunk 拼接体<b>同构</b>（同一压缩产出面，2026-10-07 实测：64000 字符
     * repeat 型宽值压缩至 779B 行内落盘、头上 30 位承载总长）。
     *
     * <p>关键步骤：载荷 &lt;4B 抛 ISE（缺 tcinfo 头）；剥 4B tcinfo 后经
     * {@link #inflate} 按方法位分派（pglz/lz4）解至低 30 位声明的原长。边界与
     * 异常语义：tcinfo/压缩流不符抛 ISE（走读错位信号）；与
     * {@link #resolveExternal} 不同，本面无降级形态（行内数据自包含，缺值不可能
     * 发生）。线程约束：单写者（与解码同线程）。</p>
     *
     * @param payload 行内压缩载荷（4B varlena 头已剥，≥4B 才含 tcinfo）
     * @return 解压后的原载荷字节
     * @throws IllegalStateException tcinfo 头缺失/未知方法码/压缩流损坏
     */
    @Override
    public byte[] decompressInlineCompressed(byte[] payload) {
        if (payload.length < 4) {
            throw new IllegalStateException("行内压缩 varlena 缺 tcinfo 头: 载荷 " + payload.length + "B");
        }
        int tcinfo = (int) u32le(payload, 0);
        int rawLen = tcinfo & EXTSIZE_MASK;
        int method = tcinfo >>> EXTSIZE_BITS;
        byte[] stream = new byte[payload.length - 4];
        System.arraycopy(payload, 4, stream, 0, stream.length);
        return inflate(method, stream, rawLen);
    }

    /**
     * 全清 chunk 归集面——两个调用面共用（2026-10 控制器裁定：全清是最简可靠形态）：
     * ①<b>逐行清窗</b>（Task 10）：{@code XactGrouper} 在每条用户表行施加后调用——
     * 镜像服务端 reorder buffer 的 toast hash 重置（decode.c 对 INSERT/UPDATE/DELETE/
     * MULTI_INSERT 末行置 {@code clear_toast_afterwards=true} → ReorderBufferToastReset），
     * 使"未变列指针"（本事务早先行消费过/跨事务）在 resolveExternal 归集面空 →
     * {@link #UNCHANGED_TOAST_MARKER}，与 engine 的 pgoutput 'u' 同形；②<b>事务终态
     * 淘汰</b>（COMMIT/ABORT/PREPARE/两阶段确认后）：防 aborted/已消费 chunk 无限滞留。
     *
     * <p>全清安全性依据：<b>valueid 全局唯一</b>——valueid 是 TOAST 写路径经 oid 计数器
     * 分配的新 oid（tuptoaster.c toast_save_datum 的 GetNewOid），跨事务不复用（计数器
     * 单调，wraparound 需 4G 个值后才发生且 PG 侧同面对待），故清空不会使后续到达的
     * chunk 与残留归集语义错位。调用时机语义：清窗 <b>早于</b> 行引用解码的场景
     * （未变列指针/交错事务的终态夹在 chunk 写入与引用行到达之间）按归集面空走
     * {@link #UNCHANGED_TOAST_MARKER}（engine 同形）；已知限制：与服务端按事务分 hash
     * 的重置不同，本清窗是全局的——交错事务 B 的行夹在 A 的 chunk 写入与 A 的引用行
     * 之间时 A 的窗口被清（服务端 A 的 hash 不受 B 影响），A 行多见一个 'u' 占位，
     * 属对拍面已知分叉（v3 记档：per-txn 窗口）。线程约束：单写者
     * （与 {@link #onChunkRow} 同线程）。</p>
     */
    public void clear() {
        chunksByToast.clear();
    }

    /**
     * 取指定值的 chunk 归集（含 relfilenode/oid 分叉的 valueid 兜底扫描）。
     *
     * <p>关键步骤：先按指针 toastrelid 直查（无重写时归集键即 oid）；未命中再遍历
     * 全部关系键按 valueid 找——主表重写（VACUUM FULL/CLUSTER）后 toast relfilenode
     * != oid 而 chunk 以 relfilenode 归集，窗口内 valueid 全局唯一（oid 计数器单调）
     * 使兜底命中无歧义。未找到返回空 TreeMap（调用方按缺失继续回查路径）。</p>
     */
    private TreeMap<Integer, byte[]> collectChunks(long toastrelid, long valueid) {
        Map<Long, TreeMap<Integer, byte[]>> byValue = chunksByToast.get(toastrelid);
        if (byValue != null) {
            TreeMap<Integer, byte[]> direct = byValue.get(valueid);
            if (direct != null) {
                return direct;
            }
        }
        for (Map<Long, TreeMap<Integer, byte[]>> candidate : chunksByToast.values()) {
            TreeMap<Integer, byte[]> found = candidate.get(valueid);
            if (found != null) {
                return found;
            }
        }
        return new TreeMap<>();
    }

    /**
     * 回查兜底并合并到已归集面（就地突变）。
     *
     * <p>关键步骤：回查键 = 指针的 toastrelid/valueid；回查行按 seq 并入归集
     * （后到覆盖——回查是 JDBC 末态，权威于 WAL 窗口内的残缺面）；空结果为 no-op
     * （"查无"语义交调用方按不完整降级）。边界与异常语义：任何 RuntimeException
     * （含 JdbcToastProbe 包装的 SQLException）都吞掉并<b>作为返回值</b>交调用方进
     * 单 WARN 点——v2 设计"回查失败不 fail 整条流"；成功/查无返回 null。
     * 线程约束：单写者。</p>
     */
    private RuntimeException mergeFetched(long toastrelid, long valueid, TreeMap<Integer, byte[]> collected) {
        Map<Long, byte[]> fetched;
        try {
            fetched = probe.fetchChunks(toastrelid, valueid);
        } catch (RuntimeException e) {
            return e;
        }
        if (fetched == null) {
            return null;
        }
        for (Map.Entry<Long, byte[]> entry : fetched.entrySet()) {
            collected.put(entry.getKey().intValue(), entry.getValue());
        }
        return null;
    }

    /**
     * 归集块的字节总数（完整性判据：== extsize）。
     */
    private static int totalSize(TreeMap<Integer, byte[]> chunks) {
        int total = 0;
        for (byte[] data : chunks.values()) {
            total += data.length;
        }
        return total;
    }

    /**
     * 按 seq 序拼接全部块到给定长度的数组（调用方已校验总长匹配）。
     */
    private static byte[] concat(TreeMap<Integer, byte[]> chunks, int extsize) {
        byte[] out = new byte[extsize];
        int pos = 0;
        for (byte[] data : chunks.values()) {
            System.arraycopy(data, 0, out, pos, data.length);
            pos += data.length;
        }
        return out;
    }

    /**
     * 就地读 little-endian u32（external 指针四字段与 tcinfo 均 LE——指针实测锚）。
     */
    private static long u32le(byte[] b, int o) {
        return (b[o] & 0xFFL) | ((b[o + 1] & 0xFFL) << 8) | ((b[o + 2] & 0xFFL) << 16) | ((b[o + 3] & 0xFFL) << 24);
    }
}
