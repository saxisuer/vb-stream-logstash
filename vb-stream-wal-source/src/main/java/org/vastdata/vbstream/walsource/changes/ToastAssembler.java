package org.vastdata.vbstream.walsource.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
 * <p><b>完整性判定与回查兜底</b>：chunk 按 valueid 归集 {@code TreeMap<seq,byte[]>}，
 * 完整当且仅当拼接总长 == extsize（缺任何一块必然短）；缺 → {@link ToastProbe}
 * 回查（窗口前指针：重启续传后 UPDATE 未变 TOAST 列指向窗口之前的 chunk）→ 仍缺
 * 或 probe 抛异常 → 返回 {@code toast-unavailable} + WARN（含 valueid/toastrelid
 * 上下文），<b>不 fail 整条流</b>（v2 设计 §5）。lz4 双防线：字典面
 * {@link #lz4Guard} 启动期 WARN 一次；运行期撞方法位 1 的 external → ISE fail-fast
 * （本实现无 lz4 解压面，静默降级会产出错值）。</p>
 *
 * <p>线程约束：<b>单写者</b>（wal-receiver 线程：onChunkRow 与 resolveExternal 同线程
 * 调用）——内部 HashMap/HashSet 非线程安全，与 v1 replay 组件同假设；已知限制：
 * 归集的 chunk 无淘汰面（aborted/删除值的 chunk 会滞留），生命周期管理落在
 * Task 5 的 XactGrouper 接线；bytea 等 TOAST 宽值经 UTF-8 {@link String} 交付对
 * 非文本类型有损（首发矩阵 TOAST 面只覆盖文本族）。</p>
 */
public final class ToastAssembler {

    private static final Logger LOG = LoggerFactory.getLogger(ToastAssembler.class);

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

    /** pg_attribute.attcompression 的 lz4 列标记字节（"char" 'l'）。 */
    private static final byte ATTCOMPRESSION_LZ4 = 'l';

    /** 回查仍缺的降级字面（v2 设计 §5：不 fail 整条流）。 */
    private static final String TOAST_UNAVAILABLE = "toast-unavailable";

    private final ToastProbe probe;

    /** chunk 归集：toast 关系键（WAL relfilenode 或 oid）→ valueid → seq → 字节。 */
    private final Map<Long, Map<Long, TreeMap<Integer, byte[]>>> chunksByToast = new HashMap<>();

    /** 已 WARN 过 lz4 列的关系名（观测节流，每关系一次）。 */
    private final Set<String> lz4WarnedRels = new HashSet<>();

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
     * 剥 18B external 指针并重建原值文本——拼装 chunk、按需回查、pglz 解压、UTF-8 解码。
     *
     * <p>关键步骤：①指针头校验（0x01/0x12 + 越界，形不符 ISE）→ 解 rawsize/extinfo/
     * valueid/toastrelid；②方法位判定——lz4 ISE fail-fast（无解压面，静默只会产出
     * 错值）；③chunk 归集（键 = toastrelid，未命中再按 valueid 全域兜底扫描——主表
     * 重写后 relfilenode 与 oid 分叉的容错）；④拼接总长 != extsize → probe 回查合并
     * 后复检；⑤未压缩（extsize == rawsize-4）拼接即载荷；压缩（&lt;）校验 chunk0 前
     * 4B tcinfo 与指针一致（(rawsize-4)|方法&lt;&lt;30，不符 ISE）后剥 4B 走
     * {@link Pglz#decompress} 解至 rawsize-4 字节；⑥UTF-8 → String。</p>
     *
     * <p>边界与异常语义：src 过短/tag 不符/tcinfo 不符/pglz 数据损坏抛 ISE；chunk
     * 缺失且回查（probe 为 null、查无、抛异常）仍不齐 → WARN（valueid/toastrelid
     * 上下文）并返回 {@code toast-unavailable}（不 fail 流）；extsize==0 返回空串。
     * 线程约束：单写者（与 {@link #onChunkRow} 同线程调用）。</p>
     *
     * @param src 完整源缓冲（记录/页字节，契约只读）
     * @param off 指针起点（已对齐）
     * @return 拼装解压后的原文文本（UTF-8）；缺 chunk 不可得时 {@code toast-unavailable}
     * @throws IllegalArgumentException src 为 null 或 off 越界
     * @throws IllegalStateException    指针形态/lz4 方法/tcinfo/pglz 数据不符
     */
    public String resolveExternal(byte[] src, int off) {
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
        if (method != METHOD_PGLZ) {
            throw new IllegalStateException(method == METHOD_LZ4
                    ? "lz4 压缩的 external TOAST 值不受支持（无 lz4 解压面）: valueid=" + valueid
                            + ", toastrelid=" + toastrelid
                    : "未知 TOAST 压缩方法码 " + method + ": valueid=" + valueid
                            + ", toastrelid=" + toastrelid);
        }
        TreeMap<Integer, byte[]> collected = collectChunks(toastrelid, valueid);
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
            return TOAST_UNAVAILABLE;
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
            byte[] pglz = new byte[concat.length - 4];
            System.arraycopy(concat, 4, pglz, 0, pglz.length);
            byte[] raw = Pglz.decompress(pglz, (int) (rawsize - 4));
            return new String(raw, StandardCharsets.UTF_8);
        }
        return new String(concat, StandardCharsets.UTF_8);
    }

    /**
     * lz4 列的字典面提前告警——{@code pg_attribute.attcompression} 为 'l' 的列在
     * 撞上运行期 fail-fast 之前先留观测痕迹（每关系 WARN 一次，防大事务刷屏）。
     *
     * <p>关键步骤：'l' 且关系名首次出现才打 WARN；'p'（pglz）与其他值 no-op。
     * 边界与异常语义：relName 为 null 抛 NPE（告警去重键要素）。线程约束：单写者
     * （与字典装配同线程）。</p>
     *
     * @param attcompression pg_attribute.attcompression（'p'=pglz、'l'=lz4）
     * @param relName        关系名（告警去重键与上下文）
     */
    public void lz4Guard(byte attcompression, String relName) {
        if (attcompression == ATTCOMPRESSION_LZ4 && lz4WarnedRels.add(relName)) {
            LOG.warn("列压缩方法为 lz4 的关系: {}——运行期撞 lz4 external TOAST 将 fail-fast", relName);
        }
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
