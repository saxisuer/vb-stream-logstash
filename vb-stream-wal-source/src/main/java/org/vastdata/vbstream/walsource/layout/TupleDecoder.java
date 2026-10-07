package org.vastdata.vbstream.walsource.layout;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * 磁盘格式 heap tuple 解码器——按逐列 kind 词典（catalog 重放产物，见
 * {@link WalLayout#pgAttributeKinds()} 等）把 WAL 载荷 / 页镜像 / multi-insert
 * entry 中的 tuple 字节解为 {@code Object[]} 值行。
 *
 * <p>三入口共享同一核心走读（{@link #decodeTupleData}，spike {@code decodeTupleData}
 * 移植）：datum 对齐相对 tuple 起点（spike 发现 7）；<b>varlena 列的落位取
 * {@link #varlenaStart}（PG att_align_pointer 同面）——短（1B 头）varlena 可紧跟
 * 定宽列未对齐落盘（当前字节非零即 datum 起点），只有 4B 头形态才有 4 对齐保证
 * （双路对拍 Task 8 实测钉）</b>；null 位图在 tuple offset 23
 * （t_bits），natts 取 infomask2 &amp; 0x07FF；varlena 双头——1B 头总长
 * {@code (b>>1)&0x7F}、4B 头总长 {@code u32le>>>2}（spike 发现 21，小端是实测锚，
 * 防大端误读回归）；dropped 列恒 null 零消耗（spike 发现 26）、skip 列消耗 varlena
 * 不取值；name 定宽 64B 按 NUL 截断；timestamp 按 epoch 2000 微秒渲染为 UTC
 * ISO-8601 字符串。值 → Java 类型映射：bool→Boolean、char→String（左引号单字符，
 * 与 spike 渲染逐字一致）、name/text/timestamp→String、bytea→byte[]、int2→Short、
 * int4→Integer、int8/oid→Long（oid 无符号语义）、float4→Float、float8→Double、
 * dropped/skip/null→null。</p>
 *
 * <p><b>v2 词典扩容（Task 7 硬前置①）</b>：date/time/timetz/timestamptz/uuid/numeric
 * 六 kind 以<b>磁盘 datum 原始字节</b>交付（date=byte[4] i32 天、time/timestamptz=
 * byte[8] i64 微秒、timetz=byte[12] i64+i32、uuid=byte[16]（'c' 对齐）、numeric=
 * varlena 内容（已剥头）byte[]）——磁盘布局解读复用渲染层 {@code DiskValueRenderer}
 * 的既有矩阵（Task 2 交付），本层只负责<b>对齐消耗与切片</b>，不重复实现格式语义
 * （控制器裁定：改码最少且单测可经渲染矩阵锚）。</p>
 *
 * <p>varlena 头分派：首字节恰 0x01（短外部 TOAST 指针）与 tag 位 0x02（4B 行内压缩
 * ——压缩后仍 &lt;2KB 的宽值不走 external 的常见形态）在注入
 * {@link VarlenaResolver} 时委派重建/解压（v2 行内接线，Task 7 硬前置②），未注入
 * 仍 ISE 拒绝（v1 catalog 重放形态）；bit0=1 的其余首字节（含奇总长的 0x03/0x07…）
 * 均为合法 1B 头。实例持 {@link WalLayout} 仅为构造对称（当前 tuple 布局无版本差异，
 * 尚未取值）；实例无共享可变状态（resolver 由调用方保证线程语义），并发安全。</p>
 */
public final class TupleDecoder {

    /** infomask 的 HEAP_HASNULL 位（htup_details.h）——置位时 t_bits 起 null 位图有效。 */
    private static final int HEAP_HASNULL = 0x0001;

    /** infomask2 的 natts 位掩码（HEAP_NATTS_MASK，htup_details.h）。 */
    private static final int HEAP_NATTS_MASK = 0x07FF;

    /** HeapTupleHeader 的 t_bits 偏移（offsetof(HeapTupleHeaderData, t_bits)=23）。 */
    private static final int TUPLE_BITS_OFFSET = 23;

    /** 短外部 varlena 指针 tag（VARATT_IS_1B_E：首字节恰为 0x01，vartag 随后）。 */
    private static final int VARLENA_TAG_1B_EXTERNAL = 0x01;

    /** 4B varlena 头的 tag 位掩码（首字节 &amp; 0x03：00=4B 未压缩、01=1B、1x=压缩/外部）。 */
    private static final int VARLENA_TAG_MASK = 0x03;

    /** 1B varlena 头 tag 位（首字节 bit0 置位即 1B 头）。 */
    private static final int VARLENA_TAG_1B = 0x01;

    /** 外部短 varlena 指针的总长（1B 头 + 1B vartag + 4×u32，varatt.h VARATT_EXTERNAL）。 */
    private static final int EXTERNAL_POINTER_SIZE = 18;

    /** timestamp 的 PostgreSQL epoch（2000-01-01T00:00:00Z）折算微秒数。 */
    private static final long EPOCH_2000_MICROS = 946_684_800_000_000L;

    private final WalLayout layout;

    /** external varlena 重组委派面（null = 保持 v1 拒绝行为）。 */
    private final VarlenaResolver externalResolver;

    /**
     * 以版本布局描述符构造解码器。
     *
     * <p>layout 当前无取值面（tuple 布局跨版本稳定），注入仅为与
     * {@code HeapViews} 各工厂同形——版本差异出现时的扩展锚点。</p>
     *
     * @param layout 版本布局描述符（非 null）
     */
    public TupleDecoder(WalLayout layout) {
        this(layout, null);
    }

    /**
     * 全参构造：在 v1 形态之上注入非 plain varlena 重建委派面——external 短指针
     * （首字节 0x01）与行内压缩（tag 0x02）不再 ISE，改调 resolver 取原值
     * （v2 行内接线；null resolver 保持 v1 拒绝行为）。
     *
     * @param layout           版本布局描述符（非 null）
     * @param externalResolver 非 plain varlena 重建面（null = 保持 v1 拒绝行为）
     */
    public TupleDecoder(WalLayout layout, VarlenaResolver externalResolver) {
        this.layout = layout;
        this.externalResolver = externalResolver;
    }

    /**
     * 解码 WAL 载荷形态的 tuple：{@code [xl_heap_header 5B][自 tuple offset 23 起字节]}。
     *
     * <p>关键步骤：infomask2 u16@off、infomask u16@off+2、t_hoff u8@off+4（xl_heap_header
     * 三成员），tupleStart = off+5-23（载荷新 tuple 省略头前 23B 中 unused 的 18B，
     * 字节流从 t_bits 位图区起接）——随后走 {@link #decodeTupleData}。边界与异常
     * 语义：raw 过短抛数组越界（上游记录走读不变量已保证载荷长度自洽）、kinds 不足
     * 抛 ISE；线程约束：纯读，并发安全。</p>
     *
     * @param raw   完整记录字节（调用方持有，契约只读）
     * @param off   载荷中 xl_heap_header 起点（insert/update 新 tuple、delete 旧 tuple 等）
     * @param kinds 逐列解码 kind 词典
     * @return 长度 natts 的值数组（映射表见类 javadoc）
     * @throws IllegalStateException natts 超词典长度或 kind 未注册
     */
    public Object[] decodePayload(byte[] raw, int off, String[] kinds) {
        int infomask2 = u16(raw, off);
        int infomask = u16(raw, off + 2);
        int tHoff = raw[off + 4] & 0xFF;
        return decodeTupleData(raw, off + 5 - TUPLE_BITS_OFFSET, tHoff, infomask, infomask2, kinds);
    }

    /**
     * 解码页内完整 tuple（带 23B HeapTupleHeader 前缀，页镜像 / INPLACE 改写面）。
     *
     * <p>关键步骤：infomask2 u16@lpOff+18、infomask u16@lpOff+20、t_hoff u8@lpOff+22，
     * tupleStart 即 lpOff——随后走 {@link #decodeTupleData}。边界与异常语义：同
     * {@link #decodePayload}；线程约束：纯读，并发安全。</p>
     *
     * @param page  页字节数组（FPW 重建页或页镜像切片，契约只读）
     * @param lpOff 行指针解析出的 tuple 页内偏移
     * @param kinds 逐列解码 kind 词典
     * @return 长度 natts 的值数组
     * @throws IllegalStateException natts 超词典长度或 kind 未注册
     */
    public Object[] decodePageTuple(byte[] page, int lpOff, String[] kinds) {
        int infomask2 = u16(page, lpOff + 18);
        int infomask = u16(page, lpOff + 20);
        int tHoff = page[lpOff + 22] & 0xFF;
        return decodeTupleData(page, lpOff, tHoff, infomask, infomask2, kinds);
    }

    /**
     * 解码 multi-insert 的一个 entry：{@code [datalen u16][xl_heap_header 5B][...]}。
     *
     * <p>关键步骤：entry 首 2B 是 u16 datalen 前缀（跳过不消费，spike 教训——
     * entry 边界靠它推进），tuple 载荷自 entryOff+2 起即 {@link #decodePayload}
     * 形态。边界与异常语义：同 {@link #decodePayload}；线程约束：纯读，并发安全。</p>
     *
     * @param raw      完整记录字节（契约只读）
     * @param entryOff 本 entry 在 raw 中的起点（datalen 前缀处）
     * @param kinds    逐列解码 kind 词典
     * @return 长度 natts 的值数组
     * @throws IllegalStateException natts 超词典长度或 kind 未注册
     */
    public Object[] decodeEntry(byte[] raw, int entryOff, String[] kinds) {
        return decodePayload(raw, entryOff + 2, kinds);
    }

    /**
     * 三入口共享的核心走读：自 tupleStart 起按 kinds 逐列解码。
     *
     * <p>关键步骤：natts = infomask2 &amp; 0x07FF；HASNULL 时拷出 tuple offset 23 起的
     * 位图（(natts+7)/8 字节）；游标自 tupleStart+t_hoff 起，逐列——位图 null 位为 0
     * 即 null 跳过，否则按 kind 分支：定宽类型先按类型对齐（对齐相对 tupleStart，
     * spike 发现 7——tuple 可坐在缓冲任意偏移）再取宽序小端值；varlena 类（text/
     * bytea/skip）经 varlenaStart 落位（当前字节非零即 1B 头未对齐起点、为零按 4
     * 对齐——Task 8 实测钉）再走双头读长；dropped 零消耗、值恒 null（发现 26）。
     * 边界与异常语义：natts &gt; kinds.length 抛 ISE（消息含两侧计数）、未注册 kind
     * 抛 ISE、raw 过短裸抛越界；线程约束：纯读 + 无实例可变状态参与，并发安全。</p>
     *
     * @param src       源缓冲（记录或页，契约只读）
     * @param tupleStart tuple 起始偏移（t_bits 位图区由此 +23）
     * @param tHoff     数据区起点（自 tupleStart 起算，MAXALIGN(23+bitmapLen)）
     * @param infomask  t_infomask（HASNULL 位）
     * @param infomask2 t_infomask2（natts 位段）
     * @param kinds     逐列解码 kind 词典
     * @return 长度 natts 的值数组
     * @throws IllegalStateException natts 超词典长度或 kind 未注册
     */
    private Object[] decodeTupleData(byte[] src, int tupleStart, int tHoff, int infomask, int infomask2, String[] kinds) {
        int natts = infomask2 & HEAP_NATTS_MASK;
        boolean hasNull = (infomask & HEAP_HASNULL) != 0;
        byte[] bitmap = null;
        if (hasNull) {
            bitmap = new byte[(natts + 7) / 8];
            System.arraycopy(src, tupleStart + TUPLE_BITS_OFFSET, bitmap, 0, bitmap.length);
        }
        if (natts > kinds.length) {
            throw new IllegalStateException("tuple has " + natts + " atts but dictionary has " + kinds.length);
        }
        Object[] vals = new Object[natts];
        int c = tupleStart + tHoff;
        for (int i = 0; i < natts; i++) {
            if (bitmap != null && (bitmap[i / 8] & (1 << (i % 8))) == 0) {
                vals[i] = null;
                continue;
            }
            switch (kinds[i]) {
                case "int2" -> { c = tupleStart + align(c - tupleStart, 2); vals[i] = (short) u16(src, c); c += 2; }
                case "int4" -> { c = tupleStart + align(c - tupleStart, 4); vals[i] = u32(src, c); c += 4; }
                case "int8" -> { c = tupleStart + align(c - tupleStart, 8); vals[i] = u64(src, c); c += 8; }
                case "oid" -> { c = tupleStart + align(c - tupleStart, 4); vals[i] = u32(src, c) & 0xFFFFFFFFL; c += 4; }
                case "float4" -> { c = tupleStart + align(c - tupleStart, 4); vals[i] = Float.intBitsToFloat(u32(src, c)); c += 4; }
                case "float8" -> { c = tupleStart + align(c - tupleStart, 8); vals[i] = Double.longBitsToDouble(u64(src, c)); c += 8; }
                case "bool" -> { vals[i] = src[c] != 0; c += 1; }
                case "char" -> { vals[i] = "'" + (char) (src[c] & 0x7F); c += 1; }
                case "name" -> {   // NameData：定宽 64B，NUL 截断
                    c = tupleStart + align(c - tupleStart, 4);
                    int n = 64;
                    for (int k = 0; k < 64; k++) {
                        if (src[c + k] == 0) {
                            n = k;
                            break;
                        }
                    }
                    // 显式 UTF-8：与 spike 平台默认字符集的有意差异（跨环境确定性）
                    vals[i] = new String(src, c, n, StandardCharsets.UTF_8);
                    c += 64;
                }
                case "timestamp" -> {   // i64 微秒，epoch 2000 起点
                    c = tupleStart + align(c - tupleStart, 8);
                    vals[i] = renderTimestamp(u64(src, c));
                    c += 8;
                }
                case "text" -> { c = varlenaStart(src, c, tupleStart); int[] next = {c}; vals[i] = readTextDatum(src, c, next); c = next[0]; }
                case "bytea", "numeric" -> { c = varlenaStart(src, c, tupleStart); int[] next = {c}; vals[i] = readDatumBytes(src, c, next); c = next[0]; }
                case "date" -> {   // i32 天（epoch 2000-01-01）——datum 原始字节，解读在渲染矩阵
                    c = tupleStart + align(c - tupleStart, 4);
                    vals[i] = slice(src, c, 4);
                    c += 4;
                }
                case "time", "timestamptz" -> {   // i64 微秒——datum 原始字节
                    c = tupleStart + align(c - tupleStart, 8);
                    vals[i] = slice(src, c, 8);
                    c += 8;
                }
                case "timetz" -> {   // i64 微秒 + i32 区偏移，共 12B——datum 原始字节
                    c = tupleStart + align(c - tupleStart, 8);
                    vals[i] = slice(src, c, 12);
                    c += 12;
                }
                case "uuid" -> {   // 16B 网络序，'c' 对齐（1 字节）——datum 原始字节
                    vals[i] = slice(src, c, 16);
                    c += 16;
                }
                case "skip" -> { c = varlenaStart(src, c, tupleStart); int[] next = {c}; skipVarlena(src, c, next); c = next[0]; }
                case "dropped" -> { /* attisdropped 列存储恒 NULL：零字节消耗（发现 26） */ }
                default -> throw new IllegalStateException("unregistered kind " + kinds[i]);
            }
        }
        return vals;
    }

    /**
     * varlena 列的落位起点（<b>PG att_align_pointer 同面</b>，双路对拍 Task 8 实测钉）：
     * 当前偏移的字节<b>非零</b>即短（1B 头）varlena 未对齐落位——直接在此起读；为零即
     * 填充字节——按 4 对齐后是 4B 头（或 external/压缩）形态。
     *
     * <p>存在动机：PG 存储层只对 4B 头 varlena 保证 4 对齐——<b>短 varlena 可紧跟定宽
     * 列（如 bool）未对齐落盘</b>（1B 头恒 bit0=1、首字节不可能为 0；填充字节恒 0，
     * 二者由此无歧义）。原实现一律先 4 对齐，撞上"bool 后未对齐 numeric 短值"即走读
     * 错位（对拍 IT 首跑 AIOOBE 实证：text "alice"(6B) + bool 后 numeric 12.345 落
     * 相对偏移 35）。已 4 对齐的偏移两分支等价（对齐幂等）。线程约束：纯读。</p>
     *
     * @param src        源缓冲
     * @param c          当前游标（前一 datum 之后的首偏移，可能未对齐）
     * @param tupleStart tuple 起始偏移（对齐的相对基准，spike 发现 7）
     * @return varlena datum 的实际起点
     */
    private static int varlenaStart(byte[] src, int c, int tupleStart) {
        return src[c] != 0 ? c : tupleStart + align(c - tupleStart, 4);
    }

    /**
     * 读一个文本 datum（text kind）——external 短指针先走 resolver 重组（返回字节经
     * UTF-8 解码——文本族的解释在此、且仅在此发生，Task 8 保真裁定），其余经
     * plain varlena 读文本。
     *
     * <p>边界与异常语义：external 且未注入 resolver 抛 ISE（v1 拒绝面）；
     * compressed varlena 恒 ISE。线程约束：纯读（resolver 的线程语义由注入方保证）。</p>
     *
     * @param src  源缓冲
     * @param c    datum 起点（经 varlenaStart 落位——1B 头短 varlena 可未 4 对齐）
     * @param next 单元素游标：返回值 = 整个 datum 之后的首偏移
     * @return 文本（external 形态为重组后的原值字符串）
     */
    private String readTextDatum(byte[] src, int c, int[] next) {
        if (isExternalPointer(src, c)) {
            return new String(resolveExternalBytes(src, c, next), StandardCharsets.UTF_8);
        }
        if (isInlineCompressed(src, c)) {
            return decompressInlineString(src, c, next);
        }
        return readVarlenaText(src, c, next);
    }

    /**
     * 读一个字节 datum（bytea/numeric kind）——external 短指针先走 resolver 重组
     * （返回字节<b>直达</b>本方法——Task 8 保真裁定：不再经 String UTF-8 往返，
     * 二进制载荷无损），其余经 plain varlena 取<b>已剥头的载荷</b>（numeric 的渲染
     * 矩阵输入契约即此形态）。
     *
     * <p>边界与异常语义：external 且未注入 resolver 抛 ISE；compressed varlena 恒 ISE。
     * 线程约束：纯读。</p>
     *
     * @param src  源缓冲
     * @param c    datum 起点（经 varlenaStart 落位——1B 头短 varlena 可未 4 对齐）
     * @param next 单元素游标：返回值 = 整个 datum 之后的首偏移
     * @return 载荷字节（external 形态为重组后的原值字节）
     */
    private byte[] readDatumBytes(byte[] src, int c, int[] next) {
        if (isExternalPointer(src, c)) {
            return resolveExternalBytes(src, c, next);
        }
        if (isInlineCompressed(src, c)) {
            return decompressInlineBytes(src, c, next);
        }
        return readVarlenaBytes(src, c, next);
    }

    /**
     * 判定 datum 是否 external 短 varlena 指针且重组面可用——首字节恰 0x01 且
     * resolver 已注入（未注入时交回 plain 读路径的拒绝面，保持 v1 行为与消息）。
     */
    private boolean isExternalPointer(byte[] src, int c) {
        return externalResolver != null && (src[c] & 0xFF) == VARLENA_TAG_1B_EXTERNAL;
    }

    /**
     * 判定 datum 是否行内压缩 varlena（4B 头 tag 0x02——压缩后仍 &lt;2KB 的宽值不走
     * external 而行内落盘，真实 PG 的常见形态）且解压面可用。
     */
    private boolean isInlineCompressed(byte[] src, int c) {
        return externalResolver != null && (src[c] & VARLENA_TAG_MASK) == 0x02;
    }

    /**
     * 委派行内压缩解压（文本形态）并推进游标越过整个 varlena——总长在 4B 头上
     * 30 位（含头，与未压缩 4B 头同式）。
     *
     * @throws IllegalStateException lz4 方法码 / tcinfo/pglz 数据不符（resolver 契约）
     */
    private String decompressInlineString(byte[] src, int c, int[] next) {
        return new String(decompressInlineBytes(src, c, next), StandardCharsets.UTF_8);
    }

    /**
     * 委派行内压缩解压（字节形态——bytea/numeric kind）并推进游标（同
     * {@link #decompressInlineString}）。
     *
     * @throws IllegalStateException lz4 方法码 / tcinfo/pglz 数据不符（resolver 契约）
     */
    private byte[] decompressInlineBytes(byte[] src, int c, int[] next) {
        int total = u32(src, c) >>> 2;
        byte[] payload = new byte[total - 4];
        System.arraycopy(src, c + 4, payload, 0, payload.length);
        next[0] = c + total;
        return externalResolver.decompressInlineCompressed(payload);
    }

    /**
     * 委派 external 重组（字节形态，Task 8 保真裁定）并推进游标越过 18B 指针（指针
     * datum 定长 18B，后续列的对齐由各 kind 的相对 tupleStart 对齐承担）。
     *
     * @throws IllegalStateException 指针形态不符 / lz4 / chunk 不齐时降级——由
     *                               resolver 契约决定（ToastAssembler：形态 ISE、缺值降级字节）
     */
    private byte[] resolveExternalBytes(byte[] src, int c, int[] next) {
        byte[] resolved = externalResolver.resolveExternal(src, c);
        next[0] = c + EXTERNAL_POINTER_SIZE;
        return resolved;
    }

    /**
     * 定宽 datum 切片（date/time/timetz/timestamptz/uuid 的原始字节交付）——
     * 越界由数组拷贝裸抛（上游记录走读不变量已保证载荷长度自洽）。
     */
    private static byte[] slice(byte[] src, int off, int len) {
        byte[] out = new byte[len];
        System.arraycopy(src, off, out, 0, len);
        return out;
    }

    /**
     * 读一个未压缩 varlena 为文本并推进游标。
     *
     * <p>关键步骤：判头——首字节 0x01（短外部 TOAST 指针）或 tag 位 0x02（4B 压缩
     * 形态）一律 ISE（v1 不支持 external/compressed；拒绝面按位精确裁定——1B 头奇
     * 总长的 tag==0x03 形态不拒，见 {@link #rejectNonPlainVarlena}）；tag 位 01 为 1B 头
     * （总长 {@code (b>>1)&0x7F}，含头 1B），tag 位 00 为 4B 头（总长
     * {@code u32le>>>2}，含头 4B——小端是 spike 实测锚，发现 21）。载荷按 UTF-8
     * 解码。边界与异常语义：非 plain varlena 抛 ISE；线程约束：纯读，并发安全。</p>
     *
     * @param src  源缓冲
     * @param c    varlena 起点（经 varlenaStart 落位——1B 头短 varlena 可未 4 对齐）
     * @param next 单元素游标：返回值 = 整个 varlena 之后的首偏移
     * @return UTF-8 文本
     * @throws IllegalStateException external/compressed varlena（v1 不支持）
     */
    private static String readVarlenaText(byte[] src, int c, int[] next) {
        int b0 = src[c] & 0xFF;
        rejectNonPlainVarlena(src, c);
        int len;
        int dataOff;
        if ((b0 & VARLENA_TAG_1B) != 0) {
            len = (b0 >> 1) & 0x7F;
            dataOff = 1;
        } else {
            len = u32(src, c) >>> 2;
            dataOff = 4;
        }
        next[0] = c + len;
        return new String(src, c + dataOff, Math.max(0, len - dataOff), StandardCharsets.UTF_8);
    }

    /**
     * 读一个未压缩 varlena 为原始字节并推进游标（bytea 面）。
     *
     * <p>头判读与 {@link #readVarlenaText} 同源（发现 21），载荷不解码直接拷出。
     * 边界与异常语义：非 plain varlena（0x01 外部指针 / tag 0x02 压缩——按位精确
     * 裁定，见 {@link #rejectNonPlainVarlena}）抛 ISE；线程约束：纯读，并发安全。</p>
     *
     * @param src  源缓冲
     * @param c    varlena 起点（经 varlenaStart 落位——1B 头短 varlena 可未 4 对齐）
     * @param next 单元素游标：返回值 = 整个 varlena 之后的首偏移
     * @return 载荷字节副本
     * @throws IllegalStateException external/compressed varlena（v1 不支持）
     */
    private static byte[] readVarlenaBytes(byte[] src, int c, int[] next) {
        int b0 = src[c] & 0xFF;
        rejectNonPlainVarlena(src, c);
        int len;
        int dataOff;
        if ((b0 & VARLENA_TAG_1B) != 0) {
            len = (b0 >> 1) & 0x7F;
            dataOff = 1;
        } else {
            len = u32(src, c) >>> 2;
            dataOff = 4;
        }
        next[0] = c + len;
        byte[] out = new byte[Math.max(0, len - dataOff)];
        System.arraycopy(src, c + dataOff, out, 0, out.length);
        return out;
    }

    /**
     * 走读跳过一个 varlena（值不物化）——词典外列（skip kind）的消耗面。
     *
     * <p>关键步骤：与 {@link #readVarlenaText} 同一头走读，仅推进游标。边界与异常
     * 语义：非 plain varlena 抛 ISE（跳列同样要读准头才能消耗恰准）；线程约束：
     * 纯读，并发安全。</p>
     *
     * @param src  源缓冲
     * @param c    varlena 起点（经 varlenaStart 落位——1B 头短 varlena 可未 4 对齐）
     * @param next 单元素游标：返回值 = 整个 varlena 之后的首偏移
     * @throws IllegalStateException external/compressed varlena（v1 不支持）
     */
    private static void skipVarlena(byte[] src, int c, int[] next) {
        readVarlenaText(src, c, next);   // 同一头走读，载荷丢弃
    }

    /**
     * 校验 varlena 首字节为 plain（1B 或 4B 未压缩）形态，否则 ISE。
     *
     * <p>拒绝面（按位精确裁定，任务书初版 "tag==0x03 拒" 是规格错误）：首字节恰为
     * 0x01（短外部 TOAST 指针，vartag 在次字节如 0x12=ONDISK，spike 发现 20——重组
     * 属 v2）与 tag 位（b0&amp;0x03）==0x02（4B 压缩 varlena）。**1B 头总长为奇数时
     * b0&amp;0x03 恰为 0x03**（空串 b0=0x03、"ab" b0=0x07）——bit0=1 即合法 1B 头，
     * 不得拒。线程约束：纯读，并发安全。</p>
     *
     * @param src 源缓冲
     * @param c   varlena 起点
     * @throws IllegalStateException external(0x01)/compressed(0x02) varlena（v1 不支持）
     */
    private static void rejectNonPlainVarlena(byte[] src, int c) {
        int b0 = src[c] & 0xFF;
        if (b0 == VARLENA_TAG_1B_EXTERNAL || (b0 & VARLENA_TAG_MASK) == 0x02) {
            throw new IllegalStateException("external/compressed varlena unsupported in v1: hdr=" + b0);
        }
    }

    /**
     * timestamp 微秒渲染为 UTC ISO-8601 字符串。
     *
     * <p>关键步骤：PG epoch（2000-01-01）折算 Unix epoch 微秒（EPOCH_2000_MICROS
     * 常量），floorDiv/floorMod 保负值正确（BC 侧微秒），LocalDateTime UTC 序列化。
     * 线程约束：纯函数，并发安全。</p>
     *
     * @param micros 自 2000-01-01T00:00:00Z 起的微秒数（可为负）
     * @return ISO-8601 本地时间字符串（无时区后缀，UTC 墙钟）
     */
    private static String renderTimestamp(long micros) {
        long epochMicros = EPOCH_2000_MICROS + micros;
        Instant inst = Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * 1000L);
        return LocalDateTime.ofInstant(inst, ZoneOffset.UTC).toString();
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b      源数组（长度须覆盖 o+2）
     * @param o      起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b      源数组（长度须覆盖 o+4）
     * @param o      起始偏移
     * @return 32 位值
     */
    private static int u32(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8) | ((b[o + 2] & 0xFF) << 16) | ((b[o + 3] & 0xFF) << 24);
    }

    /**
     * 就地读 little-endian u64。
     *
     * @param b      源数组（长度须覆盖 o+8）
     * @param o      起始偏移
     * @return 64 位值
     */
    private static long u64(byte[] b, int o) {
        return (u32(b, o) & 0xFFFFFFFFL) | ((long) u32(b, o + 4) << 32);
    }

    /**
     * 向上对齐到 2 的幂。
     *
     * @param v 值
     * @param a 对齐要求（2 的幂）
     * @return 对齐后的值
     */
    private static int align(int v, int a) {
        return (v + a - 1) & ~(a - 1);
    }

    /**
     * 非 plain varlena（TOAST external 短指针 / 行内压缩）的重建委派面——
     * {@code TupleDecoder} 在 v2 词典扩容下的行内接线接缝：解码器只识别首字节
     * 0x01（external 指针）与 tag 位 0x02（行内压缩）两类形态并推进游标，原值重建
     * （chunk 拼装 + pglz 解压 + 回查兜底）由实现方（典型 {@code ToastAssembler}）
     * 承担——pglz 解压算力驻 changes 层，layout 层保持格式分发职责单一。
     *
     * <p>实现契约：指针/压缩形态不符、lz4 方法码抛 {@link IllegalStateException}
     * （fail-fast 面）；external chunk 不齐等不可得形态<b>不抛</b>，返回降级字面
     * （{@code toast-unavailable}——不 fail 整条流，v2 设计 §5）。返回值是<b>原值
     * 字节</b>（Task 8 保真裁定）——文本解释权归解码器（UTF-8）、字节面直达渲染
     * 层，不经 String 有损往返。线程约束：与解码同线程调用（wal-receiver 单写者
     * 上下文）。</p>
     */
    public interface VarlenaResolver {

        /**
         * 自 src 的 off 起剥 18B external 指针并重建原值字节。
         *
         * @param src 完整源缓冲（契约只读）
         * @param off 指针起点（external 指针是 1B 头短 varlena——无 4 对齐保证，
         *            实现按字节起读，与 {@code varlenaStart} 的未对齐落位形态衔接）
         * @return 重组后的原值字节（不可得时为降级字面的 UTF-8 字节）
         * @throws IllegalStateException 指针形态/lz4 方法码不符
         */
        byte[] resolveExternal(byte[] src, int off);

        /**
         * 解压行内压缩 varlena 的载荷（4B varlena 头已剥——payload 即
         * {@code [u32 tcinfo = exhdrlen 低 30 位 | 方法&lt;&lt;30][pglz 流]}，
         * 与 external 压缩值的 chunk 拼接体同构）。
         *
         * @param payload 载荷字节（长度 ≥4 才可能有 tcinfo 头）
         * @return 解压后的原载荷字节（长度 = tcinfo 低 30 位）
         * @throws IllegalStateException tcinfo 头缺失/lz4 方法码/pglz 数据不符
         */
        byte[] decompressInlineCompressed(byte[] payload);
    }
}
