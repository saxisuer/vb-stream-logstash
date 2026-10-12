package org.vastdata.vbstream.walsource.changes;

/**
 * openGauss attcmprmode 列压缩（页字典压缩）解压面——ASTORE 压缩页/压缩元组里
 * 单列压缩值的"字节→原值 datum"纯函数转录（openGauss v5.0.0
 * {@code src/gausskernel/storage/page/pagecompress.cpp} 的
 * {@code FetchAttrCmprMeta}/{@code GetAttrCmprValSize}/{@code UncompressOneAttr}
 * 三面，常量出自 {@code src/include/storage/pagecompress.h} L49-54）。
 *
 * <p><b>格式概述</b>（与 vanilla TOAST 压缩完全不同的机制）：OG 行存列压缩按
 * <b>页</b>组织——每页 special 区（WAL MULTI_INSERT 里 {@code isCompressed≠0} 的
 * i16 前缀压缩区）承载逐列串联的压缩 meta（{@code SetCmprHeaderData} 布局：
 * 每列 {@code [mode u8][mode 专属区]}）；页上元组打 {@code HEAP_COMPRESSED}
 * （infomask 0x0010）标记，各列以压缩形态落位。四种算法：DELTA（数字列差值
 * 编码，大端差值 + meta 基值）/ DICT（页级字典，值区 1B item id）/ PREFIX
 * （字符串公共前缀，meta 存前缀、值区存后缀 varlena/cstring）/ NUMSTR（纯数字
 * 串 nibble 半字节编码，低 nibble 在前 + 偏置 1）。pg_attribute.attcmprmode 是
 * <b>建列时的算法提示</b>（ATT_CMPR_* 与本面 mode 值同源），实际每页每列的
 * mode 以压缩头首字节为准——解压分派只认 meta，不读 catalog 提示。</p>
 *
 * <p><b>PG 档可达性结论（2026-10-12 og-plan 容器实测 + 源码双护栏，重要）</b>：
 * openGauss 5.0.0 对<b>行存表禁用列压缩</b>——{@code CREATE TABLE ... COMPRESS}、
 * {@code ALTER ... SET (compression=yes)}、{@code WITH (compresslevel)} 三路 DDL
 * 均报 "row-oriented table does not support compression"（tablecmds.cpp
 * L12270-12274 + ATDisableROTCompress 双护栏；PG 档 m2pg 与 A 档 postgres 双库
 * 同拒）；vanilla PG 14 的 {@code SET COMPRESSION} 语法在 OG 不存在；全库
 * pg_class.relcmprs ≤ 1（PAGE_PLAIN 不算压缩）、attcmprmode ∈ {0, 0x7F}。即本面
 * 在 5.0.0 全新行存面<b>天然不可达</b>（压缩路径仅列存/历史升级库可达，A 档列存
 * 留 M3）——本类按任务书降级条款"解压面照常实现 + 手造向量测"交付，黄金向量
 * 依压缩侧源码语义手造（真实服务端字节不可采，记档 task-4-report）。</p>
 *
 * <p><b>挂接点裁定</b>：任务书预设"挂 varlena 接缝（ToastAssembler 同族）"经
 * 源码核实<b>不成立</b>——attcmprmode 压缩不是 varlena 头 tag（1B/0x02 tag 面是
 * TOAST pglz/lz4 的），压缩值在元组里按 attlen 形态落位（DELTA 定宽裸字节、
 * DICT 单字节 id 等），与 varlena 接缝零交集；真正的接线位是未来的"压缩元组
 * 解码路径"（HEAP_COMPRESSED 元组 + 页字典——MULTI_INSERT 压缩区解析仍 ISE，
 * T3 记档 M2 范围外）。本类因此以<b>纯库形态</b>交付（无调用方），零扰动既有面。</p>
 *
 * <p>错误语义（M2 spec §4）：解压失败（mode 未注册/meta 形态不符/id 越界/载荷
 * 损坏）一律 {@link IllegalStateException}——列压缩失败 = 格式错位硬失败，区别于
 * TOAST 值面的降级契约。线程约束：纯函数无共享可变状态，并发安全。</p>
 */
final class OgColumnCompression {

    /* ---------------- 常量（pagecompress.h L49-54 / pagecompress.cpp L122-123，og v5.0.0） ---------------- */

    /** 压缩 mode：不压缩（NONE）——该列在本页存原值，meta 区仅 1B mode 字节。 */
    static final int CMPR_NONE = 0;

    /** 压缩 mode：DELTA 差值编码（数字列；meta 带 bytes 宽度 + 基值）。 */
    static final int CMPR_DELTA = 1;

    /** 压缩 mode：DICT 页级字典（meta 带字典项；值区 1B item id 或单项时零消耗）。 */
    static final int CMPR_DICT = 2;

    /** 压缩 mode：PREFIX 前缀压缩（变宽字符串列；meta 带公共前缀，值区存后缀）。 */
    static final int CMPR_PREFIX = 3;

    /** 压缩 mode：NUMSTR 数字串半字节编码（CMPR_DELTA 的字符串特化）。 */
    static final int CMPR_NUMSTR = 4;

    /** catalog 里 toast/index 列的"未定义"占位值（pagecompress.h CMPR_UNDEF）——压缩头里出现即格式错位。 */
    static final int CMPR_UNDEF = 0x7F;

    /** DICT 值区消耗（pagecompress.cpp DICT_ITEMID_SIZE）——单项字典（itemNum==1）时为 0。 */
    private static final int DICT_ITEMID_SIZE = 1;

    /** DELTA bytes 上界（压缩侧 Assert(bytes < 8)，g_max_value_in_bytes 尺寸表 8 项）。 */
    private static final int DELTA_BYTES_MAX = 7;

    /** 工具类不可实例化。 */
    private OgColumnCompression() {
    }

    /* ---------------- 值对象 ---------------- */

    /**
     * 单列压缩 meta（{@code FetchAttrCmprMeta} 转录产物）。
     *
     * @param mode      压缩算法（CMPR_* 常量；NONE 列其余字段无意义）
     * @param deltaBytes DELTA：差值字节宽度（0 = 全列同值零消耗形态）
     * @param deltaMin  DELTA：基值（原值 datum 的 attlen 宽小端原字节）
     * @param dictItems DICT：字典项（定宽=attlen 裸字节 / varlena=含头完整字节 /
     *                  cstring=含 NUL 字节）
     * @param prefixLen PREFIX：前缀长度
     * @param prefix    PREFIX：前缀字节
     * @param metaSize  本列 meta 在压缩头内的总字节数（走列步进面）
     */
    record CmprAttrMeta(int mode, int deltaBytes, byte[] deltaMin, byte[][] dictItems,
                        int prefixLen, byte[] prefix, int metaSize) {
    }

    /**
     * 单列压缩值的解压产物。
     *
     * @param datum 原值 datum 字节：定宽列 = attlen 宽小端裸字节；varlena（attlen=-1）
     *              = 重建的 4B 头完整 varlena（解压侧恒 SET_VARSIZE）；cstring
     *              （attlen=-2）= 含尾部 NUL；DELTA dropped 列（typeOid=0）为 null
     * @param compressedSize 值区消耗字节数（GetAttrCmprValSize 同面：DELTA=bytes、
     *                       DICT=1 或单项 0、PREFIX/NUMSTR=varlena 总长或 strlen+1）
     */
    record AttrVal(byte[] datum, int compressedSize) {
    }

    /* ---------------- meta 解析面 ---------------- */

    /**
     * 解析压缩头内一列的压缩 meta（{@code FetchAttrCmprMeta} 逐行转录）。
     *
     * <p>关键步骤：首字节 mode 分派——DELTA 读 {@code [bytes u8][基值 attlen 字节]}
     * （小端，store_att_byval 面）；DICT 读 {@code [itemNum u8][逐项]}（定宽每项
     * attlen 字节、varlena 每项 VARSIZE_ANY 总长含头、cstring 每项 strlen+1）；
     * PREFIX 读 {@code [len u8][前缀字节]}；NUMSTR/NONE 仅 1B mode。</p>
     *
     * <p>边界与异常语义：mode 未注册（含 0x7F）抛 ISE 附 mode；DELTA 落在
     * attlen&lt;=0 列抛 ISE（压缩侧只对定宽数字列选 DELTA）；DELTA bytes&gt;7 抛
     * ISE；PREFIX len=0 抛 ISE（空前缀不构成压缩，C 侧 Assert）；meta 越界抛
     * 数组越界（上游页/MULTI_INSERT 走读不变量已保证压缩区自洽）。线程约束：
     * 纯读，并发安全。</p>
     *
     * @param meta   压缩头字节（MULTI_INSERT 压缩区或页 special 区内容）
     * @param off    本列 meta 起点
     * @param attlen 列宽（正=定宽字节数；-1=varlena；-2=cstring）
     * @return 解析产物（含 metaSize 走列步进）
     * @throws IllegalStateException mode/形态不符（格式错位硬失败）
     */
    static CmprAttrMeta parseMeta(byte[] meta, int off, int attlen) {
        int mode = meta[off] & 0xFF;
        switch (mode) {
            case CMPR_DELTA -> {
                if (attlen <= 0) {
                    throw new IllegalStateException("cmpr DELTA on non-fixed column attlen=" + attlen);
                }
                int deltaBytes = meta[off + 1] & 0xFF;
                if (deltaBytes > DELTA_BYTES_MAX) {
                    throw new IllegalStateException("cmpr DELTA bytes out of range: " + deltaBytes);
                }
                byte[] min = slice(meta, off + 2, attlen);
                return new CmprAttrMeta(mode, deltaBytes, min, null, 0, null, 2 + attlen);
            }
            case CMPR_DICT -> {
                int itemNum = meta[off + 1] & 0xFF;
                byte[][] items = new byte[itemNum][];
                int p = off + 2;
                for (int i = 0; i < itemNum; i++) {
                    int itemLen = dictItemSize(meta, p, attlen);
                    items[i] = slice(meta, p, itemLen);
                    p += itemLen;
                }
                return new CmprAttrMeta(mode, 0, null, items, 0, null, p - off);
            }
            case CMPR_PREFIX -> {
                if (attlen != -1 && attlen != -2) {
                    throw new IllegalStateException("cmpr PREFIX on non-varlena column attlen=" + attlen);
                }
                int len = meta[off + 1] & 0xFF;
                if (len == 0) {
                    throw new IllegalStateException("cmpr PREFIX with empty prefix");
                }
                byte[] prefix = slice(meta, off + 2, len);
                return new CmprAttrMeta(mode, 0, null, null, len, prefix, 2 + len);
            }
            case CMPR_NUMSTR, CMPR_NONE -> {
                return new CmprAttrMeta(mode, 0, null, null, 0, null, 1);
            }
            default -> throw new IllegalStateException("unregistered cmpr mode " + mode);
        }
    }

    /**
     * 便捷重载：自偏移 0 解析单列 meta。
     *
     * @param meta   压缩头字节
     * @param attlen 列宽
     * @return 解析产物
     * @throws IllegalStateException 同 {@link #parseMeta(byte[], int, int)}
     */
    static CmprAttrMeta parseMeta(byte[] meta, int attlen) {
        return parseMeta(meta, 0, attlen);
    }

    /**
     * 走读整份压缩头（{@code SetCmprHeaderData} 布局：逐列 meta 串联，顺序 =
     * 元组列序）。
     *
     * <p>边界与异常语义：列数与 attlens 一致由调用方保证，逐列解析异常同
     * {@link #parseMeta(byte[], int, int)} 上抛；线程约束：纯读。</p>
     *
     * @param header  压缩头字节
     * @param attlens 逐列 attlen（长度 = 列数）
     * @return 逐列 meta（NONE 列 mode=CMPR_NONE）
     * @throws IllegalStateException 任一列 meta 形态不符
     */
    static CmprAttrMeta[] parseCmprHeader(byte[] header, int[] attlens) {
        CmprAttrMeta[] metas = new CmprAttrMeta[attlens.length];
        int off = 0;
        for (int i = 0; i < attlens.length; i++) {
            metas[i] = parseMeta(header, off, attlens[i]);
            off += metas[i].metaSize();
        }
        return metas;
    }

    /**
     * 压缩头的 MAXALIGN 前总字节数（{@code SetCmprHeaderData} 收口：
     * m_cmprHeaderSize = MAXALIGN(逐列累计)——MULTI_INSERT WAL 的 i16 size 前缀
     * 即此值）。
     *
     * @param header  压缩头字节
     * @param attlens 逐列 attlen
     * @return MAXALIGN 前的累计字节数
     * @throws IllegalStateException 同 {@link #parseCmprHeader(byte[], int[])}
     */
    static int cmprHeaderSize(byte[] header, int[] attlens) {
        int off = 0;
        for (int attlen : attlens) {
            off += parseMeta(header, off, attlen).metaSize();
        }
        return off;
    }

    /**
     * NUMSTR 的 meta 占位（该 mode meta 区仅 1B mode 字节，无专属区——测试与
     * 未来走列装配的便捷面）。
     *
     * @return mode=CMPR_NUMSTR 的单字节 meta 产物
     */
    static CmprAttrMeta numstrMeta() {
        return new CmprAttrMeta(CMPR_NUMSTR, 0, null, null, 0, null, 1);
    }

    /* ---------------- 解压面 ---------------- */

    /**
     * 解压元组值区的一列压缩值为原值 datum（{@code UncompressOneAttr} 的 datum
     * 返回重载逐行转录，L1398-1446）。
     *
     * <p>关键步骤：按 meta.mode 分派——DELTA：bytes==0 零消耗返基值；否则大端读
     * 差值、按列类型宽度回绕加基值（int2/int4/oid 定宽回绕、int8 不回绕，plus
     * 函数族语义）。DICT：单项字典零消耗返 item0；否则读 1B id 查项。PREFIX：
     * 值区后缀（varlena 含头读载荷 / cstring 至 NUL）拼 meta 前缀，varlena 重建
     * 恒 4B 头。NUMSTR：nibble 逐字节展开为 ASCII 数字（低 nibble 在前、配对字节
     * 偏置 1、尾字节高 nibble 0xF 标记奇长尾位）。</p>
     *
     * <p>边界与异常语义：DELTA 的 typeOid 不在数字类型表抛 ISE 附 oid（C 侧
     * binarySearch NULL 后解引用即崩）；typeOid==0（dropped 列）datum 返 null、
     * compressedSize 照常推进（C 侧"值不可算，跳过继续走列"注释语义）；DICT id
     * 越界、NUMSTR 空载荷/偏置零字节均 ISE；值区越界裸抛数组越界。线程约束：
     * 纯读，并发安全。</p>
     *
     * @param meta    本列压缩 meta（{@link #parseMeta} 产物，mode 必非 CMPR_NONE）
     * @param typeOid 列类型 oid（DELTA 分派数字类型表；0=dropped 列）
     * @param attlen  列宽
     * @param tuple   元组数据区字节（契约只读）
     * @param off     本列压缩值起点
     * @return 原值 datum + 值区消耗
     * @throws IllegalStateException 格式错位（mode/typeOid/id/载荷不符）
     */
    static AttrVal uncompressAttr(CmprAttrMeta meta, long typeOid, int attlen, byte[] tuple, int off) {
        return switch (meta.mode()) {
            case CMPR_DELTA -> delta(meta, typeOid, attlen, tuple, off);
            case CMPR_DICT -> dict(meta, tuple, off);
            case CMPR_PREFIX -> prefix(meta, attlen, tuple, off);
            case CMPR_NUMSTR -> numstr(attlen, tuple, off);
            default -> throw new IllegalStateException("uncompress on mode " + meta.mode());
        };
    }

    /**
     * DELTA 解压（{@code DeltaUncompress} L575-586 + plus 函数族 L2510-2635）。
     *
     * <p>关键步骤：bytes==0 → 零消耗返基值原字节；否则按 meta.bytes 大端读差值，
     * 基值按 attlen 宽小端有符号读出，相加后按宽度两补截断回 attlen 字节。类型
     * 表（g_number_types）：int8(20)/int2(21)/int4(23)/oid(26)/date(1082)/
     * time(1083)/timestamp(1114)/timestamptz(1184)/time_stamp(12089)——宽度语义
     * 由 attlen 承载（2/4/8 字节），表外 oid ISE。</p>
     *
     * @throws IllegalStateException typeOid 不在数字类型表且非 0（dropped）
     */
    private static AttrVal delta(CmprAttrMeta meta, long typeOid, int attlen, byte[] tuple, int off) {
        if (meta.deltaBytes() == 0) {
            return new AttrVal(meta.deltaMin().clone(), 0);
        }
        long diff = 0;
        for (int i = 0; i < meta.deltaBytes(); i++) {
            diff = (diff << 8) | (tuple[off + i] & 0xFF);
        }
        if (typeOid == 0) {
            return new AttrVal(null, meta.deltaBytes());
        }
        if (!isNumberType(typeOid)) {
            throw new IllegalStateException("cmpr DELTA on non-number type oid " + typeOid);
        }
        long min = readSignedLE(meta.deltaMin(), attlen);
        long sum = min + diff;
        return new AttrVal(writeLE(sum, attlen), meta.deltaBytes());
    }

    /**
     * DICT 解压（{@code DictUncompress} L589-614）。
     *
     * <p>关键步骤：单项字典（itemNum==1）零消耗返 item0；否则值区 1B item id，
     * id ≥ itemNum 抛 ISE（C 侧 dictItems+id 越界读，Java 面显式化）。datum 为
     * 字典项原字节（定宽裸字节 / varlena 含头 / cstring 含 NUL——DictUncompress
     * 返回 itemData 指针原样，attlen 语义已折进 meta 的字典项字节）。</p>
     *
     * @throws IllegalStateException id 越界（格式错位硬失败）
     */
    private static AttrVal dict(CmprAttrMeta meta, byte[] tuple, int off) {
        byte[][] items = meta.dictItems();
        if (items.length == 1) {
            return new AttrVal(items[0].clone(), 0);
        }
        int id = tuple[off] & 0xFF;
        if (id >= items.length) {
            throw new IllegalStateException("cmpr DICT id " + id + " out of " + items.length + " items");
        }
        return new AttrVal(items[id].clone(), DICT_ITEMID_SIZE);
    }

    /**
     * PREFIX 解压（{@code PrefixUncompress} L765-830）。
     *
     * <p>关键步骤：attlen=-1——值区是完整 varlena（1B/4B 头皆可，压缩侧短后缀
     * 用 SET_VARSIZE_SHORT），取载荷为后缀；重建恒 4B 头 varlena
     * {@code [u32 (4+prefix.len+suffix.len)<<2 LE][prefix][suffix]}（解压侧恒
     * SET_VARSIZE——即使 1B 头装得下）。attlen=-2——值区至 NUL 为后缀，重建
     * {@code prefix+suffix+NUL}。值区消耗：varlena=VARSIZE_ANY 总长；cstring=
     * strlen+1。</p>
     */
    private static AttrVal prefix(CmprAttrMeta meta, int attlen, byte[] tuple, int off) {
        byte[] prefixBytes = meta.prefix();
        if (attlen == -1) {
            int total = varlenaSize(tuple, off);
            int dataOff = (tuple[off] & 0x01) != 0 ? 1 : 4;
            int suffixLen = total - dataOff;
            byte[] datum = new byte[4 + prefixBytes.length + suffixLen];
            writeU32LE(datum, (long) datum.length << 2);
            System.arraycopy(prefixBytes, 0, datum, 4, prefixBytes.length);
            System.arraycopy(tuple, off + dataOff, datum, 4 + prefixBytes.length, suffixLen);
            return new AttrVal(datum, total);
        }
        int suffixLen = 0;
        while (tuple[off + suffixLen] != 0) {
            suffixLen++;
        }
        byte[] datum = new byte[prefixBytes.length + suffixLen + 1];
        System.arraycopy(prefixBytes, 0, datum, 0, prefixBytes.length);
        System.arraycopy(tuple, off, datum, prefixBytes.length, suffixLen);
        datum[datum.length - 1] = 0;
        return new AttrVal(datum, suffixLen + 1);
    }

    /**
     * NUMSTR 解压（{@code NumstrUncompress} L295-383）。
     *
     * <p>关键步骤：载荷（attlen=-1 取 varlena 载荷、-2 取 NUL 前 cstring）逐字节
     * 展开两 ASCII 数字——低 nibble 在前（配对字节 = {@code (d0)|(d1<<4)+1}，解码
     * 取 {@code (b-1)}），尾字节高 nibble==0xF 表示奇长尾位（低 nibble 单数字，
     * 无偏置）；重建 attlen=-1 恒 4B 头 varlena、-2 追加 NUL。空载荷 ISE（压缩侧
     * 对空串不压缩）；配对字节 0x00 ISE——合法编码器恒产 ≥1 字节（偏置保证），
     * 0 即损坏（C 侧静默解出乱字符，与格式错位硬失败契约不符故收紧）。</p>
     *
     * @throws IllegalStateException 空载荷 / 偏置零字节（格式错位硬失败）
     */
    private static AttrVal numstr(int attlen, byte[] tuple, int off) {
        int srcLen;
        int consumed;
        byte[] src;
        if (attlen == -1) {
            int total = varlenaSize(tuple, off);
            int dataOff = (tuple[off] & 0x01) != 0 ? 1 : 4;
            srcLen = total - dataOff;
            src = slice(tuple, off + dataOff, srcLen);
            consumed = total;
        } else {
            srcLen = 0;
            while (tuple[off + srcLen] != 0) {
                srcLen++;
            }
            src = slice(tuple, off, srcLen);
            consumed = srcLen + 1;
        }
        if (srcLen == 0) {
            throw new IllegalStateException("cmpr NUMSTR empty payload");
        }
        byte[] digits = new byte[srcLen * 2 + 1];
        int pos = 0;
        for (int i = 0; i < srcLen - 1; i++) {
            int b = src[i] & 0xFF;
            if (b == 0) {
                throw new IllegalStateException("cmpr NUMSTR zero code byte at " + i);
            }
            pos = emitDigitPair(digits, pos, b - 1);
        }
        int last = src[srcLen - 1] & 0xFF;
        if ((last & 0xF0) == 0xF0) {
            digits[pos++] = (byte) ((last & 0x0F) + '0');
        } else {
            if (last == 0) {
                throw new IllegalStateException("cmpr NUMSTR zero code byte at " + (srcLen - 1));
            }
            pos = emitDigitPair(digits, pos, last - 1);
        }
        if (attlen == -1) {
            byte[] datum = new byte[4 + pos];
            writeU32LE(datum, (long) datum.length << 2);
            System.arraycopy(digits, 0, datum, 4, pos);
            return new AttrVal(datum, consumed);
        }
        byte[] datum = new byte[pos + 1];
        System.arraycopy(digits, 0, datum, 0, pos);
        return new AttrVal(datum, consumed);
    }

    /**
     * 展开一个配对编码字节为两 ASCII 数字（低 nibble 在前）并推进游标。
     *
     * @param digits 输出缓冲
     * @param pos    游标
     * @param unbias 已去偏置的编码字节
     * @return 推进后的游标
     */
    private static int emitDigitPair(byte[] digits, int pos, int unbias) {
        digits[pos++] = (byte) ((unbias & 0x0F) + '0');
        digits[pos++] = (byte) (((unbias & 0xF0) >> 4) + '0');
        return pos;
    }

    /* ---------------- 底层走读 ---------------- */

    /**
     * varlena 总长（VARSIZE_ANY 面：1B 头 {@code (b>>1)&0x7F}、4B 头
     * {@code u32le>>>2}——与 {@code TupleDecoder} 双头读长同源，小端是实测锚）。
     *
     * @param src 源缓冲
     * @param off varlena 起点
     * @return 含头总长
     */
    private static int varlenaSize(byte[] src, int off) {
        int b0 = src[off] & 0xFF;
        return (b0 & 0x01) != 0 ? ((b0 >> 1) & 0x7F) : (int) (readU32LE(src, off) >>> 2);
    }

    /**
     * DICT 字典项长度（FetchAttrCmprMeta DICT 分支的逐项步进）：定宽 = attlen；
     * varlena = VARSIZE_ANY 含头；cstring = strlen+1。
     *
     * @throws IllegalArgumentException attlen 非法（0 或 &lt;-2——上游词典口径错）
     */
    private static int dictItemSize(byte[] meta, int p, int attlen) {
        if (attlen > 0) {
            return attlen;
        }
        if (attlen == -1) {
            return varlenaSize(meta, p);
        }
        if (attlen == -2) {
            int n = 0;
            while (meta[p + n] != 0) {
                n++;
            }
            return n + 1;
        }
        throw new IllegalArgumentException("bad attlen " + attlen);
    }

    /**
     * DELTA 数字类型表判定（g_number_types L56-66：int8/int2/int4/oid/date/time/
     * timestamp/timestamptz/time_stamp 九型——宽度语义由 attlen 承载）。
     */
    private static boolean isNumberType(long typeOid) {
        return typeOid == 20 || typeOid == 21 || typeOid == 23 || typeOid == 26
                || typeOid == 1082 || typeOid == 1083 || typeOid == 1114
                || typeOid == 1184 || typeOid == 12089;
    }

    /**
     * attlen 宽小端有符号读（fetch_att 面：2/4 字节符号扩展、8 字节直读）。
     */
    private static long readSignedLE(byte[] b, int len) {
        return switch (len) {
            case 2 -> (short) (b[0] & 0xFF | (b[1] & 0xFF) << 8);
            case 4 -> (int) readU32LE(b, 0);
            case 8 -> (readU32LE(b, 0) & 0xFFFFFFFFL) | readU32LE(b, 4) << 32;
            default -> throw new IllegalStateException("cmpr DELTA bad attlen " + len);
        };
    }

    /**
     * 值按宽度两补截断后写 attlen 宽小端（int16Plus/int32Plus/uint32Plus 的
     * "不关心溢出"回绕语义——按位与截断即同结果）。
     */
    private static byte[] writeLE(long v, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) (v >>> (8 * i));
        }
        return out;
    }

    /**
     * 就地读 little-endian u32。
     */
    private static long readU32LE(byte[] b, int o) {
        return (b[o] & 0xFFL) | (b[o + 1] & 0xFFL) << 8 | (b[o + 2] & 0xFFL) << 16 | (b[o + 3] & 0xFFL) << 24;
    }

    /**
     * 就地写 little-endian u32（重建 varlena 4B 头：总长 &lt;&lt; 2——两处调用恒写
     * 偏移 0，参数面收窄为无偏移形态）。
     */
    private static void writeU32LE(byte[] b, long v) {
        b[0] = (byte) v;
        b[1] = (byte) (v >>> 8);
        b[2] = (byte) (v >>> 16);
        b[3] = (byte) (v >>> 24);
    }

    /**
     * 切片拷出。
     */
    private static byte[] slice(byte[] src, int off, int len) {
        byte[] out = new byte[len];
        System.arraycopy(src, off, out, 0, len);
        return out;
    }
}
