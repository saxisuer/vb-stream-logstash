package org.vastdata.vbstream.walsource.layout;

import java.io.ByteArrayOutputStream;

/**
 * 截断 UPDATE 新元组的字节拼装重建工具（Task 8.5）——v1
 * {@code CatalogReplay.reconstructTruncated} 通用拼装算法的参数化抽取（用户表行形态：
 * 拼装源是<b>记录自带的 FULL 身份旧元组</b>，非 v1 的跨记录 rawTailStore）。
 *
 * <p><b>实源钉（REL_18_STABLE src/backend/access/heap/heapam.c，2026-10 curl）</b>：
 * 块 data 布局 = {@code [prefix u16?][suffix u16?][xl_heap_header 5B]} 随后——
 * prefix&gt;0 时 {@code [位图+垫 (t_hoff-23)B][数据区自 t_hoff+prefix 起的 mid 段]}
 * 分两段注册；prefix=0 时位图到数据尾（减 suffixlen 字节）一整段。重建语义照服务端
 * 回放 {@code heap_xlog_update}：<b>prefix 段自旧元组自己的 t_hoff 起取</b>（旧/新
 * 位图宽可不同——v1 用新 tHoff 切旧 tail，仅目录表无 null 位图时恰等价），suffix 段
 * 自旧元组字节<b>末端</b>取 suffixlen 字节；位图/头字段（infomask/infomask2/t_hoff）
 * 由记录携带。</p>
 *
 * <p><b>调用前置（调用方保证，本类不复核）</b>：flags 已置至少一个截断位；旧元组载荷
 * （{@code [xl_heap_header 5B][自 offset 23 起字节]}）是 REPLICA IDENTITY FULL 的
 * <b>全列</b>形态（ExtractReplicaIdentity 整行返回）——KEY 形态非键列全 null，拼装
 * 只会产出错值，属拒绝面。边界与异常语义：长度越界（手造/畸形字节）数组越界裸抛、
 * 词典不匹配抛 ISE（fail-fast 与解码面同风格）。线程约束：静态纯函数，并发安全。</p>
 */
public final class TruncatedUpdateSplice {

    /** HeapTupleHeader 的 t_bits 偏移（offsetof(HeapTupleHeaderData, t_bits)=23）。 */
    private static final int TUPLE_BITS_OFFSET = 23;

    /**
     * 工具类防实例化。
     */
    private TruncatedUpdateSplice() {
    }

    /**
     * 截断 UPDATE 新元组重建：按块 data 的截断头走读拼装完整 tail（自 tuple offset 23
     * 起字节），回填记录携带的 xl_heap_header 后交 {@link TupleDecoder#decodePayload}。
     *
     * <p>关键步骤：①截断头走读——PREFIX/SUFFIX 位各自消费一个 u16；②新元组头三字段
     * （infomask2/infomask/t_hoff）自记录读、旧元组 t_hoff 自旧载荷第 5 字节读；③拼装
     * ——prefix=0：记录剩余整段 + 旧 tail 末 suffix 字节；prefix&gt;0：记录位图段 +
     * <b>旧 tail 自 (旧t_hoff-23) 起的 prefix 字节</b> + 记录 mid 段 + 旧 tail 末 suffix
     * 字节；④[5B 头][tail] 组装解码。边界与异常语义：suffix/prefix 越过旧 tail 边界
     * 由数组越界裸抛（上游记录走读自洽前提）；natts 超词典抛 ISE（解码器契约）。
     * 线程约束：纯函数，并发安全。</p>
     *
     * @param decoder     行解码器（调用方实例——external varlena 委派面随之生效）
     * @param raw         完整记录字节（契约只读）
     * @param blk         新元组所在块（须携带 data——截断形态新元组恒在块 data，镜像
     *                    形态与截断互斥：FPW 需求时 log_heap_update 不做省略）
     * @param flags       xl_heap_update 的 flags（main@7）
     * @param oldTupleOff FULL 身份旧元组载荷在 raw 中的起点（main 区 sizeOfHeapUpdate 之后）
     * @param oldTailLen  旧元组 tail 字节数（mainLen - sizeOfHeapUpdate - 5）
     * @param kinds       逐列解码词典
     * @return 重建后的新元组值数组（长度 natts）
     * @throws IllegalStateException 词典不匹配（解码器契约）
     */
    public static Object[] reconstructNewTuple(TupleDecoder decoder, byte[] raw, BlockRef blk,
            int flags, int oldTupleOff, int oldTailLen, String[] kinds) {
        int cur = blk.dataOff();
        int prefix = 0;
        int suffix = 0;
        if ((flags & HeapOps.XLH_UPDATE_PREFIX_FROM_OLD) != 0) {
            prefix = u16(raw, cur);
            cur += 2;
        }
        if ((flags & HeapOps.XLH_UPDATE_SUFFIX_FROM_OLD) != 0) {
            suffix = u16(raw, cur);
            cur += 2;
        }
        int newInfomask2 = u16(raw, cur);
        int newInfomask = u16(raw, cur + 2);
        int newTHoff = raw[cur + 4] & 0xFF;
        cur += 5;
        int oldTHoff = raw[oldTupleOff + 4] & 0xFF;
        int oldTailOff = oldTupleOff + 5;
        int bitmapLen = newTHoff - TUPLE_BITS_OFFSET;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (prefix == 0) {
            // 只截后缀：记录携带 [位图+垫+数据 减末尾 suffix 字节] 连续整段
            out.write(raw, cur, blk.dataLen() - (cur - blk.dataOff()));
        } else {
            out.write(raw, cur, bitmapLen);                     // 位图 + 垫（新元组自己的）
            cur += bitmapLen;
            out.write(raw, oldTailOff + (oldTHoff - TUPLE_BITS_OFFSET), prefix);   // 旧元组数据区前缀
            out.write(raw, cur, blk.dataLen() - (cur - blk.dataOff()));            // mid 段
        }
        if (suffix > 0) {
            out.write(raw, oldTailOff + oldTailLen - suffix, suffix);              // 旧元组末端后缀
        }
        byte[] tail = out.toByteArray();
        byte[] payload = new byte[5 + tail.length];
        payload[0] = (byte) newInfomask2;
        payload[1] = (byte) (newInfomask2 >>> 8);
        payload[2] = (byte) newInfomask;
        payload[3] = (byte) (newInfomask >>> 8);
        payload[4] = (byte) newTHoff;
        System.arraycopy(tail, 0, payload, 5, tail.length);
        return decoder.decodePayload(payload, 0, kinds);
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b 源数组（长度须覆盖 o+2）
     * @param o 起始偏移
     * @return 16 位值
     */
    private static int u16(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8);
    }
}
