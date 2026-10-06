package org.vastdata.vbstream.walsource.layout;

/**
 * 磁盘格式 tuple 逐列解码 kind 词典——pg_type oid 到解码 kind 字符串的映射，
 * 以及两个非类型字面 kind（{@link #DROPPED} / {@link #SKIP}）。
 *
 * <p>catalog 重放（pg_attribute 词典重建）以本映射把列的 atttypid 换成本解码层的
 * kind 字符串；未知 oid 一律 ISE fail-fast（词典完备性契约——宁可拒解也不猜格式），
 * 映射集合照抄 spike 实测锚（spike {@code kindForTypeOid}）。
 * <b>v2 扩容（Task 7 硬前置①）</b>：对齐渲染矩阵（{@code DiskValueRenderer}）的
 * 词典外六 oid（date/time/timetz/timestamptz/numeric/uuid）与文本族三 oid
 * （json/bpchar/varchar）、字节族 bytea、u32 族 xid/cid 一并收录——本映射与渲染矩阵
 * 的 oid 集合保持一致是值面自洽的前提（kind 解出 datum 字节 → 矩阵渲染）。
 * 工具类不可实例化，静态方法纯函数并发安全。</p>
 */
public final class DecodeKinds {

    /** 非类型 kind：attisdropped 列占位——存储恒 NULL、零字节消耗（spike 发现 26）。 */
    public static final String DROPPED = "dropped";

    /** 非类型 kind：词典外 varlena 列（aclitem[]/text[] 等）——走读消耗字节不取值。 */
    public static final String SKIP = "skip";

    /**
     * 工具类防实例化。
     */
    private DecodeKinds() {
    }

    /**
     * pg_type oid → 解码 kind。
     *
     * <p>关键步骤：switch 覆盖渲染矩阵全集（v1 十一内建 + v2 扩容十 oid：
     * bytea/xid/cid/json/bpchar/varchar/date/time/timetz/timestamptz/numeric/uuid，
     * pg_type.h catalog 起始段固定 oid），其余全部落 default。边界与异常语义：
     * 未知 oid 抛 ISE（消息含 oid 值）；线程约束：纯函数，并发安全。</p>
     *
     * @param oid 列类型的 pg_type oid（atttypid）
     * @return 解码 kind 字符串（本层 {@code TupleDecoder} 的 switch 分支名）
     * @throws IllegalStateException oid 不在内建映射集合
     */
    public static String forTypeOid(long oid) {
        return switch ((int) oid) {
            case 16 -> "bool";        // bool
            case 17 -> "bytea";       // bytea varlena
            case 18 -> "char";        // "char"（内部单字节类型）
            case 19 -> "name";        // NameData 定宽 64B
            case 20 -> "int8";        // int8
            case 21 -> "int2";        // int2
            case 23 -> "int4";        // int4
            case 25, 114, 1042, 1043 -> "text";   // text/json/bpchar/varchar（文本族 varlena）
            case 26, 28, 29 -> "oid"; // oid/xid/cid（u32 无符号语义）
            case 700 -> "float4";     // float4
            case 701 -> "float8";     // float8
            case 1082 -> "date";      // date（i32 天，epoch 2000——datum 字节交渲染矩阵）
            case 1083 -> "time";      // time（i64 微秒）
            case 1114 -> "timestamp"; // timestamp（微秒，epoch 2000）
            case 1184 -> "timestamptz"; // timestamptz（i64 微秒 UTC）
            case 1266 -> "timetz";    // timetz（i64 微秒 + i32 区偏移，12B 定宽）
            case 1700 -> "numeric";   // numeric（varlena 内容，格式头自证短/长/特殊）
            case 2950 -> "uuid";      // uuid（16B 网络序）
            default -> throw new IllegalStateException("unregistered type oid " + oid);
        };
    }
}
