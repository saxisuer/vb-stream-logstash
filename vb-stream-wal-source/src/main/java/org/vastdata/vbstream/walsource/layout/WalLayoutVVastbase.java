package org.vastdata.vbstream.walsource.layout;

/**
 * VastBase G100 V3.0（基于 openGauss 6.0.0 内核，2026-03-31 Build 9）第四布局档:
 * 记录层与 OG 5.0.0 基本同构（页魔数/记录头/块引用——132019 条真实 WAL
 * 记录零 resync 实证，spec §0.2），两处记录层增量与词典差异——
 * ① heap UPDATE/DELETE 的 main 前缀较 OG 再加 xid u64 尾（见
 * {@link #sizeOfHeapUpdate()}/{@link #sizeOfHeapDelete()}，2026-10-13 T6 实测锚）；
 * ② A 档 {@code date} 落私有类型 {@code oradate}=9040（见
 * {@link #remapDmlTypeOid(long)}）；其余差异集中在 catalog 词典几何——
 * ① NAMEDATALEN 64→128（name 类型全 catalog 加宽，读区偏移统一 +64B）；
 * ② pg_class 42 列（OG 40）：attnum 25 插 relhassecids，尾部增 relminmxid/relrewrite；
 * ③ pg_attribute 29 列（OG 24）：attnum 20/21 插 attalias/attisquoted，尾部增
 * attidentity/attextraopt/attdroppedname。
 * 全部列序/attlen 锚定 2026-10-13 真库实查（spec 附录 A）。判别经 relname attlen
 * 探测（WalLayouts 三参重载）；majorVersion=6 使 v5 检查点拒载回落全新引导。
 * <b>槽位口径</b>：本档 {@link #pgClassKinds()} 数组自带 oidhdr 前导槽使数组槽
 * 恰等于 attnum；护栏访问器（{@link #classParttypeIndex()} 等）沿用 OG 的
 * "attnum-1 物理槽"口径，消费面 +{@code classValsSlotDelta()}(=1，继承) 落回
 * attnum 值行槽位——与 OG 档同式桥接（{@code CatalogReplay.classDecodeKinds()}
 * 对已带前导槽的词典不再二次注入）。
 * 线程约束:无状态单例,并发安全。
 */
public final class WalLayoutVVastbase extends WalLayoutVOpenGauss {

    /** 单例。 */
    public static final WalLayoutVVastbase INSTANCE = new WalLayoutVVastbase();

    /** pg_attribute 词典（29 项,无 oidhdr——元组无 oid）,列序 2026-10-13 实查。 */
    private static final String[] PG_ATTRIBUTE_KINDS = {
            "oid",     // attrelid
            "name128", // attname(VastBase NAMEDATALEN 128)
            "oid",     // atttypid
            "int4",    // attstattarget(PG≤16 位次)
            "int2",    // attlen
            "int2",    // attnum                  <-- 槽 5(继承 attrAttnumIndex)
            "int4",    // attndims
            "int4",    // attcacheoff
            "int4",    // atttypmod
            "bool",    // attbyval
            "char",    // attstorage
            "char",    // attalign
            "bool",    // attnotnull
            "bool",    // atthasdef
            "bool",    // attisdropped           <-- 槽 14(继承 attrDroppedIndex)
            "bool",    // attislocal
            "int1",    // attcmprmode
            "int4",    // attinhcount
            "oid",     // attcollation            ---- 分叉点(槽 19 起)----
            "name128", // attalias(VastBase 插入,实查 typid 19/attlen 128)
            "bool",    // attisquoted(VastBase 插入)
            "skip",    // attacl
            "skip",    // attoptions
            "skip",    // attfdwoptions
            "skip",    // attinitdefval
            "int1",    // attkvtype
            "char",    // attidentity(VastBase 插入,typid 18)
            "int8",    // attextraopt(VastBase 插入,typid 20/attlen 8)
            "name128", // attdroppedname(VastBase 插入)
    };

    /** pg_class 词典（oidhdr + 42 项 = 43,数组槽 = attnum——oidhdr 前导）。 */
    private static final String[] PG_CLASS_KINDS = {
            TupleDecoder.OIDHDR_KIND, // 元组头 oid(9.2 式,继承 classOidInTupleHeader)
            "name128", // relname(VastBase NAMEDATALEN 128)
            "oid",     // relnamespace
            "oid",     // reltype
            "oid",     // reloftype
            "oid",     // relowner
            "oid",     // relam
            "oid",     // relfilenode            <-- attnum 7
            "oid",     // reltablespace
            "float8",  // relpages
            "float8",  // reltuples
            "int4",    // relallvisible
            "oid",     // reltoastrelid          <-- attnum 12
            "oid",     // reltoastidxid
            "oid",     // reldeltarelid          <-- 物理槽 13(护栏,分叉前)
            "oid",     // reldeltaidx
            "oid",     // relcudescrelid         <-- 物理槽 15(护栏,分叉前)
            "oid",     // relcudescidx
            "bool",    // relhasindex
            "bool",    // relisshared
            "char",    // relpersistence
            "char",    // relkind                <-- attnum 21
            "int2",    // relnatts
            "int2",    // relchecks
            "bool",    // relhasoids             ---- 分叉点(槽 24/attnum 25 起)----
            "bool",    // relhassecids(VastBase 插入)
            "bool",    // relhaspkey
            "bool",    // relhasrules
            "bool",    // relhastriggers
            "bool",    // relhassubclass
            "int1",    // relcmprs
            "bool",    // relhasclusterkey
            "bool",    // relrowmovement
            "char",    // parttype               <-- attnum 33(OG 32;物理槽 32)
            "int4",    // relfrozenxid
            "skip",    // relacl
            "text",    // reloptions             <-- 物理槽 35(护栏;UStore/段页式 sniff 面)
            "char",    // relreplident
            "int8",    // relfrozenxid64
            "oid",     // relbucket              <-- 物理槽 38(护栏)
            "skip",    // relbucketkey
            "int8",    // relminmxid             <-- attnum 41(OG 40)
            "oid",     // relrewrite(VastBase 新增尾列)
    };

    /** 私有构造器:单例描述符。 */
    private WalLayoutVVastbase() {
    }

    /** {@inheritDoc}——29 项(OG 24 + attalias/attisquoted 尾部三列,槽 19 起分叉)。 */
    @Override
    public String[] pgAttributeKinds() {
        return PG_ATTRIBUTE_KINDS.clone();
    }

    /** {@inheritDoc}——43 项(oidhdr + 42;槽 24 起分叉,读区偏移 +64)。 */
    @Override
    public String[] pgClassKinds() {
        return PG_CLASS_KINDS.clone();
    }

    /** {@inheritDoc}——148(relname 128 + 5×oid;OG 84 + 64,实查列宽推演)。 */
    @Override
    public int pgClassRelfilenodeDataOffset() {
        return 148;
    }

    /** {@inheritDoc}——180(148 + reltablespace 4 + float8×2 d 对齐 + relallvisible 4)。 */
    @Override
    public int pgClassReltoastrelidDataOffset() {
        return 180;
    }

    /** {@inheritDoc}——207(180 + oid×5 + bool×2 + char;类注释锚,投影走词典槽 21)。 */
    @Override
    public int pgClassRelkindDataOffset() {
        return 207;
    }

    /** {@inheritDoc}——32(attnum 33;relhassecids 插入使 OG 31 + 1——流内存储护栏判别面)。 */
    @Override
    public int classParttypeIndex() {
        return 32;
    }

    /** {@inheritDoc}——35(OG 34 + 1;UStore/段页式 reloptions sniff 判别面)。 */
    @Override
    public int classReloptionsIndex() {
        return 35;
    }

    /** {@inheritDoc}——38(OG 37 + 1;bucket/段页式判别面)。 */
    @Override
    public int classRelbucketIndex() {
        return 38;
    }

    /** {@inheritDoc}——128(VastBase NAMEDATALEN 加宽;V17/V18/OG 64)。 */
    @Override
    public int nameDatumWidth() {
        return 128;
    }

    /** {@inheritDoc}——6(档号延续:OG=5;v5 检查点在本档拒载回落全新引导)。 */
    @Override
    public int majorVersion() {
        return 6;
    }

    /** {@inheritDoc}——90204(VastBase 与 OG/vanilla 9.2 撞号;档位判别在 WalLayouts 探测)。 */
    @Override
    public boolean supports(int pgVersionNum) {
        return pgVersionNum == 90204;
    }

    /**
     * {@inheritDoc}——21（T6 2026-10-13 真库实测锚，vastdump 探针 UPDATE info=0x40）：
     * VastBase 的 xl_heap_update = OG 老结构 5B（old@0/new@2/flags@4——字段偏移与
     * OG 等同，{@code HeapViews} OG 分支零改动）+ LogCSN u64 + <b>xid u64 殿后</b>，
     * 且 xid 尾无条件在场（OG 仅 INIT_PAGE 变体多 pd_xid_base 前缀、无常态 xid 尾）
     * ——旧行 tuple 载荷起点 main+21（以 13 读会落在 xid 尾中段，natts 读出垃圾）。
     * INIT_PAGE 变体（info 0x80 位）在此基础上再前置 pd_xid_base u64（+8——INSERT
     * 探针实证该前缀形态，UPDATE 同构推导），旧元组偏移的 shift 由 XactGrouper 补。
     */
    @Override
    public int sizeOfHeapUpdate() {
        return 21;
    }

    /**
     * {@inheritDoc}——19（T6 2026-10-13 真库实测锚，vastdump 探针 DELETE info=0x10）：
     * xl_heap_delete = OG 老结构 3B（offnum@0/flags@2——字段偏移与 OG 等同）+
     * LogCSN u64 + xid u64 殿后；旧行 tuple 载荷起点 main+19。INIT_PAGE 变体同
     * {@link #sizeOfHeapUpdate()} 的 +8 形态。
     */
    @Override
    public int sizeOfHeapDelete() {
        return 19;
    }

    /**
     * {@inheritDoc}——9040（oradate）→ 1114（timestamp）：VastBase A 档 DDL 关键字
     * {@code date} 映射私有类型 {@code oradate}（oid 9040，typlen=8——2026-10-13
     * 真库 pg_type 实查），磁盘表示与 timestamp 同构（8B epoch-2000 微秒，vastkind
     * 探针以 timestamp kind 解码 INSERT 元组逐值正确实证）——DML 面归一到 timestamp
     * 后 {@code DecodeKinds}/{@code DiskValueRenderer} 零改动。Oracle DATE 整秒粒度
     * （微秒输入服务端截断）使渲染恒无小数秒，与基准路 oradate_out 文本天然同形。
     * catalog 面 atttypid 保持原值 9040（AttrRow 对拍 JDBC 不受本重映射影响）。
     */
    @Override
    public long remapDmlTypeOid(long typeOid) {
        return typeOid == 9040 ? 1114 : typeOid;
    }
}
