package org.vastdata.vbstream.walsource.layout;

/**
 * PG 18（REL_18_STABLE）的 WAL 物理布局描述符——唯一实例 {@link #INSTANCE}，
 * 所有常量转录自 PG 18 源码头文件与 live PG 18.6 服务端实测（spike
 * wal-parse 实证锚定），出处逐项标注于各常量。常量全为静态 final，
 * 任意线程共享读取安全；kinds 访问器返回防御副本。
 */
public final class WalLayoutV18 implements WalLayout {

    /** 唯一实例：零状态描述符，无并发约束。 */
    public static final WalLayoutV18 INSTANCE = new WalLayoutV18();

    /** 页魔数 0xD118 // xlog_internal.h (REL_18_STABLE)：XLOG_PAGE_MAGIC。 */
    private static final int PAGE_MAGIC = 0xD118;
    /** 页大小 8192 // xlog_internal.h (REL_18_STABLE)：XLOG_BLCKSZ（2 的幂）。 */
    private static final int WAL_BLOCK_SIZE = 8192;
    /** 短页头 24 // xlog_internal.h (REL_18_STABLE)：MAXALIGN(sizeof(20B header))。 */
    private static final int SHORT_PAGE_HEADER_SIZE = 24;
    /** 长页头 40 // xlog_internal.h (REL_18_STABLE)：MAXALIGN(sizeof(36B header))。 */
    private static final int LONG_PAGE_HEADER_SIZE = 40;
    /** 记录头 24 // xlogrecord.h (REL_18_STABLE)：SizeOfXLogRecord。 */
    private static final int RECORD_HEADER_SIZE = 24;
    /** xl_heap_update 前缀 14 // heapam_xlog.h (REL_18_STABLE)：offsetof(new_offnum)+2，spike 实测锚。 */
    private static final int SIZE_OF_HEAP_UPDATE = 14;
    /** xl_heap_delete 前缀 8 // heapam_xlog.h (REL_18_STABLE)。 */
    private static final int SIZE_OF_HEAP_DELETE = 8;
    /** xl_heap_inplace 固定前缀 20 // heapam_xlog.h (REL_18_STABLE)：MinSizeOfHeapInplace = offsetof(msgs)——
     * offnum u16@0 + pad2 + dbId u32@4 + tsId u32@8 + relcacheInitFileInval bool@12 + pad3 + nmsgs int@16，
     * 柔性 msgs[] 起于 20（非 offsetof(offnum)+sizeof(uint16) 的首字段尺寸 2）。 */
    private static final int SIZE_OF_HEAP_INPLACE = 20;

    /** pg_class 数据区 relfilenode 偏移 88——推算依据（非头文件直出处）：pg_class 列 1-7
     * （oid 4B + name 64B + 5×oid 20B）定宽合计 4+64+20=88，relfilenode 紧随其后；
     * spike replayInplace（WalParseSpike）以 dataOff+88 实读新 relfilenode 实证锚定。 */
    private static final int PGCLASS_RELFILENODE_DATA_OFFSET = 88;
    /** pg_class 数据区 reltoastrelid 偏移 112——推算依据：relfilenode 自占 [88,92)，
     *其后 5 个定宽列（reltablespace oid + relpages int4 + reltuples float4 +
     * relallvisible int4 + relallfrozen int4）各 4B 合计 20B 占 [92,112)（审查 Low-5
     * 勘误：首版注释误书"合计 24B"，5×4=20——88 锚定法以"两版差恰单列宽"自检）；
     * spike replayInplace 以 dataOff+112 实读新 reltoastrelid 实证锚定。 */
    private static final int PGCLASS_RELTOASTRELID_DATA_OFFSET = 112;

    /** pg_attribute 词典槽位：attisdropped=16（V18 无 attcacheoff——PG 18 删除该列）。 */
    private static final int ATTR_DROPPED_INDEX = 16;

    /** pg_class 词典槽位：reltoastrelid=13（relallfrozen@12 后移一位，PG 18 增列）。 */
    private static final int CLASS_TOAST_RELID_INDEX = 13;

    /**
     * pg_attribute 列解码词典（25 项），逐行转录自 spike PGATTR_KINDS，
     * 原始出处 pg_attribute.h (REL_18_STABLE)。注意 attstattarget 在 PG 17 起
     * 移入尾部 varlen 区（≤16 紧跟 atttypid）——大版本漂移的活例。
     */
    private static final String[] PG_ATTRIBUTE_KINDS = {
            "oid",   // attrelid
            "name",  // attname (NameData, 64B fixed)
            "oid",   // atttypid
            "int2",  // attlen
            "int2",  // attnum
            "int4",  // atttypmod
            "int2",  // attndims
            "bool",  // attbyval
            "char",  // attalign
            "char",  // attstorage
            "char",  // attcompression
            "bool",  // attnotnull
            "bool",  // atthasdef
            "bool",  // atthasmissing
            "char",  // attidentity
            "char",  // attgenerated
            "bool",  // attisdropped
            "bool",  // attislocal
            "int2",  // attinhcount
            "oid",   // attcollation
            "int2",  // attstattarget (nullable)
            "skip",  // attacl aclitem[]
            "skip",  // attoptions text[]
            "skip",  // attfdwoptions text[]
            "skip",  // attmissingval anyarray
    };

    /**
     * pg_class 列解码词典（34 项），逐行转录自 spike PGCLASS_KINDS，原始出处为
     * live PG 18.6 服务端 pg_attribute 于 pg_class 的行（attnum 升序）。
     * relfilenode 在 index 7、reltoastrelid 在 index 13（两解码锚列）。
     */
    private static final String[] PG_CLASS_KINDS = {
            "oid",   // oid (catalogs store oid as a regular first column)
            "name",  // relname
            "oid",   // relnamespace
            "oid",   // reltype
            "oid",   // reloftype
            "oid",   // relowner
            "oid",   // relam
            "oid",   // relfilenode          <-- index 7
            "oid",   // reltablespace
            "int4",  // relpages
            "float4",// reltuples
            "int4",  // relallvisible
            "int4",  // relallfrozen
            "oid",   // reltoastrelid        <-- index 13
            "bool",  // relhasindex
            "bool",  // relisshared
            "char",  // relpersistence
            "char",  // relkind
            "int2",  // relnatts
            "int2",  // relchecks
            "bool",  // relhasrules
            "bool",  // relhastriggers
            "bool",  // relhassubclass
            "bool",  // relrowsecurity
            "bool",  // relforcerowsecurity
            "bool",  // relispopulated
            "char",  // relreplident
            "bool",  // relispartition
            "oid",   // relrewrite
            "int4",  // relfrozenxid (xid)
            "int4",  // relminmxid (multi-xid)
            "skip",  // relacl aclitem[]
            "skip",  // reloptions text[]
            "skip",  // relpartbound pg_node_tree
    };

    /** 私有构造器：单例描述符，经 {@link #INSTANCE} 访问。 */
    private WalLayoutV18() {
    }

    /** {@inheritDoc}——PG 18 页魔数 0xD118。 */
    @Override
    public int pageMagic() {
        return PAGE_MAGIC;
    }

    /** {@inheritDoc}——8192，2 的幂（按位与取页内偏移的前提）。 */
    @Override
    public int walBlockSize() {
        return WAL_BLOCK_SIZE;
    }

    /** {@inheritDoc}——24。 */
    @Override
    public int shortPageHeaderSize() {
        return SHORT_PAGE_HEADER_SIZE;
    }

    /** {@inheritDoc}——40（XLP_LONG_HEADER 置位的段首页）。 */
    @Override
    public int longPageHeaderSize() {
        return LONG_PAGE_HEADER_SIZE;
    }

    /** {@inheritDoc}——24（SizeOfXLogRecord）。 */
    @Override
    public int recordHeaderSize() {
        return RECORD_HEADER_SIZE;
    }

    /** {@inheritDoc}——14（offsetof(new_offnum)+2，spike 实测锚）。 */
    @Override
    public int sizeOfHeapUpdate() {
        return SIZE_OF_HEAP_UPDATE;
    }

    /** {@inheritDoc}——8。 */
    @Override
    public int sizeOfHeapDelete() {
        return SIZE_OF_HEAP_DELETE;
    }

    /** {@inheritDoc}——20（MinSizeOfHeapInplace = offsetof(msgs)）。 */
    @Override
    public int sizeOfHeapInplace() {
        return SIZE_OF_HEAP_INPLACE;
    }

    /** {@inheritDoc}——返回防御副本，调用方改写不污染静态词典。 */
    @Override
    public String[] pgAttributeKinds() {
        return PG_ATTRIBUTE_KINDS.clone();
    }

    /** {@inheritDoc}——返回防御副本，调用方改写不污染静态词典。 */
    @Override
    public String[] pgClassKinds() {
        return PG_CLASS_KINDS.clone();
    }

    /** {@inheritDoc}——88（列 1-7 定宽 4+64+20 合计，spike replayInplace 实证）。 */
    @Override
    public int pgClassRelfilenodeDataOffset() {
        return PGCLASS_RELFILENODE_DATA_OFFSET;
    }

    /** {@inheritDoc}——16（attisdropped，pg_attribute.h (REL_18_STABLE) 列序）。 */
    @Override
    public int attrDroppedIndex() {
        return ATTR_DROPPED_INDEX;
    }

    /** {@inheritDoc}——13（reltoastrelid；live PG 18.6 pg_attribute 于 pg_class 的行序）。 */
    @Override
    public int classToastRelidIndex() {
        return CLASS_TOAST_RELID_INDEX;
    }

    /** {@inheritDoc}——112（relfilenode@88 后 5 个定宽列各 4B 合计 20B，spike 实证）。 */
    @Override
    public int pgClassReltoastrelidDataOffset() {
        return PGCLASS_RELTOASTRELID_DATA_OFFSET;
    }

    /** {@inheritDoc}——180000-189999 全区间（含小版本位）。 */
    @Override
    public boolean supports(int pgVersionNum) {
        return pgVersionNum >= 180000 && pgVersionNum < 190000;
    }

    /** {@inheritDoc}——18。 */
    @Override
    public int majorVersion() {
        return 18;
    }
}
