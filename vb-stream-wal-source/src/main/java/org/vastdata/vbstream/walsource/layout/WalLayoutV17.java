package org.vastdata.vbstream.walsource.layout;

/**
 * PG 17（REL_17_STABLE）的 WAL 物理布局描述符——唯一实例 {@link #INSTANCE}，
 * 所有常量转录自 PG 17 源码头文件（REL_17_STABLE 分支）与 live postgres:17
 * 服务端实查，出处逐项标注于各常量。常量全为静态 final，任意线程共享读取安全；
 * kinds 访问器返回防御副本。
 *
 * <p><strong>V17 / V18 差异表（Task 16 头文件 + 活库双源转录，其余常量一致）</strong>：</p>
 * <table border="1">
 *   <caption>V17 vs V18 布局常量差异（Task 16 转录）</caption>
 *   <tr><th>常量</th><th>V17</th><th>V18</th><th>依据</th></tr>
 *   <tr><td>pageMagic</td><td>0xD116</td><td>0xD118</td>
 *       <td>xlog_internal.h：XLOG_PAGE_MAGIC（两分支 diff 实证仅此一行差异）</td></tr>
 *   <tr><td>sizeOfHeapInplace</td><td>2</td><td>20</td>
 *       <td>heapam_xlog.h：V17 {@code SizeOfHeapInplace}=offsetof(offnum)+2（结构仅
 *       offnum u16）；V18 增 dbId/tsId/relcacheInitFileInval/nmsgs/msgs[]
 *       （{@code MinSizeOfHeapInplace}=offsetof(nmsgs)+4）——共享失效消息内联进
 *       记录是 PG 18 增量。两版块 data 区均为新 tuple 版本，重放直读路径不受影响</td></tr>
 *   <tr><td>pgAttributeKinds</td><td>26 项</td><td>25 项</td>
 *       <td>pg_attribute.h：V17 仍含 attcacheoff（int4，词典槽 5）——PG 18 删除
 *       该列；attstattarget 两版同在尾部 varlen 区（"PG 16 及以前在 atttypid 后"
 *       的 spike 记录由此廓清：移位发生在 PG 17，V17/V18 同形）</td></tr>
 *   <tr><td>attrDroppedIndex</td><td>17</td><td>16</td>
 *       <td>attcacheoff 存在使 attisdropped 及其后列整体后移一位</td></tr>
 *   <tr><td>pgClassKinds</td><td>33 项</td><td>34 项</td>
 *       <td>live postgres:17 实查（attnum 升序）：V17 无 relallfrozen——
 *       PG 18 于 relallvisible 后增列 int4</td></tr>
 *   <tr><td>classToastRelidIndex</td><td>12</td><td>13</td>
 *       <td>同上：relallfrozen 缺席使 reltoastrelid 前移一位</td></tr>
 *   <tr><td>pgClassReltoastrelidDataOffset</td><td>108</td><td>112</td>
 *       <td>relfilenode 自占 [88,92)，其后 V17 为 4 个定宽列（tablespace oid +
 *       relpages int4 + reltuples float4 + relallvisible int4）合计 16B 占 [92,108)；
 *       V18 多 relallfrozen int4——两版差恰 4B（审查 High-1 勘误：首版 104 为漏计
 *       relfilenode 自身 4B 的算术错，其假绿由收尾 ALTER 全行 UPD 掩盖）</td></tr>
 *   <tr><td>其余（walBlockSize 8192 / 页头 24、40 / 记录头 24 / heapUpdate 14 /
 *       heapDelete 8 / relfilenode 偏移 88）</td><td colspan="2">一致</td>
 *       <td>xlog_internal.h / xlogrecord.h / heapam_xlog.h 两分支逐 struct diff
 *       实证（heap 家族仅 inplace 一处差异）；17 全矩阵对拍 IT（Wal17SyncIT）端到端复核</td></tr>
 * </table>
 */
public final class WalLayoutV17 implements WalLayout {

    /** 唯一实例：零状态描述符，无并发约束。 */
    public static final WalLayoutV17 INSTANCE = new WalLayoutV17();

    /** 页魔数 0xD116 // xlog_internal.h (REL_17_STABLE)：XLOG_PAGE_MAGIC。 */
    private static final int PAGE_MAGIC = 0xD116;
    /** 页大小 8192 // xlog_internal.h (REL_17_STABLE)：XLOG_BLCKSZ（2 的幂，与 V18 一致）。 */
    private static final int WAL_BLOCK_SIZE = 8192;
    /** 短页头 24 // xlog_internal.h (REL_17_STABLE)：MAXALIGN(sizeof(20B header))，与 V18 一致。 */
    private static final int SHORT_PAGE_HEADER_SIZE = 24;
    /** 长页头 40 // xlog_internal.h (REL_17_STABLE)：MAXALIGN(sizeof(36B header))，与 V18 一致。 */
    private static final int LONG_PAGE_HEADER_SIZE = 40;
    /** 记录头 24 // xlogrecord.h (REL_17_STABLE)：SizeOfXLogRecord = offsetof(xl_crc)+4
     * （tot_len u32 + xid u32 + prev u64 + info u8 + rmid u8 + pad2 + crc u32），与 V18 一致。 */
    private static final int RECORD_HEADER_SIZE = 24;
    /** xl_heap_update 前缀 14 // heapam_xlog.h (REL_17_STABLE)：SizeOfHeapUpdate =
     * offsetof(new_offnum)+2（old_xmax u32 + old_offnum u16 + old_infobits u8 + flags u8 +
     * new_xmax u32 + new_offnum u16），与 V18 一致（两分支该 struct 逐字段相同）。 */
    private static final int SIZE_OF_HEAP_UPDATE = 14;
    /** xl_heap_delete 前缀 8 // heapam_xlog.h (REL_17_STABLE)：SizeOfHeapDelete =
     * offsetof(flags)+1（xmax u32 + offnum u16 + infobits u8 + flags u8），与 V18 一致。 */
    private static final int SIZE_OF_HEAP_DELETE = 8;
    /** xl_heap_inplace 固定前缀 2 // heapam_xlog.h (REL_17_STABLE)：SizeOfHeapInplace =
     * offsetof(offnum)+sizeof(OffsetNumber)——V17 结构仅 offnum u16；dbId/tsId/
     * relcacheInitFileInval/nmsgs/msgs[]（柔性共享失效消息）是 PG 18 增量
     * （V18 MinSizeOfHeapInplace=20）。块 data 区（新 tuple 版本）两版同形，
     * 重放直读不消费 main 区 msgs。 */
    private static final int SIZE_OF_HEAP_INPLACE = 2;

    /** pg_class 数据区 relfilenode 偏移 88——推算依据同 V18：列 1-7（oid 4B + name 64B
     * + 5×oid 20B）定宽合计 88，relfilenode 紧随其后；live postgres:17 实查列序一致。 */
    private static final int PGCLASS_RELFILENODE_DATA_OFFSET = 88;
    /** pg_class 数据区 reltoastrelid 偏移 108——推算依据（审查 High-1 勘误：relfilenode
     * 自占 [88,92)）：其后 4 个定宽列（reltablespace oid + relpages int4 + reltuples
     * float4 + relallvisible int4）合计 16B 占 [92,108)，reltoastrelid@108；V18 多
     * relallfrozen int4 为 112——两版差恰 4B（单列差），88 锚定法以此自检。 */
    private static final int PGCLASS_RELTOASTRELID_DATA_OFFSET = 108;

    /** pg_class 数据区 relkind 偏移 115——推算依据：reltoastrelid 自占 [108,112)，
     * 其后 relhasindex bool + relisshared bool + relpersistence char 各 1B 占
     * [112,115)，relkind 紧随（V18 为 119——两版差恰 4B 与 toast 偏移自洽）。 */
    private static final int PGCLASS_RELKIND_DATA_OFFSET = 115;

    /** pg_attribute 词典槽位：attisdropped=17（V17 含 attcacheoff@5，其后列整体后移）。 */
    private static final int ATTR_DROPPED_INDEX = 17;

    /** pg_class 词典槽位：reltoastrelid=12（V17 无 relallfrozen，较 V18 前移一位）。 */
    private static final int CLASS_TOAST_RELID_INDEX = 12;

    /** pg_class 词典槽位：relkind=16（relpersistence@15 后一位；V18 增 relallfrozen
     * 使其后移至 17——数据区偏移 V17 115 / V18 119，差恰 4B 与 toast 偏移自洽）。 */
    private static final int CLASS_RELKIND_INDEX = 16;

    /**
     * pg_attribute 列解码词典（26 项），逐行转录自 pg_attribute.h (REL_17_STABLE)
     * CATALOG 定义 + live postgres:17 实查（两源逐列核对一致）。与 V18 的 25 项相比
     * 恰多 attcacheoff（int4，词典槽 5——PG 18 删除）；attstattarget 两版同在尾部
     * varlen 区（V18 kinds[20] / V17 kinds[21]，可空 int2）。
     */
    private static final String[] PG_ATTRIBUTE_KINDS = {
            "oid",   // attrelid
            "name",  // attname (NameData, 64B fixed)
            "oid",   // atttypid
            "int2",  // attlen
            "int2",  // attnum
            "int4",  // attcacheoff (V17 独有, PG 18 删除)
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
            "bool",  // attisdropped         <-- 槽 17
            "bool",  // attislocal
            "int2",  // attinhcount
            "oid",   // attcollation
            "int2",  // attstattarget (nullable, varlen 区首列)
            "skip",  // attacl aclitem[]
            "skip",  // attoptions text[]
            "skip",  // attfdwoptions text[]
            "skip",  // attmissingval anyarray
    };

    /**
     * pg_class 列解码词典（33 项），逐行转录自 live postgres:17 服务端 pg_attribute
     * 于 pg_class 的行（attnum 升序）。与 V18 的 34 项相比恰缺 relallfrozen
     * （int4，V18 词典槽 12——PG 18 于 relallvisible 后增列）；relfilenode 在
     * 词典槽 7（与 V18 一致）、reltoastrelid 在词典槽 12（V18 为 13）。
     */
    private static final String[] PG_CLASS_KINDS = {
            "oid",   // oid (catalogs store oid as a regular first column)
            "name",  // relname
            "oid",   // relnamespace
            "oid",   // reltype
            "oid",   // reloftype
            "oid",   // relowner
            "oid",   // relam
            "oid",   // relfilenode          <-- index 7（与 V18 一致）
            "oid",   // reltablespace
            "int4",  // relpages
            "float4",// reltuples
            "int4",  // relallvisible
            "oid",   // reltoastrelid        <-- index 12（V18 为 13）
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
    private WalLayoutV17() {
    }

    /** {@inheritDoc}——PG 17 页魔数 0xD116（XLOG_PAGE_MAGIC，REL_17_STABLE）。 */
    @Override
    public int pageMagic() {
        return PAGE_MAGIC;
    }

    /** {@inheritDoc}——8192，2 的幂（按位与取页内偏移的前提），与 V18 一致。 */
    @Override
    public int walBlockSize() {
        return WAL_BLOCK_SIZE;
    }

    /** {@inheritDoc}——24，与 V18 一致。 */
    @Override
    public int shortPageHeaderSize() {
        return SHORT_PAGE_HEADER_SIZE;
    }

    /** {@inheritDoc}——40（XLP_LONG_HEADER 置位的段首页），与 V18 一致。 */
    @Override
    public int longPageHeaderSize() {
        return LONG_PAGE_HEADER_SIZE;
    }

    /** {@inheritDoc}——24（SizeOfXLogRecord），与 V18 一致。 */
    @Override
    public int recordHeaderSize() {
        return RECORD_HEADER_SIZE;
    }

    /** {@inheritDoc}——14（SizeOfHeapUpdate = offsetof(new_offnum)+2），与 V18 一致。 */
    @Override
    public int sizeOfHeapUpdate() {
        return SIZE_OF_HEAP_UPDATE;
    }

    /** {@inheritDoc}——8（SizeOfHeapDelete），与 V18 一致。 */
    @Override
    public int sizeOfHeapDelete() {
        return SIZE_OF_HEAP_DELETE;
    }

    /** {@inheritDoc}——2（SizeOfHeapInplace = offsetof(offnum)+2，V17 结构仅 offnum）。 */
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

    /** {@inheritDoc}——88（列 1-7 定宽 4+64+20 合计，与 V18 一致）。 */
    @Override
    public int pgClassRelfilenodeDataOffset() {
        return PGCLASS_RELFILENODE_DATA_OFFSET;
    }

    /** {@inheritDoc}——108（relfilenode 自占 [88,92) 后 4 个定宽列合计 16B；V18 为 112，
     *  两版差恰 4B）。 */
    @Override
    public int pgClassReltoastrelidDataOffset() {
        return PGCLASS_RELTOASTRELID_DATA_OFFSET;
    }

    /** {@inheritDoc}——115（reltoastrelid@108 自占 4B 后三个单字节列；V18 为 119）。 */
    @Override
    public int pgClassRelkindDataOffset() {
        return PGCLASS_RELKIND_DATA_OFFSET;
    }

    /** {@inheritDoc}——17（attcacheoff@5 使 attisdropped 后移一位；V18 为 16）。 */
    @Override
    public int attrDroppedIndex() {
        return ATTR_DROPPED_INDEX;
    }

    /** {@inheritDoc}——12（V17 无 relallfrozen；V18 为 13）。 */
    @Override
    public int classToastRelidIndex() {
        return CLASS_TOAST_RELID_INDEX;
    }

    /** {@inheritDoc}——16（relpersistence@15 后一位；V18 为 17）。 */
    @Override
    public int classRelkindIndex() {
        return CLASS_RELKIND_INDEX;
    }

    /** {@inheritDoc}——170000-179999 全区间（含小版本位）。 */
    @Override
    public boolean supports(int pgVersionNum) {
        return pgVersionNum >= 170000 && pgVersionNum < 180000;
    }

    /** {@inheritDoc}——17。 */
    @Override
    public int majorVersion() {
        return 17;
    }
}
