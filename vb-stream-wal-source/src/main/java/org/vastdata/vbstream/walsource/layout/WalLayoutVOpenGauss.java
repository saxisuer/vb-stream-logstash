package org.vastdata.vbstream.walsource.layout;

/**
 * openGauss 5.0.0 的 WAL 物理布局描述符——唯一实例 {@link #INSTANCE}。常量转录自
 * openGauss-server v5.0.0 源码头文件与 live opengauss:5.0.0 容器实测（2026-10-12
 * og-plan 容器，pg_read_binary_file 字节面锚定），出处逐项标注于各常量。常量全为
 * 静态 final，任意线程共享读取安全；kinds 访问器返回防御副本。
 * heap 记录前缀（sizeOfHeapUpdate/Delete/Inplace）已由 Task 2 真实语料锚定
 * （常量注记含消费序与锚 LSN），不再 ISE。
 *
 * <p><strong>与任务书原值的勘误（字节实测推翻计划期推导，详见任务报告）</strong>：</p>
 * <table border="1">
 *   <caption>计划值 vs 实测值（og-plan 容器元组/WAL 字节锚）</caption>
 *   <tr><th>常量</th><th>计划值</th><th>实测值</th><th>勘误依据</th></tr>
 *   <tr><td>pgClassKinds</td><td>41 项</td><td>40 项</td>
 *       <td>catalog 元组 oid 在 9.2 式元组头（[t_hoff-4,t_hoff)，实锚 t_offprobe 行
 *       oid=16389@28/t_hoff=32）而非数据区首列；infomask2 低 11 位 natts=40 实锚；
 *       数据区首列即 relname@0（元组字节直读）——V17/V18 的 kinds[0]="oid" 口径
 *       （PG 12+ oid 降为普通数据列）在 OG 不成立</td></tr>
 *   <tr><td>pgClassReltoastrelidDataOffset</td><td>108</td><td>116</td>
 *       <td>计划按 vanilla 列宽（int4/float4）推导；OG 实查 relpages/reltuples 均
 *       float8（attlen=8，live pg_attribute 实查），读区后移净 8B；实测元组
 *       reltoastrelid=16392 恰在 dataOff+116</td></tr>
 *   <tr><td>pgClassRelfilenodeDataOffset</td><td>88（沿 V17）</td><td>84</td>
 *       <td>同 oid-in-header 勘误：数据区少 4B 头部 oid 列；实测 relfilenode=16389
 *       在 dataOff+84（与调研报告 §1.1 的 9.2 拼装口径一致）</td></tr>
 *   <tr><td>pgClassRelkindDataOffset</td><td>135（差 20）</td><td>143（差 28）</td>
 *       <td>五个 oid 列（20B）+ float8 加宽（8B）；实测 relkind='r'@143、
 *       relnatts=2@144 自洽</td></tr>
 *   <tr><td>relNodeEncodedSize</td><td>16</td><td>14</td>
 *       <td>Task 2 语料字节验证（本任务显式验收项）：Decoder 消费序为
 *       RelFileNodeOld 12B + extra_flag u16；16B RelFileNode（bucketNode/opt）
 *       只随块 id 的 BKID_HAS_BUCKET_OR_SEGPAGE(0x80) 扩展位出现（该位
 *       fail-fast 拒绝）。计划期的 16 系按 bucket 变体推定——语料 283 条全量
 *       走读在 14 下不变量全成立、16 下即断裂</td></tr>
 * </table>
 *
 * <p><strong>Task 2 语料锚定补充（2026-10-12，og-plan 32KB 窗口 / 283 条记录）</strong>：
 * sizeOfHeapUpdate=13（SizeOfOldHeapUpdate 5 + LogCSN 8——HOT_UPDATE 0x2359908
 * 旧行 xl_heap_header 恰在 main+13）、sizeOfHeapDelete=11（同构，DELETE
 * 0x2359AD8）、sizeOfHeapInplace=2（heap_inplace_update 不记 CSN）；块引用消费序
 * xlogreader.cpp DecodeXLogRecord：定位器 12+2、blkno 后 last_lsn u64、镜像头
 * 4B 仅洞字段（2 条 FPI 记录实测）——常量注记逐项含出处。</p>
 *
 * <p>WAL 面常量（magic/页头/记录头）与任务书一致，且经段页字节链式走读复核：
 * 15 条记录 MAXALIGN 步进下 prev 链逐条自洽（pageaddr+off 对齐）。</p>
 */
public final class WalLayoutVOpenGauss implements WalLayout {

    /** 唯一实例：零状态描述符，无并发约束。 */
    public static final WalLayoutVOpenGauss INSTANCE = new WalLayoutVOpenGauss();

    /** 页魔数 0xD074 // openGauss v5.0.0 src/include/access/xlog_basic.h：XLOG_PAGE_MAGIC
     * （OG 私有值——9.2 基线后 WAL 格式自增；段首字节实测 74 d0，段页/数据页多页复核）。 */
    private static final int PAGE_MAGIC = 0xD074;
    /** 页大小 8192 // openGauss v5.0.0 xlog_basic.h：XLOG_BLCKSZ（段文件 16MB / 8192 几何实测）。 */
    private static final int WAL_BLOCK_SIZE = 8192;
    /** 短页头 24 // openGauss v5.0.0 xlog_basic.h：XLogPageHeaderData——magic u16@0 +
     * info u16@2 + tli u32@4 + pageaddr u64@8 + xlp_rem_len u32@16（回移植 9.3+）+
     * OG 私有 xlp_total_len u32@20，MAXALIGN=24（数据页实测记录起于页内偏移 24）。 */
    private static final int SHORT_PAGE_HEADER_SIZE = 24;
    /** 长页头 40 // openGauss v5.0.0 xlog_basic.h：XLogLongPageHeaderData——短头 +
     * sysid u64@24 + tli/seg_size u32@32/36（段首页实测 sysid=0xadf99cc1b0c1774）。 */
    private static final int LONG_PAGE_HEADER_SIZE = 40;
    /** 记录头 32 // openGauss v5.0.0 xlog_basic.h：XLogRecord——tot_len u32@0 + term u32@4
     * （单机实测 0x1）+ xid u32@8 + pad u32@12 + prev u64@16（8 对齐垫 4B）+ info u8@24 +
     * rmid u8@25 + bucket_id u16@26 + crc u32@28；编译对齐实测 32（15 条记录链式走读
     * prev 链逐条自洽）。 */
    private static final int RECORD_HEADER_SIZE = 32;
    /** 记录头 xid 偏移 8 // xlog_basic.h：XLogRecord（term u32 前置使 xid 后移，实测）。 */
    private static final int RECORD_XID_OFFSET = 8;
    /** 记录头 info 偏移 24 // xlog_basic.h：XLogRecord（prev u64@16 之后，实测）。 */
    private static final int RECORD_INFO_OFFSET = 24;
    /** 记录头 rmid 偏移 25 // xlog_basic.h：XLogRecord（紧随 info，实测）。 */
    private static final int RECORD_RMD_OFFSET = 25;
    /** 关系定位器伴随区宽 14 // openGauss v5.0.0 xlogreader.cpp DecodeXLogRecord：
     * 常规表定位器 = RelFileNodeOld（relfilenode.h，spc/db/rel u32×3=12B）+
     * 紧随的 extra_flag u16；bucket/段页表的 16B RelFileNode（bucketNode int2 +
     * opt uint2）只在块 id 置 BKID_HAS_BUCKET_OR_SEGPAGE(0x80) 时出现——该扩展位
     * 由 {@link #rejectBlockIdExtensionBits()} fail-fast 拒绝，不走本宽度。
     * 语料实测（2026-10-12 og-plan 容器，32KB 窗口 283 条记录全量走读：定位器
     * spc=1663/db=14806/rel=16395 逐字节干净、不变量全成立）——Task 1 期间按
     * 源码推定的 16B 系 bucket 变体的宽度，常规表实为 12+2。 */
    private static final int RELNODE_ENCODED_SIZE = 14;

    /** 块引用 blkno 后 last_lsn u64 宽 8 // openGauss v5.0.0 xloginsert.cpp
     * XLogRecordAssemble 逐块追加（SAME_REL 块同有）、xlogreader.cpp 同序消费；
     * 语料实测每块引用 blkno 后 8B 恒在（页 LSN 链回溯用，当前任务不消费）。 */
    private static final int BLOCK_REF_LAST_LSN_BYTES = 8;

    /** heap UPDATE main 前缀 13 = SizeOfOldHeapUpdate 5 + LogCSN 8 // openGauss
     * v5.0.0 htup.h xl_heap_update（常态走 SizeOfOldHeapUpdate：old_offnum u16@0 +
     * new_offnum u16@2 + flags u8@4）+ heapam.cpp log_heap_update 在 xlrec 后经
     * LogCSN 追加 CommitSeqNo u64（wal_level=logical 时 XLogLogicalInfoActive 恒
     * 真——本模块 DML 面前提）。语料实测：HOT_UPDATE lsn=0x2359908 的旧行
     * xl_heap_header 恰在 main+13（t_hoff=24 锚）。变体偏离：UPDATE+INIT_PAGE 在
     * xlrec 前另有 pd_xid_base u64（前缀 21）；xmax 为 MultiXactId 时走全量
     * SizeOfHeapUpdate（u64 xmax 布局，前缀漂移）——消费面（Task 3+）须以
     * flags/INIT_PAGE 位分派，本常量锚常态逻辑日志形态。 */
    private static final int SIZE_OF_HEAP_UPDATE = 13;

    /** heap DELETE main 前缀 11 = SizeOfOldHeapDelete 3 + LogCSN 8 // openGauss
     * v5.0.0 htup.h xl_heap_delete（常态走 SizeOfOldHeapDelete：offnum u16@0 +
     * flags u8@2）+ LogCSN u64 同 UPDATE。语料实测：DELETE lsn=0x2359AD8 旧行
     * xl_heap_header 恰在 main+11（'x'x64 旧行 75B 完整收口）。同 UPDATE 的
     * MultiXactId 变体偏离注记。 */
    private static final int SIZE_OF_HEAP_DELETE = 11;

    /** heap INPLACE main 前缀 2 // openGauss v5.0.0 htup.h xl_heap_inplace 仅
     * {offnum u16}——heapam.cpp heap_inplace_update 不调 LogCSN（无 CSN 块），
     * 新元组数据在块 0 载荷非 main。语料实测：两条 INPLACE（lsn=0x23587D8/
     * 0x2358918）mainLen 恒 2。与 V18 的 20（含 dbId/tsId/msgs[] 前缀）语义
     * 不同源：OG 无共享失效消息内联面。 */
    private static final int SIZE_OF_HEAP_INPLACE = 2;

    /** pg_class 数据区 relfilenode 偏移 84——推算依据：OG catalog 元组 oid 在 9.2 式
     * 元组头（[t_hoff-4,t_hoff)）非数据区（infomask2 natts=40 实锚），数据区列序即
     * attnum 1-40：relname 64B [0,64) + 5×oid [64,84)，relfilenode@84；实测元组
     * relfilenode=16389 恰在 dataOff+84（V17/V18 为 88——含 oid 数据列的口径差）。 */
    private static final int PGCLASS_RELFILENODE_DATA_OFFSET = 84;
    /** pg_class 数据区 reltoastrelid 偏移 116——推算依据：relfilenode 自占 [84,88)、
     * reltablespace oid [88,92) 后 relpages float8（OG 加宽，attalign='d' 实查——
     * 92 非 8 对齐，垫 4B）[96,104)、reltuples float8 [104,112)、relallvisible int4
     * [112,116)、reltoastrelid@116；实测元组 reltoastrelid=16392 恰在 dataOff+116
     * （任务书 108 系按 vanilla 列宽 int4/float4 推导的勘误值，见类注释勘误表）。 */
    private static final int PGCLASS_RELTOASTRELID_DATA_OFFSET = 116;
    /** pg_class 数据区 relkind 偏移 143——推算依据：reltoastrelid 自占 [116,120) 后
     * 五个 oid（reltoastidxid/reldeltarelid/reldeltaidx/relcudescrelid/relcudescidx）
     * [120,140)、relhasindex bool 140、relisshared 141、relpersistence char 142、
     * relkind@143（实测 'r'@143、relnatts=2@144 自洽锚）。 */
    private static final int PGCLASS_RELKIND_DATA_OFFSET = 143;

    /** pg_attribute 词典槽位：attisdropped=14（attnum 15——OG 词典无 oid 数据列，
     * V17/V18 的 17/16 均含前导 oid/attcacheoff 差；OG 有 attcacheoff@7 但
     * attstattarget@4 前移，见 PG_ATTRIBUTE_KINDS 列序注）。 */
    private static final int ATTR_DROPPED_INDEX = 14;

    /** pg_class 词典槽位：reltoastrelid=11（attnum 12——无 oid 数据列使全槽位较
     * V17 的 12 前移一位；live 实查 attnum 锚）。 */
    private static final int CLASS_TOAST_RELID_INDEX = 11;

    /** pg_class 词典槽位：relkind=20（attnum 21——relpersistence@19 后一位；
     * 较 V17 的 16 差 = 五个 oid 列 + oid 数据列缺席 + attstattarget 位次变化，
     * live 实查 attnum 锚）。 */
    private static final int CLASS_RELKIND_INDEX = 20;

    /**
     * pg_attribute 列解码词典（24 项），逐行转录自 live opengauss:5.0.0 服务端
     * pg_attribute 于 pg_attribute 自身的行（attnum 升序，2026-10-12 og-plan 容器）。
     * 元组无 oid（pg_class.relhasoids=f 实查，数据区首列即 attrelid——元组字节
     * attrelid@0/attname@4/...attislocal@98/attcmprmode@99/attinhcount@100/
     * attcollation@104 逐位实测自洽）。与 V17 的 26 项/V18 的 25 项相比：attstattarget
     * 回到 attnum 4（PG 16 及以前位次——PG 17 才移尾）；保留 attcacheoff@7；新增
     * attcmprmode@16（int1，OG 列压缩模式——解码分支随 catalog 任务补）、
     * attinitdefval@22（bytea varlena）、attkvtype@23（int1）；无 atthasmissing/
     * attidentity/attgenerated。int1 与 int4 宽的 attinhcount（V17/18 为 int2）
     * 均为 OG 列形差异。
     */
    private static final String[] PG_ATTRIBUTE_KINDS = {
            "oid",   // attrelid
            "name",  // attname (NameData, 64B fixed)
            "oid",   // atttypid
            "int4",  // attstattarget（PG≤16 位次，V17/V18 已移尾部 varlen 区）
            "int2",  // attlen
            "int2",  // attnum
            "int4",  // attndims
            "int4",  // attcacheoff（OG 保留——PG 18 删除；恒 -1）
            "int4",  // atttypmod
            "bool",  // attbyval
            "char",  // attstorage
            "char",  // attalign
            "bool",  // attnotnull
            "bool",  // atthasdef
            "bool",  // attisdropped         <-- 槽 14（attnum 15）
            "bool",  // attislocal
            "int1",  // attcmprmode（OG 独有，列压缩模式；TupleDecoder int1 分支 Task 7 落地）
            "int4",  // attinhcount（OG int4——V17/V18 为 int2）
            "oid",   // attcollation
            "skip",  // attacl aclitem[]
            "skip",  // attoptions text[]
            "skip",  // attfdwoptions text[]
            "skip",  // attinitdefval bytea（OG 独有，varlena）
            "int1",  // attkvtype（OG 独有；TupleDecoder int1 分支 Task 7 落地）
    };

    /**
     * pg_class 列解码词典（40 项），逐行转录自 live opengauss:5.0.0 服务端
     * pg_attribute 于 pg_class 的行（attnum 升序，2026-10-12 og-plan 容器）。
     * <b>无前导 oid 数据列</b>（区别于 V17/V18 的 kinds[0]="oid"——OG catalog 元组
     * oid 在 9.2 式元组头，infomask2 natts=40 实锚），故全槽位较 V17/V18 前移一位：
     * relfilenode 槽 6、reltoastrelid 槽 11、relkind 槽 20。与 V17 的 33 项相比的
     * OG 差异：relpages/reltuples 加宽 float8；reltoastrelid 后多 reltoastidxid 与
     * 列存四件（reldeltarelid/reldeltaidx/relcudescrelid/relcudescidx）；增
     * relhasoids/relhaspkey/relcmprs/relhasclusterkey/relrowmovement/parttype/
     * relreplident/relfrozenxid64/relbucket/relbucketkey；删 relispartition/
     * relrewrite/relfrozenxid(xid32 仍在)/relforcerowsecurity/relispopulated 等。
     */
    private static final String[] PG_CLASS_KINDS = {
            "name",   // relname（数据区首列——oid 在元组头）
            "oid",    // relnamespace
            "oid",    // reltype
            "oid",    // reloftype
            "oid",    // relowner
            "oid",    // relam
            "oid",    // relfilenode          <-- index 6（V17/V18 为 7）
            "oid",    // reltablespace
            "float8", // relpages（OG 加宽——vanilla int4）
            "float8", // reltuples（OG 加宽——vanilla float4）
            "int4",   // relallvisible
            "oid",    // reltoastrelid        <-- index 11（attnum 12，V17 为 12）
            "oid",    // reltoastidxid（OG 独有）
            "oid",    // reldeltarelid（OG 列存 delta 表）
            "oid",    // reldeltaidx
            "oid",    // relcudescrelid（OG 列存 cudesc 表）
            "oid",    // relcudescidx
            "bool",   // relhasindex
            "bool",   // relisshared
            "char",   // relpersistence
            "char",   // relkind              <-- index 20（attnum 21）
            "int2",   // relnatts
            "int2",   // relchecks
            "bool",   // relhasoids（OG 保留——9.2 式 oid 体系）
            "bool",   // relhaspkey（OG 独有）
            "bool",   // relhasrules
            "bool",   // relhastriggers
            "bool",   // relhassubclass
            "int1",   // relcmprs（OG 独有，表级压缩模式；TupleDecoder int1 分支 Task 7 落地）
            "bool",   // relhasclusterkey（OG 独有）
            "bool",   // relrowmovement（OG 独有）
            "char",   // parttype（OG 分区判别列——分区表 relkind 仍 'r'，'p' 语义由本列承担）
            "int4",   // relfrozenxid（xid32，4B）
            "skip",   // relacl aclitem[]
            "text",   // reloptions text[]（Task 7 改 skip→text——UStore/段页式 fail-fast 护栏
                      //   的判别面：storage_type=ustore / segment=on 仅经本列可判，relam 恒 0
                      //   无分叉；数组原文按 varlena 文本解出供 sniff，null 表恒 null 位图占位）
            "char",   // relreplident（OG 回移植 9.4+ REPLICA IDENTITY 佐证）
            "int8",   // relfrozenxid64（OG 独有，64 位 xid）
            "oid",    // relbucket（OG 独有）
            "skip",   // relbucketkey int2vector（OG 独有）
            "int8",   // relminmxid（OG xid 为 8B——vanilla multixact 4B）
    };

    /** 私有构造器：单例描述符，经 {@link #INSTANCE} 访问。 */
    private WalLayoutVOpenGauss() {
    }

    /** {@inheritDoc}——openGauss 5.0.0 页魔数 0xD074（xlog_basic.h，段首字节实测）。 */
    @Override
    public int pageMagic() {
        return PAGE_MAGIC;
    }

    /** {@inheritDoc}——8192，2 的幂（按位与取页内偏移的前提）。 */
    @Override
    public int walBlockSize() {
        return WAL_BLOCK_SIZE;
    }

    /** {@inheritDoc}——24（含 OG 私有 xlp_total_len@20）。 */
    @Override
    public int shortPageHeaderSize() {
        return SHORT_PAGE_HEADER_SIZE;
    }

    /** {@inheritDoc}——40（XLP_LONG_HEADER 置位的段首页）。 */
    @Override
    public int longPageHeaderSize() {
        return LONG_PAGE_HEADER_SIZE;
    }

    /** {@inheritDoc}——32（term/pad 前置 + prev 8 对齐垫，链式走读实测锚）。 */
    @Override
    public int recordHeaderSize() {
        return RECORD_HEADER_SIZE;
    }

    /**
     * {@inheritDoc}——13 = SizeOfOldHeapUpdate(5) + LogCSN(8)，常态逻辑日志形态
     * （htup.h + heapam.cpp log_heap_update 消费序 + 语料 0x2359908 旧行位实测）；
     * INIT_PAGE/MultiXactId 变体的前缀漂移见常量注记，消费面按 info/flags 分派。
     */
    @Override
    public int sizeOfHeapUpdate() {
        return SIZE_OF_HEAP_UPDATE;
    }

    /**
     * {@inheritDoc}——11 = SizeOfOldHeapDelete(3) + LogCSN(8)（语料 0x2359AD8
     * 旧行位实测：'x'x64 旧行 75B 自 main+11 完整收口）。
     */
    @Override
    public int sizeOfHeapDelete() {
        return SIZE_OF_HEAP_DELETE;
    }

    /**
     * {@inheritDoc}——2（xl_heap_inplace {offnum u16}，无 CSN 块；语料两条
     * INPLACE 记录 mainLen 恒 2 实测锚）。
     */
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

    /** {@inheritDoc}——84（oid 在元组头非数据区，数据区首列 relname@0；实测锚）。 */
    @Override
    public int pgClassRelfilenodeDataOffset() {
        return PGCLASS_RELFILENODE_DATA_OFFSET;
    }

    /** {@inheritDoc}——116（relpages/reltuples float8 加宽净 8B；实测锚，
     *  任务书 108 的勘误值，见类注释勘误表）。 */
    @Override
    public int pgClassReltoastrelidDataOffset() {
        return PGCLASS_RELTOASTRELID_DATA_OFFSET;
    }

    /** {@inheritDoc}——143（五个 oid 列 + float8 加宽；实测 'r'@143 自洽锚）。 */
    @Override
    public int pgClassRelkindDataOffset() {
        return PGCLASS_RELKIND_DATA_OFFSET;
    }

    /** {@inheritDoc}——14（attnum 15，OG 词典无 oid/attidentity 等前置列）。 */
    @Override
    public int attrDroppedIndex() {
        return ATTR_DROPPED_INDEX;
    }

    /** {@inheritDoc}——11（attnum 12，无 oid 数据列使槽位较 V17 前移一位）。 */
    @Override
    public int classToastRelidIndex() {
        return CLASS_TOAST_RELID_INDEX;
    }

    /** {@inheritDoc}——20（attnum 21，relpersistence@19 后一位）。 */
    @Override
    public int classRelkindIndex() {
        return CLASS_RELKIND_INDEX;
    }

    /** {@inheritDoc}——8（term u32 前置，xlog_basic.h，链式走读实测）。 */
    @Override
    public int recordXidOffset() {
        return RECORD_XID_OFFSET;
    }

    /** {@inheritDoc}——24（prev u64@16 之后，链式走读实测）。 */
    @Override
    public int recordInfoOffset() {
        return RECORD_INFO_OFFSET;
    }

    /** {@inheritDoc}——25（紧随 info，链式走读实测）。 */
    @Override
    public int recordRmidOffset() {
        return RECORD_RMD_OFFSET;
    }

    /**
     * {@inheritDoc}——14（RelFileNodeOld 12B + extra_flag u16；16B bucket 变体由
     * 扩展位 fail-fast 拒绝不走本宽度——Decoder 消费序 + 语料实测，见常量注记）。
     */
    @Override
    public int relNodeEncodedSize() {
        return RELNODE_ENCODED_SIZE;
    }

    /** {@inheritDoc}——8（xloginsert.cpp 逐块追加 last_lsn u64，语料实测）。 */
    @Override
    public int blockRefLastLsnBytes() {
        return BLOCK_REF_LAST_LSN_BYTES;
    }

    /** {@inheritDoc}——true（XLogRecordBlockImageHeader 仅 hole_offset/hole_length
     *  两 u16，无 bimg_info 与压缩面，xlogrecord.h）。 */
    @Override
    public boolean imageHeaderIsHoleOnly() {
        return true;
    }

    /** {@inheritDoc}——true：bucket/segpage(0x80)/TDE(0x40) 扩展位未支持前 fail-fast，
     *  防普通块头走读静默错位。 */
    @Override
    public boolean rejectBlockIdExtensionBits() {
        return true;
    }

    /** {@inheritDoc}——true：页清理走 9.2 系 XLOG_HEAP2_CLEAN 老格式（htup.h L705-746
     *  opcode 表 + L905-912 xl_heap_clean——SizeOfHeapClean=12，latestRemovedXid 为
     *  u64[OG TransactionId，c.h L450]；块0 data 无 per 段计数，unused 由余长推导）。
     *  字节实测锚：og-plan 容器 VACUUM t_corpus 期 CLEAN 记录 LSN=0x2367EF8（180B，
     *  main 12B = xid u64 12984 + nredirected 20 + ndead 11；块0 data 104B = 20 对
     *  redirect + 11 dead + 1 unused——(104-80-22)/2 余长推导自洽，2026-10-12 采集）。 */
    @Override
    public boolean legacyHeap2Clean() {
        return true;
    }

    /** {@inheritDoc}——true（Task 7 catalog OG 适配：heap 视图老结构分派 + 引导 SQL 方言）。 */
    @Override
    public boolean openGaussDialect() {
        return true;
    }

    /** {@inheritDoc}——true（pg_class 元组 oid 在 9.2 式元组头 [t_hoff-4,t_hoff)，
     *  实锚 t_offprobe 行 oid=16389@28/t_hoff=32——TupleDecoder 经虚拟前导
     *  kind "oidhdr" 注入值行槽 0）。 */
    @Override
    public boolean classOidInTupleHeader() {
        return true;
    }

    /** {@inheritDoc}——1（值行槽位 = 物理词典槽位 + 1：class 腿解码注入虚拟头 oid 槽，
     *  使 ClassRow 槽位镜像与 V17/V18 的 oid@0/relname@1 口径对齐）。 */
    @Override
    public int classValsSlotDelta() {
        return 1;
    }

    /** {@inheritDoc}——5（OG 词典含 attstattarget@3 的 PG≤16 位次使 attnum 后移一位）。 */
    @Override
    public int attrAttnumIndex() {
        return 5;
    }

    /** {@inheritDoc}——13（attnum 14，live 实查——列存 delta 表判别列）。 */
    @Override
    public int classReldeltarelidIndex() {
        return 13;
    }

    /** {@inheritDoc}——15（attnum 16，live 实查——列存 cudesc 判别列，主判据）。 */
    @Override
    public int classRelcudescrelidIndex() {
        return 15;
    }

    /** {@inheritDoc}——31（attnum 32，live 实查——'n' 普通 / 'p' 分区父表）。 */
    @Override
    public int classParttypeIndex() {
        return 31;
    }

    /** {@inheritDoc}——34（attnum 35，live 实查——UStore/段页式经 reloptions sniff）。 */
    @Override
    public int classReloptionsIndex() {
        return 34;
    }

    /** {@inheritDoc}——37（attnum 38，live 实查——普通表 NULL、段页式实测 2）。 */
    @Override
    public int classRelbucketIndex() {
        return 37;
    }

    /** {@inheritDoc}——-8：OG 沿 9.2 系系统列编号，tableoid=-7、xc_node_id=-8
     *  （og-plan 容器 live 实查 public.t_corpus 负 attnum 集 [-8,-1] 无 -2——
     *  2026-10-12，Task 10 实修：CatalogReplay 词典错位 WARN 的合法下界）。 */
    @Override
    public int attrMinSystemAttnum() {
        return -8;
    }

    /** {@inheritDoc}——true（Task 8 零页容忍档）：pg_receivexlog 落盘段对服务端流内
     *  跳过的未初始化页/段尾零区以零填充补位（og-plan 容器 strace 实锚 write(8192B
     *  全零) 连发），file 通道 walker 页界撞整零页时跳过整页计 zeroPageSkips——
     *  V17/V18 档缺省 false 行为不变。 */
    @Override
    public boolean tolerateAllZeroPages() {
        return true;
    }

    /** {@inheritDoc}——openGauss 全系恒报 90204（版本撞号由 forServer(int,String)
     *  判别器消解，仅 openGauss 5.0.x 文本路由到本档），故仅精确匹配 90204。 */
    @Override
    public boolean supports(int pgVersionNum) {
        return pgVersionNum == 90204;
    }

    /** {@inheritDoc}——5（OG 自有版本口径；非 PG 9——防 pgVersion 语义混用）。 */
    @Override
    public int majorVersion() {
        return 5;
    }
}
