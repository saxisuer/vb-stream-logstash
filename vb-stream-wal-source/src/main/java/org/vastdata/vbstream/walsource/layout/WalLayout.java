package org.vastdata.vbstream.walsource.layout;

/**
 * 一套 PG 大版本的 WAL 物理布局描述符：页/记录头尺寸、heap 记录负载结构尺寸、
 * 系统目录（pg_attribute / pg_class）的列解码词典与关键列数据区偏移——这些数值
 * 随 PG 大版本漂移（如 PG 17 起 attstattarget 移位），解析层一切字节级换算都
 * 从本接口取值，禁止散落硬编码。
 *
 * <p>实例经 {@link WalLayouts#forServerVersion(int)} 按 {@code server_version_num}
 * 分发获得；未知大版本在启动期 fail-fast。所有取值方法为纯常量访问，实现须保证
 * 线程安全（推荐单例 + 常量）；kinds 数组返回防御副本以免调用方改写静态词典。</p>
 */
public interface WalLayout {

    /**
     * WAL 页头魔数（XLOG_PAGE_MAGIC）。
     *
     * <p>页遍历器在每个页边界校验本值——不匹配即流错位或版本错配，须立即失败。
     * PG 每逢布局变更递增低字节（如 18 为 {@code 0xD118}）。</p>
     *
     * @return 16 位页魔数（如 0xD118）
     */
    int pageMagic();

    /**
     * WAL 页大小（XLOG_BLCKSZ）。
     *
     * <p>页边界换算（{@code lsn & (size-1)}，要求 2 的幂）与跨页 contrecord 缝合
     * 均以本值为模。编译期固定 8192，与 --with-wal-blocksize 默认一致。</p>
     *
     * @return 页字节数（8192）
     */
    int walBlockSize();

    /**
     * 短页头尺寸（MAXALIGN(sizeof(XLogPageHeaderData))）。
     *
     * <p>每个非段首页的页头长度；页头跨 chunk 分裂时以本值为等待下限。</p>
     *
     * @return 短页头字节数（24）
     */
    int shortPageHeaderSize();

    /**
     * 长页头尺寸（MAXALIGN(sizeof(XLogLongPageHeaderData))）。
     *
     * <p>仅段首页携带（XLP_LONG_HEADER 置位），额外含段创始信息（含系统标识符
     * 与 blcksz，可交叉校验连接目标）。</p>
     *
     * @return 长页头字节数（40）
     */
    int longPageHeaderSize();

    /**
     * WAL 记录头尺寸（SizeOfXLogRecord）。
     *
     * <p>totLen/xid/info/rmid 等固定前缀长度；记录体遍历从本偏移起读块引用链。</p>
     *
     * @return 记录头字节数（24）
     */
    int recordHeaderSize();

    /**
     * xl_heap_update 主数据区固定前缀尺寸（SizeOfHeapUpdate）。
     *
     * <p>UPDATE 记录主数据中位于首个 tuple 载荷（replica identity 旧行，若有）
     * 之前的结构字节数；跳过它即到可解 tuple 区。取值照抄 spike 实测锚
     * （heapam_xlog.h，REL_18_STABLE）。</p>
     *
     * @return 结构前缀字节数（14）
     */
    int sizeOfHeapUpdate();

    /**
     * xl_heap_delete 主数据区固定前缀尺寸（SizeOfHeapDelete）。
     *
     * <p>DELETE 记录主数据中位于旧 tuple 载荷（replica identity，若有）之前的
     * 结构字节数（heapam_xlog.h，REL_18_STABLE）。</p>
     *
     * @return 结构前缀字节数（8）
     */
    int sizeOfHeapDelete();

    /**
     * xl_heap_inplace 主数据区固定前缀尺寸（MinSizeOfHeapInplace）。
     *
     * <p>柔性 msgs[]（共享失效消息数组）之前的固定结构字节数
     * （offsetof(xl_heap_inplace, msgs)=20，heapam_xlog.h，REL_18_STABLE）：
     * offnum u16@0 + pad2 + dbId u32@4 + tsId u32@8 +
     * relcacheInitFileInval bool@12 + pad3 + nmsgs int@16——后续任务消费。</p>
     *
     * @return 固定结构前缀字节数（20）
     */
    int sizeOfHeapInplace();

    /**
     * pg_attribute 行解码词典（每列一个解码 kind）。
     *
     * <p>逐列 kind 与 pg_attribute.h 目录定义同序（各版本 STABLE 分支）；varlena
     * 列记 "skip"。数组长度即目录列数，目录回放解码据此走位——V17 26 项
     * （含 PG 18 删除的 attcacheoff）、V18 25 项；attisdropped 槽位经
     * {@link #attrDroppedIndex()} 取。返回防御副本。</p>
     *
     * @return 列 kind 数组（V17 26 项 / V18 25 项）
     */
    String[] pgAttributeKinds();

    /**
     * pg_class 行解码词典（每列一个解码 kind）。
     *
     * <p>逐列 kind 转录自 live 服务端 pg_attribute 于 pg_class 的行
     * （attnum 升序）；relfilenode 在偏移 7（V17/V18 一致）、reltoastrelid 在
     * 偏移 {@link #classToastRelidIndex()}（V17=12，V18=13——PG 18 增列
     * relallfrozen 所致）。返回防御副本。</p>
     *
     * @return 列 kind 数组（V17 33 项 / V18 34 项）
     */
    String[] pgClassKinds();

    /**
     * pg_attribute 解码词典中 attisdropped 的槽位（0 起）。
     *
     * <p>行投影（AttrRow）按本槽位取 dropped 标志——列序随大版本漂移
     * （V17=17：多 attcacheoff；V18=16），投影索引必须与词典同版本取值，
     * 禁止跨版本硬编码。</p>
     *
     * @return attisdropped 槽位（V17 17 / V18 16）
     */
    int attrDroppedIndex();

    /**
     * pg_class 解码词典中 reltoastrelid 的槽位（0 起）。
     *
     * <p>行投影（ClassRow）按本槽位取 toast 关系 oid；与
     * {@link #pgClassReltoastrelidDataOffset()} 成对（同列的词典槽位与数据区
     * 偏移两副面孔）——PG 18 在 relallvisible 后增列 relallfrozen 使 V18 槽位
     * 后移一位。</p>
     *
     * @return reltoastrelid 槽位（V17 12 / V18 13）
     */
    int classToastRelidIndex();

    /**
     * pg_class 解码词典中 relkind 的槽位（0 起）。
     *
     * <p>行投影（ClassRow）按本槽位取 relkind（v2 表过滤的分派键）；槽位随
     * reltoastrelid 同步漂移（V17=16 / V18=17——PG 18 增列 relallfrozen 所致），
     * 禁止跨版本硬编码。词典 kind 为 "char"（解码为左引号单字符，投影时剥引号）。</p>
     *
     * @return relkind 槽位（V17 16 / V18 17）
     */
    int classRelkindIndex();

    /**
     * pg_class 数据区 relfilenode 列偏移（自 t_hoff 起）。
     *
     * <p>列 1-7（oid + name64B + 5×oid）共 88 字节定宽，relfilenode 紧随其后；
     * XLOG_HEAP_INPLACE 原地改写路径直接按本偏移读新 relfilenode。</p>
     *
     * @return 数据区偏移（88）
     */
    int pgClassRelfilenodeDataOffset();

    /**
     * pg_class 数据区 reltoastrelid 列偏移（自 t_hoff 起）。
     *
     * <p>relfilenode@88 后跟 5 个定宽列，reltoastrelid 落在 112；与上一偏移
     * 配对用于 INPLACE 路径的 toast 关系跟随。</p>
     *
     * @return 数据区偏移（112）
     */
    int pgClassReltoastrelidDataOffset();

    /**
     * pg_class 数据区 relkind 列偏移（自 t_hoff 起）。
     *
     * <p>reltoastrelid 后跟 relhasindex bool + relisshared bool + relpersistence
     * char（各 1B）后 relkind 落位：V17=115（reltoastrelid@108）、V18=119（@112）
     * ——两版差恰 4B 与 reltoastrelid 偏移自洽。与 {@link #classRelkindIndex()}
     * 成对（同列的词典槽位与数据区偏移两副面孔）；消费面：值编码读区的末列
     * （ClassRow.relkind 投影的重建），INPLACE 路径不读（relkind 不因原地改写变化）。</p>
     *
     * @return 数据区偏移（V17 115 / V18 119）
     */
    int pgClassRelkindDataOffset();

    /**
     * WAL 记录头 xl_xid 字段偏移。
     *
     * <p>缺省 4＝V17/V18 现行值（xlogrecord.h：tot_len u32@0 + xid u32@4——
     * {@code WalRecordParser} 历内联值）；openGauss 记录头在 xid 前多 term u32
     * 字段（xlog_basic.h XLogRecord），覆盖为 8。记录头内联偏移改由本接口供给
     * 时（openGauss 适配），V17/V18 路径取值须与原内联值逐字节相等。</p>
     *
     * @return 头内偏移（V17/18=4，OG=8）
     */
    default int recordXidOffset() {
        return 4;
    }

    /**
     * WAL 记录头 xl_info 字段偏移。
     *
     * <p>缺省 16＝V17/V18 现行值（prev u64@8 之后，{@code WalRecordParser}
     * 历内联值）；openGauss 因 term/pad 前置字段与 prev 8 对齐垫整体后移为 24。</p>
     *
     * @return 头内偏移（V17/18=16，OG=24）
     */
    default int recordInfoOffset() {
        return 16;
    }

    /**
     * WAL 记录头 xl_rmid 字段偏移（紧随 xl_info）。
     *
     * <p>缺省 17＝V17/V18 现行值（{@code WalRecordParser} 历内联值）；
     * openGauss 为 25。</p>
     *
     * @return 头内偏移（V17/18=17，OG=25）
     */
    default int recordRmidOffset() {
        return 17;
    }

    /**
     * 块引用中关系定位器（含紧随其后的版本私有伴随字段）的编码宽度
     * （SAME_REL 未置位时）。
     *
     * <p>缺省 12＝V17/V18 现行值（spc/db/relNode 三 u32，xlogrecord.h
     * XLogRecordBlockHeader 的 RelFileNode 定位器）。openGauss 常规表定位器为
     * RelFileNodeOld（同 12B）+ 紧随的 extra_flag u16（xlogreader.cpp
     * DecodeXLogRecord 消费序），合计 14——extra_flag 读跳不参与 watched 匹配
     * 判据；bucket/段页表的 16B RelFileNode（含 bucketNode/opt）只在块 id 置
     * BKID_HAS_BUCKET_OR_SEGPAGE(0x80) 时出现，该扩展位由
     * {@link #rejectBlockIdExtensionBits()} fail-fast 拒绝，不走本宽度。</p>
     *
     * @return 定位器伴随区字节数（V17/18=12，OG=14）
     */
    default int relNodeEncodedSize() {
        return 12;
    }

    /**
     * 块引用 blkno 之后跟随的 last_lsn u64 宽度。
     *
     * <p>openGauss 的每个块引用在 blkno 之后恒记 last_lsn u64（页 LSN 链回溯用，
     * xloginsert.cpp XLogRecordAssemble 逐块追加、xlogreader.cpp 同序消费）——
     * 当前任务不消费该字段，走读按本宽度跳过。缺省 0＝V17/V18 块引用止于 blkno。</p>
     *
     * @return 跳过字节数（V17/18=0，OG=8）
     */
    default int blockRefLastLsnBytes() {
        return 0;
    }

    /**
     * 块引用镜像头是否为"仅洞字段"形态（无 bimg_info / 无压缩面）。
     *
     * <p>openGauss 的 XLogRecordBlockImageHeader 仅 {hole_offset u16,
     * hole_length u16}（4B，xlogrecord.h——9.5 之前的形制），无 bimg_info 字节、
     * 无压缩镜像支持；镜像长度恒由 {@code walBlockSize() - hole_length} 推导。
     * 缺省 false＝V17/V18 现代形态（镜像头含 bimg_info u8，压缩镜像带显式
     * holeLen u16）。</p>
     *
     * @return true 表示镜像头仅两 u16 洞字段（仅 OG）
     */
    default boolean imageHeaderIsHoleOnly() {
        return false;
    }

    /**
     * 块引用 id 字节高位扩展位（openGauss：0x80=BKID_HAS_BUCKET_OR_SEGPAGE、
     * 0x40=BKID_HAS_TDE_PAGE）是否启动期 fail-fast 拒绝。
     *
     * <p>缺省 false＝V17/V18 无扩展位语义（id 上界 32 内天然不命中高位）；
     * openGauss 覆盖 true——遇到扩展位记录 ISE（消息含 LSN 与违例位），
     * 防止按普通块头走读产生静默错位（逐位支持随后续任务按需放开）。</p>
     *
     * @return true 表示扩展位 fail-fast（仅 OG）
     */
    default boolean rejectBlockIdExtensionBits() {
        return false;
    }

    /**
     * heap2 页清理记录是否为 openGauss 的 9.2 系老格式（XLOG_HEAP2_CLEAN 单 opcode）。
     *
     * <p>缺省 false＝V17/V18 现行形态（XLOG_HEAP2_PRUNE_{ON_ACCESS,VACUUM_SCAN,
     * VACUUM_CLEANUP} 三 opcode，main = reason u8 + flags u8（XLHP_* 位）+ 可选冲突
     * 水位，段计数内联在块0 data 各段首）。openGauss 覆盖 true：页清理是
     * XLOG_HEAP2_CLEAN（htup.h L732，opcode 0x10）单 opcode + info 高位
     * NO_REPAIR_PAGE（L746），main = xl_heap_clean（L905-912，SizeOfHeapClean=12：
     * latestRemovedXid <strong>u64</strong>——OG TransactionId 是 uint64（c.h L450），
     * 非 vanilla 的 4B——+ nredirected u16@8 + ndead u16@10），块0 data = redirected
     * 对（2n u16）+ dead（n u16）+ unused（余长推导，redo_heapam.cpp
     * HeapXlogCleanOperatorPage 同式——<strong>无 per 段计数</strong>）；且 FPI 存储
     * 时段数组不随记（xloginsert.cpp：needs_data = !needs_backup），块无 data 即空
     * 清理。两族 opcode 撞号（OG CLEAN 0x10 == vanilla PRUNE_ON_ACCESS），解析分派
     * 必须经本开关而非 opcode 集判定。</p>
     *
     * @return true 表示页清理记录走 OG 老格式解析（仅 OG）
     */
    default boolean legacyHeap2Clean() {
        return false;
    }

    /**
     * 判定本描述符是否覆盖给定服务端版本号。
     *
     * <p>按大版本全区间判定（如 V18 覆盖 180000-189999，含小版本与发行版位）。</p>
     *
     * @param pgVersionNum PG 的 server_version_num（如 180000）
     * @return 区间内 true
     */
    boolean supports(int pgVersionNum);

    /**
     * 本描述符的 PG 大版本号。
     *
     * @return 大版本（如 18）
     */
    int majorVersion();
}
