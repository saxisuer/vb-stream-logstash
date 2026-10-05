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
     * xl_heap_inplace 主数据区固定前缀尺寸（SizeOfHeapInplace）。
     *
     * <p>INPLACE（pg_class TRUNCATE/ANALYZE 原地改写路径）主数据中位于 offnum
     * 之后的结构字节数（heapam_xlog.h，REL_18_STABLE）——后续任务消费。</p>
     *
     * @return 结构前缀字节数
     */
    int sizeOfHeapInplace();

    /**
     * pg_attribute 行解码词典（每列一个解码 kind）。
     *
     * <p>逐列 kind 与 pg_attribute.h 目录定义同序（REL_18_STABLE）；varlena 列
     * 记 "skip"。数组长度即目录列数，目录回放解码据此走位。返回防御副本。</p>
     *
     * @return 列 kind 数组（25 项）
     */
    String[] pgAttributeKinds();

    /**
     * pg_class 行解码词典（每列一个解码 kind）。
     *
     * <p>逐列 kind 转录自 live PG 18 服务端 pg_attribute 于 pg_class 的行
     * （attnum 升序）；relfilenode 在偏移 7、reltoastrelid 在偏移 13。
     * 返回防御副本。</p>
     *
     * @return 列 kind 数组（34 项）
     */
    String[] pgClassKinds();

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
