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
