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
     * DML 解码/渲染面的列类型 oid 重映射（方言专属类型 → 渲染矩阵等价 oid）。
     *
     * <p>背景：方言库会给 SQL 标准类型挂私有 oid（如 VastBase A 档 DDL 关键字
     * {@code date} 落 {@code oradate}=9040），磁盘表示与某矩阵内类型同构（8B
     * epoch-2000 微秒 = timestamp）——DML 面（{@code TableFilter} 组装
     * {@code ColumnMeta} 时）经本方法把方言 oid 归一到等价矩阵 oid，解码词典
     * （{@code DecodeKinds}）与渲染矩阵（{@code DiskValueRenderer}）零改动。
     * catalog 字典/重放面<strong>不经本方法</strong>（atttypid 原值进 AttrRow，
     * 对拍 JDBC 实查仍按原 oid）。</p>
     *
     * <p>缺省恒等（vanilla/OG 档无重映射）。</p>
     *
     * @param typeOid pg_attribute 原始 atttypid
     * @return DML 解码/渲染面的等价 oid（缺省原值）
     */
    default long remapDmlTypeOid(long typeOid) {
        return typeOid;
    }

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
     * 本描述符是否属 openGauss 方言族（5.x：9.2 系 heap 记录结构 + SQL 面差异）。
     *
     * <p>消费面（Task 7 catalog OG 适配）：① heap 视图字段偏移——OG 的
     * xl_heap_update/delete 走 SizeOfOld* 老结构（字段前移），INSERT 的
     * INIT_PAGE 变体在 xlrec 前多 pd_xid_base u64（语料锚，见
     * {@code HeapViews} 各工厂）；② {@code CatalogBootstrap} 的引导 SQL 方言
     * （LSN 函数 {@code pg_current_xlog_location()}——OG 无 flush 档，及 pg_class
     * 种子的 OG 列单投影）。缺省 false＝V17/V18 现行 vanilla 行为，路径字节零变化。</p>
     *
     * @return true 表示 openGauss 方言档（仅 OG）
     */
    default boolean openGaussDialect() {
        return false;
    }

    /**
     * catalog pg_class 元组的 oid 是否在 9.2 式元组头（[t_hoff-4,t_hoff)）而非数据列。
     *
     * <p>OG 5.0.0 的 pg_class 元组 oid 居元组头（t_hoff=32、oid@28，infomask2
     * natts=40 实锚——区别于 V17/V18 的 kinds[0]="oid" 数据列口径）。消费面
     * （Task 7）：{@code TupleDecoder} 以虚拟前导 kind {@code "oidhdr"} 把头 oid
     * 注入解码值行槽 0，使 ClassRow 的词典槽位镜像与 V17/V18 对齐（值行长度 =
     * 词典长度 + {@link #classValsSlotDelta()}）。缺省 false。</p>
     *
     * @return true 表示 oid 在元组头（仅 OG）
     */
    default boolean classOidInTupleHeader() {
        return false;
    }

    /**
     * pg_class 解码值行相对物理词典（{@link #pgClassKinds()}）槽位的整体位移。
     *
     * <p>OG 档为 1：class 腿解码在词典前注入虚拟头 oid 槽（见
     * {@link #classOidInTupleHeader()}），值行槽位 = 物理词典槽位 + 1（relname
     * 物理 0 → 值行 1，恰与 V17/V18 的 oid@0/relname@1 口径对齐；reltoastrelid
     * 物理 11 → 值行 12、relkind 物理 20 → 值行 21）。消费面：
     * {@code CatalogRow.ClassRow#fromDecoded} 与 CatalogReplay 的采纳值行组装。
     * 缺省 0＝V17/V18 值行槽位即词典槽位。</p>
     *
     * @return 值行槽位位移（V17/18=0，OG=1）
     */
    default int classValsSlotDelta() {
        return 0;
    }

    /**
     * pg_attribute 解码词典中 attnum 的槽位（0 起）。
     *
     * <p>OG 词典 24 项含 attstattarget@3（PG≤16 位次——V17 起移尾），使 attnum
     * 落槽 5（V17/V18 为 4）。消费面：{@code CatalogRow.AttrRow#fromDecoded} 与
     * CatalogReplay 的 attr 采纳值行组装。缺省 4＝V17/V18 现行值。</p>
     *
     * @return attnum 槽位（V17/18=4，OG=5）
     */
    default int attrAttnumIndex() {
        return 4;
    }

    /**
     * pg_class 解码词典中 reldeltarelid（OG 列存 delta 表 oid）的物理槽位。
     *
     * <p>列存表 fail-fast 判别列之一（reldeltarelid≠0 → 列存）；V17/V18 无此列，
     * 缺省 -1 表示"本档无该判别面"（存储引擎护栏整体不激活）。OG=13（attnum 14，
     * live 实查锚）。值行消费时加 {@link #classValsSlotDelta()}。</p>
     *
     * @return 物理词典槽位（V17/18=-1 缺席，OG=13）
     */
    default int classReldeltarelidIndex() {
        return -1;
    }

    /**
     * pg_class 解码词典中 relcudescrelid（OG 列存 cudesc 表 oid）的物理槽位。
     *
     * <p>列存表（ORIENTATION=COLUMN）的主判别列：relcudescrelid≠0 即列存
     * （og-plan 实测：og_t_colstore 行 relcudescrelid=16447）。V17/V18 无此列
     * 缺省 -1。OG=15（attnum 16，live 实查锚）。</p>
     *
     * @return 物理词典槽位（V17/18=-1 缺席，OG=15）
     */
    default int classRelcudescrelidIndex() {
        return -1;
    }

    /**
     * pg_class 解码词典中 parttype（OG 分区判别列）的物理槽位。
     *
     * <p>OG 分区表的父表 relkind 仍 'r'，分区语义由本列承担（'n'=普通 /
     * 'p'=分区父表，og-plan 实测）。fail-fast 判别列：≠'n' 即 ISE。V17/V18 无此列
     * 缺省 -1。OG=31（attnum 32，live 实查锚）。</p>
     *
     * @return 物理词典槽位（V17/18=-1 缺席，OG=31）
     */
    default int classParttypeIndex() {
        return -1;
    }

    /**
     * pg_class 解码词典中 reloptions（OG 词典内为 text——护栏消费面）的物理槽位。
     *
     * <p>OG 的 UStore（storage_type=ustore）与段页式（segment=on）仅经 reloptions
     * 可判（relam 恒 0，og-plan 实测 og_t_ustore 行与 astore 仅 reloptions 分叉）
     * ——故 OG 词典该列 kind 为 "text"（V17/V18 为 skip，无消费面）。fail-fast：
     * 含 "storage_type=ustore" / "segment=on" 即 ISE。V17/V18 无此判别缺省 -1。
     * OG=34（attnum 35，live 实查锚）。</p>
     *
     * @return 物理词典槽位（V17/18=-1 缺席，OG=34）
     */
    default int classReloptionsIndex() {
        return -1;
    }

    /**
     * pg_class 解码词典中 relbucket（OG bucket/段页式判别列）的物理槽位。
     *
     * <p>OG 普通表 relbucket 为 NULL、段页式表（SEGMENT=ON）实测 relbucket=2
     * （og-plan og_t_seg 行）；判别：非 null 且 ≠0 即 ISE。V17/V18 无此列缺省
     * -1。OG=37（attnum 38，live 实查锚）。</p>
     *
     * @return 物理词典槽位（V17/18=-1 缺席，OG=37）
     */
    default int classRelbucketIndex() {
        return -1;
    }

    /**
     * pg_attribute 系统列 attnum 的合法下界（含）。
     *
     * <p>消费面：{@code CatalogReplay} 的 attr 行事件诊断——attnum==0 或低于本下界
     * 即"疑似词典错位"WARN（真错位信号；合法负 attnum 系统列行不落字典、不告警）。
     * 数值随方言分叉（两档均 live 实查锚，2026-10-12）：vanilla PG 12+ 删 oid 系统
     * 列后系统列重编号至 tableoid=-6（PG 18 实查 {@code t} 的负 attnum 集
     * [-6,-1]）；openGauss 沿 9.2 系编号多两列——tableoid=-7、xc_node_id=-8
     * （og-plan 容器实查 {@code public.t_corpus} 负 attnum 集 [-8,-1] 无 -2）。
     * 缺省 -6＝V17/V18 现行 vanilla 口径，路径行为不变。</p>
     *
     * @return 合法系统列 attnum 下界（V17/18=-6，OG=-8，VastBase=-9——M4：6.0.0 内核
     *         多一列 rowid 把 xc_node_id 挤到 -9）
     */
    default int attrMinSystemAttnum() {
        return -6;
    }

    /**
     * walker 是否容忍<strong>整零页</strong>（页界遇整页全零且页头魔数不符 → 跳过整页
     * 计 zeroPageSkips，而非再同步/ISE）——Task 8 零页容忍门控面。
     *
     * <p>为什么 OG 档需要：pg_receivexlog 落盘的段文件是按段几何补位的字节流——服务端
     * 流内跳过的未初始化页与完成段的尾零区由客户端<strong>零填充</strong>写到文件补位
     * （og-plan 容器 strace 实锚：{\@code write(fd, 8192B 全零)} 连发；完成段恒 16MB
     * 零垫尾），FileWalSource tail 该目录时 walker 必然撞上无页头的整零页。缺省
     * false＝V17/V18 现行行为逐字节不变——vanilla 收流走 pgjdbc 流面，walsender 流内
     * 恒不投递整零页（跳过的未初始化页以地址跳跃暴露，由既有 dropped==0 良性重锚
     * 消化），两档 walker 路径零回归。</p>
     *
     * @return true 表示整零页跳过整页容忍（仅 OG；缺省 false）
     */
    default boolean tolerateAllZeroPages() {
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
     * name 类型（NameData）在本档的定宽字节数。
     *
     * <p>V17/V18/OG 5.0 均为 64（NAMEDATALEN 缺省）；VastBase G100 加宽至 128
     * （live 实查 pg_attribute 的 name 列 attlen=128，spec §0.2）。消费面：nsp/partition
     * 词典分派与读区重建几何的 name 差计算。缺省 64。</p>
     *
     * @return name 定宽字节数（64 / VastBase 128）
     */
    default int nameDatumWidth() {
        return 64;
    }

    /**
     * 本描述符的 PG 大版本号。
     *
     * @return 大版本（如 18）
     */
    int majorVersion();
}
