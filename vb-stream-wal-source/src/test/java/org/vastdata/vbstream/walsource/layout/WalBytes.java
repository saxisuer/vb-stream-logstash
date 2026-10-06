package org.vastdata.vbstream.walsource.layout;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * WAL 单条记录的手造字节 DSL（测试源码根，与被测同包，后续任务复用）。
 *
 * <p>以流式 API 按 PG {@code XLogRecordAssemble} 的装配序拼出一条完整记录字节数组：
 * 24B 记录头 → block 头链（每块 id/fork_flags/data_length 内嵌，image 头/定位器/块号按
 * 真实布局展开）→ TOPLEVEL_XID 标记（若有）→ main data 终止头（DATA_SHORT/LONG，仅当
 * main 非空）→ 载荷区（逐块 [image][data] 按块序，spike 发现 6，最后 main data）。
 * 调用方方法链的调用次序不改变输出——块头/标记/终止头的发射序由本类固定，与 PG 一致。</p>
 *
 * <p>常量转录 REL_18_STABLE xlogrecord.h，是与被测 {@link WalRecordParser} 相互独立的
 * 第二份转录：两侧对协议各写各的理解，round-trip 断言有出入即刻红（双源互证）。
 * {@code build()} 不做 MAXALIGN 尾部补齐——parser 不要求补齐，需要的测试自便。</p>
 */
public final class WalBytes {

    /** 记录头尺寸（SizeOfXLogRecord，xlogrecord.h）——测试基建自持常量，不经 WalLayout。 */
    private static final int REC_HDR_SIZE = 24;

    /** fork_flags 的"携带页镜像"位（BKPBLOCK_HAS_IMAGE）。 */
    static final int FLAG_HAS_IMAGE = 0x10;

    /** fork_flags 的"携带块内 data"位（BKPBLOCK_HAS_DATA）。 */
    static final int FLAG_HAS_DATA = 0x20;

    /** fork_flags 的"定位器同前块"位（BKPBLOCK_SAME_REL）。 */
    static final int FLAG_SAME_REL = 0x80;

    /** bimg_info 的"镜像有洞"位（BKPIMAGE_HAS_HOLE）。 */
    static final int BKPIMAGE_HAS_HOLE = 0x01;

    /** bimg_info 的压缩算法位掩码（pglz|lz4|zstd）——置位时洞长以显式 u16 写出。 */
    static final int BKPIMAGE_COMPRESS_MASK = 0x04 | 0x08 | 0x10;

    /** main data 终止头 id：短载荷（u8 长度）。 */
    private static final int ID_DATA_SHORT = 255;

    /** main data 终止头 id：长载荷（u32 长度）。 */
    private static final int ID_DATA_LONG = 254;

    /** TOPLEVEL_XID 标记 id（子事务记录归并，spike 发现 25）。 */
    private static final int ID_TOPLEVEL_XID = 252;

    /** DATA_SHORT 的 u8 长度上限——main 载荷不超过该值即选短终止头。 */
    private static final int DATA_SHORT_MAX = 255;

    /** WAL 页大小（XLOG_BLCKSZ，xlog_internal.h）——整页拼装的自持常量。 */
    private static final int PAGE_SIZE = 8192;

    /** 短页头尺寸（MAXALIGN(sizeof(XLogPageHeaderData))）——页内记录段自此后展开。 */
    private static final int SHORT_PAGE_HEADER_SIZE = 24;

    /** XLogPageHeaderData 定偏移：xlp_pageaddr u64@8（页起始 LSN，页导航唯一权威）。 */
    private static final int PAGEADDR_OFFSET = 8;

    /** XLogPageHeaderData 定偏移：xlp_rem_len u32@16（续体页的剩余字节数）。 */
    private static final int REM_LEN_OFFSET = 16;

    /** 测试源码根整页拼装用：PG 18 页魔数（XLOG_PAGE_MAGIC，xlog_internal.h）。 */
    public static final int XLOG_PAGE_MAGIC = 0xD118;

    /** 页头 info 位 XLP_FIRST_IS_CONTRECORD：本页首条记录是窗口之前记录的续体。 */
    public static final int XLP_FIRST_IS_CONTRECORD = 0x0001;

    /** 页头 info 位 XLP_LONG_HEADER：段首长页头（额外 16B 段创始信息）。 */
    public static final int XLP_LONG_HEADER = 0x0002;

    /** rmgrlist.h 枚举序：RM_XACT_ID（xact 助手自持常量——与被测 HeapOps 双源互证）。 */
    private static final int RM_XACT_ID = 1;

    /** xact.h：XLOG_XACT_PREPARE（opcode 位段 0x10——两阶段准备）。 */
    private static final int XLOG_XACT_PREPARE = 0x10;

    /** xact.h：XLOG_XACT_ASSIGNMENT（opcode 位段 0x50——子事务归并映射）。 */
    private static final int XLOG_XACT_ASSIGNMENT = 0x50;

    /** xact.h：XLOG_XACT_HAS_INFO（info 附加位 0x80——main 携带 xinfo 块链）。 */
    private static final int XLOG_XACT_HAS_INFO = 0x80;

    /** xact.h：XACT_XINFO_HAS_TWOPHASE（1<<4——twophase chunk 存在）。 */
    private static final int XINFO_HAS_TWOPHASE = 1 << 4;

    /** xact.h：XACT_XINFO_HAS_GID（1<<7——gid 字符串跟随 twophase chunk）。 */
    private static final int XINFO_HAS_GID = 1 << 7;

    private static final byte[] EMPTY = {};

    private final int rmid;
    private final int info;
    private final int xid;
    private final List<BlockSpec> blocks = new ArrayList<>();
    private BlockSpec current;
    private int toplevelXid;
    private byte[] mainData = EMPTY;

    /**
     * 起点：声明记录头的 rmid/info/xid 三要素。
     *
     * @param rmid  资源管理器 id（rmgrlist.h 0-based 枚举序）
     * @param info  记录 info 字节（含操作码高位与标志位）
     * @param xid   记录头 xid（无事务记录传 0）
     * @return 可链式拼装的本 DSL 实例
     */
    public static WalBytes record(int rmid, int info, int xid) {
        return new WalBytes(rmid, info, xid);
    }

    /**
     * 私有构造：三要素只经 {@link #record(int, int, int)} 工厂进入，字段此后只读。
     *
     * @param rmid 资源管理器 id
     * @param info 记录 info 字节
     * @param xid  记录头 xid
     */
    private WalBytes(int rmid, int info, int xid) {
        this.rmid = rmid;
        this.info = info;
        this.xid = xid;
    }

    /**
     * 追加一个携带完整定位器（spc/db/relNode）的 block 引用。
     *
     * <p>成为"当前块"——后续 {@link #image(byte[], int, int)} / {@link #data(byte[])} 落于本块。
     * block id 按追加序（0,1,...）发射。</p>
     *
     * @param fork     fork 号（0=main）
     * @param spc      表空间 oid
     * @param db       数据库 oid
     * @param relNode  relation node
     * @param blockNo  块号
     * @return 本实例（链式）
     */
    public WalBytes block(int fork, long spc, long db, long relNode, int blockNo) {
        BlockSpec spec = new BlockSpec(fork, blockNo, false);
        spec.spc = spc;
        spec.db = db;
        spec.relNode = relNode;
        blocks.add(spec);
        current = spec;
        return this;
    }

    /**
     * 追加一个 SAME_REL block 引用：fork_flags 置 {@code 0x80} 且不写定位器字节，
     * 定位器语义由 parser 端的 lastLoc 复用补齐（spike 走读行为）。
     *
     * <p>若之前没有任何完整定位器，本方法照常产出字节——parse 侧按
     * "SAME_REL with no previous locator" fail-fast，测试借此直击该错误分支。</p>
     *
     * @param fork     fork 号
     * @param blockNo  块号
     * @return 本实例（链式）
     */
    public WalBytes sameRel(int fork, int blockNo) {
        BlockSpec spec = new BlockSpec(fork, blockNo, true);
        blocks.add(spec);
        current = spec;
        return this;
    }

    /**
     * 为当前块声明页镜像：fork_flags 置 HAS_IMAGE 位并展开镜像头
     * （imageLen u16 + holeOffset u16 + bimgInfo u8；HAS_HOLE 且压缩位有效时再补显式
     * holeLen u16——未压缩的洞长可由 {@code XLOG_BLCKSZ - imageLen} 推导，不写）。
     *
     * @param img         镜像字节（写出长度即该数组长度）
     * @param holeOffset  镜像头中的洞偏移
     * @param bimgInfo    镜像标志字节（HAS_HOLE/APPLY/压缩算法位）
     * @return 本实例（链式）
     * @throws IllegalStateException 尚未声明任何 block（无当前块可落）
     */
    public WalBytes image(byte[] img, int holeOffset, int bimgInfo) {
        requireCurrentBlock();
        current.image = img;
        current.holeOffset = holeOffset;
        current.bimgInfo = bimgInfo;
        return this;
    }

    /**
     * 压缩镜像的显式洞长——仅 {@code bimgInfo} 同时置 HAS_HOLE 与压缩位时被写出，
     * 测试用它直击 parser 的"显式 holeLen u16"读取分支。
     *
     * @param holeLen 洞长（字节）
     * @return 本实例（链式）
     */
    public WalBytes holeLen(int holeLen) {
        requireCurrentBlock();
        current.holeLen = holeLen;
        return this;
    }

    /**
     * 为当前块声明块内 data：fork_flags 置 HAS_DATA 位，长度以内嵌 u16
     * {@code data_length} 写进 block 头（spike 走读：data 长度内嵌于块头前缀）。
     *
     * @param d 块内数据字节
     * @return 本实例（链式）
     * @throws IllegalStateException 尚未声明任何 block
     */
    public WalBytes data(byte[] d) {
        requireCurrentBlock();
        current.data = d;
        return this;
    }

    /**
     * 声明 TOPLEVEL_XID 标记（块头链之后、终止头之前发射，与 PG 装配序一致）。
     *
     * <p>多次调用后写覆盖先写；0 视为不写标记。</p>
     *
     * @param toplevelXid 顶层事务 id
     * @return 本实例（链式）
     */
    public WalBytes toplevel(int toplevelXid) {
        this.toplevelXid = toplevelXid;
        return this;
    }

    /**
     * 声明 main data：按长度自动选 DATA_SHORT（≤255，u8 长度）或 DATA_LONG（u32 长度）
     * 终止头。空数组/不调用 = 无 main data——**不写终止头**，终止判据落给
     * datatotal（spike 发现 2：无 main data 的记录没有 DATA_SHORT/LONG 终止头）。
     *
     * @param m main data 字节
     * @return 本实例（链式）
     */
    public WalBytes main(byte[] m) {
        this.mainData = m;
        return this;
    }

    /**
     * 拼出完整记录字节数组（含 24B 头，totLen 即最终数组长度）。
     *
     * <p>装配序：记录头占位 → 逐块头（id/fork_flags/data_length [+ 镜像头] [+ 定位器
     * 除非 SAME_REL] + 块号）→ TOPLEVEL_XID（若有）→ main 终止头（若 main 非空）；
     * 载荷区逐块 [image][data] 按块序随后，main data 殿后。尾部不做 MAXALIGN 补齐。</p>
     *
     * @return 记录字节数组（u32 totLen@0 与数组长度一致）
     */
    public byte[] build() {
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (int i = 0; i < blocks.size(); i++) {
            BlockSpec b = blocks.get(i);
            int forkFlags = b.fork
                    | (b.hasImage() ? FLAG_HAS_IMAGE : 0)
                    | (b.hasData() ? FLAG_HAS_DATA : 0)
                    | (b.sameRel ? FLAG_SAME_REL : 0);
            headers.write(i);                                   // block id = 注册序
            headers.write(forkFlags);
            put16(headers, b.hasData() ? b.data.length : 0);
            if (b.hasImage()) {
                put16(headers, b.image.length);
                put16(headers, b.holeOffset);
                headers.write(b.bimgInfo);
                if ((b.bimgInfo & BKPIMAGE_HAS_HOLE) != 0
                        && (b.bimgInfo & BKPIMAGE_COMPRESS_MASK) != 0) {
                    put16(headers, b.holeLen);                   // 压缩镜像的显式洞长
                }
            }
            if (!b.sameRel) {
                put32(headers, b.spc);
                put32(headers, b.db);
                put32(headers, b.relNode);
            }
            put32(headers, b.blockNo);
            // 载荷区按块序 [image][data] 追加（spike 发现 6）
            if (b.hasImage()) {
                payload.writeBytes(b.image);
            }
            if (b.hasData()) {
                payload.writeBytes(b.data);
            }
        }
        if (toplevelXid != 0) {
            headers.write(ID_TOPLEVEL_XID);
            put32(headers, toplevelXid);
        }
        if (mainData.length > 0) {
            if (mainData.length <= DATA_SHORT_MAX) {
                headers.write(ID_DATA_SHORT);
                headers.write(mainData.length);
            } else {
                headers.write(ID_DATA_LONG);
                put32(headers, mainData.length);
            }
            payload.writeBytes(mainData);
        }
        int totLen = REC_HDR_SIZE + headers.size() + payload.size();
        byte[] rec = new byte[totLen];
        // 记录头（XLogRecord）：totLen u32@0、xid u32@4、xl_prev 8B 占位（0）、
        // info u8@16、rmid u8@17、padding+xl_crc 6B 占位（0）
        put32(rec, 0, totLen);
        put32(rec, 4, xid);
        rec[16] = (byte) info;
        rec[17] = (byte) rmid;
        byte[] hdr = headers.toByteArray();
        byte[] pay = payload.toByteArray();
        System.arraycopy(hdr, 0, rec, REC_HDR_SIZE, hdr.length);
        System.arraycopy(pay, 0, rec, REC_HDR_SIZE + hdr.length, pay.length);
        return rec;
    }

    /**
     * 拼一个无特殊标志的完整 WAL 页（短页头 + 逐记录段 + 尾零到页大小）。
     *
     * <p>页头按 XLogPageHeaderData 展开：magic u16@0、info u16@2（0）、tli u32@4（1）、
     * pageaddr u64@8、rem_len u32@16（0），MAXALIGN 后即 24B。等价于
     * {@link #page(long, int, int, byte[]...)} 的 {@code (pageAddr, 0, 0, segments)} 便捷档。</p>
     *
     * @param pageAddr       页起始 LSN（须已页对齐；写入页头 xlp_pageaddr）
     * @param recordSegments 页内记录段字节（通常为 {@code build()} 输出；逐段 MAXALIGN 起点排布）
     * @return 8192 字节整页
     */
    public static byte[] page(long pageAddr, byte[]... recordSegments) {
        return page(pageAddr, 0, 0, recordSegments);
    }

    /**
     * 拼一个可带页头标志的完整 WAL 页（短页头 + 逐记录段 + 尾零到页大小）。
     *
     * <p>关键步骤：页头 24B 按定偏移写入（info 携带 {@link #XLP_FIRST_IS_CONTRECORD} 等
     * 标志、rem_len 声明续体剩余字节——续体页首段为跨页记录的余部，紧贴页头展开）；
     * 随后逐记录段排布——每段起点 MAXALIGN 到 8（首段落于 24，天然对齐；续体余部
     * 之后的首个完整记录由此正确垫齐，与 PG 记录起点对齐规则一致）；尾部补零到
     * {@code PAGE_SIZE}。边界与异常语义：段超页容量抛 IAE（拼装误用即刻失败）；
     * pageAddr 不页对齐不做前置拦截——页头校验面留给被测 walker。</p>
     *
     * @param pageAddr       页起始 LSN（写入 xlp_pageaddr）
     * @param xlpFlags       页头 info 标志位（如 {@link #XLP_FIRST_IS_CONTRECORD}）
     * @param xlpRemLen      xlp_rem_len（续体页的剩余字节数；非续体页传 0）
     * @param recordSegments 页内记录段字节（续体页首段为记录余部，其后段为完整记录）
     * @return 8192 字节整页
     * @throws IllegalArgumentException 任一段（含垫齐）超出页容量
     */
    public static byte[] page(long pageAddr, int xlpFlags, int xlpRemLen, byte[]... recordSegments) {
        byte[] page = new byte[PAGE_SIZE];
        put16(page, 0, XLOG_PAGE_MAGIC);
        put16(page, 2, xlpFlags);
        put32(page, 4, 1L);                       // xlp_tli：任意合法时间线
        put64(page, PAGEADDR_OFFSET, pageAddr);
        put32(page, REM_LEN_OFFSET, xlpRemLen);
        int pos = SHORT_PAGE_HEADER_SIZE;
        for (byte[] seg : recordSegments) {
            pos = (pos + 7) & ~7;                 // 记录起点 MAXALIGN（首段 24 已对齐）
            if (pos + seg.length > PAGE_SIZE) {
                throw new IllegalArgumentException("segment of " + seg.length
                        + " bytes overflows WAL page at offset " + pos);
            }
            System.arraycopy(seg, 0, page, pos, seg.length);
            pos += seg.length;
        }
        return page;
    }

    /**
     * 拼一条 RM_XACT_ID 记录（COMMIT/ABORT/ASSIGNMENT 之外的极简档）——main data 仅
     * {@code xact_time i64}（MinSizeOfXactCommit=8，REL_18 xact.h 的 xl_xact_commit
     * 首成员），可选 TOPLEVEL_XID 标记（子事务 ABORT 归并用）。
     *
     * <p>布局钉（REL_18_STABLE src/backend/access/rmgrdesc/xact.c ParseCommitRecord）：
     * 无 HAS_INFO 位的 commit/abort main 就是裸 8B 时间戳；带附加块的形态走
     * {@link #xactRecordWithGid}。</p>
     *
     * @param opcode        XLOG_XACT_* 操作码（调用方传 HeapOps 常量）
     * @param xid           记录头 xid
     * @param xactTimeMicros 提交/中止时间戳（自 2000-01-01 起微秒）
     * @param toplevelXid   顶层事务 id（0 = 不写 TOPLEVEL_XID 标记）
     * @return 完整记录字节数组
     */
    public static byte[] xactRecord(int opcode, int xid, long xactTimeMicros, int toplevelXid) {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put64(m, xactTimeMicros);
        WalBytes b = record(RM_XACT_ID, opcode, xid).main(m.toByteArray());
        return toplevelXid != 0 ? b.toplevel(toplevelXid).build() : b.build();
    }

    /**
     * 拼一条 COMMIT_PREPARED / ABORT_PREPARED 记录——info 置 XLOG_XACT_HAS_INFO(0x80)，
     * main 走 {@code [xact_time i64][xinfo u32][twophase chunk: xid u32][gid NUL 结尾]}。
     *
     * <p>布局钉（xactdesc.c ParseCommitRecord 的块链走读）：xinfo 置
     * XACT_XINFO_HAS_TWOPHASE(1&lt;&lt;4)，gid 非 null 再置 HAS_GID(1&lt;&lt;7)——中间块
     * （dbinfo/subxacts/relfilelocators/stats/invals）全缺位，游标直达 twophase chunk。
     * <b>归属键 = twophase chunk 的 xid</b>（xact_redo_commit 用 parsed.twophase_xid 而非
     * 记录头 xid——COMMIT PREPARED 在新事务里执行，记录头是新 xid）。gid 传 null = 无
     * GID chunk 形态（wal_level&lt;logical 的真实形态——gid 归 PREPARE 记录侧承载，
     * 确认侧回落桶上挂起值）。</p>
     *
     * @param opcode       XLOG_XACT_COMMIT_PREPARED / XLOG_XACT_ABORT_PREPARED
     * @param headerXid    记录头 xid（真实形态下是执行命令的新事务 xid，非归属键）
     * @param xactTimeMicros 时间戳（自 2000-01-01 起微秒）
     * @param twophaseXid  被确认/回滚的已准备事务 xid（归属键）
     * @param gid          两阶段全局事务名（null = 不携带 GID chunk）
     * @return 完整记录字节数组
     */
    public static byte[] xactRecordWithGid(int opcode, int headerXid, long xactTimeMicros,
                                           int twophaseXid, String gid) {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put64(m, xactTimeMicros);
        put32(m, XINFO_HAS_TWOPHASE | (gid != null ? XINFO_HAS_GID : 0));
        put32(m, twophaseXid & 0xFFFFFFFFL);
        if (gid != null) {
            m.writeBytes(gid.getBytes(StandardCharsets.UTF_8));
            m.write(0);                                         // NUL 结尾（块链注释原文）
        }
        return record(RM_XACT_ID, opcode | XLOG_XACT_HAS_INFO, headerXid)
                .main(m.toByteArray()).build();
    }

    /**
     * 拼一条 PREPARE 记录——main data = 两阶段状态文件头（xl_xact_prepare，
     * sizeof=72B）+ gid（<b>真实形态：gidlen = strlen+1 含尾 NUL</b>，twophase.c
     * SaveTransactionState 的 {@code hdr->gidlen = strlen(gid) + 1} 同面——解侧须剥尾
     * NUL 才得原 gid）。
     *
     * <p>头布局钉（REL_18 xact.h xl_xact_prepare = TwoPhaseFileHeader，C 对齐后）：
     * magic u32@0、total_len u32@4、xid u32@8、database u32@12、prepared_at i64@16、
     * owner u32@24、nsubxacts@28、ncommitrels@32、nabortrels@36、ncommitstats@40、
     * nabortstats@44、ninvalmsgs@48、initfileinval u8@52、<b>1B C 对齐垫@53</b>、
     * gidlen u16@54、origin_lsn u64@56、origin_timestamp i64@64；gid 自 MAXALIGN(72)=72
     * 起（gidlen 字节，含尾 NUL）。</p>
     *
     * @param xid             被准备的事务 xid（头 xid@8 与记录头 xid 同值）
     * @param preparedAtMicros 准备时间戳（自 2000-01-01 起微秒）
     * @param gid             两阶段全局事务名（≤199B，留尾 NUL 位）
     * @return 完整记录字节数组
     */
    public static byte[] prepareRecord(int xid, long preparedAtMicros, String gid) {
        byte[] gidBytes = gid.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, 0x140601L);                                    // magic（TWO_PHASE_MAGIC 位形）
        put32(m, 72L + gidBytes.length + 1);                    // total_len
        put32(m, xid & 0xFFFFFFFFL);                            // xid@8
        put32(m, 16385L);                                       // database@12
        put64(m, preparedAtMicros);                             // prepared_at@16
        put32(m, 10L);                                          // owner@24
        for (int i = 28; i < 52; i += 4) {
            put32(m, 0L);                                       // 六个计数全零 @28..48
        }
        m.write(0);                                             // initfileinval u8@52
        m.write(0);                                             // C 对齐垫@53（gidlen 对齐至 54）
        put16(m, gidBytes.length + 1);                          // gidlen@54 = strlen+1（含尾 NUL）
        put64(m, 0L);                                           // origin_lsn@56
        put64(m, 0L);                                           // origin_timestamp@64
        m.writeBytes(gidBytes);                                 // gid@72
        m.write(0);                                             // 尾 NUL（gidlen 计入）
        return record(RM_XACT_ID, XLOG_XACT_PREPARE, xid).main(m.toByteArray()).build();
    }

    /**
     * 拼一条 ASSIGNMENT 记录——main = {@code xl_xact_assignment {xtop u32@0,
     * nsubxacts i32@4, subxacts u32[]@8}（spike 发现 25 的子 xid 归并兜底通道）。
     *
     * @param topXid 顶层事务 id
     * @param subxids 被归并的子事务 xid 列表
     * @return 完整记录字节数组
     */
    public static byte[] assignmentRecord(int topXid, int... subxids) {
        ByteArrayOutputStream m = new ByteArrayOutputStream();
        put32(m, topXid & 0xFFFFFFFFL);
        put32(m, subxids.length);
        for (int sub : subxids) {
            put32(m, sub & 0xFFFFFFFFL);
        }
        return record(RM_XACT_ID, XLOG_XACT_ASSIGNMENT, topXid).main(m.toByteArray()).build();
    }

    /**
     * 断言当前存在可落 image/data 的块，否则 DSL 误用即刻失败。
     *
     * @throws IllegalStateException 尚未调用 block/sameRel
     */
    private void requireCurrentBlock() {
        if (current == null) {
            throw new IllegalStateException("image/data require a preceding block(...) or sameRel(...)");
        }
    }

    /**
     * 向流写 little-endian u16。
     *
     * @param out 目标流
     * @param v   16 位值（仅低 16 位有效）
     */
    private static void put16(ByteArrayOutputStream out, int v) {
        out.write(v);
        out.write(v >>> 8);
    }

    /**
     * 向流写 little-endian u32。
     *
     * @param out 目标流
     * @param v   32 位值
     */
    private static void put32(ByteArrayOutputStream out, long v) {
        out.write((int) (v & 0xFF));
        out.write((int) (v >>> 8) & 0xFF);
        out.write((int) (v >>> 16) & 0xFF);
        out.write((int) (v >>> 24) & 0xFF);
    }

    /**
     * 向流写 little-endian u64（xact 记录的 i64 时间戳用）。
     *
     * @param out 目标流
     * @param v   64 位值
     */
    private static void put64(ByteArrayOutputStream out, long v) {
        for (int i = 0; i < 8; i++) {
            out.write((int) ((v >>> (8 * i)) & 0xFF));
        }
    }

    /**
     * 就地写 little-endian u32（记录头 totLen/xid 用）。
     *
     * @param rec    目标数组
     * @param offset 起始偏移
     * @param v      32 位值
     */
    private static void put32(byte[] rec, int offset, long v) {
        rec[offset] = (byte) (v & 0xFF);
        rec[offset + 1] = (byte) ((v >>> 8) & 0xFF);
        rec[offset + 2] = (byte) ((v >>> 16) & 0xFF);
        rec[offset + 3] = (byte) ((v >>> 24) & 0xFF);
    }

    /**
     * 就地写 little-endian u16（页头 magic/info 用）。
     *
     * @param target 目标数组
     * @param offset 起始偏移
     * @param v      16 位值（仅低 16 位有效）
     */
    private static void put16(byte[] target, int offset, int v) {
        target[offset] = (byte) (v & 0xFF);
        target[offset + 1] = (byte) ((v >>> 8) & 0xFF);
    }

    /**
     * 就地写 little-endian u64（页头 xlp_pageaddr 用）。
     *
     * @param target 目标数组
     * @param offset 起始偏移
     * @param v      64 位值
     */
    private static void put64(byte[] target, int offset, long v) {
        for (int i = 0; i < 8; i++) {
            target[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /**
     * 单个 block 引用的拼装中间态——头区字段与载荷字节在 build 时按协议布局展开。
     */
    private static final class BlockSpec {
        final int fork;
        final int blockNo;
        final boolean sameRel;
        long spc;
        long db;
        long relNode;
        byte[] image;
        int holeOffset;
        int bimgInfo;
        int holeLen;
        byte[] data;

        /**
         * 记下块的身份字段；定位器三件组与 image/data 由链式方法补齐。
         *
         * @param fork    fork 号
         * @param blockNo 块号
         * @param sameRel true 时头区不写定位器（fork_flags 置 SAME_REL 位）
         */
        BlockSpec(int fork, int blockNo, boolean sameRel) {
            this.fork = fork;
            this.blockNo = blockNo;
            this.sameRel = sameRel;
        }

        /** 是否已声明页镜像。 */
        boolean hasImage() {
            return image != null;
        }

        /** 是否已声明块内 data。 */
        boolean hasData() {
            return data != null;
        }
    }
}
