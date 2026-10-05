package org.vastdata.vbstream.walsource.layout;

import java.io.ByteArrayOutputStream;
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
