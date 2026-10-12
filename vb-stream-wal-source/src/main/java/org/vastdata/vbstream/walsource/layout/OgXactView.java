package org.vastdata.vbstream.walsource.layout;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * openGauss v5.0.0 XACT 记录族 main data 视图（M2 T2）——把四记录族 + 七 opcode
 * （含 OG 独有 COMMIT_COMPACT/ABORT_WITH_XID）转录为强类型 record，纯函数零状态。
 *
 * <p><b>源码出处（openGauss-server v5.0.0，本地克隆 /tmp/og-src）</b>：
 * {@code src/include/access/xact.h}（结构定义 + opcode 表 L121-128）、
 * {@code src/include/storage/smgr/relfilenode.h}（ColFileNodeRel=RelFileNodeOld 12B +
 * forknum 4B + ownerid 4B = 20B；ColFileNode = RelFileNode 16B + 同尾 = 24B）、
 * {@code src/include/access/twophase.h}（TwoPhaseFileHeader：gid[200]@53、
 * ncommitlibrarys@256；TwoPhaseFileHeaderNew = +2×i32 = 272B）、
 * {@code src/gausskernel/storage/access/transam/xact.cpp}（RecordTransactionCommit/
 * RecordTransactionAbort/AssignTransactionId 的 XLogRegisterData 注册序）与
 * {@code .../twophase.cpp}（SaveTransactionState/EndPrepare 注册序）。</p>
 *
 * <p><b>与 vanilla（REL_18 xact.h）的关键差异</b>：①<b>TransactionId 是 u64</b>
 * （c.h L450 {@code typedef uint64 TransactionId}）——subxacts 数组元素、两阶段确认的
 * 2pc xid、ASSIGNMENT 的 xtop 均按 8B 读；②commit 结构是<b>平铺计数字段</b>而非
 * vanilla 的 xinfo 位块链——nrels/nsubxacts/nmsgs/dbId/tsId/nlibrary 恒内联
 * （{@code offsetof(xnodes)=48}），xinfo u64@16 是 OG 自有标志位
 * （relcache-init-inval 等 4 位），<b>无 vanilla 的 HAS_DBINFO/SUBXACTS/TWOPHASE/
 * GID 位块语义</b>；③两阶段确认（COMMIT_PREPARED/ABORT_PREPARED）以 <b>u64 xid 前缀
 * + 同构 commit/abort 结构</b>承载归属键，main 内<b>无 gid</b>（gid 只在 PREPARE 的
 * 两阶段文件头里）；④回滚在 logical 档恒为 <b>ABORT_WITH_XID(0x70)</b>——subxacts
 * 殿后追加 curId u64（恒等于记录头 xid，解析面不消费）；⑤commit/abort 尾随
 * RecentXmin u64（单机构建恒注册）与 invalmsgs（16B/条）——本视图不消费、不校验其
 * 收口（只保证 subxacts 段读取自洽）。</p>
 *
 * <p><b>语料锚（og-plan 容器 opengauss:5.0.0，m2pg 库[PG 档]，wal_level=logical，
 * 2026-10-12 采集）</b>：见 {@code src/test/resources/ogcorpus/xact-anchor.txt}——
 * 12 条 XACT 记录覆盖 COMMIT(nsub=0/1/2、nrels=0/1)/ABORT_WITH_XID(顶层/子)/
 * ASSIGNMENT/PREPARE(gid)/COMMIT_PREPARED/ABORT_PREPARED 全形态；锚 LSN 与字段值
 * 逐项记档（COMMIT lsn=0x29263B8、带子 lsn=0x2926A38、ABORT_WITH_XID lsn=0x2926508/
 * 0x2926718、ASSIGNMENT lsn=0x29267F8、PREPARE lsn=0x2926E00/0x29270E8、
 * COMMIT_PREPARED lsn=0x29272E0、ABORT_PREPARED lsn=0x2927348、nrels=1 xnode 24B 锚
 * lsn=0x292A2B8、nsub=2 锚 lsn=0x292A710）。COMMIT_COMPACT(0x60) 未锚（logical 档
 * 恒全量形态），布局为源码推导。</p>
 *
 * <p>线程约束：静态纯函数、零共享状态，任意线程并发安全。所有读取只依赖
 * {@link WalRecord#raw()}/{@link WalRecord#mainOff()}/{@link WalRecord#mainLen()}
 * 契约（走读已完成前提），越界由前置长度校验抛 ISE（消息含 LSN 与字段上下文）。</p>
 */
public final class OgXactView {

    /** PG epoch（2000-01-01T00:00:00Z）折算微秒——XACT 时间戳的换算源（同 XactGrouper）。 */
    private static final long EPOCH_2000_MICROS = 946_684_800_000_000L;

    /** xl_xact_commit 的 xnodes 偏移（offsetof(xnodes)=48——xact_time/csn/xinfo 三 i64
     *  + nrels/nsubxacts/nmsgs/dbId/tsId/nlibrary 六计数字段；源码 + 语料 56B 最小 main
     *  自洽锚）。 */
    private static final int COMMIT_XNODES_OFF = 48;

    /** xl_xact_abort 的 xnodes 偏移（offsetof(xnodes)=20——xact_time i64 + nrels/
     * nsubxacts/nlibrary 三 i32；源码 + 语料 28B main[ABORT_WITH_XID 含 curId] 自洽锚）。 */
    private static final int ABORT_XNODES_OFF = 20;

    /** 两阶段确认前缀：2pc xid u64@0，commit/abort 结构整体 +8（源码
     * xl_xact_commit_prepared/xl_xact_abort_prepared + 语料 0x29272E0/0x2927348 锚）。 */
    private static final int PREPARED_XID_PREFIX = 8;

    /** xl_xact_commit_compact 的 subxacts 偏移（xact_time i64@0 + csn u64@8 +
     * nsubxacts i32@16 后 8 对齐到 24——<b>源码推导未语料锚</b>，logical 档恒全量形态）。 */
    private static final int COMPACT_SUBXACTS_OFF = 24;

    /** xl_xact_assignment 的 subxacts 偏移（xtop u64@0 + nsubxacts i32@8 后 8 对齐到
     * 16；语料 0x29267F8 main=24B=16+8×1 锚）。 */
    private static final int ASSIGNMENT_SUBXACTS_OFF = 16;

    /** twophase.h：TwoPhaseFileHeader 的 gid 定长区起点（magic/total_len/xid/database/
     * pad/prepared_at/owner/nsubxacts/ncommitrels/nabortrels/ninvalmsgs/initfileinval
     * 之后；语料 'og_t2_g1'@53 锚）。 */
    private static final int PREPARE_GID_OFF = 53;

    /** twophase.h：GIDSIZE=200——gid 定长区 [53,253)，strncpy_s(gid, GIDSIZE, ..., GIDSIZE-1)
     * 保证区内必有 NUL。 */
    private static final int GID_SIZE = 200;

    /** twophase.h：TWOPHASE_MAGIC（9.2 基线头形，sizeof=264——children 紧随其后）。 */
    private static final int TWOPHASE_MAGIC_OLD = 0x57F94532;

    /** twophase.h：TWOPHASE_MAGIC_NEW / TWOPHASE_MAGIC_COMPRESSION（TwoPhaseFileHeaderNew
     *  272B：+ncommitrels_temp/nabortrels_temp 两 i32；5.0.0 新装实例即此形——语料
     *  0x2926E00 magic=0x57F94534 实锚）。两 magic 的头形同宽，子事务数组同在头后。 */
    private static final int TWOPHASE_MAGIC_NEW = 0x57F94533;

    private static final int TWOPHASE_MAGIC_COMPRESSION = 0x57F94534;

    /** TwoPhaseFileHeaderNew 头尺寸（272B——magic NEW/COMPRESSION 两形同宽）。 */
    private static final int PREPARE_HEADER_SIZE_NEW = 272;

    /** TwoPhaseFileHeader 老头尺寸（264B——magic OLD 形）。 */
    private static final int PREPARE_HEADER_SIZE_OLD = 264;

    /** commitTs 的换算辅助可见性（测试断言同口径换算）：PG 微秒 → Instant。 */
    private static final long NANOS_PER_MICRO = 1_000L;

    /**
     * 工具类防实例化。
     */
    private OgXactView() {
    }

    /**
     * 提交族 main 的解析面（COMMIT 0x00 / COMMIT_COMPACT 0x60 / COMMIT_PREPARED 0x30）。
     *
     * @param commitTs    提交时间戳（xact_time i64@0——COMMIT_PREPARED 在 +8）
     * @param twophaseXid 两阶段归属 xid（u64@0——仅 COMMIT_PREPARED；其余为 0）
     * @param subxacts    提交归并的子事务 xid 数组（u64 元素——空数组为无子）
     */
    public record OgCommit(Instant commitTs, long twophaseXid, long[] subxacts) {
    }

    /**
     * 回滚族 main 的解析面（ABORT 0x20 / ABORT_WITH_XID 0x70 / ABORT_PREPARED 0x40）。
     *
     * @param abortTs     回滚时间戳（xact_time i64@0——ABORT_PREPARED 在 +8）
     * @param twophaseXid 两阶段归属 xid（u64@0——仅 ABORT_PREPARED；其余为 0）
     * @param subxacts    一并回滚的子事务 xid 数组（u64 元素）
     */
    public record OgAbort(Instant abortTs, long twophaseXid, long[] subxacts) {
    }

    /**
     * PREPARE 记录 main（两阶段状态文件头）的解析面。
     *
     * @param xid        被挂起事务的 xid（u64@8）
     * @param preparedAt 准备时间戳（i64@24）
     * @param gid        全局事务名（gid 定长区 [53,253) 内 NUL 结尾截取）
     * @param subxacts   随事务挂起的子事务 xid 数组（文件头之后、按 nsubxacts 截取）
     */
    public record OgPrepare(long xid, Instant preparedAt, String gid, long[] subxacts) {
    }

    /**
     * ASSIGNMENT 记录 main 的解析面。
     *
     * @param xtop    顶层事务 xid（u64@0）
     * @param subxacts 归并到顶层的子事务 xid 数组（u64[]@16）
     */
    public record OgAssignment(long xtop, long[] subxacts) {
    }

    /**
     * 解析提交族记录的 main：COMMIT / COMMIT_COMPACT / COMMIT_PREPARED 三形态分派。
     *
     * <p>关键步骤：opcode 三路分派（其余 opcode ISE）；COMMIT_PREPARED 先取 u64 前缀
     * xid、结构整体 +8；COMMIT/COMPACT 读 xact_time 为 commitTs；全量形态按
     * nrels（xnodes 步进宽经 {@link HeapOps#OG_XLR_REL_COMPRESS} 位分派 20/24B）走
     * 过关系数组后读 nsubxacts 个 u64；COMPACT 无 xnodes 段、subxacts 直接在 24 起。
     * 边界与异常语义：main 短于对应结构前缀或 subxacts 尾越界抛 ISE（消息含 LSN 与
     * 计数上下文）；invalmsgs/RecentXmin/library 尾段不校验收口（不消费）。线程约束：
     * 静态纯函数。</p>
     *
     * @param rec 走读完成的 XACT 记录
     * @return 提交族解析面
     * @throws IllegalStateException opcode 错配或 main 过短/越界
     */
    public static OgCommit parseCommit(WalRecord rec) {
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
        switch (op) {
            case HeapOps.XLOG_XACT_COMMIT, HeapOps.OG_XLOG_XACT_COMMIT_COMPACT -> {
                requireMainLen(rec, 16, "xl_xact_commit");
                Instant ts = instantOfPgMicros(i64(raw, off));
                if (op == HeapOps.OG_XLOG_XACT_COMMIT_COMPACT) {
                    int n = s32(raw, off + 16);
                    return new OgCommit(ts, 0, subxacts(rec, off + COMPACT_SUBXACTS_OFF, n));
                }
                return new OgCommit(ts, 0, commitSubxacts(rec, off));
            }
            case HeapOps.XLOG_XACT_COMMIT_PREPARED -> {
                requireMainLen(rec, PREPARED_XID_PREFIX + 16, "xl_xact_commit_prepared");
                long xid = u64(raw, off);
                Instant ts = instantOfPgMicros(i64(raw, off + PREPARED_XID_PREFIX));
                long[] subs = commitSubxacts(rec, off + PREPARED_XID_PREFIX);
                return new OgCommit(ts, xid, subs);
            }
            default -> throw new IllegalStateException(String.format(
                    "og xact commit 解析收到非提交族 opcode 0x%02X: lsn=%d", op, rec.lsn()));
        }
    }

    /**
     * 解析回滚族记录的 main：ABORT / ABORT_WITH_XID / ABORT_PREPARED 三形态分派。
     *
     * <p>关键步骤：opcode 三路分派；ABORT_PREPARED 取 u64 前缀 xid、结构 +8；其余
     * twophaseXid=0；nsubxacts@8（prepared 形 +8 后 @16），xnodes@20（+8 后 @28），
     * 按 nrels 与 compress 位步进后读 nsubxacts 个 u64。ABORT_WITH_XID 的 curId 尾缀
     * 恒等于记录头 xid，不消费。边界与异常语义：main 过短/subxacts 越界抛 ISE。
     * 线程约束：静态纯函数。</p>
     *
     * @param rec 走读完成的 XACT 记录
     * @return 回滚族解析面
     * @throws IllegalStateException opcode 错配或 main 过短/越界
     */
    public static OgAbort parseAbort(WalRecord rec) {
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        int op = rec.info() & HeapOps.XLOG_XACT_OPMASK;
        switch (op) {
            case HeapOps.XLOG_XACT_ABORT, HeapOps.OG_XLOG_XACT_ABORT_WITH_XID -> {
                requireMainLen(rec, ABORT_XNODES_OFF, "xl_xact_abort");
                Instant ts = instantOfPgMicros(i64(raw, off));
                return new OgAbort(ts, 0, abortSubxacts(rec, off));
            }
            case HeapOps.XLOG_XACT_ABORT_PREPARED -> {
                requireMainLen(rec, PREPARED_XID_PREFIX + ABORT_XNODES_OFF, "xl_xact_abort_prepared");
                long xid = u64(raw, off);
                Instant ts = instantOfPgMicros(i64(raw, off + PREPARED_XID_PREFIX));
                return new OgAbort(ts, xid, abortSubxacts(rec, off + PREPARED_XID_PREFIX));
            }
            default -> throw new IllegalStateException(String.format(
                    "og xact abort 解析收到非回滚族 opcode 0x%02X: lsn=%d", op, rec.lsn()));
        }
    }

    /**
     * 解析 PREPARE 记录的 main（两阶段状态文件头 + 其后的两阶段记录链首段）。
     *
     * <p>关键步骤：校验 main ≥ 老头 264B 与 magic ∈ {OLD, NEW, COMPRESSION}（其余值
     * ISE——版本未转录的两阶段头形）；xid u64@8、prepared_at i64@24；gid 在 [53,253)
     * 定长区内找 NUL 截取（区内无 NUL 抛 ISE——strncpy_s 语义下面内必有，此为手造/
     * 异源字节的防御面）；nsubxacts i32@36 的子事务 u64 数组紧随文件头（老 magic 264B /
     * 新两 magic 272B——SaveTransactionState 注册序：头、children、commitrels、
     * abortrels、invalmsgs、libraries）。边界与异常语义：越界抛 ISE。线程约束：静态
     * 纯函数。</p>
     *
     * @param rec 走读完成的 PREPARE 记录
     * @return PREPARE 解析面
     * @throws IllegalStateException main 过短 / magic 未识别 / gid 无 NUL / 子数组越界
     */
    public static OgPrepare parsePrepare(WalRecord rec) {
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        requireMainLen(rec, PREPARE_HEADER_SIZE_OLD, "xl_xact_prepare");
        int magic = (int) u32(raw, off);
        int headerSize = switch (magic) {
            case TWOPHASE_MAGIC_OLD -> PREPARE_HEADER_SIZE_OLD;
            case TWOPHASE_MAGIC_NEW, TWOPHASE_MAGIC_COMPRESSION -> PREPARE_HEADER_SIZE_NEW;
            default -> throw new IllegalStateException(String.format(
                    "og prepare 头 magic=0x%08X 未识别（已转录集：OLD/NEW/COMPRESSION）: lsn=%d",
                    magic, rec.lsn()));
        };
        long xid = u64(raw, off + 8);
        Instant preparedAt = instantOfPgMicros(i64(raw, off + 24));
        int gidEnd = -1;
        for (int i = 0; i < GID_SIZE; i++) {
            if (raw[off + PREPARE_GID_OFF + i] == 0) {
                gidEnd = PREPARE_GID_OFF + i;
                break;
            }
        }
        if (gidEnd < 0) {
            throw new IllegalStateException("og prepare gid 区 [" + PREPARE_GID_OFF + ","
                    + (PREPARE_GID_OFF + GID_SIZE) + ") 内无 NUL 结尾: lsn=" + rec.lsn());
        }
        String gid = new String(raw, off + PREPARE_GID_OFF, gidEnd - PREPARE_GID_OFF,
                StandardCharsets.UTF_8);
        int n = s32(raw, off + 36);
        return new OgPrepare(xid, preparedAt, gid, subxacts(rec, off + headerSize, n));
    }

    /**
     * 解析 ASSIGNMENT 记录的 main：xtop u64@0 / nsubxacts i32@8 / subxacts u64[]@16。
     *
     * <p>边界与异常语义：main &lt; 16 或子数组越界抛 ISE（消息含 n 与 mainLen）。
     * 线程约束：静态纯函数。</p>
     *
     * @param rec 走读完成的 ASSIGNMENT 记录
     * @return 归并映射面
     * @throws IllegalStateException main 过短或子数组越界
     */
    public static OgAssignment parseAssignment(WalRecord rec) {
        byte[] raw = rec.raw();
        int off = rec.mainOff();
        requireMainLen(rec, ASSIGNMENT_SUBXACTS_OFF, "xl_xact_assignment");
        long xtop = u64(raw, off);
        int n = s32(raw, off + 8);
        return new OgAssignment(xtop, subxacts(rec, off + ASSIGNMENT_SUBXACTS_OFF, n));
    }

    /**
     * 全量 commit 形态的 subxacts 段读取：跳过 nrels 个 xnode（compress 位分派
     * 20B ColFileNodeRel / 24B ColFileNode）后按 nsubxacts 读 u64 数组。
     *
     * @param rec  记录（越界校验源）
     * @param base xl_xact_commit 结构起点（COMMIT_PREPARED 已含 +8 前缀）
     * @return 子事务 xid 数组
     */
    private static long[] commitSubxacts(WalRecord rec, int base) {
        byte[] raw = rec.raw();
        int nrels = s32(raw, base + 24);
        int xnodeSize = (rec.info() & HeapOps.OG_XLR_REL_COMPRESS) != 0 ? 24 : 20;
        int nsub = s32(raw, base + 28);
        return subxacts(rec, base + COMMIT_XNODES_OFF + nrels * xnodeSize, nsub);
    }

    /**
     * abort 形态的 subxacts 段读取：跳过 nrels 个 xnode（compress 位分派同 commit）
     * 后按 nsubxacts 读 u64 数组（ABORT_WITH_XID 的 curId 尾缀在子数组之后，不消费）。
     *
     * @param rec  记录（越界校验源）
     * @param base xl_xact_abort 结构起点（ABORT_PREPARED 已含 +8 前缀）
     * @return 子事务 xid 数组
     */
    private static long[] abortSubxacts(WalRecord rec, int base) {
        byte[] raw = rec.raw();
        int nrels = s32(raw, base + 8);
        int xnodeSize = (rec.info() & HeapOps.OG_XLR_REL_COMPRESS) != 0 ? 24 : 20;
        int nsub = s32(raw, base + 12);
        return subxacts(rec, base + ABORT_XNODES_OFF + nrels * xnodeSize, nsub);
    }

    /**
     * 自 pos 起读 n 个 u64 的公共面：负 n（结构垃圾）或越界抛 ISE（消息含 n/pos/
     * mainLen 与 LSN——防御手造或异源字节的计数垃圾）。
     *
     * @param rec 记录
     * @param pos 子数组起点（相对 raw 绝对偏移）
     * @param n   元素数
     * @return n 元素数组
     */
    private static long[] subxacts(WalRecord rec, int pos, int n) {
        if (n < 0 || pos < 0 || pos + 8L * n > rec.mainOff() + rec.mainLen()) {
            throw new IllegalStateException(String.format(
                    "og xact subxacts 越界: n=%d pos=%d mainOff=%d mainLen=%d lsn=%d",
                    n, pos, rec.mainOff(), rec.mainLen(), rec.lsn()));
        }
        long[] out = new long[n];
        byte[] raw = rec.raw();
        for (int i = 0; i < n; i++) {
            out[i] = u64(raw, pos + 8 * i);
        }
        return out;
    }

    /**
     * main 最小长度校验（不足抛 ISE——消息含结构名/需长/实长/LSN）。
     *
     * @param rec    记录
     * @param need   需要的最小字节数
     * @param struct 结构名（异常消息上下文）
     */
    private static void requireMainLen(WalRecord rec, int need, String struct) {
        if (rec.mainLen() < need) {
            throw new IllegalStateException(String.format(
                    "og %s main too short: %d < %d, lsn=%d", struct, rec.mainLen(), need, rec.lsn()));
        }
    }

    /**
     * PG 微秒时间戳（epoch 2000）→ Instant（floorDiv/floorMod 保负值正确——BC 侧）。
     *
     * @param micros 自 2000-01-01T00:00:00Z 起微秒
     * @return Instant
     */
    private static Instant instantOfPgMicros(long micros) {
        long epochMicros = EPOCH_2000_MICROS + micros;
        return Instant.ofEpochSecond(Math.floorDiv(epochMicros, 1_000_000L),
                Math.floorMod(epochMicros, 1_000_000L) * NANOS_PER_MICRO);
    }

    /**
     * 就地读 little-endian u32（无符号 long 语义）。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 32 位值
     */
    private static long u32(byte[] b, int o) {
        return (b[o] & 0xFFL) | ((b[o + 1] & 0xFFL) << 8) | ((b[o + 2] & 0xFFL) << 16)
                | ((b[o + 3] & 0xFFL) << 24);
    }

    /**
     * 就地读 little-endian u64（OG TransactionId 的 8B 宽）。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 64 位值
     */
    private static long u64(byte[] b, int o) {
        return u32(b, o) | (u32(b, o + 4) << 32);
    }

    /**
     * 就地读 little-endian i32（计数字段——负值由调用方防御）。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 32 位有符号值
     */
    private static int s32(byte[] b, int o) {
        return (int) u32(b, o);
    }

    /**
     * 就地读 little-endian i64（XACT 时间戳——{@link #u64} 的同位读，返回载体即
     * 两位补全的 64 位值，时间戳换算经 epoch 微秒算术保语义）。
     *
     * @param b 源数组
     * @param o 起始偏移
     * @return 64 位值
     */
    private static long i64(byte[] b, int o) {
        return u64(b, o);
    }
}
