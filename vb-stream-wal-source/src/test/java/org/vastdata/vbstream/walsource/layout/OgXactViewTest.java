package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OgXactView} 的失败先行测试——手造四记录族字节（双源互证的拼装面）+
 * og-plan 真实语料锚（xact-a/xact-b 窗口，锚 LSN 与字段值见
 * {@code src/test/resources/ogcorpus/xact-anchor.txt}）。
 *
 * <p>断言面四层：</p>
 * <ol>
 *   <li>语料窗口 A 的 12 条 XACT 记录全量解析（COMMIT×5 / ABORT_WITH_XID×2 /
 *   ASSIGNMENT / PREPARE×2 / COMMIT_PREPARED / ABORT_PREPARED）——任何字段偏移
 *   错位直接红；</li>
 *   <li>锚记录逐项核对：commitTs（epoch 微秒换算 Instant）、subxacts 数组
 *   （u64 元素——nsub=0/1/2 三形态）、ASSIGNMENT 的 xtop/subs、PREPARE 的
 *   xid/gid、两阶段确认的 u64 前缀归属键；</li>
 *   <li>窗口 B 补锚：nrels=1 + XLR_REL_COMPRESS 位（xnodes 24B/项步进——20B
 *   假设下 subxacts 段读位漂移即红）与 nsub=2 多子归并；</li>
 *   <li>手造面：四族字节自建自断（compress 位开/关两形 xnodes 步进、两阶段确认
 *   前缀 +8 平移、PREPARE 新老 magic 两头形 + gid NUL 语义、ASSIGNMENT u64 几何）
 *   + ISE 拒面（main 过短/opcode 错族/magic 未识别/gid 无 NUL/计数越界）。</li>
 * </ol>
 */
class OgXactViewTest {

    /** 窗口 A 基准 LSN（页对齐）：[0x2926000,0x2928000)。 */
    private static final long WINDOW_A_BASE = 0x2926000L;

    /** 窗口 B 基准 LSN（页对齐）：[0x292A000,0x292C000)。 */
    private static final long WINDOW_B_BASE = 0x292A000L;

    // ---- 语料锚 LSN（anchor.txt，2026-10-12 og-plan 实测） ----

    private static final long LSN_COMMIT_PLAIN = 0x29263B8L;
    private static final long LSN_COMMIT_DDL = 0x2926150L;
    private static final long LSN_ABORT_TOP = 0x2926508L;
    private static final long LSN_ABORT_SUB = 0x2926718L;
    private static final long LSN_COMMIT_S3_TOP = 0x2926798L;
    private static final long LSN_ASSIGNMENT = 0x29267F8L;
    private static final long LSN_COMMIT_WITH_SUB1 = 0x2926A38L;
    private static final long LSN_COMMIT_WITH_SUB2 = 0x2926CB0L;
    private static final long LSN_PREPARE_G1 = 0x2926E00L;
    private static final long LSN_PREPARE_G2 = 0x29270E8L;
    private static final long LSN_COMMIT_PREPARED = 0x29272E0L;
    private static final long LSN_ABORT_PREPARED = 0x2927348L;
    private static final long LSN_COMMIT_NRELS1_COMPRESS = 0x292A2B8L;
    private static final long LSN_COMMIT_TWO_SUBS = 0x292A710L;

    /** s1 提交 xact_time=845098299218221（anchor.txt）→ Instant（2000 epoch + 微秒）。 */
    private static final Instant COMMIT_PLAIN_TS = Instant.ofEpochSecond(1_791_783_099L, 218_221_000L);

    /** s6 PREPARE prepared_at=845098299233218 → Instant。 */
    private static final Instant PREPARE_G1_TS = Instant.ofEpochSecond(1_791_783_099L, 233_218_000L);

    /** s7 PREPARE prepared_at=845098299236464 → Instant。 */
    private static final Instant PREPARE_G2_TS = Instant.ofEpochSecond(1_791_783_099L, 236_464_000L);

    /** 语料事务 xid（anchor.txt：13879-13893）。 */
    private static final long XID_COMMIT_PLAIN = 13879L;
    private static final long XID_S3_TOP = 13881L;
    private static final long XID_S3_SUB = 13882L;
    private static final long XID_S4_TOP = 13883L;
    private static final long XID_S4_SUB = 13884L;
    private static final long XID_S5_SUB = 13886L;
    private static final long XID_PREPARE_G1 = 13887L;
    private static final long XID_PREPARE_G2 = 13888L;

    /** 窗口 A 的 XACT 记录族普查（任何布局回归改变计数即红）。 */
    private static final int WINDOW_A_XACT_COUNT = 12;

    /** 页头 info 位（xlog_basic.h——迷你走读与 OgWalRecordParserTest 同源）。 */
    private static final int XLP_FIRST_IS_CONTRECORD = 0x0001;

    private static final int XLP_LONG_HEADER = 0x0002;

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    /**
     * 语料窗口 A：12 条 XACT 记录全量解析 + 锚记录逐项核对（提交/回滚/归并/两阶段
     * 全形态）。
     *
     * <p>关键步骤：迷你段走读（页头跳过/续体缝合/MAXALIGN 推进）→ rmid==RM_XACT_ID
     * 过滤 → 按 opcode 分族经 {@link OgXactView} 解析（错位即 ISE 红）→ 锚 LSN
     * 精确断言 commitTs/subxacts/twophaseXid/xtop/gid。</p>
     */
    @Test
    void parsesCorpusWindowAnchoredXactFamilies() throws IOException {
        Map<Long, WalRecord> byLsn = xactRecordsOf(loadCorpus("xact-a.bin"), WINDOW_A_BASE);
        assertEquals(WINDOW_A_XACT_COUNT, byLsn.size(),
                "窗口 A XACT 记录族普查面（anchor.txt 12 条）");

        // ① 普通提交：nsub=0、commitTs 换算、归属 = 记录头 xid
        WalRecord plainRec = anchored(byLsn, LSN_COMMIT_PLAIN);
        assertEquals(XID_COMMIT_PLAIN, plainRec.xid() & 0xFFFFFFFFL);
        OgXactView.OgCommit plain = OgXactView.parseCommit(plainRec);
        assertEquals(COMMIT_PLAIN_TS, plain.commitTs());
        assertEquals(0, plain.twophaseXid());
        assertArrayEquals(new long[0], plain.subxacts());

        // DDL 提交（nmsgs=4 尾段——解析面不消费，只须不越界）
        OgXactView.parseCommit(anchored(byLsn, LSN_COMMIT_DDL));

        // ② 顶层回滚（ABORT_WITH_XID 0x70——logical 档恒为此形态）
        OgXactView.OgAbort topAbort = OgXactView.parseAbort(anchored(byLsn, LSN_ABORT_TOP));
        assertEquals(0, topAbort.twophaseXid());
        assertArrayEquals(new long[0], topAbort.subxacts());

        // ③ SAVEPOINT 子回滚：记录头 xid 即子 xid（curId 尾缀不消费）
        assertEquals(XID_S3_SUB, anchored(byLsn, LSN_ABORT_SUB).xid() & 0xFFFFFFFFL);
        OgXactView.OgAbort subAbort = OgXactView.parseAbort(anchored(byLsn, LSN_ABORT_SUB));
        assertArrayEquals(new long[0], subAbort.subxacts());

        // ③ 的顶层提交：已回滚子事务不入 commit 子列表（nsub=0）
        WalRecord s3Top = anchored(byLsn, LSN_COMMIT_S3_TOP);
        assertEquals(XID_S3_TOP, s3Top.xid() & 0xFFFFFFFFL);
        assertArrayEquals(new long[0], OgXactView.parseCommit(s3Top).subxacts());

        // ④ ASSIGNMENT：xtop u64@0 / nsub i32@8 / subs u64[]@16
        OgXactView.OgAssignment asg = OgXactView.parseAssignment(anchored(byLsn, LSN_ASSIGNMENT));
        assertEquals(XID_S4_TOP, asg.xtop());
        assertArrayEquals(new long[] {XID_S4_SUB}, asg.subxacts());

        // ④/⑤ 提交带子归并：subxacts u64 数组（nsub=1）
        assertArrayEquals(new long[] {XID_S4_SUB},
                OgXactView.parseCommit(anchored(byLsn, LSN_COMMIT_WITH_SUB1)).subxacts());
        assertArrayEquals(new long[] {XID_S5_SUB},
                OgXactView.parseCommit(anchored(byLsn, LSN_COMMIT_WITH_SUB2)).subxacts());

        // ⑥⑦ PREPARE：xid u64@8 / gid@53 NUL 截取 / prepared_at@24
        OgXactView.OgPrepare g1 = OgXactView.parsePrepare(anchored(byLsn, LSN_PREPARE_G1));
        assertEquals(XID_PREPARE_G1, g1.xid());
        assertEquals("og_t2_g1", g1.gid());
        assertEquals(PREPARE_G1_TS, g1.preparedAt());
        assertArrayEquals(new long[0], g1.subxacts());
        OgXactView.OgPrepare g2 = OgXactView.parsePrepare(anchored(byLsn, LSN_PREPARE_G2));
        assertEquals(XID_PREPARE_G2, g2.xid());
        assertEquals("og_t2_g2", g2.gid());
        assertEquals(PREPARE_G2_TS, g2.preparedAt());

        // 两阶段确认：u64 前缀归属键（记录头 xid=0——执行命令的新事务无 xid）
        OgXactView.OgCommit cp = OgXactView.parseCommit(anchored(byLsn, LSN_COMMIT_PREPARED));
        assertEquals(XID_PREPARE_G1, cp.twophaseXid());
        assertArrayEquals(new long[0], cp.subxacts());
        OgXactView.OgAbort ap = OgXactView.parseAbort(anchored(byLsn, LSN_ABORT_PREPARED));
        assertEquals(XID_PREPARE_G2, ap.twophaseXid());
        assertArrayEquals(new long[0], ap.subxacts());
    }

    /**
     * 语料窗口 B 补锚：nrels=1 + XLR_REL_COMPRESS（xnodes 每项 24B——20B 假设下
     * subxacts 读位漂移即红）与 nsub=2 嵌套 savepoint 归并。
     */
    @Test
    void parsesCorpusWindowBCompressedXnodesAndMultiSub() throws IOException {
        Map<Long, WalRecord> byLsn = xactRecordsOf(loadCorpus("xact-b.bin"), WINDOW_B_BASE);
        OgXactView.OgCommit nrels1 = OgXactView.parseCommit(
                anchored(byLsn, LSN_COMMIT_NRELS1_COMPRESS));
        assertArrayEquals(new long[0], nrels1.subxacts(),
                "compress 位下 xnodes 24B/项步进后 subxacts 段读位");
        OgXactView.OgCommit twoSubs = OgXactView.parseCommit(anchored(byLsn, LSN_COMMIT_TWO_SUBS));
        assertArrayEquals(new long[] {13892L, 13893L}, twoSubs.subxacts());
    }

    /**
     * 手造提交族：nrels/nsub 计数段 + compress 位两形 xnodes 步进（20B/24B）+
     * COMMIT_PREPARED 的 u64 前缀平移。
     */
    @Test
    void handBuiltCommitFamilyWalksCountsAndPrefix() {
        // nrels=2（无 compress → 20B/项）、nsub=2：subxacts 在 48+40=88 起
        byte[] main = new byte[48 + 2 * 20 + 2 * 8 + 8];
        put64(main, 0, 1_000_000L);        // xact_time
        put64(main, 8, 555L);              // csn
        put64(main, 16, 0L);               // xinfo
        put32(main, 24, 2);                // nrels
        put32(main, 28, 2);                // nsubxacts
        put32(main, 32, 0);                // nmsgs
        put32(main, 36, 24581);            // dbId
        put32(main, 40, 1663);             // tsId
        put32(main, 44, 0);                // nlibrary
        put64(main, 88, 7001L);            // subxact[0]
        put64(main, 96, 7002L);            // subxact[1]
        WalRecord rec = parse(ogXact(HeapOps.XLOG_XACT_COMMIT, 7000, main));
        OgXactView.OgCommit c = OgXactView.parseCommit(rec);
        assertArrayEquals(new long[] {7001L, 7002L}, c.subxacts());
        assertEquals(Instant.ofEpochSecond(946_684_801L), c.commitTs());

        // compress 位（info 0x04）：xnodes 24B/项——同计数下 subxacts 前移 8B，
        // 读位对 20B 假设漂移（手造第二形钉步进敏感性）
        byte[] mainC = new byte[48 + 2 * 24 + 8 + 8];
        System.arraycopy(main, 0, mainC, 0, 48);
        put32(mainC, 28, 1);               // nsubxacts=1
        put64(mainC, 48 + 48, 7001L);      // 48+2*24=96 起
        WalRecord recC = parse(ogXact(HeapOps.XLOG_XACT_COMMIT | HeapOps.OG_XLR_REL_COMPRESS,
                7000, mainC));
        assertArrayEquals(new long[] {7001L},
                OgXactView.parseCommit(recC).subxacts());

        // COMMIT_PREPARED：u64 前缀 xid + commit 结构整体 +8（nrels@32/nsub@36/xnodes@56）
        byte[] mainP = new byte[8 + 56 + 8];
        put64(mainP, 0, 7005L);            // 2pc xid
        put64(mainP, 8, 2_000_000L);       // xact_time
        put32(mainP, 8 + 24, 0);           // nrels
        put32(mainP, 8 + 28, 1);           // nsubxacts
        put64(mainP, 56, 7006L);           // subxact[0]
        OgXactView.OgCommit cp = OgXactView.parseCommit(parse(ogXact(
                HeapOps.XLOG_XACT_COMMIT_PREPARED | HeapOps.OG_XLR_REL_COMPRESS, 0, mainP)));
        assertEquals(7005L, cp.twophaseXid());
        assertArrayEquals(new long[] {7006L}, cp.subxacts());
        assertEquals(Instant.ofEpochSecond(946_684_802L), cp.commitTs());
    }

    /**
     * 手造回滚族：ABORT_WITH_XID 的 xnodes 步进 + 子数组 + curId 尾缀（不消费）、
     * ABORT_PREPARED 前缀平移。
     */
    @Test
    void handBuiltAbortFamilyWalksCompressedAndPrepared() {
        // ABORT_WITH_XID：nrels=1（compress → 24B）+ nsub=1 + curId u64 尾
        byte[] main = new byte[20 + 24 + 8 + 8];
        put64(main, 0, 3_000_000L);        // xact_time
        put32(main, 8, 1);                 // nrels
        put32(main, 12, 1);                // nsubxacts
        put32(main, 16, 0);                // nlibrary
        put64(main, 44, 7101L);            // subxact[0]（20+24 起）
        put64(main, 52, 7100L);            // curId 尾缀（== 记录头 xid，不消费）
        WalRecord rec = parse(ogXact(HeapOps.OG_XLOG_XACT_ABORT_WITH_XID
                | HeapOps.OG_XLR_REL_COMPRESS, 7100, main));
        OgXactView.OgAbort a = OgXactView.parseAbort(rec);
        assertEquals(0, a.twophaseXid());
        assertArrayEquals(new long[] {7101L}, a.subxacts());

        // ABORT_PREPARED：u64 前缀 + abort 结构 +8（nrels@16/nsub@20/xnodes@28）
        byte[] mainP = new byte[8 + 28];
        put64(mainP, 0, 7105L);            // 2pc xid
        put64(mainP, 8, 4_000_000L);       // xact_time
        put32(mainP, 16, 0);               // nrels
        put32(mainP, 20, 0);               // nsubxacts
        OgXactView.OgAbort ap = OgXactView.parseAbort(parse(ogXact(
                HeapOps.XLOG_XACT_ABORT_PREPARED, 0, mainP)));
        assertEquals(7105L, ap.twophaseXid());
        assertArrayEquals(new long[0], ap.subxacts());
    }

    /**
     * 手造 PREPARE：新老两头形（magic 0x57F94534 → 272B / 0x57F94532 → 264B）的
     * xid/gid/subxacts 几何 + gid NUL 语义 + magic 拒面。
     */
    @Test
    void handBuiltPrepareWalksHeaderVariantsAndGid() {
        byte[] main = new byte[272 + 2 * 8];
        put32(main, 0, 0x57F94534L);       // TWOPHASE_MAGIC_COMPRESSION（5.0.0 新装形）
        put32(main, 4, main.length);       // total_len
        put64(main, 8, 7201L);             // xid
        put32(main, 16, 24581);            // database
        put64(main, 24, 5_000_000L);       // prepared_at
        put32(main, 32, 10);               // owner
        put32(main, 36, 2);                // nsubxacts
        byte[] gid = "seam_gid_9".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(gid, 0, main, 53, gid.length);   // gid 区 NUL 垫底（新数组恒 0）
        put64(main, 272, 7202L);
        put64(main, 280, 7203L);
        OgXactView.OgPrepare p = OgXactView.parsePrepare(parse(ogXact(
                HeapOps.XLOG_XACT_PREPARE, 7201, main)));
        assertEquals(7201L, p.xid());
        assertEquals("seam_gid_9", p.gid());
        assertEquals(Instant.ofEpochSecond(946_684_805L), p.preparedAt());
        assertArrayEquals(new long[] {7202L, 7203L}, p.subxacts());

        // 老 magic（0x57F94532 → 264B 头）：subxacts 自 264 起
        byte[] mainOld = new byte[264 + 8];
        put32(mainOld, 0, 0x57F94532L);
        put64(mainOld, 8, 7205L);
        put32(mainOld, 36, 1);
        put64(mainOld, 264, 7206L);
        OgXactView.OgPrepare old = OgXactView.parsePrepare(parse(ogXact(
                HeapOps.XLOG_XACT_PREPARE, 7205, mainOld)));
        assertArrayEquals(new long[] {7206L}, old.subxacts());

        // magic 拒面：未识别头形 ISE
        byte[] mainBad = new byte[272];
        put32(mainBad, 0, 0xDEADBEEFL);
        IllegalStateException eMagic = assertThrows(IllegalStateException.class,
                () -> OgXactView.parsePrepare(parse(ogXact(HeapOps.XLOG_XACT_PREPARE, 1, mainBad))));
        assertTrue(eMagic.getMessage().contains("0xDEADBEEF"), eMagic.getMessage());

        // gid 无 NUL 拒面：200B 定长区填满非零
        byte[] mainNoNul = new byte[272];
        put32(mainNoNul, 0, 0x57F94534L);
        Arrays.fill(mainNoNul, 53, 53 + 200, (byte) 'g');
        IllegalStateException eNul = assertThrows(IllegalStateException.class,
                () -> OgXactView.parsePrepare(parse(ogXact(HeapOps.XLOG_XACT_PREPARE, 1, mainNoNul))));
        assertTrue(eNul.getMessage().contains("无 NUL"), eNul.getMessage());
    }

    /**
     * 手造 ASSIGNMENT 的 u64 几何 + 全族 ISE 拒面（main 过短 / 计数越界 / opcode 错族）。
     */
    @Test
    void handBuiltAssignmentAndRejectionFaces() {
        byte[] main = new byte[16 + 2 * 8];
        put64(main, 0, 7301L);             // xtop
        put32(main, 8, 2);                 // nsubxacts
        put64(main, 16, 7302L);
        put64(main, 24, 7303L);
        OgXactView.OgAssignment asg = OgXactView.parseAssignment(parse(ogXact(
                HeapOps.XLOG_XACT_ASSIGNMENT, 7302, main)));
        assertEquals(7301L, asg.xtop());
        assertArrayEquals(new long[] {7302L, 7303L}, asg.subxacts());

        // main 过短（< 16）
        IllegalStateException eShort = assertThrows(IllegalStateException.class,
                () -> OgXactView.parseAssignment(parse(ogXact(HeapOps.XLOG_XACT_ASSIGNMENT, 1, new byte[15]))));
        assertTrue(eShort.getMessage().contains("too short"), eShort.getMessage());

        // 子数组越界（n=2 但 main 只容 1）
        byte[] mainOverflow = new byte[16 + 8];
        put32(mainOverflow, 8, 2);
        IllegalStateException eOverflow = assertThrows(IllegalStateException.class,
                () -> OgXactView.parseAssignment(parse(ogXact(HeapOps.XLOG_XACT_ASSIGNMENT, 1, mainOverflow))));
        assertTrue(eOverflow.getMessage().contains("越界"), eOverflow.getMessage());

        // commit main 过短（< 16）与 opcode 错族（回滚记录喂 parseCommit）
        assertThrows(IllegalStateException.class,
                () -> OgXactView.parseCommit(parse(ogXact(HeapOps.XLOG_XACT_COMMIT, 1, new byte[8]))));
        IllegalStateException eOp = assertThrows(IllegalStateException.class,
                () -> OgXactView.parseCommit(parse(ogXact(HeapOps.OG_XLOG_XACT_ABORT_WITH_XID, 1, new byte[28]))));
        assertTrue(eOp.getMessage().contains("opcode"), eOp.getMessage());
        assertThrows(IllegalStateException.class,
                () -> OgXactView.parseAbort(parse(ogXact(HeapOps.XLOG_XACT_COMMIT, 1, new byte[56]))));
    }

    // ---- 测试基建 ----

    /**
     * 迷你段走读 + rmid==RM_XACT_ID 过滤（形态与 OgWalRecordParserTest 同源：页头
     * 跳过/续体缝合/MAXALIGN 推进；XACT 记录无块引用，走读只定 main 偏移）。
     *
     * @param seg  窗口字节（起点页对齐）
     * @param base 窗口基准 LSN
     * @return lsn → 走读记录（仅 XACT rmid）
     */
    private Map<Long, WalRecord> xactRecordsOf(byte[] seg, long base) {
        Map<Long, WalRecord> out = new HashMap<>();
        for (SegRecord sr : walkSegment(seg)) {
            if (sr.rec()[25] == HeapOps.RM_XACT_ID) {
                out.put(sr.lsn(), WalRecordParser.parse(sr.rec(), sr.lsn(), og));
            }
        }
        assertFalse(out.isEmpty(), "窗口内无 XACT 记录——语料漂移或走读断裂（基准 "
                + Long.toHexString(base) + "）");
        return out;
    }

    /**
     * 迷你段走读：逐页校验 magic → 记录链 MAXALIGN 推进 + 跨页续体缝合。
     *
     * @param seg 窗口字节
     * @return 逐记录 (lsn, 已缝合记录字节)
     */
    private static List<SegRecord> walkSegment(byte[] seg) {
        List<SegRecord> out = new ArrayList<>();
        int pageSize = 8192;
        for (int page = 0; page + pageSize <= seg.length; page += pageSize) {
            assertEquals(0xD074, leU16(seg, page) & 0xFFFF, "页魔数错配于窗口偏移 " + page);
            int xlpInfo = leU16(seg, page + 2);
            long pageAddr = leU64(seg, page + 8);
            int remLen = leU32(seg, page + 16);
            int hdr = (xlpInfo & XLP_LONG_HEADER) != 0 ? 40 : 24;
            int pos = page + hdr;
            boolean first = true;
            while (pos + 4 <= page + pageSize && leU32(seg, pos) != 0) {
                if (first && (xlpInfo & XLP_FIRST_IS_CONTRECORD) != 0) {
                    pos += (remLen + 7) & ~7;
                    first = false;
                    continue;
                }
                int totLen = leU32(seg, pos);
                assertTrue(totLen >= 32, "totLen 短于记录头——链断裂");
                long lsn = pageAddr + (pos - page);
                out.add(new SegRecord(lsn, stitchRecord(seg, page, pos, totLen)));
                first = false;
                pos += (totLen + 7) & ~7;
            }
        }
        return out;
    }

    /**
     * 取一条记录的 totLen 字节（跨页记录缝合续体，跳过中间页短页头）。
     *
     * @param seg    窗口字节
     * @param page   记录起始所在页偏移
     * @param pos    记录起点偏移
     * @param totLen 记录总长
     * @return 恰长记录字节
     */
    private static byte[] stitchRecord(byte[] seg, int page, int pos, int totLen) {
        byte[] rec = new byte[totLen];
        int copied = Math.min(totLen, page + 8192 - pos);
        System.arraycopy(seg, pos, rec, 0, copied);
        int cur = page;
        while (copied < totLen) {
            cur += 8192;
            assertTrue(leU16(seg, cur) == 0xD074 && (leU16(seg, cur + 2) & XLP_FIRST_IS_CONTRECORD) != 0,
                    "续体页标志缺失于窗口偏移 " + cur);
            int take = Math.min(totLen - copied, 8192 - 24);
            System.arraycopy(seg, cur + 24, rec, copied, take);
            copied += take;
        }
        return rec;
    }

    /**
     * 手造 OG XACT 记录字节：32B 记录头（totLen/term/xid u64/prev/info/rmid）+
     * 终止头（main ≤255B 用 DATA_SHORT，更长用 DATA_LONG——PREPARE 的两阶段文件头
     * 恒超 255B）+ main（XACT 记录无块引用）。
     *
     * @param info info 字节（opcode + 可选 OG_XLR_REL_COMPRESS 位）
     * @param xid  记录头 xid（低 32 位面）
     * @param main main data 字节
     * @return 记录字节数组
     */
    private static byte[] ogXact(int info, long xid, byte[] main) {
        boolean shortHeader = main.length <= 255;
        int headerLen = shortHeader ? 2 : 5;
        byte[] rec = new byte[32 + headerLen + main.length];
        put32(rec, 0, rec.length);
        put32(rec, 4, 1);                  // term（单机实测恒 0x1）
        put64(rec, 8, xid);                // xid u64@8（TransactionId 8B 宽）
        put32(rec, 28, 0);                 // crc（走读不校验）
        rec[24] = (byte) info;
        rec[25] = (byte) HeapOps.RM_XACT_ID;
        if (shortHeader) {
            rec[32] = (byte) 255;          // DATA_SHORT
            rec[33] = (byte) main.length;
        } else {
            rec[32] = (byte) 254;          // DATA_LONG
            put32(rec, 33, main.length);
        }
        System.arraycopy(main, 0, rec, 32 + headerLen, main.length);
        return rec;
    }

    /**
     * 走读一条手造记录（lsn 取哨兵值）。
     *
     * @param bytes 记录字节
     * @return 走读结果
     */
    private WalRecord parse(byte[] bytes) {
        return WalRecordParser.parse(bytes, 0x100L, og);
    }

    /**
     * 加载 ogcorpus 语料窗口。
     *
     * @param name 资源名（xact-a.bin / xact-b.bin）
     * @return 窗口字节
     * @throws IOException 资源缺失或读取失败（语料是断言面组成部分，不降级）
     */
    private static byte[] loadCorpus(String name) throws IOException {
        try (InputStream in = OgXactViewTest.class.getResourceAsStream("/ogcorpus/" + name)) {
            assertNotNull(in, "语料资源缺失：ogcorpus/" + name);
            ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
            in.transferTo(out);
            return out.toByteArray();
        }
    }

    /**
     * 按 LSN 取锚记录，缺失即失败。
     *
     * @param byLsn 走读索引
     * @param lsn   锚 LSN
     * @return 解析结果（必存在）
     */
    private static WalRecord anchored(Map<Long, WalRecord> byLsn, long lsn) {
        WalRecord r = byLsn.get(lsn);
        assertNotNull(r, "锚记录缺失于 LSN " + Long.toHexString(lsn));
        return r;
    }

    /**
     * 就地写 little-endian u32。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      32 位值
     */
    private static void put32(byte[] b, int offset, long v) {
        for (int i = 0; i < 4; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /**
     * 就地写 little-endian u64。
     *
     * @param b      目标数组
     * @param offset 起始偏移
     * @param v      64 位值
     */
    private static void put64(byte[] b, int offset, long v) {
        for (int i = 0; i < 8; i++) {
            b[offset + i] = (byte) ((v >>> (8 * i)) & 0xFF);
        }
    }

    /**
     * 就地读 little-endian u16。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 16 位值
     */
    private static int leU16(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8);
    }

    /**
     * 就地读 little-endian u32。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 32 位值
     */
    private static int leU32(byte[] b, int offset) {
        return (b[offset] & 0xFF) | ((b[offset + 1] & 0xFF) << 8)
                | ((b[offset + 2] & 0xFF) << 16) | ((b[offset + 3] & 0xFF) << 24);
    }

    /**
     * 就地读 little-endian u64。
     *
     * @param b      源数组
     * @param offset 起始偏移
     * @return 64 位值
     */
    private static long leU64(byte[] b, int offset) {
        return (leU32(b, offset) & 0xFFFFFFFFL) | (leU32(b, offset + 4) & 0xFFFFFFFFL) << 32;
    }

    /**
     * 迷你段走读的产出单元：记录起始 LSN + 已缝合记录字节。
     *
     * @param lsn 记录起始 LSN
     * @param rec 记录 totLen 字节
     */
    private record SegRecord(long lsn, byte[] rec) {
    }
}
