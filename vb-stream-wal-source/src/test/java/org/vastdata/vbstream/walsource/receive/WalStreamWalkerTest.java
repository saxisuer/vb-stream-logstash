package org.vastdata.vbstream.walsource.receive;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * WalStreamWalker 流走读状态机的失败先行测试——页/记录导航 + pageaddr 锚定协议。
 *
 * <p>断言面来自任务书 Step 1 五用例：单页多记录顺序产出与 consumedLsn 推进 /
 * 记录跨页经 contrecord 缝合 / chunk 任意切分（16 字节粒度）与一次 feed 全等 /
 * 流首孤立续体页跳过计数 / pageaddr 失配 32B 受控再同步（产出后续记录且
 * resyncs==1）与失配 3 页 ISE。字节面全部经 {@link WalBytes} 整页拼装 helper
 * （{@code page(pageAddr, ...segments)}）构造——页头 pageaddr 由 DSL 写入，
 * 与被测 walker 的锚定校验互为独立转录（双源互证）。</p>
 */
class WalStreamWalkerTest {

    /** rmgrlist.h 0-based 枚举序：RM_HEAP_ID=10（heap）。 */
    private static final int RM_HEAP_ID = 10;

    /** rmgrlist.h 0-based 枚举序：RM_XACT_ID=1（事务，census 分桶用）。 */
    private static final int RM_XACT_ID = 1;

    /** 跨页大记录的 main data 尺寸——totLen 超页容量（8168 可用）即必跨页。 */
    private static final int BIG_MAIN_LEN = 9000;

    private final WalLayout layout = WalLayoutV18.INSTANCE;

    /** 用例 1：单页三记录顺序产出、census 记账、consumedLsn 推进到末记录 MAXALIGN 尾。 */
    @Test
    void singlePageMultipleRecordsEmitInOrderAndCursorAdvances() {
        long pageAddr = 0x10000L;
        byte[] r1 = smallRec(RM_HEAP_ID, 0x00, 11);
        byte[] r2 = smallRec(RM_XACT_ID, 0x20, 22);
        byte[] r3 = smallRec(RM_HEAP_ID, 0x00, 33);
        byte[] page = WalBytes.page(pageAddr, r1, r2, r3);

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(pageAddr + page.length, page);

        assertEquals(3, out.size());
        assertEquals(3, metrics.records.sum());
        long expectedLsn = pageAddr + 24;
        for (WalRecord rec : out) {
            assertEquals(expectedLsn, rec.lsn());
            expectedLsn += align8(rec.totLen());
        }
        // 整页已到齐：页尾零垫被消费（len-pos==pageRemain 即跳页），consumedLsn 落页尾
        assertEquals(pageAddr + page.length, walker.consumedLsn());
        assertEquals(0, metrics.resyncs.sum());
        assertEquals(0, metrics.contrecords.sum());
        assertEquals(0, metrics.orphanSkips.sum());
        // census 键形如 "rmid/info高半字节十六进制"（info&0xF0）：r1/r3 同桶计 2
        assertEquals(2L, metrics.censusSnapshot().get("10/0"));
        assertEquals(1L, metrics.censusSnapshot().get("1/20"));
    }

    /** 用例 2：记录跨页（首段占满 p1 数据区、p2 首段为续体余部）→ contrecord 缝合完整。 */
    @Test
    void recordSpanningPagesIsStitchedViaContrecord() {
        long p1 = 0x20000L;
        long p2 = p1 + 8192;
        byte[] big = bigRec();
        int firstSegLen = 8192 - 24;
        byte[] firstSeg = Arrays.copyOfRange(big, 0, firstSegLen);
        byte[] remainder = Arrays.copyOfRange(big, firstSegLen, big.length);
        byte[] trailing = smallRec(RM_HEAP_ID, 0x00, 44);
        byte[] page1 = WalBytes.page(p1, firstSeg);
        byte[] page2 = WalBytes.page(p2, WalBytes.XLP_FIRST_IS_CONTRECORD, remainder.length,
                remainder, trailing);

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(p1 + 2 * 8192, concat(page1, page2));

        assertEquals(2, out.size());
        WalRecord stitched = out.get(0);
        assertEquals(p1 + 24, stitched.lsn());
        assertEquals(big.length, stitched.totLen());
        assertArrayEquals(big, stitched.raw());
        assertEquals(1, metrics.contrecords.sum());
        assertEquals(0, metrics.resyncs.sum());
        // 续体余部之后的首个完整记录起点 = 页头 + 余部，MAXALIGN 垫齐
        assertEquals(p2 + 24 + align8(remainder.length), out.get(1).lsn());
        assertEquals(p2 + 8192, walker.consumedLsn());
    }

    /** 用例 3：同一三页流按 16 字节粒度逐段 feed，与一次 feed 全等（记录序/LSN/raw/consumedLsn）。 */
    @Test
    void arbitraryChunkSplittingYieldsIdenticalOutput() {
        long p1 = 0x28000L;
        long p2 = p1 + 8192;
        long p3 = p2 + 8192;
        byte[] r1 = smallRec(RM_HEAP_ID, 0x00, 1);
        byte[] r2 = smallRec(RM_HEAP_ID, 0x00, 2);
        byte[] big = bigRec();
        byte[] r3 = smallRec(RM_HEAP_ID, 0x00, 3);
        int firstSegLen = 8192 - 24;
        byte[] firstSeg = Arrays.copyOfRange(big, 0, firstSegLen);
        byte[] remainder = Arrays.copyOfRange(big, firstSegLen, big.length);
        byte[] stream = concat(
                WalBytes.page(p1, r1, r2),
                WalBytes.page(p2, firstSeg),
                WalBytes.page(p3, WalBytes.XLP_FIRST_IS_CONTRECORD, remainder.length, remainder, r3));

        List<WalRecord> oneShot = new ArrayList<>();
        WalStreamWalker whole = new WalStreamWalker(layout, new WalStreamMetrics(), oneShot::add);
        whole.feed(p1 + stream.length, stream);
        assertEquals(4, oneShot.size());

        List<WalRecord> sliced = new ArrayList<>();
        WalStreamWalker piecewise = new WalStreamWalker(layout, new WalStreamMetrics(), sliced::add);
        for (int off = 0; off < stream.length; off += 16) {
            int end = Math.min(off + 16, stream.length);
            piecewise.feed(p1 + end, Arrays.copyOfRange(stream, off, end));
        }

        assertEquals(oneShot.size(), sliced.size());
        for (int i = 0; i < oneShot.size(); i++) {
            assertEquals(oneShot.get(i).lsn(), sliced.get(i).lsn());
            assertEquals(oneShot.get(i).totLen(), sliced.get(i).totLen());
            assertArrayEquals(oneShot.get(i).raw(), sliced.get(i).raw());
        }
        assertEquals(whole.consumedLsn(), piecewise.consumedLsn());
    }

    /** 用例 4：流首孤立续体页（contrecord 头在窗口之前，发现 11）——余部跳过后继续产记录、单独计数。 */
    @Test
    void orphanContrecordPageIsSkippedAndCounted() {
        long p = 0x30000L;
        int orphanLen = 100;
        byte[] orphanTail = new byte[orphanLen];
        Arrays.fill(orphanTail, (byte) 0xEE);
        byte[] rec = smallRec(RM_HEAP_ID, 0x00, 55);
        byte[] page = WalBytes.page(p, WalBytes.XLP_FIRST_IS_CONTRECORD, orphanLen, orphanTail, rec);

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(p + page.length, page);

        assertEquals(1, out.size());
        assertEquals(1, metrics.orphanSkips.sum());
        // 孤儿余部跳过单列一计（orphanSkips），不与跨页缝合（contrecords）混计
        assertEquals(0, metrics.contrecords.sum());
        // 跳过终点 = MAXALIGN(页头 24 + rem_len)，首个可读记录自此起步
        assertEquals(p + align8(24 + orphanLen), out.get(0).lsn());
        assertEquals(p + 8192, walker.consumedLsn());
    }

    /**
     * 用例 5a：pageaddr 失配 32B（spike 发现 23b 的漂移形态）——受控再同步产出后续记录且 resyncs==1。
     *
     * <p>构造：页字节为真实连续流（pageaddr 全部合法页对齐），漂移注入在调用方游标——
     * 第二个 feed 的 chunkEndLsn 多报 32B（keepalive/data 交错下的裸算漂移形态）。衔接
     * 校验失配丢弃 carry 后，新 chunk 以错误锚点起步：p1 页头被误读为记录头（magic 落进
     * totLen 位），缝合至 p2 页头处无 contrecord 标志而失配 → 扫描命中 p1 页头
     * （pageaddr 与游标推算差 -8224，容差内）零丢弃重锚，p1/p2 记录全部产出且 LSN
     * 空间切回 pageaddr 权威值——chunkEndLsn 裸算依赖废除的正面验收。</p>
     */
    @Test
    void pageaddrMismatchWithinTwoPageToleranceResyncsAndContinues() {
        long a = 0x40000L;
        byte[] r0 = smallRec(RM_HEAP_ID, 0x00, 1);
        byte[] r1 = smallRec(RM_HEAP_ID, 0x00, 2);
        byte[] r2 = smallRec(RM_HEAP_ID, 0x00, 3);
        byte[] p0 = WalBytes.page(a, r0);
        byte[] p1 = WalBytes.page(a + 8192, r1);
        byte[] p2 = WalBytes.page(a + 2 * 8192, r2);

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        // feed 1 干净（游标准确），p0 正常产出
        walker.feed(a + 8192, p0);
        // 重连窗口（Task 13 契约：resetAnchor 后首块回落调用方游标锚）——feed 2 的
        // chunkEndLsn 多报 32B：chunkStart ≠ carry 尾 → 丢 carry 重启，锚点错位
        walker.resetAnchor();
        walker.feed(a + 3 * 8192 + 32, concat(p1, p2));

        assertEquals(3, out.size());
        assertEquals(1, metrics.resyncs.sum());
        assertEquals(a + 24, out.get(0).lsn());
        // 重锚后 p1/p2 首记录 LSN 以页头 pageaddr 自述为准（真实地址，无 32B 污染）
        assertEquals(a + 8192 + 24, out.get(1).lsn());
        assertEquals(a + 2 * 8192 + 24, out.get(2).lsn());
        assertEquals(a + 3 * 8192, walker.consumedLsn());
    }

    /**
     * 用例 5b（Task 13 锚定协议升级）：连接中段的调用方游标漂移（keepalive walEnd
     * 跳变形态）被自维护数据锚免疫——chunkEndLsn 多报 32B 不再触发丢 carry/假再同步，
     * 记录照常产出、地址空间零污染（resyncs==0、carryDrops==0）。
     */
    @Test
    void callerCursorDriftMidStreamIgnoredBySelfAnchor() {
        long a = 0x40000L;
        byte[] r0 = smallRec(RM_HEAP_ID, 0x00, 1);
        byte[] r1 = smallRec(RM_HEAP_ID, 0x00, 2);
        byte[] r2 = smallRec(RM_HEAP_ID, 0x00, 3);
        byte[] p0 = WalBytes.page(a, r0);
        byte[] p1 = WalBytes.page(a + 8192, r1);
        byte[] p2 = WalBytes.page(a + 2 * 8192, r2);

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(a + 8192, p0);
        // 不 resetAnchor（连接内）：chunkEndLsn 多报 32B 被自锚忽略（DEBUG 观测）
        walker.feed(a + 3 * 8192 + 32, concat(p1, p2));

        assertEquals(3, out.size(), "自锚下记录须全部产出");
        assertEquals(0, metrics.resyncs.sum(), "游标漂移不得触发再同步");
        assertEquals(0, metrics.carryDrops.sum(), "游标漂移不得丢弃 carry");
        assertEquals(a + 24, out.get(0).lsn());
        assertEquals(a + 8192 + 24, out.get(1).lsn());
        assertEquals(a + 2 * 8192 + 24, out.get(2).lsn());
        assertEquals(a + 3 * 8192, walker.consumedLsn());
    }

    /** 用例 5b：pageaddr 失配 3 页（超 2*walBlockSize 容差）——ISE fail-fast，含已产出记录的部分交付语义。 */
    @Test
    void pageaddrMismatchBeyondTwoPagesThrowsIllegalState() {
        long a = 0x50000L;
        byte[] r0 = smallRec(RM_HEAP_ID, 0x00, 1);
        byte[] r1 = smallRec(RM_HEAP_ID, 0x00, 2);
        byte[] r2 = smallRec(RM_HEAP_ID, 0x00, 3);
        byte[] p0 = WalBytes.page(a, r0);
        // p1 页头声称超前期望 3 页、p2 与 p1 自洽续排——扫描窗口内无合法页头可再同步
        byte[] p1 = WalBytes.page(a + 4 * 8192, r1);
        byte[] p2 = WalBytes.page(a + 5 * 8192, r2);
        byte[] stream = concat(p0, p1, p2);

        List<WalRecord> out = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(layout, new WalStreamMetrics(), out::add);
        assertThrows(IllegalStateException.class, () -> walker.feed(a + stream.length, stream));
        // 失配页之前的合法记录已交付（sink 先于失败被回调）
        assertEquals(1, out.size());
        assertEquals(a + 24, out.get(0).lsn());
    }

    /** 回归（审查 F1）：跨页缝合消费续体页头后必须推进期望页址——后续整页页界零再同步通过。 */
    @Test
    void recordSpanningPagesThenNextPageValidatesWithoutResync() {
        long p1 = 0x68000L;
        long p2 = p1 + 8192;
        long p3 = p2 + 8192;
        byte[] big = bigRec();
        int firstSegLen = 8192 - 24;
        byte[] firstSeg = Arrays.copyOfRange(big, 0, firstSegLen);
        byte[] remainder = Arrays.copyOfRange(big, firstSegLen, big.length);
        byte[] trailing = smallRec(RM_HEAP_ID, 0x00, 66);
        byte[] r3 = smallRec(RM_HEAP_ID, 0x00, 77);
        byte[] stream = concat(
                WalBytes.page(p1, firstSeg),
                WalBytes.page(p2, WalBytes.XLP_FIRST_IS_CONTRECORD, remainder.length, remainder, trailing),
                WalBytes.page(p3, r3));

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(p1 + 3 * 8192, stream);

        assertEquals(3, out.size());
        assertEquals(1, metrics.contrecords.sum());
        // F1 钉死：缝合路径推进 expectedPageAddr 后，P3 页界正常通过——无伪再同步
        assertEquals(0, metrics.resyncs.sum());
        assertEquals(p1 + 24, out.get(0).lsn());
        assertEquals(p2 + 24 + align8(remainder.length), out.get(1).lsn());
        assertEquals(p3 + 24, out.get(2).lsn());
        assertEquals(p3 + 8192, walker.consumedLsn());
    }

    /** 回归（审查 F4）：记录跨三页（双续体页）缝合完整，且跨出后续页界零再同步。 */
    @Test
    void recordSpanningThreePagesStitchesBothContinuations() {
        long p1 = 0x6A000L;
        long p2 = p1 + 8192;
        long p3 = p2 + 8192;
        long p4 = p3 + 8192;
        int cap = 8192 - 24;
        byte[] mainData = new byte[17000];
        Arrays.fill(mainData, (byte) 0x5C);
        byte[] big = WalBytes.record(0, 0x30, 0)
                .block(0, 1663L, 16385L, 24600L, 0)
                .image(new byte[300], 40, 0x01)
                .main(mainData)
                .build();
        byte[] s1 = Arrays.copyOfRange(big, 0, cap);
        byte[] s2 = Arrays.copyOfRange(big, cap, 2 * cap);
        byte[] s3 = Arrays.copyOfRange(big, 2 * cap, big.length);
        byte[] r4 = smallRec(RM_HEAP_ID, 0x00, 88);
        byte[] stream = concat(
                WalBytes.page(p1, s1),
                WalBytes.page(p2, WalBytes.XLP_FIRST_IS_CONTRECORD, s2.length, s2),
                WalBytes.page(p3, WalBytes.XLP_FIRST_IS_CONTRECORD, s3.length, s3, r4),
                WalBytes.page(p4, smallRec(RM_HEAP_ID, 0x00, 99)));

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(p1 + 4 * 8192, stream);

        assertEquals(3, out.size());
        assertArrayEquals(big, out.get(0).raw());   // 三页缝合逐字节完整
        assertEquals(p1 + 24, out.get(0).lsn());
        assertEquals(2, metrics.contrecords.sum());
        assertEquals(0, metrics.resyncs.sum());     // 双续体页期望逐页推进，P4 页界零再同步
        assertEquals(p3 + 24 + align8(s3.length), out.get(1).lsn());
        assertEquals(p4 + 24, out.get(2).lsn());
        assertEquals(p4 + 8192, walker.consumedLsn());
    }

    /** 回归（审查 F2）：非零丢弃再同步后缓冲有效长度收缩——同 feed 无幻影记录、consumedLsn 无虚进。 */
    @Test
    void nonZeroDropResyncShrinksBufferAndContinuesWithoutPhantoms() {
        long a = 0x70000L;
        byte[] r0 = smallRec(RM_HEAP_ID, 0x00, 1);
        byte[] r2 = smallRec(RM_HEAP_ID, 0x00, 2);
        byte[] p1bad = WalBytes.page(a + 8192, smallRec(RM_HEAP_ID, 0x00, 3));
        p1bad[0] = 0x00;   // 页头 magic 打坏：页头校验失败 → 整页 8192B 作错位字节丢弃
        p1bad[1] = 0x00;
        byte[] stream = concat(WalBytes.page(a, r0), p1bad, WalBytes.page(a + 2 * 8192, r2));

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        walker.feed(a + 3 * 8192, stream);

        assertEquals(1, metrics.resyncs.sum());     // 单次 8192B 丢弃再同步
        // 无幻影：丢弃左移后陈旧尾不重复产出 r2（修复前缓冲尾驻留 p2 副本被二次解析）
        assertEquals(2, out.size());
        assertEquals(a + 24, out.get(0).lsn());
        assertEquals(a + 2 * 8192 + 24, out.get(1).lsn());
        // 无虚进：停点=真实流末（修复前 len 未收缩致游标越过有效尾）
        assertEquals(a + 3 * 8192, walker.consumedLsn());
    }

    /** 回归（审查 F3）：carry 衔接失配丢弃 pending carry 计数 carryDrops，丢后页头锚定恢复产出。 */
    @Test
    void carryAnchorDriftDropsPendingCarryAndCounts() {
        long a = 0x78000L;
        byte[] p0 = WalBytes.page(a, smallRec(RM_HEAP_ID, 0x00, 1));
        byte[] p1 = WalBytes.page(a + 8192, smallRec(RM_HEAP_ID, 0x00, 2));

        List<WalRecord> out = new ArrayList<>();
        WalStreamMetrics metrics = new WalStreamMetrics();
        WalStreamWalker walker = new WalStreamWalker(layout, metrics, out::add);
        // feed 1 半页：r0 产出，页尾零垫未到齐 → 非空 carry
        walker.feed(a + 4096, Arrays.copyOfRange(p0, 0, 4096));
        assertEquals(1, out.size());
        // 重连窗口（resetAnchor 后首块回落调用方游标锚）——feed 2 的 chunkEndLsn 多报
        // 32B：chunkStart 与 carry 尾不连续 → 丢 carry 计数
        walker.resetAnchor();
        walker.feed(a + 4096 + 8192 + 32, p1);

        assertEquals(1, metrics.carryDrops.sum());
        assertEquals(2, out.size());
        // 丢弃后以错锚点重启 → 页头锚定重锚（resyncs 1 次）→ p1 记录正常产出
        assertEquals(1, metrics.resyncs.sum());
        assertEquals(a + 8192 + 24, out.get(1).lsn());
    }

    /**
     * 造一条小体积完整记录（单 block data + 8B main），尺寸可控便于页内排布推算。
     *
     * @param rmid 资源管理器 id
     * @param info 记录 info 字节
     * @param xid  记录头 xid
     * @return 记录字节数组（长度即 totLen）
     */
    private static byte[] smallRec(int rmid, int info, int xid) {
        return WalBytes.record(rmid, info, xid)
                .block(0, 1663L, 16385L, 24600L, 1)
                .data(new byte[] {1, 2, 3, 4})
                .main(new byte[8])
                .build();
    }

    /**
     * 造一条必跨页的大记录（FPI 形态：300B 镜像 + 9000B main），totLen ≈ 9.3KB。
     *
     * @return 记录字节数组（长度即 totLen，超出单页 8168 可用容量）
     */
    private static byte[] bigRec() {
        byte[] mainData = new byte[BIG_MAIN_LEN];
        Arrays.fill(mainData, (byte) 0x7A);
        return WalBytes.record(0, 0x30, 0)
                .block(0, 1663L, 16385L, 24600L, 0)
                .image(new byte[300], 40, 0x01)
                .main(mainData)
                .build();
    }

    /**
     * MAXALIGN 到 8——记录起点/终点对齐推算（与 walker 的页内对齐规则同式）。
     *
     * @param v 字节数
     * @return 向上对齐到 8 的值
     */
    private static int align8(int v) {
        return (v + 7) & ~7;
    }

    /**
     * 顺序拼接若干字节段（页拼流用）。
     *
     * @param chunks 待拼字节段
     * @return 拼接结果
     */
    private static byte[] concat(byte[]... chunks) {
        int total = 0;
        for (byte[] c : chunks) {
            total += c.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] c : chunks) {
            System.arraycopy(c, 0, out, pos, c.length);
            pos += c.length;
        }
        return out;
    }
}
