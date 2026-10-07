package org.vastdata.vbstream.walsource.changes;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import net.jpountz.lz4.LZ4Factory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TOAST 重组（{@link ToastAssembler}）的失败先行测试——手造 18B external 指针与
 * chunk 行字节，断言拼装/解压/回查/降级四面前置 + pglz/lz4 双方法解压面。
 *
 * <p><b>布局实测锚</b>（2026-10-07 docker PG 18.6 容器 src/docker，全文推导记录在
 * task-3-report.md）：</p>
 * <ul>
 *   <li>external 指针（18B 短 varlena）：{@code 01 12 | rawsize u32 LE | extinfo u32 LE
 *       | valueid u32 LE | toastrelid u32 LE}——压缩样本（raw 载荷 38400 字节）实测
 *       {@code 011204960000 9c540000 7b830000 79830000}：rawsize=38404（载荷+4 头）、
 *       extinfo=0x549C=21660（extsize 低 30 位、方法位高 2 位=00 pglz）、valueid=33659、
 *       toastrelid=33657；未压缩样本（19200 载荷）{@code 0112044b0000 004b0000 ...}：
 *       extsize==rawsize-4；lz4 样本 extinfo=0x40004E8A——方法位（extinfo&gt;&gt;30）=01；</li>
 *   <li>TOAST chunk_data（压缩值）：**拼接整体** = [u32 LE tcinfo = (rawsize-4) 低 30 位
 *       | 方法&lt;&lt;30][pglz 流]——pglz 样本 chunk0 首 4B 实测 {@code 00960000}（LE 38400）、
 *       lz4 样本 chunk0 首 4B 实测 {@code 00960040}（0x40009600）；未压缩值无前缀、
 *       chunk 拼接即原文载荷；两形态 chunk 拼接总字节数均 == extsize（1996/块上限）。</li>
 * </ul>
 *
 * <p>任务书 Step 1 五用例（未压缩 3 chunk 拼装+extsize 校验 / pglz 手造压缩块解压往返 /
 * chunk 缺失 stub probe 回查命中 / 回查也缺失 toast-unavailable / lz4 解压往返[原 guard
 * 用例随 lz4 支持落地而废]）+ 实测布局锚定的补充用例（多 chunk 压缩分界、指针 tag 拒绝、
 * tcinfo 校验、relfilenode 分叉容错、probe 抛异常降级、lz4 行内压缩往返、未知方法码 ISE）。</p>
 */
class ToastAssemblerTest {

    /** 实测锚的 toast 关系 oid（任意测试值，与 chunk 归集键一致即可）。 */
    private static final long TOAST_REL = 33657L;

    /** 实测锚的 valueid（任意测试值）。 */
    private static final long VALUE_ID = 33659L;

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    /**
     * 挂 ListAppender 捕获被测类 WARN 行——降级/guard 的"一次且带上下文"断言面。
     */
    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(ToastAssembler.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
    }

    /**
     * 摘除 appender，防泄漏到后续测试类。
     */
    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
    }

    /**
     * 任务书 ①：未压缩 external——28 字节载荷分 3 chunk（10/10/8），乱序喂入
     * （seq 2,0,1）钉 TreeMap 按 seq 归集；extsize(28)==rawsize(32)-4 走原文拼接
     * 路径，UTF-8 解回原文。
     */
    @Test
    void uncompressedExternalAssemblesThreeChunksInSeqOrder() {
        ToastAssembler assembler = new ToastAssembler(null);
        String text = "hello toast assembly world!!";      // 28 字节
        byte[] payload = text.getBytes(StandardCharsets.UTF_8);
        feedChunks(assembler, TOAST_REL, VALUE_ID, payload, 10);

        byte[] resolved = assembler.resolveExternal(pointer(payload.length + 4, payload.length, 0, VALUE_ID, TOAST_REL), 0);

        assertEquals(text, new String(resolved, StandardCharsets.UTF_8));
    }

    /**
     * 实测布局钉（压缩 chunk 布局）：压缩值拼接 = [tcinfo 4B][pglz 流]，且 4B 前缀可
     * 跨 chunk 分界——手造 pglz（PglzTest 用例 2 同源：{@code [0x08,'a','b','c',0x05,
     * 0x03]} 解压 11 字节 "abcabcabcab"）拆两个 chunk：chunk0 = tcinfo(11 LE) + 前 3
     * 字节 pglz，chunk1 = 余 3 字节。extsize=10 &lt; rawsize-4=11 → pglz 路径。
     */
    @Test
    void pglzExternalSplitsHeaderAndStreamAcrossChunks() {
        ToastAssembler assembler = new ToastAssembler(null);
        byte[] pglz = {0x08, 'a', 'b', 'c', 0x05, 0x03};
        byte[] chunk0 = concat(u32le(11), copyOfRange(pglz, 0, 3));
        byte[] chunk1 = copyOfRange(pglz, 3, 6);
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 0, chunk0));
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 1, chunk1));

        byte[] resolved = assembler.resolveExternal(pointer(15, 10, 0, VALUE_ID, TOAST_REL), 0);

        assertEquals("abcabcabcab", new String(resolved, StandardCharsets.UTF_8));
    }

    /**
     * 任务书 ③：chunk 缺失 → stub probe 回查命中——只喂 seq 0，probe 返回 seq 1/2；
     * 断言 probe 收到的 toastOid/valueid 与指针一致（回查键语义），拼装完整。
     */
    @Test
    void missingChunkFallsBackToProbeFetch() {
        StubProbe probe = new StubProbe();
        String text = "probe-fallback-payload-0123456789";   // 33 字节
        byte[] payload = text.getBytes(StandardCharsets.UTF_8);
        TreeMap<Long, byte[]> fetched = new TreeMap<>();
        fetched.put(1L, copyOfRange(payload, 10, 20));
        fetched.put(2L, copyOfRange(payload, 20, payload.length));
        probe.serve(TOAST_REL, VALUE_ID, fetched);
        ToastAssembler assembler = new ToastAssembler(probe);
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 0, copyOfRange(payload, 0, 10)));

        byte[] resolved = assembler.resolveExternal(pointer(payload.length + 4, payload.length, 0, VALUE_ID, TOAST_REL), 0);

        assertEquals(text, new String(resolved, StandardCharsets.UTF_8));
        assertEquals(TOAST_REL, probe.lastToastOid);
        assertEquals(VALUE_ID, probe.lastValueid);
    }

    /**
     * Task 10 值面裁定：归集面<b>全空</b>（未变列指针/跨事务/重启窗口前）→ 返回
     * unchanged-TOAST 哨兵（{@code ==} 身份识别）且<b>不回查</b>——engine 的 pgoutput
     * 同形发 'u'（proto.c LOGICALREP_COLUMN_UNCHANGED），回查反而产出 engine 没有的值
     * 破坏双路对拍。
     */
    @Test
    void emptyCollectionReturnsUnchangedMarkerWithoutProbe() {
        StubProbe probe = new StubProbe();
        probe.serve(TOAST_REL, VALUE_ID, Map.of());   // 即便可回查也不应触达
        ToastAssembler assembler = new ToastAssembler(probe);

        byte[] resolved = assembler.resolveExternal(pointer(35, 31, 0, VALUE_ID, TOAST_REL), 0);

        assertTrue(resolved == ToastAssembler.UNCHANGED_TOAST_MARKER,
                "归集面空应返回 unchanged 哨兵（身份比较），实得 " + Arrays.toString(resolved));
        assertEquals(-1L, probe.lastToastOid, "归集面空不触发回查");
        assertTrue(appender.list.stream().noneMatch(e -> e.getLevel() == Level.WARN), "无 WARN（非降级路径）");
    }

    /**
     * 任务书 ④（Task 10 收窄后）：归集面<b>非空但不完整</b>、回查也缺 →
     * {@code toast-unavailable} + WARN 一次（含 valueid/toastrelid 上下文）——先喂
     * seq 0 使归集面非空（空面走 unchanged 哨兵分支不回查），probe 返回的 chunk 仍
     * 不齐（只有 seq 1），不 fail 整条流。
     */
    @Test
    void probeMissDegradesToUnavailableAndWarnsOnce() {
        StubProbe probe = new StubProbe();
        TreeMap<Long, byte[]> partial = new TreeMap<>();
        partial.put(1L, new byte[10]);
        probe.serve(TOAST_REL, VALUE_ID, partial);
        ToastAssembler assembler = new ToastAssembler(probe);
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 0, new byte[10]));

        byte[] resolved = assembler.resolveExternal(pointer(35, 31, 0, VALUE_ID, TOAST_REL), 0);

        assertEquals("toast-unavailable", new String(resolved, StandardCharsets.UTF_8));
        List<ILoggingEvent> warns = appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
        assertEquals(1, warns.size());
        assertTrue(warns.get(0).getFormattedMessage().contains(String.valueOf(VALUE_ID)), "WARN 行含 valueid 上下文");
        assertTrue(warns.get(0).getFormattedMessage().contains(String.valueOf(TOAST_REL)), "WARN 行含 toastrelid 上下文");
    }

    /**
     * probe 抛异常（基础设施故障经 ISE 包装）同样降级不 fail——WARN 行携带异常，
     * 值面 {@code toast-unavailable}（v2 设计：回查失败不 fail 整条流）。归集面先喂
     * seq 0 使非空（Task 10 契约：空面不回查走 unchanged 哨兵）。
     */
    @Test
    void probeFailureDegradesToUnavailable() {
        StubProbe probe = new StubProbe();
        probe.failWith(new IllegalStateException("toast probe connection down"));
        ToastAssembler assembler = new ToastAssembler(probe);
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 0, new byte[10]));

        byte[] resolved = assembler.resolveExternal(pointer(35, 31, 0, VALUE_ID, TOAST_REL), 0);

        assertEquals("toast-unavailable", new String(resolved, StandardCharsets.UTF_8));
        assertEquals(1, appender.list.stream().filter(e -> e.getLevel() == Level.WARN).count());
    }

    /**
     * lz4 压缩 external 解压往返（原 ISE 拒绝用例随 lz4 支持落地改为解压断言）——
     * 40 字节周期 3 载荷经 lz4-java 压缩器压成裸 block，chunk 拼接 =
     * {@code [tcinfo = (rawsize-4) | 1<<30][lz4 流]}（实测锚：lz4 样本 chunk0 首 4B
     * {@code 00960040}），指针方法位（extinfo&gt;&gt;30）=1；extsize=tcinfo+lz4 流长
     * &lt; rawsize-4 → 压缩路径分派到 {@link Lz4}，解回原文。
     */
    @Test
    void lz4CompressedExternalRoundTrips() {
        ToastAssembler assembler = new ToastAssembler(null);
        String text = "abc".repeat(13) + "a";               // 40 字节周期 3 可压缩载荷
        byte[] payload = text.getBytes(StandardCharsets.US_ASCII);
        byte[] block = LZ4Factory.fastestInstance().fastCompressor()
                .compress(payload, 0, payload.length);
        byte[] stream = concat(u32le(payload.length | (1 << 30)), block);
        feedChunks(assembler, TOAST_REL, VALUE_ID, stream, 6);   // 跨多 chunk 分界

        byte[] resolved = assembler.resolveExternal(
                pointer(payload.length + 4, stream.length, 1, VALUE_ID, TOAST_REL), 0);

        assertEquals(text, new String(resolved, StandardCharsets.US_ASCII));
    }

    /**
     * 未知方法位 ISE——方法码 2（既非 pglz=0 也非 lz4=1，varatt.h 未定义的保留值）
     * 是走读错位/格式演进信号，仍 ISE fail-fast（lz4 落地后唯一的方法面拒绝分支）。
     */
    @Test
    void unknownCompressionMethodStillThrowsIllegalState() {
        ToastAssembler assembler = new ToastAssembler(null);
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 0, new byte[10]));

        assertThrows(IllegalStateException.class,
                () -> assembler.resolveExternal(pointer(15, 10, 2, VALUE_ID, TOAST_REL), 0));
    }

    /**
     * lz4 行内压缩 varlena 解压往返——载荷 = {@code [tcinfo = 原长 | 1<<30][lz4 流]}
     * （与 external 压缩值同构，方法位在 tcinfo 高 2 位），经
     * {@code decompressInlineCompressed} 分派到 {@link Lz4} 解回原文；无降级形态
     * （行内数据自包含）。
     */
    @Test
    void lz4InlineCompressedRoundTrips() {
        ToastAssembler assembler = new ToastAssembler(null);
        String text = "0123456789abcdef".repeat(64);        // 1024 字节周期 16 载荷
        byte[] payload = text.getBytes(StandardCharsets.US_ASCII);
        byte[] block = LZ4Factory.fastestInstance().fastCompressor()
                .compress(payload, 0, payload.length);
        byte[] varlenaPayload = concat(u32le(payload.length | (1 << 30)), block);

        byte[] resolved = assembler.decompressInlineCompressed(varlenaPayload);

        assertEquals(text, new String(resolved, StandardCharsets.US_ASCII));
    }

    /**
     * lz4 列的<b>未变列指针</b>（归集面空——值不在当前窗口）返回 unchanged-TOAST
     * 哨兵而非 ISE（Task 13 顺手修：空面判定先于方法位检查）——engine 的 pgoutput
     * 对未变 TOAST 列恒发 'u' 与压缩方法无关，先查方法位会把该形态错杀成 ISE 杀流。
     */
    @Test
    void lz4UnchangedPointerWithEmptyWindowReturnsMarkerNotIllegalState() {
        ToastAssembler assembler = new ToastAssembler(null);

        byte[] resolved = assembler.resolveExternal(pointer(35, 31, 1, VALUE_ID, TOAST_REL), 0);

        assertTrue(resolved == ToastAssembler.UNCHANGED_TOAST_MARKER,
                "lz4 未变列指针的空归集面应走哨兵（== 身份），实得 "
                        + Arrays.toString(resolved));
    }

    /**
     * 指针 tag 拒绝——首字节非 0x01（外部短 varlena）或次字节非 0x12
     * （VARTAG_ONDISK=18）是走读错位信号，ISE fail-fast 不进入拼装。
     */
    @Test
    void nonOnDiskPointerRejected() {
        ToastAssembler assembler = new ToastAssembler(null);
        byte[] badTag = pointer(15, 10, 0, VALUE_ID, TOAST_REL);
        badTag[1] = 0x13;                                   // 非 ONDISK tag

        byte[] badHeader = pointer(15, 10, 0, VALUE_ID, TOAST_REL);
        badHeader[0] = 0x02;                                // 非 1B 外部头

        assertThrows(IllegalStateException.class, () -> assembler.resolveExternal(badTag, 0));
        assertThrows(IllegalStateException.class, () -> assembler.resolveExternal(badHeader, 0));
    }

    /**
     * 压缩头与指针一致性校验——chunk 拼接的 tcinfo（首 4B）与指针的
     * rawsize-4/方法位不符（此处 tcinfo=99 而指针 rawsize-4=11）抛 ISE
     * （归集面错位/混入异值 chunk 的信号），不产出错值。
     */
    @Test
    void compressedHeaderMismatchWithPointerThrows() {
        ToastAssembler assembler = new ToastAssembler(null);
        byte[] chunk0 = concat(u32le(99), new byte[] {0x08, 'a', 'b'});
        byte[] chunk1 = {'c', 0x05, 0x03};
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 0, chunk0));
        assembler.onChunkRow(TOAST_REL, chunkRow(VALUE_ID, 1, chunk1));

        assertThrows(IllegalStateException.class,
                () -> assembler.resolveExternal(pointer(15, 10, 0, VALUE_ID, TOAST_REL), 0));
    }

    /**
     * chunk 归集键与指针 toastrelid 分叉时的 valueid 容错——主表重写
     * （VACUUM FULL/CLUSTER）后 toast relfilenode != 关系 oid，chunk 以 relfilenode
     * 键归集而指针携带 oid：按 valueid 全域兜底扫描仍命中（窗口内 valueid 全局唯一）。
     */
    @Test
    void chunkKeyedUnderDivergentRelfilenodeStillResolvesByValueid() {
        ToastAssembler assembler = new ToastAssembler(null);
        byte[] payload = "divergent-key tolerance".getBytes(StandardCharsets.UTF_8);
        feedChunks(assembler, TOAST_REL + 7, VALUE_ID, payload, 10);   // 归集键 != 指针 toastrelid

        byte[] resolved = assembler.resolveExternal(pointer(payload.length + 4, payload.length, 0, VALUE_ID, TOAST_REL), 0);

        assertEquals("divergent-key tolerance", new String(resolved, StandardCharsets.UTF_8));
    }

    // ---- 手造字节辅助（布局锚见类 javadoc） ----

    /**
     * 构造 18B external 指针：{@code [0x01][0x12][rawsize u32 LE]
     * [extsize|方法<<30 u32 LE][valueid u32 LE][toastrelid u32 LE]}。
     *
     * @param rawsize 原始 varlena 总长（载荷+4 头）
     * @param extsize 外部存储载荷长（== chunk 拼接总长）
     * @param method  压缩方法码（0=pglz、1=lz4，占 extinfo 高 2 位）
     * @param valueid TOAST 值 id
     * @param toastrelid toast 关系 oid
     * @return 18 字节指针
     */
    private static byte[] pointer(int rawsize, int extsize, int method, long valueid, long toastrelid) {
        byte[] p = new byte[18];
        p[0] = 0x01;
        p[1] = 0x12;
        putU32le(p, 2, rawsize);
        putU32le(p, 6, extsize | (method << 30));
        putU32le(p, 10, (int) valueid);
        putU32le(p, 14, (int) toastrelid);
        return p;
    }

    /**
     * 构造 toast chunk 三列值行（{@code TupleDecoder} 词典 oid/int4/bytea 的
     * Java 映射：Long/Integer/byte[]）。
     */
    private static Object[] chunkRow(long valueid, int seq, byte[] data) {
        return new Object[] {valueid, seq, data};
    }

    /**
     * 把 payload 按给定块大切分并按乱序喂入（seq 从尾块起倒序——钉 TreeMap 归集
     * 不依赖喂入次序）。
     */
    private static void feedChunks(ToastAssembler assembler, long toastKey, long valueid, byte[] payload, int chunkSize) {
        int seq = 0;
        Map<Integer, byte[]> parts = new HashMap<>();
        for (int i = 0; i < payload.length; i += chunkSize) {
            parts.put(seq++, copyOfRange(payload, i, Math.min(i + chunkSize, payload.length)));
        }
        for (int s = parts.size() - 1; s >= 0; s--) {
            assembler.onChunkRow(toastKey, chunkRow(valueid, s, parts.get(s)));
        }
    }

    /**
     * 拼接两个字节数组（手造 chunk 的 tcinfo 前缀 + pglz 分段）。
     */
    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    /**
     * 4 字节小端 u32 编码（tcinfo/rawsize 等 LE 锚）。
     */
    private static byte[] u32le(int v) {
        return new byte[] {(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }

    /**
     * 就地写 4 字节小端 u32（指针构造）。
     */
    private static void putU32le(byte[] b, int o, int v) {
        b[o] = (byte) v;
        b[o + 1] = (byte) (v >>> 8);
        b[o + 2] = (byte) (v >>> 16);
        b[o + 3] = (byte) (v >>> 24);
    }

    /**
     * {@code Arrays.copyOfRange} 的直写替身（避免与被测代码同名工具混淆）。
     */
    private static byte[] copyOfRange(byte[] src, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(src, from, out, 0, to - from);
        return out;
    }

    /**
     * 可编程 stub probe——按 (toastOid, valueid) 供 chunk 表或直接抛异常，
     * 记录最后一次回查键供断言。
     */
    private static final class StubProbe implements ToastProbe {

        private final Map<Long, Map<Long, Map<Long, byte[]>>> served = new HashMap<>();
        private RuntimeException failure;
        private long lastToastOid = -1;
        private long lastValueid = -1;

        /**
         * 登记一组供回查命中的 chunk（toastOid+valueid 键 → seq → 字节）。
         */
        void serve(long toastOid, long valueid, Map<Long, byte[]> chunks) {
            served.computeIfAbsent(toastOid, k -> new HashMap<>()).put(valueid, chunks);
        }

        /**
         * 设定回查即抛的异常（基础设施故障路径）。
         */
        void failWith(RuntimeException e) {
            this.failure = e;
        }

        /**
         * 返回登记的 chunk 表（未登记为空表）；记录回查键；有 failure 时抛出。
         */
        @Override
        public Map<Long, byte[]> fetchChunks(long toastOid, long valueid) {
            lastToastOid = toastOid;
            lastValueid = valueid;
            if (failure != null) {
                throw failure;
            }
            Map<Long, Map<Long, byte[]>> byValue = served.get(toastOid);
            return byValue == null ? Map.of() : byValue.getOrDefault(valueid, Map.of());
        }
    }
}
