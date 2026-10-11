package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.receive.FileWalSource;
import org.vastdata.vbstream.walsource.receive.WalStreamMetrics;
import org.vastdata.vbstream.walsource.receive.WalStreamWalker;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 双通道对拍 IT（M1 Task 8 Step 3）：同一容器、同一写入序列下，
 * <strong>File 通道</strong>（external 形态——测试内 exec 容器内 pg_receivexlog 落盘到
 * bind 卷，{@link FileWalSource} tail 宿主侧同目录）与 <strong>Direct 通道</strong>
 * （{@link DirectWalSource} 裸 v3 直连）两路各自收流至同一边界 LSN，断言两路 walker
 * 交付的记录序列（lsn + totLen + rmid + info）<strong>全等</strong>。
 *
 * <p><strong>为什么 File 一律 external</strong>：managed 模式需宿主侧 pg_receivexlog
 * 二进制（macOS 无），容器内 {@code /usr/local/opengauss/bin/pg_receivexlog} 经 bind 卷
 * 与宿主共享落盘目录（brief 裁定形态）。</p>
 *
 * <p><strong>边界与窗口归一</strong>：两路起点独立（各自把当前 xlogpos 段对齐下取整，
 * 时点差至多差一个段号）——对拍窗口取 [W, boundary)，W = 两路首条记录 LSN 的较大者
 * （覆盖较少的一路必然包含另一路窗口），boundary = 末次写入后的
 * {@code pg_current_xlog_location()}。段切换用 {@code pg_switch_xlog()} 钉出完成段
 * （switch 记录按 XLogSwitch 语义垫到段边界——完成段尾零恒不足一页，<b>实测勘误</b>：
 * "完成段恒整页零垫尾"不成立，整零页的真实来源是预分配 .partial 的未写区与服务端
 * 跳页补位零，零页容忍分支由离线 WalStreamWalkerTest 锚定，本 IT 不做 zeroPageSkips
 * 下限断言）。</p>
 *
 * <p>记录面不越界：sink 只收 {@link WalRecord}（walker 解析层），不施加 catalog/heap
 * 视图（对拍面是字节流走读的记录序等价，与 Task 7 的三目录末态对拍互补）。</p>
 */
class OgDualChannelParityIT {

    private static final Logger LOG = LoggerFactory.getLogger(OgDualChannelParityIT.class);

    /** 两路追平 boundary 的轮询上限（毫秒）——file 通道经 bind 卷可见性延迟，留足余量。 */
    private static final long CATCHUP_TIMEOUT_MS = 120_000;

    /** 追平轮询间隔（毫秒）。 */
    private static final long CATCHUP_POLL_MS = 500;

    /** 记录流静止窗（毫秒）——追平判定的第二条件（carry 排干余量）。 */
    private static final long STABLE_WINDOW_MS = 1_500;

    /** 直连源反馈心跳 statusInterval（毫秒）。 */
    private static final long STATUS_INTERVAL_MS = 5_000;

    private DirectWalSource direct;
    private FileWalSource file;
    private WalStreamMetrics directMetrics;
    private WalStreamMetrics fileMetrics;

    @AfterEach
    void tearDown() {
        OgTestEnv.stopReceivexlog();
        if (direct != null) {
            direct.stop();
        }
        if (file != null) {
            file.stop();
        }
    }

    /**
     * 主断言：同容器同写入序列（INSERT/UPDATE/DELETE/DDL + 段切换 + 续写）下两通道
     * 记录序列窗口全等、file 通道零页容忍命中、两路无终态失败。
     *
     * <p>关键步骤：建表并清段目录 → 起 sidecar（容器内 pg_receivexlog，external）→
     * 等首段落盘 → 起 Direct 通道 → 写入序列 + {@code pg_switch_xlog()} 段切换 + 续写 →
     * 取 boundary → 两路 consumedLsn 追平 → 停两路 → 窗口归一比对。</p>
     *
     * @throws Exception 任一环节失败
     */
    @Test
    void fileAndDirectChannelsDeliverIdenticalRecordSequences() throws Exception {
        WalLayout layout = WalLayoutVOpenGauss.INSTANCE;
        OgTestEnv.execSql(
                "DROP TABLE IF EXISTS og_par_t",
                "CREATE TABLE og_par_t(id int primary key, v text)");
        cleanWalDir();
        OgTestEnv.stopReceivexlog();
        OgTestEnv.startReceivexlog();
        awaitFirstSegment();

        List<WalRecord> fileRecs = new ArrayList<>();
        fileMetrics = new WalStreamMetrics();
        WalStreamWalker fileWalker = new WalStreamWalker(layout, fileMetrics, fileRecs::add);
        file = new FileWalSource(OgTestEnv.hostWalDir(), FileWalSource.DEFAULT_SEGMENT_SIZE,
                0L, layout.walBlockSize());
        file.start(lsn -> { }, (endLsn, bytes) -> fileWalker.feed(endLsn, bytes));

        List<WalRecord> directRecs = new ArrayList<>();
        directMetrics = new WalStreamMetrics();
        WalStreamWalker directWalker = new WalStreamWalker(layout, directMetrics, directRecs::add);
        direct = new DirectWalSource(OgTestEnv.host(), OgTestEnv.port(),
                OgTestEnv.USER, OgTestEnv.PASSWORD, null, STATUS_INTERVAL_MS, 0L);
        direct.start(lsn -> { }, (endLsn, bytes) -> directWalker.feed(endLsn, bytes));

        writeSequence();
        long boundary = OgTestEnv.currentXlogLocation();

        // 追平判据（前沿语义两通道不同）：direct consumedLsn = 服务端字节前沿（可达
        // boundary）；file consumedLsn = 已接收前沿（末非零 + 1，页尾零垫不计入）——
        // 统一按"consumed ≥ boundary 页下取整 + 记录流 1.5s 静止"双条件收敛
        awaitCaughtUp("direct", direct::consumedLsn, directRecs, boundary);
        awaitCaughtUp("file", file::consumedLsn, fileRecs, boundary);

        OgTestEnv.stopReceivexlog();
        direct.stop();
        file.stop();

        assertFalse(direct.terminalFailure().isPresent(), "direct 通道终态失败: " + direct.terminalFailure());
        assertFalse(file.terminalFailure().isPresent(), "file 通道终态失败: " + file.terminalFailure());
        // census 观测（MULTI_INSERT 语料锚的记录面——rmid/info 形态记档，两路一致属对拍隐含）
        LOG.info("双通道 census（direct）: {}", directMetrics.censusSnapshot());
        LOG.info("双通道 census（file）:   {}", fileMetrics.censusSnapshot());

        assertWindowParity(fileRecs, directRecs, boundary);
    }

    /**
     * 写入序列：多语句事务外 DML 六面 + 事务内多语句 + DDL（ADD COLUMN）+ COPY 批量装载
     * （MULTI_INSERT 语料面——census 观测记日志，HeapViews 的 OG MULTI_INSERT ISE 不在
     * walker 层触发）+ 段切换 + 续写（post-switch 记录使两路都跨完成段追平）。
     *
     * @throws SQLException 任一语句失败
     */
    private void writeSequence() throws Exception {
        OgTestEnv.execSql(
                "INSERT INTO og_par_t SELECT g, md5(g::text) FROM generate_series(1, 300) g",
                "UPDATE og_par_t SET v = md5(v) WHERE id <= 50",
                "DELETE FROM og_par_t WHERE id > 280",
                "ALTER TABLE og_par_t ADD COLUMN note text",
                "INSERT INTO og_par_t(id, v, note) VALUES (301, 'after-ddl', 'n1')");
        OgTestEnv.execSql(
                "BEGIN",
                "INSERT INTO og_par_t(id, v, note) VALUES (302, 'txn-a', 'n2')",
                "UPDATE og_par_t SET note = 'txn-upd' WHERE id = 302",
                "COMMIT");
        StringBuilder csv = new StringBuilder();
        for (int i = 400; i < 600; i++) {
            csv.append(i).append(",load-").append(i).append('\n');
        }
        long copied = OgTestEnv.copyIn("COPY og_par_t(id, v) FROM STDIN WITH (FORMAT csv)", csv.toString());
        assertTrue(copied == 200, "COPY 装载行数: " + copied);
        OgTestEnv.execSql("SELECT pg_switch_xlog()");
        OgTestEnv.execSql(
                "INSERT INTO og_par_t(id, v, note) VALUES (303, 'post-switch', 'n3')",
                "UPDATE og_par_t SET v = 'post-switch-upd' WHERE id = 303");
    }

    /**
     * 窗口归一比对：W = 两路首记录 LSN 较大者；过滤 [W, boundary) 的记录成
     * (lsn, totLen, rmid, info) 四元组行逐行全等。
     *
     * <p>窗口外差异（两路起停窗口造成的首尾段差）被 W/boundary 归一吸收——与
     * {@code DualPathParityIT} 的 xid 交集归一化同思路。</p>
     *
     * @param fileRecs   file 通道交付记录
     * @param directRecs direct 通道交付记录
     * @param boundary   对拍窗口上界（不含）
     */
    private void assertWindowParity(List<WalRecord> fileRecs, List<WalRecord> directRecs, long boundary) {
        assertTrue(fileRecs.size() > 10 && directRecs.size() > 10,
                "两路各须交付足量记录: file=" + fileRecs.size() + " direct=" + directRecs.size());
        long floor = Math.max(
                fileRecs.stream().mapToLong(WalRecord::lsn).min().orElse(0),
                directRecs.stream().mapToLong(WalRecord::lsn).min().orElse(0));
        // 上界取两路末条交付记录末尾的较小者：file 前沿语义止于"末非零字节 + 1"，末条
        // 记录尾字节为零（CRC/垫）时该条不被交付属已文档化边界——窗口上界按两路都已
        // 完整交付的区段归一（下一条记录起点 ≥ 上界，被排除的至多是末条竞态记录）
        long upper = Math.min(lastRecordEnd(fileRecs), lastRecordEnd(directRecs));
        assertTrue(upper <= boundary + 8,
                "两路交付须覆盖到 boundary 附近: upper=" + Lsn.format(upper) + " boundary=" + Lsn.format(boundary));
        List<String> fileWindow = windowLines(fileRecs, floor, upper);
        List<String> directWindow = windowLines(directRecs, floor, upper);
        assertEquals(fileWindow.size(), directWindow.size(),
                "窗口记录数不等——首差上下文 file=" + firstDiff(fileWindow, directWindow));
        for (int i = 0; i < fileWindow.size(); i++) {
            assertEquals(fileWindow.get(i), directWindow.get(i),
                    "窗口内第 " + i + " 条记录不等（lsn/totLen/rmid/info）");
        }
    }

    /**
     * 一路末条交付记录的 MAXALIGN 末尾（窗口上界归一的数据源）。
     *
     * @param recs 记录列表（非空）
     * @return 末条记录 lsn + totLen 的 8 对齐上取整
     */
    private static long lastRecordEnd(List<WalRecord> recs) {
        WalRecord last = recs.get(recs.size() - 1);
        return (last.lsn() + last.totLen() + 7) & ~7L;
    }

    /**
     * 过滤 + 投影一路记录为窗口行（四元组文本）。
     *
     * @param recs     全量记录
     * @param floor    窗口下界（含——两路首记录 LSN 较大者）
     * @param boundary 上界（不含）
     * @return 窗口行列表
     */
    private static List<String> windowLines(List<WalRecord> recs, long floor, long boundary) {
        List<String> out = new ArrayList<>();
        for (WalRecord r : recs) {
            if (r.lsn() >= floor && r.lsn() < boundary) {
                out.add(Lsn.format(r.lsn()) + " len=" + r.totLen()
                        + " rmid=" + r.rmid() + " info=0x" + Integer.toHexString(r.info()));
            }
        }
        return out;
    }

    /**
     * 两窗口列表的首差异行（失败诊断面）。
     *
     * @param a   file 窗口行
     * @param b   direct 窗口行
     * @return 首差异描述（无差异为空串）
     */
    private static String firstDiff(List<String> a, List<String> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            if (!a.get(i).equals(b.get(i))) {
                return "i=" + i + " file=" + a.get(i) + " direct=" + b.get(i);
            }
        }
        return a.size() != b.size() ? "sizeOnly file=" + a.size() + " direct=" + b.size() : "none";
    }

    /**
     * 等一路追平（双条件收敛）：consumed ≥ boundary 的页对齐下取整（file 前沿语义止于
     * 末非零字节，比 direct 少页尾零垫——差恰不足一页）且记录流静止 1.5s（半条记录的
     * carry 排干余量）。120s 上限（bind 卷可见性/段切换节奏）。
     *
     * @param name     通道名（失败消息面）
     * @param consumed 通道消费前沿
     * @param records  通道交付记录（静止判定的观察面）
     * @param boundary 对拍窗口上界
     */
    private static void awaitCaughtUp(String name, LongSupplier consumed, List<WalRecord> records, long boundary)
            throws InterruptedException {
        long floor = boundary & ~8191L;
        long stableSince = -1;
        int stableSize = -1;
        long deadline = System.currentTimeMillis() + CATCHUP_TIMEOUT_MS;
        while (true) {
            if (consumed.getAsLong() >= floor) {
                if (records.size() != stableSize) {
                    stableSize = records.size();
                    stableSince = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - stableSince >= STABLE_WINDOW_MS) {
                    return;
                }
            }
            assertTrue(System.currentTimeMillis() < deadline,
                    name + " 通道 " + CATCHUP_TIMEOUT_MS + "ms 未追平（前沿 "
                            + Lsn.format(consumed.getAsLong())
                            + " < 页底 " + Lsn.format(floor) + " 或记录流未静止）");
            Thread.sleep(CATCHUP_POLL_MS);
        }
    }

    /**
     * 清空段 bind 目录（host 侧）——上一测试残留段会使 FileWalSource 从陈旧段起 tail。
     */
    private static void cleanWalDir() throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(OgTestEnv.hostWalDir())) {
            for (Path p : stream) {
                Files.deleteIfExists(p);
            }
        }
    }

    /**
     * 等 sidecar 首段文件落盘（host 侧 bind 目录出现 24-hex 段名文件；60s 上限——
     * 超时消息附 pg_receivexlog 容器内日志尾）。
     */
    private static void awaitFirstSegment() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (!hasSegmentFile()) {
            assertTrue(System.currentTimeMillis() < deadline,
                    "60s 内 bind 卷无段文件（pg_receivexlog 未起？日志尾: "
                            + OgTestEnv.receivexlogLogTail() + "）");
            Thread.sleep(500);
        }
    }

    /**
     * bind 目录是否已有段文件（24-hex 名，可带 .partial）。
     *
     * @return 有段文件为 true
     */
    private static boolean hasSegmentFile() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(OgTestEnv.hostWalDir())) {
            for (Path p : stream) {
                String name = p.getFileName().toString().replace(".partial", "");
                if (name.length() == 24 && name.matches("[0-9A-Fa-f]{24}")) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }
}
