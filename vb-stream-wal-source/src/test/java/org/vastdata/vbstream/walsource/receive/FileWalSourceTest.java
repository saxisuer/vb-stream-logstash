package org.vastdata.vbstream.walsource.receive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * FileWalSource（目录 tail 推模式字节源）的失败先行测试——段发现/增长读/rename 切段/续传起点。
 *
 * <p>字节面全部经 {@link WalBytes} 整页拼装 helper 构造（与 WalStreamWalkerTest 同源），
 * sink 接线复刻 Task 8 形态：chunkSink 直喂 {@link WalStreamWalker}，以"记录序列零丢零重
 * + LSN 连续"验收源的分块/锚语义（锚 = 段号×段尺寸+文件内偏移，页头 pageaddr 随后接管
 * 纠偏）。段尺寸走参数面（16MB 生产值对离线测试过大，用 2 页 = 16KB 档）。增长/rename
 * 由测试线程直接写临时目录模拟 pg_receivexlog 的落盘行为。</p>
 */
class FileWalSourceTest {

    /** rmgrlist.h 0-based 枚举序：RM_HEAP_ID=10（heap）。 */
    private static final int RM_HEAP_ID = 10;

    /** 测试档段尺寸：恰 2 页（生产 16MB，参数化面见 FileWalSource.DEFAULT_SEGMENT_SIZE）。 */
    private static final long SEG_SIZE = 2L * 8192;

    /** 异步等待的上限（毫秒）——tail 轮询 50ms 节拍下所有场景秒内收敛，10s 已是宽松界。 */
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    @TempDir
    Path dir;

    /** 用例 ①：目录预置完整段文件（2 页合法页头+记录链）→ start 后全记录产出、consumedLsn 前进到段尾。 */
    @Test
    void prebuiltCompleteSegmentDeliversAllRecordsAndCursorReachesSegmentEnd() throws Exception {
        long base = 3L * SEG_SIZE;
        byte[] p0 = WalBytes.page(base, rec(1), rec(2));
        byte[] p1 = WalBytes.page(base + 8192, rec(3));
        Files.write(dir.resolve(segName(3)), concat(p0, p1));

        List<WalRecord> records = new ArrayList<>();
        List<Long> frontier = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192);
        source.start(frontier::add, walker::feed);

        long end = base + SEG_SIZE;
        try {
            awaitTrue(() -> source.consumedLsn() == end, "consumedLsn 须推进到段尾 " + end);
        } finally {
            source.stop();   // 失败路径也停源（防线程越过 JUnit 临时目录清理后刷暂态 WARN）
        }
        source.stop();   // 幂等

        assertEquals(List.of(base + 24, base + 24 + align8(rec(1).length), base + 8192 + 24),
                records.stream().map(WalRecord::lsn).toList(), "两页三记录零丢零重");
        assertEquals(end, source.consumedLsn());
        assertFalse(frontier.isEmpty(), "chunkEndLsnSink 须有前沿通知");
        assertEquals(end, frontier.get(frontier.size() - 1), "前沿通知末值 = 末块锚");
    }

    /** 用例 ②：.partial 分两次增长 + rename 切段 → 记录序列跨段连续（零丢零重）。 */
    @Test
    void partialGrowthThenRenameSwitchDeliversContinuousRecordSequence() throws Exception {
        long baseA = 3L * SEG_SIZE;
        long baseB = 4L * SEG_SIZE;
        byte[] p0 = WalBytes.page(baseA, rec(1), rec(2));
        byte[] p1 = WalBytes.page(baseA + 8192, rec(3));
        byte[] pb0 = WalBytes.page(baseB, rec(4));
        Path partial = dir.resolve(segName(3) + ".partial");
        Files.write(partial, p0);

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            awaitTrue(() -> source.consumedLsn() == baseA + 8192, "首写半段须被读尽");
            Files.write(partial, p1, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            awaitTrue(() -> source.consumedLsn() == baseA + SEG_SIZE, "增长段须被读尽");

            // pg_receivexlog 完成段 rename + 下一段 .partial 出现
            Files.move(partial, dir.resolve(segName(3)));
            Files.write(dir.resolve(segName(4) + ".partial"), pb0);
            awaitTrue(() -> source.consumedLsn() == baseB + 8192, "rename 后须切到下一段并读尽");
        } finally {
            source.stop();
        }

        assertEquals(List.of(baseA + 24, baseA + 24 + align8(rec(1).length), baseA + 8192 + 24, baseB + 24),
                records.stream().map(WalRecord::lsn).toList(), "跨段记录序列连续（零丢零重）");
        assertEquals(baseB + 8192, source.consumedLsn());
    }

    /** 用例 ②b：目录里既有完成段又有新 partial（external 模式任意组合）→ 按段号序全量消费。 */
    @Test
    void mixedCompletedAndPartialSegmentsConsumedInSegmentOrder() throws Exception {
        long baseA = 3L * SEG_SIZE;
        long baseB = 4L * SEG_SIZE;
        long baseC = 5L * SEG_SIZE;
        Files.write(dir.resolve(segName(3)),
                concat(WalBytes.page(baseA, rec(1), rec(2)), WalBytes.page(baseA + 8192, rec(3))));
        Files.write(dir.resolve(segName(4)), WalBytes.page(baseB, rec(4)));
        Files.write(dir.resolve(segName(5) + ".partial"), WalBytes.page(baseC, rec(5)));

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            awaitTrue(() -> source.consumedLsn() == baseC + 8192, "三段须按序全量读尽");
        } finally {
            source.stop();
        }

        assertEquals(List.of(baseA + 24, baseA + 24 + align8(rec(1).length), baseA + 8192 + 24,
                        baseB + 24, baseC + 24),
                records.stream().map(WalRecord::lsn).toList(), "完成段与 partial 混排仍按段号序交付");
    }

    /** 用例 ②c：断点续传——fromLsn 所在段的页对齐下取整起点起读，之前段/页全部跳过。 */
    @Test
    void resumeFromMidSegmentStartsPageAlignedAndSkipsEarlierSegments() throws Exception {
        long baseA = 3L * SEG_SIZE;
        long baseB = 4L * SEG_SIZE;
        Files.write(dir.resolve(segName(3)),
                concat(WalBytes.page(baseA, rec(1), rec(2)), WalBytes.page(baseA + 8192, rec(3))));
        Files.write(dir.resolve(segName(4)),
                concat(WalBytes.page(baseB, rec(4)), WalBytes.page(baseB + 8192, rec(5))));

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        // fromLsn 落 B 段第 1 页中段 → 页对齐下取整 = baseB + 8192：只读 B 页 1
        long from = baseB + 8192 + 100;
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, from, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            awaitTrue(() -> source.consumedLsn() == baseB + SEG_SIZE, "续传只读 fromLsn 所在段（页对齐起点）");
        } finally {
            source.stop();
        }

        assertEquals(List.of(baseB + 8192 + 24), records.stream().map(WalRecord::lsn).toList(),
                "A 段与 B 页 0 的记录不得交付（跳过已读过的）");
    }

    /** 用例 ③：假二进制验证拉起面——配置经参数传递、密码只经 PGPASSWORD 环境变量（绝不经命令行）+ 终止链。 */
    @Test
    void receivexlogProcessPassesConfigViaArgsAndEnvNeverPasswordOnCommandLine() throws Exception {
        Path envFile = dir.resolve("cap-env.txt");
        Path argsFile = dir.resolve("cap-args.txt");
        Path bin = writeScript("#!/bin/sh\n"
                + "echo \"PGPASSWORD=$PGPASSWORD\" > '" + envFile + "'\n"
                + "for a in \"$@\"; do echo \"$a\" >> '" + argsFile + "'; done\n"
                + "sleep 3600\n");

        ReceivexlogProcess proc = new ReceivexlogProcess(bin.toString(), dir,
                "og-host", 5432, "gauss", "s3cret-pass", "og_slot");
        proc.start();
        try {
            awaitTrue(() -> Files.exists(argsFile), "假二进制须被拉起并捕获参数");
            assertTrue(proc.processAlive(), "长驻假二进制须存活");

            List<String> args = Files.readAllLines(argsFile);
            assertTrue(args.contains("-D") && args.contains(dir.toString()), "-D 与目录须在参数中");
            assertTrue(args.contains("-h") && args.contains("og-host"), "-h 须在参数中");
            assertTrue(args.contains("-p") && args.contains("5432"), "-p 须在参数中");
            assertTrue(args.contains("-U") && args.contains("gauss"), "-U 须在参数中");
            assertTrue(args.contains("--slot") && args.contains("og_slot"), "--slot 须在参数中");
            assertFalse(String.join(" ", args).contains("s3cret-pass"),
                    "密码绝不经命令行（会进 ps）");
            assertEquals(List.of("PGPASSWORD=s3cret-pass"), Files.readAllLines(envFile),
                    "密码须经 PGPASSWORD 环境变量传入");
        } finally {
            proc.stop();
        }
        awaitTrue(() -> !proc.processAlive(), "stop 后子进程须退出（SIGTERM 链）");
        proc.stop();   // 幂等
        assertTrue(proc.terminalFailure().isEmpty(), "正常停机非终态失败");
    }

    /** 用例 ③b：子进程连续退出 → 指数退避重拉至 maxAttempts 后终态失败（不测真 pg_receivexlog）。 */
    @Test
    void supervisorRelaunchesWithBackoffUntilTerminalFailure() throws Exception {
        Path counter = dir.resolve("launches.txt");
        Path bin = writeScript("#!/bin/sh\necho x >> '" + counter + "'\nexit 7\n");

        // 快速退避档（20ms 基数 / 2 次上限）：3 次拉起后终态，总耗时 ~60ms
        ReceivexlogProcess proc = new ReceivexlogProcess(bin.toString(), dir,
                null, 0, null, null, null, 20, 2);
        proc.start();
        awaitTrue(() -> proc.terminalFailure().isPresent(), "连续退出须耗尽重拉置终态");
        awaitTrue(() -> !proc.processAlive(), "终态后无活子进程");
        assertEquals(3, Files.readAllLines(counter).size(), "failures>max 前恰拉起 max+1 次");
        assertTrue(proc.terminalFailure().isPresent());
        proc.stop();
    }

    /** 用例 ③c：managed 组合——FileWalSource start 拉起 sidecar 子进程，stop 连带终止。 */
    @Test
    void managedFileSourceStartsAndStopsSidecarProcess() throws Exception {
        Path bin = writeScript("#!/bin/sh\nsleep 3600\n");
        ReceivexlogProcess proc = new ReceivexlogProcess(bin.toString(), dir,
                null, 0, null, null, null);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192, proc);
        source.start(lsn -> { }, (lsn, bytes) -> { });
        try {
            awaitTrue(proc::processAlive, "managed start 须拉起子进程");
        } finally {
            source.stop();
        }
        awaitTrue(() -> !proc.processAlive(), "source stop 须连带终止子进程");
    }

    /**
     * 用例 ③d（fix round 1）：退出重拉 churn 中 stop——无孤儿子进程且监督线程时限内终止。
     *
     * <p>竞态窗构造：短命子进程（50ms 退出）+ 快速退避（10ms）形成持续重拉 churn；
     * 落 stop 前先放置长命开关（flag 后续子进程 {@code sleep 3600}）——stop 窗口内
     * 恰好拉起的子进程即为孤儿形态（长命 + 无人销毁），ProcessHandle 逐 pid 验尸。
     * 锁互斥修复下无论落点如何：先获锁则拉起面放弃、后获锁则 stop 销毁新句柄。</p>
     */
    @Test
    void stopDuringRelaunchChurnLeavesNoOrphanChildAndSupervisorExits() throws Exception {
        Path pids = dir.resolve("pids.txt");
        Path longRunFlag = dir.resolve("longrun.flag");
        Path bin = writeScript("#!/bin/sh\n"
                + "echo $$ >> '" + pids + "'\n"
                + "if [ -f '" + longRunFlag + "' ]; then sleep 3600; else sleep 0.05; exit 0; fi\n");

        ReceivexlogProcess proc = new ReceivexlogProcess(bin.toString(), dir,
                null, 0, null, null, null, 10, 1_000);
        proc.start();
        try {
            awaitTrue(() -> proc.launches() >= 2, "须形成退出重拉 churn（竞态窗前提）");
            Files.writeString(longRunFlag, "on");   // 放大：stop 窗内的新拉起变长命（孤儿可见）
        } finally {
            proc.stop();
        }
        awaitTrue(() -> !proc.supervisorAlive(), "stop 后监督线程须终止");
        // pid 文件按需验尸（子进程可能在脚本首行 echo 前被销毁——记录滞后于内存计数，
        // 不作数量下限锚）；孤儿判定的确定性锚是句柄面 processAlive
        for (String pid : Files.readAllLines(pids)) {
            boolean alive = ProcessHandle.of(Long.parseLong(pid.trim()))
                    .map(ProcessHandle::isAlive)
                    .orElse(false);
            assertFalse(alive, "拉起过的子进程 " + pid + " 不得存活（孤儿子进程）");
        }
        assertFalse(proc.processAlive(), "stop 后句柄面不得有活子进程（stop 窗内新拉起须被销毁/放弃）");
    }

    /**
     * 写一个可执行的假二进制 shell 脚本（模拟 pg_receivexlog 的可观测面）。
     *
     * @param content 脚本内容（首行 shebang 由调用方给出）
     * @return 脚本路径（已置属主可执行位）
     * @throws IOException 写文件/置权限失败
     */
    private Path writeScript(String content) throws IOException {
        Path script = dir.resolve("fake-receivexlog-" + System.nanoTime() + ".sh");
        Files.writeString(script, content);
        Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"));
        return script;
    }

    /**
     * 造一条小体积完整记录（单 block data + 8B main），与 WalStreamWalkerTest.smallRec 同构。
     *
     * @param xid 记录头 xid（兼作用例内的序号标识）
     * @return 记录字节数组（长度即 totLen）
     */
    private static byte[] rec(int xid) {
        return WalBytes.record(RM_HEAP_ID, 0x00, xid)
                .block(0, 1663L, 16385L, 24600L, 1)
                .data(new byte[] {1, 2, 3, 4})
                .main(new byte[8])
                .build();
    }

    /**
     * 组装段文件名（pg_receivexlog 命名契约）：tli 固定 1，段号拆 8+8 十六进制高低调。
     *
     * @param segNo 绝对段号
     * @return 24 字符段文件名（不含 .partial 后缀）
     */
    private static String segName(long segNo) {
        return String.format("%08X%08X%08X", 1L, segNo >>> 32, segNo & 0xFFFFFFFFL);
    }

    /**
     * MAXALIGN 到 8——记录终点对齐推算（与 walker 的页内对齐规则同式）。
     *
     * @param v 字节数
     * @return 向上对齐到 8 的值
     */
    private static int align8(int v) {
        return (v + 7) & ~7;
    }

    /**
     * 顺序拼接若干字节段（页拼段文件用）。
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

    /**
     * 异步条件等待：20ms 节拍轮询 cond 直至为真，超时 fail。
     *
     * <p>tail 线程与测试线程的可见性经 stop() join 建立的 happens-before 兜底，本处
     * 仅轮询 volatile 镜像（consumedLsn）。</p>
     *
     * @param cond 轮询条件
     * @param message 超时失败信息
     */
    private static void awaitTrue(BooleanSupplier cond, String message) {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("await 被中断: " + message);
            }
        }
        fail("等待超时: " + message);
    }
}
