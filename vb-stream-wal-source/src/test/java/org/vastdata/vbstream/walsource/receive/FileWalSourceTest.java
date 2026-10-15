package org.vastdata.vbstream.walsource.receive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.layout.WalBytes;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
 *
 * <p>静态目录档（offline-replay Task 2）用例组：启动期审计 fail-fast（起点段缺席/tli
 * 混杂/段号缺口）+ 读尽确定性 EOF/endLsn 截停/.partial 前沿——段文件经
 * {@link #writeSegment} 稀疏造、零 rename、零 POSIX 权限依赖（Windows 本机可跑）。</p>
 */
class FileWalSourceTest {

    /** rmgrlist.h 0-based 枚举序：RM_HEAP_ID=10（heap）。 */
    private static final int RM_HEAP_ID = 10;

    /** 测试档段尺寸：恰 2 页（生产 16MB，参数化面见 FileWalSource.DEFAULT_SEGMENT_SIZE）。 */
    private static final long SEG_SIZE = 2L * 8192;

    /** 异步等待的上限（毫秒）——tail 轮询 50ms 节拍下所有场景秒内收敛，10s 已是宽松界。 */
    private static final long AWAIT_TIMEOUT_MS = 10_000;

    /** 每 xlogid 段数：0x100000000 / 段尺寸（XLogSegmentsPerXLogId 同式；16KB 档 = 262144）。 */
    private static final long SEGS_PER_XLOG_ID = 0x100000000L / SEG_SIZE;

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

    /**
     * 用例 ①g（log/seg 段名语义锚——真库 lsn ≥ 4GB 形态）：段名 24-hex 的语义是
     * tli(8) + log(8) + seg(8)，其中 log = 段号 / 每-xlogid-段数、seg = 段号 % 同除数
     * （XLogSegmentsPerXLogId = 4GB/段尺寸；16KB 档除数 262144）——段号恰达除数边界时
     * 标准名是 log=1/seg=0。若解析按 (log&lt;&lt;32)|seg 直拼，该形态段号错位 2^32 量级、
     * 寻址整体错乱（真库 pg_xlog 直读实测 walker pageaddr 锚定 ISE，spike 第三轮锚）；
     * 断言标准命名段文件的记录按真 LSN 交付。
     */
    @Test
    void standardLogSegNameAtFourGbBoundaryAddressesCorrectLsn() throws Exception {
        long segNo = SEGS_PER_XLOG_ID;                  // 262144：标准名 log=1/seg=0（4GB 边界）
        long base = segNo * SEG_SIZE;
        byte[] p0 = WalBytes.page(base, rec(1), rec(2));
        byte[] p1 = WalBytes.page(base + 8192, rec(3));
        Files.write(dir.resolve(segName(segNo)), concat(p0, p1));

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            awaitTrue(() -> source.consumedLsn() == base + SEG_SIZE, "4GB 边界段须按真段号寻址读尽");
        } finally {
            source.stop();
        }

        assertEquals(List.of(base + 24, base + 24 + align8(rec(1).length), base + 8192 + 24),
                records.stream().map(WalRecord::lsn).toList(), "边界段三记录零丢零重");
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
            // .partial 交付上界 = 已接收前沿（末非零 + 1，Task 8）——页尾零垫不计入
            awaitTrue(() -> source.consumedLsn() == baseA + lastNonZeroPlusOne(p0), "首写半段须被读尽");
            Files.write(partial, p1, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            awaitTrue(() -> source.consumedLsn() == baseA + 8192 + lastNonZeroPlusOne(p1), "增长段须被读尽");

            // pg_receivexlog 完成段 rename + 下一段 .partial 出现
            Files.move(partial, dir.resolve(segName(3)));
            Files.write(dir.resolve(segName(4) + ".partial"), pb0);
            awaitTrue(() -> source.consumedLsn() == baseB + lastNonZeroPlusOne(pb0), "rename 后须切到下一段并读尽");
        } finally {
            source.stop();
        }

        assertEquals(List.of(baseA + 24, baseA + 24 + align8(rec(1).length), baseA + 8192 + 24, baseB + 24),
                records.stream().map(WalRecord::lsn).toList(), "跨段记录序列连续（零丢零重）");
        assertEquals(baseB + lastNonZeroPlusOne(pb0), source.consumedLsn());
    }

    /**
     * 用例 ②f（Task 8 修复锚——OG pg_receivexlog 预分配形态）：.partial 是<b>预分配整段
     * 尺寸的全零文件</b>、数据按接收序原地写入（og-plan strace 实锚）——按文件长度 tail
     * 会把未写区预分配零当数据交付，walker 跳零页冲到段尾后原地写入的真记录永不重读。
     * 断言：交付上界 = 已接收前沿（不冲段尾）；frontier 处原地续写后续交付、记录零丢失。
     */
    @Test
    void preallocatedPartialTailDeliversOnlyReceivedFrontierThenFollowsInplaceWrites() throws Exception {
        long base = 3L * SEG_SIZE;
        byte[] p0 = WalBytes.page(base, rec(1));
        byte[] p1 = WalBytes.page(base + 8192, rec(2));
        Path partial = dir.resolve(segName(3) + ".partial");
        byte[] preallocated = new byte[(int) SEG_SIZE];
        System.arraycopy(p0, 0, preallocated, 0, p0.length);
        Files.write(partial, preallocated);   // 预分配整段（零尾）

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            awaitTrue(() -> source.consumedLsn() == base + lastNonZeroPlusOne(p0),
                    "预分配零尾不得被交付（前沿 = 首页末非零）");
            // 原地续写第二页（pg_receivexlog 写序形态——数据落在已"存在"的零区）
            try (var raf = new RandomAccessFile(partial.toFile(), "rw")) {
                raf.seek(8192);
                raf.write(p1);
            }
            awaitTrue(() -> source.consumedLsn() == base + 8192 + lastNonZeroPlusOne(p1),
                    "原地续写须按前沿续交付");
        } finally {
            source.stop();
        }

        assertEquals(List.of(base + 24, base + 8192 + 24),
                records.stream().map(WalRecord::lsn).toList(), "两记录零丢失（预分配零尾不吞记录）");
    }

    /** 用例 ②b：目录里既有完成段又有新 partial（external 模式任意组合）→ 按段号序全量消费。 */
    @Test
    void mixedCompletedAndPartialSegmentsConsumedInSegmentOrder() throws Exception {
        long baseA = 3L * SEG_SIZE;
        long baseB = 4L * SEG_SIZE;
        long baseC = 5L * SEG_SIZE;
        byte[] pageC = WalBytes.page(baseC, rec(5));
        Files.write(dir.resolve(segName(3)),
                concat(WalBytes.page(baseA, rec(1), rec(2)), WalBytes.page(baseA + 8192, rec(3))));
        Files.write(dir.resolve(segName(4)), WalBytes.page(baseB, rec(4)));
        Files.write(dir.resolve(segName(5) + ".partial"), pageC);

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            // 尾段是 .partial：交付上界 = 已接收前沿（末非零 + 1，Task 8）
            awaitTrue(() -> source.consumedLsn() == baseC + lastNonZeroPlusOne(pageC), "三段须按序全量读尽");
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

    /**
     * 用例 ②d（log/seg 段名语义锚——续传面）：fromLsn ≥ 4GB 时目标段的标准名是
     * log=1/seg=0 形态——直拼解析下目录键错位 2^32 量级，{@code ceilingKey(真段号)}
     * 选中的是错位段、锚错乱挂起（真库实测流中断形态）；断言 fromLsn 越过 4GB 边界
     * 仍选中标准名目标段、更早段全部跳过。
     */
    @Test
    void resumeFromLsnBeyondFourGbSelectsStandardNamedSegment() throws Exception {
        long segLo = SEGS_PER_XLOG_ID - 1;      // 262143：标准名 log=0/seg=262143（边界前一段）
        long segHi = SEGS_PER_XLOG_ID;          // 262144：标准名 log=1/seg=0（fromLsn 目标段）
        long baseLo = segLo * SEG_SIZE;
        long baseHi = segHi * SEG_SIZE;
        Files.write(dir.resolve(segName(segLo)),
                concat(WalBytes.page(baseLo, rec(1)), WalBytes.page(baseLo + 8192, rec(2))));
        Files.write(dir.resolve(segName(segHi)),
                concat(WalBytes.page(baseHi, rec(3)), WalBytes.page(baseHi + 8192, rec(4))));

        List<WalRecord> records = new ArrayList<>();
        WalStreamWalker walker = new WalStreamWalker(WalLayoutV18.INSTANCE, new WalStreamMetrics(), records::add);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, baseHi, 8192);
        source.start(lsn -> { }, walker::feed);

        try {
            awaitTrue(() -> source.consumedLsn() == baseHi + SEG_SIZE, "4GB 后续传须选中标准名目标段读尽");
        } finally {
            source.stop();
        }

        assertEquals(List.of(baseHi + 24, baseHi + 8192 + 24), records.stream().map(WalRecord::lsn).toList(),
                "只交付目标段记录（4GB 边界前一段跳过）");
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
            // 等末参（og_slot）在场而非仅文件存在——shell 逐参 append 与测试读存在竞态窗
            // （全量套件高负载下曾假红：文件已建但参数未写完）
            awaitTrue(() -> fileHasLine(argsFile, "og_slot"), "假二进制须被拉起并捕获全部参数");
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
     * 用例 ③e（Task 6 接线债——Task 4 遗留）：managed 模式 sidecar 终态失败（连续重拉耗尽）
     * → tail 联动停机并把失败传播为源级 {@code terminalFailure()}（根因链保留 sidecar 的
     * 终态异常）——不传播则源停更而宿主无终态可查（进程空转）。
     *
     * <p>构造：立即退出的假二进制 + 快速退避（10ms 基数 / 上限 1 次）→ 2 次拉起后监督
     * 线程置终态；目录保持空（tail 无可读段）→ 断言唯一推进面就是终态传播。</p>
     */
    @Test
    void sidecarTerminalFailurePropagatesToSourceAndStopsTail() throws Exception {
        Path bin = writeScript("#!/bin/sh\nexit 7\n");
        ReceivexlogProcess proc = new ReceivexlogProcess(bin.toString(), dir,
                null, 0, null, null, null, 10, 1);
        FileWalSource source = new FileWalSource(dir, SEG_SIZE, 0, 8192, proc);
        source.start(lsn -> { }, (lsn, bytes) -> { });
        try {
            awaitTrue(() -> source.terminalFailure().isPresent(), "sidecar 终态须传播为源级终态失败");
        } finally {
            source.stop();
        }
        Throwable t = source.terminalFailure().orElseThrow();
        assertTrue(t.getMessage().contains("pg_receivexlog"), "终态消息须指明 sidecar 失败根源");
        assertNotNull(t.getCause(), "根因链须保留 sidecar 的终态异常（排障面）");
        awaitTrue(() -> !proc.supervisorAlive(), "源停机须连带监督线程终止");
    }

    // ---- 静态目录档（offline-replay Task 2）：启动期审计 fail-fast + 清单读尽确定性 EOF ----

    /**
     * 静态档确定性 EOF：单段目录（非零字节全量写 + 文件长恰交付断言依据）读尽后
     * eofReached 置位、sink 收到的字节总量 = 文件全长（无轮询等待——读尽即终态）。
     */
    @Test
    void staticDirectoryReachesDeterministicEof() throws Exception {
        writeSegment(dir, "000000010000000000000003", 3 * 8192 + 4096);
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, 0L);
        List<byte[]> chunks = new ArrayList<>();
        src.start(lsn -> { }, (end, b) -> chunks.add(b));
        try {
            awaitTrue(src::eofReached, "静态档单段读尽须确定性置 eofReached（无轮询等待）");
        } finally {
            src.stop();
        }
        long total = chunks.stream().mapToLong(b -> b.length).sum();
        assertEquals(3 * 8192 + 4096, total, "已写字节全量交付（完成段按文件全长读）");
        assertFalse(src.terminalFailure().isPresent(), "确定性读尽非终态失败");
    }

    /**
     * 静态档起点段缺失 fail-fast：fromLsn 落段 5 而目录只有段 3——启动期 ISE（tail 档
     * ceilingKey 静默跳后继段语义在静态档不可接受，spec §3.3）；且审计失败不置 started
     * （终审 F2）——同实例重试 start 重新审计而非"already started"单程拒绝。
     */
    @Test
    void staticDirectoryRejectsMissingStartSegment() throws Exception {
        writeSegment(dir, "000000010000000000000003", 8192);
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 5L * SEG_SIZE, 8192, 0L);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> src.start(lsn -> { }, (end, b) -> { }));
        assertTrue(e.getMessage().contains("段 5"), "消息含期望段号: " + e.getMessage());
        IllegalStateException retry = assertThrows(IllegalStateException.class,
                () -> src.start(lsn -> { }, (end, b) -> { }));
        assertTrue(retry.getMessage().contains("段 5"),
                "审计失败后重试 start 重新审计（非 already started）: " + retry.getMessage());
    }

    /**
     * 静态档 tli 混杂 fail-fast：目录混入 tli=2 段名——ISE 列出冲突 tli（真库 pg_xlog
     * 混历史分支段的正解，spec §3.3）。
     */
    @Test
    void staticDirectoryRejectsMixedTimeline() throws Exception {
        writeSegment(dir, "000000010000000000000003", 8192);
        writeSegment(dir, "000000020000000000000004", 8192);
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, 0L);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> src.start(lsn -> { }, (end, b) -> { }));
        assertTrue(e.getMessage().contains("tli"), "消息含 tli 冲突: " + e.getMessage());
    }

    /**
     * 静态档段号缺口 fail-fast：目录只有段 3 与段 5（缺段 4）——快照读尽语义下中段缺口
     * 即 WAL 字节丢失，启动期 ISE 消息含缺口段号（spec §3.3 连续性校验）。
     */
    @Test
    void staticDirectoryRejectsSegmentGap() throws Exception {
        writeSegment(dir, "000000010000000000000003", 8192);
        writeSegment(dir, "000000010000000000000005", 8192);
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, 0L);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> src.start(lsn -> { }, (end, b) -> { }));
        assertTrue(e.getMessage().contains("段 4"), "消息含缺口段号: " + e.getMessage());
    }

    /**
     * 静态档 endLsn 上界截停：endLsn 落段 3 的段内偏移（页 1 中段）、清单尚有段 4 残余
     * ——末块截断至恰 endLsn、eofReached 置位、消费前沿钉在上界，残余段零交付。
     */
    @Test
    void staticDirectoryStopsAtEndLsn() throws Exception {
        writeSegment(dir, "000000010000000000000003", SEG_SIZE);
        writeSegment(dir, "000000010000000000000004", SEG_SIZE);
        long end = 3L * SEG_SIZE + 8192 + 100;
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, end);
        List<byte[]> chunks = new ArrayList<>();
        List<Long> frontier = new ArrayList<>();
        src.start(frontier::add, (e, b) -> chunks.add(b));
        try {
            awaitTrue(src::eofReached, "endLsn 到达须置 eofReached 并退出");
        } finally {
            src.stop();
        }
        long total = chunks.stream().mapToLong(b -> b.length).sum();
        assertEquals(8192 + 100, total, "交付总量恰到 endLsn（残余段零交付）");
        assertEquals(end, frontier.get(frontier.size() - 1), "末块锚 = endLsn");
        assertEquals(end, src.consumedLsn(), "消费前沿钉在 endLsn");
    }

    /**
     * 静态档 .partial 前沿交付上限：段文件预分配稀疏零区（前 8292B 非零、其后零至
     * 段尾）——交付上限 = 末非零字节 + 1，稀疏零区不交付（与 tail 档 receivedFrontier
     * 同式，静态档一次性扫定）。
     */
    @Test
    void staticDirectoryPartialSegmentFrontier() throws Exception {
        writeSegment(dir, "000000010000000000000003.partial", 8192 + 100);
        try (RandomAccessFile raf = new RandomAccessFile(
                dir.resolve("000000010000000000000003.partial").toFile(), "rw")) {
            raf.setLength(SEG_SIZE);   // 尾部扩出稀疏零区（模拟 pg_receivexlog 预分配形态）
        }
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, 0L);
        List<byte[]> chunks = new ArrayList<>();
        src.start(lsn -> { }, (end, b) -> chunks.add(b));
        try {
            awaitTrue(src::eofReached, ".partial 前沿读尽须置 eofReached");
        } finally {
            src.stop();
        }
        long total = chunks.stream().mapToLong(b -> b.length).sum();
        assertEquals(8192 + 100, total, ".partial 交付上限 = 末非零字节+1（稀疏零区不交付）");
    }

    /**
     * 静态档<strong>末段</strong>解析边界软 EOF（2026-10-15 真库 pg_xlog 拷段实证）：
     * 末段交付中 sink 抛 {@link WalStreamParseException}（页链锚定丢失/记录头损坏——
     * 裸拷 pg_xlog 活跃段撞回收残留旧 WAL 的写前沿指纹）→ 降级软 EOF——eofReached
     * 置位、terminalFailure 空、撞点之前的字节照常交付（宿主据此落最终检查点优雅退出
     * exit 0，无需 endLsn 上界）。
     */
    @Test
    void staticDirectoryLastSegmentParseBoundarySoftEofs() throws Exception {
        writeSegment(dir, "000000010000000000000003", SEG_SIZE);
        writeSegment(dir, "000000010000000000000004", SEG_SIZE);
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, 0L);
        List<byte[]> chunks = new ArrayList<>();
        // 段 3（清单首段）正常交付；段 4（清单末段）首块抛解析边界异常——模拟 walker 撞进
        // 回收残留旧 WAL 的写前沿
        src.start(lsn -> { }, (end, b) -> {
            if (end > 4L * SEG_SIZE) {
                throw new WalStreamParseException("simulated anchor lost at " + end);
            }
            chunks.add(b);
        });
        try {
            awaitTrue(src::eofReached, "末段解析边界须降级软 EOF（eofReached 置位）");
        } finally {
            src.stop();
        }
        assertFalse(src.terminalFailure().isPresent(), "末段软 EOF 非终态失败");
        long total = chunks.stream().mapToLong(b -> b.length).sum();
        assertTrue(total >= SEG_SIZE, "撞点之前的段 3 全量交付（实测 total=" + total + "）");
        assertTrue(src.consumedLsn() <= 4L * SEG_SIZE,
                "消费前沿停在撞点之前（实测 " + src.consumedLsn() + "）");
    }

    /**
     * 静态档<strong>非末段</strong>解析边界仍终态失败：清单首段（后面还有段）交付中撞
     * {@link WalStreamParseException}——段间断链 = 真缺口/真损坏，不得软 EOF 静默截停
     * （否则后续段数据整体丢失）；terminalFailure 暴露根因、eofReached 不置位。
     */
    @Test
    void staticDirectoryNonLastSegmentParseBoundaryStaysTerminal() throws Exception {
        writeSegment(dir, "000000010000000000000003", SEG_SIZE);
        writeSegment(dir, "000000010000000000000004", SEG_SIZE);
        FileWalSource src = FileWalSource.staticDirectory(dir, SEG_SIZE, 0L, 8192, 0L);
        List<byte[]> chunks = new ArrayList<>();
        src.start(lsn -> { }, (end, b) -> {
            if (end > 3L * SEG_SIZE) {
                throw new WalStreamParseException("simulated anchor lost at " + end);
            }
            chunks.add(b);
        });
        try {
            awaitTrue(() -> src.terminalFailure().isPresent(), "非末段解析边界须终态失败");
        } finally {
            src.stop();
        }
        assertTrue(src.terminalFailure().orElseThrow() instanceof WalStreamParseException,
                "终态根因为解析边界异常原样透传");
        assertFalse(src.eofReached(), "终态失败不置 eofReached（失败 ≠ 读尽）");
    }

    /**
     * 判定文本文件是否已含指定行（文件级 await 的内容条件——吸收 shell 逐行 append 的
     * 写入竞态窗；读失败按未就绪处理）。
     *
     * @param file 目标文件
     * @param line 期望行
     * @return 文件存在且含该行为 true
     */
    private static boolean fileHasLine(Path file, String line) {
        try {
            return Files.exists(file) && Files.readAllLines(file).contains(line);
        } catch (IOException e) {
            return false;
        }
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
     * 造一个静态档测试段文件：文件长恰 len、内容全为非零字节（0x5A 填充）——计划裁定
     * 语义：完成段按文件全长交付（用例断言交付总量 = len 依赖此），.partial 的稀疏
     * 零尾由调用方事后 setLength 扩出（本辅助只负责非零前段）。
     *
     * @param dir  段目录
     * @param name 段文件名（24-hex，可带 .partial 后缀）
     * @param len  文件长度（= 非零前段长度）
     * @throws IOException 建文件/写失败
     */
    private static void writeSegment(Path dir, String name, long len) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(dir.resolve(name).toFile(), "rw")) {
            raf.setLength(len);
            byte[] fill = new byte[(int) len];
            Arrays.fill(fill, (byte) 0x5A);
            raf.seek(0);
            raf.write(fill);
        }
    }

    /**
     * 造一条小体积完整记录（单 block data + 8B main），与 WalStreamWalkerTest.smallRec 同构。
     *
     * <p>末字节置非零（0x5A）：DSL 的 crc 占位与 MAXALIGN 垫是零字节，而 .partial 通道的
     * 交付上界是"末非零字节 + 1"（{@code receivedFrontier}，Task 8）——尾零记录在该语义
     * 下永不完整交付；真实 WAL 记录以 CRC32 收尾（全零概率可忽略），测试语料以末字节
     * 非零对齐该前提。</p>
     *
     * @param xid 记录头 xid（兼作用例内的序号标识）
     * @return 记录字节数组（长度即 totLen）
     */
    private static byte[] rec(int xid) {
        byte[] r = WalBytes.record(RM_HEAP_ID, 0x00, xid)
                .block(0, 1663L, 16385L, 24600L, 1)
                .data(new byte[] {1, 2, 3, 4})
                .main(new byte[8])
                .build();
        r[r.length - 1] = 0x5A;
        return r;
    }

    /**
     * 字节序的末非零字节 + 1（.partial 已接收前沿语义的测试侧镜像——与
     * {@code FileWalSource.receivedFrontier} 的判定同式，双源互证）。
     *
     * @param bytes 已写区字节
     * @return 末非零字节后一位的偏移（全零为 0）
     */
    private static int lastNonZeroPlusOne(byte[] bytes) {
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (bytes[i] != 0) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * 组装段文件名（pg_receivexlog / pg_xlog 命名契约）：tli 固定 1，段号按每-xlogid-段数
     * 拆 log（高 8 位十六进制）与 seg（低 8 位）——seg &lt; 除数（段名字段的真实值域）。
     *
     * @param segNo 绝对段号
     * @return 24 字符段文件名（不含 .partial 后缀）
     */
    private static String segName(long segNo) {
        return String.format("%08X%08X%08X", 1L, segNo / SEGS_PER_XLOG_ID, segNo % SEGS_PER_XLOG_ID);
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
