package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MessageSpool（Chronicle Queue 溢写池）单元测试：append/readRange 往返、瞬态清空语义、
 * 滚动文件删除数学（注入式纯函数验证）与低水位释放端到端。
 * 所有用例用 {@code LegacyRollCycles.MINUTELY}（滚动文件名 {@code yyyyMMdd-HHmm.cq4}，UTC）。
 */
class MessageSpoolTest {

    /** MINUTELY cycle 号基数：{@code 2026-01-01T00:00Z} 距默认纪元（1970-01-01 UTC）的分钟数——
     * 即假想文件 {@code 20260101-0000.cq4} 对应的 cycle 号（brief 注：以 Duration 计算硬数字，保持断言可读）。 */
    private static final long CYCLE_20260101_0000 =
            Duration.between(Instant.EPOCH, Instant.parse("2026-01-01T00:00:00Z")).toMinutes();

    /**
     * append → readRange 闭区间往返：三条帧按 index 升序完整读回，且从区间中部（第二条）起步
     * 也能读到正确的字节与区间后缀。
     * 关键步骤：append 三条单字节帧记录返回 index → [a..c] 全区间回读断言字节序 →
     * [b..c] 子区间回读断言按 0 起序号依次为第 2、3 条（moveToIndex 落到非索引间距倍数的 index；
     * brief 原断言只查 {2}，对 [b..c] 两条帧不成立，改按序号断言后缀）。
     * 边界：单字节载荷最小化；CQ index 由实现分配（非 0 起），断言只依赖相对顺序。
     *
     * @param dir 瞬态队列目录
     */
    @Test
    void appendReadRangeRoundTrip(@TempDir Path dir) {
        try (MessageSpool spool = new MessageSpool(dir, LegacyRollCycles.MINUTELY)) {
            long a = spool.append(new byte[]{1});
            long b = spool.append(new byte[]{2});
            long c = spool.append(new byte[]{3});
            List<byte[]> got = new ArrayList<>();
            spool.readRange(a, c, (framed, idx) -> got.add(framed));
            assertArrayEquals(new byte[][]{{1}, {2}, {3}}, got.toArray(new byte[0][]));
            spool.readRange(b, c, (framed, idx) -> assertEquals(2 + idx, framed[0]));
        }
    }

    /**
     * 重开清空（瞬态工作区语义）：同一目录第二次构造 MessageSpool 后，旧实例写入的内容不可见——
     * 构造先清空目录再建队列，杜绝陈旧状态（重启后复制槽重发，spool 不跨进程续用）。
     * 关键步骤：s1 写入一条并关闭 → s2 同目录重开 → readRange(0,100) 期望读不到任何帧。
     * 边界：起点 index 0 在新队列中不存在（cycle 0 无滚动文件），读到队尾即空，不抛异常。
     *
     * @param dir 瞬态队列目录
     */
    @Test
    void reopenWipesStaleContent(@TempDir Path dir) {
        try (MessageSpool s1 = new MessageSpool(dir, LegacyRollCycles.MINUTELY)) {
            s1.append(new byte[]{9});
        }
        try (MessageSpool s2 = new MessageSpool(dir, LegacyRollCycles.MINUTELY)) {
            List<byte[]> got = new ArrayList<>();
            s2.readRange(0, 100, (f, i) -> got.add(f));   // 旧内容不存在：0 起步读不到即空
            assertTrue(got.isEmpty());
        }
    }

    /**
     * 注入式删除数学：构造三个假滚动文件（cycle 号 C+0/C+1/C+2），neededCycle=C+2 时
     * 只删 C+0 一档——严格低于 neededCycle-1 的才可删，当前档与上一档永不触碰。
     * 关键步骤：写入三个假 {@code .cq4} 文件（内容无关，只看文件名）→ 以 neededCycle=C+2
     * 调纯函数 deletableFiles → 断言仅 cycle 最低档入选。
     * 边界：目录中还有队列元数据文件（{@code metadata.cq4t}，后缀不同）时不产生干扰（本用例不建队列）。
     *
     * @param dir 假滚动文件所在目录
     * @throws IOException 写假文件失败
     */
    @Test
    void releaseBelowNeverTouchesRecentCycles(@TempDir Path dir) throws IOException {
        // 注入式删除数学：构造假滚动文件名（MINUTELY 命名 YYYYMMDD-HHMM.cq4）
        Files.writeString(dir.resolve("20260101-0000.cq4"), "x");
        Files.writeString(dir.resolve("20260101-0001.cq4"), "x");
        Files.writeString(dir.resolve("20260101-0002.cq4"), "x");
        // neededCycle=C+2（20260101-0002 的 cycle 号）→ 只删 cycle C+0 一档（保留 needed 与 needed-1）
        List<Path> doomed = MessageSpool.deletableFiles(LegacyRollCycles.MINUTELY, dir, CYCLE_20260101_0000 + 2);
        assertEquals(List.of(dir.resolve("20260101-0000.cq4")), doomed);
    }

    /**
     * 低水位释放端到端：真实队列写入一条后以自身 lastAppendedIndex 为低水位释放——
     * needed cycle 即当前写入档，理论可删集为空，返回 0 且后续 append 不受影响。
     * 关键步骤：append 一条 → releaseBelow(lastAppendedIndex()) 断言删除数 0（当前档与上一档都保留）。
     * 边界：队列目录里同时存在当前滚动文件与 {@code metadata.cq4t}，均不得入选。
     *
     * @param dir 瞬态队列目录
     */
    @Test
    void liveReleaseBelowEndToEnd(@TempDir Path dir) throws IOException {
        try (MessageSpool spool = new MessageSpool(dir, LegacyRollCycles.MINUTELY)) {
            spool.append(new byte[]{1});
            assertEquals(0, spool.releaseBelow(spool.lastAppendedIndex())); // 当前 cycle，无文件可删
        }
    }
}
