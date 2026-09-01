package org.vastdata.debezium.connector.postgresql.stream;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MessagePipe} 单测:append/readRange 往返、index 暴露、wipe-on-open、错位 fail-fast、
 * releaseBelow 档位节流、deletableFiles 纯函数删除规则、close 幂等。引擎
 * {@code vb-stream-engine} 的 {@code MessagePipeTest}(157 行)前五用例 1:1 翻译,
 * 后三用例为本模块补钉(纯函数直测/幂等/递归清残留)。
 * 夹具:{@code @TempDir} 独立目录,每用例新建管道(CQ 的 mmap 需 --add-opens,根 pom
 * surefire argLine 已带)。真库行为与本类无关——管道是纯本地磁盘设施。
 */
class MessagePipeTest {

    @TempDir
    Path dir;

    /**
     * append → readRange 往返且逐条回调真实 CQ index(seq ≡ CQ index 的核心契约):三条消息
     * (I/I/C,载荷形态覆盖数据消息与控制消息)按 index 升序读回,回调首参恰为各自 append 的
     * 返回值,payload 为与队列内存不共享的副本且内容保真。
     * 关键步骤:append 三条记录各自返回 index → readRange(i0, i2) 收集回调的 (idx, payload) →
     * 断言 index 序等于 append 返回序、载荷字节逐条对应。
     * 边界:载荷最小化(1~2 字节);断言只依赖 append 返回值,不假设 index 从 0 起。
     */
    @Test
    void readRangeExposesRealIndexPerMessage() throws IOException {
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            long i0 = pipe.append(new byte[]{'I', 1});
            long i1 = pipe.append(new byte[]{'I', 2});
            long i2 = pipe.append(new byte[]{'C'});
            List<Long> indexes = new ArrayList<>();
            List<byte[]> payloads = new ArrayList<>();
            pipe.readRange(i0, i2, (idx, payload) -> {
                indexes.add(idx);
                payloads.add(payload);
            });
            assertEquals(List.of(i0, i1, i2), indexes);       // index 单调且即真实 CQ index
            assertEquals((byte) 1, payloads.get(0)[1]);       // payload 为副本、内容保真
            assertEquals((byte) 2, payloads.get(1)[1]);
            assertEquals((byte) 'C', payloads.get(2)[0]);
        }
    }

    /**
     * 单条闭区间 [second..second]:区间端点重合时恰好回调一次且 index 等于该条自身——
     * 回放"一段一条"桶的退化情形。
     * 关键步骤:append 三条(B/I/C)→ 只取第三条的 index 同时作首尾端点回读 → 断言单次回调。
     * 边界:前两条在区间之外,必须被 moveToIndex 定位与区间过滤跳过;端点重合不得使区间落空。
     */
    @Test
    void singleMessageRangeYieldsExactlyOne() throws IOException {
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            long first = pipe.append(new byte[]{'B'});
            pipe.append(new byte[]{'I', 9});
            long second = pipe.append(new byte[]{'C'});
            List<Long> seen = new ArrayList<>();
            pipe.readRange(second, second, (idx, p) -> seen.add(idx));
            assertEquals(List.of(second), seen);
        }
    }

    /**
     * wipe-on-open:同目录第二次构造 MessagePipe 后旧实例写入的内容不可见——构造先清空目录
     * 再建队列(瞬态工作区语义:真源是复制槽,管道不跨重启续用)。
     * 关键步骤:first 管道写入一条并 close → second 管道同目录重开 → readRange(0, 100)
     * 期望读不到任何消息。
     * 边界:起点 index 0 在新队列中不存在(cycle 0 无滚动文件),读到队尾即空手而归,不抛异常。
     */
    @Test
    void wipeOnOpenClearsStaleFiles() throws IOException {
        try (MessagePipe first = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            first.append(new byte[]{'B'});
        }
        try (MessagePipe second = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            List<byte[]> seen = new ArrayList<>();
            second.readRange(0, 100, (idx, p) -> seen.add(p));
            assertEquals(List.of(), seen);        // 旧数据整体抹掉,空手而归不抛
        }
    }

    /**
     * 区间起点错位 fail-fast:readRange 以从未存在的 index 起步、且落点仍落在区间内时抛
     * IllegalStateException——落点越过了区间起点,说明起点已被删除或从未存在,宁可报错也
     * 不猜(低水位失效时的最后防线)。
     * 关键步骤:append 两条拿真实 index → 以 first-1(低于队列首条,从未存在)为起点、
     * first+1 为终点回读 → moveToIndex 落到队列首条 first,first ∈ (first-1, first+1]
     * 且 ≠ firstIndex,触发错位守卫,断言 ISE。
     * 边界:起点必须取真实 index 的相对值——真实 CQ index 含 cycle 位(~1e17 量级),字面
     * 小 index(如 9999)作起点时首个数据文档必然先命中"越过区间上界"分支静默返回(探针
     * 实证),覆盖不到错位路径。
     */
    @Test
    void mismatchedStartIndexFailsFast() throws IOException {
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            long first = pipe.append(new byte[]{'B'});
            pipe.append(new byte[]{'C'});
            assertThrows(IllegalStateException.class,
                    () -> pipe.readRange(first - 1, first + 1, (idx, p) -> { }));
        }
    }

    /**
     * releaseBelow 节流(引擎 1.7.1 Task 3 修复):needed cycle 档位未推进时跳过目录扫描——
     * 同档位内可删集不可能变化(滚动文件只随 append 前沿出现在当前/未来档位,不回填旧档名),
     * 删档检查延后到下一次档位推进,删除语义仍为惰性(残留只占磁盘不影响正确性)。
     * 关键步骤:建管道 append 一条取真实 index,锚定当前档位 c = toCycle(index) → 注入陈旧档名
     * (2020-01-01)文件 → releaseBelow(档 c) 首调即扫并删除 → 再注入一个陈旧文件 → 同档 c
     * 二调被节流跳过(文件仍在,删档延后)→ 档 c+1 三调补扫(文件被删)。
     * 边界:c+1 的入参 index 用 {@code toIndex(c+1, 0)} 构造({@code toCycle(toIndex(c,0)) == c}
     * 恒等还原档号,不假设 index 的位布局);真实数据文件(档 c)全程保留——c 不满足
     * {@code < (c+1)-1},证明节流没有变成"就近清空"。
     */
    @Test
    void releaseBelowSkipsScanUntilNeededCycleAdvances() throws IOException {
        LegacyRollCycles cycle = LegacyRollCycles.MINUTELY;
        try (MessagePipe pipe = new MessagePipe(dir, cycle)) {
            long realIndex = pipe.append(new byte[]{'B'});
            int needed = cycle.toCycle(realIndex);
            Path stale = dir.resolve("20200101-0000.cq4");
            Files.createFile(stale);
            pipe.releaseBelow(realIndex);                 // 首调即扫:陈旧档删除
            assertEquals(1, countRollFiles(dir),
                    () -> "首调应删除唯一陈旧文件、只留真实数据文件: " + dir);
            Files.createFile(stale);                      // 同档位内再次出现陈旧文件
            pipe.releaseBelow(realIndex);                 // 同档二调:节流跳过,删档延后
            assertTrue(Files.exists(stale),
                    () -> "needed cycle 未推进时应跳过扫描(删档延后): " + stale);
            pipe.releaseBelow(cycle.toIndex(needed + 1, 0L));   // 档位推进:补扫补删
            assertTrue(Files.notExists(stale),
                    () -> "档位推进后应补删同档位期间出现的陈旧文件: " + stale);
            assertTrue(countRollFiles(dir) >= 1,
                    () -> "队列真实数据滚动文件应全程保留: " + dir);
        }
    }

    /**
     * deletableFiles 纯函数直测(静态方法零 CQ 依赖,注入假文件名验证删除规则):cycle 号小于
     * {@code neededCycle - 1} 才入选——needed 与 needed-1 两档保留(上一档里可能还有低水位
     * 之前的在途条目,删除永远保守);解析失败的文件名换算 MAX_VALUE 永不入选(外来文件宁可
     * 漏删不可错删);队列元数据 {@code metadata.cq4t} 后缀不同,根本不进候选。
     * 关键步骤:注入四个 .cq4(远古档/needed-1 档/needed 档/不可解析名)与一个 .cq4t →
     * 以"20260101-0000"换算的 cycle 作 neededCycle 调 deletableFiles → 断言入选集恰为远古档
     * 一个文件,其余四个全部保留在盘上。
     * 边界:档号换算复用被测公式同源的毫秒÷周期时长(minuteCycle 辅助),不假设位布局。
     */
    @Test
    void deletableFilesKeepsNeededAndPreviousCycleOnly() throws IOException {
        LegacyRollCycles rc = LegacyRollCycles.MINUTELY;
        touch("20200101-0000.cq4");                       // 远古档:唯一可删
        touch("20251231-2359.cq4");                       // needed-1 档:保留
        touch("20260101-0000.cq4");                       // needed 档:保留
        touch("garbage.cq4");                             // 解析失败:永不入选
        touch("metadata.cq4t");                           // 后缀不同:不是候选
        long needed = minuteCycle(rc, "20260101-0000");
        List<Path> doomed = MessagePipe.deletableFiles(rc, dir, needed);
        assertEquals(List.of(dir.resolve("20200101-0000.cq4")), doomed,
                "只有比 needed-1 还老的档位入选删除");
        for (String kept : new String[]{ "20251231-2359.cq4", "20260101-0000.cq4", "garbage.cq4", "metadata.cq4t" }) {
            assertTrue(Files.exists(dir.resolve(kept)), "应保留(漏删不错删): " + kept);
        }
    }

    /**
     * close 幂等:第二次 close 不抛任何异常——三步(tailer→appender→queue)各自 WARN 吸收
     * RuntimeException,重复关闭已关资源至多触发被吸收的异常,不得向调用方泄漏
     * (shutdown hook 的排干路径可能再次 close 兜底)。
     * 关键步骤:建管道写入一条 → close 两次 → 第二次调用正常返回(断言即"未抛")。
     * 边界:close 后不再 append/readRange(那是调用方违约,不在本用例覆盖面)。
     */
    @Test
    void closeIsIdempotent() throws IOException {
        MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY);
        pipe.append(new byte[]{'B'});
        pipe.close();
        pipe.close();                                      // 第二次:WARN 吸收,不上抛
    }

    /**
     * wipe-on-open 递归清残留:目录里非队列自有的外来内容(嵌套目录树、孤立文件)也一并
     * 清掉——wipeDirectory 是 Files.walk 逆序递归删除,不只认 CQ 文件名;残留的陈旧 index
     * 会让回读错位,所以"必须干净"。
     * 关键步骤:预置 sub/deeper/stale-index 嵌套树与孤立 orphan.cq4 → 构造 MessagePipe →
     * 断言两处残留消失 → append/readRange 往返证明清空后队列可用。
     * 边界:嵌套目录必须子项先于父项删(逆序),任一删不掉应 UncheckedIOException fail-fast
     * (正向路径此处覆盖,失败路径属 OS 占用场景,归引擎 it 的 gc 重试模式,本模块不测)。
     */
    @Test
    void wipeOnOpenRemovesForeignResidueRecursively() throws IOException {
        Path nested = Files.createDirectories(dir.resolve("sub/deeper"));
        Files.writeString(nested.resolve("stale-index"), "stale");
        Files.writeString(dir.resolve("orphan.cq4"), "not a queue file");
        try (MessagePipe pipe = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            assertTrue(Files.notExists(dir.resolve("sub")), "嵌套残留目录树应被递归清空");
            assertTrue(Files.notExists(dir.resolve("orphan.cq4")), "孤立残留文件应被清空");
            long idx = pipe.append(new byte[]{'B'});
            List<byte[]> seen = new ArrayList<>();
            pipe.readRange(idx, idx, (i, p) -> seen.add(p));
            assertEquals(1, seen.size(), "清空重建后的队列应可正常往返");
        }
    }

    /**
     * 在临时目录下创建空文件(注入假滚动文件名)。
     *
     * @param name 文件名(含假档名与后缀)
     */
    private void touch(String name) throws IOException {
        Files.createFile(dir.resolve(name));
    }

    /**
     * 按被测公式同源的方式换算档号:时间戳 → UTC epochMillis →
     * {@code (millis - defaultEpoch) / lengthInMillis}(与 {@code MessagePipe.parseCycle} 的
     * 换算一致,MINUTELY 下即分钟数)——为 deletableFiles 纯函数用例构造 neededCycle,
     * 不假设 index 的位布局。
     *
     * @param rc    滚动周期(取 defaultEpoch/lengthInMillis)
     * @param stamp yyyyMMdd-HHmm 形态的 UTC 时间戳(与滚动文件名同格式)
     * @return 该时间戳所在 cycle 号
     */
    private static long minuteCycle(LegacyRollCycles rc, String stamp) {
        LocalDateTime t = LocalDateTime.parse(stamp, DateTimeFormatter.ofPattern("yyyyMMdd-HHmm", Locale.ROOT));
        return (t.toInstant(ZoneOffset.UTC).toEpochMilli() - rc.defaultEpoch()) / rc.lengthInMillis();
    }

    /**
     * 统计管道目录下的滚动数据文件数(.cq4 后缀;metadata.cq4t 后缀不同天然排除)——
     * 节流用例的两面断言辅助:删除精确落在低水位之下,真实数据文件不计入"被删"。
     *
     * @param pipeDir 管道目录
     * @return 滚动数据文件(.cq4)个数
     */
    private static int countRollFiles(Path pipeDir) throws IOException {
        try (java.util.stream.Stream<Path> entries = Files.list(pipeDir)) {
            return (int) entries
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".cq4"))
                    .count();
        }
    }
}
