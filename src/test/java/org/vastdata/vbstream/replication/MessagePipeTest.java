package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** MessagePipe 单测：append/readRange 往返、index 暴露、wipe-on-open、错位 fail-fast。夹具：@TempDir 独立目录，每用例新建管道。 */
class MessagePipeTest {

    @TempDir
    Path dir;

    /**
     * append → readRange 往返且逐条回调真实 CQ index（seq ≡ CQ index 的核心契约）：三条消息
     * （I/I/C，载荷形态覆盖数据消息与控制消息）按 index 升序读回，回调首参恰为各自 append 的
     * 返回值，payload 为与队列内存不共享的副本且内容保真。
     * 关键步骤：append 三条记录各自返回 index → readRange(i0, i2) 收集回调的 (idx, payload) →
     * 断言 index 序等于 append 返回序、载荷字节逐条对应。
     * 边界：载荷最小化（1~2 字节）；断言只依赖 append 返回值，不假设 index 从 0 起。
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
     * 单条闭区间 [second..second]：区间端点重合时恰好回调一次且 index 等于该条自身——
     * 回放"一段一条"桶的退化情形。
     * 关键步骤：append 三条（B/I/C）→ 只取第三条的 index 同时作首尾端点回读 → 断言单次回调。
     * 边界：前两条在区间之外，必须被 moveToIndex 定位与区间过滤跳过；端点重合不得使区间落空。
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
     * wipe-on-open：同目录第二次构造 MessagePipe 后旧实例写入的内容不可见——构造先清空目录
     * 再建队列（瞬态工作区语义：真源是复制槽，管道不跨重启续用）。
     * 关键步骤：first 管道写入一条并 close → second 管道同目录重开 → readRange(0, 100)
     * 期望读不到任何消息。
     * 边界：起点 index 0 在新队列中不存在（cycle 0 无滚动文件），读到队尾即空手而归，不抛异常。
     */
    @Test
    void wipeOnOpenClearsStaleFiles() throws IOException {
        try (MessagePipe first = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            first.append(new byte[]{'B'});
        }
        try (MessagePipe second = new MessagePipe(dir, LegacyRollCycles.MINUTELY)) {
            List<byte[]> seen = new ArrayList<>();
            second.readRange(0, 100, (idx, p) -> seen.add(p));
            assertEquals(List.of(), seen);        // 旧数据整体抹掉，空手而归不抛
        }
    }

    /**
     * 区间起点错位 fail-fast：readRange 以从未存在的 index 起步、且落点仍落在区间内时抛
     * IllegalStateException——落点越过了区间起点，说明起点已被删除或从未存在，宁可报错也
     * 不猜（低水位失效时的最后防线）。
     * 关键步骤：append 两条拿真实 index → 以 first-1（低于队列首条，从未存在）为起点、
     * first+1 为终点回读 → moveToIndex 落到队列首条 first，first ∈ (first-1, first+1]
     * 且 ≠ firstIndex，触发错位守卫，断言 ISE。
     * 边界：起点必须取真实 index 的相对值——真实 CQ index 含 cycle 位（~1e17 量级），字面
     * 小 index（如 9999）作起点时首个数据文档必然先命中"越过区间上界"分支静默返回（探针
     * 实证），覆盖不到错位路径。
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
}
