package org.vastdata.vbstream.walsource.receive;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link WalStreamMetrics} census 面的行为保持锚：键形 {@code "rmid/hex(info&0xF0)"}、
 * 分桶计数与快照输出在 census 预计算键表 + LongAdder 改造（2026-10-17 性能优化）前后
 * 逐字不变——Main 冒烟行 censusTop3 消费 {@code censusSnapshot()}，本测试是回归锚
 * 而非失败驱动。
 */
class WalStreamMetricsTest {

    /**
     * 构造最小合法 WalRecord（24B 空头形态）：blocks 空、main 空、raw 长度与 totLen
     * 一致——census 只读 rmid/info 两字段，其余字段取无害占位值。
     *
     * @param rmid 资源管理器 id
     * @param info 记录 info 字节
     * @return 可供 countRecord 消费的最小记录
     */
    private static WalRecord rec(int rmid, int info) {
        return new WalRecord(0, 24, 1, 0, rmid, info, List.of(), 24, 0, new byte[24]);
    }

    /**
     * census 键形与计数在预计算键表改造下不变：同 (rmid, 高 nibble) 两条记录落同键
     * 累加、不同高 nibble（0xA0 vs 0xB0）分桶；快照大小恰为出现过的形态数。
     */
    @Test
    void censusKeyFormatAndCountSurvivePrecomputedTable() {
        WalStreamMetrics m = new WalStreamMetrics();
        WalRecord r = rec(10, 0xA0);
        m.countRecord(r);
        m.countRecord(r);
        m.countRecord(rec(10, 0xB0));
        Map<String, Long> snap = m.censusSnapshot();
        assertEquals(2L, snap.get("10/a0"));
        assertEquals(1L, snap.get("10/b0"));
        assertEquals(2, snap.size());
    }

    /**
     * 键表懒填充的重复命中路径：同形态第三条记录不再拼接仍落同键（首次已填充
     * censusKeys 槽位），计数为 3；同时验证 rmid 高值域（u8 上界 255）槽位寻址正常。
     */
    @Test
    void censusReusesLazyFilledKeyAcrossHits() {
        WalStreamMetrics m = new WalStreamMetrics();
        for (int i = 0; i < 3; i++) {
            m.countRecord(rec(255, 0x00));
        }
        Map<String, Long> snap = m.censusSnapshot();
        assertEquals(3L, snap.get("255/0"));
        assertEquals(1, snap.size());
    }
}
