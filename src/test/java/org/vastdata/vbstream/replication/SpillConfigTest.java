package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SpillConfig 单测：默认值、系统属性覆盖、禁用逃生门与未知滚动周期 fail-fast。
 * 注：MINUTELY/HOURLY 断言用 LegacyRollCycles——chronicle-queue 2026.6 已把这三个枚举值
 * 从 RollCycles 迁入 LegacyRollCycles（计划文本中的 RollCycles.MINUTELY 写法不可编译）。
 */
class SpillConfigTest {

    /** 测试可能注入 vb.spill.* 系统属性，逐条清理防止串扰后续用例（含其他测试类的默认值断言）。 */
    @AfterEach
    void cleanupSystemProperties() {
        System.getProperties().stringPropertyNames().stream()
                .filter(key -> key.startsWith("vb.spill."))
                .forEach(System::clearProperty);
    }

    /** 未设任何属性时三个分量必须落在 spec 默认值上：64MiB 阈值、工作目录 spill-queue、MINUTELY 滚动。 */
    @Test
    void defaultsMatchSpec() {
        SpillConfig c = SpillConfig.fromSystemProperties();
        assertEquals(64L * 1024 * 1024, c.thresholdBytes());
        assertEquals(Path.of("spill-queue"), c.dir());
        assertEquals(LegacyRollCycles.MINUTELY, c.rollCycle());
        assertTrue(c.spillEnabled());
    }

    /**
     * 覆盖路径：thresholdBytes=0 时 spillEnabled() 必须为 false（纯内存逃生门），
     * rollCycle 属性 "hourly" 以小写形式解析为 LegacyRollCycles.HOURLY（枚举名大小写宽容）。
     */
    @Test
    void overridesAndDisabled() {
        System.setProperty("vb.spill.thresholdBytes", "0");
        System.setProperty("vb.spill.dir", "/tmp/x");
        System.setProperty("vb.spill.rollCycle", "hourly");
        SpillConfig c = SpillConfig.fromSystemProperties();
        assertFalse(c.spillEnabled());          // ≤0 = 纯内存逃生门
        assertEquals(LegacyRollCycles.HOURLY, c.rollCycle());  // 大小写宽容
    }

    /** rollCycle 属性给出未知枚举名时必须 fail-fast 抛 IllegalArgumentException（而非静默回落默认）。 */
    @Test
    void unknownRollCycleFailsFast() {
        System.setProperty("vb.spill.rollCycle", "nope");
        assertThrows(IllegalArgumentException.class, SpillConfig::fromSystemProperties);
    }
}
