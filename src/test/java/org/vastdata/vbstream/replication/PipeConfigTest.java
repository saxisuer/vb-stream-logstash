package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** PipeConfig 配置面单测：默认值、系统属性覆盖、非法 rollCycle 启动期 fail-fast。逐用例设置系统属性并在 finally 清理，防用例间串扰。 */
class PipeConfigTest {

    /** 设置系统属性并在测试结束后恢复原值（缺失则移除），保证用例隔离。 */
    private static void withProp(String key, String value, Runnable body) {
        String old = System.getProperty(key);
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
        try {
            body.run();
        } finally {
            if (old == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, old);
            }
        }
    }

    @Test
    void defaultsMatchSpec() {
        PipeConfig cfg = PipeConfig.fromSystemProperties();
        assertEquals(Path.of("pipe-queue"), cfg.dir());
        assertEquals(LegacyRollCycles.MINUTELY, cfg.rollCycle());
    }

    @Test
    void overridesBothProperties() {
        withProp("vb.pipe.dir", "my-pipe", () ->
                withProp("vb.pipe.rollCycle", "hourly", () -> {
                    PipeConfig cfg = PipeConfig.fromSystemProperties();
                    assertEquals(Path.of("my-pipe"), cfg.dir());
                    assertEquals(LegacyRollCycles.HOURLY, cfg.rollCycle());
                }));
    }

    @Test
    void unknownRollCycleFailsFastWithUsableValues() {
        withProp("vb.pipe.rollCycle", "NOPE", () ->
                assertThrows(IllegalArgumentException.class, PipeConfig::fromSystemProperties));
    }
}
