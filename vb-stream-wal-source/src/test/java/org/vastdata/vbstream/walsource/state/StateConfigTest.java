package org.vastdata.vbstream.walsource.state;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link StateConfig} 三键解析与校验的离线单测（零 Docker，秒级）——缺省值、dir 禁用
 * 形态（缺键/空白串）、显式覆盖与非法值 fail-fast。
 */
class StateConfigTest {

    /**
     * 缺省解析：无任何键时 dir=null（禁用）、节拍取默认 30s/1000——单一默认值来源
     * 不在调用方复刻。
     */
    @Test
    void emptyPropertiesDisableStateAndDefaultCadence() {
        StateConfig cfg = StateConfig.fromProperties(new Properties());
        assertNull(cfg.dir(), "缺 dir 键应为禁用形态（dir=null）");
        assertFalse(cfg.enabled(), "dir=null 即禁用");
        assertEquals(StateConfig.DEFAULT_INTERVAL_MS, cfg.intervalMs());
        assertEquals(StateConfig.DEFAULT_EVENTS_THRESHOLD, cfg.eventsThreshold());
    }

    /**
     * dir 键的禁用双形态：空白串与缺键等价（显式空串视为禁用而非 Path.of("")）。
     */
    @Test
    void blankDirDisablesState() {
        Properties p = new Properties();
        p.setProperty(StateConfig.KEY_DIR, "   ");
        assertNull(StateConfig.fromProperties(p).dir(), "空白 dir 应判禁用");
    }

    /**
     * 三键显式覆盖全量生效（fromProperties 解析面 + enabled 判定）。
     */
    @Test
    void explicitKeysOverrideDefaults() {
        Properties p = new Properties();
        p.setProperty(StateConfig.KEY_DIR, "/tmp/wal-state");
        p.setProperty(StateConfig.KEY_INTERVAL_MS, "5000");
        p.setProperty(StateConfig.KEY_EVENTS, "37");
        StateConfig cfg = StateConfig.fromProperties(p);
        assertEquals(Path.of("/tmp/wal-state"), cfg.dir());
        assertTrue(cfg.enabled());
        assertEquals(5_000L, cfg.intervalMs());
        assertEquals(37L, cfg.eventsThreshold());
        assertNotNull(StateConfig.enabled(Path.of("/tmp/wal-state")), "工厂档应可用");
    }

    /**
     * 非数字节拍值 fail-fast（NumberFormatException——与 WalSource 端口键同语义，
     * 不静默跑错节拍）。
     */
    @Test
    void nonNumericCadenceFailsFast() {
        Properties p = new Properties();
        p.setProperty(StateConfig.KEY_INTERVAL_MS, "abc");
        assertThrows(NumberFormatException.class, () -> StateConfig.fromProperties(p));
    }

    /**
     * 非正节拍构造期 IAE（&le;0 的节拍永触发或永不触发，属配置面错误）。
     */
    @Test
    void nonPositiveCadenceRejected() {
        assertThrows(IllegalArgumentException.class, () -> new StateConfig(Path.of("/tmp/s"), 0, 100));
        assertThrows(IllegalArgumentException.class, () -> new StateConfig(Path.of("/tmp/s"), 1000, -1));
    }
}
