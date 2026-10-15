package org.vastdata.vbstream.walsource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.api.WalSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 配置文件层（{@link WalConfigFile}）与 Main 三层合并（文件 → {@code -Dvb.wal.*} → 门面
 * 默认）的离线单测：白名单过滤/空串语义/fail-fast 面/vb.config 替换与保留键语义，以及
 * {@code Main.collectConfig} 的覆盖次序。全部走临时目录与临时系统属性（{@link AfterEach}
 * 清理防跨用例泄漏）。
 */
class WalConfigFileTest {

    /** 每用例后清掉测试自设的系统属性（vb.config 与 vb.wal.* 采样键）。 */
    @AfterEach
    void clearSystemProps() {
        System.clearProperty(WalConfigFile.CONFIG_FILE_KEY);
        System.clearProperty(WalSource.KEY_HOST);
        System.clearProperty(WalSource.KEY_PORT);
    }

    /**
     * 白名单内的键进结果、白名单外键忽略不抛：文件是 wal-source 专属命名空间（键面封闭），
     * 未知键按拼写错 WARN 吞掉——断言面为结果集内容（WARN 是日志面不在单测断言范围）。
     */
    @Test
    void loadFileKeepsWhitelistedKeysAndDropsUnknown(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("walconfig.properties");
        Files.writeString(file, """
                vb.wal.port=15700
                vb.wal.og.source=direct
                vb.wal.unknown.key=x
                totally.alien=y
                """);
        Properties props = WalConfigFile.loadFile(file);
        assertEquals("15700", props.getProperty(WalSource.KEY_PORT));
        assertEquals("direct", props.getProperty(WalSource.KEY_OG_SOURCE));
        assertEquals(2, props.size(), "白名单外两键应被忽略");
    }

    /**
     * 文件层空串视为未设：空值键不进结果（留给 -D 或门面默认值）——与 -D 空串语义
     * 对齐，避免"空串覆盖默认值"的歧义面。
     */
    @Test
    void loadFileTreatsBlankValueAsUnset(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("walconfig.properties");
        Files.writeString(file, "vb.wal.host=\n");
        Properties props = WalConfigFile.loadFile(file);
        assertTrue(props.isEmpty(), "空串值的键不应进结果");
    }

    /**
     * 文件不存在/格式损坏 fail-fast 抛 ISE：vb.config 或默认名指向的文件读不出来是
     * 确定性错误（路径错/文件坏），静默回落纯 -D 会让配置形同虚设——消息须含路径。
     */
    @Test
    void loadFileFailsFastOnMissingOrMalformed(@TempDir Path dir) throws IOException {
        Path missing = dir.resolve("nope.properties");
        IllegalStateException e1 = assertThrows(IllegalStateException.class,
                () -> WalConfigFile.loadFile(missing));
        assertTrue(e1.getMessage().contains("nope.properties"), "消息含路径便于归因");

        Path malformed = dir.resolve("broken.properties");
        // 损坏的 \\u 转义是 Properties.load 真会抛的内容面错误（IllegalArgumentException）
        // ——行尾续行符残缺等多数"看似坏"内容它均宽容吞下，构不成失败面
        Files.writeString(malformed, "vb.wal.host=\\uzzzz\n");
        assertThrows(IllegalStateException.class, () -> WalConfigFile.loadFile(malformed));
    }

    /**
     * 默认名缺失走空集（纯 -D 形态，行为与文件支持前一致）；默认名存在则被读取。
     */
    @Test
    void loadBaseFallsBackToEmptyWhenDefaultMissing(@TempDir Path dir) throws IOException {
        assertTrue(WalConfigFile.loadBase(dir.resolve("walconfig.properties")).isEmpty());

        Path def = dir.resolve("walconfig.properties");
        Files.writeString(def, "vb.wal.port=15700\n");
        assertEquals("15700",
                WalConfigFile.loadBase(def).getProperty(WalSource.KEY_PORT));
    }

    /**
     * vb.config 显式指定时整份替换默认查找：读显式文件、无视工作目录默认名；vb.config
     * 是保留键，不进门面配置（白名单天然排除）。
     */
    @Test
    void explicitConfigPathReplacesDefaultAndStaysReserved(@TempDir Path dir) throws IOException {
        Path def = dir.resolve("walconfig.properties");
        Files.writeString(def, "vb.wal.port=11111\n");
        Path explicit = dir.resolve("my.properties");
        Files.writeString(explicit, "vb.wal.port=22222\n");
        System.setProperty(WalConfigFile.CONFIG_FILE_KEY, explicit.toString());

        Properties props = WalConfigFile.loadBase(def);
        assertEquals("22222", props.getProperty(WalSource.KEY_PORT), "显式文件值胜过默认名");
        assertNull(props.getProperty(WalConfigFile.CONFIG_FILE_KEY), "vb.config 不进门面配置");
    }

    /**
     * vb.config 指向不存在的文件 fail-fast 抛 ISE：显式路径错误不静默回落（对齐
     * vb-stream-reader 先例——明确的错误比悄悄换源更可排查）。
     */
    @Test
    void explicitConfigPathMissingFailsFast(@TempDir Path dir) {
        System.setProperty(WalConfigFile.CONFIG_FILE_KEY, dir.resolve("nope.properties").toString());
        assertThrows(IllegalStateException.class, () -> WalConfigFile.loadBase(dir.resolve("walconfig.properties")));
    }

    /**
     * Main 三层合并次序（collectConfig 参数化注入文件层）：文件值打底 → -D 同键覆盖 →
     * 无 -D 的键保留文件值；-D 空串视为未设（不覆盖文件值——空串与缺键同归"未指定"）。
     */
    @Test
    void collectConfigMergesFileBaseThenSysPropOverlays() {
        Properties fileBase = new Properties();
        fileBase.setProperty(WalSource.KEY_HOST, "file-host");
        fileBase.setProperty(WalSource.KEY_PORT, "15700");

        System.setProperty(WalSource.KEY_HOST, "sysprop-host");
        System.setProperty(WalSource.KEY_PORT, "");   // 空串=未设——文件值应保留

        Properties cfg = Main.collectConfig(fileBase);
        assertEquals("sysprop-host", cfg.getProperty(WalSource.KEY_HOST), "-D 覆盖文件同键");
        assertEquals("15700", cfg.getProperty(WalSource.KEY_PORT), "-D 空串不覆盖文件值");
        assertFalse(cfg.containsKey(WalConfigFile.CONFIG_FILE_KEY));
    }

    /**
     * 离线回放两键（offline.dir/offline.endLsn）进白名单——文件层可配离线档的目录与
     * 上界；{@code vb.wal.mode} 是 Main 层直读分派键（计划裁定不进门面配置/白名单），
     * 文件里写了也按白名单外键忽略——离线档切换只走 {@code -Dvb.wal.mode=offline}。
     */
    @Test
    void loadFileKeepsOfflineReplayKeysButModeStaysExcluded(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("walconfig.properties");
        Files.writeString(file, """
                vb.wal.offline.dir=D:/wal-archive
                vb.wal.offline.endLsn=2/071BF288
                vb.wal.mode=offline
                """);
        Properties props = WalConfigFile.loadFile(file);
        assertEquals("D:/wal-archive", props.getProperty(WalSource.KEY_OFFLINE_DIR));
        assertEquals("2/071BF288", props.getProperty(WalSource.KEY_OFFLINE_END_LSN));
        assertNull(props.getProperty("vb.wal.mode"), "vb.wal.mode 是 Main 层分派键，不进门面白名单");
    }
}
