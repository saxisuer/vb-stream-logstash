package org.vastdata.vbstream.walsource.api;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.changes.RenderOptions;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link WalSource} 配置面新增两键（{@code vb.wal.tables} 白名单 / {@code vb.wal.dml}
 * DML 输出开关）的离线解析测试（Task 7）——纯构造期行为，不建连接：白名单逗号切分与
 * 空白容忍、dml 缺省 true、严格布尔解析（非法值构造期 IAE fail-fast）、无 listener 的
 * v1 单参构造器回落纯 catalog 形态。离线回放档两键（offline-replay Task 4）：offline.dir
 * 缺省 null / 显式存档、offline.endLsn 非法 LSN 文本构造期 IAE、无 dir 时 startOffline
 * 启动期 ISE（消息附键名）。
 */
class WalSourceConfigTest {

    /** 词典外补充键的最小配置载体（其余键走默认）。 */
    private static Properties props(String key, String value) {
        Properties cfg = new Properties();
        cfg.setProperty(key, value);
        return cfg;
    }

    /** 白名单解析：逗号切分、逐项 trim、空项跳过。 */
    @Test
    void tablesWhitelistSplitsTrimsAndSkipsEmptyEntries() {
        WalSource source = new WalSource(props("vb.wal.tables", "public.a,  public.b ,, public.c"), null);
        assertEquals(Set.of("public.a", "public.b", "public.c"), source.tables());
    }

    /** 白名单缺省：空集 = 用户表全放行。 */
    @Test
    void tablesWhitelistDefaultsToEmptyForAllTables() {
        WalSource source = new WalSource(new Properties(), null);
        assertTrue(source.tables().isEmpty(), "缺省空集 = 全放行（TableFilter 语义）");
    }

    /** dml 缺省 true（有 listener 即开 DML 面）。 */
    @Test
    void dmlDefaultsTrueWithListener() {
        assertTrue(new WalSource(new Properties(), noop()).dmlEnabled());
        assertTrue(new WalSource(props("vb.wal.dml", "true"), noop()).dmlEnabled());
        assertFalse(new WalSource(props("vb.wal.dml", "false"), noop()).dmlEnabled());
    }

    /** dml 非布尔值构造期 IAE（fail-fast 优于静默当 false 吞掉 DML 面）。 */
    @Test
    void dmlNonBooleanValueFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.dml", "yes"), null));
    }

    /** offline.dir 键解析（缺省 null——startOffline 时 ISE；显式设置即存档）。 */
    @Test
    void offlineDirKeyParsed() {
        WalSource s = new WalSource(props(WalSource.KEY_OFFLINE_DIR, "D:/wal"));
        assertEquals(Path.of("D:/wal"), s.offlineDir());
        assertNull(new WalSource(new Properties()).offlineDir());
    }

    /** offline.endLsn 非法十六进制 LSN 文本构造期 IAE（fail-fast 优于运行期跳错位）。 */
    @Test
    void offlineEndLsnRejectsGarbage() {
        Properties p = props(WalSource.KEY_OFFLINE_END_LSN, "not-an-lsn");
        assertThrows(IllegalArgumentException.class, () -> new WalSource(p));
    }

    /** mode=offline 而 offline.dir 缺失——startOffline 启动期 ISE（spec §5）。 */
    @Test
    void startOfflineWithoutDirFails() {
        WalSource s = new WalSource(new Properties());   // 无 offline.dir
        IllegalStateException e = assertThrows(IllegalStateException.class, s::startOffline);
        assertTrue(e.getMessage().contains("vb.wal.offline.dir"));
    }

    /** 全空操作的 listener（配置面测试只关心装配开关，不消费事件）。 */
    private static ChangeOutputListener noop() {
        return new ChangeOutputListener() {
            @Override
            public void onBegin(BatchBegin begin) {
            }

            @Override
            public void onRow(RowChange row) {
            }

            @Override
            public void onEnd(BatchEnd end) {
            }

            @Override
            public void onAborted(BatchAborted aborted) {
            }
        };
    }

    /** v1 单参构造器：无 listener 即纯 catalog 形态（DML 面恒关，配置键不复活）。 */
    @Test
    void legacyConstructorWithoutListenerStaysPureCatalogForm() {
        WalSource source = new WalSource(props("vb.wal.dml", "true"));
        assertFalse(source.dmlEnabled(), "无输出 listener 时 DML 面不装配");
    }

    /**
     * 渲染面四键（{@code vb.wal.render.*}，2026-10-17 配置化）：门面构造期解析为
     * {@link RenderOptions}（观测 getter 供宿主核对），缺省档逐字段等于 {@link RenderOptions#PG}
     * ——对拍锚不动。
     */
    @Test
    void renderOptionsParsedWithPgDefaults() {
        assertEquals(RenderOptions.PG, new WalSource(new Properties()).renderOptions(),
                "四键全缺 = PG 缺省档");
    }

    /** 渲染键全开档解析：plain 浮点 + default 内建 date 格式 + 阈值 0（不截断）。 */
    @Test
    void renderOptionsFullCustomProfileParsed() {
        Properties cfg = new Properties();
        cfg.setProperty("vb.wal.render.float.style", "plain");
        cfg.setProperty("vb.wal.render.date.format", "default");
        cfg.setProperty("vb.wal.render.truncate", "0");
        RenderOptions opts = new WalSource(cfg).renderOptions();
        assertEquals(RenderOptions.FloatStyle.PLAIN, opts.floatStyle());
        assertEquals("2026-10-07 12:34:56",
                opts.dateFormat().format(LocalDateTime.of(2026, 10, 7, 12, 34, 56)),
                "default 内建档 = yyyy-MM-dd HH:mm:ss");
        assertEquals(0, opts.truncateLimit(), "0 = 不截断");
    }

    /** 渲染键非法值：门面构造期 IAE（与其余配置键同族 fail-fast，消息附键名）。 */
    @Test
    void renderOptionsInvalidValueFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.render.float.style", "sci")));
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.render.date.format", "not-a-pattern?")));
    }
}
