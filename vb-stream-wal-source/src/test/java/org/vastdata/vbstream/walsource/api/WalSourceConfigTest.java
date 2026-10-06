package org.vastdata.vbstream.walsource.api;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;

import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link WalSource} 配置面新增两键（{@code vb.wal.tables} 白名单 / {@code vb.wal.dml}
 * DML 输出开关）的离线解析测试（Task 7）——纯构造期行为，不建连接：白名单逗号切分与
 * 空白容忍、dml 缺省 true、严格布尔解析（非法值构造期 IAE fail-fast）、无 listener 的
 * v1 单参构造器回落纯 catalog 形态。
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
}
