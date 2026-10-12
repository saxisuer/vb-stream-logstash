package org.vastdata.vbstream.walsource.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.state.StateStore;
import org.vastdata.vbstream.walsource.replay.CatalogStores;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 档启动期兼容模式护栏的离线测试（M2 Task 1）——OG 档 DML 面请求在场时探测
 * 库级 {@code pg_database.datcompatibility}，<strong>非 PG 档 ISE</strong>（附档位与
 * M3/M4 指引），两块断言面：
 * <ol>
 *   <li><b>档位判定面</b>（{@code WalSource.isPgCompatMode}，伪 JDBC 脚本化
 *       {@link #DATCOMPAT_QUERY}）：'PG' 白名单放行；'A'（Oracle 风格）/ 'B'（MySQL
 *       风格）/ 'C'（PG 兼容历史别名——og-plan 实测 5.0.0 仍接受建库且存储值与 'PG'
 *       相异，未在 M2 验证面）判定非 PG 档；探测不可判定（无行/null/空白值/探测
 *       SQLException）<strong>fail-open 放行</strong>——护栏是兼容性预检不是正确性
 *       锚，可判定的 A/B 档照 ISE，不可判定面静默砖死启动不如 WARN 放行。</li>
 *   <li><b>全路径 fail-fast 面</b>（伪 Driver 顶替 DriverManager，复用
 *       {@link OgWalSourceConfigTest} 的脚本化设施）：Main 形态（listener + dml 缺省
 *       true）在 'A' 档库上 start() 抛 ISE——消息含档位值与 M3/M4 指引，且护栏先于
 *       槽副作用（槽查询未被触达）；'PG' 档放行 + DML 面装配的断言面在
 *       {@link OgWalSourceConfigTest} 的 ⑤ 反转用例。</li>
 * </ol>
 * 依据（og-plan 容器 2026-10-12 实测）：postgres 缺省库 {@code datcompatibility='A'}、
 * {@code CREATE DATABASE m2pg DBCOMPATIBILITY 'PG'} 库为 {@code 'PG'}——PG 档的表示
 * 形态即 'PG' 单值（空/NULL 不是 PG 档表示，归不可判定面）。M1 全部 OG IT 存活依据：
 * 护栏仅在 DML 面请求在场（listener + vb.wal.dml=true）时探测——catalog-only 形态
 * （无 listener 或显式 dml=false）与兼容模式无关（M1 IT 即跑在 'A' 缺省库上全绿）。
 */
class OgCompatModeGuardTest {

    /** 兼容模式探测 SQL（与实现字面一致——FakeJdbc 按精确文本脚本化即形态锚）。 */
    static final String DATCOMPAT_QUERY =
            "SELECT datcompatibility FROM pg_database WHERE datname = current_database()";

    /** 检查点目录（全路径用例的续传种子——绕开引导的 OG 内部 SQL 适配）。 */
    @TempDir
    Path stateDir;

    /** OG external file 通道的落盘目录（空目录即纯等待，不产记录）。 */
    @TempDir
    Path walDir;

    /** 可判定档位面：'PG' 白名单放行；'A'/'B'/'C' 判定非 PG 档（C 是 PG 兼容历史别名但未在 M2 验证面）。 */
    @Test
    void definiteCompatModesResolveAgainstWhitelist() throws Exception {
        OgWalSourceConfigTest.FakeJdbc pg = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"PG"}).build();
        assertTrue(WalSource.isPgCompatMode(pg.connection()), "'PG' 档须放行（M2 支持面）");
        assertTrue(pg.executed.contains(DATCOMPAT_QUERY), "探测 SQL 形态锚（实现与测试字面一致）");
        for (String mode : new String[] {"A", "B", "C"}) {
            OgWalSourceConfigTest.FakeJdbc jdbc = OgWalSourceConfigTest.FakeJdbc.scripted()
                    .rows(DATCOMPAT_QUERY, new Object[] {mode}).build();
            assertFalse(WalSource.isPgCompatMode(jdbc.connection()),
                    "'" + mode + "' 档须判定非 PG（A=M3 / B=M4 / C 历史别名未验证）");
        }
    }

    /** 不可判定面 fail-open：无行 / null 值 / 空白值 / 探测 SQLException 均放行（护栏仅对可判定档位生效）。 */
    @Test
    void undeterminableProbeFailsOpen() throws Exception {
        OgWalSourceConfigTest.FakeJdbc noRow = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY).build();
        assertTrue(WalSource.isPgCompatMode(noRow.connection()), "无行（pg_database 查不到当前库）按不可判定放行");
        OgWalSourceConfigTest.FakeJdbc nullValue = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {null}).build();
        assertTrue(WalSource.isPgCompatMode(nullValue.connection()), "null 档位值按不可判定放行");
        OgWalSourceConfigTest.FakeJdbc blank = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {" "}).build();
        assertTrue(WalSource.isPgCompatMode(blank.connection()), "空白档位值按不可判定放行");
        assertTrue(WalSource.isPgCompatMode(probeThrowingConnection()),
                "探测 SQLException（权限/瞬断）按不可判定放行——fail-open 优于静默砖死启动");
    }

    /**
     * Main 形态（listener 在场 + vb.wal.dml 缺省 true）在 'A' 档库上 start() 启动期 ISE：
     * 消息含档位值与 M3/M4 指引，且护栏先于槽副作用——槽存在性查询未被触达（FakeJdbc
     * 未脚本面一经调用即 AssertionError，本用例显式断言槽查询缺席）。
     */
    @Test
    void ogStartWithDmlListenerOnNonPgCompatDatabaseFailsFastBeforeSlotSideEffects() throws Exception {
        long storedLsn = 0x10028L;
        new StateStore(stateDir, 5).checkpoint(new CatalogStores(), storedLsn);
        OgWalSourceConfigTest.FakeJdbc jdbc = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows("SELECT current_setting('server_version_num')", new Object[] {"90204"})
                .rows("SELECT version()", new Object[] {"(openGauss 5.0.0 build abc) compiled at 2023"})
                .rows(DATCOMPAT_QUERY, new Object[] {"A"})
                .build();
        Driver real = DriverManager.getDriver("jdbc:postgresql://localhost:5432/postgres");
        Driver fake = OgWalSourceConfigTest.proxyDriver(jdbc);
        DriverManager.deregisterDriver(real);
        DriverManager.registerDriver(fake);
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_OG_RX_MODE, "external");
        cfg.setProperty(WalSource.KEY_OG_RX_DIR, walDir.toString());
        cfg.setProperty(WalSource.KEY_STATE_DIR, stateDir.toString());
        ChangeOutputListener noop = new ChangeOutputListener() {
            @Override public void onBegin(ChangeOutputListener.BatchBegin begin) { }
            @Override public void onRow(ChangeOutputListener.RowChange row) { }
            @Override public void onEnd(ChangeOutputListener.BatchEnd end) { }
            @Override public void onAborted(ChangeOutputListener.BatchAborted aborted) { }
        };
        WalSource source = new WalSource(cfg, noop);
        try {
            IllegalStateException ise = assertThrows(IllegalStateException.class, source::start,
                    "'A' 档库 + DML 面请求在场须启动期 ISE（M2 仅支持 PG 兼容模式库）");
            assertTrue(ise.getMessage().contains("datcompatibility=A"),
                    "ISE 消息须含实际档位值: " + ise.getMessage());
            assertTrue(ise.getMessage().contains("M3") && ise.getMessage().contains("M4"),
                    "ISE 消息须附 M3（A 档）/ M4（B 档）里程碑指引: " + ise.getMessage());
            assertFalse(jdbc.executed.stream().anyMatch(sql -> sql.contains("pg_replication_slots")),
                    "护栏须先于槽副作用（槽查询未被触达）");
        } finally {
            source.close();
            DriverManager.deregisterDriver(fake);
            DriverManager.registerDriver(real);
        }
    }

    /**
     * 探测即抛 SQLException 的伪会话（fail-open 分支的输入面）：createStatement 返回的
     * 语句执行任意查询即抛 SQLException——探测面捕获后归不可判定。
     *
     * @return 连接代理
     */
    private static Connection probeThrowingConnection() {
        Statement throwing = (Statement) Proxy.newProxyInstance(OgCompatModeGuardTest.class.getClassLoader(),
                new Class<?>[] {Statement.class}, (p, method, args) -> {
                    throw new SQLException("probe unreachable (scripted)");
                });
        return (Connection) Proxy.newProxyInstance(OgCompatModeGuardTest.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (p, method, args) -> switch (method.getName()) {
                    case "createStatement" -> throwing;
                    case "close" -> null;
                    default -> throw new AssertionError("Connection." + method.getName() + " 未脚本化");
                });
    }
}
