package org.vastdata.vbstream.walsource.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
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
 * openGauss 档启动期兼容模式护栏的离线测试（M2 Task 1 落地，M3 Task 3 白名单放宽
 * {'PG'} → {'PG','A'}，M4 Task 6 起白名单按 layout 分档——VastBase 档
 * {'PG','A','B'}、OG 5.0.0 档维持 {'PG','A'}）——OG 档 DML 面请求在场时探测库级
 * {@code pg_database.datcompatibility}，<strong>白名单外档位 ISE</strong>（附档位值
 * 与分档指引），两块断言面：
 * <ol>
 *   <li><b>档位判定面</b>（{@code WalSource.isDmlSupportedCompatMode}，伪 JDBC 脚本化
 *       {@link #DATCOMPAT_QUERY}）：'PG'/'A'（Oracle 风格，M3 起支持——A 档原生分区
 *       DML 面经 pg_partition 第四字典捕获）白名单放行（两档 layout 白名单同集）；
 *       'B'（MySQL 风格，M4 起 VastBase 档开门——OG 5.0.0 档 B 档 typemap 未验证
 *       维持拒绝，spec §0 裁定 a）按 layout 分；'C'（PG 兼容历史别名——og-plan 实测
 *       5.0.0 仍接受建库且存储值与 'PG' 相异，未验证）两 layout 均判定非支持档；探测
 *       不可判定（无行/null/空白值/探测 SQLException）<strong>fail-open 放行</strong>
 *       ——护栏是兼容性预检不是正确性锚，可判定的白名单外档位照 ISE，不可判定面
 *       静默砖死启动不如 WARN 放行。</li>
 *   <li><b>全路径面</b>（伪 Driver 顶替 DriverManager，复用
 *       {@link OgWalSourceConfigTest} 的脚本化设施）：Main 形态（listener + dml 缺省
 *       true）在 <strong>OG 布局</strong>（version() 含 openGauss branding）+ 'B' 档库上
 *       start() 抛 ISE——消息含档位值与 M4 指引，且护栏先于槽副作用（槽查询未被触达）；
 *       <strong>VastBase 布局</strong>（version() 无 branding、relname attlen=128 三参
 *       分发命中）+ 'B' 档库（M4 开门）正常装配 DML 面；'A' 档库（M3 放行）与 'PG'
 *       档库（M2 放行，{@link OgWalSourceConfigTest} ⑤ 反转用例）正常装配 DML 面。</li>
 * </ol>
 * 依据（og-plan 容器 2026-10-12 实测）：postgres 缺省库 {@code datcompatibility='A'}、
 * {@code CREATE DATABASE m2pg DBCOMPATIBILITY 'PG'} 库为 {@code 'PG'}——PG 档的表示
 * 形态即 'PG' 单值（空/NULL 不是 PG 档表示，归不可判定面）。M1 全部 OG IT 存活依据：
 * 护栏仅在 DML 面请求在场（listener + vb.wal.dml=true）时探测——catalog-only 形态
 * （无 listener 或显式 dml=false）与兼容模式无关（M1 IT 即跑在 'A' 缺省库上全绿；
 * M3 起 'A' 档库对 DML 面同样开门）。
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

    /** 可判定档位面：'PG'/'A' 白名单放行（两档 layout 白名单同集）；'B' 按 layout 分（M4）；'C'（PG 兼容历史别名但未验证）两 layout 均拒。 */
    @Test
    void definiteCompatModesResolveAgainstWhitelist() throws Exception {
        OgWalSourceConfigTest.FakeJdbc pg = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"PG"}).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(pg.connection()), "'PG' 档须放行（M2 支持面）");
        assertTrue(pg.executed.contains(DATCOMPAT_QUERY), "探测 SQL 形态锚（实现与测试字面一致）");
        OgWalSourceConfigTest.FakeJdbc oracle = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"A"}).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(oracle.connection()),
                "'A' 档须放行（M3 起 Oracle 风格档开门——postgres 缺省库即此档）");
        OgWalSourceConfigTest.FakeJdbc jdbc = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"B"}).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(jdbc.connection(), WalLayoutVVastbase.INSTANCE),
                "'B' 档在 VastBase 布局须放行（M4 开门）");
        assertFalse(WalSource.isDmlSupportedCompatMode(jdbc.connection(), WalLayoutVOpenGauss.INSTANCE),
                "'B' 档在 OG 5.0.0 布局须判定非支持（typemap 未验证，M4 只开 VastBase 档）");
        OgWalSourceConfigTest.FakeJdbc legacy = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"C"}).build();
        assertFalse(WalSource.isDmlSupportedCompatMode(legacy.connection()),
                "'C' 档须判定非支持（PG 兼容历史别名未验证——单参调用面即 OG 白名单语义）");
        assertFalse(WalSource.isDmlSupportedCompatMode(legacy.connection(), WalLayoutVVastbase.INSTANCE),
                "'C' 档在 VastBase 布局同样判定非支持（两 layout 均拒）");
    }

    /**
     * M4 护栏分档：B 档白名单按 layout 分——VastBase 档放行（{'PG','A','B'}，M4 开门）、
     * OG 5.0.0 档维持拒绝（B 档 typemap 未验证，spec §0 裁定 a）；'C' 档两 layout 均拒绝
     * （PG 兼容历史别名未验证）。同一脚本化连接复用（FakeJdbc 行集按语句逐次回放）。
     */
    @Test
    void bCompatModeWhitelistSplitsByLayout() throws Exception {
        OgWalSourceConfigTest.FakeJdbc b = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"B"}).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(b.connection(), WalLayoutVVastbase.INSTANCE),
                "B 档在 VastBase 布局须放行（M4 开门）");
        assertFalse(WalSource.isDmlSupportedCompatMode(b.connection(), WalLayoutVOpenGauss.INSTANCE),
                "B 档在 OG 5.0.0 布局维持 ISE（未验证，spec §0 裁定 a）");
        OgWalSourceConfigTest.FakeJdbc c = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {"C"}).build();
        assertFalse(WalSource.isDmlSupportedCompatMode(c.connection(), WalLayoutVVastbase.INSTANCE),
                "'C' 档（PG 兼容历史别名未验证）两 layout 均拒绝");
        assertFalse(WalSource.isDmlSupportedCompatMode(c.connection(), WalLayoutVOpenGauss.INSTANCE),
                "'C' 档在 OG 5.0.0 布局同样拒绝");
    }

    /** 不可判定面 fail-open：无行 / null 值 / 空白值 / 探测 SQLException 均放行（护栏仅对可判定档位生效）。 */
    @Test
    void undeterminableProbeFailsOpen() throws Exception {
        OgWalSourceConfigTest.FakeJdbc noRow = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(noRow.connection()), "无行（pg_database 查不到当前库）按不可判定放行");
        OgWalSourceConfigTest.FakeJdbc nullValue = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {null}).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(nullValue.connection()), "null 档位值按不可判定放行");
        OgWalSourceConfigTest.FakeJdbc blank = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows(DATCOMPAT_QUERY, new Object[] {" "}).build();
        assertTrue(WalSource.isDmlSupportedCompatMode(blank.connection()), "空白档位值按不可判定放行");
        assertTrue(WalSource.isDmlSupportedCompatMode(probeThrowingConnection()),
                "探测 SQLException（权限/瞬断）按不可判定放行——fail-open 优于静默砖死启动");
    }

    /**
     * Main 形态（listener 在场 + vb.wal.dml 缺省 true）在 'A' 档库上 start() 正常装配
     * DML 面（M3 Task 3 放行——白名单 {'PG','A'}）：探测 SQL 真实下发（护栏在装配
     * 路径上）+ start 正常返回 + {@code dmlEnabled()} 保持 true。脚本面与
     * {@link OgWalSourceConfigTest} ⑤ 的 PG 档放行用例同构（state 预置 pgVersion=5
     * 检查点走续传，绕开引导的 OG 内部 SQL 适配）。
     */
    @Test
    void ogStartWithDmlListenerOnACompatDatabaseAssemblesDmlFace() throws Exception {
        long storedLsn = 0x10028L;
        new StateStore(stateDir, 5).checkpoint(new CatalogStores(), storedLsn);
        OgWalSourceConfigTest.FakeJdbc jdbc = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows("SELECT current_setting('server_version_num')", new Object[] {"90204"})
                .rows("SELECT version()", new Object[] {"(openGauss 5.0.0 build abc) compiled at 2023"})
                .rows(DATCOMPAT_QUERY, new Object[] {"A"})
                .rows("SELECT current_setting('server_version_num')::int", new Object[] {"90204"})
                .rows(OgWalSourceConfigTest.SLOT_QUERY, new Object[] {"0/10000"})
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
            @Override public void onTruncate(ChangeOutputListener.TruncateEvent event) { }
            @Override public void onEnd(ChangeOutputListener.BatchEnd end) { }
            @Override public void onAborted(ChangeOutputListener.BatchAborted aborted) { }
        };
        WalSource source = new WalSource(cfg, noop);
        try {
            source.start();
            assertTrue(jdbc.executed.contains(DATCOMPAT_QUERY),
                    "A 档放行也须先探测库级兼容模式（护栏在装配路径上）");
            assertTrue(source.dmlEnabled(),
                    "'A' 档库上 DML listener 须正常装配（M3 放行——白名单 {'PG','A'}）");
        } finally {
            source.close();
            DriverManager.deregisterDriver(fake);
            DriverManager.registerDriver(real);
        }
    }

    /**
     * Main 形态（listener 在场 + vb.wal.dml 缺省 true）在 <strong>OG 5.0.0 布局</strong>
     * + 'B' 档库上 start() 启动期 ISE（M2 Task 1 用例的 M3/M4 连动——原 'A' 档输入面随
     * M3 白名单放宽换 'B'；M4 起 'B' 仅 VastBase 档开门，本用例 version() 含 openGauss
     * branding 命中 OG 布局，是分档后维持 ISE 的一腿——VastBase 布局放行腿见
     * {@link #vastbaseStartWithDmlListenerOnBCompatDatabaseAssemblesDmlFace}）：消息含
     * 档位值与 M4 指引，且护栏先于槽副作用——槽存在性查询未被触达（FakeJdbc 未脚本面
     * 一经调用即 AssertionError，本用例显式断言槽查询缺席）。
     */
    @Test
    void ogStartWithDmlListenerOnBCompatDatabaseFailsFastBeforeSlotSideEffects() throws Exception {
        long storedLsn = 0x10028L;
        new StateStore(stateDir, 5).checkpoint(new CatalogStores(), storedLsn);
        OgWalSourceConfigTest.FakeJdbc jdbc = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows("SELECT current_setting('server_version_num')", new Object[] {"90204"})
                .rows("SELECT version()", new Object[] {"(openGauss 5.0.0 build abc) compiled at 2023"})
                .rows(DATCOMPAT_QUERY, new Object[] {"B"})
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
            @Override public void onTruncate(ChangeOutputListener.TruncateEvent event) { }
            @Override public void onEnd(ChangeOutputListener.BatchEnd end) { }
            @Override public void onAborted(ChangeOutputListener.BatchAborted aborted) { }
        };
        WalSource source = new WalSource(cfg, noop);
        try {
            IllegalStateException ise = assertThrows(IllegalStateException.class, source::start,
                    "'B' 档库 + DML 面请求在场须启动期 ISE（M4 支持面——白名单 {'PG','A'}）");
            assertTrue(ise.getMessage().contains("datcompatibility=B"),
                    "ISE 消息须含实际档位值: " + ise.getMessage());
            assertTrue(ise.getMessage().contains("M4"),
                    "ISE 消息须附 M4（B 档）里程碑指引: " + ise.getMessage());
            assertFalse(jdbc.executed.stream().anyMatch(sql -> sql.contains("pg_replication_slots")),
                    "护栏须先于槽副作用（槽查询未被触达）");
        } finally {
            source.close();
            DriverManager.deregisterDriver(fake);
            DriverManager.registerDriver(real);
        }
    }

    /**
     * Main 形态（listener 在场 + vb.wal.dml 缺省 true）在 <strong>VastBase 布局</strong>
     * + 'B' 档库上 start() 正常装配 DML 面（M4 Task 6 开门——VastBase 档白名单
     * {'PG','A','B'}）：判别链走 version() 无 openGauss branding + relname attlen=128
     * 三参分发命中 VastBase 布局（脚本面与 OG 布局 ISE 用例同构，state 预置 pgVersion=6
     * 检查点走续传——VastBase 档位校验的排他锚），护栏探测 'B' 后按 VastBase 档放行
     * （探测 SQL 真实下发——护栏在装配路径上）。
     */
    @Test
    void vastbaseStartWithDmlListenerOnBCompatDatabaseAssemblesDmlFace() throws Exception {
        long storedLsn = 0x10028L;
        new StateStore(stateDir, 6).checkpoint(new CatalogStores(), storedLsn);
        OgWalSourceConfigTest.FakeJdbc jdbc = OgWalSourceConfigTest.FakeJdbc.scripted()
                .rows("SELECT current_setting('server_version_num')", new Object[] {"90204"})
                .rows("SELECT version()", new Object[] {"PostgreSQL 9.2.4 (G100 V3.0, compiled at 2026)"})
                .rows(WalSource.RELNAME_PROBE_SQL, new Object[] {"128"})
                .rows(DATCOMPAT_QUERY, new Object[] {"B"})
                .rows("SELECT current_setting('server_version_num')::int", new Object[] {"90204"})
                .rows(OgWalSourceConfigTest.SLOT_QUERY, new Object[] {"0/10000"})
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
            @Override public void onTruncate(ChangeOutputListener.TruncateEvent event) { }
            @Override public void onEnd(ChangeOutputListener.BatchEnd end) { }
            @Override public void onAborted(ChangeOutputListener.BatchAborted aborted) { }
        };
        WalSource source = new WalSource(cfg, noop);
        try {
            source.start();
            assertTrue(jdbc.executed.contains(WalSource.RELNAME_PROBE_SQL),
                    "判别链须先经 relname attlen 探测命中 VastBase 布局（探测值 128）");
            assertTrue(jdbc.executed.contains(DATCOMPAT_QUERY),
                    "B 档放行也须先探测库级兼容模式（护栏在装配路径上）");
            assertTrue(source.dmlEnabled(),
                    "'B' 档库 + VastBase 布局上 DML listener 须正常装配（M4 开门——白名单 {'PG','A','B'}）");
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
