package org.vastdata.vbstream.walsource.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.receive.PhysicalSlotManager;
import org.vastdata.vbstream.walsource.receive.WalByteSource;
import org.vastdata.vbstream.walsource.replay.CatalogStores;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;
import org.vastdata.vbstream.walsource.state.StateStore;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 档配置面 + 槽生命周期 OG 变体 + 判别器装配缝的离线测试（M1 Task 6）——
 * 三块断言面：
 * <ol>
 *   <li><b>配置解析</b>：{@code vb.wal.og.*} 五键（source/rx.mode/rx.dir/rx.bin/jdbc.prefix）
 *       的缺省值、合法值与非法值构造期 IAE（fail-fast 优于静默跑错通道）；空白的
 *       rx.bin/rx.dir 显式配置同样 IAE（spec §4）。</li>
 *   <li><b>PG 17/18 档零回归锚</b>：SQL 会话 URL 在 OG 键在场但 jdbc.prefix 缺省时
 *       仍为 {@code jdbc:postgresql://...}（PG 档完全不感知新键）；判别器升级
 *       {@code forServer(num, text)} 对 17/18 区间返回与 {@code forServerVersion(num)}
 *       同实例（行为零变化）；90204 + openGauss 5.0 文本路由到 OG 档。</li>
 *   <li><b>{@code PhysicalSlotManager.ensureSlotOg}</b>（伪 JDBC 会话）：建槽语句形态
 *       {@code pg_create_physical_replication_slot(?, false)}——第二参是 dummy_standby
 *       而非 vanilla 的 immediately_reserve；位点函数 xlog 化（新建/NULL 回落均走
 *       {@code pg_current_xlog_location()}）；存量槽 restart_lsn 复用路径不建槽；
 *       vanilla {@code ensureSlot} 的 {@code (?, true)} + {@code pg_current_wal_flush_lsn()}
 *       回归钉。</li>
 *   <li><b>{@code CatalogSynchronizer.startOg} 装配缝</b>（续传路径——离线绕开 bootstrap
 *       的 OG 内部适配[Task 7 范围]）：字节源工厂收续传 stored lsn、源被 start/stop 连带、
 *       观测面（consumedLsn 种子化/metrics/terminalFailure）就绪。</li>
 * </ol>
 * 伪 JDBC 经动态代理按 SQL 文本脚本化应答（本模块离线测试的首个 JDBC 接缝——只覆盖
 * PhysicalSlotManager 触碰的五个方法面，未脚本化方法即断言失败，防静默假绿）。
 */
class OgWalSourceConfigTest {

    /** 槽存在性查询语句（vanilla/OG 共用——pg_replication_slots 视图两系同形）。 */
    private static final String SLOT_QUERY =
            "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = ? AND slot_type = 'physical'";

    /** 检查点目录（startOg 续传路径用例的落盘面）。 */
    @TempDir
    Path stateDir;

    /** OG external file 通道的落盘目录（全装配门面用例的字节源 tail 面——空目录即纯等待）。 */
    @TempDir
    Path walDir;

    /** 词典外补充键的最小配置载体（其余键走默认）。 */
    private static Properties props(String key, String value) {
        Properties cfg = new Properties();
        cfg.setProperty(key, value);
        return cfg;
    }

    // ---- ① 配置解析面 ----

    /** 五键缺省值：file / managed / wal-og-queue / pg_receivexlog / jdbc:postgresql:（PG 档零感知前提）。 */
    @Test
    void ogConfigKeysDefaultToFileManagedQueueAndPostgresqlPrefix() {
        WalSource source = new WalSource(new Properties());
        assertEquals("file", source.ogSource(), "vb.wal.og.source 缺省 file（sidecar 通道）");
        assertEquals("managed", source.ogRxMode(), "vb.wal.og.rx.mode 缺省 managed（自拉起子进程）");
        assertEquals(Path.of("wal-og-queue"), source.ogRxDir(), "vb.wal.og.rx.dir 缺省 wal-og-queue");
        assertEquals("pg_receivexlog", source.ogRxBin(), "vb.wal.og.rx.bin 缺省 pg_receivexlog");
        assertEquals("jdbc:postgresql:", source.ogJdbcPrefix(),
                "jdbc.prefix 缺省 jdbc:postgresql:——PG 17/18 档对 OG 键完全不感知");
    }

    /** og.source 合法值 direct 通过、非法值构造期 IAE。 */
    @Test
    void ogSourceAcceptsDirectAndRejectsUnknownValues() {
        assertEquals("direct", new WalSource(props("vb.wal.og.source", "direct")).ogSource());
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.og.source", "grpc")),
                "非 file|direct 须构造期 IAE（fail-fast）");
    }

    /** og.rx.mode 合法值 external 通过、非法值构造期 IAE。 */
    @Test
    void ogRxModeAcceptsExternalAndRejectsUnknownValues() {
        assertEquals("external", new WalSource(props("vb.wal.og.rx.mode", "external")).ogRxMode());
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.og.rx.mode", "auto")),
                "非 managed|external 须构造期 IAE");
    }

    /** og.jdbc.prefix 仅两选：jdbc:opengauss:（opengauss-jdbc 驱动分流）/ jdbc:postgresql:（pgjdbc+md5 运维自担）。 */
    @Test
    void ogJdbcPrefixAcceptsOnlyOpengaussOrPostgresql() {
        assertEquals("jdbc:opengauss:", new WalSource(props("vb.wal.og.jdbc.prefix", "jdbc:opengauss:")).ogJdbcPrefix());
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.og.jdbc.prefix", "jdbc:mysql:")),
                "未知前缀须构造期 IAE（DriverManager 会抛 NoSuitableDriver，晚抛不如早抛）");
    }

    /** 显式空白的 rx.bin / rx.dir 构造期 IAE（spec §4：缺失属配置面错误——默认仅在键缺席时给）。 */
    @Test
    void blankOgRxBinOrDirFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.og.rx.bin", "  ")),
                "显式空白 rx.bin 须 IAE");
        assertThrows(IllegalArgumentException.class,
                () -> new WalSource(props("vb.wal.og.rx.dir", "")),
                "显式空白 rx.dir 须 IAE");
    }

    // ---- ② PG 档零回归锚 ----

    /** PG 路零触碰锚：OG 键全部在场但 jdbc.prefix 缺省时，SQL 会话 URL 仍为 jdbc:postgresql://host:port/db。 */
    @Test
    void sqlUrlStaysPostgresqlForPgPathEvenWithAllOgKeysSet() {
        Properties cfg = new Properties();
        cfg.setProperty("vb.wal.og.source", "direct");
        cfg.setProperty("vb.wal.og.rx.mode", "external");
        cfg.setProperty("vb.wal.og.rx.dir", "/tmp/og");
        cfg.setProperty("vb.wal.og.rx.bin", "/opt/og/bin/pg_receivexlog");
        WalSource source = new WalSource(cfg);
        assertEquals("jdbc:postgresql://localhost:5432/postgres", source.sqlJdbcUrl(),
                "OG 键不得影响 PG 档 SQL 会话 URL（缺省前缀 = 现状拼接逐字节不变）");
    }

    /** OG 驱动分流锚：jdbc.prefix=jdbc:opengauss: 时 URL 换前缀（DriverManager 按前缀选驱动，代码零分支）。 */
    @Test
    void sqlUrlUsesConfiguredOpengaussPrefixForOgDriver() {
        WalSource source = new WalSource(props("vb.wal.og.jdbc.prefix", "jdbc:opengauss:"));
        assertEquals("jdbc:opengauss://localhost:5432/postgres", source.sqlJdbcUrl());
    }

    /** 判别器升级零变化锚：forServer(num, text) 对 17/18 区间返回与 forServerVersion(num) 同实例。 */
    @Test
    void serverDispatchUpgradeReturnsSameInstancesForPgVersions() {
        assertSame(WalLayouts.forServerVersion(180002), WalLayouts.forServer(180002, null),
                "18 区间：新重载返回同实例（versionText 不参与该档判据）");
        assertSame(WalLayouts.forServerVersion(170005), WalLayouts.forServer(170005, "PostgreSQL 17.5"),
                "17 区间：同实例");
        assertSame(WalLayoutVOpenGauss.INSTANCE,
                WalLayouts.forServer(90204, "(openGauss 5.0.0 build abc) compiled at 2023"),
                "90204 + openGauss 5.0 文本路由 OG 档（装配分派的入口）");
    }

    // ---- ③ PhysicalSlotManager.ensureSlotOg（伪 JDBC） ----

    /** 槽不存在 → 建槽语句第二参为 false（dummy_standby，非 vanilla 的 immediately_reserve）+ 新建回落 xlog 位点函数。 */
    @Test
    void ensureSlotOgCreatesWithDummyStandbyFalseAndFallsBackToXlogLocation() throws SQLException {
        FakeJdbc jdbc = FakeJdbc.scripted()
                .rows(SLOT_QUERY)                                            // 无行 = 槽不存在
                .rows("SELECT * FROM pg_create_physical_replication_slot(?, false)",
                        new Object[] {"og_slot"})                               // 建槽成功（返回列面不消费）
                .rows("SELECT pg_current_xlog_location()", new Object[] {"0/1234"})
                .build();
        long resume = new PhysicalSlotManager(jdbc.connection()).ensureSlotOg("og_slot");
        assertEquals(0x1234L, resume, "新建路径返回建槽时刻位点（xlog 命名函数）");
        assertTrue(jdbc.executed.contains("SELECT * FROM pg_create_physical_replication_slot(?, false)"),
                "OG 建槽第二参须为 false（dummy_standby）");
        assertFalse(jdbc.executed.stream().anyMatch(sql -> sql.contains("(?, true)")),
                "不得出现 vanilla 的 immediately_reserve=true 形态");
        assertTrue(jdbc.executed.contains("SELECT pg_current_xlog_location()"),
                "OG 无 flush 档函数——回落 pg_current_xlog_location()");
        assertFalse(jdbc.executed.stream().anyMatch(sql -> sql.contains("pg_current_wal_flush_lsn")),
                "不得触碰 vanilla 的 flush LSN 函数");
    }

    /** 槽已存在且 restart_lsn 非 NULL → 直接复用（不建槽、不查位点函数）。 */
    @Test
    void ensureSlotOgReturnsRestartLsnOfExistingSlotWithoutCreate() throws SQLException {
        FakeJdbc jdbc = FakeJdbc.scripted()
                .rows(SLOT_QUERY, new Object[] {"0/5678"})
                .build();
        long resume = new PhysicalSlotManager(jdbc.connection()).ensureSlotOg("og_slot");
        assertEquals(0x5678L, resume, "复用路径返回 restart_lsn");
        assertFalse(jdbc.executed.stream().anyMatch(sql -> sql.contains("pg_create_physical_replication_slot")),
                "存量槽复用不得建槽");
    }

    /** 槽存在但 restart_lsn 为 NULL（未 reserve/从未附槽）→ 回落当前 xlog 位点（与"新建即从现在收"语义对齐）。 */
    @Test
    void ensureSlotOgNullRestartLsnFallsBackToXlogLocation() throws SQLException {
        FakeJdbc jdbc = FakeJdbc.scripted()
                .rows(SLOT_QUERY, new Object[] {null})
                .rows("SELECT pg_current_xlog_location()", new Object[] {"0/9ABC"})
                .build();
        long resume = new PhysicalSlotManager(jdbc.connection()).ensureSlotOg("og_slot");
        assertEquals(0x9ABCL, resume);
        assertFalse(jdbc.executed.stream().anyMatch(sql -> sql.contains("pg_create_physical_replication_slot")),
                "NULL restart_lsn 是复用路径的回落面，不是建槽面");
    }

    /** vanilla ensureSlot 回归钉：建槽 (?, true)（immediately_reserve）+ flush LSN 函数（重构后逐字不变）。 */
    @Test
    void ensureSlotVanillaKeepsImmediatelyReserveTrueAndFlushFunction() throws SQLException {
        FakeJdbc jdbc = FakeJdbc.scripted()
                .rows(SLOT_QUERY)
                .rows("SELECT * FROM pg_create_physical_replication_slot(?, true)",
                        new Object[] {"wal_source"})
                .rows("SELECT pg_current_wal_flush_lsn()", new Object[] {"0/1000"})
                .build();
        long resume = new PhysicalSlotManager(jdbc.connection()).ensureSlot("wal_source");
        assertEquals(0x1000L, resume);
        assertTrue(jdbc.executed.contains("SELECT * FROM pg_create_physical_replication_slot(?, true)"),
                "vanilla 建槽保持 immediately_reserve=true");
        assertTrue(jdbc.executed.contains("SELECT pg_current_wal_flush_lsn()"),
                "vanilla 位点函数保持 flush 档");
    }

    // ---- ④ CatalogSynchronizer.startOg 装配缝（续传路径——bootstrap 内部归 Task 7，离线绕开） ----

    /**
     * startOg 续传路径装配缝：pgVersion=5 检查点在场 → 跳过 bootstrap（绕开 Task 7 的
     * OG 内部 SQL 适配），字节源工厂收到的起点 = stored lsn（ensureSlotOg 的 P₀ 不越前），
     * 源被 start、观测面（consumedLsn 种子化/metrics/terminalFailure/resumedFromState）就绪，
     * stop() 连带停源且最终检查点 + 槽推进走通（槽推进 SQL 形态一并钉）。
     */
    @Test
    void startOgResumePathAssemblesByteSourceWithStoredLsnAndObservationFaces() throws Exception {
        long storedLsn = 0x10028L;
        new StateStore(stateDir, 5).checkpoint(new CatalogStores(), storedLsn);
        FakeJdbc jdbc = FakeJdbc.scripted()
                .rows("SELECT current_setting('server_version_num')::int", new Object[] {"90204"})
                .rows(SLOT_QUERY, new Object[] {"0/10000"})                       // P₀ = 0x10000 < stored
                .rows("SELECT pg_replication_slot_advance(?, ?::pg_lsn)", new Object[] {"0/10000"})
                .build();
        RecordingSource source = new RecordingSource();
        CatalogSynchronizer sync = CatalogSynchronizer.startOg(jdbc.connection(), "og_slot",
                WalLayoutVOpenGauss.INSTANCE, StateConfig.enabled(stateDir), 0L,
                lsn -> {
                    source.factoryLsn = lsn;
                    return source;
                });
        try {
            assertEquals(storedLsn, source.factoryLsn, "字节源工厂起点 = 续传 stored lsn");
            assertEquals(storedLsn, sync.consumedLsn(), "前沿种子化 = 续传起点（首块前的报告面）");
            assertTrue(sync.resumedFromState(), "检查点在场且 lsn ≥ P₀ → 续传（跳过 bootstrap）");
            assertNotNull(sync.streamMetrics(), "OG 档 walker 计数须可观测（receiver==null 路的来源）");
            assertTrue(sync.terminalFailure().isEmpty());
        } finally {
            sync.stop();
        }
        assertTrue(source.started, "装配须 start 字节源");
        assertTrue(source.stopped, "stop() 须连带停源");
        assertFalse(jdbc.executed.contains("SELECT pg_current_xlog_location()"),
                "P₀ 来自存量槽 restart_lsn——不触位点函数");
    }

    // ---- ⑤ OG 档全装配门面的 DML listener 装配（M2 T1 撤销 M1 WARN 降级） ----

    /**
     * README/Main 形态的 OG 档全装配在 <strong>PG 兼容模式库</strong>上正常装配 DML 面
     * （M2 T1 撤销 M1 的 WARN 降级）：冒烟 {@code Main} 固定以双参构造注入
     * {@code OutputRenderer}（listener 非 null）且 {@code vb.wal.dml} 缺省 true——M1
     * 曾因"DML 面属 M2"对在场 listener WARN 降级 catalog-only（终审 I-1，{@code
     * dmlEnabled()} 归 false）；M2 起 DML 面开门，{@code ogCatalogOnly} 位退役、
     * listener 在场且 dml 开着时 <strong>postApply/pendingFloor 与 PG 17/18 路同构装配</strong>
     * （{@code dmlEnabled()} 保持 true，Main 周期行的 {@code dml=[...]} 段随之恢复），
     * 兼容模式护栏对 PG 档库放行。
     *
     * <p>关键步骤：伪 Driver 顶替 DriverManager 里的真 pgjdbc（临时注销/恢复——只影响
     * 本用例执行窗）驱动 {@code WalSource.start()} 全路径（版本判别 → 兼容模式探测 →
     * OG 装配 → external file 通道空目录 tail）；state 预置 pgVersion=5 检查点走续传
     * （绕开引导的 OG 内部 SQL 适配）。断言：探测 SQL 真实下发（护栏在装配路径上）+
     * start 正常返回 + {@code dmlEnabled()} 保持 true。</p>
     */
    @Test
    void ogStartWithMainShapedDmlListenerAssemblesDmlFaceOnPgCompatDatabase() throws Exception {
        long storedLsn = 0x10028L;
        new StateStore(stateDir, 5).checkpoint(new CatalogStores(), storedLsn);
        FakeJdbc jdbc = FakeJdbc.scripted()
                .rows("SELECT current_setting('server_version_num')", new Object[] {"90204"})
                .rows("SELECT version()", new Object[] {"(openGauss 5.0.0 build abc) compiled at 2023"})
                .rows(OgCompatModeGuardTest.DATCOMPAT_QUERY, new Object[] {"PG"})
                .rows("SELECT current_setting('server_version_num')::int", new Object[] {"90204"})
                .rows(SLOT_QUERY, new Object[] {"0/10000"})
                .build();
        Driver real = DriverManager.getDriver("jdbc:postgresql://localhost:5432/postgres");
        Driver fake = proxyDriver(jdbc);
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
            source.start();
            assertTrue(jdbc.executed.contains(OgCompatModeGuardTest.DATCOMPAT_QUERY),
                    "DML 面请求在场的 OG 装配须先探测库级兼容模式（护栏在装配路径上）");
            assertTrue(source.dmlEnabled(),
                    "PG 兼容档库上 DML listener 须正常装配（M1 的 ogCatalogOnly 降级位已退役）");
        } finally {
            source.close();
            DriverManager.deregisterDriver(fake);
            DriverManager.registerDriver(real);
        }
    }

    /**
     * 伪 JDBC Driver（顶替 DriverManager 里的真 pgjdbc 驱动 {@code WalSource.start()} 的
     * 建连面）：只认 {@code jdbc:postgresql:} 前缀 URL，connect 返回 {@link FakeJdbc}
     * 的脚本化连接代理；未脚本化方法面按 Driver 接口惯例值兜底（jdbcCompliant/
     * getParentLogger 不进断言面）。包可见供 {@link OgCompatModeGuardTest} 复用。
     *
     * @param jdbc 脚本化伪会话
     * @return Driver 代理
     */
    static Driver proxyDriver(FakeJdbc jdbc) {
        return (Driver) Proxy.newProxyInstance(OgWalSourceConfigTest.class.getClassLoader(),
                new Class<?>[] {Driver.class}, (p, method, args) -> switch (method.getName()) {
                    case "acceptsURL" -> ((String) args[0]).startsWith("jdbc:postgresql:");
                    case "connect" -> ((String) args[0]).startsWith("jdbc:postgresql:")
                            ? jdbc.connection() : null;
                    case "jdbcCompliant" -> Boolean.FALSE;
                    case "getParentLogger" -> java.util.logging.Logger.getGlobal();
                    default -> null;
                });
    }

    /**
     * 记录型字节源（startOg 装配缝测试的捕获面）：不产字节（walker 零喂入），只记录
     * 工厂起点、start/stop 生命周期。
     */
    private static final class RecordingSource implements WalByteSource {

        /** 工厂回调收到的流起点（构造缝断言锚）。 */
        long factoryLsn;

        /** start 是否被装配层调用。 */
        boolean started;

        /** stop 是否被同步器连带调用。 */
        boolean stopped;

        /**
         * 记录 start 调用（回调不保存——本捕获面不模拟字节产流）。
         *
         * @param chunkEndLsnSink 前沿通知（忽略）
         * @param chunkSink       字节交付（忽略）
         */
        @Override
        public void start(LongConsumer chunkEndLsnSink, BiConsumer<Long, byte[]> chunkSink) {
            started = true;
        }

        /** 记录停机（幂等位语义忽略——单次调用断言面）。 */
        @Override
        public void stop() {
            stopped = true;
        }

        /**
         * 消费前沿（未产流恒 0）。
         *
         * @return 0
         */
        @Override
        public long consumedLsn() {
            return 0L;
        }

        /** {@link AutoCloseable#close} 的无检收窄——委托 {@link #stop()}。 */
        @Override
        public void close() {
            stop();
        }
    }

    /**
     * 伪 JDBC 会话：动态代理按 SQL 文本脚本化查询应答，并记录执行过的语句文本。
     *
     * <p>只覆盖 {@link PhysicalSlotManager} 触碰的方法面（prepareStatement/createStatement/
     * setString/executeQuery/next/getString/close）；未脚本化方法一经调用即抛
     * {@link AssertionError}——防止"假驱动吞掉真实调用"的静默假绿。包可见供
     * {@link OgCompatModeGuardTest} 复用。线程约束：单线程顺序使用（槽管理器本就
     * 单连接串行）。</p>
     */
    static final class FakeJdbc {

        /** 已执行语句文本（包可见供同包护栏测试断言"探测先于槽副作用"）。 */
        final List<String> executed = new ArrayList<>();

        private final Map<String, List<Object[]>> scripted = new HashMap<>();

        private FakeJdbc() {
        }

        /**
         * 脚本构造入口（链式收集 SQL → 行集映射）。
         *
         * @return 链式收集器
         */
        static ScriptedBuilder scripted() {
            return new ScriptedBuilder();
        }

        /**
         * 造 Connection 代理（语句创建即记录 SQL 文本；查询应答按脚本行集回放）。
         *
         * @return 代理连接
         */
        Connection connection() {
            return proxy(Connection.class, (p, method, args) -> switch (method.getName()) {
                case "prepareStatement" -> preparedStatement((String) args[0]);
                case "createStatement" -> statement();
                case "isClosed" -> Boolean.FALSE;
                case "isValid" -> Boolean.TRUE;
                case "close", "setAutoCommit" -> null;
                default -> throw new AssertionError("Connection." + method.getName() + " 未脚本化");
            });
        }

        /**
         * PreparedStatement 代理：setString 静默接受（参数绑定不进断言面——语句形态按 SQL
         * 文本钉），executeQuery 按该语句的脚本行集回放。
         */
        private PreparedStatement preparedStatement(String sql) {
            executed.add(sql);
            return proxy(PreparedStatement.class, (p, method, args) -> switch (method.getName()) {
                case "setString", "setLong", "setInt" -> null;
                case "executeQuery" -> resultSet(sql);
                case "close" -> null;
                default -> throw new AssertionError("PreparedStatement." + method.getName() + " 未脚本化");
            });
        }

        /**
         * Statement 代理：executeQuery(String) 记录语句文本并回放脚本。
         */
        private Statement statement() {
            return proxy(Statement.class, (p, method, args) -> switch (method.getName()) {
                case "executeQuery" -> {
                    String sql = (String) args[0];
                    executed.add(sql);
                    yield resultSet(sql);
                }
                case "close" -> null;
                default -> throw new AssertionError("Statement." + method.getName() + " 未脚本化");
            });
        }

        /**
         * ResultSet 代理：next 推进脚本行游标、getString 取当前行列值（null 列回 null
         * ——restart_lsn NULL 回落路径的输入面）。
         */
        private ResultSet resultSet(String sql) {
            Iterator<Object[]> rows = scripted.getOrDefault(sql, List.of()).iterator();
            Object[] current = new Object[1];
            return proxy(ResultSet.class, (p, method, args) -> switch (method.getName()) {
                case "next" -> {
                    current[0] = rows.hasNext() ? rows.next() : null;
                    yield current[0] != null;
                }
                case "getString" -> current[0] == null ? null : ((Object[]) current[0])[((Integer) args[0]) - 1];
                case "getInt" -> Integer.parseInt(String.valueOf(((Object[]) current[0])[((Integer) args[0]) - 1]));
                case "close" -> null;
                default -> throw new AssertionError("ResultSet." + method.getName() + " 未脚本化");
            });
        }

        /**
         * 单接口动态代理快捷构造。
         *
         * @param iface 接口类
         * @param handler 分派器
         * @return 代理实例
         */
        private static <T> T proxy(Class<T> iface, InvocationHandler handler) {
            @SuppressWarnings("unchecked")
            T out = (T) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(), new Class<?>[] {iface}, handler);
            return out;
        }

        /**
         * 链式脚本收集器（build 时灌入 FakeJdbc 的脚本映射）。
         */
        static final class ScriptedBuilder {

            private final FakeJdbc target = new FakeJdbc();

            /**
             * 登记一条 SQL 的应答行集。
             *
             * @param sql  语句文本（须与被测代码字面完全一致——形态断言的锚）
             * @param rows 行（每行一 Object[]；零参 = 空结果集）
             * @return 本收集器（链式）
             */
            ScriptedBuilder rows(String sql, Object[]... rows) {
                target.scripted.put(sql, List.of(rows));
                return this;
            }

            /**
             * 收口为伪会话。
             *
             * @return 伪 JDBC 会话
             */
            FakeJdbc build() {
                return target;
            }
        }
    }
}
