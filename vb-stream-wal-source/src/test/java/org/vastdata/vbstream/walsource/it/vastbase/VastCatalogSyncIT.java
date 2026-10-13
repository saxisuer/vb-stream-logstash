package org.vastdata.vbstream.walsource.it.vastbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.replay.CatalogReplay;
import org.vastdata.vbstream.walsource.replay.CatalogRow;
import org.vastdata.vbstream.walsource.replay.CatalogStores;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;
import org.vastdata.vbstream.walsource.state.StateStore;
import org.vastdata.vbstream.walsource.state.StoredState;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * VastBase G100 catalog 同步对拍（手动档，vastbase T5——适配核心闭环的端到端验收）：
 * 真库（非容器）干扰 DDL 序列下，同步器四字典末态 vs JDBC REPEATABLE READ 实查
 * <strong>逐行全等</strong>。断言面翻译自 M1 {@code OgCatalogSyncIT}（干扰风暴 + 全 DDL
 * 场景 + 停流对拍），对拍器语义复刻 {@code WalSyncItBase}（跨包不可见——包内自持同源
 * 实现：ctid 键控行字典、relfilenodeOf/toastOf/schemaOf/relkindOf/nameOf 末态、tracked
 * 双 ctid、列序严格升序），并增 VastBase 专属两面：
 * <ol>
 *   <li><b>第四字典（pg_partition）行级对拍</b>——分区 CREATE/ADD/DROP PARTITION 干扰
 *       序列下，{@code partitionRows} vs JDBC 实查按 ctid 键逐行全等 + 分区身份链
 *       {@code partitionOf}（'p' 行 relfilenode 路由锚）与 JDBC 末态一致、已 DROP 分区
 *       路由归空；</li>
 *   <li><b>门面判别链</b>——{@link WalSource} 全装配经真库判别（90204 + version() 无
 *       branding + relname attlen=128 探测 → {@code WalLayoutVVastbase} → OG 装配分支
 *       {@code startOpenGauss}），收流/检查点走通且落 <b>v6</b> 检查点（判别若误落
 *       OG 5 档，StateStore 以 pgVersion=6 load 即拒载——majorVersion 是判别落点的
 *       可加载证据）。</li>
 * </ol>
 *
 * <p><strong>本 IT 即 spec §0.2 全装配试跑折在 {@code tuple has 42 atts but dictionary
 * has 40} 的那个面</strong>——引导（OG 种子 SQL 在 VastBase 42/29 列超集上的显式列名
 * 投影）与重放（43/29 项词典 + 读区偏移 +64B）任一错位，都会以行级 diff 或解析 ISE
 * （接收线程终态死亡 → await 超时）在这里红。</p>
 *
 * <p><strong>真库纪律（长活共享环境）</strong>：表/槽/schema 一律 {@code vb_wal_} 前缀
 * + finally/@AfterEach 清理；不触碰既有业务表（orders/snapshot/gs_系/ora_alerts/
 * test_lock/utl_file_dir）与既有逻辑槽（sync_orders_slot/vb_e2e_*）；对拍范围按本用例
 * oid 集圈定（真库背景活动的他表行不进断言面）；干扰循环缩至 20 轮（真库轻量纪律）。
 * state 目录 = {@code target/vastbase-it/} 下独立子目录 + 用例级 wipe（Windows 纪律）。
 * 环境锚（write 位点档位点函数/VACUUM/CHECKPOINT 权限/两参建槽变体）见
 * {@link VastTestEnv} 类 javadoc。</p>
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastCatalogSyncIT {

    private static final Logger LOG = LoggerFactory.getLogger(VastCatalogSyncIT.class);

    /** VastBase 档布局单例（记录层与 OG 5.0.0 同构，词典几何 name128——判别在门面探测）。 */
    private static final WalLayout LAYOUT = WalLayoutVVastbase.INSTANCE;

    /** direct 通道反馈心跳的 statusInterval（毫秒）——对齐 OG 侧同步 IT 的 5s 档。 */
    private static final long DIRECT_STATUS_INTERVAL_MS = 5_000;

    /** 干扰循环轮数（真库轻量纪律——OG 容器版是 8s 时窗，这里缩为 20 轮计次）。 */
    private static final int INTERFERENCE_ROUNDS = 20;

    /** 干扰线程 join 上限（毫秒）。 */
    private static final long INTERFERENCE_JOIN_MS = 15_000;

    /** 干扰循环单轮内执行随机 CHECKPOINT 的概率（沿 OG 侧配方裁剪档）。 */
    private static final double CHECKPOINT_PROBABILITY = 0.3;

    /** 对拍等待接收前沿的轮询节拍（毫秒）。 */
    private static final long POLL_INTERVAL_MS = 100;

    /** 对拍等待接收前沿的超时（毫秒）——真库跨网，较容器档放宽到 60s。 */
    private static final long POLL_TIMEOUT_MS = 60_000;

    /** 跨用例的槽名序号（每用例独立槽，兜底清理统一删除）。 */
    private static final AtomicLong SLOT_SEQ = new AtomicLong();

    /** 本用例启动的同步器/门面——AfterEach 兜底停机。 */
    private final List<AutoCloseable> closables = new ArrayList<>();

    /** 本用例持有的引导会话（probe / 槽推进复用）——AfterEach 兜底关闭。 */
    private final List<Connection> conns = new ArrayList<>();

    /** 本用例用过的槽——AfterEach 兜底删除。 */
    private final List<String> slots = new ArrayList<>();

    /** 对拍主用例的同步器（startVastSync 后赋值——场景内 interest 登记持有面）。 */
    private CatalogSynchronizer currentSync;

    /**
     * 对拍主用例：干扰 + 全 DDL 场景（含分区第四字典面）下，VastBase 同步器四字典
     * 末态与 JDBC REPEATABLE READ 实查逐行全等，并把重放与 walker 指标全量记档
     * （resyncs 观测面不设硬断言——内容正确性由对拍承载）。
     */
    @Test
    void adversarialDdlSequenceEndsInParity() throws Exception {
        String slot = registerSlot(VastTestEnv.PREFIX + "it_syn");
        Path stateDir = VastTestEnv.newStateDir("catalog-sync");
        long mainOid = prepareTables();
        currentSync = startVastSync(slot, VastTestEnv.stateConfig(stateDir), 0L, mainOid);
        CatalogSynchronizer sync = currentSync;

        Thread storm = vastInterferenceThread(INTERFERENCE_ROUNDS,
                "vb_wal_adv", "vb_wal_adv_renamed", "vb_wal_adv_toast", "vb_wal_adv_part");
        storm.start();
        long droppedP2Filenode;
        long scopedOid;
        try (Connection c1 = VastTestEnv.newSqlConnection();
             Connection c2 = VastTestEnv.newSqlConnection()) {
            ScenarioOutcome outcome = runDdlScenario(c1, c2);
            droppedP2Filenode = outcome.droppedP2Filenode();
            scopedOid = outcome.scopedOid();
        }
        storm.join(INTERFERENCE_JOIN_MS);
        assertTrue(!storm.isAlive(), "干扰线程应在 join 上限内退出");

        // 确定性收尾：死元组清在目标位点之前 + 两表 autovacuum 关闭 + 尾写取 write 目标
        //（分区母表/分区无 pg_class 存储行——M3 锚"分区子表不在 pg_class"，无需关其 autovacuum）
        long target;
        try (Connection c = VastTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
            st.execute("VACUUM ANALYZE pg_class");
            st.execute("VACUUM ANALYZE pg_attribute");
            st.execute("ALTER TABLE vb_wal_adv_renamed SET (autovacuum_enabled = false)");
            st.execute("ALTER TABLE vb_wal_adv_toast SET (autovacuum_enabled = false)");
            st.execute("ALTER TABLE vb_wal_adv_nsp.scoped SET (autovacuum_enabled = false)");
            st.execute("INSERT INTO vb_wal_adv_tail VALUES (1)");
            target = VastTestEnv.writeLsn(c);
        }
        awaitConsumed(sync, target, "VastBase 干扰 DDL 场景接收前沿到达对拍目标 " + Lsn.format(target));

        long toastOid;
        long motherOid;
        try (Connection c = VastTestEnv.newSqlConnection()) {
            toastOid = oidOf(c, "vb_wal_adv_toast");
            motherOid = oidOf(c, "vb_wal_adv_part");
        }
        sync.stop();
        assertTrue(new StateStore(stateDir, LAYOUT.majorVersion()).load().isPresent(),
                "停机最终检查点应落盘可加载（VastBase 档 majorVersion=" + LAYOUT.majorVersion() + "）");

        try (Connection probe = VastTestEnv.newSqlConnection()) {
            assertVastCatalogMatchesJdbc(sync, probe, mainOid, droppedP2Filenode,
                    mainOid, toastOid, motherOid, scopedOid);
        }
        Map<String, Long> metrics = sync.metrics();
        LOG.info("VastBase 对拍指标 slot={}: 重放 {} / walker {}", slot, metrics, sync.streamMetrics());
        assertTrue(metrics.getOrDefault(CatalogStores.CatalogMetrics.REPLAYED, 0L) > 0,
                "场景应产生 catalog 行事件: " + metrics);
    }

    /**
     * 门面判别链用例：{@link WalSource} 全装配（OG 档 direct 通道 + opengauss-jdbc 前缀，
     * 参数形态照 OG 侧门面全装配用例翻译）在真库上走通
     * <b>判别 → ensureSlotOg 建槽 → OG 种子 SQL 引导（42/29 列超集）→ direct 收流 →
     * 词典重放 → 检查点</b>——接收前沿追平写入目标、接收线程零终态失败，close 落
     * <b>v6 检查点</b>（StateStore 以 pgVersion=6 load 成功 = 判别落点
     * {@code WalLayoutVVastbase} 的可加载证据——误落 OG 5 档即拒载、误落 vanilla 9.2
     * 则 start 期 ISE）。
     */
    @Test
    void walSourceFacadeDiscriminatesVastbaseAndCheckpointsMajorVersionSix() throws Exception {
        String slot = registerSlot(VastTestEnv.PREFIX + "it_src");
        Path stateDir = VastTestEnv.newStateDir("facade");
        Properties cfg = VastTestEnv.params();
        cfg.setProperty(WalSource.KEY_SLOT, slot);
        cfg.setProperty(StateConfig.KEY_DIR, stateDir.toString());
        cfg.setProperty(StateConfig.KEY_EVENTS, "1");
        WalSource source = new WalSource(cfg);
        closables.add(source);
        source.start();
        assertTrue(source.stateDir() != null, "配置了 state.dir 时门面应暴露状态目录");
        try (Connection c = VastTestEnv.newSqlConnection()) {
            execDdl(c,
                    "DROP TABLE IF EXISTS vb_wal_src_smoke",
                    "CREATE TABLE vb_wal_src_smoke (id int primary key, v text)",
                    "INSERT INTO vb_wal_src_smoke VALUES (1, 'a'), (2, 'b')",
                    "ALTER TABLE vb_wal_src_smoke ADD COLUMN extra int",
                    "UPDATE vb_wal_src_smoke SET extra = 3 WHERE id = 1");
            long target = VastTestEnv.writeLsn(c);
            awaitTrue(() -> source.consumedLsn() >= target, "VastBase 门面全管线接收前沿");
        }
        assertTrue(source.receiverTerminalFailure().isEmpty(),
                "门面接收线程不应终态失败（词典几何错位会以解析 ISE 终态暴露——spike 折点复现面）");
        source.close();
        Optional<StoredState> stored = new StateStore(stateDir, LAYOUT.majorVersion()).load();
        assertTrue(stored.isPresent(), "close 的最终检查点应落盘可加载");
        assertTrue(stored.get().lsn() > 0, "检查点 lsn 应为有效位点");
        assertTrue(new StateStore(stateDir, 5).load().isEmpty(),
                "同一检查点以 OG 档 pgVersion=5 load 应拒载——v6 是 VastBase 档判别落点的排他证据");
        LOG.info("WalSource VastBase 门面判别链验收: slot={} v6 检查点 {} 终态前沿 {}",
                slot, Lsn.format(stored.get().lsn()), Lsn.format(source.consumedLsn()));
    }

    /**
     * 场景产出（{@link #runDdlScenario} 的返回面）：分区母表 oid 之外的中途登记面。
     *
     * @param droppedP2Filenode 已 DROP 分区 p2 的 relfilenode（DROP 前捕获——路由归空
     *                          断言的输入）
     * @param scopedOid         新 schema 内表的 oid（schemaOf 对拍面）
     */
    private record ScenarioOutcome(long droppedP2Filenode, long scopedOid) {
    }

    /**
     * 场景前置：清场重建被测主表/尾写表（建表在同步器 start 之前——interest oid 供引导
     * 定位 tracked 双 ctid）；分区表刻意不在此建——CREATE 落在流内（第四字典 INS 面）。
     *
     * @return 主表 vb_wal_adv 的 oid
     * @throws SQLException DDL 失败
     */
    private long prepareTables() throws SQLException {
        VastTestEnv.sql(
                "DROP TABLE IF EXISTS vb_wal_adv",
                "DROP TABLE IF EXISTS vb_wal_adv_renamed",
                "DROP TABLE IF EXISTS vb_wal_adv_toast",
                "DROP TABLE IF EXISTS vb_wal_adv_tail",
                "DROP TABLE IF EXISTS vb_wal_adv_part",
                "DROP SCHEMA IF EXISTS vb_wal_adv_nsp CASCADE",
                "CREATE TABLE vb_wal_adv (id int primary key, payload text)",
                "CREATE TABLE vb_wal_adv_tail (v int)");
        try (Connection c = VastTestEnv.newSqlConnection()) {
            return oidOf(c, "vb_wal_adv");
        }
    }

    /**
     * DDL 场景序列主体（OG 对抗 IT 的翻译 + VastBase 分区面）：死元组三连 → 列增删 →
     * 改名 → 截断二连 → toast 表宽行 + CREATE SCHEMA（nsp 字典面）→ <b>分区面</b>
     * （CREATE 带两分区 + ADD p3 + 路由 INSERT + DROP p2——pg_class 母表行 + pg_partition
     * 第四字典 INS/DEL 双面）→ 双连接交错 DDL → CHECKPOINT×3（后继首写 FPW 页镜像路径）。
     * 每步独立自动提交；分区 DROP 前捕获 p2 relfilenode 供路由归空断言。
     *
     * @param c1 主会话
     * @param c2 交错会话（toast/分区/scoped 段）
     * @return 场景产出（见 {@link ScenarioOutcome}）
     * @throws SQLException         DDL 失败
     * @throws InterruptedException 交错段 join 被中断
     */
    private ScenarioOutcome runDdlScenario(Connection c1, Connection c2)
            throws SQLException, InterruptedException {
        execDdl(c1,
                "INSERT INTO vb_wal_adv SELECT g, 'payload-' || g || '-' || repeat('x', g % 40) FROM generate_series(1, 300) g",
                "UPDATE vb_wal_adv SET payload = payload || '-u' WHERE id % 2 = 0",
                "DELETE FROM vb_wal_adv WHERE id % 3 = 0",
                "ALTER TABLE vb_wal_adv ADD COLUMN extra_tag int",
                "ALTER TABLE vb_wal_adv ADD COLUMN note text DEFAULT 'seed'",
                "ALTER TABLE vb_wal_adv DROP COLUMN payload",
                "ALTER TABLE vb_wal_adv RENAME TO vb_wal_adv_renamed",
                "TRUNCATE vb_wal_adv_renamed",
                "INSERT INTO vb_wal_adv_renamed (id, extra_tag, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                "TRUNCATE vb_wal_adv_renamed");
        // 带 toast 的表 + 宽行（toast 关系流内重建：pg_class INS + 主表行 reltoastrelid 变更）；
        // CREATE SCHEMA + 新 schema 建表（pg_namespace INS 字典面）；interest 集中途登记
        // （CHM 弱一致）——新关系的 pg_class 行立即进入自愈候选域
        execDdl(c2,
                "CREATE TABLE vb_wal_adv_toast (id int primary key, wide text)",
                "INSERT INTO vb_wal_adv_toast SELECT g, repeat('x', 12000) FROM generate_series(1, 20) g",
                "CREATE SCHEMA vb_wal_adv_nsp",
                "CREATE TABLE vb_wal_adv_nsp.scoped (id int, v text)",
                "INSERT INTO vb_wal_adv_nsp.scoped VALUES (1, 'nsp')");
        currentSync.stores().interestRelOids().add(oidOf(c2, "vb_wal_adv_toast"));
        currentSync.stores().interestRelOids().add(oidOf(c2, "vb_wal_adv_nsp.scoped"));
        // 分区面（VastBase 原生分区语法——T0 spike 验证）：CREATE 带两分区 + ADD p3 产生
        // pg_class 母表行（parttype='p'，M2 修复 B 放行入字典）与 pg_partition 'r'+'p' 行
        // （第四字典 INS）；路由 INSERT 落分区自身 relfilenode；DROP p2 是第四字典 DEL 面
        execDdl(c2,
                "CREATE TABLE vb_wal_adv_part (id int, v text) PARTITION BY RANGE(id)"
                        + " (PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN (20))",
                "ALTER TABLE vb_wal_adv_part ADD PARTITION p3 VALUES LESS THAN (30)");
        long motherOid = oidOf(c2, "vb_wal_adv_part");
        currentSync.stores().interestRelOids().add(motherOid);
        long droppedP2Filenode = queryLong(c2,
                "SELECT relfilenode FROM pg_partition WHERE parentid = " + motherOid + " AND relname = 'p2'");
        execDdl(c2,
                "INSERT INTO vb_wal_adv_part VALUES (5, 'pa'), (15, 'pb'), (25, 'pc')",
                "ALTER TABLE vb_wal_adv_part DROP PARTITION p2");
        interleaveDdl(c1, c2);
        // CHECKPOINT×3：后继首写强制 FPW 页镜像路径（页内行提取面）
        try (Statement st = c1.createStatement()) {
            for (int i = 0; i < 3; i++) {
                st.execute("CHECKPOINT");
                Thread.sleep(100);
            }
        }
        return new ScenarioOutcome(droppedP2Filenode, oidOf(c2, "vb_wal_adv_nsp.scoped"));
    }

    /**
     * 两连接交错 DDL：CountDownLatch 双方就绪后同时放行——c1 改主表、c2 改 toast 表，
     * 两表的 pg_attribute/pg_class 行变更在 WAL 流交错下发（同页竞争的乱序覆盖面）。
     * c2 侧失败经 AtomicReference 带回主线程断言（静默吞掉会让场景缺一条腿而假绿）。
     *
     * @param c1 主表会话
     * @param c2 toast 表会话
     * @throws SQLException         主表侧 ALTER 失败
     * @throws InterruptedException 放行等待/join 被中断
     */
    private void interleaveDdl(Connection c1, Connection c2) throws SQLException, InterruptedException {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Exception> sideFailure = new AtomicReference<>();
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                execDdl(c2, "ALTER TABLE vb_wal_adv_toast ADD COLUMN appended text DEFAULT 'w'");
            } catch (Exception e) {
                sideFailure.set(e);
            }
        }, "vast-interleaved-ddl");
        t2.start();
        ready.countDown();
        go.countDown();
        execDdl(c1, "ALTER TABLE vb_wal_adv_renamed ADD COLUMN interleaved int");
        t2.join(10_000);
        assertTrue(!t2.isAlive(), "交错 DDL 线程应在 join 上限内退出");
        assertTrue(sideFailure.get() == null, "交错 DDL 侧（toast 表）不应失败: " + sideFailure.get());
    }

    /**
     * 起一个 VastBase 档同步器（{@code CatalogSynchronizer.startOg} 直连真库 direct 通道
     * ——槽走 ensureSlotOg 两参 dummy_standby 变体、种子 SQL/位点函数 OG 面、字节源
     * {@link DirectWalSource} 无槽裸流，与 OG 侧同步 IT 的通道形态一致；布局直取
     * {@code WalLayoutVVastbase.INSTANCE}——判别链由门面用例单独验收，两用例合璧覆盖
     * 判别→装配→对拍全链）。
     *
     * @param slot           物理槽名
     * @param cfg            检查点配置（null = 禁用）
     * @param forcedStartLsn 显式流起点（0 = 按决策）
     * @param interestOid    tracked 面 oid
     * @return 已运行的同步器（登记进兜底停机面）
     * @throws SQLException 槽管理或引导失败
     */
    private CatalogSynchronizer startVastSync(String slot, StateConfig cfg, long forcedStartLsn,
            long... interestOid) throws SQLException {
        Connection c = VastTestEnv.newSqlConnection();
        conns.add(c);
        CatalogSynchronizer sync = CatalogSynchronizer.startOg(c, slot, LAYOUT, cfg, forcedStartLsn,
                startLsn -> new DirectWalSource(VastTestEnv.host(), VastTestEnv.port(),
                        VastTestEnv.user(), VastTestEnv.pass(), null, DIRECT_STATUS_INTERVAL_MS, startLsn),
                interestOid);
        closables.add(sync::stop);
        return sync;
    }

    /**
     * VastBase 干扰线程工厂（OG 配方的轮次化裁剪：autovacuum toggle + ANALYZE 风暴 +
     * 随机 CHECKPOINT）：独立连接跑固定轮数——逐表 {@code ALTER TABLE SET
     * (autovacuum_enabled)} 真/假交替（合法反复改 pg_class 行）+ {@code ANALYZE}
     * （pg_class INPLACE + 统计写放大——分区母表面顺带翻动 pg_partition 行），每轮以
     * 30% 概率插入 CHECKPOINT（后继首写带 FPW 页镜像路径）。
     *
     * <p>容错语义：单条语句失败（表在场景推进中改名/尚未建/已删的时间窗交错）仅 DEBUG
     * 记录并继续；连接级失败才终止线程并 WARN——干扰不得掩盖主断言，也不得因场景时序
     * 提前退场。</p>
     *
     * @param rounds 轮数
     * @param tables 被测表名集（RENAME 前后名 + toast + 分区母表都传入）
     * @return 未启动线程（调用方 start）
     */
    private Thread vastInterferenceThread(int rounds, String... tables) {
        return new Thread(() -> {
            try (Connection c = VastTestEnv.newSqlConnection()) {
                boolean on = true;
                for (int round = 0; round < rounds; round++) {
                    String toggle = on ? "true" : "false";
                    try (Statement st = c.createStatement()) {
                        for (String tbl : tables) {
                            try {
                                st.execute("ALTER TABLE " + tbl + " SET (autovacuum_enabled = " + toggle + ")");
                                st.execute("ANALYZE " + tbl);
                            } catch (SQLException e) {
                                LOG.debug("干扰语句跳过（{} 场景时序窗口内正常）: {}", tbl, e.getMessage());
                            }
                        }
                        if (Math.random() < CHECKPOINT_PROBABILITY) {
                            try {
                                st.execute("CHECKPOINT");
                            } catch (SQLException e) {
                                LOG.debug("干扰 CHECKPOINT 跳过: {}", e.getMessage());
                            }
                        }
                    } catch (SQLException e) {
                        LOG.debug("干扰轮次跳过: {}", e.getMessage());
                    }
                    on = !on;
                }
                LOG.info("VastBase 干扰线程退出: {} 轮 toggle+ANALYZE(+随机CHECKPOINT) 风暴", rounds);
            } catch (SQLException e) {
                LOG.warn("VastBase 干扰线程连接级失败（风暴提前终止）: {}", e.getMessage());
            }
        }, "vast-catalog-interference");
    }

    /**
     * 对拍主断言（{@code WalSyncItBase#assertCatalogMatchesJdbc} 的语义复刻 + VastBase
     * 第四字典两面）：停流后的快照四字典 vs JDBC REPEATABLE READ 单事务实查。
     *
     * <p>关键步骤：① 两侧各自按基表 oid 收集 pg_class 行并发现 toast oid（并集为对拍
     * 范围——两侧行模型本身含 reltoastrelid，单侧陈旧由 pg_class 行对拍暴露）；② JDBC
     * 侧单 REPEATABLE READ 事务内实查 attr/class/nsp-join 全行（ctid 折键 + 行 record
     * 组装）；③ attr/class 行逐行全等（diff 打印）+ 每基表 relfilenodeOf/toastOf/
     * schemaOf/relkindOf/nameOf 对 JDBC 末态 + 列序严格升序；④ tracked 双 ctid 对 JDBC
     * 现居位；⑤ <b>pg_partition 行级对拍</b>（parentid 圈定本场景分区族）+ 分区身份链
     * {@code partitionOf}（'p' 行路由锚，含已 DROP 分区归空）。边界与异常语义：调用方
     * 须先 {@code stop()}（冻结态对拍）；JDBC 查询无行取空集参与对拍。线程约束：测试
     * 线程单次调用。</p>
     *
     * @param sync              已停流的同步器
     * @param probe             对拍查询会话（本方法临时接管事务边界，结束复原）
     * @param trackedOid        tracked 基表 oid（tracked 双 ctid 校验面，须在 relOids 中）
     * @param droppedP2Filenode 场景中已 DROP 分区的 relfilenode（路由归空断言输入；0 跳过）
     * @param relOids           对拍关系 oid 集（基表，toast 自动并入）
     * @throws SQLException JDBC 实查失败
     */
    private void assertVastCatalogMatchesJdbc(CatalogSynchronizer sync, Connection probe,
            long trackedOid, long droppedP2Filenode, long... relOids) throws SQLException {
        TreeSet<Long> baseOids = new TreeSet<>();
        for (long oid : relOids) {
            baseOids.add(oid);
        }
        // 对拍范围并集：两侧各自发现的 toast oid 都并入（单侧陈旧时由 pg_class 行对拍暴露）
        TreeSet<Long> scope = new TreeSet<>(baseOids);
        scope.addAll(discoverToastOids(sync.stores().classRows(), baseOids));

        boolean oldAutoCommit = probe.getAutoCommit();
        probe.setAutoCommit(false);
        Map<Long, CatalogRow.ClassRow> jdbcClass;
        Map<Long, CatalogRow.AttrRow> jdbcAttr;
        Map<Long, String> jdbcSchemas;
        Map<Long, CatalogRow.PartitionRow> jdbcPartitions;
        try (Statement tx = probe.createStatement()) {
            tx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            jdbcClass = queryClassRows(probe, scope);
            scope.addAll(discoverToastOids(jdbcClass, baseOids));
            jdbcClass = queryClassRows(probe, scope);
            jdbcAttr = queryAttrRows(probe, scope);
            jdbcSchemas = querySchemaNames(probe, scope);
            jdbcPartitions = queryPartitionRows(probe, baseOids);
            probe.commit();
        } finally {
            probe.setAutoCommit(oldAutoCommit);
        }

        Map<Long, CatalogRow.ClassRow> oursClass = new TreeMap<>();
        sync.stores().classRows().forEach((k, v) -> {
            if (scope.contains(v.relOid())) {
                oursClass.put(k, v);
            }
        });
        Map<Long, CatalogRow.AttrRow> oursAttr = new TreeMap<>();
        sync.stores().attrRows().forEach((k, v) -> {
            if (scope.contains(v.attrelid())) {
                oursAttr.put(k, v);
            }
        });
        Map<Long, CatalogRow.PartitionRow> oursPartitions = new TreeMap<>();
        sync.stores().partitionRows().forEach((k, v) -> {
            if (baseOids.contains(v.parentid())) {
                oursPartitions.put(k, v);
            }
        });

        // 诊断先于断言：对拍面两侧规模 + 重放指标全量（失败时日志即证据）
        LOG.info("VastBase 对拍诊断: 字典 class {} / attr {} / partition {} 行（全库）, 对拍范围我方"
                        + " class {} / attr {} / partition {} 行, 指标 {}",
                sync.stores().classRows().size(), sync.stores().attrRows().size(),
                sync.stores().partitionRows().size(), oursClass.size(), oursAttr.size(),
                oursPartitions.size(), sync.metrics());
        assertRowsIdentical("pg_class", oursClass, jdbcClass, "oid 范围 " + scope);
        assertRowsIdentical("pg_attribute", oursAttr, jdbcAttr, "oid 范围 " + scope);
        assertRowsIdentical("pg_partition", oursPartitions, jdbcPartitions, "parentid 范围 " + baseOids);

        CatalogSnapshot snapshot = sync.snapshot();
        List<String> problems = new ArrayList<>();
        for (long oid : baseOids) {
            CatalogRow.ClassRow jdbcRow = firstByOid(jdbcClass, oid);
            if (jdbcRow == null) {
                problems.add("oid " + oid + " 在 JDBC pg_class 无行（关系已删？对拍语义请核对）");
                continue;
            }
            OptionalLong filenode = snapshot.relfilenodeOf(oid);
            if (filenode.isEmpty() || filenode.getAsLong() != jdbcRow.relfilenode()) {
                problems.add("oid " + oid + " relfilenodeOf=" + describe(filenode)
                        + " 期望 " + jdbcRow.relfilenode());
            }
            OptionalLong toast = snapshot.toastOf(oid);
            if (toast.isEmpty() || toast.getAsLong() != jdbcRow.reltoastrelid()) {
                problems.add("oid " + oid + " toastOf=" + describe(toast)
                        + " 期望 " + jdbcRow.reltoastrelid());
            }
            // v2 扩链面：schema（relnamespace→pg_namespace.nspname）/relkind/name 三查询
            // 与 JDBC 末态对拍——CREATE SCHEMA 流内 nsp 字典构建的端到端锚
            Optional<String> schema = snapshot.schemaOf(oid);
            if (schema.isEmpty() || !schema.get().equals(jdbcSchemas.get(oid))) {
                problems.add("oid " + oid + " schemaOf=" + schema + " 期望 " + jdbcSchemas.get(oid));
            }
            Optional<String> relkind = snapshot.relkindOf(oid);
            if (relkind.isEmpty() || !relkind.get().equals(jdbcRow.relkind())) {
                problems.add("oid " + oid + " relkindOf=" + relkind + " 期望 " + jdbcRow.relkind());
            }
            Optional<String> name = snapshot.nameOf(oid);
            if (name.isEmpty() || !name.get().equals(jdbcRow.relname())) {
                problems.add("oid " + oid + " nameOf=" + name + " 期望 " + jdbcRow.relname());
            }
            assertColumnOrderAscending(snapshot, oid);
        }
        // 分区身份链（第四字典的路由面）：'p' 行 relfilenode → partitionOf 恰回该行身份
        // （分区名 + parentid）；已 DROP 分区归空（无陈旧路由）
        for (CatalogRow.PartitionRow row : jdbcPartitions.values()) {
            if (!"p".equals(row.parttype())) {
                continue;
            }
            Optional<CatalogSnapshot.PartitionIdentity> identity = snapshot.partitionOf(row.relfilenode());
            if (identity.isEmpty() || identity.get().parentid() != row.parentid()
                    || !identity.get().relname().equals(row.relname())) {
                problems.add("partitionOf(" + row.relfilenode() + ")=" + identity
                        + " 期望 (relname=" + row.relname() + ", parentid=" + row.parentid() + ")");
            }
        }
        if (droppedP2Filenode > 0 && snapshot.partitionOf(droppedP2Filenode).isPresent()) {
            problems.add("已 DROP 分区 relfilenode=" + droppedP2Filenode
                    + " 的 partitionOf 应归空（陈旧路由）: " + snapshot.partitionOf(droppedP2Filenode));
        }
        if (!problems.isEmpty()) {
            fail("relfilenodeOf/toastOf/schemaOf/partitionOf 对拍失败:\n  " + String.join("\n  ", problems));
        }

        assertTrackedMatchesJdbc(sync, jdbcClass, trackedOid);
        LOG.info("VastBase 对拍全等: 范围 {}（attr {} 行 / class {} 行 / partition {} 行）, tracked {} -> {}",
                scope, oursAttr.size(), oursClass.size(), oursPartitions.size(),
                trackedOid, fmtCtid(sync.stores().trackedTableCtid()));
    }

    /**
     * tracked 双 ctid 校验：tracked 表位须等于该表行的 JDBC 现居 ctid；tracked toast 位
     * 须等于其 toast 关系行的 JDBC 现居 ctid（表无 toast 时期望 0）。
     *
     * @param sync       已停流的同步器
     * @param jdbcClass  JDBC 侧 class 行字典（ctid 键）
     * @param trackedOid tracked 基表 oid
     */
    private static void assertTrackedMatchesJdbc(CatalogSynchronizer sync,
            Map<Long, CatalogRow.ClassRow> jdbcClass, long trackedOid) {
        // 两遍扫描：遍历序不定，toast 关系行可能先于表行出现——首遍配对表行并发现
        // toast oid，次遍再配对 toast 行（单遍的 else-if 会漏配）
        Long jdbcTableCtid = null;
        long toastOid = 0;
        for (Map.Entry<Long, CatalogRow.ClassRow> e : jdbcClass.entrySet()) {
            if (e.getValue().relOid() == trackedOid) {
                jdbcTableCtid = e.getKey();
                toastOid = e.getValue().reltoastrelid();
            }
        }
        Long jdbcToastCtid = null;
        if (toastOid != 0) {
            for (Map.Entry<Long, CatalogRow.ClassRow> e : jdbcClass.entrySet()) {
                if (e.getValue().relOid() == toastOid) {
                    jdbcToastCtid = e.getKey();
                }
            }
        }
        assertTrue(jdbcTableCtid != null, "tracked 表 oid " + trackedOid + " 应在 JDBC 对拍集内");
        assertEquals(jdbcTableCtid.longValue(), sync.stores().trackedTableCtid(),
                "trackedTableCtid 须等于 JDBC 现居 ctid " + fmtCtid(jdbcTableCtid));
        long expectToast = jdbcToastCtid == null ? 0L : jdbcToastCtid;
        assertEquals(expectToast, sync.stores().trackedToastCtid(),
                "trackedToastCtid 须等于 toast 关系 JDBC 现居 ctid（无 toast 期望 0）");
    }

    /**
     * 列序断言：{@code columnsOf} 返回的 attnum 严格升序且无重复（dropped 占位保留在
     * 其原键位——列内容的正确性由全等对拍覆盖，此处钉住次序面）。
     *
     * @param snapshot 快照视图
     * @param relOid   关系 oid
     */
    private static void assertColumnOrderAscending(CatalogSnapshot snapshot, long relOid) {
        int prev = 0;
        for (CatalogSnapshot.Column col : snapshot.columnsOf(relOid)) {
            assertTrue(col.attnum() > prev,
                    "oid " + relOid + " 列序须严格升序: attnum " + col.attnum() + " 未越过 " + prev);
            prev = col.attnum();
        }
    }

    /**
     * 逐行全等断言（diff 打印——可诊断性）：键并集逐项比对，仅一侧有键、或双侧值不等
     * 各打一行差异（ctid 键 + 双方行值），汇总后 fail。
     *
     * @param catalog 目录名（失败消息面）
     * @param ours    快照侧行字典（ctid 键）
     * @param actual  JDBC 侧行字典（ctid 键）
     * @param context 失败消息上下文（范围描述）
     * @param <T>     行模型类型（record 值等价）
     */
    private static <T> void assertRowsIdentical(String catalog, Map<Long, T> ours,
            Map<Long, T> actual, String context) {
        if (ours.equals(actual)) {
            return;
        }
        List<String> lines = new ArrayList<>();
        TreeSet<Long> keys = new TreeSet<>(ours.keySet());
        keys.addAll(actual.keySet());
        for (Long k : keys) {
            T o = ours.get(k);
            T a = actual.get(k);
            if (o == null) {
                lines.add("  仅 JDBC 侧 " + fmtCtid(k) + " -> " + a);
            } else if (a == null) {
                lines.add("  仅快照侧 " + fmtCtid(k) + " -> " + o);
            } else if (!o.equals(a)) {
                lines.add("  字段不一致 " + fmtCtid(k) + ":\n    快照=" + o + "\n    JDBC =" + a);
            }
        }
        fail(catalog + " 对拍不一致（" + context + "; 快照 " + ours.size() + " 行 / JDBC "
                + actual.size() + " 行）——差异行:\n" + String.join("\n", lines));
    }

    /**
     * JDBC 实查 pg_class 全行（对拍范围 oid 集；显式列名投影——列序差异免疫）。
     *
     * @param c    会话连接（REPEATABLE READ 事务内）
     * @param oids 对拍 oid 集
     * @return ctid 键 → ClassRow
     * @throws SQLException 查询失败
     */
    private static Map<Long, CatalogRow.ClassRow> queryClassRows(Connection c, TreeSet<Long> oids)
            throws SQLException {
        Map<Long, CatalogRow.ClassRow> out = new TreeMap<>();
        String sql = "SELECT ctid::text, oid, relname, relnamespace, reltype, reloftype,"
                + " relowner, relam, relfilenode, reltoastrelid, relkind::text"
                + " FROM pg_class WHERE oid IN " + inList(oids);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.put(parseCtidKey(rs.getString(1)), new CatalogRow.ClassRow(
                        rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getLong(5), rs.getLong(6),
                        rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getLong(10), rs.getString(11)));
            }
        }
        return out;
    }

    /**
     * JDBC 实查 pg_attribute 全行（attnum&gt;0，含 dropped 占位）。
     *
     * @param c    会话连接（REPEATABLE READ 事务内）
     * @param oids 对拍 oid 集
     * @return ctid 键 → AttrRow
     * @throws SQLException 查询失败
     */
    private static Map<Long, CatalogRow.AttrRow> queryAttrRows(Connection c, TreeSet<Long> oids)
            throws SQLException {
        Map<Long, CatalogRow.AttrRow> out = new TreeMap<>();
        String sql = "SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                + " FROM pg_attribute WHERE attnum > 0 AND attrelid IN " + inList(oids);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.put(parseCtidKey(rs.getString(1)), new CatalogRow.AttrRow(
                        rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getInt(5), rs.getBoolean(6)));
            }
        }
        return out;
    }

    /**
     * JDBC 实查对拍范围的 schema 名（pg_class⋈pg_namespace——nsp 字典面的 JDBC 末态源）。
     *
     * @param c    连接（REPEATABLE READ 事务内）
     * @param oids 对拍 oid 集
     * @return oid → nspname
     * @throws SQLException 查询失败
     */
    private static Map<Long, String> querySchemaNames(Connection c, TreeSet<Long> oids) throws SQLException {
        Map<Long, String> out = new TreeMap<>();
        String sql = "SELECT c.oid, n.nspname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid IN " + inList(oids);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.put(rs.getLong(1), rs.getString(2));
            }
        }
        return out;
    }

    /**
     * JDBC 实查 pg_partition 全行（第四字典对拍面，parentid 圈定本场景分区族——真库
     * 背景活动的他表分区行不进断言面；投影列 = 第四字典种子 SQL 同面）。
     *
     * @param c        连接（REPEATABLE READ 事务内）
     * @param baseOids 分区母表候选 oid 集（非母表 oid 无行）
     * @return ctid 键 → PartitionRow
     * @throws SQLException 查询失败
     */
    private static Map<Long, CatalogRow.PartitionRow> queryPartitionRows(Connection c, TreeSet<Long> baseOids)
            throws SQLException {
        Map<Long, CatalogRow.PartitionRow> out = new TreeMap<>();
        String sql = "SELECT ctid::text, oid, relname, parttype::text, parentid, relfilenode,"
                + " reltoastrelid FROM pg_partition WHERE parentid IN " + inList(baseOids);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.put(parseCtidKey(rs.getString(1)), new CatalogRow.PartitionRow(
                        rs.getLong(2), rs.getString(3), rs.getString(4), rs.getLong(5),
                        rs.getLong(6), rs.getLong(7)));
            }
        }
        return out;
    }

    /**
     * class 行字典中发现 toast oid（基表行的 reltoastrelid 非 0 值；快照/JDBC 两侧同式，
     * 并集后由 pg_class 行对拍暴露任何单侧陈旧）。
     *
     * @param classRows 行字典（任意一侧）
     * @param baseOids  基表 oid 集
     * @return 发现的 toast oid 集（不含基表自身）
     */
    private static TreeSet<Long> discoverToastOids(Map<Long, CatalogRow.ClassRow> classRows,
            TreeSet<Long> baseOids) {
        TreeSet<Long> out = new TreeSet<>();
        for (CatalogRow.ClassRow row : classRows.values()) {
            if (baseOids.contains(row.relOid()) && row.reltoastrelid() != 0) {
                out.add(row.reltoastrelid());
            }
        }
        return out;
    }

    /**
     * 按 oid 取首行（oid 在对拍集内唯一，首配即全配）。
     *
     * @param rows 行字典（任意一侧）
     * @param oid  关系 oid
     * @return 行模型；无行 null
     */
    private static CatalogRow.ClassRow firstByOid(Map<Long, CatalogRow.ClassRow> rows, long oid) {
        for (CatalogRow.ClassRow row : rows.values()) {
            if (row.relOid() == oid) {
                return row;
            }
        }
        return null;
    }

    /**
     * 顺序执行 DDL/SQL 语句（各自独立自动提交，任一失败即抛）。
     *
     * @param c          会话连接
     * @param statements 语句列表
     * @throws SQLException 任一语句失败（已执行的不回滚）
     */
    private static void execDdl(Connection c, String... statements) throws SQLException {
        try (Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
                LOG.debug("DDL 执行: {}", sql);
            }
        }
    }

    /**
     * 单值 long 查询便捷档（恰返一行一列）。
     *
     * @param c   会话连接
     * @param sql 查询
     * @return 首行首列值
     * @throws SQLException 查询失败
     */
    private static long queryLong(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * 表名解析 oid（regclass 转换——表不存在时异常上抛，调用方保证已建；schema 限定名
     * 同样可解析）。
     *
     * @param c         会话连接
     * @param tableName 表名（可 schema 限定）
     * @return 关系 oid
     * @throws SQLException 查询失败
     */
    private static long oidOf(Connection c, String tableName) throws SQLException {
        return queryLong(c, "SELECT '" + tableName + "'::regclass::oid");
    }

    /**
     * 轮询等待同步器消费前沿到达目标位点（100ms 节拍，真库档 60s 超时 fail）。
     *
     * @param sync    已运行的同步器
     * @param target  目标位点（含）
     * @param message 超时失败消息描述
     */
    private static void awaitConsumed(CatalogSynchronizer sync, long target, String message) {
        awaitTrue(() -> sync.consumedLsn() >= target, message);
        LOG.info("接收前沿到达 {}（快照前沿 {}）", Lsn.format(target), Lsn.format(sync.snapshot().lsn()));
    }

    /**
     * 轮询等待条件成立（100ms 节拍，{@value #POLL_TIMEOUT_MS}ms 超时 fail）。
     *
     * @param cond    等待条件
     * @param message 超时失败消息描述
     */
    private static void awaitTrue(BooleanSupplier cond, String message) {
        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + message);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("等待被中断: " + message);
            }
        }
    }

    /**
     * 登记一个独立槽名（前置清理残留 + 记账进兜底删除面）。
     *
     * @param prefix 槽名前缀（后接自增序号）
     * @return 本用例唯一的槽名
     */
    private String registerSlot(String prefix) {
        String slot = prefix + "_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        VastTestEnv.dropPhysicalSlotQuietly(slot);
        return slot;
    }

    /**
     * 兜底清理之一（测试对象面）：DROP 本 IT 全部场景表/schema（含 RENAME 后名与
     * 分区表——分区随母表级联删除）——真库卫生纪律；正常路径测试内不再单独 DROP（本钩子是
     * 唯一清理面，异常路径与正常路径都走这里）；单语失败 WARN 不掩盖后续清理步。
     */
    @AfterEach
    void cleanupVastObjects() {
        try {
            VastTestEnv.sql(
                    "DROP TABLE IF EXISTS vb_wal_adv",
                    "DROP TABLE IF EXISTS vb_wal_adv_renamed",
                    "DROP TABLE IF EXISTS vb_wal_adv_toast",
                    "DROP TABLE IF EXISTS vb_wal_adv_tail",
                    "DROP TABLE IF EXISTS vb_wal_adv_part",
                    "DROP TABLE IF EXISTS vb_wal_src_smoke",
                    "DROP SCHEMA IF EXISTS vb_wal_adv_nsp CASCADE");
        } catch (SQLException e) {
            LOG.warn("清理 VastBase 场景表/schema 失败（忽略）: {}", e.getMessage());
        }
    }

    /**
     * 兜底清理之二（管线面）：停全部同步器/门面 → 关全部引导会话 → 删全部槽（正常路径测试内已完成，
     * 这里是异常路径保险；停机次序即依赖次序：字节源先于会话消亡）。
     */
    @AfterEach
    void cleanupVast() {
        for (AutoCloseable c : closables) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.warn("兜底停机失败（忽略）: {}", e.getMessage());
            }
        }
        for (Connection c : conns) {
            try {
                c.close();
            } catch (SQLException e) {
                LOG.warn("关闭 VastBase 引导会话失败: {}", e.getMessage());
            }
        }
        for (String s : slots) {
            VastTestEnv.dropPhysicalSlotQuietly(s);
        }
    }

    /**
     * oid 集渲染 SQL IN 列表（集合非空由调用方场景保证——relOids 至少含基表）。
     *
     * @param oids oid 集
     * @return 形如 {@code (1,2,3)} 的列表文本
     */
    private static String inList(TreeSet<Long> oids) {
        return "(" + String.join(",", oids.stream().map(String::valueOf).toList()) + ")";
    }

    /**
     * PG 文本 ctid "(block,off)" 折为 ctid 键（委托主代码 {@link CatalogReplay#ctidKey}
     * ——快照侧字典与本侧 JDBC 实查的键控同源，杜绝两套折键式的漂移面）。
     *
     * @param ctid 服务端文本形态
     * @return ctid 键
     */
    private static long parseCtidKey(String ctid) {
        String[] parts = ctid.replaceAll("[() ]", "").split(",");
        return CatalogReplay.ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    /**
     * ctid 键的可读形态（diff 打印面）。
     *
     * @param key ctid 键
     * @return 形如 {@code (block 3, off 12)} 的文本
     */
    private static String fmtCtid(long key) {
        return "(block " + (key >>> 16) + ", off " + (key & 0xFFFF) + ")";
    }

    /**
     * OptionalLong 的诊断渲染。
     *
     * @param v 待渲染值
     * @return empty 显示 "empty"，否则数值
     */
    private static String describe(OptionalLong v) {
        return v.isPresent() ? String.valueOf(v.getAsLong()) : "empty";
    }
}
