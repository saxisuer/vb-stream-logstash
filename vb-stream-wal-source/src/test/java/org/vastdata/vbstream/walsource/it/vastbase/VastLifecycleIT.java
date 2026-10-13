package org.vastdata.vbstream.walsource.it.vastbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVVastbase;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;
import org.vastdata.vbstream.walsource.state.StateStore;
import org.vastdata.vbstream.walsource.state.StoredState;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * VastBase G100 生命周期 IT（手动档 {@code -Dvb.it.vastbase=true}，vastbase T7——收官
 * 验收）：检查点续传（stop → 剩余 DDL → 同 stateDir 续传对拍）/ state 损坏回落（截断
 * 检查点 3 字节 → load 拒载 → 重启全新引导）/ {@link WalSource} 门面全装配
 * （og.source=direct + opengauss-jdbc 前缀）两轮启停冒烟——{@code OgLifecycleIT} 三用例
 * 的真库翻译（对拍断言复用 {@link VastCatalogSyncIT#assertVastCatalogMatchesJdbc}——
 * 传 {@code droppedP2Filenode=0} 跳过分区归空面：本 IT 场景无分区表）。
 *
 * <p><strong>write 位点续传语义（沿 OG 档观察面）</strong>：VastBase 无 flush 档位点
 * 函数（实测 {@code pg_current_wal_flush_lsn()} 不存在，{@link VastTestEnv} 环境锚）——
 * 引导 B 与对拍目标前沿均取 {@code pg_current_xlog_location()}（write 位点）。理论窗口
 * 比 vanilla 略窄一档（write ≥ flush，种子快照时点 S ≥ B(write)），服务端物理流仍自
 * flushed 数据起发——续传正确性由对拍断言承载（窗口丢失即对拍红，不放宽断言压绿）；
 * 槽推进面按观察记档（{@code observeSlotAdvance}——restart_lsn 保留行为不设硬断言，
 * 推进报告成功时才升级为一致性断言）。真库为长活共享环境（背景活动在场），续传起点
 * 恰等断言（{@code snapshot().lsn() == storedLsn}）在 start 返回后微秒窗内读取——
 * 背景记录抵达前完成，实测稳定。</p>
 *
 * <p><strong>真库纪律</strong>：表/槽 {@code vb_wal_} 前缀 + finally/@AfterEach 清理；
 * state 目录 = {@code target/vastbase-it/} 下独立子目录 + 用例级 wipe（Windows 纪律，
 * {@link VastTestEnv#newStateDir}）；不触碰既有业务对象。</p>
 */
@EnabledIfSystemProperty(named = "vb.it.vastbase", matches = "true")
class VastLifecycleIT {

    private static final Logger LOG = LoggerFactory.getLogger(VastLifecycleIT.class);

    /** VastBase 档布局单例（记录头与 OG 5.0.0 同构，heap UPDATE/DELETE 前缀 21/19 除外，词典几何 name128——判别在门面探测）。 */
    private static final WalLayout LAYOUT = WalLayoutVVastbase.INSTANCE;

    /** direct 通道反馈心跳的 statusInterval（毫秒）——对齐 OG 侧同步 IT 的 5s 档。 */
    private static final long DIRECT_STATUS_INTERVAL_MS = 5_000;

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

    /**
     * 场景 ①：半程 DDL → stop（含最终检查点 + 槽推进观测）→ 同 stateDir 续传 →
     * 剩余 DDL（含 RENAME / TRUNCATE 二连 INPLACE relfilenode / DROP COLUMN dropped
     * 占位）→ 对拍全等；续传起点恰为 stored lsn、重启后前沿不回退。
     */
    @Test
    void checkpointResumeAcrossRestartReplaysRemainingDdlIdentical() throws Exception {
        String slot = registerSlot(VastTestEnv.PREFIX + "it_life");
        Path stateDir = VastTestEnv.newStateDir("life-checkpoint");
        long mainOid = prepareTables("vb_wal_life");
        StateStore store = new StateStore(stateDir, LAYOUT.majorVersion());
        // events 阈值压到 1：任一 catalog 行事件即触发——运行中周期检查点必然先行覆盖
        StateConfig cfg = VastTestEnv.stateConfig(stateDir);

        CatalogSynchronizer sync1 = startVastSync(slot, cfg, 0L, mainOid);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO vb_wal_life SELECT g, 'p-' || g || '-' || repeat('x', g % 40)"
                            + " FROM generate_series(1, 200) g",
                    "UPDATE vb_wal_life SET payload = payload || '-u' WHERE id % 2 = 0",
                    "DELETE FROM vb_wal_life WHERE id % 3 = 0",
                    "ALTER TABLE vb_wal_life ADD COLUMN extra int",
                    "ALTER TABLE vb_wal_life ADD COLUMN note text DEFAULT 'seed'",
                    "INSERT INTO vb_wal_life_tail VALUES (1)");
            long target = VastTestEnv.writeLsn(c);
            awaitConsumed(sync1, target, "VastBase 半程 DDL 接收前沿");
        }
        awaitTrue(store::exists, "运行中周期检查点应已落盘（events 阈值 1——任一 catalog 行事件即触发）");

        sync1.stop();
        Optional<StoredState> stored = store.load();
        assertTrue(stored.isPresent(), "停机后应存在可加载的完整检查点");
        long storedLsn = stored.get().lsn();
        assertTrue(storedLsn > 0, "检查点 lsn 应为有效位点");
        assertEquals(storedLsn, sync1.lastCheckpointLsn(), "最终检查点 lsn 与落盘值一致");
        observeSlotAdvance(slot, sync1, storedLsn);

        // 续传：新会话 + 新同步器实例，同槽同 stateDir（write 位点语义观察面见类 javadoc）
        CatalogSynchronizer sync2 = startVastSync(slot, cfg, 0L, mainOid);
        assertTrue(sync2.resumedFromState(), "存在有效检查点时应走续传（跳过引导）");
        assertEquals(storedLsn, sync2.snapshot().lsn(),
                "续传起点应恰为 stored lsn（引导路径的种子是引导时刻 write LSN，二者可区分）");

        try (Connection c = VastTestEnv.newSqlConnection()) {
            // 含 RENAME（读区回填修复的回归面）与 TRUNCATE 二连（INPLACE relfilenode）
            execDdl(c,
                    "ALTER TABLE vb_wal_life DROP COLUMN payload",
                    "ALTER TABLE vb_wal_life RENAME TO vb_wal_life_renamed",
                    "TRUNCATE vb_wal_life_renamed",
                    "INSERT INTO vb_wal_life_renamed (id, extra, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                    "ALTER TABLE vb_wal_life_renamed ADD COLUMN late_col int",
                    "ALTER TABLE vb_wal_life_renamed SET (autovacuum_enabled = false)");
            // 确定性收尾（同对拍 IT）：死元组清在目标位点之前 + 尾写取 write 目标
            execDdl(c, "VACUUM ANALYZE pg_class", "VACUUM ANALYZE pg_attribute");
            execDdl(c, "INSERT INTO vb_wal_life_tail VALUES (2)");
            long target = VastTestEnv.writeLsn(c);
            awaitConsumed(sync2, target, "VastBase 剩余 DDL 接收前沿");
        }
        assertTrue(sync2.consumedLsn() >= storedLsn, "重启后消费前沿不应回退到 stored lsn 之前");
        sync2.stop();

        try (Connection probe = VastTestEnv.newSqlConnection()) {
            VastCatalogSyncIT.assertVastCatalogMatchesJdbc(sync2, probe, mainOid, 0, mainOid);
        }
        LOG.info("VastBase 检查点续传对拍全等: stored {} -> 终态 {} 指标 {} / walker {}",
                Lsn.format(storedLsn), Lsn.format(sync2.snapshot().lsn()),
                sync2.metrics(), sync2.streamMetrics());
    }

    /**
     * 场景 ②：状态文件截断 3 字节 → load 拒载（全文件 CRC 必然失配的行为断言）→
     * 重启走全新引导 → 剩余 DDL → 对拍仍全等（安全侧：宁可重引导，不可错位窗口重放）。
     */
    @Test
    void truncatedStateFileFallsBackToFreshBootstrapAndStillMatches() throws Exception {
        String slot = registerSlot(VastTestEnv.PREFIX + "it_trunc");
        Path stateDir = VastTestEnv.newStateDir("life-truncated");
        long mainOid = prepareTables("vb_wal_trunc");
        StateStore store = new StateStore(stateDir, LAYOUT.majorVersion());
        StateConfig cfg = VastTestEnv.stateConfig(stateDir);

        CatalogSynchronizer sync1 = startVastSync(slot, cfg, 0L, mainOid);
        try (Connection c = VastTestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO vb_wal_trunc SELECT g, 'p-' || g FROM generate_series(1, 100) g",
                    "ALTER TABLE vb_wal_trunc ADD COLUMN extra int",
                    "INSERT INTO vb_wal_trunc_tail VALUES (1)");
            long target = VastTestEnv.writeLsn(c);
            awaitConsumed(sync1, target, "VastBase 首段 DDL 接收前沿");
        }
        sync1.stop();
        assertTrue(store.load().isPresent(), "停机检查点应可加载");

        // 截断 3 字节：全文件 CRC 必然失配 → load 拒载回落 empty
        Path stateFile = stateDir.resolve(StateStore.FILE_NAME);
        byte[] bytes = Files.readAllBytes(stateFile);
        Files.write(stateFile, Arrays.copyOf(bytes, bytes.length - 3));
        assertTrue(new StateStore(stateDir, LAYOUT.majorVersion()).load().isEmpty(),
                "截断后的检查点应拒载（load 返回 empty）");

        CatalogSynchronizer sync2 = startVastSync(slot, cfg, 0L, mainOid);
        assertFalse(sync2.resumedFromState(), "检查点拒载时应走全新引导");
        assertTrue(sync2.snapshot().lsn() > 0, "引导路径的种子前沿应为引导时刻 write LSN");

        try (Connection c = VastTestEnv.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE vb_wal_trunc DROP COLUMN payload",
                    "TRUNCATE vb_wal_trunc",
                    "INSERT INTO vb_wal_trunc (id, extra) VALUES (7, 7)",
                    "ALTER TABLE vb_wal_trunc SET (autovacuum_enabled = false)",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "INSERT INTO vb_wal_trunc_tail VALUES (2)");
            long target = VastTestEnv.writeLsn(c);
            awaitConsumed(sync2, target, "VastBase 回落引导后剩余 DDL 接收前沿");
        }
        sync2.stop();
        assertTrue(store.load().isPresent(), "回落引导会话的停机检查点应重新落盘且可加载");

        try (Connection probe = VastTestEnv.newSqlConnection()) {
            VastCatalogSyncIT.assertVastCatalogMatchesJdbc(sync2, probe, mainOid, 0, mainOid);
        }
    }

    /**
     * 场景 ③（门面装配缝的端到端验收）：{@link WalSource} 带 VastBase 档配置
     * （{@code vb.wal.og.source=direct} + {@code vb.wal.og.jdbc.prefix=jdbc:opengauss:}
     * + state 三键，经 {@link VastTestEnv#params()} 定形）起全管线（判别 → ensureSlotOg
     * 建槽 → OG 种子 SQL 引导[42/29 列超集投影] → direct 字节源 → 检查点 + 槽推进），
     * close 最终检查点落盘可加载（nsp 种子段含 public），同配置再起一轮（续传形态）
     * 起停干净——判别落点的 v6 排他证据归 {@code VastCatalogSyncIT} 门面用例，本用例
     * 聚焦生命周期面。
     */
    @Test
    void walSourceFacadeDirectChannelCheckpointsAndResumes() throws Exception {
        String slot = registerSlot(VastTestEnv.PREFIX + "it_src");
        Path stateDir = VastTestEnv.newStateDir("life-facade");
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
            awaitTrue(() -> source.consumedLsn() >= target, "WalSource VastBase 全管线接收前沿");
        }
        assertTrue(source.receiverTerminalFailure().isEmpty(),
                "门面接收线程不应终态失败（词典几何错位会以解析 ISE 终态暴露）");
        source.close();
        Optional<StoredState> stored = new StateStore(stateDir, LAYOUT.majorVersion()).load();
        assertTrue(stored.isPresent(), "close 的最终检查点应落盘可加载");
        assertTrue(stored.get().nspRows().values().stream().anyMatch(r -> "public".equals(r.nspname())),
                "检查点 nsp 段应含 public 模式（bootstrap 种子 + v2 序列化）");

        // 同配置第二轮：续传形态起停干净（启停面回归，不重复对拍）
        WalSource second = new WalSource(cfg);
        closables.add(second);
        second.start();
        try (Connection c = VastTestEnv.newSqlConnection()) {
            execDdl(c, "INSERT INTO vb_wal_src_smoke VALUES (3, 'c')");
            long target = VastTestEnv.writeLsn(c);
            awaitTrue(() -> second.consumedLsn() >= target, "第二轮续传接收前沿");
        }
        second.close();
        assertTrue(second.consumedLsn() >= stored.get().lsn(), "第二轮前沿不应回退到检查点之前");
        LOG.info("WalSource VastBase 门面两轮启停完成: 首轮检查点 {} 终态前沿 {}",
                Lsn.format(stored.get().lsn()), Lsn.format(second.consumedLsn()));
    }

    // ---- 测试基建 ----

    /**
     * 场景前置：清场重建被测主表与尾写表，返回主表 oid（建表在同步器 start 之前
     * ——interest oid 供引导定位 tracked 双 ctid）。
     *
     * @param table 被测主表名（{@code vb_wal_} 前缀；尾写表名 = 主表名 + "_tail"）
     * @return 主表 oid
     * @throws SQLException DDL 失败
     */
    private long prepareTables(String table) throws SQLException {
        VastTestEnv.sql(
                "DROP TABLE IF EXISTS " + table,
                "DROP TABLE IF EXISTS " + table + "_renamed",
                "DROP TABLE IF EXISTS " + table + "_tail",
                "CREATE TABLE " + table + " (id int primary key, payload text)",
                "CREATE TABLE " + table + "_tail (v int)");
        try (Connection c = VastTestEnv.newSqlConnection()) {
            return oidOf(c, table);
        }
    }

    /**
     * 起一个 VastBase 档同步器（{@code CatalogSynchronizer.startOg} 直连真库 direct 通道
     * ——槽走 ensureSlotOg 两参 dummy_standby 变体、种子 SQL/位点函数 OG 面、字节源
     * {@link DirectWalSource} 无槽裸流，与 {@code VastCatalogSyncIT} 的通道形态一致；
     * 布局直取 {@code WalLayoutVVastbase.INSTANCE}——判别链由门面用例验收）。
     *
     * @param slot           物理槽名
     * @param cfg            检查点配置
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
     * 观测槽推进面（沿 OG 档 {@code OgSyncItBase#observeSlotAdvance} 形态——不硬断言）：
     * 读当前 restart_lsn 与检查点 lsn 对比记档；同步器报告推进成功
     * （lastSlotAdvanceLsn 到位）时才做一致性断言（客户端视角与服务端状态背离即异常）。
     *
     * @param slot         槽名
     * @param sync         刚停机的同步器（推进观测源）
     * @param checkpointLsn 最终检查点 lsn
     * @throws SQLException 槽查询失败
     */
    private static void observeSlotAdvance(String slot, CatalogSynchronizer sync, long checkpointLsn)
            throws SQLException {
        String restart = null;
        try (Connection c = VastTestEnv.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slot + "'")) {
            if (rs.next()) {
                restart = rs.getString(1);
            }
        }
        long advanced = sync.lastSlotAdvanceLsn();
        LOG.info("VastBase 槽推进观测: slot={} checkpoint={} lastAdvance={} restart_lsn={}",
                slot, Lsn.format(checkpointLsn), Lsn.format(advanced), restart);
        if (advanced >= checkpointLsn && checkpointLsn > 0 && restart != null) {
            assertTrue(Lsn.parse(restart) >= checkpointLsn,
                    "推进已报告成功但 restart_lsn 未落位: " + restart + " < " + Lsn.format(checkpointLsn));
        }
    }

    /**
     * 顺序执行 DDL/SQL 语句（各自独立自动提交，任一失败即抛——已执行语句不回滚，
     * 清理路径用 DROP IF EXISTS 幂等吸收）。
     *
     * @param c          会话连接
     * @param statements 语句列表
     * @throws SQLException 任一语句失败
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
     * 表名解析 oid（regclass 转换——表不存在时异常上抛，调用方保证已建）。
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
     * 兜底清理之一（测试对象面）：DROP 本 IT 全部场景表（含 RENAME 后名）——真库卫生
     * 纪律；正常路径不单独 DROP（本钩子是唯一清理面）；单语失败 WARN 不掩盖后续清理步。
     */
    @AfterEach
    void cleanupVastObjects() {
        try {
            VastTestEnv.sql(
                    "DROP TABLE IF EXISTS vb_wal_life",
                    "DROP TABLE IF EXISTS vb_wal_life_renamed",
                    "DROP TABLE IF EXISTS vb_wal_life_tail",
                    "DROP TABLE IF EXISTS vb_wal_trunc",
                    "DROP TABLE IF EXISTS vb_wal_trunc_tail",
                    "DROP TABLE IF EXISTS vb_wal_src_smoke");
        } catch (SQLException e) {
            LOG.warn("清理 VastBase 生命周期场景表失败（忽略）: {}", e.getMessage());
        }
    }

    /**
     * 兜底清理之二（管线面）：停全部同步器/门面 → 关全部引导会话 → 删全部槽（正常路径
     * 测试内已完成，这里是异常路径保险；停机次序即依赖次序：字节源先于会话消亡）。
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
}
