package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.layout.Lsn;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 生命周期 IT（Task 15，spec §8 生命周期行）——检查点续传 / 损坏回落全新引导 /
 * 位点不回退 / 丢页跳变自愈 / 门面全装配 / <strong>引导窗口并发 DDL 不丢行</strong>
 * （终审 C1）六形态，全部复用 {@link WalSyncItBase} 的场景工具与
 * 停流对拍器（ctid 键控逐行全等）。状态目录用 {@link TempDir} 每用例独立。
 *
 * <p>场景对位：① 半程 DDL → stop（含最终检查点 + 槽推进）→ 新同步器同目录续传
 * （运行中周期检查点由 {@code events} 阈值压低驱动先行覆盖）→ 剩余 DDL → 对拍
 * 全等；② 状态文件截断 3 字节 → load 拒载（行为断言）→ 重启走全新引导 → 对拍
 * 仍全等；③ 续传起点恰为 stored lsn（{@code snapshot().lsn()} 种子断言——不回退
 * 不跳段，引导路径的种子会是引导时刻 flush LSN，二者可区分）；④ 丢页注入——
 * 检查点一致点之后跳过 3 页起新流（gap 内 watched 目录变更全部丢失），后续 DDL
 * 驱动 watched 目录记录，验证"丢失后 watched 后续事件把 catalog <strong>末态</strong>追平"
 * （末态回填语义，开放风险验证：失败即风险证实，如实记录）。</p>
 */
class WalLifecycleIT extends WalSyncItBase {

    private static final Logger LOG = LoggerFactory.getLogger(WalLifecycleIT.class);

    /** 跨用例的槽名序号（每用例独立槽，finally 统一清理）。 */
    private static final AtomicLong SLOT_SEQ = new AtomicLong();

    /** 本用例状态目录（每用例独立临时目录）。 */
    @TempDir
    Path stateDir;

    /** 本用例启动的同步器——AfterEach 兜底停机。 */
    private final List<CatalogSynchronizer> syncs = new ArrayList<>();

    /** 本用例持有的引导会话（healer probe / 槽推进复用）——AfterEach 兜底关闭。 */
    private final List<Connection> conns = new ArrayList<>();

    /** 本用例用过的槽——AfterEach 兜底删除。 */
    private final List<String> slots = new ArrayList<>();

    /**
     * 兜底清理：停全部同步器 → 关全部引导会话 → 删全部槽（正常路径测试内已完成，
     * 这里是异常路径保险；停机次序即依赖次序：probe 先于会话消亡）。
     */
    @AfterEach
    void cleanup() {
        for (CatalogSynchronizer s : syncs) {
            s.stop();
        }
        for (Connection c : conns) {
            try {
                c.close();
            } catch (SQLException e) {
                LOG.warn("关闭引导会话失败: {}", e.getMessage());
            }
        }
        for (String s : slots) {
            Interference.dropPhysicalSlotQuietly(s);
        }
    }

    /**
     * 场景 ①+③：半程 DDL → stop（最终检查点 + 槽推进）→ 同 stateDir 续传 → 剩余
     * DDL → 对拍全等；续传起点恰为 stored lsn、重启后前沿不回退。
     */
    @Test
    void checkpointResumeAcrossRestartReplaysRemainingDdlIdentical() throws Exception {
        String slot = "wal_life_it_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Interference.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_life");
        StateStore store = new StateStore(stateDir, layout.majorVersion());
        // events 阈值压到 1：任一 catalog 行事件即触发——运行中周期检查点必然先行覆盖
        // （replayed 计数只算 watched 目录的行事件，轻量 DDL 不足以越过默认 1000）
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startSync(slot, cfg, mainOid);
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO t_life SELECT g, 'p-' || g || '-' || repeat('x', g % 40) FROM generate_series(1, 200) g",
                    "UPDATE t_life SET payload = payload || '-u' WHERE id % 2 = 0",
                    "DELETE FROM t_life WHERE id % 3 = 0",
                    "ALTER TABLE t_life ADD COLUMN extra int",
                    "ALTER TABLE t_life ADD COLUMN note text DEFAULT 'seed'",
                    "INSERT INTO t_life_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync1, target, "半程 DDL 接收前沿");
        }
        awaitTrue(store::exists, "运行中周期检查点应已落盘（events 阈值 1——任一 catalog 行事件即触发）");

        sync1.stop();
        Optional<StoredState> stored = store.load();
        assertTrue(stored.isPresent(), "停机后应存在可加载的完整检查点");
        long storedLsn = stored.get().lsn();
        assertTrue(storedLsn > 0, "检查点 lsn 应为有效位点");
        assertEquals(storedLsn, sync1.lastCheckpointLsn(), "最终检查点 lsn 与落盘值一致");
        assertSlotAdvancedTo(slot, storedLsn);

        // 续传：新会话 + 新同步器实例，同槽同 stateDir
        CatalogSynchronizer sync2 = startSync(slot, cfg, mainOid);
        assertTrue(sync2.resumedFromState(), "存在有效检查点时应走续传（跳过引导）");
        assertEquals(storedLsn, sync2.snapshot().lsn(),
                "续传起点应恰为 stored lsn（引导路径的种子是引导时刻 flush LSN，二者可区分）");

        try (Connection c = Interference.newSqlConnection()) {
            // 含 RENAME——审查 High-1 修复（读区回填）后回归：修复前该形态在单会话
            // （无检查点/续传参与）即复现 relnamespace/reltype/relowner/relam 归零
            execDdl(c,
                    "ALTER TABLE t_life DROP COLUMN payload",
                    "ALTER TABLE t_life RENAME TO t_life_renamed",
                    "TRUNCATE t_life_renamed",
                    "INSERT INTO t_life_renamed (id, extra, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                    "ALTER TABLE t_life_renamed ADD COLUMN late_col int",
                    "ALTER TABLE t_life_renamed SET (autovacuum_enabled = false)");
            // 确定性收尾（同对抗性 IT）：死元组清在目标位点之前 + 尾写取 flush 目标
            execDdl(c, "VACUUM ANALYZE pg_class", "VACUUM ANALYZE pg_attribute");
            execDdl(c, "INSERT INTO t_life_tail VALUES (2)");
            long target = flushLsn(c);
            awaitConsumed(sync2, target, "剩余 DDL 接收前沿");
        }
        assertTrue(sync2.consumedLsn() >= storedLsn, "重启后消费前沿不应回退到 stored lsn 之前");
        sync2.stop();

        try (Connection probe = Interference.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
    }

    /**
     * 场景 ②：状态文件截断 3 字节 → load 拒载（CRC 必然失配的行为断言）→ 重启走
     * 全新引导 → 剩余 DDL → 对拍仍全等（安全侧：宁可重引导，不可错位窗口重放）。
     */
    @Test
    void truncatedStateFileFallsBackToFreshBootstrapAndStillMatches() throws Exception {
        String slot = "wal_life_it_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Interference.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_trunc");
        StateStore store = new StateStore(stateDir, layout.majorVersion());
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startSync(slot, cfg, mainOid);
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO t_trunc SELECT g, 'p-' || g FROM generate_series(1, 100) g",
                    "ALTER TABLE t_trunc ADD COLUMN extra int",
                    "INSERT INTO t_trunc_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync1, target, "首段 DDL 接收前沿");
        }
        sync1.stop();
        assertTrue(store.load().isPresent(), "停机检查点应可加载");

        // 截断 3 字节：全文件 CRC（尾 4B 之外全文件面）必然失配 → load 拒载回落 empty
        Path stateFile = stateDir.resolve(StateStore.FILE_NAME);
        byte[] bytes = Files.readAllBytes(stateFile);
        Files.write(stateFile, Arrays.copyOf(bytes, bytes.length - 3));
        assertTrue(new StateStore(stateDir, layout.majorVersion()).load().isEmpty(), "截断后的检查点应拒载（load 返回 empty）");

        CatalogSynchronizer sync2 = startSync(slot, cfg, mainOid);
        assertFalse(sync2.resumedFromState(), "检查点拒载时应走全新引导");
        assertTrue(sync2.snapshot().lsn() > 0, "引导路径的种子前沿应为引导时刻 flush LSN");

        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_trunc DROP COLUMN payload",
                    "TRUNCATE t_trunc",
                    "INSERT INTO t_trunc (id, extra) VALUES (7, 7)",
                    "ALTER TABLE t_trunc SET (autovacuum_enabled = false)",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "INSERT INTO t_trunc_tail VALUES (2)");
            long target = flushLsn(c);
            awaitConsumed(sync2, target, "回落引导后剩余 DDL 接收前沿");
        }
        sync2.stop();
        assertTrue(store.load().isPresent(), "回落引导会话的停机检查点应重新落盘且可加载");

        try (Connection probe = Interference.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
    }

    /**
     * 场景 ④（丢页注入，Task 13 裁定 2 的跟进——开放风险验证）：检查点一致点之后
     * 跳过一段 WAL（gap 内含 watched 目录的真实变更），从"前沿 + 3 页页对齐"起新流
     * ——gap 记录对重放面等于凭空丢失；随后续 DDL 驱动 watched 目录记录，验证
     * "丢失后 watched 后续事件把 catalog <strong>末态</strong>追平"（精确采纳为末态
     * 回填语义——探测时刻末态，丢页窗口的中间代际不可恢复也不必恢复，v1 承诺面是
     * 末态全等）。失败 = 风险证实，如实记录不掩盖。
     */
    @Test
    void lostPagesSkipThenSubsequentWatchedEventsCatchUp() throws Exception {
        String slot = "wal_life_it_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Interference.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_gap");
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startSync(slot, cfg, mainOid);
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO t_gap SELECT g, 'p-' || g FROM generate_series(1, 100) g",
                    "INSERT INTO t_gap_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync1, target, "首段 DDL 接收前沿");
        }
        sync1.stop();
        Optional<StoredState> stored = new StateStore(stateDir, layout.majorVersion()).load();
        assertTrue(stored.isPresent(), "检查点一致点应已落盘");
        long frontier = stored.get().lsn();

        // gap：watched 目录的真实变更（这些记录将被跳过，永不进重放面）
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_gap ADD COLUMN gap_col int",
                    "ALTER TABLE t_gap SET (autovacuum_enabled = true)",
                    "ANALYZE t_gap",
                    "UPDATE t_gap SET payload = payload || '-gap' WHERE id % 2 = 0",
                    "DELETE FROM t_gap WHERE id % 4 = 0",
                    "ALTER TABLE t_gap SET (autovacuum_enabled = false)");
        }
        // 跳变位点：当前 flush + 3 页、页对齐（与接收器页导航同式下取整）
        long page = layout.walBlockSize();
        long jump;
        try (Connection c = Interference.newSqlConnection()) {
            jump = (flushLsn(c) + 3L * page) & ~(page - 1);
            // 越过跳变位点再补一段 WAL（用户表 heap 写，非 watched）——确保服务端
            // flush 已越过跳变位点，START_REPLICATION 不因"未来位点"被拒
            while (flushLsn(c) < jump + page) {
                execDdl(c, "INSERT INTO t_gap_tail SELECT g FROM generate_series(1, 200) g");
            }
        }
        LOG.info("丢页注入: 检查点前沿 {} -> 跳变起点 {}（跳过 {} 页）",
                Lsn.format(frontier), Lsn.format(jump), (jump - frontier) / page);

        CatalogSynchronizer sync2 = CatalogSynchronizer.start(openConn(), slot, layout,
                Interference.username(), Interference.password(), cfg, jump, mainOid);
        syncs.add(sync2);
        assertTrue(sync2.resumedFromState(), "带有效检查点的跳变仍应自检查点恢复字典");
        assertTrue(sync2.consumedLsn() >= jump, "跳变起点不回退（页对齐下取整不越过跳变位点）");

        // 后续 DDL 驱动 watched 目录记录：截断自愈面（DROP 引用 gap 期插入的 attr 行）、
        // INPLACE 全行改写（SET/ANALYZE/TRUNCATE）、CHECKPOINT 后首写 FPW 页镜像、
        // VACUUM PRUNE 清陈旧行——对拍全等即"追平"成立
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_gap DROP COLUMN gap_col",
                    "ALTER TABLE t_gap SET (autovacuum_enabled = true)",
                    "ANALYZE t_gap",
                    "TRUNCATE t_gap",
                    "INSERT INTO t_gap SELECT g, 'z-' || g FROM generate_series(1, 50) g",
                    "CHECKPOINT",
                    "ALTER TABLE t_gap SET (autovacuum_enabled = true)",
                    "ANALYZE t_gap",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "ALTER TABLE t_gap SET (autovacuum_enabled = false)",
                    "INSERT INTO t_gap_tail VALUES (99)");
            long target = flushLsn(c);
            awaitConsumed(sync2, target, "丢页后续 DDL 接收前沿");
        }
        sync2.stop();

        try (Connection probe = Interference.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
        // 协议断言面：末态追平须由真实后续记录驱动（resyncs==0）——重同步回退补页
        // 读取了本应丢失的 gap 页会把场景偷换成"未丢页"，属空转假绿
        assertEquals(0L, sync2.streamMetrics().resyncs.sum(),
                "丢页场景应零再同步（末态追平由后续 DDL 驱动, 非重同步补页）: " + sync2.streamMetrics());
        LOG.info("丢页末态追平验证通过: 指标 {} / 接收器 {}", sync2.metrics(), sync2.streamMetrics());
    }

    /**
     * 场景 ⑤（Task C 全装配验收）：{@link WalSource} 门面带 {@code vb.wal.state.dir}
     * 六键 + state 三键起全管线（引导 + 重放 + 检查点 + 槽推进），close 最终检查点
     * 落盘可加载，同配置再起一轮（续传形态）起停干净。
     */
    @Test
    void walSourceFacadeAssemblesFullPipelineAndCheckpoints() throws Exception {
        String slot = "wal_life_src_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Interference.dropPhysicalSlotQuietly(slot);

        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, Interference.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(Interference.port()));
        cfg.setProperty(WalSource.KEY_DB, Interference.databaseName());
        cfg.setProperty(WalSource.KEY_USER, Interference.username());
        cfg.setProperty(WalSource.KEY_PASS, Interference.password());
        cfg.setProperty(WalSource.KEY_SLOT, slot);
        cfg.setProperty(StateConfig.KEY_DIR, stateDir.toString());

        WalSource source = new WalSource(cfg);
        source.start();
        assertNotNull(source.stateDir(), "配置了 state.dir 时门面应暴露状态目录");
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "CREATE TABLE t_src_smoke (id int primary key, v text)",
                    "INSERT INTO t_src_smoke VALUES (1, 'a'), (2, 'b')",
                    "ALTER TABLE t_src_smoke ADD COLUMN extra int",
                    "UPDATE t_src_smoke SET extra = 3 WHERE id = 1");
            long target = flushLsn(c);
            awaitTrue(() -> source.consumedLsn() >= target, "WalSource 全管线接收前沿");
        }
        source.close();
        Optional<StoredState> stored = new StateStore(stateDir, layout.majorVersion()).load();
        assertTrue(stored.isPresent(), "close 的最终检查点应落盘可加载");
        assertSlotAdvancedTo(slot, stored.get().lsn());

        // 同配置第二轮：续传形态起停干净（启停面回归，不重复对拍）
        WalSource second = new WalSource(cfg);
        second.start();
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c, "INSERT INTO t_src_smoke VALUES (3, 'c')");
            long target = flushLsn(c);
            awaitTrue(() -> second.consumedLsn() >= target, "第二轮续传接收前沿");
        }
        second.close();
        assertTrue(second.consumedLsn() >= stored.get().lsn(), "第二轮前沿不应回退到检查点之前");
    }

    /**
     * 场景 ⑥（终审 C1 复测 + Task 16 跟进项第一嫌疑验证）：bootstrap 进行时另一连接
     * 持续 ADD/DROP COLUMN——两连接制造引导窗口重叠。修复前形态（种子查询全部完成
     * 后才读 flush LSN，时点 B ≥ 快照 S）下，窗口 (S,B] 内提交的 ADD COLUMN 的
     * pg_attribute INSERT 末尾 ≤ appliedLsn 种子（= B），被过滤线永久跳过且种子快照
     * 又看不到——丢行无自愈通道；修复后 flush LSN 于 RR 事务首句读取（B ≤ S），窗口
     * 记录必进重放面（upsert 幂等消化重叠）。断言对拍全等（40 轮 ADD/DROP 后 attr 面
     * 含 40 条 dropped 占位行，缺一条即红）。<strong>17 侧不复刻</strong>：已知
     * "干扰风暴与 DDL 并发交织偶发丢 INSERT"跟进项（Task 16）会混淆本场景的归因，
     * 18 侧已验即记档。
     */
    @Test
    void concurrentDdlDuringBootstrapWindowIsNotLostToFilterLine() throws Exception {
        String slot = "wal_life_conc_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Interference.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_conc");
        StateConfig cfg = new StateConfig(null, 30_000, 1_000);    // state 禁用——本场景只考引导窗口

        // 引导窗口风暴：与 start() 内部 bootstrap（RR 事务种子全库两目录，数十毫秒级）
        // 并发地反复 ADD/DROP——每次 ADD 都插一条 pg_attribute 行、DROP 置 dropped 占位
        Thread storm = new Thread(() -> {
            try (Connection c = Interference.newSqlConnection(); Statement st = c.createStatement()) {
                for (int i = 0; i < 40; i++) {
                    st.execute("ALTER TABLE t_conc ADD COLUMN storm_col int");
                    st.execute("ALTER TABLE t_conc DROP COLUMN storm_col");
                }
            } catch (SQLException e) {
                LOG.warn("引导并发 DDL 风暴提前终止: {}", e.getMessage());
            }
        }, "bootstrap-ddl-storm");
        storm.start();

        CatalogSynchronizer sync = startSync(slot, cfg, mainOid);
        storm.join(30_000);
        assertTrue(!storm.isAlive(), "风暴线程应在 join 上限内退出");

        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_conc ADD COLUMN final_col int",
                    "INSERT INTO t_conc SELECT g, 'p-' || g, g FROM generate_series(1, 50) g",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "INSERT INTO t_conc_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync, target, "引导并发 DDL 场景接收前沿");
        }
        sync.stop();

        try (Connection probe = Interference.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync, probe, mainOid, mainOid);
        }
        LOG.info("引导并发 DDL 窗口不丢行验证通过: 指标 {} / 接收器 {}", sync.metrics(), sync.streamMetrics());
    }

    /**
     * 场景前置：清场重建被测主表与尾写表，返回主表 oid（建表在同步器 start 之前
     * ——interest oid 供引导定位 tracked 双 ctid）。
     *
     * @param table 被测主表名（尾写表名 = 主表名 + "_tail"）
     * @return 主表 oid
     * @throws SQLException DDL 失败
     */
    private long prepareTables(String table) throws SQLException {
        try (Connection c = Interference.newSqlConnection()) {
            execDdl(c,
                    "DROP TABLE IF EXISTS " + table,
                    "DROP TABLE IF EXISTS " + table + "_renamed",
                    "DROP TABLE IF EXISTS " + table + "_tail",
                    "CREATE TABLE " + table + " (id int primary key, payload text)",
                    "CREATE TABLE " + table + "_tail (v int)");
            return oidOf(c, table);
        }
    }

    /**
     * 起一个同步器（带凭据 + state 配置，自愈接线档），登记进兜底清理面。
     *
     * @param slot    物理槽名
     * @param cfg     检查点配置
     * @param mainOid tracked 基表 oid
     * @return 已运行的同步器
     * @throws SQLException 槽管理或引导失败
     */
    private CatalogSynchronizer startSync(String slot, StateConfig cfg, long mainOid) throws SQLException {
        CatalogSynchronizer sync = CatalogSynchronizer.start(openConn(), slot, layout,
                Interference.username(), Interference.password(), cfg, 0L, mainOid);
        syncs.add(sync);
        return sync;
    }

    /**
     * 开一条引导会话（healer probe / 槽推进复用），登记进兜底清理面。
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 连接失败
     */
    private Connection openConn() throws SQLException {
        Connection c = Interference.newSqlConnection();
        conns.add(c);
        return c;
    }

    /**
     * 断言物理槽的 restart_lsn 已被 {@code pg_replication_slot_advance} 推进到检查点
     * lsn（Task 8 裁定方案 a：严格持久化之后推进）。
     *
     * @param slot         槽名
     * @param checkpointLsn 检查点 lsn（推进目标下界）
     * @throws SQLException 查询失败
     */
    private static void assertSlotAdvancedTo(String slot, long checkpointLsn) throws SQLException {
        long restartLsn;
        try (Connection c = Interference.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slot + "'")) {
            assertTrue(rs.next(), "槽 " + slot + " 应存在");
            String restart = rs.getString(1);
            assertNotNull(restart, "推进后的 restart_lsn 不应为 NULL");
            restartLsn = Lsn.parse(restart);
        }
        assertTrue(restartLsn >= checkpointLsn,
                "槽 restart_lsn " + Lsn.format(restartLsn) + " 应已推进到检查点 lsn "
                        + Lsn.format(checkpointLsn) + " 及之后");
    }
}
