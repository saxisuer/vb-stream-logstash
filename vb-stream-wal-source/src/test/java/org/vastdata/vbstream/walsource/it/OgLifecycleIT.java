package org.vastdata.vbstream.walsource.it;

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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 5.0.0 生命周期 IT（M1 Task 9）：检查点续传（stop → DDL → start 续传对拍）/
 * 损坏回落（截断 state 文件 → 重启全新引导）/ WalSource 门面 OG 档全装配三形态，
 * 全部复用 {@code WalSyncItBase} 停流对拍器（ctid 键控逐行全等）。状态目录用
 * {@link TempDir} 每用例独立。
 *
 * <p><strong>write 位点续传语义观察（Task 7 遗留实测项）</strong>：OG 无 flush 档
 * 位点函数——引导 B 与对拍目标前沿均取 {@code pg_current_xlog_location()}（write
 * 位点）。理论窗口比 vanilla 略窄一档（write ≥ flush，种子快照时点 S ≥ B(write) 使
 * (B,S] 窗口更小），服务端物理流仍自 flushed 数据起发——续传正确性由对拍断言承载
 * （窗口丢失即对拍红，如实报告不放宽断言压绿）；槽推进面按观察记档
 * （{@code observeSlotAdvance}——OG restart_lsn 保留行为未实测收口，Task 6 concern 4）。</p>
 */
class OgLifecycleIT extends OgSyncItBase {

    private static final Logger LOG = LoggerFactory.getLogger(OgLifecycleIT.class);

    /** 本用例状态目录（每用例独立临时目录）。 */
    @TempDir
    Path stateDir;

    /**
     * 场景 ①：半程 DDL → stop（含最终检查点 + 槽推进观测）→ 同 stateDir 续传 →
     * 剩余 DDL → 对拍全等；续传起点恰为 stored lsn、重启后前沿不回退。
     */
    @Test
    void checkpointResumeAcrossRestartReplaysRemainingDdlIdentical() throws Exception {
        String slot = registerSlot("og_life_it");
        long mainOid = prepareTables("og_life");
        StateStore store = new StateStore(stateDir, layout.majorVersion());
        // events 阈值压到 1：任一 catalog 行事件即触发——运行中周期检查点必然先行覆盖
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startOgSync(slot, cfg, 0L, mainOid);
        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO og_life SELECT g, 'p-' || g || '-' || repeat('x', g % 40) FROM generate_series(1, 200) g",
                    "UPDATE og_life SET payload = payload || '-u' WHERE id % 2 = 0",
                    "DELETE FROM og_life WHERE id % 3 = 0",
                    "ALTER TABLE og_life ADD COLUMN extra int",
                    "ALTER TABLE og_life ADD COLUMN note text DEFAULT 'seed'",
                    "INSERT INTO og_life_tail VALUES (1)");
            long target = writeLsn(c);
            awaitConsumed(sync1, target, "OG 半程 DDL 接收前沿");
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
        CatalogSynchronizer sync2 = startOgSync(slot, cfg, 0L, mainOid);
        assertTrue(sync2.resumedFromState(), "存在有效检查点时应走续传（跳过引导）");
        assertEquals(storedLsn, sync2.snapshot().lsn(),
                "续传起点应恰为 stored lsn（引导路径的种子是引导时刻 write LSN，二者可区分）");

        try (Connection c = OgTestEnv.newSqlConnection()) {
            // 含 RENAME（vanilla 读区回填修复的回归面）与 TRUNCATE 二连（INPLACE relfilenode）
            execDdl(c,
                    "ALTER TABLE og_life DROP COLUMN payload",
                    "ALTER TABLE og_life RENAME TO og_life_renamed",
                    "TRUNCATE og_life_renamed",
                    "INSERT INTO og_life_renamed (id, extra, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                    "ALTER TABLE og_life_renamed ADD COLUMN late_col int",
                    "ALTER TABLE og_life_renamed SET (autovacuum_enabled = false)");
            // 确定性收尾（同对抗 IT）：死元组清在目标位点之前 + 尾写取 write 目标
            execDdl(c, "VACUUM ANALYZE pg_class", "VACUUM ANALYZE pg_attribute");
            execDdl(c, "INSERT INTO og_life_tail VALUES (2)");
            long target = writeLsn(c);
            awaitConsumed(sync2, target, "OG 剩余 DDL 接收前沿");
        }
        assertTrue(sync2.consumedLsn() >= storedLsn, "重启后消费前沿不应回退到 stored lsn 之前");
        sync2.stop();

        try (Connection probe = OgTestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
        LOG.info("OG 检查点续传对拍全等: stored {} -> 终态 {} 指标 {} / walker {}",
                Lsn.format(storedLsn), Lsn.format(sync2.snapshot().lsn()), sync2.metrics(), sync2.streamMetrics());
    }

    /**
     * 场景 ②：状态文件截断 3 字节 → load 拒载（CRC 必然失配的行为断言）→ 重启走
     * 全新引导 → 剩余 DDL → 对拍仍全等（安全侧：宁可重引导，不可错位窗口重放）。
     */
    @Test
    void truncatedStateFileFallsBackToFreshBootstrapAndStillMatches() throws Exception {
        String slot = registerSlot("og_trunc_it");
        long mainOid = prepareTables("og_trunc");
        StateStore store = new StateStore(stateDir, layout.majorVersion());
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startOgSync(slot, cfg, 0L, mainOid);
        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO og_trunc SELECT g, 'p-' || g FROM generate_series(1, 100) g",
                    "ALTER TABLE og_trunc ADD COLUMN extra int",
                    "INSERT INTO og_trunc_tail VALUES (1)");
            long target = writeLsn(c);
            awaitConsumed(sync1, target, "OG 首段 DDL 接收前沿");
        }
        sync1.stop();
        assertTrue(store.load().isPresent(), "停机检查点应可加载");

        // 截断 3 字节：全文件 CRC 必然失配 → load 拒载回落 empty
        Path stateFile = stateDir.resolve(StateStore.FILE_NAME);
        byte[] bytes = Files.readAllBytes(stateFile);
        Files.write(stateFile, Arrays.copyOf(bytes, bytes.length - 3));
        assertTrue(new StateStore(stateDir, layout.majorVersion()).load().isEmpty(),
                "截断后的检查点应拒载（load 返回 empty）");

        CatalogSynchronizer sync2 = startOgSync(slot, cfg, 0L, mainOid);
        assertFalse(sync2.resumedFromState(), "检查点拒载时应走全新引导");
        assertTrue(sync2.snapshot().lsn() > 0, "引导路径的种子前沿应为引导时刻 write LSN");

        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE og_trunc DROP COLUMN payload",
                    "TRUNCATE og_trunc",
                    "INSERT INTO og_trunc (id, extra) VALUES (7, 7)",
                    "ALTER TABLE og_trunc SET (autovacuum_enabled = false)",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "INSERT INTO og_trunc_tail VALUES (2)");
            long target = writeLsn(c);
            awaitConsumed(sync2, target, "OG 回落引导后剩余 DDL 接收前沿");
        }
        sync2.stop();
        assertTrue(store.load().isPresent(), "回落引导会话的停机检查点应重新落盘且可加载");

        try (Connection probe = OgTestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
    }

    /**
     * 场景 ③（Task 6 装配缝的端到端验收）：{@link WalSource} 门面带 OG 档配置
     * （{@code vb.wal.og.source=direct} + {@code vb.wal.og.jdbc.prefix=jdbc:opengauss:}
     * + state 三键）起全管线（ensureSlotOg → bootstrap → direct 字节源 → 检查点 +
     * 槽推进），close 最终检查点落盘可加载（nsp 种子段含 public），同配置再起一轮
     * （续传形态）起停干净。
     */
    @Test
    void walSourceFacadeOgDirectChannelCheckpointsAndResumes() throws Exception {
        String slot = registerSlot("og_src_it");
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, OgTestEnv.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(OgTestEnv.port()));
        cfg.setProperty(WalSource.KEY_DB, OgTestEnv.DATABASE);
        cfg.setProperty(WalSource.KEY_USER, OgTestEnv.USER);
        cfg.setProperty(WalSource.KEY_PASS, OgTestEnv.PASSWORD);
        cfg.setProperty(WalSource.KEY_SLOT, slot);
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        cfg.setProperty(StateConfig.KEY_DIR, stateDir.toString());
        cfg.setProperty(StateConfig.KEY_EVENTS, "1");

        WalSource source = new WalSource(cfg);
        source.start();
        assertNotNull(source.stateDir(), "配置了 state.dir 时门面应暴露状态目录");
        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c,
                    "CREATE TABLE og_src_smoke (id int primary key, v text)",
                    "INSERT INTO og_src_smoke VALUES (1, 'a'), (2, 'b')",
                    "ALTER TABLE og_src_smoke ADD COLUMN extra int",
                    "UPDATE og_src_smoke SET extra = 3 WHERE id = 1");
            long target = writeLsn(c);
            awaitTrue(() -> source.consumedLsn() >= target, "WalSource OG 全管线接收前沿");
        }
        source.close();
        Optional<StoredState> stored = new StateStore(stateDir, layout.majorVersion()).load();
        assertTrue(stored.isPresent(), "close 的最终检查点应落盘可加载");
        assertTrue(stored.get().nspRows().values().stream().anyMatch(r -> "public".equals(r.nspname())),
                "检查点 nsp 段应含 public 模式（bootstrap 种子 + v2 序列化）");

        // 同配置第二轮：续传形态起停干净（启停面回归，不重复对拍）
        WalSource second = new WalSource(cfg);
        second.start();
        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c, "INSERT INTO og_src_smoke VALUES (3, 'c')");
            long target = writeLsn(c);
            awaitTrue(() -> second.consumedLsn() >= target, "第二轮续传接收前沿");
        }
        second.close();
        assertTrue(second.consumedLsn() >= stored.get().lsn(), "第二轮前沿不应回退到检查点之前");
        LOG.info("WalSource OG 门面两轮启停完成: 首轮检查点 {} 终态前沿 {}",
                Lsn.format(stored.get().lsn()), Lsn.format(second.consumedLsn()));
        OgTestEnv.execSql("DROP TABLE IF EXISTS og_src_smoke");
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
        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c,
                    "DROP TABLE IF EXISTS " + table,
                    "DROP TABLE IF EXISTS " + table + "_renamed",
                    "DROP TABLE IF EXISTS " + table + "_tail",
                    "CREATE TABLE " + table + " (id int primary key, payload text)",
                    "CREATE TABLE " + table + "_tail (v int)");
            return oidOf(c, table);
        }
    }
}
