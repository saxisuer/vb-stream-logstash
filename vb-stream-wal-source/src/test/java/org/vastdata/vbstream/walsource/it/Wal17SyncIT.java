package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayoutV17;
import org.vastdata.vbstream.walsource.layout.WalLayoutV18;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.replay.CatalogStores;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PG 17 全矩阵 IT（Task 16）——WalLayoutV17 转录的端到端复核：{@link Wal17TestEnv}
 * （postgres:17，干扰参数同 {@link Interference}）上复用 {@link WalSyncItBase} 的
 * 对拍基类跑三面：
 *
 * <p>① <strong>对抗场景精简版</strong>（{@code @RepeatedTest(2)}）：核心 DDL 序列
 * （死元组三连 → 列增删 → 改名 → 截断二连 → toast 表宽行 → CHECKPOINT×3 的 FPW
 * 页镜像路径）+ 一段干扰（autovacuum toggle + ANALYZE 风暴）——全部走 V17 差异面：
 * pg_attribute 26 列词典（attcacheoff@5）、pg_class 33 列词典 + reltoastrelid 数据区
 * 偏移 108 的 INPLACE 直读、attisdropped 槽 17 的行投影；停流对拍逐行全等。
 * <strong>风暴后置次序裁定（实测踩坑）</strong>：交织形态实测 1/4 执行丢一条 ADD
 * COLUMN 的 attr INSERT 事件（快照缺行假红）——根因未查、以次序规避（跟进项记档，
 * 控制器裁定）：18 侧对抗性 IT 出现同类"快照侧缺行"假红时，回查此处与 replay 链
 * （attr INSERT 在页竞争窗口的到达/解码），而非继续靠次序规避。
 * ② <strong>生命周期场景复跑（① 续传 / ② 损坏回落 / ③ 丢页自愈）</strong>：18 侧
 * WalLifecycleIT 全四形态在 17 容器重演——probe/采纳路径按 V17 词典槽位（26/33 项）
 * 组装值行，几何错误恰能被对拍暴露。③ <strong>跨版本交叉</strong>：17 容器的
 * server_version_num 分发到 V17、两描述符 supports 互拒、160000 启动期 ISE；另有
 * <strong>版本注入 fail-fast</strong>（审查 Med-3）：显式传 V18 layout 对 17 容器
 * start → 启动期 ISE 且零槽副作用（CatalogSynchronizer 的双向版本校验）。</p>
 */
class Wal17SyncIT extends WalSyncItBase {

    private static final Logger LOG = LoggerFactory.getLogger(Wal17SyncIT.class);

    /** 干扰线程持续时长（毫秒）——精简版取对抗性 IT（8s）的一半。 */
    private static final long INTERFERENCE_MS = 4_000;

    /** 干扰线程 join 上限（毫秒）。 */
    private static final long INTERFERENCE_JOIN_MS = 10_000;

    /** 跨用例的槽名序号（每用例独立槽，finally 统一清理）。 */
    private static final AtomicLong SLOT_SEQ = new AtomicLong();

    /** 本用例状态目录（生命周期场景用，独立临时目录）。 */
    @TempDir
    Path stateDir;

    /** 本用例启动的同步器——AfterEach 兜底停机。 */
    private final List<CatalogSynchronizer> syncs = new ArrayList<>();

    /** 本用例持有的引导会话——AfterEach 兜底关闭。 */
    private final List<Connection> conns = new ArrayList<>();

    /** 本用例用过的槽——AfterEach 兜底删除。 */
    private final List<String> slots = new ArrayList<>();

    /**
     * 版本钩子覆写：本 IT 绑定 PG 17 容器（基类布局字段构造期经本方法解析为
     * V17 描述符——覆写在字段初始化前生效，实现不依赖子类字段）。
     */
    @Override
    protected int serverVersion() {
        try {
            int v = Wal17TestEnv.serverVersionNum();
            assertTrue(v >= 170000 && v < 180000,
                    "postgres:17 容器版本应落 17 区间, 实际 " + v + "（镜像漂移？）");
            return v;
        } catch (SQLException e) {
            throw new IllegalStateException("读取 PG 17 容器 server_version_num 失败", e);
        }
    }

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
            Wal17TestEnv.dropPhysicalSlotQuietly(s);
        }
    }

    /**
     * 对抗场景精简版（PG 17）：核心 DDL 序列 + 干扰段 + CHECKPOINT×3 下，快照字典与
     * JDBC REPEATABLE READ 实查逐行全等，且布局恰为 V17 描述符（容器上的正确分发
     * 断言）——WalLayoutV17 差异面（attcacheoff 词典列、reltoastrelid@108 的 INPLACE
     * 直读、attisdropped@17 投影）全部途经。复跑两次覆盖竞态。
     */
    @RepeatedTest(2)
    void compactAdversarialDdlOnPg17MatchesJdbc() throws Exception {
        assertEquals(WalLayoutV17.INSTANCE, layout, "17 容器应分发 V17 描述符");
        String slot = "wal17_it_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Wal17TestEnv.dropPhysicalSlotQuietly(slot);

        long mainOid = prepareTables("t_adv17");
        long toastOid;
        Connection bootstrapConn = openConn();
        CatalogSynchronizer sync = CatalogSynchronizer.start(bootstrapConn, slot, layout,
                Wal17TestEnv.username(), Wal17TestEnv.password(), mainOid);
        try {
            // 次序裁定（实测踩坑，根因未查——跟进项记档见类 javadoc）：DDL 序列先行、
            // 干扰风暴随后——风暴与 attr 行 INSERT 并发窗口会把 pg_attribute 撕成高频
            // 页竞争（实测 1/4 执行丢一条 ADD COLUMN attr 行的 INSERT 事件，末态对拍
            // 假红）；风暴后置仍全覆盖干扰面（renamed 表 reloptions 深位列截断更新 +
            // ANALYZE 的 pg_class INPLACE + CHECKPOINT 后首写 FPW），18 侧对抗性 IT 的
            // 交织形态不在此复刻——本 IT 的卖点是版本矩阵
            try (Connection c1 = Wal17TestEnv.newSqlConnection();
                 Connection c2 = Wal17TestEnv.newSqlConnection()) {
                runCoreDdlScenario(c1, c2, sync);
            }
            Thread storm = interferenceThread();
            storm.start();
            storm.join(INTERFERENCE_JOIN_MS);
            assertTrue(!storm.isAlive(), "干扰线程应在 join 上限内退出");

            // 确定性收尾（同对抗性 IT）：死元组清在目标位点之前 + 两表 autovacuum 关闭 + 尾写取目标
            long target;
            try (Connection c = Wal17TestEnv.newSqlConnection(); Statement st = c.createStatement()) {
                st.execute("VACUUM ANALYZE pg_class");
                st.execute("VACUUM ANALYZE pg_attribute");
                st.execute("ALTER TABLE t_adv17_renamed SET (autovacuum_enabled = false)");
                st.execute("ALTER TABLE t_adv17_toast SET (autovacuum_enabled = false)");
                st.execute("INSERT INTO t_adv17_tail VALUES ((random() * 1000000)::int)");
                target = flushLsn(c);
            }
            awaitConsumed(sync, target, "PG17 对拍目标前沿 " + Lsn.format(target));
            try (Connection c = Wal17TestEnv.newSqlConnection()) {
                toastOid = oidOf(c, "t_adv17_toast");
            }
        } finally {
            sync.stop();
            bootstrapConn.close();
        }

        try (Connection probe = Wal17TestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync, probe, mainOid, mainOid, toastOid);
        }
        Map<String, Long> metrics = sync.metrics();
        LOG.info("PG17 对拍指标 slot={}: 重放 {} / 接收器 {}", slot, metrics, sync.streamMetrics());
        assertTrue(sync.streamMetrics().resyncs.sum() <= 2,
                "PG17 对抗场景重同步应容差内（≤2）: " + sync.streamMetrics());
        assertTrue(metrics.getOrDefault(CatalogStores.CatalogMetrics.REPLAYED, 0L) > 0,
                "场景应产生 catalog 行事件: " + metrics);
    }

    /**
     * 生命周期场景 ① 复跑（PG 17）：半程 DDL → stop（最终检查点 + 槽推进）→ 同
     * stateDir 续传（起点恰为 stored lsn、resumedFromState 真）→ 剩余 DDL（含
     * RENAME/TRUNCATE——V17 读区回填与 INPLACE@108 的版本敏感路径）→ 对拍全等。
     */
    @Test
    void checkpointResumeAcrossRestartOnPg17MatchesJdbc() throws Exception {
        String slot = "wal17_life_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Wal17TestEnv.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_life17");
        StateStore store = new StateStore(stateDir, layout.majorVersion());
        // events 阈值压到 1：任一 catalog 行事件即触发运行中周期检查点（对齐 18 侧生命周期 IT）
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startSync(slot, cfg, mainOid);
        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO t_life17 SELECT g, 'p-' || g || '-' || repeat('x', g % 40) FROM generate_series(1, 200) g",
                    "UPDATE t_life17 SET payload = payload || '-u' WHERE id % 2 = 0",
                    "DELETE FROM t_life17 WHERE id % 3 = 0",
                    "ALTER TABLE t_life17 ADD COLUMN extra int",
                    "ALTER TABLE t_life17 ADD COLUMN note text DEFAULT 'seed'",
                    "INSERT INTO t_life17_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync1, target, "PG17 半程 DDL 接收前沿");
        }
        awaitTrue(store::exists, "运行中周期检查点应已落盘（events 阈值 1）");

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
        assertEquals(storedLsn, sync2.snapshot().lsn(), "续传起点应恰为 stored lsn");

        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_life17 DROP COLUMN payload",
                    "ALTER TABLE t_life17 RENAME TO t_life17_renamed",
                    "TRUNCATE t_life17_renamed",
                    "INSERT INTO t_life17_renamed (id, extra, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                    "ALTER TABLE t_life17_renamed ADD COLUMN late_col int",
                    "ALTER TABLE t_life17_renamed SET (autovacuum_enabled = false)",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "INSERT INTO t_life17_tail VALUES (2)");
            long target = flushLsn(c);
            awaitConsumed(sync2, target, "PG17 剩余 DDL 接收前沿");
        }
        assertTrue(sync2.consumedLsn() >= storedLsn, "重启后消费前沿不应回退");
        sync2.stop();

        try (Connection probe = Wal17TestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
        LOG.info("PG17 续传对拍全等: 指标 {} / 接收器 {}", sync2.metrics(), sync2.streamMetrics());
    }

    /**
     * 生命周期场景 ② 复跑（PG 17，审查 Low-4）：状态文件截断 3 字节 → load 拒载
     * （CRC 必然失配的行为断言）→ 重启走全新引导 → 剩余 DDL → 对拍仍全等——回落
     * 引导后 healer 采纳面按 V17 词典槽位组装值行，几何错误恰能被对拍暴露。
     */
    @Test
    void truncatedStateFileOnPg17FallsBackToFreshBootstrapAndStillMatches() throws Exception {
        String slot = "wal17_trunc_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Wal17TestEnv.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_trunc17");
        StateStore store = new StateStore(stateDir, layout.majorVersion());
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startSync(slot, cfg, mainOid);
        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO t_trunc17 SELECT g, 'p-' || g FROM generate_series(1, 100) g",
                    "ALTER TABLE t_trunc17 ADD COLUMN extra int",
                    "INSERT INTO t_trunc17_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync1, target, "PG17 首段 DDL 接收前沿");
        }
        sync1.stop();
        assertTrue(store.load().isPresent(), "停机检查点应可加载");

        // 截断 3 字节：全文件 CRC 必然失配 → load 拒载回落 empty
        Path stateFile = stateDir.resolve(StateStore.FILE_NAME);
        byte[] bytes = Files.readAllBytes(stateFile);
        Files.write(stateFile, Arrays.copyOf(bytes, bytes.length - 3));
        assertTrue(new StateStore(stateDir, layout.majorVersion()).load().isEmpty(), "截断后的检查点应拒载（load 返回 empty）");

        CatalogSynchronizer sync2 = startSync(slot, cfg, mainOid);
        assertFalse(sync2.resumedFromState(), "检查点拒载时应走全新引导");
        assertTrue(sync2.snapshot().lsn() > 0, "引导路径的种子前沿应为引导时刻 flush LSN");

        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_trunc17 DROP COLUMN payload",
                    "TRUNCATE t_trunc17",
                    "INSERT INTO t_trunc17 (id, extra) VALUES (7, 7)",
                    "ALTER TABLE t_trunc17 SET (autovacuum_enabled = false)",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "INSERT INTO t_trunc17_tail VALUES (2)");
            long target = flushLsn(c);
            awaitConsumed(sync2, target, "PG17 回落引导后剩余 DDL 接收前沿");
        }
        sync2.stop();
        assertTrue(store.load().isPresent(), "回落引导会话的停机检查点应重新落盘且可加载");

        try (Connection probe = Wal17TestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
    }

    /**
     * 生命周期场景 ③ 复跑（PG 17，审查 Low-4，开放风险验证）：检查点一致点之后跳过
     * 一段 WAL（gap 内含 watched 目录的真实变更），从"前沿 + 3 页页对齐"起新流；随后
     * 续 DDL 驱动 watched 目录记录，验证"丢失后 watched 后续事件把 catalog 末态追平"
     * ——丢页断链迫使 healer 采纳路径高频介入，其按 V17 词典槽位（26/33 项 + 漂移槽
     * 17/12）组装值行的几何正确性由末态全等承载；resyncs==0 防重同步补页偷换场景。
     */
    @Test
    void lostPagesSkipThenSubsequentWatchedEventsCatchUpOnPg17() throws Exception {
        String slot = "wal17_gap_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Wal17TestEnv.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_gap17");
        StateConfig cfg = new StateConfig(stateDir, 30_000, 1);

        CatalogSynchronizer sync1 = startSync(slot, cfg, mainOid);
        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "INSERT INTO t_gap17 SELECT g, 'p-' || g FROM generate_series(1, 100) g",
                    "INSERT INTO t_gap17_tail VALUES (1)");
            long target = flushLsn(c);
            awaitConsumed(sync1, target, "PG17 首段 DDL 接收前沿");
        }
        sync1.stop();
        Optional<StoredState> stored = new StateStore(stateDir, layout.majorVersion()).load();
        assertTrue(stored.isPresent(), "检查点一致点应已落盘");
        long frontier = stored.get().lsn();

        // gap：watched 目录的真实变更（这些记录将被跳过，永不进重放面）
        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_gap17 ADD COLUMN gap_col int",
                    "ALTER TABLE t_gap17 SET (autovacuum_enabled = true)",
                    "ANALYZE t_gap17",
                    "UPDATE t_gap17 SET payload = payload || '-gap' WHERE id % 2 = 0",
                    "DELETE FROM t_gap17 WHERE id % 4 = 0",
                    "ALTER TABLE t_gap17 SET (autovacuum_enabled = false)");
        }
        long page = layout.walBlockSize();
        long jump;
        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            jump = (flushLsn(c) + 3L * page) & ~(page - 1);
            // 越过跳变位点再补一段 WAL，确保服务端 flush 已越过（START_REPLICATION 不拒未来位点）
            while (flushLsn(c) < jump + page) {
                execDdl(c, "INSERT INTO t_gap17_tail SELECT g FROM generate_series(1, 200) g");
            }
        }
        LOG.info("PG17 丢页注入: 检查点前沿 {} -> 跳变起点 {}（跳过 {} 页）",
                Lsn.format(frontier), Lsn.format(jump), (jump - frontier) / page);

        CatalogSynchronizer sync2 = CatalogSynchronizer.start(openConn(), slot, layout,
                Wal17TestEnv.username(), Wal17TestEnv.password(), cfg, jump, mainOid);
        syncs.add(sync2);
        assertTrue(sync2.resumedFromState(), "带有效检查点的跳变仍应自检查点恢复字典");
        assertTrue(sync2.consumedLsn() >= jump, "跳变起点不回退（页对齐下取整不越过跳变位点）");

        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "ALTER TABLE t_gap17 DROP COLUMN gap_col",
                    "ALTER TABLE t_gap17 SET (autovacuum_enabled = true)",
                    "ANALYZE t_gap17",
                    "TRUNCATE t_gap17",
                    "INSERT INTO t_gap17 SELECT g, 'z-' || g FROM generate_series(1, 50) g",
                    "CHECKPOINT",
                    "ALTER TABLE t_gap17 SET (autovacuum_enabled = true)",
                    "ANALYZE t_gap17",
                    "VACUUM ANALYZE pg_class",
                    "VACUUM ANALYZE pg_attribute",
                    "ALTER TABLE t_gap17 SET (autovacuum_enabled = false)",
                    "INSERT INTO t_gap17_tail VALUES (99)");
            long target = flushLsn(c);
            awaitConsumed(sync2, target, "PG17 丢页后续 DDL 接收前沿");
        }
        sync2.stop();

        try (Connection probe = Wal17TestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync2, probe, mainOid, mainOid);
        }
        assertEquals(0L, sync2.streamMetrics().resyncs.sum(),
                "丢页场景应零再同步（末态追平由后续 DDL 驱动, 非重同步补页）: " + sync2.streamMetrics());
        LOG.info("PG17 丢页末态追平验证通过: 指标 {} / 接收器 {}", sync2.metrics(), sync2.streamMetrics());
    }

    /**
     * 版本注入 fail-fast（审查 Med-3，任务书原意的容器级实现）：显式传 V18 layout 对
     * 17 容器 start → 启动期 ISE（消息含 server_version_num），且校验先于任何槽/流
     * 副作用（槽计数为零）——错配布局的 INPLACE 直读会把 relallvisible 当 reltoastrelid，
     * 宁可拒启不可错解。
     */
    @Test
    void v18LayoutInjectionIntoPg17ContainerFailsFastBeforeSideEffects() throws Exception {
        String slot = "wal17_xver_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        Wal17TestEnv.dropPhysicalSlotQuietly(slot);
        long mainOid = prepareTables("t_xver17");
        try (Connection c = openConn()) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> CatalogSynchronizer.start(c, slot, WalLayoutV18.INSTANCE,
                            Wal17TestEnv.username(), Wal17TestEnv.password(), mainOid));
            assertTrue(ex.getMessage().contains("server_version_num"),
                    "ISE 消息应携带版本号便于排障: " + ex.getMessage());
        }
        // 零副作用面：版本校验先于建槽——错配退出不留槽残留
        try (Connection c = Wal17TestEnv.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '" + slot + "'")) {
            rs.next();
            assertEquals(0, rs.getInt(1), "版本错配 fail-fast 须先于建槽（无副作用净退出）");
        }
    }

    /**
     * 跨版本交叉（17 容器上的分发断言）：server_version_num 落 17 区间 → 分发
     * V17 描述符且 V18/V17 互相 supports 全 false、伪版本 160000/190000 启动期
     * ISE——离线转录的"跨版本 fail-fast"在真容器版本探测面上的复核（注入面的
     * 容器级实现见 {@link #v18LayoutInjectionIntoPg17ContainerFailsFastBeforeSideEffects}）。
     */
    @Test
    void crossVersionDispatchOnPg17Container() throws Exception {
        int v = Wal17TestEnv.serverVersionNum();
        assertTrue(v >= 170000 && v < 180000, "postgres:17 容器版本应落 17 区间: " + v);
        assertEquals(WalLayoutV17.INSTANCE, WalLayouts.forServerVersion(v));
        assertEquals(17, WalLayouts.forServerVersion(v).majorVersion());
        assertFalse(WalLayoutV17.INSTANCE.supports(180000), "V17 不得覆盖 18 区间");
        assertFalse(WalLayoutV18.INSTANCE.supports(v), "V18 不得覆盖 17 区间");
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(160000));
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(190000));
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
        try (Connection c = Wal17TestEnv.newSqlConnection()) {
            execDdl(c,
                    "DROP TABLE IF EXISTS " + table,
                    "DROP TABLE IF EXISTS " + table + "_renamed",
                    "DROP TABLE IF EXISTS " + table + "_toast",
                    "DROP TABLE IF EXISTS " + table + "_tail",
                    "CREATE TABLE " + table + " (id int primary key, payload text)",
                    "CREATE TABLE " + table + "_tail (v int)");
            return oidOf(c, table);
        }
    }

    /**
     * 核心 DDL 序列（对抗性 IT 的精简档，无交错 latch 段）：死元组三连 → 列增删 →
     * 改名 → 截断二连（V17 INPLACE relfilenode 直读@88/108）→ toast 表宽行（流内
     * toast 关系重建 + interest 登记）→ CHECKPOINT×3（后继首写 FPW 页镜像路径）。
     *
     * @param c1   主会话
     * @param c2   toast 表会话
     * @param sync 运行中的同步器（interest 集登记面）
     * @throws SQLException DDL 失败
     * @throws InterruptedException  CHECKPOINT 间隔睡眠被中断
     */
    private void runCoreDdlScenario(Connection c1, Connection c2, CatalogSynchronizer sync)
            throws SQLException, InterruptedException {
        execDdl(c1,
                "INSERT INTO t_adv17 SELECT g, 'payload-' || g || '-' || repeat('x', g % 40) FROM generate_series(1, 300) g",
                "UPDATE t_adv17 SET payload = payload || '-u' WHERE id % 2 = 0",
                "DELETE FROM t_adv17 WHERE id % 3 = 0",
                "ALTER TABLE t_adv17 ADD COLUMN extra_tag int",
                "ALTER TABLE t_adv17 ADD COLUMN note text DEFAULT 'seed'",
                "ALTER TABLE t_adv17 DROP COLUMN payload",
                "ALTER TABLE t_adv17 RENAME TO t_adv17_renamed",
                "TRUNCATE t_adv17_renamed",
                "INSERT INTO t_adv17_renamed (id, extra_tag, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                "TRUNCATE t_adv17_renamed");
        // toast 表 + 宽行（toast 关系流内重建：pg_class INS + 主表行 reltoastrelid INPLACE@V17 偏移 108）
        execDdl(c2,
                "CREATE TABLE t_adv17_toast (id int primary key, wide text)",
                "INSERT INTO t_adv17_toast SELECT g, repeat('x', 12000) FROM generate_series(1, 20) g");
        sync.stores().interestRelOids().add(oidOf(c2, "t_adv17_toast"));
        // CHECKPOINT×3：后继首写（含干扰风暴的 catalog 更新）强制 FPW 页镜像路径
        try (Statement st = c1.createStatement()) {
            for (int i = 0; i < 3; i++) {
                st.execute("CHECKPOINT");
                Thread.sleep(100);
            }
        }
    }

    /**
     * 干扰线程工厂（对抗性 IT 的精简档，4s 窗口）：循环对被测表名（RENAME 前后 +
     * toast 表）{@code ALTER TABLE SET (autovacuum_enabled)} 真/假交替 + ANALYZE
     * 风暴——合法地反复改 pg_class 行（reloptions 深位列驱动截断更新值编码路径）。
     * 单条语句失败（表在场景推进中改名的时间窗）仅 DEBUG 继续；连接级失败终止线程。
     *
     * @return 未启动线程（调用方 start）
     */
    private Thread interferenceThread() {
        return new Thread(() -> {
            try (Connection c = Wal17TestEnv.newSqlConnection()) {
                boolean on = true;
                long deadline = System.currentTimeMillis() + INTERFERENCE_MS;
                int rounds = 0;
                while (System.currentTimeMillis() < deadline) {
                    String toggle = on ? "true" : "false";
                    for (String tbl : new String[]{"t_adv17", "t_adv17_renamed", "t_adv17_toast"}) {
                        try (Statement st = c.createStatement()) {
                            st.execute("ALTER TABLE " + tbl + " SET (autovacuum_enabled = " + toggle + ")");
                            st.execute("ANALYZE " + tbl);
                        } catch (SQLException e) {
                            LOG.debug("干扰语句跳过（{} 场景时序窗口内正常）: {}", tbl, e.getMessage());
                        }
                    }
                    on = !on;
                    rounds++;
                }
                LOG.info("PG17 干扰线程退出: {} 轮 toggle+ANALYZE 风暴", rounds);
            } catch (SQLException e) {
                LOG.warn("PG17 干扰线程连接级失败（风暴提前终止）: {}", e.getMessage());
            }
        }, "pg17-catalog-interference");
    }

    /**
     * 起一个同步器（带凭据 + state 配置），登记进兜底清理面。
     *
     * @param slot    物理槽名
     * @param cfg     检查点配置
     * @param mainOid tracked 基表 oid
     * @return 已运行的同步器
     * @throws SQLException 槽管理或引导失败
     */
    private CatalogSynchronizer startSync(String slot, StateConfig cfg, long mainOid) throws SQLException {
        CatalogSynchronizer sync = CatalogSynchronizer.start(openConn(), slot, layout,
                Wal17TestEnv.username(), Wal17TestEnv.password(), cfg, 0L, mainOid);
        syncs.add(sync);
        return sync;
    }

    /**
     * 开一条引导会话，登记进兜底清理面。
     *
     * @return 已认证的 JDBC 连接
     * @throws SQLException 连接失败
     */
    private Connection openConn() throws SQLException {
        Connection c = Wal17TestEnv.newSqlConnection();
        conns.add(c);
        return c;
    }

    /**
     * 断言物理槽的 restart_lsn 已被推进到检查点 lsn 及之后（与 18 侧生命周期 IT 同式）。
     *
     * @param slot         槽名
     * @param checkpointLsn 检查点 lsn（推进目标下界）
     * @throws SQLException 查询失败
     */
    private static void assertSlotAdvancedTo(String slot, long checkpointLsn) throws SQLException {
        long restartLsn;
        try (Connection c = Wal17TestEnv.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slot + "'")) {
            assertTrue(rs.next(), "槽 " + slot + " 应存在");
            String restart = rs.getString(1);
            assertTrue(restart != null, "推进后的 restart_lsn 不应为 NULL");
            restartLsn = Lsn.parse(restart);
        }
        assertTrue(restartLsn >= checkpointLsn,
                "槽 restart_lsn " + Lsn.format(restartLsn) + " 应已推进到检查点 lsn "
                        + Lsn.format(checkpointLsn) + " 及之后");
    }
}
