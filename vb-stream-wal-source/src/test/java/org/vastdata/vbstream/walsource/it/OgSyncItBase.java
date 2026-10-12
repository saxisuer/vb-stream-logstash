package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;
import org.vastdata.vbstream.walsource.state.StateConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 5.0.0 catalog 同步 IT 的公共基类（M1 Task 9）——在 {@link WalSyncItBase}
 * 对拍器之上接 OG 环境：布局覆写直取 {@link WalLayoutVOpenGauss} 单例（90204 与
 * vanilla 9.2 撞号，判别面是版本文本而非数值，不走 forServerVersion 数值分发）、
 * {@code CatalogSynchronizer.startOg} + DirectWalSource（裸 v3 直连——Task 8 双通道
 * IT 已验的通道，无 sidecar 依赖）的起停辅助、跨用例清理面（同步器 → 引导会话 → 槽，
 * 停机次序即依赖次序）与 OG 干扰线程工厂（autovacuum toggle / ANALYZE 风暴 / 随机
 * CHECKPOINT——沿 {@code WalAdversarialIT} 配方裁剪，JDBC 侧驱动，语法与 vanilla 同）。
 *
 * <p><strong>对拍目标前沿差异</strong>：OG 无 flush 档位点函数（{@code pg_current_xlog_
 * location()} 是 write 位点）——vanilla 基类的 {@code flushLsn} 在 OG 不可用，本基类
 * 提供 {@link #writeLsn}（Task 7 遗留实测项：write 位点续传语义的观察面，见
 * OgLifecycleIT 场景注记）。</p>
 */
abstract class OgSyncItBase extends WalSyncItBase {

    private static final Logger LOG = LoggerFactory.getLogger(OgSyncItBase.class);

    /** OG 直连通道反馈心跳的 statusInterval（毫秒）——对齐 Task 8 双通道 IT 的 5s 档。 */
    private static final long DIRECT_STATUS_INTERVAL_MS = 5_000;

    /** 干扰循环单轮内执行随机 CHECKPOINT 的概率（{@code WalAdversarialIT} 配方的裁剪档）。 */
    private static final double CHECKPOINT_PROBABILITY = 0.3;

    /** 跨用例的槽名序号（每用例独立槽，兜底清理统一删除）。 */
    private static final AtomicLong SLOT_SEQ = new AtomicLong();

    /** 本用例启动的同步器——AfterEach 兜底停机。 */
    private final List<CatalogSynchronizer> syncs = new ArrayList<>();

    /** 本用例持有的引导会话（probe / 槽推进复用）——AfterEach 兜底关闭。 */
    private final List<Connection> conns = new ArrayList<>();

    /** 本用例用过的槽——AfterEach 兜底删除。 */
    private final List<String> slots = new ArrayList<>();

    /**
     * 布局覆写：openGauss 档直取单例描述符（撞号判别见类 javadoc）。
     *
     * @return {@code WalLayoutVOpenGauss.INSTANCE}
     */
    @Override
    protected WalLayout resolveLayout() {
        return WalLayoutVOpenGauss.INSTANCE;
    }

    /**
     * 兜底清理：停全部同步器 → 关全部引导会话 → 删全部槽（正常路径测试内已完成，
     * 这里是异常路径保险；停机次序即依赖次序：probe 先于会话消亡）。
     */
    @AfterEach
    void cleanupOg() {
        for (CatalogSynchronizer s : syncs) {
            s.stop();
        }
        for (Connection c : conns) {
            try {
                c.close();
            } catch (SQLException e) {
                LOG.warn("关闭 OG 引导会话失败: {}", e.getMessage());
            }
        }
        for (String s : slots) {
            OgTestEnv.dropPhysicalSlotQuietly(s);
        }
    }

    /**
     * 登记一个独立槽名（前置清理残留 + 记账进兜底删除面）。
     *
     * @param prefix 槽名前缀（后接自增序号）
     * @return 本用例唯一的槽名
     */
    protected String registerSlot(String prefix) {
        String slot = prefix + "_" + SLOT_SEQ.incrementAndGet();
        slots.add(slot);
        OgTestEnv.dropPhysicalSlotQuietly(slot);
        return slot;
    }

    /**
     * 开一条引导会话（healer probe / 槽推进复用），登记进兜底清理面——
     * {@code startOg} 后归同步器独占（单写者契约）。
     *
     * @return 已认证的 opengauss-jdbc 连接
     * @throws SQLException 连接失败
     */
    protected Connection openOgConn() throws SQLException {
        Connection c = OgTestEnv.newSqlConnection();
        conns.add(c);
        return c;
    }

    /**
     * 起一个 OG 档同步器（direct 通道装配——槽确保走 ensureSlotOg、续传/引导决策与
     * bootstrap 共核、字节源为 {@link DirectWalSource} 无槽裸流：物理流本身不附槽，
     * 保留窗口由检查点后槽推进承载，与 Task 8 双通道 IT 的通道形态一致）。
     *
     * @param slot           物理槽名
     * @param cfg            检查点配置（null = 禁用）
     * @param forcedStartLsn 显式流起点（0 = 按决策）
     * @param interestOid    tracked 面 oid
     * @return 已运行的同步器（登记进兜底停机面）
     * @throws SQLException 槽管理或引导失败
     */
    protected CatalogSynchronizer startOgSync(String slot, StateConfig cfg, long forcedStartLsn,
            long... interestOid) throws SQLException {
        Connection c = openOgConn();
        CatalogSynchronizer sync = CatalogSynchronizer.startOg(c, slot, layout, cfg, forcedStartLsn,
                startLsn -> new DirectWalSource(OgTestEnv.host(), OgTestEnv.port(),
                        OgTestEnv.USER, OgTestEnv.PASSWORD, null, DIRECT_STATUS_INTERVAL_MS, startLsn),
                interestOid);
        syncs.add(sync);
        return sync;
    }

    /**
     * 服务端当前 write 位点（OG 对拍目标前沿的取值面——OG 无 flush 档函数，与引导
     * 一致性次序里的 B 同源；write 位点续传语义的观察面，Task 7 遗留实测项）。
     *
     * @param c 会话连接
     * @return {@code pg_current_xlog_location()} 打包 long
     * @throws SQLException 查询失败
     */
    protected static long writeLsn(Connection c) throws SQLException {
        return Lsn.parse(queryString(c, "SELECT pg_current_xlog_location()"));
    }

    /**
     * 观测槽推进面（不硬断言——Task 6 concern 4 的 OG 物理槽 restart_lsn 保留窗口
     * 行为未实测收口，本任务为观察非红线）：读当前 restart_lsn 与检查点 lsn 对比
     * 记档；同步器报告推进成功（lastSlotAdvanceLsn 到位）时才做一致性断言。
     *
     * @param slot         槽名
     * @param sync         刚停机的同步器（推进观测源）
     * @param checkpointLsn 最终检查点 lsn
     * @throws SQLException 槽查询失败
     */
    protected static void observeSlotAdvance(String slot, CatalogSynchronizer sync, long checkpointLsn)
            throws SQLException {
        String restart = null;
        try (Connection c = OgTestEnv.newSqlConnection();
             var st = c.createStatement();
             var rs = st.executeQuery(
                     "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slot + "'")) {
            if (rs.next()) {
                restart = rs.getString(1);
            }
        }
        long advanced = sync.lastSlotAdvanceLsn();
        LOG.info("OG 槽推进观测: slot={} checkpoint={} lastAdvance={} restart_lsn={}",
                slot, Lsn.format(checkpointLsn), Lsn.format(advanced), restart);
        // 推进报告成功（lastAdvance ≥ checkpoint）时，restart_lsn 须真实落位——
        // 客户端视角与服务端状态背离即推进面异常（此时才升级为断言）
        if (advanced >= checkpointLsn && checkpointLsn > 0 && restart != null) {
            assertTrue(Lsn.parse(restart) >= checkpointLsn,
                    "推进已报告成功但 restart_lsn 未落位: " + restart + " < " + Lsn.format(checkpointLsn));
        }
    }

    /**
     * OG 干扰线程工厂（沿 {@code WalAdversarialIT} 配方裁剪：autovacuum toggle +
     * ANALYZE 风暴 + 随机 CHECKPOINT）：独立连接循环直至 deadline——逐表
     * {@code ALTER TABLE SET (autovacuum_enabled)} 真/假交替（合法反复改 pg_class 行）+
     * {@code ANALYZE}（pg_class INPLACE + 统计写放大），每轮以 30% 概率插入 CHECKPOINT
     * （后继首写带 FPW 页镜像路径）。
     *
     * <p>容错语义：单条语句失败（表在场景推进中改名/尚未建/已删的时间窗交错，或
     * OG 对个别 reloption 的支持差异）仅 DEBUG 记录并继续；连接级失败才终止线程并
     * WARN——干扰不得掩盖主断言，也不得因场景时序提前退场。</p>
     *
     * @param durationMs 持续时长（毫秒）
     * @param tables     被测表名集（RENAME 前后名都传入）
     * @return 未启动线程（调用方 start）
     */
    protected Thread ogInterferenceThread(long durationMs, String... tables) {
        return new Thread(() -> {
            try (Connection c = OgTestEnv.newSqlConnection()) {
                boolean on = true;
                long deadline = System.currentTimeMillis() + durationMs;
                int rounds = 0;
                while (System.currentTimeMillis() < deadline) {
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
                    rounds++;
                }
                LOG.info("OG 干扰线程退出: {} 轮 toggle+ANALYZE(+随机CHECKPOINT) 风暴", rounds);
            } catch (SQLException e) {
                LOG.warn("OG 干扰线程连接级失败（风暴提前终止）: {}", e.getMessage());
            }
        }, "og-catalog-interference");
    }
}
