package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.replay.CatalogStores;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 5.0.0 catalog 末态对拍 IT（M1 Task 9——M1 验收核心之一）：真 OG 容器上
 * 干扰面（autovacuum toggle / ANALYZE 风暴 / 随机 CHECKPOINT）与全 DDL 场景并发推进，
 * 停流后把同步器 ctid 字典与 JDBC REPEATABLE READ 实查<strong>逐行全等</strong>对拍
 * （attr/class 三表 + relfilenodeOf/toastOf/schemaOf/relkindOf/nameOf + tracked 双
 * ctid + 列序——断言面与 vanilla {@code WalAdversarialIT} 同构，经
 * {@code WalSyncItBase#assertCatalogMatchesJdbc}）。
 *
 * <p><strong>Task 7 真容器冒烟已实证三目录末态对拍全等</strong>（throwaway harness，
 * DDL 序列含 CREATE TABLE/SCHEMA/RENAME/INSERT/UPDATE）——本 IT 把它固化为正式验收：
 * 场景面加宽（DROP COLUMN/TRUNCATE 二连/toast 宽行/交错 DDL/CHECKPOINT×3）并叠加
 * 干扰线程。DML 场景刻意避开 JDBC CopyManager 批量路径（M1 期 OG MULTI_INSERT 解析
 * 即 ISE；M2 T3/T7 已转录真几何并以 COPY 场景对拍验收，本 IT 保持逐条 INSERT 形态
 * ——catalog 面对拍不依赖 MULTI_INSERT）。</p>
 *
 * <p><strong>场景刻意避开的 OG 护栏面</strong>：不建列存（ORIENTATION=COLUMN）/段页式
 * 表——种子期与流内护栏对这类表 ISE（M1 Task 7 设计取舍），场景含它即测的是护栏而非
 * 对拍；分区表护栏已按 M2 T7 修复 B 放宽（parttype='p' 母表行放行入字典），M1 场景
 * 面维持不含分区——分区 DML 行为的验收在 M2 对拍 IT。</p>
 */
class OgCatalogSyncIT extends OgSyncItBase {

    private static final Logger LOG = LoggerFactory.getLogger(OgCatalogSyncIT.class);

    /** 干扰线程持续时长（毫秒）——DDL 场景在其窗口内并发推进。 */
    private static final long INTERFERENCE_MS = 8_000;

    /** 干扰线程 join 上限（毫秒）。 */
    private static final long INTERFERENCE_JOIN_MS = 15_000;

    /**
     * 对拍主用例：干扰 + 全 DDL 场景下，OG 同步器快照字典与 JDBC REPEATABLE READ
     * 实查逐行全等（ctid/字段/列序/toast 映射/tracked），并把重放与 walker 指标
     * 全量记档（resyncs 观测面不设硬断言——内容正确性由对拍承载）。
     */
    @Test
    void adversarialDdlStormReplaysOgCatalogIdenticalToJdbc() throws Exception {
        String slot = registerSlot("og_adv_it");
        long mainOid = prepareTables();
        currentSync = startOgSync(slot, null, 0L, mainOid);
        CatalogSynchronizer sync = currentSync;

        Thread storm = ogInterferenceThread(INTERFERENCE_MS, "og_adv", "og_adv_renamed", "og_adv_toast");
        storm.start();
        try (Connection c1 = OgTestEnv.newSqlConnection();
             Connection c2 = OgTestEnv.newSqlConnection()) {
            runDdlScenario(c1, c2);
        }
        storm.join(INTERFERENCE_JOIN_MS);
        assertTrue(!storm.isAlive(), "干扰线程应在 join 上限内退出");

        // 确定性收尾：死元组清在目标位点之前 + 两表 autovacuum 关闭 + 尾写取 write 目标
        long target;
        try (Connection c = OgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
            st.execute("VACUUM ANALYZE pg_class");
            st.execute("VACUUM ANALYZE pg_attribute");
            st.execute("ALTER TABLE og_adv_renamed SET (autovacuum_enabled = false)");
            st.execute("ALTER TABLE og_adv_toast SET (autovacuum_enabled = false)");
            st.execute("INSERT INTO og_adv_tail VALUES (1)");
            target = writeLsn(c);
        }
        awaitConsumed(sync, target, "OG 干扰 DDL 场景接收前沿到达对拍目标 " + Lsn.format(target));

        long toastOid;
        try (Connection c = OgTestEnv.newSqlConnection()) {
            toastOid = oidOf(c, "og_adv_toast");
        }
        sync.stop();

        try (Connection probe = OgTestEnv.newSqlConnection()) {
            assertCatalogMatchesJdbc(sync, probe, mainOid, mainOid, toastOid);
        }
        Map<String, Long> metrics = sync.metrics();
        LOG.info("OG 对拍指标 slot={}: 重放 {} / walker {}", slot, metrics, sync.streamMetrics());
        assertTrue(metrics.getOrDefault(CatalogStores.CatalogMetrics.REPLAYED, 0L) > 0,
                "场景应产生 catalog 行事件: " + metrics);
    }

    /**
     * 场景前置：清场重建被测主表与尾写表，返回主表 oid（建表在同步器 start 之前
     * ——interest oid 供引导定位 tracked 双 ctid）。
     *
     * @return 主表 og_adv 的 oid
     * @throws SQLException DDL 失败
     */
    private long prepareTables() throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection()) {
            execDdl(c,
                    "DROP TABLE IF EXISTS og_adv",
                    "DROP TABLE IF EXISTS og_adv_renamed",
                    "DROP TABLE IF EXISTS og_adv_toast",
                    "DROP TABLE IF EXISTS og_adv_tail",
                    "DROP SCHEMA IF EXISTS og_adv_nsp CASCADE",
                    "CREATE TABLE og_adv (id int primary key, payload text)",
                    "CREATE TABLE og_adv_tail (v int)");
            return oidOf(c, "og_adv");
        }
    }

    /**
     * DDL 场景序列主体（vanilla 对抗 IT 的裁剪移植）：死元组三连 → 列增删 → 改名 →
     * 截断二连 → toast 表宽行 + CREATE SCHEMA（nsp 字典面）→ 双连接交错 DDL →
     * CHECKPOINT×3（后继首写 FPW 页镜像路径）。每步独立自动提交，批量行全部走逐条
     * INSERT（避开 MULTI_INSERT 记录形态，类 javadoc）。
     *
     * @param c1 主会话
     * @param c2 交错会话（toast 表 + 交错 DDL 段）
     * @throws SQLException          DDL 失败
     * @throws InterruptedException  交错段 join 被中断
     */
    private void runDdlScenario(Connection c1, Connection c2) throws SQLException, InterruptedException {
        execDdl(c1,
                "INSERT INTO og_adv SELECT g, 'payload-' || g || '-' || repeat('x', g % 40) FROM generate_series(1, 300) g",
                "UPDATE og_adv SET payload = payload || '-u' WHERE id % 2 = 0",
                "DELETE FROM og_adv WHERE id % 3 = 0",
                "ALTER TABLE og_adv ADD COLUMN extra_tag int",
                "ALTER TABLE og_adv ADD COLUMN note text DEFAULT 'seed'",
                "ALTER TABLE og_adv DROP COLUMN payload",
                "ALTER TABLE og_adv RENAME TO og_adv_renamed",
                "TRUNCATE og_adv_renamed",
                "INSERT INTO og_adv_renamed (id, extra_tag, note) VALUES (1, 1, 'a'), (2, 2, 'b')",
                "TRUNCATE og_adv_renamed");
        // 带 toast 的表 + 宽行（toast 关系流内重建：pg_class INS + 主表行 reltoastrelid 变更）；
        // CREATE SCHEMA + 新 schema 建表（pg_namespace INS 字典面）；interest 集中途登记
        // （CHM 弱一致）——新关系的 pg_class 行立即进入自愈候选域
        execDdl(c2,
                "CREATE TABLE og_adv_toast (id int primary key, wide text)",
                "INSERT INTO og_adv_toast SELECT g, repeat('x', 12000) FROM generate_series(1, 20) g",
                "CREATE SCHEMA og_adv_nsp",
                "CREATE TABLE og_adv_nsp.scoped (id int, v text)",
                "INSERT INTO og_adv_nsp.scoped VALUES (1, 'nsp')");
        currentSync.stores().interestRelOids().add(oidOf(c2, "og_adv_toast"));
        currentSync.stores().interestRelOids().add(oidOf(c2, "og_adv_nsp.scoped"));
        interleaveDdl(c1, c2);
        // CHECKPOINT×3：后继首写强制 FPW 页镜像路径（页内行提取面）
        try (Statement st = c1.createStatement()) {
            for (int i = 0; i < 3; i++) {
                st.execute("CHECKPOINT");
                Thread.sleep(100);
            }
        }
    }

    /**
     * 两连接交错 DDL：CountDownLatch 双方就绪后同时放行——c1 改主表、c2 改 toast 表，
     * 两表的 pg_attribute/pg_class 行变更在 WAL 流交错下发（同页竞争的乱序覆盖面）。
     * c2 侧失败经 AtomicReference 带回主线程断言（静默吞掉会让场景缺一条腿而假绿）。
     *
     * @param c1 主表会话
     * @param c2 toast 表会话
     * @throws SQLException          主表侧 ALTER 失败
     * @throws InterruptedException  放行等待/join 被中断
     */
    private void interleaveDdl(Connection c1, Connection c2) throws SQLException, InterruptedException {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Exception> sideFailure = new AtomicReference<>();
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                execDdl(c2, "ALTER TABLE og_adv_toast ADD COLUMN appended text DEFAULT 'w'");
            } catch (Exception e) {
                sideFailure.set(e);
            }
        }, "og-interleaved-ddl");
        t2.start();
        ready.countDown();
        go.countDown();
        execDdl(c1, "ALTER TABLE og_adv_renamed ADD COLUMN interleaved int");
        t2.join(10_000);
        assertTrue(!t2.isAlive(), "交错 DDL 线程应在 join 上限内退出");
        assertTrue(sideFailure.get() == null, "交错 DDL 侧（toast 表）不应失败: " + sideFailure.get());
    }

    /** 本用例同步器（startOgSync 后赋值——runDdlScenario 的 interest 登记持有面）。 */
    private CatalogSynchronizer currentSync;
}
