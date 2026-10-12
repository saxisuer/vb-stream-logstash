package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M2 双路对拍冒烟 IT（T6）——验证对拍 machinery 本身：m2pg（PG 兼容档）库上
 * 一笔事务的 INSERT/UPDATE/DELETE 三行变更，wal 直解路（{@link WalSource} DML 面 +
 * {@link OgParityBaseline.WalCapture} 离线捕获）与逻辑解码基准路
 * （{@link OgParityBaseline}——opengauss-jdbc in-process 逻辑流 + OG pgoutput 帧归一化器）
 * 各自独立槽位捕获，按 xid 交集归一化 diff 断言空（对齐 {@code DualPathParityIT} 形态；
 * engine 路不可复用——vanilla pgjdbc 对 OG 逻辑流首读 FATAL，T0 路③实证）。
 *
 * <p><b>场景形态裁定</b>：①三行 DML 走<b>单事务</b>（I→U→D 同一行的完整生命周期）——
 * 既钉住基准路完整帧链 {@code B→R→I/U/D→C}（分事务会拆成三段窗口、增起停噪声），
 * 也让桶内行序断言无歧义；②表 {@code REPLICA IDENTITY FULL}——OG 的 DEFAULT 身份
 * U/D 帧 'K' 段非身份列以 'n' 占位（spike §2），而 wal 路对键元组解码的尾列补 null
 * 渲染面与占位语义的归一化是 T7 记档项，FULL 身份两路恒整行前像（'O' 段，og-plan
 * 容器 2026-10-12 实测锚）使冒烟的对拍面无该已知分叉；③断言式三行全对（路 B）——
 * 双路 diff 之外另按字面断言三行内容，machinery 假绿（两侧同错）即红。</p>
 *
 * <p><b>时序（spike 坑 2）</b>：建表 → 建 publication → 建逻辑槽 → 起 wal 直解路（物理槽
 * + 引导）→ 写 DML → 双路追平 → 基准路起流收帧（槽建于 DML 前，起流晚于提交也完整
 * 收达）。槽/publication 命名带 {@code m2par} 前缀避开 M1 物理槽清理面。需本机 Docker。</p>
 *
 * <p><b>分区冒烟扩展（M3 Task 4，为 T5 分区专项铺轨）</b>：分区表（RANGE 两分区）单事务
 * I/U/D 双路 diff 空——wal 路经 pg_partition 第四字典身份链（M3 T2）以<b>母表名</b>
 * 渲染分区行、基准路 R/I/U/D 帧天然报母表（T0 锚② {@code PARTITION_NAME=mother}），
 * 输出母表名双路同形即本用例的受测面；分区名（p1/p2）双路渲染面零出现。两条实测前提
 * （og-plan 2026-10-13 锚，M3 T4 探针）：①OG 的 {@code FOR ALL TABLES} publication
 * <b>不含分区表</b>——分区场景 publication 须显式 {@code FOR TABLE <母表>}（缺此则基准
 * 路对分区 DML 收空块，双路交集为空假红）；②母表 {@code REPLICA IDENTITY FULL}
 * 传播到分区行——U/D 帧带 'O' 整行旧元组，DEFAULT 身份的 'K' 段占位分叉天然规避。
 * 本用例兼作 {@link OgTestEnv.CompatMode} 参数化基建的端到端消费锚（M3 T4 产、T5 的
 * A 档全量复验消费）。</p>
 */
class OgParitySmokeIT {

    private static final Logger LOG = LoggerFactory.getLogger(OgParitySmokeIT.class);

    /** 对拍表名（m2pg 库 public schema）。 */
    private static final String TABLE = "t_m2_smoke";

    /** publication 名（FOR ALL TABLES——m2pg 库内只有本场景的表）。 */
    private static final String PUBLICATION = "m2par_pub";

    /** 基准路逻辑槽名。 */
    private static final String LOGICAL_SLOT = "m2par_logic_smoke";

    /** wal 直解路物理槽名。 */
    private static final String PHYSICAL_SLOT = "m2par_wal_smoke";

    /** 分区冒烟的母表名（m2pg 库 public schema——分区行经身份链归一到本表身份渲染）。 */
    private static final String TABLE_PART = "t_m2_smoke_part";

    /** 分区冒烟的基准路逻辑槽名（独立槽名——与普通冒烟互不清理）。 */
    private static final String LOGICAL_SLOT_PART = "m2par_logic_smoke_part";

    /** 分区冒烟的 wal 直解路物理槽名。 */
    private static final String PHYSICAL_SLOT_PART = "m2par_wal_smoke_part";

    /** 两路各自的追平等待上限（毫秒）——含 wal 路引导余量。 */
    private static final long AWAIT_MS = 60_000;

    /** 轮询间隔（毫秒）。 */
    private static final long POLL_MS = 200;

    /** 本用例的 WalSource（AfterEach 兜底关闭）。 */
    private WalSource wal;

    /**
     * 兜底清理：关 WalSource（幂等）→ 删两路槽（正常路径测试内已完成，这里是异常路径
     * 保险——槽名带 m2par 前缀，不触碰 M1 物理槽清理面）。
     */
    @AfterEach
    void cleanupSlots() {
        if (wal != null) {
            wal.close();
        }
        OgParityBaseline.dropLogicalSlotQuietly(LOGICAL_SLOT);
        OgTestEnv.dropPhysicalSlotQuietly(PHYSICAL_SLOT);
    }

    /**
     * 分区冒烟槽的兜底清理（独立 @AfterEach——分区用例自己的两路槽；正常路径用例内
     * 已完成，这里是异常路径保险。JUnit 每用例新实例，普通冒烟用例跑本清理是槽不
     * 存在的空转 no-op）。
     */
    @AfterEach
    void cleanupPartitionSlots() {
        OgParityBaseline.dropLogicalSlotQuietly(LOGICAL_SLOT_PART, OgTestEnv.CompatMode.PG);
        OgTestEnv.dropPhysicalSlotQuietly(PHYSICAL_SLOT_PART);
    }

    /**
     * 冒烟对拍：m2pg 上单事务 I/U/D 三行，双路归一化 diff 空 + 断言式三行全对。
     *
     * <p>关键步骤：①重置场景（表/publication 重建——DDL 先于两路槽位与引导）；②建逻辑
     * 槽；③起 wal 直解路（direct 通道 + opengauss-jdbc SQL 会话前缀 + DML 白名单钉到
     * 对拍表）；④单事务写 I/U/D；⑤等待 wal 路发射 1 桶（volatile 计数轮询）；⑥基准路
     * capture 收 1 个提交事务；⑦关 wal 路（close 含接收线程 join——WalCapture 的
     * happens-before）→ 双路 diff + 字面断言。边界与异常语义：任一路等待超时/启动失败
     * 即 fail；两路捕获到窗口差事务时 assertParity 的交集归一化吸收（INFO 留痕）。</p>
     *
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    @Test
    void insertUpdateDeleteSingleTxnParityAcrossPaths() throws Exception {
        resetScenario();
        OgParityBaseline.createLogicalSlot(LOGICAL_SLOT);

        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfig(), capture);
        wal.start();
        assertTrue(wal.dmlEnabled(), "wal 直解路 DML 面应装配（listener 在场 + dml 缺省 true）");

        try (Connection conn = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG)) {
            conn.setAutoCommit(false);
            exec(conn,
                    "INSERT INTO " + TABLE + " VALUES (1, 'alpha', 12.5)",
                    "UPDATE " + TABLE + " SET name = 'alpha-u', val = 13.75 WHERE id = 1",
                    "DELETE FROM " + TABLE + " WHERE id = 1");
            conn.commit();
        }

        await(() -> wal.dmlEmittedBuckets() >= 1,
                "OG 对拍冒烟: wal 路未发射事务桶（buckets=" + wal.dmlEmittedBuckets()
                        + ", terminal=" + wal.receiverTerminalFailure() + "）");
        List<OgBlock> baseline = OgParityBaseline.capture(LOGICAL_SLOT, PUBLICATION, 1, 30_000);
        wal.close();

        List<OgBlock> walBlocks = capture.blocks();
        OgParityBaseline.assertParity(baseline, walBlocks, "OG 对拍冒烟", 0);

        // 路 B 断言式三行全对（双路 diff 之外的字面锚——machinery 两侧同错即红）
        assertEquals(1, walBlocks.size(), "wal 路应恰一个事务块:\n" + walBlocks);
        List<String> rows = walBlocks.get(0).rows();
        assertEquals(List.of(
                "INSERT public." + TABLE + " BEFORE=- AFTER=[id=1, name=alpha, val=12.5]",
                "UPDATE public." + TABLE
                        + " BEFORE=[id=1, name=alpha, val=12.5] AFTER=[id=1, name=alpha-u, val=13.75]",
                "DELETE public." + TABLE + " BEFORE=[id=1, name=alpha-u, val=13.75] AFTER=-"),
                rows, "wal 路三行字面全对（I/U/D 完整生命周期）");
        LOG.info("OG 对拍冒烟通过: xid={} commitLsn=0x{} 行数={}",
                walBlocks.get(0).xid(), Long.toHexString(walBlocks.get(0).commitLsn()), rows.size());
    }

    /**
     * 分区冒烟对拍（M3 Task 4）：RANGE 两分区母表上单事务四行 DML——INSERT 各路由
     * p1/p2、UPDATE 落 p1、DELETE 落 p2（I/U/D 三形态在两分区上分布覆盖），双路归一化
     * diff 空 + 断言式四行全对（全行母表名——输出母表名同形验证）+ 分区名零出现。
     *
     * <p>关键步骤：①重置分区场景（母表 FULL 身份 + 显式 {@code FOR TABLE} publication
     * ——两条实测前提见类 javadoc）；②建逻辑槽（{@link OgTestEnv.CompatMode} 参数化
     * 入口的端到端消费锚）；③起 wal 直解路（白名单钉<b>母表名</b>——M3 白名单基准
     * 形态，分区行经第四字典身份链归一后匹配）；④单事务写四行；⑤等 wal 路发射 1 桶 →
     * 基准路收 1 个提交事务 → close → diff + 字面断言。边界与异常语义：任一路等待超时
     * 即 fail；双路窗口差事务由 assertParity 交集归一化吸收（INFO 留痕）。</p>
     *
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    @Test
    void partitionedTableDmlParityAcrossPaths() throws Exception {
        resetPartitionScenario();
        OgParityBaseline.createLogicalSlot(LOGICAL_SLOT_PART, OgTestEnv.CompatMode.PG);

        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(walSourceConfigFor(PHYSICAL_SLOT_PART, "public." + TABLE_PART,
                OgTestEnv.DATABASE_M2PG), capture);
        wal.start();
        assertTrue(wal.dmlEnabled(), "分区冒烟: wal 直解路 DML 面应装配");

        try (Connection conn = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG)) {
            conn.setAutoCommit(false);
            exec(conn,
                    "INSERT INTO " + TABLE_PART + " VALUES (1, 10, 'alpha', 12.5)",
                    "INSERT INTO " + TABLE_PART + " VALUES (2, 150, 'beta', 2.25)",
                    "UPDATE " + TABLE_PART + " SET name = 'alpha-u', val = 13.75 WHERE id = 1",
                    "DELETE FROM " + TABLE_PART + " WHERE id = 2");
            conn.commit();
        }

        await(() -> wal.dmlEmittedBuckets() >= 1,
                "分区冒烟: wal 路未发射事务桶（buckets=" + wal.dmlEmittedBuckets()
                        + ", terminal=" + wal.receiverTerminalFailure() + "）");
        List<OgBlock> baseline = OgParityBaseline.capture(LOGICAL_SLOT_PART, PUBLICATION, 1, 30_000,
                OgTestEnv.CompatMode.PG);
        wal.close();

        List<OgBlock> walBlocks = capture.blocks();
        OgParityBaseline.assertParity(baseline, walBlocks, "分区冒烟", 0);

        // 路 B 断言式四行全对——全行母表名（输出母表名同形，T0 锚② PARTITION_NAME=mother）
        assertEquals(1, walBlocks.size(), "分区冒烟 wal 路应恰一个事务块:\n" + walBlocks);
        List<String> rows = walBlocks.get(0).rows();
        assertEquals(List.of(
                "INSERT public." + TABLE_PART + " BEFORE=- AFTER=[id=1, k=10, name=alpha, val=12.5]",
                "INSERT public." + TABLE_PART + " BEFORE=- AFTER=[id=2, k=150, name=beta, val=2.25]",
                "UPDATE public." + TABLE_PART
                        + " BEFORE=[id=1, k=10, name=alpha, val=12.5]"
                        + " AFTER=[id=1, k=10, name=alpha-u, val=13.75]",
                "DELETE public." + TABLE_PART
                        + " BEFORE=[id=2, k=150, name=beta, val=2.25] AFTER=-"),
                rows, "分区冒烟 wal 路四行字面全对（I 路由 p1/p2 + U 落 p1 + D 落 p2，全行母表名）");
        assertTrue(rows.stream().noneMatch(r -> r.contains(".p1") || r.contains(".p2")),
                "分区名不得出现在渲染面（双路归一到母表名）:\n" + rows);
        LOG.info("OG 分区冒烟通过: xid={} commitLsn=0x{} 行数={}",
                walBlocks.get(0).xid(), Long.toHexString(walBlocks.get(0).commitLsn()), rows.size());
    }

    /**
     * 分区场景重置：两路槽位前置清理 → 母表重建（RANGE 两分区 + 母表 REPLICA IDENTITY
     * FULL——传播到分区行使 U/D 帧带 'O' 整行旧元组，类 javadoc 前提②）→ publication
     * 重建（<b>显式 {@code FOR TABLE} 母表</b>——OG 的 FOR ALL TABLES 不含分区表，类
     * javadoc 前提①）。DDL 先于建槽/引导（spike 坑 2 的时序契约）。
     *
     * @throws SQLException DDL 失败
     */
    private static void resetPartitionScenario() throws SQLException {
        OgParityBaseline.dropLogicalSlotQuietly(LOGICAL_SLOT_PART, OgTestEnv.CompatMode.PG);
        OgTestEnv.dropPhysicalSlotQuietly(PHYSICAL_SLOT_PART);
        try (Connection c = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE_PART);
            st.execute("CREATE TABLE " + TABLE_PART
                    + " (id int4, k int4, name text, val numeric) PARTITION BY RANGE (k)"
                    + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200))");
            st.execute("ALTER TABLE " + TABLE_PART + " REPLICA IDENTITY FULL");
            st.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION);
            st.execute("CREATE PUBLICATION " + PUBLICATION + " FOR TABLE public." + TABLE_PART);
        }
    }

    /**
     * 分区冒烟的 wal 直解路门面配置：direct 通道 + opengauss-jdbc SQL 会话前缀 + DML
     * 白名单钉<b>母表名</b>（M3 白名单基准形态——写母表名即覆盖其全部分区）；库名显式
     * 参数化（CompatMode 维度基建的同构面——T5 双库复跑时场景代码仅换此参）。
     *
     * @param physicalSlot 物理槽名
     * @param table        白名单表全名（母表）
     * @param database     目标库名
     * @return WalSource 构造入参配置
     */
    private static Properties walSourceConfigFor(String physicalSlot, String table, String database) {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, OgTestEnv.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(OgTestEnv.port()));
        cfg.setProperty(WalSource.KEY_DB, database);
        cfg.setProperty(WalSource.KEY_USER, OgTestEnv.USER);
        cfg.setProperty(WalSource.KEY_PASS, OgTestEnv.PASSWORD);
        cfg.setProperty(WalSource.KEY_SLOT, physicalSlot);
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        cfg.setProperty(WalSource.KEY_TABLES, table);
        return cfg;
    }

    /**
     * 场景重置：两路槽位前置清理 → 表重建（FULL 身份）→ publication 重建（FOR ALL
     * TABLES）。DDL 先于建槽/引导——两路起流时见表的最终形态。
     *
     * @throws SQLException DDL 失败
     */
    private static void resetScenario() throws SQLException {
        OgParityBaseline.dropLogicalSlotQuietly(LOGICAL_SLOT);
        OgTestEnv.dropPhysicalSlotQuietly(PHYSICAL_SLOT);
        try (Connection c = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " (id int4 PRIMARY KEY, name text, val numeric)");
            st.execute("ALTER TABLE " + TABLE + " REPLICA IDENTITY FULL");
            st.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION);
            st.execute("CREATE PUBLICATION " + PUBLICATION + " FOR ALL TABLES");
        }
    }

    /**
     * wal 直解路门面配置：六键指向 m2pg（PG 兼容档——DML 护栏放行前提）+ direct 通道
     * + opengauss-jdbc SQL 会话前缀（OG 档 sha256）+ DML 白名单钉到对拍表；state 缺省
     * 禁用（冒烟单 pass，无需续传面）。
     *
     * @return WalSource 构造入参配置
     */
    private static Properties walSourceConfig() {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, OgTestEnv.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(OgTestEnv.port()));
        cfg.setProperty(WalSource.KEY_DB, OgTestEnv.DATABASE_M2PG);
        cfg.setProperty(WalSource.KEY_USER, OgTestEnv.USER);
        cfg.setProperty(WalSource.KEY_PASS, OgTestEnv.PASSWORD);
        cfg.setProperty(WalSource.KEY_SLOT, PHYSICAL_SLOT);
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        cfg.setProperty(WalSource.KEY_TABLES, "public." + TABLE);
        return cfg;
    }

    /**
     * 轮询等待条件成立（{@link #POLL_MS} 间隔、{@link #AWAIT_MS} 上限），超时 fail。
     *
     * @param cond 条件（幂等读）
     * @param what 等待对象描述（超时消息上下文）
     * @throws InterruptedException 睡眠被中断
     */
    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_MS;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("等待超时: " + what);
            }
            Thread.sleep(POLL_MS);
        }
    }

    /**
     * 在连接上顺序执行语句（各自独立执行——事务边界由调用方的 autoCommit/commit 控制）。
     *
     * @param conn 连接
     * @param sqls 语句
     * @throws SQLException 任一语句失败
     */
    private static void exec(Connection conn, String... sqls) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : sqls) {
                st.execute(sql);
            }
        }
    }
}
