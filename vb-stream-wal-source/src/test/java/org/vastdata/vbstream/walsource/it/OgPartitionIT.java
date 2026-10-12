package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.WalCapture;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M3 分区专项 IT（Task 5 Step 2，PG+A 双档参数化）——pg_partition 第四字典
 * 身份链（T2 捕获链）的端到端验收面，六个专项：
 *
 * <ul>
 *   <li><b>range/list 捕获对拍</b>：两种分区策略的母表单事务混写（I/U/D × 双分区路由），
 *       wal 直解路（母表身份渲染，T0 锚② {@code PARTITION_NAME=mother}）与逻辑解码
 *       基准路（R/I/U/D 帧天然报母表）按 xid 交集逐行 diff 空——输出命名对齐基准路。</li>
 *   <li><b>白名单四形态</b>（wal 路专项）：母表名（基准形态——覆盖全部分区）/ 分区名
 *       （兼容增强——只命中该分区路由，不外溢）/ 双命中（并集）/ 都不中（排除）。</li>
 *   <li><b>流内 CREATE·DROP PARTITION 竞速</b>（as-of）：wal 路引导后 ADD PARTITION
 *       （第四字典流内 INS）→ 新分区行即刻可解析 → DROP PARTITION（字典行删除）→
 *       幸存分区行照常捕获。</li>
 *   <li><b>EXCHANGE 后 TableMeta 失效</b>（wal 路专项）：交换段号后旧缓存身份即时
 *       作废——换挂母表的新段行渲母表、被换出的段上行渲普通表（失效第四调用点钉）。</li>
 *   <li><b>UPDATE 跨分区迁移两行形态</b>：分区键 UPDATE 跨分区在行级落 DELETE+INSERT
 *       两行（母表身份），基准路对拍 diff 空。</li>
 * </ul>
 *
 * <p><b>两条实测前提（T4 记档锚，og-plan 2026-10-13）</b>：①OG 的 {@code FOR ALL TABLES}
 * publication <b>不含分区表</b>——凡走基准路的场景 publication 显式 {@code FOR TABLE
 * <母表>}；②母表 {@code REPLICA IDENTITY FULL} 传播分区行——U/D 帧 'O' 整行旧元组，
 * DEFAULT 身份的 'K' 段占位分叉天然规避。分区 DDL 语法（ADD/DROP/EXCHANGE PARTITION）
 * 与跨分区 UPDATE 双档实测同构（og-plan 2026-10-13 PG/A 双库探针）。场景库 = m2pg/m2a
 * （{@link OgTestEnv.CompatMode} 维度，场景体零复制）；schema {@code m3part} 每场景
 * CASCADE 清场。需本机 Docker。</p>
 */
class OgPartitionIT {

    /** 分区场景 schema（每场景 CASCADE 清场——与 OgDmlParityIT 的 parity schema 隔离）。 */
    private static final String SCHEMA = "m3part";

    /** publication 名（显式 FOR TABLE 母表——前提①，FOR ALL TABLES 不含分区表）。 */
    private static final String PUBLICATION = "m3part_pub";

    /** 基准路 capture 的 drain 上限（毫秒）。 */
    private static final long CAPTURE_MS = 30_000;

    /** 当前用例的兼容档（缺省 PG——AfterEach 删逻辑槽按档取库）。 */
    private OgTestEnv.CompatMode mode = OgTestEnv.CompatMode.PG;

    /** 当前用例的两路槽名。 */
    private String logicalSlot;

    private String physicalSlot;

    /** 当前用例的 WalSource（AfterEach 兜底关闭）。 */
    private WalSource wal;

    // ---- 专项一：range/list 捕获对拍 ----

    /** range+list 双策略母表单事务混写对拍（PG 档）。 */
    @Test
    void rangeAndListPartitionedDmlParityAcrossPaths() throws Exception {
        rangeAndListParityScenario(OgTestEnv.CompatMode.PG);
    }

    /** range+list 双策略母表单事务混写对拍（A 档——M3 开门目标档）。 */
    @Test
    void rangeAndListPartitionedDmlParityAcrossPathsACompat() throws Exception {
        rangeAndListParityScenario(OgTestEnv.CompatMode.A);
    }

    /**
     * 专项一场景体：RANGE 两分区 + LIST 两分区各一母表（FULL 身份 + 显式 FOR TABLE
     * publication，两条前提），单事务 4 INSERT（双策略 × 双分区路由）+ 1 UPDATE（range
     * p1）+ 1 DELETE（list pb）；断言三面：assertParity 交集逐行 diff 空（输出命名对齐
     * 基准路的受测面）/ wal 路六行全母表名 + I/U/D 形态分布逐条数对 / 分区名
     * （.p1/.p2/.pa/.pb）零出现。
     *
     * @param compatMode 兼容档（取场景库）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private void rangeAndListParityScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE m3part.t_prange (id int4, k int4, v text) PARTITION BY RANGE (k)"
                        + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200))",
                "CREATE TABLE m3part.t_plist (id int4, k int4, v text) PARTITION BY LIST (k)"
                        + " (PARTITION pa VALUES (1), PARTITION pb VALUES (2))",
                "ALTER TABLE m3part.t_prange REPLICA IDENTITY FULL",
                "ALTER TABLE m3part.t_plist REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"m3part.t_prange", "m3part.t_plist"}, compatMode);
        slots("rl1", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        WalCapture capture = new WalCapture();
        wal = new WalSource(walSourceConfig(compatMode,
                new String[]{"m3part.t_prange", "m3part.t_plist"}), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "range/list 分区对拍: DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn,
                        "INSERT INTO m3part.t_prange VALUES (1, 10, 'ra'), (2, 150, 'rb')",
                        "INSERT INTO m3part.t_plist VALUES (3, 1, 'la'), (4, 2, 'lb')",
                        "UPDATE m3part.t_prange SET v = 'ra-u' WHERE id = 1",
                        "DELETE FROM m3part.t_plist WHERE id = 4");
                conn.commit();
            }
            OgDmlParityIT.await(() -> wal.dmlEmittedBuckets() >= 1,
                    "range/list 分区对拍: wal 路未发射事务桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } finally {
            wal.close();
        }
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 1, CAPTURE_MS, compatMode);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "range/list 分区对拍", 0);

        List<String> rows = capture.blocks().get(0).rows();
        assertEquals(6, rows.size(), "六行（4 I + 1 U + 1 D）:\n" + rows);
        assertEquals(2, rows.stream().filter(r -> r.startsWith("INSERT m3part.t_prange")).count(),
                "range 母表应两行 INSERT（双分区路由）:\n" + rows);
        assertEquals(2, rows.stream().filter(r -> r.startsWith("INSERT m3part.t_plist")).count(),
                "list 母表应两行 INSERT（双分区路由）:\n" + rows);
        assertEquals(1, rows.stream().filter(r -> r.startsWith("UPDATE m3part.t_prange")).count(),
                "range 母表应一行 UPDATE:\n" + rows);
        assertEquals(1, rows.stream().filter(r -> r.startsWith("DELETE m3part.t_plist")).count(),
                "list 母表应一行 DELETE:\n" + rows);
        assertTrue(rows.stream().noneMatch(r ->
                        r.contains(".p1") || r.contains(".p2") || r.contains(".pa") || r.contains(".pb")),
                "分区名不得出现在渲染面（双路归一到母表名）:\n" + rows);
    }

    // ---- 专项二：白名单四形态 ----

    /** 白名单形态 1/4：母表名条目 = 基准形态（PG 档）。 */
    @Test
    void whitelistMotherNameCoversAllPartitions() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.PG, "wf-mother",
                new String[]{"m3part.t_prange", "m3part.t_ctrl"}, 3, "母表名（基准形态）", true);
    }

    /** 白名单形态 1/4 的 A 档形态。 */
    @Test
    void whitelistMotherNameCoversAllPartitionsACompat() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.A, "wf-mother",
                new String[]{"m3part.t_prange", "m3part.t_ctrl"}, 3, "母表名（基准形态）", true);
    }

    /** 白名单形态 2/4：分区名条目 = 兼容增强——只命中该分区路由，不外溢（PG 档）。 */
    @Test
    void whitelistPartitionNameAdmitsOnlyThatPartition() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.PG, "wf-part",
                new String[]{"m3part.p1", "m3part.t_ctrl"}, 2, "分区名（兼容增强）", false);
    }

    /** 白名单形态 2/4 的 A 档形态。 */
    @Test
    void whitelistPartitionNameAdmitsOnlyThatPartitionACompat() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.A, "wf-part",
                new String[]{"m3part.p1", "m3part.t_ctrl"}, 2, "分区名（兼容增强）", false);
    }

    /** 白名单形态 3/4：双命中 = 并集（PG 档）。 */
    @Test
    void whitelistMotherAndPartitionBothAdmitUnion() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.PG, "wf-both",
                new String[]{"m3part.t_prange", "m3part.p1", "m3part.t_ctrl"}, 3, "双命中（并集）", true);
    }

    /** 白名单形态 3/4 的 A 档形态。 */
    @Test
    void whitelistMotherAndPartitionBothAdmitUnionACompat() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.A, "wf-both",
                new String[]{"m3part.t_prange", "m3part.p1", "m3part.t_ctrl"}, 3, "双命中（并集）", true);
    }

    /** 白名单形态 4/4：母表名/分区名都不中 = 排除（仅控制行，PG 档）。 */
    @Test
    void whitelistNeitherMotherNorPartitionExcludesPartitionRows() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.PG, "wf-neither",
                new String[]{"m3part.t_ctrl"}, 1, "都不中（排除）", false);
    }

    /** 白名单形态 4/4 的 A 档形态。 */
    @Test
    void whitelistNeitherMotherNorPartitionExcludesPartitionRowsACompat() throws Exception {
        whitelistRoundScenario(OgTestEnv.CompatMode.A, "wf-neither",
                new String[]{"m3part.t_ctrl"}, 1, "都不中（排除）", false);
    }

    /**
     * 专项二单形态场景体（<b>每形态独立全量 reset</b>——schema CASCADE 重建表族使上一
     * 用例的重放尾窗（页对齐引导起点与 B 之间的重叠段）落在已 DROP 的旧段号上、字典
     * miss 零发射，桶计数才可精确断言；四形态共表不 reset 会互吃对方的重放尾窗）：
     * RANGE 母表（p1&lt;100/p2&lt;200）+ 控制普通表，单事务两分区路由行 + 控制行，按 T2
     * 落定的运行期双查语义断言实付行数与外溢负例；分区名全程零出现（白名单命中分区名
     * 也以母表身份发射）。
     *
     * @param compatMode   兼容档（取场景库）
     * @param suffix       槽名后缀
     * @param whitelist    本形态白名单（形态变量）
     * @param expectedRows 期望实付行数
     * @param formName     形态名（断言消息上下文）
     * @param p2Admitted   true = p2 路由行应入发射面（母表名/双命中形态）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private void whitelistRoundScenario(OgTestEnv.CompatMode compatMode, String suffix, String[] whitelist,
            int expectedRows, String formName, boolean p2Admitted) throws Exception {
        String[] ddl = {
                "CREATE TABLE m3part.t_prange (id int4, k int4, v text) PARTITION BY RANGE (k)"
                        + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200))",
                "CREATE TABLE m3part.t_ctrl (id int4, tag text)",
                "ALTER TABLE m3part.t_ctrl REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"m3part.t_prange"}, compatMode);
        slots(suffix, compatMode);
        WalCapture capture = new WalCapture();
        wal = new WalSource(walSourceConfig(compatMode, whitelist), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), formName + ": DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn,
                        "INSERT INTO m3part.t_prange VALUES (1, 10, 'p1-route')",
                        "INSERT INTO m3part.t_prange VALUES (2, 150, 'p2-route')",
                        "INSERT INTO m3part.t_ctrl VALUES (9, 'ctrl')");
                conn.commit();
            }
            OgDmlParityIT.await(() -> wal.dmlEmittedBuckets() >= 1,
                    formName + ": wal 路未发射事务桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            assertTrue(wal.receiverTerminalFailure().isEmpty(),
                    formName + ": 接收器不得终态失败: "
                            + wal.receiverTerminalFailure().map(Throwable::toString).orElse(""));
            assertEquals(1, wal.dmlEmittedBuckets(), formName + ": 应恰一桶");
            assertEquals(expectedRows, wal.dmlEmittedRows(), formName + ": 实付行数");
        } finally {
            wal.close();
        }
        List<String> rows = capture.blocks().get(0).rows();
        assertEquals(expectedRows, rows.size(), formName + ": 块内行数:\n" + rows);
        assertTrue(rows.stream().anyMatch(r -> r.contains("tag=ctrl")), formName + ": 控制行在场:\n" + rows);
        assertEquals(p2Admitted, rows.stream().anyMatch(r -> r.contains("p2-route")),
                formName + ": p2 路由行" + (p2Admitted ? "应随母表名形态入发射面" : "不得外溢（分区名条目只命中 p1）")
                        + ":\n" + rows);
        assertTrue(rows.stream().noneMatch(r -> r.contains(".p1") || r.contains(".p2")),
                formName + ": 分区名零出现:\n" + rows);
    }

    // ---- 专项三：流内 CREATE·DROP PARTITION 竞速（as-of） ----

    /** 流内 ADD/DROP PARTITION 竞速（PG 档）。 */
    @Test
    void inStreamAddDropPartitionAsOfRace() throws Exception {
        inStreamAddDropPartitionScenario(OgTestEnv.CompatMode.PG);
    }

    /** 流内 ADD/DROP PARTITION 竞速（A 档）。 */
    @Test
    void inStreamAddDropPartitionAsOfRaceACompat() throws Exception {
        inStreamAddDropPartitionScenario(OgTestEnv.CompatMode.A);
    }

    /**
     * 专项三场景体：母表建表仅带 p1（wal 路引导时第四字典只有 p1）→ 流内 ADD PARTITION
     * p2（pg_partition INSERT 记录）→ 单事务双路由行（p1 行 + <b>新建 p2 行</b>——as-of：
     * 第四字典流内演进后新段号即刻可解析）→ DROP PARTITION p1（字典行删除）→ 追加 p2
     * 行（幸存分区照常捕获）。基准路收 4 个 C 帧（两笔 DDL 空块 + 两笔行事务——OG 对
     * 无已发布变更事务仍发 B..C 空块的记档语义），交集对拍 diff 空；wal 侧三行全母表名。
     *
     * @param compatMode 兼容档（取场景库）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private void inStreamAddDropPartitionScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE m3part.t_race (id int4, k int4, v text) PARTITION BY RANGE (k)"
                        + " (PARTITION p1 VALUES LESS THAN (100))",
                "ALTER TABLE m3part.t_race REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"m3part.t_race"}, compatMode);
        slots("race", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        WalCapture capture = new WalCapture();
        wal = new WalSource(walSourceConfig(compatMode, new String[]{"m3part.t_race"}), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "分区 DDL 竞速: DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(true);
                OgDmlParityIT.exec(conn, "ALTER TABLE m3part.t_race ADD PARTITION p2 VALUES LESS THAN (200)");
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn, "INSERT INTO m3part.t_race VALUES (1, 10, 'p1-row'), (2, 150, 'p2-new')");
                conn.commit();
                conn.setAutoCommit(true);
                OgDmlParityIT.exec(conn, "ALTER TABLE m3part.t_race DROP PARTITION p1");
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn, "INSERT INTO m3part.t_race VALUES (3, 180, 'p2-late')");
                conn.commit();
            }
            OgDmlParityIT.await(() -> wal.dmlEmittedBuckets() >= 2,
                    "分区 DDL 竞速: wal 路未发射 2 个桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            assertTrue(wal.receiverTerminalFailure().isEmpty(),
                    "分区 DDL 竞速: 接收器不得终态失败: "
                            + wal.receiverTerminalFailure().map(Throwable::toString).orElse(""));
        } finally {
            wal.close();
        }
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 4, CAPTURE_MS, compatMode);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "分区 DDL 竞速", 0);

        List<String> rows = capture.blocks().stream().flatMap(b -> b.rows().stream()).toList();
        assertEquals(3, rows.size(), "三行（p1-row + p2-new + p2-late）:\n" + rows);
        assertTrue(rows.stream().allMatch(r -> r.startsWith("INSERT m3part.t_race")),
                "三行应全为母表身份（含流内新建 p2 的路由行——as-of）:\n" + rows);
        assertTrue(rows.stream().anyMatch(r -> r.contains("p2-new")) && rows.stream().anyMatch(r -> r.contains("p2-late")),
                "流内 ADD PARTITION 后的新分区行与 DROP 后幸存分区行都应捕获:\n" + rows);
        assertTrue(rows.stream().noneMatch(r -> r.contains(".p1") || r.contains(".p2")),
                "分区名不得出现在渲染面:\n" + rows);
    }

    // ---- 专项四：EXCHANGE 后 TableMeta 失效 ----

    /** EXCHANGE PARTITION 后 TableMeta 缓存失效（PG 档）。 */
    @Test
    void exchangePartitionInvalidatesTableMetaCache() throws Exception {
        exchangePartitionScenario(OgTestEnv.CompatMode.PG);
    }

    /** EXCHANGE PARTITION 后 TableMeta 缓存失效（A 档）。 */
    @Test
    void exchangePartitionInvalidatesTableMetaCacheACompat() throws Exception {
        exchangePartitionScenario(OgTestEnv.CompatMode.A);
    }

    /**
     * 专项四场景体（wal 路专项——EXCHANGE 是段号换绑，基准路不在此面）：段号缓存暖场
     * （EXCHANGE 前 INSERT 进 p1，缓存 R_p1→母表身份）→ {@code EXCHANGE PARTITION (p1)
     * WITH TABLE t_swap}（pg_partition 'p' 行 1×HOT_UPDATE 换 relfilenode + pg_class
     * t_swap 行同步换——T0 锚④）→ 段二双写：INSERT 进母表路由 p1（落<b>原 t_swap 段号
     * </b>，应渲母表）+ INSERT 进 t_swap（落<b>原 p1 段号</b>，应渲 t_swap——失效缺位时
     * 陈旧缓存会把本段渲成母表，本行的正确渲染正是失效调用点的钉）。
     *
     * @param compatMode 兼容档（取场景库）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private void exchangePartitionScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE m3part.t_pex (id int4, k int4, v text) PARTITION BY RANGE (k)"
                        + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200))",
                "CREATE TABLE m3part.t_swap (id int4, k int4, v text)",
                "ALTER TABLE m3part.t_pex REPLICA IDENTITY FULL",
                "ALTER TABLE m3part.t_swap REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"m3part.t_pex"}, compatMode);
        slots("exch", compatMode);
        WalCapture capture = new WalCapture();
        wal = new WalSource(walSourceConfig(compatMode,
                new String[]{"m3part.t_pex", "m3part.t_swap"}), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "EXCHANGE 失效: DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn, "INSERT INTO m3part.t_pex VALUES (1, 10, 'seed')");   // 暖段号缓存
                conn.commit();
                conn.setAutoCommit(true);
                OgDmlParityIT.exec(conn, "ALTER TABLE m3part.t_pex EXCHANGE PARTITION (p1) WITH TABLE m3part.t_swap");
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn,
                        "INSERT INTO m3part.t_pex VALUES (2, 20, 'via-new-seg')",
                        "INSERT INTO m3part.t_swap VALUES (9, 99, 'via-old-seg')");
                conn.commit();
            }
            OgDmlParityIT.await(() -> wal.dmlEmittedBuckets() >= 2,
                    "EXCHANGE 失效: wal 路未发射 2 个桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            assertTrue(wal.receiverTerminalFailure().isEmpty(),
                    "EXCHANGE 失效: 接收器不得终态失败: "
                            + wal.receiverTerminalFailure().map(Throwable::toString).orElse(""));
        } finally {
            wal.close();
        }
        List<String> rows = capture.blocks().stream().flatMap(b -> b.rows().stream()).toList();
        assertEquals(3, rows.size(), "三行（暖场 1 + 段二 2）:\n" + rows);
        assertEquals("INSERT m3part.t_swap BEFORE=- AFTER=[id=9, k=99, v=via-old-seg]",
                rows.get(2), "EXCHANGE 后写进原 p1 段号的行应按 t_swap 身份渲染（段号换绑 + 缓存失效的钉）:\n" + rows);
        assertTrue(rows.stream().filter(r -> r.startsWith("INSERT m3part.t_pex")).count() == 2
                        && rows.stream().anyMatch(r -> r.contains("v=seed"))
                        && rows.stream().anyMatch(r -> r.contains("via-new-seg")),
                "母表两行（暖场 + 换绑后的新段路由行）应全为母表身份:\n" + rows);
        assertTrue(rows.stream().noneMatch(r -> r.contains(".p1") || r.contains(".p2")),
                "分区名不得出现在渲染面:\n" + rows);
    }

    // ---- 专项五：UPDATE 跨分区迁移两行形态 ----

    /** UPDATE 跨分区迁移的两行形态（PG 档）。 */
    @Test
    void crossPartitionUpdateMoveEmitsDeleteInsertPair() throws Exception {
        crossPartitionUpdateScenario(OgTestEnv.CompatMode.PG);
    }

    /** UPDATE 跨分区迁移的两行形态（A 档）。 */
    @Test
    void crossPartitionUpdateMoveEmitsDeleteInsertPairACompat() throws Exception {
        crossPartitionUpdateScenario(OgTestEnv.CompatMode.A);
    }

    /**
     * 专项五场景体：分区键 UPDATE（k: 10→150，p1→p2 迁移）在行级落 <b>DELETE + INSERT
     * 两行</b>（两分区各自的段上）——非单行 UPDATE；单事务先 INSERT 原行再迁移，基准路
     * 对拍 diff 空（帧形态两路同构）；wal 侧字面断言三行全母表名、迁移对的 DELETE 前像
     * = 原行 / INSERT 后像 = 迁移行。
     *
     * @param compatMode 兼容档（取场景库）
     * @throws Exception 连接/启动/等待路径的底层异常
     */
    private void crossPartitionUpdateScenario(OgTestEnv.CompatMode compatMode) throws Exception {
        String[] ddl = {
                "CREATE TABLE m3part.t_cpm (id int4, k int4, v text) PARTITION BY RANGE (k)"
                        + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200))",
                "ALTER TABLE m3part.t_cpm REPLICA IDENTITY FULL",
        };
        resetScenario(ddl, new String[]{"m3part.t_cpm"}, compatMode);
        slots("cpm", compatMode);
        OgParityBaseline.createLogicalSlot(logicalSlot, compatMode);
        WalCapture capture = new WalCapture();
        wal = new WalSource(walSourceConfig(compatMode, new String[]{"m3part.t_cpm"}), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "跨分区 UPDATE: DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(compatMode.database())) {
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn,
                        "INSERT INTO m3part.t_cpm VALUES (1, 10, 'orig')",
                        "UPDATE m3part.t_cpm SET k = 150 WHERE id = 1");
                conn.commit();
            }
            OgDmlParityIT.await(() -> wal.dmlEmittedBuckets() >= 1,
                    "跨分区 UPDATE: wal 路未发射事务桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
        } finally {
            wal.close();
        }
        List<OgBlock> baseline = OgParityBaseline.capture(logicalSlot, PUBLICATION, 1, CAPTURE_MS, compatMode);
        OgParityBaseline.assertParity(baseline, capture.blocks(), "跨分区 UPDATE", 0);

        List<String> rows = capture.blocks().get(0).rows();
        assertEquals(3, rows.size(), "一笔跨分区 UPDATE = 两行（DELETE + INSERT 迁移对）+ 原始 INSERT 共三行:\n" + rows);
        assertEquals("INSERT m3part.t_cpm BEFORE=- AFTER=[id=1, k=10, v=orig]", rows.get(0),
                "原始 INSERT 行（p1 路由）");
        assertEquals("DELETE m3part.t_cpm BEFORE=[id=1, k=10, v=orig] AFTER=-", rows.get(1),
                "迁移对应含 DELETE（旧分区段，前像 = 原行）:\n" + rows);
        assertEquals("INSERT m3part.t_cpm BEFORE=- AFTER=[id=1, k=150, v=orig]", rows.get(2),
                "迁移对应含 INSERT（新分区段，后像 = 迁移行）:\n" + rows);
        assertTrue(rows.stream().noneMatch(r -> r.contains(".p1") || r.contains(".p2")),
                "分区名不得出现在渲染面:\n" + rows);
    }

    // ---- 场景骨架 ----

    /**
     * 场景重置：目标库内 schema CASCADE 清场 + 场景建表 + publication 重建（<b>显式
     * FOR TABLE</b>——前提①，FOR ALL TABLES 不含分区表；多表逗号清单）。DDL 先于建槽/
     * 引导（spike 坑 2 时序契约）。
     *
     * @param tableDdl       场景建表语句
     * @param publishedTables 进 publication 的表全名清单（空清单 = 不建 publication——
     *                       纯 wal 路场景免建）
     * @param mode           兼容档（取目标库）
     * @throws SQLException DDL 失败
     */
    private static void resetScenario(String[] tableDdl, String[] publishedTables,
            OgTestEnv.CompatMode mode) throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection(mode.database()); Statement st = c.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            st.execute("CREATE SCHEMA " + SCHEMA);
            for (String ddl : tableDdl) {
                st.execute(ddl);
            }
            if (publishedTables.length > 0) {
                st.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION);
                st.execute("CREATE PUBLICATION " + PUBLICATION + " FOR TABLE "
                        + String.join(", ", publishedTables));
            }
        }
    }

    /**
     * 设定当前用例的兼容档与两路槽名并前置清理残留（A 档槽名附 {@code _a} 后缀——
     * 物理槽是集群级对象，与 PG 档同名互踩）。
     *
     * @param suffix 槽名后缀（前缀 m3part_logic_/m3part_wal_ 固定）
     * @param mode   兼容档
     */
    private void slots(String suffix, OgTestEnv.CompatMode mode) {
        this.mode = mode;
        String tagged = suffix + (mode == OgTestEnv.CompatMode.A ? "_a" : "");
        logicalSlot = "m3part_logic_" + tagged;
        physicalSlot = "m3part_wal_" + tagged;
        OgParityBaseline.dropLogicalSlotQuietly(logicalSlot, mode);
        OgTestEnv.dropPhysicalSlotQuietly(physicalSlot);
    }

    /**
     * wal 直解路门面配置：direct 通道 + opengauss-jdbc 前缀 + 库名按档 + 物理槽取
     * {@link #slots} 当前设定 + DML 白名单（state 缺省禁用——本 IT 全部单 pass，无续传面）。
     *
     * @param mode      兼容档（取库名）
     * @param whitelist DML 白名单表全名集
     * @return WalSource 构造入参配置
     */
    private Properties walSourceConfig(OgTestEnv.CompatMode mode, String[] whitelist) {
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, OgTestEnv.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(OgTestEnv.port()));
        cfg.setProperty(WalSource.KEY_DB, mode.database());
        cfg.setProperty(WalSource.KEY_USER, OgTestEnv.USER);
        cfg.setProperty(WalSource.KEY_PASS, OgTestEnv.PASSWORD);
        cfg.setProperty(WalSource.KEY_SLOT, physicalSlot);
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        cfg.setProperty(WalSource.KEY_TABLES, String.join(",", whitelist));
        return cfg;
    }

    /**
     * 兜底清理：关 WalSource（幂等）→ 删两路槽（异常路径保险；删逻辑槽按当前用例的
     * 兼容档取库）。
     */
    @AfterEach
    void cleanupSlots() {
        if (wal != null) {
            wal.close();
        }
        if (logicalSlot != null) {
            OgParityBaseline.dropLogicalSlotQuietly(logicalSlot, mode);
        }
        if (physicalSlot != null) {
            OgTestEnv.dropPhysicalSlotQuietly(physicalSlot);
        }
    }
}
