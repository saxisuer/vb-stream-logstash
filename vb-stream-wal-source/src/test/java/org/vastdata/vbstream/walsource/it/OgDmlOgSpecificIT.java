package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.OgBlock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M2 的 OG 特有面 IT（T7 Step 2，按场景改编裁定 2/3——上游实测已证两面前提
 * 不可达，记档降级为轻量护栏面）：
 *
 * <ul>
 *   <li><b>attcmprmode 列压缩（裁定 2，T4 结论）</b>：OG 5.0.0 行存（ASTORE）面列压缩
 *       DDL 天然不可达——三源证实（relcmprs≤1 / attcmprmode∈{0,0x7F}）。IT 面只钉
 *       护栏：压缩 DDL（{@code CREATE TABLE ... COMPRESS}）被服务端拒绝（"row-oriented
 *       table does not support compression"，og-plan 实测锚）；普通表 attcmprmode 只取
 *       {0, 127=CMPR_UNDEF}（无真压缩 mode——T4 值域锚）且 DML 对拍不受扰。四算法纯函数
 *       的黄金向量归 T4 离线交付（OgColumnCompression*）。</li>
 *   <li><b>分区 DML 捕获（M3 T2 落地——T0 锚② 语义重排）</b>：分区母表行入字典
 *       （T7 修复 B：存储护栏 parttype='p' 放行）；分区 DML 行落 pg_partition
 *       relfilenode，经第四字典身份链归一到<b>母表身份</b>捕获（TableMeta =
 *       schema.母表名 + 母表 oid + 母表列集——基准路 pgoutput R/I 帧同形，锚②
 *       {@code PARTITION_NAME=mother}）。母表白名单两形态（内/外）：在内 = 分区行
 *       以母表名发射（4 行 + 控制行 1 行同桶）；在外 = 分区行照常 miss、控制行照常
 *       发射。分区名（p1/pa）全程零出现（渲染面归一锚）。</li>
 * </ul>
 *
 * <p>复用 {@link OgDmlParityIT} 的对拍骨架与场景重置面（包内共享）。需本机 Docker。</p>
 */
class OgDmlOgSpecificIT {

    /** 对拍骨架宿主（包内共享的 runParity/resetScenario 等面——非测试运行实例）。 */
    private final OgDmlParityIT harness = new OgDmlParityIT();

    /** 当前用例的 wal 直解路（分区场景自管；AfterEach 兜底关闭）。 */
    private WalSource wal;

    /**
     * attcmprmode 护栏：压缩 DDL 拒绝 + 普通表 attcmprmode 全 0 + DML 对拍不受扰。
     *
     * <p>关键步骤：①场景 DDL 后先尝试 {@code CREATE TABLE ... COMPRESS}——断言
     * SQLException 且消息含行存不支持压缩判别串（表未建成）；②查本场景表的
     * pg_attribute.attcmprmode 全 0（ASTORE 常态——列压缩面 DDL 不可达的库侧钉）；
     *③单事务 I/U/D 对拍 diff 空（流不受压缩探测扰动）。</p>
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void compressedColumnDdlRejectedForRowStoreAndDmlParityUnaffected() throws Exception {
        String[] ddl = {
                "CREATE TABLE parity.t_parity (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_parity REPLICA IDENTITY FULL",
        };
        harness.runParity("压缩列护栏", "ogc1", ddl, new String[]{"parity.t_parity"}, 1, 1, 0, conn -> {
            SQLException e = assertThrows(SQLException.class, () -> OgDmlParityIT.exec(conn,
                    "CREATE TABLE parity.t_compc (id int, v text) COMPRESS"),
                    "ASTORE 行存的压缩 DDL 应被服务端拒绝（T4：列压缩面 DDL 天然不可达）");
            assertTrue(e.getMessage() != null && e.getMessage().contains("does not support compression"),
                    "拒绝消息须含行存不支持压缩判别串: " + e.getMessage());
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT count(*) FROM pg_attribute"
                                 + " WHERE attrelid = 'parity.t_parity'::regclass AND attnum > 0"
                                 + " AND attcmprmode NOT IN (0, 127)")) {
                rs.next();
                assertEquals(0, rs.getInt(1),
                        "普通表 attcmprmode 应只取 {0, 127=CMPR_UNDEF}（ASTORE 无压缩态——"
                                + "真压缩 mode 1..4 在 5.0.0 行存 DDL 面不可达，T4 三源锚）");
            }
            conn.setAutoCommit(false);
            OgDmlParityIT.exec(conn, "INSERT INTO parity.t_parity VALUES (1, 'cmp-guard')");
            OgDmlParityIT.exec(conn, "UPDATE parity.t_parity SET name = 'cmp-guard-upd' WHERE id = 1");
            conn.commit();
        });
    }

    /**
     * 分区 DML 捕获（M3 T2，白名单形态 A——母表在内）：range+list 各一 + 控制普通表，
     * 单事务混写——断言 wal 路无终态失败、恰一个桶恰五行实付（4 分区行 + 1 控制行），
     * 分区行以<b>母表名</b>（parity.t_prange/parity.t_plist）发射、分区名
     * （p1/p2/pa/pb）零出现（锚② {@code PARTITION_NAME=mother} 渲染面归一）。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void partitionDmlCapturedUnderMotherIdentityWithMotherInWhitelist() throws Exception {
        runPartitionScenario("分区捕获（母表在内）", "ogp1",
                new String[]{"parity.t_prange", "parity.t_plist", "parity.t_ctrl"}, 5);
    }

    /**
     * 分区 DML 捕获的白名单形态 B（母表在外——白名单只钉控制表）：分区行按母表身份
     * 双匹配（母表名/分区名都不中）照常 miss——恰一行实付（控制行）、分区表名零泄漏
     * （控制普通表行不受分区面扰动）。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void partitionDmlOutOfWhitelistStaysExcludedWithControlRowIntact() throws Exception {
        runPartitionScenario("分区捕获（母表在外）", "ogp2",
                new String[]{"parity.t_ctrl"}, 1);
    }

    /**
     * 分区场景共享体：重置（控制表 + range/list 分区表——母表行经 T7 修复 B 入字典，
     * 修复前引导期即砖）→ 起 wal 路（direct + 白名单按形态）→ 单事务混写（分区 4 行 +
     * 控制 1 行）→ await 桶 1 → 断言实付行数按白名单形态（5=分区行捕获 / 1=排除）、
     * 无终态失败、分区名零出现。
     *
     * @param scenario     场景名
     * @param suffix       槽名后缀
     * @param whitelist    DML 白名单（形态 A 含母表 / 形态 B 只含控制表）
     * @param expectedRows 期望实付行数（形态 A=5 含分区行 / 形态 B=1 仅控制行）
     * @throws Exception 连接/启动路径的底层异常
     */
    private void runPartitionScenario(String scenario, String suffix, String[] whitelist, int expectedRows)
            throws Exception {
        OgDmlParityIT.resetScenario(new String[]{
                "CREATE TABLE parity.t_ctrl (id int4 NOT NULL, name text, PRIMARY KEY (id))",
                "ALTER TABLE parity.t_ctrl REPLICA IDENTITY FULL",
        });
        try (Connection c = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE parity.t_prange (id int, k int, v text) PARTITION BY RANGE (k)"
                    + " (PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN (200))");
            st.execute("CREATE TABLE parity.t_plist (id int, k int, v text) PARTITION BY LIST (k)"
                    + " (PARTITION pa VALUES (1), PARTITION pb VALUES (2))");
        }
        harness.slots(suffix);
        OgParityBaseline.WalCapture capture = new OgParityBaseline.WalCapture();
        wal = new WalSource(OgDmlParityIT.walSourceConfig(harness.physicalSlotName(), whitelist, null), capture);
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), scenario + ": DML 面应装配");
            try (Connection conn = OgTestEnv.newSqlConnection(OgTestEnv.DATABASE_M2PG)) {
                conn.setAutoCommit(false);
                OgDmlParityIT.exec(conn,
                        "INSERT INTO parity.t_prange VALUES (1, 10, 'range-a'), (2, 150, 'range-b')",
                        "INSERT INTO parity.t_plist VALUES (3, 1, 'list-a'), (4, 2, 'list-b')",
                        "INSERT INTO parity.t_ctrl VALUES (9, 'ctrl-row')");
                conn.commit();
            }
            OgDmlParityIT.await(() -> wal.dmlEmittedBuckets() >= 1,
                    scenario + ": wal 路未发射事务桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            assertTrue(wal.receiverTerminalFailure().isEmpty(),
                    scenario + ": 分区 DML 不砖——接收器不得终态失败: "
                            + wal.receiverTerminalFailure().map(Throwable::toString).orElse(""));
            assertEquals(1, wal.dmlEmittedBuckets(), scenario + ": 应恰一个桶（单事务混写）");
            assertEquals(expectedRows, wal.dmlEmittedRows(),
                    scenario + ": 实付行数应按白名单形态（母表在内=分区行捕获 5 行 / 在外=仅控制行 1 行）");
        } finally {
            wal.close();
            harness.cleanupSlots();
            wal = null;
        }
        // 捕获块断言在 close 之后（WalCapture 的 happens-before 经接收线程 join 建立）
        List<OgBlock> blocks = capture.blocks();
        assertEquals(1, blocks.size(), scenario + ": 捕获块数");
        assertEquals(expectedRows, blocks.get(0).rows().size(), scenario + ": 块内行数");
        assertTrue(blocks.get(0).rows().stream().anyMatch(r -> r.contains("parity.t_ctrl")),
                scenario + ": 控制表行应在发射面:\n" + blocks.get(0).rows());
        if (expectedRows > 1) {
            assertTrue(blocks.get(0).rows().stream().filter(r -> r.contains("parity.t_prange")).count() == 2
                            && blocks.get(0).rows().stream().filter(r -> r.contains("parity.t_plist")).count() == 2,
                    scenario + ": 分区行应以母表名各两行发射（锚② PARTITION_NAME=mother）:\n"
                            + blocks.get(0).rows());
        }
        assertTrue(blocks.get(0).rows().stream().noneMatch(r ->
                        r.contains(".p1") || r.contains(".p2") || r.contains(".pa") || r.contains(".pb")),
                scenario + ": 分区名不得出现在渲染面（归一到母表名）:\n" + blocks.get(0).rows());
    }

    /** 兜底清理：分区场景异常路径的 wal 关闭与两路槽删除（正常路径场景内已完成）。 */
    @AfterEach
    void cleanup() {
        if (wal != null) {
            wal.close();
        }
        harness.cleanupSlots();
    }
}
