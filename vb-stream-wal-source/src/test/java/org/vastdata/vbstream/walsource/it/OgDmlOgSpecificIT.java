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
 *   <li><b>分区 DML 捕获（裁定 3，T5/R1 结论）</b>：M2 语义 = <b>不砖 + fail-safe
 *       有痕跳过</b>——分区母表行入字典（T7 修复 B：存储护栏 parttype='p' 放行，本 IT
 *       是其端到端锚——修复前引导期即 ISE）；分区 DML 行落 pg_partition relfilenode
 *       的三表字典 miss 跳过（发射计数不含分区行）。<b>完整捕获不断言</b>（M3 的
 *       pg_partition 第四字典）。母表白名单两形态（内/外）：行为可断言面 = 两形态下
 *       分区行同样缺席（路由落子表 relfilenode、字典 miss 先于白名单闸——T5 实测），
 *       控制普通表行两形态照常发射。</li>
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
     * 分区 DML fail-safe（不砖 + 发射计数不含分区行）：range+list 各一 + 控制普通表，
     * 单事务混写——断言 wal 路无终态失败、恰一个桶恰一行（仅控制行；分区行字典
     * miss 跳过）、流持续可用。白名单形态 A（母表在内）。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void partitionDmlFailsSafeWithMotherInWhitelist() throws Exception {
        runPartitionScenario("分区 fail-safe（母表在内）", "ogp1",
                new String[]{"parity.t_prange", "parity.t_plist", "parity.t_ctrl"});
    }

    /**
     * 分区 DML fail-safe 的白名单形态 B（母表在外——白名单只钉控制表）：行为面与
     * 形态 A 等价（分区 relfilenode 的字典 miss 先于白名单闸，T5 路由实测锚——母表
     * 不承载行、白名单内外对分区行同为 miss；完整分区白名单归 M3 第四字典）。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void partitionDmlFailsSafeWithMotherOutOfWhitelist() throws Exception {
        runPartitionScenario("分区 fail-safe（母表在外）", "ogp2",
                new String[]{"parity.t_ctrl"});
    }

    /**
     * 分区场景共享体：重置（控制表 + range/list 分区表——母表行经 T7 修复 B 入字典，
     * 修复前引导期即砖）→ 起 wal 路（direct + 白名单按形态）→ 单事务混写（分区 4 行 +
     * 控制 1 行）→ await 桶 1 → 断言恰一行（控制行）、分区行零泄漏、无终态失败。
     *
     * @param scenario  场景名
     * @param suffix    槽名后缀
     * @param whitelist DML 白名单（形态 A 含母表 / 形态 B 只含控制表）
     * @throws Exception 连接/启动路径的底层异常
     */
    private void runPartitionScenario(String scenario, String suffix, String[] whitelist) throws Exception {
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
                    scenario + ": wal 路未发射控制行桶（buckets=" + wal.dmlEmittedBuckets()
                            + ", terminal=" + wal.receiverTerminalFailure() + "）");
            assertTrue(wal.receiverTerminalFailure().isEmpty(),
                    scenario + ": 分区 DML 不砖——接收器不得终态失败: "
                            + wal.receiverTerminalFailure().map(Throwable::toString).orElse(""));
            assertEquals(1, wal.dmlEmittedBuckets(), scenario + ": 应恰一个桶（控制行事务）");
            assertEquals(1, wal.dmlEmittedRows(),
                    scenario + ": 应恰一行实付——分区行属 pg_partition relfilenode 字典 miss（fail-safe 跳过，M3 第四字典前不捕获）");
        } finally {
            wal.close();
            harness.cleanupSlots();
            wal = null;
        }
        // 捕获块断言在 close 之后（WalCapture 的 happens-before 经接收线程 join 建立）
        List<OgBlock> blocks = capture.blocks();
        assertEquals(1, blocks.size(), scenario + ": 捕获块数");
        assertEquals(1, blocks.get(0).rows().size(), scenario + ": 块内行数");
        assertTrue(blocks.get(0).rows().get(0).contains("parity.t_ctrl"),
                scenario + ": 唯一行应是控制表行:\n" + blocks.get(0).rows().get(0));
        assertTrue(!blocks.get(0).rows().get(0).contains("t_prange")
                && !blocks.get(0).rows().get(0).contains("t_plist"),
                scenario + ": 分区行不得泄漏进发射面");
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
