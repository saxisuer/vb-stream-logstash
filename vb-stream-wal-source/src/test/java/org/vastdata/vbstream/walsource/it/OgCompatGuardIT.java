package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.it.OgParityBaseline.WalCapture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss M2 T1 库级兼容模式护栏的 IT 面（T7 Step 3）——非 PG 档库（m2a='A' Oracle
 * 风格 / m2b='B' MySQL 风格，{@code OgTestEnv.ensureM2Databases} 静态幂等建）上以
 * Main 形态（listener 在场 + {@code vb.wal.dml} 缺省 true）启动 {@link WalSource}，
 * 启动期 ISE <b>fail-fast</b>：异常消息含档位值（{@code datcompatibility=A/B}）与
 * 里程碑指引（A 档=M3 / B 档=M4），且护栏先于槽副作用——物理槽不被创建（T1 的
 * "ISE 先于槽副作用" 离线锚在真容器的复验面）。
 *
 * <p>离线档位判定面归 {@code OgCompatModeGuardTest}（伪 JDBC 脚本化）；本 IT 只钉
 * 真容器两档的启动期行为。需本机 Docker。</p>
 */
class OgCompatGuardIT {

    /**
     * m2a（'A' 档）fail-fast：DML 面启动期 ISE，消息含 {@code datcompatibility=A} 与
     * M3 指引；物理槽未创建。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void m2aOracleCompatDatabaseFailsFastDmlFaceWithM3Guidance() throws Exception {
        assertNonPgCompatDatabaseFailsFast(OgTestEnv.DATABASE_M2A, "A", "M3");
    }

    /**
     * m2b（'B' 档）fail-fast：DML 面启动期 ISE，消息含 {@code datcompatibility=B} 与
     * M4 指引；物理槽未创建。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void m2bMysqlCompatDatabaseFailsFastDmlFaceWithM4Guidance() throws Exception {
        assertNonPgCompatDatabaseFailsFast(OgTestEnv.DATABASE_M2B, "B", "M4");
    }

    /**
     * 护栏断言共享体：Main 形态配置（direct 通道 + opengauss-jdbc 前缀 + 白名单钉表）
     * 指向目标库启动——期望启动期 ISE（消息三断言：档位值/里程碑指引/PG 档建库指引），
     * 且槽副作用缺席（pg_replication_slots 无本槽行）；WalSource 兜底 close（释放已建
     * SQL 会话）。
     *
     * @param database     目标库名（m2a/m2b）
     * @param compatLetter 期望档位字母（A/B）
     * @param milestone    期望里程碑指引（M3/M4）
     * @throws Exception 连接/查询路径的底层异常
     */
    private static void assertNonPgCompatDatabaseFailsFast(String database, String compatLetter, String milestone)
            throws Exception {
        String slot = "m2par_guard_" + compatLetter.toLowerCase();
        OgTestEnv.dropPhysicalSlotQuietly(slot);
        Properties cfg = new Properties();
        cfg.setProperty(WalSource.KEY_HOST, OgTestEnv.host());
        cfg.setProperty(WalSource.KEY_PORT, String.valueOf(OgTestEnv.port()));
        cfg.setProperty(WalSource.KEY_DB, database);
        cfg.setProperty(WalSource.KEY_USER, OgTestEnv.USER);
        cfg.setProperty(WalSource.KEY_PASS, OgTestEnv.PASSWORD);
        cfg.setProperty(WalSource.KEY_SLOT, slot);
        cfg.setProperty(WalSource.KEY_OG_SOURCE, "direct");
        cfg.setProperty(WalSource.KEY_OG_JDBC_PREFIX, "jdbc:opengauss:");
        cfg.setProperty(WalSource.KEY_TABLES, "public.t_guard");
        WalSource wal = new WalSource(cfg, new WalCapture());
        try {
            IllegalStateException ise = assertThrows(IllegalStateException.class, wal::start,
                    "非 PG 档库上 DML 面启动应 fail-fast（" + database + "）");
            String msg = ise.getMessage();
            assertTrue(msg != null && msg.contains("datcompatibility=" + compatLetter),
                    "ISE 消息须含档位值 datcompatibility=" + compatLetter + ": " + msg);
            assertTrue(msg != null && msg.contains(milestone),
                    "ISE 消息须含里程碑指引 " + milestone + ": " + msg);
            assertTrue(msg != null && msg.contains("DBCOMPATIBILITY 'PG'"),
                    "ISE 消息须含 PG 档建库指引: " + msg);
        } finally {
            wal.close();
        }
        assertEquals(0, slotRowCount(slot), "护栏须先于槽副作用——物理槽 " + slot + " 不应被创建");
    }

    /**
     * 查物理槽存在行数（postgres 库的 pg_replication_slots）。
     *
     * @param slot 槽名
     * @return 匹配行数（-1 = 查询失败，交由调用方断言面暴露）
     */
    private static int slotRowCount(String slot) throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slot);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
