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
 * openGauss 库级兼容模式护栏的 IT 面（M2 T7 Step 3 落地，M3 Task 3 白名单放宽连动）
 * ——m2a='A'（Oracle 风格）自 M3 起<strong>放行</strong>（白名单 {'PG','A'}）、m2b='B'
 * （MySQL 风格，{@code OgTestEnv.ensureM2Databases} 静态幂等建）维持 ISE：以 Main
 * 形态（listener 在场 + {@code vb.wal.dml} 缺省 true）启动 {@link WalSource}——'A'
 * 档 start 正常装配 DML 面（{@code dmlEnabled()} 保持 true）；'B' 档启动期 ISE
 * <b>fail-fast</b>：异常消息含档位值（{@code datcompatibility=B}）与里程碑指引
 * （M4），且护栏先于槽副作用——物理槽不被创建（离线 "ISE 先于槽副作用" 锚在真容器
 * 的复验面）。
 *
 * <p>离线档位判定面归 {@code OgCompatModeGuardTest}（伪 JDBC 脚本化）；本 IT 只钉
 * 真容器两档的启动期行为。需本机 Docker。</p>
 */
class OgCompatGuardIT {

    /**
     * m2a（'A' 档）放行（M3 Task 3 连动——M2 时此用例断言 ISE 附 M3 指引）：Main 形态
     * DML 面正常装配——start 正常返回、{@code dmlEnabled()} 保持 true；槽副作用随
     * 装配发生属预期（兜底 close 后静默清槽）。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void m2aOracleCompatDatabaseAssemblesDmlFaceSinceM3() throws Exception {
        String slot = "m3par_guard_a";
        OgTestEnv.dropPhysicalSlotQuietly(slot);
        Properties cfg = guardShapedConfig(OgTestEnv.DATABASE_M2A, slot);
        WalSource wal = new WalSource(cfg, new WalCapture());
        try {
            wal.start();
            assertTrue(wal.dmlEnabled(), "M3 起 'A' 档库 DML 面放行——dmlEnabled 须保持 true");
        } finally {
            wal.close();
        }
        OgTestEnv.dropPhysicalSlotQuietly(slot);
    }

    /**
     * m2b（'B' 档）fail-fast：DML 面启动期 ISE，消息含 {@code datcompatibility=B} 与
     * M4 指引；物理槽未创建。
     *
     * @throws Exception 连接/启动路径的底层异常
     */
    @Test
    void m2bMysqlCompatDatabaseFailsFastDmlFaceWithM4Guidance() throws Exception {
        assertUnsupportedCompatDatabaseFailsFast(OgTestEnv.DATABASE_M2B, "B");
    }

    /**
     * Main 形态共享配置体（direct 通道 + opengauss-jdbc 前缀 + 白名单钉表）指向目标
     * 库——m2a 放行用例与 m2b ISE 用例同一形态，仅库名/槽名相异。
     *
     * @param database 目标库名（m2a/m2b）
     * @param slot     物理槽名
     * @return 可直接交 {@link WalSource} 双参构造的配置
     */
    private static Properties guardShapedConfig(String database, String slot) {
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
        return cfg;
    }

    /**
     * 护栏 ISE 断言共享体（m2b）：Main 形态配置指向白名单外档位库启动——期望启动期
     * ISE（消息三断言：档位值/里程碑指引/PG 档建库指引），且槽副作用缺席
     * （pg_replication_slots 无本槽行）；WalSource 兜底 close（释放已建 SQL 会话）。
     *
     * @param database     目标库名（m2b）
     * @param compatLetter 期望档位字母（B）
     * @throws Exception 连接/查询路径的底层异常
     */
    private static void assertUnsupportedCompatDatabaseFailsFast(String database, String compatLetter)
            throws Exception {
        String slot = "m2par_guard_" + compatLetter.toLowerCase();
        OgTestEnv.dropPhysicalSlotQuietly(slot);
        WalSource wal = new WalSource(guardShapedConfig(database, slot), new WalCapture());
        try {
            IllegalStateException ise = assertThrows(IllegalStateException.class, wal::start,
                    "白名单外档位库上 DML 面启动应 fail-fast（" + database + "）");
            String msg = ise.getMessage();
            assertTrue(msg != null && msg.contains("datcompatibility=" + compatLetter),
                    "ISE 消息须含档位值 datcompatibility=" + compatLetter + ": " + msg);
            assertTrue(msg != null && msg.contains("M4"),
                    "ISE 消息须含里程碑指引 M4: " + msg);
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
