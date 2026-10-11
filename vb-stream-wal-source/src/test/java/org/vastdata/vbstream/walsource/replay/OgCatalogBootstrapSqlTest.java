package org.vastdata.vbstream.walsource.replay;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayoutV17;
import org.vastdata.vbstream.walsource.layout.WalLayoutVOpenGauss;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 档引导 SQL 的离线断言（Task 7 失败测试 ①）——SQL 文本面：pg_class
 * 种子的 41 列显式投影（oid 系统列 + 40 数据列，live pg_attribute attnum&gt;0 口径
 * ——og-plan 实测 40 数据列，任务书"41 还是 40"的疑间以 live 为准：投影 = 41 列
 * 含 oid）与位点函数 {@code pg_current_xlog_location()}（9.2 系命名、无 flush 档，
 * og-plan 实测可用）；V17/V18 路径 SQL 保持原样（零回归锚）。
 *
 * <p>另含投影位次 ↔ layout 词典槽位的交叉锚：OG 列单按 attnum 升序与
 * {@code pgClassKinds()} 词典同序，SQL 投影位置（ctid=1、oid=2、数据列=3+槽位）
 * 恰等于 3+layout 访问器值——两套独立转录互证，错位即红。</p>
 */
class OgCatalogBootstrapSqlTest {

    private final WalLayout og = WalLayoutVOpenGauss.INSTANCE;

    /**
     * 用例 ①a：OG 档 pg_class 种子 SQL 是 41 列（oid + 40 数据列）显式投影——含
     * OG 独有的 reldeltarelid/relcudescrelid/parttype/relcmprs/relbucket 等列名
     * （存储引擎护栏的种子期判别面），relkind 带 ::text 转换（与 vanilla 种子同口径）。
     */
    @Test
    void ogClassSeedSqlProjectsAllFortyOneColumnsExplicitly() {
        String sql = CatalogBootstrap.classSeedSql(og);
        List<String> exprs = projection(sql);
        assertEquals(42, exprs.size(), "ctid::text + oid + 40 数据列 = 42 表达式");
        assertEquals("ctid::text", exprs.get(0));
        assertEquals("oid", exprs.get(1));
        for (String col : new String[]{
                "relname", "relnamespace", "reltype", "reloftype", "relowner", "relam",
                "relfilenode", "reltablespace", "relpages", "reltuples", "relallvisible",
                "reltoastrelid", "reltoastidxid", "reldeltarelid", "reldeltaidx",
                "relcudescrelid", "relcudescidx", "relhasindex", "relisshared", "relpersistence",
                "relkind::text", "relnatts", "relchecks", "relhasoids", "relhaspkey",
                "relhasrules", "relhastriggers", "relhassubclass", "relcmprs", "relhasclusterkey",
                "relrowmovement", "parttype", "relfrozenxid", "relacl", "reloptions",
                "relreplident", "relfrozenxid64", "relbucket", "relbucketkey", "relminmxid"}) {
            assertTrue(exprs.contains(col), "OG 种子投影缺列: " + col);
        }
        assertTrue(sql.endsWith("FROM pg_class"));
    }

    /**
     * 用例 ①b：投影位次 ↔ layout 词典槽位交叉锚——数据列的 SQL 位置恰为
     * 3 + 词典物理槽位（ctid=1、oid=2 占前两位；relkind 带 ::text 转换剥后对位）。
     * 错位会让 OG 种子行的 ClassRow 构造取错列，本锚离线即红。
     */
    @Test
    void ogClassSeedProjectionPositionsMirrorLayoutDictSlots() {
        List<String> exprs = projection(CatalogBootstrap.classSeedSql(og));
        // 0 位 ctid::text、1 位 oid，数据列 = 2 + 词典物理槽位（SQL 结果集 1 起位 = 3 + 槽位）
        assertEquals("relname", exprs.get(2 + 0), "词典槽 0 = relname（oid 在元组头非数据列）");
        assertEquals("relfilenode", exprs.get(2 + 6), "词典槽 6 = relfilenode（V17/V18 为 7）");
        assertEquals("reltoastrelid", exprs.get(2 + og.classToastRelidIndex()));
        assertEquals("relkind::text", exprs.get(2 + og.classRelkindIndex()));
        assertEquals("reldeltarelid", exprs.get(2 + og.classReldeltarelidIndex()));
        assertEquals("relcudescrelid", exprs.get(2 + og.classRelcudescrelidIndex()));
        assertEquals("parttype", exprs.get(2 + og.classParttypeIndex()));
        assertEquals("reloptions", exprs.get(2 + og.classReloptionsIndex()));
        assertEquals("relbucket", exprs.get(2 + og.classRelbucketIndex()));
    }

    /**
     * 用例 ①c：位点函数版本感知——OG 用 {@code pg_current_xlog_location()}（9.2 系
     * 命名，无 flush 档，og-plan 实测可用），V17/V18 保持
     * {@code pg_current_wal_flush_lsn()}（零回归锚）。
     */
    @Test
    void flushLsnSqlIsVersionDispatched() {
        assertEquals("SELECT pg_current_xlog_location()", CatalogBootstrap.flushLsnSql(og));
        assertEquals("SELECT pg_current_wal_flush_lsn()",
                CatalogBootstrap.flushLsnSql(WalLayoutV17.INSTANCE));
    }

    /**
     * 用例 ①d（零回归锚）：V17 档的 pg_class 种子 SQL 与既有 vanilla 投影逐字一致
     * ——OG 适配不得波及 vanilla 引导路径。
     */
    @Test
    void vanillaClassSeedSqlIsUnchanged() {
        String sql = CatalogBootstrap.classSeedSql(WalLayoutV17.INSTANCE);
        assertEquals("SELECT ctid::text, oid, relname, relnamespace, reltype, reloftype,"
                + " relowner, relam, relfilenode, reltoastrelid, relkind::text FROM pg_class", sql);
        assertFalse(sql.contains("relcudescrelid"), "vanilla 投影不得携带 OG 列");
    }

    /**
     * 剥出 SELECT 投影表达式清单（剥 "SELECT " 前缀与 " FROM ..." 后缀，逗号切分）。
     *
     * @param sql 种子查询 SQL
     * @return 投影表达式列表（保序）
     */
    private static List<String> projection(String sql) {
        String body = sql.substring("SELECT ".length(), sql.indexOf(" FROM "));
        return Arrays.stream(body.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
