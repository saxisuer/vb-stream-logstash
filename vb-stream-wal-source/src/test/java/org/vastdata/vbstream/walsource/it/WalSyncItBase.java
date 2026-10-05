package org.vastdata.vbstream.walsource.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.replay.CatalogReplay;
import org.vastdata.vbstream.walsource.replay.CatalogRow;
import org.vastdata.vbstream.walsource.replay.CatalogSynchronizer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * catalog 同步 IT 的公共基类（Task 13，spec §8/§10）——同步器起停辅助、DDL 场景执行
 * 工具与<strong>对拍器</strong>：停流后把 {@link CatalogSynchronizer#stores()} 的
 * ctid 键控行字典与 JDBC REPEATABLE READ 单事务实查逐行全等断言（ctid 键、AttrRow
 * 五字段 / ClassRow 九字段、列序），失败时打印双方差异行（可诊断性，spec §10）。
 *
 * <p>对拍范围（调用方传 relOid 集合）：基表 oid 的 pg_attribute 全行（attnum&gt;0，
 * 含 dropped 占位）+ pg_class 行；两侧行模型各自发现的 reltoastrelid 非 oid 一并并入
 * 对拍集（toast 关系的 attr/class 行同样全等）——toast 面覆盖"重建 toast"场景。
 * 另做三面强校验：{@code relfilenodeOf/toastOf} 对 JDBC 末态、tracked 双 ctid 对
 * 该表/toast 行的 JDBC 现居 ctid、{@code columnsOf} 列序严格升序。环境固定用
 * {@link Interference} 干扰容器（autovacuum 拉满 + wal_compression=off）。</p>
 */
abstract class WalSyncItBase {

    private static final Logger LOG = LoggerFactory.getLogger(WalSyncItBase.class);

    /** 对拍等待接收前沿的轮询节拍（毫秒）。 */
    private static final long POLL_INTERVAL_MS = 100;

    /** 对拍等待接收前沿的超时（毫秒）。 */
    private static final long POLL_TIMEOUT_MS = 30_000;

    /** 本基类布局（干扰容器单例就绪后解析，PG 18 → V18 描述符）——子类起同步器共用。 */
    protected final WalLayout layout = WalLayouts.forServerVersion(serverVersion());

    /**
     * 干扰容器服务端版本号（布局分发输入）。
     *
     * @return 十进制版本号（如 180000）
     */
    protected static int serverVersion() {
        try {
            return Interference.serverVersionNum();
        } catch (SQLException e) {
            throw new IllegalStateException("读取干扰容器 server_version_num 失败", e);
        }
    }

    /**
     * 顺序执行 DDL/SQL 语句（各自独立自动提交，任一失败即抛）。
     *
     * @param c          会话连接
     * @param statements 语句列表
     * @throws SQLException 任一语句失败（已执行的不回滚）
     */
    protected static void execDdl(Connection c, String... statements) throws SQLException {
        try (Statement st = c.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
                LOG.debug("DDL 执行: {}", sql);
            }
        }
    }

    /**
     * 单值 long 查询便捷档（恰返一行一列）。
     *
     * @param c   会话连接
     * @param sql 查询
     * @return 首行首列值
     * @throws SQLException 查询失败
     */
    protected static long queryLong(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * 表名解析 oid（regclass 转换——表不存在时异常上抛，调用方保证已建）。
     *
     * @param c         会话连接
     * @param tableName 表名（须可被 current_schema 解析）
     * @return 关系 oid
     * @throws SQLException 查询失败
     */
    protected static long oidOf(Connection c, String tableName) throws SQLException {
        return queryLong(c, "SELECT '" + tableName + "'::regclass::oid");
    }

    /**
     * 当前 flush 位点（对拍目标前沿的取值面）。
     *
     * @param c 会话连接
     * @return {@code pg_current_wal_flush_lsn()} 打包 long
     * @throws SQLException 查询失败
     */
    protected static long flushLsn(Connection c) throws SQLException {
        return Lsn.parse(queryString(c, "SELECT pg_current_wal_flush_lsn()"));
    }

    /**
     * 轮询等待同步器消费前沿到达目标位点（100ms 节拍，超时 fail）。
     *
     * @param sync    已运行的同步器
     * @param target  目标位点（含）
     * @param message 超时失败消息描述
     */
    protected static void awaitConsumed(CatalogSynchronizer sync, long target, String message) {
        awaitTrue(() -> sync.consumedLsn() >= target, message);
        LOG.info("接收前沿到达 {}（快照前沿 {}）", Lsn.format(target), Lsn.format(sync.snapshot().lsn()));
    }

    /**
     * 轮询等待条件成立（100ms 节拍，30s 超时 fail）。
     *
     * @param cond    等待条件
     * @param message 超时失败消息描述
     */
    protected static void awaitTrue(BooleanSupplier cond, String message) {
        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + message);
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("等待被中断: " + message);
            }
        }
    }

    /**
     * 对拍主断言：停流后的快照字典 vs JDBC REPEATABLE READ 单事务实查，逐行全等。
     *
     * <p>关键步骤：① 两侧各自按基表 oid 收集 pg_class 行并发现 toast oid（并集为
     * 对拍范围——两侧行模型本身含 reltoastrelid 字段，若两侧发现不一致会在 pg_class
     * 行对拍暴露）；② JDBC 侧单 REPEATABLE READ 事务内实查 attr/class 全行（ctid 折键
     * + 行 record 组装）；③ 三断言面——attr 全行 / class 全等（diff 打印）、
     * relfilenodeOf/toastOf、tracked 双 ctid 对 JDBC 现居位；④ columnsOf 列序严格升序。
     * 边界与异常语义：调用方须先 {@link CatalogSynchronizer#stop()}（冻结态对拍）；
     * JDBC 查询无行（关系已删）取空集参与对拍。线程约束：测试线程单次调用。</p>
     *
     * @param sync       已停流的同步器
     * @param probe      对拍查询会话（本方法临时接管事务边界，结束复原）
     * @param trackedOid tracked 基表 oid（tracked 双 ctid 校验面，须在 relOids 中）
     * @param relOids    对拍关系 oid 集（基表，toast 自动并入）
     * @throws SQLException JDBC 实查失败
     */
    protected void assertCatalogMatchesJdbc(CatalogSynchronizer sync, Connection probe,
            long trackedOid, long... relOids) throws SQLException {
        TreeSet<Long> baseOids = new TreeSet<>();
        for (long oid : relOids) {
            baseOids.add(oid);
        }
        // 对拍范围并集：两侧各自发现的 toast oid 都并入（单侧陈旧时由 pg_class 行对拍暴露）
        TreeSet<Long> scope = new TreeSet<>(baseOids);
        scope.addAll(discoverToastOids(sync.stores().classRows(), baseOids));

        boolean oldAutoCommit = probe.getAutoCommit();
        probe.setAutoCommit(false);
        Map<Long, CatalogRow.ClassRow> jdbcClass;
        Map<Long, CatalogRow.AttrRow> jdbcAttr;
        try (Statement tx = probe.createStatement()) {
            tx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            jdbcClass = queryClassRows(probe, scope);
            scope.addAll(discoverToastOidsJdbc(jdbcClass, baseOids));
            jdbcClass = queryClassRows(probe, scope);
            jdbcAttr = queryAttrRows(probe, scope);
            probe.commit();
        } finally {
            probe.setAutoCommit(oldAutoCommit);
        }

        Map<Long, CatalogRow.ClassRow> oursClass = new TreeMap<>();
        sync.stores().classRows().forEach((k, v) -> {
            if (scope.contains(v.relOid())) {
                oursClass.put(k, v);
            }
        });
        Map<Long, CatalogRow.AttrRow> oursAttr = new TreeMap<>();
        sync.stores().attrRows().forEach((k, v) -> {
            if (scope.contains(v.attrelid())) {
                oursAttr.put(k, v);
            }
        });

        // 诊断先于断言（spec §10）：对拍面两侧规模 + 重放指标全量（失败时日志即证据）
        LOG.info("对拍诊断: 字典 class {} 行 / attr {} 行（全库）, 对拍范围我方 class {} / attr {} 行, 指标 {}",
                sync.stores().classRows().size(), sync.stores().attrRows().size(),
                oursClass.size(), oursAttr.size(), sync.metrics());
        assertRowsIdentical("pg_class", oursClass, jdbcClass, "oid 范围 " + scope);
        assertRowsIdentical("pg_attribute", oursAttr, jdbcAttr, "oid 范围 " + scope);

        CatalogSnapshot snapshot = sync.snapshot();
        List<String> problems = new ArrayList<>();
        for (long oid : baseOids) {
            CatalogRow.ClassRow jdbcRow = firstByOid(jdbcClass, oid);
            if (jdbcRow == null) {
                problems.add("oid " + oid + " 在 JDBC pg_class 无行（关系已删？对拍语义请核对）");
                continue;
            }
            OptionalLong filenode = snapshot.relfilenodeOf(oid);
            if (filenode.isEmpty() || filenode.getAsLong() != jdbcRow.relfilenode()) {
                problems.add("oid " + oid + " relfilenodeOf=" + describe(filenode)
                        + " 期望 " + jdbcRow.relfilenode());
            }
            OptionalLong toast = snapshot.toastOf(oid);
            if (toast.isEmpty() || toast.getAsLong() != jdbcRow.reltoastrelid()) {
                problems.add("oid " + oid + " toastOf=" + describe(toast)
                        + " 期望 " + jdbcRow.reltoastrelid());
            }
            assertColumnOrderAscending(snapshot, oid);
        }
        if (!problems.isEmpty()) {
            fail("relfilenodeOf/toastOf 对拍失败:\n  " + String.join("\n  ", problems));
        }

        assertTrackedMatchesJdbc(sync, jdbcClass, trackedOid);
        LOG.info("对拍全等: 范围 {}（attr {} 行 / class {} 行）, tracked {} -> {}",
                scope, oursAttr.size(), oursClass.size(),
                trackedOid, fmtCtid(sync.stores().trackedTableCtid()));
    }

    /**
     * tracked 双 ctid 校验：tracked 表位须等于该表行的 JDBC 现居 ctid；tracked toast 位
     * 须等于其 toast 关系行的 JDBC 现居 ctid（表无 toast 时期望 0）。
     *
     * @param sync      已停流的同步器
     * @param jdbcClass JDBC 侧 class 行字典（ctid 键）
     * @param trackedOid tracked 基表 oid
     */
    private static void assertTrackedMatchesJdbc(CatalogSynchronizer sync,
            Map<Long, CatalogRow.ClassRow> jdbcClass, long trackedOid) {
        // 两遍扫描（审查 Low-①）：遍历序不定，toast 关系行可能先于表行出现——首遍
        // 配对表行并发现 toast oid，次遍再配对 toast 行（单遍的 else-if 会漏配）
        Long jdbcTableCtid = null;
        long toastOid = 0;
        for (Map.Entry<Long, CatalogRow.ClassRow> e : jdbcClass.entrySet()) {
            if (e.getValue().relOid() == trackedOid) {
                jdbcTableCtid = e.getKey();
                toastOid = e.getValue().reltoastrelid();
            }
        }
        Long jdbcToastCtid = null;
        if (toastOid != 0) {
            for (Map.Entry<Long, CatalogRow.ClassRow> e : jdbcClass.entrySet()) {
                if (e.getValue().relOid() == toastOid) {
                    jdbcToastCtid = e.getKey();
                }
            }
        }
        assertTrue(jdbcTableCtid != null, "tracked 表 oid " + trackedOid + " 应在 JDBC 对拍集内");
        assertEquals(jdbcTableCtid.longValue(), sync.stores().trackedTableCtid(),
                "trackedTableCtid 须等于 JDBC 现居 ctid " + fmtCtid(jdbcTableCtid));
        long expectToast = jdbcToastCtid == null ? 0L : jdbcToastCtid;
        assertEquals(expectToast, sync.stores().trackedToastCtid(),
                "trackedToastCtid 须等于 toast 关系 JDBC 现居 ctid（无 toast 期望 0）");
    }

    /**
     * 列序断言：{@code columnsOf} 返回的 attnum 严格升序且无重复（dropped 占位保留在
     * 其原键位——发现 26 的字典语义由全等对拍覆盖内容，此处钉住次序面）。
     *
     * @param snapshot 快照视图
     * @param relOid   关系 oid
     */
    private static void assertColumnOrderAscending(CatalogSnapshot snapshot, long relOid) {
        int prev = 0;
        for (CatalogSnapshot.Column col : snapshot.columnsOf(relOid)) {
            assertTrue(col.attnum() > prev,
                    "oid " + relOid + " 列序须严格升序: attnum " + col.attnum() + " 未越过 " + prev);
            prev = col.attnum();
        }
    }

    /**
     * 逐行全等断言（diff 打印，spec §10 可诊断性）——键并集逐项比对：仅一侧有键、
     * 或双侧值不等，各打一行差异（ctid 键 + 双方行值），汇总后 fail。
     *
     * @param catalog 目录名（失败消息面）
     * @param ours     快照侧行字典（ctid 键）
     * @param actual   JDBC 侧行字典（ctid 键）
     * @param context  失败消息上下文（oid 范围）
     * @param <T>      行模型类型（record 值等价）
     */
    private static <T> void assertRowsIdentical(String catalog, Map<Long, T> ours,
            Map<Long, T> actual, String context) {
        if (ours.equals(actual)) {
            return;
        }
        List<String> lines = new ArrayList<>();
        TreeSet<Long> keys = new TreeSet<>(ours.keySet());
        keys.addAll(actual.keySet());
        for (Long k : keys) {
            T o = ours.get(k);
            T a = actual.get(k);
            if (o == null) {
                lines.add("  仅 JDBC 侧 " + fmtCtid(k) + " -> " + a);
            } else if (a == null) {
                lines.add("  仅快照侧 " + fmtCtid(k) + " -> " + o);
            } else if (!o.equals(a)) {
                lines.add("  字段不一致 " + fmtCtid(k) + ":\n    快照=" + o + "\n    JDBC =" + a);
            }
        }
        fail(catalog + " 对拍不一致（" + context + "; 快照 " + ours.size() + " 行 / JDBC "
                + actual.size() + " 行）——差异行:\n" + String.join("\n", lines));
    }

    /**
     * JDBC 实查 pg_class 全行（对拍范围 oid 集）。
     *
     * @param c    会话连接（REPEATABLE READ 事务内）
     * @param oids 对拍 oid 集
     * @return ctid 键 → ClassRow
     * @throws SQLException 查询失败
     */
    private static Map<Long, CatalogRow.ClassRow> queryClassRows(Connection c, TreeSet<Long> oids)
            throws SQLException {
        Map<Long, CatalogRow.ClassRow> out = new TreeMap<>();
        String sql = "SELECT ctid::text, oid, relname, relnamespace, reltype, reloftype,"
                + " relowner, relam, relfilenode, reltoastrelid FROM pg_class WHERE oid IN " + inList(oids);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.put(parseCtidKey(rs.getString(1)), new CatalogRow.ClassRow(
                        rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getLong(5), rs.getLong(6),
                        rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getLong(10)));
            }
        }
        return out;
    }

    /**
     * JDBC 实查 pg_attribute 全行（attnum&gt;0，含 dropped 占位）。
     *
     * @param c    会话连接（REPEATABLE READ 事务内）
     * @param oids 对拍 oid 集
     * @return ctid 键 → AttrRow
     * @throws SQLException 查询失败
     */
    private static Map<Long, CatalogRow.AttrRow> queryAttrRows(Connection c, TreeSet<Long> oids)
            throws SQLException {
        Map<Long, CatalogRow.AttrRow> out = new TreeMap<>();
        String sql = "SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                + " FROM pg_attribute WHERE attnum > 0 AND attrelid IN " + inList(oids);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.put(parseCtidKey(rs.getString(1)), new CatalogRow.AttrRow(
                        rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getInt(5), rs.getBoolean(6)));
            }
        }
        return out;
    }

    /**
     * 快照侧 class 行字典中发现 toast oid（基表行的 reltoastrelid 非 0 值）。
     *
     * @param classRows 快照侧行字典
     * @param baseOids  基表 oid 集
     * @return 发现的 toast oid 集（不含基表自身）
     */
    private static TreeSet<Long> discoverToastOids(Map<Long, CatalogRow.ClassRow> classRows, TreeSet<Long> baseOids) {
        TreeSet<Long> out = new TreeSet<>();
        for (CatalogRow.ClassRow row : classRows.values()) {
            if (baseOids.contains(row.relOid()) && row.reltoastrelid() != 0) {
                out.add(row.reltoastrelid());
            }
        }
        return out;
    }

    /**
     * JDBC 侧 class 行中发现 toast oid（与快照侧同式，两侧并集后由 pg_class 行对拍
     * 暴露任何单侧陈旧）。
     *
     * @param jdbcClass JDBC 侧行字典
     * @param baseOids  基表 oid 集
     * @return 发现的 toast oid 集
     */
    private static TreeSet<Long> discoverToastOidsJdbc(Map<Long, CatalogRow.ClassRow> jdbcClass,
            TreeSet<Long> baseOids) {
        return discoverToastOids(jdbcClass, baseOids);
    }

    /**
     * 按 oid 取首行（oid 在对拍集内唯一，首配即全配）。
     *
     * @param rows 行字典（任意一侧）
     * @param oid  关系 oid
     * @return 行模型；无行 null
     */
    private static CatalogRow.ClassRow firstByOid(Map<Long, CatalogRow.ClassRow> rows, long oid) {
        for (CatalogRow.ClassRow row : rows.values()) {
            if (row.relOid() == oid) {
                return row;
            }
        }
        return null;
    }

    /**
     * 单值字符串查询助手。
     *
     * @param c   会话连接
     * @param sql 恰返一行一列的查询
     * @return 首列字符串值
     * @throws SQLException 查询失败
     */
    private static String queryString(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    /**
     * oid 集渲染 SQL IN 列表（集合非空由调用方场景保证——relOids 至少含基表）。
     *
     * @param oids oid 集
     * @return 形如 {@code (1,2,3)} 的列表文本
     */
    private static String inList(TreeSet<Long> oids) {
        return "(" + String.join(",", oids.stream().map(String::valueOf).toList()) + ")";
    }

    /**
     * PG 文本 ctid "(block,off)" 折为 ctid 键（式同 {@link CatalogReplay#ctidKey}；
     * 测试侧独立实现——主代码的 {@code CatalogBootstrap.parseCtidKey} 为 replay 包
     * 可见（it 包不可达），{@code JdbcProbe.ProbedRow.parse} 面向 ProbedRow 载体，
     * 裸键解析此处自持最薄）。
     *
     * @param ctid 服务端文本形态
     * @return ctid 键
     */
    private static long parseCtidKey(String ctid) {
        String[] parts = ctid.replaceAll("[() ]", "").split(",");
        return CatalogReplay.ctidKey(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    /**
     * ctid 键的可读形态（diff 打印面）。
     *
     * @param key ctid 键
     * @return 形如 {@code (block 3, off 12)} 的文本
     */
    private static String fmtCtid(long key) {
        return "(block " + (key >>> 16) + ", off " + (key & 0xFFFF) + ")";
    }

    /**
     * OptionalLong 的诊断渲染。
     *
     * @param v 待渲染值
     * @return empty 显示 "empty"，否则数值
     */
    private static String describe(OptionalLong v) {
        return v.isPresent() ? String.valueOf(v.getAsLong()) : "empty";
    }
}
