package org.vastdata.vbstream.walsource.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * JDBC 一致性引导（spec §6①）——把 pg_attribute / pg_class 两张 watched 目录的全行
 * 快照（含物理 ctid）灌入 {@link CatalogStores}，回填两目录 relfilenode，并按
 * interest oid 定位 tracked 双 ctid。
 *
 * <p><strong>一致性顺序（调用方契约）</strong>：槽 P₀ 须已由调用方先经
 * {@code PhysicalSlotManager.ensureSlot} 取得（建槽自身的 WAL 写入落在引导快照之前）
 * → 本类在 <strong>REPEATABLE READ 单事务</strong>内执行全部种子查询（首句
 * {@code SET TRANSACTION ISOLATION LEVEL REPEATABLE READ}，两表快照同一时点）→
 * 返回同事务内的 {@code pg_current_wal_flush_lsn()}——调用方以 max(P₀, 返回值)
 * 起流，窗口重叠由 ctid 键控 upsert 幂等消化。</p>
 *
 * <p>种子查询面（spike main 种子段的全表扩展）：pg_attribute 全表取
 * {@code attnum > 0}（裁定与 spike 一致——系统列不进字典面）；pg_class
 * <strong>不限 relkind 全表</strong>（toast/index 行也要：tracked toast 行与 INPLACE
 * 对象都是 pg_class 行）。线程约束：持外部 Connection 单连接，非线程安全——装配
 * 线程一次性调用。</p>
 */
public final class CatalogBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBootstrap.class);

    /** pg_attribute 全行种子查询（attnum>0：系统列不进字典，裁定与 spike 一致）。 */
    private static final String ATTR_SEED_SQL =
            "SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                    + " FROM pg_attribute WHERE attnum > 0";

    /** pg_class 全行种子查询（不限 relkind：toast/index 行也在跟踪面）。 */
    private static final String CLASS_SEED_SQL =
            "SELECT ctid::text, oid, relname, relnamespace, reltype, reloftype,"
                    + " relowner, relam, relfilenode, reltoastrelid FROM pg_class";

    /** interest 关系的 pg_class 行定位（tracked 表行位 + toast oid 顺取）。 */
    private static final String INTEREST_CLASS_SQL =
            "SELECT ctid::text, reltoastrelid FROM pg_class WHERE oid = ?";

    /** toast 关系的行位定位（trackedToastCtid 回填）。 */
    private static final String TOAST_CTID_SQL =
            "SELECT ctid::text FROM pg_class WHERE oid = ?";

    private final Connection connection;

    private final WalLayout layout;

    /**
     * 装配引导器。
     *
     * @param connection 工作会话（调用方持有生命周期；建议独占使用——本方法会临时
     *                   切 autoCommit 与事务隔离级，结束后复原）
     * @param layout     版本布局描述符（种子行模型经行 record 投影，词典/偏移校验的
     *                   预留锚点，当前无直接取值面）
     */
    public CatalogBootstrap(Connection connection, WalLayout layout) {
        this.connection = connection;
        this.layout = layout;
    }

    /**
     * 执行一致性引导：REPEATABLE READ 单事务内灌满两目录全行 + 回填 relfilenode 与
     * tracked 双 ctid，返回同事务内的 flush LSN。
     *
     * <p>关键步骤：① 记住并关闭 autoCommit，首句 SET TRANSACTION ISOLATION LEVEL
     * REPEATABLE READ（此后所有查询共享同一时点快照）；② 回填
     * {@code pg_relation_filenode('pg_attribute'/'pg_class'::regclass)}（块匹配面，
     * 未引导时重放引擎按 0 不匹配）；③ 两目录全行种子（ctid 文本折键 + 行模型落
     * 字典）；④ 按 interest oid 定位 tracked 表行位与 toast 行位（v1 tracked 面单表：
     * 多个命中取最小 oid 并 WARN）；⑤ 同事务内查 flush LSN → commit 复原 autoCommit
     * → 返回。边界与异常语义：任一 SQLException 走 rollback + 复原 autoCommit 后原样
     * 上抛（半灌状态不留在 stores——调用方废弃本 stores 重建）；interest oid 查无行
     * WARN 后跳过（关系尚未创建）。<strong>会话副作用</strong>：本方法临时接管调用方
     * 会话的事务边界——关闭 autoCommit、置 REPEATABLE READ、结束时 commit——调用方
     * 在传入连接上若有<strong>未决事务会被一并 commit</strong>（请以干净会话传入）。
     * 线程约束：装配线程单次调用。</p>
     *
     * @param stores 引导目标状态容器（本方法为首个也是唯一写者）
     * @return 引导完成时点的 {@code pg_current_wal_flush_lsn()}（打包 long）
     * @throws SQLException 任一种子/位点查询失败
     */
    public long bootstrap(CatalogStores stores) throws SQLException {
        boolean oldAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (Statement tx = connection.createStatement()) {
            tx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            seedRelfilenodes(stores);
            int attrCount = seedAttrRows(stores);
            int classCount = seedClassRows(stores);
            locateTracked(stores);
            long flushLsn = currentFlushLsn(tx);
            connection.commit();
            LOG.info("catalog 引导完成 (layout PG{}): pg_attribute {} 行 / pg_class {} 行, pgAttrRelnode={}, pgClassRelnode={},"
                            + " trackedTable={}, trackedToast={}, 快照 flush LSN={}",
                    layout.majorVersion(), attrCount, classCount,
                    stores.pgAttrRelfilenode(), stores.pgClassRelfilenode(),
                    stores.trackedTableCtid(), stores.trackedToastCtid(), Lsn.format(flushLsn));
            return flushLsn;
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(oldAutoCommit);
        }
    }

    /**
     * PG 文本 ctid "(block,off)" 折为 {@link CatalogReplay#ctidKey(int, int)} 键
     * （spike {@code replaceAll("[() ]","").split(",")} 的等价面）。
     *
     * <p>边界与异常语义：空段/非数字/缺逗号抛 {@link IllegalArgumentException}
     * （引导查询产物形态违约属服务端异常，fail-fast 优于静默错位）。</p>
     *
     * @param ctid 服务端文本形态，如 "(123,45)"
     * @return ctid 键
     */
    static long parseCtidKey(String ctid) {
        int open = ctid.indexOf('(');
        int comma = ctid.indexOf(',', open + 1);
        int close = ctid.indexOf(')', comma + 1);
        if (open < 0 || comma <= open || close <= comma) {
            throw new IllegalArgumentException("malformed ctid (expect (block,off)): " + ctid);
        }
        return CatalogReplay.ctidKey(
                Integer.parseInt(ctid.substring(open + 1, comma).trim()),
                Integer.parseInt(ctid.substring(comma + 1, close).trim()));
    }

    /**
     * 回填两目录的 relfilenode（重放引擎的块匹配面，spec §6① 步骤②）。
     *
     * @param stores 状态容器
     * @throws SQLException 查询失败
     */
    private void seedRelfilenodes(CatalogStores stores) throws SQLException {
        try (Statement st = connection.createStatement()) {
            stores.pgAttrRelfilenode(queryLong(st, "SELECT pg_relation_filenode('pg_attribute'::regclass)"));
            stores.pgClassRelfilenode(queryLong(st, "SELECT pg_relation_filenode('pg_class'::regclass)"));
        }
    }

    /**
     * pg_attribute 全行种子：ctid 折键 + AttrRow 落 attrRows（attnum&gt;0 全部关系，
     * 含 dropped 占位行——发现 26 的字典语义）。
     *
     * @param stores 状态容器
     * @return 灌入行数
     * @throws SQLException 查询失败
     */
    private int seedAttrRows(CatalogStores stores) throws SQLException {
        int count = 0;
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(ATTR_SEED_SQL)) {
            while (rs.next()) {
                stores.attrRows().put(parseCtidKey(rs.getString(1)), new CatalogRow.AttrRow(
                        rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getInt(5), rs.getBoolean(6)));
                count++;
            }
        }
        return count;
    }

    /**
     * pg_class 全行种子：ctid 折键 + ClassRow 落 classRows（不限 relkind——toast/index
     * 行也在跟踪面，INPLACE 对象与 tracked toast 依赖它们）。
     *
     * @param stores 状态容器
     * @return 灌入行数
     * @throws SQLException 查询失败
     */
    private int seedClassRows(CatalogStores stores) throws SQLException {
        int count = 0;
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(CLASS_SEED_SQL)) {
            while (rs.next()) {
                stores.classRows().put(parseCtidKey(rs.getString(1)), new CatalogRow.ClassRow(
                        rs.getLong(2), rs.getString(3), rs.getLong(4), rs.getLong(5),
                        rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getLong(10)));
                count++;
            }
        }
        return count;
    }

    /**
     * 按 interest oid 定位 tracked 双 ctid（v1 tracked 面单表）：逐 oid 查 pg_class
     * 行位与 reltoastrelid，<strong>存在行的 oid 中取最小者</strong>独占 tracked 面
     * （interestRelOids 是 HashSet 无序——取 min 保证多次引导确定性），其余命中 WARN
     * 跳过。
     *
     * <p>边界与异常语义：oid 查无行 WARN 跳过（关系尚未创建——后续经流内 toast 收养
     * /Task 12 自愈补齐）；reltoastrelid=0 不查 toast 行位（无 toast 关系）；toast
     * 行查无行 WARN 后 trackedToast 留 0（流内 INS 收养兜底）。</p>
     *
     * @param stores 状态容器（interestRelOids 已由调用方注册）
     * @throws SQLException 查询失败
     */
    private void locateTracked(CatalogStores stores) throws SQLException {
        List<long[]> existing = new ArrayList<>();    // 元素 {oid, tableCtid, toastOid}
        try (PreparedStatement ps = connection.prepareStatement(INTEREST_CLASS_SQL)) {
            for (long oid : stores.interestRelOids()) {
                ps.setLong(1, oid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        existing.add(new long[]{oid, parseCtidKey(rs.getString(1)), rs.getLong(2)});
                    } else {
                        LOG.warn("interest oid {} 在 pg_class 无行（尚未创建？）——跳过 tracked 定位", oid);
                    }
                }
            }
        }
        if (existing.isEmpty()) {
            return;
        }
        existing.sort(Comparator.comparingLong(a -> a[0]));
        if (existing.size() > 1) {
            LOG.warn("interest 命中 {} 个 oid，v1 tracked 面单表——仅跟随最小 oid {}，其余不跟踪",
                    existing.size(), existing.get(0)[0]);
        }
        long[] winner = existing.get(0);
        stores.trackedTableCtid(winner[1]);
        if (winner[2] != 0) {
            try (PreparedStatement ps = connection.prepareStatement(TOAST_CTID_SQL)) {
                ps.setLong(1, winner[2]);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        stores.trackedToastCtid(parseCtidKey(rs.getString(1)));
                    } else {
                        LOG.warn("toast 关系 oid {} 在 pg_class 无行——trackedToast 暂缺（流内收养兜底）", winner[2]);
                    }
                }
            }
        }
    }

    /**
     * 单值 long 查询便捷档。
     *
     * @param st   复用语句
     * @param sql  恰返一行的单列查询
     * @return 首行首列值
     * @throws SQLException 查询失败
     */
    private static long queryLong(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * 同事务内的当前 flush 位点（write 位点可能领先 flush——START_REPLICATION 起点
     * 用它会被拒，spike 注记；REPEATABLE READ 不冻结该全局量，返回值 ≥ 种子快照
     * 时点，多出的窗口重叠由 ctid upsert 幂等消化）。
     *
     * @param tx 事务语句（保证与种子同一事务）
     * @return flush LSN（打包 long）
     * @throws SQLException 查询失败
     */
    private static long currentFlushLsn(Statement tx) throws SQLException {
        try (ResultSet rs = tx.executeQuery("SELECT pg_current_wal_flush_lsn()")) {
            rs.next();
            return Lsn.parse(rs.getString(1));
        }
    }
}
