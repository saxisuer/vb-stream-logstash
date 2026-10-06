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
 * JDBC 一致性引导（spec §6①）——把 pg_attribute / pg_class / pg_namespace 三张
 * watched 目录的全行快照（含物理 ctid）灌入 {@link CatalogStores}，回填三目录
 * relfilenode，并按 interest oid 定位 tracked 双 ctid（v2 扩第三目录 pg_namespace
 * ——schema 名解析的字典源）。
 *
 * <p><strong>一致性顺序（调用方契约）</strong>：槽 P₀ 须已由调用方先经
 * {@code PhysicalSlotManager.ensureSlot} 取得（建槽自身的 WAL 写入落在引导快照之前）
 * → 本类在 <strong>REPEATABLE READ 单事务</strong>内执行：首句
 * {@code SET TRANSACTION ISOLATION LEVEL REPEATABLE READ}，<strong>紧随其后的第一句
 * 读 {@code pg_current_wal_flush_lsn()}（B）——先于任何种子查询</strong>（使 B ≤ 种子
 * 快照时点 S：快照未建立前读不到"未来"，而种子快照后的提交其 WAL 记录末尾必 &gt; B，
 * 落进重放窗口），此后两表种子查询共享同一时点快照——调用方以 max(P₀, B) 起流，
 * (B,S] 窗口重叠按 spec §6① 原意<strong>重放</strong>消化（INS/UPD ctid 键控 upsert
 * 幂等、截断更新走 ctid 寻址末态采纳收敛——引导后小段自愈 probe 噪声属预期）。</p>
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

    /** pg_class 全行种子查询（不限 relkind：toast/index 行也在跟踪面；v2 增选 relkind——
     * 表过滤的分派键，char 输出经 getText 为裸单字符）。 */
    private static final String CLASS_SEED_SQL =
            "SELECT ctid::text, oid, relname, relnamespace, reltype, reloftype,"
                    + " relowner, relam, relfilenode, reltoastrelid, relkind::text FROM pg_class";

    /** pg_namespace 全行种子查询（v2 扩链第三 watched 目录，schema 名解析源）。 */
    private static final String NSP_SEED_SQL =
            "SELECT ctid::text, oid, nspname FROM pg_namespace";

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
     * tracked 双 ctid，返回同事务内<strong>首句读取</strong>的 flush LSN。
     *
     * <p>关键步骤：① 记住并关闭 autoCommit，首句 SET TRANSACTION ISOLATION LEVEL
     * REPEATABLE READ；② <strong>紧随其后（任何种子查询之前）读 flush LSN（B）</strong>
     * ——次序是一致性契约的一部分：B 必须不晚于种子快照时点 S（终审 C1 修复——原
     * "种子之后再读"形态下 (S,B] 并发提交的 DDL/ANALYZE 效果既不在种子快照、WAL 记录
     * 又被 appliedLsn 过滤线永久跳过，pg_attribute INSERT 丢失无自愈通道）；③ 回填
     * {@code pg_relation_filenode('pg_attribute'/'pg_class'::regclass)}（块匹配面，
     * 未引导时重放引擎按 0 不匹配，此后所有种子查询共享同一时点快照）；④ 两目录全行
     * 种子（ctid 文本折键 + 行模型落字典）；⑤ 按 interest oid 定位 tracked 表行位与
     * toast 行位（v1 tracked 面单表：多个命中取最小 oid 并 WARN）→ commit 复原
     * autoCommit → 返回 B。边界与异常语义：任一 SQLException 走 rollback + 复原
     * autoCommit 后原样上抛（半灌状态不留在 stores——调用方废弃本 stores 重建）；
     * interest oid 查无行 WARN 后跳过（关系尚未创建）。<strong>会话副作用</strong>：
     * 本方法临时接管调用方会话的事务边界——关闭 autoCommit、置 REPEATABLE READ、结束
     * 时 commit——调用方在传入连接上若有<strong>未决事务会被一并 commit</strong>
     * （请以干净会话传入）。线程约束：装配线程单次调用。</p>
     *
     * @param stores 引导目标状态容器（本方法为首个也是唯一写者）
     * @return 事务首句时点的 {@code pg_current_wal_flush_lsn()}（打包 long，≤ 种子快照时点）
     * @throws SQLException 任一种子/位点查询失败
     */
    public long bootstrap(CatalogStores stores) throws SQLException {
        boolean oldAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (Statement tx = connection.createStatement()) {
            tx.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            // flush LSN 必须先于全部种子查询读取（终审 C1）：保证 B ≤ 种子快照时点 S——
            // 种子快照后的任何提交其记录末尾必 > B，进重放窗口；(B,S] 重叠由 upsert 幂等消化
            long flushLsn = currentFlushLsn(tx);
            seedRelfilenodes(stores);
            int attrCount = seedAttrRows(stores);
            int classCount = seedClassRows(stores);
            int nspCount = seedNspRows(stores);
            locateTracked(stores);
            connection.commit();
            LOG.info("catalog 引导完成 (layout PG{}): pg_attribute {} 行 / pg_class {} 行 / pg_namespace {} 行,"
                            + " pgAttrRelnode={}, pgClassRelnode={}, pgNspRelnode={},"
                            + " trackedTable={}, trackedToast={}, 首句 flush LSN={}（≤ 种子快照时点）",
                    layout.majorVersion(), attrCount, classCount, nspCount,
                    stores.pgAttrRelfilenode(), stores.pgClassRelfilenode(), stores.pgNspRelnode(),
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
     * 回填三目录的 relfilenode（重放引擎的块匹配面，spec §6① 步骤②；v2 增
     * pg_namespace）。
     *
     * @param stores 状态容器
     * @throws SQLException 查询失败
     */
    private void seedRelfilenodes(CatalogStores stores) throws SQLException {
        try (Statement st = connection.createStatement()) {
            stores.pgAttrRelfilenode(queryLong(st, "SELECT pg_relation_filenode('pg_attribute'::regclass)"));
            stores.pgClassRelfilenode(queryLong(st, "SELECT pg_relation_filenode('pg_class'::regclass)"));
            stores.pgNspRelnode(queryLong(st, "SELECT pg_relation_filenode('pg_namespace'::regclass)"));
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
     * 行也在跟踪面，INPLACE 对象与 tracked toast 依赖它们；v2 增投影 relkind）。
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
                        rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9),
                        rs.getLong(10), rs.getString(11)));
                count++;
            }
        }
        return count;
    }

    /**
     * pg_namespace 全行种子：ctid 折键 + NspRow 落 nspRows（v2 扩链——schema 名解析
     * 的字典源；CREATE SCHEMA/DROP SCHEMA 经流内重放增量维护）。
     *
     * @param stores 状态容器
     * @return 灌入行数
     * @throws SQLException 查询失败
     */
    private int seedNspRows(CatalogStores stores) throws SQLException {
        int count = 0;
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(NSP_SEED_SQL)) {
            while (rs.next()) {
                stores.nspRows().put(parseCtidKey(rs.getString(1)),
                        new CatalogRow.NspRow(rs.getLong(2), rs.getString(3)));
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
     * 用它会被拒，spike 注记；REPEATABLE READ 不冻结该全局量，<strong>必须先于全部
     * 种子查询调用</strong>——使返回值 B ≤ 种子快照时点 S（种子快照后的提交记录末尾
     * 必 &gt; B、进重放窗口），(B,S] 窗口重叠由 ctid 键控 upsert 幂等重放消化）。
     *
     * @param tx 事务语句（保证与种子同一事务）
     * @return flush LSN（打包 long，读取时点 ≤ 种子快照时点）
     * @throws SQLException 查询失败
     */
    private static long currentFlushLsn(Statement tx) throws SQLException {
        try (ResultSet rs = tx.executeQuery("SELECT pg_current_wal_flush_lsn()")) {
            rs.next();
            return Lsn.parse(rs.getString(1));
        }
    }
}
