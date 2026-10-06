package org.vastdata.vbstream.walsource.replay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link JdbcProbe} 的薄 JDBC 实现（Task 13 IT / 生产接线用）——持调用方的普通
 * SQL 会话做 pg_class 单行点查，无状态、无自有生命周期（会话关闭归调用方）。
 *
 * <p>线程约束：单个 pgjdbc Connection 非线程安全——本实现按重放线程单写者上下文
 * 使用（与 {@link CatalogReplay} 同缝）；跨线程共享须由调用方串行化。</p>
 */
public final class JdbcProbeImpl implements JdbcProbe {

    /** 点查语句（列序与 {@link CatalogRow.ClassRow} 组件序严格对应，ctid 居首；
     * v2 增选 relkind——char 输出经 ::text 为裸单字符）。 */
    private static final String SQL = "SELECT ctid::text, relname, relnamespace, reltype, reloftype, "
            + "relowner, relam, relfilenode, reltoastrelid, relkind::text FROM pg_class WHERE oid=?";

    /** pg_attribute 按 ctid 点查语句（attr 面精确采纳探测，列序与 AttrRow 组件序对应）。 */
    private static final String ATTR_BY_CTID_SQL =
            "SELECT ctid::text, attrelid, attname, atttypid, attnum, attisdropped"
                    + " FROM pg_attribute WHERE ctid = ?::tid";

    /** pg_class 按 ctid 点查语句（class 面精确采纳探测，列序与 ClassRow 组件序对应；
     * v2 增选 relkind）。 */
    private static final String CLASS_BY_CTID_SQL = "SELECT ctid::text, oid, relname, relnamespace, reltype,"
            + " reloftype, relowner, relam, relfilenode, reltoastrelid, relkind::text"
            + " FROM pg_class WHERE ctid = ?::tid";

    private final Connection connection;

    /**
     * 构造探测器。
     *
     * @param connection 普通 SQL 会话（引导连接即可复用；生命周期归调用方）
     */
    public JdbcProbeImpl(Connection connection) {
        this.connection = connection;
    }

    /**
     * 点查 pg_class 末态行：prepare → 绑 oid → 取首行（ctid 文本经
     * {@link ProbedRow#parse} 折键，其余列按序组装 {@link CatalogRow.ClassRow}）。
     *
     * <p>边界与异常语义：空结果集返回 null（行不存在，候选拒绝路径）；SQLException
     * 包装 IllegalStateException 上抛（fail-fast：探测通道故障属基础设施异常，
     * 不在"连续失败不升级"的校验失败语义内——后者指校验判否，见
     * {@link SelfHealer#validate}）。</p>
     *
     * @param relOid 关系 oid
     * @return 末态行；行不存在 null
     */
    @Override
    public ProbedRow currentClassRow(long relOid) {
        try (PreparedStatement ps = connection.prepareStatement(SQL)) {
            ps.setLong(1, relOid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return ProbedRow.parse(rs.getString(1), new CatalogRow.ClassRow(relOid,
                        rs.getString(2), rs.getLong(3), rs.getLong(4), rs.getLong(5),
                        rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getString(10)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("pg_class 末态探测失败: oid=" + relOid, e);
        }
    }

    /**
     * 按物理 ctid 点查 pg_attribute 末态行：prepare → 绑 ctid 文本（{@code ?::tid} 由
     * 服务端转换）→ 取首行组装 {@link CatalogRow.AttrRow}。
     *
     * <p>边界与异常语义：空结果集返回 null（该位无行——attr 采纳拒绝路径）；
     * SQLException 包装 IllegalStateException 上抛（fail-fast，探测通道故障同
     * {@link #currentClassRow} 语义）。</p>
     *
     * @param ctidText ctid 文本形态（"(block,off)"）
     * @return 末态行；该位无行 null
     */
    @Override
    public CatalogRow.AttrRow currentAttrRowByCtid(String ctidText) {
        try (PreparedStatement ps = connection.prepareStatement(ATTR_BY_CTID_SQL)) {
            ps.setString(1, ctidText);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new CatalogRow.AttrRow(rs.getLong(2), rs.getString(3),
                        rs.getLong(4), rs.getInt(5), rs.getBoolean(6));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("pg_attribute ctid 探测失败: " + ctidText, e);
        }
    }

    /**
     * 按物理 ctid 点查 pg_class 末态行：prepare → 绑 ctid 文本（{@code ?::tid} 服务端
     * 转换）→ 取首行经 {@link ProbedRow#parse} 折键组装。
     *
     * <p>边界与异常语义：空结果集返回 null（该位无行——class 采纳拒绝路径）；
     * SQLException 包装 IllegalStateException 上抛（fail-fast 语义同
     * {@link #currentClassRow(long)}）。</p>
     *
     * @param ctidText ctid 文本形态（"(block,off)"）
     * @return 末态行（ctid 键 + 行模型）；该位无行 null
     */
    @Override
    public ProbedRow currentClassRowByCtid(String ctidText) {
        try (PreparedStatement ps = connection.prepareStatement(CLASS_BY_CTID_SQL)) {
            ps.setString(1, ctidText);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return ProbedRow.parse(rs.getString(1), new CatalogRow.ClassRow(rs.getLong(2),
                        rs.getString(3), rs.getLong(4), rs.getLong(5), rs.getLong(6),
                        rs.getLong(7), rs.getLong(8), rs.getLong(9), rs.getLong(10), rs.getString(11)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("pg_class ctid 探测失败: " + ctidText, e);
        }
    }
}
