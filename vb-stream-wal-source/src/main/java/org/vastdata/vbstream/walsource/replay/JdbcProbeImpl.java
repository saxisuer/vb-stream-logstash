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

    /** 点查语句（列序与 {@link CatalogRow.ClassRow} 组件序严格对应，ctid 居首）。 */
    private static final String SQL = "SELECT ctid::text, relname, relnamespace, reltype, reloftype, "
            + "relowner, relam, relfilenode, reltoastrelid FROM pg_class WHERE oid=?";

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
                        rs.getLong(6), rs.getLong(7), rs.getLong(8), rs.getLong(9)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("pg_class 末态探测失败: oid=" + relOid, e);
        }
    }
}
