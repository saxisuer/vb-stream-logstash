package org.vastdata.vbstream.walsource.changes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * {@link ToastProbe} 的 JDBC 实现——持调用方普通 SQL 会话两段式回查 toast chunk。
 *
 * <p><b>两段式的原因（实测钉，2026-10-07 docker PG 18.6）</b>：toast 表名
 * {@code pg_toast.pg_toast_<主表oid>} 的后缀是<b>主表</b> oid，而 external 指针携带的
 * {@code va_toastrelid} 是 toast 关系自身 oid（实测主表 33654 → toast oid 33657 →
 * 表名 {@code pg_toast_33654}）——先查 {@code pg_class.relname}（OID 索引点查）再按
 * 解析出的 relname 查 chunk（静态 SQL 无法按 oid 动态寻表）。relname 须匹配
 * {@code ^pg_toast_\d+$} 白名单后才拼入 SQL（catalog 值进标识符位的注入防御）。</p>
 *
 * <p>线程约束：单个 pgjdbc Connection 非线程安全——按 wal-receiver 单线程上下文
 * 使用，跨线程共享须由调用方串行化。</p>
 */
public final class JdbcToastProbe implements ToastProbe {

    /** toast 关系 oid → relname 的点查（relname 即拼名原料）。 */
    private static final String RELNAME_SQL = "SELECT relname FROM pg_class WHERE oid=?";

    /** relname 白名单：pg_toast_ + 纯数字（catalog 自产形态，防标识符注入）。 */
    private static final Pattern TOAST_RELNAME = Pattern.compile("pg_toast_\\d+");

    private final Connection connection;

    /**
     * 构造探测器。
     *
     * @param connection 普通 SQL 会话（生命周期归调用方）
     */
    public JdbcToastProbe(Connection connection) {
        this.connection = connection;
    }

    /**
     * 回查指定 TOAST 值的全量 chunk：①pg_class 点查 relname（白名单校验后拼入
     * 标识符位）；②按 chunk_id 取行（ORDER BY chunk_seq 保序，虽然返回 Map 无序，
     * 上游拼装按 seq 键控，此处排序仅作确定性），chunk_data 经 getBytes 取原始字节。
     *
     * <p>边界与异常语义：任一段 0 行（relname 缺失 / 无 chunk 行）返回空映射
     * （"查无"语义交上游降级判定）；SQLException 包装 IllegalStateException 上抛
     * ——基础设施故障由 {@code ToastAssembler} 捕获降级，不在本层吞。</p>
     *
     * @param toastOid external 指针的 va_toastrelid（toast 关系 oid）
     * @param valueid  external 指针的 va_valueid（chunk_id 查键）
     * @return chunk_seq → chunk_data（空映射 = 无行）
     */
    @Override
    public Map<Long, byte[]> fetchChunks(long toastOid, long valueid) {
        String relname;
        try (PreparedStatement ps = connection.prepareStatement(RELNAME_SQL)) {
            ps.setLong(1, toastOid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Map.of();
                }
                relname = rs.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("toast 关系名解析失败: toastOid=" + toastOid, e);
        }
        if (!TOAST_RELNAME.matcher(relname).matches()) {
            throw new IllegalStateException("toast 关系名不符合 pg_toast_<oid> 形态: " + relname);
        }
        Map<Long, byte[]> chunks = new LinkedHashMap<>();
        String sql = "SELECT chunk_seq, chunk_data FROM pg_toast.\"" + relname + "\" WHERE chunk_id=? ORDER BY chunk_seq";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, valueid);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    chunks.put(rs.getLong(1), rs.getBytes(2));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("toast chunk 回查失败: relname=" + relname + ", valueid=" + valueid, e);
        }
        return chunks;
    }
}
