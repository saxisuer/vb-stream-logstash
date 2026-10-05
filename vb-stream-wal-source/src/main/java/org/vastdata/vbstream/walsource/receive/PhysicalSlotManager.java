package org.vastdata.vbstream.walsource.receive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 物理复制槽的生命周期管理器：确保槽存在并给出续传起点 LSN。
 *
 * <p>语义（任务书 Task 8）：槽不存在 → {@code pg_create_physical_replication_slot} 建槽并
 * 返回创建时刻的 {@code pg_current_wal_flush_lsn()}（流从"现在"起收，之前的事务不在窗口）；
 * 槽已存在 → 查 {@code pg_replication_slots.restart_lsn}（物理槽的续传起点列；逻辑槽才是
 * confirmed_flush_lsn）返回，实现重启续传。建槽撞 42710（duplicate_object）视为他方并发
 * 建槽的竞态——捕获后重查存在槽走复用路径，不向调用方扩散。</p>
 *
 * <p>已知限制（记档，Task 15 部分收敛）：建槽带 {@code immediately_reserve=true}，
 * restart_lsn 自建槽时刻非 NULL 且可被 {@code pg_replication_slot_advance} 推进
 * （Task 15 检查点后推槽的依托）；存量槽若由未 reserve 形态建出（restart_lsn
 * NULL），复用路径回落返回 {@code pg_current_wal_flush_lsn()}（等效"槽在但从未消费
 * 过"的新建语义）。另注：pgjdbc 42.7.13 的物理流构造器无槽选项（START_REPLICATION
 * 不带 SLOT），槽当前作位点锚与 WAL 保留策略的载体（推进 restart_lsn 即收缩保留
 * 窗口下界），见 {@link WalStreamReceiver} javadoc。</p>
 *
 * <p>线程约束：持有外部 {@link Connection} 单连接，非线程安全——调用方保证串行调用
 * （典型为装配线程一次性 ensureSlot 后交给接收器）。</p>
 */
public final class PhysicalSlotManager {

    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotManager.class);

    /** 槽不存在时 {@link #querySlotResumeLsn(String)} 的返回哨兵（0 是合法 LSN 低位，不可作哨兵）。 */
    private static final long SLOT_ABSENT = -1L;

    private final Connection connection;

    /**
     * 装配一个槽管理器。
     *
     * @param connection 工作会话（调用方持有生命周期；须有建槽权限——典型 superuser/角色
     *                   pg_replication）
     */
    public PhysicalSlotManager(Connection connection) {
        this.connection = connection;
    }

    /**
     * 确保物理槽存在并返回续传起点 LSN。
     *
     * <p>关键步骤：① 查 {@code pg_replication_slots}（slot_name + slot_type='physical' 双条件，
     * 同名逻辑槽不算命中）——存在即按 restart_lsn（NULL 回落 flush LSN）返回；② 不存在则
     * {@code pg_create_physical_replication_slot(?, true)} 建槽（<strong>immediately_reserve
     * = true</strong>，Task 15：restart_lsn 自建槽时刻即保留 WAL——未 reserve 的槽
     * restart_lsn 为 NULL，既不可被 {@code pg_replication_slot_advance} 推进（服务端
     * "never previously reserved WAL" 拒绝），保留窗口也不成立）后返回建槽时刻 flush
     * LSN；③ 建槽抛 42710 = 并发竞态，静默重查走复用路径。
     * 边界与异常语义：42710 以外的 SQLException 原样上抛（权限不足/连接失效等）；重查仍无槽
     * （异常形态）按新建路径返回 flush LSN 并 WARN。</p>
     *
     * @param name 槽名（物理槽命名约束同逻辑槽：小写/数字/下划线）
     * @return 续传起点 LSN（新建=建槽时刻 flush LSN；复用=restart_lsn）
     * @throws SQLException 查询/建槽失败（42710 除外，已被竞态路径吸收）
     */
    public long ensureSlot(String name) throws SQLException {
        long existing = querySlotResumeLsn(name);
        if (existing != SLOT_ABSENT) {
            LOG.info("物理槽 {} 已存在，续传起点 restart_lsn={}", name, Lsn.format(existing));
            return existing;
        }
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM pg_create_physical_replication_slot(?, true)")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
            }
            LOG.info("物理槽 {} 创建成功（immediately_reserve=true, restart_lsn 已保留）", name);
        } catch (SQLException e) {
            if (!"42710".equals(e.getSQLState())) {
                throw e;
            }
            LOG.info("物理槽 {} 被并发创建（SQLState 42710）——重查续传位点", name);
            long raced = querySlotResumeLsn(name);
            if (raced != SLOT_ABSENT) {
                return raced;
            }
            LOG.warn("物理槽 {} 竞态重查仍无行——按新建语义返回当前 flush LSN", name);
        }
        return currentFlushLsn();
    }

    /**
     * 查存在物理槽的续传位点。
     *
     * <p>关键步骤：slot_name + slot_type 双条件查询 restart_lsn；无行返回
     * {@link #SLOT_ABSENT}；有行但 restart_lsn 为 NULL（建槽未 reserve、从未附槽）回落
     * 当前 flush LSN——与"新建即从现在收"语义对齐。</p>
     *
     * @param name 槽名
     * @return 续传起点 LSN；槽不存在返回 -1
     * @throws SQLException 查询失败
     */
    private long querySlotResumeLsn(String name) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = ? AND slot_type = 'physical'")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return SLOT_ABSENT;
                }
                String restart = rs.getString(1);
                if (restart == null) {
                    long flush = currentFlushLsn();
                    LOG.info("槽 {} 的 restart_lsn 尚为 NULL（未 reserve/未附槽）——回落 flush LSN {}",
                            name, Lsn.format(flush));
                    return flush;
                }
                return Lsn.parse(restart);
            }
        }
    }

    /**
     * 当前 WAL flush 位点（写入已落盘位置——START_REPLICATION 的合法起点上限）。
     *
     * <p>注意用 flush 而非 {@code pg_current_wal_lsn()}：write 位点可能领先 flush，
     * 用它起流会被服务端拒绝（spike main 的实证注记）。</p>
     *
     * @return flush LSN（打包 long）
     * @throws SQLException 查询失败
     */
    private long currentFlushLsn() throws SQLException {
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_current_wal_flush_lsn()")) {
            rs.next();
            return Lsn.parse(rs.getString(1));
        }
    }
}
