package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.core.BaseConnection;
import org.postgresql.copy.CopyManager;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalLayouts;
import org.vastdata.vbstream.walsource.layout.WalRecord;
import org.vastdata.vbstream.walsource.receive.PhysicalSlotManager;
import org.vastdata.vbstream.walsource.receive.WalStreamReceiver;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 物理槽管理 + pgjdbc 接收器 + census 对拍 pg_waldump 的首个真 PG 集成测试。
 *
 * <p>断言面：① {@link PhysicalSlotManager#ensureSlot(String)} 的建槽/复用语义（physical 槽落库、
 * 重复调用返回续传位点）；② 接收器收流的 rmid 维度 census 与容器内 {@code pg_waldump} 同窗口
 * 对拍（每 rmid 计数差 ≤ 2 容差——双方起点附近的页对齐/边界记录差异由此吸收）且 resyncs==0；
 * ③ 断流重连的两形态——<strong>两段式</strong>（stop → 新实例从 consumedLsn 续起，位点不回退、
 * 严格后继、census 对拍）与<strong>原地重连</strong>（收流中途 pg_terminate_backend 杀接收器后端，
 * 重连续传 + reconnects &gt; 0）；④ RMGR_NAMES 与 {@code pg_get_wal_resource_managers()} 权威表
 * 逐项对表（census 对拍地基）；⑤ 首连即失败（错误端口）时 resume 兜底为页对齐初值而非 0
 * （High-1 回归）+ stop 后再 start 抛 ISE。容器重启级重连（Task 15 生命周期 IT）不在本类
 * 覆盖，属已知限制。</p>
 */
class WalReceiveTest {

    /** rmid 显示名映射：容器 {@code pg_get_wal_resource_managers()} 权威 ordinal 转录（22 项，
     *  postgres:18 实测）——与 pg_waldump 输出的 rmgr 名对拍用；由
     *  {@link #rmgrNamesMatchContainerResourceManagers()} 逐项对表防版本/转录漂移。 */
    private static final String[] RMGR_NAMES = {
            "XLOG",               // 0
            "Transaction",        // 1
            "Storage",            // 2
            "CLOG",               // 3
            "Database",           // 4
            "Tablespace",         // 5
            "MultiXact",          // 6
            "RelMap",             // 7
            "Standby",            // 8
            "Heap2",              // 9
            "Heap",               // 10
            "Btree",              // 11
            "Hash",               // 12
            "Gin",                // 13
            "Gist",               // 14
            "Sequence",           // 15
            "SPGist",             // 16
            "BRIN",               // 17
            "CommitTs",           // 18
            "ReplicationOrigin",  // 19
            "Generic",            // 20
            "LogicalMessage",     // 21
    };

    /** RM_HEAP_ID（rmgrlist.h 枚举序 10）——census 非平凡断言用。 */
    private static final int RM_HEAP_ID = 10;

    /** RM_HEAP2_ID（枚举序 9）——COPY 触发的 multi-insert 走 heap2。 */
    private static final int RM_HEAP2_ID = 9;

    /** pg_waldump 记录行的 rmgr 名捕获。 */
    private static final Pattern RMGR_LINE = Pattern.compile("^rmgr: (\\S+)\\s");

    /** 本类各测试的槽名（独立槽防跨类残留干扰；finally 统一清理）。 */
    private final List<String> slots = new ArrayList<>();

    /** 本测试启动过的接收器——AfterEach 兜底 stop，防非守护线程漏关拖住 surefire JVM。 */
    private final List<WalStreamReceiver> receivers = new ArrayList<>();

    /**
     * 兜底清理：停接收器 + 删槽——正常路径各测试内已清理，这里是异常路径保险。
     */
    @AfterEach
    void cleanup() {
        for (WalStreamReceiver r : receivers) {
            r.stop();
        }
        for (String s : slots) {
            WalTestEnv.dropPhysicalSlotQuietly(s);
        }
    }

    /** 用例 1：ensureSlot 建 physical 槽并返回续传位点；重复调用按存在槽返回 restart_lsn 路径。 */
    @Test
    void ensureSlotCreatesPhysicalSlotAndReturnsResumePoint() throws Exception {
        String slot = "wal_it_slot_mgr";
        slots.add(slot);
        WalTestEnv.dropPhysicalSlotQuietly(slot);
        try (Connection c = WalTestEnv.newSqlConnection()) {
            PhysicalSlotManager mgr = new PhysicalSlotManager(c);

            long created = mgr.ensureSlot(slot);
            assertTrue(created > 0, "建槽返回的 flush LSN 应为正值: " + Lsn.format(created));
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT slot_type, active FROM pg_replication_slots WHERE slot_name = '" + slot + "'")) {
                assertTrue(rs.next(), "槽应已存在");
                assertEquals("physical", rs.getString(1), "应为物理槽");
            }

            long resumed = mgr.ensureSlot(slot);
            assertTrue(resumed > 0, "复用路径也应返回正值位点: " + Lsn.format(resumed));
        }
    }

    /** 用例 2：接收器收流 census 与 pg_waldump 同窗口对拍（rmid 维度 ≤2 容差）+ resyncs==0。 */
    @Test
    void receivedCensusMatchesPgWaldumpWithinTolerance() throws Exception {
        String slot = "wal_it_census";
        slots.add(slot);
        WalTestEnv.dropPhysicalSlotQuietly(slot);
        WalLayout layout = WalLayouts.forServerVersion(WalTestEnv.serverVersionNum());

        long start;
        try (Connection c = WalTestEnv.newSqlConnection()) {
            start = new PhysicalSlotManager(c).ensureSlot(slot);
        }

        List<WalRecord> received = new ArrayList<>();
        WalStreamReceiver receiver = new WalStreamReceiver(WalTestEnv.host(), WalTestEnv.port(),
                WalTestEnv.databaseName(), WalTestEnv.username(), WalTestEnv.password(), layout, slot);
        receivers.add(receiver);
        receiver.start(received::add, start);

        long endLsn;
        try (Connection w = WalTestEnv.newSqlConnection()) {
            try (Statement st = w.createStatement()) {
                st.execute("DROP TABLE IF EXISTS t_wal_census");
                st.execute("CREATE TABLE t_wal_census (id int, s text)");
                for (int i = 0; i < 5; i++) {
                    st.execute("INSERT INTO t_wal_census VALUES (" + i + ", 'v" + i + "')");
                }
            }
            // COPY 走 heap2 multi-insert 路径（spike S1c 实证：普通 multi-VALUES 仍逐行 heap_insert）
            new CopyManager(w.unwrap(BaseConnection.class)).copyIn(
                    "COPY t_wal_census FROM STDIN WITH (FORMAT text)", new StringReader("10\ta\n11\tb\n12\tc\n"));
            endLsn = Lsn.parse(queryString(w, "SELECT pg_current_wal_flush_lsn()"));
            // 尾部推动：多写一条窗口外记录，保证流前沿越过 endLsn（否则末条记录可能滞留 carry 无法判定收齐）
            try (Statement st = w.createStatement()) {
                st.execute("INSERT INTO t_wal_census VALUES (99, 'tail')");
            }
        }

        awaitTrue(() -> receiver.consumedLsn() >= endLsn, 30_000,
                "接收前沿到达 " + Lsn.format(endLsn) + "（实际 " + Lsn.format(receiver.consumedLsn()) + "）");
        receiver.stop();

        Map<Integer, Long> ours = rmidCensus(received, start, endLsn);
        Map<String, Long> oracle = waldumpCensus(WalTestEnv.pgWaldump(start, endLsn));
        assertCensusNearOracle(ours, oracle);
        assertTrue(ours.getOrDefault(RM_HEAP_ID, 0L) > 0, "窗口内应见到 Heap 记录: " + ours);
        assertTrue(ours.getOrDefault(RM_HEAP2_ID, 0L) > 0, "COPY 应触发 heap2 multi-insert: " + ours);
        assertEquals(0, receiver.metrics().resyncs.sum(), "全程应零再同步（pageaddr 锚定协议成立）");
    }

    /** 用例 3：两段式续传——stop 后新实例从 consumedLsn 续起，位点不回退、census 是严格后继窗口。 */
    @Test
    void restartFromConsumedLsnContinuesWithoutRegression() throws Exception {
        String slot = "wal_it_two_phase";
        slots.add(slot);
        WalTestEnv.dropPhysicalSlotQuietly(slot);
        WalLayout layout = WalLayouts.forServerVersion(WalTestEnv.serverVersionNum());

        WalTestEnv.execSql("DROP TABLE IF EXISTS t_wal_two_phase",
                "CREATE TABLE t_wal_two_phase (id int, s text)");
        long start;
        try (Connection c = WalTestEnv.newSqlConnection()) {
            start = new PhysicalSlotManager(c).ensureSlot(slot);
        }

        // 段 1：收流到 end1 并停机
        List<WalRecord> seg1 = new ArrayList<>();
        WalStreamReceiver r1 = new WalStreamReceiver(WalTestEnv.host(), WalTestEnv.port(),
                WalTestEnv.databaseName(), WalTestEnv.username(), WalTestEnv.password(), layout, slot);
        receivers.add(r1);
        r1.start(seg1::add, start);
        long end1;
        try (Connection w = WalTestEnv.newSqlConnection()) {
            try (Statement st = w.createStatement()) {
                for (int i = 0; i < 3; i++) {
                    st.execute("INSERT INTO t_wal_two_phase VALUES (" + i + ", 's1-" + i + "')");
                }
            }
            end1 = Lsn.parse(queryString(w, "SELECT pg_current_wal_flush_lsn()"));
            try (Statement st = w.createStatement()) {
                st.execute("INSERT INTO t_wal_two_phase VALUES (98, 'tail1')");
            }
        }
        awaitTrue(() -> r1.consumedLsn() >= end1, 30_000, "段 1 前沿到达 " + Lsn.format(end1));
        r1.stop();
        long consumed1 = r1.consumedLsn();
        assertTrue(consumed1 >= end1, "停机前沿不应低于已写窗口末端");
        long maxSeg1 = seg1.stream().mapToLong(WalRecord::lsn).max().orElse(0);
        assertTrue(maxSeg1 < consumed1, "段 1 全部记录起点应严格先于停机前沿（后继性分界）");

        // 段 2：新实例从 consumedLsn 续起
        List<WalRecord> seg2 = new ArrayList<>();
        WalStreamReceiver r2 = new WalStreamReceiver(WalTestEnv.host(), WalTestEnv.port(),
                WalTestEnv.databaseName(), WalTestEnv.username(), WalTestEnv.password(), layout, slot);
        receivers.add(r2);
        r2.start(seg2::add, consumed1);
        long end2;
        try (Connection w = WalTestEnv.newSqlConnection()) {
            try (Statement st = w.createStatement()) {
                for (int i = 3; i < 6; i++) {
                    st.execute("INSERT INTO t_wal_two_phase VALUES (" + i + ", 's2-" + i + "')");
                }
            }
            new CopyManager(w.unwrap(BaseConnection.class)).copyIn(
                    "COPY t_wal_two_phase FROM STDIN WITH (FORMAT text)", new StringReader("20\tx\n21\ty\n"));
            end2 = Lsn.parse(queryString(w, "SELECT pg_current_wal_flush_lsn()"));
            try (Statement st = w.createStatement()) {
                st.execute("INSERT INTO t_wal_two_phase VALUES (99, 'tail2')");
            }
        }
        awaitTrue(() -> r2.consumedLsn() >= end2, 30_000, "段 2 前沿到达 " + Lsn.format(end2));
        r2.stop();
        long consumed2 = r2.consumedLsn();
        assertTrue(consumed2 > consumed1, "位点不应回退: " + Lsn.format(consumed1) + " -> " + Lsn.format(consumed2));

        long seg2Floor = consumed1 & ~0x1FFFL; // 段 2 实际起点按页对齐下取（接收器契约，8KB 页）
        for (WalRecord r : seg2) {
            assertTrue(r.lsn() >= seg2Floor,
                    "段 2 记录不应早于起点页: " + Lsn.format(r.lsn()) + " < " + Lsn.format(seg2Floor));
        }
        // 段 2 在 [consumed1, end2) 窗口的 census 对拍 oracle——缺失（回退跳段）或重收都会体现在计数差
        Map<Integer, Long> ours = rmidCensus(seg2, consumed1, end2);
        Map<String, Long> oracle = waldumpCensus(WalTestEnv.pgWaldump(consumed1, end2));
        assertCensusNearOracle(ours, oracle);
        assertTrue(ours.getOrDefault(RM_HEAP_ID, 0L) > 0, "段 2 窗口应含段 2 写入的 Heap 记录: " + ours);
        assertEquals(0, r2.metrics().resyncs.sum(), "段 2 续传应零再同步");
    }

    /** 用例 4：RMGR_NAMES 映射表与容器权威表逐项对齐（census 对拍的地基，防漂移静默错位）。 */
    @Test
    void rmgrNamesMatchContainerResourceManagers() throws Exception {
        Map<Integer, String> authoritative = WalTestEnv.walResourceManagers();
        List<String> problems = new ArrayList<>();
        for (int i = 0; i < Math.max(RMGR_NAMES.length, authoritative.size()); i++) {
            String mine = i < RMGR_NAMES.length ? RMGR_NAMES[i] : "<缺项>";
            String theirs = authoritative.get(i);
            if (theirs == null || !theirs.equals(mine)) {
                problems.add("rmid=" + i + ": 表=" + mine + " 容器=" + theirs);
            }
        }
        assertEquals(RMGR_NAMES.length, authoritative.size(),
                "表长与容器权威表不一致\n  " + String.join("\n  ", problems));
        assertTrue(problems.isEmpty(), "RMGR_NAMES 与 pg_get_wal_resource_managers() 漂移:\n  "
                + String.join("\n  ", problems));
    }

    /** 用例 5：收流中途杀接收器后端（pg_terminate_backend）——原地重连续传：位点不回退、
     *  后继窗口 census 对拍、reconnects &gt; 0、resyncs==0。 */
    @Test
    void backendTerminationMidStreamReconnectsInPlace() throws Exception {
        String slot = "wal_it_kill";
        slots.add(slot);
        WalTestEnv.dropPhysicalSlotQuietly(slot);
        WalLayout layout = WalLayouts.forServerVersion(WalTestEnv.serverVersionNum());
        WalTestEnv.execSql("DROP TABLE IF EXISTS t_wal_kill", "CREATE TABLE t_wal_kill (id int, s text)");
        long start;
        try (Connection c = WalTestEnv.newSqlConnection()) {
            start = new PhysicalSlotManager(c).ensureSlot(slot);
        }

        List<WalRecord> received = new ArrayList<>();
        WalStreamReceiver receiver = new WalStreamReceiver(WalTestEnv.host(), WalTestEnv.port(),
                WalTestEnv.databaseName(), WalTestEnv.username(), WalTestEnv.password(), layout, slot);
        receivers.add(receiver);
        receiver.start(received::add, start);
        awaitTrue(() -> countWalsenders() >= 1, 15_000, "walsender 注册进 pg_stat_replication");

        long mid;
        long endB;
        try (Connection w = WalTestEnv.newSqlConnection()) {
            try (Statement st = w.createStatement()) {
                for (int i = 0; i < 3; i++) {
                    st.execute("INSERT INTO t_wal_kill VALUES (" + i + ", 'pre-" + i + "')");
                }
            }
            long endA = Lsn.parse(queryString(w, "SELECT pg_current_wal_flush_lsn()"));
            try (Statement st = w.createStatement()) {
                st.execute("INSERT INTO t_wal_kill VALUES (97, 'tailA')");
            }
            awaitTrue(() -> receiver.consumedLsn() >= endA, 30_000, "杀连接前前沿到达 " + Lsn.format(endA));
            mid = receiver.consumedLsn();

            // 杀接收器的 walsender 后端——readPending 随即 SQLException，接收器应原地重连。
            // 坑位（psql 隔离实证 + EXPLAIN 铁证）：不能写
            //   SELECT ... FROM pg_stat_replication WHERE pg_terminate_backend(pid)
            // —— pg_stat_replication 是视图（pg_stat_get_wal_senders JOIN pg_stat_get_activity），
            // 谓词会被下推到 pg_stat_get_activity 扫描层，那里的 pid 指向全部后端，
            // 一次杀光含查询者自身（idle 服务器上亦复现自杀伤）。必须先取 pid 再按值 terminate。
            List<Long> walsenderPids = new ArrayList<>();
            try (Statement st = w.createStatement();
                 ResultSet rs = st.executeQuery("SELECT pid FROM pg_stat_replication")) {
                while (rs.next()) {
                    walsenderPids.add(rs.getLong(1));
                }
            }
            assertEquals(1, walsenderPids.size(), "应恰有一个活跃 walsender（接收器后端）: " + walsenderPids);
            try (Statement st = w.createStatement()) {
                st.execute("SELECT pg_terminate_backend(" + walsenderPids.get(0) + ")");
            }

            try (Statement st = w.createStatement()) {
                for (int i = 3; i < 6; i++) {
                    st.execute("INSERT INTO t_wal_kill VALUES (" + i + ", 'post-" + i + "')");
                }
            }
            new CopyManager(w.unwrap(BaseConnection.class)).copyIn(
                    "COPY t_wal_kill FROM STDIN WITH (FORMAT text)", new StringReader("30\tu\n31\tv\n"));
            endB = Lsn.parse(queryString(w, "SELECT pg_current_wal_flush_lsn()"));
            try (Statement st = w.createStatement()) {
                st.execute("INSERT INTO t_wal_kill VALUES (99, 'tailB')");
            }
            awaitTrue(() -> receiver.consumedLsn() >= endB, 30_000,
                    "重连后前沿到达 " + Lsn.format(endB) + "（当前 " + Lsn.format(receiver.consumedLsn()) + "）");
        }
        receiver.stop();

        assertTrue(receiver.consumedLsn() > mid, "重连后位点不应回退: mid=" + Lsn.format(mid)
                + " now=" + Lsn.format(receiver.consumedLsn()));
        assertTrue(receiver.metrics().reconnects.sum() > 0, "断流应触发至少一次重连");
        assertEquals(0, receiver.metrics().resyncs.sum(), "重连续传应零再同步");
        Map<Integer, Long> ours = rmidCensus(received, mid, endB);
        Map<String, Long> oracle = waldumpCensus(WalTestEnv.pgWaldump(mid, endB));
        assertCensusNearOracle(ours, oracle);
        assertTrue(ours.getOrDefault(RM_HEAP_ID, 0L) > 0, "后继窗口应含杀连接后写入的 Heap 记录: " + ours);
    }

    /** 用例 6：首连即失败（错误端口）——重试进行中 resume 兜底为页对齐初值而非 0（High-1 回归）。 */
    @Test
    void wrongPortFirstConnectionFailureKeepsAlignedStart() throws Exception {
        String slot = "wal_it_wrong_port";
        slots.add(slot);
        WalLayout layout = WalLayouts.forServerVersion(WalTestEnv.serverVersionNum());
        long start;
        try (Connection c = WalTestEnv.newSqlConnection()) {
            start = new PhysicalSlotManager(c).ensureSlot(slot);
        }
        long aligned = start & ~0x1FFFL;

        WalStreamReceiver receiver = new WalStreamReceiver(WalTestEnv.host(), WalTestEnv.port() + 1,
                WalTestEnv.databaseName(), WalTestEnv.username(), WalTestEnv.password(), layout, slot);
        receivers.add(receiver);
        receiver.start(r -> { }, start);
        // 两次失败（退避 1s+2s）即足以证明重试循环运转且 consumedLsn 未被 0 哨兵污染——
        // 不等满 5 次停机（31s 退避太慢），停机终态由单元逻辑与 javadoc 承担
        awaitTrue(() -> receiver.metrics().reconnects.sum() >= 2, 15_000, "首连失败重试至少两次");
        assertEquals(aligned, receiver.consumedLsn(),
                "首连失败期间前沿应保持页对齐初值（High-1：不得回退到 0/0）");
        receiver.stop();
        assertThrows(IllegalStateException.class, () -> receiver.start(r -> { }, start),
                "stop 后再 start 应抛 ISE");
    }

    /**
     * 当前 pg_stat_replication 行数（活跃 walsender 数）。
     *
     * @return walsender 数（查询失败返回 0——awaitTrue 轮询形态）
     */
    private static int countWalsenders() {
        try (Connection c = WalTestEnv.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_stat_replication")) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            return 0;
        }
    }

    /**
     * 轮询等待条件成立（100ms 节拍）。
     *
     * @param cond      等待条件
     * @param timeoutMs 超时毫秒
     * @param what      超时失败消息描述
     */
    private static void awaitTrue(BooleanSupplier cond, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + what);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("等待被中断: " + what);
            }
        }
    }

    /**
     * 单值字符串查询助手。
     *
     * @param c   会话连接
     * @param sql 期望恰好一行一列的查询
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
     * 把记录列表按 rmid 聚合计数，窗口过滤 {@code [fromIncl, endExcl)}（LSN 起点）。
     *
     * @param recs     记录列表
     * @param fromIncl 窗口起点（含）
     * @param endExcl  窗口终点（不含）
     * @return rmid → 条数（升序 TreeMap，失败消息可读）
     */
    private static Map<Integer, Long> rmidCensus(List<WalRecord> recs, long fromIncl, long endExcl) {
        Map<Integer, Long> m = new TreeMap<>();
        for (WalRecord r : recs) {
            if (r.lsn() >= fromIncl && r.lsn() < endExcl) {
                m.merge(r.rmid(), 1L, Long::sum);
            }
        }
        return m;
    }

    /**
     * 解析 pg_waldump 输出行为 rmgr 名 → 条数。
     *
     * @param lines pg_waldump 记录行（{@code "rmgr: "} 前缀已过滤）
     * @return rmgr 名 → 条数
     */
    private static Map<String, Long> waldumpCensus(List<String> lines) {
        Map<String, Long> m = new TreeMap<>();
        for (String line : lines) {
            Matcher matcher = RMGR_LINE.matcher(line);
            if (matcher.find()) {
                m.merge(matcher.group(1), 1L, Long::sum);
            }
        }
        return m;
    }

    /**
     * census 对拍：每 rmid 双方计数差 ≤ 2 即过（双方起点页对齐/边界记录差异的容差），
     * oracle 出现未知 rmgr 名直接失败（映射表与容器版本漂移须显式暴露）。
     *
     * @param ours   接收器侧 rmid census
     * @param oracle pg_waldump 侧 rmgr 名 census
     */
    private static void assertCensusNearOracle(Map<Integer, Long> ours, Map<String, Long> oracle) {
        List<String> problems = new ArrayList<>();
        boolean[] seen = new boolean[RMGR_NAMES.length];
        for (Map.Entry<String, Long> e : oracle.entrySet()) {
            int rmid = -1;
            for (int i = 0; i < RMGR_NAMES.length; i++) {
                if (RMGR_NAMES[i].equals(e.getKey())) {
                    rmid = i;
                    seen[i] = true;
                    break;
                }
            }
            if (rmid < 0) {
                problems.add("oracle 未知 rmgr 名: " + e.getKey() + " (RMGR_NAMES 与容器版本漂移?)");
                continue;
            }
            long diff = ours.getOrDefault(rmid, 0L) - e.getValue();
            if (Math.abs(diff) > 2) {
                problems.add(RMGR_NAMES[rmid] + "(rmid=" + rmid + "): ours=" + ours.getOrDefault(rmid, 0L)
                        + " oracle=" + e.getValue() + " 差=" + diff);
            }
        }
        for (Map.Entry<Integer, Long> e : ours.entrySet()) {
            if (!seen[e.getKey()]) {
                String name = e.getKey() < RMGR_NAMES.length ? RMGR_NAMES[e.getKey()] : "?";
                long oracleCount = oracle.getOrDefault(name, 0L);
                long diff = e.getValue() - oracleCount;
                if (Math.abs(diff) > 2) {
                    problems.add(name + "(rmid=" + e.getKey() + "): ours=" + e.getValue()
                            + " oracle=" + oracleCount + " 差=" + diff);
                }
            }
        }
        if (!problems.isEmpty()) {
            fail("census 对拍失败 (容差 2):\n  " + String.join("\n  ", problems)
                    + "\n  ours=" + ours + "\n  oracle=" + oracle);
        }
    }
}
