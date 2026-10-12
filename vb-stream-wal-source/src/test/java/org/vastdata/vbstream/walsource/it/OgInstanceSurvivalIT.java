package org.vastdata.vbstream.walsource.it;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;
import org.vastdata.vbstream.walsource.receive.DirectWalSource;
import org.vastdata.vbstream.walsource.receive.FileWalSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * openGauss 5.0.0 实例存活红线 IT（M1 Task 9——<strong>本测试不过则 M1 不算完成</strong>）：
 * Direct 档（裸 v3 直连）循环 {@value #ROUNDS} 轮"连 → 收 {@value #RECEIVE_MS}ms →
 * 杀连接"，每轮后断言实例仍接受 SQL（{@code SELECT 1} + 探针行可查）且容器日志无
 * {@code Gaussdb exit} 标记——杀直连物理流会话不得把 gaussdb 实例带崩。
 *
 * <p><strong>杀连接两种形态交替</strong>（奇数轮服务端杀 / 偶数轮客户端杀，覆盖
 * 两类死亡向量）：奇数轮 {@code pg_terminate_backend} 杀服务端 walsender 进程
 * （pid 先物化再按值 terminate——规避 {@code pg_stat_replication} 视图谓词下推
 * 自杀面）；偶数轮客户端 {@code source.stop()} 的裸 socket 关闭（无 CopyDone 的
 * 突然断开）。源以 {@code maxAttempts=1} 装配——服务端杀即触发 IOException 落入
 * 终态面（不自动重连），轮次边界干净。</p>
 *
 * <p><strong>日志检查面</strong>：gaussdb 由容器 entrypoint 前台拉起，postmaster
 * 死亡的退出痕迹打到容器日志——{@code OgTestEnv} 的 log consumer 滚动缓冲提供
 * 快照，本 IT 前置断言基线无标记（环境异常前置暴露）、每轮断言仍无标记。</p>
 */
class OgInstanceSurvivalIT {

    private static final Logger LOG = LoggerFactory.getLogger(OgInstanceSurvivalIT.class);

    /** 循环轮数。 */
    private static final int ROUNDS = 5;

    /** 每轮接收窗（毫秒）——初块后持续收流的最短时长。 */
    private static final long RECEIVE_MS = 3_000;

    /** 直连源心跳 statusInterval（毫秒，心跳周期 = 一半）。 */
    private static final long STATUS_INTERVAL_MS = 1_000;

    /** 首块数据到达的等待上限（毫秒）。 */
    private static final long FIRST_CHUNK_TIMEOUT_MS = 15_000;

    /** 服务端杀后源进入终态的等待上限（毫秒）。 */
    private static final long TERMINAL_TIMEOUT_MS = 15_000;

    /** 日志退出标记（小写比对——openGauss 消息大小写形态不作假设）。 */
    private static final String EXIT_MARKER = "gaussdb exit";

    /**
     * 主断言：5 轮杀连接循环后实例存活且持续可服务——每轮三面断言（源面：数据
     * 已收 / 终态符合杀法；实例面：SELECT 1 可查 + 探针行计数递增；日志面：无
     * {@code Gaussdb exit}）。
     *
     * @throws Exception 任一环节失败
     */
    @Test
    void fiveRoundsOfKilledDirectConnectionsLeaveInstanceAliveAndServing() throws Exception {
        String baseline = OgTestEnv.containerLogSnapshot();
        assertFalse(baseline.toLowerCase().contains(EXIT_MARKER),
                "前置：容器日志不应已有 gaussdb 退出标记（环境异常）: " + tailFor(baseline));
        OgTestEnv.execSql(
                "DROP TABLE IF EXISTS og_survive_probe",
                "CREATE TABLE og_survive_probe(id int primary key, v text)");

        for (int round = 1; round <= ROUNDS; round++) {
            boolean serverSideKill = round % 2 == 1;
            // maxAttempts=1：服务端杀的 IOException 直接落终态（无重连），轮次边界干净
            DirectWalSource source = new DirectWalSource(OgTestEnv.host(), OgTestEnv.port(),
                    OgTestEnv.USER, OgTestEnv.PASSWORD, null, STATUS_INTERVAL_MS,
                    FileWalSource.DEFAULT_SEGMENT_SIZE, 30_000, 1_000, 1, 0L);
            source.start(lsn -> { }, (lsn, bytes) -> { });
            try {
                awaitFirstChunk(source);
                // 观察窗内驱动真实 'w' 流量（INSERT 走逐条路径，不触发 MULTI_INSERT 面）
                OgTestEnv.execSql("INSERT INTO og_survive_probe VALUES (" + round + ", 'round-" + round + "')");
                Thread.sleep(RECEIVE_MS);
            } finally {
                if (serverSideKill) {
                    terminateWalsenders();
                    awaitTerminal(source);
                }
                source.stop();
            }
            LOG.info("存活轮 {}/{} 完成（杀法={}）: 源前沿 {}",
                    round, ROUNDS, serverSideKill ? "服务端terminate" : "客户端socket关闭",
                    Lsn.format(source.consumedLsn()));
            assertInstanceServingSql(round);
            assertNoGaussdbExit(round);
        }
        OgTestEnv.execSql("DROP TABLE og_survive_probe");
    }

    /**
     * 等首块 'w' 数据到达（consumedLsn&gt;0——直连源初连即收到段起点到当前前沿的
     * 积压流，常理秒级；超时或终态失败即红）。
     *
     * @param source 直连源
     * @throws InterruptedException 轮询被中断
     */
    private static void awaitFirstChunk(DirectWalSource source) throws InterruptedException {
        long deadline = System.currentTimeMillis() + FIRST_CHUNK_TIMEOUT_MS;
        while (source.consumedLsn() == 0) {
            assertTrue(source.terminalFailure().isEmpty(),
                    "首块前终态失败: " + source.terminalFailure());
            assertTrue(System.currentTimeMillis() < deadline,
                    FIRST_CHUNK_TIMEOUT_MS + "ms 内未收到首块 'w' 数据");
            Thread.sleep(200);
        }
    }

    /**
     * 服务端杀 walsender：先物化本端的 walsender pid 列表，再逐个按值
     * {@code pg_terminate_backend}（谓词下推自杀面的规避写法——物化后 terminate 与
     * 视图读解耦）。<strong>OG 实测锚</strong>：{@code pg_stat_replication.application_name}
     * 报 {@code "WalSender to Standby[vb-wal-source]"}（名字带前缀包裹）——按
     * {@code LIKE '%vb-wal-source%'} 匹配而非全等。无 walsender 时为 no-op（源尚未
     * 建立流——由调用方 awaitTerminal 的超时面兜住）。
     *
     * @throws SQLException 查询/terminate 失败
     */
    private static void terminateWalsenders() throws SQLException {
        List<Long> pids = new ArrayList<>();
        try (Connection c = OgTestEnv.newSqlConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT pid FROM pg_stat_replication WHERE application_name LIKE '%vb-wal-source%'")) {
            while (rs.next()) {
                pids.add(rs.getLong(1));
            }
        }
        LOG.info("服务端杀 walsender: 命中 {} 个 pid {}", pids.size(), pids);
        try (Connection c = OgTestEnv.newSqlConnection(); Statement st = c.createStatement()) {
            for (long pid : pids) {
                st.execute("SELECT pg_terminate_backend(" + pid + ")");
            }
        }
    }

    /**
     * 等源进入终态（服务端杀的验收面：IOException 计入失败账目，maxAttempts=1 下
     * 一步到位）——超时即红（terminate 未生效或源未感知断流）。
     *
     * @param source 直连源
     * @throws InterruptedException 轮询被中断
     */
    private static void awaitTerminal(DirectWalSource source) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TERMINAL_TIMEOUT_MS;
        while (source.terminalFailure().isEmpty()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("服务端杀后 " + TERMINAL_TIMEOUT_MS
                        + "ms 内源未进入终态（terminate 未生效？前沿 "
                        + Lsn.format(source.consumedLsn()) + "）");
            }
            Thread.sleep(200);
        }
        LOG.info("服务端杀生效: 终态 = {}", source.terminalFailure().get().toString());
    }

    /**
     * 实例存活断言：新连接 {@code SELECT 1} + 探针表计数恰等于已完成轮数——杀
     * walsender 会话不得影响普通 SQL 服务面。
     *
     * @param round 已完成的轮次（探针行计数期望）
     * @throws SQLException 查询失败（本身就是断言失败面）
     */
    private static void assertInstanceServingSql(int round) throws SQLException {
        try (Connection c = OgTestEnv.newSqlConnection();
             Statement st = c.createStatement()) {
            st.execute("SELECT 1");
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM og_survive_probe")) {
                assertTrue(rs.next() && rs.getInt(1) == round,
                        "轮 " + round + " 后探针行计数应恰为 " + round + "（实例可服务面）");
            }
        }
    }

    /**
     * 日志红线断言：容器日志滚动缓冲不含 {@code Gaussdb exit} 标记（小写比对）——
     * 失败消息附日志尾（排障面：postmaster 死亡时的最后痕迹）。
     *
     * @param round 已完成的轮次（失败消息定位面）
     */
    private static void assertNoGaussdbExit(int round) {
        String log = OgTestEnv.containerLogSnapshot();
        assertFalse(log.toLowerCase().contains(EXIT_MARKER),
                "轮 " + round + " 后容器日志出现 gaussdb 退出标记——实例存活红线被击穿。日志尾:\n"
                        + tailFor(log));
    }

    /**
     * 日志尾片段（失败诊断面）。
     *
     * @param log 完整日志快照
     * @return 末 2000 字符
     */
    private static String tailFor(String log) {
        return log.length() <= 2000 ? log : log.substring(log.length() - 2000);
    }
}
