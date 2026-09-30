package org.vastdata.debezium.connector.postgresql.stream.it;

import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真库验收(1.9.7 线):存量槽 two_phase 属性与配置不匹配时启动期拒绝(带 DROP SLOT
 * 迁移指引)。单测({@code ReplicationSessionTest} 的目录脚本假件)锚定分支语义,本类补
 * 真库面——真 42710 路径 + {@code pg_replication_slots} 真目录行。场景:先以
 * two_phase=false 建槽(模拟存量槽)→ 以 two_phase=true 配置启动引擎 → 启动失败且异常
 * 链文案含槽名与 DROP SLOT 指引 → 槽未被删(拒绝不得有副作用)。
 *
 * <p><b>失败上报面(1.9.7 线的基座原生通道,与 3.6.1 的差异)</b>:3.6.1 版向基座
 * {@code start} 注入自定义 CompletionCallback 并以 Awaitility 轮询回调置位;新基座
 * {@code start} 无回调注入口(CompletionCallback 固定为基座失败通道),改用
 * {@code expectEngineFailure() → start → awaitEngine() → engineFailure()} 三步:预声明
 * 预期失败(豁免基座的连带抛出)→ 启动 → <b>awaitEngine 等引擎 run() 返回</b>(失败
 * 即返回,60s 卡死探测兜底;若 R5 预检缺失呈"静默复用"bug 形态,引擎长跑不停,
 * awaitEngine 60s 超时 fail——与 3.6.1 版 Awaitility 60s 轮询超时等价的硬断言)→
 * {@code engineFailure()} 取 CompletionCallback 捕获的原始异常,经
 * {@link #renderThrowableChain} 全链渲染后断言(引擎侧失败被层层包装,槽名/指引在链尾,
 * 单层 getMessage 会漏)。需要本机 Docker。
 */
class SlotTwoPhaseMismatchIT extends StreamITBase {

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "ms4_slot_mismatch";

    /** publication 名:ensureSlot 在建流之前即拒绝,publication 实际不被消费,占位即可。 */
    private static final String PUB = "pub_ms4_mismatch";

    /** 每用例独立的管道目录。 */
    @TempDir(cleanup = CleanupMode.NEVER)
    Path pipeDir;

    /**
     * 每用例前清残留槽(幂等):上次异常退出留下的同名槽会让本用例的"以 two_phase=false
     * 建存量槽"夹具直接撞 42710 假失败。
     */
    @BeforeEach
    void cleanResidualSlot() {
        StreamPgTestEnv.dropSlotQuietly(SLOT);
    }

    /**
     * 每用例后清理:先停引擎(启动失败时为幂等 no-op)再删槽。
     */
    @AfterEach
    void dropSlot() {
        stopEngineAndDropSlot(SLOT);
    }

    /**
     * 场景:two_phase=false 存量槽 × two_phase=true 配置 → 启动期拒绝。关键步骤:SQL 直接
     * 建 two_phase=false 槽(第 4 参 false)→ 以 baseConfig(two_phase=true,on 档)预声明
     * 预期失败后 start(失败信号由基座 CompletionCallback 捕获)→ <b>硬断言①</b>失败信号
     * 到达:awaitEngine 等引擎停机(失败即返回;静默复用是 R5 修复前的 bug 形态,引擎
     * 长跑不停,60s 卡死探测超时 fail)且 engineFailure 非空 → <b>硬断言②</b>异常链渲染
     * 文本含槽名与 DROP SLOT 迁移指引 → 失败后槽仍在(拒绝零副作用)。
     * 边界:start() 异步起引擎即返回,失败发生于引擎线程(ensureSlot 预检),由 awaitEngine
     * 的完成闩而非同步返回值承载断言①;基座的 expectEngineFailure 标记使后续基座清理
     * (consumeRecords 快速通道/@AfterEach 兜底)对已捕获失败零连带抛出。
     */
    @Test
    void mismatchedExistingSlotFailsStartupWithMigrationHint() throws Exception {
        StreamPgTestEnv.execSql(
                "SELECT pg_create_logical_replication_slot('" + SLOT + "', 'pgoutput', false, false)");
        assertTrue(slotExists(SLOT), "夹具:存量槽应已建成");

        expectEngineFailure();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, PUB, pipeDir).with("slot.streaming", "on").build());
        awaitEngine();
        Throwable failure = engineFailure();
        assertNotNull(failure, "引擎必须以失败告终(失败信号经基座 CompletionCallback 捕获),不得成功运行");

        String chain = renderThrowableChain(null, failure);
        assertTrue(chain.contains(SLOT),
                "失败文案应含槽名(当前链: " + chain + ")");
        assertTrue(chain.contains("DROP SLOT"),
                "失败文案应含 DROP SLOT 迁移指引(当前链: " + chain + ")");
        assertTrue(slotExists(SLOT), "启动拒绝不得有删槽副作用");
    }

    /**
     * 槽是否存在于 pg_replication_slots 目录(副作用断言的观察面:拒绝路径不得删槽)。
     *
     * @param slotName 槽名
     * @return 目录中存在该槽为 true;查询失败抛 AssertionError(环境异常 fail-fast)
     */
    private static boolean slotExists(String slotName) {
        try (Connection c = StreamPgTestEnv.newSqlConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1) > 0;
            }
        }
        catch (SQLException e) {
            throw new AssertionError("pg_replication_slots 目录查询失败", e);
        }
    }
}
