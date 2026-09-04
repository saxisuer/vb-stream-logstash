package org.vastdata.debezium.connector.postgresql.stream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MS4 R5 真库验收:存量槽 two_phase 属性与配置不匹配时启动期拒绝(带 DROP SLOT 迁移
 * 指引)。单测({@code ReplicationSessionTest} 的目录脚本假件)锚定分支语义,本类补真库
 * 面——真 42710 路径 + {@code pg_replication_slots} 真目录行。场景:先以 two_phase=false
 * 建槽(模拟 MS3 之前的存量槽)→ 以 two_phase=true 配置启动引擎 → 启动失败且异常链文案
 * 含槽名与 DROP SLOT 指引 → 槽未被删(拒绝不得有副作用)。
 *
 * <p>失败上报面(基座原生路径,经源码核实):基座 {@code start(Class, Configuration,
 * CompletionCallback, Predicate)} 把自定义 CompletionCallback 包进 wrapper(失败时先回调
 * 后放行 latch)——引擎侧 execute 的 IllegalStateException 经 {@code errorHandler.
 * setProducerThrowable} 转 ConnectException 塞入 ChangeEventQueue,task.poll 抛出后
 * AsyncEmbeddedEngine 经 {@code callCompletionHandler} 回调 {@code handle(false, msg,
 * realError)},异常链的 cause 即 ISE 原文。taskStarted 与失败都可能晚于 start() 返回,
 * 故以 Awaitility 轮询回调信号而非假设同步。需要本机 Docker。
 */
class SlotTwoPhaseMismatchIT extends StreamITBase {

    /** 本测试类专用复制槽名。 */
    private static final String SLOT = "ms4_slot_mismatch";

    /** publication 名:ensureSlot 在建流之前即拒绝,publication 实际不被消费,占位即可。 */
    private static final String PUB = "pub_ms4_mismatch";

    /** 每用例独立的管道目录。 */
    @TempDir
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
     * 建 two_phase=false 槽(第 4 参 false)→ 以 baseConfig(two_phase=true,on 档)start 并
     * 注入捕获 CompletionCallback → <b>硬断言①</b>失败信号 60s 内到达(轮询回调置位;静默
     * 复用是 R5 修复前的 bug 形态,永不触发回调即超时 fail)→ <b>硬断言②</b>回调为失败且
     * 异常链渲染文本含槽名与 DROP SLOT 迁移指引 → 失败后槽仍在(拒绝零副作用)。
     * 边界:start() 返回仅代表 latch 放行(taskStarted 或失败先到者),失败回调可能更晚,
     * 故由 Awaitility 而非同步返回值承载断言①。
     */
    @Test
    void mismatchedExistingSlotFailsStartupWithMigrationHint() throws Exception {
        StreamPgTestEnv.execSql(
                "SELECT pg_create_logical_replication_slot('" + SLOT + "', 'pgoutput', false, false)");
        assertTrue(slotExists(SLOT), "夹具:存量槽应已建成");

        AtomicBoolean completionReached = new AtomicBoolean(false);
        AtomicBoolean succeeded = new AtomicBoolean(true);
        AtomicReference<String> message = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        start(PostgresStreamConnector.class,
                baseConfig(SLOT, PUB, pipeDir).with("slot.streaming", "on").build(),
                (success, msg, err) -> {
                    completionReached.set(true);
                    succeeded.set(success);
                    message.set(msg);
                    error.set(err);
                },
                null);

        await("two_phase 不匹配的存量槽必须启动失败(静默复用是 R5 修复前的 bug 形态)")
                .atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilTrue(completionReached);
        assertFalse(succeeded.get(), "引擎必须以失败告终,不得成功运行");
        String chain = renderThrowableChain(message.get(), error.get());
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

    /**
     * 渲染失败回调的完整异常链文本(回调 msg + 沿 cause 链逐层消息拼接):引擎侧失败被
     * 层层包装(ConnectException → IllegalStateException 原文),槽名与 DROP SLOT 指引
     * 在链尾——单层 getMessage 断言会漏,必须全链拼接。
     *
     * @param message 回调的消息参数(可能为 null)
     * @param error   回调的异常参数(可能为 null)
     * @return msg 与各层 cause 消息以 " | " 连接的文本(null 段跳过)
     */
    private static String renderThrowableChain(String message, Throwable error) {
        StringBuilder sb = new StringBuilder(String.valueOf(message));
        Throwable t = error;
        while (t != null) {
            sb.append(" | ").append(t.getMessage());
            t = t.getCause();
        }
        return sb.toString();
    }
}
