package org.vastdata.debezium.connector.postgresql.stream.it;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.vastdata.debezium.connector.postgresql.stream.PostgresStreamConnector;

import java.util.concurrent.TimeUnit;

import io.debezium.config.Configuration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 自建基座 {@link AbstractStreamIT} 的停机/失败通道契约测试(基座审查修复的钉子,
 * Task 9/10 大量 IT 依赖这两个语义):①<b>未 start 的用例零成本收敛</b>——
 * stopConnector 对从未启动的引擎是纯 no-op(不挂 60s 等 latch、不抛
 * AssertionError,3.6.1 基座同语义:teardown 对未启动引擎打日志即返回);
 * ②<b>失败路径可预期、可断言、可干净收敛</b>——{@code expectEngineFailure()} 置位后,
 * 失败信号经 CompletionCallback 捕获并经 {@code engineFailure()} 暴露给用例断言,
 * awaitEngine/assertNoEngineFailure/@AfterEach 不再因该<b>预期</b>失败连带抛出
 * (SlotTwoPhaseMismatchIT 一类"启动期拒绝"用例:用例断言通过后 teardown 必须绿)。
 * <p>零 Docker:场景②用不可达数据库端口(localhost:1 连接拒绝)驱动真引擎走
 * 完整失败路径(task.start 抛 → EmbeddedEngine 终止 → CompletionCallback(false))。
 */
class AbstractStreamITContractTest extends StreamITBase {

    /**
     * 契约①:不调 start 的用例,stopConnector 必须是快速 no-op。
     * 关键步骤:直接调 stopConnector(引擎从未 start)→ 断言不抛且耗时亚秒级。
     * 边界:@AfterEach 的兜底 afterEachStop 会再走一遍同路径(修复前该用例
     * 每处挂 60s 后抛 AssertionError)。
     */
    @Test
    @Timeout(30)
    void stopConnectorWithoutStartIsFastNoOp() {
        long startedAt = System.nanoTime();
        assertDoesNotThrow(this::stopConnector, "never-started 引擎的 stopConnector 不得抛错");
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        assertTrue(elapsedMs < 5_000,
                "never-started 引擎的 stopConnector 应为纯 no-op(实测 " + elapsedMs + "ms)");
    }

    /**
     * 契约②:expectEngineFailure 路径能拿到失败信号、断言后干净收敛。
     * 关键步骤:置预期失败标记 → 以不可达数据库(localhost:1)start 真引擎 →
     * awaitEngine(纯等待语义:run 失败退出后闩开,<b>不隐式抛失败</b>)→
     * engineFailure() 取 CompletionCallback 捕获的异常并断言非空、异常链可渲染 →
     * 显式 assertNoEngineFailure 不因预期失败抛出;@AfterEach 的 stopConnector
     * 同样干净(修复前 awaitEngine 尾部的失败断言使本用例与 teardown 双红)。
     * 边界:失败形态为 task.start 的连接拒绝(SQLException 链),真实转发路径
     * 经 EmbeddedEngine 的 CompletionCallback——与 Task 10 SlotTwoPhaseMismatchIT
     * 的启动期拒绝(ensureSlot 的 IllegalStateException 链)同通道。
     */
    @Test
    @Timeout(90)
    void expectEngineFailureCapturesSignalAndConvergesCleanly() throws InterruptedException {
        expectEngineFailure();
        start(PostgresStreamConnector.class, unreachableDbConfig());

        awaitEngine();
        Throwable failure = engineFailure();
        assertNotNull(failure, "失败信号应经 CompletionCallback 捕获并经 engineFailure() 暴露");
        String chain = renderThrowableChain(null, failure);
        assertFalse(chain.isBlank(), "捕获的异常链应可渲染出非空文本: " + failure);
        assertDoesNotThrow(this::assertNoEngineFailure, "预期失败豁免后显式断言不得抛出");
    }

    /**
     * 组装必失败的最小连接器配置:连接四件套指向 localhost:1(本机无监听,
     * TCP 立即拒绝——task.start 的 charset 探测连接在装配第一步即失败),
     * 逻辑名/槽/publication/快照档给齐使失败定位在连接而非配置缺失。
     *
     * @return 指向不可达数据库的连接器配置
     */
    private static Configuration unreachableDbConfig() {
        return Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", 1)
                .with("database.dbname", "testdb")
                .with("database.user", "test")
                .with("database.password", "test")
                .with("database.server.name", "itcontract")
                .with("slot.name", "itcontract_never_created")
                .with("publication.name", "pub_itcontract")
                .with("snapshot.mode", "never")
                .build();
    }
}
