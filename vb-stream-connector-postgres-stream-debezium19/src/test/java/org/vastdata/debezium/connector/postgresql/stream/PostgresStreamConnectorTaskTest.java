package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.Field;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link PostgresStreamConnectorTask} 装配面单测(Task 7,1.9.7 形态):元数据方法
 * (version/getAllConfigurationFields)直取 Module 与 ALL_FIELDS、doStop 在未 start 的
 * 零装配状态下安全收敛(字段 null 跳过契约)。刻意不调 start(Configuration)——真装配
 * 首步即连库取 charset,离线单测不具备;全链路生命周期留 Task 8 起的 embedded engine IT。
 * 3.6.1 版用例的取舍:preStart 用例整体删除(1.9.7 的 BaseSourceTask 无此钩子——任务
 * 上下文改在 start(Configuration) 内部建立,无独立可离线断言的构造面);connectorName
 * 断言删除(1.9.7 基类无抽象 connectorName(),连接器名经 CdcSourceTaskContext 构造参
 * 流入 MDC/指标,不再有独立可覆写的访问器);doStop 零装配契约用例新增补位——它是
 * 启动中途失败(装配抛错后 Connect runtime 仍会调 stop)的唯一防护面。
 */
class PostgresStreamConnectorTaskTest {

    /**
     * 用例①元数据:version 直取 Module 常量——日志与指标上下文按此归因,指向父类 PG
     * 连接器会污染运维面的版本判断(kafka 3.1.0 的 Task.version() 是抽象方法,必须实现)。
     */
    @Test
    void versionReturnsModuleVersion() {
        PostgresStreamConnectorTask task = new PostgresStreamConnectorTask();
        assertEquals(Module.version(), task.version(), "version 应为本模块 Module 版本");
    }

    /**
     * 用例②配置面:getAllConfigurationFields 返回 PostgresStreamConnectorConfig.ALL_FIELDS
     * (同一实例)——基类用它做配置完整性校验,换成父类集合会漏掉本模块新配置项
     * (slot.streaming 等 7 项)的校验。
     */
    @Test
    void allConfigurationFieldsMatchConfigAllFields() {
        Iterable<Field> expected = PostgresStreamConnectorConfig.ALL_FIELDS;
        Iterable<Field> actual = new PostgresStreamConnectorTask().getAllConfigurationFields();
        assertEquals(expected, actual, "任务的配置面应与 PostgresStreamConnectorConfig.ALL_FIELDS 同源");
    }

    /**
     * 用例③doStop 零装配契约:未 start 的任务直接 doStop 不抛异常——1.9.7 的
     * BaseSourceTask.stop() 对任何状态都会调 doStop(启动失败路径含在内),装配字段
     * (queue/jdbcConnection/schema)全 null 时的安全跳过是任务停机次序的边界保证。
     */
    @Test
    void doStopWithoutStartIsSafe() {
        PostgresStreamConnectorTask task = new PostgresStreamConnectorTask();
        assertDoesNotThrow(task::doStop, "零装配状态(未 start)的 doStop 应安全跳过全部字段");
    }
}
