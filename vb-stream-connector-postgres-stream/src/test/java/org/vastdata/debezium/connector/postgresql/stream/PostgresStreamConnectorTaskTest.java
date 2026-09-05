package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.Configuration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link PostgresStreamConnectorTask} 骨架单测:元数据方法(version/connectorName/
 * getAllConfigurationFields)直取 Module 与 ALL_FIELDS、preStart 在最小配置下返回携带
 * 正确 config 类型的非 null 上下文。刻意不调 start(Map)——MS1 任务骨架不连库,
 * 全链路生命周期留 MS2 embedded engine;基类 start(Map) 会立即解引用 preStart 返回值,
 * 故 preStart 的非 null 契约必须在此先行锁定。
 */
class PostgresStreamConnectorTaskTest {

    /**
     * 构造最小可用的 PG 连接配置:Debezium 必填四件套(hostname/port/user/database,
     * 缺省 localhost/5432/postgres/postgres)+ snapshot.mode=no_data(镜像 taskConfigs
     * 注入后的直接构造最小合法面——MS5 起构造器对非 no_data 快照模式 fail-fast)。
     * 任务骨架不连库,这些值只用于走通 {@code PostgresStreamConnectorConfig} 构造链。
     *
     * @return 可交给 preStart 的最小 {@link Configuration}
     */
    private static Configuration minimalConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "no_data");
        return Configuration.from(props);
    }

    /**
     * 用例①元数据:version/connectorName 直取 Module 常量——日志与指标上下文按此归因,
     * 指向父类 PG 连接器会污染运维面的版本与命名。
     */
    @Test
    void metadataMethodsReturnModuleConstants() {
        PostgresStreamConnectorTask task = new PostgresStreamConnectorTask();
        assertEquals(Module.NAME, task.connectorName(), "connectorName 应为本模块 Module.NAME");
        assertEquals(Module.version(), task.version(), "version 应为本模块 Module 版本");
    }

    /**
     * 用例②配置面:getAllConfigurationFields 返回 PostgresStreamConnectorConfig.ALL_FIELDS
     * (同一实例)——基类用它做配置完整性校验,换成父类集合会漏掉 4 个新配置项的校验。
     */
    @Test
    void allConfigurationFieldsMatchConfigAllFields() {
        assertEquals(PostgresStreamConnectorConfig.ALL_FIELDS, new PostgresStreamConnectorTask().getAllConfigurationFields(),
                "任务的配置面应与 PostgresStreamConnectorConfig.ALL_FIELDS 同源");
    }

    /**
     * 用例③preStart 契约:最小配置下返回非 null 的 CdcSourceTaskContext(基类 start(Map)
     * 立即解引用,返回 null 即必崩),且其 getConfig() 是 PostgresStreamConnectorConfig
     * (MS2 的快照/offset 体系将按该类型取用流式配置项)。
     */
    @Test
    void preStartBuildsNonNullTaskContext() {
        var context = new PostgresStreamConnectorTask().preStart(minimalConfig());
        assertNotNull(context, "preStart 返回 null 会让基类 start(Map) 立即 NPE");
        assertInstanceOf(PostgresStreamConnectorConfig.class, context.getConfig(),
                "任务上下文应携带 PostgresStreamConnectorConfig(MS2 消费流式配置项)");
    }
}
