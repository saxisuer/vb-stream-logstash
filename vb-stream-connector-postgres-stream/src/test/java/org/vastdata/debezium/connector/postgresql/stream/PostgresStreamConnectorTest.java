package org.vastdata.debezium.connector.postgresql.stream;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PostgresStreamConnector} 连接器级单测:taskClass/version 的模块元数据正确性、
 * config() 暴露面(Connect REST 可见的 ConfigDef 含 4 个新 key 且默认值与 Field 声明一致)、
 * getConfigFields() 与 ALL_FIELDS 同源。纯方法级断言,不调 start(Map)——连接器生命周期
 * 留给 MS2 的 embedded engine 场景。
 */
class PostgresStreamConnectorTest {

    /**
     * 用例①taskClass 正确:Connect runtime 据此实例化任务类,指错类型即整条流水线起不来。
     */
    @Test
    void taskClassIsPostgresStreamConnectorTask() {
        assertEquals(PostgresStreamConnectorTask.class, new PostgresStreamConnector().taskClass(),
                "taskClass 应指向本连接器的任务实现");
    }

    /**
     * 用例②version 来自 Module:与插件元数据同源(而非父类 PG 连接器的版本号),
     * 避免运维面看到 debezium 版本误判插件行为。
     */
    @Test
    void versionComesFromModule() {
        assertEquals(Module.version(), new PostgresStreamConnector().version(),
                "version 应取自本模块 Module,而非继承的 PG 连接器版本");
    }

    /**
     * 用例③config() 暴露面:父类 configDef() 之上补 4 个新 key,ConfigDef 默认值与
     * 对应 Field.defaultValue() 一致(REST 推荐值与引擎实际读取面错位会造成"界面写 A、
     * 运行读 B"的隐性配置漂移);getConfigFields() 返回的 ALL_FIELDS 同含 4 个新名。
     */
    @Test
    void configDefExposesNewKeysWithFieldDefaults() {
        ConfigDef def = new PostgresStreamConnector().config();
        Map<String, Object> defaults = def.defaultValues();
        Set<String> expected = Set.of("slot.streaming", "slot.two.phase", "pipe.dir", "pipe.roll.cycle");
        assertTrue(defaults.keySet().containsAll(expected),
                "Connect REST 暴露的 ConfigDef 应含 4 个新 key");
        assertEquals(PostgresStreamConnectorConfig.SLOT_STREAMING.defaultValue(), defaults.get("slot.streaming"),
                "slot.streaming 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.SLOT_TWO_PHASE.defaultValue(), defaults.get("slot.two.phase"),
                "slot.two.phase 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.PIPE_DIR.defaultValue(), defaults.get("pipe.dir"),
                "pipe.dir 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.PIPE_ROLL_CYCLE.defaultValue(), defaults.get("pipe.roll.cycle"),
                "pipe.roll.cycle 的 ConfigDef 默认值应与 Field 声明一致");

        Set<String> fieldNames = new HashSet<>();
        for (io.debezium.config.Field field : new PostgresStreamConnector().getConfigFields()) {
            fieldNames.add(field.name());
        }
        assertTrue(fieldNames.containsAll(expected),
                "getConfigFields() 返回的 ALL_FIELDS 应含 4 个新配置名");
    }
}
