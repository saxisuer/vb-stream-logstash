package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.Field;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PostgresStreamConnector} 连接器级单测:taskClass/version 的模块元数据正确性、
 * config() 暴露面(Connect REST 可见的 ConfigDef 含 5 个新 key 且默认值与 Field 声明一致)、
 * getConfigFields() 与 ALL_FIELDS 同源(1.9.7 连接器基类无 getConfigFields 钩子,
 * 本方法是模块自备的配置面单一来源)。纯方法级断言,不调 start(Map) 以外的生命周期
 * (taskConfigs 用例经 start 注入 props)——引擎行为留给后续任务的 embedded engine 场景。
 */
class PostgresStreamConnectorTest {

    /**
     * 用例①taskClass 正确:Connect runtime 据此实例化任务类,指错类型即整条流水线起不来。
     * 任务类自 Task 7 起为 1.9.7 全装配形态(基类 BaseSourceTask 的真实现),类引用
     * 一致性锚定不变。
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
     * 用例③config() 暴露面:父类 configDef() 之上补 5 个新 key,ConfigDef 默认值与
     * 对应 Field.defaultValue() 一致(REST 推荐值与引擎实际读取面错位会造成"界面写 A、
     * 运行读 B"的隐性配置漂移);getConfigFields() 返回的 ALL_FIELDS 同含 5 个新名。
     */
    @Test
    void configDefExposesNewKeysWithFieldDefaults() {
        ConfigDef def = new PostgresStreamConnector().config();
        Map<String, Object> defaults = def.defaultValues();
        Set<String> expected = Set.of("slot.streaming", "slot.two.phase", "pipe.dir", "pipe.roll.cycle",
                "slot.feedback.interval.ms");
        assertTrue(defaults.keySet().containsAll(expected),
                "Connect REST 暴露的 ConfigDef 应含 5 个新 key");
        assertEquals(PostgresStreamConnectorConfig.SLOT_STREAMING.defaultValue(), defaults.get("slot.streaming"),
                "slot.streaming 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.SLOT_TWO_PHASE.defaultValue(), defaults.get("slot.two.phase"),
                "slot.two.phase 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.PIPE_DIR.defaultValue(), defaults.get("pipe.dir"),
                "pipe.dir 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.PIPE_ROLL_CYCLE.defaultValue(), defaults.get("pipe.roll.cycle"),
                "pipe.roll.cycle 的 ConfigDef 默认值应与 Field 声明一致");
        assertEquals(PostgresStreamConnectorConfig.SLOT_FEEDBACK_INTERVAL_MS.defaultValue(), defaults.get("slot.feedback.interval.ms"),
                "slot.feedback.interval.ms 的 ConfigDef 默认值应与 Field 声明一致");

        Set<String> fieldNames = new HashSet<>();
        for (Field field : new PostgresStreamConnector().getConfigFields()) {
            fieldNames.add(field.name());
        }
        assertTrue(fieldNames.containsAll(expected),
                "getConfigFields() 返回的 ALL_FIELDS 应含 5 个新配置名");
    }

    /**
     * 用例④taskConfigs 默认注入:缺省的 snapshot.mode 与 provide.transaction.metadata
     * 被置入本连接器默认(never / true),用户显式配置的值原样透传不被覆盖——
     * putIfAbsent 语义;显式值的合法性拒收责任在 REST 校验与构造器 fail-fast,
     * 注入层不做静默改写(改写会让"配置了 A、运行了 B"的漂移难排查)。
     */
    @Test
    void taskConfigsInjectsDefaultsWithoutOverridingExplicitValues() {
        PostgresStreamConnector connector = new PostgresStreamConnector();
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("provide.transaction.metadata", "false");
        connector.start(props);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size(), "单任务连接器应返回 1 份任务配置");
        Map<String, String> taskConfig = taskConfigs.get(0);
        assertEquals("never", taskConfig.get("snapshot.mode"),
                "缺省 snapshot.mode 应注入 never");
        assertEquals("false", taskConfig.get("provide.transaction.metadata"),
                "显式 provide.transaction.metadata=false 不应被默认 true 覆盖");
    }

    /**
     * 用例⑤事务元数据默认开:缺省的 provide.transaction.metadata 经 taskConfigs 注入为
     * true(MS5 语义——流式连接器默认发 BEGIN/END 事务边界标记,下游可按事务分组);
     * 同时显式 snapshot.mode=initial 原样透传(不做静默改写,拒收在防线后置层)。
     */
    @Test
    void transactionMetadataDefaultsToTrueViaInjection() {
        PostgresStreamConnector connector = new PostgresStreamConnector();
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "initial");
        connector.start(props);
        Map<String, String> taskConfig = connector.taskConfigs(1).get(0);
        assertEquals("true", taskConfig.get("provide.transaction.metadata"),
                "缺省 provide.transaction.metadata 应注入 true(事务元数据默认开)");
        assertEquals("initial", taskConfig.get("snapshot.mode"),
                "显式 snapshot.mode 应原样透传(拒收责任在 REST 校验/构造器 fail-fast)");
    }

    /**
     * 用例⑥config() 展示面覆盖默认:snapshot.mode 的 ConfigDef 默认值改写为 never、
     * provide.transaction.metadata 改写为 true——REST 推荐值与本连接器实际支持面
     * (仅 never)与默认行为(事务元数据开)一致,防"界面推荐 initial、运行必拒"的
     * 配置面裂缝(父 configDef 的默认 initial/false 不再适用)。
     */
    @Test
    void configDefOverridesSnapshotModeAndTransactionMetadataDefaults() {
        ConfigDef def = new PostgresStreamConnector().config();
        assertEquals("never", def.defaultValues().get("snapshot.mode"),
                "REST 暴露面 snapshot.mode 默认值应展示 never(唯一支持值)");
        assertEquals(Boolean.TRUE, def.defaultValues().get("provide.transaction.metadata"),
                "REST 暴露面 provide.transaction.metadata 默认值应展示 true(事务元数据默认开)");
    }

    /**
     * 用例⑦REST validate() 端到端走本连接器校验器:snapshot.mode=initial 经
     * {@code validate(Map)}(Connect REST PUT /connector 配置校验的同路径)须在
     * snapshot.mode 的 ConfigValue 上产出非零 errorMessages——1.9.7 父类
     * {@code PostgresConnector.validateAllFields} 硬编码父 ALL_FIELDS(父 SNAPSHOT_MODE
     * 接受 initial),不覆盖则 REST 校验报零问题、首个拒收推迟到任务侧构造器,"三层防线"
     * 的 REST 层落空。离线性:1.9.7 的 RelationalBaseSourceConnector.validate 仅当
     * 连接相关字段(server.name/hostname/port/user/password)零问题才调 validateConnection
     * (连库),本用例缺 database.server.name(required 错误先行挡下),故天然离线可测。
     */
    @Test
    void restValidateRejectsInitialSnapshotMode() {
        PostgresStreamConnector connector = new PostgresStreamConnector();
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "initial");
        Config config = connector.validate(props);
        ConfigValue snapshotMode = config.configValues().stream()
                .filter(value -> "snapshot.mode".equals(value.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("validate 结果应含 snapshot.mode 的 ConfigValue"));
        assertFalse(snapshotMode.errorMessages().isEmpty(),
                "REST validate() 应经本连接器 ALL_FIELDS 驱动校验,对 initial 产出非零 problems");
    }
}
