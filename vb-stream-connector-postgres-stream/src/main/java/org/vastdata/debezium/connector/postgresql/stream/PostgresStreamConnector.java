package org.vastdata.debezium.connector.postgresql.stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnector;

/**
 * 流式连接器的 Connect 入口:继承 {@link PostgresConnector} 复用其插槽管理、
 * 连接校验等生命周期骨架,仅做三处最小覆盖——任务类指向本模块的
 * {@link PostgresStreamConnectorTask}、版本/名称取自本模块 {@link Module}、
 * Connect REST 配置暴露面在父类 configDef() 之上补 5 个新配置项。
 *
 * <p>MS1 形态:仅元数据与配置面,不改变连接器启动/校验行为;
 * 流式源协调器的接入在 MS2 经任务的 start(Configuration) 完成。
 */
public class PostgresStreamConnector extends PostgresConnector {

    /**
     * 返回模块版本:覆盖父类以取本模块 {@link Module} 的版本号,
     * 避免运维面看到 io.debezium 的版本误判插件行为。
     *
     * @return {@link Module#version()},永不抛错
     */
    @Override
    public String version() {
        return Module.version();
    }

    /**
     * 返回任务实现类:Connect runtime 据此实例化每个任务实例。
     *
     * @return 恒为 {@link PostgresStreamConnectorTask}
     */
    @Override
    public Class<? extends Task> taskClass() {
        return PostgresStreamConnectorTask.class;
    }

    /**
     * 构造 Connect REST 暴露的配置定义:取父类静态 configDef() 的可变副本
     * (每次调用新建 ConfigDef,不改父类静态状态),再 define 6 个新配置项
     * ——类型/默认值与 {@link PostgresStreamConnectorConfig} 的 Field 声明一一对应
     * (默认值显式取 Field.defaultValue(),防两处字面量漂移),重要性/描述为暴露面专用文案。
     *
     * @return 含父类全部配置项 + 6 个新配置项的 {@link ConfigDef}
     */
    @Override
    public ConfigDef config() {
        ConfigDef def = PostgresStreamConnectorConfig.configDef();
        def.define(PostgresStreamConnectorConfig.SLOT_STREAMING.name(), ConfigDef.Type.STRING,
                PostgresStreamConnectorConfig.SLOT_STREAMING.defaultValue(), ConfigDef.Importance.LOW,
                "Streaming mode for in-progress large transactions: 'off' (replay after commit), 'on' (stream while running) or 'parallel' (streaming with parallel apply; requires slot.two.phase=true). Case-insensitive.");
        def.define(PostgresStreamConnectorConfig.SLOT_TWO_PHASE.name(), ConfigDef.Type.BOOLEAN,
                PostgresStreamConnectorConfig.SLOT_TWO_PHASE.defaultValue(), ConfigDef.Importance.LOW,
                "Whether the replication slot is created with two-phase commit support (required for slot.streaming=parallel).");
        def.define(PostgresStreamConnectorConfig.PIPE_DIR.name(), ConfigDef.Type.STRING,
                PostgresStreamConnectorConfig.PIPE_DIR.defaultValue(), ConfigDef.Importance.LOW,
                "Directory of the Chronicle Queue pipe buffering raw messages between the reader and consumer threads (transient workspace, wiped on restart).");
        def.define(PostgresStreamConnectorConfig.PIPE_ROLL_CYCLE.name(), ConfigDef.Type.STRING,
                PostgresStreamConnectorConfig.PIPE_ROLL_CYCLE.defaultValue(), ConfigDef.Importance.LOW,
                "Roll cycle of the Chronicle Queue pipe (LegacyRollCycles enum name, case-insensitive).");
        def.define(PostgresStreamConnectorConfig.SLOT_FEEDBACK_INTERVAL_MS.name(), ConfigDef.Type.INT,
                PostgresStreamConnectorConfig.SLOT_FEEDBACK_INTERVAL_MS.defaultValue(), ConfigDef.Importance.LOW,
                "Interval in milliseconds between LSN status feedback to the server (the confirmed LSN is capped at the output frontier so unoutput transactions are resent after a crash). Values are truncated to whole seconds.");
        def.define(PostgresStreamConnectorConfig.SLOT_MESSAGES.name(), ConfigDef.Type.BOOLEAN,
                PostgresStreamConnectorConfig.SLOT_MESSAGES.defaultValue(), ConfigDef.Importance.LOW,
                "Whether to request logical messages ('M') from the server by adding messages=true to the slot options (PostgreSQL 14+). When enabled, logical messages are parsed and logged, and non-transactional ones safely advance the output frontier; they are never emitted to topics.");
        return def;
    }

    /**
     * 返回引擎侧读取的完整配置字段集(含父类字段与本模块 6 新项),
     * 供 embedded engine / 校验框架做配置完整性检查。
     *
     * @return {@link PostgresStreamConnectorConfig#ALL_FIELDS}(Set 不可变,可安全共享)
     */
    @Override
    public Field.Set getConfigFields() {
        return PostgresStreamConnectorConfig.ALL_FIELDS;
    }
}
