package org.vastdata.debezium.connector.postgresql.stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import java.util.List;
import java.util.Map;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnector;

/**
 * 流式连接器的 Connect 入口:继承 {@link PostgresConnector} 复用其插槽管理、
 * 连接校验等生命周期骨架,仅做四处最小覆盖——任务类指向本模块的
 * {@link PostgresStreamConnectorTask}、版本/名称取自本模块 {@link Module}、
 * taskConfigs 默认值注入(MS5:snapshot.mode=no_data + provide.transaction.metadata=true)、
 * Connect REST 配置暴露面在父类 configDef() 之上补 6 个新配置项并覆盖两个
 * 默认值展示(snapshot.mode / provide.transaction.metadata)。
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
     * 覆盖字段校验驱动集:父类 {@code PostgresConnector.validateAllFields} 硬编码
     * <b>父</b>{@code PostgresConnectorConfig.ALL_FIELDS}(其 SNAPSHOT_MODE 接受
     * initial 等),REST {@code validate(Map)} 端点(Connect PUT 配置校验)经虚分派
     * 调本方法——不覆盖则 {@code snapshot.mode=initial} 在 REST 校验报零问题,首个
     * 拒收推迟到任务侧构造器,"校验/注入/构造器"三层防线的 REST 层落空。改用
     * {@link #getConfigFields()}(即 {@link PostgresStreamConnectorConfig#ALL_FIELDS},
     * snapshot.mode 为同名替换的仅-no_data Field)驱动,使 {@code validateSnapshotMode}
     * 在 REST 层真实生效。副作用面:父流程仅当全字段零问题才调 validateConnection
     * (连库),校验错误先行挡下时 REST 校验天然离线。
     *
     * @param config 待校验的完整配置(REST 请求原文构造)
     * @return 字段名 → ConfigValue(含 errorMessages)的校验结果
     */
    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(getConfigFields());
    }

    /**
     * 构造 Connect REST 暴露的配置定义:取父类静态 configDef() 的可变副本
     * (每次调用新建 ConfigDef,不改父类静态状态),再覆盖两个默认值展示
     * (snapshot.mode=no_data——默认值取 {@code SNAPSHOT_MODE_NO_DATA.defaultValue()}
     * 防与 Field 声明漂移;provide.transaction.metadata=true——展示值有意偏离父 Field
     * 默认 false(事务元数据默认开,与 {@link #taskConfigs(int)} 注入面一致),无本模块
     * Field 可源、只能字面量 TRUE)并 define 6 个新配置项——类型/默认值与
     * {@link PostgresStreamConnectorConfig} 的 Field 声明一一对应(默认值显式取
     * Field.defaultValue(),防两处字面量漂移),重要性/描述为暴露面专用文案。
     *
     * @return 含父类全部配置项(两处默认值覆盖)+ 6 个新配置项的 {@link ConfigDef}
     */
    @Override
    public ConfigDef config() {
        ConfigDef def = PostgresStreamConnectorConfig.configDef();
        // ConfigDef.define 对已定义名抛 "defined twice"(Connect 4.x),同名默认值覆盖
        // 须先从 configKeys 挖旧再补新——与 ALL_FIELDS 的 Field 同名替换同一坑形。
        def.configKeys().remove(PostgresStreamConnectorConfig.SNAPSHOT_MODE_NO_DATA.name());
        def.configKeys().remove(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA.name());
        def.define("snapshot.mode", ConfigDef.Type.STRING,
                PostgresStreamConnectorConfig.SNAPSHOT_MODE_NO_DATA.defaultValue(), ConfigDef.Importance.MEDIUM,
                "Streaming-only connector: snapshot.mode=no_data is the only supported value.");
        def.define("provide.transaction.metadata", ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.MEDIUM,
                "Streaming-only connector: transaction metadata (BEGIN/END transaction boundary markers) is enabled by default.");
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
     * 责任:任务配置的默认值注入——snapshot.mode 与 provide.transaction.metadata 缺省时
     * 显式置入本连接器默认(no_data / true)再委派父类。为什么不用 Field 默认值覆盖:父类
     * 静态 Field 的默认(initial / false)在运行期读取方(getSnapshotMode 等)经<b>父 Field
     * 引用</b>回落,子类同名字段替换不改变父引用的回落值;taskConfigs 是两框架
     * (Connect runtime 与 embedded engine——后者同样经 {@code SourceConnector.taskConfigs(int)}
     * 取任务配置)共同的配置必经点,在此注入使默认值对父类读取方同样生效。
     * 实现形态:委派父类 taskConfigs(int) 后对返回的每份任务配置 putIfAbsent——
     * 父实现把 start(Map) 存的 props 拷成新 HashMap 再返回,列表后处理安全。
     * 边界:用户显式配置的值原样透传(不覆盖——非法 snapshot.mode 的拒收责任在 REST
     * 校验与 {@link PostgresStreamConnectorConfig} 构造器 fail-fast,注入层不做静默改写);
     * 注入键恰两个,其余配置零触碰;start 未调用(props 为 null)时父实现返回空列表,
     * 注入零次。
     *
     * @param maxTasks Connect runtime 给出的最大任务数(单任务连接器,透传父类)
     * @return 注入默认值后的任务配置列表(每份均为父实现的新建可变 map)
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = super.taskConfigs(maxTasks);
        taskConfigs.forEach(taskConfig -> {
            taskConfig.putIfAbsent("snapshot.mode", "no_data");
            taskConfig.putIfAbsent("provide.transaction.metadata", "true");
        });
        return taskConfigs;
    }

    /**
     * 返回引擎侧读取的完整配置字段集(含父类字段与本模块 6 新项,snapshot.mode 为
     * 同名替换的仅-no_data Field),
     * 供 embedded engine / 校验框架做配置完整性检查。
     *
     * @return {@link PostgresStreamConnectorConfig#ALL_FIELDS}(Set 不可变,可安全共享)
     */
    @Override
    public Field.Set getConfigFields() {
        return PostgresStreamConnectorConfig.ALL_FIELDS;
    }
}
