package org.vastdata.debezium.connector.postgresql.stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

import org.vastdata.debezium.connector.postgresql.PostgresConnectorConfig;
import org.vastdata.debezium.connector.postgresql.connection.PostgresConnection;

/**
 * 流式连接器的 Connect 入口:直接继承 debezium-core 的
 * {@link RelationalBaseSourceConnector} 自有装配生命周期骨架(vanilla 1.9.7
 * {@code PostgresConnector} 的成员面照搬——start/stop 的 props 持有、taskConfigs 的单任务
 * 拷贝、config()/validateConnection/validateAllFields 八成员),不引入 vanilla 根包连接器;
 * 任务类指向本模块的 {@link PostgresStreamConnectorTask}、版本/名称取自本模块 {@link Module}、
 * taskConfigs 默认值注入(never + provide.transaction.metadata=true)、
 * Connect REST 配置暴露面在自有 {@link PostgresStreamConnectorConfig#configDef()} 之上补 6 个
 * 新配置项并覆盖两个默认值展示(snapshot.mode / provide.transaction.metadata)。
 *
 * <p>1.9.7 形态注记:本版连接器基类(RelationalBaseSourceConnector)无 3.x 的
 * bean/DebeziumHeaderProducer 校验段,validate 继承面即父类原生流程——仅当连接相关五字段
 * (server.name/hostname/port/user/password)零问题才调 {@link #validateConnection(Map, Configuration)}
 * (连库探测 wal_level/角色);{@link #getConfigFields()} 在本版无基类钩子,作为模块自备的
 * 配置面单一来源保留。适配一处:{@code validateConnection} 日志里的连接串由 vanilla
 * {@code PostgresConnection.connectionString()}(Task 2 裁剪版已删)改为按 URL 模板
 * {@code jdbc:postgresql://host:port/db} 从配置内联拼装(仅日志文案,探测语义零改动)。
 */
public class PostgresStreamConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamConnector.class);

    private Map<String, String> props;

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
     * 责任:接收并持有 Connect runtime 下发的连接器级配置(vanilla {@code PostgresConnector.start}
     * 同款形态)——真正的资源建立推迟到任务侧 {@code PostgresStreamConnectorTask.start(Configuration)},
     * 连接器实例本身无状态可建。幂等:重复 start 覆盖旧 props。
     *
     * @param props 连接器配置原文(键值对)
     */
    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    /**
     * 覆盖字段校验驱动集:1.9.7 vanilla 父类 {@code PostgresConnector.validateAllFields}硬编码
     * <b>父</b>{@code PostgresConnectorConfig.ALL_FIELDS}(其 SNAPSHOT_MODE 接受
     * initial 等),REST {@code validate(Map)} 端点(Connect PUT 配置校验)经虚分派
     * 调本方法——不覆盖则 {@code snapshot.mode=initial} 在 REST 校验报零问题,首个
     * 拒收推迟到任务侧构造器,"校验/注入/构造器"三层防线的 REST 层落空。改用
     * {@link #getConfigFields()}(即 {@link PostgresStreamConnectorConfig#ALL_FIELDS},
     * snapshot.mode 为同名替换的仅-never Field)驱动,使 {@code validateSnapshotMode}
     * 在 REST 层真实生效。副作用面:1.9.7 父流程(RelationalBaseSourceConnector.
     * validate)仅当连接相关字段(server.name/hostname/port/user/password)零问题才调
     * validateConnection(连库)——任一必填连接字段缺失或非法时 REST 校验天然离线,
     * 与快照档错误本身无关。
     *
     * @param config 待校验的完整配置(REST 请求原文构造)
     * @return 字段名 → ConfigValue(含 errorMessages)的校验结果
     */
    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(getConfigFields());
    }

    /**
     * 构造 Connect REST 暴露的配置定义:取 {@link PostgresStreamConnectorConfig#configDef()} 的
     * 可变副本(每次调用新建 ConfigDef,不改静态状态),再覆盖两个默认值展示
     * (snapshot.mode=never——默认值取 {@code SNAPSHOT_MODE_NEVER.defaultValue()}
     * 防与 Field 声明漂移;provide.transaction.metadata=true——展示值有意偏离父 Field
     * 默认 false(1.9.7 同 3.6.1;事务元数据默认开,与 {@link #taskConfigs(int)} 注入面
     * 一致),无本模块 Field 可源、只能字面量 TRUE)并 define 6 个新配置项——类型/默认值与
     * {@link PostgresStreamConnectorConfig} 的 Field 声明一一对应(默认值显式取
     * Field.defaultValue(),防两处字面量漂移),重要性/描述为暴露面专用文案。
     *
     * @return 含父类全部配置项(两处默认值覆盖)+ 6 个新配置项的 {@link ConfigDef}
     */
    @Override
    public ConfigDef config() {
        ConfigDef def = PostgresStreamConnectorConfig.configDef();
        // ConfigDef.define 对已定义名抛 "defined twice",同名默认值覆盖
        // 须先从 configKeys 挖旧再补新——与 ALL_FIELDS 的 Field 同名替换同一坑形。
        def.configKeys().remove(PostgresStreamConnectorConfig.SNAPSHOT_MODE_NEVER.name());
        def.configKeys().remove(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA.name());
        def.define("snapshot.mode", ConfigDef.Type.STRING,
                PostgresStreamConnectorConfig.SNAPSHOT_MODE_NEVER.defaultValue(), ConfigDef.Importance.MEDIUM,
                "Streaming-only connector: snapshot.mode=never is the only supported value.");
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
     * 显式置入本连接器默认(never / true)再委派。为什么不用 Field 默认值覆盖:父类
     * 静态 Field 的默认(initial / false)在运行期读取方(snapshotMode 等)经<b>父 Field
     * 引用</b>回落,子类同名字段替换不改变父引用的回落值;taskConfigs 是两框架
     * (Connect runtime 与 embedded engine——后者同样经 {@code SourceConnector.taskConfigs(int)}
     * 取任务配置)共同的配置必经点,在此注入使默认值对父类读取方同样生效。
     * 实现形态:基础任务配置列表(1.9.7 vanilla {@code PostgresConnector.taskConfigs} 同款——
     * 把 start(Map) 存的 props 拷成新 HashMap 单元素列表;start 未调用时为空列表)后对每份
     * 任务配置 putIfAbsent——列表后处理安全。
     * 边界:用户显式配置的值原样透传(不覆盖——非法 snapshot.mode 的拒收责任在 REST
     * 校验与 {@link PostgresStreamConnectorConfig} 构造器 fail-fast,注入层不做静默改写);
     * 注入键恰两个,其余配置零触碰。
     *
     * @param maxTasks Connect runtime 给出的最大任务数(单任务连接器,透传)
     * @return 注入默认值后的任务配置列表(每份均为新建可变 map)
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // this will always have just one task with the given list of properties
        List<Map<String, String>> taskConfigs = props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
        taskConfigs.forEach(taskConfig -> {
            taskConfig.putIfAbsent("snapshot.mode", "never");
            taskConfig.putIfAbsent("provide.transaction.metadata", "true");
        });
        return taskConfigs;
    }

    /**
     * 责任:连接器级停机——丢弃 start 持有的 props(vanilla {@code PostgresConnector.stop}
     * 同款);任务实例与复制槽等资源的收敛由各任务自身的 stop 链路负责,连接器层无持有资源。
     * 幂等:重复 stop 安全(props 置 null 后 taskConfigs 返回空列表)。
     */
    @Override
    public void stop() {
        this.props = null;
    }

    /**
     * 责任:REST {@code validate(Map)} 端点的连库探测段(仅在连接相关五字段零问题时被
     * 父类流程调用)——建校验专用连接、执行探测 SQL、检查服务端 wal_level=logical 与用户
     * LOGIN/REPLICATION 角色(含 AWS 角色组等价判定),失败消息落在 hostname 字段的
     * ConfigValue 上(vanilla {@code PostgresConnector.validateConnection} 同款形态)。
     * 关键步骤:database/slot/plugin 三字段任一有错即返回(离线门槛);连接建立失败/SQL
     * 失败仅记 hostname 错误消息不抛出(校验结果聚合留给框架)。
     * 适配一处:日志用的连接串由 {@code PostgresConnection.connectionString()}(裁剪版已删)
     * 改为按 URL 模板 {@code jdbc:postgresql://host:port/db} 从配置内联拼装——仅日志文案。
     * 边界:wal_level 非 logical 记 hostname 错误;角色缺失仅 ERROR 日志(vanilla 同款,
     * 不落 ConfigValue)。
     *
     * @param configValues 字段校验结果(字段名 → ConfigValue,含 errorMessages)
     * @param config 待校验的完整配置
     */
    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        final ConfigValue slotNameValue = configValues.get(PostgresConnectorConfig.SLOT_NAME.name());
        final ConfigValue pluginNameValue = configValues.get(PostgresConnectorConfig.PLUGIN_NAME.name());
        if (!databaseValue.errorMessages().isEmpty() || !slotNameValue.errorMessages().isEmpty()
                || !pluginNameValue.errorMessages().isEmpty()) {
            return;
        }

        final PostgresConnectorConfig postgresConfig = new PostgresConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        final String connectionString = "jdbc:postgresql://" + config.getString(RelationalDatabaseConnectorConfig.HOSTNAME)
                + ":" + config.getString(RelationalDatabaseConnectorConfig.PORT)
                + "/" + config.getString(RelationalDatabaseConnectorConfig.DATABASE_NAME);
        // Try to connect to the database ...
        try (PostgresConnection connection = new PostgresConnection(postgresConfig.getJdbcConfig(), PostgresConnection.CONNECTION_VALIDATE_CONNECTION)) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                // check connection
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'", connectionString,
                        connection.username());
                // check server wal_level
                final String walLevel = connection.queryAndMap(
                        "SHOW wal_level",
                        connection.singleResultMapper(rs -> rs.getString("wal_level"), "Could not fetch wal_level"));
                if (!"logical".equals(walLevel)) {
                    final String errorMessage = "Postgres server wal_level property must be \"logical\" but is: " + walLevel;
                    LOGGER.error(errorMessage);
                    hostnameValue.addErrorMessage(errorMessage);
                }
                // check user for LOGIN and REPLICATION roles
                if (!connection.queryAndMap(
                        "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication," +
                        // for AWS the user might not have directly the rolreplication rights, but can be assigned
                        // to one of those role groups: rds_superuser, rdsadmin or rdsrepladmin
                                " CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser" +
                                ", CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin" +
                                ", CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin" +
                                ", CAST(array_position(ARRAY(SELECT b.rolname" +
                                " FROM pg_catalog.pg_auth_members m" +
                                " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                                " WHERE m.member = r.oid), 'rds_replication') AS BOOL) IS TRUE AS aws_replication" +
                                " FROM pg_roles r WHERE r.rolname = current_user",
                        connection.singleResultMapper(rs -> rs.getBoolean("rolcanlogin")
                                && (rs.getBoolean("rolreplication")
                                        || rs.getBoolean("aws_superuser")
                                        || rs.getBoolean("aws_admin")
                                        || rs.getBoolean("aws_repladmin")
                                        || rs.getBoolean("aws_replication")),
                                "Could not fetch roles"))) {
                    final String errorMessage = "Postgres roles LOGIN and REPLICATION are not assigned to user: " + connection.username();
                    LOGGER.error(errorMessage);
                }
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'", connectionString,
                        connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
            }
        }
    }

    /**
     * 返回引擎侧读取的完整配置字段集(含父类字段与本模块 6 新项,snapshot.mode 为
     * 同名替换的仅-never Field):1.9.7 连接器基类无 3.x BaseSourceConnector 的
     * getConfigFields 钩子(故无 {@code @Override}),本方法是模块自备的配置面单一
     * 来源——{@link #validateAllFields} 的校验驱动集与本模块测试的配置完整性断言
     * 均取此集合,防校验面与引擎读取面漂移。
     *
     * @return {@link PostgresStreamConnectorConfig#ALL_FIELDS}(Set 不可变,可安全共享)
     */
    public Field.Set getConfigFields() {
        return PostgresStreamConnectorConfig.ALL_FIELDS;
    }
}
