package org.vastdata.debezium.connector.postgresql.stream;

/**
 * 连接器模块元数据:版本号与标识名,供 Connector.version() 与日志/指标上下文引用。
 * 形态对齐 io.debezium.connector.postgresql.Module(常量直读,不经 build.version 资源加载——
 * 本模块暂无打包资源注入环节,MS6 打包时若引入再切换)。
 */
public final class Module {

    /** 模块版本,随仓库 1.0-SNAPSHOT。 */
    public static final String VERSION = "1.0-SNAPSHOT";

    /** 连接器逻辑名(日志与配置校验上下文用),与 Debezium PG 的 "postgresql" 区分。 */
    public static final String NAME = "postgresql-stream";

    /** 指标/日志上下文名(Debezium CdcSourceTaskContext 语境)。 */
    public static final String CONTEXT_NAME = "PostgresStream";

    private Module() {
        // 常量类不可实例化
    }

    /**
     * 返回模块版本号。
     *
     * @return 常量 VERSION,永不抛错
     */
    public static String version() {
        return VERSION;
    }
}
