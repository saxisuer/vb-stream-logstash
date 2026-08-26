package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.StreamingMode;

/**
 * 复制会话配置。默认值对准 src/docker 的 compose 环境（localhost:55432），
 * 全部可经 -Dvb.pg.* 系统属性覆盖。
 */
public record ReplicationConfig(
        String host, int port, String database, String user, String password,
        String slotName, String publicationNames,
        int protoVersion, StreamingMode streamingMode, boolean twoPhase,
        int feedbackIntervalSeconds) {

    public static ReplicationConfig fromSystemProperties() {
        return new ReplicationConfig(
                prop("vb.pg.host", "localhost"),
                Integer.parseInt(prop("vb.pg.port", "55432")),
                prop("vb.pg.database", "postgres"),
                prop("vb.pg.user", "postgres"),
                prop("vb.pg.password", "postgres"),
                prop("vb.pg.slot", "vb_cdc_slot"),
                prop("vb.pg.publication", "vb_pub"),
                Integer.parseInt(prop("vb.pg.protoVersion", "4")),
                StreamingMode.valueOf(prop("vb.pg.streaming", "parallel").toUpperCase()),
                Boolean.parseBoolean(prop("vb.pg.twoPhase", "true")),
                Integer.parseInt(prop("vb.pg.feedbackSeconds", "10")));
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/%s".formatted(host, port, database);
    }

    /** pgjdbc 复制连接要求 replication=database。 */
    public String replicationUrl() {
        return jdbcUrl() + "?replication=database";
    }

    /** START_REPLICATION 的 streaming 参数值。 */
    public String streamingParam() {
        return switch (streamingMode) {
            case OFF -> "off";
            case ON -> "on";
            case PARALLEL -> "parallel";
        };
    }

    private static String prop(String key, String defaultValue) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
