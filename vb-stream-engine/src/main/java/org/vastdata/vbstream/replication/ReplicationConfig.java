package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.StreamingMode;

/**
 * 复制会话配置。默认值对准 src/docker 的 compose 环境（localhost:55432），
 * 全部可经 -Dvb.pg.* 系统属性覆盖。
 *
 * @param binary pgoutput 二进制值模式（PG 16+ 的 START_REPLICATION 参数，非槽属性）：true 时数据列以
 *               各类型 typsend 二进制表示走 TupleData 的 'b' 种类下发，false（默认）走 't' 文本；
 *               二进制表示不保证跨 PG 大版本稳定，开启即锚定服务端大版本
 */
public record ReplicationConfig(
        String host, int port, String database, String user, String password,
        String slotName, String publicationNames,
        int protoVersion, StreamingMode streamingMode, boolean twoPhase, boolean binary,
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
                StreamingMode.valueOf(prop("vb.pg.streaming", "parallel").toUpperCase(java.util.Locale.ROOT)),
                Boolean.parseBoolean(prop("vb.pg.twoPhase", "true")),
                Boolean.parseBoolean(prop("vb.pg.binary", "false")),
                Integer.parseInt(prop("vb.pg.feedbackSeconds", "10")));
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/%s".formatted(host, port, database);
    }

    /**
     * pgjdbc 复制连接要求 replication=database，且必须同时 assumeMinServerVersion&ge;9.4——
     * 否则驱动不把 replication 参数放进启动包（pgjdbc 文档规定），服务端按普通会话解析
     * START_REPLICATION 直接报语法错（真实 PG 18 集成首跑发现）。
     */
    public String replicationUrl() {
        return jdbcUrl() + "?replication=database&assumeMinServerVersion=9.4";
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
