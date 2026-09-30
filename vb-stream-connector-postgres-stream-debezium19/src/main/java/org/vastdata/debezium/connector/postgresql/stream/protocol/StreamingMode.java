package org.vastdata.debezium.connector.postgresql.stream.protocol;

/**
 * START_REPLICATION 的 streaming 参数档位——映射复制流启动选项，并决定解码器
 * 对 StreamAbort 附加字段的读取形态。引擎同名枚举的 1:1 重写。
 */
public enum StreamingMode {
    /** 不流式：大事务在提交后整体回放（Begin..Commit 传统路径）。 */
    OFF,
    /** 流式：进行中的大事务边收边发（流块内消息前置 xid）。 */
    ON,
    /** 流式 + 并行：StreamAbort 随消息携带 abortLsn/abortTimestamp 附加字段。 */
    PARALLEL
}
