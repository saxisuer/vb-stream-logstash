package org.vastdata.debezium.connector.postgresql.stream.protocol;

/**
 * TRUNCATE 消息的选项位——一条 TRUNCATE 语句的修饰符集合（EnumSet 打包进消息）。
 * 引擎同名枚举的 1:1 重写。
 */
public enum TruncateOption {
    /** 级联截断外键引用的表（协议选项位 bit0，数值 1）。 */
    CASCADE,
    /** 重启被截断表的序列（协议选项位 bit1，数值 2）。 */
    RESTART_IDENTITY
}
