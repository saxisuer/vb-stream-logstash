package org.vastdata.debezium.connector.postgresql.stream;

/**
 * 元组值 → Java 值的类型化转换接缝(MS2 设计新增,引擎无对应物):pgoutput 文本形态的列值
 * 是按列类型编码的字符串("t"/"f"、十六进制 bytea、微秒时间戳……),转成 Connect schema
 * 期望的 Java 类型(Integer/Boolean/BigDecimal/Instant/byte[])需要 per-type 解析。
 * 该解析在 Debezium 侧由 {@code PgOutputReplicationMessage.getValue} +
 * {@code ReplicationMessageColumnValueResolver} 承担,但其入口签名要真实
 * {@code PostgresType}/{@code TypeRegistry} 对象(仅能从连库的注册表构造)——本接口把
 * "typeId/typeExpression + 原始字符串 → Java 值"收窄成可注入的纯函数,使
 * {@link RowChangeEmitter} 的结构行为(取舍/恢复/位序)可离线单测,而类型化本体归
 * 生产实现 {@link TypeRegistryColumnValueMapper}(真库验证归 Task 8 IT)。
 *
 * <p>方法面刻意两枚:Text 与 UnchangedToast 的哨兵(Binary/Null 由 emitter 直接处理,
 * 无类型参与)。参数带 {@code typeExpression}(如 {@code numeric(10,2)})是因为 TOAST
 * 哨兵的类型分派(数组/hstore/uuid 各有专属标记对象)依赖带修饰的类型名。
 *
 * <p>线程约束:实现应为无状态或显式声明单写者;生产实现无状态,任意线程可调
 * (实际仅 consumer 线程,见 R1 账本)。
 */
public interface ColumnValueMapper {

    /**
     * 责任:把文本形态的列值转成 Connect schema 期望的 Java 值。
     * 边界与异常语义:按实现方声明——生产实现按 vanilla 解析器语义(NumberFormatException
     * 等)原样上抛,由消费路径 fail-fast 承担;rawValue 为 null 不进入本方法
     * (Null 形态由 emitter 直接置 null)。
     *
     * @param columnName     wire 列名(诊断归因)
     * @param typeId         wire 列类型 oid(类型分派的真源)
     * @param typeExpression 带修饰的类型表达式(如 {@code varchar(10)}——部分类型解析需要)
     * @param rawValue       文本形态原值('t' 种类字节的载荷)
     * @return 类型化 Java 值(可能为 null——如空串场景由实现方语义决定)
     */
    Object text(String columnName, int typeId, String typeExpression, String rawValue);

    /**
     * 责任:产出"未变更 TOAST 列"的哨兵值——值不可得而非 null,Debezium 值转换器
     * (UnchangedToastedPlaceholder 体系)把哨兵对象渲染为配置的占位值。
     * 边界:仅在 before 同列值不可得时被调用(恢复优先,见 {@link RowChangeEmitter});
     * 返回值为哨兵标记对象,不得是 null(否则与 SQL NULL 混同)。
     *
     * @param columnName     wire 列名
     * @param typeId         wire 列类型 oid(数组/hstore/uuid 的哨兵分派依据)
     * @param typeExpression 带修饰的类型表达式(哨兵分派依据,如 {@code text[]})
     * @param optional       列可选性(随哨兵元数据携带)
     * @return 哨兵标记对象(生产实现为 vanilla {@code UnchangedToastedReplicationMessageColumn}
     *         的类型专属标记之一)
     */
    Object unchangedToast(String columnName, int typeId, String typeExpression, boolean optional);
}
