package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.connector.postgresql.connection.pgoutput.PgOutputReplicationMessage;

import java.util.Objects;

/**
 * {@link ColumnValueMapper} 的生产实现:类型化转换本体委派 vanilla 的
 * {@code PgOutputReplicationMessage.getValue}(public static,DBZ 3.6.1.Final 实测签名)
 * 与 {@code UnchangedToastedReplicationMessageColumn} 哨兵构造——两处都是 vanilla
 * pgoutput 解码路径的同一份代码,不另造转换逻辑。
 *
 * <p><b>连接供给器的 fail-fast 口径(MS2 已文档化限制)</b>:vanilla 在流式线程上持有
 * 复制连接,数组/未知类型列的解析会经 {@code PgConnectionSupplier} 取活连接
 * (PgArray 构造等);本连接器的 consumer 线程不持有任何 JDBC 连接(main 连接归
 * reader 线程的 'R' enrich 独占,R3),故供给器直接抛 SQLException 终止——
 * 数组与未注册类型的实时转换属 MS2 不支持的列族,静默错值比 fail-fast 更糟。
 * Task 8 IT 起按需重议(如 reader 线程旁路代转)。
 *
 * <p>线程约束:无状态(仅持不可变注册表与开关),任意线程可调;实际仅 consumer 线程
 * (R1)。真库行为归 Task 8,离线单测经假 {@link ColumnValueMapper} 观察调用面。
 */
public final class TypeRegistryColumnValueMapper implements ColumnValueMapper {

    /** 数组/未知类型列解析需要活连接时的 fail-fast 供给器(见类 javadoc 的限制口径)。 */
    private static final PgConnectionSupplier NO_CONNECTION = () -> {
        throw new DebeziumException(
                "MS2 不支持数组/未知类型列的回放期类型化转换(需要 consumer 线程不持有的 JDBC 连接)");
    };

    /** 连库类型注册表(Task.start 经 createTypeRegistry 建立的共享实例)。 */
    private final TypeRegistry typeRegistry;

    /** 未知类型是否照发(vanilla includeUnknownDatatypes 配置,受保护方法,经 Field 直读)。 */
    private final boolean includeUnknownDatatypes;

    /**
     * 构造生产映射器。
     *
     * @param typeRegistry            连库类型注册表(oid → PostgresType)
     * @param includeUnknownDatatypes 未知类型列是否照发(与 vanilla 同名配置同义)
     */
    public TypeRegistryColumnValueMapper(TypeRegistry typeRegistry, boolean includeUnknownDatatypes) {
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
        this.includeUnknownDatatypes = includeUnknownDatatypes;
    }

    /**
     * 责任:文本值经 vanilla {@code PgOutputReplicationMessage.getValue} 按 oid 对应的
     * PostgresType 解析(Integer/Long/BigDecimal/Instant/bytea 字节……)。
     * 边界:解析失败(值与类型不符)按 vanilla 异常语义原样上抛——消费路径 fail-fast;
     * oid 未注册时 {@code TypeRegistry#get} 返回 UNKNOWN,由 includeUnknownDatatypes
     * 决定照发或抛(同 vanilla)。
     */
    @Override
    public Object text(String columnName, int typeId, String typeExpression, String rawValue) {
        return PgOutputReplicationMessage.getValue(columnName, typeRegistry.get(typeId), typeExpression,
                rawValue, NO_CONNECTION, includeUnknownDatatypes, typeRegistry);
    }

    /**
     * 责任:构造 vanilla 的未变更 TOAST 哨兵列并取其标记值——标记对象由带修饰类型名分派
     * (text[]/bytea[]/hstore/uuid[] 各有专属标记,其余通用 UNCHANGED_TOAST_VALUE),
     * Debezium 值转换器再把标记渲染为配置占位值。
     * 边界:PostgresType 经注册表解析(未注册回落 UNKNOWN,通用标记);getValue 的连接
     * 参数对哨兵路径无意义(vanilla 实现忽略),传 null。
     */
    @Override
    public Object unchangedToast(String columnName, int typeId, String typeExpression, boolean optional) {
        return new UnchangedToastedReplicationMessageColumn(columnName, typeRegistry.get(typeId),
                typeExpression, optional).getValue(null, false);
    }
}
