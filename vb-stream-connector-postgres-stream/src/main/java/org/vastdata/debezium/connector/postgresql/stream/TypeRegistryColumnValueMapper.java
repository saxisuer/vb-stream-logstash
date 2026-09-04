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
 * <p><b>连接供给器的 fail-fast 口径(已知限制,记档见 docs/superpowers/specs/
 * 2026-09-02-ms2-r1-r3-audit.md 的"已知限制与延期"节)</b>:vanilla 在流式线程上持有
 * 复制连接,数组列的解析经 {@code PgConnectionSupplier} 取活连接(PgArray 构造,
 * 惰性解析——转换器 getArray() 时才真用连接);本连接器的 consumer 线程不持有任何
 * JDBC 连接(main 连接归 reader 线程的 'R' enrich 独占,R3),故供给器直接抛
 * {@link DebeziumException}(非受检,穿透 vanilla asArray 只捕 SQLException 的
 * catch,经供给器的受检签名合法上抛)终止 consumer——<b>比 vanilla 的失败静默
 * null(WARN 后丢值)更安全,维持现状</b>;可行支持路径(reader 线程旁路代转 /
 * consumer 期短开只读连接)都破坏 R1/R3 单写者假设,真需求出现再议。
 * 语义钉子:{@code TypeRegistryColumnValueMapperTest}(DebeziumException 穿透
 * 不静默 null)。
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
     * 决定照发原串或<b>静默返回 null</b>(vanilla asDefault 同款:false 时不抛,仅 DEBUG
     * 日志后返回 null——勘误:早先 javadoc 写"照发或抛"与 vanilla 不符);
     * 数组类型列(isArrayType,elementType 非空)在 vanilla 分派进 asArray——经本实现的
     * fail-fast 供给器终止(见类 javadoc 的已知限制口径)。
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
