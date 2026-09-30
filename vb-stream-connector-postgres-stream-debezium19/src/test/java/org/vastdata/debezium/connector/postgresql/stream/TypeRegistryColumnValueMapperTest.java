package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TypeRegistryColumnValueMapper} 的 fail-fast 语义钉子单测(离线,零 PG):
 * <b>数组类型列经真生产实现({@code NO_CONNECTION} 供给器形态)必须以
 * {@link DebeziumException} 穿透,而非 vanilla 的静默 null</b>——vanilla
 * {@code AbstractColumnValue.asArray} 无条件 {@code new PgArray(connection.get(), ...)}
 * 且只捕 SQLException,供给器抛的非受检异常穿透 catch 直达 consumer fail-fast;
 * 该差异是已记档的已知限制(docs/superpowers/specs/2026-09-02-ms2-r1-r3-audit.md
 * "已知限制与延期"节),本测试钉住"穿透"这一半语义不回退。
 *
 * <p>夹具路线(离线构造 vanilla 连库对象):{@link TypeRegistry} 唯一构造子要求活
 * {@code PostgresConnection} 且构造即 prime(全量类型查询)——离线经
 * {@code sun.misc.Unsafe#allocateInstance} 越过构造子,再反射注入 {@code oidToType}
 * 映射(字段名与 3.6.1.Final sources 实证一致;升版若改名,本测试以反射失败显式红);
 * 数组 {@link PostgresType} 经反射调其私有 7 参构造子(name/oid/jdbcId/typeInfo/
 * enumValues/parentType/elementType),elementType 置 {@code UNKNOWN}(非 null 即
 * {@code isArrayType()==true})、parentType 置 null({@code isRootType()==true},
 * 对齐 vanilla 分派前置件)。无并发面。
 */
class TypeRegistryColumnValueMapperTest {

    /** text[] 数组类型的测试 oid(_text 真实 oid,任意未注册值亦可——注册表由夹具注入)。 */
    private static final int TEXT_ARRAY_OID = 1007;

    /**
     * 责任:钉住数组列的 fail-fast 语义——数组形态的 PostgresType 经真生产映射器
     * (text → vanilla getValue → isArray 分派 → asArray 取连接)时,供给器
     * {@code NO_CONNECTION} 的 {@link DebeziumException} 必须原样穿透调用方:
     * 步骤①构造 elementType 非空/parentType 空的类型并断言数组前置件成立
     * (isArrayType 且 isRootType——否则测试自身夹具失效);②经注入 oidToType 的
     * 离线注册表构造生产映射器;③调用 text 断言抛 DebeziumException(非 null 返回、
     * 非其它异常形态)。若将来改成静默 null(对齐 vanilla)或换成其它异常,本测试红——
     * 届时须同步修订审计文档的已知限制口径。
     */
    @Test
    void arrayColumnTypeFailsFastWithDebeziumExceptionInsteadOfVanillaSilentNull() {
        PostgresType arrayType = arrayType(TEXT_ARRAY_OID);
        assertTrue(arrayType.isArrayType(), "夹具前置件:elementType 非空即数组形态");
        assertTrue(arrayType.isRootType(), "夹具前置件:parentType 空——vanilla 分派在数组检查前先走父类型解析");

        TypeRegistryColumnValueMapper mapper =
                new TypeRegistryColumnValueMapper(registryWith(TEXT_ARRAY_OID, arrayType), false);

        DebeziumException thrown = assertThrows(DebeziumException.class,
                () -> mapper.text("tags", TEXT_ARRAY_OID, "_text", "{seam}"),
                "数组列必须经供给器 fail-fast(DebeziumException 穿透 vanilla asArray 只捕 SQLException 的 catch)");
        assertTrue(thrown.getMessage().contains("数组"),
                "异常应来自 NO_CONNECTION 供给器的已知限制口径文案: " + thrown.getMessage());
    }

    /**
     * 责任:离线构造数组形态的 PostgresType——反射调 vanilla 私有 7 参构造子
     * (name, oid, jdbcId, typeInfo, enumValues, parentType, elementType;typeInfo/enumValues
     * 置 null——本路径只触 isArrayType/isRootType/getOid,不触 typeInfo 支撑的
     * length/scale),elementType 置 UNKNOWN(非 null → isArrayType)、parentType 置
     * null(→ isRootType,跳过 vanilla 的父类型递归直接进数组分派)。
     * 边界:构造子形态变化(升版改签名/参数个数)抛 IllegalStateException 显式红,
     * 不产静默错误夹具。
     *
     * @param oid 测试类型 oid(仅诊断与注册表键用途)
     * @return 数组形态的 PostgresType(name 固定 "_text")
     */
    private static PostgresType arrayType(int oid) {
        try {
            Constructor<?> sevenArg = java.util.Arrays.stream(PostgresType.class.getDeclaredConstructors())
                    .filter(c -> c.getParameterCount() == 7)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("PostgresType 7 参私有构造子缺席(vanilla 升版?)"));
            sevenArg.setAccessible(true);
            return (PostgresType) sevenArg.newInstance("_text", oid, Types.ARRAY, null, null, null, PostgresType.UNKNOWN);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("反射构造数组 PostgresType 失败(vanilla 升版改构造子?)", e);
        }
    }

    /**
     * 责任:离线产可用 TypeRegistry——Unsafe.allocateInstance 越过要求活连接的构造子
     * (构造即 prime 全量类型查询,离线必败),再反射注入 {@code oidToType} 映射使
     * {@code get(oid)} 命中即返(不触 resolveUnknownType 的 SQL 路径;命中分支只读
     * 该 map,无其它字段参与,实证于 3.6.1.Final sources)。
     * 边界:字段名变化(升版)抛 IllegalStateException 显式红。
     *
     * @param oid  注册键(测试类型 oid)
     * @param type 注册值(数组形态类型)
     * @return 注入完成的离线注册表实例
     */
    private static TypeRegistry registryWith(int oid, PostgresType type) {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            sun.misc.Unsafe unsafe = (sun.misc.Unsafe) theUnsafe.get(null);
            TypeRegistry registry = (TypeRegistry) unsafe.allocateInstance(TypeRegistry.class);
            Field oidToType = TypeRegistry.class.getDeclaredField("oidToType");
            oidToType.setAccessible(true);
            Map<Integer, PostgresType> map = new HashMap<>();
            map.put(oid, type);
            oidToType.set(registry, map);
            return registry;
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("离线构造 TypeRegistry 失败(vanilla 升版改字段名?)", e);
        }
    }
}
