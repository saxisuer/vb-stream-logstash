package org.vastdata.debezium.connector.postgresql.stream;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.UnchangedToastedPlaceholder;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * 全串输出的 schema 侧转换器(values.as.string=true 时装配进
 * {@link StreamPostgresSchema},与 vanilla {@link PostgresValueConverter} 二选一):
 * Connect 记录的值与 schema 是<b>双轨</b>的——只透传值({@link StringColumnValueMapper})
 * 不够,schema 声明 INT64 而值是 String 时 Struct 构造/序列化直接抛 DataException。
 * 本类把两条轨一起改形:
 * <ul>
 *   <li>{@link #schemaBuilder(Column)} 恒返回 {@code SchemaBuilder.string()}——
 *       所有列(含 key 列)的 Connect schema 均为 STRING,optionality 仍由
 *       TableSchema 构建侧按列元数据统一施加(vanilla 各类型分支同款契约)</li>
 *   <li>{@link #converter(Column, org.apache.kafka.connect.data.Field)} 恒返回恒等
 *       函数——struct 构建期对已透传的 String 不做二次类型化(否则 vanilla 转换链会对
 *       String 调 {@code ((Number)x).intValue()} 之类,ClassCastException)</li>
 * </ul>
 *
 * <p>构造参数面镜像 vanilla {@code PostgresValueConverter.of}(decimal/temporal 等
 * mode 原样透传父构造器——本类的两个覆写不读它们,保留参数面只为父类其余行为
 * (如 toString)不因缺省漂移)。
 *
 * <p>已知边界:Binary 元组形态(槽开 binary)的 byte[] 值与 STRING schema 类型不符,
 * emit 期 DataException fail-fast——本连接器建槽不开 binary,防御性记档。
 *
 * <p>线程约束:不可变,任意线程可调;schema 构建实际仅 consumer 线程(R1)。
 */
public class StringValueConverter extends PostgresValueConverter {

    /**
     * 构造全串转换器(经 {@link #of} 间接使用)。
     *
     * @param databaseCharset 库字符集(vanilla 同源)
     * @param decimalMode     decimal 处理模式(本类覆写不读,透传父构造器)
     * @param temporalPrecisionMode 时间精度模式(同上)
     * @param defaultOffset   时区偏移(同上)
     * @param bigIntUnsignedMode 大整数无符号模式(同上)
     * @param includeUnknownDatatypes 未知类型开关(同上)
     * @param typeRegistry    类型注册表(同上)
     * @param hStoreMode      hstore 处理模式(同上)
     * @param binaryMode      binary 处理模式(同上)
     * @param intervalMode    interval 处理模式(同上)
     * @param unchangedToastedPlaceholder TOAST 占位渲染器(同上)
     * @param moneyFractionDigits money 小数位(同上)
     */
    protected StringValueConverter(Charset databaseCharset, DecimalMode decimalMode,
                                   TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                   BigIntUnsignedMode bigIntUnsignedMode, boolean includeUnknownDatatypes,
                                   TypeRegistry typeRegistry,
                                   PostgresConnectorConfig.HStoreHandlingMode hStoreMode,
                                   io.debezium.config.CommonConnectorConfig.BinaryHandlingMode binaryMode,
                                   PostgresConnectorConfig.IntervalHandlingMode intervalMode,
                                   UnchangedToastedPlaceholder unchangedToastedPlaceholder, int moneyFractionDigits) {
        super(databaseCharset, decimalMode, temporalPrecisionMode, defaultOffset, bigIntUnsignedMode,
                includeUnknownDatatypes, typeRegistry, hStoreMode, binaryMode, intervalMode,
                unchangedToastedPlaceholder, moneyFractionDigits);
    }

    /**
     * 责任:按配置构造全串转换器——参数取值逐项镜像 vanilla
     * {@code PostgresValueConverter.of}(DBZ 3.6.1.Final 同源口径;其中 4 项 mode 的
     * vanilla 取值器是 protected,以公开等价物解析:Field 常量直读 + 枚举 parse,
     * 与监督壳读 INCLUDE_UNKNOWN_DATATYPES 的既有模式一致)。
     *
     * @param connectorConfig 连接器配置
     * @param databaseCharset 库字符集(Task.start 临时连接产出)
     * @param typeRegistry    类型注册表(main 连接的共享实例)
     * @return 全串转换器实例
     */
    public static StringValueConverter of(PostgresConnectorConfig connectorConfig, Charset databaseCharset,
                                          TypeRegistry typeRegistry) {
        return new StringValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                connectorConfig.getConfig().getBoolean(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES),
                typeRegistry,
                PostgresConnectorConfig.HStoreHandlingMode.parse(
                        connectorConfig.getConfig().getString(PostgresConnectorConfig.HSTORE_HANDLING_MODE)),
                connectorConfig.binaryHandlingMode(),
                PostgresConnectorConfig.IntervalHandlingMode.parse(
                        connectorConfig.getConfig().getString(PostgresConnectorConfig.INTERVAL_HANDLING_MODE)),
                new UnchangedToastedPlaceholder(connectorConfig),
                connectorConfig.getConfig().getInteger(PostgresConnectorConfig.MONEY_FRACTION_DIGITS));
    }

    /**
     * 责任:任何列恒返回 STRING schema 构建器(全串模式的 schema 侧本体)。
     * 关键步骤:不看列类型 oid——int4/timestamptz/numeric/bool 全部一视同仁;
     * optionality 不在此施加(TableSchema 构建侧按列元数据统一处理,vanilla 各类型
     * 分支同款契约)。
     * 边界:column 为 null 的行为随 vanilla(NPE)。
     */
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        return SchemaBuilder.string();
    }

    /**
     * 责任:任何列恒返回恒等 converter——值已在 {@link StringColumnValueMapper}
     * 透传为 String,struct 构建期不做二次类型化。
     * 边界:null 值原样通过(Null 形态语义保留);Binary 元组的 byte[] 会因与
     * STRING schema 不符在 Struct 层 fail-fast(见类 javadoc 已知边界)。
     */
    @Override
    public ValueConverter converter(Column column, org.apache.kafka.connect.data.Field fieldDefn) {
        return data -> data;
    }
}
