package org.vastdata.debezium.connector.postgresql.stream;

import java.util.Objects;

import io.debezium.connector.postgresql.PostgresConnectorConfig;

/**
 * {@link ColumnValueMapper} 的全串输出实现(values.as.string=true 时的生产实现,
 * 与 {@link TypeRegistryColumnValueMapper} 二选一装配):pgoutput 文本形态的列值
 * <b>原文透传</b>——不按类型解析成 Integer/BigDecimal/Instant,下游拿到的是 PG
 * 的文本表示(SELECT 同款口径)。类型化转换的责任整体移交下游(Logstash 的
 * date/mutate filter 等),换来三处收益(记档见 {@code PostgresStreamConnectorConfig.
 * VALUES_AS_STRING} 的 Field javadoc):
 * <ul>
 *   <li>数组列不再触达 vanilla {@code PgArray} 的活连接解析——R1/R3 记档的
 *       fail-fast 已知限制在此模式下消解,{@code {1,2,3}} 原文照发</li>
 *   <li>未知类型不再依赖 includeUnknownDatatypes 的静默 null 回落——原文照发</li>
 *   <li>numeric/timestamp 保留精确文本,无 epoch 微秒时区换算</li>
 * </ul>
 *
 * <p>TOAST 未变哨兵在本模式下直接是<b>占位字符串</b>(vanilla 同款默认值,经
 * {@code getUnavailableValuePlaceholder} 取得——配置面继承父连接器的占位值配置),
 * 不再走 {@code UnchangedToastedReplicationMessageColumn} 的类型专属标记对象体系:
 * 标记对象依赖值转换器渲染占位,而本模式的 schema 侧是恒等 converter
 * ({@code StringValueConverter}),标记无人渲染会以裸 Object 进 STRING schema。
 *
 * <p>线程约束:无状态(仅持不可变占位字符串),任意线程可调;实际仅 consumer 线程(R1)。
 */
public final class StringColumnValueMapper implements ColumnValueMapper {

    /** TOAST 未变列的占位字符串(vanilla 默认 {@code __debezium_unavailable_value},配置面同源)。 */
    private final String toastPlaceholder;

    /**
     * 构造全串映射器。
     *
     * @param connectorConfig 连接器配置(TOAST 占位值的来源,与 vanilla 占位配置同源)
     */
    public StringColumnValueMapper(PostgresConnectorConfig connectorConfig) {
        Objects.requireNonNull(connectorConfig, "connectorConfig");
        this.toastPlaceholder = new String(connectorConfig.getUnavailableValuePlaceholder());
    }

    /**
     * 责任:文本值原文返回(恒等透传,零解析)。
     * 边界:rawValue 为 null 不进入本方法(Null 形态由 emitter 直接置 null);
     * 值与列类型不符无从谈起——本实现不解释类型。
     */
    @Override
    public Object text(String columnName, int typeId, String typeExpression, String rawValue) {
        return rawValue;
    }

    /**
     * 责任:返回 TOAST 未变列的占位字符串(值不可得而非 null 的显式标记)。
     * 边界:恒非 null;typeId/typeExpression/optional 在全串模式下不参与分派
     * (占位无类型形态之别),参数仅为接口契约保留。
     */
    @Override
    public Object unchangedToast(String columnName, int typeId, String typeExpression, boolean optional) {
        return toastPlaceholder;
    }
}
