package org.vastdata.debezium.connector.postgresql.stream;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.ValueConverter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * {@link StringValueConverter} 离线单测:全串模式的 schema 侧行为——schemaBuilder 对
 * 各典型类型列(int4/numeric/timestamptz/bool/text[])恒返回 STRING 构建器、
 * converter 恒等(值原样通过含 null)。纯构造断言,不连库(TypeRegistry 传 null:
 * 两个覆写均不读它)。
 */
class StringValueConverterTest {

    /**
     * 构造最小可用的 PG 连接配置(必填四件套 + snapshot.mode=no_data,镜像
     * {@code PostgresStreamConnectorConfigTest.configWith} 的合法基础面)。
     *
     * @return 可用于构造转换器的连接器配置
     */
    private static PostgresStreamConnectorConfig defaultConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "no_data");
        return new PostgresStreamConnectorConfig(Configuration.from(props));
    }

    /**
     * 以列名/oid 建一个测试列({@code Column.editor()} 是 Debezium 公开的
     * ColumnEditor 工厂,与生产 RelationTableFactory 同款)。
     *
     * @param name 列名
     * @param oid  PG 类型 oid
     * @return 已定形的列
     */
    private static Column column(String name, int oid) {
        ColumnEditor editor = Column.editor().name(name).nativeType(oid);
        editor.type("text");
        return editor.create();
    }

    /**
     * 用例①schemaBuilder 类型无关:数值/时间/布尔/数组各 oid 均产出 STRING schema
     * (Type.INT32 语义上覆盖 int2/int4/int8/float/numeric/bool/timestamp 全family——
     * 全串契约就是不看 oid)。
     */
    @Test
    void schemaBuilderIsStringForEveryType() {
        StringValueConverter converter = StringValueConverter.of(defaultConfig(), null, null);
        int[] representativeOids = { 20, 21, 23, 25, 700, 1000, 1007, 1114, 1184, 1700, 16 };
        for (int oid : representativeOids) {
            Schema schema = converter.schemaBuilder(column("c", oid)).build();
            assertEquals(Schema.Type.STRING, schema.type(), "oid=" + oid + " 的 schema 必须是 STRING");
        }
    }

    /**
     * 用例②converter 恒等:任意 oid/任意值,convert 返回同一引用——String 已在值侧
     * 透传完毕,struct 构建期的二次类型化必须不存在;null 原样通过(Null 形态语义保留)。
     */
    @Test
    void converterIsIdentityIncludingNull() {
        StringValueConverter converter = StringValueConverter.of(defaultConfig(), null, null);
        ValueConverter vc = converter.converter(column("c", 23), null);
        String value = "12345.678901234567890";
        assertSame(value, vc.convert(value), "已透传的 String 必须原引用通过(恒等)");
        assertNull(vc.convert(null), "null 必须原样通过");
    }

    /**
     * 用例③与 vanilla 构造共存性:of() 在缺省配置下可正常构造(4 项 protected mode
     * 经公开等价物解析不抛错)——构造路径回归锚。
     */
    @Test
    void ofConstructsWithDefaultConfig() {
        StringValueConverter converter = StringValueConverter.of(defaultConfig(), null, null);
        assertEquals(SchemaBuilder.string().build().type(),
                converter.schemaBuilder(column("c", 23)).build().type(),
                "缺省配置构造的实例行为不变");
    }
}
