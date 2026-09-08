package org.vastdata.debezium.connector.postgresql.stream;

import java.util.HashMap;
import java.util.Map;

import io.debezium.config.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link StringColumnValueMapper} 离线单测:全串模式的值侧行为——text 原文恒等透传
 * (数值/布尔/时间/数组各形态零解析)、unchangedToast 返回占位字符串(与 vanilla
 * unavailable.value.placeholder 配置同源、可覆盖)。纯构造断言,不连库。
 */
class StringColumnValueMapperTest {

    /**
     * 构造最小可用的 PG 连接配置(必填四件套 + snapshot.mode=no_data,镜像
     * {@code PostgresStreamConnectorConfigTest.configWith} 的合法基础面)。
     *
     * @param overrides 用例覆盖项;空 Map 即纯默认面
     * @return 可直接交给构造器的不可变 {@link Configuration}
     */
    private static PostgresStreamConnectorConfig configWith(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.put("snapshot.mode", "no_data");
        props.putAll(overrides);
        return new PostgresStreamConnectorConfig(Configuration.from(props));
    }

    /**
     * 用例①text 恒等透传:numeric/布尔/带时区时间戳/数组文本各给一例,返回值就是
     * 入参同一引用(零解析零拷贝)——全串模式的核心契约,任何形态的格式化/解析都会
     * 破坏"PG 文本原文"的下游口径。
     */
    @Test
    void textReturnsRawVerbatim() {
        StringColumnValueMapper mapper = new StringColumnValueMapper(configWith(Map.of()));
        for (String raw : new String[] {
                "12345.678901234567890123456789",
                "t",
                "2026-09-09 12:34:56.123456+08",
                "{1,2,3}",
                "\\xdeadbeef",
                ""}) {
            assertEquals(raw, mapper.text("c", 0, "text", raw), "text() 必须原文恒等: " + raw);
        }
    }

    /**
     * 用例②TOAST 占位默认值:缺省配置下占位字符串为 vanilla 同款
     * {@code __debezium_unavailable_value}——与类型化路径的占位口径一致,下游工具
     * 对该哨兵值的既有识别不因模式切换而失效。
     */
    @Test
    void unchangedToastUsesVanillaDefaultPlaceholder() {
        StringColumnValueMapper mapper = new StringColumnValueMapper(configWith(Map.of()));
        Object placeholder = mapper.unchangedToast("c", 0, "text", true);
        assertNotNull(placeholder, "占位必须非 null(与 SQL NULL 区分)");
        assertEquals("__debezium_unavailable_value", placeholder, "默认占位应与 vanilla 同款");
    }

    /**
     * 用例③TOAST 占位可配置:unavailable.value.placeholder 显式覆盖时占位随配置
     * (getUnavailableValuePlaceholder 的字符串路径)——占位配置面在全串模式下继续生效。
     */
    @Test
    void unchangedToastHonorsPlaceholderOverride() {
        StringColumnValueMapper mapper = new StringColumnValueMapper(
                configWith(Map.of("unavailable.value.placeholder", "__custom_toast__")));
        assertEquals("__custom_toast__", mapper.unchangedToast("c", 0, "text", true),
                "占位应随 unavailable.value.placeholder 覆盖");
    }
}
