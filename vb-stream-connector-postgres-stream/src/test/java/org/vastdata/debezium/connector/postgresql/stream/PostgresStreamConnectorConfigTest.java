package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.Configuration;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PostgresStreamConnectorConfig} 配置面单测:五个新配置项的默认值解析、
 * slot.streaming 的枚举校验(非法值拒收)、parallel×two_phase 联合校验
 * (spec §5.1 启动期 fail-fast)、slot.feedback.interval.ms 的正整数校验与毫秒→秒换算、
 * ALL_FIELDS 对父集合的扩展完整性、大小写宽容。
 * 纯构造 + {@code Configuration.validate(Field.Set)} 断言,不连库、不起 Connect runtime。
 */
class PostgresStreamConnectorConfigTest {

    /**
     * 构造最小可用的 PG 连接配置:补齐 Debezium 必填四件套
     * (hostname/port/user/database,缺省 localhost/5432/postgres/postgres),
     * 再叠加各用例自己的覆盖项,保证 {@code PostgresStreamConnectorConfig} 构造器
     * 与 validate 面向的是"合法基础 + 待测覆盖"的配置。
     *
     * @param overrides 用例覆盖项(键=配置名,值=字符串形式的配置值);空 Map 即纯默认面
     * @return 可直接交给构造器/validate 的不可变 {@link Configuration}
     */
    private static Configuration configWith(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.putAll(overrides);
        return Configuration.from(props);
    }

    /**
     * 用例①默认值解析:四项全不配置时,streamingMode() 解析为 ON、twoPhase() 为 true、
     * pipeDir() 为 pg-stream-pipe-queue、pipeRollCycle() 为 MINUTELY——与 Field 声明的
     * 默认值一一对应(MS2 管道与复制流启动按此默认面接线,默认错位会静默改变行为)。
     */
    @Test
    void defaultsResolveToSpecValues() {
        PostgresStreamConnectorConfig config = new PostgresStreamConnectorConfig(configWith(Map.of()));
        assertEquals(StreamingMode.ON, config.streamingMode(), "slot.streaming 默认应解析为 ON");
        assertTrue(config.twoPhase(), "slot.two.phase 默认应为 true");
        assertEquals("pg-stream-pipe-queue", config.pipeDir(), "pipe.dir 默认应为引擎侧管道目录名");
        assertEquals("MINUTELY", config.pipeRollCycle(), "pipe.roll.cycle 默认应为 MINUTELY");
    }

    /**
     * 用例②联合校验 fail-fast:slot.streaming=parallel 且 slot.two.phase=false 时,
     * validate 恰好产生 1 条错误消息(记在联合校验发起方 slot.streaming 的 ConfigValue 上,
     * 值本身非法的不是 two.phase——false 是合法布尔);消息文本须指明平行流的 two_phase 前置。
     */
    @Test
    void parallelStreamingWithTwoPhaseDisabledFailsValidation() {
        Map<String, ConfigValue> problems = configWith(Map.of("slot.streaming", "parallel", "slot.two.phase", "false"))
                .validate(PostgresStreamConnectorConfig.ALL_FIELDS);
        assertEquals(1, problems.get("slot.streaming").errorMessages().size(),
                "parallel × two.phase=false 应恰好 1 条联合校验错误");
        assertTrue(problems.get("slot.streaming").errorMessages().get(0).contains("slot.streaming=parallel requires slot.two.phase=true"),
                "错误消息应指明 parallel 的 two_phase=true 前置");
        assertTrue(problems.get("slot.two.phase").errorMessages().isEmpty(),
                "two.phase=false 是合法布尔值,自身不应另计错误");
    }

    /**
     * 用例③枚举校验:slot.streaming=bad 不在 OFF/ON/PARALLEL 名单内,validate 恰好
     * 1 条错误且消息列出合法值集合——启动期把拼写错误挡在连库之前。
     */
    @Test
    void invalidStreamingEnumValueRejected() {
        Map<String, ConfigValue> problems = configWith(Map.of("slot.streaming", "bad"))
                .validate(PostgresStreamConnectorConfig.ALL_FIELDS);
        assertEquals(1, problems.get("slot.streaming").errorMessages().size(),
                "非法枚举值应恰好 1 条错误");
        assertTrue(problems.get("slot.streaming").errorMessages().get(0).contains("OFF, ON, PARALLEL"),
                "错误消息应列出合法值集合 OFF, ON, PARALLEL");
    }

    /**
     * 用例④ALL_FIELDS 扩展完整性:除 4 个新名(slot.streaming/slot.two.phase/pipe.dir/
     * pipe.roll.cycle)外,仍包含父类既有字段(slot.name 为代表)——证明是父集合的
     * 扩展而非替换,父类校验面(必填项等)不被丢弃。
     */
    @Test
    void allFieldsExtendParentSetWithFourNewKeys() {
        Set<String> names = new HashSet<>();
        for (io.debezium.config.Field field : PostgresStreamConnectorConfig.ALL_FIELDS) {
            names.add(field.name());
        }
        assertTrue(names.containsAll(Set.of("slot.streaming", "slot.two.phase", "pipe.dir", "pipe.roll.cycle")),
                "ALL_FIELDS 应含 4 个新配置名");
        assertTrue(names.contains("slot.name"), "ALL_FIELDS 应保留父类既有字段(以 slot.name 为代表)");
    }

    /**
     * 用例⑤大小写宽容:on/ON/On 解析为同一档位 ON,off 解析为 OFF——Field 校验与
     * streamingMode() 两侧一致地按大小写无关解析,用户配置面不必整大写。
     */
    @Test
    void streamingModeIsCaseInsensitive() {
        for (String value : new String[]{ "on", "ON", "On" }) {
            PostgresStreamConnectorConfig config = new PostgresStreamConnectorConfig(configWith(Map.of("slot.streaming", value)));
            assertEquals(StreamingMode.ON, config.streamingMode(), "值 '" + value + "' 应大小写无关地解析为 ON");
        }
        assertEquals(StreamingMode.OFF,
                new PostgresStreamConnectorConfig(configWith(Map.of("slot.streaming", "off"))).streamingMode(),
                "小写 off 应解析为 OFF");
    }

    /**
     * 用例⑥反馈间隔默认:slot.feedback.interval.ms 不配置时默认 10000ms,
     * feedbackIntervalSeconds() 整除换算为 10 秒——复制会话 run 循环的 forceUpdateStatus
     * 节流周期(默认错位会静默改变 LSN 反馈频率,运维面从 pg_stat_replication 读到的进度随之失真)。
     */
    @Test
    void feedbackIntervalDefaultsToTenSeconds() {
        PostgresStreamConnectorConfig config = new PostgresStreamConnectorConfig(configWith(Map.of()));
        assertEquals(10, config.feedbackIntervalSeconds(), "缺省 10000ms 应换算为 10 秒反馈间隔");
    }

    /**
     * 用例⑦正整数校验:slot.feedback.interval.ms=0 不满足 isPositiveInteger,validate
     * 恰好 1 条错误——启动期把非正值挡在连库之前(0 或负数进 run 循环会让状态包发送节流失控)。
     */
    @Test
    void nonPositiveFeedbackIntervalRejected() {
        Map<String, ConfigValue> problems = configWith(Map.of("slot.feedback.interval.ms", "0"))
                .validate(PostgresStreamConnectorConfig.ALL_FIELDS);
        assertEquals(1, problems.get("slot.feedback.interval.ms").errorMessages().size(),
                "非正整数应恰好 1 条校验错误");
    }

    /**
     * 用例⑧显式值换算 + ALL_FIELDS 收录:15000ms 显式配置换算 15 秒(毫秒→秒整除,
     * 亚秒值截断语义见 getter javadoc);ALL_FIELDS 与 getConfigFields() 同源含新名
     * slot.feedback.interval.ms——证明是父集合的继续扩展而非漏挂。
     */
    @Test
    void feedbackIntervalExplicitValueConvertsAndFieldJoinsAllFields() {
        assertEquals(15, new PostgresStreamConnectorConfig(configWith(Map.of("slot.feedback.interval.ms", "15000")))
                .feedbackIntervalSeconds(), "显式 15000ms 应换算为 15 秒");

        Set<String> names = new HashSet<>();
        for (io.debezium.config.Field field : PostgresStreamConnectorConfig.ALL_FIELDS) {
            names.add(field.name());
        }
        assertTrue(names.contains("slot.feedback.interval.ms"), "ALL_FIELDS 应含 slot.feedback.interval.ms");

        Set<String> connectorFieldNames = new HashSet<>();
        for (io.debezium.config.Field field : new PostgresStreamConnector().getConfigFields()) {
            connectorFieldNames.add(field.name());
        }
        assertTrue(connectorFieldNames.contains("slot.feedback.interval.ms"),
                "getConfigFields() 返回的 ALL_FIELDS 应同含新配置名");
    }

    /**
     * 用例⑨rollCycle() 枚举解析默认:pipe.roll.cycle 不配置时 {@code rollCycle()} 返回
     * {@link LegacyRollCycles#MINUTELY}——管道装配(MessagePipe 构造)直接消费枚举实例,
     * 默认错位会同时改变滚动文件粒度与低水位删除的档位节奏。
     */
    @Test
    void rollCycleDefaultsToMinutelyEnum() {
        PostgresStreamConnectorConfig config = new PostgresStreamConnectorConfig(configWith(Map.of()));
        assertEquals(LegacyRollCycles.MINUTELY, config.rollCycle(),
                "pipe.roll.cycle 默认应解析为 LegacyRollCycles.MINUTELY 枚举单例");
    }

    /**
     * 用例⑩大小写宽容:minutely/houRly 经 {@code rollCycle()} 解析为同一枚举单例
     * (MINUTELY/HOURLY)——校验器与 getter 两侧都按 equalsIgnoreCase 在 LegacyRollCycles
     * 中查找(引擎 PipeConfig.parseRollCycle 同语义),配置面不必整大写。
     */
    @Test
    void rollCycleIsCaseInsensitive() {
        assertEquals(LegacyRollCycles.MINUTELY,
                new PostgresStreamConnectorConfig(configWith(Map.of("pipe.roll.cycle", "minutely"))).rollCycle(),
                "小写 minutely 应解析为 MINUTELY");
        assertEquals(LegacyRollCycles.HOURLY,
                new PostgresStreamConnectorConfig(configWith(Map.of("pipe.roll.cycle", "houRly"))).rollCycle(),
                "混合大小写 houRly 应解析为 HOURLY");
    }

    /**
     * 用例⑪未知 rollCycle 拒收:pipe.roll.cycle=NOPE 时 validate 恰好 1 条错误且消息附
     * 可用值清单(以 MINUTELY 为代表)——启动期把拼写错误挡在建管道之前(残余到管道构造
     * 后才炸会拖垮 reader 线程,且队列目录可能已被 wipe)。
     */
    @Test
    void unknownRollCycleRejectedWithUsableValues() {
        Map<String, ConfigValue> problems = configWith(Map.of("pipe.roll.cycle", "NOPE"))
                .validate(PostgresStreamConnectorConfig.ALL_FIELDS);
        assertEquals(1, problems.get("pipe.roll.cycle").errorMessages().size(),
                "未知滚动周期应恰好 1 条校验错误");
        assertTrue(problems.get("pipe.roll.cycle").errorMessages().get(0).contains("MINUTELY"),
                "错误消息应附可用值清单(以 MINUTELY 为代表)");
    }
}
