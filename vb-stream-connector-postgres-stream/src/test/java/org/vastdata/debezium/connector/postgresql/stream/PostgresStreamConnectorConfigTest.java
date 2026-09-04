package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PostgresStreamConnectorConfig} 配置面单测:五个新配置项的默认值解析、
 * slot.streaming 的枚举校验(非法值拒收)、parallel×two_phase 联合校验
 * (spec §5.1 启动期 fail-fast)、slot.feedback.interval.ms 的正整数校验与毫秒→秒换算、
 * ALL_FIELDS 对父集合的扩展完整性、大小写宽容、snapshot.mode 的仅-no_data 三层防线
 * (Field 校验 + taskConfigs 注入默认 + 构造器 fail-fast 兜底,MS5)。
 * 纯构造 + {@code Configuration.validate(Field.Set)} 断言,不连库、不起 Connect runtime
 * (taskConfigs 注入面经真实 {@link PostgresStreamConnector#taskConfigs(int)} 驱动)。
 */
class PostgresStreamConnectorConfigTest {

    /**
     * 构造最小可用的 PG 连接配置:补齐 Debezium 必填四件套
     * (hostname/port/user/database,缺省 localhost/5432/postgres/postgres)与
     * snapshot.mode=no_data(直接构造的最小合法面——镜像 PostgresStreamConnector.
     * taskConfigs 注入后的配置;MS5 起构造器对非 no_data fail-fast),
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
        props.put("snapshot.mode", "no_data");
        props.putAll(overrides);
        return Configuration.from(props);
    }

    /**
     * 构造未经 taskConfigs 注入的原始最小配置(必填四件套 + 覆盖项,无 snapshot.mode)
     * ——专用于断言构造器 fail-fast 兜底:父 Field 默认 initial,缺省直接构造必抛。
     *
     * @param overrides 用例覆盖项(不含 snapshot.mode 时即父默认回落面)
     * @return 未注入本连接器默认值的原始 {@link Configuration}
     */
    private static Map<String, String> uninjectedProps(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put("hostname", "localhost");
        props.put("port", "5432");
        props.put("user", "postgres");
        props.put("database", "postgres");
        props.putAll(overrides);
        return props;
    }

    /**
     * 用例①默认值解析:四项全不配置时,streamingMode() 解析为 ON、twoPhase() 为 true、
     * pipeDir() 为 pg-stream-pipe-queue、rollCycle() 为 MINUTELY——与 Field 声明的
     * 默认值一一对应(MS2 管道与复制流启动按此默认面接线,默认错位会静默改变行为)。
     */
    @Test
    void defaultsResolveToSpecValues() {
        PostgresStreamConnectorConfig config = new PostgresStreamConnectorConfig(configWith(Map.of()));
        assertEquals(StreamingMode.ON, config.streamingMode(), "slot.streaming 默认应解析为 ON");
        assertTrue(config.twoPhase(), "slot.two.phase 默认应为 true");
        assertEquals("pg-stream-pipe-queue", config.pipeDir(), "pipe.dir 默认应为引擎侧管道目录名");
        assertEquals(LegacyRollCycles.MINUTELY, config.rollCycle(), "pipe.roll.cycle 默认应为 MINUTELY");
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

    /**
     * 用例⑫slot.messages 默认 false + 显式解析:缺省时 messagesEnabled() 为 false
     * (门控关闭——槽选项维持 4 项,PG 不下发 'M',行为与 MS3 完全一致,MS3.5 spec §3.1
     * 的默认关裁定);显式 "true" 解析为 true、显式 "false" 仍为 false——布尔开关两侧
     * 口径一致,非布尔值会被 isBoolean 校验挡在启动期。
     */
    @Test
    void messagesDefaultsToFalseAndExplicitValuesParse() {
        assertFalse(new PostgresStreamConnectorConfig(configWith(Map.of())).messagesEnabled(),
                "slot.messages 缺省应为 false(门控默认关)");
        assertTrue(new PostgresStreamConnectorConfig(configWith(Map.of("slot.messages", "true"))).messagesEnabled(),
                "显式 true 应解析为 true");
        assertFalse(new PostgresStreamConnectorConfig(configWith(Map.of("slot.messages", "false"))).messagesEnabled(),
                "显式 false 应解析为 false");
        ConfigValue defaulted = configWith(Map.of()).validate(PostgresStreamConnectorConfig.ALL_FIELDS).get("slot.messages");
        assertTrue(defaulted == null || defaulted.errorMessages().isEmpty(),
                "缺省(默认 false)不产生 slot.messages 校验错误");
    }

    /**
     * 用例⑬slot.messages 收录进三处配置面:ALL_FIELDS、{@code getConfigFields()} 与
     * Connect REST 暴露面 {@code config()}(define 的默认值取 Field.defaultValue(),防两处
     * 字面量漂移)——漏挂任一处会出现"校验认得但 REST 不可配"或"REST 可配但引擎读不到"
     * 的配置面裂缝。
     */
    @Test
    void messagesFieldJoinsAllFieldsConnectorFieldsAndRestConfigDef() {
        Set<String> names = new HashSet<>();
        for (io.debezium.config.Field field : PostgresStreamConnectorConfig.ALL_FIELDS) {
            names.add(field.name());
        }
        assertTrue(names.contains("slot.messages"), "ALL_FIELDS 应含 slot.messages");

        Set<String> connectorFieldNames = new HashSet<>();
        for (io.debezium.config.Field field : new PostgresStreamConnector().getConfigFields()) {
            connectorFieldNames.add(field.name());
        }
        assertTrue(connectorFieldNames.contains("slot.messages"), "getConfigFields() 应同含 slot.messages");

        org.apache.kafka.common.config.ConfigDef def = new PostgresStreamConnector().config();
        assertTrue(def.names().contains("slot.messages"), "Connect REST configDef 应暴露 slot.messages");
        assertEquals(Boolean.FALSE, def.defaultValues().get("slot.messages"),
                "REST 暴露面默认值应取 Field.defaultValue()(false)");
    }

    /**
     * 用例⑭snapshot.mode=no_data 支持面:显式配置(小写)时 validate 零问题且构造成功、
     * getSnapshotMode() 解析为 NO_DATA——本连接器唯一支持的快照模式(流式-only,不做快照
     * 数据抽取,该职能属 vanilla postgresql-connector);小写面与父类枚举解析(大小写宽容)
     * 两侧一致,用户配置面不必整小写。
     */
    @Test
    void snapshotModeNoDataIsAccepted() {
        Configuration config = configWith(Map.of("snapshot.mode", "no_data"));
        Map<String, ConfigValue> problems = config.validate(PostgresStreamConnectorConfig.ALL_FIELDS);
        assertTrue(problems.get("snapshot.mode") == null || problems.get("snapshot.mode").errorMessages().isEmpty(),
                "snapshot.mode=no_data 不应产生校验问题");
        assertEquals(PostgresConnectorConfig.SnapshotMode.NO_DATA, new PostgresStreamConnectorConfig(config).getSnapshotMode(),
                "no_data 应构造成功且解析为 NO_DATA");
    }

    /**
     * 用例⑮snapshot.mode=initial 拒收:validate 恰好 1 条问题且文案含 "no_data only"
     * (启动期把非法快照模式挡在连库之前);REST 校验被绕过时构造器 fail-fast 兜底抛
     * {@link ConnectException}——三层防线(REST 校验/注入默认/构造器)缺一不可。
     */
    @Test
    void snapshotModeInitialIsRejected() {
        Map<String, ConfigValue> problems = configWith(Map.of("snapshot.mode", "initial"))
                .validate(PostgresStreamConnectorConfig.ALL_FIELDS);
        assertEquals(1, problems.get("snapshot.mode").errorMessages().size(),
                "snapshot.mode=initial 应恰好 1 条校验错误");
        assertTrue(problems.get("snapshot.mode").errorMessages().get(0).contains("no_data only"),
                "错误消息应说明仅支持 no_data");
        assertThrows(ConnectException.class,
                () -> new PostgresStreamConnectorConfig(configWith(Map.of("snapshot.mode", "initial"))),
                "直构造路径(绕过 REST 校验)应由构造器 fail-fast 抛 ConnectException");
    }

    /**
     * 用例⑯缺省 = no_data 只经注入成立:未注入的原始配置直接构造抛 ConnectException
     * (getSnapshotMode() 经<b>父 Field 引用</b>回落到默认 initial——子类同名字段替换
     * 不改变父引用的回落值,故"缺省 = no_data"必须由注入保证);经连接器 taskConfigs
     * 注入后的配置构造成功且解析为 NO_DATA——Connect runtime 与 embedded engine 两侧
     * 的默认值供给路径。
     */
    @Test
    void snapshotModeDefaultsToNoDataViaTaskConfigsInjection() {
        Map<String, String> raw = uninjectedProps(Map.of());
        assertThrows(ConnectException.class,
                () -> new PostgresStreamConnectorConfig(Configuration.from(raw)),
                "注入前直接构造应抛 ConnectException(父默认 initial 的 fail-fast 兜底)");
        PostgresStreamConnector connector = new PostgresStreamConnector();
        connector.start(raw);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size(), "单任务连接器应返回 1 份任务配置");
        assertEquals(PostgresConnectorConfig.SnapshotMode.NO_DATA,
                new PostgresStreamConnectorConfig(Configuration.from(taskConfigs.get(0))).getSnapshotMode(),
                "注入 snapshot.mode=no_data 后应构造成功且解析为 NO_DATA");
    }
}
