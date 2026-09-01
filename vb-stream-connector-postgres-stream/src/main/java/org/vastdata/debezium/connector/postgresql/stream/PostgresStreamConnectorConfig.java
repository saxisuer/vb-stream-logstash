package org.vastdata.debezium.connector.postgresql.stream;

import java.util.Locale;

import org.apache.kafka.common.config.ConfigDef.Type;

import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig;

/**
 * 流式连接器配置:在父类 {@link PostgresConnectorConfig} 的完整配置面之上追加
 * 四个本模块专属配置项——slot.streaming(流式档位)、slot.two.phase(两阶段提交)、
 * pipe.dir / pipe.roll.cycle(reader 与 consumer 之间的 Chronicle Queue 管道参数,
 * 对应引擎侧 {@code vb.pipe.dir} / {@code vb.pipe.rollCycle})。
 *
 * <p>校验语义(spec §5.1 启动期 fail-fast):slot.streaming 只接受
 * OFF/ON/PARALLEL(大小写宽容);PARALLEL 档必须搭配 slot.two.phase=true,
 * 否则 {@link #validateSlotStreaming} 记 1 条问题并使配置整体不通过——
 * PG 侧 parallel 流式解码以 two_phase 为前置,漏配到运行期才失败代价过高。
 *
 * <p>{@link #ALL_FIELDS} = 父 ALL_FIELDS 扩展 4 新 Field(floor 语义,父类必填项、
 * 校验器全部保留),供任务侧配置完整性校验;Connect REST 暴露面见
 * {@link PostgresStreamConnector#config()}。
 *
 * <p>构造后不可变;getter 均为无副作用读取,任意线程可并发调用。
 */
public class PostgresStreamConnectorConfig extends PostgresConnectorConfig {

    /** 流式档位:OFF(提交后整体回放)/ ON(进行中大事务边收边发)/ PARALLEL(流式+并行),默认 ON,大小写宽容。 */
    public static final Field SLOT_STREAMING = Field.create("slot.streaming")
            .withDisplayName("Slot streaming")
            .withType(Type.STRING)
            .withDefault("on")
            .withValidation(PostgresStreamConnectorConfig::validateSlotStreaming);

    /** 两阶段提交开关:建槽带 two_phase 选项(PG 15+ 的 PARALLEL 档前置),默认 true,布尔校验。 */
    public static final Field SLOT_TWO_PHASE = Field.create("slot.two.phase")
            .withDisplayName("Slot two-phase")
            .withType(Type.BOOLEAN)
            .withDefault(true)
            .withValidation(Field::isBoolean);

    /** 管道目录:Chronicle Queue 主缓冲的工作目录(瞬态,重启自动清空),默认 pg-stream-pipe-queue。 */
    public static final Field PIPE_DIR = Field.create("pipe.dir")
            .withDisplayName("Pipe directory")
            .withType(Type.STRING)
            .withDefault("pg-stream-pipe-queue");

    /** 管道滚动周期:LegacyRollCycles 枚举名(大小写宽容,由 MS2 管道装配解析),默认 MINUTELY。 */
    public static final Field PIPE_ROLL_CYCLE = Field.create("pipe.roll.cycle")
            .withDisplayName("Pipe roll cycle")
            .withType(Type.STRING)
            .withDefault("MINUTELY");

    /** 本连接器的完整配置面:父 ALL_FIELDS + 4 新 Field(新 Set,不改父类静态集合)。 */
    public static final Field.Set ALL_FIELDS = PostgresConnectorConfig.ALL_FIELDS.with(SLOT_STREAMING, SLOT_TWO_PHASE, PIPE_DIR, PIPE_ROLL_CYCLE);

    /**
     * 以给定的不可变配置构造:单行 super 委派父构造器(public,负责快照模式、
     * 处理模式等既有配置项的解析与默认值回落);本类不新增构造期解析——4 个新项
     * 均由 getter 惰性读取,校验责任在 {@link #validateSlotStreaming}。
     *
     * @param config 连接器配置;应已通过 ALL_FIELDS 校验(未校验也能构造,行为按默认值回落)
     */
    public PostgresStreamConnectorConfig(Configuration config) {
        super(config);
    }

    /**
     * slot.streaming 的枚举 + 联合校验器(Field.ValidationOutput 三元契约):
     * 合法返回 0;值不在 OFF/ON/PARALLEL(大小写宽容)名单时向 problems 记
     * 1 条含合法值清单的消息并返回 1;值为 PARALLEL 且 slot.two.phase 显式/默认为
     * false 时,向 problems 记 SLOT_TWO_PHASE 视角的 1 条消息并返回 1
     * (注意:错误消息由 Field 校验框架统一落在被校验字段 slot.streaming 的
     * ConfigValue 上,problems.accept 的第一个参数只参与消息前缀格式化)。
     *
     * @param config   待校验的完整配置
     * @param field    被校验字段(恒为 SLOT_STREAMING)
     * @param problems 问题接收器;校验失败时恰好 accept 一次
     * @return 0 表示通过,1 表示发现 1 个问题
     */
    static int validateSlotStreaming(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        final StreamingMode mode;
        try {
            mode = StreamingMode.valueOf(value.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e) {
            problems.accept(field, value, "Invalid value '" + value + "': expected one of OFF, ON, PARALLEL (case-insensitive)");
            return 1;
        }
        if (mode == StreamingMode.PARALLEL && !config.getBoolean(SLOT_TWO_PHASE)) {
            problems.accept(SLOT_TWO_PHASE, false, "slot.streaming=parallel requires slot.two.phase=true");
            return 1;
        }
        return 0;
    }

    /**
     * 返回日志/指标上下文名(Debezium CdcSourceTaskContext 语境),
     * 覆盖父类以指向本模块而非 io.debezium 的 PG 连接器。
     *
     * @return 常量 {@link Module#CONTEXT_NAME}
     */
    @Override
    public String getContextName() {
        return Module.CONTEXT_NAME;
    }

    /**
     * 返回连接器逻辑名(配置校验与日志归因用),
     * 覆盖父类以指向本模块而非 io.debezium 的 PG 连接器。
     *
     * @return 常量 {@link Module#NAME}
     */
    @Override
    public String getConnectorName() {
        return Module.NAME;
    }

    /**
     * 解析流式档位:大写化后 valueOf,大小写宽容;非法值已由
     * {@link #validateSlotStreaming} 在启动期挡下,此处不再容错
     * (未经校验直接构造时若值非法将抛 IllegalArgumentException,属调用方违约)。
     *
     * @return OFF/ON/PARALLEL 之一;配置缺省时按 Field 默认值回落为 ON
     */
    public StreamingMode streamingMode() {
        return StreamingMode.valueOf(getConfig().getString(SLOT_STREAMING).toUpperCase(Locale.ROOT));
    }

    /**
     * 读取两阶段提交开关。
     *
     * @return 配置的布尔值;缺省时按 Field 默认值回落为 true
     */
    public boolean twoPhase() {
        return getConfig().getBoolean(SLOT_TWO_PHASE);
    }

    /**
     * 读取管道工作目录名。
     *
     * @return 配置的目录名;缺省时按 Field 默认值回落为 pg-stream-pipe-queue
     */
    public String pipeDir() {
        return getConfig().getString(PIPE_DIR);
    }

    /**
     * 读取管道滚动周期名(LegacyRollCycles 枚举名,由 MS2 管道装配解析)。
     *
     * @return 配置的周期名;缺省时按 Field 默认值回落为 MINUTELY
     */
    public String pipeRollCycle() {
        return getConfig().getString(PIPE_ROLL_CYCLE);
    }
}
