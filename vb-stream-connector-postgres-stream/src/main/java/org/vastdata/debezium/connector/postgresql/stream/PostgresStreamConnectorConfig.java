package org.vastdata.debezium.connector.postgresql.stream;

import java.util.Arrays;
import java.util.Locale;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;
import org.apache.kafka.common.config.ConfigDef.Type;

import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.postgresql.PostgresConnectorConfig;

/**
 * 流式连接器配置:在父类 {@link PostgresConnectorConfig} 的完整配置面之上追加
 * 六个本模块专属配置项——slot.streaming(流式档位)、slot.two.phase(两阶段提交)、
 * pipe.dir / pipe.roll.cycle(reader 与 consumer 之间的 Chronicle Queue 管道参数,
 * 对应引擎侧 {@code vb.pipe.dir} / {@code vb.pipe.rollCycle})、slot.feedback.interval.ms
 * (复制会话的 LSN 反馈节流周期,对应引擎侧 {@code vb.pg.feedbackSeconds},默认 10 秒)、
 * slot.messages('M' 逻辑消息门控,MS3.5,默认 false)。
 *
 * <p>校验语义(spec §5.1 启动期 fail-fast):slot.streaming 只接受
 * OFF/ON/PARALLEL(大小写宽容);PARALLEL 档必须搭配 slot.two.phase=true,
 * 否则 {@link #validateSlotStreaming} 记 1 条问题并使配置整体不通过——
 * PG 侧 parallel 流式解码以 two_phase 为前置,漏配到运行期才失败代价过高。
 * pipe.roll.cycle 只接受 {@link LegacyRollCycles} 枚举名(大小写宽容,引擎
 * PipeConfig.parseRollCycle 同语义),未知值由 {@link #validateRollCycle} 记 1 条
 * 附可用值清单的问题——拼写错误残余到建管道才炸会拖垮 reader 线程,且管道目录可能已被 wipe。
 *
 * <p>{@link #ALL_FIELDS} = 父 ALL_FIELDS 扩展 6 新 Field(floor 语义,父类必填项、
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

    /** 管道滚动周期:LegacyRollCycles 枚举名(大小写宽容,validateRollCycle 启动期校验),默认 MINUTELY。 */
    public static final Field PIPE_ROLL_CYCLE = Field.create("pipe.roll.cycle")
            .withDisplayName("Pipe roll cycle")
            .withType(Type.STRING)
            .withDefault("MINUTELY")
            .withValidation(PostgresStreamConnectorConfig::validateRollCycle);

    /** LSN 反馈间隔(毫秒):复制会话 run 轮询循环 forceUpdateStatus 的节流周期(确认值经输出前沿封顶),默认 10000(=10 秒),正整数校验。 */
    public static final Field SLOT_FEEDBACK_INTERVAL_MS = Field.create("slot.feedback.interval.ms")
            .withDisplayName("Slot feedback interval (ms)")
            .withType(Type.INT)
            .withDefault(10000)
            .withValidation(Field::isPositiveInteger);

    /**
     * 'M' 逻辑消息门控(MS3.5,spec §3.1):true 时 START_REPLICATION 槽选项追加第 5 项
     * {@code messages=true}(PG 14+,vanilla Debezium 同款)——PG 开始下发非事务/事务性
     * 逻辑消息,连接器逐条解析记录(INFO 日志留痕,内容经 {@link MessagePreview} 预览)
     * 且非事务消息参与输出前沿的安全推进(护栏 {@code StreamedTransactionAssembler.
     * safeMessageAdvance},确认值压到未输出桶 commitLsn 之下)。
     * <b>不发射下游</b>(不进 Kafka topic,发射仍延期)。默认 false——槽选项维持 4 项,
     * PG 不下发 'M',行为与 MS3 及之前完全一致。
     */
    public static final Field SLOT_MESSAGES = Field.create("slot.messages")
            .withDisplayName("Slot messages")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withValidation(Field::isBoolean);

    /** 本连接器的完整配置面:父 ALL_FIELDS + 6 新 Field(新 Set,不改父类静态集合)。 */
    public static final Field.Set ALL_FIELDS = PostgresConnectorConfig.ALL_FIELDS.with(SLOT_STREAMING, SLOT_TWO_PHASE, PIPE_DIR, PIPE_ROLL_CYCLE,
            SLOT_FEEDBACK_INTERVAL_MS, SLOT_MESSAGES);

    /**
     * 以给定的不可变配置构造:单行 super 委派父构造器(public,负责快照模式、
     * 处理模式等既有配置项的解析与默认值回落);本类不新增构造期解析——5 个新项
     * 均由 getter 惰性读取,校验责任在 {@link #validateSlotStreaming} 与各 Field
     * 声明的校验器。
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
     * pipe.roll.cycle 的枚举校验器(Field.ValidationOutput 三元契约,引擎 PipeConfig.
     * parseRollCycle 的校验面形态):合法返回 0;值不在 {@link LegacyRollCycles} 枚举名
     * 名单(大小写宽容)时向 problems 记 1 条附可用值清单的消息并返回 1——拼写错误应在
     * 启动期暴露而非静默回落(残余到建管道才 IAE 会拖垮 reader 线程)。
     *
     * @param config   待校验的完整配置
     * @param field    被校验字段(恒为 PIPE_ROLL_CYCLE)
     * @param problems 问题接收器;校验失败时恰好 accept 一次
     * @return 0 表示通过,1 表示发现 1 个问题
     */
    static int validateRollCycle(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        try {
            parseRollCycle(value);
            return 0;
        }
        catch (IllegalArgumentException e) {
            problems.accept(field, value, "Invalid value '" + value + "': expected one of "
                    + Arrays.toString(LegacyRollCycles.values()) + " (case-insensitive)");
            return 1;
        }
    }

    /**
     * 大小写宽容地解析滚动周期枚举名(引擎 PipeConfig.parseRollCycle 逐行同语义)。只在
     * {@link LegacyRollCycles} 中查找:chronicle-queue 2026.6 已把 MINUTELY/HOURLY/DAILY
     * 从 {@code RollCycles} 迁入 {@code LegacyRollCycles},且两者枚举名无重叠。
     * 未命中即抛 {@link IllegalArgumentException} 并附全部可用名。校验器与 {@link #rollCycle()}
     * 共用此解析,保证两侧口径一致。
     *
     * @param name 配置给出的枚举名(任意大小写)
     * @return 对应的 RollCycle 实例(LegacyRollCycles 枚举单例)
     */
    private static RollCycle parseRollCycle(String name) {
        return Arrays.stream(LegacyRollCycles.values())
                .filter(rc -> rc.name().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown pipe.roll.cycle '%s', usable values: %s"
                                .formatted(name, Arrays.toString(LegacyRollCycles.values()))));
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
     * 读取管道工作目录名(MessagePipe 构造方自行 {@code Path.of} 成路径——瞬态工作区,
     * 相对工作目录解析)。
     *
     * @return 配置的目录名;缺省时按 Field 默认值回落为 pg-stream-pipe-queue
     */
    public String pipeDir() {
        return getConfig().getString(PIPE_DIR);
    }

    /**
     * 解析管道滚动周期为枚举单例(MessagePipe 构造的直接入参形态):经
     * {@link #parseRollCycle} 大小写宽容解析。非法值已由 {@link #validateRollCycle}
     * 在启动期挡下,此处不再容错(未经校验直接构造时若值非法将抛
     * IllegalArgumentException,属调用方违约)。
     *
     * @return LegacyRollCycles 枚举单例;缺省时按 Field 默认值回落为 MINUTELY
     */
    public RollCycle rollCycle() {
        return parseRollCycle(getConfig().getString(PIPE_ROLL_CYCLE));
    }

    /**
     * 读取 LSN 反馈间隔并换算为秒:复制会话的 run 循环(pgjdbc withStatusInterval 同
     * 粒度)以秒节流 forceUpdateStatus,配置面按 Kafka Connect 惯例用毫秒。整除换算——
     * 亚秒值(如 500)截断为 0,即每轮都反馈(最快档,间隔计时永不满),不会静默翻倍。
     *
     * @return 配置毫秒值整除 1000;缺省时按 Field 默认值回落为 10
     */
    public int feedbackIntervalSeconds() {
        return getConfig().getInteger(SLOT_FEEDBACK_INTERVAL_MS) / 1000;
    }

    /**
     * 读取 'M' 逻辑消息门控开关:开启后复制会话的槽选项追加 {@code messages=true}
     * (PG 开始下发逻辑消息),连接器对 'M' 逐条解析记录并让非事务消息参与前沿安全推进
     * (不发射下游——语义细节见 {@link #SLOT_MESSAGES} 的 Field javadoc)。
     *
     * @return 配置的布尔值;缺省时按 Field 默认值回落为 false
     */
    public boolean messagesEnabled() {
        return getConfig().getBoolean(SLOT_MESSAGES);
    }
}
