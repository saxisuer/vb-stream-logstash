package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;

import java.nio.file.Path;
import java.util.Arrays;

/**
 * spill（流式事务组装内存外溢）配置。不可变；默认值见设计文档 §7：
 * 阈值 64MiB、目录 {@code spill-queue}、滚动周期 MINUTELY，全部可经 {@code -Dvb.spill.*} 系统属性覆盖。
 * 阈值 {@code vb.spill.thresholdBytes=0}（或负数）是纯内存逃生门——组装器退回里程碑 1.5 的全堆内行为。
 *
 * @param thresholdBytes 全局 MEMORY 桶字节和阈值；&le;0 表示禁用 spill（见 {@link #spillEnabled()}）
 * @param dir            Chronicle Queue spill 目录（瞬态工作区，打开时整体清空重建，不跨重启续用）
 * @param rollCycle      spill 队列滚动周期（默认 {@link LegacyRollCycles#MINUTELY}），决定滚动文件粒度与删除水位档位
 */
public record SpillConfig(long thresholdBytes, Path dir, RollCycle rollCycle) {

    /**
     * 从系统属性构造配置：{@code vb.spill.thresholdBytes}（默认 67108864，即 64MiB）、
     * {@code vb.spill.dir}（默认 {@code spill-queue}）、{@code vb.spill.rollCycle}
     * （默认 {@code MINUTELY}，枚举名大小写宽容）。
     * 属性缺失或为空白串时取默认值；thresholdBytes 非法数字抛 {@link NumberFormatException}、
     * rollCycle 无法识别抛 {@link IllegalArgumentException}（消息附可用值列表），均属启动期 fail-fast。
     *
     * @return 按当前系统属性解析出的配置实例
     */
    public static SpillConfig fromSystemProperties() {
        return new SpillConfig(
                Long.parseLong(prop("vb.spill.thresholdBytes", "67108864")),
                Path.of(prop("vb.spill.dir", "spill-queue")),
                parseRollCycle(prop("vb.spill.rollCycle", "MINUTELY")));
    }

    /**
     * spill 是否启用：仅当阈值 &gt;0。阈值 &le;0 是显式的纯内存逃生门
     * （保留里程碑 1.5 行为作对照基线与故障回退路径），此时 dir/rollCycle 不参与任何 IO。
     *
     * @return 阈值为正返回 true；否则 false
     */
    public boolean spillEnabled() {
        return thresholdBytes > 0;
    }

    /**
     * 大小写宽容地解析滚动周期枚举名。只在 {@link LegacyRollCycles} 中查找：
     * chronicle-queue 2026.6 已把 MINUTELY/HOURLY/DAILY 从 {@code RollCycles} 迁入
     * {@code LegacyRollCycles}（spec §7 规定的默认值即 MINUTELY），且两者枚举名无重叠。
     * 未命中即抛 {@link IllegalArgumentException} 并附全部可用名——拼写错误应在启动期暴露而非静默回落。
     *
     * @param name 属性给出的枚举名（任意大小写）
     * @return 对应的 RollCycle 实例（LegacyRollCycles 枚举单例）
     */
    private static RollCycle parseRollCycle(String name) {
        return Arrays.stream(LegacyRollCycles.values())
                .filter(rc -> rc.name().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown vb.spill.rollCycle '%s', usable values: %s"
                                .formatted(name, Arrays.toString(LegacyRollCycles.values()))));
    }

    /**
     * 读系统属性，缺失或空白串（全空白视为未设置，与 ReplicationConfig 同约定）时回落默认值。
     *
     * @param key          属性名
     * @param defaultValue 默认值
     * @return 属性值或默认值，永不返回 null/空白
     */
    private static String prop(String key, String defaultValue) {
        String value = System.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
