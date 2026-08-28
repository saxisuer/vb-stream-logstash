package org.vastdata.vbstream.replication;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.LegacyRollCycles;

import java.nio.file.Path;
import java.util.Arrays;

/**
 * 解耦管道（reader → Chronicle Queue → consumer 的主缓冲）配置。不可变；默认值见 1.7 设计 §8：
 * 目录 {@code pipe-queue}、滚动周期 MINUTELY，可经 {@code -Dvb.pipe.*} 覆盖。
 * 管道是解耦架构的地基（没有"禁用"逃生门——绕过管道等于回到 1.6 同步阻塞形态）。
 *
 * @param dir       管道目录（瞬态工作区，打开时整体清空重建，不跨重启续用；真源是复制槽）
 * @param rollCycle 管道队列滚动周期（默认 {@link LegacyRollCycles#MINUTELY}），决定滚动文件粒度与删除水位档位
 */
public record PipeConfig(Path dir, RollCycle rollCycle) {

    /**
     * 从系统属性构造配置：{@code vb.pipe.dir}（默认 {@code pipe-queue}）、{@code vb.pipe.rollCycle}
     * （默认 {@code MINUTELY}，枚举名大小写宽容）。属性缺失或空白取默认；rollCycle 无法识别抛
     * {@link IllegalArgumentException}（消息附可用值列表），启动期 fail-fast。
     *
     * @return 按当前系统属性解析出的配置实例
     */
    public static PipeConfig fromSystemProperties() {
        return new PipeConfig(
                Path.of(prop("vb.pipe.dir", "pipe-queue")),
                parseRollCycle(prop("vb.pipe.rollCycle", "MINUTELY")));
    }

    /**
     * 大小写宽容地解析滚动周期枚举名。只在 {@link LegacyRollCycles} 中查找：
     * chronicle-queue 2026.6 已把 MINUTELY/HOURLY/DAILY 从 {@code RollCycles} 迁入
     * {@code LegacyRollCycles}（1.7 设计 §8 规定的默认值即 MINUTELY），且两者枚举名无重叠。
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
                        "unknown vb.pipe.rollCycle '%s', usable values: %s"
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
