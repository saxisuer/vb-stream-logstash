package org.vastdata.vbstream.replication;

import java.util.Arrays;

/**
 * 输出形态（2.0 spec §1.1）：STREAMING=流式事件交付（默认，回放期堆 O(单条)——逐条解码逐条
 * 回调，半截事务可能已部分输出，End 返回即完整消费确认）；BLOCK=边界适配器重组整块
 * （1.7 语义逃生门，堆 O(事务)、原子交付——{@code vb.output.mode=block} 启用）。
 *
 * <p>两模式的内存语义差异（内存有界性的最终一步）：STREAMING 下回放期堆内不再物化整事务
 * 变更列表；BLOCK 经 {@link BlockOutputAdapter} 攒集，恢复 1.7 的 O(事务) 瞬态。
 */
public enum OutputMode {
    STREAMING, BLOCK;

    /**
     * 读系统属性 {@code vb.output.mode}（默认 STREAMING，大小写宽容；未知值抛
     * {@link IllegalArgumentException} 附可用值——风格同 PipeConfig 的 rollCycle 解析，
     * 启动期 fail-fast）。
     *
     * @return 输出形态枚举值
     */
    public static OutputMode fromSystemProperties() {
        String v = System.getProperty("vb.output.mode", "streaming").trim();
        return Arrays.stream(values()).filter(m -> m.name().equalsIgnoreCase(v)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown vb.output.mode '%s', usable values: %s".formatted(v, Arrays.toString(values()))));
    }
}
