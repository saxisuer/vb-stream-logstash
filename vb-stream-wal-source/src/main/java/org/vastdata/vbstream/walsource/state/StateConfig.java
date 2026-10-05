package org.vastdata.vbstream.walsource.state;

import java.nio.file.Path;
import java.util.Properties;

/**
 * 检查点持久化的配置面（spec §7：目录 {@code vb.wal.state.dir} 可配，周期
 * {@code vb.wal.state.interval.ms} / 事件阈值 {@code vb.wal.state.events} 先到为准）
 * ——三键的单一解析点，{@link org.vastdata.vbstream.walsource.replay.CatalogSynchronizer}
 * 与 {@link org.vastdata.vbstream.walsource.api.WalSource} 共用，默认值不双写。
 *
 * <p><strong>启停语义</strong>：{@code dir == null}（键缺省或显式空串）= 检查点
 * 禁用——同步器只重放不落盘、不推槽，行为等同 Task 13 前的无状态形态；dir 非 null
 * 即启用。record 不可变，构造后跨线程共享安全。</p>
 *
 * @param dir             检查点目录（null = 禁用；{@link StateStore#FILE_NAME} 落于此）
 * @param intervalMs      周期检查点间隔（毫秒，默认 30s；&le;0 构造期 IAE）
 * @param eventsThreshold 事件数阈值（catalog 行事件条数，默认 1000；&le;0 构造期 IAE）
 */
public record StateConfig(Path dir, long intervalMs, long eventsThreshold) {

    /** 检查点目录键（缺省/空串 = 禁用）。 */
    public static final String KEY_DIR = "vb.wal.state.dir";

    /** 周期检查点间隔键（毫秒，默认 {@value #DEFAULT_INTERVAL_MS}）。 */
    public static final String KEY_INTERVAL_MS = "vb.wal.state.interval.ms";

    /** 事件数阈值键（catalog 行事件条数，默认 {@value #DEFAULT_EVENTS_THRESHOLD}）。 */
    public static final String KEY_EVENTS = "vb.wal.state.events";

    /** 周期检查点间隔默认值（毫秒，spec §7 的 30s）。 */
    public static final long DEFAULT_INTERVAL_MS = 30_000;

    /** 事件数阈值默认值（条，spec §7 的 1000）。 */
    public static final long DEFAULT_EVENTS_THRESHOLD = 1_000;

    /**
     * 紧凑构造校验：两个节拍值必须为正（&le;0 的节拍永触发或永不触发，属配置面
     * 错误——fail-fast 优于静默跑错节拍）；dir 不校验存在性（{@link StateStore}
     * 惰性建目录）。
     */
    public StateConfig {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("vb.wal.state.interval.ms 须为正: " + intervalMs);
        }
        if (eventsThreshold <= 0) {
            throw new IllegalArgumentException("vb.wal.state.events 须为正: " + eventsThreshold);
        }
    }

    /**
     * 从配置源解析三键（缺键取默认；dir 键缺省或空白 = null 即禁用）。
     *
     * <p>关键步骤：dir 取 {@link #KEY_DIR} 原值，null 或 {@code trim()} 后空判禁用；
     * 间隔/阈值各自 {@code getProperty(键, 默认值文本)} 后 {@code Long.parseLong}——
     * 非数字构造期 NumberFormatException（fail-fast，与 WalSource 端口键同语义）。</p>
     *
     * @param cfg 配置源（典型 WalSource 的六键 + state 三键 Properties）
     * @return 解析产物（dir 可为 null = 禁用）
     */
    public static StateConfig fromProperties(Properties cfg) {
        String dirText = cfg.getProperty(KEY_DIR);
        Path dir = dirText == null || dirText.isBlank() ? null : Path.of(dirText);
        long intervalMs = Long.parseLong(cfg.getProperty(KEY_INTERVAL_MS, String.valueOf(DEFAULT_INTERVAL_MS)));
        long events = Long.parseLong(cfg.getProperty(KEY_EVENTS, String.valueOf(DEFAULT_EVENTS_THRESHOLD)));
        return new StateConfig(dir, intervalMs, events);
    }

    /**
     * 以默认节拍启用检查点（目录即可达性由 StateStore 惰性处理）。
     *
     * @param dir 检查点目录
     * @return 默认节拍（30s / 1000 条）的配置
     */
    public static StateConfig enabled(Path dir) {
        return new StateConfig(dir, DEFAULT_INTERVAL_MS, DEFAULT_EVENTS_THRESHOLD);
    }

    /**
     * 检查点是否启用（dir 非 null 即启用——落盘/续传/槽推进整个生命周期面挂此判定）。
     *
     * @return 启用为 true
     */
    public boolean enabled() {
        return dir != null;
    }
}
