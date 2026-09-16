package org.vastdata.vbstream.reader;

import org.vastdata.vbstream.reader.file.OutputFormat;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * reader 的输出形态配置：{@code vb.reader.mode=log}（默认，逐条 INFO 渲染）或
 * {@code file}（CDC 记录落地为文件——tmp → fsync → 原子 rename，COMMIT 边界
 * 切分，offset 与文件 publish 严格联动，见 file 包）。file 形态的落地格式由
 * {@code vb.reader.format} 选择：{@code binary}（默认，VBFG 二进制）或 {@code sql}
 * （可执行 SQL 文本），见 {@link OutputFormat}。
 *
 * <p>键来源两层（与 {@link ReaderProperties} 的三层合并同风格，输出形态层无 Debezium
 * 语义）：配置文件的 {@code reader.*} 裸键（reader 自用输出形态命名空间，不透传
 * Debezium；取名 {@code reader.*} 以避开 engine 已占用的 {@code vb.output.*}）为基础值，
 * 系统属性 {@code -Dvb.reader.*} 覆盖，默认值兜底。{@code task} 缺省取 {@code topic.prefix}
 * （落地文件名前缀与任务标识同源）。纯数据 record，解析入口见
 * {@link ReaderProperties#resolveOutput(Properties)}。
 */
public record OutputConfig(Mode mode, OutputFormat format, Path dataDir, Path tmpDir, String task,
                           int rollMaxRecords, long rollIntervalMs) {

    /** 输出形态：LOG（INFO 渲染）/ FILE（VBFG 落地文件）。 */
    public enum Mode {LOG, FILE}

    /** 条数切分上限的兜底默认（对齐 vb-cdc-file-transform 的 capture.roll.max-records）。 */
    static final int DEFAULT_ROLL_MAX_RECORDS = 1000;

    /** 时长切分间隔的兜底默认毫秒（对齐 vb-cdc-file-transform 的 capture.roll.interval-ms）。 */
    static final long DEFAULT_ROLL_INTERVAL_MS = 10_000L;

    /**
     * 责任：从已合并的输出形态键集组装 {@link OutputConfig}。关键步骤：mode 解析（大小写宽容，
     * 非法值抛 IAE——启动期 fail-fast，与 {@code vb.output.mode} 同哲学）→ format 解析
     * （{@link OutputFormat#parse}，缺省 binary，非法值同样 fail-fast）→ 目录/滚动参数
     * 带默认读取 → task 缺省取 Debezium props 的 {@code topic.prefix}（再兜底
     * {@code vb-stream-reader}）。边界：数值键非法抛 NumberFormatException（同样启动期暴露）。
     *
     * @param readerProps    已合并的 reader.* 键集（文件基础值被系统属性覆盖后的终态）
     * @param debeziumProps Debezium props（仅读 topic.prefix 作 task 兜底）
     * @return 组装完成的输出形态配置
     */
    static OutputConfig from(Map<String, String> readerProps, Properties debeziumProps) {
        String modeValue = readerProps.getOrDefault("reader.mode", "log");
        Mode mode = switch (modeValue.toLowerCase(Locale.ROOT)) {
            case "log" -> Mode.LOG;
            case "file" -> Mode.FILE;
            default -> throw new IllegalArgumentException(
                    "未知的 vb.reader.mode: " + modeValue + "（可选 log|file）");
        };
        OutputFormat format = OutputFormat.parse(readerProps.getOrDefault("reader.format", "binary"));
        String task = readerProps.getOrDefault("reader.task",
                debeziumProps.getProperty("topic.prefix", "vb-stream-reader"));
        int maxRecords = Integer.parseInt(
                readerProps.getOrDefault("reader.roll.max-records", String.valueOf(DEFAULT_ROLL_MAX_RECORDS)));
        long intervalMs = Long.parseLong(
                readerProps.getOrDefault("reader.roll.interval-ms", String.valueOf(DEFAULT_ROLL_INTERVAL_MS)));
        return new OutputConfig(mode, format,
                Path.of(readerProps.getOrDefault("reader.data-dir", "data/cdc-files")),
                Path.of(readerProps.getOrDefault("reader.tmp-dir", "data/cdc-tmp")),
                task, maxRecords, intervalMs);
    }
}
