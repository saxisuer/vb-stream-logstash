package org.vastdata.vbstream.reader.file;

import org.vastdata.vbstream.format.FileNaming;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * file 输出形态的落地格式:格式枚举即工厂——每值携带文件扩展名与写入器构造,滚动/发布/offset
 * 联动收在格式无关的 {@link FileRollingWriter} 外壳。移植自 vb-cdc-file-transform 仓
 * cdc-capture 的 OutputFormat(2026-09-16 快照;json 不实现,见 spec §8——接口保留扩展点)。
 *
 * <p>扩展名即消费分派依据(文件名字典序 = 消费顺序与格式无关);binary 与 vb-cdc-file-transform
 * 的 cdc-sink 逐字节互通,sql 为可直接执行的语句文本。
 */
public enum OutputFormat {

    /** VBFG 二进制格式(默认)——与对方仓 cdc-sink 消费端互通。 */
    BINARY("bin"),

    /** SQL 文本格式——IR 渲染的可执行语句(裸重放语义,主键冲突由执行方处理)。 */
    SQL("sql");

    private final String extension;

    OutputFormat(String extension) {
        this.extension = extension;
    }

    /** 落地文件的扩展名(文件名契约见 {@link FileNaming})。 */
    public String extension() {
        return extension;
    }

    /**
     * 责任:按格式建事件写入器(tmp .part 文件的实现细节由各格式自理)。
     * 边界:构造 IO 失败原样上抛 IOException(调用方 FileRollingWriter 停引擎,offset 不动)。
     */
    EventFileWriter newWriter(Path file, int seq, String task) throws IOException {
        return switch (this) {
            case BINARY -> new VbfgEventWriter(file, seq, task);
            case SQL -> new SqlEventWriter(file, seq, task);
        };
    }

    /**
     * 责任:配置值 → 格式(大小写宽容)。边界:非法值抛 IAE(启动期 fail-fast,报错文案
     * 指向 {@code reader.format} 键与可选值)。
     */
    public static OutputFormat parse(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "binary" -> BINARY;
            case "sql" -> SQL;
            default -> throw new IllegalArgumentException(
                    "未知的 reader.format: " + value + "(可选 binary, sql)");
        };
    }
}
