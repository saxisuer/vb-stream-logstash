package org.vastdata.vbstream.reader.format;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 落地文件命名与序号管理。
 *
 * <pre>&lt;task&gt;-&lt;seq 16位零填充&gt;-&lt;yyyyMMddHHmmss&gt;.bin</pre>
 *
 * seq 单调递增；16 位零填充保证<b>文件名字典序 = 消费顺序</b>（滚动写入、按序消费共用此约定）。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 FileNaming（2026-09-11 快照）。
 */
public final class FileNaming {

    public static final int SEQ_WIDTH = 16;
    public static final long MAX_SEQ = 9_999_999_999_999_999L;

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final Pattern NAME = Pattern.compile("(.+)-(\\d{" + SEQ_WIDTH + "})-(\\d{14})\\.(bin|json)");

    private FileNaming() {
    }

    /** 责任：组装文件名。边界：seq 域外（负数或超 16 位上限）抛 IAE。 */
    public static String fileName(String task, long seq, LocalDateTime time, String extension) {
        if (seq < 0 || seq > MAX_SEQ) {
            throw new IllegalArgumentException("seq 超出 %d 位上限: %d".formatted(SEQ_WIDTH, seq));
        }
        return ("%s-%0" + SEQ_WIDTH + "d-%s.%s").formatted(task, seq, TS.format(time), extension);
    }

    /** 解析文件名中的 seq；非法命名返回 empty（宽容——目录里可能混有其他文件）。 */
    public static OptionalLong parseSeqOpt(String fileName) {
        Matcher m = NAME.matcher(fileName);
        if (!m.matches()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(m.group(2)));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    /** 扫描目录取该任务当前最大 seq + 1（重启恢复序号，不依赖额外状态文件）；目录为空返回 1。 */
    public static long nextSeq(Path dataDir, String task) {
        try (Stream<Path> files = Files.list(dataDir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(n -> n.startsWith(task + "-"))
                    .map(FileNaming::parseSeqOpt)
                    .filter(OptionalLong::isPresent)
                    .mapToLong(OptionalLong::getAsLong)
                    .max().orElse(0L) + 1;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
