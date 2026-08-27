package org.vastdata.vbstream.replication;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.ObjIntConsumer;
import java.util.stream.Stream;

/**
 * Chronicle Queue 溢写池（spill 池）生命周期管理（spec §4/§7）：SPILLED 桶单元以
 * {@link SpoolFrame} 帧字节原样落盘、按 CQ index 区间回读、按 cycle 低水位删除过老滚动文件。
 * 帧语义（编码/解码）完全由调用方负责，本类只搬运 byte[]。
 *
 * <p><b>瞬态工作区语义</b>：构造时先清空目录既有内容再建队列——重启后复制槽会从确认位点重发，
 * 残留的旧 spill 数据毫无价值且有害（陈旧 index 会让回读错位），故整体抹除重来。
 *
 * <p><b>线程约束</b>：非线程安全。appender/tailer 均为构造时创建的单实例资源，假定单写者
 * （复制读取线程）顺序调用全部公开方法，参照 {@code PgOutputDecoder} 的单写者假设。
 */
final class MessageSpool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MessageSpool.class);

    /** 滚动数据文件后缀（binary WireType 约定 {@code .cq4}；队列元数据表文件为 {@code .cq4t}，后缀不同天然排除）。 */
    private static final String ROLL_FILE_SUFFIX = ".cq4";

    private final Path dir;
    private final RollCycle rollCycle;
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;
    private final ExcerptTailer tailer;

    /**
     * 建池：清空目录内容后按指定滚动周期建单例 Chronicle Queue，并预创建 appender/tailer。
     * 关键步骤：确保目录存在（不存在则连父目录一起建）→ 递归删除目录下全部既有内容
     * （子项先于父项，删除失败抛 {@link UncheckedIOException} fail-fast）→
     * {@code ChronicleQueue.singleBuilder(dir).rollCycle(rollCycle).build()} 建队列 →
     * 预创建 appender 与 tailer（单实例复用，index 记账依赖同一 appender 的 lastIndexAppended）。
     * 边界与异常语义：目录不可清空/队列建不起来即抛异常，本实例半开资源由队列 build 失败时的
     * 内部语义兜底（wipe 发生在 build 之前，失败时无 CQ 资源可泄漏）；rollCycle/dir 为 null 抛 NPE。
     *
     * @param dir       溢写池目录（瞬态工作区，构造即清空）
     * @param rollCycle 滚动周期（决定滚动文件粒度与低水位删除档位）
     */
    MessageSpool(Path dir, RollCycle rollCycle) {
        this.dir = Objects.requireNonNull(dir, "dir 不能为 null");
        this.rollCycle = Objects.requireNonNull(rollCycle, "rollCycle 不能为 null");
        wipeDirectory(this.dir);
        this.queue = ChronicleQueue.singleBuilder(dir).rollCycle(rollCycle).build();
        this.appender = queue.createAppender();
        this.tailer = queue.createTailer();
        LOG.info("MessageSpool 已建立：目录 {}，滚动周期 {}", dir, rollCycle);
    }

    /**
     * 追加一条已帧化字节，返回其 CQ index（单调递增，可作回读区间端点与低水位入参）。
     * 关键步骤：{@code appender.writeBytes(BytesStore.wrap(framed))} 原样落盘 →
     * 以 {@code appender.lastIndexAppended()} 取刚写入条目的 index 返回。
     * 边界与异常语义：framed 为 null 抛 NPE；空数组合法（帧层只搬字节）；CQ 内部错误
     * （磁盘满、映射失败）按 CQ 运行时异常上抛——写入失败必须让调用方感知，不能静默丢单元。
     *
     * @param framed 调用方帧化完毕的完整帧字节（本类不解释内容）
     * @return 本条在队列中的 index（含 cycle 与序号）
     */
    long append(byte[] framed) {
        Objects.requireNonNull(framed, "framed 不能为 null");
        appender.writeBytes(BytesStore.wrap(framed));
        return appender.lastIndexAppended();
    }

    /**
     * 按 index 升序回读闭区间 [firstIndex..lastIndex] 的帧字节，逐条回调
     * {@code framedConsumer.accept(framed, ordinal)}（ordinal 为区间内 0 起递增序号）。
     * 关键步骤：tailer 置 FORWARD 方向后 {@code moveToIndex(firstIndex)} 起步 →
     * 循环 readingDocument：非 present 即到队尾停；元数据文档跳过；index 大于 lastIndex 停；
     * index 小于 firstIndex 跳过（防御落点偏早，正常不发生）；首个消费条目 index 必须等于
     * firstIndex，否则抛 {@link IllegalStateException}（moveToIndex 落点越过 first——
     * 区间起点已被删除或从未存在，属协议错位，fail-fast 不可猜测）→ 读出文档剩余字节为帧数组回调。
     * 边界与异常语义：区间不存在且队列无后续条目时（如清空重开后 readRange(0,100)）读到队尾
     * 自然空手而归、不抛异常；lastIndex 小于 firstIndex 时同样空手而归（无条目可满足区间）。
     *
     * @param firstIndex      区间起点 index（含）
     * @param lastIndex       区间终点 index（含）
     * @param framedConsumer 逐条消费者（帧字节副本，区间内 0 起序号）
     */
    void readRange(long firstIndex, long lastIndex, ObjIntConsumer<byte[]> framedConsumer) {
        Objects.requireNonNull(framedConsumer, "framedConsumer 不能为 null");
        tailer.direction(TailerDirection.FORWARD);
        tailer.moveToIndex(firstIndex);
        boolean firstSeen = false;
        int ordinal = 0;
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    return;                                 // 队列读尽（区间起点不存在时即空手而归）
                }
                if (dc.isMetaData()) {
                    continue;                               // 队列自管理的元数据文档，不属数据帧
                }
                long idx = dc.index();
                if (idx > lastIndex) {
                    return;                                 // 越过区间上界，停
                }
                if (idx < firstIndex) {
                    continue;                               // 落点偏早（防御），跳到区间起点
                }
                if (!firstSeen) {
                    if (idx != firstIndex) {
                        throw new IllegalStateException(
                                "readRange 区间起点错位：期望 index %d，实际落点 %d（区间必须存在，起点可能已被删除）"
                                        .formatted(firstIndex, idx));
                    }
                    firstSeen = true;
                }
                framedConsumer.accept(readFrameBytes(dc), ordinal++);
            }
        }
    }

    /**
     * 低水位释放：删除严格低于 {@code rollCycle.toCycle(lowestNeededIndex) - 1} 的 cycle 滚动文件
     * （保留 needed 档与 needed-1 档——上一档仍可能含低水位之前的在途序号，删除永远保守）。
     * 关键步骤：由 lowestNeededIndex 反解 needed cycle → 纯函数 {@link #deletableFiles} 算可删集 →
     * 逐个 {@code Files.deleteIfExists}，每删一个 WARN 文件名留痕；单文件删除失败仅 WARN 不上抛
     * （残留文件只占磁盘，不影响正确性，下次再删）→ 返回实际删除数。
     * 边界与异常语义：目录列举失败按"无可删"处理返回 0（WARN 留痕）。
     *
     * @param lowestNeededIndex 仍被需要的最低 index（含其所在 cycle 与上一 cycle 均保留）
     * @return 实际删除的滚动文件数
     */
    long releaseBelow(long lowestNeededIndex) {
        long neededCycle = rollCycle.toCycle(lowestNeededIndex);
        long deleted = 0;
        for (Path doomed : deletableFiles(rollCycle, dir, neededCycle)) {
            try {
                if (Files.deleteIfExists(doomed)) {
                    LOG.warn("MessageSpool 已删除滚动文件 {}（低水位 cycle {} 之下）", doomed, neededCycle);
                    deleted++;
                }
            } catch (IOException e) {
                LOG.warn("MessageSpool 删除滚动文件 {} 失败（下次释放重试）", doomed, e);
            }
        }
        return deleted;
    }

    /**
     * 最近一次 append 返回的 index；未 append 过时 CQ 抛 {@link IllegalStateException}
     * （调用方保证先 append 后查询）。
     *
     * @return 最近写入条目的 index
     */
    long lastAppendedIndex() {
        return appender.lastIndexAppended();
    }

    /**
     * 释放池资源：tailer → appender → queue 逆序关闭，任何一步失败仅 WARN 不上抛
     * （close 不应掩盖业务异常；最坏代价是句柄延迟回收）。
     */
    @Override
    public void close() {
        try {
            tailer.close();
        } catch (RuntimeException e) {
            LOG.warn("MessageSpool tailer 关闭失败（忽略）", e);
        }
        try {
            appender.close();
        } catch (RuntimeException e) {
            LOG.warn("MessageSpool appender 关闭失败（忽略）", e);
        }
        try {
            queue.close();
            LOG.info("MessageSpool 已关闭：目录 {}", dir);
        } catch (RuntimeException e) {
            LOG.warn("MessageSpool queue 关闭失败（忽略）", e);
        }
    }

    /**
     * 纯函数删除数学：列出 dir 下 cycle 号严格低于 {@code neededCycle - 1} 的滚动文件
     * （保留 needed 与 needed-1 档），不执行任何 IO 删除——供单测注入假文件名验证。
     * 关键步骤：目录不存在或列举失败返回空列表（保守：无可删）→ 过滤以 {@code .cq4} 结尾的常规文件
     * → 剥后缀按 {@code rc.format()} 模式解析为 UTC 时间戳 → 换算 cycle 号
     * {@code (epochMillis - defaultEpoch) / lengthInMillis}（与 {@code rc.toCycle} 同基）→
     * 严格小于 {@code neededCycle - 1} 者入选。
     * 边界与异常语义：文件名剥后缀后解析失败（不匹配滚动周期时间戳模式）跳过并 WARN——
     * 外来文件宁可漏删不可错删；队列元数据文件 {@code metadata.cq4t} 后缀不同，天然不在候选内。
     * 线程约束：无状态纯读，线程安全。
     *
     * @param rc          滚动周期（提供文件名格式与 cycle 换算参数）
     * @param dir         滚动文件所在目录
     * @param neededCycle 仍需保留的 cycle 号（其本身与减一档均保留）
     * @return 可删除文件的列表（可能为空，不保证顺序）
     */
    static List<Path> deletableFiles(RollCycle rc, Path dir, long neededCycle) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        DateTimeFormatter format = DateTimeFormatter.ofPattern(rc.format(), Locale.ROOT);
        try (Stream<Path> entries = Files.list(dir)) {
            return entries
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.endsWith(ROLL_FILE_SUFFIX);
                    })
                    .filter(p -> parseCycle(rc, format, p) < neededCycle - 1)
                    .toList();
        } catch (IOException e) {
            LOG.warn("MessageSpool 列举滚动目录 {} 失败，本次不删除", dir, e);
            return List.of();
        }
    }

    /**
     * 解析单个滚动文件的 cycle 号：剥掉 {@code .cq4} 后缀后按滚动周期格式读作 UTC 时间戳，
     * 再按周期时长换算距纪元的档位数。解析失败（后缀剥掉后不匹配格式）返回 {@link Long#MAX_VALUE}
     * 并 WARN——永不入选删除集（外来文件宁可漏删不可错删）。
     *
     * @param rc     滚动周期
     * @param format 与 rc.format() 对应的解析器
     * @param file   候选文件
     * @return 文件名对应的 cycle 号；无法解析返回 Long.MAX_VALUE
     */
    private static long parseCycle(RollCycle rc, DateTimeFormatter format, Path file) {
        String name = file.getFileName().toString();
        String stem = name.substring(0, name.length() - ROLL_FILE_SUFFIX.length());
        try {
            LocalDateTime timestamp = LocalDateTime.parse(stem, format);
            long epochMillis = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
            return (epochMillis - rc.defaultEpoch()) / rc.lengthInMillis();
        } catch (DateTimeParseException e) {
            LOG.warn("MessageSpool 跳过无法解析的滚动文件名 {}（不匹配周期 {} 格式）", name, rc.format());
            return Long.MAX_VALUE;
        }
    }

    /**
     * 递归清空目录内容（保留目录本身）：深度优先逆序删除（子项先于父项），任一删除失败抛
     * {@link UncheckedIOException} fail-fast——瞬态工作区语义要求"必须干净"，清不干净即拒绝开池。
     *
     * @param dir 待清空目录（不存在则先创建）
     */
    private static void wipeDirectory(Path dir) {
        try {
            Files.createDirectories(dir);
            try (Stream<Path> paths = Files.walk(dir)) {
                paths.filter(p -> !p.equals(dir))
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                                throw new UncheckedIOException("清空溢写目录失败：" + p, e);
                            }
                        });
            }
        } catch (IOException e) {
            throw new UncheckedIOException("无法准备溢写目录 " + dir, e);
        }
    }

    /**
     * 读出当前文档的剩余字节为帧数组副本：readingDocument 定位后 wire 的 bytes 已指向
     * 文档载荷起点，readRemaining 即载荷长度（writeBytes 写入的原样字节）。
     *
     * @param dc 已定位到数据文档的上下文
     * @return 载荷字节副本（与队列内存不共享）
     */
    private static byte[] readFrameBytes(DocumentContext dc) {
        Bytes<?> bytes = dc.wire().bytes();
        byte[] framed = new byte[(int) bytes.readRemaining()];
        bytes.read(framed);
        return framed;
    }
}
