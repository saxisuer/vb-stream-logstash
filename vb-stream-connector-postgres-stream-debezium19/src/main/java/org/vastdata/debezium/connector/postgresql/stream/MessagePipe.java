package org.vastdata.debezium.connector.postgresql.stream;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

/**
 * 主缓冲管道(引擎 1.7 设计 §4.2):一条 CQ 记录 = 一条完整 pgoutput 消息(含控制消息,
 * 为建立 seq 时间线),无信封帧——CQ 自身的 index 即消息 seq(readRange 逐条回调真实 index),
 * 调用方不再帧化/解帧,本类只负责搬运 byte[]:按 index 区间回读,按 cycle 低水位删除过老的
 * 滚动文件。引擎 {@code org.vastdata.vbstream.replication.MessagePipe}(351 行)的 1:1 重写
 * (文字参照,非依赖)。
 *
 * <p><b>瞬态工作区</b>:构造时先清空目录里已有的内容再建队列。真源是复制槽,重启后 PG 会从
 * 确认位点重发,残留的旧管道数据毫无价值且有害(陈旧的 index 会让回读错位),所以整体抹掉
 * 重来。由此还有一个推论:<b>管道目录在进程内独占</b>——同一 JVM 里第二个实例指向同一目录,
 * 会把前一个实例的队列文件清掉;要多实例并存必须各配各的目录。
 *
 * <p><b>线程约束</b>:跨线程分工——{@code append}/{@code lastAppendedIndex}/
 * {@code releaseBelow}/{@code close} 由 reader 线程调用,{@code readRange} 由 consumer
 * 线程调用。appender 与 tailer 都是构造时创建的单实例资源,各自单线程使用;跨线程的可见性
 * 与顺序由 Chronicle Queue 的单 appender/多 tailer 内存模型保证(官方支持的使用方式)。
 * 两类方法不得交叉线程调用(appender 非线程安全,tailer 的 moveToIndex 游标也只属于一个线程)。
 */
final class MessagePipe implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MessagePipe.class);

    /** 滚动数据文件后缀(binary WireType 约定 {@code .cq4};队列元数据表文件为 {@code .cq4t},后缀不同天然排除)。 */
    private static final String ROLL_FILE_SUFFIX = ".cq4";

    /**
     * 滚动周期格式 → 解析器的进程级缓存(引擎 1.7.1 Task 3 修复:原每次 {@link #deletableFiles} 调用
     * 都 {@code DateTimeFormatter.ofPattern} 新建解析器)。{@link DateTimeFormatter} 不可变且线程
     * 安全,按模式串记忆化后语义不变;周期格式是有限枚举(RollCycle 实现集合),缓存有界。
     */
    private static final ConcurrentMap<String, DateTimeFormatter> FORMATTER_BY_PATTERN = new ConcurrentHashMap<>();

    private final Path dir;
    private final RollCycle rollCycle;
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;
    private final ExcerptTailer tailer;

    /**
     * 上次 {@link #releaseBelow} 实际扫描过的 needed cycle 档位({@link Long#MIN_VALUE} = 从未
     * 扫过):节流状态(引擎 1.7.1 Task 3 修复——组装器每个桶完结点都调 releaseBelow,而滚动
     * 周期 MINUTELY 下档位极少推进,同档位重复扫描是纯浪费)。只在 reader 线程读写,无并发问题。
     */
    private long lastScannedCycle = Long.MIN_VALUE;

    /**
     * 建管道:先清空目录内容,再按指定滚动周期建一个 Chronicle Queue,并预先创建 appender/tailer。
     *
     * <p>步骤:目录不存在就连父目录一起建 → 递归删除目录下全部已有内容(子项先于父项删,
     * 删不掉抛 {@link UncheckedIOException} fail-fast)→ 建队列 → 预创建 appender 与 tailer
     * (单实例反复用,index 记账依赖同一个 appender 的 lastIndexAppended)。
     *
     * <p>边界:目录清不掉或队列建不起来直接抛异常(清空发生在建队列之前,失败时没有 CQ 资源
     * 可泄漏);rollCycle/dir 为 null 抛 NPE。
     *
     * @param dir       管道目录(瞬态工作区,构造时清空)
     * @param rollCycle 滚动周期(决定滚动文件的粒度与低水位删除的档位)
     */
    MessagePipe(Path dir, RollCycle rollCycle) {
        this.dir = Objects.requireNonNull(dir, "dir 不能为 null");
        this.rollCycle = Objects.requireNonNull(rollCycle, "rollCycle 不能为 null");
        wipeDirectory(this.dir);
        this.queue = ChronicleQueue.singleBuilder(dir).rollCycle(rollCycle).build();
        this.appender = queue.createAppender();
        this.tailer = queue.createTailer();
        LOG.info("MessagePipe 已建立:目录 {},滚动周期 {}", dir, rollCycle);
    }

    /**
     * 追加一条完整消息字节,返回其 CQ index(单调递增,即该消息的 seq,可作回读区间端点与
     * 低水位入参)。关键步骤:{@code appender.writeBytes(BytesStore.wrap(payload))} 原样落盘 →
     * 以 {@code appender.lastIndexAppended()} 取刚写入条目的 index 返回。
     * 边界与异常语义:payload 为 null 抛 NPE;空数组合法(本类只搬字节);调用方保证字节构造
     * 后不变(本方法只搬字节,不复制);CQ 内部错误(磁盘满、映射失败)按 CQ 运行时异常上抛——
     * 写入失败必须让调用方感知,不能静默丢单元。
     *
     * @param payload 一条完整 pgoutput 消息的字节(含类型字节与可选流式前缀,本类不解释内容)
     * @return 本条在队列中的 index(含 cycle 与序号),即该消息的 seq
     */
    long append(byte[] payload) {
        Objects.requireNonNull(payload, "payload 不能为 null");
        appender.writeBytes(BytesStore.wrap(payload));
        return appender.lastIndexAppended();
    }

    /**
     * 按 index 升序回读闭区间 [firstIndex..lastIndex] 的消息字节,每读出一条就回调
     * {@code payloadConsumer.accept(idx, payload)}——idx 是该条自身的真实 CQ index(即 append
     * 时的返回值,调用方作 seq 用,如 Relation 版本的 asOf 查询),payload 是与队列内存不共享
     * 的副本。
     *
     * <p>步骤:tailer 置为 FORWARD 方向,{@code moveToIndex(firstIndex)} 定位起步 → 循环读文档:
     * 读不到(到队尾)就结束;队列自己的元数据文档跳过;index 超过 lastIndex 就结束;index 还
     * 小于 firstIndex 就跳过(防御 moveToIndex 落点偏早,正常不会发生);读到的第一条 index
     * 必须恰好等于 firstIndex,否则抛 {@link IllegalStateException}——落点越过了区间起点,
     * 说明起点已被删除或从未存在,属于错位,宁可报错也不猜。最后把文档剩余字节读成数组交给回调。
     *
     * <p>边界:区间不存在且队列后面也没有内容时(比如清空重开后 readRange(0,100)),读到队尾
     * 自然空手而归,不抛异常;lastIndex 小于 firstIndex 时同样空手而归。
     *
     * @param firstIndex      区间起点 index(含)
     * @param lastIndex       区间终点 index(含)
     * @param payloadConsumer 逐条消费者(payload 副本 + 该条真实 CQ index,调用方作 seq 用)
     */
    void readRange(long firstIndex, long lastIndex, BiConsumer<Long, byte[]> payloadConsumer) {
        Objects.requireNonNull(payloadConsumer, "payloadConsumer 不能为 null");
        tailer.direction(TailerDirection.FORWARD);
        tailer.moveToIndex(firstIndex);
        boolean firstSeen = false;
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent()) {
                    return;                                 // 队列读尽(区间起点不存在时即空手而归)
                }
                if (dc.isMetaData()) {
                    continue;                               // 队列自管理的元数据文档,不属消息数据
                }
                long idx = dc.index();
                if (idx > lastIndex) {
                    return;                                 // 越过区间上界,停
                }
                if (idx < firstIndex) {
                    continue;                               // 落点偏早(防御),跳到区间起点
                }
                if (!firstSeen) {
                    if (idx != firstIndex) {
                        throw new IllegalStateException(
                                "readRange 区间起点错位:期望 index %d,实际落点 %d(区间必须存在,起点可能已被删除)"
                                        .formatted(firstIndex, idx));
                    }
                    firstSeen = true;
                }
                payloadConsumer.accept(idx, readFrameBytes(dc));
            }
        }
    }

    /**
     * 低水位释放:删除比"低水位所在 cycle 再往前一档"还老的滚动文件(needed 档与 needed-1 档
     * 都保留——上一档里可能还有低水位之前的在途条目,删除永远保守)。
     *
     * <p>步骤:由 lowestNeededIndex 反算出 needed cycle → <b>节流检查</b>(引擎 1.7.1 Task 3):
     * 档位与上次实际扫描相同就直接返回 0——同档位内可删集不可能变化(滚动文件只随 append
     * 前沿出现在当前/未来档位,不回填旧档名),删档检查延后到下一次档位推进,删除语义仍为惰性
     * → 用纯函数 {@link #deletableFiles} 算出可删集合 → 逐个删除,每删一个记 WARN 留下文件名;
     * 单个文件删不掉只 WARN 不上抛(残留文件只是占磁盘,不影响正确性,下次档位推进的扫描再删)
     * → 返回实际删除数。目录列举失败按"没有可删的"处理,返回 0。
     *
     * <p>边界:节流按"档位未变"判等而非"未减小"——水位回退(理论不可能,见引擎侧调用方
     * {@code TransactionAssembler#pipeWatermark} 的单调性)时宁可多扫一次也不漏删。
     *
     * @param lowestNeededIndex 仍被需要的最低 index(它所在的 cycle 和上一档 cycle 都保留)
     * @return 实际删除的滚动文件数(节流命中时恒 0)
     */
    long releaseBelow(long lowestNeededIndex) {
        long neededCycle = rollCycle.toCycle(lowestNeededIndex);
        if (neededCycle == lastScannedCycle) {
            return 0L;      // 同档位已扫过:可删集不可能变化,删档检查延后到档位推进
        }
        lastScannedCycle = neededCycle;
        long deleted = 0;
        for (Path doomed : deletableFiles(rollCycle, dir, neededCycle)) {
            try {
                if (Files.deleteIfExists(doomed)) {
                    LOG.warn("MessagePipe 已删除滚动文件 {}(低水位 cycle {} 之下)", doomed, neededCycle);
                    deleted++;
                }
            } catch (IOException e) {
                LOG.warn("MessagePipe 删除滚动文件 {} 失败(下次档位推进的扫描重试)", doomed, e);
            }
        }
        return deleted;
    }

    /**
     * 最近一次 append 返回的 index;未 append 过时 CQ 抛 {@link IllegalStateException}
     * (调用方保证先 append 后查询)。
     *
     * @return 最近写入条目的 index
     */
    long lastAppendedIndex() {
        return appender.lastIndexAppended();
    }

    /**
     * 责任:管道目录当前磁盘占用(MS5 Task 4 的 MBean 观测面)——{@code Files.walk} 递归
     * 遍历 dir,对全部常规文件求 size 和(含滚动数据文件 {@code .cq4} 与队列元数据
     * {@code .cq4t}——运维关心的是目录整体占用,不做文件甄别)。
     * 关键步骤:遍历是惰性流,reader 线程可能并发追加/低水位删除文件——单个文件的 size
     * 读失败(遍历中被删)按 0 计入,不让一次观测被瞬态 IO 抖动打翻。
     * 边界:目录级遍历失败(整体 IO 异常)记 WARN 返回 -1(未知哨兵,与"未装配"同形,
     * 调用方<b>不得</b>把 -1 当真实字节数累计)。
     * 线程约束:只读遍历,任意线程可调——但生产调用点是 consumer 的统计 tick
     * (MBean 读路径不碰 IO,见 StreamMetricsBridge 的线程模型),JMX 侧读到的是 tick
     * 内的采样快照。
     *
     * @return 目录全部常规文件的字节总和;目录遍历失败返回 -1
     */
    long diskUsageBytes() {
        try (Stream<Path> paths = Files.walk(dir)) {
            return paths.filter(Files::isRegularFile)
                    .mapToLong(MessagePipe::sizeOrZero)
                    .sum();
        }
        catch (IOException e) {
            LOG.warn("MessagePipe 遍历管道目录 {} 求磁盘占用失败", dir, e);
            return -1L;
        }
    }

    /**
     * 单文件 size 的 IO 容错读取:文件在遍历流推进到它之前被低水位删除(并发删档)按
     * 0 计入——观测语义"此刻目录里存在且可读的文件之和"。
     *
     * @param file 遍历到的常规文件
     * @return 文件字节数;读失败(已被删/不可访问)返回 0
     */
    private static long sizeOrZero(Path file) {
        try {
            return Files.size(file);
        }
        catch (IOException e) {
            return 0L;
        }
    }

    /**
     * 释放管道资源:tailer → appender → queue 逆序关闭,任何一步失败仅 WARN 不上抛
     * (close 不应掩盖业务异常;最坏代价是句柄延迟回收)。幂等:重复 close 已关资源至多触发
     * 被吸收的 WARN,不上抛。
     */
    @Override
    public void close() {
        try {
            tailer.close();
        } catch (RuntimeException e) {
            LOG.warn("MessagePipe tailer 关闭失败(忽略)", e);
        }
        try {
            appender.close();
        } catch (RuntimeException e) {
            LOG.warn("MessagePipe appender 关闭失败(忽略)", e);
        }
        try {
            queue.close();
            LOG.info("MessagePipe 已关闭:目录 {}", dir);
        } catch (RuntimeException e) {
            LOG.warn("MessagePipe queue 关闭失败(忽略)", e);
        }
    }

    /**
     * 纯函数的删除计算:列出 dir 下 cycle 号比 {@code neededCycle - 1} 还小的滚动文件
     * (needed 与 needed-1 两档保留),本身不做任何删除 IO——单测可以注入假文件名来验证删除规则。
     *
     * <p>步骤:目录不存在或列举失败返回空列表(保守起见当作无可删)→ 挑出以 {@code .cq4} 结尾的
     * 常规文件 → 去掉后缀,按 {@code rc.format()} 的模式解析成 UTC 时间戳(解析器经
     * {@link #FORMATTER_BY_PATTERN} 按模式串记忆化,引擎 1.7.1 Task 3 前每次调用新建),再换算成
     * cycle 号(换算公式与 {@code rc.toCycle} 同一基准)→ cycle 号小于 {@code neededCycle - 1}
     * 的入选。
     *
     * <p>边界:文件名去掉后缀后解析不出时间戳(不匹配滚动周期的命名模式)就跳过并 WARN——
     * 外来文件宁可漏删也不可错删;队列元数据文件 {@code metadata.cq4t} 后缀不同,天然不在候选内。
     * 无状态纯读,线程安全。
     *
     * @param rc          滚动周期(提供文件名格式与 cycle 换算参数)
     * @param dir         滚动文件所在目录
     * @param neededCycle 仍需保留的 cycle 号(它本身和减一档都保留)
     * @return 可删除文件的列表(可能为空,顺序不保证)
     */
    static List<Path> deletableFiles(RollCycle rc, Path dir, long neededCycle) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        DateTimeFormatter format = formatterFor(rc.format());
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
            LOG.warn("MessagePipe 列举滚动目录 {} 失败,本次不删除", dir, e);
            return List.of();
        }
    }

    /**
     * 取滚动周期格式对应的解析器(进程级记忆化):{@code DateTimeFormatter.ofPattern} 构造成本
     * 为 µs 级且格式串来自有限枚举的 {@link RollCycle} 实现——按模式串缓存后重复调用零构造成本。
     * 线程安全(CHM + 不可变 value;竞争窗口内最坏重复构造一次,无害)。
     *
     * @param pattern 滚动周期的文件名格式模式串({@code RollCycle.format()})
     * @return 该模式串的解析器(不可变,可共享)
     */
    private static DateTimeFormatter formatterFor(String pattern) {
        return FORMATTER_BY_PATTERN.computeIfAbsent(pattern, p -> DateTimeFormatter.ofPattern(p, Locale.ROOT));
    }

    /**
     * 解析单个滚动文件的 cycle 号:剥掉 {@code .cq4} 后缀后按滚动周期格式读作 UTC 时间戳,
     * 再按周期时长换算距纪元的档位数。解析失败(后缀剥掉后不匹配格式)返回 {@link Long#MAX_VALUE}
     * 并 WARN——永不入选删除集(外来文件宁可漏删不可错删)。
     *
     * @param rc     滚动周期
     * @param format 与 rc.format() 对应的解析器
     * @param file   候选文件
     * @return 文件名对应的 cycle 号;无法解析返回 Long.MAX_VALUE
     */
    private static long parseCycle(RollCycle rc, DateTimeFormatter format, Path file) {
        String name = file.getFileName().toString();
        String stem = name.substring(0, name.length() - ROLL_FILE_SUFFIX.length());
        try {
            LocalDateTime timestamp = LocalDateTime.parse(stem, format);
            long epochMillis = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
            return (epochMillis - rc.defaultEpoch()) / rc.lengthInMillis();
        } catch (DateTimeParseException e) {
            LOG.warn("MessagePipe 跳过无法解析的滚动文件名 {}(不匹配周期 {} 格式)", name, rc.format());
            return Long.MAX_VALUE;
        }
    }

    /**
     * 递归清空目录内容(保留目录本身):深度优先逆序删除(子项先于父项),任一删除失败抛
     * {@link UncheckedIOException} fail-fast——瞬态工作区语义要求"必须干净",清不干净即拒绝
     * 开管道。
     *
     * @param dir 待清空目录(不存在则先创建)
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
                                throw new UncheckedIOException("清空管道目录失败:" + p, e);
                            }
                        });
            }
        } catch (IOException e) {
            throw new UncheckedIOException("无法准备管道目录 " + dir, e);
        }
    }

    /**
     * 读出当前文档的剩余字节为数组副本:readingDocument 定位后 wire 的 bytes 已指向
     * 文档载荷起点,readRemaining 即载荷长度(writeBytes 写入的原样字节)。
     *
     * @param dc 已定位到数据文档的上下文
     * @return 载荷字节副本(与队列内存不共享)
     */
    private static byte[] readFrameBytes(DocumentContext dc) {
        Bytes<?> bytes = dc.wire().bytes();
        byte[] framed = new byte[(int) bytes.readRemaining()];
        bytes.read(framed);
        return framed;
    }
}
