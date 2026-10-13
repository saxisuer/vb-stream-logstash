package org.vastdata.vbstream.walsource.bench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * VastBase 真库 WAL 语料的离线载体（wal-bench）——一次性采集工具（{@code target/probe/}
 * 的 CorpusProbe，不入库）把物理复制流一个窗口的原始字节落到
 * {@code vb-stream-wal-source/target/corpus/vast-corpus.bin}，采集侧元信息
 * （起止 LSN/记录数/四 watched 目录物理段号）落到同目录 {@code vast-corpus.meta}。
 * 本类在基准 trial 装载期一次读入两者，为 {@code VastCorpusBenchmark} 提供只读语料与
 * L2 重放所需的 watched 段号。
 *
 * <p><strong>路径解析</strong>：系统属性 {@code vb.bench.corpus} 可显式指定语料位置
 * （窄行对照等第二语料形态的入口）——值指向 bin <b>文件</b>时 meta 取同名 {@code .meta}
 * 兄弟文件（{@code vast-corpus-narrow.bin} → {@code vast-corpus-narrow.meta}），指向
 * <b>目录</b>时取目录内 {@code vast-corpus.bin}/{@code vast-corpus.meta}；显式指向的
 * bin/meta 任一缺失抛 ISE fail-fast（显式意图不静默回落）。属性未设/空白时走缺省探测：
 * 先按仓库根相对路径 {@code vb-stream-wal-source/target/corpus}、再按模块根相对路径
 * {@code target/corpus}（JMH 运行目录两档都常见）；两处皆缺 {@code vast-corpus.bin}
 * 时抛 ISE fail-fast 附采集指引（不静默跑空基准）。语料位于
 * {@code target/} 下——对 wal-source 模块跑 {@code mvn clean} 会连带删除，复测前须
 * 备份或重采（见 docs/benchmarks-baseline.md 的 wal-source 段）。</p>
 *
 * <p>线程约束：装载后全字段只读，任意线程并发读安全。</p>
 */
public final class WalCorpus {

    private static final Logger LOG = LoggerFactory.getLogger(WalCorpus.class);

    /** 语料目录的候选相对路径（仓库根 / 模块根两档运行目录）。 */
    private static final String[] LOCATIONS = {"vb-stream-wal-source/target/corpus", "target/corpus"};

    /** 语料字节文件名。 */
    private static final String BIN_NAME = "vast-corpus.bin";

    /** 语料元信息文件名。 */
    private static final String META_NAME = "vast-corpus.meta";

    /** 语料位置覆盖键（值 = bin 文件或语料目录路径；未设/空白走缺省探测）。 */
    static final String CORPUS_PROPERTY = "vb.bench.corpus";

    private final byte[] data;
    private final Map<String, Long> meta;

    /**
     * 私有构造：字段经 {@link #load()} 装配。
     */
    private WalCorpus(byte[] data, Map<String, Long> meta) {
        this.data = data;
        this.meta = meta;
    }

    /**
     * 装载语料与元信息。
     *
     * <p>关键步骤：经 {@link #resolveCorpusPaths()} 解析 bin/meta 路径对（系统属性
     * {@value #CORPUS_PROPERTY} 显式指定，未设走缺省探测）→ 读入全部字节 → 解析 meta
     * 的 {@code key=value} 行（非 {@code workload.*} 的数值键收进 map，缺失键取 0——
     * 四 watched 段号缺失时 L2 第四/三腿不激活，属可运行但缺腿的形态，装载期 WARN）。
     * 边界与异常语义：bin 缺失抛 ISE 附采集指引；meta 缺失/不可读抛 ISE（bin 在而
     * meta 缺说明采集半途而废，起始 LSN 无从得知，不猜）；IO 失败包装 ISE。</p>
     *
     * @return 已装载语料
     */
    public static WalCorpus load() {
        Path[] paths = resolveCorpusPaths();
        byte[] bin;
        Map<String, Long> meta;
        try {
            bin = Files.readAllBytes(paths[0]);
            meta = parseMeta(paths[1]);
        } catch (IOException e) {
            throw new IllegalStateException("语料读取失败: " + paths[0], e);
        }
        WalCorpus corpus = new WalCorpus(bin, meta);
        LOG.info("语料装载: corpus={} bytes={} records={} startLsn={} endLsn={} 收流侧 resyncs={}",
                paths[0], corpus.bytes(), corpus.records(), Long.toHexString(corpus.startLsn()),
                Long.toHexString(corpus.endLsn()), corpus.resyncs());
        if (corpus.pgClassFilenode() == 0) {
            LOG.warn("meta 缺 watched 段号——L2 的字典重放腿不会命中任何 catalog 块（缺腿运行）");
        }
        return corpus;
    }

    /**
     * 解析语料 bin/meta 路径对：{@value #CORPUS_PROPERTY} 系统属性显式指定（bin 文件
     * 或语料目录两种形态），未设/空白回落缺省两档候选目录探测。
     *
     * <p>关键步骤：属性值是<b>文件</b>时 bin 即该文件、meta 为同名 {@code .bin} 后缀
     * 换 {@code .meta} 的兄弟文件（无 {@code .bin} 后缀则直接追加 {@code .meta}）；
     * 是<b>目录</b>时取目录内 {@value #BIN_NAME}/{@value #META_NAME}；显式形态下
     * bin/meta 任一缺失即 ISE（显式意图不静默回落缺省）。缺省形态逐一探测
     * {@link #LOCATIONS} 候选目录的 bin 文件，皆缺抛 ISE 附采集指引。
     * 边界与异常语义：属性指向不存在的路径（既非文件也非目录）抛 ISE。</p>
     *
     * @return 两元素数组 [bin 路径, meta 路径]
     */
    static Path[] resolveCorpusPaths() {
        String override = System.getProperty(CORPUS_PROPERTY, "").trim();
        if (!override.isEmpty()) {
            Path target = Path.of(override);
            Path bin;
            if (Files.isDirectory(target)) {
                bin = target.resolve(BIN_NAME);
            } else if (Files.isRegularFile(target)) {
                bin = target;
            } else {
                throw new IllegalStateException("vb.bench.corpus 显式指向不存在的路径: " + override);
            }
            String name = bin.getFileName().toString();
            Path meta = bin.resolveSibling(name.endsWith(".bin")
                    ? name.substring(0, name.length() - ".bin".length()) + ".meta"
                    : name + ".meta");
            if (!Files.isRegularFile(bin) || !Files.isRegularFile(meta)) {
                throw new IllegalStateException("显式语料不完整（vb.bench.corpus=" + override
                        + "）：需要 bin=" + bin + " 与 meta=" + meta);
            }
            return new Path[]{bin, meta};
        }
        for (String loc : LOCATIONS) {
            Path candidate = Path.of(loc);
            if (Files.isRegularFile(candidate.resolve(BIN_NAME))) {
                return new Path[]{candidate.resolve(BIN_NAME), candidate.resolve(META_NAME)};
            }
        }
        throw new IllegalStateException("WAL 语料缺失（探测过 " + String.join("/", LOCATIONS)
                + " 下的 " + BIN_NAME + "）——语料经一次性采集工具落盘：用 DirectWalSource 连真库"
                + " 172.16.107.133:55433/test_a 采一段含 CREATE TABLE 起始的密集 DML 窗口"
                + "（工具源码在 target/probe/CorpusProbe.java，详见 docs/benchmarks-baseline.md"
                + " 的 wal-source 段采集说明），或从备份位置恢复 target/corpus/ 两文件");
    }

    /**
     * 解析 meta 文件：仅收纯数值键（{@code workload.*} 构成键与注释行跳过），坏行忽略。
     *
     * @param file meta 文件路径
     * @return 数值键集
     * @throws IOException 读失败
     */
    private static Map<String, Long> parseMeta(Path file) throws IOException {
        Map<String, Long> out = new HashMap<>();
        for (String line : Files.readAllLines(file)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = trimmed.substring(0, eq);
            if (key.startsWith("workload.")) {
                continue;
            }
            try {
                out.put(key, Long.parseUnsignedLong(trimmed.substring(eq + 1)));
            } catch (NumberFormatException e) {
                // 非数值键（将来 meta 扩展）——跳过
            }
        }
        return out;
    }

    /**
     * 语料原始字节（WAL 流 payload 连续拼接，起点即 {@link #startLsn()}）。
     *
     * @return 全量字节（调用方约定只读）
     */
    public byte[] data() {
        return data;
    }

    /**
     * 语料字节数。
     *
     * @return 字节总数
     */
    public long bytes() {
        return data.length;
    }

    /**
     * 语料规模换算（MiB，1024 进制——MB/s 结论的计量单位）。
     *
     * @return MiB 值
     */
    public double mib() {
        return data.length / 1024.0 / 1024.0;
    }

    /**
     * 首块字节对应的绝对 LSN（采集侧 DirectWalSource 首块 dataStart）。
     *
     * @return 起始 LSN（未采集为 0）
     */
    public long startLsn() {
        return meta.getOrDefault("startLsn", 0L);
    }

    /**
     * 末块锚（末字节之后的绝对 LSN）。
     *
     * @return 结束 LSN
     */
    public long endLsn() {
        return meta.getOrDefault("endLsn", 0L);
    }

    /**
     * 采集侧走读记录数（walker 实测——回放侧每次解析须产出同量记录，差值即回放缺陷）。
     *
     * @return 记录数
     */
    public long records() {
        return meta.getOrDefault("records", 0L);
    }

    /**
     * 采集侧再同步计数（观测面——非零提示语料里有锚定事件，回放侧同走锚定路径）。
     *
     * @return resyncs
     */
    public long resyncs() {
        return meta.getOrDefault("resyncs", 0L);
    }

    /**
     * pg_attribute 的物理段号（L2 watched 注册面，采集期 {@code pg_relation_filenode} 实查）。
     *
     * @return 段号（未采到为 0——第三/一腿不激活）
     */
    public long pgAttrFilenode() {
        return meta.getOrDefault("pgAttrFilenode", 0L);
    }

    /**
     * pg_class 的物理段号。
     *
     * @return 段号（未采到为 0）
     */
    public long pgClassFilenode() {
        return meta.getOrDefault("pgClassFilenode", 0L);
    }

    /**
     * pg_namespace 的物理段号。
     *
     * @return 段号（未采到为 0）
     */
    public long pgNspFilenode() {
        return meta.getOrDefault("pgNspFilenode", 0L);
    }

    /**
     * pg_partition 的物理段号（第四 watched 腿，分区路由字典）。
     *
     * @return 段号（未采到为 0——分区 DML 的字典 miss 面扩大，fail-safe 跳过）
     */
    public long pgPartFilenode() {
        return meta.getOrDefault("pgPartFilenode", 0L);
    }
}
