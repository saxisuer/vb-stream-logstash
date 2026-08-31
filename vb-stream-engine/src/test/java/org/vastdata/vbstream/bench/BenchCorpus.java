package org.vastdata.vbstream.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 基准语料的统一取用点（Task 13）：五个 JMH 基准类共用的语料路径常量与"缺失即带指引失败"
 * 的加载入口。语料由集成测试 {@code it.BenchCorpusRecordTest} 录制生成并提交进库
 * （路径即录制测试的落盘路径，单一事实来源）；基准 {@code @Setup} 一律经 {@link #load()}
 * 取数——文件缺失时抛带修复指引的 {@link IllegalStateException}（JMH 把 @Setup 异常视为
 * 该基准立即失败，正是期望的 fail-fast 形态），而非让每个基准各自重复判空逻辑。
 */
public final class BenchCorpus {

    /** 语料文件位置（相对模块根——surefire 与 java -cp 直跑 JMH 的工作目录均为模块根）。 */
    public static final Path CORPUS_FILE = Path.of("src/test/resources/bench-corpus/corpus.bin");

    private BenchCorpus() {
    }

    /**
     * 责任：加载基准语料，缺失时抛带指引的异常。
     * 关键步骤：文件存在且为常规文件 → 委托 {@link CorpusLoader#load}；否则抛
     * {@link IllegalStateException}，消息指明两个修复入口（先跑录制测试，或整跑 mvn test——
     * 录制测试本身就在其中自动生成语料）。
     * 边界与异常语义：语料损坏（长度声明非法/中段截断）由 CorpusLoader 的 IOException 上抛
     * ——@Setup 可声明 throws，两种异常在 JMH 下都会让该基准以错误终止。
     *
     * @return 语料消息字节列表（非空；健康断言由录制侧保证）
     * @throws java.io.IOException 语料文件存在但格式损坏
     */
    public static List<byte[]> load() throws java.io.IOException {
        if (!Files.isRegularFile(CORPUS_FILE)) {
            throw new IllegalStateException(
                    "基准语料缺失: " + CORPUS_FILE.toAbsolutePath()
                            + " —— 先运行录制测试生成: mvn test -Dtest=BenchCorpusRecordTest"
                            + "（常规 mvn test 也会经该测试自动生成）");
        }
        return CorpusLoader.load(CORPUS_FILE);
    }
}
