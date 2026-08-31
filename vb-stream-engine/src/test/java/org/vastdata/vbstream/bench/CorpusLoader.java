package org.vastdata.vbstream.bench;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JMH 基准语料文件的读写器（Task 13）。文件格式为最简长度前缀流：{@code [I32 len][len 字节]}
 * 逐消息重复到文件尾，全部 big-endian——不引入任何消息类型/索引元数据，读写双方只搬运字节，
 * 语义解释完全交给 {@code PgOutputDecoder}（与复制流的"单条完整消息字节"契约一致）。
 *
 * <p>语料由集成测试 {@code it.BenchCorpusRecordTest} 从真实 PG 录制生成并提交进库
 * （{@code src/test/resources/bench-corpus/corpus.bin}）；基准类在 {@code @Setup} 里
 * {@link #load} 整个列表后循环推送。格式缺陷的容错策略是 fail-fast 抛 {@link IOException}
 * （长度越界/中段截断），唯文件尾不足 4 字节的残段按"文件结束"处理（写入方保证记录对齐，
 * 干净文件不会命中该路径）。
 *
 * <p>无状态纯函数工具类：线程安全；{@code load} 返回的列表与内部缓冲均为调用方独占。
 */
public final class CorpusLoader {

    /** 单条消息长度上限（防御损坏文件把 len 读成垃圾大数后盲目分配）：语料中最大消息为 ~16KB 的流式 Insert，64MB 远超足够。 */
    private static final int MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

    private CorpusLoader() {
    }

    /**
     * 责任：把语料文件完整读入内存为消息字节列表。
     * 关键步骤：循环 {@code readInt} 取长度 → 校验 1..MAX_MESSAGE_BYTES（0 与负数同样拒绝——
     * 协议消息至少含 1 个类型字节，len=0 必为格式损坏）→ {@code readFully} 读满该条入列；
     * {@code readInt} 在文件尾抛 {@link EOFException} 视为正常结束跳出循环。
     * 边界与异常语义：文件不存在抛 {@link IOException}（由 {@code Files.newInputStream} 原样上抛）；
     * 长度声明超出实际内容时 {@code readFully} 以 EOFException 终止（属 IOException 子类，fail-fast）；
     * 文件尾不足 4 字节的残片被静默当作结束（见类 javadoc 的对齐前提）。
     *
     * @param file 语料文件路径（非 null）
     * @return 消息字节列表（保序、每条至少 1 字节；空文件返回空列表）
     * @throws IOException 读取失败或格式损坏
     */
    public static List<byte[]> load(Path file) throws IOException {
        Objects.requireNonNull(file, "file 不能为 null");
        List<byte[]> messages = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            while (true) {
                int len;
                try {
                    len = in.readInt();
                } catch (EOFException e) {
                    return messages;                        // 干净的记录边界上到达文件尾
                }
                if (len <= 0 || len > MAX_MESSAGE_BYTES) {
                    throw new IOException("语料文件 %s 的消息长度声明 %d 非法（期望 1..%d）——文件损坏或非本格式"
                            .formatted(file, len, MAX_MESSAGE_BYTES));
                }
                byte[] buf = new byte[len];
                in.readFully(buf);
                messages.add(buf);
            }
        }
    }

    /**
     * 责任：把消息字节列表写为语料文件（{@link #load} 的逆，round-trip 保序保字节）。
     * 关键步骤：父目录不存在则创建（录制测试直接写入 src/test/resources 下的目录）→
     * 循环 {@code writeInt(payload.length)} + {@code write(payload)}。
     * 边界与异常语义：messages 或任一元素为 null 抛 NPE（空列表合法，产出空文件）；
     * 元素长度为 0 会写出 load 侧拒绝的记录（len=0）——本方法不校验，属调用方契约
     * （真实消息恒 ≥1 字节）；IO 失败原样上抛，文件可能残留半截（调用方重写覆盖即可）。
     *
     * @param file     目标文件（非 null；父目录自动创建）
     * @param messages 消息字节列表（非 null，元素非 null）
     * @throws IOException 写入失败
     */
    public static void dump(Path file, List<byte[]> messages) throws IOException {
        Objects.requireNonNull(file, "file 不能为 null");
        Objects.requireNonNull(messages, "messages 不能为 null");
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(file)))) {
            for (byte[] message : messages) {
                Objects.requireNonNull(message, "messages 含 null 元素");
                out.writeInt(message.length);
                out.write(message);
            }
        }
    }

    /**
     * 责任：递归删除一个目录及其全部内容（JMH 基准 teardown 里对临时溢写目录的
     * "@TempDir 语义"清理——JMH 无 JUnit 的临时目录机制，需手工等价物）。
     * 关键步骤：{@code Files.walk} 深度优先收集 → 逆序（子先于父）逐个 delete。
     * 边界与异常语义：dir 不存在为无操作；任一删除失败抛 {@link IOException}
     * （溢写 mmap 未释放时 macOS 上删除目录会失败——调用方须先 close 池再清理）。
     *
     * @param dir 待删除目录（不存在则无操作）
     * @throws IOException 删除失败
     */
    public static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            for (Path p : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
