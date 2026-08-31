package org.vastdata.vbstream.bench;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CorpusLoader} 的 round-trip 与损坏文件 fail-fast 单测（Task 13 Step 1）：
 * 覆盖空列表、跨幅长度的混合列表（1B..64KB，对应语料里控制消息与大载荷流式 Insert 的两端）、
 * 以及两类损坏输入——长度声明为 0、长度声明超出实际内容（截断文件）——都必须抛 IOException
 * 而非静默产出错位数据。
 */
class CorpusLoaderTest {

    /**
     * round-trip：dump 后 load 必须还原出同序、逐字节相等的列表。
     * 关键步骤：构造长度跨幅的伪消息（含空邻界之外的 1 字节最小消息与 64KB 大载荷）→
     * dump 到临时文件 → load 回来 → 断言条数与逐元素 Arrays.equals（byte[] 的 List.equals
     * 退化为引用相等，必须逐元素比）。
     * 边界：空列表 round-trip 出空文件、load 回空列表，一并覆盖。
     */
    @Test
    void dumpThenLoadRoundTrips(@TempDir Path dir) throws IOException {
        Random random = new Random(42);
        byte[] small = new byte[1];
        byte[] medium = new byte[37];
        byte[] large = new byte[64 * 1024];
        random.nextBytes(small);
        random.nextBytes(medium);
        random.nextBytes(large);
        List<byte[]> messages = List.of(small, medium, large, new byte[] {'B'});

        Path file = dir.resolve("corpus.bin");
        CorpusLoader.dump(file, messages);
        long expectedBytes = messages.stream().mapToLong(m -> 4L + m.length).sum();
        assertEquals(expectedBytes, Files.size(file), "文件大小应等于 Σ(4 字节长度头 + 消息字节)");
        List<byte[]> loaded = CorpusLoader.load(file);
        assertEquals(messages.size(), loaded.size(), "条数应一致");
        for (int i = 0; i < messages.size(); i++) {
            assertTrue(Arrays.equals(messages.get(i), loaded.get(i)), "第 " + i + " 条应逐字节相等");
        }

        CorpusLoader.dump(dir.resolve("empty.bin"), List.of());
        assertTrue(CorpusLoader.load(dir.resolve("empty.bin")).isEmpty(), "空列表 round-trip 出空列表");
    }

    /**
     * 损坏文件 fail-fast 之一：长度声明为 0（协议消息至少含类型字节，0 必为格式损坏）。
     * 手工构造 [I32 0] 前缀，load 必须抛 IOException，不得产出空消息条目。
     */
    @Test
    void zeroLengthHeaderIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("zero.bin");
        Files.write(file, new byte[] {0, 0, 0, 0});
        assertThrows(IOException.class, () -> CorpusLoader.load(file));
    }

    /**
     * 损坏文件 fail-fast 之二：长度声明超出实际内容（截断文件）——readFully 必须以
     * EOFException（IOException 子类）终止而非部分填充返回。
     * 手工构造 [I32 10][4 字节] 的截断记录。
     */
    @Test
    void truncatedPayloadIsRejected(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("truncated.bin");
        Files.write(file, new byte[] {0, 0, 0, 10, 1, 2, 3, 4});
        assertThrows(IOException.class, () -> CorpusLoader.load(file));
    }
}
