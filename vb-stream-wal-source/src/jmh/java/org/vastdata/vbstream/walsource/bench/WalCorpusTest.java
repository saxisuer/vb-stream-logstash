package org.vastdata.vbstream.walsource.bench;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link WalCorpus} 语料位置解析的 {@value WalCorpus#CORPUS_PROPERTY} 覆盖面测试
 * （wal-bench 窄行对照的入口）——只测显式属性三分支（文件/目录/缺失 fail-fast），
 * 缺省探测分支依赖仓库 {@code target/corpus} 是否在场（环境态），不作确定性断言。
 * 测试落在 jmh 源码根（与被测类同根，仅 {@code -Pjmh} 档编译参与），常规
 * {@code mvn test} 不编译本类。
 */
class WalCorpusTest {

    /**
     * 属性指向 bin 文件：meta 取同名 {@code .bin} 换 {@code .meta} 的兄弟文件，
     * 装载出的字节数与 meta 数值键（含四 watched 段号）逐项对上。
     *
     * @throws Exception 临时文件写入失败
     */
    @Test
    void explicitFilePropertySelectsSiblingMeta() throws Exception {
        Path dir = Files.createTempDirectory("wal-corpus-test");
        Path bin = dir.resolve("vast-corpus-narrow.bin");
        Files.write(bin, new byte[]{1, 2, 3});
        Files.write(dir.resolve("vast-corpus-narrow.meta"), List.of(
                "# probe metadata",
                "startLsn=16",
                "endLsn=32",
                "bytes=3",
                "records=7",
                "resyncs=0",
                "pgAttrFilenode=101",
                "pgClassFilenode=102",
                "pgNspFilenode=103",
                "pgPartFilenode=104",
                "workload.inserts=100"));
        System.setProperty(WalCorpus.CORPUS_PROPERTY, bin.toString());
        try {
            WalCorpus corpus = WalCorpus.load();
            assertEquals(3, corpus.bytes());
            assertEquals(3, corpus.data().length);
            assertEquals(16, corpus.startLsn());
            assertEquals(32, corpus.endLsn());
            assertEquals(7, corpus.records());
            assertEquals(101, corpus.pgAttrFilenode());
            assertEquals(102, corpus.pgClassFilenode());
            assertEquals(103, corpus.pgNspFilenode());
            assertEquals(104, corpus.pgPartFilenode());
        } finally {
            System.clearProperty(WalCorpus.CORPUS_PROPERTY);
        }
    }

    /**
     * 属性指向目录：bin/meta 取目录内缺省名（{@code vast-corpus.bin/.meta}），
     * meta 的 {@code workload.*} 构成键被跳过（records 不受其影响）。
     *
     * @throws Exception 临时文件写入失败
     */
    @Test
    void explicitDirectoryPropertyResolvesDefaultNames() throws Exception {
        Path dir = Files.createTempDirectory("wal-corpus-test");
        Files.write(dir.resolve("vast-corpus.bin"), new byte[]{9});
        Files.write(dir.resolve("vast-corpus.meta"), List.of(
                "startLsn=1",
                "endLsn=2",
                "records=4",
                "pgClassFilenode=55",
                "workload.rows=999999"));
        System.setProperty(WalCorpus.CORPUS_PROPERTY, dir.toString());
        try {
            WalCorpus corpus = WalCorpus.load();
            assertEquals(1, corpus.bytes());
            assertEquals(4, corpus.records());
            assertEquals(55, corpus.pgClassFilenode());
        } finally {
            System.clearProperty(WalCorpus.CORPUS_PROPERTY);
        }
    }

    /**
     * 无 {@code .bin} 后缀的文件形态：meta 名直接追加 {@code .meta}（不截断任何后缀）。
     *
     * @throws Exception 临时文件写入失败
     */
    @Test
    void filePropertyWithoutBinSuffixAppendsMetaName() throws Exception {
        Path dir = Files.createTempDirectory("wal-corpus-test");
        Path bin = dir.resolve("mycorpus");
        Files.write(bin, new byte[]{5, 6});
        Files.write(dir.resolve("mycorpus.meta"), List.of("records=2"));
        System.setProperty(WalCorpus.CORPUS_PROPERTY, bin.toString());
        try {
            WalCorpus corpus = WalCorpus.load();
            assertEquals(2, corpus.bytes());
            assertEquals(2, corpus.records());
        } finally {
            System.clearProperty(WalCorpus.CORPUS_PROPERTY);
        }
    }

    /**
     * 属性指向不存在的路径：ISE fail-fast（消息含属性键与路径——显式意图不静默回落
     * 缺省探测）。
     */
    @Test
    void missingExplicitTargetFailsFast() {
        Path nowhere = Path.of("no-such-dir", "no-such-corpus.bin");
        System.setProperty(WalCorpus.CORPUS_PROPERTY, nowhere.toString());
        try {
            IllegalStateException e = assertThrows(IllegalStateException.class, WalCorpus::load);
            assertTrue(e.getMessage().contains(WalCorpus.CORPUS_PROPERTY),
                    "消息应含属性键: " + e.getMessage());
            assertTrue(e.getMessage().contains(nowhere.toString()),
                    "消息应含路径: " + e.getMessage());
        } finally {
            System.clearProperty(WalCorpus.CORPUS_PROPERTY);
        }
    }

    /**
     * bin 在而 meta 缺（显式语料不完整）：ISE fail-fast（消息含 bin 与 meta 两侧
     * 期望路径）。
     *
     * @throws Exception 临时文件写入失败
     */
    @Test
    void explicitTargetWithoutMetaFailsFast() throws Exception {
        Path dir = Files.createTempDirectory("wal-corpus-test");
        Path bin = dir.resolve("vast-corpus-narrow.bin");
        Files.write(bin, new byte[]{1});
        System.setProperty(WalCorpus.CORPUS_PROPERTY, bin.toString());
        try {
            IllegalStateException e = assertThrows(IllegalStateException.class, WalCorpus::load);
            assertTrue(e.getMessage().contains("vast-corpus-narrow.meta"),
                    "消息应含期望 meta 路径: " + e.getMessage());
        } finally {
            System.clearProperty(WalCorpus.CORPUS_PROPERTY);
        }
    }
}
