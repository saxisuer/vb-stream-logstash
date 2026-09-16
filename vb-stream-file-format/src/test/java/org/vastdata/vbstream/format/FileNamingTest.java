package org.vastdata.vbstream.format;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link FileNaming} 文件命名契约单测:三后缀(bin/json/sql)组装与解析、四参重载、
 * seq 从数据目录恢复(跨后缀——重启换格式时目录里可能混有旧后缀文件,seq 恢复与后缀无关)。
 */
class FileNamingTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /**
     * 场景:四参 fileName 按传入后缀组装(bin/sql);文件名形态
     * {@code <task>-<seq 16位零填充>-<yyyyMMddHHmmss>.<ext>}。
     */
    @Test
    void fileNameAssemblesWithRequestedExtension() {
        LocalDateTime t = LocalDateTime.of(2026, 9, 16, 8, 30, 0);
        assertEquals("tk-0000000000000007-20260916083000.sql",
                FileNaming.fileName("tk", 7, t, "sql"));
        assertEquals("tk-0000000000000007-20260916083000.bin",
                FileNaming.fileName("tk", 7, t, "bin"));
    }

    /**
     * 场景:parseSeqOpt 对三种后缀都解析出 seq;非法命名(缺段时间戳/宽度不对)返回 empty。
     */
    @Test
    void parseSeqAcceptsAllThreeSuffixes() {
        assertEquals(OptionalLong.of(42), FileNaming.parseSeqOpt("tk-0000000000000042-20260101000000.bin"));
        assertEquals(OptionalLong.of(42), FileNaming.parseSeqOpt("tk-0000000000000042-20260101000000.json"));
        assertEquals(OptionalLong.of(42), FileNaming.parseSeqOpt("tk-0000000000000042-20260101000000.sql"));
        assertTrue(FileNaming.parseSeqOpt("tk-42-20260101000000.bin").isEmpty(), "seq 宽度不足不解析");
        assertTrue(FileNaming.parseSeqOpt("tk-0000000000000042-20260101.bin").isEmpty(), "时间戳段残缺不解析");
    }

    /**
     * 场景:nextSeq 跨后缀恢复——目录里预置 .sql/.json 的高 seq 文件(上次会话用了别的格式),
     * 新构造侧扫目录取 max+1,与后缀无关。
     *
     * @throws IOException 目录扫描失败上抛
     */
    @Test
    void nextSeqRestoresAcrossSuffixes() throws IOException {
        Files.writeString(dir.resolve("tk-0000000000000009-20260101000000.bin"), "old");
        Files.writeString(dir.resolve("tk-0000000000000099-20260101000000.sql"), "old");
        Files.writeString(dir.resolve("tk-0000000000000050-20260101000000.json"), "old");
        assertEquals(100, FileNaming.nextSeq(dir.toAbsolutePath(), "tk"),
                "取三种后缀中的最大 seq 99 + 1");
        assertEquals(1, FileNaming.nextSeq(dir.toAbsolutePath(), "other"),
                "目录里没有本 task 的文件时从 1 起");
    }
}
