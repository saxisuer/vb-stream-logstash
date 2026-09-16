package org.vastdata.vbstream.reader.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.dataRecord;
import static org.vastdata.vbstream.reader.file.ConnectTestRecords.marker;

/**
 * {@link SqlEventWriter} 的文本形态单测(零 PG):头注释/事务边界裸 BEGIN;COMMIT;/数据事件
 * 渲染语句,逐行拼出与手写期望一致的文件内容。记录手造见 {@link ConnectTestRecords};
 * 事务边界记录的 raw 载荷 SQL 写入器不消费(只取 marker 字段),传 null 即可。
 */
class SqlEventWriterTest {

    @TempDir(cleanup = CleanupMode.NEVER)
    Path dir;

    /**
     * 场景:完整事务写出的文件 = 头注释 + BEGIN; + INSERT 语句 + COMMIT;——SqlRenderer
     * 渲染 ParsedEvent 的端到端文本面(值字面量规则由 SqlRendererTest 锚定,此处只验管线)。
     *
     * @throws IOException 文件读写失败上抛
     */
    @Test
    void writesHeaderBeginStatementAndCommit() throws IOException {
        Path file = dir.resolve("out.sql");
        try (SqlEventWriter w = new SqlEventWriter(file, 7, "tk")) {
            w.writeBegin(marker(true, "5"), null);
            w.writeEvent(dataRecord(1, "hello"));
            w.writeCommit(marker(false, "5"), null);
            w.finish();
            assertEquals(1, w.recordCount(), "recordCount 记数据事件数(不含 BEGIN/COMMIT)");
        }
        String content = Files.readString(file);
        assertEquals("-- vb-stream task=tk seq=0000000000000007\n"
                + "BEGIN;\n"
                + "INSERT INTO \"public\".\"t\" (\"id\", \"payload\") VALUES (1, 'hello');\n"
                + "COMMIT;\n", content);
    }

    /**
     * 场景:非 ASCII 载荷的字符集——写入侧必须显式 UTF-8(本机平台默认 GBK 时若 getBytes
     * 回退默认,中文以 GBK 落盘、Files.readString(恒 UTF-8)读回必乱码,本用例即红——防回归
     * 锚);中文值原样往返且单引号按 SQL 规则翻倍。
     *
     * @throws IOException 文件读写失败上抛
     */
    @Test
    void chinesePayloadRoundTripsAsUtf8() throws IOException {
        Path file = dir.resolve("out-cn.sql");
        try (SqlEventWriter w = new SqlEventWriter(file, 1, "tk")) {
            w.writeEvent(dataRecord(1, "你好'引号"));
            w.finish();
        }
        assertEquals("-- vb-stream task=tk seq=0000000000000001\n"
                + "INSERT INTO \"public\".\"t\" (\"id\", \"payload\") VALUES (1, '你好''引号');\n",
                Files.readString(file), "中文载荷按 UTF-8 落盘,读回原值(引号翻倍)");
    }
}
