package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.vastdata.vbstream.format.ParsedEvent;
import org.vastdata.vbstream.format.sql.SqlRenderer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * SQL 文本格式的事件写入器:envelope → IR → {@link SqlRenderer} 渲染语句逐行写文件。
 * 事务边界渲染为裸 {@code BEGIN;}/{@code COMMIT;};文件头一行 {@code --} 注释携带 task/seq
 * (lsn/txid 不落文件)。无 FOOTER/CRC——完整性靠 tmp .part 暂存 + 原子 rename 的 publish
 * 机制(与 binary 同一外壳,约定"数据目录里无 .part 后缀即完整文件")。
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 SqlEventWriter(2026-09-16 快照)。
 */
final class SqlEventWriter implements EventFileWriter {

    private final FileOutputStream fos;
    private final OutputStream out;
    private long events;

    /**
     * 责任:打开目标文件并写头注释行(task/seq 进注释——执行侧忽略,消费侧可溯源)。
     * 边界:文件已存在则从头部覆写(调用方约定写 tmp 目录的全新文件)。
     */
    SqlEventWriter(Path file, int seq, String task) throws IOException {
        this.fos = new FileOutputStream(file.toFile());
        this.out = new BufferedOutputStream(fos);
        out.write(("-- vb-stream task=%s seq=%016d\n".formatted(task, seq)).getBytes());
    }

    /** 责任:写裸 BEGIN;(事务外壳语句——多事务共文件时逐事务包裹)。 */
    @Override
    public void writeBegin(TransactionMarker marker, SourceRecord raw) throws IOException {
        out.write("BEGIN;\n".getBytes());
    }

    /**
     * 责任:渲染一条数据事件为语句行。边界:渲染失败(无主键 u/d、载荷形态非法等)抛
     * IllegalArgumentException → 调用方停引擎、offset 不推进、重启重放(不丢数据契约)。
     */
    @Override
    public void writeEvent(SourceRecord record) throws IOException {
        ParsedEvent event = EnvelopeParser.parse(record);
        out.write(SqlRenderer.render(event).getBytes());
        events++;
    }

    /** 责任:写裸 COMMIT;。 */
    @Override
    public void writeCommit(TransactionMarker marker, SourceRecord raw) throws IOException {
        out.write("COMMIT;\n".getBytes());
    }

    /** 责任:flush + fsync(publish 前的完整性保证,之后由调用方原子 rename)。幂等:重复调用直接返回。 */
    @Override
    public void finish() throws IOException {
        out.flush();
        fos.getFD().sync();
    }

    /** 已写数据事件数(不含 BEGIN/COMMIT 边界语句——与 binary 的 recordCount 口径差异记档)。 */
    @Override
    public long recordCount() {
        return events;
    }

    /** 责任:关闭底层流(未 finish 的半成品由调用方删除 tmp 文件)。 */
    @Override
    public void close() throws IOException {
        out.close();
    }
}
