package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;

/**
 * 落地文件的事件写入接口：具体格式（当前 VBFG 二进制，未来可扩展 json）各自的实现。
 *
 * <p>事务边界（BEGIN/COMMIT）由 {@code FileRollingWriter} 解析后传入；数据事件只传原始
 * {@link SourceRecord}，由实现自行决定解析方式。逻辑移植自 vb-cdc-file-transform 仓
 * cdc-capture 的 EventFileWriter（2026-09-11 快照）。
 */
interface EventFileWriter extends AutoCloseable {

    void writeBegin(TransactionMarker marker, SourceRecord raw) throws IOException;

    void writeEvent(SourceRecord record) throws IOException;

    void writeCommit(TransactionMarker marker, SourceRecord raw) throws IOException;

    /** 完成写入（FOOTER/CRC/flush/fsync），之后由调用方原子 rename 发布。 */
    void finish() throws IOException;

    long recordCount();

    @Override
    void close() throws IOException;
}
