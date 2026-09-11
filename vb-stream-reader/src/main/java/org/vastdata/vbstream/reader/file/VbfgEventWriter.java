package org.vastdata.vbstream.reader.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.vastdata.vbstream.reader.format.ChangeFileWriter;

import java.io.IOException;
import java.nio.file.Path;

/**
 * VBFG 二进制格式的事件写入器：解析 envelope → 内部精简格式写入 {@link ChangeFileWriter}。
 * 事务内的 lsn 由本类跟踪（BEGIN 记录携带最近一次事件的 lsn——事务边界记录本身无数据 lsn）。
 * 逻辑移植自 vb-cdc-file-transform 仓 cdc-capture 的 BinaryEventWriter（2026-09-11 快照，
 * txid 解析增加纯数字形态的直接解析——本项目连接器的事务 id 为纯数字，hash 兜底会丢失可读性）。
 */
final class VbfgEventWriter implements EventFileWriter {

    private final ChangeFileWriter out;
    private long lastLsn;

    VbfgEventWriter(Path file, int seq, String task) throws IOException {
        this.out = new ChangeFileWriter(file, seq, task);
    }

    @Override
    public void writeBegin(TransactionMarker marker, SourceRecord raw) throws IOException {
        out.writeBegin(txidLong(marker.txid()), lastLsn);
    }

    @Override
    public void writeEvent(SourceRecord record) throws IOException {
        ParsedEvent event = EnvelopeParser.parse(record);
        lastLsn = event.lsn();
        if (event.truncate()) {
            out.writeTruncate(event.tableDef());
        } else {
            out.writeEvent(event.tableDef(), event.op(), event.values());
        }
    }

    @Override
    public void writeCommit(TransactionMarker marker, SourceRecord raw) throws IOException {
        out.writeCommit(txidLong(marker.txid()), lastLsn, marker.timestampMs());
    }

    @Override
    public void finish() throws IOException {
        out.finish();
    }

    @Override
    public long recordCount() {
        return out.recordCount();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    /**
     * 责任：txid 字符串 → 长整型。关键步骤：纯数字（本项目连接器形态）直接 parseLong；
     * 复合形态 {@code "750:1234"}（vanilla）高 32 位装事务序号、低 32 位装子序号打包；
     * 其余形态哈希兜底（保持可区分，可读性损失可接受）。
     * 边界：数字段解析失败落入哈希兜底，不抛。
     */
    private static long txidLong(String txid) {
        if (txid == null) {
            return 0L;
        }
        int colon = txid.indexOf(':');
        if (colon < 0) {
            try {
                return Long.parseLong(txid);
            } catch (NumberFormatException ignored) {
                // 落入哈希兜底
            }
        } else if (colon > 0) {
            try {
                return Long.parseLong(txid.substring(0, colon)) << 32
                        | (Long.parseLong(txid.substring(colon + 1)) & 0xFFFFFFFFL);
            } catch (NumberFormatException ignored) {
                // 落入哈希兜底
            }
        }
        return txid.hashCode();
    }
}
