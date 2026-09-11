package org.vastdata.vbstream.reader.format;

/** 事务提交标记。文件切分只发生在 COMMIT 之后，保证每个文件自包含完整事务。 */
public record CommitRecord(long txid, long lsn, long timestamp) implements Record {
}
