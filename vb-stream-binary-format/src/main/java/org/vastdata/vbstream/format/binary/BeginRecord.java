package org.vastdata.vbstream.format.binary;

/** 事务开始标记。 */
public record BeginRecord(long txid, long lsn) implements Record {
}
