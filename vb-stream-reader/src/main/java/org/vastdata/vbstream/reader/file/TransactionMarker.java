package org.vastdata.vbstream.reader.file;

/**
 * 事务元数据标记（连接器 {@code provide.transaction.metadata=true} 时输出的
 * BEGIN/END 记录解析结果）。txid 为字符串形态——vanilla 形如 {@code "750:1234"}，
 * 本项目连接器为纯数字（与元数据提供者同源）。
 */
record TransactionMarker(boolean begin, String txid, long eventCount, long timestampMs) {
}
