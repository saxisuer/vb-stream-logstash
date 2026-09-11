package org.vastdata.vbstream.format;

/**
 * 落地文件内的记录。文件布局见 {@link ChangeFileWriter}。
 * 移植自 vb-cdc-file-transform 仓 cdc-file-format 的 Record（2026-09-11 快照）。
 */
public sealed interface Record
        permits TableDefRecord, BeginRecord, EventRecord, CommitRecord, TruncateRecord {
}
