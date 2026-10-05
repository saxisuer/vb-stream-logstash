package org.vastdata.vbstream.walsource.replay;

/**
 * 截断 UPDATE 的重建产物——spike {@code Reconstructed} 同名结构（字段签名按任务书）。
 *
 * <p>两重建路径（值编码 {@code reconstructClassTruncated} / rawTail splice
 * {@code reconstructTruncated}）共用本载体：tail 落 rawTailStore 供后续截断继续
 * splice，row 进 {@link HeapEvent} 交付。Task 12 自愈校验以本 record 为候选面；
 * Task 13 增<strong>派生档</strong>（prefix &gt; 88：值编码不可重建，按候选行 +
 * 记录中段派生九字段值行）——tail 为 null（无从重建完整 tail，下游条件跳过
 * tail 落键，后续截断走值编码/自愈路径）。</p>
 *
 * @param tail 重建后的 raw tail（自 heap-tuple offset 23 起的完整字节）；派生档 null
 * @param row  值行——重建档为全列解码产物，派生档仅填 {@link CatalogRow.ClassRow}
 *             消费的九个词典槽位（其余槽 null，fromDecoded 不读）
 */
public record Reconstruction(byte[] tail, Object[] row) {
}
