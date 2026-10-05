package org.vastdata.vbstream.walsource.replay;

/**
 * 截断 UPDATE 的重建产物——spike {@code Reconstructed} 同名结构（字段签名按任务书）。
 *
 * <p>两重建路径（值编码 {@code reconstructClassTruncated} / rawTail splice
 * {@code reconstructTruncated}）共用本载体：tail 落 rawTailStore 供后续截断继续
 * splice，row 进 {@link HeapEvent} 交付。Task 12 自愈校验以本 record 为候选面。</p>
 *
 * @param tail 重建后的 raw tail（自 heap-tuple offset 23 起的完整字节）
 * @param row  重建 tail 按记录携带的头字段（infomask/infomask2/t_hoff）解码的值行
 */
public record Reconstruction(byte[] tail, Object[] row) {
}
