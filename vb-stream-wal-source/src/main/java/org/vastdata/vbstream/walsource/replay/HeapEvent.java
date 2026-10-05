package org.vastdata.vbstream.walsource.replay;

/**
 * 一条 watched 关系（catalog）上的 heap 级行事件——{@code CatalogReplay.heapEvents}
 * 的提取产物，由 {@code applyCatalogRecord} 施加到 {@link CatalogStores}。
 *
 * <p>注意：{@code row} 是数组引用，record 生成的 equals 对数组字段按引用比较——
 * 事件不得用于值等同断言，须逐列取值（测试与 Task 12 自愈校验同理）。</p>
 *
 * @param op      操作码：{@link #INS} / {@link #DEL} / {@link #UPD}
 * @param oldCtid 旧行物理 ctid 键（INS 为 0；UPD 为旧位、DEL 为被删位）
 * @param newCtid 新行物理 ctid 键（DEL 为 0；ctid 键式见
 *                {@code CatalogReplay.ctidKey}）
 * @param row     解码后的新行值（DEL 为 null；长度 = natts）
 */
public record HeapEvent(int op, long oldCtid, long newCtid, Object[] row) {

    /** 操作码：插入（含 MULTI_INSERT 逐行、image 路径提取）。 */
    public static final int INS = 0;

    /** 操作码：删除。 */
    public static final int DEL = 1;

    /** 操作码：更新（含截断重建与 image 路径提取）。 */
    public static final int UPD = 2;
}
