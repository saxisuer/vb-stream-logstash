package org.vastdata.vbstream.walsource.changes;

import java.time.Instant;
import java.util.Map;

/**
 * 变更事件输出契约（v2 MS2）——发射层把一笔已提交事务以
 * {@code onBegin(BatchBegin) → onRow(RowChange)* → onEnd(BatchEnd)} 的单回调序列
 * 交付下游（对拍/落地渲染），回滚事务单发 {@code onAborted(BatchAborted)}。
 * 事件 record 家族与 {@link DmlKind} 枚举按仓库收纳惯例（参照 replay 包
 * {@code CatalogRow} 的 record 收纳方式）嵌套于本文件。
 *
 * <p><strong>回调语义</strong>：onBegin 先于本事务任何 onRow；onEnd 返回 = 下游确认
 * 完整消费（调用方据此推进输出前沿——与引擎 2.0 的 End 锚定语义同构）；onAborted
 * 与 Begin/Row 序列互斥（回滚事务不回放行）。值面：RowChange 的 before/after 是
 * {@code DiskValueRenderer} 解码后的列名→值 map（INSERT 的 before 为 null、DELETE
 * 的 after 为 null、UPDATE 双像齐——replica identity 面由发射层裁剪）。</p>
 *
 * <p>线程约束：实现方按发射线程（consumer）单线程回调假设；接口自身无状态，
 * 任意实现形态自由。</p>
 */
public interface ChangeOutputListener {

    /**
     * 一笔事务的批量回放开始（对拍契约字段齐）。
     *
     * <p>注意 {@code expectedChanges} 的语义是 <strong>Begin 发射时点的记账值</strong>
     * ——流式发射形态下桶尚在填充，该值可为 0 或部分值（预检 #6 裁定）；下游头行的
     * 最终计数一律以 {@link BatchEnd#expectedChanges}（End 携带的记账终值）组装，
     * 不得用本值。</p>
     *
     * @param xid             事务 id
     * @param twoPhase        是否两阶段提交形态（COMMIT PREPARED 触发的发射）
     * @param gid             两阶段全局事务名（非两阶段为 null）
     * @param commitLsn       提交记录 LSN（xid 归属锚）
     * @param endLsn          提交记录末尾 LSN（输出前沿推进锚）
     * @param commitTs        提交时间戳（提交记录携带）
     * @param expectedChanges Begin 时点的预期变更数（记账值，可为 0——语义见方法 javadoc）
     */
    record BatchBegin(long xid, boolean twoPhase, String gid, long commitLsn, long endLsn,
                      Instant commitTs, long expectedChanges) {
    }

    /**
     * 单行变更（值 = 磁盘格式解码后的可读对象）。
     *
     * <p><strong>本 record 刻意无 originXid 字段</strong>（预检 #5 裁定）：桶内流式
     * 大事务的归属记账是发射层（Grouper）的私有面，不泄漏进输出契约——同一笔 Begin/
     * End 序列内的 RowChange 天然同源。</p>
     *
     * @param table  表身份 + 列序（as-of 快照解析，行自描述）
     * @param dml    行操作种类（wal-source 自有枚举，不 import 引擎）
     * @param before 前像（UPDATE/DELETE 的 replica identity 列或全列；INSERT 为 null）
     * @param after  后像（INSERT/UPDATE；DELETE 为 null）
     */
    record RowChange(TableMeta table, DmlKind dml, Map<String, Object> before, Map<String, Object> after) {
    }

    /**
     * 一笔事务的批量回放结束。
     *
     * @param xid             事务 id（与 {@link BatchBegin#xid()} 同源）
     * @param emittedChanges  实际交付的行数（过滤后实付数）
     * @param expectedChanges 记账终值——下游头行的权威变更计数（预检 #6 裁定：
     *                        Begin 携带的是时点值，最终值由本字段承载）
     */
    record BatchEnd(long xid, long emittedChanges, long expectedChanges) {
    }

    /**
     * 一笔回滚（中止）事务的信号——不回放行，仅告知下游该事务存在且被丢弃
     * （对拍面据此对齐事务计数）。
     *
     * @param xid 事务 id
     */
    record BatchAborted(long xid) {
    }

    /**
     * 行操作种类（wal-source 自有枚举——刻意不复用引擎枚举，模块依赖方向单向）。
     */
    enum DmlKind {

        /** 插入（XLOG_HEAP_INSERT / MULTI_INSERT entry）。 */
        INSERT,

        /** 更新（XLOG_HEAP_UPDATE / HOT UPDATE）。 */
        UPDATE,

        /** 删除（XLOG_HEAP_DELETE）。 */
        DELETE
    }

    /**
     * 批量回放开始回调（回放线程；先于本事务任何 onRow）。
     *
     * @param begin 事务头事件
     */
    void onBegin(BatchBegin begin);

    /**
     * 单行变更回调（回放线程；Begin 之后、End 之前，按 WAL 序）。
     *
     * @param row 行变更事件
     */
    void onRow(RowChange row);

    /**
     * 批量回放结束回调（回放线程；返回 = 下游确认完整消费，输出前沿随之推进）。
     *
     * @param end 事务尾事件
     */
    void onEnd(BatchEnd end);

    /**
     * 回滚事务信号回调（回放线程；与 Begin/Row 序列互斥）。
     *
     * @param aborted 回滚事件
     */
    void onAborted(BatchAborted aborted);
}
