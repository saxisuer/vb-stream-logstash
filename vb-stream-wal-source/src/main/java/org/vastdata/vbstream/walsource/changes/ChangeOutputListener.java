package org.vastdata.vbstream.walsource.changes;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 变更事件输出契约（v2 MS2）——发射层把一笔已提交事务以
 * {@code onBegin(BatchBegin) → (onRow(RowChange) | onTruncate(TruncateEvent))* →
 * onEnd(BatchEnd)} 的单回调序列交付下游（对拍/落地渲染），回滚事务单发
 * {@code onAborted(BatchAborted)}。事件 record 家族与 {@link DmlKind} 枚举按仓库
 * 收纳惯例（参照 replay 包 {@code CatalogRow} 的 record 收纳方式）嵌套于本文件。
 *
 * <p><strong>回调语义</strong>：onBegin 先于本事务任何 onRow/onTruncate；onEnd 返回 =
 * 下游确认完整消费（调用方据此推进输出前沿——与引擎 2.0 的 End 锚定语义同构）；
 * onAborted 与 Begin/Row/Truncate 序列互斥（回滚事务不回放行）。值面：RowChange 的
 * before/after 是 {@code DiskValueRenderer} 解码后的列名→值 map（INSERT 的 before
 * 为 null、DELETE 的 after 为 null、UPDATE 双像齐——replica identity 面由发射层
 * 裁剪）。</p>
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
     * <p><strong>位址三字段（2026-10-17 契约扩展）</strong>——END 发射时由发射层
     * （Grouper）填充：{@code xid} 与 {@link BatchBegin#xid()} 同源（桶 xid——存活
     * 子事务行归并到顶层身份）、{@code commitLsn} 与 {@link BatchBegin#commitLsn()}
     * 同值（普通=COMMIT 记录 lsn / 2PC=COMMIT PREPARED 记录 lsn）、{@code lsn} = 该行
     * 所在 heap 记录的起始 LSN（到达期记账——TOAST chunk 是独立记录不占行 lsn；
     * MULTI_INSERT 多条目共享同一条记录 lsn）。v2 "桶内归属是私有面"的预检 #5 裁定
     * 就此收窄为 <b>originXid 不外泄</b>（子事务真源 id）；顶层位址三字段按 2026-10-17
     * 需求进入契约。</p>
     *
     * @param xid        事务归属 id（与 Begin/End 同源；发射时填充）
     * @param commitLsn  提交记录 lsn（与 BatchBegin.commitLsn 同值；发射时填充）
     * @param lsn        行所在 heap 记录起始 LSN（到达期已知；发射时填充）
     * @param table      表身份 + 列序（as-of 快照解析，行自描述）
     * @param op         行操作种类（wal-source 自有枚举，不 import 引擎）
     * @param before     前像（UPDATE/DELETE 的 replica identity 列或全列；INSERT 为 null）
     * @param after      后像（INSERT/UPDATE；DELETE 为 null）
     */
    record RowChange(long xid, long commitLsn, long lsn, TableMeta table, DmlKind op,
                     Map<String, Object> before, Map<String, Object> after) {

        /**
         * 四参便捷构造（位址三字段占位 0）：手造行事件的单测/宿主装配面——真实发射
         * 路径（Grouper）走全参构造，位址字段由发射层保证非零。
         *
         * @param table  表身份 + 列序
         * @param op     行操作种类
         * @param before 前像（可 null）
         * @param after  后像（可 null）
         */
        RowChange(TableMeta table, DmlKind op, Map<String, Object> before, Map<String, Object> after) {
            this(0L, 0L, 0L, table, op, before, after);
        }
    }

    /**
     * 单语句 TRUNCATE 事件（一条语句可截多表——payload relids 数组，过滤后命中表集）。
     *
     * <p>位址三字段语义与 {@link RowChange} 同源：发射层（Grouper）在 End 发射期填充
     * （到达期 commitLsn 未知占位 0）。{@code options} 经 EnumSet 拷贝——迭代序恒为
     * 声明序（渲染 toString 与 engine 逐字节同形的保证，Set.copyOf 迭代序无规格保证）。</p>
     *
     * @param xid        事务归属 id（与 Begin/End 同源；发射时填充）
     * @param commitLsn  提交记录 lsn（与 BatchBegin.commitLsn 同值；发射时填充）
     * @param lsn        truncate 记录起始 LSN（发射时填充）
     * @param tables     过滤后命中的表身份（payload 序保持；至少一表——全 miss 不产事件）
     * @param options    选项位集
     */
    record TruncateEvent(long xid, long commitLsn, long lsn, List<TableMeta> tables,
                         Set<TruncateOption> options) {

        /**
         * 两参便捷构造（位址三字段占位 0）：手造事件的单测/宿主装配面。
         *
         * @param tables   表身份集
         * @param options  选项位集
         */
        TruncateEvent(List<TableMeta> tables, Set<TruncateOption> options) {
            this(0L, 0L, 0L, tables, options);
        }

        /** 紧凑构造器：tables 防御拷贝；options 经 EnumSet 拷贝保声明序迭代。
         * （显式 public：接口嵌套 record 的无修饰紧凑构造器 javac 17 拒编——访问级
         * 与 record 同为 public，显式化后等义通过。） */
        public TruncateEvent {
            tables = List.copyOf(tables);
            options = options.isEmpty()
                    ? EnumSet.noneOf(TruncateOption.class)
                    : EnumSet.copyOf(options);
        }
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

    /** TRUNCATE 选项位（wal-source 自有枚举——声明序与 engine TruncateOption 一致，
     * EnumSet.toString 渲染同形；不 import 引擎，DmlKind 同惯例）。 */
    enum TruncateOption {

        /** heapam_xlog.h XLH_TRUNCATE_CASCADE（1<<0——级联截子表/分区）。 */
        CASCADE,

        /** heapam_xlog.h XLH_TRUNCATE_RESTART_SEQS（1<<1——RESTART IDENTITY）。 */
        RESTART_IDENTITY
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
     * 单语句 TRUNCATE 回调（回放线程；Begin 之后、End 之前，按 WAL 序——与 onRow
     * 同缝互排）。回滚事务的 truncate 随桶丢弃，不回调。
     *
     * @param event truncate 事件
     */
    void onTruncate(TruncateEvent event);

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
