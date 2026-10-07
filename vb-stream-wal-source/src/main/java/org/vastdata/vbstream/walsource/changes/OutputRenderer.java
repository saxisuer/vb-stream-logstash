package org.vastdata.vbstream.walsource.changes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 事务变更的 CDC 控制台输出器（v2 MS2 输出层）——{@link ChangeOutputListener} 的
 * 日志形态实现，输出契约复刻 engine 的 {@code ConsoleRenderer} 事务块格式
 * （vb-stream-engine ConsoleRenderer L108-116/L129-137 的 TXN-BEGIN/逐行/TXN-END
 * 三段样板，行值截断同 L200-203）：
 *
 * <pre>
 * TXN-BEGIN xid=773 kind=NORMAL gid=null commitLsn=0x46f1234 commitTs=2026-10-06T05:11:22.123456Z changes=3
 *   [1] INSERT public.t1 BEFORE=- AFTER=[id=1, s=hello]
 *   [2] UPDATE public.t1 BEFORE=[id=1] AFTER=[id=1, s=world]
 *   [3] DELETE public.t1 BEFORE=[id=1] AFTER=-
 * TXN-END   xid=773
 * </pre>
 *
 * <p><b>pending 缓冲</b>：头行的 {@code changes} 是终值语义（Begin 发射时点可为 0
 * 或部分值，预检 #6 裁定）——onBegin 只缓存头字段（xid/kind/gid/commitLsn/
 * commitTs）并开桶，onRow 把渲染好的行文本攒进当前桶，<b>onEnd 时才以
 * {@code BatchEnd.expectedChanges} 组装头行</b>并整桶 flush（头行 + 行文本 + 尾行
 * 逐条 INFO）。onAborted 丢弃整桶——零输出（回滚事务在输出面不留痕迹，事务计数
 * 对齐由事件面而非日志面承担）。</p>
 *
 * <p><b>行归属</b>：{@code RowChange} 刻意无 xid（预检 #5 裁定），行归属 = 最近一次
 * onBegin 开的桶（发射层 {@code XactGrouper} 的批量序列 Begin→Row*→End 天然满足）；
 * 交错多事务经 per-xid pending map 各自独立，flush 序 = End 到达序（= 提交序）。
 * 无开桶时 onRow 抛 ISE fail-fast（契约违背信号，不产半截行）。</p>
 *
 * <p><b>值面前提</b>：RowChange 的 before/after 值已是 {@code XactGrouper.renderRow}
 * 经 {@link DiskValueRenderer} 渲染后的 PG text 形态 String——本层只做占位与截断：
 * dropped 列恒 {@code ∅}（值不消费的占位语义，同 {@code DiskValueRenderer.renderColumn}
 * 惯例）、NULL 值字面 {@code NULL}（engine 的 TupleValue.Null 同形）、超 64 字符截断
 * 附原长（{@code ...(<原长>B)} 后缀，engine 的 truncate 同规则）；前像子集（replica
 * identity 只带键列）按 {@link TableMeta} 列序渲染 map 中<b>存在的列</b>——列序权威
 * 是 TableMeta，不从 map 迭代序取。</p>
 *
 * <p>线程约束：<b>单写者</b>（与 {@code XactGrouper} 同一发射线程顺序回调；多线程
 * 回调需外部串行化）；slf4j logger 自身线程安全。输出走 CDC 专用 logger
 * {@code org.vastdata.vbstream.walsource.cdc}（INFO）——与系统诊断日志分流的
 * 通道契约。</p>
 */
public final class OutputRenderer implements ChangeOutputListener {

    /** CDC 数据通道专用 logger 名（与 engine 的 {@code org.vastdata.vbstream.cdc} 分流惯例同构）。 */
    private static final Logger CDC = LoggerFactory.getLogger("org.vastdata.vbstream.walsource.cdc");

    /** 诊断 logger（契约违背类 WARN——不混入 CDC 数据通道）。 */
    private static final Logger LOG = LoggerFactory.getLogger(OutputRenderer.class);

    /** 值渲染截断阈值（超出截 64 字符并附原长字节数——engine ConsoleRenderer 同规则）。 */
    private static final int VALUE_TRUNCATE_LIMIT = 64;

    /** NORMAL 形态的 kind 头行值（枚举名形态，与 engine Transaction.Kind 对齐）。 */
    private static final String KIND_NORMAL = "NORMAL";

    /** 两阶段形态的 kind 头行值。 */
    private static final String KIND_TWO_PHASE = "TWO_PHASE";

    /** per-xid 待决桶（Begin 开桶缓存头字段 + 行文本，End flush / Aborted 丢弃）。 */
    private final Map<Long, Pending> pending = new HashMap<>();

    /** 当前开桶（最近一次 onBegin；RowChange 无 xid，onRow 的行归属锚）。 */
    private Pending current;

    /**
     * 批量回放开始（契约源：engine ConsoleRenderer onEvent 的 Begin 分支 L129-132——
     * 头行字段在此缓存，changes 留待 End 终值）。
     *
     * <p>关键步骤：建 Pending（头字段 + 空行列表）、入 per-xid map、置为当前开桶
     * （后续 onRow 归属于此）。边界与异常语义：同 xid 重复 onBegin（End/Aborted 缺位
     * ——xid 回绕前的序列错乱信号）WARN 一行后覆盖旧桶（旧桶行随覆盖丢失，观测面
     * 留痕而非静默吞）。线程约束：单写者。</p>
     *
     * @param begin 事务头事件
     */
    @Override
    public void onBegin(BatchBegin begin) {
        if (pending.containsKey(begin.xid())) {
            LOG.warn("同 xid 重复 onBegin 覆盖待决桶（前桶 End/Aborted 缺位）: xid={}",
                    begin.xid());
        }
        Pending p = new Pending(begin.twoPhase() ? KIND_TWO_PHASE : KIND_NORMAL,
                begin.gid(), begin.commitLsn(), begin.commitTs());
        pending.put(begin.xid(), p);
        current = p;
    }

    /**
     * 单行变更（契约源：engine ConsoleRenderer onEvent 的 TxChange 分支 L133-134——
     * {@code "  [seq] "} 前缀 + DML/表/BEFORE/AFTER 四段；行号在 flush 期分配，
     * 此处只攒行文本）。
     *
     * <p>边界与异常语义：无开桶（Begin 前的孤儿行）抛 ISE fail-fast——契约违背信号。
     * 线程约束：单写者。</p>
     *
     * @param row 行变更事件
     * @throws IllegalStateException 无开桶（onRow 先于任何未终结的 onBegin）
     */
    @Override
    public void onRow(RowChange row) {
        if (current == null) {
            throw new IllegalStateException("onRow 先于 onBegin 到达（契约：Begin 先于本事务任何 Row）");
        }
        current.rows.add(renderChange(row));
    }

    /**
     * 批量回放结束（契约源：engine ConsoleRenderer onTransaction L108-116 的整块
     * 输出形态）——以 {@code BatchEnd.expectedChanges} 终值组装头行（Begin 时点值
     * 不进头行，预检 #6 裁定），头行 + 攒齐的行文本（行号此刻分配）+ 尾行
     * （{@code TXN-END} 后三空格）逐条 INFO。
     *
     * <p>边界与异常语义：无待决桶（End 无对应 Begin，或重复 End）整段跳过 + DEBUG——
     * 不产半截事务块；尾行发出后返回即下游确认完整消费。线程约束：单写者。</p>
     *
     * @param end 事务尾事件
     */
    @Override
    public void onEnd(BatchEnd end) {
        Pending p = pending.remove(end.xid());
        if (p == null) {
            CDC.debug("TXN-END 无待决桶（Begin 缺位或已终结）: xid={}", end.xid());
            return;
        }
        if (current == p) {
            current = null;
        }
        CDC.info("TXN-BEGIN xid={} kind={} gid={} commitLsn=0x{} commitTs={} changes={}",
                end.xid(), p.kind, p.gid, Long.toHexString(p.commitLsn), p.commitTs, end.expectedChanges());
        int seq = 1;
        for (String line : p.rows) {
            CDC.info("  [{}] {}", seq++, line);
        }
        CDC.info("TXN-END   xid={}", end.xid());
    }

    /**
     * 回滚事务信号——丢弃整桶：头行/行/尾零输出（回滚在日志面不留痕迹；gid null
     * 经 slf4j 渲染为字面 {@code null} 的对齐面不受影响）。
     *
     * <p>关键步骤：per-xid map 摘桶；被弃桶若恰为当前开桶则清 current（后续孤儿
     * onRow 走 ISE 防串桶）。线程约束：单写者。</p>
     *
     * @param aborted 回滚事件
     */
    @Override
    public void onAborted(BatchAborted aborted) {
        Pending p = pending.remove(aborted.xid());
        if (current == p) {
            current = null;
        }
    }

    /**
     * 单条变更行文本：{@code DML schema.table BEFORE=tuple AFTER=tuple}（契约源：
     * engine ConsoleRenderer renderChange 的 RowChange 分支 L142-146——BEFORE/AFTER
     * 缺位渲染 {@code -}）。值已是渲染后 String，本层只占位与截断。
     *
     * <p>线程约束：静态纯函数。</p>
     *
     * @param row 行变更事件
     * @return 行文本（不含 {@code "  [seq] "} 前缀——行号 flush 期分配）
     */
    private static String renderChange(RowChange row) {
        return "%s %s.%s BEFORE=%s AFTER=%s".formatted(row.dml(), row.table().schema(),
                row.table().table(), tupleOf(row.before(), row.table()),
                tupleOf(row.after(), row.table()));
    }

    /**
     * 元组渲染：按 {@link TableMeta} 列序遍历，渲染 map 中<b>存在的列</b>为
     * {@code name=value} 项后取 {@link List#toString()}（{@code [name=value, ...]}
     * 形态——契约源：engine ConsoleRenderer tupleOf L165-173）。
     *
     * <p>关键步骤：null 元组（INSERT 前 / DELETE 后）返回 {@code -}；map 缺键的列
     * 跳过（前像 replica identity 子集）；dropped 列恒 {@code ∅}、null 值字面
     * {@code NULL}、其余截断 64。边界：空 map 渲染 {@code []}（与 null 的 {@code -}
     * 区分——空像与无像语义不同）。线程约束：静态纯函数。</p>
     *
     * @param tuple 列名→渲染值 map（可为 null；值 String 或 null）
     * @param table 表身份 + 列序（列序权威）
     * @return tuple 文本或 {@code -}
     */
    private static String tupleOf(Map<String, Object> tuple, TableMeta table) {
        if (tuple == null) {
            return "-";
        }
        List<String> parts = new ArrayList<>();
        for (ColumnMeta col : table.columns()) {
            if (!tuple.containsKey(col.name())) {
                continue;
            }
            parts.add(col.name() + "=" + valueOf(col, tuple.get(col.name())));
        }
        return parts.toString();
    }

    /**
     * 单列值文本：dropped 恒 {@code ∅}（占位语义，值不消费）、null 恒 {@code NULL}
     * （与空串区分）、超 {@link #VALUE_TRUNCATE_LIMIT} 字符截断附原长（契约源：
     * engine ConsoleRenderer truncate L200-203 的 {@code ...(<原长>B)} 后缀）。
     *
     * <p>非 String 对象兜底 String.valueOf（RowChange 值面契约是 String/null，
     * 兜底仅为防御面——不 fail-fast：值面形态错误不应吞掉整事务输出）。
     * 线程约束：静态纯函数。</p>
     *
     * @param col   列元数据（dropped 判定）
     * @param value 渲染后值（String 或 null）
     * @return 列值文本
     */
    private static String valueOf(ColumnMeta col, Object value) {
        if (col.dropped()) {
            return "∅";
        }
        if (value == null) {
            return "NULL";
        }
        String s = value.toString();
        return s.length() > VALUE_TRUNCATE_LIMIT
                ? s.substring(0, VALUE_TRUNCATE_LIMIT) + "...(" + s.length() + "B)"
                : s;
    }

    /**
     * 待决事务桶：Begin 头字段缓存（changes 除外——终值在 End）+ 攒齐的行文本。
     * flush 期头行以本桶头字段 + End 终值组装。
     */
    private static final class Pending {

        final String kind;
        final String gid;
        final long commitLsn;
        final Instant commitTs;
        final List<String> rows = new ArrayList<>();

        /**
         * 以 Begin 事件字段建桶（xid 不入桶——per-xid map 的键已承载归属）。
         *
         * @param kind      头行 kind 值（NORMAL / TWO_PHASE）
         * @param gid       两阶段 gid（非两阶段 null——头行渲染字面 null）
         * @param commitLsn 提交记录 LSN（头行 hex 无补零）
         * @param commitTs  提交时间戳（Instant.toString 的 ISO-8601 形态）
         */
        Pending(String kind, String gid, long commitLsn, Instant commitTs) {
            this.kind = kind;
            this.gid = gid;
            this.commitLsn = commitLsn;
            this.commitTs = commitTs;
        }
    }
}
