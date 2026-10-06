package org.vastdata.vbstream.walsource.changes;

import org.vastdata.vbstream.walsource.api.CatalogSnapshot;
import org.vastdata.vbstream.walsource.layout.WalLayout;
import org.vastdata.vbstream.walsource.layout.WalRecord;

import java.sql.Connection;
import java.util.Set;

/**
 * 用户表变更流门面（v2 MS2 接线层，Task 7）——单入口把走读完成的 WAL 记录委派给
 * 内建的组装三件套（TableFilter / ToastAssembler(+JdbcToastProbe) / XactGrouper），
 * 是 WalSource sink 分发的第二消费者（catalog 同步器之后，as-of 次序保证）。
 *
 * <p><b>装配裁定</b>：{@link XactGrouper} 构造内自建 TableFilter 与 TupleDecoder，
 * 本门面只补齐它构造不吸收的两件——ToastAssembler（probe 取构造入参 {@code sqlConn}
 * 建的 {@link JdbcToastProbe}，null 连接 = 回查禁用的纯回放形态）与白名单透传；
 * external 行内接线经 XactGrouper 构造时的 {@code toast::resolveExternal} 委派完成
 * （TupleDecoder 的 external varlena 解析面）。</p>
 *
 * <p>线程约束：<b>单写者</b>——onRecord 与 catalog 同步器的 apply 在同一 wal-receiver
 * 线程顺序调用（WalSource 分发次序 {@code sync.apply → onRecord}，先施加字典再解码
 * 行，as-of 正确性）；{@code sqlConn} 由此与同步器共享同一会话（SelfHealer probe 同
 * 线程串行，无并发使用——会话归 wal-receiver 单写者上下文的既有契约不破）。
 * {@link #emittedBuckets()} / {@link #emittedRows()} 为 volatile 读，任意线程可轮询。</p>
 */
public final class ChangeStream {

    private final XactGrouper grouper;

    /**
     * 装配变更流门面。
     *
     * <p>关键步骤：{@code sqlConn} 非 null 时建 {@link JdbcToastProbe}（TOAST 缺 chunk
     * 的回查兜底）→ ToastAssembler → XactGrouper（内建 TableFilter，whitelist 透传
     * null/空 = 用户表全放行）。边界与异常语义：sqlConn 为 null 合法（probe 禁用——
     * 缺 chunk 值降级 {@code toast-unavailable}，适用于无 SQL 会话的纯回放形态）。</p>
     *
     * @param snapshot  catalog as-of 快照（活引用视图——查询随字典重放推进）
     * @param sqlConn   普通 SQL 会话（TOAST 回查兜底；null = 禁用；生命周期归调用方，
     *                  单写者上下文使用）
     * @param layout    版本布局描述符（heap 视图解析用）
     * @param whitelist 表白名单（{@code schema.table} 全名集；null/空 = 全放行）
     * @param out       变更事件输出契约实现（发射线程 = onRecord 调用线程）
     */
    public ChangeStream(CatalogSnapshot snapshot, Connection sqlConn, WalLayout layout,
                        Set<String> whitelist, ChangeOutputListener out) {
        ToastProbe probe = sqlConn == null ? null : new JdbcToastProbe(sqlConn);
        this.grouper = new XactGrouper(snapshot, whitelist, layout, new ToastAssembler(probe), out);
    }

    /**
     * 喂一条走读完成的 WAL 记录（wal-receiver 线程）——委派内建 XactGrouper 的
     * rmid 分发（XACT 终态 / heap 行入桶 + toast chunk 采集 / 其余静默）。
     *
     * <p>边界与异常语义：解码/走读错位抛 ISE fail-fast（与组装器同面）；非变更承载
     * 记录（catalog 字典表 heap、PRUNE、XLOG 等）无害跳过。线程约束：单写者。</p>
     *
     * @param rec 走读完成的记录（raw 契约只读）
     */
    public void onRecord(WalRecord rec) {
        grouper.onRecord(rec);
    }

    /**
     * DML 观测面：已发射事务桶数（透传 {@link XactGrouper#emittedBuckets()}——Main
     * 周期 smoke 行的数据源）。
     *
     * @return 已发射桶数（会话累计；任意线程可读）
     */
    public long emittedBuckets() {
        return grouper.emittedBuckets();
    }

    /**
     * DML 观测面：已发射行数（过滤后实付口径，透传 {@link XactGrouper#emittedRows()}）。
     *
     * @return 已发射行数（会话累计；任意线程可读）
     */
    public long emittedRows() {
        return grouper.emittedRows();
    }

    /**
     * DML 观测面：截断 UPDATE 行级跳过计数（liveness guard 的有痕丢弃观测面，
     * Task 8——透传 {@link XactGrouper#skippedTruncatedRows()}）。
     *
     * @return 会话累计跳过的截断 UPDATE 行数（任意线程可读）
     */
    public long skippedTruncatedRows() {
        return grouper.skippedTruncatedRows();
    }
}
