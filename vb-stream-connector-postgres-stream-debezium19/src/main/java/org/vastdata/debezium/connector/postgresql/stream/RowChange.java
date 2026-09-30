package org.vastdata.debezium.connector.postgresql.stream;

import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TupleData;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * 行变更。引擎 {@code org.vastdata.vbstream.replication.RowChange} 的 1:1 重写
 * (文字参照,非依赖)+ <b>seq 偏差组件</b>(见 {@link TxChange#seq()})。
 * before/after 语义沿用协议:INSERT 仅 after;UPDATE before 可选(replica identity 决定)、
 * after 必有;DELETE 仅 before。两者统一 Optional 以避免 null 组件。
 *
 * @param dml       DML 种类
 * @param relation  变更时刻的 wire 表元数据快照(嵌入而非引用 registry——表定义变化时协议会
 *                  重发 Relation,逐变更快照天然对齐;协议列序真源。Debezium 渲染视图的
 *                  Table 由下游按 (relationOid, seq) 从桶快照解析,不嵌入本 record)
 * @param before    旧元组:DELETE 必有;UPDATE 取决于 replica identity;INSERT 恒 empty
 * @param after     新元组:INSERT/UPDATE 必有;DELETE 恒 empty
 * @param streamXid 所属(子)事务 xid,见 {@link TxChange#streamXid()}
 * @param seq       消息序号(CQ index),见 {@link TxChange#seq()}——connector 偏差组件
 */
public record RowChange(DmlKind dml, PgOutputMessage.Relation relation,
                        Optional<TupleData> before, Optional<TupleData> after,
                        OptionalLong streamXid, long seq) implements TxChange {

    /** null 宽容归一:组装器可能传 null(如 Delete 的 after),统一归为 empty,避免 null 组件。 */
    public RowChange {
        before = before == null ? Optional.empty() : before;
        after = after == null ? Optional.empty() : after;
    }
}
