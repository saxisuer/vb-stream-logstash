package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TupleData;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * 行变更。before/after 语义沿用协议：INSERT 仅 after；UPDATE before 可选（replica identity 决定）、
 * after 必有；DELETE 仅 before。两者统一 Optional 以避免 null 组件（对 spec §3 的实现细化）。
 *
 * @param dml       DML 种类
 * @param relation  变更时刻的表元数据快照（嵌入而非引用 registry——表定义变化时协议会重发 Relation，
 *                  逐变更快照天然对齐；下游自包含，无需 registry）
 * @param before    旧元组：DELETE 必有；UPDATE 取决于 replica identity；INSERT 恒 empty
 * @param after     新元组：INSERT/UPDATE 必有；DELETE 恒 empty
 * @param streamXid 所属（子）事务 xid，见 {@link TxChange#streamXid()}
 */
public record RowChange(DmlKind dml, PgOutputMessage.Relation relation,
                        Optional<TupleData> before, Optional<TupleData> after,
                        OptionalLong streamXid) implements TxChange {

    /** null 宽容归一：组装器可能传 null（如 Delete 的 after），统一归为 empty，避免 null 组件。 */
    public RowChange {
        before = before == null ? Optional.empty() : before;
        after = after == null ? Optional.empty() : after;
    }
}
