package org.vastdata.debezium.connector.postgresql.stream;

import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.TruncateOption;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * TRUNCATE 变更。一条 TRUNCATE 语句可截断多表:一次变更携带全部受影响表的 wire Relation 快照。
 * 引擎 {@code org.vastdata.vbstream.replication.TruncateChange} 的 1:1 重写(文字参照,
 * 非依赖)+ <b>seq 偏差组件</b>(见 {@link TxChange#seq()})。
 *
 * @param relations 全部受影响表的 wire 元数据快照(顺序与协议 relationOids 一致;
 *                  Debezium Table 由下游按 (oid, seq) 从桶快照解析)
 * @param options   TRUNCATE 选项(CASCADE / RESTART_IDENTITY)
 * @param streamXid 所属(子)事务 xid,见 {@link TxChange#streamXid()}
 * @param seq       消息序号(CQ index),见 {@link TxChange#seq()}——connector 偏差组件
 */
public record TruncateChange(List<PgOutputMessage.Relation> relations, Set<TruncateOption> options,
                             OptionalLong streamXid, long seq) implements TxChange {

    /** 防御性拷贝:options/relations 收集为不可变集合,保证值对象语义;relations/options 为 null 或含 null 元素时抛 NullPointerException(List.copyOf/Set.copyOf 行为)。 */
    public TruncateChange {
        relations = List.copyOf(relations);
        options = Set.copyOf(options);
    }
}
