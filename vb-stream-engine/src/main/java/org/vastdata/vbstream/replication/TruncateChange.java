package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;
import org.vastdata.vbstream.protocol.TruncateOption;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * TRUNCATE 变更。一条 TRUNCATE 语句可截断多表：一次变更携带全部受影响表的 Relation 快照。
 *
 * @param relations  全部受影响表的元数据快照（顺序与协议 relationOids 一致）
 * @param options    TRUNCATE 选项（CASCADE / RESTART_IDENTITY）
 * @param streamXid  所属（子）事务 xid，见 {@link TxChange#streamXid()}
 */
public record TruncateChange(List<PgOutputMessage.Relation> relations, Set<TruncateOption> options,
                             OptionalLong streamXid) implements TxChange {

    /** 防御性拷贝：options/relations 收集为不可变集合，保证值对象语义；relations/options 为 null 或含 null 元素时抛 NullPointerException（List.copyOf/Set.copyOf 行为）。 */
    public TruncateChange {
        relations = List.copyOf(relations);
        options = Set.copyOf(options);
    }
}
