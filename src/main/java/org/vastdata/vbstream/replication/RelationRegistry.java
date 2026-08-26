package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * oid → Relation 元数据缓存。Relation 消息（含流式块内重复下发）统一入缓存；DML 前必有 Relation。
 * 写入方为 run 循环单线程，但 listener 可能查询，保守起见用 ConcurrentHashMap 保证线程安全。
 */
public final class RelationRegistry {

    private final Map<Integer, PgOutputMessage.Relation> relations = new ConcurrentHashMap<>();

    public void accept(PgOutputMessage message) {
        if (message instanceof PgOutputMessage.Relation relation) {
            relations.put(relation.relationOid(), relation);
        }
    }

    public Optional<PgOutputMessage.Relation> find(int relationOid) {
        return Optional.ofNullable(relations.get(relationOid));
    }

    /** 缓存 miss 即协议流异常（Relation 必先于 DML 到达），fail-fast。 */
    public PgOutputMessage.Relation require(int relationOid) {
        PgOutputMessage.Relation relation = relations.get(relationOid);
        if (relation == null) {
            throw new IllegalStateException("Relation oid=" + relationOid + " 未先行到达，协议流异常");
        }
        return relation;
    }
}
