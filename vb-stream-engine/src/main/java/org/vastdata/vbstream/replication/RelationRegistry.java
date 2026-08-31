package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * oid → Relation 元数据缓存。Relation 消息（含流式块内重复下发）统一入缓存；DML 前必有 Relation。
 * 写入方为 run 循环单线程，但 listener 可能查询，保守起见用 ConcurrentHashMap 保证线程安全。
 * 可继承用于版本化扩展（如 {@link VersionedRelationRegistry} 按 seq 保留多版本、支持 asOf 查询；
 * 注意子类自行接管全部读写，不再复用本类的并发缓存）。
 * 实现 {@link RelationLookup}（1.7 设计 §4.3）：find 语义（miss 返回 empty，供渲染降级）天然吻合，
 * 使逐消息渲染回调（{@code PgOutputListener.onMessage}）以接口形态同时接纳本类与其不可变快照形态。
 */
public class RelationRegistry implements RelationLookup {

    private final Map<Integer, PgOutputMessage.Relation> relations = new ConcurrentHashMap<>();

    public void accept(PgOutputMessage message) {
        if (message instanceof PgOutputMessage.Relation relation) {
            relations.put(relation.relationOid(), relation);
        }
    }

    /** {@link RelationLookup#find} 的实现：最新覆盖式视图，miss 返回 empty（语义见接口）。 */
    @Override
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
