package org.vastdata.vbstream.replication;

import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.Optional;

/**
 * Relation 宽松查询视图（find 语义：miss 返回 empty，供渲染降级为 "oid:N"）。
 * 存在动机（1.7 设计 §4.3）：逐消息渲染的 Relation 来源随线程不同——live 解码点在 reader 线程
 * 传版本日志最新视图，回放解码点在 consumer 线程传桶内不可变快照；本接口是两者的公共形态，
 * 使 ConsoleRenderer 不依赖具体 registry 实现。
 * 线程约束：接口本身无状态；实现方自行保证线程语义（{@link RelationRegistry} 系并发安全，
 * {@link VersionedRelationRegistry} 版仅限 reader 单写者线程，{@link RelationSnapshot} 版不可变、线程任意）。
 */
public interface RelationLookup {

    /**
     * 责任：按 oid 查最新已知 Relation（宽松视图）。
     * 边界：未知 oid 返回 {@link Optional#empty()}——调用方降级渲染（如 "oid:N"），不 fail-fast；
     * 需要严格语义（缓存 miss 即协议流异常）的场景请用各实现的 require 系方法。
     *
     * @param relationOid 表 oid
     * @return 最新已知的表定义；未知返回 empty
     */
    Optional<PgOutputMessage.Relation> find(int relationOid);
}
