package org.vastdata.debezium.connector.postgresql.stream;

import java.util.Optional;

/**
 * {@link ResolvedRelation} 宽松查询视图(find 语义:miss 返回 empty,供渲染降级为 "oid:N")。
 * 存在动机(引擎 1.7 设计 §4.3,随 MS2 重写沿用):逐消息观测点(decodedObserver)的
 * Relation 来源随线程不同——live 解码点在 reader 线程传版本日志最新视图
 * ({@link VersionedRelationRegistry}),回放解码点在 consumer 线程传桶内不可变快照
 * ({@link RelationSnapshot});本接口是两者的公共形态,使逐消息回调不依赖具体 registry
 * 实现——若回放侧闭包引用 reader 的 HashMap registry 即构成数据竞争。
 * 线程约束:接口本身无状态;实现方自行保证线程语义({@link VersionedRelationRegistry}
 * 仅限 reader 单写者线程,{@link RelationSnapshot} 不可变、线程任意)。
 */
public interface RelationLookup {

    /**
     * 责任:按 oid 查最新已知 {@link ResolvedRelation}(宽松视图)。
     * 边界:未知 oid 返回 {@link Optional#empty()}——调用方降级渲染(如 "oid:N"),不 fail-fast;
     * 需要严格语义(缓存 miss 即协议流异常)的场景请用各实现的 require 系方法
     * ({@link VersionedRelationRegistry#require(int, long)} / {@link RelationSnapshot#require(int, long)},
     * 后者包私有、经桶快照在回放路径可达)。
     *
     * @param relationOid 表 oid
     * @return 最新已知的表定义(wire + Table 双形态);未知返回 empty
     */
    Optional<ResolvedRelation> find(int relationOid);
}
