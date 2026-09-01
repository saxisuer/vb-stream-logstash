package org.vastdata.debezium.connector.postgresql.stream;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 不可变 {@link ResolvedRelation} 版本快照:reader 在桶交接(handoff)瞬间从
 * {@link VersionedRelationRegistry} 拷出(各 oid 取 seq ≤ maxSeq 的版本前缀,全新列表浅拷),
 * 随冻结桶交给 consumer 线程回放渲染——consumer 不共享 reader 的 registry,跨线程数据竞争由
 * "构造时拷贝"消除,零并发改造。<b>发布语义</b>:快照交出即与 registry 脱钩,此后 reader 继续
 * 追加 / 剪枝都不影响本快照的查询结果(引擎 1.7 设计 §4.3 的语义随 MS2 1:1 重写沿用)。
 *
 * <p>查询语义与版本日志逐字对齐:{@link #require(int, long)} 二分取 ≤ asOfSeq 的最新版
 * (回放渲染按"变更时刻"的表定义解释旧行,DDL 后不按新 schema 错解;floorIndex 与
 * {@code VersionedRelationRegistry} 同款二分);{@link #find(int)} 是快照内最新版的宽松视图
 * (live 解码点同形态,{@link RelationLookup} 实现)。被快照省略的 oid(无版本或全部晚于
 * maxSeq)require 时以"未先行到达"fail-fast——报错时机与直查 registry 一致。
 *
 * <p>线程约束:不可变、任意线程。构造后状态终生不变(入参 map 及其列表在构造前已由
 * {@link VersionedRelationRegistry#snapshot} 私有化,无外部可达引用)。
 */
final class RelationSnapshot implements RelationLookup {

    /**
     * 单版本条目。
     *
     * @param seq 该版本记入版本日志时的消息序号(组装器分配,单调递增)
     * @param rel 该时刻的表定义(wire + Table 双形态,不可变 record,引用可与 registry 侧
     *            安全共享——浅拷语义)
     */
    record Entry(long seq, ResolvedRelation rel) {}

    /** oid → 该 oid 的版本前缀(按 seq 升序拷入,仅由 registry.snapshot 构造,此后只读)。 */
    private final Map<Integer, List<Entry>> versions;

    /**
     * 责任:以已就绪的版本前缀表构造快照。
     * 边界:包私有可见性收窄——仅 {@link VersionedRelationRegistry#snapshot} 构造;入参 map 与
     * 其内列表归本实例私有(调用方新建、无泄漏),本类不再防御性拷贝(registry 侧已 List.copyOf)。
     * 线程约束:构造发生在 reader 单写者线程,构造完成后任意线程只读。
     *
     * @param versions oid → 升序版本前缀(各 oid 无版本或全部晚于截止 seq 时该 key 整个省略)
     */
    RelationSnapshot(Map<Integer, List<Entry>> versions) {
        this.versions = versions;   // 仅由 registry.snapshot 构造,入参即私有(构造后不再改)
    }

    /**
     * 责任:取 oid 在 asOfSeq 时刻生效的 {@link ResolvedRelation}(≤ asOfSeq 的最新版本)。
     * 关键步骤:查该 oid 的版本前缀 → 手写二分求 floor(seq ≤ asOfSeq 的最大下标,写法照
     * {@code VersionedRelationRegistry.floorIndex})→ 返回命中版本的 rel。
     * 边界:oid 被快照省略(无版本/全部晚于 maxSeq)或全部版本晚于 asOfSeq 时抛
     * {@link IllegalStateException},消息风格与 {@code VersionedRelationRegistry.require} 一致
     * (附 oid 与 asOf 便于定位,"Relation 未先行到达"= 协议流异常)。
     * 线程约束:不可变、任意线程(consumer 回放线程调用)。
     *
     * @param relationOid 表 oid
     * @param asOfSeq     查询时刻的消息序号(取该时刻已到达的最新版本)
     * @return asOfSeq 时刻生效的表定义(wire + Table 双形态)
     */
    ResolvedRelation require(int relationOid, long asOfSeq) {
        List<Entry> list = versions.get(relationOid);
        int idx = (list == null) ? -1 : floorIndex(list, asOfSeq);
        if (idx < 0) {
            throw new IllegalStateException(
                    "Relation oid=" + relationOid + " 未先行到达（asOf seq=" + asOfSeq + "），协议流异常");
        }
        return list.get(idx).rel();
    }

    /**
     * 责任:快照内该 oid 的最新版本(宽松视图)。
     * 关键步骤:取版本前缀末位(升序保证末位即最新)。
     * 边界:oid 被省略或前缀为空返回 {@link Optional#empty()}——调用方降级渲染,不 fail-fast。
     * 线程约束:不可变、任意线程。
     */
    @Override
    public Optional<ResolvedRelation> find(int relationOid) {
        List<Entry> list = versions.get(relationOid);
        return (list == null || list.isEmpty()) ? Optional.empty()
                : Optional.of(list.get(list.size() - 1).rel());
    }

    /**
     * 责任:手写二分求 floor——"seq ≤ target 的最新版本"下标。
     * 关键步骤:标准两端收缩——中位 seq ≤ target 记为候选并右移下界,否则左移上界。
     * 边界:前缀为空或全部版本 seq &gt; target 时返回 -1(由调用方按"未先行到达"fail-fast 处理)。
     * 前置:list 按 seq 升序(registry.snapshot 拷入顺序保证);静态无状态、线程任意。
     */
    private static int floorIndex(List<Entry> list, long target) {
        int lo = 0;
        int hi = list.size() - 1;
        int ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (list.get(mid).seq() <= target) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return ans;
    }
}
