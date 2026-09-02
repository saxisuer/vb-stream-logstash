package org.vastdata.debezium.connector.postgresql.stream;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;
import org.vastdata.debezium.connector.postgresql.stream.protocol.PgOutputMessage;
import org.vastdata.debezium.connector.postgresql.stream.protocol.RelationColumn;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RelationSnapshot} 单测:快照截止(maxSeq 之上版本被截)、asOf 二分(≤ asOfSeq 最新版)、
 * 省略 oid 的 require fail-fast("未先行到达")、find 快照内最新视图,以及发布语义补钉——
 * 快照交出后 reader 继续追加 / prune 不影响已发快照(handoff 瞬间冻结的关键拼图)。
 * 引擎 {@code vb-stream-engine} 的 {@code RelationSnapshotTest}(62 行)前三用例 1:1 翻译,
 * 第四用例为补钉。Relation 无需经真实字节构造——直接 new record(组件见 protocol 包);
 * Table 以 {@code Table.editor()} 造最小假 Table,不连库。
 */
class RelationSnapshotTest {

    /**
     * 构造单 oid 的 wire Relation 样本。
     * 边界:streamXid 置 empty(快照语义只关心 oid/表名,与流式前缀无关)、replicaIdentity
     * 取默认 'd'、列集与 Table 侧单列同名同序。
     */
    private static PgOutputMessage.Relation rel(int oid, String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table, 'd',
                List.of(new RelationColumn("c", 25, -1, true)));
    }

    /**
     * 构造最小假 Debezium Table:TableId(public.table) + 单列 c(text)。
     * 步骤:Table.editor() 起手,tableId 定位,addColumn 补一列(jdbcType=VARCHAR、typeName=text),
     * create() 收口——模拟 Task 7 JDBC enrich 的最小形态。
     * 边界:catalog 传 null(PG 连接器惯例,TableId 内部归一为空串);不连库。
     */
    private static Table table(String name) {
        return Table.editor()
                .tableId(new TableId(null, "public", name))
                .addColumn(Column.editor().name("c").jdbcType(Types.VARCHAR).type("text").create())
                .create();
    }

    /**
     * 构造 wire + Table 同名配对的版本载荷。
     * 边界:无——纯样本工厂;两形态表名一致是测试约定(真实流里 Task 7 enrich 保证一致)。
     */
    private static ResolvedRelation resolved(int oid, String name) {
        return new ResolvedRelation(rel(oid, name), table(name));
    }

    /**
     * 快照只含 ≤ maxSeq 的版本前缀:maxSeq=20 截去 v3;快照内 asOf 二分按 ≤ asOfSeq 取版
     * (15→v1、20→v2),find 亦只见截止后的最新版。
     * 关键步骤:三版本入日志 → snapshot(Set.of(1), 20) → require 两个查询点 + find 断言
     * (未截断的全量拷入会得 v3);20 查询点顺带断言 Table 侧同版本。
     * 边界:截止 seq 恰落在版本切换点上(20 = v2 的 seq)。
     */
    @Test
    void snapshotCutsVersionsAboveMaxSeq() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, resolved(1, "v1"));
        registry.accept(20L, resolved(1, "v2"));
        registry.accept(30L, resolved(1, "v3"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 20L);
        assertEquals("v2", snap.require(1, 20L).wire().table());   // ≤20 的最新版
        assertEquals("v2", snap.require(1, 20L).table().id().table());   // Table 视图同版本
        assertEquals("v1", snap.require(1, 15L).wire().table());
        assertEquals(Optional.of("v2"), snap.find(1).map(r -> r.wire().table()));   // 未截断的全量拷入会得 v3
    }

    /**
     * oid 无任何版本时被快照省略:require 以"未先行到达"fail-fast,find 走宽松视图返回 empty。
     * 关键步骤:空 registry 上 snapshot(Set.of(99), 100) → 两向断言。
     * 边界:省略的 oid 不含 key——回放期报错时机与直查 registry 一致。
     */
    @Test
    void oidWithoutVersionsIsOmittedAndRequiresFails() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        RelationSnapshot snap = registry.snapshot(Set.of(99), 100L);
        assertThrows(IllegalStateException.class, () -> snap.require(99, 50L));  // "未先行到达"
        assertTrue(snap.find(99).isEmpty());
    }

    /**
     * find 返回快照内该 oid 的最新版本(≤ maxSeq 截止后的末位),miss 为 empty。
     * 关键步骤:两版本入日志 → snapshot 截止 6 → find 答 new(末位),TableId 互证。
     * 边界:多版本下 find 取末位而非首版。
     */
    @Test
    void findReturnsLatestWithinSnapshot() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(5L, resolved(1, "old"));
        registry.accept(6L, resolved(1, "new"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 6L);
        assertEquals(Optional.of("new"), snap.find(1).map(r -> r.wire().table()));
        assertEquals(Optional.of("new"), snap.find(1).map(r -> r.table().id().table()));
    }

    /**
     * 发布语义补钉(引擎测试未覆盖,红线点名):快照在 handoff 瞬间冻结——交出后 reader 继续
     * accept 新版本 / pruneBelow 剪低水位,都不影响已发快照的查询结果(consumer 不共享
     * registry,前缀拷贝 + 各自独立的列表使两者无共享可变状态)。
     * 关键步骤:v1@10 / v2@20 入日志 → snapshot 截止 20 → registry 再入 v3@30 并 pruneBelow(50)
     * (registry 侧只剩 v2)→ 快照侧 require(1,20) 仍答 v2、find 仍答 v2(v3 从未入快照,
     * 剪枝也够不着快照自己的列表)→ registry 侧互证剪枝确实发生了:prune(50) 的 floor 是
     * v3@30,registry 只剩 v3,require(1,29)(v2 生效期内)抛 ISE 即 v1/v2 已被剪掉。
     * 边界:快照查询点取截止 seq 本身;若实现误共享列表(如浅引用 registry 的 List),
     * prune 的 subList.clear 会连带剪空快照、本用例即红。
     */
    @Test
    void snapshotStaysFrozenAfterLaterAcceptAndPrune() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, resolved(1, "v1"));
        registry.accept(20L, resolved(1, "v2"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 20L);
        registry.accept(30L, resolved(1, "v3"));     // reader 交接后继续追加
        registry.pruneBelow(50L);                    // 并在下一个完结点剪枝
        assertEquals("v2", snap.require(1, 20L).wire().table());   // 快照不动
        assertEquals("v1", snap.require(1, 15L).wire().table());   // 前缀里的旧版也不被剪
        assertEquals(Optional.of("v2"), snap.find(1).map(r -> r.wire().table()));
        assertEquals("v3", registry.require(1, 50L).wire().table());   // registry 侧 floor(50)=v3
        assertThrows(IllegalStateException.class, () -> registry.require(1, 29L));   // v1/v2 已剪
    }
}
