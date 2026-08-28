package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RelationSnapshot 单测：快照截止（maxSeq 之上版本被截）、asOf 二分（≤ asOfSeq 最新版）、
 * 省略 oid 的 require fail-fast（"未先行到达"）、find 快照内最新视图。
 * Relation 消息无需经真实 PgWire 构造——直接 new Relation record（组件见 protocol 包）。
 */
class RelationSnapshotTest {

    /**
     * 构造单 oid 的 Relation 消息 record。
     * 边界：streamXid 置 empty（快照语义只关心 oid/表名，与流式前缀无关）、replicaIdentity 取默认 'd'、
     * 列集为空（渲染断言仅用 table 组件）。
     */
    private static PgOutputMessage.Relation rel(int oid, String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table, 'd', List.of());
    }

    /** 快照只含 ≤ maxSeq 的版本前缀：maxSeq=20 截去 v3；快照内 asOf 二分按 ≤ asOfSeq 取版（15→v1、20→v2）。 */
    @Test
    void snapshotCutsVersionsAboveMaxSeq() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(10L, rel(1, "v1"));
        registry.accept(20L, rel(1, "v2"));
        registry.accept(30L, rel(1, "v3"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 20L);
        assertEquals("v2", snap.require(1, 20L).table());   // ≤20 的最新版
        assertEquals("v1", snap.require(1, 15L).table());
    }

    /** oid 无任何版本时被快照省略：require 以"未先行到达"fail-fast，find 走宽松视图返回 empty。 */
    @Test
    void oidWithoutVersionsIsOmittedAndRequiresFails() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        RelationSnapshot snap = registry.snapshot(Set.of(99), 100L);
        assertThrows(IllegalStateException.class, () -> snap.require(99, 50L));  // "未先行到达"
        assertTrue(snap.find(99).isEmpty());
    }

    /** find 返回快照内该 oid 的最新版本（≤ maxSeq 截止后的末位），miss 为 empty。 */
    @Test
    void findReturnsLatestWithinSnapshot() {
        VersionedRelationRegistry registry = new VersionedRelationRegistry();
        registry.accept(5L, rel(1, "old"));
        registry.accept(6L, rel(1, "new"));
        RelationSnapshot snap = registry.snapshot(Set.of(1), 6L);
        assertEquals(Optional.of("new"), snap.find(1).map(PgOutputMessage.Relation::table));
    }
}
