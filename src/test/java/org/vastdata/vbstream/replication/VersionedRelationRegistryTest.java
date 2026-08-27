package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * VersionedRelationRegistry 的版本语义验收：同 oid 多版本按 seq 时间线记录，
 * asOf 二分取"变更发生时刻"的定义（DDL 中途重发 Relation 后，旧 seq 的行不能按新 schema 渲染）；
 * prune 的边界保留（恰好 == minSeq 保留、每 oid 至少留最新一条）与旧接缝"最新视图"兼容。
 */
class VersionedRelationRegistryTest {

    private final VersionedRelationRegistry reg = new VersionedRelationRegistry();

    /**
     * 构造单列 Relation 样本。
     * 步骤：固定 schema=public、replicaIdentity='d'、一列 (name="c", typeId=25 text, typmod=-1, partOfKey=true)，
     * 仅 oid 与表名随参数变化，用表名区分版本（断言读 {@code table()}）。
     * 边界：无——纯样本工厂，不触达被测状态。
     */
    private static PgOutputMessage.Relation rel(int oid, String name) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", name,
                'd', List.of(new Column("c", 25, -1, true)));
    }

    @Test
    void asOfTakesLatestVersionAtOrBeforeSeq() {
        reg.accept(10, rel(1, "v1"));
        reg.accept(50, rel(1, "v2"));
        assertEquals("v1", reg.require(1, 49).table());   // 边界：恰在切换前
        assertEquals("v2", reg.require(1, 50).table());   // 边界：恰在切换 seq 上
        assertEquals("v2", reg.require(1, 999).table());
    }

    @Test
    void missingOrFutureVersionFailsFast() {
        reg.accept(10, rel(1, "v1"));
        assertThrows(IllegalStateException.class, () -> reg.require(2, 99));   // 无版本
        assertThrows(IllegalStateException.class, () -> reg.require(1, 9));    // 全部在未来
    }

    @Test
    void pruneKeepsAtLeastLatestAndExactBoundary() {
        reg.accept(10, rel(1, "v1")); reg.accept(50, rel(1, "v2"));
        reg.pruneBelow(50);
        assertEquals("v2", reg.require(1, 60).table());
        assertThrows(IllegalStateException.class, () -> reg.require(1, 49));
        reg.pruneBelow(10_000);                       // 过度剪枝保护
        assertEquals("v2", reg.require(1, 10_001).table());
    }

    @Test
    void inheritedLatestViewForLegacyContract() {
        reg.accept(10, rel(1, "v1")); reg.accept(50, rel(1, "v2"));
        assertEquals("v2", reg.require(1).table());   // 旧签名 = 最新版
    }
}
