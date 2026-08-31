package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.Column;
import org.vastdata.vbstream.protocol.PgOutputMessage;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationRegistryTest {

    private static PgOutputMessage.Relation relation(int oid, String table) {
        return new PgOutputMessage.Relation(OptionalLong.empty(), oid, "public", table,
                'd', List.of(new Column("id", 23, -1, true)));
    }

    @Test
    void cachesLatestRelationByOid() {
        RelationRegistry registry = new RelationRegistry();
        registry.accept(relation(100, "t_a"));
        registry.accept(relation(200, "t_b"));
        registry.accept(relation(100, "t_a_v2")); // 同 oid 再下发即定义变化
        assertEquals("t_a_v2", registry.require(100).table());
        assertTrue(registry.find(999).isEmpty());
    }

    @Test
    void ignoresNonRelationMessages() {
        RelationRegistry registry = new RelationRegistry();
        registry.accept(new PgOutputMessage.Begin(1, java.time.Instant.EPOCH, 2));
        assertTrue(registry.find(1).isEmpty());
    }

    @Test
    void requireMissingOidFailsFast() {
        RelationRegistry registry = new RelationRegistry();
        assertThrows(IllegalStateException.class, () -> registry.require(123));
    }
}
