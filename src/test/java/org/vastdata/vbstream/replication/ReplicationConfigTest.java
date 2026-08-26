package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.vastdata.vbstream.protocol.StreamingMode;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationConfigTest {

    @AfterEach
    void cleanupSystemProperties() {
        System.clearProperty("vb.pg.host");
        System.clearProperty("vb.pg.port");
        System.clearProperty("vb.pg.slot");
        System.clearProperty("vb.pg.streaming");
    }

    @Test
    void defaultsTargetLocalComposeEnv() {
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        assertEquals("localhost", config.host());
        assertEquals(55432, config.port());
        assertEquals("vb_cdc_slot", config.slotName());
        assertEquals("vb_pub", config.publicationNames());
        assertEquals(4, config.protoVersion());
        assertEquals(StreamingMode.PARALLEL, config.streamingMode());
        assertEquals(true, config.twoPhase());
    }

    @Test
    void systemPropertiesOverrideDefaults() {
        System.setProperty("vb.pg.host", "db.example.com");
        System.setProperty("vb.pg.port", "6543");
        System.setProperty("vb.pg.slot", "s1");
        System.setProperty("vb.pg.streaming", "on");
        ReplicationConfig config = ReplicationConfig.fromSystemProperties();
        assertEquals("db.example.com", config.host());
        assertEquals(6543, config.port());
        assertEquals("s1", config.slotName());
        assertEquals(StreamingMode.ON, config.streamingMode());
    }

    @Test
    void buildsJdbcAndReplicationUrls() {
        ReplicationConfig config = new ReplicationConfig("h", 5432, "db", "u", "p",
                "slot", "pub", 4, StreamingMode.PARALLEL, true, 10);
        assertEquals("jdbc:postgresql://h:5432/db", config.jdbcUrl());
        assertEquals("jdbc:postgresql://h:5432/db?replication=database", config.replicationUrl());
        assertEquals("parallel", config.streamingParam());
    }
}
