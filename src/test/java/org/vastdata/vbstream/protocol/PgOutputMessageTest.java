package org.vastdata.vbstream.protocol;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PgOutputMessageTest {

    @Test
    void logicalMsgContentValueEquality() {
        PgOutputMessage.LogicalMsg a = new PgOutputMessage.LogicalMsg(
                OptionalLong.empty(), true, 4L, "p", new byte[]{1, 2});
        PgOutputMessage.LogicalMsg b = new PgOutputMessage.LogicalMsg(
                OptionalLong.empty(), true, 4L, "p", new byte[]{1, 2});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void truncateRelationOidsValueEquality() {
        PgOutputMessage.Truncate a = new PgOutputMessage.Truncate(
                OptionalLong.of(7L), EnumSet.of(TruncateOption.CASCADE), new int[]{1, 2});
        PgOutputMessage.Truncate b = new PgOutputMessage.Truncate(
                OptionalLong.of(7L), EnumSet.of(TruncateOption.CASCADE), new int[]{1, 2});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
