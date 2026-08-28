package org.vastdata.vbstream.replication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** capFeedback 纯函数单测：前沿 ≤0 视为无 cap（首个事务输出前与 1.6 行为一致）、否则取 min。 */
class PgReplicationSessionTest {

    /** 前沿 ≤0（尚未有任何事务输出，含负数防御值）不封顶：反馈值原样返回已收到的 LSN。 */
    @Test
    void zeroOrNegativeFrontierMeansNoCap() {
        assertEquals(500L, PgReplicationSession.capFeedback(500L, 0L));
        assertEquals(500L, PgReplicationSession.capFeedback(500L, -1L));
    }

    /** 正前沿取 min 封顶；前沿不会超过已收到（防御性取 min，超过时返回已收到值——不得确认未收到的位点）。 */
    @Test
    void positiveFrontierCapsToMinimum() {
        assertEquals(300L, PgReplicationSession.capFeedback(500L, 300L));
        assertEquals(300L, PgReplicationSession.capFeedback(300L, 500L));  // 前沿不会超过已收到，防御性取 min
    }
}
