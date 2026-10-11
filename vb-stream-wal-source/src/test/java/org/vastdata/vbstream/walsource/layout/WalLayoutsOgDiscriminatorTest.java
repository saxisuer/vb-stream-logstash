package org.vastdata.vbstream.walsource.layout;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WalLayouts.forServer(int, String) openGauss 判别器的失败先行测试。
 *
 * <p>判别背景：openGauss 全系版本报 server_version_num=90204（与 vanilla 9.2 撞号），
 * 唯一区分面是 version() 文本（实测 5.0.0 形如 "(openGauss 5.0.0 build ...) compiled
 * at ..."）。断言面：90204+openGauss 5.0.x 文本分发 OG 档；90204 无 openGauss 标记
 * （vanilla 9.2）ISE 拒绝；openGauss 其他主版本 ISE 附版本文本；V17/V18 分发不受
 * 新入口影响；旧 forServerVersion(int) 行为零变化（90204 仍 ISE）。</p>
 */
class WalLayoutsOgDiscriminatorTest {

    /** openGauss 5.0.0 容器实测 version() 形态（og-plan，2026-10-12）。 */
    private static final String OG_500_TEXT =
            "(openGauss 5.0.0 build ) compiled at 2023-04-18 09:47:39 commit 0 last mr"
                    + " on x86_64-unknown-linux-gnu, compiled by g++ (GCC) 7.3.0, 64-bit";

    /**
     * 90204 + openGauss 5.0.x 版本文本分发到 OG 描述符单例；openGauss 5.0.x 全区间
     * （90204 恒定）与 5.0.1 等小版本文本同达。
     */
    @Test
    void openGaussVersionTextDispatchesToOgLayout() {
        assertSame(WalLayoutVOpenGauss.INSTANCE, WalLayouts.forServer(90204, OG_500_TEXT));
        assertSame(WalLayoutVOpenGauss.INSTANCE,
                WalLayouts.forServer(90204, "(openGauss 5.0.1 build de3a5d15) compiled at 2024-01-02"));
    }

    /**
     * 90204 无 openGauss 标记＝vanilla PG 9.2：ISE 拒绝且消息含"非目标"与版本文本
     * （vanilla 9.2 无复制槽/无 flush LSN 函数/pgjdbc 拒 replication 参数，四重硬阻塞）；
     * versionText 为 null 同走拒绝面（判别信息缺失不得静默猜测）。
     */
    @Test
    void vanillaPg92VersionNumberIsRejectedAsNonTarget() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> WalLayouts.forServer(90204, "PostgreSQL 9.2.4 on x86_64-pc-linux-gnu"));
        assertTrue(ex.getMessage().contains("非目标"), ex.getMessage());
        assertTrue(ex.getMessage().contains("PostgreSQL 9.2.4"), ex.getMessage());
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServer(90204, null));
    }

    /** openGauss 其他主版本（6.x/4.x 等）ISE，消息携带版本文本便于排障。 */
    @Test
    void unsupportedOpenGaussMajorIsRejectedWithVersionText() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> WalLayouts.forServer(90204, "(openGauss 6.0.0 build 1234) compiled at 2025-01-01"));
        assertTrue(ex.getMessage().contains("openGauss"), ex.getMessage());
        assertTrue(ex.getMessage().contains("6.0.0"), ex.getMessage());
    }

    /** V17/V18 分发不受新入口影响：带任意版本文本仍按版本号区间路由。 */
    @Test
    void legacyMajorsDispatchUnchangedThroughNewEntryPoint() {
        assertSame(WalLayoutV18.INSTANCE, WalLayouts.forServer(180002, "PostgreSQL 18.2"));
        assertSame(WalLayoutV17.INSTANCE, WalLayouts.forServer(170005, "PostgreSQL 17.5"));
        assertEquals(18, WalLayouts.forServer(189999, null).majorVersion());
    }

    /**
     * 完全未知的大版本在新入口同样 ISE（消息含版本号），与旧入口拒绝语义一致；
     * 旧入口 forServerVersion(int) 行为零变化——90204 在无判别器的旧入口仍 ISE
     * （openGauss 只能经 forServer(int, String) 显式判别到达，防误路由）。
     */
    @Test
    void unknownMajorsAndLegacyEntryPointKeepZeroChangeSemantics() {
        IllegalStateException viaNew = assertThrows(IllegalStateException.class,
                () -> WalLayouts.forServer(160000, "PostgreSQL 16.9"));
        assertTrue(viaNew.getMessage().contains("160000"), viaNew.getMessage());
        assertThrows(IllegalStateException.class, () -> WalLayouts.forServerVersion(90204));
        assertEquals(18, WalLayouts.forServerVersion(180000).majorVersion());
        assertEquals(17, WalLayouts.forServerVersion(179999).majorVersion());
    }
}
