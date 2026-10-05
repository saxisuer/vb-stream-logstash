package org.vastdata.vbstream.walsource;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** 模块接入冒烟：包存在且构建连通（真实断言留给后续任务，此处钉 groupId 约定）。 */
class ModuleBuildTest {
    @Test
    void modulePackageIsRooted() {
        assertEquals("org.vastdata.vbstream.walsource", ModuleBuildTest.class.getPackageName());
    }
}
